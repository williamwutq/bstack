//! Runtime Type Information (RTTI) — a persisted, self-describing schema stack.
//!
//! RTTI lets a **general program** read and interpret `bstack_raii` structures on
//! disk with **no compiled-in Rust types** — just `bstack_raii::rtti::…`. It is a
//! path *parallel* to the static macro-generated one; code that never opts in
//! never dispatches here.
//!
//! ## Model (see `BYTECODE.md` for the exact wire format)
//!
//! The RTTI file **is a plain [`bstack::BStack`]**: a sequence of records, one per
//! registered type, each appended with a single [`BStack::push`] (atomic +
//! crash-safe by bstack's own contract — no allocator, no WAL, no hand-rolled
//! header). A type's identity on the wire is its **ordinal** in that stack (the
//! order it was appended); because the stack is append-only, an ordinal is never
//! renumbered, so it is a stable, compact, `u32`-sized handle that resolves to the
//! full [`EightCC`] and descriptor by a direct array index.
//!
//! On open the whole stack is scanned into memory ([`RttiRegistry`]) so every
//! lookup is O(1). The scanned *structure* is immutable and safe to cache; a
//! **mutable class-variable value**, however, must be read live from the bstack —
//! another handle can rewrite its fixed-size slot in place.
//!
//! ## Status
//!
//! Skeleton. The load path ([`RttiRegistry::open`] → record scan → ordinal / tag
//! maps) is in place, and the on-disk RTTI-typed pointer is the existing
//! [`ForeignRepr`] (a foreign pointer already carries a `type_index` — see its
//! docs). Parsing a record body into fields/variants/shapes, the `#[bstack_class]`
//! emitter, `sync()`, and the interpreter are TODO.

use std::collections::HashMap;
use std::io;
use std::path::Path;

use bstack::BStack;

use crate::foreign::ForeignRepr;
use crate::layout::EightCC;

/// A type's stable identity within the one RTTI stack: its 0-based ordinal (the
/// order it was appended). Append-only ⇒ never renumbered.
pub type RttiOrdinal = u32;

// -- On-disk framing (mirrors BYTECODE.md) ---------------------------------

/// Bytes of a record's framing header: `eightcc[8] + body_len:u32 + _pad:u32`,
/// after which the `TypeDesc` body begins (8-aligned).
const RECORD_HEADER_LEN: u64 = 16;

/// Bytes of the fixed `TypeDesc` prefix, before the variable-length type name.
const TYPEDESC_HEAD_LEN: u64 = 18;

const FLAG_ENUM: u8 = 0b0000_0001;
#[allow(dead_code)] // consumed once the interpreter handles rc targets
const FLAG_RC: u8 = 0b0000_0010;
#[allow(dead_code)] // consumed once the interpreter handles rc,weak targets
const FLAG_WEAK: u8 = 0b0000_0100;

#[inline]
fn align8(n: u64) -> u64 {
    (n + 7) & !7
}

/// Build an RTTI-typed pointer: a [`ForeignRepr`] to `(file_id, offset)` tagged
/// with `ordinal`. `file_id == 0` ⇒ `SELF`. For an untyped pointer (type recovered
/// from the target block header on deref) use [`ForeignRepr::new`] directly.
pub fn typed_ptr(file_id: u64, offset: u64, ordinal: RttiOrdinal) -> ForeignRepr {
    ForeignRepr::new(file_id, offset).with_type_index(ordinal + 1)
}

// -- Parsed, in-memory schema (structure only) -----------------------------

/// Whether a record describes a `struct` or an `enum`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RttiKind {
    Struct,
    Enum,
}

/// The info-complex node — a field's type structure, its leaves carrying the RAII
/// kind the interpreter dispatches on. Parsing from the wire is TODO.
#[derive(Clone, Debug)]
pub enum Shape {
    Pod {
        width: u32,
    },
    Owned(EightCC),
    Strong(EightCC),
    Weak(EightCC),
    Ref(EightCC),
    Embed(EightCC),
    Foreign(EightCC),
    Option(Box<Shape>),
    Array {
        n: u32,
        inner: Box<Shape>,
    },
    Vec(Box<Shape>),
    Tuple(Vec<Shape>),
    /// A class variable stored inline in the record. `value` is read live for the
    /// mutable case (`mutable == true`), snapshotted here for the const case.
    Class {
        mutable: bool,
        inner: Box<Shape>,
        value: Vec<u8>,
    },
}

/// One field of a type: its name, its absolute byte offset within the block's
/// `OnDisk` (unused for `CLASS` fields), and its shape.
#[derive(Clone, Debug)]
pub struct RttiField {
    pub name: String,
    pub offset: u32,
    pub shape: Shape,
}

/// A parsed type descriptor. Structure only — mutable class-variable *values* are
/// read live from the stack, never cached here.
#[derive(Clone, Debug)]
pub struct RttiType {
    pub tag: EightCC,
    pub name: String,
    pub kind: RttiKind,
    pub ondisk_size: u64,
    /// Struct fields (for an enum, the per-variant fields are TODO).
    pub fields: Vec<RttiField>,
}

// -- The in-memory registry ------------------------------------------------

/// A scanned RTTI record: its tag and where its framing header begins in the
/// stack. Ordinal = position in [`RttiRegistry::records`].
struct RecordRef {
    tag: EightCC,
    offset: u64,
}

/// The whole RTTI stack loaded into memory: the ordered records plus a
/// tag→ordinal index. Holds the open [`BStack`] so mutable class-variable values
/// can be read (and later written) live.
pub struct RttiRegistry {
    stack: BStack,
    records: Vec<RecordRef>,
    by_tag: HashMap<EightCC, RttiOrdinal>,
}

impl RttiRegistry {
    /// Open an existing RTTI stack and scan every record into memory.
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let stack = BStack::open(path)?;
        let mut reg = Self {
            stack,
            records: Vec::new(),
            by_tag: HashMap::new(),
        };
        reg.scan()?;
        Ok(reg)
    }

    /// Walk the stack front-to-back, recording each record's tag and offset and
    /// building the tag→ordinal map. A repeated tag is corruption (eightcc is the
    /// resolution key; two distinct types must never share one).
    fn scan(&mut self) -> io::Result<()> {
        let len = self.stack.len()?;
        let mut off = 0u64;
        while off < len {
            let mut header = [0u8; RECORD_HEADER_LEN as usize];
            self.stack.get_into(off, &mut header)?;
            let tag = EightCC(header[0..8].try_into().unwrap());
            let body_len = u32::from_le_bytes(header[8..12].try_into().unwrap());

            let ordinal = self.records.len() as RttiOrdinal;
            if self.by_tag.insert(tag, ordinal).is_some() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "[BSTACK0700] duplicate RTTI eightcc — two types share one tag",
                ));
            }
            self.records.push(RecordRef { tag, offset: off });

            off += align8(RECORD_HEADER_LEN + body_len as u64);
        }
        Ok(())
    }

    /// Number of registered types.
    pub fn len(&self) -> usize {
        self.records.len()
    }

    /// Whether no types are registered.
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    /// The ordinal a tag resolves to, if present.
    pub fn ordinal_of(&self, tag: EightCC) -> Option<RttiOrdinal> {
        self.by_tag.get(&tag).copied()
    }

    /// The tag at an ordinal, if in range.
    pub fn tag_of(&self, ordinal: RttiOrdinal) -> Option<EightCC> {
        self.records.get(ordinal as usize).map(|r| r.tag)
    }

    /// Resolve a pointer's `type_index` to a live ordinal. `None` for an untyped
    /// pointer or an out-of-range index.
    pub fn resolve_ptr(&self, ptr: ForeignRepr) -> Option<RttiOrdinal> {
        let ord = ptr.rtti_ordinal()?;
        ((ord as usize) < self.records.len()).then_some(ord)
    }

    /// Parse the full descriptor for a type. Currently reads the fixed `TypeDesc`
    /// prefix + name; field/variant/shape parsing is TODO.
    pub fn load_type(&self, ordinal: RttiOrdinal) -> io::Result<RttiType> {
        let rec = self.records.get(ordinal as usize).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                "[BSTACK0701] RTTI ordinal out of range",
            )
        })?;
        let body = rec.offset + RECORD_HEADER_LEN;

        let mut head = [0u8; TYPEDESC_HEAD_LEN as usize];
        self.stack.get_into(body, &mut head)?;
        let flags = head[0];
        let name_len = u16::from_le_bytes(head[2..4].try_into().unwrap()) as usize;
        let ondisk_size = u64::from_le_bytes(head[10..18].try_into().unwrap());
        let kind = if flags & FLAG_ENUM != 0 {
            RttiKind::Enum
        } else {
            RttiKind::Struct
        };

        let mut name_buf = vec![0u8; name_len];
        self.stack
            .get_into(body + TYPEDESC_HEAD_LEN, &mut name_buf)?;
        let name = String::from_utf8(name_buf).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "[BSTACK0702] RTTI type name is not UTF-8",
            )
        })?;

        Ok(RttiType {
            tag: rec.tag,
            name,
            kind,
            ondisk_size,
            // TODO: parse Field[count] / Variant[count] + their Shape blobs per
            // BYTECODE.md, starting at `body + align8(TYPEDESC_HEAD_LEN + name_len)`.
            fields: Vec::new(),
        })
    }
}
