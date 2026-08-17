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
//! ## Codec
//!
//! [`encode_type`] / [`decode_type`] are the symmetric serialize / deserialize
//! pair for a type's record body; [`RttiRegistry::append`] frames + `push`es one,
//! [`RttiRegistry::load_type`] reads + decodes one. The `Shape` grammar parses
//! recursively — safe because a shape's depth is bounded by the *source type*
//! nesting, never by data depth (the data walk, added later, is the one that must
//! stay non-recursive).
//!
//! ## Status
//!
//! Read + write of struct and enum records is in place; the on-disk RTTI-typed
//! pointer is the existing [`ForeignRepr`] (its `type_index` is the ordinal `+ 1`).
//! The `#[bstack_class]` macro fills [`RTTI_TYPES`] at link time, and [`sync`]
//! appends every missing schema to a file. [`RttiRegistry::read_value`] /
//! [`RttiRegistry::read_ptr`] are the non-recursive **read interpreter** — they walk
//! the schema over a live data file into a [`Value`] tree with no compiled-in types.
//! Still TODO: the *mutating* interpreter (set / teardown / clone) and live
//! mutable-class-variable writes.

use std::collections::HashMap;
use std::io;
use std::path::Path;

use bstack::BStack;
use linkme::distributed_slice;

use crate::foreign::ForeignRepr;
use crate::layout::EightCC;

/// Re-exports so the `#[bstack_class]` macro's generated registration code can name
/// `linkme` without the downstream crate depending on it directly. The generated
/// element uses `#[linkme(crate = ::bstack_raii::rtti::linkme)]` to override
/// linkme's hard-coded `::linkme` path against this re-export.
#[doc(hidden)]
pub use linkme;
#[doc(hidden)]
pub use linkme::distributed_slice as distributed_slice_reexport;

/// A type's stable identity within the one RTTI stack: its 0-based ordinal (the
/// order it was appended). Append-only ⇒ never renumbered.
pub type RttiOrdinal = u32;

// -- On-disk framing (mirrors BYTECODE.md) ---------------------------------

/// Bytes of a record's framing header: `eightcc[8] + body_len:u32 + _pad:u32`,
/// after which the `TypeDesc` body begins (8-aligned).
const RECORD_HEADER_LEN: u64 = 16;

const FLAG_ENUM: u8 = 0b0000_0001;
const FLAG_RC: u8 = 0b0000_0010;
const FLAG_WEAK: u8 = 0b0000_0100;

mod shape_tag {
    pub const POD: u8 = 0x00;
    pub const OWNED: u8 = 0x01;
    pub const STRONG: u8 = 0x02;
    pub const WEAK: u8 = 0x03;
    pub const REF: u8 = 0x04;
    pub const EMBED: u8 = 0x05;
    pub const FOREIGN: u8 = 0x06;
    pub const OPTION: u8 = 0x10;
    pub const ARRAY: u8 = 0x11;
    pub const VEC: u8 = 0x12;
    pub const TUPLE: u8 = 0x13;
    pub const CLASS: u8 = 0x20;
}

#[inline]
fn align8(n: u64) -> u64 {
    (n + 7) & !7
}

fn corrupt(msg: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg.into())
}

/// Build an RTTI-typed pointer: a [`ForeignRepr`] to `(file_id, offset)` tagged
/// with `ordinal`. `file_id == 0` ⇒ `SELF`. For an untyped pointer (type recovered
/// from the target block header on deref) use [`ForeignRepr::new`] directly.
pub fn typed_ptr(file_id: u64, offset: u64, ordinal: RttiOrdinal) -> ForeignRepr {
    ForeignRepr::new(file_id, offset).with_type_index(ordinal + 1)
}

// -- Little-endian cursor codec --------------------------------------------

/// Append-only little-endian writer over a growing byte buffer.
#[derive(Default)]
struct Writer {
    buf: Vec<u8>,
}

impl Writer {
    fn u8(&mut self, v: u8) {
        self.buf.push(v);
    }
    fn u16(&mut self, v: u16) {
        self.buf.extend_from_slice(&v.to_le_bytes());
    }
    fn u32(&mut self, v: u32) {
        self.buf.extend_from_slice(&v.to_le_bytes());
    }
    fn u64(&mut self, v: u64) {
        self.buf.extend_from_slice(&v.to_le_bytes());
    }
    fn i64(&mut self, v: i64) {
        self.buf.extend_from_slice(&v.to_le_bytes());
    }
    fn eightcc(&mut self, v: EightCC) {
        self.buf.extend_from_slice(&v.0);
    }
    fn bytes(&mut self, b: &[u8]) {
        self.buf.extend_from_slice(b);
    }
    /// Pad with zero bytes up to the next `a`-byte boundary (`a` a power of two).
    fn align(&mut self, a: usize) {
        while !self.buf.len().is_multiple_of(a) {
            self.buf.push(0);
        }
    }
}

/// Bounds-checked little-endian reader over a byte slice.
struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }
    fn take(&mut self, n: usize) -> io::Result<&'a [u8]> {
        let end = self
            .pos
            .checked_add(n)
            .filter(|&e| e <= self.buf.len())
            .ok_or_else(|| corrupt("[BSTACK0804] truncated RTTI record"))?;
        let s = &self.buf[self.pos..end];
        self.pos = end;
        Ok(s)
    }
    fn u8(&mut self) -> io::Result<u8> {
        Ok(self.take(1)?[0])
    }
    fn u16(&mut self) -> io::Result<u16> {
        Ok(u16::from_le_bytes(self.take(2)?.try_into().unwrap()))
    }
    fn u32(&mut self) -> io::Result<u32> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn u64(&mut self) -> io::Result<u64> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    fn i64(&mut self) -> io::Result<i64> {
        Ok(i64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    fn eightcc(&mut self) -> io::Result<EightCC> {
        Ok(EightCC(self.take(8)?.try_into().unwrap()))
    }
    fn string(&mut self, n: usize) -> io::Result<String> {
        String::from_utf8(self.take(n)?.to_vec())
            .map_err(|_| corrupt("[BSTACK0802] RTTI name is not valid UTF-8"))
    }
    /// Skip zero-padding up to the next `a`-byte boundary.
    fn align(&mut self, a: usize) -> io::Result<()> {
        let aligned = (self.pos + a - 1) & !(a - 1);
        if aligned > self.buf.len() {
            return Err(corrupt("[BSTACK0804] truncated RTTI record (alignment)"));
        }
        self.pos = aligned;
        Ok(())
    }
}

// -- Parsed, in-memory schema (structure only) -----------------------------

/// The info-complex node — a field's type structure, its leaves carrying the RAII
/// kind the interpreter dispatches on.
#[derive(Clone, Debug, PartialEq)]
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
    /// A class variable stored inline in the record. For the const case the bytes
    /// are the snapshot here; for the mutable case they are the initial value (the
    /// live value is read from the stack at the field's slot).
    Class {
        mutable: bool,
        inner: Box<Shape>,
        value: Vec<u8>,
    },
}

impl Shape {
    fn encode(&self, w: &mut Writer) {
        use shape_tag as t;
        match self {
            Shape::Pod { width } => {
                w.u8(t::POD);
                w.u32(*width);
            }
            Shape::Owned(cc) => {
                w.u8(t::OWNED);
                w.eightcc(*cc);
            }
            Shape::Strong(cc) => {
                w.u8(t::STRONG);
                w.eightcc(*cc);
            }
            Shape::Weak(cc) => {
                w.u8(t::WEAK);
                w.eightcc(*cc);
            }
            Shape::Ref(cc) => {
                w.u8(t::REF);
                w.eightcc(*cc);
            }
            Shape::Embed(cc) => {
                w.u8(t::EMBED);
                w.eightcc(*cc);
            }
            Shape::Foreign(cc) => {
                w.u8(t::FOREIGN);
                w.eightcc(*cc);
            }
            Shape::Option(inner) => {
                w.u8(t::OPTION);
                inner.encode(w);
            }
            Shape::Array { n, inner } => {
                w.u8(t::ARRAY);
                w.u32(*n);
                inner.encode(w);
            }
            Shape::Vec(inner) => {
                w.u8(t::VEC);
                inner.encode(w);
            }
            Shape::Tuple(items) => {
                w.u8(t::TUPLE);
                w.u8(items.len() as u8);
                for it in items {
                    it.encode(w);
                }
            }
            Shape::Class {
                mutable,
                inner,
                value,
            } => {
                w.u8(t::CLASS);
                w.u8(u8::from(*mutable));
                inner.encode(w);
                w.u32(value.len() as u32);
                w.bytes(value);
            }
        }
    }

    fn decode(r: &mut Reader) -> io::Result<Shape> {
        use shape_tag as t;
        let tag = r.u8()?;
        Ok(match tag {
            t::POD => Shape::Pod { width: r.u32()? },
            t::OWNED => Shape::Owned(r.eightcc()?),
            t::STRONG => Shape::Strong(r.eightcc()?),
            t::WEAK => Shape::Weak(r.eightcc()?),
            t::REF => Shape::Ref(r.eightcc()?),
            t::EMBED => Shape::Embed(r.eightcc()?),
            t::FOREIGN => Shape::Foreign(r.eightcc()?),
            t::OPTION => Shape::Option(Box::new(Shape::decode(r)?)),
            t::ARRAY => {
                let n = r.u32()?;
                Shape::Array {
                    n,
                    inner: Box::new(Shape::decode(r)?),
                }
            }
            t::VEC => Shape::Vec(Box::new(Shape::decode(r)?)),
            t::TUPLE => {
                let k = r.u8()? as usize;
                let mut items = Vec::with_capacity(k);
                for _ in 0..k {
                    items.push(Shape::decode(r)?);
                }
                Shape::Tuple(items)
            }
            t::CLASS => {
                let mutable = r.u8()? != 0;
                let inner = Box::new(Shape::decode(r)?);
                let value_len = r.u32()? as usize;
                let value = r.take(value_len)?.to_vec();
                Shape::Class {
                    mutable,
                    inner,
                    value,
                }
            }
            other => {
                return Err(corrupt(format!(
                    "[BSTACK0803] unknown RTTI shape tag {other:#04x}"
                )));
            }
        })
    }
}

/// One field of a type: its name, its absolute byte offset within the block's
/// `OnDisk` (unused for `CLASS` fields), and its shape.
#[derive(Clone, Debug, PartialEq)]
pub struct RttiField {
    pub name: String,
    pub offset: u32,
    pub shape: Shape,
}

impl RttiField {
    fn encode(&self, w: &mut Writer) {
        let mut sw = Writer::default();
        self.shape.encode(&mut sw);
        let name = self.name.as_bytes();
        w.u32(self.offset);
        w.u16(name.len() as u16);
        w.u16(sw.buf.len() as u16);
        w.bytes(name);
        w.align(4); // name pad → shape 4-aligned
        w.bytes(&sw.buf);
        w.align(4); // end pad → next field 4-aligned
    }

    fn decode(r: &mut Reader) -> io::Result<RttiField> {
        let offset = r.u32()?;
        let name_len = r.u16()? as usize;
        let shape_len = r.u16()? as usize;
        let name = r.string(name_len)?;
        r.align(4)?;
        let shape_start = r.pos;
        let shape = Shape::decode(r)?;
        if r.pos - shape_start != shape_len {
            return Err(corrupt("[BSTACK0805] RTTI field shape length mismatch"));
        }
        r.align(4)?;
        Ok(RttiField {
            name,
            offset,
            shape,
        })
    }
}

/// One variant of an enum: its name, discriminant value, and fields (offsets
/// relative to the payload).
#[derive(Clone, Debug, PartialEq)]
pub struct RttiVariant {
    pub name: String,
    pub disc_value: i64,
    pub fields: Vec<RttiField>,
}

impl RttiVariant {
    fn encode(&self, w: &mut Writer) {
        w.align(8); // each variant is 8-aligned
        w.i64(self.disc_value);
        let name = self.name.as_bytes();
        w.u16(name.len() as u16);
        w.u16(self.fields.len() as u16);
        w.u32(0); // _pad
        w.bytes(name);
        w.align(8); // name pad → fields aligned
        for f in &self.fields {
            f.encode(w);
        }
    }

    fn decode(r: &mut Reader) -> io::Result<RttiVariant> {
        r.align(8)?;
        let disc_value = r.i64()?;
        let name_len = r.u16()? as usize;
        let field_count = r.u16()? as usize;
        let _pad = r.u32()?;
        let name = r.string(name_len)?;
        r.align(8)?;
        let mut fields = Vec::with_capacity(field_count);
        for _ in 0..field_count {
            fields.push(RttiField::decode(r)?);
        }
        Ok(RttiVariant {
            name,
            disc_value,
            fields,
        })
    }
}

/// The struct fields or enum variants of a type.
#[derive(Clone, Debug, PartialEq)]
pub enum RttiBody {
    Struct(Vec<RttiField>),
    Enum(RttiEnum),
}

/// The enum-specific header + variants.
#[derive(Clone, Debug, PartialEq)]
pub struct RttiEnum {
    pub disc_width: u8,
    pub disc_off: u16,
    pub payload_off: u16,
    pub variants: Vec<RttiVariant>,
}

/// A parsed type descriptor. Structure only — mutable class-variable *values* are
/// read live from the stack, never cached here.
#[derive(Clone, Debug, PartialEq)]
pub struct RttiType {
    pub tag: EightCC,
    pub name: String,
    /// Target has an inline refcount (`rc` mode) — how a `Strong` field to it is
    /// bumped on clone.
    pub rc: bool,
    /// Target has a control block (`rc, weak` mode).
    pub weak: bool,
    pub ondisk_size: u64,
    pub body: RttiBody,
}

/// Serialize a type's record **body** (the `TypeDesc`, without the record framing).
pub fn encode_type(ty: &RttiType) -> Vec<u8> {
    let mut w = Writer::default();

    let (flags_kind, disc_width, count, disc_off, payload_off) = match &ty.body {
        RttiBody::Struct(fields) => (0u8, 0u8, fields.len() as u16, 0u16, 0u16),
        RttiBody::Enum(e) => (
            FLAG_ENUM,
            e.disc_width,
            e.variants.len() as u16,
            e.disc_off,
            e.payload_off,
        ),
    };
    let mut flags = flags_kind;
    if ty.rc {
        flags |= FLAG_RC;
    }
    if ty.weak {
        flags |= FLAG_WEAK;
    }

    let name = ty.name.as_bytes();
    w.u8(flags);
    w.u8(disc_width);
    w.u16(name.len() as u16);
    w.u16(count);
    w.u16(disc_off);
    w.u16(payload_off);
    w.u64(ty.ondisk_size);
    w.bytes(name);
    w.align(8); // name pad → body 8-aligned

    match &ty.body {
        RttiBody::Struct(fields) => {
            for f in fields {
                f.encode(&mut w);
            }
        }
        RttiBody::Enum(e) => {
            for v in &e.variants {
                v.encode(&mut w);
            }
        }
    }
    w.buf
}

/// Deserialize a type's record **body** back into an [`RttiType`], given its tag
/// (which lives in the record framing, not the body).
pub fn decode_type(tag: EightCC, body: &[u8]) -> io::Result<RttiType> {
    let mut r = Reader::new(body);
    let flags = r.u8()?;
    let disc_width = r.u8()?;
    let name_len = r.u16()? as usize;
    let count = r.u16()? as usize;
    let disc_off = r.u16()?;
    let payload_off = r.u16()?;
    let ondisk_size = r.u64()?;
    let name = r.string(name_len)?;
    r.align(8)?;

    let body = if flags & FLAG_ENUM != 0 {
        let mut variants = Vec::with_capacity(count);
        for _ in 0..count {
            variants.push(RttiVariant::decode(&mut r)?);
        }
        RttiBody::Enum(RttiEnum {
            disc_width,
            disc_off,
            payload_off,
            variants,
        })
    } else {
        let mut fields = Vec::with_capacity(count);
        for _ in 0..count {
            fields.push(RttiField::decode(&mut r)?);
        }
        RttiBody::Struct(fields)
    };

    Ok(RttiType {
        tag,
        name,
        rc: flags & FLAG_RC != 0,
        weak: flags & FLAG_WEAK != 0,
        ondisk_size,
        body,
    })
}

/// Frame a body into a full record: `eightcc + body_len + _pad + body`, padded to 8.
fn encode_record(ty: &RttiType) -> Vec<u8> {
    let body = encode_type(ty);
    let mut w = Writer::default();
    w.eightcc(ty.tag);
    w.u32(body.len() as u32);
    w.u32(0); // _pad
    w.bytes(&body);
    w.align(8); // whole record 8-aligned
    w.buf
}

// -- Compile-time registration (linkme) ------------------------------------

/// One compiled-in type's link-time registration: a builder for its parsed
/// schema descriptor. The `#[bstack_class]` macro emits exactly one of these per
/// type into [`RTTI_TYPES`]; [`sync`] walks the slice and appends any missing
/// schema to the file.
pub struct RttiRegistration {
    /// Builds the type's descriptor. Allocates (`String`/`Vec`); called once per
    /// type at [`sync`] time, never on a hot path.
    pub build: fn() -> RttiType,
}

/// The set of every `#[bstack_class]` type compiled into this binary, collected
/// at **link time** via `linkme` — no life-before-main (unlike `inventory`'s
/// `ctor`), no instantiation required, no hand-enumeration. Each entry is emitted
/// by the macro. [`sync`] is the sole consumer.
#[distributed_slice]
pub static RTTI_TYPES: [RttiRegistration];

/// Open (creating if absent) the RTTI file at `path`, append every compiled-in
/// schema it does not already carry, and return the loaded registry. Idempotent
/// and safe to call on every open. Runs eightcc-collision detection. This is the
/// producer-side entry point; see [`RttiRegistry::sync_compiled`] for the details.
pub fn sync(path: impl AsRef<Path>) -> io::Result<RttiRegistry> {
    let mut reg = RttiRegistry::open(path)?;
    reg.sync_compiled()?;
    Ok(reg)
}

// -- The in-memory registry ------------------------------------------------

/// A scanned RTTI record: its tag, where its framing header begins in the stack,
/// and its body length. Ordinal = position in [`RttiRegistry::records`].
struct RecordRef {
    tag: EightCC,
    offset: u64,
    body_len: u32,
}

/// The whole RTTI stack loaded into memory: the ordered records plus a
/// tag→ordinal index. Holds the open [`BStack`] so mutable class-variable values
/// can be read (and later written) live, and so new types can be appended.
pub struct RttiRegistry {
    stack: BStack,
    records: Vec<RecordRef>,
    by_tag: HashMap<EightCC, RttiOrdinal>,
}

impl RttiRegistry {
    /// Open (creating if absent) an RTTI stack and scan every record into memory.
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

    /// Walk the stack front-to-back, recording each record's tag / offset / length
    /// and building the tag→ordinal map. A repeated tag is corruption (eightcc is
    /// the resolution key; two distinct types must never share one).
    fn scan(&mut self) -> io::Result<()> {
        let len = self.stack.len()?;
        let mut off = 0u64;
        while off < len {
            let mut header = [0u8; RECORD_HEADER_LEN as usize];
            self.stack.get_into(off, &mut header)?;
            let tag = EightCC(header[0..8].try_into().unwrap());
            let body_len = u32::from_le_bytes(header[8..12].try_into().unwrap());
            self.index(tag, off, body_len)?;
            off += align8(RECORD_HEADER_LEN + body_len as u64);
        }
        Ok(())
    }

    /// Record one scanned/appended entry, rejecting a duplicate tag.
    fn index(&mut self, tag: EightCC, offset: u64, body_len: u32) -> io::Result<RttiOrdinal> {
        let ordinal = self.records.len() as RttiOrdinal;
        if self.by_tag.insert(tag, ordinal).is_some() {
            return Err(corrupt(
                "[BSTACK0800] duplicate RTTI eightcc — two types share one tag",
            ));
        }
        self.records.push(RecordRef {
            tag,
            offset,
            body_len,
        });
        Ok(ordinal)
    }

    /// Append a new type's record to the stack and index it. Errors if its tag is
    /// already registered.
    pub fn append(&mut self, ty: &RttiType) -> io::Result<RttiOrdinal> {
        if self.by_tag.contains_key(&ty.tag) {
            return Err(corrupt(
                "[BSTACK0800] duplicate RTTI eightcc — type already registered",
            ));
        }
        let body_len = encode_type(ty).len() as u32;
        let record = encode_record(ty);
        let offset = self.stack.push(&record)?;
        self.index(ty.tag, offset, body_len)
    }

    /// Append every compiled-in [`RTTI_TYPES`] descriptor this file is missing, in
    /// registration order. Idempotent: a type already present (matched by tag) is
    /// skipped. Returns the number of newly appended types.
    ///
    /// Runs **eightcc-collision detection** — the write-side guard: because a tag
    /// is the resolution key, two *distinct* types (different names) hashing to one
    /// eightcc is corruption, caught here rather than silently overwriting. A tag
    /// that is already on disk under the *same* name is a benign re-sync.
    pub fn sync_compiled(&mut self) -> io::Result<usize> {
        let mut appended = 0;
        // Guards a collision *within* the compiled-in set (two builders, one tag).
        let mut seen: HashMap<EightCC, String> = HashMap::new();
        for reg in RTTI_TYPES.iter() {
            let ty = (reg.build)();
            if let Some(prev) = seen.get(&ty.tag) {
                if *prev != ty.name {
                    return Err(corrupt(format!(
                        "[BSTACK0806] RTTI eightcc collision: '{prev}' and '{}' \
                         hash to one tag",
                        ty.name
                    )));
                }
                continue; // same type registered twice — nothing to do
            }
            seen.insert(ty.tag, ty.name.clone());

            match self.ordinal_of(ty.tag) {
                Some(ord) => {
                    // Already on disk: verify it is the same type, else collision.
                    let existing = self.load_type(ord)?;
                    if existing.name != ty.name {
                        return Err(corrupt(format!(
                            "[BSTACK0806] RTTI eightcc collision: on-disk '{}' vs \
                             compiled '{}' share one tag",
                            existing.name, ty.name
                        )));
                    }
                }
                None => {
                    self.append(&ty)?;
                    appended += 1;
                }
            }
        }
        Ok(appended)
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

    /// Read + decode the full descriptor for a type.
    pub fn load_type(&self, ordinal: RttiOrdinal) -> io::Result<RttiType> {
        let rec = self.records.get(ordinal as usize).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                "[BSTACK0801] RTTI ordinal out of range",
            )
        })?;
        let mut body = vec![0u8; rec.body_len as usize];
        self.stack
            .get_into(rec.offset + RECORD_HEADER_LEN, &mut body)?;
        decode_type(rec.tag, &body)
    }
}

// -- The read interpreter --------------------------------------------------

/// A structured value read out of a data file **with no compiled-in Rust type** —
/// the interpreter's output. Mirrors the [`Shape`] grammar. A reader (debugger,
/// generic serializer, repair tool) matches on this instead of a concrete type.
#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    /// Raw POD bytes (a leaf): the on-disk little-endian bytes, undecoded.
    Pod(Box<[u8]>),
    /// A followed child block (`owned` / `strong` / `embed`): its tag and named
    /// fields, in declaration order.
    Block {
        tag: EightCC,
        fields: Box<[(String, Value)]>,
    },
    /// A followed enum block: its tag, the active variant's name, and that variant's
    /// named fields.
    Enum {
        tag: EightCC,
        variant: String,
        fields: Box<[(String, Value)]>,
    },
    /// A reference that is **not** followed (`weak` / `ref` / cross-file `foreign`):
    /// the target's tag and the raw stored offset (`0` == null).
    Ref { tag: EightCC, offset: u64 },
    /// An absent nullable (`Option` niche `0`, or an empty/absent vector slot).
    Null,
    /// A present `Option`, wrapping its inner value.
    Some(Box<Value>),
    /// A fixed `[T; N]` array of elements.
    Array(Box<[Value]>),
    /// A dynamic `Vec<T>` / `String` of elements.
    Vec(Box<[Value]>),
    /// A tuple of elements.
    Tuple(Box<[Value]>),
    /// A class variable's value bytes (read from the **schema** record, not an
    /// instance — a class variable is not per-instance).
    Class(Box<[u8]>),
}

/// One step of the non-recursive walk. The interpreter runs a `work` stack of these
/// against a `results` value stack: leaf steps push a [`Value`]; an `Assemble*` step
/// pops the `n` values its children pushed and combines them into one.
enum Op {
    /// Read the block of type `ord` at `block_off` (its whole `OnDisk`, header + fields).
    Block {
        ord: RttiOrdinal,
        block_off: u64,
    },
    /// Interpret one shape at an absolute data offset.
    Shape {
        shape: Shape,
        offset: u64,
    },
    /// Pop `n` field values (child-first order) and assemble a struct block.
    MakeBlock {
        tag: EightCC,
        names: Vec<String>,
    },
    /// Pop `n` field values and assemble an enum block.
    MakeEnum {
        tag: EightCC,
        variant: String,
        names: Vec<String>,
    },
    /// Pop `n` values into an array / vec / tuple.
    MakeArray(usize),
    MakeVec(usize),
    MakeTuple(usize),
    /// Pop one value and wrap it in `Some`.
    MakeSome,
}

/// Bytes of a `VecDesc` (`data_off:u64` @0, `data_size:u64` @8) — the inline
/// descriptor of a persistent vector.
const VECDESC_LEN: u64 = 16;
/// A byte-vec data block's header (`len:u64` @0, `cap:u64` @8, elements from 16).
const BYTEVEC_HEADER: u64 = 16;
/// Bytes of a `ForeignRepr` on the wire.
const FOREIGN_REPR_LEN: u64 = 16;

fn read_u64_at(data: &BStack, off: u64) -> io::Result<u64> {
    let mut b = [0u8; 8];
    data.get_into(off, &mut b)?;
    Ok(u64::from_le_bytes(b))
}

impl RttiRegistry {
    /// Read a structure of type `ordinal` at `block_off` in `data` into a [`Value`]
    /// tree — the core RTTI operation: interpret an on-disk structure with no
    /// compiled-in Rust type.
    ///
    /// The walk is **non-recursive** (an explicit `work` stack), so arbitrarily deep
    /// or self-referential data cannot blow the call stack. It **follows** owning
    /// edges (`owned` / `strong` / `embed`) into child blocks, and **stops** at
    /// non-owning ones (`weak` / `ref` / `foreign`), recording just their offset —
    /// which also breaks any reference cycle. A node budget guards against a corrupt
    /// file describing an unterminated walk.
    pub fn read_value(
        &self,
        data: &BStack,
        ordinal: RttiOrdinal,
        block_off: u64,
    ) -> io::Result<Value> {
        let mut cache: HashMap<RttiOrdinal, RttiType> = HashMap::new();
        let mut work: Vec<Op> = vec![Op::Block {
            ord: ordinal,
            block_off,
        }];
        let mut results: Vec<Value> = Vec::new();
        // Bounds the total nodes visited: a corrupt schema/data pair (or a strong
        // cycle) can otherwise loop forever.
        let mut budget: u64 = 4_000_000;

        while let Some(op) = work.pop() {
            budget = budget.checked_sub(1).ok_or_else(|| {
                corrupt("[BSTACK0807] RTTI interpret budget exceeded (corrupt data or a cycle?)")
            })?;
            match op {
                Op::Block { ord, block_off } => {
                    if let std::collections::hash_map::Entry::Vacant(e) = cache.entry(ord) {
                        e.insert(self.load_type(ord)?);
                    }
                    let ty = &cache[&ord];
                    match &ty.body {
                        RttiBody::Struct(fields) => {
                            let names = fields.iter().map(|f| f.name.clone()).collect();
                            // Assemble marker first (popped last), then fields in
                            // order (so they pop child-first into the marker).
                            let field_ops: Vec<Op> = fields
                                .iter()
                                .map(|f| Op::Shape {
                                    shape: f.shape.clone(),
                                    offset: block_off + f.offset as u64,
                                })
                                .collect();
                            work.push(Op::MakeBlock { tag: ty.tag, names });
                            work.extend(field_ops);
                        }
                        RttiBody::Enum(e) => {
                            let raw = read_disc(data, block_off + e.disc_off as u64, e.disc_width)?;
                            let mask = disc_mask(e.disc_width);
                            let variant = e
                                .variants
                                .iter()
                                .find(|v| (v.disc_value as u64) & mask == raw)
                                .ok_or_else(|| {
                                    corrupt(format!(
                                        "[BSTACK0808] no RTTI variant for discriminant {raw}"
                                    ))
                                })?;
                            let names = variant.fields.iter().map(|f| f.name.clone()).collect();
                            let payload_base = block_off + e.payload_off as u64;
                            let field_ops: Vec<Op> = variant
                                .fields
                                .iter()
                                .map(|f| Op::Shape {
                                    shape: f.shape.clone(),
                                    offset: payload_base + f.offset as u64,
                                })
                                .collect();
                            work.push(Op::MakeEnum {
                                tag: ty.tag,
                                variant: variant.name.clone(),
                                names,
                            });
                            work.extend(field_ops);
                        }
                    }
                }

                Op::Shape { shape, offset } => match shape {
                    Shape::Pod { width } => {
                        let mut buf = vec![0u8; width as usize];
                        data.get_into(offset, &mut buf)?;
                        results.push(Value::Pod(buf.into()));
                    }
                    Shape::Class { value, .. } => {
                        // A class variable's value is schema-side, not per-instance.
                        results.push(Value::Class(value.into()));
                    }
                    Shape::Owned(tag) | Shape::Strong(tag) => {
                        let child = read_u64_at(data, offset)?;
                        if child == 0 {
                            results.push(Value::Null);
                        } else {
                            let ord = self.ordinal_of(tag).ok_or_else(unknown_tag)?;
                            work.push(Op::Block {
                                ord,
                                block_off: child,
                            });
                        }
                    }
                    Shape::Embed(tag) => {
                        // The child's whole OnDisk is inline at this slot (no pointer).
                        let ord = self.ordinal_of(tag).ok_or_else(unknown_tag)?;
                        work.push(Op::Block {
                            ord,
                            block_off: offset,
                        });
                    }
                    Shape::Weak(tag) | Shape::Ref(tag) => {
                        results.push(Value::Ref {
                            tag,
                            offset: read_u64_at(data, offset)?,
                        });
                    }
                    Shape::Foreign(tag) => {
                        // ForeignRepr { file_id:u32, type_index:u32, offset:u64 } — the
                        // address is the trailing u64; cross-file targets are not
                        // followed here.
                        let off = read_u64_at(data, offset + (FOREIGN_REPR_LEN - 8))?;
                        results.push(Value::Ref { tag, offset: off });
                    }
                    Shape::Option(inner) => {
                        // The niche: a `0` in the leading u64 of the slot is `None`.
                        if read_u64_at(data, offset)? == 0 {
                            results.push(Value::Null);
                        } else {
                            work.push(Op::MakeSome);
                            work.push(Op::Shape {
                                shape: *inner,
                                offset,
                            });
                        }
                    }
                    Shape::Array { n, inner } => {
                        let stride = self.shape_stride(&inner, &mut cache)?;
                        let elem_ops: Vec<Op> = (0..n as u64)
                            .map(|i| Op::Shape {
                                shape: (*inner).clone(),
                                offset: offset + i * stride,
                            })
                            .collect();
                        work.push(Op::MakeArray(n as usize));
                        work.extend(elem_ops);
                    }
                    Shape::Vec(inner) => {
                        let data_off = read_u64_at(data, offset)?; // VecDesc.data_off @0
                        if data_off == 0 {
                            results.push(Value::Vec(Box::default()));
                        } else {
                            let len = read_u64_at(data, data_off)?; // element count @0
                            let base = data_off + BYTEVEC_HEADER;
                            let stride = self.shape_stride(&inner, &mut cache)?;
                            let elem_ops: Vec<Op> = (0..len)
                                .map(|i| Op::Shape {
                                    shape: (*inner).clone(),
                                    offset: base + i * stride,
                                })
                                .collect();
                            work.push(Op::MakeVec(len as usize));
                            work.extend(elem_ops);
                        }
                    }
                    Shape::Tuple(items) => {
                        let mut elem_ops: Vec<Op> = Vec::with_capacity(items.len());
                        let mut off = offset;
                        for it in &items {
                            elem_ops.push(Op::Shape {
                                shape: it.clone(),
                                offset: off,
                            });
                            off += self.shape_stride(it, &mut cache)?;
                        }
                        work.push(Op::MakeTuple(items.len()));
                        work.extend(elem_ops);
                    }
                },

                Op::MakeBlock { tag, names } => {
                    let fields = pop_named(&mut results, &names)?;
                    results.push(Value::Block {
                        tag,
                        fields: fields.into(),
                    });
                }
                Op::MakeEnum {
                    tag,
                    variant,
                    names,
                } => {
                    let fields = pop_named(&mut results, &names)?;
                    results.push(Value::Enum {
                        tag,
                        variant,
                        fields: fields.into(),
                    });
                }
                Op::MakeArray(n) => {
                    let v = pop_n(&mut results, n)?;
                    results.push(Value::Array(v.into()));
                }
                Op::MakeVec(n) => {
                    let v = pop_n(&mut results, n)?;
                    results.push(Value::Vec(v.into()));
                }
                Op::MakeTuple(n) => {
                    let v = pop_n(&mut results, n)?;
                    results.push(Value::Tuple(v.into()));
                }
                Op::MakeSome => {
                    let inner = results
                        .pop()
                        .ok_or_else(|| corrupt("[BSTACK0809] RTTI interpret stack underflow"))?;
                    results.push(Value::Some(Box::new(inner)));
                }
            }
        }

        match results.len() {
            1 => Ok(results.pop().unwrap()),
            n => Err(corrupt(format!(
                "[BSTACK0809] RTTI interpret produced {n} values (expected 1)"
            ))),
        }
    }

    /// Read the structure a typed [`ForeignRepr`] points at, within `data`. The
    /// pointer must be **typed** (carry an RTTI ordinal) and refer to the current
    /// file (`file_id == 0`); cross-file resolution is a later phase.
    pub fn read_ptr(&self, data: &BStack, ptr: ForeignRepr) -> io::Result<Value> {
        let ord = self.resolve_ptr(ptr).ok_or_else(|| {
            corrupt("[BSTACK080A] cannot read an untyped / out-of-range RTTI pointer")
        })?;
        self.read_value(data, ord, ptr.offset())
    }

    /// The on-disk byte width of one element of `shape` — the stride for array / vec /
    /// tuple element addressing. References are a `u64` offset; a foreign is a
    /// `ForeignRepr`; an embedded child is its whole block; a vector is its inline
    /// `VecDesc`.
    fn shape_stride(
        &self,
        shape: &Shape,
        cache: &mut HashMap<RttiOrdinal, RttiType>,
    ) -> io::Result<u64> {
        Ok(match shape {
            Shape::Pod { width } => *width as u64,
            Shape::Owned(_) | Shape::Strong(_) | Shape::Weak(_) | Shape::Ref(_) => 8,
            Shape::Foreign(_) => FOREIGN_REPR_LEN,
            Shape::Vec(_) => VECDESC_LEN,
            Shape::Option(inner) => self.shape_stride(inner, cache)?,
            Shape::Array { n, inner } => *n as u64 * self.shape_stride(inner, cache)?,
            Shape::Tuple(items) => {
                let mut sum = 0u64;
                for it in items {
                    sum += self.shape_stride(it, cache)?;
                }
                sum
            }
            Shape::Embed(tag) => {
                let ord = self.ordinal_of(*tag).ok_or_else(unknown_tag)?;
                if let std::collections::hash_map::Entry::Vacant(e) = cache.entry(ord) {
                    e.insert(self.load_type(ord)?);
                }
                cache[&ord].ondisk_size
            }
            // A class variable is not part of the instance layout.
            Shape::Class { .. } => 0,
        })
    }
}

fn unknown_tag() -> io::Error {
    corrupt("[BSTACK080B] RTTI pointer/field references an unregistered type tag")
}

/// The low-`width`-byte mask for comparing a stored discriminant against a variant's
/// (sign-extended) value.
fn disc_mask(width: u8) -> u64 {
    if width >= 8 {
        u64::MAX
    } else {
        (1u64 << (width * 8)) - 1
    }
}

/// Read a `width`-byte discriminant at `off`, zero-extended to `u64`.
fn read_disc(data: &BStack, off: u64, width: u8) -> io::Result<u64> {
    let mut b = [0u8; 8];
    let w = width as usize;
    data.get_into(off, &mut b[..w])?;
    Ok(u64::from_le_bytes(b))
}

/// Pop the `n` values a container's children pushed, restoring declaration order.
/// Children are pushed onto `work` in forward order, so they execute (and land on
/// `results`) in reverse — this hands back `[c0, c1, …]`.
fn pop_n(results: &mut Vec<Value>, n: usize) -> io::Result<Vec<Value>> {
    let start = results
        .len()
        .checked_sub(n)
        .ok_or_else(|| corrupt("[BSTACK0809] RTTI interpret stack underflow"))?;
    let mut v = results.split_off(start);
    v.reverse();
    Ok(v)
}

/// Pop `names.len()` values and pair them with the field names, in order.
fn pop_named(results: &mut Vec<Value>, names: &[String]) -> io::Result<Vec<(String, Value)>> {
    let vals = pop_n(results, names.len())?;
    Ok(names.iter().cloned().zip(vals).collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cc(s: &str) -> EightCC {
        EightCC::from_name(s)
    }

    fn sample_struct() -> RttiType {
        RttiType {
            tag: cc("Sample"),
            name: "my_sample_type".to_string(),
            rc: true,
            weak: false,
            ondisk_size: 64,
            body: RttiBody::Struct(vec![
                RttiField {
                    name: "id".to_string(),
                    offset: 16,
                    shape: Shape::Pod { width: 8 },
                },
                RttiField {
                    name: "child".to_string(),
                    offset: 24,
                    shape: Shape::Owned(cc("Child")),
                },
                RttiField {
                    name: "names".to_string(),
                    offset: 32,
                    shape: Shape::Vec(Box::new(Shape::Pod { width: 1 })),
                },
                RttiField {
                    name: "kind".to_string(),
                    offset: 0,
                    shape: Shape::Class {
                        mutable: false,
                        inner: Box::new(Shape::Pod { width: 4 }),
                        value: vec![7, 0, 0, 0],
                    },
                },
            ]),
        }
    }

    fn sample_enum() -> RttiType {
        RttiType {
            tag: cc("Expr"),
            name: "expr_node".to_string(),
            rc: false,
            weak: false,
            ondisk_size: 48,
            body: RttiBody::Enum(RttiEnum {
                disc_width: 2,
                disc_off: 16,
                payload_off: 24,
                variants: vec![
                    RttiVariant {
                        name: "Leaf".to_string(),
                        disc_value: 0,
                        fields: vec![RttiField {
                            name: "value".to_string(),
                            offset: 0,
                            shape: Shape::Pod { width: 8 },
                        }],
                    },
                    RttiVariant {
                        name: "Node".to_string(),
                        disc_value: 300,
                        fields: vec![
                            RttiField {
                                name: "left".to_string(),
                                offset: 0,
                                shape: Shape::Owned(cc("Expr")),
                            },
                            RttiField {
                                name: "right".to_string(),
                                offset: 8,
                                shape: Shape::Owned(cc("Expr")),
                            },
                        ],
                    },
                ],
            }),
        }
    }

    #[test]
    fn rtti_codec_struct_roundtrips() {
        let ty = sample_struct();
        let body = encode_type(&ty);
        let back = decode_type(ty.tag, &body).unwrap();
        assert_eq!(ty, back);
    }

    #[test]
    fn rtti_codec_enum_roundtrips() {
        let ty = sample_enum();
        let body = encode_type(&ty);
        let back = decode_type(ty.tag, &body).unwrap();
        assert_eq!(ty, back);
    }

    // Two types registered into the global slice via linkme, standing in for what
    // the `#[bstack_class]` macro will emit. `sync_compiled` must discover both.
    fn reg_pair_a() -> RttiType {
        RttiType {
            tag: cc("SyncRegA"),
            name: "sync_reg_a".to_string(),
            rc: false,
            weak: false,
            ondisk_size: 16,
            body: RttiBody::Struct(vec![RttiField {
                name: "x".to_string(),
                offset: 0,
                shape: Shape::Pod { width: 8 },
            }]),
        }
    }

    fn reg_pair_b() -> RttiType {
        RttiType {
            tag: cc("SyncRegB"),
            name: "sync_reg_b".to_string(),
            rc: false,
            weak: false,
            ondisk_size: 24,
            body: RttiBody::Struct(vec![RttiField {
                name: "child".to_string(),
                offset: 0,
                shape: Shape::Owned(cc("SyncRegA")),
            }]),
        }
    }

    #[distributed_slice(RTTI_TYPES)]
    static REG_A: RttiRegistration = RttiRegistration { build: reg_pair_a };
    #[distributed_slice(RTTI_TYPES)]
    static REG_B: RttiRegistration = RttiRegistration { build: reg_pair_b };

    #[test]
    fn rtti_sync_registers_compiled_types() {
        let path = std::env::temp_dir().join(format!(
            "bstack_raii_rtti_sync_{}.stack",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));

        {
            let mut reg = RttiRegistry::open(&path).unwrap();
            // First sync appends at least our two registered types.
            let n = reg.sync_compiled().unwrap();
            assert!(n >= 2, "expected the two registered types, appended {n}");
            assert!(reg.ordinal_of(cc("SyncRegA")).is_some());
            assert!(reg.ordinal_of(cc("SyncRegB")).is_some());
            assert_eq!(
                reg.load_type(reg.ordinal_of(cc("SyncRegA")).unwrap())
                    .unwrap(),
                reg_pair_a()
            );
            // Idempotent: a second sync appends nothing.
            assert_eq!(reg.sync_compiled().unwrap(), 0);
        }

        // Reopen and re-sync: the on-disk types are recognized, nothing re-appended.
        {
            let mut reg = RttiRegistry::open(&path).unwrap();
            assert_eq!(reg.sync_compiled().unwrap(), 0);
            assert!(reg.ordinal_of(cc("SyncRegB")).is_some());
        }

        // The free `sync(path)` entry point is equivalent.
        {
            let reg = sync(&path).unwrap();
            assert!(reg.ordinal_of(cc("SyncRegA")).is_some());
        }

        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn rtti_registry_append_load_and_reopen() {
        let path = std::env::temp_dir().join(format!(
            "bstack_raii_rtti_{}.stack",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));

        let a = sample_struct();
        let b = sample_enum();

        {
            let mut reg = RttiRegistry::open(&path).unwrap();
            assert!(reg.is_empty());
            assert_eq!(reg.append(&a).unwrap(), 0);
            assert_eq!(reg.append(&b).unwrap(), 1);
            // duplicate tag is rejected
            assert!(reg.append(&a).is_err());
            // read back live from the stack
            assert_eq!(reg.load_type(0).unwrap(), a);
            assert_eq!(reg.load_type(1).unwrap(), b);
            // typed pointer resolves through the ordinal
            let ord = reg.ordinal_of(b.tag).unwrap();
            assert_eq!(reg.resolve_ptr(typed_ptr(0, 4096, ord)), Some(1));
            assert_eq!(reg.resolve_ptr(ForeignRepr::new(0, 4096)), None); // untyped
        }

        // reopen: the scan rebuilds the same maps from disk
        {
            let reg = RttiRegistry::open(&path).unwrap();
            assert_eq!(reg.len(), 2);
            assert_eq!(reg.tag_of(0), Some(a.tag));
            assert_eq!(reg.load_type(0).unwrap(), a);
            assert_eq!(reg.load_type(1).unwrap(), b);
        }

        std::fs::remove_file(&path).ok();
    }
}
