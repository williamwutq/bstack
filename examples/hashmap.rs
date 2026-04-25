//! Persistent key-value hashmap backed by two bstack files.
//!
//! ## Files
//!
//! | File | Contents |
//! |------|----------|
//! | `strings.bstack` | append-only null-terminated `key\0value\0` pairs |
//! | `index.bstack`   | 256 × u64 slots (2 048 bytes); slot = FNV-hash(key) & 0xFF; value = byte offset into strings file, or u64::MAX for empty |
//!
//! ## Limitations (by design)
//!
//! - 256 fixed slots — not growable.
//! - No collision resolution: a second insert to the same slot overwrites the
//!   first. Look up by key verifies the stored key and returns `None` on mismatch.
//! - The string pool is append-only; deleted or overwritten entries are not reclaimed.
//!
//! ## How to run
//!
//! ```
//! cargo run --example hashmap --features set
//! ```
//!
//! The `set` feature is required for in-place slot updates in `index.bstack`.

#[cfg(not(feature = "set"))]
compile_error!("the `hashmap` example requires `--features set`");

use bstack::BStack;
use std::io;
use std::path::Path;

const SLOTS: u64 = 256;
const SLOT_SIZE: u64 = 8; // one u64 per slot
const TABLE_BYTES: usize = (SLOTS * SLOT_SIZE) as usize; // 2 048
const EMPTY: u64 = u64::MAX; // 0xFF…FF — initial table fill

struct PersistentHashMap {
    strings: BStack,
    index: BStack,
}

impl PersistentHashMap {
    fn open(strings_path: impl AsRef<Path>, index_path: impl AsRef<Path>) -> io::Result<Self> {
        let strings = BStack::open(strings_path)?;
        let index = BStack::open(index_path)?;
        if index.is_empty()? {
            // All 0xFF bytes → every slot reads as u64::MAX (EMPTY).
            index.push(&[0xFF_u8; TABLE_BYTES])?;
        }
        Ok(Self { strings, index })
    }

    fn hash(key: &str) -> u8 {
        // FNV-1a folded to 8 bits.
        let mut h: u32 = 2_166_136_261;
        for &b in key.as_bytes() {
            h ^= b as u32;
            h = h.wrapping_mul(16_777_619);
        }
        (h ^ (h >> 8) ^ (h >> 16) ^ (h >> 24)) as u8
    }

    /// Insert `key` → `value`. Overwrites any previous entry that hashed to the same slot.
    #[cfg(feature = "set")]
    fn insert(&self, key: &str, value: &str) -> io::Result<()> {
        let slot = Self::hash(key) as u64;
        let mut entry = Vec::with_capacity(key.len() + value.len() + 2);
        entry.extend_from_slice(key.as_bytes());
        entry.push(0); // null separator
        entry.extend_from_slice(value.as_bytes());
        entry.push(0); // null terminator
        let offset = self.strings.push(&entry)?;
        self.index.set(slot * SLOT_SIZE, &offset.to_le_bytes())
    }

    /// Look up `key`. Returns `None` if the slot is empty or if the stored key
    /// does not match (hash collision with a different key).
    fn get(&self, key: &str) -> io::Result<Option<String>> {
        let slot = Self::hash(key) as u64;
        let mut buf = [0u8; 8];
        self.index.get_into(slot * SLOT_SIZE, &mut buf)?;
        let offset = u64::from_le_bytes(buf);
        if offset == EMPTY {
            return Ok(None);
        }
        // Read from offset to EOF; we only need up to the second null byte.
        let data = self.strings.peek(offset)?;
        let key_end = data.iter().position(|&b| b == 0).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "corrupt entry: missing key null")
        })?;
        if &data[..key_end] != key.as_bytes() {
            return Ok(None); // same slot, different key
        }
        let val_start = key_end + 1;
        let val_end = data[val_start..]
            .iter()
            .position(|&b| b == 0)
            .map(|p| val_start + p)
            .unwrap_or(data.len());
        Ok(Some(
            String::from_utf8_lossy(&data[val_start..val_end]).into_owned(),
        ))
    }
}

#[cfg(feature = "set")]
fn main() -> io::Result<()> {
    let map = PersistentHashMap::open("strings.bstack", "index.bstack")?;

    map.insert("name", "Alice")?;
    map.insert("city", "Boston")?;
    map.insert("lang", "Rust")?;

    for key in &["name", "city", "lang", "missing"] {
        println!("{:?} => {:?}", key, map.get(key)?);
    }

    println!("\nindex size:   {} bytes ({} slots x 8)", map.index.len()?, SLOTS);
    println!("strings size: {} bytes (append-only pool)", map.strings.len()?);

    Ok(())
}

#[cfg(not(feature = "set"))]
fn main() {}
