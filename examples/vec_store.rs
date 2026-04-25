//! Variable-length "vec" records packed into a bstack.
//!
//! ## Layout
//!
//! ```text
//! ┌──────────┬─────────────────┬─────┬──────────┬────────┬─────┐
//! │ len: u32 │ data: len bytes │ 0x00│ len: u32 │  data  │ 0x00│ ...
//! └──────────┴─────────────────┴─────┴──────────┴────────┴─────┘
//! ^-- record 0 starts here          ^-- record 1 starts here
//! ```
//!
//! Each record is a little-endian u32 length, followed by that many data bytes,
//! followed by **at least one** 0x00 padding byte. The sentinel lets a reader
//! skip past unknown data and find the next record boundary without a separate
//! index.
//!
//! The same framing pattern generalises to variable-size nodes in graph or tree
//! structures stored in a flat file: prefix each node with its byte length,
//! append a 0x00 sentinel, and a linear scan can walk every node without
//! needing an external index or fixed-size slots. This gives you a rudimentary
//! variable-node-size persistent data structure on top of any append-only log.

use bstack::BStack;
use std::io;
use std::path::Path;

struct VecStore(BStack);

impl VecStore {
    fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        Ok(VecStore(BStack::open(path)?))
    }

    /// Append a record. Returns its logical start offset in the store.
    fn push_vec(&self, data: &[u8]) -> io::Result<u64> {
        let len = data.len() as u32;
        let mut record = Vec::with_capacity(5 + data.len());
        record.extend_from_slice(&len.to_le_bytes());
        record.extend_from_slice(data);
        record.push(0x00); // padding sentinel
        self.0.push(&record)
    }

    /// Read the record whose header starts exactly at `pos`.
    /// Returns `(data, next_record_pos)`.
    fn read_vec_at(&self, pos: u64) -> io::Result<(Vec<u8>, u64)> {
        let mut len_buf = [0u8; 4];
        self.0.get_into(pos, &mut len_buf)?;
        let len = u32::from_le_bytes(len_buf) as u64;
        let data = self.0.get(pos + 4, pos + 4 + len)?;
        Ok((data, pos + 4 + len + 1))
    }

    /// Walk to the nth record (0-indexed) and return its data.
    fn get_nth_vec(&self, n: usize) -> io::Result<Vec<u8>> {
        let mut pos = 0u64;
        for _ in 0..n {
            let mut len_buf = [0u8; 4];
            self.0.get_into(pos, &mut len_buf)?;
            let len = u32::from_le_bytes(len_buf) as u64;
            pos += 4 + len + 1;
        }
        let (data, _) = self.read_vec_at(pos)?;
        Ok(data)
    }

    /// Scan from the beginning and return the first record whose start offset
    /// is >= `pos`, along with its offset.
    fn get_vec_after(&self, pos: u64) -> io::Result<(u64, Vec<u8>)> {
        let total = self.0.len()?;
        let mut cur = 0u64;
        while cur < total {
            let mut len_buf = [0u8; 4];
            self.0.get_into(cur, &mut len_buf)?;
            let len = u32::from_le_bytes(len_buf) as u64;
            if cur >= pos {
                let data = self.0.get(cur + 4, cur + 4 + len)?;
                return Ok((cur, data));
            }
            cur += 4 + len + 1;
        }
        Err(io::Error::new(
            io::ErrorKind::NotFound,
            "no record at or after given position",
        ))
    }
}

fn main() -> io::Result<()> {
    let store = VecStore::open("vec_store_example.bstack")?;

    // Push variable-length records.
    let offsets = [
        store.push_vec(b"alpha")?,
        store.push_vec(b"bb")?,
        store.push_vec(b"ccc")?,
        store.push_vec(b"dddd")?,
    ];
    println!("record start offsets: {:?}", offsets);
    println!("total store size:     {} bytes", store.0.len()?);

    // Read by index.
    println!("\nby index:");
    for i in 0..4 {
        let data = store.get_nth_vec(i)?;
        println!("  [{}] {:?}", i, String::from_utf8_lossy(&data));
    }

    // Sequential scan using read_vec_at.
    println!("\nsequential scan:");
    let mut pos = 0u64;
    let total = store.0.len()?;
    while pos < total {
        let (data, next) = store.read_vec_at(pos)?;
        println!("  @ {:>4}: {:?}", pos, String::from_utf8_lossy(&data));
        pos = next;
    }

    // Find first record at or after a raw file offset.
    let search_from = offsets[1] + 1; // somewhere inside record 1
    let (found_at, data) = store.get_vec_after(search_from)?;
    println!(
        "\nfirst record at or after offset {}: {:?} (starts at {})",
        search_from,
        String::from_utf8_lossy(&data),
        found_at,
    );

    Ok(())
}
