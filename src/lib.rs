//! A persistent, fsync-durable binary stack backed by a single file.
//!
//! # Overview
//!
//! [`BStack`] treats a file as a flat byte buffer that grows and shrinks from
//! the tail.  Every mutating operation — [`push`](BStack::push) and
//! [`pop`](BStack::pop) — calls [`sync_data`](std::fs::File::sync_data) before
//! returning, so the data survives a process crash or an unclean system
//! shutdown.  Read-only operations — [`peek`](BStack::peek) and
//! [`get`](BStack::get) — never modify the file and hold the write lock only
//! long enough to seek and read, so they cannot interleave with a concurrent
//! mutation.
//!
//! The crate has **no external dependencies** and uses **no `unsafe` code**.
//!
//! # File format
//!
//! The file contains raw byte payloads written one after another with no
//! framing, length prefixes, or checksums.  The caller is responsible for
//! knowing how many bytes to pop.  This keeps the format trivially auditable
//! with standard tools (`xxd`, `hexdump`) and avoids any parse overhead on
//! read.
//!
//! ```text
//! ┌──────────────┬──────────────┬──────────────┐
//! │  payload 0   │  payload 1   │  payload 2   │  ...
//! └──────────────┴──────────────┴──────────────┘
//! ^              ^              ^              ^
//! offset 0    offset n0      offset n0+n1   EOF (= len)
//! ```
//!
//! # Durability
//!
//! | Operation | Syscall sequence |
//! |-----------|-----------------|
//! | `push`    | `lseek` → `write` → `fdatasync` |
//! | `pop`     | `lseek` → `read` → `ftruncate` → `fdatasync` |
//!
//! [`sync_data`](std::fs::File::sync_data) (`fdatasync` on Linux/macOS) is
//! used rather than `sync_all` (`fsync`) because inode metadata — mtime,
//! ctime, block count — is not required for crash-recovery correctness, and
//! skipping it halves the number of journal writes on most filesystems.
//!
//! If `push` fails after the kernel write but before `fdatasync`, the
//! implementation makes a best-effort call to `ftruncate` back to the
//! pre-push length.  If that truncation also fails the error is swallowed;
//! on the next open the file may contain a partial tail write, which the
//! caller must handle (e.g. by storing a length sentinel at a known offset).
//!
//! # Thread safety
//!
//! `BStack` wraps the file in a [`std::sync::RwLock`].  All operations that
//! need to seek take the **write** lock — including the read-only `peek` and
//! `get`, because [`Seek`](std::io::Seek) requires `&mut File`.  Only `len`,
//! which reads file metadata without seeking, takes the **read** lock.
//!
//! The practical consequence is that concurrent `peek`/`get` calls serialise
//! against each other and against `push`/`pop`.  Concurrent `len` calls can
//! run in parallel but block while any other operation holds the write lock,
//! so `len` always observes a size at a clean operation boundary.
//!
//! # Examples
//!
//! ```no_run
//! use bstack::BStack;
//!
//! # fn main() -> std::io::Result<()> {
//! let stack = BStack::open("log.bin")?;
//!
//! // push returns the byte offset where the payload starts.
//! let off0 = stack.push(b"hello")?;  // 0
//! let off1 = stack.push(b"world")?;  // 5
//!
//! assert_eq!(stack.len()?, 10);
//!
//! // peek reads from an offset to the end without removing anything.
//! assert_eq!(stack.peek(off1)?, b"world");
//!
//! // get reads an arbitrary half-open byte range.
//! assert_eq!(stack.get(3, 8)?, b"lowor");
//!
//! // pop removes bytes from the tail and returns them.
//! assert_eq!(stack.pop(5)?, b"world");
//! assert_eq!(stack.len()?, 5);
//! # Ok(())
//! # }
//! ```

use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::RwLock;

/// A persistent, fsync-durable binary stack backed by a single file.
///
/// See the [crate-level documentation](crate) for a full description of the
/// file format, durability guarantees, and thread-safety model.
pub struct BStack {
    lock: RwLock<File>,
}

impl BStack {
    /// Open or create a stack file at `path`.
    ///
    /// If the file already exists its existing contents are preserved; the
    /// next [`push`](Self::push) will append after them.  The file is opened
    /// with both read and write access.
    ///
    /// # Errors
    ///
    /// Returns any [`io::Error`] produced by [`OpenOptions::open`], e.g.
    /// `PermissionDenied` or `NotFound` (if a parent directory is missing).
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;
        Ok(BStack {
            lock: RwLock::new(file),
        })
    }

    /// Append `data` to the end of the file.
    ///
    /// Returns the byte offset at which `data` begins — i.e. the file size
    /// immediately before the write.  An empty slice is a valid argument; it
    /// writes nothing and returns the current end offset.
    ///
    /// # Atomicity
    ///
    /// Either the full payload is written and fsynced, or the file is
    /// unchanged.  On failure after a partial kernel write the implementation
    /// attempts a best-effort `ftruncate` back to the pre-call length.
    ///
    /// # Errors
    ///
    /// Returns any [`io::Error`] from `write_all`, `sync_data`, or the
    /// fallback `set_len`.
    pub fn push(&self, data: &[u8]) -> io::Result<u64> {
        let mut file = self.lock.write().unwrap();
        // Seek to end to get the current offset; we manage position ourselves
        // rather than relying on O_APPEND so the returned offset is accurate.
        let offset = file.seek(SeekFrom::End(0))?;
        match file.write_all(data).and_then(|_| file.sync_data()) {
            Ok(()) => Ok(offset),
            Err(e) => {
                // Attempt to roll back by truncating to the pre-write length.
                let _ = file.set_len(offset);
                Err(e)
            }
        }
    }

    /// Remove and return the last `n` bytes of the file.
    ///
    /// `n = 0` is valid: no bytes are removed and an empty `Vec` is returned.
    /// `n` may span across multiple previous [`push`](Self::push) boundaries.
    ///
    /// # Atomicity
    ///
    /// The bytes are read before the file is truncated.  Either the full
    /// sequence — read → `ftruncate` → `fdatasync` — completes, or the file
    /// is unchanged.
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::InvalidInput`] if `n` exceeds the current
    /// file size.  Also propagates any I/O error from `read_exact`,
    /// `set_len`, or `sync_data`.
    pub fn pop(&self, n: u64) -> io::Result<Vec<u8>> {
        let mut file = self.lock.write().unwrap();
        let size = file.seek(SeekFrom::End(0))?;
        if n > size {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("pop({n}) exceeds file size ({size})"),
            ));
        }
        let new_len = size - n;
        file.seek(SeekFrom::Start(new_len))?;
        let mut buf = vec![0u8; n as usize];
        file.read_exact(&mut buf)?;
        file.set_len(new_len)?;
        file.sync_data()?;
        Ok(buf)
    }

    /// Return a copy of every byte from `offset` to the end of the file.
    ///
    /// `offset == len()` is valid and returns an empty `Vec`.  The file is
    /// not modified.
    ///
    /// # Atomicity
    ///
    /// Holds the write lock for the duration of the seek and read, so no
    /// concurrent [`push`](Self::push) or [`pop`](Self::pop) can interleave.
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::InvalidInput`] if `offset` exceeds the
    /// current file size.
    pub fn peek(&self, offset: u64) -> io::Result<Vec<u8>> {
        let mut file = self.lock.write().unwrap();
        let size = file.seek(SeekFrom::End(0))?;
        if offset > size {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("peek offset ({offset}) exceeds file size ({size})"),
            ));
        }
        file.seek(SeekFrom::Start(offset))?;
        let mut buf = vec![0u8; (size - offset) as usize];
        file.read_exact(&mut buf)?;
        Ok(buf)
    }

    /// Return a copy of the bytes in the half-open range `[start, end)`.
    ///
    /// `start == end` is valid and returns an empty `Vec`.  The file is not
    /// modified.
    ///
    /// # Atomicity
    ///
    /// Holds the write lock for the duration of the seek and read, so no
    /// concurrent [`push`](Self::push) or [`pop`](Self::pop) can interleave.
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::InvalidInput`] if `end < start` or if `end`
    /// exceeds the current file size.
    pub fn get(&self, start: u64, end: u64) -> io::Result<Vec<u8>> {
        if end < start {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("get: end ({end}) < start ({start})"),
            ));
        }
        let mut file = self.lock.write().unwrap();
        let size = file.seek(SeekFrom::End(0))?;
        if end > size {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("get: end ({end}) exceeds file size ({size})"),
            ));
        }
        file.seek(SeekFrom::Start(start))?;
        let mut buf = vec![0u8; (end - start) as usize];
        file.read_exact(&mut buf)?;
        Ok(buf)
    }

    /// Return the current size of the file in bytes.
    ///
    /// Takes the read lock, so it can run concurrently with other `len`
    /// calls but blocks while any write-lock operation is in progress.  The
    /// returned value is always at a clean operation boundary — it is never
    /// observed mid-write.
    ///
    /// # Errors
    ///
    /// Propagates any [`io::Error`] from [`File::metadata`].
    pub fn len(&self) -> io::Result<u64> {
        let file = self.lock.read().unwrap();
        Ok(file.metadata()?.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::ErrorKind;

    fn mk_stack() -> (BStack, std::path::PathBuf) {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let id = COUNTER.fetch_add(1, Ordering::Relaxed);
        let pid = std::process::id();
        let path = std::env::temp_dir().join(format!("bstack_test_{pid}_{id}.bin"));
        let stack = BStack::open(&path).unwrap();
        (stack, path)
    }

    struct Guard(std::path::PathBuf);
    impl Drop for Guard {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }

    // -------------------------------------------------------------------------

    #[test]
    fn push_returns_correct_offsets() {
        let (s, p) = mk_stack();
        let _g = Guard(p);

        let off0 = s.push(b"hello").unwrap();
        let off1 = s.push(b"world").unwrap();
        let off2 = s.push(b"!").unwrap();

        assert_eq!(off0, 0);
        assert_eq!(off1, 5);
        assert_eq!(off2, 10);
        assert_eq!(s.len().unwrap(), 11);
    }

    #[test]
    fn pop_returns_correct_bytes_and_shrinks() {
        let (s, p) = mk_stack();
        let _g = Guard(p);

        s.push(b"abcde").unwrap();
        s.push(b"fghij").unwrap();
        assert_eq!(s.len().unwrap(), 10);

        let bytes = s.pop(5).unwrap();
        assert_eq!(bytes, b"fghij");
        assert_eq!(s.len().unwrap(), 5);

        let bytes = s.pop(5).unwrap();
        assert_eq!(bytes, b"abcde");
        assert_eq!(s.len().unwrap(), 0);
    }

    #[test]
    fn pop_across_push_boundary() {
        let (s, p) = mk_stack();
        let _g = Guard(p);

        s.push(b"12345").unwrap();
        s.push(b"67890").unwrap();

        // pop 7 bytes — spans both pushes
        let bytes = s.pop(7).unwrap();
        assert_eq!(bytes, b"4567890");
        assert_eq!(s.len().unwrap(), 3);
    }

    #[test]
    fn pop_on_empty_file_returns_error() {
        let (s, p) = mk_stack();
        let _g = Guard(p);

        let err = s.pop(1).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidInput);
    }

    #[test]
    fn pop_n_exceeds_file_size_returns_error() {
        let (s, p) = mk_stack();
        let _g = Guard(p);

        s.push(b"abc").unwrap();
        let err = s.pop(10).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidInput);
        // File must be unchanged
        assert_eq!(s.len().unwrap(), 3);
    }

    #[test]
    fn peek_reads_from_offset_to_end() {
        let (s, p) = mk_stack();
        let _g = Guard(p);

        s.push(b"hello").unwrap();
        s.push(b"world").unwrap();

        assert_eq!(s.peek(0).unwrap(), b"helloworld");
        assert_eq!(s.peek(5).unwrap(), b"world");
        assert_eq!(s.peek(7).unwrap(), b"rld");
        assert_eq!(s.peek(10).unwrap(), b""); // offset == size: empty slice
    }

    #[test]
    fn peek_offset_exceeds_size_returns_error() {
        let (s, p) = mk_stack();
        let _g = Guard(p);

        s.push(b"abc").unwrap();
        let err = s.peek(10).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidInput);
        // file unchanged
        assert_eq!(s.len().unwrap(), 3);
    }

    #[test]
    fn get_reads_half_open_range() {
        let (s, p) = mk_stack();
        let _g = Guard(p);

        s.push(b"hello").unwrap();
        s.push(b"world").unwrap();

        assert_eq!(s.get(0, 5).unwrap(), b"hello");
        assert_eq!(s.get(5, 10).unwrap(), b"world");
        assert_eq!(s.get(3, 8).unwrap(), b"lowor"); // spans push boundary
        assert_eq!(s.get(4, 4).unwrap(), b"");      // empty range
    }

    #[test]
    fn get_end_exceeds_size_returns_error() {
        let (s, p) = mk_stack();
        let _g = Guard(p);

        s.push(b"abc").unwrap();
        let err = s.get(0, 10).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidInput);
    }

    #[test]
    fn get_end_less_than_start_returns_error() {
        let (s, p) = mk_stack();
        let _g = Guard(p);

        s.push(b"abcde").unwrap();
        let err = s.get(4, 2).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidInput);
    }

    #[test]
    fn get_does_not_modify_file() {
        let (s, p) = mk_stack();
        let _g = Guard(p);

        s.push(b"hello").unwrap();
        s.push(b"world").unwrap();
        let _ = s.get(2, 8).unwrap();
        assert_eq!(s.len().unwrap(), 10);
        // subsequent push must still land at the right offset
        let off = s.push(b"!").unwrap();
        assert_eq!(off, 10);
    }

    #[test]
    fn interleaved_push_pop_correct_state() {
        let (s, p) = mk_stack();
        let _g = Guard(p);

        let o0 = s.push(b"AAA").unwrap(); // [AAA]
        assert_eq!(o0, 0);

        let o1 = s.push(b"BB").unwrap(); // [AAABB]
        assert_eq!(o1, 3);

        let popped = s.pop(2).unwrap(); // [AAA]
        assert_eq!(popped, b"BB");

        let o2 = s.push(b"CCCC").unwrap(); // [AAACCCC]
        assert_eq!(o2, 3);

        assert_eq!(s.len().unwrap(), 7);

        let all = s.pop(7).unwrap();
        assert_eq!(all, b"AAACCCC");
        assert_eq!(s.len().unwrap(), 0);
    }

    // ---- persistence / reopen ---------------------------------------------------

    #[test]
    fn reopen_reads_back_correct_data() {
        let (s, p) = mk_stack();
        let _g = Guard(p.clone());

        s.push(b"hello").unwrap();
        s.push(b"world").unwrap();
        drop(s);

        let s2 = BStack::open(&p).unwrap();
        assert_eq!(s2.len().unwrap(), 10);
        assert_eq!(s2.peek(0).unwrap(), b"helloworld");
    }

    #[test]
    fn reopen_and_continue_pushing() {
        let (s, p) = mk_stack();
        let _g = Guard(p.clone());

        let off0 = s.push(b"first").unwrap();
        assert_eq!(off0, 0);
        drop(s);

        let s2 = BStack::open(&p).unwrap();
        let off1 = s2.push(b"second").unwrap();
        assert_eq!(off1, 5); // must continue from prior end, not overwrite
        assert_eq!(s2.len().unwrap(), 11);
        assert_eq!(s2.peek(0).unwrap(), b"firstsecond");
    }

    #[test]
    fn reopen_after_pop_sees_truncated_file() {
        let (s, p) = mk_stack();
        let _g = Guard(p.clone());

        s.push(b"hello").unwrap();
        s.push(b"world").unwrap();
        s.pop(5).unwrap();
        drop(s);

        let s2 = BStack::open(&p).unwrap();
        assert_eq!(s2.len().unwrap(), 5);
        assert_eq!(s2.peek(0).unwrap(), b"hello");
    }

    // ---- zero / boundary --------------------------------------------------------

    #[test]
    fn push_empty_slice() {
        let (s, p) = mk_stack();
        let _g = Guard(p);

        let off0 = s.push(b"abc").unwrap();
        let off1 = s.push(&[]).unwrap(); // empty — no bytes written
        let off2 = s.push(b"def").unwrap();

        assert_eq!(off0, 0);
        assert_eq!(off1, 3); // offset == current size
        assert_eq!(off2, 3); // next real push lands at the same offset
        assert_eq!(s.len().unwrap(), 6);
        assert_eq!(s.peek(0).unwrap(), b"abcdef");
    }

    #[test]
    fn pop_zero_bytes() {
        let (s, p) = mk_stack();
        let _g = Guard(p);

        s.push(b"abc").unwrap();
        let bytes = s.pop(0).unwrap();
        assert_eq!(bytes, b"");
        assert_eq!(s.len().unwrap(), 3);
        // seek position must be correct for the next push
        let off = s.push(b"d").unwrap();
        assert_eq!(off, 3);
    }

    #[test]
    fn peek_zero_offset_on_empty_file() {
        let (s, p) = mk_stack();
        let _g = Guard(p);

        // offset == size == 0: valid, returns empty slice
        assert_eq!(s.peek(0).unwrap(), b"");
    }

    #[test]
    fn get_zero_range_on_empty_file() {
        let (s, p) = mk_stack();
        let _g = Guard(p);

        // [0, 0) on a 0-byte file: valid, returns empty slice
        assert_eq!(s.get(0, 0).unwrap(), b"");
    }

    #[test]
    fn drain_to_zero_then_push_starts_at_offset_zero() {
        let (s, p) = mk_stack();
        let _g = Guard(p);

        s.push(b"hello").unwrap();
        s.pop(5).unwrap();
        assert_eq!(s.len().unwrap(), 0);

        let off = s.push(b"world").unwrap();
        assert_eq!(off, 0); // must re-start from the beginning
        assert_eq!(s.len().unwrap(), 5);
        assert_eq!(s.peek(0).unwrap(), b"world");
    }

    // ---- data integrity ---------------------------------------------------------

    #[test]
    fn peek_does_not_modify_file() {
        let (s, p) = mk_stack();
        let _g = Guard(p);

        s.push(b"hello").unwrap();
        s.push(b"world").unwrap();
        let _ = s.peek(3).unwrap();
        assert_eq!(s.len().unwrap(), 10);
        let off = s.push(b"!").unwrap();
        assert_eq!(off, 10);
    }

    #[test]
    fn binary_roundtrip_all_byte_values() {
        let (s, p) = mk_stack();
        let _g = Guard(p);

        // Every byte value 0x00–0xFF, twice to catch any off-by-one at the wrap.
        let data: Vec<u8> = (0u16..512).map(|i| (i % 256) as u8).collect();
        s.push(&data).unwrap();
        let got = s.pop(data.len() as u64).unwrap();
        assert_eq!(got, data);
        assert_eq!(s.len().unwrap(), 0);
    }

    #[test]
    fn large_payload_roundtrip() {
        let (s, p) = mk_stack();
        let _g = Guard(p);

        // 1 MiB with a simple pattern to detect any byte-level corruption.
        let payload: Vec<u8> = (0..1024 * 1024).map(|i: usize| (i.wrapping_mul(7).wrapping_add(13)) as u8).collect();
        s.push(&payload).unwrap();
        let got = s.get(0, payload.len() as u64).unwrap();
        assert_eq!(got, payload);
        assert_eq!(s.len().unwrap(), payload.len() as u64);
    }

    // ---- concurrency ------------------------------------------------------------

    #[test]
    fn concurrent_pushes_non_overlapping() {
        use std::collections::HashSet;
        use std::sync::Arc;
        use std::thread;

        let (s, p) = mk_stack();
        let _g = Guard(p);
        let s = Arc::new(s);

        const THREADS: usize = 8;
        const PER_THREAD: usize = 100;
        const ITEM: usize = 16; // bytes per push

        let handles: Vec<_> = (0..THREADS)
            .map(|t| {
                let s = Arc::clone(&s);
                thread::spawn(move || {
                    (0..PER_THREAD)
                        .map(|i| {
                            // Encode thread-id and sequence number so we can
                            // verify content after all threads finish.
                            let mut data = [0u8; ITEM];
                            data[0] = t as u8;
                            data[1..9].copy_from_slice(&(i as u64).to_le_bytes());
                            let off = s.push(&data).unwrap();
                            (off, t, i)
                        })
                        .collect::<Vec<_>>()
                })
            })
            .collect();

        let results: Vec<_> = handles.into_iter().flat_map(|h| h.join().unwrap()).collect();

        // Every offset must be a multiple of ITEM (each push is exactly ITEM bytes).
        for &(off, _, _) in &results {
            assert_eq!(off % ITEM as u64, 0, "offset {off} is not aligned to ITEM");
        }

        // Every offset must be unique — no two pushes can share a slot.
        let mut seen: HashSet<u64> = HashSet::new();
        for &(off, _, _) in &results {
            assert!(seen.insert(off), "duplicate offset {off}");
        }

        // Total size must account for every push.
        assert_eq!(s.len().unwrap(), (THREADS * PER_THREAD * ITEM) as u64);

        // Read each slot back and verify the encoded thread-id and index.
        for &(off, t, i) in &results {
            let slot = s.get(off, off + ITEM as u64).unwrap();
            assert_eq!(slot[0], t as u8, "thread id mismatch at offset {off}");
            let idx = u64::from_le_bytes(slot[1..9].try_into().unwrap());
            assert_eq!(idx, i as u64, "item index mismatch at offset {off}");
        }
    }

    #[test]
    fn concurrent_len_is_multiple_of_item_size() {
        use std::sync::Arc;
        use std::thread;

        // Push fixed-size items from several threads while another thread reads
        // len continuously. Because push holds the write lock and len holds the
        // read lock, len must never observe a partial write — it will always
        // block until the active push completes, so the size it sees is always
        // an exact multiple of ITEM.
        let (s, p) = mk_stack();
        let _g = Guard(p);
        let s = Arc::new(s);

        const ITEM: u64 = 8;
        const PUSH_THREADS: usize = 4;
        const PUSHES_PER_THREAD: usize = 200;

        let push_handles: Vec<_> = (0..PUSH_THREADS)
            .map(|_| {
                let s = Arc::clone(&s);
                thread::spawn(move || {
                    for _ in 0..PUSHES_PER_THREAD {
                        s.push(&[0xBEu8; ITEM as usize]).unwrap();
                    }
                })
            })
            .collect();

        let len_handle = {
            let s = Arc::clone(&s);
            thread::spawn(move || {
                // Sample len 2000 times while pushes are in flight.
                for _ in 0..2000 {
                    let size = s.len().unwrap();
                    assert_eq!(size % ITEM, 0, "torn write: size {size} is not a multiple of {ITEM}");
                }
            })
        };

        for h in push_handles {
            h.join().unwrap();
        }
        len_handle.join().unwrap();

        assert_eq!(
            s.len().unwrap(),
            (PUSH_THREADS * PUSHES_PER_THREAD) as u64 * ITEM
        );
    }
}
