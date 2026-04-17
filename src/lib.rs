use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::RwLock;

pub struct BStack {
    lock: RwLock<File>,
}

impl BStack {
    /// Open or create a stack file at the given path.
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

    /// Append `data` to the end of the file. Returns the byte offset
    /// where the data starts. Atomic: either fully written and fsynced,
    /// or the file is unchanged.
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

    /// Read and remove the last `n` bytes. Returns the bytes. Atomic:
    /// either fully read + truncated + fsynced, or the file is unchanged.
    /// Returns an error if `n` exceeds the current file size.
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

    /// Read all bytes from `offset` to the end of the file without modifying
    /// it. Atomic: holds the write lock for the duration so no concurrent
    /// push/pop can interleave. Returns an error if `offset` exceeds the
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

    /// Read a copy of the bytes in the half-open range `[start, end)` without
    /// modifying the file. Atomic: holds the write lock for the duration.
    /// Returns an error if `end < start` or `end` exceeds the current file
    /// size.
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
        use std::time::{SystemTime, UNIX_EPOCH};
        let name = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .subsec_nanos();
        let path = std::env::temp_dir().join(format!("bstack_test_{name}.bin"));
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
}
