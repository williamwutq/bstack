#[cfg(test)]
mod tests {
    use crate::{BStack, HEADER_SIZE, MAGIC};
    use std::fs::OpenOptions;
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
    // Original functional tests (unchanged behaviour)
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
        assert_eq!(s.peek(10).unwrap(), b"");
    }

    #[test]
    fn peek_offset_exceeds_size_returns_error() {
        let (s, p) = mk_stack();
        let _g = Guard(p);

        s.push(b"abc").unwrap();
        let err = s.peek(10).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidInput);
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
        assert_eq!(s.get(3, 8).unwrap(), b"lowor");
        assert_eq!(s.get(4, 4).unwrap(), b"");
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
        let off = s.push(b"!").unwrap();
        assert_eq!(off, 10);
    }

    #[test]
    fn interleaved_push_pop_correct_state() {
        let (s, p) = mk_stack();
        let _g = Guard(p);

        let o0 = s.push(b"AAA").unwrap();
        assert_eq!(o0, 0);
        let o1 = s.push(b"BB").unwrap();
        assert_eq!(o1, 3);
        let popped = s.pop(2).unwrap();
        assert_eq!(popped, b"BB");
        let o2 = s.push(b"CCCC").unwrap();
        assert_eq!(o2, 3);
        assert_eq!(s.len().unwrap(), 7);
        let all = s.pop(7).unwrap();
        assert_eq!(all, b"AAACCCC");
        assert_eq!(s.len().unwrap(), 0);
    }

    // ---- persistence / reopen -----------------------------------------------

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
        assert_eq!(off1, 5);
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

    // ---- zero / boundary ----------------------------------------------------

    #[test]
    fn push_empty_slice() {
        let (s, p) = mk_stack();
        let _g = Guard(p);

        let off0 = s.push(b"abc").unwrap();
        let off1 = s.push(&[]).unwrap();
        let off2 = s.push(b"def").unwrap();

        assert_eq!(off0, 0);
        assert_eq!(off1, 3);
        assert_eq!(off2, 3);
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
        let off = s.push(b"d").unwrap();
        assert_eq!(off, 3);
    }

    #[test]
    fn peek_zero_offset_on_empty_file() {
        let (s, p) = mk_stack();
        let _g = Guard(p);

        assert_eq!(s.peek(0).unwrap(), b"");
    }

    #[test]
    fn get_zero_range_on_empty_file() {
        let (s, p) = mk_stack();
        let _g = Guard(p);

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
        assert_eq!(off, 0);
        assert_eq!(s.len().unwrap(), 5);
        assert_eq!(s.peek(0).unwrap(), b"world");
    }

    // ---- data integrity -----------------------------------------------------

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

        let payload: Vec<u8> = (0..1024 * 1024)
            .map(|i: usize| (i.wrapping_mul(7).wrapping_add(13)) as u8)
            .collect();
        s.push(&payload).unwrap();
        let got = s.get(0, payload.len() as u64).unwrap();
        assert_eq!(got, payload);
        assert_eq!(s.len().unwrap(), payload.len() as u64);
    }

    // ---- header / magic / format --------------------------------------------

    #[test]
    fn new_file_has_valid_header() {
        let (s, p) = mk_stack();
        let _g = Guard(p.clone());
        drop(s); // close BStack so we can read the raw file

        let raw = std::fs::read(&p).unwrap();
        assert_eq!(
            raw.len(),
            HEADER_SIZE as usize,
            "new file should be exactly 16 bytes"
        );
        assert_eq!(&raw[0..8], &MAGIC, "magic mismatch");
        let clen = u64::from_le_bytes(raw[8..16].try_into().unwrap());
        assert_eq!(clen, 0, "committed length should be 0 for empty stack");
    }

    #[test]
    fn header_committed_len_matches_after_pushes() {
        let (s, p) = mk_stack();
        let _g = Guard(p.clone());

        s.push(b"hello").unwrap(); // 5 bytes
        s.push(b"world").unwrap(); // 5 bytes
        drop(s);

        let raw = std::fs::read(&p).unwrap();
        let clen = u64::from_le_bytes(raw[8..16].try_into().unwrap());
        assert_eq!(clen, 10);
        assert_eq!(raw.len() as u64, HEADER_SIZE + 10);
    }

    #[test]
    fn header_committed_len_matches_after_pop() {
        let (s, p) = mk_stack();
        let _g = Guard(p.clone());

        s.push(b"hello").unwrap();
        s.push(b"world").unwrap();
        s.pop(5).unwrap();
        drop(s);

        let raw = std::fs::read(&p).unwrap();
        let clen = u64::from_le_bytes(raw[8..16].try_into().unwrap());
        assert_eq!(clen, 5);
        assert_eq!(raw.len() as u64, HEADER_SIZE + 5);
    }

    #[test]
    fn open_rejects_bad_magic() {
        let path = {
            use std::sync::atomic::{AtomicU64, Ordering};
            static C: AtomicU64 = AtomicU64::new(0);
            let id = C.fetch_add(1, Ordering::Relaxed);
            std::env::temp_dir().join(format!("bstack_badmagic_{}.bin", id))
        };
        let _g = Guard(path.clone());

        // Write 16 bytes with wrong magic.
        let mut bad: Vec<u8> = b"WRONGHDR".to_vec();
        bad.extend_from_slice(&0u64.to_le_bytes());
        std::fs::write(&path, &bad).unwrap();

        let err = BStack::open(&path).err().unwrap();
        assert_eq!(err.kind(), ErrorKind::InvalidData);
        assert!(err.to_string().contains("magic"));
    }

    #[test]
    fn open_rejects_truncated_header() {
        let path = {
            use std::sync::atomic::{AtomicU64, Ordering};
            static C: AtomicU64 = AtomicU64::new(0);
            let id = C.fetch_add(1, Ordering::Relaxed);
            std::env::temp_dir().join(format!("bstack_smallfile_{}.bin", id))
        };
        let _g = Guard(path.clone());

        // Only 8 bytes — too short for a valid header.
        std::fs::write(&path, b"tooshort").unwrap();

        let err = BStack::open(&path).err().unwrap();
        assert_eq!(err.kind(), ErrorKind::InvalidData);
    }

    #[test]
    fn recovery_truncates_partial_push() {
        // Simulate a push that wrote data but crashed before updating clen.
        let (s, p) = mk_stack();
        let _g = Guard(p.clone());

        s.push(b"committed").unwrap(); // 9 bytes, clen = 9
        drop(s);

        // Directly append 5 "phantom" bytes to the file (clen still says 9).
        {
            use std::io::Write;
            let mut f = OpenOptions::new().append(true).open(&p).unwrap();
            f.write_all(b"ghost").unwrap();
            // Do NOT update the header — simulating a crash after write but
            // before the header update + fsync.
        }

        // Verify the raw file has 16 + 9 + 5 = 30 bytes but clen = 9.
        let raw = std::fs::read(&p).unwrap();
        assert_eq!(raw.len(), (HEADER_SIZE + 9 + 5) as usize);
        let clen_before = u64::from_le_bytes(raw[8..16].try_into().unwrap());
        assert_eq!(clen_before, 9);

        // Reopen: recovery should truncate the phantom 5 bytes.
        let s2 = BStack::open(&p).unwrap();
        assert_eq!(s2.len().unwrap(), 9);
        assert_eq!(s2.peek(0).unwrap(), b"committed");
        drop(s2);

        // Raw file should now be exactly 16 + 9 = 25 bytes.
        let raw2 = std::fs::read(&p).unwrap();
        assert_eq!(raw2.len(), (HEADER_SIZE + 9) as usize);
    }

    #[test]
    fn recovery_repairs_header_after_partial_pop() {
        // Simulate a pop that truncated the file but crashed before updating clen.
        let (s, p) = mk_stack();
        let _g = Guard(p.clone());

        s.push(b"hello").unwrap(); // 5 bytes
        s.push(b"world").unwrap(); // 5 bytes
        drop(s);

        // Manually truncate the file to remove "world" (back to 16 + 5 = 21),
        // but leave clen at 10 — simulating a crash after ftruncate but before
        // the header write + fsync.
        {
            let f = OpenOptions::new().write(true).open(&p).unwrap();
            f.set_len(HEADER_SIZE + 5).unwrap();
            // Header still says clen = 10.
        }

        let raw = std::fs::read(&p).unwrap();
        assert_eq!(raw.len(), (HEADER_SIZE + 5) as usize);
        let clen_before = u64::from_le_bytes(raw[8..16].try_into().unwrap());
        assert_eq!(
            clen_before, 10,
            "header should still claim 10 before recovery"
        );

        // Reopen: recovery should set clen = 5 to match actual file size.
        let s2 = BStack::open(&p).unwrap();
        assert_eq!(s2.len().unwrap(), 5);
        assert_eq!(s2.peek(0).unwrap(), b"hello");
        drop(s2);

        let raw2 = std::fs::read(&p).unwrap();
        let clen_after = u64::from_le_bytes(raw2[8..16].try_into().unwrap());
        assert_eq!(clen_after, 5, "clen should be repaired to 5 after recovery");
    }

    // ---- peek_into ----------------------------------------------------------

    #[test]
    fn peek_into_fills_buffer() {
        let (s, p) = mk_stack();
        let _g = Guard(p);

        s.push(b"hello").unwrap();
        s.push(b"world").unwrap();

        let mut buf = [0u8; 5];
        s.peek_into(5, &mut buf).unwrap();
        assert_eq!(&buf, b"world");

        let mut buf2 = [0u8; 10];
        s.peek_into(0, &mut buf2).unwrap();
        assert_eq!(&buf2, b"helloworld");
    }

    #[test]
    fn peek_into_empty_buf_is_noop() {
        let (s, p) = mk_stack();
        let _g = Guard(p);

        s.peek_into(0, &mut []).unwrap();
    }

    #[test]
    fn peek_into_range_exceeds_size_returns_error() {
        let (s, p) = mk_stack();
        let _g = Guard(p);

        s.push(b"abc").unwrap();
        let mut buf = [0u8; 5];
        let err = s.peek_into(0, &mut buf).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidInput);
    }

    #[test]
    fn peek_into_matches_peek() {
        let (s, p) = mk_stack();
        let _g = Guard(p);

        s.push(b"helloworld").unwrap();
        let expected = s.peek(3).unwrap();
        let mut buf = vec![0u8; expected.len()];
        s.peek_into(3, &mut buf).unwrap();
        assert_eq!(buf, expected);
    }

    // ---- get_into -----------------------------------------------------------

    #[test]
    fn get_into_fills_buffer() {
        let (s, p) = mk_stack();
        let _g = Guard(p);

        s.push(b"hello").unwrap();
        s.push(b"world").unwrap();

        let mut buf = [0u8; 5];
        s.get_into(3, &mut buf).unwrap();
        assert_eq!(&buf, b"lowor");
    }

    #[test]
    fn get_into_empty_buf_is_noop() {
        let (s, p) = mk_stack();
        let _g = Guard(p);

        s.push(b"abc").unwrap();
        s.get_into(1, &mut []).unwrap();
    }

    #[test]
    fn get_into_end_exceeds_size_returns_error() {
        let (s, p) = mk_stack();
        let _g = Guard(p);

        s.push(b"abc").unwrap();
        let mut buf = [0u8; 5];
        let err = s.get_into(0, &mut buf).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidInput);
    }

    #[test]
    fn get_into_matches_get() {
        let (s, p) = mk_stack();
        let _g = Guard(p);

        s.push(b"helloworld").unwrap();
        let expected = s.get(2, 8).unwrap();
        let mut buf = vec![0u8; 6];
        s.get_into(2, &mut buf).unwrap();
        assert_eq!(buf, expected);
    }

    // ---- pop_into -----------------------------------------------------------

    #[test]
    fn pop_into_fills_buffer_and_shrinks() {
        let (s, p) = mk_stack();
        let _g = Guard(p);

        s.push(b"abcde").unwrap();
        s.push(b"fghij").unwrap();

        let mut buf = [0u8; 5];
        s.pop_into(&mut buf).unwrap();
        assert_eq!(&buf, b"fghij");
        assert_eq!(s.len().unwrap(), 5);

        s.pop_into(&mut buf).unwrap();
        assert_eq!(&buf, b"abcde");
        assert_eq!(s.len().unwrap(), 0);
    }

    #[test]
    fn pop_into_empty_buf_is_noop() {
        let (s, p) = mk_stack();
        let _g = Guard(p);

        s.push(b"abc").unwrap();
        s.pop_into(&mut []).unwrap();
        assert_eq!(s.len().unwrap(), 3);
    }

    #[test]
    fn pop_into_exceeds_size_returns_error() {
        let (s, p) = mk_stack();
        let _g = Guard(p);

        s.push(b"abc").unwrap();
        let mut buf = [0u8; 10];
        let err = s.pop_into(&mut buf).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidInput);
        assert_eq!(s.len().unwrap(), 3);
    }

    #[test]
    fn pop_into_matches_pop() {
        let (s, p) = mk_stack();
        let _g = Guard(p);

        s.push(b"helloworld").unwrap();
        let expected = s.pop(5).unwrap();

        let (s2, p2) = mk_stack();
        let _g2 = Guard(p2);
        s2.push(b"helloworld").unwrap();
        let mut buf = vec![0u8; 5];
        s2.pop_into(&mut buf).unwrap();
        assert_eq!(buf, expected);
        assert_eq!(s2.len().unwrap(), 5);
    }

    // ---- set (feature-gated) ------------------------------------------------

    #[cfg(feature = "set")]
    #[test]
    fn set_overwrites_middle_bytes() {
        let (s, p) = mk_stack();
        let _g = Guard(p);

        s.push(b"helloworld").unwrap();
        s.set(5, b"WORLD").unwrap();
        assert_eq!(s.peek(0).unwrap(), b"helloWORLD");
        assert_eq!(s.len().unwrap(), 10);
    }

    #[cfg(feature = "set")]
    #[test]
    fn set_at_start() {
        let (s, p) = mk_stack();
        let _g = Guard(p);

        s.push(b"helloworld").unwrap();
        s.set(0, b"HELLO").unwrap();
        assert_eq!(s.peek(0).unwrap(), b"HELLOworld");
    }

    #[cfg(feature = "set")]
    #[test]
    fn set_at_exact_end_boundary() {
        let (s, p) = mk_stack();
        let _g = Guard(p);

        s.push(b"hello").unwrap();
        s.set(3, b"LO").unwrap();
        assert_eq!(s.peek(0).unwrap(), b"helLO");
    }

    #[cfg(feature = "set")]
    #[test]
    fn set_empty_slice_is_noop() {
        let (s, p) = mk_stack();
        let _g = Guard(p);

        s.push(b"hello").unwrap();
        s.set(2, b"").unwrap();
        assert_eq!(s.peek(0).unwrap(), b"hello");
        assert_eq!(s.len().unwrap(), 5);
    }

    #[cfg(feature = "set")]
    #[test]
    fn set_does_not_change_file_size() {
        let (s, p) = mk_stack();
        let _g = Guard(p);

        s.push(b"abcde").unwrap();
        s.set(1, b"XYZ").unwrap();
        assert_eq!(s.len().unwrap(), 5);
        assert_eq!(s.peek(0).unwrap(), b"aXYZe");
    }

    #[cfg(feature = "set")]
    #[test]
    fn set_rejects_write_past_end() {
        let (s, p) = mk_stack();
        let _g = Guard(p);

        s.push(b"hello").unwrap();
        let err = s.set(3, b"TOOLONG").unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidInput);
        // File must be unchanged.
        assert_eq!(s.peek(0).unwrap(), b"hello");
    }

    #[cfg(feature = "set")]
    #[test]
    fn set_rejects_offset_past_end() {
        let (s, p) = mk_stack();
        let _g = Guard(p);

        s.push(b"hello").unwrap();
        let err = s.set(10, b"x").unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidInput);
    }

    #[cfg(feature = "set")]
    #[test]
    fn set_persists_across_reopen() {
        let (s, p) = mk_stack();
        let _g = Guard(p.clone());

        s.push(b"helloworld").unwrap();
        s.set(5, b"WORLD").unwrap();
        drop(s);

        let s2 = BStack::open(&p).unwrap();
        assert_eq!(s2.peek(0).unwrap(), b"helloWORLD");
    }

    // ---- concurrency --------------------------------------------------------

    #[cfg(any(unix, windows))]
    #[test]
    fn concurrent_reads_do_not_serialise() {
        // On Unix and Windows, peek and get use a cursor-safe positional read
        // (pread(2) on Unix; ReadFile+OVERLAPPED on Windows) and hold only the
        // read lock, so they must be able to run simultaneously. We verify this
        // by spinning up many reader threads on a pre-populated stack and
        // confirming that they all finish with correct data — no deadlock, no
        // torn reads.
        use std::sync::Arc;
        use std::thread;

        let (s, p) = mk_stack();
        let _g = Guard(p);

        // Write 8 fixed-size records of 16 bytes each.
        const RECORDS: usize = 8;
        const RSIZE: u64 = 16;
        for i in 0..RECORDS {
            let mut rec = [0u8; RSIZE as usize];
            rec[0] = i as u8;
            s.push(&rec).unwrap();
        }

        let s = Arc::new(s);

        // Spawn 32 reader threads; each reads every record via both peek and get.
        let handles: Vec<_> = (0..32)
            .map(|_| {
                let s = Arc::clone(&s);
                thread::spawn(move || {
                    for i in 0..RECORDS {
                        let off = i as u64 * RSIZE;
                        let via_get = s.get(off, off + RSIZE).unwrap();
                        assert_eq!(via_get[0], i as u8);

                        // peek from this record's offset; the first byte of
                        // the returned slice must still be `i`.
                        let via_peek = s.peek(off).unwrap();
                        assert_eq!(via_peek[0], i as u8);
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }
    }

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
        const ITEM: usize = 16;

        let handles: Vec<_> = (0..THREADS)
            .map(|t| {
                let s = Arc::clone(&s);
                thread::spawn(move || {
                    (0..PER_THREAD)
                        .map(|i| {
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

        let results: Vec<_> = handles
            .into_iter()
            .flat_map(|h| h.join().unwrap())
            .collect();

        for &(off, _, _) in &results {
            assert_eq!(off % ITEM as u64, 0, "offset {off} is not aligned to ITEM");
        }

        let mut seen: HashSet<u64> = HashSet::new();
        for &(off, _, _) in &results {
            assert!(seen.insert(off), "duplicate offset {off}");
        }

        assert_eq!(s.len().unwrap(), (THREADS * PER_THREAD * ITEM) as u64);

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
                for _ in 0..2000 {
                    let size = s.len().unwrap();
                    assert_eq!(
                        size % ITEM,
                        0,
                        "torn write: size {size} is not a multiple of {ITEM}"
                    );
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
