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

    // ---- discard ------------------------------------------------------------

    #[test]
    fn discard_removes_bytes_from_tail() {
        let (s, p) = mk_stack();
        let _g = Guard(p);

        s.push(b"abcde").unwrap();
        s.push(b"fghij").unwrap();
        assert_eq!(s.len().unwrap(), 10);

        s.discard(5).unwrap();
        assert_eq!(s.len().unwrap(), 5);
        assert_eq!(s.peek(0).unwrap(), b"abcde");

        s.discard(5).unwrap();
        assert_eq!(s.len().unwrap(), 0);
    }

    #[test]
    fn discard_zero_is_noop() {
        let (s, p) = mk_stack();
        let _g = Guard(p);

        s.push(b"abc").unwrap();
        s.discard(0).unwrap();
        assert_eq!(s.len().unwrap(), 3);
        assert_eq!(s.peek(0).unwrap(), b"abc");
    }

    #[test]
    fn discard_exceeds_size_returns_error() {
        let (s, p) = mk_stack();
        let _g = Guard(p);

        s.push(b"abc").unwrap();
        let err = s.discard(10).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidInput);
        assert_eq!(s.len().unwrap(), 3);
    }

    #[test]
    fn discard_on_empty_returns_error() {
        let (s, p) = mk_stack();
        let _g = Guard(p);

        let err = s.discard(1).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidInput);
    }

    #[test]
    fn discard_leaves_correct_tail() {
        let (s, p) = mk_stack();
        let _g = Guard(p);

        s.push(b"helloworld").unwrap();
        s.discard(5).unwrap();
        assert_eq!(s.len().unwrap(), 5);
        assert_eq!(s.peek(0).unwrap(), b"hello");
    }

    #[test]
    fn discard_persists_across_reopen() {
        let (s, p) = mk_stack();
        let _g = Guard(p.clone());

        s.push(b"hello").unwrap();
        s.push(b"world").unwrap();
        s.discard(5).unwrap();
        drop(s);

        let s2 = BStack::open(&p).unwrap();
        assert_eq!(s2.len().unwrap(), 5);
        assert_eq!(s2.peek(0).unwrap(), b"hello");
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

    // ---- extend -------------------------------------------------------------

    #[test]
    fn extend_appends_zeros() {
        let (s, p) = mk_stack();
        let _g = Guard(p);

        s.push(b"abc").unwrap();
        let off = s.extend(3).unwrap();
        assert_eq!(off, 3);
        assert_eq!(s.len().unwrap(), 6);
        assert_eq!(s.peek(0).unwrap(), b"abc\x00\x00\x00");
    }

    #[test]
    fn extend_zero_is_noop() {
        let (s, p) = mk_stack();
        let _g = Guard(p);

        s.push(b"hello").unwrap();
        let off = s.extend(0).unwrap();
        assert_eq!(off, 5);
        assert_eq!(s.len().unwrap(), 5);
        assert_eq!(s.peek(0).unwrap(), b"hello");
    }

    #[test]
    fn extend_persists_across_reopen() {
        let (s, p) = mk_stack();
        let _g = Guard(p.clone());

        s.push(b"hi").unwrap();
        s.extend(2).unwrap();
        drop(s);

        let s2 = BStack::open(&p).unwrap();
        assert_eq!(s2.peek(0).unwrap(), b"hi\x00\x00");
    }

    // ---- zero (feature-gated) -----------------------------------------------

    #[cfg(feature = "set")]
    #[test]
    fn zero_overwrites_with_zeros() {
        let (s, p) = mk_stack();
        let _g = Guard(p);

        s.push(b"helloworld").unwrap();
        s.zero(5, 5).unwrap();
        assert_eq!(s.peek(0).unwrap(), b"hello\x00\x00\x00\x00\x00");
        assert_eq!(s.len().unwrap(), 10);
    }

    #[cfg(feature = "set")]
    #[test]
    fn zero_at_start() {
        let (s, p) = mk_stack();
        let _g = Guard(p);

        s.push(b"helloworld").unwrap();
        s.zero(0, 5).unwrap();
        assert_eq!(s.peek(0).unwrap(), b"\x00\x00\x00\x00\x00world");
    }

    #[cfg(feature = "set")]
    #[test]
    fn zero_at_exact_end_boundary() {
        let (s, p) = mk_stack();
        let _g = Guard(p);

        s.push(b"hello").unwrap();
        s.zero(3, 2).unwrap();
        assert_eq!(s.peek(0).unwrap(), b"hel\x00\x00");
    }

    #[cfg(feature = "set")]
    #[test]
    fn zero_zero_is_noop() {
        let (s, p) = mk_stack();
        let _g = Guard(p);

        s.push(b"hello").unwrap();
        s.zero(2, 0).unwrap();
        assert_eq!(s.peek(0).unwrap(), b"hello");
        assert_eq!(s.len().unwrap(), 5);
    }

    #[cfg(feature = "set")]
    #[test]
    fn zero_does_not_change_file_size() {
        let (s, p) = mk_stack();
        let _g = Guard(p);

        s.push(b"abcde").unwrap();
        s.zero(1, 3).unwrap();
        assert_eq!(s.len().unwrap(), 5);
        assert_eq!(s.peek(0).unwrap(), b"a\x00\x00\x00e");
    }

    #[cfg(feature = "set")]
    #[test]
    fn zero_rejects_write_past_end() {
        let (s, p) = mk_stack();
        let _g = Guard(p);

        s.push(b"hello").unwrap();
        let err = s.zero(3, 3).unwrap_err(); // 3+3=6 > 5
        assert_eq!(err.kind(), ErrorKind::InvalidInput);
        // File must be unchanged.
        assert_eq!(s.peek(0).unwrap(), b"hello");
    }

    #[cfg(feature = "set")]
    #[test]
    fn zero_rejects_offset_past_end() {
        let (s, p) = mk_stack();
        let _g = Guard(p);

        s.push(b"hello").unwrap();
        let err = s.zero(10, 1).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidInput);
    }

    #[cfg(feature = "set")]
    #[test]
    fn zero_persists_across_reopen() {
        let (s, p) = mk_stack();
        let _g = Guard(p.clone());

        s.push(b"helloworld").unwrap();
        s.zero(5, 5).unwrap();
        drop(s);

        let s2 = BStack::open(&p).unwrap();
        assert_eq!(s2.peek(0).unwrap(), b"hello\x00\x00\x00\x00\x00");
    }

    // ---- io::Write ----------------------------------------------------------

    #[test]
    fn write_appends_and_survives_reopen() {
        use std::io::Write;

        let (mut s, p) = mk_stack();
        let _g = Guard(p.clone());

        s.write_all(b"hello").unwrap();
        s.write_all(b"world").unwrap();
        assert_eq!(s.len().unwrap(), 10);
        assert_eq!(s.peek(0).unwrap(), b"helloworld");

        drop(s);
        let s2 = BStack::open(&p).unwrap();
        assert_eq!(s2.peek(0).unwrap(), b"helloworld");
    }

    #[test]
    fn write_returns_exact_byte_count() {
        use std::io::Write;

        let (mut s, p) = mk_stack();
        let _g = Guard(p);

        assert_eq!(s.write(b"abcde").unwrap(), 5);
        assert_eq!(s.write(b"").unwrap(), 0);
        assert_eq!(s.write(b"x").unwrap(), 1);
        assert_eq!(s.len().unwrap(), 6);
    }

    #[test]
    fn write_empty_slice_is_noop() {
        use std::io::Write;

        let (mut s, p) = mk_stack();
        let _g = Guard(p);

        s.write_all(b"abc").unwrap();
        s.write_all(b"").unwrap();
        assert_eq!(s.len().unwrap(), 3);
        assert_eq!(s.peek(0).unwrap(), b"abc");
    }

    #[test]
    fn write_flush_is_noop() {
        use std::io::Write;

        let (mut s, p) = mk_stack();
        let _g = Guard(p);

        s.push(b"data").unwrap();
        s.flush().unwrap();
        assert_eq!(s.len().unwrap(), 4);
    }

    #[test]
    fn write_shared_ref() {
        use std::io::Write;

        let (s, p) = mk_stack();
        let _g = Guard(p);

        let mut r: &BStack = &s;
        r.write_all(b"abc").unwrap();
        r.write_all(b"def").unwrap();
        assert_eq!(s.peek(0).unwrap(), b"abcdef");
    }

    #[test]
    fn write_shared_ref_returns_exact_byte_count() {
        use std::io::Write;

        let (s, p) = mk_stack();
        let _g = Guard(p);

        let mut r: &BStack = &s;
        assert_eq!(r.write(b"hello").unwrap(), 5);
        assert_eq!(r.write(b"").unwrap(), 0);
    }

    #[test]
    fn write_via_io_copy() {
        use std::io::{Cursor, copy};

        let (mut s, p) = mk_stack();
        let _g = Guard(p);

        let mut src = Cursor::new(b"copied data");
        copy(&mut src, &mut s).unwrap();
        assert_eq!(s.peek(0).unwrap(), b"copied data");
    }

    #[test]
    fn write_via_bufwriter() {
        use std::io::{BufWriter, Write};

        let (s, p) = mk_stack();
        let _g = Guard(p);

        // BufWriter<&BStack> batches writes internally; the final flush
        // pushes everything to the stack as one atomic append.
        let mut bw = BufWriter::new(&s);
        bw.write_all(b"buf").unwrap();
        bw.write_all(b"fered").unwrap();
        bw.flush().unwrap();
        drop(bw);

        assert_eq!(s.peek(0).unwrap(), b"buffered");
    }

    // ---- BStackReader / io::Read --------------------------------------------

    #[test]
    fn reader_reads_bytes_sequentially() {
        use std::io::Read;

        let (s, p) = mk_stack();
        let _g = Guard(p);

        s.push(b"hello").unwrap();
        s.push(b"world").unwrap();

        let mut reader = s.reader();
        let mut buf = [0u8; 5];
        reader.read_exact(&mut buf).unwrap();
        assert_eq!(&buf, b"hello");
        reader.read_exact(&mut buf).unwrap();
        assert_eq!(&buf, b"world");
    }

    #[test]
    fn reader_returns_zero_at_eof() {
        use std::io::Read;

        let (s, p) = mk_stack();
        let _g = Guard(p);

        s.push(b"hi").unwrap();
        let mut reader = s.reader();
        let mut buf = [0u8; 10];
        let n = reader.read(&mut buf).unwrap();
        assert_eq!(n, 2);
        assert_eq!(&buf[..2], b"hi");

        assert_eq!(reader.read(&mut buf).unwrap(), 0);
        assert_eq!(reader.read(&mut buf).unwrap(), 0); // stable after EOF
    }

    #[test]
    fn reader_empty_buf_returns_zero_without_advancing() {
        use std::io::Read;

        let (s, p) = mk_stack();
        let _g = Guard(p);

        s.push(b"hello").unwrap();
        let mut reader = s.reader();

        assert_eq!(reader.read(&mut []).unwrap(), 0);
        assert_eq!(reader.position(), 0); // cursor unchanged
    }

    #[test]
    fn reader_read_from_empty_stack() {
        use std::io::Read;

        let (s, p) = mk_stack();
        let _g = Guard(p);

        let mut reader = s.reader();
        let mut buf = [0u8; 4];
        assert_eq!(reader.read(&mut buf).unwrap(), 0);
    }

    #[test]
    fn reader_read_exact_fails_at_eof() {
        use std::io::Read;

        let (s, p) = mk_stack();
        let _g = Guard(p);

        s.push(b"hi").unwrap();
        let mut reader = s.reader();
        let mut buf = [0u8; 10]; // larger than payload

        let err = reader.read_exact(&mut buf).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::UnexpectedEof);
    }

    #[test]
    fn reader_partial_reads_advance_cursor() {
        use std::io::Read;

        let (s, p) = mk_stack();
        let _g = Guard(p);

        s.push(b"abcdefghij").unwrap();
        let mut reader = s.reader();

        let mut buf = [0u8; 3];
        reader.read_exact(&mut buf).unwrap();
        assert_eq!(&buf, b"abc");
        assert_eq!(reader.position(), 3);

        reader.read_exact(&mut buf).unwrap();
        assert_eq!(&buf, b"def");
        assert_eq!(reader.position(), 6);
    }

    #[test]
    fn reader_read_to_end() {
        use std::io::Read;

        let (s, p) = mk_stack();
        let _g = Guard(p);

        s.push(b"hello").unwrap();
        s.push(b"world").unwrap();

        let mut reader = s.reader_at(3);
        let mut out = Vec::new();
        reader.read_to_end(&mut out).unwrap();
        assert_eq!(out, b"loworld");
        assert_eq!(reader.position(), 10);
    }

    #[test]
    fn reader_at_starts_at_given_offset() {
        use std::io::Read;

        let (s, p) = mk_stack();
        let _g = Guard(p);

        s.push(b"helloworld").unwrap();

        let mut reader = s.reader_at(5);
        let mut buf = [0u8; 5];
        reader.read_exact(&mut buf).unwrap();
        assert_eq!(&buf, b"world");
    }

    #[test]
    fn reader_from_trait() {
        use std::io::Read;

        let (s, p) = mk_stack();
        let _g = Guard(p);

        s.push(b"test").unwrap();

        let mut reader = crate::BStackReader::from(&s);
        let mut buf = [0u8; 4];
        reader.read_exact(&mut buf).unwrap();
        assert_eq!(&buf, b"test");
    }

    #[test]
    fn reader_via_bufreader() {
        use std::io::{BufRead, BufReader};

        let (s, p) = mk_stack();
        let _g = Guard(p);

        s.push(b"line one\nline two\n").unwrap();

        let reader = BufReader::new(s.reader());
        let lines: Vec<String> = reader.lines().map(|l| l.unwrap()).collect();
        assert_eq!(lines, ["line one", "line two"]);
    }

    // ---- BStackReader / io::Seek --------------------------------------------

    #[test]
    fn reader_seek_from_start() {
        use std::io::{Read, Seek, SeekFrom};

        let (s, p) = mk_stack();
        let _g = Guard(p);

        s.push(b"helloworld").unwrap();
        let mut reader = s.reader();

        assert_eq!(reader.seek(SeekFrom::Start(5)).unwrap(), 5);
        assert_eq!(reader.position(), 5);

        let mut buf = [0u8; 5];
        reader.read_exact(&mut buf).unwrap();
        assert_eq!(&buf, b"world");
    }

    #[test]
    fn reader_seek_from_end() {
        use std::io::{Read, Seek, SeekFrom};

        let (s, p) = mk_stack();
        let _g = Guard(p);

        s.push(b"helloworld").unwrap();
        let mut reader = s.reader();

        assert_eq!(reader.seek(SeekFrom::End(-5)).unwrap(), 5);

        let mut buf = [0u8; 5];
        reader.read_exact(&mut buf).unwrap();
        assert_eq!(&buf, b"world");
    }

    #[test]
    fn reader_seek_from_end_zero_returns_len() {
        use std::io::{Seek, SeekFrom};

        let (s, p) = mk_stack();
        let _g = Guard(p);

        s.push(b"helloworld").unwrap();
        let mut reader = s.reader();

        let pos = reader.seek(SeekFrom::End(0)).unwrap();
        assert_eq!(pos, s.len().unwrap());
    }

    #[test]
    fn reader_seek_from_current() {
        use std::io::{Read, Seek, SeekFrom};

        let (s, p) = mk_stack();
        let _g = Guard(p);

        s.push(b"helloworld").unwrap();
        let mut reader = s.reader();

        reader.seek(SeekFrom::Current(3)).unwrap();
        assert_eq!(reader.seek(SeekFrom::Current(2)).unwrap(), 5);
        assert_eq!(reader.position(), 5);

        let mut buf = [0u8; 5];
        reader.read_exact(&mut buf).unwrap();
        assert_eq!(&buf, b"world");
    }

    #[test]
    fn reader_seek_rewind_and_reread() {
        use std::io::{Read, Seek, SeekFrom};

        let (s, p) = mk_stack();
        let _g = Guard(p);

        s.push(b"abcde").unwrap();
        let mut reader = s.reader();

        let mut buf = [0u8; 5];
        reader.read_exact(&mut buf).unwrap();
        assert_eq!(&buf, b"abcde");

        reader.seek(SeekFrom::Start(0)).unwrap();
        assert_eq!(reader.position(), 0);
        reader.read_exact(&mut buf).unwrap();
        assert_eq!(&buf, b"abcde");
    }

    #[test]
    fn reader_seek_read_seek_read() {
        use std::io::{Read, Seek, SeekFrom};

        let (s, p) = mk_stack();
        let _g = Guard(p);

        s.push(b"ABCDEFGHIJ").unwrap();
        let mut reader = s.reader();
        let mut buf = [0u8; 3];

        reader.seek(SeekFrom::Start(7)).unwrap();
        reader.read_exact(&mut buf).unwrap();
        assert_eq!(&buf, b"HIJ");

        reader.seek(SeekFrom::Start(2)).unwrap();
        reader.read_exact(&mut buf).unwrap();
        assert_eq!(&buf, b"CDE");
    }

    #[test]
    fn reader_seek_before_start_returns_error() {
        use std::io::{Seek, SeekFrom};

        let (s, p) = mk_stack();
        let _g = Guard(p);

        s.push(b"hello").unwrap();
        let mut reader = s.reader();

        assert_eq!(
            reader.seek(SeekFrom::End(-10)).unwrap_err().kind(),
            ErrorKind::InvalidInput
        );
        assert_eq!(
            reader.seek(SeekFrom::Current(-1)).unwrap_err().kind(),
            ErrorKind::InvalidInput
        );
    }

    #[test]
    fn reader_seek_past_end_then_read_returns_zero() {
        use std::io::{Read, Seek, SeekFrom};

        let (s, p) = mk_stack();
        let _g = Guard(p);

        s.push(b"hi").unwrap();
        let mut reader = s.reader();

        reader.seek(SeekFrom::Start(100)).unwrap();
        let mut buf = [0u8; 4];
        assert_eq!(reader.read(&mut buf).unwrap(), 0);
    }

    #[cfg(any(unix, windows))]
    #[test]
    fn concurrent_readers_do_not_block_each_other() {
        use std::io::Read;
        use std::sync::Arc;
        use std::thread;

        let (s, p) = mk_stack();
        let _g = Guard(p);

        let payload: Vec<u8> = (0u8..=255).cycle().take(1024).collect();
        s.push(&payload).unwrap();

        let s = Arc::new(s);

        let handles: Vec<_> = (0..16)
            .map(|i| {
                let s = Arc::clone(&s);
                let expected = payload.clone();
                thread::spawn(move || {
                    let mut reader = s.reader_at(i * 4);
                    let mut out = Vec::new();
                    reader.read_to_end(&mut out).unwrap();
                    assert_eq!(out, &expected[i as usize * 4..]);
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }
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

// -------------------------------------------------------------------------
// Allocator tests

#[cfg(all(test, feature = "alloc"))]
mod alloc_tests {
    use crate::BStack;
    use crate::alloc::{BStackAllocator, BStackSlice, LinearBStackAllocator};
    use std::io::{Read, Seek, SeekFrom};
    use std::sync::atomic::{AtomicU64, Ordering};

    fn mk_alloc() -> (LinearBStackAllocator, std::path::PathBuf) {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let id = COUNTER.fetch_add(1, Ordering::Relaxed);
        let pid = std::process::id();
        let path = std::env::temp_dir().join(format!("bstack_alloc_test_{pid}_{id}.bin"));
        let stack = BStack::open(&path).unwrap();
        (LinearBStackAllocator::new(stack), path)
    }

    struct Guard(std::path::PathBuf);
    impl Drop for Guard {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }

    // 1. alloc returns correct offset and len
    #[test]
    fn alloc_offset_and_len() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let s = alloc.alloc(32).unwrap();
        assert_eq!(s.start(), 0);
        assert_eq!(s.len(), 32);
        assert!(!s.is_empty());
        assert_eq!(s.end(), 32);
    }

    // 2. alloc(0) is a valid no-op
    #[test]
    fn alloc_zero_len() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let s = alloc.alloc(0).unwrap();
        assert_eq!(s.len(), 0);
        assert!(s.is_empty());
        assert_eq!(alloc.len().unwrap(), 0);
    }

    // 3. successive allocs produce non-overlapping regions
    #[test]
    fn alloc_sequential_offsets() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let a = alloc.alloc(8).unwrap();
        let b = alloc.alloc(16).unwrap();
        assert_eq!(a.start(), 0);
        assert_eq!(a.len(), 8);
        assert_eq!(b.start(), 8);
        assert_eq!(b.len(), 16);
        assert_eq!(alloc.len().unwrap(), 24);
    }

    // 4. read returns zero bytes from a freshly allocated region
    #[test]
    fn alloc_read_zeros() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let s = alloc.alloc(4).unwrap();
        let data = s.read().unwrap();
        assert_eq!(data, vec![0u8; 4]);
    }

    // 5. read_into with exact-size buffer succeeds
    #[test]
    fn read_into_exact_size() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let s = alloc.alloc(4).unwrap();
        let mut buf = [0u8; 4];
        s.read_into(&mut buf).unwrap();
        assert_eq!(buf, [0u8; 4]);
    }

    // 6. read_into with a shorter buffer reads only what fits
    #[test]
    fn read_into_shorter_buffer() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let s = alloc.alloc(4).unwrap();
        let mut buf = [0xffu8; 3];
        s.read_into(&mut buf).unwrap(); // reads 3 of the 4 slice bytes
        assert_eq!(buf, [0u8; 3]);
    }

    // 6b. read_into with a longer buffer fills only self.len bytes, leaves rest untouched
    #[test]
    fn read_into_longer_buffer() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let s = alloc.alloc(4).unwrap();
        let mut buf = [0xffu8; 6];
        s.read_into(&mut buf).unwrap();
        assert_eq!(&buf[..4], [0u8; 4]);
        assert_eq!(&buf[4..], [0xffu8; 2]); // untouched
    }

    // 7. read_range_into reads the correct sub-range
    #[test]
    fn read_range_into_correct() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let _ = alloc.alloc(8).unwrap(); // offset 0..8, all zeros
        let s = BStackSlice::new(&alloc, 0, 8);
        let mut buf = [0u8; 3];
        s.read_range_into(2, &mut buf).unwrap(); // reads bytes at relative offsets 2, 3, 4
        assert_eq!(buf, [0u8; 3]);
    }

    // 8. read_range_into out of bounds → InvalidInput
    #[test]
    fn read_range_into_out_of_bounds() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let s = alloc.alloc(4).unwrap();
        let mut buf = [0u8; 3];
        let err = s.read_range_into(2, &mut buf).unwrap_err(); // 2+3 > 4
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
    }

    // 9. realloc tail-grow increases len
    #[test]
    fn realloc_tail_grow() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let s = alloc.alloc(8).unwrap();
        let s2 = alloc.realloc(s, 16).unwrap();
        assert_eq!(s2.start(), 0);
        assert_eq!(s2.len(), 16);
        assert_eq!(alloc.len().unwrap(), 16);
    }

    // 10. realloc tail-shrink decreases len
    #[test]
    fn realloc_tail_shrink() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let s = alloc.alloc(16).unwrap();
        let s2 = alloc.realloc(s, 8).unwrap();
        assert_eq!(s2.start(), 0);
        assert_eq!(s2.len(), 8);
        assert_eq!(alloc.len().unwrap(), 8);
    }

    // 11. realloc with same len is a no-op
    #[test]
    fn realloc_same_len() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let s = alloc.alloc(8).unwrap();
        let s2 = alloc.realloc(s, 8).unwrap();
        assert_eq!(s2.start(), 0);
        assert_eq!(s2.len(), 8);
        assert_eq!(alloc.len().unwrap(), 8);
    }

    // 12. realloc non-tail → Unsupported
    #[test]
    fn realloc_non_tail_unsupported() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let s = alloc.alloc(8).unwrap();
        let _ = alloc.alloc(4).unwrap(); // push another on top
        let err = alloc.realloc(s, 16).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::Unsupported);
    }

    // 13. dealloc tail reclaims space
    #[test]
    fn dealloc_tail_reclaims() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let s = alloc.alloc(16).unwrap();
        assert_eq!(alloc.len().unwrap(), 16);
        alloc.dealloc(s).unwrap();
        assert_eq!(alloc.len().unwrap(), 0);
    }

    // 14. dealloc non-tail is no-op
    #[test]
    fn dealloc_non_tail_noop() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let s = alloc.alloc(8).unwrap();
        let _ = alloc.alloc(4).unwrap(); // push another on top
        alloc.dealloc(s).unwrap(); // non-tail: no-op
        assert_eq!(alloc.len().unwrap(), 12); // nothing reclaimed
    }

    // 15. BStackSliceReader sequential read
    #[test]
    fn slice_reader_sequential() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let s = alloc.alloc(8).unwrap();
        let mut reader = s.reader();
        assert_eq!(reader.position(), 0);
        let mut buf = [0u8; 4];
        let n = reader.read(&mut buf).unwrap();
        assert_eq!(n, 4);
        assert_eq!(reader.position(), 4);
        let n = reader.read(&mut buf).unwrap();
        assert_eq!(n, 4);
        assert_eq!(reader.position(), 8);
        // EOF
        let n = reader.read(&mut buf).unwrap();
        assert_eq!(n, 0);
    }

    // 16. BStackSliceReader seek
    #[test]
    fn slice_reader_seek() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let s = alloc.alloc(8).unwrap();
        let mut reader = s.reader();
        let pos = reader.seek(SeekFrom::End(0)).unwrap();
        assert_eq!(pos, 8);
        let pos = reader.seek(SeekFrom::Current(-4)).unwrap();
        assert_eq!(pos, 4);
        let pos = reader.seek(SeekFrom::Start(2)).unwrap();
        assert_eq!(pos, 2);
    }

    // 17. seek before start → InvalidInput
    #[test]
    fn slice_reader_seek_before_start() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let s = alloc.alloc(8).unwrap();
        let mut reader = s.reader();
        let err = reader.seek(SeekFrom::Current(-1)).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
    }

    // 18. reader_at positions correctly
    #[test]
    fn slice_reader_at() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let s = alloc.alloc(8).unwrap();
        let reader = s.reader_at(5);
        assert_eq!(reader.position(), 5);
    }

    // 19. into_stack recovers the BStack
    #[test]
    fn into_stack_recovers() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let _ = alloc.alloc(4).unwrap();
        let stack = alloc.into_stack();
        assert_eq!(stack.len().unwrap(), 4);
    }

    // -------------------------------------------------------------------------
    // write/zero tests (require `set` feature)

    #[cfg(feature = "set")]
    #[test]
    fn write_read_roundtrip() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let s = alloc.alloc(5).unwrap();
        s.write(b"hello").unwrap();
        assert_eq!(s.read().unwrap(), b"hello");
    }

    // write with shorter data writes only what's provided, leaves rest untouched
    #[cfg(feature = "set")]
    #[test]
    fn write_shorter_data() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let s = alloc.alloc(5).unwrap();
        s.write(b"hi").unwrap(); // writes 2 of the 5 slice bytes
        let data = s.read().unwrap();
        assert_eq!(data, b"hi\x00\x00\x00");
    }

    // write with longer data writes only self.len bytes
    #[cfg(feature = "set")]
    #[test]
    fn write_longer_data() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let s = alloc.alloc(3).unwrap();
        s.write(b"hello").unwrap(); // writes only 3 bytes
        assert_eq!(s.read().unwrap(), b"hel");
    }

    #[cfg(feature = "set")]
    #[test]
    fn write_range_partial() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let s = alloc.alloc(5).unwrap();
        s.write_range(1, b"abc").unwrap();
        let data = s.read().unwrap();
        assert_eq!(data, b"\x00abc\x00");
    }

    #[cfg(feature = "set")]
    #[test]
    fn write_range_out_of_bounds() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let s = alloc.alloc(5).unwrap();
        let err = s.write_range(3, b"abc").unwrap_err(); // 3+3 > 5
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
    }

    #[cfg(feature = "set")]
    #[test]
    fn zero_clears_slice() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let s = alloc.alloc(4).unwrap();
        s.write(b"abcd").unwrap();
        s.zero().unwrap();
        assert_eq!(s.read().unwrap(), vec![0u8; 4]);
    }

    #[cfg(feature = "set")]
    #[test]
    fn zero_range_partial() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let s = alloc.alloc(4).unwrap();
        s.write(b"abcd").unwrap();
        s.zero_range(1, 2).unwrap();
        assert_eq!(s.read().unwrap(), b"a\x00\x00d");
    }

    #[cfg(feature = "set")]
    #[test]
    fn zero_range_out_of_bounds() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let s = alloc.alloc(4).unwrap();
        let err = s.zero_range(3, 2).unwrap_err(); // 3+2 > 4
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
    }

    // 20. subslice creates correct sub-slice
    #[test]
    fn subslice_correct() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let s = alloc.alloc(10).unwrap();
        let sub = s.subslice(2, 8);
        assert_eq!(sub.start(), 2);
        assert_eq!(sub.len(), 6);
        assert_eq!(sub.start(), 2);
        assert_eq!(sub.range(), 2..8);
    }

    // 21. subslice with empty range
    #[test]
    fn subslice_empty() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let s = alloc.alloc(10).unwrap();
        let sub = s.subslice(5, 5);
        assert_eq!(sub.start(), 5);
        assert_eq!(sub.len(), 0);
        assert!(sub.is_empty());
    }

    // 22. subslice panics on invalid range
    #[test]
    #[should_panic(expected = "range start must be <= end")]
    fn subslice_invalid_range_start_greater() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let s = alloc.alloc(10).unwrap();
        let _ = s.subslice(8, 5); // start > end
    }

    // 23. subslice panics on out of bounds
    #[test]
    #[should_panic(expected = "range end must be <= slice length")]
    fn subslice_out_of_bounds() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let s = alloc.alloc(10).unwrap();
        let _ = s.subslice(5, 15); // end > len
    }

    // 24. start returns offset
    #[test]
    fn start_returns_offset() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let s = alloc.alloc(10).unwrap();
        assert_eq!(s.start(), 0);
        let sub = s.subslice(3, 7);
        assert_eq!(sub.start(), 3);
    }

    // 25. range returns correct range
    #[test]
    fn range_returns_correct() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let s = alloc.alloc(10).unwrap();
        assert_eq!(s.range(), 0..10);
        let sub = s.subslice(2, 8);
        assert_eq!(sub.range(), 2..8);
    }

    // ---- Debug --------------------------------------------------------------

    #[test]
    fn bstack_debug_contains_version_and_len() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let dbg = format!("{:?}", alloc.stack());
        assert!(dbg.contains("BStack"), "{dbg}");
        assert!(dbg.contains("version"), "{dbg}");
        assert!(dbg.contains("len"), "{dbg}");
        // Version must be a recognisable semver string.
        assert!(dbg.contains("0.1"), "{dbg}");
    }

    #[test]
    fn slice_reader_debug_uses_public_fields() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let s = alloc.alloc(10).unwrap();
        let reader = s.reader_at(3);
        let dbg = format!("{:?}", reader);
        assert!(dbg.contains("BStackSliceReader"), "{dbg}");
        assert!(dbg.contains("start"), "{dbg}");
        assert!(dbg.contains("end"), "{dbg}");
        assert!(dbg.contains("len"), "{dbg}");
        assert!(dbg.contains("cursor"), "{dbg}");
        // Raw struct field "offset" must not appear in output.
        assert!(!dbg.contains("\"offset\""), "raw field in debug: {dbg}");
    }

    #[cfg(feature = "set")]
    #[test]
    fn slice_writer_debug_uses_public_fields() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let s = alloc.alloc(10).unwrap();
        let writer = s.writer_at(3);
        let dbg = format!("{:?}", writer);
        assert!(dbg.contains("BStackSliceWriter"), "{dbg}");
        assert!(dbg.contains("start"), "{dbg}");
        assert!(dbg.contains("end"), "{dbg}");
        assert!(dbg.contains("len"), "{dbg}");
        assert!(dbg.contains("cursor"), "{dbg}");
        assert!(!dbg.contains("\"offset\""), "raw field in debug: {dbg}");
    }

    // ---- Ord for BStackSliceReader ------------------------------------------

    #[test]
    fn reader_ord_by_absolute_position() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let s = alloc.alloc(10).unwrap();
        let r0 = s.reader_at(0);
        let r5 = s.reader_at(5);
        let r10 = s.reader_at(10);
        assert!(r0 < r5);
        assert!(r5 < r10);
        assert!(r0 < r10);
        assert_eq!(r5.cmp(&s.reader_at(5)), std::cmp::Ordering::Equal);
    }

    #[test]
    fn reader_ord_earlier_slice_before_later() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let a = alloc.alloc(8).unwrap(); // offset 0..8
        let b = alloc.alloc(8).unwrap(); // offset 8..16
        assert!(a.reader() < b.reader());
    }

    #[test]
    fn reader_ord_same_abs_position_shorter_len_less() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let s = alloc.alloc(10).unwrap();
        let short = s.subslice(0, 3).reader();
        let long_ = s.subslice(0, 8).reader();
        // Both cursors are at absolute position 0; shorter slice is less.
        assert!(short < long_);
    }

    // ---- Ord for BStackSliceWriter ------------------------------------------

    #[cfg(feature = "set")]
    #[test]
    fn writer_ord_by_absolute_position() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let s = alloc.alloc(10).unwrap();
        let w0 = s.writer_at(0);
        let w5 = s.writer_at(5);
        assert!(w0 < w5);
        assert_eq!(w5.cmp(&s.writer_at(5)), std::cmp::Ordering::Equal);
    }

    #[cfg(feature = "set")]
    #[test]
    fn writer_ord_earlier_slice_before_later() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let a = alloc.alloc(8).unwrap(); // offset 0..8
        let b = alloc.alloc(8).unwrap(); // offset 8..16
        assert!(a.writer() < b.writer());
    }

    // ---- Cross-type PartialOrd (reader ↔ writer) ----------------------------

    #[cfg(feature = "set")]
    #[test]
    fn reader_writer_cross_partial_ord() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let s = alloc.alloc(10).unwrap();
        let r3 = s.reader_at(3);
        let w5 = s.writer_at(5);
        let w3 = s.writer_at(3);
        let r5 = s.reader_at(5);
        assert!(r3 < w5);
        assert!(w3 < r5);
        assert_eq!(r3.partial_cmp(&w3), Some(std::cmp::Ordering::Equal));
        assert_eq!(w5.partial_cmp(&r5), Some(std::cmp::Ordering::Equal));
    }

    #[cfg(feature = "set")]
    #[test]
    fn reader_writer_cross_ord_transitivity() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let s = alloc.alloc(20).unwrap();
        // r2 < w8 < r15: check transitivity across types
        let r2 = s.reader_at(2);
        let w8 = s.writer_at(8);
        let r15 = s.reader_at(15);
        assert!(r2 < w8);
        assert!(w8 < r15);
        assert!(r2 < r15);
    }
}

// -------------------------------------------------------------------------
// FirstFitBStackAllocator tests

#[cfg(all(test, feature = "alloc", feature = "set"))]
mod first_fit_tests {
    use crate::BStack;
    use crate::alloc::{BStackAllocator, FirstFitBStackAllocator};
    use std::sync::atomic::{AtomicU64, Ordering};

    // Layout constants mirrored from the allocator (kept local to tests).
    const ALFF_HDR_OFFSET: u64 = 48; // arena start = OFFSET_SIZE(16) + HEADER_SIZE(32)
    const BLOCK_OVERHEAD: u64 = 24; // BLOCK_HEADER_SIZE(16) + BLOCK_FOOTER_SIZE(8)
    const MIN_PAYLOAD: u64 = 16;
    const FREE_HEAD_OFFSET: u64 = 32; // absolute payload offset of free_head field

    fn mk_ff(id_prefix: &str) -> (FirstFitBStackAllocator, std::path::PathBuf) {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let id = COUNTER.fetch_add(1, Ordering::Relaxed);
        let pid = std::process::id();
        let path = std::env::temp_dir().join(format!("bstack_ff_test_{id_prefix}_{pid}_{id}.bin"));
        let stack = BStack::open(&path).unwrap();
        (FirstFitBStackAllocator::new(stack).unwrap(), path)
    }

    struct Guard(std::path::PathBuf);
    impl Drop for Guard {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }

    // -----------------------------------------------------------------------
    // Initialisation

    #[test]
    fn new_empty_stack_initialises_header() {
        let (alloc, path) = mk_ff("init");
        let _g = Guard(path);
        // Stack should contain exactly the 48-byte header region
        assert_eq!(alloc.len().unwrap(), ALFF_HDR_OFFSET);
    }

    #[test]
    fn new_rejects_bad_magic() {
        static C: AtomicU64 = AtomicU64::new(0);
        let id = C.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "bstack_ff_badmagic_{}_{}.bin",
            std::process::id(),
            id
        ));
        let _g = Guard(path.clone());
        {
            let stack = BStack::open(&path).unwrap();
            // Push 48 bytes with wrong magic
            let mut hdr = [0u8; 48];
            hdr[16..24].copy_from_slice(b"WRONGHDR");
            stack.push(&hdr).unwrap();
        }
        let stack = BStack::open(&path).unwrap();
        assert!(FirstFitBStackAllocator::new(stack).is_err());
    }

    #[test]
    fn new_reopens_existing_file() {
        let (alloc, path) = mk_ff("reopen");
        let _g = Guard(path.clone());
        let s = alloc.alloc(32).unwrap();
        s.write(b"hello world reopen test!!!!!!!!! ").unwrap();
        let s_start = s.start();
        let _ = s;
        drop(alloc.into_stack());

        let stack2 = BStack::open(&path).unwrap();
        let alloc2 = FirstFitBStackAllocator::new(stack2).unwrap();
        let mut buf = [0u8; 11];
        alloc2.stack().get_into(s_start, &mut buf).unwrap();
        assert_eq!(&buf, b"hello world");
    }

    // -----------------------------------------------------------------------
    // Alloc: offsets, alignment, and zero-init

    #[test]
    fn alloc_first_block_payload_starts_after_header() {
        let (alloc, path) = mk_ff("first_off");
        let _g = Guard(path);
        let s = alloc.alloc(16).unwrap();
        assert_eq!(s.start(), ALFF_HDR_OFFSET + 16); // payload after block header
    }

    #[test]
    fn alloc_returns_len_as_requested() {
        let (alloc, path) = mk_ff("req_len");
        let _g = Guard(path);
        let s = alloc.alloc(17).unwrap(); // not a multiple of 8 or 16
        assert_eq!(s.len(), 17);
    }

    #[test]
    fn alloc_zero_initialises_payload() {
        let (alloc, path) = mk_ff("zero_init");
        let _g = Guard(path);
        let s = alloc.alloc(64).unwrap();
        assert_eq!(s.read().unwrap(), vec![0u8; 64]);
    }

    #[test]
    fn alloc_rounds_up_to_min_16_bytes() {
        let (alloc, path) = mk_ff("min16");
        let _g = Guard(path);
        let s1 = alloc.alloc(4).unwrap();
        let s2 = alloc.alloc(4).unwrap();
        // Second alloc must start 40 bytes after first (min 16 payload + 24 overhead)
        assert_eq!(s2.start() - s1.start(), MIN_PAYLOAD + BLOCK_OVERHEAD);
    }

    #[test]
    fn alloc_rounds_up_to_multiple_of_8() {
        let (alloc, path) = mk_ff("align8");
        let _g = Guard(path);
        let s1 = alloc.alloc(17).unwrap(); // rounds to 24
        let s2 = alloc.alloc(16).unwrap();
        assert_eq!(s2.start() - s1.start(), 24 + BLOCK_OVERHEAD);
    }

    #[test]
    fn alloc_sequential_non_overlapping() {
        let (alloc, path) = mk_ff("seq");
        let _g = Guard(path);
        let a = alloc.alloc(16).unwrap();
        let b = alloc.alloc(32).unwrap();
        let c = alloc.alloc(64).unwrap();
        assert!(a.end() <= b.start());
        assert!(b.end() <= c.start());
        assert_eq!(b.start(), a.start() + 16 + BLOCK_OVERHEAD);
        assert_eq!(c.start(), b.start() + 32 + BLOCK_OVERHEAD);
    }

    // -----------------------------------------------------------------------
    // Dealloc: tail discard

    #[test]
    fn dealloc_tail_shrinks_stack() {
        let (alloc, path) = mk_ff("dealloc_tail");
        let _g = Guard(path);
        let s = alloc.alloc(16).unwrap();
        let before = alloc.len().unwrap();
        alloc.dealloc(s).unwrap();
        assert_eq!(alloc.len().unwrap(), before - 16 - BLOCK_OVERHEAD);
        assert_eq!(alloc.len().unwrap(), ALFF_HDR_OFFSET);
    }

    #[test]
    fn dealloc_non_tail_preserves_stack_len() {
        let (alloc, path) = mk_ff("dealloc_nontail");
        let _g = Guard(path);
        let a = alloc.alloc(16).unwrap();
        let _b = alloc.alloc(16).unwrap();
        let before = alloc.len().unwrap();
        alloc.dealloc(a).unwrap(); // non-tail
        assert_eq!(alloc.len().unwrap(), before); // stack stays the same size
    }

    #[test]
    fn dealloc_cascade_removes_free_tail() {
        // Scenario: alloc A, B. dealloc A (goes to free list). dealloc B (tail discard).
        // cascade_discard_free_tail should then discard A too.
        let (alloc, path) = mk_ff("cascade");
        let _g = Guard(path);
        let a = alloc.alloc(16).unwrap();
        let b = alloc.alloc(16).unwrap();
        alloc.dealloc(a).unwrap(); // non-tail: A goes to free list
        alloc.dealloc(b).unwrap(); // tail: B discarded, then A becomes tail → cascaded
        // After cascade, stack should be back to just the allocator header
        assert_eq!(alloc.len().unwrap(), ALFF_HDR_OFFSET);
    }

    #[test]
    fn dealloc_cascade_multi_level() {
        // A, B, C all allocated. dealloc A, B (both non-tail). dealloc C (tail).
        // Cascade should remove B, then A.
        let (alloc, path) = mk_ff("cascade_multi");
        let _g = Guard(path);
        let a = alloc.alloc(16).unwrap();
        let b = alloc.alloc(16).unwrap();
        let c = alloc.alloc(16).unwrap();
        alloc.dealloc(a).unwrap();
        alloc.dealloc(b).unwrap();
        alloc.dealloc(c).unwrap(); // cascade removes B then A
        assert_eq!(alloc.len().unwrap(), ALFF_HDR_OFFSET);
    }

    // -----------------------------------------------------------------------
    // Free-list reuse

    #[test]
    fn alloc_reuses_freed_block() {
        let (alloc, path) = mk_ff("reuse");
        let _g = Guard(path);
        let a = alloc.alloc(16).unwrap();
        let _b = alloc.alloc(16).unwrap(); // keep tail allocated so A isn't cascade-discarded
        let a_start = a.start();
        alloc.dealloc(a).unwrap();
        let c = alloc.alloc(16).unwrap(); // should reuse A's slot
        assert_eq!(c.start(), a_start);
    }

    #[test]
    fn reused_block_is_zero_initialised() {
        let (alloc, path) = mk_ff("reuse_zero");
        let _g = Guard(path);
        let a = alloc.alloc(32).unwrap();
        let _b = alloc.alloc(16).unwrap();
        a.write(b"dirty data from previous use!!!!").unwrap();
        alloc.dealloc(a).unwrap();
        let c = alloc.alloc(32).unwrap();
        assert_eq!(c.read().unwrap(), vec![0u8; 32]);
    }

    #[test]
    fn free_list_respects_first_fit_order() {
        let (alloc, path) = mk_ff("first_fit");
        let _g = Guard(path);
        // Interleave with allocated separators so adjacent free blocks can't coalesce.
        let a = alloc.alloc(16).unwrap();
        let _sep1 = alloc.alloc(16).unwrap(); // separator: stays allocated
        let b = alloc.alloc(16).unwrap();
        let _sep2 = alloc.alloc(16).unwrap(); // keeps b non-tail
        let a_start = a.start();
        let b_start = b.start();
        // Free list after both deallocs (prepend): head → b → a
        alloc.dealloc(a).unwrap();
        alloc.dealloc(b).unwrap();
        // First fit returns b (head); no-split (exact 16-byte match)
        let x = alloc.alloc(16).unwrap();
        assert_eq!(x.start(), b_start);
        // Second alloc returns a
        let y = alloc.alloc(16).unwrap();
        assert_eq!(y.start(), a_start);
    }

    // -----------------------------------------------------------------------
    // Block splitting

    #[test]
    fn alloc_splits_large_free_block() {
        let (alloc, path) = mk_ff("split");
        let _g = Guard(path);
        // Alloc a 64-byte block, then a sentinel, then free the 64-byte block.
        let big = alloc.alloc(64).unwrap();
        let _sentinel = alloc.alloc(16).unwrap();
        let big_start = big.start();
        alloc.dealloc(big).unwrap();

        // Split puts the 16-byte allocation at the BACK of the 64-byte block.
        // remaining = 64 - 16 - 24 = 24; allocated payload = big_start + 24 + 24 = big_start + 48
        let small = alloc.alloc(16).unwrap();
        assert_eq!(small.start(), big_start + 48);
        assert_eq!(small.len(), 16);

        // The 24-byte free remainder occupies the front (big_start)
        let remainder = alloc.alloc(24).unwrap();
        assert_eq!(remainder.start(), big_start);
    }

    #[test]
    fn alloc_takes_whole_block_when_split_would_be_too_small() {
        let (alloc, path) = mk_ff("nosplit");
        let _g = Guard(path);
        // A 32-byte free block: 32 - 24 - 1 = 7 < MIN_PAYLOAD(16), so no split for a 17-byte request
        // (rounds to 24, and 32 - 24 - 24 = -16 → no split)
        let block = alloc.alloc(32).unwrap();
        let _sentinel = alloc.alloc(16).unwrap();
        let block_start = block.start();
        alloc.dealloc(block).unwrap();
        let reused = alloc.alloc(24).unwrap(); // 32 - 24 - 24 < 0 → no split
        assert_eq!(reused.start(), block_start);
        assert_eq!(reused.len(), 24); // len is what was requested
    }

    // -----------------------------------------------------------------------
    // Coalescing

    #[test]
    fn coalesce_right_merges_with_next_free_block() {
        let (alloc, path) = mk_ff("coal_right");
        let _g = Guard(path);
        let a = alloc.alloc(16).unwrap();
        let b = alloc.alloc(16).unwrap();
        let _sentinel = alloc.alloc(16).unwrap();
        let a_start = a.start();
        alloc.dealloc(b).unwrap(); // B goes to free list first
        alloc.dealloc(a).unwrap(); // A coalesces right with B → merged = 16+16+24 = 56 bytes

        // Should get back the merged block (a_start) for a 48-byte request
        let merged = alloc.alloc(48).unwrap();
        assert_eq!(merged.start(), a_start);
    }

    #[test]
    fn coalesce_left_merges_into_prev_free_block() {
        let (alloc, path) = mk_ff("coal_left");
        let _g = Guard(path);
        let a = alloc.alloc(16).unwrap();
        let b = alloc.alloc(16).unwrap();
        let _sentinel = alloc.alloc(16).unwrap();
        let a_start = a.start();
        alloc.dealloc(a).unwrap(); // A goes to free list
        alloc.dealloc(b).unwrap(); // B coalesces left into A → merged block starts at a_start

        let merged = alloc.alloc(48).unwrap();
        assert_eq!(merged.start(), a_start);
    }

    #[test]
    fn coalesce_both_sides() {
        let (alloc, path) = mk_ff("coal_both");
        let _g = Guard(path);
        let a = alloc.alloc(16).unwrap();
        let b = alloc.alloc(16).unwrap();
        let c = alloc.alloc(16).unwrap();
        let _sentinel = alloc.alloc(16).unwrap();
        let a_start = a.start();
        alloc.dealloc(a).unwrap();
        alloc.dealloc(c).unwrap();
        alloc.dealloc(b).unwrap(); // B coalesces with both A and C → 16+16+16+24+24 = 96 bytes

        let merged = alloc.alloc(88).unwrap(); // 3×16 + 2×24 - overhead = 88 bytes of payload
        assert_eq!(merged.start(), a_start);
    }

    #[test]
    fn coalesce_data_is_zeroed_in_reused_merged_block() {
        let (alloc, path) = mk_ff("coal_zero");
        let _g = Guard(path);
        let a = alloc.alloc(16).unwrap();
        let b = alloc.alloc(16).unwrap();
        let _sentinel = alloc.alloc(16).unwrap();
        a.write(b"AAAAAAAAAAAAAAAA").unwrap();
        b.write(b"BBBBBBBBBBBBBBBB").unwrap();
        alloc.dealloc(b).unwrap();
        alloc.dealloc(a).unwrap(); // right-coalesce
        let merged = alloc.alloc(48).unwrap();
        assert_eq!(merged.read().unwrap(), vec![0u8; 48]);
    }

    // -----------------------------------------------------------------------
    // Realloc

    #[test]
    fn realloc_tail_grow() {
        let (alloc, path) = mk_ff("realloc_tail_grow");
        let _g = Guard(path);
        let s = alloc.alloc(16).unwrap();
        let s2 = alloc.realloc(s, 32).unwrap();
        assert_eq!(s2.start(), s.start());
        assert_eq!(s2.len(), 32);
        assert_eq!(alloc.len().unwrap(), ALFF_HDR_OFFSET + 32 + BLOCK_OVERHEAD);
    }

    #[test]
    fn realloc_tail_shrink() {
        let (alloc, path) = mk_ff("realloc_tail_shrink");
        let _g = Guard(path);
        let s = alloc.alloc(32).unwrap();
        let s2 = alloc.realloc(s, 16).unwrap();
        assert_eq!(s2.start(), s.start());
        assert_eq!(s2.len(), 16);
        assert_eq!(alloc.len().unwrap(), ALFF_HDR_OFFSET + 16 + BLOCK_OVERHEAD);
    }

    #[test]
    fn realloc_tail_preserves_data() {
        let (alloc, path) = mk_ff("realloc_tail_data");
        let _g = Guard(path);
        let s = alloc.alloc(16).unwrap();
        s.write(b"hello world!!!!").unwrap();
        let s2 = alloc.realloc(s, 32).unwrap();
        let data = s2.read().unwrap();
        assert_eq!(&data[..15], b"hello world!!!!");
        assert_eq!(&data[16..], vec![0u8; 16]);
    }

    #[test]
    fn realloc_same_aligned_len_is_noop() {
        let (alloc, path) = mk_ff("realloc_same");
        let _g = Guard(path);
        let s = alloc.alloc(16).unwrap();
        let before_len = alloc.len().unwrap();
        let s2 = alloc.realloc(s, 16).unwrap();
        assert_eq!(s2.start(), s.start());
        assert_eq!(alloc.len().unwrap(), before_len);
    }

    #[test]
    fn realloc_nontail_moves_to_new_block() {
        let (alloc, path) = mk_ff("realloc_move");
        let _g = Guard(path);
        let a = alloc.alloc(16).unwrap();
        let _b = alloc.alloc(16).unwrap(); // keeps a non-tail
        let a_start = a.start();
        let a2 = alloc.realloc(a, 64).unwrap(); // no free block of size 64 → extends
        assert!(a2.start() != a_start); // moved
        assert_eq!(a2.len(), 64);
    }

    #[test]
    fn realloc_nontail_preserves_data() {
        let (alloc, path) = mk_ff("realloc_move_data");
        let _g = Guard(path);
        let a = alloc.alloc(16).unwrap();
        let _b = alloc.alloc(16).unwrap();
        a.write(b"preserved!!!!!!!").unwrap();
        let a2 = alloc.realloc(a, 32).unwrap();
        let data = a2.read().unwrap();
        assert_eq!(&data[..16], b"preserved!!!!!!!");
        assert_eq!(&data[16..], vec![0u8; 16]);
    }

    #[test]
    fn realloc_nontail_frees_old_block_for_reuse() {
        let (alloc, path) = mk_ff("realloc_old_free");
        let _g = Guard(path);
        let a = alloc.alloc(16).unwrap();
        let _b = alloc.alloc(16).unwrap();
        let a_start = a.start();
        let _a2 = alloc.realloc(a, 64).unwrap();
        // Old A slot (16 bytes) should now be in the free list
        let reused = alloc.alloc(16).unwrap();
        assert_eq!(reused.start(), a_start);
    }

    #[test]
    fn realloc_nontail_same_block_when_fits() {
        let (alloc, path) = mk_ff("realloc_inplace");
        let _g = Guard(path);
        // alloc 64, then make it non-tail, then realloc to 32 — block is big enough, no move
        let a = alloc.alloc(64).unwrap();
        let _b = alloc.alloc(16).unwrap();
        let a_start = a.start();
        let a2 = alloc.realloc(a, 32).unwrap();
        assert_eq!(a2.start(), a_start); // stayed in place
        assert_eq!(a2.len(), 32);
    }

    // -----------------------------------------------------------------------
    // Realloc: in-place merge with adjacent free block

    #[test]
    fn realloc_inplace_merge_no_split() {
        // A(16) | B(16=free) | C(sentinel)
        // merged_size = 16+24+16 = 56; grow A to 56 → exact fit, no split.
        let (alloc, path) = mk_ff("merge_nosplit");
        let _g = Guard(path);
        let a = alloc.alloc(16).unwrap();
        let b = alloc.alloc(16).unwrap();
        let _c = alloc.alloc(16).unwrap();
        let a_start = a.start();
        alloc.dealloc(b).unwrap();
        let a2 = alloc.realloc(a, 56).unwrap();
        assert_eq!(a2.start(), a_start);
        assert_eq!(a2.len(), 56);
    }

    #[test]
    fn realloc_inplace_merge_with_split() {
        // A(16) | B(80=free) | C(sentinel)
        // merged = 16+24+80 = 120; grow A to 32 → remainder = 120-32-24 = 64.
        let (alloc, path) = mk_ff("merge_split");
        let _g = Guard(path);
        let a = alloc.alloc(16).unwrap();
        let b = alloc.alloc(80).unwrap();
        let _c = alloc.alloc(16).unwrap();
        let a_start = a.start();
        alloc.dealloc(b).unwrap();
        let a2 = alloc.realloc(a, 32).unwrap();
        assert_eq!(a2.start(), a_start);
        assert_eq!(a2.len(), 32);
        // The 64-byte remainder should be back in the free list.
        let rem = alloc.alloc(64).unwrap();
        assert_eq!(rem.start(), a_start + 32 + BLOCK_OVERHEAD);
    }

    #[test]
    fn realloc_inplace_merge_preserves_data_and_zeroes_new_area() {
        // Grow in-place via merge; existing bytes survive, new bytes are zero.
        let (alloc, path) = mk_ff("merge_data");
        let _g = Guard(path);
        let a = alloc.alloc(16).unwrap();
        let b = alloc.alloc(64).unwrap();
        let _c = alloc.alloc(16).unwrap();
        a.write(b"0123456789ABCDEF").unwrap();
        alloc.dealloc(b).unwrap();
        // merged = 16+24+64 = 104; grow to 40 → remainder = 104-40-24 = 40
        let a2 = alloc.realloc(a, 40).unwrap();
        let data = a2.read().unwrap();
        assert_eq!(&data[..16], b"0123456789ABCDEF");
        assert_eq!(&data[16..], vec![0u8; 24]);
    }

    #[test]
    fn realloc_inplace_merge_split_remainder_is_zero_initialised() {
        // The split remainder is fresh free space; next alloc into it should be zeroed.
        let (alloc, path) = mk_ff("merge_rem_zero");
        let _g = Guard(path);
        let a = alloc.alloc(16).unwrap();
        let b = alloc.alloc(80).unwrap();
        let _c = alloc.alloc(16).unwrap();
        // Write garbage into B so the overlap area is dirty before freeing.
        b.write(&vec![0xFFu8; 80]).unwrap();
        alloc.dealloc(b).unwrap();
        let _a2 = alloc.realloc(a, 32).unwrap(); // merge + split
        let rem = alloc.alloc(64).unwrap();
        assert_eq!(rem.read().unwrap(), vec![0u8; 64]);
    }

    #[test]
    fn realloc_inplace_merge_threshold_boundary() {
        // merged_size = aligned_new_len + BLOCK_OVERHEAD + MIN_PAYLOAD exactly → split happens,
        // remainder == MIN_PAYLOAD (= 16 bytes, the smallest valid free block).
        // A(16) | B(56=free) | C(sentinel)
        // merged = 16+24+56 = 96; grow A to 16+BLOCK_OVERHEAD+MIN_PAYLOAD subtracted away:
        // aligned_new_len = 96 - 24 - 16 = 56; but that leaves remainder=16. Use aligned_new_len=56.
        // split condition: 96 >= 56 + 24 + 16 = 96 ✓ (>=, not >)
        let (alloc, path) = mk_ff("merge_boundary");
        let _g = Guard(path);
        let a = alloc.alloc(16).unwrap();
        let b = alloc.alloc(56).unwrap();
        let _c = alloc.alloc(16).unwrap();
        let a_start = a.start();
        alloc.dealloc(b).unwrap();
        let a2 = alloc.realloc(a, 56).unwrap();
        assert_eq!(a2.start(), a_start);
        assert_eq!(a2.len(), 56);
        // Remainder of exactly 16 bytes should be in the free list.
        let rem = alloc.alloc(16).unwrap();
        assert_eq!(rem.start(), a_start + 56 + BLOCK_OVERHEAD);
    }

    #[test]
    fn realloc_inplace_merge_below_threshold_no_split() {
        // merged_size = aligned_new_len + BLOCK_OVERHEAD + MIN_PAYLOAD - 8 → no split.
        // A(16) | B(48=free) | C(sentinel)
        // merged = 16+24+48 = 88; aligned_new_len = 88 - 24 - 16 + 8 = 56.
        // split condition: 88 >= 56 + 40 = 96? No → no split.
        let (alloc, path) = mk_ff("merge_nosplit_thresh");
        let _g = Guard(path);
        let a = alloc.alloc(16).unwrap();
        let b = alloc.alloc(48).unwrap();
        let _c = alloc.alloc(16).unwrap();
        let a_start = a.start();
        alloc.dealloc(b).unwrap();
        let a2 = alloc.realloc(a, 56).unwrap();
        assert_eq!(a2.start(), a_start);
        assert_eq!(a2.len(), 56);
        // No remainder in free list — next alloc must extend the stack.
        let before = alloc.len().unwrap();
        let _x = alloc.alloc(16).unwrap();
        assert!(alloc.len().unwrap() > before);
    }

    // -----------------------------------------------------------------------
    // Recovery: partial-split header repair

    #[test]
    fn recovery_partial_split_repairs_header() {
        // Manually construct the on-disk state left by a crash between the
        // zero_buff write (which wrote the inner footer and second sub-block)
        // and the header-shrink write.  After reopening, recovery must detect
        // the three-point signature and fix the header.
        //
        // Logical layout (post-crash):
        //   [48..64)  block-A header : size=80(H), flags=0
        //   [64..96)  block-A payload (first 32 bytes = valid user data)
        //   [96..104) inner footer   : 32(R)
        //   [104..120) second sub-block header : size=24(F), is_free=1
        //   [120..144) second sub-block payload : zeros
        //   [144..152) outer footer  : 24(F)
        //   [152..168) sentinel header : size=16, flags=0
        //   [168..184) sentinel payload
        //   [184..192) sentinel footer : 16
        //
        // Recovery: detects H=80, F=24 → R=32; validates inner footer and
        // second header; repairs block-A header to 32; adds free block at 120.
        static C: AtomicU64 = AtomicU64::new(0);
        let id = C.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "bstack_ff_partial_split_{}_{}.bin",
            std::process::id(),
            id
        ));
        let _g = Guard(path.clone());

        {
            let stack = BStack::open(&path).unwrap();
            // ALFF header: 16 zero bytes + magic + recovery_needed=1 + rest zeros
            let mut alff = [0u8; 48];
            alff[16..24].copy_from_slice(b"ALFF\x00\x01\x00\x00");
            alff[24..28].copy_from_slice(&1u32.to_le_bytes()); // recovery_needed
            stack.push(&alff).unwrap();

            // Block A header: size=80, flags=0 (allocated, but header not yet shrunk)
            let mut a_hdr = [0u8; 16];
            a_hdr[..8].copy_from_slice(&80u64.to_le_bytes());
            stack.push(&a_hdr).unwrap();

            // Block A payload (80 bytes): inner footer + second sub-block embedded
            let mut a_pay = [0u8; 80];
            // [32..40): inner footer = R=32
            a_pay[32..40].copy_from_slice(&32u64.to_le_bytes());
            // [40..48): second sub-block size = F=24
            a_pay[40..48].copy_from_slice(&24u64.to_le_bytes());
            // [48..52): is_free = 1
            a_pay[48..52].copy_from_slice(&1u32.to_le_bytes());
            // [52..80): zeros (reserved + second sub-block payload)
            stack.push(&a_pay).unwrap();

            // Outer footer: F=24
            stack.push(&24u64.to_le_bytes()).unwrap();

            // Sentinel block: header(size=16,flags=0) + payload(16 zeros) + footer(16)
            let mut sent = [0u8; 40];
            sent[..8].copy_from_slice(&16u64.to_le_bytes());
            sent[32..40].copy_from_slice(&16u64.to_le_bytes());
            stack.push(&sent).unwrap();
        }

        let alloc = FirstFitBStackAllocator::new(BStack::open(&path).unwrap()).unwrap();
        // After recovery: block-A header fixed to 32, free block at 120 (size=24).
        // alloc(24) must return the repaired free block.
        let s = alloc.alloc(24).unwrap();
        assert_eq!(s.start(), 120); // ALFF_HDR_OFFSET(48) + block_hdr(16) + R(32) + footer(8) + hdr(16) = 120
    }

    // -----------------------------------------------------------------------
    // Persistence

    #[test]
    fn alloc_persists_across_reopen() {
        let (alloc, path) = mk_ff("persist");
        let _g = Guard(path.clone());
        let s = alloc.alloc(8).unwrap();
        s.write(b"durably!").unwrap();
        let start = s.start();
        drop(alloc.into_stack());

        let stack2 = BStack::open(&path).unwrap();
        let alloc2 = FirstFitBStackAllocator::new(stack2).unwrap();
        let mut buf = [0u8; 8];
        alloc2.stack().get_into(start, &mut buf).unwrap();
        assert_eq!(&buf, b"durably!");
    }

    #[test]
    fn free_list_persists_across_reopen() {
        let (alloc, path) = mk_ff("persist_free");
        let _g = Guard(path.clone());
        let a = alloc.alloc(16).unwrap();
        let _b = alloc.alloc(16).unwrap();
        let a_start = a.start();
        alloc.dealloc(a).unwrap();
        drop(alloc.into_stack());

        let stack2 = BStack::open(&path).unwrap();
        let alloc2 = FirstFitBStackAllocator::new(stack2).unwrap();
        let reused = alloc2.alloc(16).unwrap();
        assert_eq!(reused.start(), a_start);
    }

    // -----------------------------------------------------------------------
    // Recovery

    #[test]
    fn recovery_rebuilds_free_list_after_corruption() {
        let (alloc, path) = mk_ff("recovery");
        let _g = Guard(path.clone());
        let a = alloc.alloc(16).unwrap();
        let b = alloc.alloc(16).unwrap();
        let _c = alloc.alloc(16).unwrap();
        let a_start = a.start();
        let _b_start = b.start();
        alloc.dealloc(a).unwrap();
        alloc.dealloc(b).unwrap();
        let stack = alloc.into_stack();

        // Corrupt: set recovery_needed=1 and scramble free_head to garbage
        stack.set(24, &1u32.to_le_bytes()).unwrap(); // flags byte → recovery_needed=1
        stack
            .set(FREE_HEAD_OFFSET, &0xDEADBEEFu64.to_le_bytes())
            .unwrap();
        drop(stack);

        // Re-open: recovery should run and rebuild the free list from is_free flags
        let stack2 = BStack::open(&path).unwrap();
        let alloc2 = FirstFitBStackAllocator::new(stack2).unwrap();

        // The merged A+B block (size=56) is recovered. alloc(16) splits it:
        // remaining=16 stays at a_start, allocated(16) goes to a_start+16+24=a_start+40.
        // Then alloc(16) takes the 16-byte remainder at a_start (no split).
        let r1 = alloc2.alloc(16).unwrap();
        let r2 = alloc2.alloc(16).unwrap();
        let mut starts = [r1.start(), r2.start()];
        starts.sort();
        // a_start + 40 and a_start (the split back-allocates, remainder is front)
        let mut expected = [a_start, a_start + 40];
        expected.sort();
        assert_eq!(starts, expected);
    }

    #[test]
    fn recovery_truncates_partial_tail_block() {
        use std::io::Write;
        let (alloc, path) = mk_ff("recovery_trunc");
        let _g = Guard(path.clone());
        let _a = alloc.alloc(16).unwrap();
        let stack = alloc.into_stack();
        let before_len = stack.len().unwrap();
        drop(stack);

        // Append partial block bytes (less than BLOCK_OVERHEAD=24) directly to the file
        {
            use std::fs::OpenOptions;
            let mut f = OpenOptions::new().append(true).open(&path).unwrap();
            f.write_all(&[0u8; 12]).unwrap(); // 12 < 24 = partial block
        }

        // Set recovery_needed via raw write to flags offset (payload offset 24)
        {
            use std::fs::OpenOptions;
            use std::io::{Seek, SeekFrom};
            let mut f = OpenOptions::new().write(true).open(&path).unwrap();
            f.seek(SeekFrom::Start(16 + 24)).unwrap(); // file_header(16) + payload_offset(24)
            f.write_all(&1u32.to_le_bytes()).unwrap();
        }

        let stack2 = BStack::open(&path).unwrap();
        let alloc2 = FirstFitBStackAllocator::new(stack2).unwrap();
        // After recovery, partial bytes should be discarded
        assert_eq!(alloc2.len().unwrap(), before_len);
    }

    // -----------------------------------------------------------------------
    // into_stack / stack() accessors

    #[test]
    fn into_stack_returns_underlying_bstack() {
        let (alloc, path) = mk_ff("into_stack");
        let _g = Guard(path);
        let _ = alloc.alloc(16).unwrap();
        let stack = alloc.into_stack();
        assert!(stack.len().unwrap() > ALFF_HDR_OFFSET);
    }

    #[test]
    fn stack_accessor_exposes_raw_reads() {
        let (alloc, path) = mk_ff("stack_acc");
        let _g = Guard(path);
        let s = alloc.alloc(8).unwrap();
        s.write(b"testdata").unwrap();
        let raw = alloc.stack().get(s.start(), s.start() + 8).unwrap();
        assert_eq!(raw, b"testdata");
    }
}

// -------------------------------------------------------------------------
// Atomic compound-operation tests

#[cfg(all(test, feature = "atomic"))]
mod atomic_tests {
    use crate::BStack;
    use std::io::ErrorKind;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn mk_stack() -> (BStack, std::path::PathBuf) {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let id = COUNTER.fetch_add(1, Ordering::Relaxed);
        let pid = std::process::id();
        let path = std::env::temp_dir().join(format!("bstack_atomic_test_{pid}_{id}.bin"));
        let stack = BStack::open(&path).unwrap();
        (stack, path)
    }

    struct Guard(std::path::PathBuf);
    impl Drop for Guard {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }

    // -----------------------------------------------------------------------
    // atrunc

    #[test]
    fn atrunc_net_truncation() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"helloworld").unwrap();
        s.atrunc(7, b"XY").unwrap();
        assert_eq!(s.len().unwrap(), 5);
        assert_eq!(s.peek(0).unwrap(), b"helXY");
    }

    #[test]
    fn atrunc_net_extension() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"hello").unwrap();
        s.atrunc(2, b"WORLD").unwrap();
        assert_eq!(s.len().unwrap(), 8);
        assert_eq!(s.peek(0).unwrap(), b"helWORLD");
    }

    #[test]
    fn atrunc_same_size() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"helloworld").unwrap();
        s.atrunc(5, b"WORLD").unwrap();
        assert_eq!(s.len().unwrap(), 10);
        assert_eq!(s.peek(0).unwrap(), b"helloWORLD");
    }

    #[test]
    fn atrunc_n_zero_pure_append() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"hello").unwrap();
        s.atrunc(0, b"!!").unwrap();
        assert_eq!(s.len().unwrap(), 7);
        assert_eq!(s.peek(0).unwrap(), b"hello!!");
    }

    #[test]
    fn atrunc_buf_empty_pure_discard() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"helloworld").unwrap();
        s.atrunc(4, b"").unwrap();
        assert_eq!(s.len().unwrap(), 6);
        assert_eq!(s.peek(0).unwrap(), b"hellow");
    }

    #[test]
    fn atrunc_noop() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"hello").unwrap();
        s.atrunc(0, b"").unwrap();
        assert_eq!(s.len().unwrap(), 5);
        assert_eq!(s.peek(0).unwrap(), b"hello");
    }

    #[test]
    fn atrunc_to_empty_then_fill() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"hello").unwrap();
        s.atrunc(5, b"new").unwrap();
        assert_eq!(s.len().unwrap(), 3);
        assert_eq!(s.peek(0).unwrap(), b"new");
    }

    #[test]
    fn atrunc_exceeds_size_returns_error() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"hello").unwrap();
        let err = s.atrunc(10, b"x").unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidInput);
        assert_eq!(s.len().unwrap(), 5);
        assert_eq!(s.peek(0).unwrap(), b"hello");
    }

    #[test]
    fn atrunc_persists_across_reopen() {
        let (s, p) = mk_stack();
        let _g = Guard(p.clone());
        s.push(b"helloworld").unwrap();
        s.atrunc(5, b"AB").unwrap();
        drop(s);
        let s2 = BStack::open(&p).unwrap();
        assert_eq!(s2.len().unwrap(), 7);
        assert_eq!(s2.peek(0).unwrap(), b"helloAB");
    }

    // -----------------------------------------------------------------------
    // splice

    #[test]
    fn splice_returns_popped_bytes() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"helloworld").unwrap();
        let removed = s.splice(5, b"XYZ").unwrap();
        assert_eq!(removed, b"world");
    }

    #[test]
    fn splice_net_extension_updates_size() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"hello").unwrap();
        let removed = s.splice(2, b"LONG!!").unwrap();
        assert_eq!(removed, b"lo");
        assert_eq!(s.len().unwrap(), 9);
        assert_eq!(s.peek(0).unwrap(), b"helLONG!!");
    }

    #[test]
    fn splice_net_truncation_correct_bytes() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"abcdefghij").unwrap(); // 10 bytes
        let removed = s.splice(6, b"XX").unwrap(); // pop last 6, push XX
        assert_eq!(removed, b"efghij"); // last 6 bytes
        assert_eq!(s.len().unwrap(), 6); // 4 remaining + 2 appended
        assert_eq!(s.peek(0).unwrap(), b"abcdXX");
    }

    #[test]
    fn splice_same_size() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"helloworld").unwrap();
        let removed = s.splice(5, b"WORLD").unwrap();
        assert_eq!(removed, b"world");
        assert_eq!(s.len().unwrap(), 10);
        assert_eq!(s.peek(0).unwrap(), b"helloWORLD");
    }

    #[test]
    fn splice_n_zero_returns_empty_appends_buf() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"hello").unwrap();
        let removed = s.splice(0, b"!!").unwrap();
        assert_eq!(removed, b"");
        assert_eq!(s.len().unwrap(), 7);
        assert_eq!(s.peek(0).unwrap(), b"hello!!");
    }

    #[test]
    fn splice_buf_empty_acts_like_pop() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"helloworld").unwrap();
        let removed = s.splice(5, b"").unwrap();
        assert_eq!(removed, b"world");
        assert_eq!(s.len().unwrap(), 5);
        assert_eq!(s.peek(0).unwrap(), b"hello");
    }

    #[test]
    fn splice_noop_returns_empty() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"hello").unwrap();
        let removed = s.splice(0, b"").unwrap();
        assert_eq!(removed, b"");
        assert_eq!(s.len().unwrap(), 5);
    }

    #[test]
    fn splice_exceeds_size_returns_error() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"abc").unwrap();
        let err = s.splice(10, b"x").unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidInput);
        assert_eq!(s.len().unwrap(), 3);
    }

    #[test]
    fn splice_persists_across_reopen() {
        let (s, p) = mk_stack();
        let _g = Guard(p.clone());
        s.push(b"helloworld").unwrap();
        let removed = s.splice(5, b"XYZ").unwrap();
        assert_eq!(removed, b"world");
        drop(s);
        let s2 = BStack::open(&p).unwrap();
        assert_eq!(s2.len().unwrap(), 8);
        assert_eq!(s2.peek(0).unwrap(), b"helloXYZ");
    }

    // -----------------------------------------------------------------------
    // splice_into

    #[test]
    fn splice_into_fills_old_appends_new() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"helloworld").unwrap();
        let mut old = [0u8; 5];
        s.splice_into(&mut old, b"XYZ").unwrap();
        assert_eq!(&old, b"world");
        assert_eq!(s.len().unwrap(), 8);
        assert_eq!(s.peek(0).unwrap(), b"helloXYZ");
    }

    #[test]
    fn splice_into_net_extension() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"hello").unwrap();
        let mut old = [0u8; 2];
        s.splice_into(&mut old, b"EXTENDED").unwrap();
        assert_eq!(&old, b"lo");
        assert_eq!(s.len().unwrap(), 11);
        assert_eq!(s.peek(0).unwrap(), b"helEXTENDED");
    }

    #[test]
    fn splice_into_net_truncation() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"abcdefghij").unwrap();
        let mut old = [0u8; 7];
        s.splice_into(&mut old, b"XY").unwrap();
        assert_eq!(&old, b"defghij");
        assert_eq!(s.len().unwrap(), 5);
        assert_eq!(s.peek(0).unwrap(), b"abcXY");
    }

    #[test]
    fn splice_into_matches_splice() {
        let (s1, p1) = mk_stack();
        let _g1 = Guard(p1);
        let (s2, p2) = mk_stack();
        let _g2 = Guard(p2);

        s1.push(b"helloworld").unwrap();
        s2.push(b"helloworld").unwrap();

        let vec_removed = s1.splice(4, b"ABCD").unwrap();
        let mut buf_removed = [0u8; 4];
        s2.splice_into(&mut buf_removed, b"ABCD").unwrap();

        assert_eq!(vec_removed.as_slice(), &buf_removed);
        assert_eq!(s1.len().unwrap(), s2.len().unwrap());
        assert_eq!(s1.peek(0).unwrap(), s2.peek(0).unwrap());
    }

    #[test]
    fn splice_into_exceeds_size_returns_error() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"abc").unwrap();
        let mut old = [0u8; 10];
        let err = s.splice_into(&mut old, b"x").unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidInput);
        assert_eq!(s.len().unwrap(), 3);
    }

    // -----------------------------------------------------------------------
    // try_extend

    #[test]
    fn try_extend_matching_size_appends_returns_true() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"hello").unwrap();
        let ok = s.try_extend(5, b"world").unwrap();
        assert!(ok);
        assert_eq!(s.len().unwrap(), 10);
        assert_eq!(s.peek(0).unwrap(), b"helloworld");
    }

    #[test]
    fn try_extend_mismatching_size_returns_false() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"hello").unwrap();
        let ok = s.try_extend(3, b"world").unwrap();
        assert!(!ok);
        assert_eq!(s.len().unwrap(), 5);
        assert_eq!(s.peek(0).unwrap(), b"hello");
    }

    #[test]
    fn try_extend_empty_buf_matching_returns_true() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"hello").unwrap();
        let ok = s.try_extend(5, b"").unwrap();
        assert!(ok);
        assert_eq!(s.len().unwrap(), 5);
    }

    #[test]
    fn try_extend_empty_buf_mismatching_returns_false() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"hello").unwrap();
        let ok = s.try_extend(0, b"").unwrap();
        assert!(!ok);
        assert_eq!(s.len().unwrap(), 5);
    }

    #[test]
    fn try_extend_persists_across_reopen() {
        let (s, p) = mk_stack();
        let _g = Guard(p.clone());
        s.push(b"hello").unwrap();
        s.try_extend(5, b"world").unwrap();
        drop(s);
        let s2 = BStack::open(&p).unwrap();
        assert_eq!(s2.peek(0).unwrap(), b"helloworld");
    }

    // -----------------------------------------------------------------------
    // try_discard

    #[test]
    fn try_discard_matching_size_discards_returns_true() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"helloworld").unwrap();
        let ok = s.try_discard(10, 5).unwrap();
        assert!(ok);
        assert_eq!(s.len().unwrap(), 5);
        assert_eq!(s.peek(0).unwrap(), b"hello");
    }

    #[test]
    fn try_discard_mismatching_size_returns_false() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"helloworld").unwrap();
        let ok = s.try_discard(7, 5).unwrap();
        assert!(!ok);
        assert_eq!(s.len().unwrap(), 10);
    }

    #[test]
    fn try_discard_n_zero_matching_returns_true() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"hello").unwrap();
        let ok = s.try_discard(5, 0).unwrap();
        assert!(ok);
        assert_eq!(s.len().unwrap(), 5);
    }

    #[test]
    fn try_discard_n_zero_mismatching_returns_false() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"hello").unwrap();
        let ok = s.try_discard(3, 0).unwrap();
        assert!(!ok);
        assert_eq!(s.len().unwrap(), 5);
    }

    #[test]
    fn try_discard_n_exceeds_size_when_matching_returns_error() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"hello").unwrap();
        let err = s.try_discard(5, 10).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidInput);
        assert_eq!(s.len().unwrap(), 5);
    }

    #[test]
    fn try_discard_persists_across_reopen() {
        let (s, p) = mk_stack();
        let _g = Guard(p.clone());
        s.push(b"helloworld").unwrap();
        s.try_discard(10, 5).unwrap();
        drop(s);
        let s2 = BStack::open(&p).unwrap();
        assert_eq!(s2.len().unwrap(), 5);
        assert_eq!(s2.peek(0).unwrap(), b"hello");
    }

    // -----------------------------------------------------------------------
    // swap / swap_into / cas  (require set + atomic)

    #[cfg(feature = "set")]
    #[test]
    fn swap_returns_old_stores_new() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"helloworld").unwrap();
        let old = s.swap(5, b"WORLD").unwrap();
        assert_eq!(old, b"world");
        assert_eq!(s.peek(0).unwrap(), b"helloWORLD");
    }

    #[cfg(feature = "set")]
    #[test]
    fn swap_empty_buf_returns_empty_noop() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"hello").unwrap();
        let old = s.swap(0, b"").unwrap();
        assert_eq!(old, b"");
        assert_eq!(s.len().unwrap(), 5);
        assert_eq!(s.peek(0).unwrap(), b"hello");
    }

    #[cfg(feature = "set")]
    #[test]
    fn swap_at_start_offset() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"helloworld").unwrap();
        let old = s.swap(0, b"HELLO").unwrap();
        assert_eq!(old, b"hello");
        assert_eq!(s.peek(0).unwrap(), b"HELLOworld");
    }

    #[cfg(feature = "set")]
    #[test]
    fn swap_does_not_change_file_size() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"abcde").unwrap();
        s.swap(1, b"XYZ").unwrap();
        assert_eq!(s.len().unwrap(), 5);
        assert_eq!(s.peek(0).unwrap(), b"aXYZe");
    }

    #[cfg(feature = "set")]
    #[test]
    fn swap_exceeds_size_returns_error() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"hello").unwrap();
        let err = s.swap(3, b"TOOLONG").unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidInput);
        assert_eq!(s.peek(0).unwrap(), b"hello");
    }

    #[cfg(feature = "set")]
    #[test]
    fn swap_persists_across_reopen() {
        let (s, p) = mk_stack();
        let _g = Guard(p.clone());
        s.push(b"helloworld").unwrap();
        s.swap(5, b"WORLD").unwrap();
        drop(s);
        let s2 = BStack::open(&p).unwrap();
        assert_eq!(s2.peek(0).unwrap(), b"helloWORLD");
    }

    #[cfg(feature = "set")]
    #[test]
    fn swap_into_fills_buf_with_old_stores_new() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"helloworld").unwrap();
        let mut buf = *b"WORLD";
        s.swap_into(5, &mut buf).unwrap();
        assert_eq!(&buf, b"world");
        assert_eq!(s.peek(0).unwrap(), b"helloWORLD");
    }

    #[cfg(feature = "set")]
    #[test]
    fn swap_into_empty_buf_is_noop() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"hello").unwrap();
        s.swap_into(0, &mut []).unwrap();
        assert_eq!(s.len().unwrap(), 5);
        assert_eq!(s.peek(0).unwrap(), b"hello");
    }

    #[cfg(feature = "set")]
    #[test]
    fn swap_into_matches_swap() {
        let (s1, p1) = mk_stack();
        let _g1 = Guard(p1);
        let (s2, p2) = mk_stack();
        let _g2 = Guard(p2);
        s1.push(b"helloworld").unwrap();
        s2.push(b"helloworld").unwrap();

        let vec_old = s1.swap(3, b"XYZ").unwrap();
        let mut buf = *b"XYZ";
        s2.swap_into(3, &mut buf).unwrap();

        assert_eq!(vec_old.as_slice(), &buf);
        assert_eq!(s1.peek(0).unwrap(), s2.peek(0).unwrap());
    }

    #[cfg(feature = "set")]
    #[test]
    fn swap_into_does_not_change_file_size() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"abcde").unwrap();
        let mut buf = *b"XYZ";
        s.swap_into(1, &mut buf).unwrap();
        assert_eq!(s.len().unwrap(), 5);
    }

    #[cfg(feature = "set")]
    #[test]
    fn swap_into_exceeds_size_returns_error() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"hello").unwrap();
        let mut buf = [0u8; 10];
        let err = s.swap_into(0, &mut buf).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidInput);
        assert_eq!(s.peek(0).unwrap(), b"hello");
    }

    #[cfg(feature = "set")]
    #[test]
    fn cas_matching_performs_exchange() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"helloworld").unwrap();
        let ok = s.cas(5, b"world", b"WORLD").unwrap();
        assert!(ok);
        assert_eq!(s.peek(0).unwrap(), b"helloWORLD");
    }

    #[cfg(feature = "set")]
    #[test]
    fn cas_mismatch_returns_false_no_change() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"helloworld").unwrap();
        let ok = s.cas(5, b"xxxxx", b"WORLD").unwrap();
        assert!(!ok);
        assert_eq!(s.peek(0).unwrap(), b"helloworld");
    }

    #[cfg(feature = "set")]
    #[test]
    fn cas_length_mismatch_returns_false() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"hello").unwrap();
        let ok = s.cas(0, b"hel", b"HELLO").unwrap();
        assert!(!ok);
        assert_eq!(s.peek(0).unwrap(), b"hello");
    }

    #[cfg(feature = "set")]
    #[test]
    fn cas_empty_slices_returns_true_noop() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"hello").unwrap();
        let ok = s.cas(0, b"", b"").unwrap();
        assert!(ok);
        assert_eq!(s.len().unwrap(), 5);
        assert_eq!(s.peek(0).unwrap(), b"hello");
    }

    #[cfg(feature = "set")]
    #[test]
    fn cas_exceeds_size_returns_error() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"hello").unwrap();
        let err = s.cas(3, b"TOOLONG", b"TOOLONG").unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidInput);
        assert_eq!(s.peek(0).unwrap(), b"hello");
    }

    #[cfg(feature = "set")]
    #[test]
    fn cas_does_not_change_file_size() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"abcde").unwrap();
        s.cas(1, b"bcd", b"XYZ").unwrap();
        assert_eq!(s.len().unwrap(), 5);
        assert_eq!(s.peek(0).unwrap(), b"aXYZe");
    }

    #[cfg(feature = "set")]
    #[test]
    fn cas_persists_across_reopen() {
        let (s, p) = mk_stack();
        let _g = Guard(p.clone());
        s.push(b"helloworld").unwrap();
        s.cas(5, b"world", b"WORLD").unwrap();
        drop(s);
        let s2 = BStack::open(&p).unwrap();
        assert_eq!(s2.peek(0).unwrap(), b"helloWORLD");
    }

    // -----------------------------------------------------------------------
    // replace

    #[test]
    fn replace_same_size() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"hello world").unwrap();
        s.replace(5, |tail| {
            tail.iter().map(|b| b.to_ascii_uppercase()).collect()
        })
        .unwrap();
        assert_eq!(s.len().unwrap(), 11);
        assert_eq!(s.peek(0).unwrap(), b"hello WORLD");
    }

    #[test]
    fn replace_net_extension() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"hello").unwrap();
        s.replace(2, |_| b"WORLD".to_vec()).unwrap();
        assert_eq!(s.len().unwrap(), 8);
        assert_eq!(s.peek(0).unwrap(), b"helWORLD");
    }

    #[test]
    fn replace_net_truncation() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"helloworld").unwrap();
        s.replace(7, |_| b"XY".to_vec()).unwrap();
        assert_eq!(s.len().unwrap(), 5);
        assert_eq!(s.peek(0).unwrap(), b"helXY");
    }

    #[test]
    fn replace_n_zero_acts_as_append() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"hello").unwrap();
        s.replace(0, |_| b"!!".to_vec()).unwrap();
        assert_eq!(s.len().unwrap(), 7);
        assert_eq!(s.peek(0).unwrap(), b"hello!!");
    }

    #[test]
    fn replace_empty_result_acts_as_discard() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"helloworld").unwrap();
        s.replace(4, |_| vec![]).unwrap();
        assert_eq!(s.len().unwrap(), 6);
        assert_eq!(s.peek(0).unwrap(), b"hellow");
    }

    #[test]
    fn replace_callback_receives_correct_bytes() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"helloworld").unwrap();
        let mut captured = Vec::new();
        s.replace(5, |tail| {
            captured = tail.to_vec();
            tail.to_vec()
        })
        .unwrap();
        assert_eq!(captured, b"world");
        assert_eq!(s.peek(0).unwrap(), b"helloworld");
    }

    #[test]
    fn replace_exceeds_size_returns_error() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"hello").unwrap();
        let err = s.replace(10, |_| vec![]).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidInput);
        assert_eq!(s.len().unwrap(), 5);
        assert_eq!(s.peek(0).unwrap(), b"hello");
    }

    #[test]
    fn replace_persists_across_reopen() {
        let (s, p) = mk_stack();
        let _g = Guard(p.clone());
        s.push(b"helloworld").unwrap();
        s.replace(5, |tail| {
            tail.iter().map(|b| b.to_ascii_uppercase()).collect()
        })
        .unwrap();
        drop(s);
        let s2 = BStack::open(&p).unwrap();
        assert_eq!(s2.len().unwrap(), 10);
        assert_eq!(s2.peek(0).unwrap(), b"helloWORLD");
    }

    // -----------------------------------------------------------------------
    // process

    #[cfg(feature = "set")]
    #[test]
    fn process_mutates_range() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"hello world").unwrap();
        s.process(6, 11, |buf| buf.make_ascii_uppercase()).unwrap();
        assert_eq!(s.len().unwrap(), 11);
        assert_eq!(s.peek(0).unwrap(), b"hello WORLD");
    }

    #[cfg(feature = "set")]
    #[test]
    fn process_middle_range() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"abcdefgh").unwrap();
        s.process(2, 5, |buf| buf.iter_mut().for_each(|b| *b = b'X'))
            .unwrap();
        assert_eq!(s.peek(0).unwrap(), b"abXXXfgh");
    }

    #[cfg(feature = "set")]
    #[test]
    fn process_callback_receives_correct_bytes() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"helloworld").unwrap();
        let mut captured = Vec::new();
        s.process(5, 10, |buf| captured = buf.to_vec()).unwrap();
        assert_eq!(captured, b"world");
        assert_eq!(s.peek(0).unwrap(), b"helloworld");
    }

    #[cfg(feature = "set")]
    #[test]
    fn process_start_end_equal_is_noop() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"hello").unwrap();
        let mut called = false;
        s.process(3, 3, |_| called = true).unwrap();
        assert!(called);
        assert_eq!(s.len().unwrap(), 5);
        assert_eq!(s.peek(0).unwrap(), b"hello");
    }

    #[cfg(feature = "set")]
    #[test]
    fn process_does_not_change_file_size() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"abcde").unwrap();
        s.process(1, 4, |buf| buf.iter_mut().for_each(|b| *b = 0))
            .unwrap();
        assert_eq!(s.len().unwrap(), 5);
    }

    #[cfg(feature = "set")]
    #[test]
    fn process_end_less_than_start_returns_error() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"hello").unwrap();
        let err = s.process(3, 2, |_| {}).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidInput);
        assert_eq!(s.peek(0).unwrap(), b"hello");
    }

    #[cfg(feature = "set")]
    #[test]
    fn process_end_exceeds_size_returns_error() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"hello").unwrap();
        let err = s.process(2, 10, |_| {}).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidInput);
        assert_eq!(s.peek(0).unwrap(), b"hello");
    }

    #[cfg(feature = "set")]
    #[test]
    fn process_persists_across_reopen() {
        let (s, p) = mk_stack();
        let _g = Guard(p.clone());
        s.push(b"helloworld").unwrap();
        s.process(5, 10, |buf| buf.make_ascii_uppercase()).unwrap();
        drop(s);
        let s2 = BStack::open(&p).unwrap();
        assert_eq!(s2.peek(0).unwrap(), b"helloWORLD");
    }
}
