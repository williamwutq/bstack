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
        let sub = s.subslice(2..8);
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
        let sub = s.subslice(5..5);
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
        let _ = s.subslice(8..5); // start > end
    }

    // 23. subslice panics on out of bounds
    #[test]
    #[should_panic(expected = "range end must be <= slice length")]
    fn subslice_out_of_bounds() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let s = alloc.alloc(10).unwrap();
        let _ = s.subslice(5..15); // end > len
    }

    // 24. start returns offset
    #[test]
    fn start_returns_offset() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let s = alloc.alloc(10).unwrap();
        assert_eq!(s.start(), 0);
        let sub = s.subslice(3..7);
        assert_eq!(sub.start(), 3);
    }

    // 25. range returns correct range
    #[test]
    fn range_returns_correct() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let s = alloc.alloc(10).unwrap();
        assert_eq!(s.range(), 0..10);
        let sub = s.subslice(2..8);
        assert_eq!(sub.range(), 2..8);
    }
}
