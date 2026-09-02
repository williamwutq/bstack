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
        let off1 = s.push([]).unwrap();
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

    // ---- resize ---------------------------------------------------------

    #[test]
    fn resize_grows_with_zeros() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"abc").unwrap();
        let initial = s.resize(6).unwrap();
        assert_eq!(initial, 3);
        assert_eq!(s.len().unwrap(), 6);
        assert_eq!(s.peek(0).unwrap(), b"abc\x00\x00\x00");
    }

    #[test]
    fn resize_shrinks() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"helloworld").unwrap();
        let initial = s.resize(5).unwrap();
        assert_eq!(initial, 10);
        assert_eq!(s.len().unwrap(), 5);
        assert_eq!(s.peek(0).unwrap(), b"hello");
    }

    #[test]
    fn resize_same_size_is_noop() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"hello").unwrap();
        let initial = s.resize(5).unwrap();
        assert_eq!(initial, 5);
        assert_eq!(s.peek(0).unwrap(), b"hello");
    }

    #[test]
    fn resize_to_zero_truncates() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"hello").unwrap();
        let initial = s.resize(0).unwrap();
        assert_eq!(initial, 5);
        assert_eq!(s.len().unwrap(), 0);
    }

    #[test]
    fn resize_shrink_below_locked_errors() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"helloworld").unwrap();
        s.lock_up_to(5).unwrap();
        let err = s.resize(3).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidInput);
        assert_eq!(s.len().unwrap(), 10);
    }

    #[test]
    fn resize_persists_across_reopen() {
        let (s, p) = mk_stack();
        let _g = Guard(p.clone());
        s.push(b"hi").unwrap();
        s.resize(4).unwrap();
        drop(s);
        let s2 = BStack::open(&p).unwrap();
        assert_eq!(s2.peek(0).unwrap(), b"hi\x00\x00");
    }

    // ---- ensure ---------------------------------------------------------

    #[test]
    fn ensure_grows_short_payload_with_zeros() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"abc").unwrap();
        let initial = s.ensure(6).unwrap();
        assert_eq!(initial, 3);
        assert_eq!(s.len().unwrap(), 6);
        assert_eq!(s.peek(0).unwrap(), b"abc\x00\x00\x00");
    }

    #[test]
    fn ensure_noop_when_already_long_enough() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"helloworld").unwrap();
        let initial = s.ensure(5).unwrap();
        assert_eq!(initial, 10);
        assert_eq!(s.len().unwrap(), 10);
        assert_eq!(s.peek(0).unwrap(), b"helloworld");
    }

    #[test]
    fn ensure_noop_when_exact_size() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"hello").unwrap();
        let initial = s.ensure(5).unwrap();
        assert_eq!(initial, 5);
        assert_eq!(s.len().unwrap(), 5);
    }

    #[test]
    fn ensure_persists_across_reopen() {
        let (s, p) = mk_stack();
        let _g = Guard(p.clone());
        s.push(b"hi").unwrap();
        s.ensure(4).unwrap();
        drop(s);
        let s2 = BStack::open(&p).unwrap();
        assert_eq!(s2.peek(0).unwrap(), b"hi\x00\x00");
    }

    // ---- ensure_with (feature-gated) -----------------------------------

    #[cfg(feature = "atomic")]
    #[test]
    fn ensure_with_grows_and_calls_callback_on_new_region() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"abc").unwrap();
        let initial = s
            .ensure_with(6, |buf| {
                assert_eq!(buf.len(), 3);
                assert_eq!(buf, &[0u8, 0, 0]);
                buf.copy_from_slice(b"XYZ");
            })
            .unwrap();
        assert_eq!(initial, 3);
        assert_eq!(s.len().unwrap(), 6);
        assert_eq!(s.peek(0).unwrap(), b"abcXYZ");
    }

    #[cfg(feature = "atomic")]
    #[test]
    fn ensure_with_skips_callback_when_already_long_enough() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"helloworld").unwrap();
        let mut called = false;
        let initial = s
            .ensure_with(5, |_| {
                called = true;
            })
            .unwrap();
        assert_eq!(initial, 10);
        assert!(!called);
        assert_eq!(s.peek(0).unwrap(), b"helloworld");
    }

    #[cfg(feature = "atomic")]
    #[test]
    fn ensure_with_persists_across_reopen() {
        let (s, p) = mk_stack();
        let _g = Guard(p.clone());
        s.push(b"hi").unwrap();
        s.ensure_with(5, |buf| buf.copy_from_slice(b"ZZZ")).unwrap();
        drop(s);
        let s2 = BStack::open(&p).unwrap();
        assert_eq!(s2.peek(0).unwrap(), b"hiZZZ");
    }

    // ---- repeat (feature-gated) ---------------------------------------------

    #[cfg(feature = "set")]
    #[test]
    fn repeat_fills_with_pattern_copies() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"............").unwrap(); // 12 bytes
        s.repeat(0, b"ab", 6).unwrap(); // "ab" x 6 = 12 bytes
        assert_eq!(s.peek(0).unwrap(), b"abababababab");
        assert_eq!(s.len().unwrap(), 12);
    }

    #[cfg(feature = "set")]
    #[test]
    fn repeat_at_offset_leaves_neighbours() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"XXXXXXXXXX").unwrap(); // 10 bytes
        s.repeat(2, b"yz", 3).unwrap(); // fills [2,8) with "yzyzyz"
        assert_eq!(s.peek(0).unwrap(), b"XXyzyzyzXX");
    }

    #[cfg(feature = "set")]
    #[test]
    fn repeat_single_byte_pattern_matches_zero_style_fill() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"helloworld").unwrap();
        s.repeat(0, b"\x00", 5).unwrap();
        assert_eq!(s.peek(0).unwrap(), b"\x00\x00\x00\x00\x00world");
    }

    #[cfg(feature = "set")]
    #[test]
    fn repeat_empty_pattern_or_zero_count_is_noop() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"helloworld").unwrap();
        s.repeat(0, b"", 5).unwrap();
        s.repeat(0, b"ab", 0).unwrap();
        assert_eq!(s.peek(0).unwrap(), b"helloworld");
    }

    #[cfg(feature = "set")]
    #[test]
    fn repeat_past_end_is_rejected() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"hello").unwrap();
        let err = s.repeat(0, b"ab", 4).unwrap_err(); // would write 8 into 5
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
        assert_eq!(s.peek(0).unwrap(), b"hello");
    }

    #[cfg(feature = "set")]
    #[test]
    fn repeat_persists_across_reopen() {
        let (s, p) = mk_stack();
        let _g = Guard(p.clone());
        s.push(b"........").unwrap(); // 8 bytes
        s.repeat(0, b"QW", 4).unwrap();
        drop(s);
        let s2 = BStack::open(&p).unwrap();
        assert_eq!(s2.peek(0).unwrap(), b"QWQWQWQW");
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
            s.push(rec).unwrap();
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
                            let off = s.push(data).unwrap();
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
                        s.push([0xBEu8; ITEM as usize]).unwrap();
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

    // ---- extend_sparse ------------------------------------------------------

    #[test]
    fn extend_sparse_writes_prefix_and_zeros_rest() {
        let (s, p) = mk_stack();
        let _g = Guard(p);

        s.push(b"abc").unwrap();
        let off = s.extend_sparse(b"XY", 6).unwrap();
        assert_eq!(off, 3);
        assert_eq!(s.len().unwrap(), 9);
        assert_eq!(s.peek(0).unwrap(), b"abcXY\x00\x00\x00\x00");
    }

    #[test]
    fn extend_sparse_full_length_prefix() {
        let (s, p) = mk_stack();
        let _g = Guard(p);

        let off = s.extend_sparse(b"hello", 5).unwrap();
        assert_eq!(off, 0);
        assert_eq!(s.peek(0).unwrap(), b"hello");
    }

    #[test]
    fn extend_sparse_empty_buf_is_pure_extend() {
        let (s, p) = mk_stack();
        let _g = Guard(p);

        s.push(b"ab").unwrap();
        let off = s.extend_sparse(b"", 4).unwrap();
        assert_eq!(off, 2);
        assert_eq!(s.peek(0).unwrap(), b"ab\x00\x00\x00\x00");
    }

    #[test]
    fn extend_sparse_zero_length_is_noop() {
        let (s, p) = mk_stack();
        let _g = Guard(p);

        s.push(b"data").unwrap();
        let off = s.extend_sparse(b"", 0).unwrap();
        assert_eq!(off, 4);
        assert_eq!(s.len().unwrap(), 4);
    }

    #[test]
    fn extend_sparse_buf_longer_than_length_errors() {
        let (s, p) = mk_stack();
        let _g = Guard(p);

        let err = s.extend_sparse(b"toolong", 3).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidInput);
        assert_eq!(s.len().unwrap(), 0);
    }

    #[test]
    fn extend_sparse_persists_across_reopen() {
        let (s, p) = mk_stack();
        let _g = Guard(p.clone());

        s.push(b"hi").unwrap();
        s.extend_sparse(b"Z", 4).unwrap();
        drop(s);

        let s2 = BStack::open(&p).unwrap();
        assert_eq!(s2.peek(0).unwrap(), b"hiZ\x00\x00\x00");
    }

    // ---- extend_sparse_batched ----------------------------------------------

    #[test]
    fn extend_sparse_batched_scatters_buffers() {
        let (s, p) = mk_stack();
        let _g = Guard(p);

        s.push(b"..").unwrap();
        let off = s
            .extend_sparse_batched(vec![(0u64, b"AA".as_slice()), (5, b"BB".as_slice())], 8)
            .unwrap();
        assert_eq!(off, 2);
        assert_eq!(s.len().unwrap(), 10);
        assert_eq!(s.peek(0).unwrap(), b"..AA\x00\x00\x00BB\x00");
    }

    #[test]
    fn extend_sparse_batched_ignores_empty_and_reorders() {
        let (s, p) = mk_stack();
        let _g = Guard(p);

        let off = s
            .extend_sparse_batched(
                vec![
                    (4u64, b"cc".as_slice()),
                    (0, b"".as_slice()),
                    (0, b"a".as_slice()),
                ],
                6,
            )
            .unwrap();
        assert_eq!(off, 0);
        assert_eq!(s.peek(0).unwrap(), b"a\x00\x00\x00cc");
    }

    #[test]
    fn extend_sparse_batched_overlap_errors() {
        let (s, p) = mk_stack();
        let _g = Guard(p);

        let err = s
            .extend_sparse_batched(vec![(0u64, b"aaa".as_slice()), (2, b"bb".as_slice())], 8)
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidInput);
        assert_eq!(s.len().unwrap(), 0);
    }

    #[test]
    fn extend_sparse_batched_out_of_range_errors() {
        let (s, p) = mk_stack();
        let _g = Guard(p);

        let err = s
            .extend_sparse_batched(vec![(3u64, b"zzz".as_slice())], 5)
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidInput);
        assert_eq!(s.len().unwrap(), 0);
    }

    #[test]
    fn extend_sparse_batched_empty_is_pure_extend() {
        let (s, p) = mk_stack();
        let _g = Guard(p);

        let off = s
            .extend_sparse_batched(Vec::<(u64, Vec<u8>)>::new(), 4)
            .unwrap();
        assert_eq!(off, 0);
        assert_eq!(s.peek(0).unwrap(), b"\x00\x00\x00\x00");
    }

    // ---- try_extend_sparse (atomic) -----------------------------------------

    #[cfg(feature = "atomic")]
    #[test]
    fn try_extend_sparse_matching_size_writes_returns_true() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"hello").unwrap();
        let ok = s.try_extend_sparse(5, b"XY", 6).unwrap();
        assert!(ok);
        assert_eq!(s.len().unwrap(), 11);
        assert_eq!(s.peek(0).unwrap(), b"helloXY\x00\x00\x00\x00");
    }

    #[cfg(feature = "atomic")]
    #[test]
    fn try_extend_sparse_mismatching_size_returns_false() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"hello").unwrap();
        let ok = s.try_extend_sparse(3, b"XY", 6).unwrap();
        assert!(!ok);
        assert_eq!(s.len().unwrap(), 5);
        assert_eq!(s.peek(0).unwrap(), b"hello");
    }

    #[cfg(feature = "atomic")]
    #[test]
    fn try_extend_sparse_buf_longer_than_length_errors_even_on_mismatch() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"hello").unwrap();
        // Size does not match (3 != 5), but the malformed request still errors.
        let err = s.try_extend_sparse(3, b"toolong", 2).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidInput);
        assert_eq!(s.len().unwrap(), 5);
    }

    #[cfg(feature = "atomic")]
    #[test]
    fn try_extend_sparse_persists_across_reopen() {
        let (s, p) = mk_stack();
        let _g = Guard(p.clone());
        s.push(b"hi").unwrap();
        s.try_extend_sparse(2, b"Z", 4).unwrap();
        drop(s);
        let s2 = BStack::open(&p).unwrap();
        assert_eq!(s2.peek(0).unwrap(), b"hiZ\x00\x00\x00");
    }

    // ---- try_extend_sparse_batched (atomic) ---------------------------------

    #[cfg(feature = "atomic")]
    #[test]
    fn try_extend_sparse_batched_matching_scatters_returns_true() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"..").unwrap();
        let ok = s
            .try_extend_sparse_batched(2, vec![(0u64, b"AA".as_slice()), (5, b"BB".as_slice())], 8)
            .unwrap();
        assert!(ok);
        assert_eq!(s.peek(0).unwrap(), b"..AA\x00\x00\x00BB\x00");
    }

    #[cfg(feature = "atomic")]
    #[test]
    fn try_extend_sparse_batched_mismatching_returns_false() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"..").unwrap();
        let ok = s
            .try_extend_sparse_batched(99, vec![(0u64, b"AA".as_slice())], 8)
            .unwrap();
        assert!(!ok);
        assert_eq!(s.len().unwrap(), 2);
    }

    #[cfg(feature = "atomic")]
    #[test]
    fn try_extend_sparse_batched_overlap_errors_even_on_mismatch() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"..").unwrap();
        let err = s
            .try_extend_sparse_batched(
                99,
                vec![(0u64, b"aaa".as_slice()), (2, b"bb".as_slice())],
                8,
            )
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidInput);
        assert_eq!(s.len().unwrap(), 2);
    }

    // The hash is keyed on the fd/handle, not the instance address, so moving
    // the value does not change it. `PartialEq` stays pointer identity, which
    // a moved value cannot violate — nothing else holds a reference to compare.
    #[test]
    fn hash_is_stable_across_a_move() {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        fn hash_of(s: &BStack) -> u64 {
            let mut h = DefaultHasher::new();
            s.hash(&mut h);
            h.finish()
        }

        let (s, path) = mk_stack();
        let _g = Guard(path);
        let before = hash_of(&s);
        let moved = Box::new(s); // forces a move to a new address
        assert_eq!(hash_of(&moved), before);
    }
}

// -------------------------------------------------------------------------
// Allocator tests

#[cfg(all(test, feature = "alloc"))]
mod alloc_tests {
    use crate::BStack;
    use crate::alloc::{BStackAllocator, BStackBulkAllocator, BStackSlice, LinearBStackAllocator};
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
        let s = unsafe { BStackSlice::from_raw_parts(&alloc, 0, 8) };
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

    // The grown tail reads back as zeros, whether it was realised by
    // `extend` (no `atomic`) or `try_extend_zeros` (with `atomic`).
    #[cfg(feature = "set")]
    #[test]
    fn realloc_tail_grow_zero_fills_the_new_bytes() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let s = alloc.alloc(8).unwrap();
        s.write([0xFFu8; 8]).unwrap();
        let s2 = alloc.realloc(s, 16).unwrap();
        let buf = s2.read().unwrap();
        assert!(buf[..8].iter().all(|&b| b == 0xFF));
        assert!(buf[8..].iter().all(|&b| b == 0));
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

    // ---- BStackBulkAllocator: alloc_bulk ------------------------------------

    // 1. Empty lengths → empty Vec, stack unchanged.
    #[test]
    fn bulk_alloc_empty() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let slices = alloc.alloc_bulk([]).unwrap();
        assert!(slices.is_empty());
        assert_eq!(alloc.len().unwrap(), 0);
    }

    // 2. Correct offsets and lengths for a multi-element batch.
    #[test]
    fn bulk_alloc_offsets_and_lens() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let slices = alloc.alloc_bulk([8_u64, 16, 32]).unwrap();
        assert_eq!(slices.len(), 3);
        assert_eq!(slices[0].start(), 0);
        assert_eq!(slices[0].len(), 8);
        assert_eq!(slices[1].start(), 8);
        assert_eq!(slices[1].len(), 16);
        assert_eq!(slices[2].start(), 24);
        assert_eq!(slices[2].len(), 32);
    }

    // 3. Stack length after bulk alloc equals sum of all lengths.
    #[test]
    fn bulk_alloc_stack_len() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        alloc.alloc_bulk([10_u64, 20, 30]).unwrap();
        assert_eq!(alloc.len().unwrap(), 60);
    }

    // 4. Single-element bulk is equivalent to a single alloc.
    #[test]
    fn bulk_alloc_single_element() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let slices = alloc.alloc_bulk([64_u64]).unwrap();
        assert_eq!(slices.len(), 1);
        assert_eq!(slices[0].start(), 0);
        assert_eq!(slices[0].len(), 64);
        assert_eq!(alloc.len().unwrap(), 64);
    }

    // 5. Zero-length entries produce valid empty slices without changing the
    //    file layout (position is preserved relative to non-zero neighbours).
    #[test]
    fn bulk_alloc_zero_len_entries() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let slices = alloc.alloc_bulk([0_u64, 8, 0]).unwrap();
        assert_eq!(slices.len(), 3);
        assert!(slices[0].is_empty());
        assert_eq!(slices[1].start(), 0);
        assert_eq!(slices[1].len(), 8);
        assert!(slices[2].is_empty());
        assert_eq!(alloc.len().unwrap(), 8);
    }

    // 6. All freshly allocated bulk regions are zero-initialised.
    #[test]
    fn bulk_alloc_reads_zeros() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let slices = alloc.alloc_bulk([4_u64, 8, 4]).unwrap();
        for s in &slices {
            assert_eq!(s.read().unwrap(), vec![0u8; s.len() as usize]);
        }
    }

    // 7. Slices from alloc_bulk are non-overlapping and contiguous.
    #[test]
    fn bulk_alloc_non_overlapping_contiguous() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let slices = alloc.alloc_bulk([5_u64, 10, 15, 20]).unwrap();
        for i in 0..slices.len() - 1 {
            assert_eq!(slices[i].end(), slices[i + 1].start());
        }
    }

    // 8. alloc_bulk followed by individual alloc places the next alloc after
    //    the bulk region (i.e. the bulk consumed exactly the right amount).
    #[test]
    fn bulk_alloc_then_individual() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        alloc.alloc_bulk([8_u64, 16]).unwrap();
        let s = alloc.alloc(4).unwrap();
        assert_eq!(s.start(), 24);
    }

    // 9. Lengths that sum to >u64::MAX return InvalidInput without touching
    //    the file.
    #[test]
    fn bulk_alloc_overflow_is_error() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let err = alloc.alloc_bulk([u64::MAX, 1]).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
        assert_eq!(alloc.len().unwrap(), 0);
    }

    // ---- BStackBulkAllocator: dealloc_bulk ---------------------------------

    // 10. Empty slice list is a no-op.
    #[test]
    fn bulk_dealloc_empty() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        alloc.alloc(32).unwrap();
        alloc.dealloc_bulk([]).unwrap();
        assert_eq!(alloc.len().unwrap(), 32);
    }

    // 11. All slices form a contiguous tail → everything reclaimed, stack empty.
    #[test]
    fn bulk_dealloc_all_tail_reclaimed() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let slices = alloc.alloc_bulk([8_u64, 16, 32]).unwrap();
        alloc.dealloc_bulk(slices).unwrap();
        assert_eq!(alloc.len().unwrap(), 0);
    }

    // 12. Only the contiguous tail region is reclaimed; slices not touching
    //     the tail (or separated from it by a gap) are ignored.
    #[test]
    fn bulk_dealloc_non_tail_slice_ignored() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let a = alloc.alloc(8).unwrap(); // 0..8  (not tail after b is allocated)
        let b = alloc.alloc(16).unwrap(); // 8..24 (tail)
        // dealloc_bulk only sees `a`; `b` is alive but not passed in.
        // Since `a` does not reach the tail the call is a no-op.
        alloc.dealloc_bulk([a]).unwrap();
        assert_eq!(alloc.len().unwrap(), 24);
        // Now dealloc b (the tail) normally to clean up.
        alloc.dealloc(b).unwrap();
    }

    // 13. Slices supplied in reverse order produce the same result.
    #[test]
    fn bulk_dealloc_reverse_order_same_result() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let slices = alloc.alloc_bulk([8_u64, 16, 32]).unwrap();
        // Reverse the Vec before passing it; all three still cover the tail.
        let mut rev = slices;
        rev.reverse();
        alloc.dealloc_bulk(rev).unwrap();
        assert_eq!(alloc.len().unwrap(), 0);
    }

    // 14. Gap between supplied slices limits how far back reclamation reaches.
    #[test]
    fn bulk_dealloc_gap_limits_reclamation() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let a = alloc.alloc(8).unwrap(); // 0..8
        let _b = alloc.alloc(8).unwrap(); // 8..16  ← not included in dealloc_bulk
        let c = alloc.alloc(8).unwrap(); // 16..24 (tail)
        // dealloc_bulk([a, c]): c touches the tail, a does not reach c,
        // so only c is reclaimed.
        alloc.dealloc_bulk([a, c]).unwrap();
        assert_eq!(alloc.len().unwrap(), 16);
    }

    // 15. Single tail slice → equivalent to single dealloc.
    #[test]
    fn bulk_dealloc_single_tail() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        alloc.alloc(8).unwrap();
        let tail = alloc.alloc(16).unwrap();
        alloc.dealloc_bulk([tail]).unwrap();
        assert_eq!(alloc.len().unwrap(), 8);
    }

    // 16. Single non-tail slice → no-op.
    #[test]
    fn bulk_dealloc_single_nontail_noop() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let head = alloc.alloc(8).unwrap();
        alloc.alloc(16).unwrap(); // keeps head non-tail
        alloc.dealloc_bulk([head]).unwrap();
        assert_eq!(alloc.len().unwrap(), 24);
    }

    // 17. alloc_bulk then dealloc_bulk round-trip leaves the stack empty.
    #[test]
    fn bulk_roundtrip_empty() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let slices = alloc.alloc_bulk([4_u64, 8, 12, 16]).unwrap();
        alloc.dealloc_bulk(slices).unwrap();
        assert!(alloc.is_empty().unwrap());
    }

    // 18. Partial dealloc_bulk reclaims only the tail suffix; a subsequent
    //     alloc_bulk reuses the freed space.
    #[test]
    fn bulk_dealloc_partial_then_realloc() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let slices = alloc.alloc_bulk([8_u64, 16, 32]).unwrap();
        let (head, tail) = slices.split_at(1);
        // Reclaim only the last two slices (tail suffix).
        alloc.dealloc_bulk(tail).unwrap();
        assert_eq!(alloc.len().unwrap(), 8);
        // head[0] (0..8) is still live; a new bulk alloc goes right after it.
        let new = alloc.alloc_bulk([4_u64, 4]).unwrap();
        assert_eq!(new[0].start(), 8);
        assert_eq!(new[1].start(), 12);
        let _ = head; // keep the borrow alive
    }

    // -------------------------------------------------------------------------
    // std-slice-style ergonomic methods on BStackSlice (ported from master)
    // -------------------------------------------------------------------------

    // ---- read-only (no extra feature) ---------------------------------------

    #[test]
    fn slice_get_in_and_out_of_bounds() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let s = alloc.alloc(4).unwrap();
        assert_eq!(s.get(0).unwrap(), Some(0)); // fresh region is zeroed
        assert_eq!(s.get(3).unwrap(), Some(0));
        assert_eq!(s.get(4).unwrap(), None); // out of bounds
    }

    #[test]
    fn slice_head_and_tail() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let s = alloc.alloc(10).unwrap();
        let h = s.head(3);
        assert_eq!(h.start(), s.start());
        assert_eq!(h.len(), 3);
        // over-long request is clamped
        assert_eq!(s.head(100).len(), 10);

        let t = s.tail(3);
        assert_eq!(t.start(), s.start() + 7);
        assert_eq!(t.len(), 3);
        assert_eq!(s.tail(100).len(), 10);
        assert_eq!(s.tail(100).start(), s.start());
    }

    #[test]
    fn slice_split_at() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let s = alloc.alloc(10).unwrap();
        let (l, r) = s.split_at(3);
        assert_eq!(l.start(), s.start());
        assert_eq!(l.len(), 3);
        assert_eq!(r.start(), s.start() + 3);
        assert_eq!(r.len(), 7);
        // boundary: mid == len
        let (l, r) = s.split_at(10);
        assert_eq!(l.len(), 10);
        assert_eq!(r.len(), 0);
    }

    #[test]
    #[should_panic(expected = "split_at: mid must be <= slice length")]
    fn slice_split_at_out_of_bounds() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let s = alloc.alloc(4).unwrap();
        let _ = s.split_at(5);
    }

    #[test]
    fn slice_split_at_mut() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let mut s = alloc.alloc(10).unwrap();
        let (l, r) = s.split_at_mut(4);
        assert_eq!(l.len(), 4);
        assert_eq!(r.len(), 6);
        assert_eq!(r.start(), s.start() + 4);
        // boundary: mid == 0
        let (l, r) = s.split_at_mut(0);
        assert_eq!(l.len(), 0);
        assert_eq!(r.len(), 10);
    }

    #[test]
    fn slice_contains_zeroed() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let s = alloc.alloc(4).unwrap();
        assert!(s.contains(0).unwrap());
        assert!(!s.contains(1).unwrap());
    }

    #[test]
    fn slice_starts_and_ends_with_zeroed() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let s = alloc.alloc(4).unwrap();
        assert!(s.starts_with(&[0, 0]).unwrap());
        assert!(!s.starts_with(&[1]).unwrap());
        assert!(!s.starts_with(&[0, 0, 0, 0, 0]).unwrap()); // longer than slice
        assert!(s.ends_with(&[0, 0]).unwrap());
        assert!(!s.ends_with(&[1]).unwrap());
        assert!(!s.ends_with(&[0, 0, 0, 0, 0]).unwrap());
    }

    #[test]
    fn slice_find_and_rfind_zeroed() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let s = alloc.alloc(4).unwrap();
        assert_eq!(s.find(0).unwrap(), Some(0));
        assert_eq!(s.find(9).unwrap(), None);
        assert_eq!(s.rfind(0).unwrap(), Some(3));
        assert_eq!(s.rfind(9).unwrap(), None);
    }

    #[test]
    fn slice_position_and_rposition_zeroed() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let s = alloc.alloc(4).unwrap();
        assert_eq!(s.position(|b| b == 0).unwrap(), Some(0));
        assert_eq!(s.position(|b| b == 7).unwrap(), None);
        assert_eq!(s.rposition(|b| b == 0).unwrap(), Some(3));
        assert_eq!(s.rposition(|b| b == 7).unwrap(), None);
    }

    // Stronger read-only coverage over non-trivial written data.
    #[cfg(feature = "set")]
    #[test]
    fn slice_search_over_written_data() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let s = alloc.alloc(6).unwrap();
        s.write(b"abcabc").unwrap();
        assert!(s.contains(b'c').unwrap());
        assert!(!s.contains(b'z').unwrap());
        assert!(s.starts_with(b"abc").unwrap());
        assert!(!s.starts_with(b"abd").unwrap());
        assert!(s.ends_with(b"abc").unwrap());
        assert!(!s.ends_with(b"abd").unwrap());
        assert_eq!(s.find(b'b').unwrap(), Some(1));
        assert_eq!(s.rfind(b'b').unwrap(), Some(4));
        assert_eq!(s.find(b'z').unwrap(), None);
        assert_eq!(s.position(|x| x == b'c').unwrap(), Some(2));
        assert_eq!(s.rposition(|x| x == b'c').unwrap(), Some(5));
    }

    // ---- write (needs `set`) ------------------------------------------------

    #[cfg(feature = "set")]
    #[test]
    fn slice_fill() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let mut s = alloc.alloc(6).unwrap();
        s.fill(0xAB).unwrap();
        assert_eq!(s.read().unwrap(), vec![0xAB; 6]);
        // boundary: empty slice fill is a no-op
        let mut e = alloc.alloc(0).unwrap();
        e.fill(0xFF).unwrap();
        assert_eq!(e.read().unwrap(), Vec::<u8>::new());
    }

    #[cfg(feature = "set")]
    #[test]
    fn slice_fill_with() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let mut s = alloc.alloc(5).unwrap();
        let mut n = 0u8;
        s.fill_with(|| {
            let v = n;
            n += 1;
            v
        })
        .unwrap();
        assert_eq!(s.read().unwrap(), vec![0, 1, 2, 3, 4]);
    }

    #[cfg(feature = "set")]
    #[test]
    fn slice_copy_from_slice() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let mut s = alloc.alloc(4).unwrap();
        s.copy_from_slice(b"WXYZ").unwrap();
        assert_eq!(s.read().unwrap(), b"WXYZ");
    }

    #[cfg(feature = "set")]
    #[test]
    #[should_panic(expected = "copy_from_slice: length mismatch")]
    fn slice_copy_from_slice_length_mismatch() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let mut s = alloc.alloc(4).unwrap();
        s.copy_from_slice(b"TOOLONG").unwrap();
    }

    // ---- atomic compound (needs `set` + `atomic`) ---------------------------

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn slice_copy_from_bstack_slice() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let src = alloc.alloc(4).unwrap();
        src.write(b"DATA").unwrap();
        let mut dst = alloc.alloc(4).unwrap();
        dst.copy_from_bstack_slice(&src).unwrap();
        assert_eq!(dst.read().unwrap(), b"DATA");
    }

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn slice_copy_from_bstack_slice_different_stack_errors() {
        let (alloc1, path1) = mk_alloc();
        let _g1 = Guard(path1);
        let (alloc2, path2) = mk_alloc();
        let _g2 = Guard(path2);
        let src = alloc1.alloc(4).unwrap();
        let mut dst = alloc2.alloc(4).unwrap();
        let err = dst.copy_from_bstack_slice(&src).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
    }

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn slice_copy_within() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let mut s = alloc.alloc(6).unwrap();
        s.write(b"ABCDEF").unwrap();
        // copy [0,2) -> dest 4
        s.copy_within(0..2, 4).unwrap();
        assert_eq!(s.read().unwrap(), b"ABCDAB");
        // boundary: empty range is a no-op
        s.copy_within(2..2, 0).unwrap();
        assert_eq!(s.read().unwrap(), b"ABCDAB");
    }

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn slice_swap() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let mut a = alloc.alloc(3).unwrap();
        let mut b = alloc.alloc(3).unwrap();
        a.write(b"AAA").unwrap();
        b.write(b"BBB").unwrap();
        a.swap(&mut b).unwrap();
        assert_eq!(a.read().unwrap(), b"BBB");
        assert_eq!(b.read().unwrap(), b"AAA");
        // boundary: swapping a slice with itself (same start) is a no-op
        let mut c = a;
        a.swap(&mut c).unwrap();
        assert_eq!(a.read().unwrap(), b"BBB");
    }

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn slice_swap_different_stack_errors() {
        let (alloc1, path1) = mk_alloc();
        let _g1 = Guard(path1);
        let (alloc2, path2) = mk_alloc();
        let _g2 = Guard(path2);
        let mut a = alloc1.alloc(3).unwrap();
        let mut b = alloc2.alloc(3).unwrap();
        let err = a.swap(&mut b).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
    }

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn slice_reverse() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let mut s = alloc.alloc(5).unwrap();
        s.write(b"abcde").unwrap();
        s.reverse().unwrap();
        assert_eq!(s.read().unwrap(), b"edcba");
        // boundary: reversing an empty slice is a no-op
        let mut e = alloc.alloc(0).unwrap();
        e.reverse().unwrap();
    }

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn slice_rotate_left() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let mut s = alloc.alloc(5).unwrap();
        s.write(b"abcde").unwrap();
        s.rotate_left(2).unwrap();
        assert_eq!(s.read().unwrap(), b"cdeab");
        // boundary: mid == len is a full rotation (identity)
        s.rotate_left(5).unwrap();
        assert_eq!(s.read().unwrap(), b"cdeab");
    }

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn slice_rotate_right() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let mut s = alloc.alloc(5).unwrap();
        s.write(b"abcde").unwrap();
        s.rotate_right(2).unwrap();
        assert_eq!(s.read().unwrap(), b"deabc");
        // boundary: k == 0 is the identity
        s.rotate_right(0).unwrap();
        assert_eq!(s.read().unwrap(), b"deabc");
    }

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    #[should_panic(expected = "rotate_left: mid must be <= slice length")]
    fn slice_rotate_left_out_of_bounds() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let mut s = alloc.alloc(4).unwrap();
        let _ = s.rotate_left(5);
    }

    // ---- BStackSlice: cas_on / cas_on_ne / cas_on_masked --------------------

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn slice_cas_on_match_swaps_and_returns_old() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let guard = alloc.alloc(2).unwrap();
        guard.write([1u8, 2]).unwrap();
        let mut target = alloc.alloc(2).unwrap();
        target.write([9u8, 9]).unwrap();
        let old = target.cas_on(&guard, [1u8, 2], [3u8, 4]).unwrap();
        assert_eq!(old, Some(vec![9, 9]));
        assert_eq!(target.read().unwrap(), [3, 4]);
    }

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn slice_cas_on_no_match_leaves_target_untouched() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let guard = alloc.alloc(2).unwrap();
        guard.write([1u8, 2]).unwrap();
        let mut target = alloc.alloc(2).unwrap();
        target.write([9u8, 9]).unwrap();
        let result = target.cas_on(&guard, [0u8, 0], [3u8, 4]).unwrap();
        assert_eq!(result, None);
        assert_eq!(target.read().unwrap(), [9, 9]);
    }

    // The guard may be the target itself — the plain single-region CAS.
    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn slice_cas_on_self_guard_swaps() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let mut target = alloc.alloc(2).unwrap();
        target.write([1u8, 2]).unwrap();
        let guard = target;
        let old = target.cas_on(&guard, [1u8, 2], [3u8, 4]).unwrap();
        assert_eq!(old, Some(vec![1, 2]));
        assert_eq!(target.read().unwrap(), [3, 4]);
    }

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn slice_cas_on_expected_length_mismatch_errors() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let guard = alloc.alloc(2).unwrap();
        let mut target = alloc.alloc(2).unwrap();
        let err = target.cas_on(&guard, [0u8], [3u8, 4]).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
    }

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn slice_cas_on_new_bytes_length_mismatch_errors() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let guard = alloc.alloc(2).unwrap();
        guard.write([1u8, 2]).unwrap();
        let mut target = alloc.alloc(2).unwrap();
        let err = target.cas_on(&guard, [1u8, 2], [3u8]).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
    }

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn slice_cas_on_cross_stack_errors() {
        let (alloc_a, path_a) = mk_alloc();
        let _g_a = Guard(path_a);
        let (alloc_b, path_b) = mk_alloc();
        let _g_b = Guard(path_b);
        let guard = alloc_a.alloc(2).unwrap();
        let mut target = alloc_b.alloc(2).unwrap();
        let err = target.cas_on(&guard, [0u8, 0], [1u8, 1]).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
    }

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn slice_cas_on_ne_no_match_swaps_and_returns_old() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let guard = alloc.alloc(2).unwrap();
        guard.write([1u8, 2]).unwrap();
        let mut target = alloc.alloc(2).unwrap();
        target.write([9u8, 9]).unwrap();
        let old = target.cas_on_ne(&guard, [0u8, 0], [3u8, 4]).unwrap();
        assert_eq!(old, Some(vec![9, 9]));
        assert_eq!(target.read().unwrap(), [3, 4]);
    }

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn slice_cas_on_ne_match_returns_none() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let guard = alloc.alloc(2).unwrap();
        guard.write([1u8, 2]).unwrap();
        let mut target = alloc.alloc(2).unwrap();
        target.write([9u8, 9]).unwrap();
        let result = target.cas_on_ne(&guard, [1u8, 2], [3u8, 4]).unwrap();
        assert_eq!(result, None);
        assert_eq!(target.read().unwrap(), [9, 9]);
    }

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn slice_cas_on_masked_match_swaps_and_returns_old() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let guard = alloc.alloc(2).unwrap();
        guard.write([0xffu8, 0x0f]).unwrap();
        let mut target = alloc.alloc(2).unwrap();
        target.write([9u8, 9]).unwrap();
        // mask = [0xff, 0xf0]: masked guard = [0xff, 0x00] == masked expected
        let old = target
            .cas_on_masked(&guard, [0xffu8, 0xf0], [0xffu8, 0x0f], [3u8, 4])
            .unwrap();
        assert_eq!(old, Some(vec![9, 9]));
        assert_eq!(target.read().unwrap(), [3, 4]);
    }

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn slice_cas_on_masked_no_match_returns_none() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let guard = alloc.alloc(1).unwrap();
        guard.write([0x0fu8]).unwrap();
        let mut target = alloc.alloc(2).unwrap();
        target.write([9u8, 9]).unwrap();
        let result = target
            .cas_on_masked(&guard, [0xffu8], [0xffu8], [3u8, 4])
            .unwrap();
        assert_eq!(result, None);
        assert_eq!(target.read().unwrap(), [9, 9]);
    }

    // ---- BStackSlice: process ----------------------------------------------

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn slice_process_transforms_in_place() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let mut s = alloc.alloc(4).unwrap();
        s.write([1u8, 2, 3, 4]).unwrap();
        s.process(|buf| {
            for b in buf.iter_mut() {
                *b *= 2;
            }
        })
        .unwrap();
        assert_eq!(s.read().unwrap(), [2, 4, 6, 8]);
    }

    // ── Foreign slices ────────────────────────────────────────────────────

    #[test]
    fn dealloc_and_realloc_reject_a_slice_from_another_instance() {
        let (a1, p1) = mk_alloc();
        let _g1 = Guard(p1);
        let (a2, p2) = mk_alloc();
        let _g2 = Guard(p2);

        let s = a1.alloc(64).unwrap();
        assert!(s.is_from(&a1));
        assert!(!s.is_from(&a2));

        let err = a2.dealloc(s).expect_err("a2 must refuse a1's slice");
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
        let err = a2.realloc(s, 128).expect_err("a2 must refuse a1's slice");
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);

        // Both allocators are untouched by the refusals: the slice is `Copy`,
        // so nothing was lost, and each still serves its own handles.
        assert_eq!(a1.len().unwrap(), 64);
        let own = a2.alloc(64).unwrap();
        a2.dealloc(own).unwrap();
        a1.dealloc(s).unwrap();
    }

    #[test]
    fn dealloc_bulk_rejects_a_batch_containing_a_foreign_slice() {
        let (a1, p1) = mk_alloc();
        let _g1 = Guard(p1);
        let (a2, p2) = mk_alloc();
        let _g2 = Guard(p2);

        let own = a2.alloc(32).unwrap();
        let foreign = a1.alloc(32).unwrap();

        // One foreign slice poisons the batch: nothing is freed, including the
        // slice that did belong to `a2`.
        let err = a2
            .dealloc_bulk([own, foreign])
            .expect_err("a2 must refuse a batch holding a1's slice");
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
        assert_eq!(a2.len().unwrap(), 32);

        a2.dealloc(own).unwrap();
        a1.dealloc(foreign).unwrap();
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
            stack.push(hdr).unwrap();
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
        let s_start = s.start();
        let before_len = alloc.len().unwrap();
        let s2 = alloc.realloc(s, 16).unwrap();
        assert_eq!(s2.start(), s_start);
        assert_eq!(s2.len(), 16);
        // A tail shrink keeps the block at its physical size (an oversized block,
        // exactly as a non-tail shrink does) rather than rewriting the header and
        // discarding the tail as separate, non-crash-atomic steps. The stack
        // length is therefore unchanged; the excess is reclaimed on free.
        assert_eq!(alloc.len().unwrap(), before_len);
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
        b.write(vec![0xFFu8; 80]).unwrap();
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
            alff[16..24].copy_from_slice(b"ALFF\x00\x01\x01\x00");
            alff[24..28].copy_from_slice(&1u32.to_le_bytes()); // recovery_needed
            stack.push(alff).unwrap();

            // Block A header: size=80, flags=0 (allocated, but header not yet shrunk)
            let mut a_hdr = [0u8; 16];
            a_hdr[..8].copy_from_slice(&80u64.to_le_bytes());
            stack.push(a_hdr).unwrap();

            // Block A payload (80 bytes): inner footer + second sub-block embedded
            let mut a_pay = [0u8; 80];
            // [32..40): inner footer = R=32
            a_pay[32..40].copy_from_slice(&32u64.to_le_bytes());
            // [40..48): second sub-block size = F=24
            a_pay[40..48].copy_from_slice(&24u64.to_le_bytes());
            // [48..52): is_free = 1
            a_pay[48..52].copy_from_slice(&1u32.to_le_bytes());
            // [52..80): zeros (reserved + second sub-block payload)
            stack.push(a_pay).unwrap();

            // Outer footer: F=24
            stack.push(24u64.to_le_bytes()).unwrap();

            // Sentinel block: header(size=16,flags=0) + payload(16 zeros) + footer(16)
            let mut sent = [0u8; 40];
            sent[..8].copy_from_slice(&16u64.to_le_bytes());
            sent[32..40].copy_from_slice(&16u64.to_le_bytes());
            stack.push(sent).unwrap();
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
        stack.set(24, 1u32.to_le_bytes()).unwrap(); // flags byte → recovery_needed=1
        stack
            .set(FREE_HEAD_OFFSET, 0xDEADBEEFu64.to_le_bytes())
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

    #[test]
    fn recovery_rolls_back_interrupted_tail_grow() {
        // A tail-block-grow `realloc` `extend`s (zero-filling) the payload before
        // rewriting the header/footer to cover it. A crash in that window leaves a
        // valid block followed by an all-zero region with no block header — which
        // the recovery scan used to read as a size-0 block and reject the whole
        // file. Recovery now rolls the extension back by truncation.
        let (alloc, path) = mk_ff("recovery_tailgrow");
        let _g = Guard(path.clone());
        let a = alloc.alloc(32).unwrap();
        a.write([0xA7u8; 32]).unwrap();
        let a_start = a.start();
        let stack = alloc.into_stack();
        let before_len = stack.len().unwrap();

        // Reproduce the stranded state: a zero-filled tail region past the block
        // with no header of its own, plus recovery_needed set.
        stack.extend(64).unwrap();
        stack.set(24, 1u32.to_le_bytes()).unwrap(); // recovery_needed = 1
        drop(stack);

        let stack2 = BStack::open(&path).unwrap();
        let alloc2 = FirstFitBStackAllocator::new(stack2)
            .expect("recovery must roll back the interrupted tail grow, not error");
        // The interrupted extension is rolled back...
        assert_eq!(alloc2.len().unwrap(), before_len);
        // ...and the block's bytes survive.
        assert_eq!(
            alloc2.stack().get(a_start, a_start + 32).unwrap(),
            vec![0xA7u8; 32]
        );
    }

    #[test]
    fn recovery_normalizes_stale_footer() {
        // Every block-resizing operation commits its new size to the header
        // before the matching footer, so a crash between the two writes leaves a
        // correct header and a stale footer. The walk follows headers, so the
        // mismatch slips through undetected yet corrupts a later neighbour's
        // coalesce (which reads this footer). Recovery now normalizes every
        // block's footer to its authoritative header size.
        let (alloc, path) = mk_ff("recovery_footer");
        let _g = Guard(path.clone());
        let a = alloc.alloc(32).unwrap();
        let _b = alloc.alloc(16).unwrap(); // keeps A a non-tail block
        let a_start = a.start();
        let stack = alloc.into_stack();

        // block_start = payload - BLOCK_HEADER_SIZE(16); header size at
        // block_start; footer at block_start + 16 + size. Corrupt the footer to a
        // clearly-bogus, non-8-aligned value (so the partial-split check ignores
        // it), leaving header/footer disagreeing.
        let block_start = a_start - 16;
        let size = u64::from_le_bytes(
            <[u8; 8]>::try_from(stack.get(block_start, block_start + 8).unwrap()).unwrap(),
        );
        let footer_pos = block_start + 16 + size;
        stack.set(footer_pos, 0xDEADu64.to_le_bytes()).unwrap();
        stack.set(24, 1u32.to_le_bytes()).unwrap(); // recovery_needed = 1
        drop(stack);

        let stack2 = BStack::open(&path).unwrap();
        let alloc2 = FirstFitBStackAllocator::new(stack2)
            .expect("recovery must heal the stale footer, not error");
        // Recovery restored the footer to the authoritative header size.
        let footer = u64::from_le_bytes(
            <[u8; 8]>::try_from(alloc2.stack().get(footer_pos, footer_pos + 8).unwrap()).unwrap(),
        );
        assert_eq!(footer, size, "recovery normalizes footer to header size");
        // The allocator remains usable.
        let r = alloc2.alloc(16).unwrap();
        r.write([0x22u8; 16]).unwrap();
        assert_eq!(r.read().unwrap(), vec![0x22u8; 16]);
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

    // -----------------------------------------------------------------------
    // Concurrency (requires the `atomic` feature, which makes the allocator
    // `Sync` via the internal `Mutex` around free-list mutation and tail
    // extension/discard).

    #[cfg(feature = "atomic")]
    #[test]
    fn concurrent_alloc_dealloc_data_integrity() {
        // N threads each repeatedly alloc → write a thread-tagged pattern →
        // read it back → dealloc.  If the Mutex did not serialise the
        // free-list mutation correctly, two threads could be handed the same
        // block (or overlapping blocks), and the read-back would observe a
        // different thread's pattern.
        //
        // Regression: the final assert_eq checks that cascade_discard_free_tail
        // is called after every add_to_free_list in the non-tail dealloc path.
        // Without the fix, the last dealloc going through the non-tail path
        // could coalesce free neighbours all the way to the physical tail and
        // leave a free block in the free list permanently.
        use std::sync::Arc;
        use std::thread;

        let (alloc, path) = mk_ff("concurrent_data");
        let _g = Guard(path);
        let alloc = Arc::new(alloc);

        const THREADS: u64 = 8;
        const ITERS: u64 = 200;

        let handles: Vec<_> = (0..THREADS)
            .map(|tid| {
                let alloc = Arc::clone(&alloc);
                thread::spawn(move || {
                    let sizes = [16u64, 24, 40, 64, 96, 128];
                    for i in 0..ITERS {
                        let len = sizes[(i as usize) % sizes.len()];
                        let slice = alloc.alloc(len).unwrap();
                        let pat = (tid as u8).wrapping_add(i as u8);
                        let buf = vec![pat; len as usize];
                        slice.write(&buf).unwrap();
                        let got = slice.read().unwrap();
                        assert_eq!(
                            got, buf,
                            "thread {tid} iter {i}: read-back mismatch \
                             (overlapping allocation?)"
                        );
                        alloc.dealloc(slice).unwrap();
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }
    }

    #[cfg(feature = "atomic")]
    #[test]
    fn concurrent_realloc_grow_shrink_data_integrity() {
        // Hammer realloc — tail-grow, tail-shrink, in-place merge, and
        // copy-and-move paths all share the same Mutex.  Each thread tracks
        // its own slice across reallocs and verifies the prefix it wrote
        // earlier is preserved across every grow.
        use std::sync::Arc;
        use std::thread;

        let (alloc, path) = mk_ff("concurrent_realloc");
        let _g = Guard(path.clone());
        let alloc = Arc::new(alloc);

        const THREADS: u64 = 6;
        const ITERS: u64 = 120;

        let handles: Vec<_> = (0..THREADS)
            .map(|tid| {
                let alloc = Arc::clone(&alloc);
                thread::spawn(move || {
                    let pat = (tid as u8).wrapping_add(0x40);
                    let mut slice = alloc.alloc(16).unwrap();
                    slice.write(vec![pat; 16]).unwrap();
                    let mut prev_len = 16u64;

                    // Sizes oscillate up and down to exercise both branches.
                    let sizes = [32u64, 64, 24, 96, 16, 128, 48];
                    for i in 0..ITERS {
                        let new_len = sizes[(i as usize) % sizes.len()];
                        slice = alloc.realloc(slice, new_len).unwrap();

                        // The bytes the *current* thread previously wrote
                        // (up to min(prev_len, new_len)) must still be intact.
                        let keep = prev_len.min(new_len) as usize;
                        let got = slice.read().unwrap();
                        for (j, &b) in got.iter().take(keep).enumerate() {
                            assert_eq!(
                                b, pat,
                                "thread {tid} iter {i}: byte {j} clobbered \
                                 by another thread's realloc"
                            );
                        }

                        // Re-stamp the full new length so the next iteration
                        // can re-verify against `pat`.
                        slice.write(vec![pat; new_len as usize]).unwrap();
                        prev_len = new_len;
                    }

                    alloc.dealloc(slice).unwrap();
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(alloc.len().unwrap(), ALFF_HDR_OFFSET);
    }

    #[cfg(feature = "atomic")]
    #[test]
    fn concurrent_tail_thrash() {
        // Hammer the tail paths — alloc → tail-grow → tail-shrink → dealloc —
        // from many threads on one allocator.  Whether each individual op
        // actually lands on the tail depends on the interleaving (another
        // thread's allocation can sit above this one), so this exercises the
        // `recovery_needed` CAS, the tail-grow extend, the in-bucket realloc
        // shrink, and the dealloc/cascade paths under contention.  With
        // cascade_discard_free_tail called after every add_to_free_list, the
        // arena is fully reclaimed even when dealloc takes the non-tail path.
        use std::sync::Arc;
        use std::thread;

        let (alloc, path) = mk_ff("tail_thrash");
        let _g = Guard(path.clone());
        let alloc = Arc::new(alloc);

        const THREADS: u64 = 6;
        const ITERS: u64 = 80;

        let handles: Vec<_> = (0..THREADS)
            .map(|_| {
                let alloc = Arc::clone(&alloc);
                thread::spawn(move || {
                    for _ in 0..ITERS {
                        let s = alloc.alloc(32).unwrap();
                        // Tail-grow path (extend underlying stack).
                        let s = alloc.realloc(s, 64).unwrap();
                        // Tail-shrink path (in-bucket — same block).
                        let s = alloc.realloc(s, 16).unwrap();
                        alloc.dealloc(s).unwrap();
                    }
                })
            })
            .collect();

        // With cascade_discard_free_tail called after every add_to_free_list,
        // a shrunken block whose dealloc goes through the non-tail path
        // (because the user-visible len is smaller than the physical block
        // size) is still reclaimed as soon as it coalesces to the tail.
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(alloc.len().unwrap(), ALFF_HDR_OFFSET);
    }

    #[cfg(feature = "atomic")]
    #[test]
    fn dealloc_non_tail_cascade_reclaims_arena() {
        // Targeted regression for missing cascade_discard_free_tail after
        // add_to_free_list in dealloc's non-tail path.  With concurrent
        // threads the last dealloc going through the non-tail path can
        // coalesce free neighbours to the physical tail, leaving a free block
        // in the free list permanently unless cascade is called.
        use std::sync::Arc;
        use std::thread;

        let (alloc, path) = mk_ff("nontail_cascade");
        let _g = Guard(path);
        let alloc = Arc::new(alloc);

        const THREADS: u64 = 4;
        const ITERS: u64 = 100;

        let handles: Vec<_> = (0..THREADS)
            .map(|_| {
                let alloc = Arc::clone(&alloc);
                thread::spawn(move || {
                    let sizes = [16u64, 32, 64, 128];
                    for i in 0..ITERS {
                        let len = sizes[(i as usize) % sizes.len()];
                        let s = alloc.alloc(len).unwrap();
                        alloc.dealloc(s).unwrap();
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(alloc.len().unwrap(), ALFF_HDR_OFFSET);
    }

    #[cfg(feature = "atomic")]
    #[test]
    fn realloc_copy_move_cascade_reclaims_arena() {
        // Targeted regression for missing cascade_discard_free_tail after
        // add_to_free_list in realloc's copy-and-move paths.  realloc copies
        // data to a new block then frees the old one via add_to_free_list;
        // the freed old block can coalesce to the tail if not immediately
        // cascade-discarded.
        use std::sync::Arc;
        use std::thread;

        let (alloc, path) = mk_ff("realloc_cascade");
        let _g = Guard(path);
        let alloc = Arc::new(alloc);

        const THREADS: u64 = 4;
        const ITERS: u64 = 60;

        let handles: Vec<_> = (0..THREADS)
            .map(|_| {
                let alloc = Arc::clone(&alloc);
                thread::spawn(move || {
                    for _ in 0..ITERS {
                        // Grow far beyond the original block to force the
                        // copy-and-move path: the 16-byte block can't be
                        // extended in place to 128 bytes when other threads
                        // hold adjacent blocks.
                        let s = alloc.alloc(16).unwrap();
                        let s = alloc.realloc(s, 128).unwrap();
                        alloc.dealloc(s).unwrap();
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(alloc.len().unwrap(), ALFF_HDR_OFFSET);
    }

    #[cfg(feature = "atomic")]
    #[test]
    fn recovery_needed_already_set_rejects_mutation() {
        // Manually set the on-disk `recovery_needed` flag to 1 (simulating a
        // prior crash mid-mutation) and verify the next free-list-touching
        // operation surfaces the CAS failure as an error instead of silently
        // proceeding on top of possibly-inconsistent state.
        use crate::BStack;

        let (alloc, path) = mk_ff("recovery_cas");
        let _g = Guard(path.clone());

        // Build a populated free list: alloc two blocks, free the first so
        // the next alloc must walk and unlink — which is what calls
        // `set_recovery_needed`.
        let a = alloc.alloc(64).unwrap();
        let _b = alloc.alloc(64).unwrap();
        alloc.dealloc(a).unwrap();

        // Manually poke the recovery_needed flag.  Layout: payload offset 24
        // = OFFSET_SIZE(16) + magic(8); the flag occupies the next 4 bytes.
        alloc
            .stack()
            .set(24u64, 1u32.to_le_bytes().as_slice())
            .unwrap();

        // Any operation that goes through `set_recovery_needed` should now
        // fail with the recovery-needed CAS error.  `alloc(64)` reuses the
        // freed slot, so it walks the free list and calls `unlink_block`.
        let err = alloc.alloc(64).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("recovery"),
            "expected recovery_needed error, got: {msg}"
        );

        // Reopening must now run recovery (because the flag is set on disk)
        // and succeed without error, leaving the stack usable again.
        drop(alloc);
        let reopened = FirstFitBStackAllocator::new(BStack::open(&path).unwrap()).unwrap();
        // The recovered allocator should still be able to satisfy an alloc.
        let _ = reopened.alloc(16).unwrap();
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

    // -----------------------------------------------------------------------
    // process_gen  (require set + atomic)

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn process_gen_reads_then_writes() {
        use crate::BStackGenOp;
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"helloworld").unwrap();
        let mut buf = [0u8; 5];
        let mut step = 0usize;
        s.process_gen(|| {
            // SAFETY: `buf` outlives this whole `process_gen` call.
            let r = match step {
                0 => Some(BStackGenOp::Read {
                    offset: 0,
                    buf: unsafe { core::mem::transmute::<&mut [u8], &mut [u8]>(&mut buf[..]) },
                }),
                1 => Some(BStackGenOp::Write {
                    offset: 5,
                    data: unsafe { core::mem::transmute::<&[u8], &[u8]>(&buf[..]) },
                }),
                _ => None,
            };
            step += 1;
            r
        })
        .unwrap();
        assert_eq!(&buf, b"hello");
        assert_eq!(s.peek(0).unwrap(), b"hellohello");
    }

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn process_gen_dependent_reads_inform_next_offset() {
        use crate::BStackGenOp;
        // Layout: [pointer: u64 LE][node "A "][node "B "]
        let (s, p) = mk_stack();
        let _g = Guard(p);
        let mut payload = Vec::new();
        payload.extend_from_slice(&8u64.to_le_bytes());
        payload.extend_from_slice(b"A ");
        payload.extend_from_slice(b"B ");
        s.push(&payload).unwrap();

        let mut ptr_buf = [0u8; 8];
        let mut node_buf = [0u8; 2];
        let mut step = 0usize;
        s.process_gen(|| {
            // SAFETY: `ptr_buf` and `node_buf` outlive this whole `process_gen` call.
            let r = match step {
                0 => Some(BStackGenOp::Read {
                    offset: 0,
                    buf: unsafe { core::mem::transmute::<&mut [u8], &mut [u8]>(&mut ptr_buf[..]) },
                }),
                1 => {
                    // The previous read has already filled `ptr_buf` by the
                    // time we're called again.
                    let target = u64::from_le_bytes(ptr_buf);
                    Some(BStackGenOp::Read {
                        offset: target,
                        buf: unsafe {
                            core::mem::transmute::<&mut [u8], &mut [u8]>(&mut node_buf[..])
                        },
                    })
                }
                _ => None,
            };
            step += 1;
            r
        })
        .unwrap();
        assert_eq!(u64::from_le_bytes(ptr_buf), 8);
        assert_eq!(&node_buf, b"A ");
    }

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn process_gen_immediate_none_is_noop() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"hello").unwrap();
        s.process_gen(|| None).unwrap();
        assert_eq!(s.len().unwrap(), 5);
        assert_eq!(s.peek(0).unwrap(), b"hello");
    }

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn process_gen_write_ends_sequence() {
        use crate::BStackGenOp;
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"helloworld").unwrap();
        let mut calls = 0usize;
        s.process_gen(|| {
            calls += 1;
            match calls {
                1 => Some(BStackGenOp::Write {
                    offset: 0,
                    data: b"HELLO",
                }),
                _ => Some(BStackGenOp::Write {
                    offset: 5,
                    data: b"WORLD",
                }),
            }
        })
        .unwrap();
        assert_eq!(calls, 1);
        assert_eq!(s.peek(0).unwrap(), b"HELLOworld");
    }

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn process_gen_swap_exchanges_two_regions_and_ends_sequence() {
        use crate::BStackGenOp;
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"helloworld").unwrap();
        let mut calls = 0usize;
        s.process_gen(|| {
            calls += 1;
            match calls {
                1 => Some(BStackGenOp::Swap {
                    a_offset: 0,
                    b_offset: 5,
                    len: 5,
                }),
                _ => Some(BStackGenOp::Write {
                    offset: 0,
                    data: b"NOPE!",
                }),
            }
        })
        .unwrap();
        assert_eq!(calls, 1, "Swap must end the sequence, like Write");
        assert_eq!(s.peek(0).unwrap(), b"worldhello");
    }

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn process_gen_swap_target_informed_by_prior_read() {
        use crate::BStackGenOp;
        // Layout: [pointer: u64 LE][block A: 8 bytes][block B: 8 bytes]
        // The pointer names which block to splice block A with — read it
        // first, then swap based on what it says, all under one lock. This
        // is the capability `cross_exchange` lacks: its two offsets are
        // fixed at call time, so they cannot depend on data read mid-call.
        let (s, p) = mk_stack();
        let _g = Guard(p);
        let mut payload = Vec::new();
        payload.extend_from_slice(&16u64.to_le_bytes()); // names block B
        payload.extend_from_slice(b"AAAAAAAA");
        payload.extend_from_slice(b"BBBBBBBB");
        s.push(&payload).unwrap();

        let mut ptr_buf = [0u8; 8];
        let mut step = 0usize;
        s.process_gen(|| {
            let r = match step {
                0 => Some(BStackGenOp::Read {
                    offset: 0,
                    // SAFETY: `ptr_buf` outlives this whole `process_gen` call.
                    buf: unsafe { core::mem::transmute::<&mut [u8], &mut [u8]>(&mut ptr_buf[..]) },
                }),
                1 => {
                    let target = u64::from_le_bytes(ptr_buf);
                    Some(BStackGenOp::Swap {
                        a_offset: 8,
                        b_offset: target,
                        len: 8,
                    })
                }
                _ => None,
            };
            step += 1;
            r
        })
        .unwrap();
        assert_eq!(s.peek(8).unwrap(), b"BBBBBBBBAAAAAAAA");
    }

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn process_gen_swap_overlapping_regions_returns_error() {
        use crate::BStackGenOp;
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"helloworld").unwrap();
        let err = s
            .process_gen(|| {
                Some(BStackGenOp::Swap {
                    a_offset: 0,
                    b_offset: 3,
                    len: 5,
                })
            })
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidInput);
        assert_eq!(s.peek(0).unwrap(), b"helloworld");
    }

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn process_gen_swap_in_locked_region_returns_error() {
        use crate::BStackGenOp;
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"helloworld").unwrap();
        s.lock_up_to(5).unwrap();
        let err = s
            .process_gen(|| {
                Some(BStackGenOp::Swap {
                    a_offset: 0,
                    b_offset: 5,
                    len: 5,
                })
            })
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidInput);
        assert_eq!(s.peek(0).unwrap(), b"helloworld");
    }

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn process_gen_does_not_change_file_size() {
        use crate::BStackGenOp;
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"helloworld").unwrap();
        s.process_gen(|| {
            Some(BStackGenOp::Write {
                offset: 0,
                data: b"HELLO",
            })
        })
        .unwrap();
        assert_eq!(s.len().unwrap(), 10);
    }

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn process_gen_read_out_of_bounds_returns_error() {
        use crate::BStackGenOp;
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"hi").unwrap();
        let mut buf = [0u8; 10];
        let mut called = false;
        let err = s
            .process_gen(|| {
                if called {
                    return None;
                }
                called = true;
                // SAFETY: `buf` outlives this whole `process_gen` call.
                Some(BStackGenOp::Read {
                    offset: 0,
                    buf: unsafe { core::mem::transmute::<&mut [u8], &mut [u8]>(&mut buf[..]) },
                })
            })
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidInput);
        assert_eq!(s.peek(0).unwrap(), b"hi");
    }

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn process_gen_write_out_of_bounds_returns_error() {
        use crate::BStackGenOp;
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"hello").unwrap();
        let err = s
            .process_gen(|| {
                Some(BStackGenOp::Write {
                    offset: 2,
                    data: b"abcdefgh",
                })
            })
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidInput);
        assert_eq!(s.peek(0).unwrap(), b"hello");
    }

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn process_gen_write_in_locked_region_returns_error() {
        use crate::BStackGenOp;
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"helloworld").unwrap();
        s.lock_up_to(5).unwrap();
        let err = s
            .process_gen(|| {
                Some(BStackGenOp::Write {
                    offset: 0,
                    data: b"HELLO",
                })
            })
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidInput);
        assert_eq!(s.peek(0).unwrap(), b"helloworld");
    }

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn process_gen_read_in_locked_region_succeeds() {
        use crate::BStackGenOp;
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"helloworld").unwrap();
        s.lock_up_to(5).unwrap();
        let mut buf = [0u8; 5];
        let mut called = false;
        s.process_gen(|| {
            if called {
                return None;
            }
            called = true;
            // SAFETY: `buf` outlives this whole `process_gen` call.
            Some(BStackGenOp::Read {
                offset: 0,
                buf: unsafe { core::mem::transmute::<&mut [u8], &mut [u8]>(&mut buf[..]) },
            })
        })
        .unwrap();
        assert_eq!(&buf, b"hello");
    }

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn process_gen_push_appends_and_ends_sequence() {
        use crate::BStackGenOp;
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"hello").unwrap();
        let mut calls = 0usize;
        s.process_gen(|| {
            calls += 1;
            match calls {
                1 => Some(BStackGenOp::Push { data: b"world" }),
                _ => Some(BStackGenOp::Push { data: b"NOPE!" }),
            }
        })
        .unwrap();
        assert_eq!(calls, 1, "Push must end the sequence, like Write");
        assert_eq!(s.len().unwrap(), 10);
        assert_eq!(s.peek(0).unwrap(), b"helloworld");
    }

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn process_gen_push_empty_data_is_noop_and_ends_sequence() {
        use crate::BStackGenOp;
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"hello").unwrap();
        s.process_gen(|| Some(BStackGenOp::Push { data: b"" }))
            .unwrap();
        assert_eq!(s.len().unwrap(), 5);
        assert_eq!(s.peek(0).unwrap(), b"hello");
    }

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn process_gen_pop_removes_and_ends_sequence() {
        use crate::BStackGenOp;
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"helloworld").unwrap();
        let mut buf = [0u8; 5];
        let mut calls = 0usize;
        s.process_gen(|| {
            calls += 1;
            match calls {
                // SAFETY: `buf` outlives this whole `process_gen` call.
                1 => Some(BStackGenOp::Pop {
                    buf: unsafe { core::mem::transmute::<&mut [u8], &mut [u8]>(&mut buf[..]) },
                }),
                _ => Some(BStackGenOp::Write {
                    offset: 0,
                    data: b"NOPE!",
                }),
            }
        })
        .unwrap();
        assert_eq!(calls, 1, "Pop must end the sequence, like Write");
        assert_eq!(&buf, b"world");
        assert_eq!(s.len().unwrap(), 5);
        assert_eq!(s.peek(0).unwrap(), b"hello");
    }

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn process_gen_pop_zero_is_noop_and_ends_sequence() {
        use crate::BStackGenOp;
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"hello").unwrap();
        s.process_gen(|| Some(BStackGenOp::Pop { buf: &mut [] }))
            .unwrap();
        assert_eq!(s.len().unwrap(), 5);
        assert_eq!(s.peek(0).unwrap(), b"hello");
    }

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn process_gen_pop_exceeds_payload_returns_error() {
        use crate::BStackGenOp;
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"hi").unwrap();
        let mut buf = [0u8; 10];
        let err = s
            .process_gen(|| {
                // SAFETY: `buf` outlives this whole `process_gen` call.
                Some(BStackGenOp::Pop {
                    buf: unsafe { core::mem::transmute::<&mut [u8], &mut [u8]>(&mut buf[..]) },
                })
            })
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidInput);
        assert_eq!(s.len().unwrap(), 2);
        assert_eq!(s.peek(0).unwrap(), b"hi");
    }

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn process_gen_pop_below_locked_returns_error() {
        use crate::BStackGenOp;
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"helloworld").unwrap();
        s.lock_up_to(8).unwrap();
        let mut buf = [0u8; 5];
        let err = s
            .process_gen(|| {
                // SAFETY: `buf` outlives this whole `process_gen` call.
                Some(BStackGenOp::Pop {
                    buf: unsafe { core::mem::transmute::<&mut [u8], &mut [u8]>(&mut buf[..]) },
                })
            })
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidInput);
        assert_eq!(s.len().unwrap(), 10);
        assert_eq!(s.peek(0).unwrap(), b"helloworld");
    }

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn process_gen_discard_removes_and_ends_sequence() {
        use crate::BStackGenOp;
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"helloworld").unwrap();
        let mut calls = 0usize;
        s.process_gen(|| {
            calls += 1;
            match calls {
                1 => Some(BStackGenOp::Discard { len: 5 }),
                _ => Some(BStackGenOp::Write {
                    offset: 0,
                    data: b"NOPE!",
                }),
            }
        })
        .unwrap();
        assert_eq!(calls, 1, "Discard must end the sequence, like Pop");
        assert_eq!(s.len().unwrap(), 5);
        assert_eq!(s.peek(0).unwrap(), b"hello");
    }

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn process_gen_discard_zero_is_noop_and_ends_sequence() {
        use crate::BStackGenOp;
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"hello").unwrap();
        s.process_gen(|| Some(BStackGenOp::Discard { len: 0 }))
            .unwrap();
        assert_eq!(s.len().unwrap(), 5);
        assert_eq!(s.peek(0).unwrap(), b"hello");
    }

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn process_gen_discard_exceeds_payload_returns_error() {
        use crate::BStackGenOp;
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"hi").unwrap();
        let err = s
            .process_gen(|| Some(BStackGenOp::Discard { len: 10 }))
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidInput);
        assert_eq!(s.len().unwrap(), 2);
        assert_eq!(s.peek(0).unwrap(), b"hi");
    }

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn process_gen_discard_below_locked_returns_error() {
        use crate::BStackGenOp;
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"helloworld").unwrap();
        s.lock_up_to(8).unwrap();
        let err = s
            .process_gen(|| Some(BStackGenOp::Discard { len: 5 }))
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidInput);
        assert_eq!(s.len().unwrap(), 10);
        assert_eq!(s.peek(0).unwrap(), b"helloworld");
    }

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn process_gen_len_informs_discard_size() {
        use crate::BStackGenOp;
        // Discard a trailing region whose length is only known once `Len` has
        // reported the current size — the buffer-free analogue of the Pop case.
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"keepDROP").unwrap();
        let mut size = 0u64;
        let mut calls = 0usize;
        s.process_gen(|| {
            calls += 1;
            match calls {
                // SAFETY: `size` outlives this whole `process_gen` call.
                1 => Some(BStackGenOp::Len {
                    out: unsafe { core::mem::transmute::<&mut u64, &mut u64>(&mut size) },
                }),
                _ => Some(BStackGenOp::Discard { len: size - 4 }),
            }
        })
        .unwrap();
        assert_eq!(size, 8);
        assert_eq!(s.len().unwrap(), 4);
        assert_eq!(s.peek(0).unwrap(), b"keep");
    }

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn process_gen_len_reports_current_size_and_continues() {
        use crate::BStackGenOp;
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"helloworld").unwrap();
        let mut size = 0u64;
        let mut calls = 0usize;
        s.process_gen(|| {
            calls += 1;
            match calls {
                // SAFETY: `size` outlives this whole `process_gen` call.
                1 => Some(BStackGenOp::Len {
                    out: unsafe { core::mem::transmute::<&mut u64, &mut u64>(&mut size) },
                }),
                _ => None,
            }
        })
        .unwrap();
        assert_eq!(calls, 2, "Len must not end the sequence, unlike Write");
        assert_eq!(size, 10);
        assert_eq!(s.len().unwrap(), 10);
        assert_eq!(s.peek(0).unwrap(), b"helloworld");
    }

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn process_gen_len_informs_pop_size() {
        use crate::BStackGenOp;
        // Layout: [count: u64 LE]["world"] — pop the trailing "world" whose
        // length is only known once `Len` has reported the current size.
        let (s, p) = mk_stack();
        let _g = Guard(p);
        let mut payload = Vec::new();
        payload.extend_from_slice(&8u64.to_le_bytes());
        payload.extend_from_slice(b"world");
        s.push(&payload).unwrap();

        let mut size = 0u64;
        let mut buf = Vec::new();
        let mut step = 0usize;
        s.process_gen(|| {
            let r = match step {
                // SAFETY: `size` outlives this whole `process_gen` call.
                0 => Some(BStackGenOp::Len {
                    out: unsafe { core::mem::transmute::<&mut u64, &mut u64>(&mut size) },
                }),
                1 => {
                    let n = (size - 8) as usize;
                    buf = vec![0u8; n];
                    // SAFETY: `buf` outlives this whole `process_gen` call.
                    Some(BStackGenOp::Pop {
                        buf: unsafe { core::mem::transmute::<&mut [u8], &mut [u8]>(&mut buf[..]) },
                    })
                }
                _ => None,
            };
            step += 1;
            r
        })
        .unwrap();
        assert_eq!(size, 13);
        assert_eq!(buf, b"world");
        assert_eq!(s.len().unwrap(), 8);
    }

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn process_gen_persists_across_reopen() {
        use crate::BStackGenOp;
        let (s, p) = mk_stack();
        let _g = Guard(p.clone());
        s.push(b"helloworld").unwrap();
        s.process_gen(|| {
            Some(BStackGenOp::Write {
                offset: 5,
                data: b"WORLD",
            })
        })
        .unwrap();
        drop(s);
        let s2 = BStack::open(&p).unwrap();
        assert_eq!(s2.peek(0).unwrap(), b"helloWORLD");
    }

    // ---- process_gen concurrency / atomicity -------------------------------
    //
    // `process_gen` exists to replace the two-lock `get_batched_gen` + `cas`
    // pattern by holding the write lock across the *entire* read-modify-write
    // sequence, closing the ABA window between the dependent reads and the
    // terminating write. The tests below exercise that guarantee directly,
    // rather than just exercising the single-threaded read/write mechanics
    // covered above.

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn process_gen_concurrent_increments_have_no_lost_updates() {
        use crate::BStackGenOp;
        use std::sync::Arc;
        use std::thread;

        // N threads each run M independent read-increment-write sequences on
        // a shared u64 counter. The final total is only correct if every
        // sequence was serialised end to end — if `process_gen` ever released
        // the write lock between its read and its write, two threads could
        // read the same value and one increment would be lost.
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(0u64.to_le_bytes()).unwrap();
        let s = Arc::new(s);

        const THREADS: usize = 8;
        const ITERS: usize = 100;

        let handles: Vec<_> = (0..THREADS)
            .map(|_| {
                let s = Arc::clone(&s);
                thread::spawn(move || {
                    for _ in 0..ITERS {
                        let mut buf = [0u8; 8];
                        let mut step = 0usize;
                        s.process_gen(|| {
                            let r = match step {
                                0 => Some(BStackGenOp::Read {
                                    offset: 0,
                                    // SAFETY: `buf` outlives this whole `process_gen` call.
                                    buf: unsafe {
                                        core::mem::transmute::<&mut [u8], &mut [u8]>(&mut buf[..])
                                    },
                                }),
                                1 => {
                                    let v = u64::from_le_bytes(buf) + 1;
                                    buf = v.to_le_bytes();
                                    Some(BStackGenOp::Write {
                                        offset: 0,
                                        // SAFETY: `buf` outlives this whole `process_gen` call.
                                        data: unsafe {
                                            core::mem::transmute::<&[u8], &[u8]>(&buf[..])
                                        },
                                    })
                                }
                                _ => None,
                            };
                            step += 1;
                            r
                        })
                        .unwrap();
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        let total = s.peek(0).unwrap();
        assert_eq!(
            u64::from_le_bytes(total[..8].try_into().unwrap()),
            (THREADS * ITERS) as u64,
            "lost update: a concurrent read-increment-write was not serialised end to end"
        );
    }

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn process_gen_concurrent_free_list_pops_each_node_exactly_once() {
        use crate::BStackGenOp;
        use std::collections::HashSet;
        use std::sync::Arc;
        use std::thread;

        // Build a singly-linked free list:
        //   [head: u64 LE][node@8.next: u64 LE][node@16.next: u64 LE]...
        // `head` points at the first free node; each node stores only the
        // offset of the next one; the last node's `next` is SENTINEL.
        //
        // Each thread "pops" exactly one node via a dependent two-read,
        // one-write `process_gen` sequence: read `head`, read that node's
        // `next`, then write `next` back into `head`. This is precisely the
        // free-list pattern that motivated replacing `get_batched_gen` + `cas`
        // with `process_gen` — if the write lock were ever released between
        // the dependent reads and the terminating write, two threads could
        // read the same `head`, "pop" the same node, and corrupt the list
        // (the classic ABA window this primitive exists to close).
        const NODES: u64 = 16;
        const NODE_SIZE: u64 = 8;
        const SENTINEL: u64 = u64::MAX;
        const FIRST_NODE: u64 = NODE_SIZE;

        let mut payload = Vec::new();
        payload.extend_from_slice(&FIRST_NODE.to_le_bytes());
        for i in 0..NODES {
            let next = if i + 1 < NODES {
                FIRST_NODE + (i + 1) * NODE_SIZE
            } else {
                SENTINEL
            };
            payload.extend_from_slice(&next.to_le_bytes());
        }

        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(&payload).unwrap();
        let s = Arc::new(s);

        let handles: Vec<_> = (0..NODES)
            .map(|_| {
                let s = Arc::clone(&s);
                thread::spawn(move || {
                    let mut head_buf = [0u8; 8];
                    let mut next_buf = [0u8; 8];
                    let mut step = 0usize;
                    let mut popped: Option<u64> = None;
                    s.process_gen(|| {
                        let r = match step {
                            0 => Some(BStackGenOp::Read {
                                offset: 0,
                                // SAFETY: `head_buf` outlives this whole `process_gen` call.
                                buf: unsafe {
                                    core::mem::transmute::<&mut [u8], &mut [u8]>(&mut head_buf[..])
                                },
                            }),
                            1 => {
                                let head = u64::from_le_bytes(head_buf);
                                if head == SENTINEL {
                                    // List already empty — abort, nothing to pop.
                                    None
                                } else {
                                    popped = Some(head);
                                    Some(BStackGenOp::Read {
                                        offset: head,
                                        // SAFETY: `next_buf` outlives this whole `process_gen` call.
                                        buf: unsafe {
                                            core::mem::transmute::<&mut [u8], &mut [u8]>(
                                                &mut next_buf[..],
                                            )
                                        },
                                    })
                                }
                            }
                            2 => Some(BStackGenOp::Write {
                                offset: 0,
                                // SAFETY: `next_buf` outlives this whole `process_gen` call.
                                data: unsafe {
                                    core::mem::transmute::<&[u8], &[u8]>(&next_buf[..])
                                },
                            }),
                            _ => None,
                        };
                        step += 1;
                        r
                    })
                    .unwrap();
                    popped
                })
            })
            .collect();

        let popped: Vec<u64> = handles
            .into_iter()
            .map(|h| {
                h.join()
                    .unwrap()
                    .expect("every thread should pop a distinct node — the list has exactly enough")
            })
            .collect();

        let mut seen = HashSet::new();
        for &off in &popped {
            assert!(
                seen.insert(off),
                "node at offset {off} was popped more than once"
            );
        }
        let expected: HashSet<u64> = (0..NODES).map(|i| FIRST_NODE + i * NODE_SIZE).collect();
        assert_eq!(seen, expected, "not every node was popped exactly once");

        let head = s.peek(0).unwrap();
        assert_eq!(
            u64::from_le_bytes(head[..8].try_into().unwrap()),
            SENTINEL,
            "free list should be empty once every node has been popped"
        );
    }

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn process_gen_excludes_concurrent_writers_until_sequence_completes() {
        use crate::BStackGenOp;
        use std::sync::{Arc, Mutex, mpsc};
        use std::thread;

        // `process_gen` must hold the write lock from its first read through
        // its terminating write — that is the entire point of replacing the
        // two-lock `get_batched_gen` + `cas` pattern with a single
        // held-lock primitive.
        //
        // Thread A parks mid-sequence — after its `Read`, still inside the
        // closure and so still holding the write lock — until told to
        // continue. Thread B then attempts `push`, which needs that very
        // same lock and so can only complete once A's whole sequence (read
        // *and* write) has run. Therefore "A finished its write" must be
        // logged before "B's push completed" no matter how the threads are
        // scheduled — `RwLock::write` is exclusive. If `process_gen` ever
        // released the lock between steps, B could race ahead and flip that
        // order (or interleave with / corrupt A's write).
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"helloworld").unwrap();
        let s = Arc::new(s);

        let events: Arc<Mutex<Vec<&'static str>>> = Arc::new(Mutex::new(Vec::new()));
        let (tx_ready, rx_ready) = mpsc::channel::<()>();
        let (tx_continue, rx_continue) = mpsc::channel::<()>();
        let (tx_b_started, rx_b_started) = mpsc::channel::<()>();

        let a = {
            let s = Arc::clone(&s);
            let events = Arc::clone(&events);
            thread::spawn(move || {
                let mut buf = [0u8; 5];
                let mut step = 0usize;
                s.process_gen(|| {
                    let r = match step {
                        0 => {
                            // `process_gen` has acquired the write lock by
                            // the time it calls us for the first time.
                            tx_ready.send(()).unwrap();
                            // SAFETY: `buf` outlives this whole `process_gen` call.
                            Some(BStackGenOp::Read {
                                offset: 0,
                                buf: unsafe {
                                    core::mem::transmute::<&mut [u8], &mut [u8]>(&mut buf[..])
                                },
                            })
                        }
                        1 => {
                            // Still inside the closure — and so still holding
                            // the write lock — block until B is poised to
                            // race us for it.
                            rx_continue.recv().unwrap();
                            events.lock().unwrap().push("A_write");
                            Some(BStackGenOp::Write {
                                offset: 5,
                                data: b"WORLD",
                            })
                        }
                        _ => None,
                    };
                    step += 1;
                    r
                })
                .unwrap();
            })
        };

        // Block until A is inside `process_gen`, certainly holding the lock.
        rx_ready.recv().unwrap();

        let b = {
            let s = Arc::clone(&s);
            let events = Arc::clone(&events);
            thread::spawn(move || {
                tx_b_started.send(()).unwrap();
                // Needs the very same write lock `process_gen` is holding;
                // can only complete once A's whole sequence does.
                s.push(b"!").unwrap();
                events.lock().unwrap().push("B_push");
            })
        };

        // Let B reach the lock before releasing A, so that any early-release
        // bug in `process_gen` would have B actively racing for it right then.
        rx_b_started.recv().unwrap();
        tx_continue.send(()).unwrap();

        a.join().unwrap();
        b.join().unwrap();

        let recorded = events.lock().unwrap();
        assert_eq!(
            &recorded[..],
            &["A_write", "B_push"][..],
            "B's push observed/affected state while A's process_gen was still mid-sequence"
        );
        drop(recorded);
        assert_eq!(s.peek(0).unwrap(), b"helloWORLD!");
    }

    // ---- lock_up_to / locked_len / open_locked_up_to -----------------------

    #[test]
    fn locked_len_is_zero_by_default() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        assert_eq!(s.locked_len(), 0);
    }

    #[test]
    fn lock_up_to_sets_boundary() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"helloworld").unwrap();
        s.lock_up_to(5).unwrap();
        assert_eq!(s.locked_len(), 5);
    }

    #[test]
    fn lock_up_to_monotonic_can_grow() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"0123456789").unwrap();
        s.lock_up_to(3).unwrap();
        s.lock_up_to(7).unwrap();
        assert_eq!(s.locked_len(), 7);
    }

    #[test]
    fn lock_up_to_monotonic_cannot_shrink() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"helloworld").unwrap();
        s.lock_up_to(5).unwrap();
        let err = s.lock_up_to(3).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidInput);
        assert_eq!(s.locked_len(), 5); // unchanged
    }

    #[test]
    fn lock_up_to_n_equal_locked_is_idempotent() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"helloworld").unwrap();
        s.lock_up_to(5).unwrap();
        s.lock_up_to(5).unwrap(); // same value — no error
        assert_eq!(s.locked_len(), 5);
    }

    #[test]
    fn lock_up_to_n_exceeds_len_returns_error() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"hello").unwrap();
        let err = s.lock_up_to(10).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidInput);
        assert_eq!(s.locked_len(), 0); // unchanged
    }

    #[test]
    fn lock_up_to_zero_is_noop() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"hello").unwrap();
        s.lock_up_to(0).unwrap();
        assert_eq!(s.locked_len(), 0);
    }

    #[test]
    fn locked_region_resets_on_reopen() {
        let (s, p) = mk_stack();
        let _g = Guard(p.clone());
        s.push(b"helloworld").unwrap();
        s.lock_up_to(5).unwrap();
        assert_eq!(s.locked_len(), 5);
        drop(s);

        // On reopen the locked partition returns to 0.
        let s2 = BStack::open(&p).unwrap();
        assert_eq!(s2.locked_len(), 0);
    }

    #[test]
    fn reads_in_locked_region_succeed() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"helloworld").unwrap();
        s.lock_up_to(5).unwrap();

        // get
        assert_eq!(s.get(0, 5).unwrap(), b"hello");
        assert_eq!(s.get(1, 4).unwrap(), b"ell");

        // get_into
        let mut buf = [0u8; 5];
        s.get_into(0, &mut buf).unwrap();
        assert_eq!(&buf, b"hello");

        // peek_into
        let mut buf2 = [0u8; 3];
        s.peek_into(2, &mut buf2).unwrap();
        assert_eq!(&buf2, b"llo");

        // peek (crosses the locked boundary — still works via rwlock path)
        assert_eq!(s.peek(0).unwrap(), b"helloworld");
    }

    #[test]
    fn pop_below_locked_returns_error() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"helloworld").unwrap();
        s.lock_up_to(5).unwrap();
        // Popping 6 bytes would leave len=4 < locked=5.
        let err = s.pop(6).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidInput);
        assert_eq!(s.len().unwrap(), 10); // unchanged
    }

    #[test]
    fn pop_exactly_to_locked_boundary_is_allowed() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"helloworld").unwrap();
        s.lock_up_to(5).unwrap();
        // Popping 5 bytes leaves len=5 == locked=5 — allowed.
        let bytes = s.pop(5).unwrap();
        assert_eq!(bytes, b"world");
        assert_eq!(s.len().unwrap(), 5);
    }

    #[test]
    fn pop_into_below_locked_returns_error() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"helloworld").unwrap();
        s.lock_up_to(5).unwrap();
        let mut buf = [0u8; 6];
        let err = s.pop_into(&mut buf).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidInput);
    }

    #[test]
    fn discard_below_locked_returns_error() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"helloworld").unwrap();
        s.lock_up_to(5).unwrap();
        let err = s.discard(6).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidInput);
        assert_eq!(s.len().unwrap(), 10);
    }

    #[test]
    fn push_after_lock_appends_past_locked_region() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"hello").unwrap();
        s.lock_up_to(5).unwrap();
        let off = s.push(b"world").unwrap();
        assert_eq!(off, 5);
        assert_eq!(s.len().unwrap(), 10);
        assert_eq!(s.get(0, 5).unwrap(), b"hello");
        assert_eq!(s.get(5, 10).unwrap(), b"world");
    }

    #[cfg(feature = "set")]
    #[test]
    fn set_in_locked_region_returns_error() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"helloworld").unwrap();
        s.lock_up_to(5).unwrap();
        let err = s.set(0, b"HELLO").unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidInput);
        // Data unchanged
        assert_eq!(s.get(0, 5).unwrap(), b"hello");
    }

    #[cfg(feature = "set")]
    #[test]
    fn zero_in_locked_region_returns_error() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"helloworld").unwrap();
        s.lock_up_to(5).unwrap();
        let err = s.zero(0, 3).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidInput);
        assert_eq!(s.get(0, 3).unwrap(), b"hel");
    }

    #[cfg(feature = "set")]
    #[test]
    fn set_past_locked_region_succeeds() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"helloworld").unwrap();
        s.lock_up_to(5).unwrap();
        s.set(5, b"WORLD").unwrap();
        assert_eq!(s.get(5, 10).unwrap(), b"WORLD");
        assert_eq!(s.get(0, 5).unwrap(), b"hello"); // locked bytes unchanged
    }

    #[test]
    fn open_locked_up_to_opens_and_locks() {
        let (s, p) = mk_stack();
        let _g = Guard(p.clone());
        s.push(b"helloworld").unwrap();
        drop(s);

        let s2 = BStack::open_locked_up_to(&p, 5).unwrap();
        assert_eq!(s2.locked_len(), 5);
        assert_eq!(s2.len().unwrap(), 10);
        assert_eq!(s2.get(0, 5).unwrap(), b"hello");
        // Write into locked region is rejected.
        assert_eq!(s2.pop(6).unwrap_err().kind(), ErrorKind::InvalidInput);
    }

    #[test]
    fn open_locked_up_to_n_exceeds_len_returns_error() {
        let (s, p) = mk_stack();
        let _g = Guard(p.clone());
        s.push(b"hello").unwrap();
        drop(s);

        let err = BStack::open_locked_up_to(&p, 10).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidInput);
    }

    #[cfg(feature = "atomic")]
    #[test]
    fn atrunc_into_locked_region_returns_error() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"helloworld").unwrap();
        s.lock_up_to(5).unwrap();
        // Removing 6 bytes would start the new tail at byte 4, inside locked.
        let err = s.atrunc(6, b"X").unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidInput);
        assert_eq!(s.len().unwrap(), 10);
    }

    #[cfg(feature = "atomic")]
    #[test]
    fn splice_into_locked_region_returns_error() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"helloworld").unwrap();
        s.lock_up_to(5).unwrap();
        let err = s.splice(6, b"Y").unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidInput);
        assert_eq!(s.len().unwrap(), 10);
    }

    #[cfg(feature = "atomic")]
    #[test]
    fn try_discard_below_locked_returns_error() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"helloworld").unwrap();
        s.lock_up_to(5).unwrap();
        let err = s.try_discard(10, 6).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidInput);
        assert_eq!(s.len().unwrap(), 10);
    }

    // -----------------------------------------------------------------------
    // try_extend_zeros  (require atomic)

    #[cfg(feature = "atomic")]
    #[test]
    fn try_extend_zeros_matching_size_appends_zeros() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"hello").unwrap();
        let ok = s.try_extend_zeros(5, 3).unwrap();
        assert!(ok);
        assert_eq!(s.len().unwrap(), 8);
        assert_eq!(s.peek(0).unwrap(), b"hello\x00\x00\x00");
    }

    #[cfg(feature = "atomic")]
    #[test]
    fn try_extend_zeros_mismatching_size_returns_false() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"hello").unwrap();
        let ok = s.try_extend_zeros(3, 3).unwrap();
        assert!(!ok);
        assert_eq!(s.len().unwrap(), 5);
        assert_eq!(s.peek(0).unwrap(), b"hello");
    }

    #[cfg(feature = "atomic")]
    #[test]
    fn try_extend_zeros_n_zero_is_noop_returns_true() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"hello").unwrap();
        let ok = s.try_extend_zeros(5, 0).unwrap();
        assert!(ok);
        assert_eq!(s.len().unwrap(), 5);
    }

    #[cfg(feature = "atomic")]
    #[test]
    fn try_extend_zeros_content_is_zeros() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"ab").unwrap();
        s.try_extend_zeros(2, 4).unwrap();
        assert_eq!(s.get(2, 6).unwrap(), b"\x00\x00\x00\x00");
    }

    #[cfg(feature = "atomic")]
    #[test]
    fn try_extend_zeros_persists_across_reopen() {
        let (s, p) = mk_stack();
        let _g = Guard(p.clone());
        s.push(b"hi").unwrap();
        s.try_extend_zeros(2, 2).unwrap();
        drop(s);
        let s2 = BStack::open(&p).unwrap();
        assert_eq!(s2.len().unwrap(), 4);
        assert_eq!(s2.peek(0).unwrap(), b"hi\x00\x00");
    }

    // -----------------------------------------------------------------------
    // get_batched  (require atomic)

    #[cfg(feature = "atomic")]
    #[test]
    fn get_batched_reads_multiple_ranges() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"helloworld").unwrap();
        let results = s.get_batched([0..5, 5..10]).unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0], b"hello");
        assert_eq!(results[1], b"world");
    }

    #[cfg(feature = "atomic")]
    #[test]
    fn get_batched_empty_input_returns_empty_vec() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"hello").unwrap();
        let results = s
            .get_batched(std::iter::empty::<std::ops::Range<u64>>())
            .unwrap();
        assert!(results.is_empty());
    }

    #[cfg(feature = "atomic")]
    #[test]
    #[allow(clippy::single_range_in_vec_init)]
    fn get_batched_zero_length_range_returns_empty_buf() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"hello").unwrap();
        let results = s.get_batched([3..3]).unwrap();
        assert_eq!(results.len(), 1);
        assert!(results[0].is_empty());
    }

    #[cfg(feature = "atomic")]
    #[test]
    #[allow(clippy::single_range_in_vec_init)]
    fn get_batched_out_of_bounds_returns_error() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"hello").unwrap();
        let err = s.get_batched([0..10]).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidInput);
    }

    #[cfg(feature = "atomic")]
    #[test]
    #[allow(clippy::single_range_in_vec_init)]
    #[allow(clippy::reversed_empty_ranges)]
    fn get_batched_end_less_than_start_returns_error() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"helloworld").unwrap();
        let err = s.get_batched([5..3]).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidInput);
    }

    #[cfg(feature = "atomic")]
    #[test]
    fn get_batched_order_matches_input() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"abcde").unwrap();
        let results = s.get_batched([4..5, 0..2, 2..4]).unwrap();
        assert_eq!(results[0], b"e");
        assert_eq!(results[1], b"ab");
        assert_eq!(results[2], b"cd");
    }

    // -----------------------------------------------------------------------
    // get_batched_into  (require atomic)

    #[cfg(feature = "atomic")]
    #[test]
    fn get_batched_into_fills_buffers() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"helloworld").unwrap();
        let mut a = [0u8; 5];
        let mut b = [0u8; 5];
        s.get_batched_into([(0, a.as_mut_slice()), (5, b.as_mut_slice())])
            .unwrap();
        assert_eq!(&a, b"hello");
        assert_eq!(&b, b"world");
    }

    #[cfg(feature = "atomic")]
    #[test]
    fn get_batched_into_empty_input_is_noop() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"hello").unwrap();
        s.get_batched_into(std::iter::empty::<(u64, &mut [u8])>())
            .unwrap();
        assert_eq!(s.len().unwrap(), 5);
    }

    #[cfg(feature = "atomic")]
    #[test]
    fn get_batched_into_out_of_bounds_returns_error() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"hi").unwrap();
        let mut buf = [0u8; 10];
        let err = s.get_batched_into([(0, buf.as_mut_slice())]).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidInput);
    }

    #[cfg(feature = "atomic")]
    #[test]
    fn get_batched_into_matches_get() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"abcdefgh").unwrap();
        let mut buf1 = [0u8; 3];
        let mut buf2 = [0u8; 4];
        s.get_batched_into([(0, buf1.as_mut_slice()), (4, buf2.as_mut_slice())])
            .unwrap();
        assert_eq!(&buf1, &s.get(0, 3).unwrap()[..]);
        assert_eq!(&buf2, &s.get(4, 8).unwrap()[..]);
    }

    // -----------------------------------------------------------------------
    // get_batched_gen  (require atomic)

    #[cfg(feature = "atomic")]
    #[test]
    fn get_batched_gen_reads_chain() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"helloworld").unwrap();
        let mut bufs: [Vec<u8>; 2] = [vec![0u8; 5], vec![0u8; 5]];
        let ptr0 = bufs[0].as_mut_ptr();
        let ptr1 = bufs[1].as_mut_ptr();
        let mut step = 0usize;
        s.get_batched_gen(|| {
            let r = match step {
                0 => Some((0u64, unsafe { std::slice::from_raw_parts_mut(ptr0, 5) })),
                1 => Some((5u64, unsafe { std::slice::from_raw_parts_mut(ptr1, 5) })),
                _ => None,
            };
            step += 1;
            r
        })
        .unwrap();
        assert_eq!(&bufs[0], b"hello");
        assert_eq!(&bufs[1], b"world");
    }

    #[cfg(feature = "atomic")]
    #[test]
    fn get_batched_gen_immediate_none_is_noop() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"hello").unwrap();
        s.get_batched_gen(|| None).unwrap();
        assert_eq!(s.len().unwrap(), 5);
    }

    #[cfg(feature = "atomic")]
    #[test]
    fn get_batched_gen_out_of_bounds_returns_error() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"hi").unwrap();
        let mut buf = [0u8; 10];
        let ptr = buf.as_mut_ptr();
        let mut called = false;
        let err = s
            .get_batched_gen(|| {
                if called {
                    return None;
                }
                called = true;
                Some((0u64, unsafe { std::slice::from_raw_parts_mut(ptr, 10) }))
            })
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidInput);
    }

    // -----------------------------------------------------------------------
    // cross_exchange  (require set + atomic)

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn cross_exchange_swaps_two_regions() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"helloworld").unwrap();
        s.cross_exchange(0, 5, 5).unwrap();
        assert_eq!(s.peek(0).unwrap(), b"worldhello");
    }

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn cross_exchange_n_zero_is_noop() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"helloworld").unwrap();
        s.cross_exchange(0, 5, 0).unwrap();
        assert_eq!(s.peek(0).unwrap(), b"helloworld");
    }

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn cross_exchange_overlapping_regions_returns_error() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"helloworld").unwrap();
        let err = s.cross_exchange(0, 3, 5).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidInput);
        assert_eq!(s.peek(0).unwrap(), b"helloworld");
    }

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn cross_exchange_out_of_bounds_returns_error() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"hello").unwrap();
        let err = s.cross_exchange(0, 3, 5).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidInput);
    }

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn cross_exchange_locked_region_returns_error() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"helloworld").unwrap();
        s.lock_up_to(5).unwrap();
        let err = s.cross_exchange(0, 5, 5).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidInput);
        assert_eq!(s.peek(0).unwrap(), b"helloworld");
    }

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn cross_exchange_persists_across_reopen() {
        let (s, p) = mk_stack();
        let _g = Guard(p.clone());
        s.push(b"abXY").unwrap();
        s.cross_exchange(0, 2, 2).unwrap();
        drop(s);
        let s2 = BStack::open(&p).unwrap();
        assert_eq!(s2.peek(0).unwrap(), b"XYab");
    }

    // -----------------------------------------------------------------------
    // copy  (require set + atomic)

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn copy_copies_bytes() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"helloworld").unwrap();
        s.copy(0, 5, 5).unwrap();
        assert_eq!(s.peek(0).unwrap(), b"hellohello");
    }

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn copy_overlapping_source_to_dest_correct() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"abcde").unwrap();
        // Copy [0,3) → [1,4): source read before write, so result is "aabcde"[0..5] = "aabcd"
        s.copy(0, 1, 3).unwrap();
        assert_eq!(s.peek(0).unwrap(), b"aabce");
    }

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn copy_n_zero_is_noop() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"hello").unwrap();
        s.copy(0, 4, 0).unwrap();
        assert_eq!(s.peek(0).unwrap(), b"hello");
    }

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn copy_out_of_bounds_returns_error() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"hello").unwrap();
        let err = s.copy(0, 0, 10).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidInput);
    }

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn copy_destination_in_locked_region_returns_error() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"helloworld").unwrap();
        s.lock_up_to(5).unwrap();
        let err = s.copy(5, 0, 5).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidInput);
        assert_eq!(s.peek(0).unwrap(), b"helloworld");
    }

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn copy_persists_across_reopen() {
        let (s, p) = mk_stack();
        let _g = Guard(p.clone());
        s.push(b"abcd").unwrap();
        s.copy(0, 2, 2).unwrap();
        drop(s);
        let s2 = BStack::open(&p).unwrap();
        assert_eq!(s2.peek(0).unwrap(), b"abab");
    }

    // -----------------------------------------------------------------------
    // eq_crds  (require set + atomic)

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn eq_crds_match_swaps_b_returns_old() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"aabbcc").unwrap(); // A=[0,2)=aa, B=[2,4)=bb
        let old = s.eq_crds(0, b"aa", 2, b"XX").unwrap();
        assert_eq!(old, Some(b"bb".to_vec()));
        assert_eq!(s.get(2, 4).unwrap(), b"XX");
    }

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn eq_crds_no_match_returns_none() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"aabbcc").unwrap();
        let result = s.eq_crds(0, b"zz", 2, b"XX").unwrap();
        assert_eq!(result, None);
        assert_eq!(s.get(2, 4).unwrap(), b"bb"); // unchanged
    }

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn eq_crds_empty_a_always_matches() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"hello").unwrap();
        let old = s.eq_crds(0, b"", 0, b"HH").unwrap();
        assert_eq!(old, Some(b"he".to_vec()));
        assert_eq!(s.get(0, 2).unwrap(), b"HH");
    }

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn eq_crds_empty_b_buf_returns_some_empty_vec() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"hello").unwrap();
        let old = s.eq_crds(0, b"hello", 0, b"").unwrap();
        assert_eq!(old, Some(Vec::new()));
        assert_eq!(s.peek(0).unwrap(), b"hello"); // unchanged
    }

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn eq_crds_b_in_locked_region_returns_error() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"helloworld").unwrap();
        s.lock_up_to(5).unwrap();
        let err = s.eq_crds(5, b"world", 0, b"HELLO").unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidInput);
    }

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn eq_crds_out_of_bounds_returns_error() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"hi").unwrap();
        let err = s.eq_crds(0, b"hello", 0, b"world").unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidInput);
    }

    // -----------------------------------------------------------------------
    // ne_crds  (require set + atomic)

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn ne_crds_no_match_swaps_b_returns_old() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"aabbcc").unwrap();
        let old = s.ne_crds(0, b"zz", 2, b"XX").unwrap();
        assert_eq!(old, Some(b"bb".to_vec()));
        assert_eq!(s.get(2, 4).unwrap(), b"XX");
    }

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn ne_crds_match_returns_none() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"aabbcc").unwrap();
        let result = s.ne_crds(0, b"aa", 2, b"XX").unwrap();
        assert_eq!(result, None);
        assert_eq!(s.get(2, 4).unwrap(), b"bb"); // unchanged
    }

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn ne_crds_empty_a_trivially_equal_returns_none() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"hello").unwrap();
        let result = s.ne_crds(0, b"", 0, b"XX").unwrap();
        assert_eq!(result, None);
    }

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn ne_crds_b_in_locked_region_returns_error() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"helloworld").unwrap();
        s.lock_up_to(5).unwrap();
        let err = s.ne_crds(5, b"XXXXX", 0, b"HELLO").unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidInput);
    }

    // -----------------------------------------------------------------------
    // masked_eq_crds  (require set + atomic)

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn masked_eq_crds_match_swaps_b_returns_old() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        // A = [0xFF, 0x0F], expected = [0xFF, 0x0F], mask = [0xFF, 0xF0]
        // masked A = [0xFF, 0x00], masked expected = [0xFF, 0x00] → equal
        s.push(b"\xff\x0f--").unwrap();
        let old = s
            .masked_eq_crds(0, b"\xff\xf0", b"\xff\x0f", 2, b"ZZ")
            .unwrap();
        assert_eq!(old, Some(b"--".to_vec()));
        assert_eq!(s.get(2, 4).unwrap(), b"ZZ");
    }

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn masked_eq_crds_no_match_returns_none() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        // A = [0x0F], expected = [0xFF], mask = [0xFF] → 0x0F != 0xFF
        s.push(b"\x0f--").unwrap();
        let result = s.masked_eq_crds(0, b"\xff", b"\xff", 1, b"ZZ").unwrap();
        assert_eq!(result, None);
        assert_eq!(s.get(1, 3).unwrap(), b"--"); // unchanged
    }

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn masked_eq_crds_mask_len_mismatch_returns_error() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"hello").unwrap();
        let err = s
            .masked_eq_crds(0, b"\xff\xff", b"\xff", 0, b"")
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidInput);
    }

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn masked_eq_crds_partial_mask_ignores_masked_out_bits() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        // A = 0xAB, expected = 0xCD, mask = 0x00 → all bits masked out → always matches
        s.push(b"\xab--").unwrap();
        let old = s.masked_eq_crds(0, b"\x00", b"\xcd", 1, b"ZZ").unwrap();
        assert_eq!(old, Some(b"--".to_vec()));
    }

    // -----------------------------------------------------------------------
    // masked_ne_crds  (require set + atomic)

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn masked_ne_crds_no_match_swaps_b_returns_old() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        // A = [0x0F], expected = [0xFF], mask = [0xFF] → 0x0F != 0xFF → swap
        s.push(b"\x0f--").unwrap();
        let old = s.masked_ne_crds(0, b"\xff", b"\xff", 1, b"ZZ").unwrap();
        assert_eq!(old, Some(b"--".to_vec()));
        assert_eq!(s.get(1, 3).unwrap(), b"ZZ");
    }

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn masked_ne_crds_match_returns_none() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        // A = [0xFF, 0x0F], expected = [0xFF, 0x0F], mask = [0xFF, 0xF0]
        // masked equal → no swap
        s.push(b"\xff\x0f--").unwrap();
        let result = s
            .masked_ne_crds(0, b"\xff\xf0", b"\xff\x0f", 2, b"ZZ")
            .unwrap();
        assert_eq!(result, None);
        assert_eq!(s.get(2, 4).unwrap(), b"--"); // unchanged
    }

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn masked_ne_crds_mask_len_mismatch_returns_error() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"hello").unwrap();
        let err = s
            .masked_ne_crds(0, b"\xff\xff", b"\xff", 0, b"")
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidInput);
    }

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn masked_ne_crds_all_bits_masked_out_always_equal_returns_none() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        // mask = 0x00 → all bits masked, always equal → no swap
        s.push(b"\xab--").unwrap();
        let result = s.masked_ne_crds(0, b"\x00", b"\xcd", 1, b"ZZ").unwrap();
        assert_eq!(result, None);
    }
}

#[cfg(test)]
mod cache_tests {
    use crate::BStack;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn mk_cached() -> (BStack, std::path::PathBuf) {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let id = COUNTER.fetch_add(1, Ordering::Relaxed);
        let pid = std::process::id();
        let path = std::env::temp_dir().join(format!("bstack_cache_test_{pid}_{id}.bin"));
        (BStack::open_cached(&path).unwrap(), path)
    }

    fn mk_uncached() -> (BStack, std::path::PathBuf) {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let id = COUNTER.fetch_add(1, Ordering::Relaxed);
        let pid = std::process::id();
        let path = std::env::temp_dir().join(format!("bstack_cache_test_uncached_{pid}_{id}.bin"));
        (BStack::open(&path).unwrap(), path)
    }

    struct Guard(std::path::PathBuf);
    impl Drop for Guard {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }

    #[test]
    fn cache_initially_empty() {
        let (s, p) = mk_cached();
        let _g = Guard(p);
        // No lock_up_to yet — cache Vec should be empty.
        assert_eq!(s.cache.lock().unwrap().len(), 0);
    }

    #[test]
    fn lock_up_to_populates_cache() {
        let (s, p) = mk_cached();
        let _g = Guard(p);
        s.push(b"hello").unwrap();
        s.lock_up_to(5).unwrap();
        let cache = s.cache.lock().unwrap();
        assert_eq!(cache.len(), 5);
        assert_eq!(&cache[..], b"hello");
    }

    #[test]
    fn get_reads_from_cache() {
        let (s, p) = mk_cached();
        let _g = Guard(p);
        s.push(b"abcdefgh").unwrap();
        s.lock_up_to(8).unwrap();
        assert_eq!(s.get(0, 8).unwrap(), b"abcdefgh");
        assert_eq!(s.get(2, 5).unwrap(), b"cde");
    }

    #[test]
    fn get_into_reads_from_cache() {
        let (s, p) = mk_cached();
        let _g = Guard(p);
        s.push(b"abcdefgh").unwrap();
        s.lock_up_to(8).unwrap();
        let mut buf = [0u8; 4];
        s.get_into(1, &mut buf).unwrap();
        assert_eq!(&buf, b"bcde");
    }

    #[test]
    fn peek_into_reads_from_cache() {
        let (s, p) = mk_cached();
        let _g = Guard(p);
        s.push(b"abcdefgh").unwrap();
        s.lock_up_to(8).unwrap();
        let mut buf = [0u8; 3];
        s.peek_into(5, &mut buf).unwrap();
        assert_eq!(&buf, b"fgh");
    }

    #[test]
    fn cache_matches_uncached_get() {
        // Ensure cached reads return identical bytes to pread-based reads.
        let data: Vec<u8> = (0u8..=255).collect();

        let (cached, cp) = mk_cached();
        let _gc = Guard(cp);
        cached.push(&data).unwrap();
        cached.lock_up_to(data.len() as u64).unwrap();

        let (uncached, up) = mk_uncached();
        let _gu = Guard(up);
        uncached.push(&data).unwrap();
        uncached.lock_up_to(data.len() as u64).unwrap();

        assert_eq!(
            cached.get(0, data.len() as u64).unwrap(),
            uncached.get(0, data.len() as u64).unwrap(),
        );
        assert_eq!(cached.get(10, 200).unwrap(), uncached.get(10, 200).unwrap(),);
    }

    #[test]
    fn sequential_lock_up_to_reallocating_growth() {
        // Two lock_up_to calls where the second grows the cache capacity.
        let (s, p) = mk_cached();
        let _g = Guard(p);
        // Push 16 bytes, lock to 8, then extend to 16.
        // next_power_of_two(8) == 8, so locking up to 16 requires reallocation.
        s.push(b"abcdefghijklmnop").unwrap(); // 16 bytes
        s.lock_up_to(8).unwrap(); // capacity = 8
        assert_eq!(s.get(0, 8).unwrap(), b"abcdefgh");
        s.lock_up_to(16).unwrap(); // capacity grows from 8 to 16, so this reallocates
        assert_eq!(s.get(0, 16).unwrap(), b"abcdefghijklmnop");
    }

    #[test]
    fn sequential_lock_up_to_exceeding_capacity_reallocates() {
        // Lock to 4 (capacity = next_power_of_two(4) = 4), then extend to 6.
        // 6 > capacity 4, so the second call reallocates the cache buffer.
        let (s, p) = mk_cached();
        let _g = Guard(p);
        s.push(b"abcdef").unwrap();
        s.lock_up_to(4).unwrap();
        assert_eq!(s.get(0, 4).unwrap(), b"abcd");
        s.lock_up_to(6).unwrap(); // 6 > capacity(4) — reallocates
        assert_eq!(s.get(0, 6).unwrap(), b"abcdef");
    }

    #[test]
    fn repeated_lock_up_to_same_length_is_no_op() {
        // Repeating lock_up_to with the current locked length should be a no-op:
        // the cached prefix remains unchanged and the same bytes stay reachable.
        let (s, p) = mk_cached();
        let _g = Guard(p);
        s.push(b"abcdefgh").unwrap();
        s.lock_up_to(4).unwrap(); // cap = next_power_of_two(4) = 4
        s.lock_up_to(4).unwrap();
        assert_eq!(s.get(0, 4).unwrap(), b"abcd");
    }

    #[test]
    fn non_reallocating_in_place_extend() {
        // lock_up_to(5) allocates capacity = next_power_of_two(5) = 8.
        // lock_up_to(7): nl=7 <= capacity=8, so the cache Vec is extended in-place
        // without reallocation (the `else` branch in lock_up_to).
        let (s, p) = mk_cached();
        let _g = Guard(p);
        s.push(b"abcdefghij").unwrap();
        s.lock_up_to(5).unwrap();
        assert_eq!(s.cache.lock().unwrap().capacity(), 8);
        assert_eq!(s.get(0, 5).unwrap(), b"abcde");
        s.lock_up_to(7).unwrap();
        assert_eq!(s.cache.lock().unwrap().capacity(), 8); // no realloc
        assert_eq!(s.get(0, 7).unwrap(), b"abcdefg");
    }

    #[test]
    fn open_locked_up_to_cached_convenience() {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let id = COUNTER.fetch_add(1, Ordering::Relaxed);
        let pid = std::process::id();
        let path = std::env::temp_dir().join(format!("bstack_cache_conv_{pid}_{id}.bin"));
        let _g = Guard(path.clone());

        // Prepare file with a regular (non-cached) stack first.
        {
            let s = BStack::open(&path).unwrap();
            s.push(b"hello world").unwrap();
        }

        // Re-open with cache, locking 11 bytes at once.
        let s = BStack::open_locked_up_to_cached(&path, 11).unwrap();
        assert_eq!(s.get(0, 11).unwrap(), b"hello world");
    }

    #[test]
    fn uncached_stack_behaviour_unchanged() {
        // Regression: a non-cached BStack must still work identically.
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let id = COUNTER.fetch_add(1, Ordering::Relaxed);
        let pid = std::process::id();
        let path = std::env::temp_dir().join(format!("bstack_nocache_{pid}_{id}.bin"));
        let _g = Guard(path.clone());

        let s = BStack::open(&path).unwrap();
        s.push(b"regression").unwrap();
        s.lock_up_to(10).unwrap();
        assert_eq!(s.get(0, 10).unwrap(), b"regression");
        assert!(!s.cache_enabled);
        assert_eq!(s.cache.lock().unwrap().len(), 0);
    }
}
