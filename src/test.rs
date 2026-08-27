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

    #[test]
    #[cfg(any(feature = "set", feature = "atomic"))]
    fn is_atomic_write_block_confinement() {
        use crate::{io_core::ATOMIC_BLOCK, is_atomic_write};
        let b = ATOMIC_BLOCK;
        // `boundary` is the logical offset whose physical position is exactly the
        // start of the second block (`HEADER_SIZE + boundary == ATOMIC_BLOCK`).
        let boundary = b - HEADER_SIZE;

        // An empty write touches no bytes and never tears.
        assert!(is_atomic_write(0, 0));
        assert!(is_atomic_write(u64::MAX, 0));

        // Filling the remainder of the first block is atomic; one more byte
        // spills into the next block.
        assert!(is_atomic_write(0, boundary));
        assert!(!is_atomic_write(0, boundary + 1));

        // A single byte on either side of a block boundary is atomic; two bytes
        // straddling it are not.
        assert!(is_atomic_write(boundary - 1, 1));
        assert!(is_atomic_write(boundary, 1));
        assert!(!is_atomic_write(boundary - 1, 2));

        // A block-aligned write of exactly one block is atomic; one byte over is
        // not.
        assert!(is_atomic_write(boundary, b));
        assert!(!is_atomic_write(boundary, b + 1));

        // An offset that overflows `u64` once the header is added cannot be
        // confined to a block.
        assert!(!is_atomic_write(u64::MAX, 1));
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
            "new file should be exactly the header size (32 bytes)"
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

        // Write a full-size header with wrong magic.
        let mut bad: Vec<u8> = b"WRONGHDR".to_vec();
        bad.resize(HEADER_SIZE as usize, 0);
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
    fn migrate_upgrades_legacy_file() {
        let path = {
            use std::sync::atomic::{AtomicU64, Ordering};
            static C: AtomicU64 = AtomicU64::new(0);
            let id = C.fetch_add(1, Ordering::Relaxed);
            std::env::temp_dir().join(format!("bstack_migrate_{}.bin", id))
        };
        let _g = Guard(path.clone());

        // Craft a legacy 0.1.x file: 16-byte header (0.1.15 magic + clen) + payload.
        let payload = b"legacy payload contents!";
        let mut legacy: Vec<u8> = b"BSTK\x00\x01\x0f\x00".to_vec();
        legacy.extend_from_slice(&(payload.len() as u64).to_le_bytes());
        legacy.extend_from_slice(payload);
        std::fs::write(&path, &legacy).unwrap();

        // A current-version open rejects it (wrong major.minor).
        assert!(BStack::open(&path).is_err());

        // Migrate, then open succeeds and the payload survives unchanged.
        BStack::migrate(&path).unwrap();
        let s = BStack::open(&path).unwrap();
        assert_eq!(s.len().unwrap(), payload.len() as u64);
        assert_eq!(s.peek(0).unwrap(), payload);
        drop(s);

        // The file now has a 32-byte header + payload, current magic, and the
        // sibling scratch file is gone.
        let raw = std::fs::read(&path).unwrap();
        assert_eq!(raw.len() as u64, HEADER_SIZE + payload.len() as u64);
        assert_eq!(&raw[0..8], &MAGIC, "migrated magic");
        let mut sibling = path.clone().into_os_string();
        sibling.push(".migrating");
        assert!(
            !std::path::Path::new(&sibling).exists(),
            "sibling left behind"
        );

        // Migrating an already-current file is rejected (not a legacy magic).
        assert!(BStack::migrate(&path).is_err());
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

    // ---- write-in-progress (wip) journal ------------------------------------

    // Build a raw 32-byte header (current magic) with the given fields.
    fn wip_header(clen: u64, wip_ptr: u64, wip_aux: u64) -> Vec<u8> {
        let mut h = MAGIC.to_vec();
        h.extend_from_slice(&clen.to_le_bytes());
        h.extend_from_slice(&wip_ptr.to_le_bytes());
        h.extend_from_slice(&wip_aux.to_le_bytes());
        h
    }

    #[cfg(feature = "set")]
    #[test]
    fn set_journals_block_spanning_write_and_reopens_clean() {
        let (s, p) = mk_stack();
        let _g = Guard(p.clone());

        // 400-byte payload; a 300-byte `set` at offset 0 spans a 256 B block
        // boundary (physical [32, 332)), so it goes through the journal rather
        // than the atomic-write fast path.
        s.push(vec![b'A'; 400]).unwrap();
        s.set(0, vec![b'B'; 300]).unwrap();

        let mut expect = vec![b'B'; 300];
        expect.extend_from_slice(&[b'A'; 100]);
        assert_eq!(s.peek(0).unwrap(), expect);
        drop(s);

        // The tail backup is gone, wip is disarmed, and the value survives reopen.
        let raw = std::fs::read(&p).unwrap();
        assert_eq!(raw.len() as u64, HEADER_SIZE + 400, "tail not truncated");
        assert_eq!(&raw[16..24], &[0u8; 8], "wip_ptr not disarmed");
        let s2 = BStack::open(&p).unwrap();
        assert_eq!(s2.peek(0).unwrap(), expect);
        assert_eq!(s2.len().unwrap(), 400);
    }

    #[test]
    fn recovery_replays_armed_same_length_set() {
        // Craft the on-disk state of a `set` crashed after arm: header armed at
        // the target, old bytes still in place, new bytes staged in the tail.
        let path = {
            use std::sync::atomic::{AtomicU64, Ordering};
            static C: AtomicU64 = AtomicU64::new(0);
            let id = C.fetch_add(1, Ordering::Relaxed);
            std::env::temp_dir().join(format!("bstack_wip_replay_{}.bin", id))
        };
        let _g = Guard(path.clone());

        let clen = 400u64;
        // Target offset a = 0 → physical wip_ptr = HEADER_SIZE.
        let mut file = wip_header(clen, HEADER_SIZE, 0);
        file.extend_from_slice(&vec![b'A'; clen as usize]); // old payload
        file.extend_from_slice(&vec![b'B'; 300]); // staged tail (new bytes)
        std::fs::write(&path, &file).unwrap();

        // Recovery on open replays the tail into [0, 300).
        let s = BStack::open(&path).unwrap();
        let mut expect = vec![b'B'; 300];
        expect.extend_from_slice(&[b'A'; 100]);
        assert_eq!(s.len().unwrap(), 400);
        assert_eq!(s.peek(0).unwrap(), expect);
        drop(s);

        // The journal is disarmed and the tail dropped.
        let raw = std::fs::read(&path).unwrap();
        assert_eq!(raw.len() as u64, HEADER_SIZE + 400);
        assert_eq!(&raw[16..24], &[0u8; 8], "wip_ptr not cleared");
    }

    #[test]
    fn recovery_rolls_back_unrecognized_wip_aux() {
        // wip_ptr armed with a wip_aux mode this build does not implement: the
        // forward-compatibility default rolls back to the committed length (no
        // replay), leaving the old bytes intact and dropping the staged tail.
        let path = {
            use std::sync::atomic::{AtomicU64, Ordering};
            static C: AtomicU64 = AtomicU64::new(0);
            let id = C.fetch_add(1, Ordering::Relaxed);
            std::env::temp_dir().join(format!("bstack_wip_rollback_{}.bin", id))
        };
        let _g = Guard(path.clone());

        let clen = 400u64;
        let mut file = wip_header(clen, HEADER_SIZE, 1); // wip_aux = 1 (unknown mode)
        file.extend_from_slice(&vec![b'A'; clen as usize]);
        file.extend_from_slice(&vec![b'B'; 300]); // staged tail, must be discarded
        std::fs::write(&path, &file).unwrap();

        let s = BStack::open(&path).unwrap();
        assert_eq!(s.len().unwrap(), 400);
        assert_eq!(
            s.peek(0).unwrap(),
            vec![b'A'; 400],
            "should roll back, not replay"
        );
        drop(s);

        let raw = std::fs::read(&path).unwrap();
        assert_eq!(raw.len() as u64, HEADER_SIZE + 400);
        assert_eq!(&raw[16..24], &[0u8; 8], "wip_ptr not cleared");
    }

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn cross_exchange_journals_and_reopens_clean() {
        let (s, p) = mk_stack();
        let _g = Guard(p.clone());

        // Region A = [0, 50), region B = [100, 150); an exchange always journals.
        let mut payload = vec![b'm'; 200];
        payload[0..50].fill(b'A');
        payload[100..150].fill(b'B');
        s.push(&payload).unwrap();

        s.cross_exchange(0, 100, 50).unwrap();

        let out = s.peek(0).unwrap();
        assert_eq!(&out[0..50], &[b'B'; 50], "A should now hold B's bytes");
        assert_eq!(&out[100..150], &[b'A'; 50], "B should now hold A's bytes");
        drop(s);

        // Tail dropped, journal disarmed, exchange persists across reopen.
        let raw = std::fs::read(&p).unwrap();
        assert_eq!(raw.len() as u64, HEADER_SIZE + 200, "tail not truncated");
        assert_eq!(&raw[16..24], &[0u8; 8], "wip_ptr not disarmed");
        let s2 = BStack::open(&p).unwrap();
        let out2 = s2.peek(0).unwrap();
        assert_eq!(&out2[0..50], &[b'B'; 50]);
        assert_eq!(&out2[100..150], &[b'A'; 50]);
    }

    #[test]
    fn recovery_rolls_back_exchange_before_flip() {
        // Crashed after A <- B's bytes but while `wip_ptr` still names region A:
        // recovery replays A's staged bytes into A, restoring the original (B
        // was never touched). The whole exchange rolls back.
        let path = {
            use std::sync::atomic::{AtomicU64, Ordering};
            static C: AtomicU64 = AtomicU64::new(0);
            let id = C.fetch_add(1, Ordering::Relaxed);
            std::env::temp_dir().join(format!("bstack_xchg_rb_{}.bin", id))
        };
        let _g = Guard(path.clone());

        let clen = 200u64;
        let mut file = wip_header(clen, HEADER_SIZE, 0); // wip_ptr = region A
        let mut payload = vec![b'm'; clen as usize];
        payload[0..50].fill(b'B'); // A already overwritten with B's bytes
        payload[100..150].fill(b'B'); // B untouched (original)
        file.extend_from_slice(&payload);
        file.extend_from_slice(&[b'A'; 50]); // tail = A's original bytes
        std::fs::write(&path, &file).unwrap();

        let s = BStack::open(&path).unwrap();
        let out = s.peek(0).unwrap();
        assert_eq!(&out[0..50], &[b'A'; 50], "A restored to original");
        assert_eq!(&out[100..150], &[b'B'; 50], "B unchanged");
        assert_eq!(s.len().unwrap(), 200);
    }

    #[test]
    fn recovery_rolls_forward_exchange_after_flip() {
        // Crashed after the flip (`wip_ptr` names region B): recovery replays A's
        // staged bytes into B, completing the exchange (A already holds B's bytes).
        let path = {
            use std::sync::atomic::{AtomicU64, Ordering};
            static C: AtomicU64 = AtomicU64::new(0);
            let id = C.fetch_add(1, Ordering::Relaxed);
            std::env::temp_dir().join(format!("bstack_xchg_rf_{}.bin", id))
        };
        let _g = Guard(path.clone());

        let clen = 200u64;
        let mut file = wip_header(clen, HEADER_SIZE + 100, 0); // wip_ptr = region B
        let mut payload = vec![b'm'; clen as usize];
        payload[0..50].fill(b'B'); // A holds B's bytes (done)
        payload[100..150].fill(b'B'); // B not yet overwritten
        file.extend_from_slice(&payload);
        file.extend_from_slice(&[b'A'; 50]); // tail = A's original bytes
        std::fs::write(&path, &file).unwrap();

        let s = BStack::open(&path).unwrap();
        let out = s.peek(0).unwrap();
        assert_eq!(&out[0..50], &[b'B'; 50], "A holds B's bytes");
        assert_eq!(&out[100..150], &[b'A'; 50], "B filled with A's bytes");
        assert_eq!(s.len().unwrap(), 200);
    }

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn copy_streams_regions_larger_than_move_chunk() {
        let (s, p) = mk_stack();
        let _g = Guard(p.clone());

        // n exceeds MOVE_CHUNK (64 KiB), so the on-disk copy streams over several
        // buffer-sized iterations rather than buffering the whole region.
        let n = 200 * 1024usize;
        let mut payload = vec![0u8; 2 * n];
        for (i, b) in payload[..n].iter_mut().enumerate() {
            *b = (i % 251) as u8; // recognizable pattern in the source
        }
        s.push(&payload).unwrap();

        s.copy(0, n as u64, n as u64).unwrap(); // [0, n) -> [n, 2n)

        let out = s.peek(0).unwrap();
        assert_eq!(&out[..n], &out[n..2 * n], "destination should equal source");
        assert_eq!(out[n + 100_000], (100_000 % 251) as u8, "deep byte copied");
        drop(s);

        let s2 = BStack::open(&p).unwrap();
        let out2 = s2.peek(0).unwrap();
        assert_eq!(&out2[..n], &out2[n..2 * n], "copy survives reopen");
    }

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn cross_exchange_streams_large_regions() {
        let (s, p) = mk_stack();
        let _g = Guard(p.clone());

        let n = 100 * 1024usize; // > MOVE_CHUNK
        let mut payload = vec![b'A'; n];
        payload.extend(std::iter::repeat_n(b'B', n));
        s.push(&payload).unwrap();

        s.cross_exchange(0, n as u64, n as u64).unwrap(); // swap [0,n) with [n,2n)

        let out = s.peek(0).unwrap();
        assert!(out[..n].iter().all(|&x| x == b'B'), "A now holds B's bytes");
        assert!(
            out[n..2 * n].iter().all(|&x| x == b'A'),
            "B now holds A's bytes"
        );
        drop(s);

        let s2 = BStack::open(&p).unwrap();
        let out2 = s2.peek(0).unwrap();
        assert!(out2[..n].iter().all(|&x| x == b'B'));
        assert!(out2[n..2 * n].iter().all(|&x| x == b'A'));
    }

    // ---- repeat -------------------------------------------------------------

    #[cfg(feature = "set")]
    #[test]
    fn repeat_fills_pattern_and_reopens_clean() {
        let (s, p) = mk_stack();
        let _g = Guard(p.clone());

        // 600-byte region; 300 copies of a 2-byte pattern spans a block, so it
        // goes through the repeat-fill journal.
        s.push(vec![b'.'; 600]).unwrap();
        s.repeat(0, b"ab", 300).unwrap();

        let expect: Vec<u8> = b"ab".iter().copied().cycle().take(600).collect();
        assert_eq!(s.peek(0).unwrap(), expect);
        drop(s);

        // The 10-byte `[k | s]` tail is dropped and the journal disarmed.
        let raw = std::fs::read(&p).unwrap();
        assert_eq!(raw.len() as u64, HEADER_SIZE + 600, "tail not truncated");
        assert_eq!(&raw[16..24], &[0u8; 8], "wip_ptr not disarmed");
        let s2 = BStack::open(&p).unwrap();
        assert_eq!(s2.peek(0).unwrap(), expect);
    }

    #[cfg(feature = "set")]
    #[test]
    fn repeat_empty_or_zero_count_is_noop() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"hello world").unwrap();
        s.repeat(0, b"", 5).unwrap(); // empty pattern
        s.repeat(0, b"xy", 0).unwrap(); // zero count
        assert_eq!(s.peek(0).unwrap(), b"hello world");
    }

    #[cfg(feature = "set")]
    #[test]
    fn repeat_rejects_past_end() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(vec![0u8; 10]).unwrap();
        // 3 copies of a 4-byte pattern = 12 bytes > 10-byte payload.
        let err = s.repeat(0, b"abcd", 3).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidInput);
    }

    #[test]
    fn recovery_replays_armed_repeat() {
        use crate::WipAux;
        // Craft a crashed repeat: header armed REPEAT at offset 0, old bytes still
        // in place, tail = [k | s]. Recovery writes k copies of s.
        let path = {
            use std::sync::atomic::{AtomicU64, Ordering};
            static C: AtomicU64 = AtomicU64::new(0);
            let id = C.fetch_add(1, Ordering::Relaxed);
            std::env::temp_dir().join(format!("bstack_wip_repeat_{}.bin", id))
        };
        let _g = Guard(path.clone());

        let clen = 600u64;
        let mut file = wip_header(clen, HEADER_SIZE, u64::from(WipAux::Repeat));
        file.extend_from_slice(&vec![b'.'; clen as usize]); // unfilled old payload
        let k: u64 = 300;
        file.extend_from_slice(&k.to_le_bytes()); // tail: k
        file.extend_from_slice(b"ab"); // tail: s
        std::fs::write(&path, &file).unwrap();

        let s = BStack::open(&path).unwrap();
        let expect: Vec<u8> = b"ab".iter().copied().cycle().take(600).collect();
        assert_eq!(s.peek(0).unwrap(), expect, "repeat replayed on recovery");
        assert_eq!(s.len().unwrap(), 600);
        drop(s);

        let raw = std::fs::read(&path).unwrap();
        assert_eq!(raw.len() as u64, HEADER_SIZE + 600);
        assert_eq!(&raw[16..24], &[0u8; 8], "wip_ptr not cleared");
    }

    #[test]
    fn recovery_replays_armed_copy() {
        use crate::WipAux;
        // Craft a crashed disjoint copy: header armed COPY with wip_ptr = dest,
        // tail = [src | n], destination bytes not yet written. Recovery replays
        // move_chunked(src → dst) from the still-intact source.
        let path = {
            use std::sync::atomic::{AtomicU64, Ordering};
            static C: AtomicU64 = AtomicU64::new(0);
            let id = C.fetch_add(1, Ordering::Relaxed);
            std::env::temp_dir().join(format!("bstack_wip_copy_{}.bin", id))
        };
        let _g = Guard(path.clone());

        let clen = 600u64;
        let (src, dst, n) = (0u64, 300u64, 200u64);
        let mut payload = vec![b'S'; n as usize]; // source [0,200)
        payload.extend_from_slice(&vec![b'.'; (clen - n) as usize]); // rest incl. dest
        let mut file = wip_header(clen, HEADER_SIZE + dst, u64::from(WipAux::Copy));
        file.extend_from_slice(&payload);
        file.extend_from_slice(&src.to_le_bytes()); // tail: src
        file.extend_from_slice(&n.to_le_bytes()); // tail: n
        std::fs::write(&path, &file).unwrap();

        let s = BStack::open(&path).unwrap();
        assert_eq!(s.len().unwrap(), 600);
        assert_eq!(
            s.get(dst, dst + n).unwrap(),
            vec![b'S'; n as usize],
            "copy replayed"
        );
        assert_eq!(
            s.get(0, n).unwrap(),
            vec![b'S'; n as usize],
            "source intact"
        );
        drop(s);

        let raw = std::fs::read(&path).unwrap();
        assert_eq!(raw.len() as u64, HEADER_SIZE + 600, "tail not truncated");
        assert_eq!(&raw[16..24], &[0u8; 8], "wip_ptr not cleared");
    }

    #[test]
    fn recovery_rolls_forward_splice_grow() {
        use crate::WipAux;
        // Crashed grow-splice: the last 20 of 100 bytes are being replaced with 50
        // bytes → clen' = 130, splice point a = 80, staging base S = clen' = 130.
        let path = {
            use std::sync::atomic::{AtomicU64, Ordering};
            static C: AtomicU64 = AtomicU64::new(0);
            let id = C.fetch_add(1, Ordering::Relaxed);
            std::env::temp_dir().join(format!("bstack_wip_splice_grow_{}.bin", id))
        };
        let _g = Guard(path.clone());

        let mut file = wip_header(100, HEADER_SIZE + 80, u64::from(WipAux::SpliceGrow));
        let mut payload = vec![b'A'; 80];
        payload.extend_from_slice(&[b'X'; 20]); // old tail region, not yet replaced
        file.extend_from_slice(&payload); // 100 committed bytes
        file.extend_from_slice(&[0u8; 30]); // gap [100,130) from the extend
        file.extend_from_slice(&[b'N'; 50]); // staged new bytes [130,180)
        std::fs::write(&path, &file).unwrap();

        let s = BStack::open(&path).unwrap();
        let mut expect = vec![b'A'; 80];
        expect.extend_from_slice(&[b'N'; 50]);
        assert_eq!(s.len().unwrap(), 130, "clen' derived from file size");
        assert_eq!(s.peek(0).unwrap(), expect, "grow splice rolled forward");
        drop(s);

        let raw = std::fs::read(&path).unwrap();
        assert_eq!(raw.len() as u64, HEADER_SIZE + 130);
        assert_eq!(&raw[16..24], &[0u8; 8], "wip_ptr not cleared");
    }

    #[test]
    fn recovery_rolls_forward_splice_shrink() {
        use crate::WipAux;
        // Crashed shrink-splice: the last 50 of 130 bytes are being replaced with
        // 20 bytes → clen' = 100, a = 80, staging base S = clen = 130.
        let path = {
            use std::sync::atomic::{AtomicU64, Ordering};
            static C: AtomicU64 = AtomicU64::new(0);
            let id = C.fetch_add(1, Ordering::Relaxed);
            std::env::temp_dir().join(format!("bstack_wip_splice_shrink_{}.bin", id))
        };
        let _g = Guard(path.clone());

        let mut file = wip_header(130, HEADER_SIZE + 80, u64::from(WipAux::SpliceShrink));
        let mut payload = vec![b'A'; 80];
        payload.extend_from_slice(&[b'X'; 50]); // old tail region
        file.extend_from_slice(&payload); // 130 committed bytes
        file.extend_from_slice(&[b'N'; 20]); // staged new bytes [130,150)
        std::fs::write(&path, &file).unwrap();

        let s = BStack::open(&path).unwrap();
        let mut expect = vec![b'A'; 80];
        expect.extend_from_slice(&[b'N'; 20]);
        assert_eq!(s.len().unwrap(), 100, "clen' derived from file size");
        assert_eq!(s.peek(0).unwrap(), expect, "shrink splice rolled forward");
        drop(s);

        let raw = std::fs::read(&path).unwrap();
        assert_eq!(raw.len() as u64, HEADER_SIZE + 100);
        assert_eq!(&raw[16..24], &[0u8; 8], "wip_ptr not cleared");
    }

    #[cfg(feature = "atomic")]
    #[test]
    fn splice_journals_grow_and_reopens_clean() {
        let (s, p) = mk_stack();
        let _g = Guard(p.clone());
        s.push(vec![b'A'; 400]).unwrap();
        // Pop the last 100, push 300 → length-changing tail replace (grow to 600).
        s.atrunc(100, vec![b'N'; 300]).unwrap();

        let mut expect = vec![b'A'; 300];
        expect.extend_from_slice(&[b'N'; 300]);
        assert_eq!(s.len().unwrap(), 600);
        assert_eq!(s.peek(0).unwrap(), expect);
        drop(s);

        let raw = std::fs::read(&p).unwrap();
        assert_eq!(
            raw.len() as u64,
            HEADER_SIZE + 600,
            "staged bytes not dropped"
        );
        assert_eq!(&raw[16..24], &[0u8; 8], "wip_ptr not disarmed");
        let s2 = BStack::open(&p).unwrap();
        assert_eq!(s2.peek(0).unwrap(), expect);
        assert_eq!(s2.len().unwrap(), 600);
    }

    #[cfg(feature = "atomic")]
    #[test]
    fn splice_journals_shrink_and_reopens_clean() {
        let (s, p) = mk_stack();
        let _g = Guard(p.clone());
        s.push(vec![b'A'; 600]).unwrap();
        // Pop the last 400, push 100 → length-changing tail replace (shrink to 300).
        s.atrunc(400, vec![b'N'; 100]).unwrap();

        let mut expect = vec![b'A'; 200];
        expect.extend_from_slice(&[b'N'; 100]);
        assert_eq!(s.len().unwrap(), 300);
        assert_eq!(s.peek(0).unwrap(), expect);
        drop(s);

        let raw = std::fs::read(&p).unwrap();
        assert_eq!(
            raw.len() as u64,
            HEADER_SIZE + 300,
            "staged bytes not dropped"
        );
        assert_eq!(&raw[16..24], &[0u8; 8], "wip_ptr not disarmed");
        let s2 = BStack::open(&p).unwrap();
        assert_eq!(s2.peek(0).unwrap(), expect);
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
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
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
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
        assert_eq!(s.len().unwrap(), 0);
    }

    #[test]
    fn extend_sparse_batched_out_of_range_errors() {
        let (s, p) = mk_stack();
        let _g = Guard(p);

        let err = s
            .extend_sparse_batched(vec![(3u64, b"zzz".as_slice())], 5)
            .unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
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
}

// -------------------------------------------------------------------------
// Allocator tests

#[cfg(all(test, feature = "alloc"))]
mod alloc_tests {
    use crate::BStack;
    use crate::alloc::{
        BStackAllocator, BStackBulkAllocator, BStackChunk, BStackRange, BStackSlice,
        LinearBStackAllocator,
    };
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
        let s = unsafe { BStackSlice::from_raw_parts(alloc.stack(), 0, 8) };
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

    // A tail grow must preserve the existing bytes and zero the newly added
    // ones. Both builds realise the growth with a single sparse `set_len`
    // (`extend`, or `try_extend_zeros` under `atomic`) rather than writing a
    // zero buffer, so this pins the contract that makes that legal.
    #[cfg(feature = "set")]
    #[test]
    fn realloc_tail_grow_preserves_data_and_zeroes_new_bytes() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let mut s = alloc.alloc(8).unwrap();
        s.write([0xABu8; 8]).unwrap();

        let grown = alloc.realloc(s, 24).unwrap();
        assert_eq!(grown.len(), 24);
        let data = grown.read().unwrap();
        assert_eq!(&data[..8], &[0xABu8; 8], "existing bytes must survive");
        assert_eq!(&data[8..], &[0u8; 16], "newly added bytes must read zero");
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
        assert_eq!(err.source.kind(), std::io::ErrorKind::Unsupported);
        // A failed realloc must hand the untouched original back to the caller.
        assert!(err.handle.is_some());
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
        let mut s = alloc.alloc(5).unwrap();
        s.write(b"hello").unwrap();
        assert_eq!(s.read().unwrap(), b"hello");
    }

    // write with shorter data writes only what's provided, leaves rest untouched
    #[cfg(feature = "set")]
    #[test]
    fn write_shorter_data() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let mut s = alloc.alloc(5).unwrap();
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
        let mut s = alloc.alloc(3).unwrap();
        s.write(b"hello").unwrap(); // writes only 3 bytes
        assert_eq!(s.read().unwrap(), b"hel");
    }

    #[cfg(feature = "set")]
    #[test]
    fn write_range_partial() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let mut s = alloc.alloc(5).unwrap();
        s.write_range(1, b"abc").unwrap();
        let data = s.read().unwrap();
        assert_eq!(data, b"\x00abc\x00");
    }

    #[cfg(feature = "set")]
    #[test]
    fn write_range_out_of_bounds() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let mut s = alloc.alloc(5).unwrap();
        let err = s.write_range(3, b"abc").unwrap_err(); // 3+3 > 5
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
    }

    #[cfg(feature = "set")]
    #[test]
    fn zero_clears_slice() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let mut s = alloc.alloc(4).unwrap();
        s.write(b"abcd").unwrap();
        s.zero().unwrap();
        assert_eq!(s.read().unwrap(), vec![0u8; 4]);
    }

    #[cfg(feature = "set")]
    #[test]
    fn zero_range_partial() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let mut s = alloc.alloc(4).unwrap();
        s.write(b"abcd").unwrap();
        s.zero_range(1, 2).unwrap();
        assert_eq!(s.read().unwrap(), b"a\x00\x00d");
    }

    #[cfg(feature = "set")]
    #[test]
    fn zero_range_out_of_bounds() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let mut s = alloc.alloc(4).unwrap();
        let err = s.zero_range(3, 2).unwrap_err(); // 3+2 > 4
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
    }

    // 20. subslice creates correct sub-slice
    #[test]
    fn subslice_correct() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let s = alloc.alloc(10).unwrap();
        let sub = s.as_slice().subslice(2, 8);
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
        let sub = s.as_slice().subslice(5, 5);
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
        let _ = s.as_slice().subslice(8, 5); // start > end
    }

    // 23. subslice panics on out of bounds
    #[test]
    #[should_panic(expected = "range end must be <= slice length")]
    fn subslice_out_of_bounds() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let s = alloc.alloc(10).unwrap();
        let _ = s.as_slice().subslice(5, 15); // end > len
    }

    // 24. start returns offset
    #[test]
    fn start_returns_offset() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let s = alloc.alloc(10).unwrap();
        assert_eq!(s.start(), 0);
        let sub = s.as_slice().subslice(3, 7);
        assert_eq!(sub.start(), 3);
    }

    // 25. range returns correct range
    #[test]
    fn range_returns_correct() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let s = alloc.alloc(10).unwrap();
        assert_eq!(s.range(), 0..10);
        let sub = s.as_slice().subslice(2, 8);
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
        assert!(dbg.contains("0.4"), "{dbg}");
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
        let writer = s.as_slice().writer_at(3);
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
        let short = s.as_slice().subslice(0, 3).reader();
        let long_ = s.as_slice().subslice(0, 8).reader();
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
        let w0 = s.as_slice().writer_at(0);
        let w5 = s.as_slice().writer_at(5);
        assert!(w0 < w5);
        assert_eq!(
            w5.cmp(&s.as_slice().writer_at(5)),
            std::cmp::Ordering::Equal
        );
    }

    #[cfg(feature = "set")]
    #[test]
    fn writer_ord_earlier_slice_before_later() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let a = alloc.alloc(8).unwrap(); // offset 0..8
        let b = alloc.alloc(8).unwrap(); // offset 8..16
        assert!(a.as_slice().writer() < b.as_slice().writer());
    }

    // ---- Cross-type PartialOrd (reader ↔ writer) ----------------------------

    #[cfg(feature = "set")]
    #[test]
    fn reader_writer_cross_partial_ord() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let s = alloc.alloc(10).unwrap();
        let r3 = s.as_slice().reader_at(3);
        let w5 = s.as_slice().writer_at(5);
        let w3 = s.as_slice().writer_at(3);
        let r5 = s.as_slice().reader_at(5);
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
        let r2 = s.as_slice().reader_at(2);
        let w8 = s.as_slice().writer_at(8);
        let r15 = s.as_slice().reader_at(15);
        assert!(r2 < w8);
        assert!(w8 < r15);
        assert!(r2 < r15);
    }

    // ---- BStackSlice: ergonomic query methods -------------------------------

    #[test]
    fn slice_get_in_and_out_of_bounds() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        #[cfg_attr(not(feature = "set"), allow(unused_mut))]
        let mut s = alloc.alloc(4).unwrap();
        #[cfg(feature = "set")]
        s.as_slice_mut().write([10u8, 20, 30, 40]).unwrap();
        let view = s.as_slice();
        #[cfg(feature = "set")]
        {
            assert_eq!(view.get(0).unwrap(), Some(10));
            assert_eq!(view.get(3).unwrap(), Some(40));
        }
        assert_eq!(view.get(4).unwrap(), None);
        assert_eq!(view.get(100).unwrap(), None);
    }

    #[test]
    fn slice_head_caps_at_len() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let s = alloc.alloc(5).unwrap();
        let view = s.as_slice();
        assert_eq!(view.head(2).len(), 2);
        assert_eq!(view.head(2).start(), view.start());
        assert_eq!(view.head(100).len(), 5);
    }

    #[test]
    fn slice_tail_caps_at_len() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let s = alloc.alloc(5).unwrap();
        let view = s.as_slice();
        let t = view.tail(2);
        assert_eq!(t.len(), 2);
        assert_eq!(t.start(), view.start() + 3);
        assert_eq!(view.tail(100).len(), 5);
    }

    #[cfg(feature = "set")]
    #[test]
    fn slice_contains() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let mut s = alloc.alloc(4).unwrap();
        s.write([1u8, 2, 3, 4]).unwrap();
        let view = s.as_slice();
        assert!(view.contains(3).unwrap());
        assert!(!view.contains(9).unwrap());
    }

    #[cfg(feature = "set")]
    #[test]
    fn slice_starts_and_ends_with() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let mut s = alloc.alloc(4).unwrap();
        s.write([1u8, 2, 3, 4]).unwrap();
        let view = s.as_slice();
        assert!(view.starts_with(&[1, 2]).unwrap());
        assert!(!view.starts_with(&[2, 3]).unwrap());
        assert!(view.ends_with(&[3, 4]).unwrap());
        assert!(!view.ends_with(&[1, 2]).unwrap());
        assert!(!view.starts_with(&[1, 2, 3, 4, 5]).unwrap());
        assert!(!view.ends_with(&[1, 2, 3, 4, 5]).unwrap());
    }

    #[cfg(feature = "set")]
    #[test]
    fn slice_find_and_rfind() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let mut s = alloc.alloc(5).unwrap();
        s.write([1u8, 2, 3, 2, 1]).unwrap();
        let view = s.as_slice();
        assert_eq!(view.find(2).unwrap(), Some(1));
        assert_eq!(view.rfind(2).unwrap(), Some(3));
        assert_eq!(view.find(9).unwrap(), None);
        assert_eq!(view.rfind(9).unwrap(), None);
    }

    #[cfg(feature = "set")]
    #[test]
    fn slice_position_and_rposition() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let mut s = alloc.alloc(5).unwrap();
        s.write([1u8, 2, 3, 4, 5]).unwrap();
        let view = s.as_slice();
        assert_eq!(view.position(|b| b > 2).unwrap(), Some(2));
        assert_eq!(view.rposition(|b| b > 2).unwrap(), Some(4));
        assert_eq!(view.position(|b| b > 10).unwrap(), None);
    }

    #[test]
    fn slice_split_at() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let s = alloc.alloc(10).unwrap();
        let (a, b) = s.as_slice().split_at(4);
        assert_eq!(a.range(), 0..4);
        assert_eq!(b.range(), 4..10);
    }

    #[test]
    #[should_panic(expected = "mid must be <= slice length")]
    fn slice_split_at_out_of_bounds() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let s = alloc.alloc(4).unwrap();
        let _ = s.as_slice().split_at(5);
    }

    #[test]
    fn slice_split_at_mut() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let mut s = alloc.alloc(10).unwrap();
        let mut view = s.as_slice_mut();
        let (a, b) = view.split_at_mut(4);
        assert_eq!(a.range(), 0..4);
        assert_eq!(b.range(), 4..10);
    }

    // ---- BStackRange: overlaps / adjacent_to ---------------------------------

    #[test]
    fn range_overlaps_shared_bytes() {
        let a = BStackRange::new(0, 10);
        let b = BStackRange::new(5, 10);
        assert!(a.overlaps(&b));
        assert!(b.overlaps(&a));
    }

    #[test]
    fn range_overlaps_false_when_touching_or_disjoint() {
        let a = BStackRange::new(0, 5);
        let touching = BStackRange::new(5, 5);
        let disjoint = BStackRange::new(10, 5);
        assert!(!a.overlaps(&touching));
        assert!(!a.overlaps(&disjoint));
    }

    #[test]
    fn range_overlaps_false_for_empty_ranges() {
        let empty = BStackRange::new(5, 0);
        let containing = BStackRange::new(0, 10);
        assert!(!empty.overlaps(&containing));
        assert!(!containing.overlaps(&empty));
        assert!(!empty.overlaps(&empty));
    }

    #[test]
    fn range_adjacent_to_touching_ranges() {
        let a = BStackRange::new(0, 5);
        let b = BStackRange::new(5, 5);
        assert!(a.adjacent_to(&b));
        assert!(b.adjacent_to(&a));
    }

    #[test]
    fn range_adjacent_to_false_when_overlapping_or_gapped() {
        let a = BStackRange::new(0, 5);
        let overlapping = BStackRange::new(4, 5);
        let gapped = BStackRange::new(6, 5);
        assert!(!a.adjacent_to(&overlapping));
        assert!(!a.adjacent_to(&gapped));
    }

    // ---- BStackRange: merge / merge_adjacent ---------------------------------

    #[test]
    fn range_merge_overlapping_returns_union() {
        let a = BStackRange::new(0, 6); // 0..6
        let b = BStackRange::new(4, 6); // 4..10
        let merged = a.merge(&b).unwrap();
        assert_eq!(merged.start(), 0);
        assert_eq!(merged.end(), 10);
    }

    #[test]
    fn range_merge_disjoint_non_empty_returns_none() {
        let a = BStackRange::new(0, 5);
        let b = BStackRange::new(10, 5);
        assert!(a.merge(&b).is_none());
    }

    #[test]
    fn range_merge_touching_non_overlapping_returns_none() {
        // merge() requires overlap, not mere adjacency.
        let a = BStackRange::new(0, 5);
        let b = BStackRange::new(5, 5);
        assert!(a.merge(&b).is_none());
    }

    #[test]
    fn range_merge_empty_is_identity() {
        let empty = BStackRange::new(100, 0);
        let other = BStackRange::new(0, 10);
        assert_eq!(empty.merge(&other), Some(other));
        assert_eq!(other.merge(&empty), Some(other));
        let other_empty = BStackRange::new(50, 0);
        assert_eq!(empty.merge(&other_empty), Some(other_empty));
    }

    #[test]
    fn range_merge_adjacent_touching_ranges() {
        let a = BStackRange::new(0, 5); // 0..5
        let b = BStackRange::new(5, 5); // 5..10
        let merged = a.merge_adjacent(&b).unwrap();
        assert_eq!(merged.start(), 0);
        assert_eq!(merged.end(), 10);
    }

    #[test]
    fn range_merge_adjacent_rejects_overlap_gap_and_empty() {
        let a = BStackRange::new(0, 5);
        let overlapping = BStackRange::new(4, 5);
        let gapped = BStackRange::new(6, 5);
        let empty_touching = BStackRange::new(5, 0);
        assert!(a.merge_adjacent(&overlapping).is_none());
        assert!(a.merge_adjacent(&gapped).is_none());
        assert!(a.merge_adjacent(&empty_touching).is_none());
    }

    // ---- BStackSlice: overlaps / adjacent_to (wrap BStackRange) -------------

    #[test]
    fn slice_overlaps_and_adjacent_to() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let a = alloc.alloc(4).unwrap(); // 0..4
        let b = alloc.alloc(4).unwrap(); // 4..8
        assert!(!a.as_slice().overlaps(&b.as_slice()));
        assert!(a.as_slice().adjacent_to(&b.as_slice()));

        let tail = a.as_slice().subslice(2, 4); // 2..4, shares bytes [2, 4) with a
        assert!(a.as_slice().overlaps(&tail));
        assert!(!a.as_slice().adjacent_to(&tail));
    }

    // ---- BStackSlice: merge / merge_adjacent (wrap BStackRange) -------------

    #[test]
    fn slice_merge_overlapping_and_touching() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let a = alloc.alloc(4).unwrap(); // 0..4
        let b = alloc.alloc(4).unwrap(); // 4..8
        // Touching, not overlapping: merge() requires an actual overlap.
        assert!(a.as_slice().merge(&b.as_slice()).is_none());
        let merged = a.as_slice().merge_adjacent(&b.as_slice()).unwrap();
        assert_eq!(merged.range(), 0..8);

        let tail = a.as_slice().subslice(2, 4); // 2..4, overlaps a
        let merged_overlap = a.as_slice().merge(&tail).unwrap();
        assert_eq!(merged_overlap.range(), 0..4);
    }

    #[test]
    fn slice_merge_rejects_different_backing_stacks() {
        let (alloc_a, path_a) = mk_alloc();
        let _ga = Guard(path_a);
        let (alloc_b, path_b) = mk_alloc();
        let _gb = Guard(path_b);
        let a = alloc_a.alloc(4).unwrap();
        let b = alloc_b.alloc(4).unwrap();
        assert!(a.as_slice().merge(&b.as_slice()).is_none());
        assert!(a.as_slice().merge_adjacent(&b.as_slice()).is_none());
    }

    // ---- BStackSlice: ergonomic write methods (feature `set`) --------------

    #[cfg(feature = "set")]
    #[test]
    fn slice_fill() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let mut s = alloc.alloc(4).unwrap();
        s.as_slice_mut().fill(7).unwrap();
        assert_eq!(s.read().unwrap(), [7, 7, 7, 7]);
    }

    #[cfg(feature = "set")]
    #[test]
    fn slice_fill_with() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let mut s = alloc.alloc(4).unwrap();
        let mut next = 0u8;
        s.as_slice_mut()
            .fill_with(|| {
                next += 1;
                next
            })
            .unwrap();
        assert_eq!(s.read().unwrap(), [1, 2, 3, 4]);
    }

    #[cfg(feature = "set")]
    #[test]
    fn slice_copy_from_slice() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let mut s = alloc.alloc(3).unwrap();
        s.as_slice_mut().copy_from_slice(&[9, 8, 7]).unwrap();
        assert_eq!(s.read().unwrap(), [9, 8, 7]);
    }

    #[cfg(feature = "set")]
    #[test]
    #[should_panic(expected = "length mismatch")]
    fn slice_copy_from_slice_length_mismatch() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let mut s = alloc.alloc(3).unwrap();
        let _ = s.as_slice_mut().copy_from_slice(&[9, 8]);
    }

    // ---- BStackSlice: atomic write methods (features `set` + `atomic`) -----

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn slice_copy_from_bstack_slice_same_stack() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let mut src = alloc.alloc(3).unwrap();
        src.write([1u8, 2, 3]).unwrap();
        let mut dst = alloc.alloc(3).unwrap();
        dst.as_slice_mut()
            .copy_from_bstack_slice(&src.as_slice())
            .unwrap();
        assert_eq!(dst.read().unwrap(), [1, 2, 3]);
    }

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn slice_copy_from_bstack_slice_cross_stack_errors() {
        let (alloc_a, path_a) = mk_alloc();
        let _g_a = Guard(path_a);
        let (alloc_b, path_b) = mk_alloc();
        let _g_b = Guard(path_b);
        let src = alloc_a.alloc(3).unwrap();
        let mut dst = alloc_b.alloc(3).unwrap();
        let err = dst
            .as_slice_mut()
            .copy_from_bstack_slice(&src.as_slice())
            .unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
    }

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn slice_copy_within_overlapping() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let mut s = alloc.alloc(5).unwrap();
        s.write([1u8, 2, 3, 4, 5]).unwrap();
        s.as_slice_mut().copy_within(0..3, 2).unwrap();
        assert_eq!(s.read().unwrap(), [1, 2, 1, 2, 3]);
    }

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn slice_swap_exchanges_contents() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let mut a = alloc.alloc(3).unwrap();
        a.write([1u8, 2, 3]).unwrap();
        let mut b = alloc.alloc(3).unwrap();
        b.write([4u8, 5, 6]).unwrap();
        let mut a_view = a.as_slice_mut();
        let mut b_view = b.as_slice_mut();
        a_view.swap(&mut b_view).unwrap();
        assert_eq!(a.read().unwrap(), [4, 5, 6]);
        assert_eq!(b.read().unwrap(), [1, 2, 3]);
    }

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn slice_swap_cross_stack_errors() {
        let (alloc_a, path_a) = mk_alloc();
        let _g_a = Guard(path_a);
        let (alloc_b, path_b) = mk_alloc();
        let _g_b = Guard(path_b);
        let mut a = alloc_a.alloc(3).unwrap();
        let mut b = alloc_b.alloc(3).unwrap();
        let mut a_view = a.as_slice_mut();
        let mut b_view = b.as_slice_mut();
        let err = a_view.swap(&mut b_view).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
    }

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn slice_reverse() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let mut s = alloc.alloc(5).unwrap();
        s.write([1u8, 2, 3, 4, 5]).unwrap();
        s.as_slice_mut().reverse().unwrap();
        assert_eq!(s.read().unwrap(), [5, 4, 3, 2, 1]);
    }

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn slice_rotate_left() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let mut s = alloc.alloc(5).unwrap();
        s.write([1u8, 2, 3, 4, 5]).unwrap();
        s.as_slice_mut().rotate_left(2).unwrap();
        assert_eq!(s.read().unwrap(), [3, 4, 5, 1, 2]);
    }

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn slice_rotate_right() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let mut s = alloc.alloc(5).unwrap();
        s.write([1u8, 2, 3, 4, 5]).unwrap();
        s.as_slice_mut().rotate_right(2).unwrap();
        assert_eq!(s.read().unwrap(), [4, 5, 1, 2, 3]);
    }

    // ---- BStackSlice: cas_on / cas_on_ne / cas_on_masked (features `set` + `atomic`) ----

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn slice_cas_on_match_swaps_and_returns_old() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let mut guard = alloc.alloc(2).unwrap();
        guard.write([1u8, 2]).unwrap();
        let mut target = alloc.alloc(2).unwrap();
        target.write([9u8, 9]).unwrap();
        let old = target
            .as_slice_mut()
            .cas_on(&guard.as_slice(), [1u8, 2], [3u8, 4])
            .unwrap();
        assert_eq!(old, Some(vec![9, 9]));
        assert_eq!(target.read().unwrap(), [3, 4]);
    }

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn slice_cas_on_no_match_leaves_target_untouched() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let mut guard = alloc.alloc(2).unwrap();
        guard.write([1u8, 2]).unwrap();
        let mut target = alloc.alloc(2).unwrap();
        target.write([9u8, 9]).unwrap();
        let result = target
            .as_slice_mut()
            .cas_on(&guard.as_slice(), [0u8, 0], [3u8, 4])
            .unwrap();
        assert_eq!(result, None);
        assert_eq!(target.read().unwrap(), [9, 9]);
    }

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn slice_cas_on_expected_length_mismatch_errors() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let guard = alloc.alloc(2).unwrap();
        let mut target = alloc.alloc(2).unwrap();
        let err = target
            .as_slice_mut()
            .cas_on(&guard.as_slice(), [0u8], [3u8, 4])
            .unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
    }

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn slice_cas_on_new_bytes_length_mismatch_errors() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let mut guard = alloc.alloc(2).unwrap();
        guard.write([1u8, 2]).unwrap();
        let mut target = alloc.alloc(2).unwrap();
        let err = target
            .as_slice_mut()
            .cas_on(&guard.as_slice(), [1u8, 2], [3u8])
            .unwrap_err();
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
        let err = target
            .as_slice_mut()
            .cas_on(&guard.as_slice(), [0u8, 0], [1u8, 1])
            .unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
    }

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn slice_cas_on_ne_no_match_swaps_and_returns_old() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let mut guard = alloc.alloc(2).unwrap();
        guard.write([1u8, 2]).unwrap();
        let mut target = alloc.alloc(2).unwrap();
        target.write([9u8, 9]).unwrap();
        let old = target
            .as_slice_mut()
            .cas_on_ne(&guard.as_slice(), [0u8, 0], [3u8, 4])
            .unwrap();
        assert_eq!(old, Some(vec![9, 9]));
        assert_eq!(target.read().unwrap(), [3, 4]);
    }

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn slice_cas_on_ne_match_returns_none() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let mut guard = alloc.alloc(2).unwrap();
        guard.write([1u8, 2]).unwrap();
        let mut target = alloc.alloc(2).unwrap();
        target.write([9u8, 9]).unwrap();
        let result = target
            .as_slice_mut()
            .cas_on_ne(&guard.as_slice(), [1u8, 2], [3u8, 4])
            .unwrap();
        assert_eq!(result, None);
        assert_eq!(target.read().unwrap(), [9, 9]);
    }

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn slice_cas_on_masked_match_swaps_and_returns_old() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let mut guard = alloc.alloc(2).unwrap();
        guard.write([0xffu8, 0x0f]).unwrap();
        let mut target = alloc.alloc(2).unwrap();
        target.write([9u8, 9]).unwrap();
        // mask = [0xff, 0xf0]: masked guard = [0xff, 0x00], masked expected = [0xff, 0x00] -> match
        let old = target
            .as_slice_mut()
            .cas_on_masked(&guard.as_slice(), [0xffu8, 0xf0], [0xffu8, 0x0f], [3u8, 4])
            .unwrap();
        assert_eq!(old, Some(vec![9, 9]));
        assert_eq!(target.read().unwrap(), [3, 4]);
    }

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn slice_cas_on_masked_no_match_returns_none() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let mut guard = alloc.alloc(1).unwrap();
        guard.write([0x0fu8]).unwrap();
        let mut target = alloc.alloc(2).unwrap();
        target.write([9u8, 9]).unwrap();
        let result = target
            .as_slice_mut()
            .cas_on_masked(&guard.as_slice(), [0xffu8], [0xffu8], [3u8, 4])
            .unwrap();
        assert_eq!(result, None);
        assert_eq!(target.read().unwrap(), [9, 9]);
    }

    // ---- BStackSlice: process (features `set` + `atomic`) -------------------

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn slice_process_transforms_in_place() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let mut s = alloc.alloc(4).unwrap();
        s.write([1u8, 2, 3, 4]).unwrap();
        s.as_slice_mut()
            .process(|buf| {
                for b in buf.iter_mut() {
                    *b *= 2;
                }
            })
            .unwrap();
        assert_eq!(s.read().unwrap(), [2, 4, 6, 8]);
    }

    // ---- BStackOwnedSlice: ergonomic method mirrors -------------------------

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn owned_slice_ergonomic_mirrors() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let mut s = alloc.alloc(5).unwrap();
        s.write([1u8, 2, 3, 4, 5]).unwrap();
        assert_eq!(s.get(0).unwrap(), Some(1));
        assert!(s.contains(3).unwrap());
        assert!(s.starts_with(&[1, 2]).unwrap());
        assert!(s.ends_with(&[4, 5]).unwrap());
        assert_eq!(s.find(3).unwrap(), Some(2));
        assert_eq!(s.rfind(3).unwrap(), Some(2));
        assert_eq!(s.position(|b| b == 4).unwrap(), Some(3));
        assert_eq!(s.rposition(|b| b == 4).unwrap(), Some(3));
        assert_eq!(s.head(2).len(), 2);
        assert_eq!(s.tail(2).len(), 2);
        let (a, b) = s.split_at(2);
        assert_eq!(a.len(), 2);
        assert_eq!(b.len(), 3);

        s.fill(0).unwrap();
        assert_eq!(s.read().unwrap(), [0, 0, 0, 0, 0]);
        s.fill_with(|| 9).unwrap();
        assert_eq!(s.read().unwrap(), [9, 9, 9, 9, 9]);
        s.copy_from_slice(&[1, 2, 3, 4, 5]).unwrap();
        assert_eq!(s.read().unwrap(), [1, 2, 3, 4, 5]);
        s.reverse().unwrap();
        assert_eq!(s.read().unwrap(), [5, 4, 3, 2, 1]);
        s.rotate_left(1).unwrap();
        assert_eq!(s.read().unwrap(), [4, 3, 2, 1, 5]);
        s.rotate_right(1).unwrap();
        assert_eq!(s.read().unwrap(), [5, 4, 3, 2, 1]);

        let mut other = alloc.alloc(5).unwrap();
        other.write([10u8, 20, 30, 40, 50]).unwrap();
        s.copy_from_bstack_slice(&other.as_slice()).unwrap();
        assert_eq!(s.read().unwrap(), [10, 20, 30, 40, 50]);
        s.copy_within(0..2, 3).unwrap();
        assert_eq!(s.read().unwrap(), [10, 20, 30, 10, 20]);

        let mut third = alloc.alloc(5).unwrap();
        third.write([1u8, 1, 1, 1, 1]).unwrap();
        s.swap(&mut third.as_slice_mut()).unwrap();
        assert_eq!(s.read().unwrap(), [1, 1, 1, 1, 1]);
        assert_eq!(third.read().unwrap(), [10, 20, 30, 10, 20]);

        let mut split_owned = alloc.alloc(4).unwrap();
        let (a, b) = split_owned.split_at_mut(1);
        assert_eq!(a.len(), 1);
        assert_eq!(b.len(), 3);
    }

    // ---- Location equality: BStackSlice / BStackOwnedSlice ------------------

    // 1. BStackSlice == BStackSlice compares coordinates, not identity of the
    //    borrowed BStack reference.
    #[test]
    fn slice_eq_slice_by_location() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let a = alloc.alloc(4).unwrap();
        let b = alloc.alloc(4).unwrap();
        assert_eq!(a.as_slice(), a.as_slice());
        assert_ne!(a.as_slice(), b.as_slice());
    }

    // 2. BStackOwnedSlice == BStackOwnedSlice compares coordinates, not
    //    handle identity: a second handle built over the same range compares
    //    equal, while a handle over a different range does not.
    #[test]
    fn owned_eq_owned_by_location() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let a = alloc.alloc(4).unwrap();
        let b = alloc.alloc(4).unwrap();
        let a_view =
            unsafe { crate::alloc::BStackOwnedSlice::from_raw_range(&alloc, a.as_range()) };
        assert_eq!(a, a_view);
        assert_ne!(a, b);
    }

    // 3. BStackSlice == BStackOwnedSlice (both directions) compares coordinates.
    #[test]
    fn slice_eq_owned_cross_type() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let owned = alloc.alloc(4).unwrap();
        let matching = owned.as_slice();
        assert_eq!(owned, matching);
        assert_eq!(matching, owned);
        let other = alloc.alloc(4).unwrap();
        assert_ne!(owned, other.as_slice());
        assert_ne!(other.as_slice(), owned);
    }

    // 4. BStackRange == BStackSlice (both directions) compares coordinates.
    #[test]
    fn range_eq_slice_cross_type() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let s = alloc.alloc(4).unwrap();
        let view = s.as_slice();
        let range = view.as_range();
        assert_eq!(range, view);
        assert_eq!(view, range);
        let other = alloc.alloc(4).unwrap();
        assert_ne!(range, other.as_slice());
        assert_ne!(other.as_slice(), range);
    }

    // 5. BStackRange == BStackOwnedSlice (both directions) compares coordinates.
    #[test]
    fn range_eq_owned_cross_type() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let owned = alloc.alloc(4).unwrap();
        let range = owned.as_range();
        assert_eq!(range, owned);
        assert_eq!(owned, range);
        let other = alloc.alloc(4).unwrap();
        assert_ne!(range, other);
        assert_ne!(other, range);
    }

    // 6. PartialOrd is cross-type consistent among BStackSlice / BStackOwnedSlice
    //    / BStackRange, ordered by (offset, len) same as each type's own Ord.
    #[test]
    fn partial_ord_cross_type_matches_offset_order() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let a = alloc.alloc(4).unwrap(); // offset 0..4
        let b = alloc.alloc(4).unwrap(); // offset 4..8
        assert!(a.start() < b.start());
        let a_slice = a.as_slice();
        let b_slice = b.as_slice();
        let b_range = b.as_range();

        // BStackSlice <-> BStackOwnedSlice
        assert!(a_slice < b);
        assert!(b > a_slice);
        // BStackSlice <-> BStackRange
        assert!(a_slice < b_range);
        assert!(b_range > a_slice);
        // BStackOwnedSlice <-> BStackRange
        assert!(a < b_range);
        assert!(b_range > a);
        // BStackSlice <-> BStackSlice (existing, sanity)
        assert!(a_slice < b_slice);
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
        let mut slices = alloc.alloc_bulk([8_u64, 16, 32]).unwrap();
        // Reclaim only the last two slices (tail suffix), keeping slices[0] alive.
        let tail = slices.drain(1..).collect::<Vec<_>>();
        alloc.dealloc_bulk(tail).unwrap();
        assert_eq!(alloc.len().unwrap(), 8);
        // slices[0] (0..8) is still live; a new bulk alloc goes right after it.
        let new = alloc.alloc_bulk([4_u64, 4]).unwrap();
        assert_eq!(new[0].start(), 8);
        assert_eq!(new[1].start(), 12);
        let _ = slices; // keep the first slice alive
    }

    // ---- BStackChunk: construction, remainder, iteration --------------------

    // 1. chunks() aligns from the start; leftover bytes are the tail remainder.
    #[test]
    fn chunks_basic_and_remainder() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let s = alloc.alloc(10).unwrap(); // 10 bytes, chunk_len 3 -> 3 chunks + 1 remainder
        let (view, rem) = s.as_slice().chunks(3);
        assert_eq!(view.chunk_len(), 3);
        assert_eq!(view.chunk_count(), 3);
        assert_eq!(view.len(), 9);
        assert!(!view.is_empty());
        assert_eq!(rem.len(), 1);
        assert_eq!(rem.start(), 9); // trailing remainder
    }

    // 2. rchunks() aligns from the end; leftover bytes are the head remainder.
    #[test]
    fn rchunks_basic_and_remainder() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let s = alloc.alloc(10).unwrap();
        let (view, rem) = s.as_slice().rchunks(3);
        assert_eq!(view.chunk_count(), 3);
        assert_eq!(view.len(), 9);
        assert_eq!(rem.len(), 1);
        assert_eq!(rem.start(), 0); // leading remainder
        assert_eq!(view.as_slice().start(), 1); // aligned region starts after remainder
    }

    // 3. An evenly-divisible length has an empty remainder either way.
    #[test]
    fn chunks_no_remainder_when_evenly_divisible() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let s = alloc.alloc(9).unwrap();
        assert!(s.as_slice().chunks(3).1.is_empty());
        assert!(s.as_slice().rchunks(3).1.is_empty());
    }

    // 4. chunk_len == 0 panics for both constructors.
    #[test]
    #[should_panic(expected = "chunk_len must be nonzero")]
    fn chunks_zero_chunk_len_panics() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let s = alloc.alloc(4).unwrap();
        let _ = s.as_slice().chunks(0);
    }

    #[test]
    #[should_panic(expected = "chunk_len must be nonzero")]
    fn rchunks_zero_chunk_len_panics() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let s = alloc.alloc(4).unwrap();
        let _ = s.as_slice().rchunks(0);
    }

    // 5. get() returns the correct bytes at each index and None out of bounds.
    #[cfg(feature = "set")]
    #[test]
    fn chunk_get_by_index() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let mut s = alloc.alloc(6).unwrap();
        s.as_slice_mut().write([1u8, 2, 3, 4, 5, 6]).unwrap();
        let (view, _rem) = s.as_slice().chunks(2);
        assert_eq!(view.get(0).unwrap().read().unwrap(), [1, 2]);
        assert_eq!(view.get(1).unwrap().read().unwrap(), [3, 4]);
        assert_eq!(view.get(2).unwrap().read().unwrap(), [5, 6]);
        assert!(view.get(3).is_none());
    }

    // 6. iter() yields chunks in order without reading anything until asked.
    #[cfg(feature = "set")]
    #[test]
    fn chunk_iter_forward() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let mut s = alloc.alloc(6).unwrap();
        s.as_slice_mut().write([1u8, 2, 3, 4, 5, 6]).unwrap();
        let (view, _rem) = s.as_slice().chunks(2);
        let collected: Vec<Vec<u8>> = view.iter().map(|c| c.read().unwrap()).collect();
        assert_eq!(collected, vec![vec![1, 2], vec![3, 4], vec![5, 6]]);
    }

    // 7. The iterator is double-ended and exact-sized.
    #[cfg(feature = "set")]
    #[test]
    fn chunk_iter_double_ended_and_sized() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let mut s = alloc.alloc(6).unwrap();
        s.as_slice_mut().write([1u8, 2, 3, 4, 5, 6]).unwrap();
        let (view, _rem) = s.as_slice().chunks(2);
        let mut it = view.iter();
        assert_eq!(it.len(), 3);
        assert_eq!(it.next().unwrap().read().unwrap(), [1, 2]);
        assert_eq!(it.next_back().unwrap().read().unwrap(), [5, 6]);
        assert_eq!(it.len(), 1);
        assert_eq!(it.next().unwrap().read().unwrap(), [3, 4]);
        assert!(it.next().is_none());
        assert!(it.next_back().is_none());
    }

    // 8. BStackChunk is usable directly in a `for` loop, by value and by
    //    reference, via IntoIterator.
    #[cfg(feature = "set")]
    #[test]
    fn chunk_into_iterator() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let mut s = alloc.alloc(4).unwrap();
        s.as_slice_mut().write([1u8, 2, 3, 4]).unwrap();
        let (view, _rem) = s.as_slice().chunks(2);
        let mut total = 0u32;
        for chunk in &view {
            total += chunk.read().unwrap().iter().map(|&b| b as u32).sum::<u32>();
        }
        assert_eq!(total, 10);
        let mut count = 0;
        for _chunk in view {
            count += 1;
        }
        assert_eq!(count, 2);
    }

    // 9. BStackOwnedSlice::chunks/rchunks mirror BStackSlice's.
    #[cfg(feature = "set")]
    #[test]
    fn owned_slice_chunks_mirror() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let mut s = alloc.alloc(6).unwrap();
        s.write([1u8, 2, 3, 4, 5, 6]).unwrap();
        assert_eq!(s.chunks(2).0.chunk_count(), 3);
        assert_eq!(s.rchunks(4).1.len(), 2);
    }

    // ---- BStackChunk: raw construction ---------------------------------------

    // 10. from_raw_parts builds a view matching what chunks() would produce
    // from the same coordinates.
    #[test]
    fn from_raw_parts_matches_chunks() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let s = alloc.alloc(9).unwrap();
        let view = unsafe { BStackChunk::from_raw_parts(alloc.stack(), s.start(), 9, 3) };
        assert_eq!(view.chunk_len(), 3);
        assert_eq!(view.chunk_count(), 3);
        assert_eq!(view.as_slice(), s.as_slice());
        assert_eq!(view, s.as_slice().chunks(3).0);
    }

    // 11. from_raw_slice builds a view matching what chunks() would produce
    // from the same slice.
    #[test]
    fn from_raw_slice_matches_chunks() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let s = alloc.alloc(9).unwrap();
        let view = unsafe { BStackChunk::from_raw_slice(s.as_slice(), 3) };
        assert_eq!(view, s.as_slice().chunks(3).0);
    }

    // 12. from_slice succeeds when the slice length is an exact, nonzero
    // multiple of chunk_len, and reads back correctly.
    #[cfg(feature = "set")]
    #[test]
    fn from_slice_valid() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let mut s = alloc.alloc(6).unwrap();
        s.as_slice_mut().write([1u8, 2, 3, 4, 5, 6]).unwrap();
        let view = BStackChunk::from_slice(s.as_slice(), 2).unwrap();
        assert_eq!(view.chunk_count(), 3);
        assert_eq!(view.get(1).unwrap().read().unwrap(), [3, 4]);
    }

    // 13. from_slice rejects a chunk_len that doesn't evenly divide the
    // slice's length.
    #[test]
    fn from_slice_rejects_uneven_length() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let s = alloc.alloc(10).unwrap();
        assert!(BStackChunk::from_slice(s.as_slice(), 3).is_none());
    }

    // 14. from_slice rejects chunk_len == 0, even for an empty slice.
    #[test]
    fn from_slice_rejects_zero_chunk_len() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let s = alloc.alloc(4).unwrap();
        assert!(BStackChunk::from_slice(s.as_slice(), 0).is_none());
        assert!(BStackChunk::from_slice(BStackSlice::empty(alloc.stack()), 0).is_none());
    }

    // 15. from_slice accepts a zero-length slice with any nonzero chunk_len.
    #[test]
    fn from_slice_accepts_empty_slice() {
        let (alloc, _path) = mk_alloc();
        let view = BStackChunk::from_slice(BStackSlice::empty(alloc.stack()), 4).unwrap();
        assert!(view.is_empty());
        assert_eq!(view.chunk_count(), 0);
    }

    // ---- BStackChunk: same_stride / same_phase / adjacent_to / overlaps -----

    #[test]
    fn chunk_same_stride_and_same_phase() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let s = alloc.alloc(16).unwrap(); // 0..16
        let (a, _) = s.as_slice().chunks(4);
        let (b, _) = s.as_slice().chunks(4);
        let (c, _) = s.as_slice().chunks(8);
        assert!(a.same_stride(&b));
        assert!(a.same_phase(&b));
        assert!(!a.same_stride(&c));
        assert!(!a.same_phase(&c));
    }

    #[test]
    fn chunk_same_phase_checks_offset_congruence() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let s = alloc.alloc(16).unwrap(); // 0..16
        let full = s.as_slice();
        let (aligned_at_0, _) = full.subslice(0, 8).chunks(4); // aligned start 0, phase 0
        let (aligned_at_4, _) = full.subslice(4, 12).chunks(4); // aligned start 4, phase 0
        let (aligned_at_2, _) = full.subslice(2, 10).chunks(4); // aligned start 2, phase 2
        assert!(aligned_at_0.same_phase(&aligned_at_4));
        assert!(!aligned_at_0.same_phase(&aligned_at_2));
    }

    #[test]
    fn chunk_adjacent_to_touching_same_phase_views() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let a = alloc.alloc(8).unwrap(); // 0..8
        let b = alloc.alloc(8).unwrap(); // 8..16
        let (chunk_a, _) = a.as_slice().chunks(4);
        let (chunk_b, _) = b.as_slice().chunks(4);
        assert!(chunk_a.adjacent_to(&chunk_b));
        assert!(!chunk_a.overlaps(&chunk_b));
    }

    #[test]
    fn chunk_overlaps_same_phase_views() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let a = alloc.alloc(8).unwrap(); // 0..8
        let (chunk_a, _) = a.as_slice().chunks(4); // aligned 0..8, phase 0
        let (chunk_tail, _) = a.as_slice().subslice(4, 8).chunks(4); // aligned 4..8, phase 0
        assert!(chunk_a.overlaps(&chunk_tail));
        assert!(!chunk_a.adjacent_to(&chunk_tail));
    }

    // Byte ranges overlapping isn't enough on its own: a phase mismatch must
    // still suppress both adjacent_to and overlaps.
    #[test]
    fn chunk_overlapping_bytes_but_different_phase_is_neither() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let a = alloc.alloc(8).unwrap(); // 0..8
        let (chunk_a, _) = a.as_slice().chunks(4); // aligned 0..8, phase 0
        let (chunk_shifted, _) = a.as_slice().subslice(2, 8).chunks(4); // aligned 2..6, phase 2
        assert!(chunk_a.as_slice().overlaps(&chunk_shifted.as_slice())); // raw byte ranges do overlap
        assert!(!chunk_a.overlaps(&chunk_shifted));
        assert!(!chunk_a.adjacent_to(&chunk_shifted));
    }

    // ---- BStackChunk: merge / merge_adjacent ---------------------------------

    #[test]
    fn chunk_merge_overlapping_same_phase() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let a = alloc.alloc(8).unwrap(); // 0..8
        let (chunk_a, _) = a.as_slice().chunks(4); // aligned 0..8, phase 0
        let (chunk_tail, _) = a.as_slice().subslice(4, 8).chunks(4); // aligned 4..8, phase 0
        let merged = chunk_a.merge(&chunk_tail).unwrap();
        assert_eq!(merged.as_slice().range(), 0..8);
        assert_eq!(merged.chunk_len(), 4);
    }

    #[test]
    fn chunk_merge_different_phase_overlap_returns_none() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let a = alloc.alloc(8).unwrap(); // 0..8
        let (chunk_a, _) = a.as_slice().chunks(4); // aligned 0..8, phase 0
        let (chunk_shifted, _) = a.as_slice().subslice(2, 8).chunks(4); // aligned 2..6, phase 2
        assert!(chunk_a.merge(&chunk_shifted).is_none());
    }

    #[test]
    fn chunk_merge_different_stride_returns_none() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let a = alloc.alloc(8).unwrap();
        let (chunk_a, _) = a.as_slice().chunks(4);
        let (chunk_b, _) = a.as_slice().chunks(2);
        assert!(chunk_a.merge(&chunk_b).is_none());
    }

    #[test]
    fn chunk_merge_empty_is_identity_regardless_of_phase() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let a = alloc.alloc(8).unwrap(); // 0..8
        let (chunk_a, _) = a.as_slice().chunks(4); // aligned 0..8, phase 0
        // An empty chunk built from an odd offset lands on a different phase.
        let (empty_chunk, _) = a.as_slice().subslice(1, 1).chunks(4);
        assert!(empty_chunk.is_empty());
        assert!(!empty_chunk.same_phase(&chunk_a));
        assert_eq!(chunk_a.merge(&empty_chunk).unwrap(), chunk_a);
        assert_eq!(empty_chunk.merge(&chunk_a).unwrap(), chunk_a);
    }

    #[test]
    fn chunk_merge_adjacent_touching_same_phase() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let a = alloc.alloc(8).unwrap(); // 0..8
        let b = alloc.alloc(8).unwrap(); // 8..16
        let (chunk_a, _) = a.as_slice().chunks(4);
        let (chunk_b, _) = b.as_slice().chunks(4);
        let merged = chunk_a.merge_adjacent(&chunk_b).unwrap();
        assert_eq!(merged.as_slice().range(), 0..16);
        assert_eq!(merged.chunk_len(), 4);
        // merge() alone doesn't cover the touching-but-non-overlapping case.
        assert!(chunk_a.merge(&chunk_b).is_none());
    }

    // Byte-adjacency alone is not enough: BStackSlice::merge_adjacent knows
    // nothing about stride, so without the same_phase (same_stride) guard a
    // byte-adjacent pair of differently-strided chunks would merge and
    // silently keep self's chunk_len, discarding other's.
    #[test]
    fn chunk_merge_adjacent_rejects_different_stride() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let a = alloc.alloc(8).unwrap(); // 0..8
        let b = alloc.alloc(6).unwrap(); // 8..14
        let (chunk_a, _) = a.as_slice().chunks(4); // aligned 0..8, stride 4
        let (chunk_b, _) = b.as_slice().chunks(3); // aligned 8..14, stride 3
        assert!(chunk_a.as_slice().adjacent_to(&chunk_b.as_slice())); // byte-adjacent
        assert!(chunk_a.merge_adjacent(&chunk_b).is_none()); // but strides differ
    }

    #[test]
    fn chunk_merge_adjacent_rejects_overlap_gap_and_empty() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let a = alloc.alloc(8).unwrap(); // 0..8
        let _b = alloc.alloc(4).unwrap(); // 8..12, leaves a gap before c
        let c = alloc.alloc(4).unwrap(); // 12..16
        let (chunk_a, _) = a.as_slice().chunks(4);
        let (chunk_tail, _) = a.as_slice().subslice(4, 8).chunks(4); // overlaps chunk_a
        let (chunk_c, _) = c.as_slice().chunks(4); // same phase, but gapped by _b
        let (empty_chunk, _) = a.as_slice().subslice(0, 0).chunks(4);

        assert!(chunk_a.merge_adjacent(&chunk_tail).is_none()); // overlap, not touch
        assert!(chunk_a.merge_adjacent(&chunk_c).is_none()); // gap
        assert!(chunk_a.merge_adjacent(&empty_chunk).is_none()); // empty operand
    }

    // ---- BStackChunk: sort / search / select ---------------------------------

    // 10. sort_by reorders whole 3-byte records by their first byte, leaving
    //     each record's remaining bytes attached and the remainder untouched.
    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn chunk_sort_by_orders_records() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let mut s = alloc.alloc(10).unwrap(); // 3 records of 3 bytes + 1 remainder byte
        s.as_slice_mut()
            .write([3u8, b'c', b'C', 1, b'a', b'A', 2, b'b', b'B', 0xFF])
            .unwrap();
        let (mut view, _rem) = s.as_slice().chunks(3);
        view.sort_by(|a, b| a[0].cmp(&b[0])).unwrap();
        let sorted = s.as_slice().read().unwrap();
        assert_eq!(sorted, [1, b'a', b'A', 2, b'b', b'B', 3, b'c', b'C', 0xFF]);
    }

    // 11. sort_by_key: same as sort_by but keyed, and stable on equal keys.
    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn chunk_sort_by_key_is_stable() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let mut s = alloc.alloc(8).unwrap(); // 4 records of 2 bytes: key, tag
        s.as_slice_mut()
            .write([1u8, b'x', 0, b'a', 1, b'y', 0, b'b'])
            .unwrap();
        let (mut view, _rem) = s.as_slice().chunks(2);
        view.sort_by_key(|c| c[0]).unwrap();
        let sorted = s.as_slice().read().unwrap();
        // Both key-0 records keep their relative order (a before b), likewise key-1.
        assert_eq!(sorted, [0, b'a', 0, b'b', 1, b'x', 1, b'y']);
    }

    // 12. binary_search_by finds a present chunk and reports an insertion
    //     point for an absent one, over already-ordered chunks.
    #[cfg(feature = "set")]
    #[test]
    fn chunk_binary_search_by_found_and_not_found() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let mut s = alloc.alloc(8).unwrap(); // 4 ordered 2-byte records: keys 1,3,5,7
        s.as_slice_mut().write([1u8, 0, 3, 0, 5, 0, 7, 0]).unwrap();
        let (view, _rem) = s.as_slice().chunks(2);
        assert_eq!(view.binary_search_by(|c| c[0].cmp(&5)).unwrap(), Ok(2));
        assert_eq!(view.binary_search_by(|c| c[0].cmp(&4)).unwrap(), Err(2));
        assert_eq!(view.binary_search_by(|c| c[0].cmp(&0)).unwrap(), Err(0));
        assert_eq!(view.binary_search_by(|c| c[0].cmp(&8)).unwrap(), Err(4));
    }

    // 13. binary_search_by_key delegates to binary_search_by via a key fn.
    #[cfg(feature = "set")]
    #[test]
    fn chunk_binary_search_by_key_works() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let mut s = alloc.alloc(6).unwrap();
        s.as_slice_mut().write([2u8, 4, 6, 8, 10, 12]).unwrap();
        let (view, _rem) = s.as_slice().chunks(2);
        assert_eq!(view.binary_search_by_key(&6, |c| c[0]).unwrap(), Ok(1));
        assert_eq!(view.binary_search_by_key(&7, |c| c[0]).unwrap(), Err(2));
    }

    // binary_search_by stays correct — both exact matches and insertion
    // points — across a region much larger than a single chunk.
    #[cfg(feature = "set")]
    #[test]
    fn chunk_binary_search_by_large_region() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let count: usize = 200;
        let mut data = vec![0u8; count * 8];
        for i in 0..count {
            data[i * 8..(i + 1) * 8].copy_from_slice(&((i as u64) * 2).to_le_bytes());
        }
        let mut s = alloc.alloc(data.len() as u64).unwrap();
        s.as_slice_mut().write(&data).unwrap();
        let (view, _rem) = s.as_slice().chunks(8);

        let key = |c: &[u8]| u64::from_le_bytes(c.try_into().unwrap());
        for i in (0..count as u64).step_by(4) {
            assert_eq!(
                view.binary_search_by(|c| key(c).cmp(&(i * 2))).unwrap(),
                Ok(i)
            );
            assert_eq!(
                view.binary_search_by(|c| key(c).cmp(&(i * 2 + 1))).unwrap(),
                Err(i + 1)
            );
        }
    }

    // 14. select_nth_by places the nth chunk where it would land in a full
    //     sort; every chunk before it compares <=, every chunk after >=.
    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn chunk_select_nth_by_partitions() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let mut s = alloc.alloc(5).unwrap(); // 5 single-byte records
        s.as_slice_mut().write([5u8, 1, 4, 2, 3]).unwrap();
        let (mut view, _rem) = s.as_slice().chunks(1);
        view.select_nth_by(2, |a, b| a[0].cmp(&b[0])).unwrap();
        let buf = s.as_slice().read().unwrap();
        assert_eq!(buf[2], 3); // the median of 1..=5 lands at index 2
        assert!(buf[..2].iter().all(|&v| v <= 3));
        assert!(buf[3..].iter().all(|&v| v >= 3));
    }

    // 15. select_nth_by_key mirrors select_nth_by with a key function.
    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn chunk_select_nth_by_key_partitions() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let mut s = alloc.alloc(5).unwrap();
        s.as_slice_mut().write([5u8, 1, 4, 2, 3]).unwrap();
        let (mut view, _rem) = s.as_slice().chunks(1);
        view.select_nth_by_key(2, |c| c[0]).unwrap();
        let buf = s.as_slice().read().unwrap();
        assert_eq!(buf[2], 3);
    }

    // 16. select_nth_by panics when n is out of bounds.
    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    #[should_panic(expected = "n must be < chunk_count")]
    fn chunk_select_nth_by_out_of_bounds_panics() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let s = alloc.alloc(3).unwrap();
        let (mut view, _rem) = s.as_slice().chunks(1);
        view.select_nth_by(3, |a, b| a.cmp(b)).unwrap();
    }

    // 17. sort_by correctly applies a permutation containing multiple
    //     disjoint cycles (a 2-cycle and a 4-cycle), exercising the
    //     in-place cycle-following permutation logic beyond a single cycle.
    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn chunk_sort_by_multiple_cycles() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let mut s = alloc.alloc(6).unwrap();
        s.as_slice_mut().write([5u8, 3, 1, 4, 2, 0]).unwrap();
        let (mut view, _rem) = s.as_slice().chunks(1);
        view.sort_by(|a, b| a.cmp(b)).unwrap();
        assert_eq!(s.as_slice().read().unwrap(), [0, 1, 2, 3, 4, 5]);
    }

    // 18. into_slice() consumes the view and returns the aligned region.
    #[test]
    fn chunk_into_slice_returns_aligned_region() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let s = alloc.alloc(10).unwrap();
        let (view, _rem) = s.as_slice().chunks(3);
        let aligned = view.into_slice();
        assert_eq!(aligned.start(), 0);
        assert_eq!(aligned.len(), 9);
    }

    // 19. with_stride() re-divides the aligned region with a new stride.
    #[test]
    fn chunk_with_stride_rechunks_aligned_region() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let s = alloc.alloc(10).unwrap();
        let (view, _rem) = s.as_slice().chunks(3); // aligned: 9 bytes, 1-byte remainder
        let (restrided, new_rem) = view.with_stride(4);
        assert_eq!(restrided.chunk_len(), 4);
        assert_eq!(restrided.chunk_count(), 2); // 9 / 4 = 2, with 1 byte left
        assert_eq!(new_rem.len(), 1);
    }

    // 20. PartialEq/Eq: same underlying region and same stride compare
    //     equal; a different stride or a different region does not.
    #[test]
    fn chunk_partial_eq_requires_same_slice_and_stride() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let s = alloc.alloc(8).unwrap();
        let (a, _) = s.as_slice().chunks(2);
        let (b, _) = s.as_slice().chunks(2);
        assert_eq!(a, b); // same region, same stride
        let (c, _) = s.as_slice().chunks(4);
        assert_ne!(a, c); // same region, different stride
        let other = alloc.alloc(8).unwrap();
        let (d, _) = other.as_slice().chunks(2);
        assert_ne!(a, d); // different region, same stride
    }

    // 21. PartialOrd/Ord: ordered first by stride, then by the underlying
    //     region.
    #[test]
    fn chunk_partial_ord_orders_by_stride_then_slice() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let s = alloc.alloc(8).unwrap();
        let (small_stride, _) = s.as_slice().chunks(2);
        let (large_stride, _) = s.as_slice().chunks(4);
        assert!(small_stride < large_stride); // stride 2 < stride 4, regardless of region

        let first = alloc.alloc(4).unwrap();
        let second = alloc.alloc(4).unwrap();
        let (first_view, _) = first.as_slice().chunks(2);
        let (second_view, _) = second.as_slice().chunks(2);
        assert!(first_view < second_view); // same stride, earlier offset sorts first
    }

    // 22. size_hint()/len() must clamp a u64 chunk count to usize::MAX rather
    //     than silently truncating it when usize is narrower than u64 (32-bit
    //     targets). The slice is constructed with a coordinate far beyond the
    //     real (tiny) backing file — safe because no I/O is performed here,
    //     only arithmetic on the coordinate.
    #[test]
    fn chunk_iter_size_hint_clamps_to_usize_max() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let s = alloc.alloc(0).unwrap();
        let huge_len = (usize::MAX as u64).saturating_add(2);
        let huge_slice = unsafe { BStackSlice::from_raw_parts(alloc.stack(), s.start(), huge_len) };
        let (view, _rem) = huge_slice.chunks(1);
        // The true chunk count (u64, unclamped) can exceed usize::MAX.
        assert!(view.chunk_count() >= usize::MAX as u64);
        let iter = view.iter();
        assert_eq!(iter.size_hint(), (usize::MAX, Some(usize::MAX)));
        assert_eq!(iter.len(), usize::MAX);
    }

    // Build `keys.len()` records of `rec_len` bytes each; record i carries
    // `keys[i]` as a little-endian u16 in its first two bytes and zeros
    // elsewhere. With rec_len == 512 the sort's block is 2048/512 == 4 records
    // and run0 == 12 records, so a few dozen records force real multi-pass
    // merges (tail-order + cross_exchange block swaps + carry passes).
    #[cfg(all(feature = "set", feature = "atomic"))]
    fn keyed_records(keys: &[u16], rec_len: usize) -> Vec<u8> {
        let mut data = vec![0u8; keys.len() * rec_len];
        for (i, &k) in keys.iter().enumerate() {
            data[i * rec_len..i * rec_len + 2].copy_from_slice(&k.to_le_bytes());
        }
        data
    }

    // Read back the per-record u16 keys after a sort over `rec_len`-byte records.
    #[cfg(all(feature = "set", feature = "atomic"))]
    fn read_keys(bytes: &[u8], rec_len: usize, n: usize) -> Vec<u16> {
        (0..n)
            .map(|i| u16::from_le_bytes([bytes[i * rec_len], bytes[i * rec_len + 1]]))
            .collect()
    }

    // 23. A region that fits the budget is fully sorted in one process pass.
    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn chunk_sort_partial_by_small_region_is_full_sort() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let mut s = alloc.alloc(6).unwrap();
        s.as_slice_mut().write([5u8, 3, 1, 6, 4, 2]).unwrap();
        let (mut view, _rem) = s.as_slice().chunks(1);
        view.sort_partial_by(|a, b| a[0].cmp(&b[0])).unwrap();
        assert_eq!(s.as_slice().read().unwrap(), [1, 2, 3, 4, 5, 6]);
    }

    // 24. A region far larger than the budget is fully (globally) sorted via
    //     multi-pass in-place merges, and the multiset is preserved.
    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn chunk_sort_partial_by_multipass_fully_sorts() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        // 30 records of 512 bytes: block=4, run0=12 -> several merge passes,
        // and 30 % 4 == 2 exercises the ragged imerge fixup.
        let keys: Vec<u16> = (0..30u16).map(|i| (i * 7 + 3) % 30).collect();
        let data = keyed_records(&keys, 512);
        let mut s = alloc.alloc(data.len() as u64).unwrap();
        s.as_slice_mut().write(&data).unwrap();
        let (mut view, _rem) = s.as_slice().chunks(512);
        view.sort_partial_by(|a, b| {
            u16::from_le_bytes([a[0], a[1]]).cmp(&u16::from_le_bytes([b[0], b[1]]))
        })
        .unwrap();
        let got = read_keys(&s.as_slice().read().unwrap(), 512, 30);
        let mut want = keys.clone();
        want.sort_unstable();
        assert_eq!(got, want);
    }

    // 25. A record count that is not a multiple of the block size is still
    //     fully sorted (drives the ragged sub-K imerge tail merge).
    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn chunk_sort_partial_by_ragged_count_fully_sorts() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        // 13 records of 512 bytes: just over run0 (12); 13 % 4 == 1.
        let keys: Vec<u16> = [9, 2, 13, 4, 11, 6, 1, 8, 3, 12, 5, 10, 7]
            .iter()
            .map(|&x| x as u16)
            .collect();
        let data = keyed_records(&keys, 512);
        let mut s = alloc.alloc(data.len() as u64).unwrap();
        s.as_slice_mut().write(&data).unwrap();
        let (mut view, _rem) = s.as_slice().chunks(512);
        view.sort_partial_by(|a, b| {
            u16::from_le_bytes([a[0], a[1]]).cmp(&u16::from_le_bytes([b[0], b[1]]))
        })
        .unwrap();
        let got = read_keys(&s.as_slice().read().unwrap(), 512, 13);
        let mut want = keys.clone();
        want.sort_unstable();
        assert_eq!(got, want);
    }

    // 26. sort_partial_by_key over a multi-pass region sorts fully by key.
    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn chunk_sort_partial_by_key_multipass_fully_sorts() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let keys: Vec<u16> = (0..25u16).map(|i| (i * 11 + 1) % 25).collect();
        let data = keyed_records(&keys, 512);
        let mut s = alloc.alloc(data.len() as u64).unwrap();
        s.as_slice_mut().write(&data).unwrap();
        let (mut view, _rem) = s.as_slice().chunks(512);
        view.sort_partial_by_key(|c| u16::from_le_bytes([c[0], c[1]]))
            .unwrap();
        let got = read_keys(&s.as_slice().read().unwrap(), 512, 25);
        let mut want = keys.clone();
        want.sort_unstable();
        assert_eq!(got, want);
    }

    // 27. Empty and single-record views are no-ops that still succeed.
    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn chunk_sort_partial_by_trivial_views_are_noops() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let mut s = alloc.alloc(1).unwrap();
        s.as_slice_mut().write([7u8]).unwrap();
        let (mut one, _rem) = s.as_slice().chunks(1);
        one.sort_partial_by(|a, b| a[0].cmp(&b[0])).unwrap();
        assert_eq!(s.as_slice().read().unwrap(), [7]);

        let empty = alloc.alloc(0).unwrap();
        let (mut ev, _rem) = empty.as_slice().chunks(1);
        ev.sort_partial_by(|a, b| a[0].cmp(&b[0])).unwrap();
    }

    // 28. Deterministic property test: over many sizes (including ragged and
    //     multi-pass), the out-of-core sort produces a fully sorted permutation
    //     of the input. Seeded LCG, run by name — not the fuzz harness.
    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn chunk_sort_partial_by_property_sorts_all_sizes() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let mut state: u64 = 0x9E3779B97F4A7C15;
        let mut next = || {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
            (state >> 33) as u16
        };
        for &n in &[
            0usize, 1, 2, 5, 11, 12, 13, 24, 25, 26, 27, 28, 29, 30, 48, 50,
        ] {
            let keys: Vec<u16> = (0..n).map(|_| next() % 1000).collect();
            let data = keyed_records(&keys, 512);
            let mut s = alloc.alloc((n * 512).max(1) as u64).unwrap();
            if n > 0 {
                s.as_slice_mut().write(&data).unwrap();
            }
            let (mut view, _rem) = s.as_slice().chunks(512);
            view.sort_partial_by(|a, b| {
                u16::from_le_bytes([a[0], a[1]]).cmp(&u16::from_le_bytes([b[0], b[1]]))
            })
            .unwrap();
            if n > 0 {
                let got = read_keys(&s.as_slice().read().unwrap()[..n * 512], 512, n);
                let mut want = keys.clone();
                want.sort_unstable();
                assert_eq!(got, want, "n={n}");
            }
            alloc.dealloc(s).unwrap();
        }
    }

    // Assert that `out` is a valid select_nth result for rank `n`: the n-th
    // record equals the n-th of the sorted input, everything before it is <=,
    // everything after is >=, and the whole thing is a permutation of the input.
    #[cfg(all(feature = "set", feature = "atomic"))]
    fn assert_selected(out: &[u16], n: usize, sorted: &[u16]) {
        assert_eq!(out[n], sorted[n], "nth value wrong at n={n}");
        assert!(out[..n].iter().all(|&x| x <= out[n]), "left side at n={n}");
        assert!(
            out[n + 1..].iter().all(|&x| x >= out[n]),
            "right side at n={n}"
        );
        let mut perm = out.to_vec();
        perm.sort_unstable();
        assert_eq!(&perm, sorted, "not a permutation at n={n}");
    }

    // 29. A region that fits the budget selects correctly in one pass.
    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn chunk_select_nth_partial_by_small_region() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let mut s = alloc.alloc(5).unwrap();
        s.as_slice_mut().write([5u8, 1, 4, 2, 3]).unwrap();
        let (mut view, _rem) = s.as_slice().chunks(1);
        view.select_nth_partial_by(2, |a, b| a[0].cmp(&b[0]))
            .unwrap();
        let buf = s.as_slice().read().unwrap();
        assert_eq!(buf[2], 3); // median of 1..=5
        assert!(buf[..2].iter().all(|&v| v <= 3));
        assert!(buf[3..].iter().all(|&v| v >= 3));
    }

    // 30. A region far larger than the budget selects correctly out-of-core
    //     (multiple partition rounds), for several ranks.
    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn chunk_select_nth_partial_by_multipass() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        // 40 records of 512 bytes: budget holds 12, so selection needs several
        // out-of-core partition rounds. Keys include duplicates.
        let keys: Vec<u16> = (0..40u16).map(|i| (i * 13 + 5) % 17).collect();
        let mut sorted = keys.clone();
        sorted.sort_unstable();
        for &n in &[0usize, 1, 19, 20, 39] {
            let data = keyed_records(&keys, 512);
            let mut s = alloc.alloc(data.len() as u64).unwrap();
            s.as_slice_mut().write(&data).unwrap();
            let (mut view, _rem) = s.as_slice().chunks(512);
            view.select_nth_partial_by(n as u64, |a, b| {
                u16::from_le_bytes([a[0], a[1]]).cmp(&u16::from_le_bytes([b[0], b[1]]))
            })
            .unwrap();
            let got = read_keys(&s.as_slice().read().unwrap(), 512, 40);
            assert_selected(&got, n, &sorted);
            alloc.dealloc(s).unwrap();
        }
    }

    // 31. select_nth_partial_by_key over a multi-round region.
    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn chunk_select_nth_partial_by_key_multipass() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let keys: Vec<u16> = (0..33u16).map(|i| (i * 7 + 1) % 33).collect();
        let mut sorted = keys.clone();
        sorted.sort_unstable();
        let data = keyed_records(&keys, 512);
        let mut s = alloc.alloc(data.len() as u64).unwrap();
        s.as_slice_mut().write(&data).unwrap();
        let (mut view, _rem) = s.as_slice().chunks(512);
        view.select_nth_partial_by_key(16, |c| u16::from_le_bytes([c[0], c[1]]))
            .unwrap();
        let got = read_keys(&s.as_slice().read().unwrap(), 512, 33);
        assert_selected(&got, 16, &sorted);
    }

    // 32. select_nth_partial_by panics when n is out of bounds.
    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    #[should_panic(expected = "n must be < chunk_count")]
    fn chunk_select_nth_partial_by_out_of_bounds_panics() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let s = alloc.alloc(3).unwrap();
        let (mut view, _rem) = s.as_slice().chunks(1);
        view.select_nth_partial_by(3, |a, b| a.cmp(b)).unwrap();
    }

    // 33. Deterministic property test: over many sizes and ranks (including
    //     duplicates and ragged counts), the out-of-core selection matches the
    //     sorted n-th and partitions correctly. Seeded LCG, run by name.
    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn chunk_select_nth_partial_by_property() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let mut state: u64 = 0xD1B54A32D192ED03;
        let mut next = || {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
            (state >> 33) as u16
        };
        for &sz in &[1usize, 2, 12, 13, 25, 37, 50] {
            let keys: Vec<u16> = (0..sz).map(|_| next() % 100).collect(); // many dups
            let mut sorted = keys.clone();
            sorted.sort_unstable();
            // A spread of ranks: ends, middle, and a pseudo-random one.
            let ranks = [
                0,
                sz / 2,
                sz.saturating_sub(1),
                (next() as usize) % sz.max(1),
            ];
            for &n in ranks.iter() {
                let data = keyed_records(&keys, 512);
                let mut s = alloc.alloc((sz * 512) as u64).unwrap();
                s.as_slice_mut().write(&data).unwrap();
                let (mut view, _rem) = s.as_slice().chunks(512);
                view.select_nth_partial_by(n as u64, |a, b| {
                    u16::from_le_bytes([a[0], a[1]]).cmp(&u16::from_le_bytes([b[0], b[1]]))
                })
                .unwrap();
                let got = read_keys(&s.as_slice().read().unwrap(), 512, sz);
                assert_selected(&got, n, &sorted);
                alloc.dealloc(s).unwrap();
            }
        }
    }

    // 34. Records wider than the sort/select budget (8192 > 6144) must not
    //     overflow the pivot-sample buffer; selection still works out-of-core.
    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn chunk_select_nth_partial_by_record_wider_than_budget() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let keys: Vec<u16> = [4u16, 1, 3, 0, 2].to_vec();
        let mut sorted = keys.clone();
        sorted.sort_unstable();
        let data = keyed_records(&keys, 8192); // 8192 > budget (6144)
        let mut s = alloc.alloc(data.len() as u64).unwrap();
        s.as_slice_mut().write(&data).unwrap();
        let (mut view, _rem) = s.as_slice().chunks(8192);
        view.select_nth_partial_by(2, |a, b| {
            u16::from_le_bytes([a[0], a[1]]).cmp(&u16::from_le_bytes([b[0], b[1]]))
        })
        .unwrap();
        let got = read_keys(&s.as_slice().read().unwrap(), 8192, 5);
        assert_selected(&got, 2, &sorted);
    }

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn chunk_swap_exchanges_two_chunks() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let mut s = alloc.alloc(6).unwrap(); // 3 records of 2 bytes
        s.as_slice_mut().write([1u8, 1, 2, 2, 3, 3]).unwrap();
        let (mut view, _rem) = s.as_slice().chunks(2);
        view.swap(0, 2).unwrap();
        assert_eq!(s.as_slice().read().unwrap(), [3, 3, 2, 2, 1, 1]);
    }

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn chunk_swap_same_index_is_noop() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let mut s = alloc.alloc(4).unwrap();
        s.as_slice_mut().write([1u8, 2, 3, 4]).unwrap();
        let (mut view, _rem) = s.as_slice().chunks(2);
        view.swap(1, 1).unwrap();
        assert_eq!(s.as_slice().read().unwrap(), [1, 2, 3, 4]);
    }

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    #[should_panic(expected = "swap: i must be < chunk_count")]
    fn chunk_swap_out_of_bounds_panics() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let s = alloc.alloc(4).unwrap();
        let (mut view, _rem) = s.as_slice().chunks(2);
        view.swap(2, 0).unwrap();
    }

    // Whole records move as units; the (key, tag) pairing within each record
    // is preserved rather than the bytes themselves being reversed.
    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn chunk_reverse_reverses_chunk_order() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let mut s = alloc.alloc(8).unwrap(); // 4 records of 2 bytes: [key, tag]
        s.as_slice_mut()
            .write([1u8, b'a', 2, b'b', 3, b'c', 4, b'd'])
            .unwrap();
        let (mut view, _rem) = s.as_slice().chunks(2);
        view.reverse().unwrap();
        assert_eq!(
            s.as_slice().read().unwrap(),
            [4, b'd', 3, b'c', 2, b'b', 1, b'a']
        );
    }

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn chunk_reverse_odd_chunk_count_leaves_middle_in_place() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let mut s = alloc.alloc(5).unwrap();
        s.as_slice_mut().write([1u8, 2, 3, 4, 5]).unwrap();
        let (mut view, _rem) = s.as_slice().chunks(1);
        view.reverse().unwrap();
        assert_eq!(s.as_slice().read().unwrap(), [5, 4, 3, 2, 1]);
    }

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn chunk_rotate_left_moves_chunks_to_front() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let mut s = alloc.alloc(8).unwrap(); // 4 records of 2 bytes
        s.as_slice_mut()
            .write([1u8, b'a', 2, b'b', 3, b'c', 4, b'd'])
            .unwrap();
        let (mut view, _rem) = s.as_slice().chunks(2);
        view.rotate_left(1).unwrap();
        assert_eq!(
            s.as_slice().read().unwrap(),
            [2, b'b', 3, b'c', 4, b'd', 1, b'a']
        );
    }

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn chunk_rotate_right_moves_chunks_to_back() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let mut s = alloc.alloc(8).unwrap();
        s.as_slice_mut()
            .write([1u8, b'a', 2, b'b', 3, b'c', 4, b'd'])
            .unwrap();
        let (mut view, _rem) = s.as_slice().chunks(2);
        view.rotate_right(1).unwrap();
        assert_eq!(
            s.as_slice().read().unwrap(),
            [4, b'd', 1, b'a', 2, b'b', 3, b'c']
        );
    }

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    #[should_panic(expected = "rotate_left: k must be <= chunk_count")]
    fn chunk_rotate_left_panics_when_k_exceeds_count() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let s = alloc.alloc(4).unwrap();
        let (mut view, _rem) = s.as_slice().chunks(2);
        view.rotate_left(3).unwrap();
    }

    #[cfg(feature = "set")]
    #[test]
    fn chunk_fill_repeats_pattern_across_all_chunks() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let mut s = alloc.alloc(10).unwrap(); // 3 records of 3 bytes + 1 remainder byte
        s.as_slice_mut()
            .write([0u8, 0, 0, 0, 0, 0, 0, 0, 0, 0xFF])
            .unwrap();
        let (mut view, _rem) = s.as_slice().chunks(3);
        view.fill(&[7, 8, 9]).unwrap();
        assert_eq!(
            s.as_slice().read().unwrap(),
            [7, 8, 9, 7, 8, 9, 7, 8, 9, 0xFF]
        );
    }

    #[cfg(feature = "set")]
    #[test]
    #[should_panic(expected = "fill: chunk length must equal chunk_len")]
    fn chunk_fill_panics_on_length_mismatch() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let s = alloc.alloc(6).unwrap();
        let (mut view, _rem) = s.as_slice().chunks(3);
        view.fill(&[1, 2]).unwrap();
    }

    #[cfg(feature = "set")]
    #[test]
    fn chunk_set_overwrites_one_chunk() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let mut s = alloc.alloc(6).unwrap(); // 3 records of 2 bytes
        s.as_slice_mut().write([1u8, 1, 2, 2, 3, 3]).unwrap();
        let (mut view, _rem) = s.as_slice().chunks(2);
        view.set(1, &[9, 9]).unwrap();
        assert_eq!(s.as_slice().read().unwrap(), [1, 1, 9, 9, 3, 3]);
    }

    #[cfg(feature = "set")]
    #[test]
    #[should_panic(expected = "set: index must be < chunk_count")]
    fn chunk_set_out_of_bounds_panics() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let s = alloc.alloc(4).unwrap();
        let (mut view, _rem) = s.as_slice().chunks(2);
        view.set(2, &[1, 2]).unwrap();
    }

    // ---- BStackChunk: read-side companions (first/last/split_at/etc.) -------

    #[cfg(feature = "set")]
    #[test]
    fn chunk_first_and_last() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let mut s = alloc.alloc(6).unwrap();
        s.as_slice_mut().write([1u8, 1, 2, 2, 3, 3]).unwrap();
        let (view, _rem) = s.as_slice().chunks(2);
        assert_eq!(view.first().unwrap().read().unwrap(), [1, 1]);
        assert_eq!(view.last().unwrap().read().unwrap(), [3, 3]);
    }

    #[test]
    fn chunk_first_and_last_empty_view() {
        let (alloc, _path) = mk_alloc();
        let view = BStackChunk::from_slice(BStackSlice::empty(alloc.stack()), 4).unwrap();
        assert!(view.first().is_none());
        assert!(view.last().is_none());
    }

    #[test]
    fn chunk_split_at_divides_view() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let s = alloc.alloc(10).unwrap(); // 3 records of 3 bytes + 1 remainder byte
        let (view, _rem) = s.as_slice().chunks(3);
        let (left, right) = view.split_at(1);
        assert_eq!(left.chunk_count(), 1);
        assert_eq!(left.chunk_len(), 3);
        assert_eq!(right.chunk_count(), 2);
        assert_eq!(right.chunk_len(), 3);
        assert_eq!(left.as_slice().range(), 0..3);
        assert_eq!(right.as_slice().range(), 3..9);
    }

    // mid == 0 and mid == chunk_count() are the two boundary splits: one side
    // of the result is empty but both sides stay valid, phase-matching views.
    #[test]
    fn chunk_split_at_boundary_mid_yields_one_empty_side() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let s = alloc.alloc(9).unwrap();
        let (view, _rem) = s.as_slice().chunks(3);

        let (left, right) = view.split_at(0);
        assert!(left.is_empty());
        assert_eq!(left.chunk_len(), 3);
        assert_eq!(right, view);

        let (left, right) = view.split_at(3);
        assert_eq!(left, view);
        assert!(right.is_empty());
        assert_eq!(right.chunk_len(), 3);
    }

    // Splitting an already-empty view is only legal at mid == 0 (its only
    // valid chunk_count()), and yields two more empty views at the same
    // phase as the original — mirrors `[].split_at(0) == (&[], &[])`.
    #[test]
    fn chunk_split_at_empty_view_returns_two_empty_chunks() {
        let (alloc, _path) = mk_alloc();
        let view = BStackChunk::from_slice(BStackSlice::empty(alloc.stack()), 4).unwrap();
        let (left, right) = view.split_at(0);
        assert!(left.is_empty());
        assert!(right.is_empty());
        assert_eq!(left, view);
        assert_eq!(right, view);
    }

    #[test]
    #[should_panic(expected = "split_at: mid must be <= chunk_count")]
    fn chunk_split_at_out_of_bounds_panics() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let s = alloc.alloc(6).unwrap();
        let (view, _rem) = s.as_slice().chunks(2);
        let _ = view.split_at(4);
    }

    // Runs under both the atomic and non-atomic implementations, exercising
    // whichever features select.
    #[cfg(feature = "set")]
    #[test]
    fn chunk_partition_point_finds_boundary() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let mut s = alloc.alloc(5).unwrap();
        s.as_slice_mut().write([1u8, 2, 3, 7, 9]).unwrap();
        let (view, _rem) = s.as_slice().chunks(1);
        let idx = view.partition_point(|c| c[0] < 5).unwrap();
        assert_eq!(idx, 3);
    }

    #[cfg(feature = "set")]
    #[test]
    fn chunk_partition_point_boundary_cases() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let mut s = alloc.alloc(4).unwrap();
        s.as_slice_mut().write([1u8, 2, 3, 4]).unwrap();
        let (view, _rem) = s.as_slice().chunks(1);
        assert_eq!(view.partition_point(|_| true).unwrap(), 4);
        assert_eq!(view.partition_point(|_| false).unwrap(), 0);
    }

    #[cfg(feature = "set")]
    #[test]
    fn chunk_partition_point_large_region() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let count: usize = 200;
        let mut data = vec![0u8; count * 8];
        for i in 0..count {
            data[i * 8..(i + 1) * 8].copy_from_slice(&(i as u64).to_le_bytes());
        }
        let mut s = alloc.alloc(data.len() as u64).unwrap();
        s.as_slice_mut().write(&data).unwrap();
        let (view, _rem) = s.as_slice().chunks(8);

        for threshold in (0..=count as u64).step_by(4) {
            let idx = view
                .partition_point(|c| u64::from_le_bytes(c.try_into().unwrap()) < threshold)
                .unwrap();
            assert_eq!(idx, threshold, "threshold {threshold}");
        }
    }

    #[cfg(feature = "set")]
    #[test]
    fn chunk_is_sorted_by_true_and_false() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let mut s = alloc.alloc(4).unwrap();
        s.as_slice_mut().write([1u8, 2, 3, 4]).unwrap();
        let (view, _rem) = s.as_slice().chunks(1);
        assert!(view.is_sorted_by(|a, b| a[0].cmp(&b[0])).unwrap());

        let mut s2 = alloc.alloc(4).unwrap();
        s2.as_slice_mut().write([1u8, 3, 2, 4]).unwrap();
        let (view2, _rem2) = s2.as_slice().chunks(1);
        assert!(!view2.is_sorted_by(|a, b| a[0].cmp(&b[0])).unwrap());
    }

    #[test]
    fn chunk_is_sorted_by_trivial_for_short_views() {
        let (alloc, path) = mk_alloc();
        let _g = Guard(path);
        let s = alloc.alloc(2).unwrap();
        let (view, _rem) = s.as_slice().chunks(2); // 1 chunk
        assert!(view.is_sorted_by(|a, b| a.cmp(b)).unwrap());
        let (empty_view, _rem2) = s.as_slice().subslice(0, 0).chunks(2);
        assert!(empty_view.is_sorted_by(|a, b| a.cmp(b)).unwrap());
    }

    // ── Foreign handles ───────────────────────────────────────────────────

    #[test]
    fn dealloc_and_realloc_reject_a_handle_from_another_instance() {
        let (a1, p1) = mk_alloc();
        let _g1 = Guard(p1);
        let (a2, p2) = mk_alloc();
        let _g2 = Guard(p2);

        let h = a1.alloc(64).unwrap();
        assert!(h.is_from(&a1));
        assert!(!h.is_from(&a2));
        let range = h.as_range();

        let err = a2.dealloc(h).expect_err("a2 must refuse a1's handle");
        assert_eq!(err.source.kind(), std::io::ErrorKind::InvalidInput);
        let h = err
            .handle
            .expect("a refused handle is returned, not leaked");
        assert_eq!(h.as_range(), range);

        let err = a2.realloc(h, 128).expect_err("a2 must refuse a1's handle");
        assert_eq!(err.source.kind(), std::io::ErrorKind::InvalidInput);
        let h = err
            .handle
            .expect("a refused handle is returned, not leaked");
        assert_eq!(h.as_range(), range);

        // Both allocators are untouched by the refusals.
        let own = a2.alloc(64).unwrap();
        a2.dealloc(own).map_err(|e| e.source).unwrap();
        a1.dealloc(h).map_err(|e| e.source).unwrap();
    }

    #[test]
    fn dealloc_bulk_rejects_a_batch_containing_a_foreign_handle() {
        let (a1, p1) = mk_alloc();
        let _g1 = Guard(p1);
        let (a2, p2) = mk_alloc();
        let _g2 = Guard(p2);

        let own = a2.alloc(32).unwrap();
        let foreign = a1.alloc(32).unwrap();

        // One foreign handle poisons the batch: nothing is freed, and every
        // handle comes back — including the one that did belong to `a2`.
        let err = a2
            .dealloc_bulk([own, foreign])
            .expect_err("a2 must refuse a batch holding a1's handle");
        assert_eq!(err.source.kind(), std::io::ErrorKind::InvalidInput);
        assert_eq!(err.handles.len(), 2);

        let mut handles = err.handles.into_iter();
        let own = handles.next().unwrap();
        let foreign = handles.next().unwrap();
        a2.dealloc(own).map_err(|e| e.source).unwrap();
        a1.dealloc(foreign).map_err(|e| e.source).unwrap();
    }
}

// -------------------------------------------------------------------------
// FirstFitBStackAllocator tests

#[cfg(all(test, feature = "alloc", feature = "set"))]
mod first_fit_tests {
    use crate::BStack;
    use crate::alloc::{
        BStackAllocator, BStackSlice, BStackUninitAllocator, FirstFitBStackAllocator,
    };
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
        let mut s = alloc.alloc(32).unwrap();
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
        let mut a = alloc.alloc(32).unwrap();
        let _b = alloc.alloc(16).unwrap();
        a.write(b"dirty data from previous use!!!!").unwrap();
        alloc.dealloc(a).unwrap();
        let c = alloc.alloc(32).unwrap();
        assert_eq!(c.read().unwrap(), vec![0u8; 32]);
    }

    // ── alloc_uninit / realloc_uninit ────────────────────────────────────────

    #[test]
    fn alloc_uninit_returns_a_usable_region() {
        let (alloc, path) = mk_ff("uninit_usable");
        let _g = Guard(path);
        let mut s = alloc.alloc_uninit(32).unwrap();
        assert_eq!(s.len(), 32);
        s.write([0xABu8; 32]).unwrap();
        assert_eq!(s.read().unwrap(), vec![0xABu8; 32]);
    }

    #[test]
    fn alloc_uninit_of_a_fresh_block_still_reads_zero() {
        // A miss creates the block with one sparse extend that writes only the
        // header and footer, so the payload between them reads back as zero.
        let (alloc, path) = mk_ff("uninit_fresh");
        let _g = Guard(path);
        let s = alloc.alloc_uninit(32).unwrap();
        assert_eq!(s.read().unwrap(), vec![0u8; 32]);
    }

    #[test]
    fn alloc_uninit_hands_back_a_recycled_block_unscrubbed() {
        // White-box counterpart to `reused_block_is_zero_initialised`: proves the
        // payload scrub really is skipped. The trait's contract is only that the
        // bytes are unspecified.
        let (alloc, path) = mk_ff("uninit_reuse");
        let _g = Guard(path);
        let mut a = alloc.alloc(32).unwrap();
        let _b = alloc.alloc(16).unwrap(); // keeps `a` off the tail
        a.write(b"dirty data from previous use!!!!").unwrap();
        let a_start = a.start();
        alloc.dealloc(a).unwrap();

        let c = alloc.alloc_uninit(32).unwrap();
        assert_eq!(c.start(), a_start, "the freed block must be the one reused");
        // dealloc writes the free-list next/prev pointers into the first 16 bytes;
        // everything after them is the previous occupant's data, untouched.
        assert_eq!(&c.read().unwrap()[16..], b"previous use!!!!");
    }

    #[test]
    fn alloc_uninit_block_survives_a_reopen() {
        // The header, flags and footer are still written, so the recovery scan
        // walks the arena cleanly and the data is intact after reopening.
        let (alloc, path) = mk_ff("uninit_reopen");
        let _g = Guard(path.clone());
        let a = alloc.alloc(32).unwrap();
        let _b = alloc.alloc(16).unwrap();
        alloc.dealloc(a).unwrap();
        let mut c = alloc.alloc_uninit(32).unwrap();
        c.write([0x77u8; 32]).unwrap();
        let (start, len) = (c.start(), c.len());
        drop(alloc);

        let reopened = FirstFitBStackAllocator::new(BStack::open(&path).unwrap()).unwrap();
        let view = unsafe { BStackSlice::from_raw_parts(reopened.stack(), start, len) };
        assert_eq!(view.read().unwrap(), vec![0x77u8; 32]);
    }

    #[test]
    fn realloc_uninit_preserves_existing_bytes_on_grow() {
        let (alloc, path) = mk_ff("uninit_grow");
        let _g = Guard(path);
        let mut a = alloc.alloc(32).unwrap();
        a.write([0x11u8; 32]).unwrap();
        let _pin = alloc.alloc(16).unwrap(); // keeps `a` off the tail

        let grown = alloc.realloc_uninit(a, 200).unwrap();
        assert_eq!(grown.len(), 200);
        assert_eq!(&grown.read().unwrap()[..32], &[0x11u8; 32]);
    }

    #[test]
    fn realloc_uninit_preserves_existing_bytes_on_shrink() {
        let (alloc, path) = mk_ff("uninit_shrink");
        let _g = Guard(path);
        let mut a = alloc.alloc(200).unwrap();
        a.write([0x22u8; 200]).unwrap();
        let _pin = alloc.alloc(16).unwrap();

        let shrunk = alloc.realloc_uninit(a, 32).unwrap();
        assert_eq!(shrunk.len(), 32);
        assert_eq!(shrunk.read().unwrap(), vec![0x22u8; 32]);
    }

    #[test]
    fn realloc_uninit_merges_with_the_next_free_block_in_place() {
        // Exercises the merge path, whose big staged write shrinks to just the
        // metadata when `init` is clear.
        let (alloc, path) = mk_ff("uninit_merge");
        let _g = Guard(path);
        let mut a = alloc.alloc(32).unwrap();
        a.write([0x33u8; 32]).unwrap();
        let b = alloc.alloc(64).unwrap();
        let _pin = alloc.alloc(16).unwrap(); // keeps `b` off the tail
        let a_start = a.start();
        alloc.dealloc(b).unwrap();

        let grown = alloc.realloc_uninit(a, 80).unwrap();
        assert_eq!(grown.start(), a_start, "must grow in place by merging");
        assert_eq!(grown.len(), 80);
        assert_eq!(&grown.read().unwrap()[..32], &[0x33u8; 32]);
        // The merged block must still be a well-formed allocation.
        alloc.dealloc(grown).unwrap();
    }

    #[test]
    fn realloc_uninit_merges_without_splitting_the_remainder() {
        // The no-split arm of the merge path, whose staged write shrinks to the
        // merged footer alone when `init` is clear.
        let (alloc, path) = mk_ff("uninit_merge_nosplit");
        let _g = Guard(path);
        let mut a = alloc.alloc(32).unwrap();
        a.write([0x44u8; 32]).unwrap();
        let b = alloc.alloc(64).unwrap();
        let _pin = alloc.alloc(16).unwrap(); // keeps `b` off the tail
        let a_start = a.start();
        alloc.dealloc(b).unwrap();

        // merged payload = 32 + 24 + 64 = 120; 96 + 24 + 16 > 120, so no split.
        let grown = alloc.realloc_uninit(a, 96).unwrap();
        assert_eq!(grown.start(), a_start, "must grow in place by merging");
        assert_eq!(grown.len(), 96);
        assert_eq!(&grown.read().unwrap()[..32], &[0x44u8; 32]);
        // The merged block must still be a well-formed allocation.
        alloc.dealloc(grown).unwrap();
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
        let mut a = alloc.alloc(16).unwrap();
        let mut b = alloc.alloc(16).unwrap();
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
        let s_start = s.start();
        let s2 = alloc.realloc(s, 32).unwrap();
        assert_eq!(s2.start(), s_start);
        assert_eq!(s2.len(), 32);
        assert_eq!(alloc.len().unwrap(), ALFF_HDR_OFFSET + 32 + BLOCK_OVERHEAD);
    }

    #[test]
    fn realloc_tail_preserves_data() {
        let (alloc, path) = mk_ff("realloc_tail_data");
        let _g = Guard(path);
        let mut s = alloc.alloc(16).unwrap();
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
        let s_start = s.start();
        let before_len = alloc.len().unwrap();
        let s2 = alloc.realloc(s, 16).unwrap();
        assert_eq!(s2.start(), s_start);
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
        let mut a = alloc.alloc(16).unwrap();
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
        let mut a = alloc.alloc(16).unwrap();
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
        let mut b = alloc.alloc(80).unwrap();
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
        let mut s = alloc.alloc(8).unwrap();
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
        let mut s = alloc.alloc(8).unwrap();
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
                        let mut slice = alloc.alloc(len).unwrap();
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

    // ── Foreign handles ───────────────────────────────────────────────────

    #[test]
    fn ff_dealloc_and_realloc_reject_a_handle_from_another_instance() {
        let (a1, p1) = mk_ff("foreign1");
        let _g1 = Guard(p1);
        let (a2, p2) = mk_ff("foreign2");
        let _g2 = Guard(p2);

        let h = a1.alloc(64).unwrap();
        assert!(h.is_from(&a1));
        assert!(!h.is_from(&a2));
        let range = h.as_range();

        let err = a2.dealloc(h).expect_err("a2 must refuse a1's handle");
        assert_eq!(err.source.kind(), std::io::ErrorKind::InvalidInput);
        let h = err
            .handle
            .expect("a refused handle is returned, not leaked");
        assert_eq!(h.as_range(), range);

        let err = a2.realloc(h, 128).expect_err("a2 must refuse a1's handle");
        assert_eq!(err.source.kind(), std::io::ErrorKind::InvalidInput);
        let h = err
            .handle
            .expect("a refused handle is returned, not leaked");
        assert_eq!(h.as_range(), range);

        // `a2`'s free list never saw the foreign block, so it still round-trips
        // its own allocations, and `a1` can still free the original region.
        let own = a2.alloc(64).unwrap();
        a2.dealloc(own).map_err(|e| e.source).unwrap();
        a1.dealloc(h).map_err(|e| e.source).unwrap();
    }
}

// -------------------------------------------------------------------------
// Atomic compound-operation tests

#[cfg(all(test, feature = "atomic"))]
mod atomic_tests {
    use crate::BStack;
    #[cfg(feature = "set")]
    use crate::{bstack_unsafe_reborrow, bstack_unsafe_reborrow_mut};
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

    // ---- try_extend_sparse --------------------------------------------------

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

    #[test]
    fn try_extend_sparse_buf_longer_than_length_errors_even_on_mismatch() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"hello").unwrap();
        // Size does not match (3 != 5), but the malformed request still errors.
        let err = s.try_extend_sparse(3, b"toolong", 2).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
        assert_eq!(s.len().unwrap(), 5);
    }

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

    // ---- try_extend_sparse_batched ------------------------------------------

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
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
        assert_eq!(s.len().unwrap(), 2);
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
                    buf: bstack_unsafe_reborrow_mut!(&mut buf[..]),
                }),
                1 => Some(BStackGenOp::Write {
                    offset: 5,
                    data: bstack_unsafe_reborrow!(&buf[..]),
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
                    buf: bstack_unsafe_reborrow_mut!(&mut ptr_buf[..]),
                }),
                1 => {
                    // The previous read has already filled `ptr_buf` by the
                    // time we're called again.
                    let target = u64::from_le_bytes(ptr_buf);
                    Some(BStackGenOp::Read {
                        offset: target,
                        buf: bstack_unsafe_reborrow_mut!(&mut node_buf[..]),
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
                    buf: bstack_unsafe_reborrow_mut!(&mut ptr_buf[..]),
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
                    buf: bstack_unsafe_reborrow_mut!(&mut buf[..]),
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
                buf: bstack_unsafe_reborrow_mut!(&mut buf[..]),
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
                    buf: bstack_unsafe_reborrow_mut!(&mut buf[..]),
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
                    buf: bstack_unsafe_reborrow_mut!(&mut buf[..]),
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
                    buf: bstack_unsafe_reborrow_mut!(&mut buf[..]),
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
    fn process_gen_atrunc_replaces_tail_and_ends_sequence() {
        use crate::BStackGenOp;
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"helloworld").unwrap();
        let mut calls = 0usize;
        s.process_gen(|| {
            calls += 1;
            match calls {
                1 => Some(BStackGenOp::Atrunc {
                    n: 5,
                    data: b"THERE!",
                }),
                _ => Some(BStackGenOp::Write {
                    offset: 0,
                    data: b"NOPE!",
                }),
            }
        })
        .unwrap();
        assert_eq!(calls, 1, "Atrunc must end the sequence, like Write");
        assert_eq!(s.len().unwrap(), 11);
        assert_eq!(s.peek(0).unwrap(), b"helloTHERE!");
    }

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn process_gen_atrunc_zero_empty_is_noop_and_ends_sequence() {
        use crate::BStackGenOp;
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"hello").unwrap();
        s.process_gen(|| Some(BStackGenOp::Atrunc { n: 0, data: b"" }))
            .unwrap();
        assert_eq!(s.len().unwrap(), 5);
        assert_eq!(s.peek(0).unwrap(), b"hello");
    }

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn process_gen_atrunc_exceeds_payload_returns_error() {
        use crate::BStackGenOp;
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"hi").unwrap();
        let err = s
            .process_gen(|| Some(BStackGenOp::Atrunc { n: 5, data: b"x" }))
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidInput);
        assert_eq!(s.peek(0).unwrap(), b"hi");
    }

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn process_gen_atrunc_below_locked_returns_error() {
        use crate::BStackGenOp;
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"helloworld").unwrap();
        s.lock_up_to(8).unwrap();
        let err = s
            .process_gen(|| Some(BStackGenOp::Atrunc { n: 5, data: b"x" }))
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidInput);
        assert_eq!(s.len().unwrap(), 10);
        assert_eq!(s.peek(0).unwrap(), b"helloworld");
    }

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn process_gen_splice_reads_tail_and_replaces_and_ends_sequence() {
        use crate::BStackGenOp;
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"helloworld").unwrap();
        let mut old = [0u8; 5];
        let mut calls = 0usize;
        s.process_gen(|| {
            calls += 1;
            match calls {
                // SAFETY: `old` outlives this whole `process_gen` call.
                1 => Some(BStackGenOp::Splice {
                    old: bstack_unsafe_reborrow_mut!(&mut old[..]),
                    new: b"THERE!",
                }),
                _ => Some(BStackGenOp::Write {
                    offset: 0,
                    data: b"NOPE!",
                }),
            }
        })
        .unwrap();
        assert_eq!(calls, 1, "Splice must end the sequence, like Pop");
        assert_eq!(&old, b"world", "removed tail read back into `old`");
        assert_eq!(s.len().unwrap(), 11);
        assert_eq!(s.peek(0).unwrap(), b"helloTHERE!");
    }

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn process_gen_splice_below_locked_returns_error() {
        use crate::BStackGenOp;
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"helloworld").unwrap();
        s.lock_up_to(8).unwrap();
        let mut old = [0u8; 5];
        let err = s
            .process_gen(|| {
                // SAFETY: `old` outlives this whole `process_gen` call.
                Some(BStackGenOp::Splice {
                    old: bstack_unsafe_reborrow_mut!(&mut old[..]),
                    new: b"x",
                })
            })
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidInput);
        assert_eq!(s.len().unwrap(), 10);
        assert_eq!(s.peek(0).unwrap(), b"helloworld");
    }

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn process_gen_sparse_scatters_and_ends_sequence() {
        use crate::BStackGenOp;
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"..").unwrap();
        // The `writes` slice must outlive the whole `process_gen` call, so it is
        // bound here rather than in the closure's return expression.
        let writes: [(u64, &[u8]); 2] = [(0, b"AA"), (5, b"BB")];
        let mut calls = 0usize;
        s.process_gen(|| {
            calls += 1;
            match calls {
                1 => Some(BStackGenOp::Sparse {
                    writes: &writes,
                    length: 8,
                }),
                _ => Some(BStackGenOp::Write {
                    offset: 0,
                    data: b"NOPE",
                }),
            }
        })
        .unwrap();
        assert_eq!(calls, 1, "Sparse must end the sequence, like Push");
        assert_eq!(s.len().unwrap(), 10);
        assert_eq!(s.peek(0).unwrap(), b"..AA\x00\x00\x00BB\x00");
    }

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn process_gen_sparse_length_informed_by_prior_len() {
        use crate::BStackGenOp;
        // Grow to a total size only known once `Len` has reported the current size.
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"hi").unwrap();
        let writes: [(u64, &[u8]); 1] = [(0, b"Z")];
        let mut size = 0u64;
        let mut calls = 0usize;
        s.process_gen(|| {
            calls += 1;
            match calls {
                // SAFETY: `size` outlives this whole `process_gen` call.
                1 => Some(BStackGenOp::Len {
                    out: bstack_unsafe_reborrow_mut!(&mut size),
                }),
                _ => Some(BStackGenOp::Sparse {
                    writes: &writes,
                    length: size + 2, // grow by (current size) + 2
                }),
            }
        })
        .unwrap();
        assert_eq!(size, 2);
        assert_eq!(s.len().unwrap(), 6);
        assert_eq!(s.peek(0).unwrap(), b"hiZ\x00\x00\x00");
    }

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn process_gen_sparse_empty_writes_zero_length_is_noop() {
        use crate::BStackGenOp;
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"hello").unwrap();
        let writes: [(u64, &[u8]); 0] = [];
        s.process_gen(|| {
            Some(BStackGenOp::Sparse {
                writes: &writes,
                length: 0,
            })
        })
        .unwrap();
        assert_eq!(s.len().unwrap(), 5);
        assert_eq!(s.peek(0).unwrap(), b"hello");
    }

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn process_gen_sparse_overlap_returns_error() {
        use crate::BStackGenOp;
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"hi").unwrap();
        let writes: [(u64, &[u8]); 2] = [(0, b"aaa"), (2, b"bb")];
        let err = s
            .process_gen(|| {
                Some(BStackGenOp::Sparse {
                    writes: &writes,
                    length: 8,
                })
            })
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidInput);
        assert_eq!(s.len().unwrap(), 2);
        assert_eq!(s.peek(0).unwrap(), b"hi");
    }

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn process_gen_sparse_out_of_range_returns_error() {
        use crate::BStackGenOp;
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"hi").unwrap();
        let writes: [(u64, &[u8]); 1] = [(3, b"zzz")];
        let err = s
            .process_gen(|| {
                Some(BStackGenOp::Sparse {
                    writes: &writes,
                    length: 5,
                })
            })
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidInput);
        assert_eq!(s.len().unwrap(), 2);
    }

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn process_gen_sparse_persists_across_reopen() {
        use crate::BStackGenOp;
        let (s, p) = mk_stack();
        let _g = Guard(p.clone());
        s.push(b"hi").unwrap();
        let writes: [(u64, &[u8]); 1] = [(0, b"Z")];
        s.process_gen(|| {
            Some(BStackGenOp::Sparse {
                writes: &writes,
                length: 4,
            })
        })
        .unwrap();
        drop(s);
        let s2 = BStack::open(&p).unwrap();
        assert_eq!(s2.peek(0).unwrap(), b"hiZ\x00\x00\x00");
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
                    out: bstack_unsafe_reborrow_mut!(&mut size),
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
                    out: bstack_unsafe_reborrow_mut!(&mut size),
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
                    out: bstack_unsafe_reborrow_mut!(&mut size),
                }),
                1 => {
                    let n = (size - 8) as usize;
                    buf = vec![0u8; n];
                    // SAFETY: `buf` outlives this whole `process_gen` call.
                    Some(BStackGenOp::Pop {
                        buf: bstack_unsafe_reborrow_mut!(&mut buf[..]),
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
                                    buf: bstack_unsafe_reborrow_mut!(&mut buf[..]),
                                }),
                                1 => {
                                    let v = u64::from_le_bytes(buf) + 1;
                                    buf = v.to_le_bytes();
                                    Some(BStackGenOp::Write {
                                        offset: 0,
                                        // SAFETY: `buf` outlives this whole `process_gen` call.
                                        data: bstack_unsafe_reborrow!(&buf[..]),
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
                                buf: bstack_unsafe_reborrow_mut!(&mut head_buf[..]),
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
                                        buf: bstack_unsafe_reborrow_mut!(&mut next_buf[..]),
                                    })
                                }
                            }
                            2 => Some(BStackGenOp::Write {
                                offset: 0,
                                // SAFETY: `next_buf` outlives this whole `process_gen` call.
                                data: bstack_unsafe_reborrow!(&next_buf[..]),
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
                                buf: bstack_unsafe_reborrow_mut!(&mut buf[..]),
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

    // ---- set_batched / inplace_gen (multi-write journal) -------------------

    // Build a staged multi-write tail block `[s | e | data]`.
    #[cfg(all(feature = "set", feature = "atomic"))]
    fn mw_block(start: u64, data: &[u8]) -> Vec<u8> {
        let mut b = start.to_le_bytes().to_vec();
        b.extend_from_slice(&(start + data.len() as u64).to_le_bytes());
        b.extend_from_slice(data);
        b
    }

    // Build a raw 32-byte header (current magic) with the given fields.
    #[cfg(all(feature = "set", feature = "atomic"))]
    fn mw_wip_header(clen: u64, wip_ptr: u64, wip_aux: u64) -> Vec<u8> {
        let mut h = crate::MAGIC.to_vec();
        h.extend_from_slice(&clen.to_le_bytes());
        h.extend_from_slice(&wip_ptr.to_le_bytes());
        h.extend_from_slice(&wip_aux.to_le_bytes());
        h
    }

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn set_batched_commits_all_writes_and_reopens_clean() {
        let (s, p) = mk_stack();
        let _g = Guard(p.clone());
        s.push(vec![b'.'; 500]).unwrap();

        s.set_batched([
            (0u64, vec![b'X'; 100]),
            (400u64, vec![b'Z'; 100]),
            (200u64, vec![b'Y'; 100]),
        ])
        .unwrap();

        let mut expect = vec![b'X'; 100];
        expect.extend_from_slice(&[b'.'; 100]);
        expect.extend_from_slice(&[b'Y'; 100]);
        expect.extend_from_slice(&[b'.'; 100]);
        expect.extend_from_slice(&[b'Z'; 100]);
        assert_eq!(s.peek(0).unwrap(), expect);
        drop(s);

        // Staging tail dropped, journal disarmed, value survives reopen.
        let raw = std::fs::read(&p).unwrap();
        assert_eq!(
            raw.len() as u64,
            crate::io_core::HEADER_SIZE + 500,
            "tail not truncated"
        );
        assert_eq!(&raw[16..24], &[0u8; 8], "wip_ptr not disarmed");
        assert_eq!(&raw[24..32], &[0u8; 8], "wip_aux not disarmed");
        let s2 = BStack::open(&p).unwrap();
        assert_eq!(s2.peek(0).unwrap(), expect);
    }

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn set_batched_rejects_overlap() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(vec![b'.'; 200]).unwrap();
        let err = s
            .set_batched([(0u64, vec![b'a'; 100]), (50u64, vec![b'b'; 100])])
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidInput);
        // File untouched.
        assert_eq!(s.peek(0).unwrap(), vec![b'.'; 200]);
    }

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn set_batched_empty_single_and_out_of_range() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(vec![b'.'; 100]).unwrap();

        // Empty iterator: no-op.
        s.set_batched(Vec::<(u64, Vec<u8>)>::new()).unwrap();
        // Empty data entries are dropped, leaving a lone effective write.
        s.set_batched([(0u64, Vec::new()), (10u64, vec![b'q'; 5])])
            .unwrap();
        assert_eq!(s.peek(10).unwrap()[..5], [b'q'; 5]);

        // Out-of-range write rejected, nothing applied.
        let err = s.set_batched([(90u64, vec![b'z'; 20])]).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidInput);
    }

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn recovery_replays_armed_multi_write() {
        let path =
            std::env::temp_dir().join(format!("bstack_mw_replay_{}.bin", std::process::id()));
        let _g = Guard(path.clone());

        let clen = 300u64;
        // wip_ptr == 0, wip_aux == MultiWrite sentinel.
        let mut file = mw_wip_header(clen, 0, u64::MAX - 5);
        file.extend_from_slice(&vec![b'.'; clen as usize]); // committed payload
        // Two staged blocks: [0,100) <- 'A', [200,300) <- 'B'.
        file.extend_from_slice(&mw_block(0, &[b'A'; 100]));
        file.extend_from_slice(&mw_block(200, &[b'B'; 100]));
        std::fs::write(&path, &file).unwrap();

        let s = BStack::open(&path).unwrap();
        let mut expect = vec![b'A'; 100];
        expect.extend_from_slice(&[b'.'; 100]);
        expect.extend_from_slice(&[b'B'; 100]);
        assert_eq!(s.len().unwrap(), 300);
        assert_eq!(s.peek(0).unwrap(), expect);
        drop(s);

        let raw = std::fs::read(&path).unwrap();
        assert_eq!(
            raw.len() as u64,
            crate::io_core::HEADER_SIZE + 300,
            "tail not truncated"
        );
        assert_eq!(&raw[24..32], &[0u8; 8], "wip_aux not cleared");
    }

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn recovery_rolls_back_corrupt_multi_write_tail() {
        let path =
            std::env::temp_dir().join(format!("bstack_mw_corrupt_{}.bin", std::process::id()));
        let _g = Guard(path.clone());

        let clen = 300u64;
        let mut file = mw_wip_header(clen, 0, u64::MAX - 5);
        file.extend_from_slice(&vec![b'.'; clen as usize]);
        // First block is valid, second names an end beyond the committed
        // payload — the whole sequence is corrupt, so nothing is applied.
        file.extend_from_slice(&mw_block(0, &[b'A'; 100]));
        file.extend_from_slice(&mw_block(250, &[b'B'; 100])); // end 350 > clen 300
        std::fs::write(&path, &file).unwrap();

        let s = BStack::open(&path).unwrap();
        assert_eq!(
            s.peek(0).unwrap(),
            vec![b'.'; 300],
            "corrupt tail must roll back, applying nothing"
        );
        drop(s);
        let raw = std::fs::read(&path).unwrap();
        assert_eq!(raw.len() as u64, crate::io_core::HEADER_SIZE + 300);
        assert_eq!(&raw[24..32], &[0u8; 8]);
    }

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn inplace_gen_reads_see_pending_writes() {
        use crate::BStackGenOp;
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"helloworld").unwrap();

        let src = b"ABCDE";
        let mut rbuf = [0u8; 10];
        let mut step = 0usize;
        s.inplace_gen(|res| {
            assert!(res.is_ok(), "unexpected feedback: {res:?}");
            // SAFETY: `src` and `rbuf` outlive this whole `inplace_gen` call.
            let r = match step {
                0 => Some(BStackGenOp::Write {
                    offset: 0,
                    data: bstack_unsafe_reborrow!(&src[..]),
                }),
                1 => Some(BStackGenOp::Read {
                    offset: 0,
                    buf: bstack_unsafe_reborrow_mut!(&mut rbuf[..]),
                }),
                _ => None,
            };
            step += 1;
            r
        })
        .unwrap();
        // The read observed the batch-so-far content: "ABCDE" overlaid on "hello".
        assert_eq!(&rbuf, b"ABCDEworld");
        assert_eq!(s.peek(0).unwrap(), b"ABCDEworld");
    }

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn inplace_gen_read_spans_multiple_edits_and_gaps() {
        use crate::BStackGenOp;
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(vec![b'.'; 30]).unwrap();

        // Several disjoint edits with gaps between them; a later read across the
        // whole payload must reflect exactly the edited spans (binary search must
        // locate the contiguous intersecting run correctly).
        let a = [b'A'; 4]; // [2, 6)
        let b = [b'B'; 4]; // [10, 14)
        let c = [b'C'; 4]; // [22, 26)
        let mut rbuf = [0u8; 30];
        let mut step = 0usize;
        s.inplace_gen(|res| {
            assert!(res.is_ok());
            let r = match step {
                0 => Some(BStackGenOp::Write {
                    offset: 2,
                    data: bstack_unsafe_reborrow!(&a[..]),
                }),
                1 => Some(BStackGenOp::Write {
                    offset: 10,
                    data: bstack_unsafe_reborrow!(&b[..]),
                }),
                2 => Some(BStackGenOp::Write {
                    offset: 22,
                    data: bstack_unsafe_reborrow!(&c[..]),
                }),
                // Read a middle window [4, 24) that clips edit A on its left,
                // fully contains B, and clips C on its right.
                3 => Some(BStackGenOp::Read {
                    offset: 4,
                    buf: bstack_unsafe_reborrow_mut!(&mut rbuf[..20]),
                }),
                _ => None,
            };
            step += 1;
            r
        })
        .unwrap();
        // [4,6)='A', [6,10)='.', [10,14)='B', [14,22)='.', [22,24)='C'.
        let mut expect = vec![b'A'; 2];
        expect.extend_from_slice(&[b'.'; 4]);
        expect.extend_from_slice(&[b'B'; 4]);
        expect.extend_from_slice(&[b'.'; 8]);
        expect.extend_from_slice(&[b'C'; 2]);
        assert_eq!(&rbuf[..20], &expect[..]);
    }

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn inplace_gen_later_write_overrides_overlap() {
        use crate::BStackGenOp;
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(vec![b'.'; 10]).unwrap();

        // a<b<c<d: write a..c then b..d — commits a..b (first), b..d (second).
        let first = [b'1'; 6]; // [0, 6)
        let second = [b'2'; 6]; // [3, 9)
        let mut step = 0usize;
        s.inplace_gen(|_res| {
            let r = match step {
                0 => Some(BStackGenOp::Write {
                    offset: 0,
                    data: bstack_unsafe_reborrow!(&first[..]),
                }),
                1 => Some(BStackGenOp::Write {
                    offset: 3,
                    data: bstack_unsafe_reborrow!(&second[..]),
                }),
                _ => None,
            };
            step += 1;
            r
        })
        .unwrap();
        // [0,3)='1', [3,9)='2', [9,10)='.'
        assert_eq!(s.peek(0).unwrap(), b"111222222.");
    }

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn inplace_gen_overlay_enclosure_and_gaps() {
        use crate::BStackGenOp;
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(vec![b'.'; 20]).unwrap();

        // A mix that exercises every overlay case and forces the new edit into a
        // non-tail sorted position each time:
        //   1. [4, 12)  = '1'                          (a plain edit)
        //   2. [14, 18) = '2'                          (a gap after it)
        //   3. [6, 8)   = '3'  -> old encloses new     (splits edit 1 in three)
        //   4. [2, 16)  = '4'  -> new encloses several (drops 3, trims 1 and 2)
        let e1 = [b'1'; 8];
        let e2 = [b'2'; 4];
        let e3 = [b'3'; 2];
        let e4 = [b'4'; 14];
        let mut step = 0usize;
        s.inplace_gen(|res| {
            assert!(res.is_ok());
            let r = match step {
                0 => Some(BStackGenOp::Write {
                    offset: 4,
                    data: bstack_unsafe_reborrow!(&e1[..]),
                }),
                1 => Some(BStackGenOp::Write {
                    offset: 14,
                    data: bstack_unsafe_reborrow!(&e2[..]),
                }),
                2 => Some(BStackGenOp::Write {
                    offset: 6,
                    data: bstack_unsafe_reborrow!(&e3[..]),
                }),
                3 => Some(BStackGenOp::Write {
                    offset: 2,
                    data: bstack_unsafe_reborrow!(&e4[..]),
                }),
                _ => None,
            };
            step += 1;
            r
        })
        .unwrap();
        // Final: [0,2)='.', [2,16)='4' (edit 4 overrode everything it covered),
        // [16,18)='2' (surviving suffix of edit 2), [18,20)='.'.
        let mut expect = vec![b'.'; 2];
        expect.extend_from_slice(&[b'4'; 14]);
        expect.extend_from_slice(&[b'2'; 2]);
        expect.extend_from_slice(&[b'.'; 2]);
        assert_eq!(s.peek(0).unwrap(), expect);
    }

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn inplace_gen_rejects_size_ops_but_still_commits() {
        use crate::BStackGenOp;
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"helloworld").unwrap();

        let data = b"HELLO";
        let writes: [(u64, &[u8]); 1] = [(0, b"Z")];
        // Feedback for op N arrives on call N+1: Write (step 0) is valid, so its
        // feedback at step 1 is Ok; the size-changing Push (step 1) and Sparse
        // (step 2) are each rejected, so their feedback at steps 2 and 3 is Err.
        let mut push_errored = false;
        let mut sparse_errored = false;
        let mut step = 0usize;
        s.inplace_gen(|res| {
            match step {
                2 => {
                    assert!(res.is_err(), "Push should have reported an error");
                    push_errored = true;
                }
                3 => {
                    assert!(res.is_err(), "Sparse should have reported an error");
                    sparse_errored = true;
                }
                _ => {}
            }
            let r = match step {
                0 => Some(BStackGenOp::Write {
                    offset: 0,
                    data: bstack_unsafe_reborrow!(&data[..]),
                }),
                1 => Some(BStackGenOp::Push { data: b"!!!" }),
                2 => Some(BStackGenOp::Sparse {
                    writes: &writes,
                    length: 4,
                }),
                _ => None,
            };
            step += 1;
            r
        })
        .unwrap();
        assert!(push_errored && sparse_errored);
        // Size unchanged; only the valid in-place write committed.
        assert_eq!(s.len().unwrap(), 10);
        assert_eq!(s.peek(0).unwrap(), b"HELLOworld");
    }

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn inplace_gen_immediate_none_is_noop() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"hello").unwrap();
        s.inplace_gen(|_| None).unwrap();
        assert_eq!(s.peek(0).unwrap(), b"hello");
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
    fn get_batched_zero_length_range_returns_empty_buf() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"hello").unwrap();
        let results = s.get_batched(core::iter::once(3..3)).unwrap();
        assert_eq!(results.len(), 1);
        assert!(results[0].is_empty());
    }

    #[cfg(feature = "atomic")]
    #[test]
    fn get_batched_out_of_bounds_returns_error() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"hello").unwrap();
        let err = s.get_batched(core::iter::once(0..10)).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidInput);
    }

    #[cfg(feature = "atomic")]
    #[test]
    fn get_batched_end_less_than_start_returns_error() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(b"helloworld").unwrap();
        #[allow(clippy::reversed_empty_ranges)]
        let err = s.get_batched(core::iter::once(5..3)).unwrap_err();
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

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn copy_disjoint_larger_than_block_journals_and_reopens_clean() {
        let (s, p) = mk_stack();
        let _g = Guard(p.clone());
        // 800-byte payload; copy 300 disjoint bytes [0,300) → [400,700). 300 > one
        // aligned block, so it takes the copy-journal path (not the atomic write).
        let mut data = vec![b'S'; 300];
        data.extend_from_slice(&vec![b'.'; 500]);
        s.push(&data).unwrap();
        s.copy(0, 400, 300).unwrap();

        assert_eq!(
            s.get(400, 700).unwrap(),
            vec![b'S'; 300],
            "disjoint copy landed"
        );
        assert_eq!(s.get(0, 300).unwrap(), vec![b'S'; 300], "source unchanged");
        drop(s);

        // Only the source coordinate was staged; the tail backup is gone, wip is
        // disarmed, and the value survives reopen.
        let raw = std::fs::read(&p).unwrap();
        assert_eq!(
            raw.len() as u64,
            crate::HEADER_SIZE + 800,
            "tail not truncated"
        );
        assert_eq!(&raw[16..24], &[0u8; 8], "wip_ptr not disarmed");
        let s2 = BStack::open(&p).unwrap();
        assert_eq!(s2.get(400, 700).unwrap(), vec![b'S'; 300]);
    }

    #[cfg(all(feature = "set", feature = "atomic"))]
    #[test]
    fn copy_same_location_is_noop() {
        let (s, p) = mk_stack();
        let _g = Guard(p);
        s.push(vec![b'Z'; 400]).unwrap();
        // from == to with a block-spanning length: the same-location short-circuit
        // returns without arming any journal.
        s.copy(50, 50, 300).unwrap();
        assert_eq!(s.get(50, 350).unwrap(), vec![b'Z'; 300]);
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
