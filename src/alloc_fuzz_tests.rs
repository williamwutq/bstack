#![cfg(all(test, feature = "alloc", feature = "set"))]

mod alloc_fuzz_tests {
    use crate::BStack;
    use crate::alloc::{BStackAllocator, FirstFitBStackAllocator};
    use rand::RngExt;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::vec;

    const MIN_PAYLOAD: u64 = 16;
    const FUZZ_COUNT: usize = 200000;

    fn mk_ff(id_prefix: &str) -> (FirstFitBStackAllocator, std::path::PathBuf) {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let id = COUNTER.fetch_add(1, Ordering::Relaxed);
        let pid = std::process::id();
        let path = std::env::temp_dir().join(format!("bstack_ff_fuzz_{id_prefix}_{pid}_{id}.bin"));
        let stack = BStack::open(&path).unwrap();
        (FirstFitBStackAllocator::new(stack).unwrap(), path)
    }

    struct Guard(std::path::PathBuf);
    impl Drop for Guard {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }

    #[test]
    fn fuzz_alloc_dealloc() {
        let (alloc, path) = mk_ff("alloc_dealloc");
        let _guard = Guard(path);

        let mut rng = rand::rng();
        let mut allocated = Vec::new();

        for _ in 0..FUZZ_COUNT {
            if rng.random_bool(0.7) || allocated.is_empty() {
                // Allocate a new block of random size.
                let size = rng.random_range(MIN_PAYLOAD..=1024);
                if let Some(block) = alloc.alloc(size).ok() {
                    // Write the block ID into the payload for later verification.
                    let id = allocated.len() as u64;
                    let mut filled_vec = vec![0u8; size as usize];
                    // Fill vec with block ID for later verification.
                    for i in 0..size {
                        let byte_idx = (i % 8) as u64;
                        filled_vec[i as usize] = ((id >> (byte_idx * 8)) & 0xFF) as u8;
                    }

                    block.write(&filled_vec).unwrap();
                    allocated.push((block, id));
                }
            } else {
                // Deallocate a random block from the allocated list.
                let idx = rng.random_range(0..allocated.len());
                let (block, id) = allocated.swap_remove(idx);
                // Verify the block contents before deallocating.
                let buf = block.read().unwrap();
                for chunk in buf.iter().enumerate() {
                    let idx = chunk.0 as u64;
                    let byte_idx = idx % 8;
                    let expected_byte = ((id >> (byte_idx * 8)) & 0xFF) as u8;
                    assert_eq!(
                        *chunk.1, expected_byte,
                        "Data corruption detected in block ID {id} at index {idx}"
                    );
                }
                alloc.dealloc(block).unwrap();
            }
        }
    }

    #[test]
    fn fuzz_alloc_realloc_dealloc() {
        let (alloc, path) = mk_ff("alloc_realloc_dealloc");
        let _guard = Guard(path);

        let mut rng = rand::rng();
        let mut allocated = Vec::new();

        for _ in 0..FUZZ_COUNT {
            if rng.random_bool(0.6) || allocated.is_empty() {
                // Allocate a new block of random size.
                let size = rng.random_range(MIN_PAYLOAD..=1024);
                if let Some(block) = alloc.alloc(size).ok() {
                    // Write the block ID into the payload for later verification.
                    let id = allocated.len() as u64;
                    let mut filled_vec = vec![0u8; size as usize];
                    for i in 0..size {
                        let byte_idx = (i % 8) as u64;
                        filled_vec[i as usize] = ((id >> (byte_idx * 8)) & 0xFF) as u8;
                    }

                    block.write(&filled_vec).unwrap();
                    allocated.push((block, id));
                }
            } else {
                // Reallocate a random block to a new size or deallocate it.
                let idx = rng.random_range(0..allocated.len());
                let (block, id) = allocated.swap_remove(idx);

                if rng.random_bool(0.8) {
                    // Reallocate to a new size.
                    let new_size = rng.random_range(MIN_PAYLOAD..=1024);
                    // Read the old block contents for verification after realloc.
                    let buf = block.read().unwrap();

                    if let Some(new_block) = alloc.realloc(block, new_size).ok() {
                        // Verify the old contents are intact after realloc.
                        for chunk in buf.iter().enumerate() {
                            let idx = chunk.0 as u64;
                            let byte_idx = idx % 8;
                            let expected_byte = ((id >> (byte_idx * 8)) & 0xFF) as u8;
                            assert_eq!(
                                *chunk.1, expected_byte,
                                "Data corruption detected during realloc of block ID {id} at index {idx}"
                            );
                        }

                        // Write new data with the same ID for verification.
                        let mut filled_vec = vec![0u8; new_size as usize];
                        for i in 0..new_size {
                            let byte_idx = (i % 8) as u64;
                            filled_vec[i as usize] = ((id >> (byte_idx * 8)) & 0xFF) as u8;
                        }
                        new_block.write(&filled_vec).unwrap();
                        allocated.push((new_block, id));
                    }
                } else {
                    // Deallocate the block.
                    let buf = block.read().unwrap();
                    for chunk in buf.iter().enumerate() {
                        let idx = chunk.0 as u64;
                        let byte_idx = idx % 8;
                        let expected_byte = ((id >> (byte_idx * 8)) & 0xFF) as u8;
                        assert_eq!(
                            *chunk.1, expected_byte,
                            "Data corruption detected before deallocation of block ID {id} at index {idx}"
                        );
                    }
                    alloc.dealloc(block).unwrap();
                }
            }
        }
    }

    /// Fuzz across repeated file reopens.
    ///
    /// Each "session" reopens the file, reconstructs slice handles from the
    /// serialised (start, len) table, verifies data integrity, then performs
    /// random alloc / realloc / dealloc operations before closing again.
    /// Data written to any live allocation must survive every reopen.
    #[test]
    fn fuzz_reopen() {
        // A serialised record of a live allocation.
        // `pattern` is the byte value written across the full payload so we
        // can verify each byte independently without storing the whole buffer.
        #[derive(Clone, Copy)]
        struct Rec {
            start: u64,
            len: u64,
            pattern: u8,
        }

        fn fill(buf: &mut [u8], pattern: u8) {
            for (i, b) in buf.iter_mut().enumerate() {
                *b = pattern.wrapping_add(i as u8);
            }
        }

        fn verify(buf: &[u8], pattern: u8, index: usize, session: usize) {
            for (i, &b) in buf.iter().enumerate() {
                assert_eq!(
                    b,
                    pattern.wrapping_add(i as u8),
                    "[{session}-{index}] data corruption at index {i}: expected {}, got {b}",
                    pattern.wrapping_add(i as u8)
                );
            }
        }

        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let id = COUNTER.fetch_add(1, Ordering::Relaxed);
        let pid = std::process::id();
        let path = std::env::temp_dir()
            .join(format!("bstack_ff_fuzz_reopen_{pid}_{id}.bin"));
        let _guard = Guard(path.clone());

        // Create the file.
        {
            let stack = BStack::open(&path).unwrap();
            FirstFitBStackAllocator::new(stack).unwrap();
        }

        let mut rng = rand::rng();
        let mut live: Vec<Rec> = Vec::new();
        let mut next_pattern: u8 = 1;

        const SESSIONS: usize = 100;
        const OPS_PER_SESSION: usize = 200;

        for session in 0..SESSIONS {
            // Open allocator for this session.
            let alloc =
                FirstFitBStackAllocator::new(BStack::open(&path).unwrap()).unwrap();

            // Reconstruct handles and verify data for every live allocation.
            for (idx, rec) in live.iter().enumerate() {
                let s = crate::alloc::BStackSlice::new(&alloc, rec.start, rec.len);
                let buf = s.read().unwrap();
                verify(&buf[..rec.len as usize], rec.pattern, idx, session);
            }

            // Perform random operations.
            for _ in 0..OPS_PER_SESSION {
                let choice = if live.is_empty() {
                    0
                } else {
                    rng.random_range(0u32..4)
                };

                match choice {
                    // Allocate a new block.
                    0 => {
                        let len = rng.random_range(MIN_PAYLOAD..=512);
                        if let Ok(s) = alloc.alloc(len) {
                            let pat = next_pattern;
                            next_pattern = next_pattern.wrapping_add(1).max(1);
                            let mut buf = vec![0u8; len as usize];
                            fill(&mut buf, pat);
                            s.write(&buf).unwrap();
                            live.push(Rec { start: s.start(), len, pattern: pat });
                        }
                    }
                    // Realloc a random live block.
                    1 => {
                        let idx = rng.random_range(0..live.len());
                        let rec = live[idx];
                        let new_len = rng.random_range(MIN_PAYLOAD..=512);
                        let s = crate::alloc::BStackSlice::new(&alloc, rec.start, rec.len);
                        if let Ok(s2) = alloc.realloc(s, new_len) {
                            // Verify the overlapping prefix survived.
                            let overlap = rec.len.min(new_len) as usize;
                            let buf = s2.read().unwrap();
                            verify(&buf[..overlap], rec.pattern, idx, session);
                            // Write fresh pattern into the reallocated block.
                            let pat = next_pattern;
                            next_pattern = next_pattern.wrapping_add(1).max(1);
                            let mut new_buf = vec![0u8; new_len as usize];
                            fill(&mut new_buf, pat);
                            s2.write(&new_buf).unwrap();
                            live[idx] = Rec { start: s2.start(), len: new_len, pattern: pat };
                        }
                    }
                    // Deallocate a random live block.
                    2 => {
                        let idx = rng.random_range(0..live.len());
                        let rec = live.swap_remove(idx);
                        let s = crate::alloc::BStackSlice::new(&alloc, rec.start, rec.len);
                        // Verify before freeing.
                        let buf = s.read().unwrap();
                        verify(&buf[..rec.len as usize], rec.pattern, idx, session);
                        alloc.dealloc(s).unwrap();
                    }
                    // Verify a random live block without mutating.
                    _ => {
                        let idx = rng.random_range(0..live.len());
                        let rec = live[idx];
                        let s = crate::alloc::BStackSlice::new(&alloc, rec.start, rec.len);
                        let buf = s.read().unwrap();
                        verify(&buf[..rec.len as usize], rec.pattern, idx, session);
                    }
                }
            }

            // Drop the allocator (closes the file) before the next session.
            drop(alloc.into_stack());
        }
    }
}
