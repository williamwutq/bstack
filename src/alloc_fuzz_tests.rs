#![cfg(all(test, feature = "alloc", feature = "set"))]

mod alloc_fuzz_tests {
    use crate::BStack;
    use crate::alloc::{BStackAllocator, FirstFitBStackAllocator};
    use rand::RngExt;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::vec;

    const MIN_PAYLOAD: u64 = 16;
    const FUZZ_COUNT: usize = 8000;

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
            if rng.random_bool(0.6) || allocated.is_empty() {
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
            if rng.random_bool(0.5) || allocated.is_empty() {
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

                if rng.random_bool(0.5) {
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
}
