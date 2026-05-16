use bstack::BStack;
use std::io::{self, ErrorKind};
use std::sync::Arc;
use std::thread;

const HEADER_SIZE: u64 = 32;

// Layout: [ magic: 4 B | version: 4 B | capacity: 8 B | flags: 8 B | checksum: 8 B ]
fn make_header(version: u32, capacity: u64, flags: u64) -> [u8; HEADER_SIZE as usize] {
    let mut hdr = [0u8; HEADER_SIZE as usize];
    hdr[0..4].copy_from_slice(b"DEMO");
    hdr[4..8].copy_from_slice(&version.to_le_bytes());
    hdr[8..16].copy_from_slice(&capacity.to_le_bytes());
    hdr[16..24].copy_from_slice(&flags.to_le_bytes());
    // Simple checksum: XOR of the first 24 bytes repeated to fill 8 bytes.
    let cksum: u64 = hdr[..24]
        .chunks(8)
        .map(|c| u64::from_le_bytes(c.try_into().unwrap()))
        .fold(0u64, u64::wrapping_add);
    hdr[24..32].copy_from_slice(&cksum.to_le_bytes());
    hdr
}

fn main() -> io::Result<()> {
    let path = "lock_up_to_example.bstack";
    let _ = std::fs::remove_file(path);

    // 1. Write header and lock it.
    println!("=== 1. Write and lock ===");
    {
        let stack = BStack::open(path)?;
        assert_eq!(stack.locked_len(), 0);

        let header = make_header(1, 1_000_000, 0b0000_0011);
        let off = stack.push(&header)?;
        println!("header at offset {off}; stack len = {}", stack.len()?);

        stack.lock_up_to(HEADER_SIZE)?;
        println!("locked_len = {}", stack.locked_len());

        // Cannot shrink.
        assert_eq!(stack.lock_up_to(HEADER_SIZE - 1).unwrap_err().kind(), ErrorKind::InvalidInput);
        println!("lock_up_to(less) → InvalidInput");

        // Idempotent.
        stack.lock_up_to(HEADER_SIZE)?;
        println!("lock_up_to(same) → ok");
    }

    // 2. Reads (lock-free) and write/shrink protection.
    println!("\n=== 2. Protection ===");
    {
        let stack = BStack::open(path)?;
        stack.lock_up_to(HEADER_SIZE)?;

        // Reads within the locked region bypass the RwLock.
        let magic = stack.get(0, 4)?;
        assert_eq!(&magic, b"DEMO");
        println!("get      → {:?}", String::from_utf8_lossy(&magic));

        let mut ver_buf = [0u8; 4];
        stack.get_into(4, &mut ver_buf)?;
        println!("get_into → version {}", u32::from_le_bytes(ver_buf));

        let mut cap_buf = [0u8; 8];
        stack.peek_into(8, &mut cap_buf)?;
        println!("peek_into → capacity {}", u64::from_le_bytes(cap_buf));

        // Write protection.
        #[cfg(feature = "set")]
        {
            assert_eq!(stack.set(0, b"XXXX").unwrap_err().kind(), ErrorKind::InvalidInput);
            println!("set in locked region → InvalidInput");
        }

        // Appending past the lock is always allowed.
        let data_off = stack.push(b"payload\n")?;
        println!("push → offset {data_off}");

        // Shrink protection: cannot discard below locked_len.
        assert_eq!(
            stack.discard(stack.len()? - HEADER_SIZE + 1).unwrap_err().kind(),
            ErrorKind::InvalidInput
        );
        println!("discard past lock → InvalidInput");

        // Discarding down to the boundary is fine.
        stack.discard(stack.len()? - HEADER_SIZE)?;
        println!("discard to boundary → ok; len = {}", stack.len()?);

        // The boundary can grow monotonically to cover new immutable data.
        stack.push(b"extra immutable block\0\0\0\0\0\0\0\0\0\0\0")?;
        stack.lock_up_to(stack.len()?)?;
        println!("extended lock; locked_len = {}", stack.locked_len());
    }

    // 3. Concurrent lock-free reads.
    println!("\n=== 3. Concurrent reads ===");
    {
        let stack = Arc::new(BStack::open(path)?);
        stack.lock_up_to(HEADER_SIZE)?;

        const READERS: usize = 8;
        let handles: Vec<_> = (0..READERS)
            .map(|_| {
                let s = Arc::clone(&stack);
                thread::spawn(move || -> io::Result<[u8; 4]> {
                    Ok(s.get(0, 4)?.try_into().unwrap())
                })
            })
            .collect();

        let mut all_ok = true;
        for (id, h) in handles.into_iter().enumerate() {
            match h.join().unwrap() {
                Ok(magic) if &magic == b"DEMO" => {}
                Ok(magic) => { eprintln!("thread {id}: wrong magic {:?}", magic); all_ok = false; }
                Err(e)    => { eprintln!("thread {id}: {e}"); all_ok = false; }
            }
        }
        println!("{READERS} concurrent reads — {}", if all_ok { "ok" } else { "FAIL" });
    }

    // 4. open_locked_up_to.
    println!("\n=== 4. open_locked_up_to ===");
    {
        let stack = BStack::open_locked_up_to(path, HEADER_SIZE)?;
        println!("locked_len = {}", stack.locked_len());

        let len = stack.len()?;
        assert_eq!(BStack::open_locked_up_to(path, len + 1).unwrap_err().kind(), ErrorKind::InvalidInput);
        println!("open_locked_up_to(len+1) → InvalidInput");
    }

    // 5. Lock is in-memory only — resets on reopen.
    println!("\n=== 5. Reopen resets lock ===");
    {
        let stack = BStack::open(path)?;
        assert_eq!(stack.locked_len(), 0);
        println!("locked_len after plain open = {}", stack.locked_len());
        assert_eq!(&stack.get(0, 4)?, b"DEMO"); // header still on disk

        #[cfg(feature = "set")]
        {
            stack.set(0, b"DEMO")?; // succeeds — no lock
            println!("set in formerly-locked region → ok");
        }
    }

    std::fs::remove_file(path).ok();
    Ok(())
}
