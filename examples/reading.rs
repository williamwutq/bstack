use bstack::BStack;
use std::io;

fn main() -> io::Result<()> {
    let stack = BStack::open("concurrent_read_example.bstack")?;

    // Push some test data
    for i in 0..10 {
        let data = format!("Entry {}\n", i).into_bytes();
        stack.push(&data)?;
    }

    println!("Stack contains {} bytes", stack.len()?);

    // Demonstrate that we can read different parts concurrently
    // (In a real concurrent program, these would be in different threads)

    let all_data = stack.peek(0)?;
    println!("All data:\n{}", String::from_utf8_lossy(&all_data));

    // Read specific ranges
    let first_five = stack.get(0, 5)?; // First 5 bytes
    println!("First 5 bytes: {:?}", String::from_utf8_lossy(&first_five));

    let middle_section = stack.get(10, 10)?; // 10 bytes starting at offset 10
    println!(
        "Bytes 10-20: {:?}",
        String::from_utf8_lossy(&middle_section)
    );

    // Use peek_into for zero-copy reading
    let mut buf = vec![0u8; 20];
    stack.peek_into(5, &mut buf)?;
    println!("Peeked into buffer: {:?}", String::from_utf8_lossy(&buf));

    Ok(())
}
