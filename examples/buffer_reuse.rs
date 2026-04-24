use bstack::BStack;
use std::io;

fn main() -> io::Result<()> {
    let stack = BStack::open("buffer_reuse_example.bstack")?;

    // Push some data
    stack.push(b"First message\n")?; // 14 bytes
    stack.push(b"Second message\n")?; // 15 bytes
    stack.push(b"Third message\n")?; // 14 bytes

    println!("Stack length: {} bytes", stack.len()?);

    // Demonstrate get_into for zero-copy reading
    let mut buf = vec![0u8; 14]; // Buffer for "First message\n"
    stack.get_into(0, &mut buf)?;
    println!("Read with get_into: {:?}", String::from_utf8_lossy(&buf));

    // Demonstrate pop_into for zero-copy popping (pops last 14 bytes: "Third message\n")
    let mut pop_buf = vec![0u8; 14];
    stack.pop_into(&mut pop_buf)?;
    println!(
        "Popped with pop_into: {:?}",
        String::from_utf8_lossy(&pop_buf)
    );

    println!("Stack length after pop: {} bytes", stack.len()?);

    // Peek remaining data
    let remaining = stack.peek(0)?;
    println!("Remaining data: {:?}", String::from_utf8_lossy(&remaining));

    Ok(())
}
