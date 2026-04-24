use bstack::BStack;
use std::io::{self, Write};

fn main() -> io::Result<()> {
    let mut stack = BStack::open("write_example.bstack")?;

    // Use BStack as a Write implementation
    writeln!(stack, "Line 1: Hello from Write trait")?;
    writeln!(stack, "Line 2: This demonstrates durability")?;
    write!(stack, "Line 3: Each write is atomic and synced")?;

    println!("Wrote {} bytes", stack.len()?);

    // Read back what we wrote
    let data = stack.peek(0)?;
    println!("\nContents:");
    println!("{}", String::from_utf8_lossy(&data));

    Ok(())
}
