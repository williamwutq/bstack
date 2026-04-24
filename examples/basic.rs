use bstack::BStack;
use std::io;

fn main() -> io::Result<()> {
    // Open or create a bstack file
    let stack = BStack::open("basic_example.bstack")?;

    println!("Initial stack length: {}", stack.len()?);

    // Push some data
    let offset1 = stack.push(b"Hello, ")?;
    println!("Pushed 'Hello, ' at offset {}", offset1);

    let offset2 = stack.push(b"world!")?;
    println!("Pushed 'world!' at offset {}", offset2);

    println!("Stack length after pushes: {}", stack.len()?);

    // Peek at the data
    let data = stack.peek(0)?;
    println!("All data: {:?}", String::from_utf8_lossy(&data));

    // Pop the last item
    let popped = stack.pop(6)?; // "world!"
    println!("Popped: {:?}", String::from_utf8_lossy(&popped));

    println!("Stack length after pop: {}", stack.len()?);

    // Peek remaining data
    let remaining = stack.peek(0)?;
    println!("Remaining data: {:?}", String::from_utf8_lossy(&remaining));

    Ok(())
}
