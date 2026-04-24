use bstack::BStack;
use std::io;

fn main() -> io::Result<()> {
    let stack = BStack::open("journal_example.bstack")?;

    // Simulate appending log entries
    let entries = vec![
        "INFO: Application started",
        "INFO: Connected to database",
        "WARN: High memory usage detected",
        "INFO: User login: alice",
        "ERROR: Failed to process request",
    ];

    for entry in entries {
        let entry_bytes = format!("{}\n", entry).into_bytes();
        let offset = stack.push(&entry_bytes)?;
        println!("Logged entry at offset {}: {}", offset, entry.trim());
    }

    println!("\nTotal log size: {} bytes", stack.len()?);

    // Read the entire log
    let log_data = stack.peek(0)?;
    println!("\nFull log contents:");
    println!("{}", String::from_utf8_lossy(&log_data));

    // Simulate tailing the last few entries
    let last_2_entries = stack.peek(stack.len()? - 50)?; // Approximate last 50 bytes
    println!("\nLast entries (approx):");
    println!("{}", String::from_utf8_lossy(&last_2_entries));

    Ok(())
}
