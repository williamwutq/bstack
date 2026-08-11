//! # Shared, reference-counted persistent objects (`bstack_raii`)
//!
//! `std::rc::Rc` / `Weak` live and die with the process. `bstack_raii` gives the
//! same ownership model — shared strong handles, non-owning weak handles, and
//! automatic cleanup when the last owner is dropped — but backed by a single,
//! crash-safe file, so the object graph *and its reference counts* survive a
//! restart.
//!
//! This models an app that persists login `Session`s. Many sessions share one
//! `Config` (reference-counted on disk). The config is freed automatically the
//! instant the last session referencing it drops, and a `Monitor` holds a weak
//! handle to observe it without keeping it alive.
//!
//! Run with: `cargo run --example sessions`

use std::io;

use bstack::FirstFitBStackAllocator;
// `BStack` / `BStackAllocator` / `BStackBlock` / `BStackRange` are re-exported by
// `bstack_raii`, so a downstream crate only depends on `bstack_raii`.
use bstack_raii::{
    BStack, BStackAllocator, BStackBlock, BStackDrop, BStackRange, TryClone, bstack_block,
};

/// A shared, reference-counted configuration. `(rc, weak)` makes it refcounted
/// and weak-observable on disk.
#[bstack_block(rc, weak)]
struct Config {
    version: u64,
    flags: u64,
}

/// A session that owns a *strong* reference to a shared `Config`.
#[bstack_block]
struct Session {
    id: u64,
    #[bstack_strong]
    config: Config,
}

/// Shared ownership with automatic, refcount-driven cleanup.
fn shared_ownership_demo(path: &std::path::Path) -> io::Result<()> {
    let alloc = FirstFitBStackAllocator::new(BStack::open(path)?)?;
    let stack = alloc.stack();

    // One config, shared by three sessions. Each `Session::new` *consumes* a
    // clone, so the on-disk strong count climbs to three.
    let config = Config::new(&alloc, 3, 0b1010)?;
    let mut sessions = Vec::new();
    for id in 0u64..3 {
        sessions.push(Session::new(&alloc, id, config.try_clone()?)?);
    }

    // A monitor observes the config without owning it.
    let monitor = config.downgrade()?;
    drop(config); // the three sessions still keep it alive

    // Read the shared config through any session's generated accessor.
    let cfg = sessions[0].handle().get_config(stack)?;
    println!(
        "shared config: version {}, flags {:#06b} (held by {} sessions)",
        cfg.get_version(stack)?,
        cfg.get_flags(stack)?,
        sessions.len(),
    );

    // Close sessions one at a time. A `Session` is a *uniquely owned* block, so
    // its teardown is explicit (`bstack_drop`) — dropping the handle alone would
    // persist it, which is what you want for a durable root. Each close releases
    // the session's strong reference to the shared config.
    while let Some(session) = sessions.pop() {
        session.bstack_drop(&alloc)?;
        let still_alive = monitor.upgrade()?.is_some();
        println!("closed a session -> shared config still alive: {still_alive}");
    }

    // The last session released the last strong reference, so the *shared* config
    // was reclaimed automatically by its refcount — no manual free of the config,
    // no leak, no dangling weak handle.
    assert!(monitor.upgrade()?.is_none());
    println!("last session closed -> shared config freed automatically");

    drop(monitor); // releases the (now-unreferenced) control block
    Ok(())
}

/// Durability: the typed block survives closing and reopening the file.
fn durability_demo(path: &std::path::Path) -> io::Result<()> {
    // Write a config, then simulate the process exiting while still holding it:
    // `mem::forget` skips the handle's destructor, so the on-disk block (and its
    // refcount) are left intact — exactly what a real exit or crash leaves behind.
    let saved: BStackRange = {
        let alloc = FirstFitBStackAllocator::new(BStack::open(path)?)?;
        let config = Config::new(&alloc, 7, 0b1)?;
        let range = config.handle().range();
        std::mem::forget(config);
        range
        // allocator dropped here -> the file is flushed and closed
    };

    // A later run reopens the same file and reads the persisted block back.
    let alloc = FirstFitBStackAllocator::new(BStack::open(path)?)?;
    let cfg = <Config as BStackBlock>::from_range(saved);
    println!(
        "after reopen: config version {} (persisted across the close/reopen)",
        cfg.get_version(alloc.stack())?,
    );
    Ok(())
}

fn main() -> io::Result<()> {
    let path = std::env::temp_dir().join("bstack_raii_sessions.bstack");
    let _ = std::fs::remove_file(&path);

    println!("== shared ownership + automatic cleanup ==");
    shared_ownership_demo(&path)?;

    let _ = std::fs::remove_file(&path);
    println!("\n== durability across a reopen ==");
    durability_demo(&path)?;

    let _ = std::fs::remove_file(&path);
    Ok(())
}
