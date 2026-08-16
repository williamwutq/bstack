//! Operation: **refcount** — shared (`rc` / `rc, weak`) semantics, exercised through
//! the public handle API (exact-count assertions that peek at the control block stay
//! as unit tests in `src/tests.rs`).

use bstack::BStackAllocator;
use bstack_raii::{BStackBlock, TryClone};

use crate::common::TempStack;
use crate::fixtures::{Shared, rc_sink};

#[test]
fn strong_clone_keeps_alive_until_last_drop() {
    let tmp = TempStack::new();
    let a = tmp.allocator();
    let stack = a.stack();

    let rc = Shared::new(&a, 5).unwrap(); // strong = 1
    let rc2 = rc.try_clone().unwrap(); // strong = 2 (shares, not deep-copied)
    drop(rc); // strong = 1, still alive
    assert_eq!(rc2.handle().get_v(stack).unwrap(), 5);
    drop(rc2); // strong = 0, freed
}

#[test]
fn weak_upgrade_reflects_strong_liveness() {
    let tmp = TempStack::new();
    let a = tmp.allocator();

    let rc = Shared::new(&a, 7).unwrap();
    let w = rc.downgrade().unwrap();
    assert!(w.upgrade().unwrap().is_some()); // strong alive → upgradeable
    drop(rc); // last strong gone
    assert!(w.upgrade().unwrap().is_none()); // no longer upgradeable
    drop(w); // releases the last weak (frees the control block)
}

#[test]
fn shared_container_clone_shares() {
    let tmp = TempStack::new();
    let a = tmp.allocator();
    let stack = a.stack();

    // An `rc` block is duplicated by a refcount bump, not a deep copy.
    let rc = rc_sink(&a).unwrap();
    let rc2 = rc.try_clone().unwrap();
    let off1 = rc.handle().range().start();
    let off2 = rc2.handle().range().start();
    assert_eq!(off1, off2); // same block (shared)
    drop(rc);
    assert_eq!(rc2.handle().get_pod(stack).unwrap(), 7); // still alive
    drop(rc2);
}
