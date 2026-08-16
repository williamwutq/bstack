//! Operation: **cast** — untyped ↔ typed via `as_slice` (upcast) and `bstack_cast!`
//! (tag-checked downcast).

use bstack::BStackAllocator;
use bstack_raii::{BStackDrop, bstack_cast};

use crate::common::TempStack;
use crate::fixtures::{Leaf, Shared};

#[test]
fn slice_downcast_is_tag_checked() {
    let tmp = TempStack::new();
    let a = tmp.allocator();
    let stack = a.stack();

    let leaf = Leaf::new(&a, 9).unwrap();
    let sl = leaf.handle().as_slice(stack);

    // Correct tag round-trips to the typed handle …
    assert_eq!(
        bstack_cast!(sl as Leaf)
            .unwrap()
            .unwrap()
            .get_v(stack)
            .unwrap(),
        9
    );
    // … a wrong tag is rejected (not UB — a clean `None`).
    assert!(bstack_cast!(sl as Shared).unwrap().is_none());

    leaf.bstack_drop(&a).unwrap();
}
