//! Operation: **atomicity** — a `replace_` never loses the *new* value. An
//! out-of-bounds array `replace_<f>_at` fails before touching the slot and hands the
//! value straight back through [`ReplaceError`]. (Mid-commit I/O-fault handback is
//! covered by the `#[cfg(feature = "fault-injection")]` unit tests in `src/tests.rs`,
//! which need bstack's fault hooks.)

use bstack_raii::BStackDrop;

use crate::common::TempStack;
use crate::fixtures::{Leaf, mut_sink};

#[test]
fn array_replace_out_of_bounds_hands_value_back() {
    let tmp = TempStack::new();
    let a = tmp.allocator();

    let h = mut_sink(&a).unwrap();
    let stray = Leaf::new(&a, 0).unwrap();
    match h.handle().replace_arr_at(&a, 9, stray) {
        Ok(_) => panic!("expected an out-of-bounds error"),
        // `value` is the intact new handle — never dropped into an orphan.
        Err(e) => e.value.unwrap().bstack_drop(&a).unwrap(),
    }
    h.bstack_drop(&a).unwrap();
}
