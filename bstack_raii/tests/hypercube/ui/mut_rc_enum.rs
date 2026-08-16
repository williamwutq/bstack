// Whole-value `#[bstack_mut]` on a shared (`rc`) enum is rejected.
use bstack_raii::bstack_enum;
#[bstack_enum(rc)]
#[bstack_mut]
enum E {
    A,
    B(u32),
}
fn main() {}
