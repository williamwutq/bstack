// `#[bstack_block]` on an enum — should point at `#[bstack_enum]`.
use bstack_raii::bstack_block;
#[bstack_block]
enum X {
    A,
    B,
}
fn main() {}
