// `#[bstack_enum]` on a struct — should point at `#[bstack_block]`.
use bstack_raii::bstack_enum;
#[bstack_enum]
struct X {
    a: u32,
}
fn main() {}
