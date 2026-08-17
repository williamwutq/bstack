// `#[bstack_class]` on an enum — enum RTTI is a later phase.
use bstack_raii::bstack_class;
#[bstack_class]
enum X {
    A,
    B,
}
fn main() {}
