// `#[bstack_mut]` on an enum goes on the enum, not a variant.
use bstack_raii::bstack_enum;
#[bstack_enum]
enum E {
    #[bstack_mut]
    A(u32),
}
fn main() {}
