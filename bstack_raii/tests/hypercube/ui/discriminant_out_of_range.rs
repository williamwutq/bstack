// A discriminant out of range for the chosen repr.
use bstack_raii::bstack_enum;
#[bstack_enum(repr(u8))]
enum E {
    A = 300,
}
fn main() {}
