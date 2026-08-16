// `repr(usize)` is not allowed (offsets are 64-bit; pick an explicit width).
use bstack_raii::bstack_enum;
#[bstack_enum(repr(usize))]
enum E {
    A,
}
fn main() {}
