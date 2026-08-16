// Duplicate discriminant values (the macro replaces the enum, so E0081 can't fire).
use bstack_raii::bstack_enum;
#[bstack_enum]
enum E {
    A = 1,
    B = 1,
}
fn main() {}
