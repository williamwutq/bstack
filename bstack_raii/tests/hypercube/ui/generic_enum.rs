// A generic enum is not supported.
use bstack_raii::bstack_enum;
#[bstack_enum]
enum E<T> {
    A(T),
}
fn main() {}
