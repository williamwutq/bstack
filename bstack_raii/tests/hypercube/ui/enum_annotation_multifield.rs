// An ownership annotation is only allowed on a single-field tuple variant.
use bstack_raii::bstack_enum;
#[bstack_enum]
enum E {
    #[bstack_strong]
    T(u32, i8),
}
fn main() {}
