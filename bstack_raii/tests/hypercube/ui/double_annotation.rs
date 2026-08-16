// Two ownership annotations on one field.
use bstack_raii::bstack_block;
#[bstack_block]
struct X {
    #[bstack_owned]
    #[bstack_ref]
    f: u32,
}
fn main() {}
