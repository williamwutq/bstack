// `#[embed]` combined with another ownership annotation.
use bstack_raii::bstack_block;
#[bstack_block]
struct X {
    #[embed]
    #[bstack_owned]
    f: u32,
}
fn main() {}
