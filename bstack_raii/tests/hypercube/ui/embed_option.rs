// `#[embed]` does not support `Option`.
use bstack_raii::bstack_block;
#[bstack_block]
struct X {
    #[embed]
    f: Option<u32>,
}
fn main() {}
