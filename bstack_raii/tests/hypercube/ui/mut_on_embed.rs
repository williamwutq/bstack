// `#[bstack_mut]` on an `#[embed]` field is not supported.
use bstack_raii::bstack_block;
#[bstack_block]
struct Leaf {
    v: u32,
}
#[bstack_block]
struct Holder {
    #[bstack_mut]
    #[embed]
    child: Leaf,
}
fn main() {}
