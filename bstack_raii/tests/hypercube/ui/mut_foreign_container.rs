// `#[bstack_mut]` on `Foreign` inside a container is not (yet) supported.
use bstack_raii::bstack_block;
#[bstack_block]
struct Leaf {
    v: u32,
}
#[bstack_block]
struct Holder {
    #[bstack_mut]
    #[bstack_owned]
    links: Vec<bstack_raii::Foreign<Leaf>>,
}
fn main() {}
