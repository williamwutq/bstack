// No pointer-to-a-pointer: `Foreign<Foreign<T>>`.
use bstack_raii::bstack_block;
#[bstack_block]
struct Leaf {
    v: u32,
}
#[bstack_block]
struct Holder {
    #[bstack_owned]
    link: bstack_raii::Foreign<bstack_raii::Foreign<Leaf>>,
}
fn main() {}
