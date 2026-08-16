// A `Foreign` field must carry an ownership annotation.
use bstack_raii::bstack_block;
#[bstack_block]
struct Leaf {
    v: u32,
}
#[bstack_block]
struct Holder {
    link: bstack_raii::Foreign<Leaf>,
}
fn main() {}
