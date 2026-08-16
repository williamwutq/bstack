// A `Vec` field stores one descriptor: no `Vec<Vec<T>>`.
use bstack_raii::bstack_block;
#[bstack_block]
struct Leaf {
    v: u32,
}
#[bstack_block]
struct Holder {
    #[bstack_owned]
    xs: Vec<Vec<Leaf>>,
}
fn main() {}
