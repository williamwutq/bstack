// No pointer to a `Vec`: `Foreign<Vec<T>>` (use `Vec<Foreign<T>>`).
use bstack_raii::bstack_block;
#[bstack_block]
struct Leaf {
    v: u32,
}
#[bstack_block]
struct Holder {
    #[bstack_owned]
    link: bstack_raii::Foreign<Vec<Leaf>>,
}
fn main() {}
