// No pointer to an array: `Foreign<[T; N]>` (use `[Foreign<T>; N]`).
use bstack_raii::bstack_block;
#[bstack_block]
struct Leaf {
    v: u32,
}
#[bstack_block]
struct Holder {
    #[bstack_owned]
    link: bstack_raii::Foreign<[Leaf; 4]>,
}
fn main() {}
