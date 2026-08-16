// `Foreign` in a tuple in a `Vec` — a foreign tuple field is fine, but not as a Vec element.
use bstack_raii::bstack_block;
#[bstack_block]
struct Leaf {
    v: u32,
}
#[bstack_block]
struct Holder {
    #[bstack_owned]
    v: Vec<(u32, bstack_raii::Foreign<Leaf>)>,
}
fn main() {}
