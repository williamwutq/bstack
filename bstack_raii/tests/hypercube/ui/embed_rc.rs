// `#[embed]` of a reference-counted block strands its refcount (not embeddable).
use bstack_raii::bstack_block;
#[bstack_block(rc)]
struct RcChild {
    v: u32,
}
#[bstack_block]
struct Holder {
    #[embed]
    r: RcChild,
}
fn main() {}
