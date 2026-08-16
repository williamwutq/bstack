// `#[embed]` needs a block, not a tuple.
use bstack_raii::bstack_block;
#[bstack_block]
struct X {
    #[embed]
    f: (u8, u8),
}
fn main() {}
