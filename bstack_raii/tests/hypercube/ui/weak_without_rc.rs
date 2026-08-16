// `weak` requires `rc`.
use bstack_raii::bstack_block;
#[bstack_block(weak)]
struct X {
    f: u32,
}
fn main() {}
