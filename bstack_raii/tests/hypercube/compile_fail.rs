//! The **negative** cells of the cube — illegal macro inputs that must fail to
//! compile with the *right* error. Driven by `trybuild` over `tests/hypercube/ui/`,
//! so these live here rather than bloating `lib.rs` doctests. Each `ui/*.rs` has a
//! committed `.stderr` snapshot (regenerate with `TRYBUILD=overwrite`).

#[test]
fn ui() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/hypercube/ui/*.rs");
}
