# The Feature Checklist

Tracks implementation status for features in the `bstack` crate. All items in each section must be completed for a feature to be considered done. This checklist may be updated as development progresses.

1. API Design
    - [ ] Define new public types, functions, and traits.
    - [ ] API is consistent with the crate's design and follows Rust conventions.
    - [ ] All invariants are documented.
    - [ ] "Raw" accessors are provided and documented as such.
    - [ ] Methods do not panic unless justified; all panics are documented.
    - [ ] Invalid input is handled gracefully via `Result` or `Option`, not panics.
    - [ ] Methods that may compromise memory safety, whether that of BStack or the heap memory, are marked `unsafe` and documented.
    - [ ] Relevant traits are implemented (e.g., `Debug`, `Clone`, `Copy`, `PartialEq`, `Read`/`Write`).
    - [ ] Breaking changes are avoided where possible; if unavoidable, documented with migration guidance.
    - [ ] Forward compatibility and extensibility are considered.
    - [ ] If a plan exists, it addresses the **Design** section of the relevant issue, with all decisions resolved or documented.
    - [ ] Use the correct feature flags.
    - [ ] No additional dependencies.
2. Implementation
    - [ ] All invariants are maintained.
    - [ ] No logical errors or overlooked edge cases.
    - [ ] No hidden assumptions.
    - [ ] Safe abstractions are preferred where practical.
    - [ ] Public APIs are re-exported in `lib.rs` and/or `mod.rs` as appropriate.
    - [ ] Implementation is reviewed.
    - [ ] If a plan exists, it addresses the **Open Questions** section of the relevant issue, with all questions resolved or documented.
    - [ ] No TODOs, FIXMEs, `todo!` macros, or `unimplemented!` macros remain in the code.
    - [ ] Interoperability with existing BStack APIs is considered and tested.
    - [ ] If not a Rust only feature, C version is implemented and tested.
3. Safety
    - [ ] All `unsafe` code is justified, minimal, and well-documented.
    - [ ] Safety invariants are clearly stated in documentation for all `unsafe` items.
    - [ ] All methods are power-fail safe by default: interruption leaves BStack in a consistent state. Any exceptions or data-loss consequences (e.g., a block lost mid-allocation) are documented.
    - [ ] Thread safety is documented for all structs and methods; required synchronization is implemented.
    - [ ] In places where atomicity is relevant, document whether operations are atomic, and if not, what the implications are for concurrent usage.
4. Optimisation
    - [ ] Implementation is optimised for time complexity, memory usage, and minimal BStack operations.
    - [ ] Trade-offs from any optimisations are documented.
    - [ ] Batching is considered.
    - [ ] Suggestions for further optimisations are added to PLANNED.md if not implemented now.
5. Testing
    - [ ] Tests cover all functionality, edge cases, and error conditions.
    - [ ] Integration tests for overall behavior, if necessary.
    - [ ] New allocator implementations are added to `alloc_fuzz_tests.rs` and pass fuzz tests.
    - [ ] CI passes.
    - [ ] CodeQL passes without warnings.
6. Documentation
    - [ ] All public items document their purpose, parameters, return values, and any panics or side effects.
    - [ ] `cargo doc` builds without warnings.
    - [ ] Doc tests pass.
    - [ ] New feature flags have meaningful documentation.
    - [ ] Changelog entry added.
    - [ ] For breaking changes, concise migration guidance provided in the changelog.
    - [ ] README and `lib.rs` updated if the feature changes public-facing behavior or usage.
    - [ ] New files have a module-level doc comment describing their purpose.
    - [ ] Important new features have an example in `examples/` showing realistic usage.
    - [ ] New BStack usage patterns have a README section, `lib.rs` documentation, and an `examples/` entry.
    - [ ] Allocators and safety-sensitive methods include a markdown table. Example columns: name, atomicity, single-operation, and relevant notes.