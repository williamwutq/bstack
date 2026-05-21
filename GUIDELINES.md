# Design Guidelines

Design principles for the `bstack` crate. These complement [Rust's API design guidelines](https://rust-lang.github.io/api-guidelines/) and inform decisions about safety, API shape, and feature scope.

---

## When to mark a method `unsafe`

### Mark `unsafe fn` when:

- **The method can compromise memory safety** — of `BStack` itself or of heap memory — regardless of how it is called. This includes use-after-free, buffer overruns, heap corruption, or any violation of Rust's memory model.

- **The method can cause silent persistent corruption from an otherwise safe context.** The key qualifier is *from a safe context*: if reaching the dangerous state already required the caller to write an `unsafe` block elsewhere in the call chain, the consuming method need not be `unsafe fn`. The hazard window is already guarded.

- **The method can trigger undefined behavior.** Note: panicking is not undefined behavior. A method that panics on misuse does not need to be `unsafe fn` on that basis alone.

### Do not mark `unsafe fn` purely because:

- **The method can lose data.** Data loss — `pop`, `discard`, truncation — is inherent to I/O work and is always intentional at the call site. Operations that lose data by design are safe.

- **The method can be misused to corrupt data when called deliberately.** Intentional misuse is the caller's responsibility, not a safety obligation on the API. For example, taking a sub-slice of an allocation and passing it to `dealloc` may corrupt allocator metadata, but `subslice` itself is a deliberate, documented operation. The corruption is a consequence of the caller's choice, not of `subslice` being called in an otherwise safe context. For another example, taking a subslice of an allocated slice and writing to it will change the data of the underlying slice, but this is an expected consequence of writing to a slice and does not require `unsafe fn` on the slicing method. (A future improvement would be to make certain slices read-only or enforce borrow semantics at the type level.)

### Unsafe traits

Mark a trait `unsafe trait` when implementors must uphold invariants that the type system cannot express or verify — for example, atomicity or crash-safety guarantees that go beyond what any individual method signature can enforce. Traits whose full contract is expressed through their method documentation do not need to be `unsafe trait`.
