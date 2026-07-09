//! Procedural macros for [`bstack_raii`](https://github.com/williamwutq/bstack).
//!
//! Three entry points, all documented in `RAII.md` at the repository root:
//!
//! * [`macro@bstack_block`] — attribute macro. Turns an ergonomic struct into a
//!   parallel `#[repr(C, packed)]` on-disk representation plus generated
//!   accessors, `BStackDrop`, `BStackCast`, and (for `(rc, weak)`)
//!   `BStackWeakable` impls.
//! * [`bstack_move`] — function-like macro. Destructures a `BStackOwned<X>`,
//!   transferring ownership of every field out as a tuple of typed handles.
//! * [`bstack_cast`] — function-like macro. Direction-inferring typed/untyped
//!   handle conversion.
//!
//! Everything below is a scaffold: the parsing/validation/codegen bodies are the
//! work to be filled in. The signatures and the emitted-shape contracts are
//! fixed so the runtime crate and downstream callers can be developed in
//! parallel.

use proc_macro::TokenStream;

/// `#[bstack_block]` — generate the on-disk layout and typed handle machinery.
///
/// Accepts optional mode arguments: `#[bstack_block]`, `#[bstack_block(rc)]`, or
/// `#[bstack_block(rc, weak)]`.
///
/// Must generate, for an input `struct X { .. }`:
/// 1. `struct XOnDisk` — `#[repr(C, packed)]`, `header: BlockHeader` first, then
///    each field lowered per its annotation:
///    * `#[bstack_owned]` / `#[bstack_strong]` / `#[bstack_weak]` /
///      `#[bstack_ref]` → `BStackRef<T>` (validate exactly one annotation on
///      each non-POD field).
///    * un-annotated field → stored inline; must be `bytemuck::Pod` (reject
///      otherwise at expansion time).
///    * `(rc)` injects `refcount: AtomicU64` after the header; `(rc, weak)`
///      instead injects `ctrl: BStackRef<XOnDiskRef>` and emits a separate
///      `struct XOnDiskRef` control block (`strong`, `weak`, back-pointer).
/// 2. Field accessor methods on `X` that `read_into` a buffer and read the field.
/// 3. `impl BStackDrop for X` — post-order: one child-handle `.bstack_drop(allocator)?`
///    per non-`#[bstack_ref]`, non-POD field, then `dealloc_range` of the block.
/// 4. `impl BStackCast for X` with an `EightCC` derived from the type name.
/// 5. `impl BStackWeakable for X { type Control = XOnDiskRef; }` only for
///    `(rc, weak)`.
#[proc_macro_attribute]
pub fn bstack_block(_args: TokenStream, item: TokenStream) -> TokenStream {
    // Scaffold: re-emit the input struct unchanged so downstream compiles.
    // TODO: parse args + fields, validate annotations, emit the items above.
    item
}

/// `bstack_move!(x)` — transfer every field out of a `BStackOwned<X>`.
///
/// Expands to a block that reads each field's `BStackRef`/POD value from
/// `XOnDisk` (capturing `ctrl` refs for `(rc, weak)` / weak fields first),
/// `dealloc_range`s the parent shell only, then reconstructs typed handles with
/// the allocator attached and returns them as a `Result<(..), io::Error>` tuple.
/// Callable only on `BStackOwned<X, A>`; not defined for `(rc)` / `(rc, weak)`
/// blocks. See RAII.md "`bstack_move!`".
#[proc_macro]
pub fn bstack_move(_input: TokenStream) -> TokenStream {
    // Scaffold: emit an unimplemented expression so any (currently nonexistent)
    // call site type-checks. TODO: implement the destructuring expansion.
    "::core::todo!(\"bstack_move! not yet implemented\")".parse().unwrap()
}

/// `bstack_cast!(handle)` — type-checked handle conversion, direction inferred
/// from the target type.
///
/// Emits `.cast_into::<X>()` / `.into_slice()` (owned) or `.cast_as::<X>()` /
/// `.as_slice()` (borrowed) depending on whether the target is a concrete
/// `#[bstack_block]` type (downcast) or a `BStackOwnedSlice` / `BStackSlice`
/// (upcast). See RAII.md "`bstack_cast!`".
#[proc_macro]
pub fn bstack_cast(_input: TokenStream) -> TokenStream {
    "::core::todo!(\"bstack_cast! not yet implemented\")".parse().unwrap()
}
