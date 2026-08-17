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

use proc_macro::TokenStream;

mod block;
mod cast;
mod emit;
mod util;
mod enum_;

/// `#[bstack_block]` — generate the on-disk layout and typed handle machinery.
///
/// # Arguments
///
/// `#[bstack_block(rc)]` / `#[bstack_block(rc, weak)]` select the refcount mode.
/// `tag = "…"` / `ctrl_tag = "…"` override the generated tags (see below).
/// `allow(overlong_tag)` / `allow(coerced_ref)` silence the corresponding
/// warnings (and a real `#[allow(deprecated)]` on the struct silences both,
/// since the warnings use the deprecation mechanism). All are optional and may
/// appear in any order.
///
/// # Generated items (for `struct X { .. }`)
///
/// * `struct X(BStackRange)` — the typed handle, and `struct XOnDisk`
///   (`#[repr(C, packed)]`, `Pod`) — the on-disk payload. `#[bstack_owned]` /
///   `#[bstack_strong]` / `#[bstack_weak]` / `#[bstack_ref]` fields lower to a
///   `u64` offset; un-annotated fields are stored inline (and must be `Pod`).
///   Wrapping a reference field in `Option<T>` makes it nullable (`0 == None`).
///   `(rc)` injects an inline `refcount`; `(rc, weak)` injects a `ctrl`
///   back-pointer and emits an `XOnDiskRef` control block.
/// * `impl BStackCast / BStackBlock / BStackDrop`, plus `BStackShared`
///   (`rc` / `rc, weak`), `BStackWeakable` (`rc, weak`), and `BStackMove`
///   (plain blocks — the `bstack_move!` target).
/// * Field accessors, `set_<field>` for weak fields, and a `new` constructor.
///
/// # EightCC tag generation
///
/// Each block's [`EightCC`](../bstack_raii/struct.EightCC.html) is an 8-byte tag
/// = a **readable ASCII prefix** over the first bytes, followed by the tail of a
/// **64-bit FNV-1a hash** of `crate_name ++ "\0" ++ type_name` (little-endian).
/// Every hash-tail byte has its high bit set, so it lands in the non-printable
/// range and reads as clearly-not-a-name in a hex dump. The hash keeps distinct
/// types apart even when their prefixes collide, and is deterministic (stable
/// across builds/versions) so it is safe as on-disk ABI.
///
/// The prefix is derived from the type name: initials of the camel-case words
/// (≥ 2 words), or the de-voweled single word, clamped to 2–5 bytes. Override it
/// with `tag = "PREFIX"` (0–8 bytes; fewer than 8 leaves room for hash, exactly
/// 8 is a fully manual tag, over 8 warns and truncates). The control block's tag
/// is the data tag with its prefix **lowercased**, or an explicit `ctrl_tag`.
#[proc_macro_attribute]
pub fn bstack_block(args: TokenStream, item: TokenStream) -> TokenStream {
    let input = syn::parse_macro_input!(item as syn::ItemStruct);
    match block::expand(args.into(), input) {
        Ok(ts) => ts.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

/// `#[bstack_enum]` — a tagged-union block.
///
/// Lowers an `enum` to a fixed-size block: a discriminant plus a payload area
/// sized to the largest variant. A variant is either:
///
/// * a **POD aggregate** — unit (`V`), an all-`Pod` tuple (`V(A, B, ..)`), or an
///   all-`Pod` struct (`V { x: A, .. }`); the fields are packed into the payload
///   in declaration order (read/written unaligned, so alignment is irrelevant),
///   with no ownership annotation; or
/// * an **annotated single-field tuple** `#[bstack_owned]` / `#[bstack_strong]` /
///   `#[bstack_weak]` / `#[bstack_ref]` `V(T)` — a `u64` offset to the child /
///   control block, released on teardown per the annotation.
///
/// All three modes are supported (`#[bstack_enum]`, `(rc)`, `(rc, weak)`).
/// Duplicate discriminant values are rejected (rustc's `E0081` cannot fire, since
/// the macro replaces the `enum`).
///
/// # Arguments
///
/// Accepts the same `rc` / `weak` / `tag = "…"` / `ctrl_tag = "…"` /
/// `allow(overlong_tag)` as [`macro@bstack_block`], plus **`repr(..)`** to fix
/// the discriminant width: `repr(u8|u16|u32|u64|i8|i16|i32|i64)`, or
/// `repr(aligned)` (== `repr(u64)`, so the 8-byte discriminant leaves the payload
/// 8-aligned and its on-disk refs get aligned writes). `usize` / `isize` are
/// rejected (bstack offsets are 64-bit). Without `repr`, the width is **inferred**
/// as the smallest integer type holding every variant's discriminant — honoring
/// explicit `= value` discriminants (Rust's rules: explicit, else previous + 1)
/// and choosing a **signed** type if any value is negative.
///
/// Generates:
/// * `struct E(BStackRange)` (the handle) and its `EOnDisk` payload.
/// * `enum EData` — the in-memory owned form: passed to `new` **and** returned by
///   `bstack_move!` (construction and destructuring are duals), and `enum EView`
///   — the read result (POD by value, owned/ref children as borrowed handles, a
///   weak variant upgraded to `Option<BStackRc>`).
/// * `impl BStackCast / BStackBlock / BStackDrop / BStackMove`, plus
///   `E::new(alloc, EData)`, `E::read(alloc) -> EView`, and `E::as_slice`.
///   `bstack_move!` and `bstack_cast!` work on enums as on structs.
///
/// An enum is always **referenced** (it is a block; store it as a field of a
/// struct — inline embedding is not supported).
#[proc_macro_attribute]
pub fn bstack_enum(args: TokenStream, item: TokenStream) -> TokenStream {
    let input = syn::parse_macro_input!(item as syn::ItemEnum);
    match enum_::expand_enum(args.into(), input) {
        Ok(ts) => ts.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

/// `bstack_move!(handle)` / `bstack_move!(owned, allocator)` — transfer every
/// field out of a block, freeing only the parent shell.
///
/// Reads each field's `BStackRef`/POD value from `XOnDisk` (capturing `ctrl`
/// refs for `(rc, weak)` / weak fields first), `dealloc_range`s the parent shell
/// only, then reconstructs typed handles and returns them as a
/// `Result<(..), io::Error>` tuple. Two forms select where the allocator comes
/// from:
///
/// * `bstack_move!(handle)` — for an allocator-carrying handle: a `BStackRc<X>`
///   (a `try_unwrap`, sole-owner only) or an `AutoDrop`-wrapped owned handle.
///   Dispatched through [`BStackMoveExpr`], inferring the impl from the type.
/// * `bstack_move!(owned, allocator)` — for a **bare** `BStackOwned<X>`, which
///   carries no allocator; the allocator is supplied explicitly (symmetric with
///   `owned.bstack_drop(allocator)`).
///
/// See RAII.md "`bstack_move!`".
#[proc_macro]
pub fn bstack_move(input: TokenStream) -> TokenStream {
    use syn::parse::Parser;
    use syn::punctuated::Punctuated;

    let parser = Punctuated::<syn::Expr, syn::Token![,]>::parse_terminated;
    let args = match parser.parse(input) {
        Ok(a) => a,
        Err(e) => return e.to_compile_error().into(),
    };
    let mut it = args.into_iter();
    let ts = match (it.next(), it.next(), it.next()) {
        // One arg: dispatch through the wrapper's own `BStackMoveExpr` (Rc or an
        // `AutoDrop`-wrapped owned handle), inferring the block impl from its type.
        (Some(expr), None, None) => {
            quote::quote!(::bstack_raii::BStackMoveExpr::bstack_move(#expr))
        }
        // Two args: a bare `BStackOwned<X>` plus its allocator.
        (Some(owned), Some(alloc), None) => {
            quote::quote!(::bstack_raii::BStackMove::bstack_move(#owned, #alloc))
        }
        _ => {
            return syn::Error::new(
                proc_macro2::Span::call_site(),
                "bstack_move! takes `handle` or `owned, allocator`",
            )
            .to_compile_error()
            .into();
        }
    };
    ts.into()
}

/// `bstack_cast!(expr as Target)` — type-checked handle conversion. The target
/// is given explicitly (a function-like macro can't read a `let` annotation) and
/// selects the direction:
///
/// * `owned as BStackOwnedSlice` — owned upcast (infallible).
/// * `slice as BStackOwned<X, _>` — owned downcast → `io::Result<Result<BStackOwned<X>, _>>`.
/// * `slice as X` — borrowed downcast off a `BStackSlice` → `io::Result<Option<X>>`.
///
/// The borrowed upcast is the generated `handle.as_slice(stack)` method.
#[proc_macro]
pub fn bstack_cast(input: TokenStream) -> TokenStream {
    match cast::expand(input.into()) {
        Ok(ts) => ts.into(),
        Err(e) => e.to_compile_error().into(),
    }
}
