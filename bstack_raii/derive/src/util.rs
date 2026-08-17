//! Shared **analysis** primitives for the derives: ownership/mode classification,
//! container-shape predicates (`vec_inner`/`option_inner`/`foreign_inner`/…),
//! `Foreign` target validation, array-shape + nested-token primitives, attribute
//! parsing, and EightCC tag generation. The per-field/per-variant code emitters
//! live in [`crate::emit`].

use proc_macro2::{Span, TokenStream};
use quote::{format_ident, quote};
use syn::parse::Parser;
use syn::punctuated::Punctuated;
use syn::{Error, Expr, ExprLit, GenericArgument, Ident, Lit, Meta, PathArguments, Token, Type};

/// The block mode from the attribute arguments.
#[derive(Clone, Copy, PartialEq)]
pub(crate) enum Mode {
    /// `#[bstack_block]`
    Plain,
    /// `#[bstack_block(rc)]`
    Rc,
    /// `#[bstack_block(rc, weak)]`
    RcWeak,
}

/// One field's ownership classification.
#[derive(Clone, Copy, PartialEq)]
pub(crate) enum Kind {
    Owned,
    Strong,
    Weak,
    Ref,
    /// `#[embed]`: an exclusively-owned child block stored **inline** (its whole
    /// on-disk form, header and all), not as a `u64` offset.
    Embed,
    /// POD field stored inline.
    Pod,
}

/// A `Vec<T>` / `String` field: its element type (tokens) and whether it's a
/// `String` (so the constructor takes `&str`). Whether the elements are POD
/// (byte storage) or blocks (offset storage) is decided by the field's ownership
/// annotation, not by inspecting the element type.
pub(crate) struct VecInfo {
    pub(crate) elem: TokenStream,
    pub(crate) is_string: bool,
}

/// Whether `ty` is the `str` type.
pub(crate) fn is_str(ty: &Type) -> bool {
    matches!(ty, Type::Path(tp) if tp.path.segments.last().is_some_and(|s| s.ident == "str"))
}

/// Whether `ty` mentions any of the given (generic type-parameter) identifiers
/// anywhere in its token tree. Used to enforce that a generic parameter is only
/// ever used in a `#[bstack_ref]` field.
pub(crate) fn tokens_mention(ts: TokenStream, params: &[&Ident]) -> bool {
    ts.into_iter().any(|t| match t {
        proc_macro2::TokenTree::Ident(id) => params.iter().any(|p| **p == id),
        proc_macro2::TokenTree::Group(g) => tokens_mention(g.stream(), params),
        _ => false,
    })
}

pub(crate) fn type_mentions_any(ty: &Type, params: &[&Ident]) -> bool {
    tokens_mention(quote!(#ty), params)
}

/// Reject a *nested* inline reference array (`[[T; N]; M]`, …) whose flattened
/// length would be a product `N * (M)` referencing a const parameter — Rust bars
/// generic parameters in an array-length *operation* on stable (a single `[T; N]`
/// with a direct const `N` is fine). POD arrays keep the nested type verbatim, so
/// this applies only where the array is flattened. `dims` is outer→inner.
pub(crate) fn reject_nested_const_dims(
    dims: &[&Expr],
    const_params: &[&Ident],
    span: &Type,
) -> syn::Result<()> {
    if dims.len() > 1
        && !const_params.is_empty()
        && dims
            .iter()
            .any(|d| tokens_mention(quote!(#d), const_params))
    {
        return Err(Error::new_spanned(
            span,
            "[BSTACK0407] a nested array `[[T; N]; M]` with a const-parameter dimension is not supported: \
             its flattened length would be a const expression (`N * M`), which stable Rust \
             forbids from using a generic parameter. Use a single `[T; N]`, or make the \
             dimensions concrete.",
        ));
    }
    Ok(())
}

/// The element type `T` of a `Vec<T>`, if `ty` is a `Vec`. Used to reject
/// nested `Vec<Vec<T>>` / `Vec<String>` with a directed error.
pub(crate) fn vec_inner(ty: &Type) -> Option<&Type> {
    let Type::Path(tp) = ty else {
        return None;
    };
    let seg = tp.path.segments.last()?;
    if seg.ident != "Vec" {
        return None;
    }
    let PathArguments::AngleBracketed(ab) = &seg.arguments else {
        return None;
    };
    match ab.args.first()? {
        GenericArgument::Type(inner) => Some(inner),
        _ => None,
    }
}

/// Directed error for a double `Option` (`Option<Option<T>>`) anywhere in the
/// container nesting.
pub(crate) fn err_double_option(ty: &Type) -> Error {
    Error::new_spanned(
        ty,
        "[BSTACK0101] nested `Option<Option<T>>` is not supported: a field / `Vec` slot lowers a \
         single `Option` to the absent/`0` niche, and a second layer has nowhere to \
         live on disk. Model the states explicitly with a `#[bstack_enum]`, e.g. \
         `enum Slot { Missing, Empty, Present(T) }`.",
    )
}

/// Directed error for a `Vec` / `String` nested inside another `Vec`
/// (`Vec<Vec<T>>`, `Vec<String>`, `Vec<Option<Vec<T>>>`, …).
pub(crate) fn err_vec_in_vec(ty: &Type) -> Error {
    Error::new_spanned(
        ty,
        "[BSTACK0102] nested `Vec<Vec<T>>` / `Vec<String>` is not supported: a `Vec` field stores one \
         inline descriptor whose elements are a single leaf (POD or a block reference), \
         not another dynamically-sized region. Wrap the inner vector in an explicit \
         `#[bstack_block]` struct and store `Vec<ThatStruct>` (annotating the element \
         per its ownership).",
    )
}

/// Directed error for a tuple used as a `Vec` element (`Vec<(A, B)>`,
/// `Vec<[(A, B); N]>`, …).
pub(crate) fn err_tuple_in_vec(ty: &Type) -> Error {
    Error::new_spanned(
        ty,
        "[BSTACK0103] a tuple is not supported as a `Vec` element: a `Vec` element must be a single \
         leaf — POD, or a block reference — and a tuple (a POD one has no `Vec` layout, \
         a `(ref, pod)` one cannot be split into offset + inline bytes) is neither. Wrap \
         it in a named `#[bstack_block]` struct and store `Vec<ThatStruct>` (annotating \
         each reference field inside it, leaving POD fields plain).",
    )
}

/// Validate the `Vec` / `Option` nesting of a field type, outermost-first. A
/// field allows at most one leading `Option` (the absent niche) around a `Vec`
/// or a leaf; a `Vec` element allows at most one `Option` around a leaf. Any
/// deeper `Vec`-in-`Vec` or `Option`-in-`Option` is rejected with a directed
/// error naming the first offending construct. Leaves (POD, blocks, arrays,
/// tuples) end the walk.
pub(crate) fn check_container_nesting(ty: &Type) -> syn::Result<()> {
    // Field top: peel at most one `Option`, then validate the bare type.
    if let Some(inner) = option_inner(ty) {
        if option_inner(inner).is_some() {
            return Err(err_double_option(ty));
        }
        return check_bare(inner);
    }
    check_bare(ty)
}

/// A "bare" (no leading `Option` to peel) type: a `Vec` whose element must be a
/// leaf-or-`Option<leaf>`, `String`, or a leaf.
pub(crate) fn check_bare(ty: &Type) -> syn::Result<()> {
    if let Some(elem) = vec_inner(ty) {
        return check_vec_elem(elem);
    }
    Ok(())
}

/// A `Vec` element: a leaf, optionally an array `[..; N]` of leaves, optionally
/// wrapped in exactly one `Option`. A `Vec` / `String` in leaf position is
/// `Vec<Vec>` (`Vec<[Vec<T>; N]>` included — arrays are peeled first); an
/// `Option<Option>` is a double option; an `Option<Vec>` is again a nested `Vec`.
pub(crate) fn check_vec_elem(ty: &Type) -> syn::Result<()> {
    // A `Vec` element may itself be a (nested) array of leaves; peel the array
    // layers and validate the innermost element.
    let mut ty = ty;
    while let Type::Array(a) = ty {
        ty = &a.elem;
    }
    if let Some(inner) = option_inner(ty) {
        if option_inner(inner).is_some() {
            return Err(err_double_option(ty));
        }
        if vec_info(inner).is_some() {
            return Err(err_vec_in_vec(ty));
        }
        if let Type::Tuple(_) = inner {
            return Err(err_tuple_in_vec(inner));
        }
        return Ok(());
    }
    if vec_info(ty).is_some() {
        return Err(err_vec_in_vec(ty));
    }
    if let Type::Tuple(_) = ty {
        return Err(err_tuple_in_vec(ty));
    }
    Ok(())
}

/// Detect `Vec<T>` / `String` field types.
pub(crate) fn vec_info(ty: &Type) -> Option<VecInfo> {
    let Type::Path(tp) = ty else {
        return None;
    };
    let seg = tp.path.segments.last()?;
    if seg.ident == "String" {
        return Some(VecInfo {
            elem: quote!(u8),
            is_string: true,
        });
    }
    if seg.ident == "Vec"
        && let PathArguments::AngleBracketed(ab) = &seg.arguments
        && let Some(GenericArgument::Type(inner)) = ab.args.first()
    {
        return Some(VecInfo {
            elem: quote!(#inner),
            is_string: false,
        });
    }
    None
}

/// Return `Some(Inner)` if `ty` is `Option<Inner>`.
pub(crate) fn option_inner(ty: &Type) -> Option<&Type> {
    let Type::Path(tp) = ty else {
        return None;
    };
    let seg = tp.path.segments.last()?;
    if seg.ident != "Option" {
        return None;
    }
    let PathArguments::AngleBracketed(ab) = &seg.arguments else {
        return None;
    };
    match ab.args.first()? {
        GenericArgument::Type(inner) => Some(inner),
        _ => None,
    }
}

/// Validate a `Foreign<T>` element/field's annotation + target: annotation required
/// (`owned/strong/weak/ref`, not POD/embed), target must be a bstack block (assertion
/// pushed into `wrapper_defs`), and `Foreign<Option<T>>` rejected. `what` names the
/// construct for error messages (e.g. "`Vec<Foreign<T>>`"). Shared by the vector and
/// array Foreign branches.
pub(crate) fn validate_foreign_target(
    kind: Kind,
    ftarget: &Type,
    span: &Type,
    what: &str,
    assert_name: Ident,
    emit_assert: bool,
    wrapper_defs: &mut Vec<TokenStream>,
) -> syn::Result<()> {
    match kind {
        Kind::Owned | Kind::Strong | Kind::Weak | Kind::Ref => {}
        Kind::Pod => {
            return Err(Error::new_spanned(
                span,
                format!(
                    "[BSTACK0302] {what} needs an ownership annotation naming the target's kind \
                     (`#[bstack_owned/strong/weak/ref]`); a bare foreign pointer targets a block"
                ),
            ));
        }
        Kind::Embed => {
            return Err(Error::new_spanned(
                span,
                "[BSTACK0301] `Foreign<T>` is a pointer and cannot be `#[embed]`ed",
            ));
        }
    }
    reject_bad_foreign_target(ftarget, span, what)?;
    // The `T: BStackBlock` check is a non-generic `const`, which can't name a struct
    // type parameter. For a generic target the bound is enforced instead through the
    // generated impls' where-clauses (see the `Usage`/`aug_generics` machinery), so
    // the caller passes `emit_assert = false`.
    if emit_assert {
        wrapper_defs.push(quote! {
            #[doc(hidden)]
            const _: fn() = {
                fn #assert_name<__T: ::bstack_raii::BStackBlock>() {}
                #assert_name::<#ftarget>
            };
        });
    }
    Ok(())
}

/// Reject a `Foreign<T>` whose target `T` is not a plain bstack block — the
/// **"no double bstack pointer"** rule. A `Foreign` is a cross-file *pointer to a
/// block*, so `T` must be a `#[bstack_block]`, never:
///
/// * **another `Foreign`** (`Foreign<Foreign<U>>`) — a pointer to a pointer;
/// * a **nullable pointer** (`Foreign<Option<Foreign<U>>>`) — a double pointer via
///   `Option`; or a plain `Foreign<Option<U>>` (nullability belongs on the *field*
///   as `Option<Foreign<U>>`);
/// * a **container** (`Foreign<Vec<U>>` / `Foreign<String>` / `Foreign<[U; N]>`) — a
///   pointer to a collection;
/// * a **tuple**.
///
/// This is deliberately **not** the `Vec<Vec<T>>`-style container-nesting rule: a
/// `Foreign` is a pointer, so a collection *of* pointers is fine — `Vec<Foreign<T>>`
/// and `[Foreign<T>; N]` are allowed. It is a pointer *to* a collection/pointer that
/// is barred. In every rejected case the fix is to bridge with an explicit
/// `#[bstack_block]` struct wrapping the offending inner type and point a `Foreign` at
/// *that*.
pub(crate) fn reject_bad_foreign_target(
    ftarget: &Type,
    span: &Type,
    what: &str,
) -> syn::Result<()> {
    let bridge = "bridge it inside an explicit `#[bstack_block]` struct and point the \
                  `Foreign` at that struct";
    if foreign_inner(ftarget).is_some() {
        return Err(Error::new_spanned(
            span,
            format!(
                "[BSTACK0303] {what}: a `Foreign` cannot point at another `Foreign` — a pointer to a \
                 pointer is not allowed. {bridge}. (A collection OF pointers such as \
                 `Vec<Foreign<T>>` / `[Foreign<T>; N]` IS allowed; it is a pointer TO a \
                 pointer/collection that is not.)"
            ),
        ));
    }
    if let Some(inner) = option_inner(ftarget) {
        if foreign_inner(inner).is_some() {
            return Err(Error::new_spanned(
                span,
                format!(
                    "[BSTACK0304] {what}: `Foreign<Option<Foreign<T>>>` is a double `Foreign` (a pointer to a \
                     nullable pointer). {bridge}."
                ),
            ));
        }
        return Err(Error::new_spanned(
            span,
            format!(
                "[BSTACK0305] {what}: use `Option<Foreign<T>>` for a nullable foreign pointer, not \
                 `Foreign<Option<T>>` — nullability belongs on the field/element, and a null \
                 element is a `Foreign` with offset 0, not a pointer to a nullable value."
            ),
        ));
    }
    if vec_info(ftarget).is_some() || is_str(ftarget) {
        return Err(Error::new_spanned(
            span,
            format!(
                "[BSTACK0306] {what}: a `Foreign` target must be a `#[bstack_block]`, not a `Vec` / `String`. \
                 `Vec<Foreign<T>>` (a vector OF pointers) is allowed, but `Foreign<Vec<T>>` (a \
                 pointer TO a vector) is not — {bridge}."
            ),
        ));
    }
    if let Type::Array(_) = ftarget {
        return Err(Error::new_spanned(
            span,
            format!(
                "[BSTACK0307] {what}: a `Foreign` target must be a `#[bstack_block]`, not an array. \
                 `[Foreign<T>; N]` (an array OF pointers) is allowed, but `Foreign<[T; N]>` is \
                 not — {bridge}."
            ),
        ));
    }
    if let Type::Tuple(_) = ftarget {
        return Err(Error::new_spanned(
            span,
            format!(
                "[BSTACK0308] {what}: a `Foreign` target must be a `#[bstack_block]`, not a tuple — {bridge}."
            ),
        ));
    }
    Ok(())
}

/// Find the `Foreign<T>` **target** `T` inside a field type, digging through the
/// field-level `Option`, a `Vec` (and its per-element `Option`), and an array (nested,
/// with per-element `Option`) — the shapes the foreign scalar / vec / array branches
/// accept. `None` if the field holds no `Foreign`. Used to compute generic bounds for
/// a foreign field's target parameter.
pub(crate) fn field_foreign_target(ty: &Type) -> Option<&Type> {
    // Field-level `Option<..>`.
    let t = option_inner(ty).unwrap_or(ty);
    // Scalar `Foreign<X>`.
    if let Some(x) = foreign_inner(t) {
        return Some(x);
    }
    // `Vec<Foreign<X>>` / `Vec<Option<Foreign<X>>>`.
    if let Some(ve) = vec_inner(t) {
        let ve = option_inner(ve).unwrap_or(ve);
        if let Some(x) = foreign_inner(ve) {
            return Some(x);
        }
    }
    // `[Foreign<X>; N]` (nested / per-element `Option`).
    if let Type::Array(_) = t {
        let mut cur = t;
        while let Type::Array(a) = cur {
            cur = &a.elem;
        }
        let cur = option_inner(cur).unwrap_or(cur);
        if let Some(x) = foreign_inner(cur) {
            return Some(x);
        }
    }
    // `(.., Foreign<X>, ..)` — a tuple element (POD / Foreign mix). Returns the first
    // foreign element's target (used for the "supported position" guard; the tuple
    // branch validates every foreign element and requires concrete targets).
    if let Type::Tuple(tup) = t {
        for e in &tup.elems {
            let e = option_inner(e).unwrap_or(e);
            if let Some(x) = foreign_inner(e) {
                return Some(x);
            }
        }
    }
    None
}

/// Every `Foreign<T>` **target** `T` reachable in a field type — digging through the
/// field-level `Option`, `Vec` (+ element `Option`), array (nested + element
/// `Option`), and tuple (each element). Used to infer generic bounds: a type param
/// that is a foreign target is a *block reference in its own file*, and every foreign
/// target's kind bound follows the field annotation.
pub(crate) fn foreign_targets_in(ty: &Type) -> Vec<&Type> {
    let mut out = Vec::new();
    collect_foreign_targets(ty, &mut out);
    out
}

pub(crate) fn collect_foreign_targets<'a>(ty: &'a Type, out: &mut Vec<&'a Type>) {
    let t = option_inner(ty).unwrap_or(ty);
    if let Some(x) = foreign_inner(t) {
        out.push(x);
        return;
    }
    if let Some(ve) = vec_inner(t) {
        collect_foreign_targets(ve, out);
        return;
    }
    if let Type::Array(_) = t {
        let mut cur = t;
        while let Type::Array(a) = cur {
            cur = &a.elem;
        }
        collect_foreign_targets(cur, out);
        return;
    }
    if let Type::Tuple(tup) = t {
        for e in &tup.elems {
            collect_foreign_targets(e, out);
        }
    }
}

/// Peek `Foreign<T>` → `T`: a cross-file wide-pointer field.
pub(crate) fn foreign_inner(ty: &Type) -> Option<&Type> {
    let Type::Path(tp) = ty else {
        return None;
    };
    let seg = tp.path.segments.last()?;
    if seg.ident != "Foreign" {
        return None;
    }
    let PathArguments::AngleBracketed(ab) = &seg.arguments else {
        return None;
    };
    match ab.args.first()? {
        GenericArgument::Type(inner) => Some(inner),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Nested fixed-size arrays `[[.. [T; N0]; N1]..; Nk]` of block references.
//
// A block-reference array — at any nesting depth — is stored **flat** on disk /
// in an enum payload as `[u64; N0*N1*..*Nk]` (row-major), one `u64` offset per
// leaf. The runtime rebuilds / consumes the nested `[[..]; ..]` shape of handles
// through the recursive helpers below, driven by a single flat counter `__k`.
// Every leaf may be an `Option<T>` (per-element nullability, `0` == `None`); an
// `Option` wrapping a *whole* sub-array is rejected (only whole-field
// `Option<[T;N]>` was ever a niche, and that stays unsupported).
// ---------------------------------------------------------------------------

/// Peel a (possibly nested) `[[..]; ..]` array type into its dimensions
/// (outer→inner) plus the innermost element and whether that element is a
/// per-element `Option<T>`. Errors if an `Option` wraps a non-leaf sub-array.
pub(crate) fn array_shape(ty: &Type) -> syn::Result<(Vec<&Expr>, &Type, bool)> {
    let mut dims: Vec<&Expr> = Vec::new();
    let mut cur = ty;
    while let Type::Array(a) = cur {
        dims.push(&a.len);
        cur = &a.elem;
    }
    let (leaf, nullable) = match option_inner(cur) {
        Some(inner) => (inner, true),
        None => (cur, false),
    };
    if let Type::Array(_) = leaf {
        return Err(Error::new_spanned(
            cur,
            "[BSTACK0104] `Option` wrapping a whole sub-array is not supported; move the \
             `Option` onto the leaf element, e.g. `[[Option<T>; N]; M]`",
        ));
    }
    Ok((dims, leaf, nullable))
}

/// The product `N0 * N1 * .. * Nk` of the dimensions as a `usize` const
/// expression (`1usize` when empty) — the flat element count.
pub(crate) fn dims_prod(dims: &[&Expr]) -> TokenStream {
    if dims.is_empty() {
        return quote!(1usize);
    }
    // A SINGLE dimension is emitted bare (`N`, not `(N)`): as an array length a
    // bare const parameter is legal on stable, whereas any operation — including a
    // parenthesised or multiplied one — is not. So `[T; N]` (single, const `N`)
    // works; nested `[[T; N]; M]` folds to `N * (M)`, which is only legal when the
    // dimensions are concrete (a const-generic nested array is a stable-Rust
    // limitation, surfacing as a const-operation error).
    let first = dims[0];
    let mut t = quote!(#first);
    for d in &dims[1..] {
        t = quote!(#t * (#d));
    }
    t
}

/// The nested handle-array type `[[.. [leaf; N0]..]; Nk]` for the given
/// dimensions (outer→inner) around a leaf token type.
pub(crate) fn nested_ty(dims: &[&Expr], leaf: &TokenStream) -> TokenStream {
    if dims.is_empty() {
        return leaf.clone();
    }
    let d = dims[0];
    let inner = nested_ty(&dims[1..], leaf);
    quote!([#inner; #d])
}

/// Build the nested array value by reading each leaf in flat, row-major order.
/// `leaf_read(k)` yields the leaf value expression for flat index ident `k`
/// (which it must NOT advance — the engine advances it). May be fallible (the
/// leaf expression may use `?`); the enclosing context must return `Result`.
pub(crate) fn nested_build(
    dims: &[&Expr],
    leaf_ty: &TokenStream,
    leaf_read: &dyn Fn(&Ident) -> TokenStream,
) -> TokenStream {
    let k = format_ident!("__k");
    let body = nested_build_inner(dims, leaf_ty, 0, &k, leaf_read);
    quote!({ let mut #k = 0usize; #body })
}

pub(crate) fn nested_build_inner(
    dims: &[&Expr],
    leaf_ty: &TokenStream,
    depth: usize,
    k: &Ident,
    leaf_read: &dyn Fn(&Ident) -> TokenStream,
) -> TokenStream {
    if dims.is_empty() {
        let val = leaf_read(k);
        return quote!({ let __e = #val; #k += 1; __e });
    }
    let d = dims[0];
    let rest = &dims[1..];
    let vv = format_ident!("__bv{depth}");
    let inner_ty = nested_ty(rest, leaf_ty);
    let body = nested_build_inner(rest, leaf_ty, depth + 1, k, leaf_read);
    quote!({
        let mut #vv = ::std::vec::Vec::with_capacity(#d);
        for _ in 0usize..(#d) {
            #vv.push(#body);
        }
        match <[#inner_ty; #d]>::try_from(#vv) {
            ::std::result::Result::Ok(__a) => __a,
            ::std::result::Result::Err(_) => unreachable!(),
        }
    })
}

/// Consume the nested array `val`, invoking `leaf_write(k, leaf)` for each leaf
/// in flat, row-major order (`k` is the flat-index ident, `leaf` the moved
/// element binding). The engine advances `k`.
pub(crate) fn nested_consume(
    dims: &[&Expr],
    val: &TokenStream,
    leaf_write: &dyn Fn(&Ident, &Ident) -> TokenStream,
) -> TokenStream {
    let k = format_ident!("__k");
    let body = nested_consume_inner(dims, val, 0, &k, leaf_write);
    quote!({ let mut #k = 0usize; #body })
}

pub(crate) fn nested_consume_inner(
    dims: &[&Expr],
    val: &TokenStream,
    depth: usize,
    k: &Ident,
    leaf_write: &dyn Fn(&Ident, &Ident) -> TokenStream,
) -> TokenStream {
    if dims.is_empty() {
        let leaf = format_ident!("__leaf");
        let w = leaf_write(k, &leaf);
        return quote!({ let #leaf = #val; #w #k += 1; });
    }
    let rest = &dims[1..];
    let cv = format_ident!("__cn{depth}");
    let inner = nested_consume_inner(rest, &quote!(#cv), depth + 1, k, leaf_write);
    quote!(for #cv in #val { #inner })
}

/// Generate the reader method for one field. `nullable` (an `Option<_>` field)
/// Parsed `#[bstack_block(...)]` / `#[bstack_enum(...)]` arguments.
pub(crate) struct Attr {
    pub(crate) mode: Mode,
    /// Explicit data-block tag prefix (`tag = "..."`).
    pub(crate) tag: Option<String>,
    /// Explicit control-block tag prefix (`ctrl_tag = "..."`).
    pub(crate) ctrl_tag: Option<String>,
    /// Suppress the overlong-tag warning (`allow(overlong_tag)`).
    pub(crate) allow_overlong: bool,
    /// Suppress the reference-coercion warning (`allow(coerced_ref)`).
    pub(crate) allow_coerced_ref: bool,
    /// `#[bstack_enum(repr(..))]`: the discriminant integer type name (e.g.
    /// `"u16"`), with `aligned` normalized to `"u64"`. Enum-only.
    pub(crate) repr: Option<String>,
}

/// Parse `rc`, `weak`, `tag = "..."`, `ctrl_tag = "..."`,
/// `allow(overlong_tag | coerced_ref | deprecated)`, and (enums)
/// `repr(u8|u16|u32|u64|i8|i16|i32|i64|aligned)` in any order.
pub(crate) fn parse_attr(attr: TokenStream) -> syn::Result<Attr> {
    let (mut rc, mut weak) = (false, false);
    let mut tag = None;
    let mut ctrl_tag = None;
    let mut allow_overlong = false;
    let mut allow_coerced_ref = false;
    let mut repr = None;

    if !attr.is_empty() {
        let metas = Punctuated::<Meta, Token![,]>::parse_terminated.parse2(attr)?;
        for meta in metas {
            match &meta {
                Meta::Path(p) => match ident_of(p).as_deref() {
                    Some("rc") => rc = true,
                    Some("weak") => weak = true,
                    _ => return Err(Error::new_spanned(&meta, unknown_opt())),
                },
                Meta::NameValue(nv) => {
                    let value = match &nv.value {
                        Expr::Lit(ExprLit {
                            lit: Lit::Str(s), ..
                        }) => s.value(),
                        other => {
                            return Err(Error::new_spanned(other, "[BSTACK0004] expected a string literal"));
                        }
                    };
                    match ident_of(&nv.path).as_deref() {
                        Some("tag") => tag = Some(value),
                        Some("ctrl_tag") => ctrl_tag = Some(value),
                        _ => return Err(Error::new_spanned(&meta, unknown_opt())),
                    }
                }
                // `repr(u16)` / `repr(aligned)` — the enum discriminant width.
                Meta::List(list) if list.path.is_ident("repr") => {
                    let r: Ident = list.parse_args().map_err(|_| {
                        Error::new_spanned(
                            list,
                            "[BSTACK0005] expected `repr(u8|u16|u32|u64|i8|i16|i32|i64|aligned)`",
                        )
                    })?;
                    repr = Some(match r.to_string().as_str() {
                        // `aligned` == `u64`: an 8-byte discriminant leaves the
                        // payload 8-aligned, so its on-disk refs get aligned writes.
                        "aligned" => "u64".to_string(),
                        name @ ("u8" | "u16" | "u32" | "u64" | "i8" | "i16" | "i32" | "i64") => {
                            name.to_string()
                        }
                        "usize" | "isize" => {
                            return Err(Error::new_spanned(
                                &r,
                                "[BSTACK0006] `repr(usize)` / `repr(isize)` are not allowed — bstack offsets \
                                 are 64-bit, so pick an explicit width (e.g. `repr(u64)`)",
                            ));
                        }
                        _ => {
                            return Err(Error::new_spanned(
                                &r,
                                "[BSTACK0005] expected `u8|u16|u32|u64|i8|i16|i32|i64|aligned`",
                            ));
                        }
                    });
                }
                // `allow(overlong_tag, coerced_ref, deprecated)` — suppress warnings.
                Meta::List(list) if list.path.is_ident("allow") => {
                    let lints =
                        list.parse_args_with(Punctuated::<Ident, Token![,]>::parse_terminated)?;
                    for lint in lints {
                        match lint.to_string().as_str() {
                            "overlong_tag" => allow_overlong = true,
                            "coerced_ref" => allow_coerced_ref = true,
                            // The warnings use the `deprecated` mechanism, so
                            // `allow(deprecated)` covers all of them.
                            "deprecated" => {
                                allow_overlong = true;
                                allow_coerced_ref = true;
                            }
                            _ => {
                                return Err(Error::new_spanned(
                                    &lint,
                                    "[BSTACK0007] expected `overlong_tag`, `coerced_ref`, or `deprecated`",
                                ));
                            }
                        }
                    }
                }
                _ => return Err(Error::new_spanned(&meta, unknown_opt())),
            }
        }
    }

    let mode = match (rc, weak) {
        (false, false) => Mode::Plain,
        (true, false) => Mode::Rc,
        (true, true) => Mode::RcWeak,
        (false, true) => {
            return Err(Error::new(
                Span::call_site(),
                "[BSTACK0002] `weak` requires `rc` (use `rc, weak`)",
            ));
        }
    };
    Ok(Attr {
        mode,
        tag,
        ctrl_tag,
        allow_overlong,
        allow_coerced_ref,
        repr,
    })
}

pub(crate) fn ident_of(path: &syn::Path) -> Option<String> {
    path.get_ident().map(|i| i.to_string())
}

/// Whether a struct attribute is `#[allow(.., deprecated, ..)]`.
pub(crate) fn is_allow_deprecated(attr: &syn::Attribute) -> bool {
    attr.path().is_ident("allow")
        && attr
            .parse_args_with(Punctuated::<Ident, Token![,]>::parse_terminated)
            .is_ok_and(|lints| lints.iter().any(|l| l == "deprecated"))
}

pub(crate) fn unknown_opt() -> &'static str {
    "[BSTACK0003] expected `rc`, `weak`, `tag = \"...\"`, `ctrl_tag = \"...\"`, `allow(...)`, or (enums) `repr(...)`"
}

// ---------------------------------------------------------------------------
// EightCC tag generation
//
// An 8-byte tag = a readable ASCII prefix (2–5 auto, or a `tag =` override) over
// the first N bytes, followed by the tail of a 64-bit FNV-1a hash of
// `crate_name ++ "\0" ++ type_name`. Every tail byte has its high bit set so it
// lands in the non-printable range and can't be mistaken for the prefix. The
// control-block tag is the same, with the prefix lowercased. See the
// `#[bstack_block]` docs.
// ---------------------------------------------------------------------------

/// The value passed to `EightCC::new([..])` plus whether the prefix was longer
/// than 8 bytes (and hence truncated).
pub(crate) struct Tag {
    pub(crate) bytes: [u8; 8],
    pub(crate) truncated: bool,
}

pub(crate) fn fnv1a64(s: &str) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in s.bytes() {
        h ^= b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

/// Compose a tag from a hash and a readable prefix. Prefix bytes overwrite the
/// (high-bit-set) hash bytes from the front; > 8 prefix bytes are truncated.
pub(crate) fn build_tag(hash: u64, prefix: &[u8]) -> Tag {
    let mut bytes = hash.to_le_bytes();
    for b in bytes.iter_mut() {
        *b |= 0x80;
    }
    let truncated = prefix.len() > 8;
    let n = prefix.len().min(8);
    bytes[..n].copy_from_slice(&prefix[..n]);
    Tag { bytes, truncated }
}

pub(crate) fn is_ascii_vowel(b: u8) -> bool {
    matches!(
        b.to_ascii_uppercase(),
        b'A' | b'E' | b'I' | b'O' | b'U' | b'Y'
    )
}

/// Split a type name into words on camel-case boundaries and separators.
pub(crate) fn split_words(name: &str) -> Vec<String> {
    let chars: Vec<char> = name.chars().collect();
    let mut words = Vec::new();
    let mut cur = String::new();
    for (i, &c) in chars.iter().enumerate() {
        if !c.is_alphanumeric() {
            if !cur.is_empty() {
                words.push(std::mem::take(&mut cur));
            }
            continue;
        }
        let boundary = !cur.is_empty()
            && ((c.is_uppercase() && chars[i - 1].is_lowercase())
                || (c.is_uppercase()
                    && chars[i - 1].is_uppercase()
                    && chars.get(i + 1).is_some_and(|n| n.is_lowercase())));
        if boundary {
            words.push(std::mem::take(&mut cur));
        }
        cur.push(c);
    }
    if !cur.is_empty() {
        words.push(cur);
    }
    words
}

/// Auto-derive a 2–5 byte uppercase prefix from a type name: initials of the
/// words if there are ≥ 2, else the de-voweled single word.
pub(crate) fn auto_prefix(name: &str) -> Vec<u8> {
    let words = split_words(name);
    let prefix: Vec<u8> = if words.len() >= 2 {
        words
            .iter()
            .filter_map(|w| w.bytes().next())
            .map(|b| b.to_ascii_uppercase())
            .take(5)
            .collect()
    } else {
        let letters: Vec<u8> = words
            .first()
            .map(|w| {
                w.bytes()
                    .filter(u8::is_ascii_alphanumeric)
                    .map(|b| b.to_ascii_uppercase())
                    .collect()
            })
            .unwrap_or_default();
        let mut v = Vec::new();
        for (i, &b) in letters.iter().enumerate() {
            // Keep the first letter always; drop vowels from the rest.
            if i == 0 || !is_ascii_vowel(b) {
                v.push(b);
            }
            if v.len() == 5 {
                break;
            }
        }
        // Fall back to the first two letters if de-voweling left < 2.
        if v.len() < 2 {
            v = letters.into_iter().take(2).collect();
        }
        v
    };
    prefix
}

/// Emit `::bstack_raii::EightCC::new([..])` from tag bytes.
pub(crate) fn eightcc_expr(bytes: &[u8; 8]) -> TokenStream {
    let bytes = bytes.iter();
    quote!(::bstack_raii::EightCC::new([#(#bytes),*]))
}

/// Classify by ownership annotation among a set of attributes (a field's or an
/// enum variant's). No annotation => `Pod`.
pub(crate) fn classify_attrs(attrs: &[syn::Attribute]) -> syn::Result<Kind> {
    let mut found: Option<Kind> = None;
    for attr in attrs {
        let Some(id) = attr.path().get_ident() else {
            continue;
        };
        let kind = match id.to_string().as_str() {
            "bstack_owned" => Kind::Owned,
            "bstack_strong" => Kind::Strong,
            "bstack_weak" => Kind::Weak,
            "bstack_ref" => Kind::Ref,
            "embed" => Kind::Embed,
            _ => continue,
        };
        if found.is_some() {
            return Err(Error::new_spanned(
                attr,
                "[BSTACK0001] at most one bstack ownership annotation is allowed here",
            ));
        }
        found = Some(kind);
    }
    Ok(found.unwrap_or(Kind::Pod))
}

/// Classify a struct field by its ownership annotation.
pub(crate) fn classify(field: &syn::Field) -> syn::Result<Kind> {
    classify_attrs(&field.attrs)
}

/// Whether a field is annotated `#[bstack_mut]`, opting it into a generated
/// `set_<field>` (currently honoured for POD and `#[bstack_ref]` fields).
pub(crate) fn is_bstack_mut(attrs: &[syn::Attribute]) -> bool {
    attrs
        .iter()
        .filter_map(|a| a.path().get_ident())
        .any(|id| id == "bstack_mut")
}

// ===========================================================================
// #[bstack_enum] — a tagged union block
// ===========================================================================
//
// An enum lowers to a fixed-size block: a 1-byte discriminant plus a payload
// area sized to the largest variant. Each variant is either a unit (no payload),
// a POD newtype `V(P)` (bytes stored inline), or an annotated newtype
// `#[bstack_owned]`/`#[bstack_ref]` `V(T)` (a `u64` offset to a child block).
// Construction goes through a generated `EData` input enum + `E::new`; reading
// through a generated `EView` + `E::read`; teardown matches the discriminant and
// frees the owned child, if any.

/// Parse a variant's explicit discriminant expression (`= <int>` / `= -<int>`)
/// into an `i128`.
pub(crate) fn parse_disc_expr(expr: &Expr) -> syn::Result<i128> {
    match expr {
        Expr::Lit(ExprLit {
            lit: Lit::Int(li), ..
        }) => li.base10_parse::<i128>(),
        Expr::Unary(syn::ExprUnary {
            op: syn::UnOp::Neg(_),
            expr,
            ..
        }) => match &**expr {
            Expr::Lit(ExprLit {
                lit: Lit::Int(li), ..
            }) => Ok(-li.base10_parse::<i128>()?),
            other => Err(Error::new_spanned(
                other,
                "[BSTACK0204] expected an integer literal discriminant",
            )),
        },
        other => Err(Error::new_spanned(
            other,
            "[BSTACK0204] expected an integer literal discriminant",
        )),
    }
}

/// The `[min, max]` bounds of an integer type name, as `i128`.
pub(crate) fn int_bounds(ty: &str) -> (i128, i128) {
    match ty {
        "u8" => (0, u8::MAX as i128),
        "u16" => (0, u16::MAX as i128),
        "u32" => (0, u32::MAX as i128),
        "u64" => (0, u64::MAX as i128),
        "i8" => (i8::MIN as i128, i8::MAX as i128),
        "i16" => (i16::MIN as i128, i16::MAX as i128),
        "i32" => (i32::MIN as i128, i32::MAX as i128),
        "i64" => (i64::MIN as i128, i64::MAX as i128),
        _ => unreachable!("discriminant repr is validated in parse_attr"),
    }
}

/// The smallest integer type name holding every value in `[min, max]` — signed
/// iff a value is negative. This is the inferred discriminant width when no
/// explicit `repr(..)` is given.
pub(crate) fn infer_disc_ty(min: i128, max: i128) -> &'static str {
    let candidates: [&str; 4] = if min < 0 {
        ["i8", "i16", "i32", "i64"]
    } else {
        ["u8", "u16", "u32", "u64"]
    };
    for ty in candidates {
        let (lo, hi) = int_bounds(ty);
        if min >= lo && max <= hi {
            return ty;
        }
    }
    candidates[3] // i64 / u64 — the widest
}
