//! Shared machinery for the `#[bstack_block]` / `#[bstack_enum]` derives:
//! ownership/mode classification, container-shape analysis, tag generation, and
//! the per-field / per-variant code emitters. Both orchestrators
//! ([`crate::block`], [`crate::enum_`]) build on this.
//!
//! (Incremental target: subdivide into `util` / `analyze` / `layout` / `emit`.)

use proc_macro2::{Span, TokenStream};
use quote::{format_ident, quote};
use syn::parse::Parser;
use syn::punctuated::Punctuated;
use syn::{
    Error, Expr, ExprLit, GenericArgument, Ident, Lit, Meta, PathArguments, Token, Type,
};

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
            "a nested array `[[T; N]; M]` with a const-parameter dimension is not supported: \
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
        "nested `Option<Option<T>>` is not supported: a field / `Vec` slot lowers a \
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
        "nested `Vec<Vec<T>>` / `Vec<String>` is not supported: a `Vec` field stores one \
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
        "a tuple is not supported as a `Vec` element: a `Vec` element must be a single \
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
        if vec_field(inner).is_some() {
            return Err(err_vec_in_vec(ty));
        }
        if let Type::Tuple(_) = inner {
            return Err(err_tuple_in_vec(inner));
        }
        return Ok(());
    }
    if vec_field(ty).is_some() {
        return Err(err_vec_in_vec(ty));
    }
    if let Type::Tuple(_) = ty {
        return Err(err_tuple_in_vec(ty));
    }
    Ok(())
}

/// Detect `Vec<T>` / `String` field types.
pub(crate) fn vec_field(ty: &Type) -> Option<VecInfo> {
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

/// Teardown for a POD `Vec<T>` / `String` field: free the vector's data block
/// (the inline descriptor is freed with the enclosing struct's block). A nullable
/// field frees nothing when the descriptor is the `0` niche.
pub(crate) fn vec_drop_stmt(fname: &Ident, elem: &TokenStream, nullable: bool) -> TokenStream {
    let free = quote! {
        ::bstack_raii::BStackVec::<#elem, __A>::from_desc(__on_disk.#fname, allocator)
            .bstack_drop()?;
    };
    if nullable {
        quote! { { if __on_disk.#fname.data_off != 0 { #free } } }
    } else {
        quote! { { #free } }
    }
}

/// Accessor for a `Vec<T>` / `String` field: read the inline descriptor at the
/// field's location into a `BStackVec` handle. A nullable field returns
/// `Option<_>` (`None` for the `0` niche).
pub(crate) fn vec_accessor(
    vis: &syn::Visibility,
    fname: &Ident,
    elem: &TokenStream,
    on_disk: &TokenStream,
    nullable: bool,
) -> TokenStream {
    let getter = format_ident!("get_{}", fname);
    let field = quote!(self.0.start() + ::core::mem::offset_of!(#on_disk, #fname) as u64);
    if nullable {
        quote! {
            #vis fn #getter<'__v, __A: ::bstack_raii::BStackRaiiAllocator>(
                &self,
                allocator: &'__v __A,
            ) -> ::std::io::Result<
                ::core::option::Option<::bstack_raii::BStackVec<'__v, #elem, __A>>
            > {
                unsafe { ::bstack_raii::BStackVec::from_field_opt(#field, allocator) }
            }
        }
    } else {
        quote! {
            #vis fn #getter<'__v, __A: ::bstack_raii::BStackRaiiAllocator>(
                &self,
                allocator: &'__v __A,
            ) -> ::std::io::Result<::bstack_raii::BStackVec<'__v, #elem, __A>> {
                unsafe { ::bstack_raii::BStackVec::from_field(#field, allocator) }
            }
        }
    }
}

/// Constructor `(param, prep, init)` for a `Vec<T>` / `String` field: allocate
/// the data block and store its descriptor inline. A nullable field takes an
/// `Option` (`None` => the `0` niche, no allocation).
pub(crate) fn vec_ctor(
    fname: &Ident,
    vinfo: &VecInfo,
    nullable: bool,
) -> (TokenStream, TokenStream, TokenStream) {
    let elem = &vinfo.elem;
    let base_param: TokenStream = if vinfo.is_string {
        quote!(&str)
    } else {
        quote!(&[#elem])
    };
    // The byte slice passed to `from_slice`, given the source binding `b`.
    let data_of = |b: TokenStream| -> TokenStream {
        if vinfo.is_string {
            quote!(#b.as_bytes())
        } else {
            b
        }
    };
    let prep = if nullable {
        let some_data = data_of(quote!(__d));
        quote! {
            let #fname: ::bstack_raii::VecDesc = match #fname {
                ::core::option::Option::Some(__d) =>
                    ::bstack_raii::BStackVec::<#elem, __A>::from_slice(allocator, #some_data)?
                        .descriptor(),
                ::core::option::Option::None => ::core::default::Default::default(),
            };
        }
    } else {
        let data = data_of(quote!(#fname));
        quote! {
            let #fname: ::bstack_raii::VecDesc =
                ::bstack_raii::BStackVec::<#elem, __A>::from_slice(allocator, #data)?
                    .descriptor();
        }
    };
    let param = if nullable {
        quote!(#fname: ::core::option::Option<#base_param>,)
    } else {
        quote!(#fname: #base_param,)
    };
    (param, prep, quote!(#fname: #fname,))
}

/// `bstack_move!` field for a `Vec<T>` / `String`: yield a detached `BStackVec`
/// carrying the inline descriptor (captured from the parent before it is freed).
/// Nullable yields `Option<_>`.
pub(crate) fn vec_move(cap: &Ident, elem: &TokenStream, nullable: bool) -> (TokenStream, TokenStream) {
    let ty = quote!(::bstack_raii::BStackVec<'__mv, #elem, __A>);
    let build = quote!(::bstack_raii::BStackVec::from_desc(#cap, __alloc));
    wrap_vec_move(ty, build, cap, nullable)
}

/// A `TryCloneIn` statement for any `Vec` / `String` field: reconstruct the
/// source vector from its (already-read) inline descriptor, deep-clone its data
/// block into `__plan` per the element relationship, and repoint `__od`'s inline
/// descriptor at the fresh block. A `data_off` of `0` (a null `Option<Vec>` /
/// unset vec) is left copied as-is. POD elements are byte-copied; owned elements
/// are deep-cloned (recursing each child); strong/weak elements are
/// re-referenced (their refcount bumped); ref elements are aliased.
pub(crate) fn vec_clone_stmt(fname: &Ident, kind: Kind, elem: &TokenStream) -> TokenStream {
    let clone_expr = match kind {
        Kind::Pod => quote! {
            ::bstack_raii::BStackVec::<#elem, __A>::from_desc(__srcdesc, allocator)
                .clone_data_into(__plan)?
        },
        Kind::Owned => quote! {
            ::bstack_raii::BStackBlockVec::<#elem, __A>::from_desc(__srcdesc, allocator)
                .clone_into(__plan, |__er, __p| {
                    <#elem as ::bstack_raii::BStackBlock>::from_range(__er)
                        .__bstack_clone_into(allocator, __p)
                })?
        },
        Kind::Strong => quote! {
            ::bstack_raii::BStackStrongVec::<#elem, __A>::from_desc(__srcdesc, allocator)
                .clone_into(__plan)?
        },
        Kind::Weak => quote! {
            ::bstack_raii::BStackWeakVec::<#elem, __A>::from_desc(__srcdesc, allocator)
                .clone_into(__plan)?
        },
        Kind::Ref => quote! {
            ::bstack_raii::BStackRefVec::<#elem, __A>::from_desc(__srcdesc, allocator)
                .clone_into(__plan)?
        },
        // `#[embed]` never reaches the vec branch (rejected earlier).
        Kind::Embed => quote!(unreachable!()),
    };
    quote! {
        {
            let __srcdesc: ::bstack_raii::VecDesc = __od.#fname;
            if __srcdesc.data_off != 0 {
                let __newdesc: ::bstack_raii::VecDesc = #clone_expr;
                __od.#fname = __newdesc;
            }
        }
    }
}

// Block-element vectors (`#[bstack_owned/strong/weak/ref] Vec<Thing>`) all share
// the same inline-descriptor offset-array storage and a uniform codegen-facing
// API (`from_field` / `from_field_opt` / `from_desc` / `from_handles` /
// `descriptor` / `bstack_drop`); only the runtime type (`vec_ty`) and the
// element-handle type differ per element relationship. These helpers are
// parameterized over both.

/// Wrap a vector move field's type/expr in `Option` when the field is nullable
/// (the `data_off == 0` niche == `None`).
pub(crate) fn wrap_vec_move(
    ty: TokenStream,
    build: TokenStream,
    cap: &Ident,
    nullable: bool,
) -> (TokenStream, TokenStream) {
    if nullable {
        (
            quote!(::core::option::Option<#ty>),
            quote! {
                if #cap.data_off != 0 {
                    ::core::option::Option::Some(#build)
                } else {
                    ::core::option::Option::None
                }
            },
        )
    } else {
        (ty, build)
    }
}

/// Teardown for a block-element `Vec<Thing>` field: run the vector's own
/// `bstack_drop` (per-element release + free the offset array). Nullable frees
/// nothing for the `0` niche.
pub(crate) fn block_vec_drop_stmt(
    fname: &Ident,
    vec_ty: TokenStream,
    elem: &TokenStream,
    nullable: bool,
) -> TokenStream {
    let free = quote! {
        ::bstack_raii::#vec_ty::<#elem, __A>::from_desc(__on_disk.#fname, allocator)
            .bstack_drop()?;
    };
    if nullable {
        quote! { { if __on_disk.#fname.data_off != 0 { #free } } }
    } else {
        quote! { { #free } }
    }
}

/// Accessor for a block-element `Vec<Thing>` field: read the inline descriptor at
/// the field's location into the vector handle. Nullable returns `Option<_>`.
pub(crate) fn block_vec_accessor(
    vis: &syn::Visibility,
    fname: &Ident,
    elem: &TokenStream,
    on_disk: &TokenStream,
    vec_ty: TokenStream,
    nullable: bool,
) -> TokenStream {
    let getter = format_ident!("get_{}", fname);
    let field = quote!(self.0.start() + ::core::mem::offset_of!(#on_disk, #fname) as u64);
    if nullable {
        quote! {
            #vis fn #getter<'__v, __A: ::bstack_raii::BStackRaiiAllocator>(
                &self,
                allocator: &'__v __A,
            ) -> ::std::io::Result<
                ::core::option::Option<::bstack_raii::#vec_ty<'__v, #elem, __A>>
            > {
                unsafe { ::bstack_raii::#vec_ty::from_field_opt(#field, allocator) }
            }
        }
    } else {
        quote! {
            #vis fn #getter<'__v, __A: ::bstack_raii::BStackRaiiAllocator>(
                &self,
                allocator: &'__v __A,
            ) -> ::std::io::Result<::bstack_raii::#vec_ty<'__v, #elem, __A>> {
                unsafe { ::bstack_raii::#vec_ty::from_field(#field, allocator) }
            }
        }
    }
}

/// Constructor `(param, prep, init)` for a block-element `Vec<Thing>` field:
/// build the vector from a `Vec` of element handles (each consumed) and store its
/// descriptor inline. Nullable takes an `Option<Vec<..>>` (`None` => the niche).
pub(crate) fn block_vec_ctor(
    fname: &Ident,
    elem: &TokenStream,
    vec_ty: TokenStream,
    handle_ty: TokenStream,
    nullable: bool,
) -> (TokenStream, TokenStream, TokenStream) {
    if nullable {
        (
            quote!(#fname: ::core::option::Option<::std::vec::Vec<#handle_ty>>,),
            quote! {
                let #fname: ::bstack_raii::VecDesc = match #fname {
                    ::core::option::Option::Some(__v) =>
                        ::bstack_raii::#vec_ty::<#elem, __A>::from_handles(allocator, __v)?
                            .descriptor(),
                    ::core::option::Option::None => ::core::default::Default::default(),
                };
            },
            quote!(#fname: #fname,),
        )
    } else {
        (
            quote!(#fname: ::std::vec::Vec<#handle_ty>,),
            quote! {
                let #fname: ::bstack_raii::VecDesc =
                    ::bstack_raii::#vec_ty::<#elem, __A>::from_handles(allocator, #fname)?
                        .descriptor();
            },
            quote!(#fname: #fname,),
        )
    }
}

/// `bstack_move!` field for a block-element `Vec<Thing>`: yield a detached vector
/// handle carrying the inline descriptor. Nullable yields `Option<_>`.
pub(crate) fn block_vec_move(
    cap: &Ident,
    elem: &TokenStream,
    vec_ty: TokenStream,
    nullable: bool,
) -> (TokenStream, TokenStream) {
    let ty = quote!(::bstack_raii::#vec_ty<'__mv, #elem, __A>);
    let build = quote!(::bstack_raii::#vec_ty::from_desc(#cap, __alloc));
    wrap_vec_move(ty, build, cap, nullable)
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

/// Per-element cross-file **teardown** dispatch, given `__fp: ForeignPtr` and
/// `allocator` in scope. Frees / decrements / releases the target in its own file:
/// `SELF` (`file_id == 0`) via the local `allocator`, a foreign id via a
/// [`ForeignHostAllocator`] over the resolved host (skipped — a permitted leak — if
/// that file is not attached). `offset == 0` (null / unset) is skipped.
/// `#[bstack_ref]` owns nothing → empty. Shared with the scalar `Foreign` field.
pub(crate) fn foreign_elem_drop(kind: Kind, ftarget: &Type) -> TokenStream {
    let helper = match kind {
        Kind::Owned => quote!(::bstack_raii::__private::foreign_drop_owned),
        Kind::Strong => quote!(::bstack_raii::__private::foreign_drop_strong),
        Kind::Weak => quote!(::bstack_raii::__private::foreign_drop_weak),
        _ => return quote!(),
    };
    quote! {
        let __off = __fp.offset();
        if __off != 0 {
            let __fid = __fp.file_id();
            if __fid == 0 {
                unsafe { #helper::<#ftarget, _>(allocator, __off)?; }
            } else if let ::core::option::Option::Some(__id) =
                ::bstack_raii::registry::FileId::from_u64(__fid)
            {
                if let ::core::option::Option::Some(__host) =
                    ::bstack_raii::registry::host_arc(__id)
                {
                    let __adapter = ::bstack_raii::ForeignHostAllocator::new(__host, __id);
                    unsafe { #helper::<#ftarget, _>(&__adapter, __off)?; }
                }
            }
        }
    }
}

/// Per-element cross-file **clone** dispatch, given `__fp: ForeignPtr`, `allocator`,
/// and `__plan` in scope. Binds `__newfp: ForeignPtr` — `#[bstack_owned]` deep-copies
/// the target (repointing the pointer), `#[bstack_strong]` / `#[bstack_weak]` share it
/// and bump its count (pointer unchanged), `#[bstack_ref]` aliases. `SELF` folds into
/// the home `__plan`; a foreign target goes through a [`ForeignHostAllocator`], and a
/// detached target file **errors** (aliasing an owner would double-free later).
pub(crate) fn foreign_elem_clone(kind: Kind, ftarget: &Type) -> TokenStream {
    let od_size = quote! {
        ::core::mem::size_of::<<#ftarget as ::bstack_raii::BStackBlock>::OnDisk>() as u64
    };
    let not_attached = |ann: &str| {
        let msg = format!("cannot clone `{ann} Foreign<T>` element: target file not attached");
        quote! {
            ::std::io::Error::new(::std::io::ErrorKind::NotFound, #msg)
        }
    };
    let malformed = quote! {
        return ::std::result::Result::Err(::std::io::Error::new(
            ::std::io::ErrorKind::InvalidData, "cannot clone `Foreign<T>`: malformed file id"));
    };
    match kind {
        Kind::Owned => {
            let err = not_attached("#[bstack_owned]");
            quote! {
                let __newfp: ::bstack_raii::ForeignRepr = {
                    let __off = __fp.offset();
                    if __off == 0 {
                        __fp
                    } else {
                        let __fid = __fp.file_id();
                        if __fid == 0 {
                            let __child = <#ftarget as ::bstack_raii::BStackBlock>::from_range(
                                ::bstack_raii::BStackRange::new(__off, #od_size));
                            let __new = __child.__bstack_clone_into(allocator, __plan)?;
                            ::bstack_raii::ForeignRepr::new(0, __new.start())
                        } else if __plan.is_measuring() {
                            // Foreign deep-clone is build-only; this value is discarded
                            // in the measure pass (home-file sizes only).
                            __fp
                        } else if let ::core::option::Option::Some(__id) =
                            ::bstack_raii::registry::FileId::from_u64(__fid)
                        {
                            let __host = ::bstack_raii::registry::host_arc(__id)
                                .ok_or_else(|| #err)?;
                            let __adapter =
                                ::bstack_raii::ForeignHostAllocator::new(__host, __id);
                            let __new_off = unsafe {
                                ::bstack_raii::__private::foreign_clone_owned::<#ftarget, _>(&__adapter, __off)? };
                            ::bstack_raii::ForeignRepr::new(__fid, __new_off)
                        } else {
                            #malformed
                        }
                    }
                };
            }
        }
        Kind::Strong => {
            let err = not_attached("#[bstack_strong]");
            quote! {
                let __newfp: ::bstack_raii::ForeignRepr = {
                    let __off = __fp.offset();
                    if __off != 0 {
                        let __fid = __fp.file_id();
                        if __fid == 0 {
                            let __data = unsafe { ::bstack_raii::BStackRef::<#ftarget>::from_range(
                                ::bstack_raii::BStackRange::new(__off, #od_size)) };
                            __plan.bump_strong(__data, allocator)?;
                        } else if __plan.is_measuring() {
                            // Foreign refcount bump is build-only (measure skips it).
                        } else if let ::core::option::Option::Some(__id) =
                            ::bstack_raii::registry::FileId::from_u64(__fid)
                        {
                            let __host = ::bstack_raii::registry::host_arc(__id)
                                .ok_or_else(|| #err)?;
                            let __adapter =
                                ::bstack_raii::ForeignHostAllocator::new(__host, __id);
                            unsafe { ::bstack_raii::__private::foreign_clone_strong::<#ftarget, _>(&__adapter, __off)?; }
                        } else {
                            #malformed
                        }
                    }
                    __fp
                };
            }
        }
        Kind::Weak => {
            let err = not_attached("#[bstack_weak]");
            quote! {
                let __newfp: ::bstack_raii::ForeignRepr = {
                    let __off = __fp.offset();
                    if __off != 0 {
                        let __fid = __fp.file_id();
                        if __fid == 0 {
                            __plan.bump_weak(__off);
                        } else if __plan.is_measuring() {
                            // Foreign refcount bump is build-only (measure skips it).
                        } else if let ::core::option::Option::Some(__id) =
                            ::bstack_raii::registry::FileId::from_u64(__fid)
                        {
                            let __host = ::bstack_raii::registry::host_arc(__id)
                                .ok_or_else(|| #err)?;
                            let __adapter =
                                ::bstack_raii::ForeignHostAllocator::new(__host, __id);
                            unsafe { ::bstack_raii::__private::foreign_clone_weak::<#ftarget, _>(&__adapter, __off)?; }
                        } else {
                            #malformed
                        }
                    }
                    __fp
                };
            }
        }
        // Ref aliases the pointer verbatim.
        _ => quote! { let __newfp: ::bstack_raii::ForeignRepr = __fp; },
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
                    "{what} needs an ownership annotation naming the target's kind \
                     (`#[bstack_owned/strong/weak/ref]`); a bare foreign pointer targets a block"
                ),
            ));
        }
        Kind::Embed => {
            return Err(Error::new_spanned(
                span,
                "`Foreign<T>` is a pointer and cannot be `#[embed]`ed",
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
pub(crate) fn reject_bad_foreign_target(ftarget: &Type, span: &Type, what: &str) -> syn::Result<()> {
    let bridge = "bridge it inside an explicit `#[bstack_block]` struct and point the \
                  `Foreign` at that struct";
    if foreign_inner(ftarget).is_some() {
        return Err(Error::new_spanned(
            span,
            format!(
                "{what}: a `Foreign` cannot point at another `Foreign` — a pointer to a \
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
                    "{what}: `Foreign<Option<Foreign<T>>>` is a double `Foreign` (a pointer to a \
                     nullable pointer). {bridge}."
                ),
            ));
        }
        return Err(Error::new_spanned(
            span,
            format!(
                "{what}: use `Option<Foreign<T>>` for a nullable foreign pointer, not \
                 `Foreign<Option<T>>` — nullability belongs on the field/element, and a null \
                 element is a `Foreign` with offset 0, not a pointer to a nullable value."
            ),
        ));
    }
    if vec_field(ftarget).is_some() || is_str(ftarget) {
        return Err(Error::new_spanned(
            span,
            format!(
                "{what}: a `Foreign` target must be a `#[bstack_block]`, not a `Vec` / `String`. \
                 `Vec<Foreign<T>>` (a vector OF pointers) is allowed, but `Foreign<Vec<T>>` (a \
                 pointer TO a vector) is not — {bridge}."
            ),
        ));
    }
    if let Type::Array(_) = ftarget {
        return Err(Error::new_spanned(
            span,
            format!(
                "{what}: a `Foreign` target must be a `#[bstack_block]`, not an array. \
                 `[Foreign<T>; N]` (an array OF pointers) is allowed, but `Foreign<[T; N]>` is \
                 not — {bridge}."
            ),
        ));
    }
    if let Type::Tuple(_) = ftarget {
        return Err(Error::new_spanned(
            span,
            format!(
                "{what}: a `Foreign` target must be a `#[bstack_block]`, not a tuple — {bridge}."
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
            "`Option` wrapping a whole sub-array is not supported; move the \
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
/// makes ref accessors return `Option<Handle>`, treating a `0` offset as `None`.
pub(crate) fn accessor(
    vis: &syn::Visibility,
    fname: &Ident,
    inner_ty: &Type,
    on_disk: &TokenStream,
    kind: Kind,
    nullable: bool,
) -> TokenStream {
    let getter = format_ident!("get_{}", fname);
    // Weak fields hold a control offset; the accessor attempts a live upgrade.
    if kind == Kind::Weak {
        return quote! {
            #vis fn #getter<'__u, __A: ::bstack_raii::BStackRaiiAllocator>(
                &self,
                allocator: &'__u __A,
            ) -> ::std::io::Result<
                ::core::option::Option<::bstack_raii::BStackRc<'__u, #inner_ty, __A>>
            > {
                let __field = self.0.start() + ::core::mem::offset_of!(#on_disk, #fname) as u64;
                ::bstack_raii::upgrade_weak_field(allocator, __field)
            }
        };
    }
    let read = quote! {
        let mut __buf = ::std::vec![0u8; ::core::mem::size_of::<#on_disk>()];
        let __r = unsafe { ::bstack_raii::BStackRef::<Self>::from_range(self.0) };
        let __od: #on_disk = *__r.read_on_disk(stack, &mut __buf)?;
    };
    if kind == Kind::Pod {
        return quote! {
            #vis fn #getter(&self, stack: &::bstack_raii::BStack) -> ::std::io::Result<#inner_ty> {
                #read
                ::std::result::Result::Ok(__od.#fname)
            }
        };
    }
    // Owned/strong/ref field: resolve the stored data offset to the handle.
    let resolve = quote! {
        <#inner_ty as ::bstack_raii::BStackBlock>::from_range(::bstack_raii::BStackRange::new(
            __od.#fname,
            ::core::mem::size_of::<<#inner_ty as ::bstack_raii::BStackBlock>::OnDisk>() as u64,
        ))
    };
    if nullable {
        quote! {
            #vis fn #getter(
                &self,
                stack: &::bstack_raii::BStack,
            ) -> ::std::io::Result<::core::option::Option<#inner_ty>> {
                #read
                if __od.#fname == 0 {
                    ::std::result::Result::Ok(::core::option::Option::None)
                } else {
                    ::std::result::Result::Ok(::core::option::Option::Some(#resolve))
                }
            }
        }
    } else {
        quote! {
            #vis fn #getter(&self, stack: &::bstack_raii::BStack) -> ::std::io::Result<#inner_ty> {
                #read
                ::std::result::Result::Ok(#resolve)
            }
        }
    }
}

/// The unsafe raw-place accessor `raw_<field>_slice`: a [`BStackSlice`] over the
/// field's **inline** storage within the record — the value's own bytes for a POD
/// field, or the `u64` offset slot for a pointer field
/// (`#[bstack_owned/strong/weak/ref]`; `#[embed]` yields the inline child's bytes).
///
/// It bypasses the typed [`accessor`]/setter, so writing through the returned slice
/// can violate the field's invariants (a bogus/aliased offset for a pointer field,
/// an un-freed owned target); it is therefore `unsafe`. Reads are always valid.
pub(crate) fn raw_slice_accessor(
    vis: &syn::Visibility,
    fname: &Ident,
    inner_ty: &Type,
    on_disk: &TokenStream,
    kind: Kind,
) -> TokenStream {
    let raw = format_ident!("raw_{}_slice", fname);
    // The field's inline byte length: a POD value, an `#[embed]` child's on-disk
    // form, or a single `u64` offset slot for the pointer kinds.
    let len = match kind {
        Kind::Pod => quote!(::core::mem::size_of::<#inner_ty>() as u64),
        Kind::Embed => {
            quote!(::core::mem::size_of::<<#inner_ty as ::bstack_raii::BStackBlock>::OnDisk>() as u64)
        }
        Kind::Owned | Kind::Strong | Kind::Weak | Kind::Ref => quote!(8u64),
    };
    quote! {
        /// Raw [`BStackSlice`] over this field's inline storage.
        ///
        /// # Safety
        /// Bypasses the field's typed invariants: writing bytes through the returned
        /// slice can corrupt a pointer field (a bogus or aliased offset) or leak an
        /// owned target. Reads are always valid.
        #vis unsafe fn #raw<'__s>(
            &self,
            stack: &'__s ::bstack_raii::BStack,
        ) -> ::bstack_raii::BStackSlice<'__s> {
            let __off = self.0.start() + ::core::mem::offset_of!(#on_disk, #fname) as u64;
            // SAFETY: `[__off, __off + #len)` is exactly this field's inline region
            // within the record, which is a live allocation of the backing stack.
            unsafe {
                ::bstack_raii::BStackSlice::from_raw_range(
                    stack,
                    ::bstack_raii::BStackRange::new(__off, #len),
                )
            }
        }
    }
}

/// The `set_<field>` mutator, generated only for `#[bstack_mut]` POD and
/// `#[bstack_ref]` fields (both a single atomic `set` of the field's inline
/// storage — a POD field owns no children, and a ref does not own its target, so
/// neither frees anything). Ownership-bearing kinds (`owned`/`strong`/`embed`) are
/// rejected upstream, since their setter must free/refcount the old target.
pub(crate) fn set_accessor(
    vis: &syn::Visibility,
    fname: &Ident,
    inner_ty: &Type,
    on_disk: &TokenStream,
    kind: Kind,
    nullable: bool,
) -> TokenStream {
    let setter = format_ident!("set_{}", fname);
    let off = quote!(self.0.start() + ::core::mem::offset_of!(#on_disk, #fname) as u64);
    match kind {
        Kind::Pod => quote! {
            /// Overwrite this POD field, as one crash-atomic `set`.
            #vis fn #setter(
                &self,
                stack: &::bstack_raii::BStack,
                value: #inner_ty,
            ) -> ::std::io::Result<()> {
                stack.set(#off, ::bstack_raii::bytemuck::bytes_of(&value))
            }
        },
        Kind::Ref if nullable => quote! {
            /// Repoint this `#[bstack_ref]` field (`None` writes the `0` null niche).
            /// The ref borrows — it does not own — its target, so nothing is freed.
            #vis fn #setter(
                &self,
                stack: &::bstack_raii::BStack,
                value: ::core::option::Option<::bstack_raii::BStackRef<#inner_ty>>,
            ) -> ::std::io::Result<()> {
                let __ptr = value.map_or(0u64, |__r| __r.into_range().start());
                stack.set(#off, __ptr.to_le_bytes())
            }
        },
        Kind::Ref => quote! {
            /// Repoint this `#[bstack_ref]` field. The ref borrows — it does not own
            /// — its target, so nothing is freed.
            #vis fn #setter(
                &self,
                stack: &::bstack_raii::BStack,
                value: ::bstack_raii::BStackRef<#inner_ty>,
            ) -> ::std::io::Result<()> {
                stack.set(#off, value.into_range().start().to_le_bytes())
            }
        },
        // Only POD / ref reach here (the caller filters).
        _ => quote!(),
    }
}

/// The `replace_<field>` mutator for ownership-bearing fields: install `value` and
/// **move the previous value out** to the caller (`mem::replace` semantics), so the
/// old target is neither leaked nor silently freed — the caller owns it and decides
/// its fate. The swap itself is one crash-atomic `set` of the field's `u64` offset
/// slot; the old value is then reconstructed from the offset it held (exactly as
/// `bstack_move!` does). Generated for `#[bstack_mut]`
/// `#[bstack_owned]`/`#[bstack_strong]`/`#[bstack_ref]` fields (a ref *also* gets
/// `set_<field>`; owned/strong get only `replace_<field>`, since their old value
/// must not be dropped on the floor).
pub(crate) fn replace_accessor(
    vis: &syn::Visibility,
    fname: &Ident,
    inner_ty: &Type,
    on_disk: &TokenStream,
    kind: Kind,
    nullable: bool,
) -> TokenStream {
    let name = format_ident!("replace_{}", fname);
    let off = quote!(self.0.start() + ::core::mem::offset_of!(#on_disk, #fname) as u64);
    let size_od =
        quote!(::core::mem::size_of::<<#inner_ty as ::bstack_raii::BStackBlock>::OnDisk>() as u64);

    // The old value's on-disk range (from the offset stored in the field slot).
    let old_range = quote!(::bstack_raii::BStackRange::new(__old_off, #size_od));

    match kind {
        // Owned / ref reconstruct handles from just an offset — no allocator, no
        // I/O — so they take `&BStack` and their reconstructions are infallible.
        // `rebuild(range)` rebuilds a handle over a known range, used both for the
        // old value (on success) and to hand the new value back (on a failed commit).
        Kind::Owned => {
            let handle_ty = quote!(::bstack_raii::BStackOwned<#inner_ty>);
            // Consume `value` into the new block's range (its block persists on disk).
            let new_range = quote!({
                let __h = __value.into_inner();
                ::bstack_raii::BStackBlock::range(&__h)
            });
            let rebuild = |range: TokenStream| {
                quote!(unsafe {
                    ::bstack_raii::BStackOwned::from_raw(
                        <#inner_ty as ::bstack_raii::BStackBlock>::from_range(#range),
                    )
                })
            };
            let recon_old = rebuild(old_range.clone());
            let rebuild_new = rebuild(quote!(__new_range));
            replace_stack_method(
                vis,
                &name,
                &handle_ty,
                &off,
                &new_range,
                &recon_old,
                &rebuild_new,
                nullable,
            )
        }
        Kind::Ref => {
            let handle_ty = quote!(::bstack_raii::BStackRef<#inner_ty>);
            let new_range = quote!(__value.into_range());
            let rebuild = |range: TokenStream| quote!(unsafe { ::bstack_raii::BStackRef::<#inner_ty>::from_range(#range) });
            let recon_old = rebuild(old_range.clone());
            let rebuild_new = rebuild(quote!(__new_range));
            replace_stack_method(
                vis,
                &name,
                &handle_ty,
                &off,
                &new_range,
                &recon_old,
                &rebuild_new,
                nullable,
            )
        }
        // Strong reconstructs a `BStackRc`, which needs the allocator — so it takes
        // `&A`. The NEW value hands back with no I/O (its raw parts are already in
        // hand); only reconstructing the OLD value reads the block (`strong_parts`),
        // the one step that can leave `value: None` on failure.
        Kind::Strong => {
            let rc_ty = quote!(::bstack_raii::BStackRc<'__r, #inner_ty, __A>);
            let ret_ty = if nullable {
                quote!(::core::option::Option<#rc_ty>)
            } else {
                quote!(#rc_ty)
            };
            // Reconstruct the old strong ref from its data offset (transfers the
            // existing count out; dropping the returned `BStackRc` decrements).
            let recon_old = quote! {
                {
                    let __old_data = unsafe {
                        ::bstack_raii::BStackRef::<#inner_ty>::from_range(#old_range)
                    };
                    match <#inner_ty as ::bstack_raii::BStackShared>::strong_parts(__old_data, allocator) {
                        ::std::result::Result::Ok((__d, __c)) => {
                            unsafe { ::bstack_raii::BStackRc::from_raw(__d, __c, allocator) }
                        }
                        // The new value is safely installed; the OLD block is now
                        // reachable only via crash-recovery. Hand back nothing.
                        ::std::result::Result::Err(__e) => {
                            return ::core::result::Result::Err(::bstack_raii::ReplaceError::lost(__e));
                        }
                    }
                }
            };
            // Consume `value` into `(new_range, ctrl)`; rebuild is infallible.
            let (consume_new, rebuild_new): (TokenStream, TokenStream) = (
                quote!({
                    let (__nd, __nc) = __value.into_raw();
                    (__nd.into_range(), __nc)
                }),
                quote!(unsafe {
                    ::bstack_raii::BStackRc::from_raw(
                        ::bstack_raii::BStackRef::<#inner_ty>::from_range(__new_range),
                        __nc,
                        allocator,
                    )
                }),
            );

            let body = if nullable {
                quote! {
                    let __off = #off;
                    let __stack = allocator.stack();
                    let mut __b = [0u8; 8];
                    if let ::std::result::Result::Err(__e) = __stack.get_into(__off, &mut __b) {
                        return ::core::result::Result::Err(::bstack_raii::ReplaceError::recovered(__e, value));
                    }
                    let __old_off = u64::from_le_bytes(__b);
                    let (__new, __back): (u64, ::core::option::Option<(::bstack_raii::BStackRange, ::core::option::Option<::bstack_raii::BStackRange>)>) =
                        match value {
                            ::core::option::Option::Some(__value) => {
                                let (__new_range, __nc) = #consume_new;
                                (__new_range.start(), ::core::option::Option::Some((__new_range, __nc)))
                            }
                            ::core::option::Option::None => (0u64, ::core::option::Option::None),
                        };
                    if let ::std::result::Result::Err(__e) = __stack.set(__off, __new.to_le_bytes()) {
                        let __handback: #ret_ty = match __back {
                            ::core::option::Option::Some((__new_range, __nc)) => {
                                ::core::option::Option::Some(#rebuild_new)
                            }
                            ::core::option::Option::None => ::core::option::Option::None,
                        };
                        return ::core::result::Result::Err(::bstack_raii::ReplaceError::recovered(__e, __handback));
                    }
                    if __old_off == 0 {
                        ::core::result::Result::Ok(::core::option::Option::None)
                    } else {
                        ::core::result::Result::Ok(::core::option::Option::Some(#recon_old))
                    }
                }
            } else {
                quote! {
                    let __off = #off;
                    let __stack = allocator.stack();
                    let mut __b = [0u8; 8];
                    if let ::std::result::Result::Err(__e) = __stack.get_into(__off, &mut __b) {
                        return ::core::result::Result::Err(::bstack_raii::ReplaceError::recovered(__e, value));
                    }
                    let __old_off = u64::from_le_bytes(__b);
                    let (__new_range, __nc) = { let __value = value; #consume_new };
                    let __new = __new_range.start();
                    if let ::std::result::Result::Err(__e) = __stack.set(__off, __new.to_le_bytes()) {
                        return ::core::result::Result::Err(
                            ::bstack_raii::ReplaceError::recovered(__e, #rebuild_new),
                        );
                    }
                    ::core::result::Result::Ok(#recon_old)
                }
            };

            quote! {
                /// Install `value` and move the previous value out to the caller. On
                /// an I/O failure the *new* value is handed back through
                /// [`ReplaceError`](::bstack_raii::ReplaceError) (never lost); the old
                /// value is never at risk.
                #vis fn #name<'__r, __A: ::bstack_raii::BStackRaiiAllocator>(
                    &self,
                    allocator: &'__r __A,
                    value: #ret_ty,
                ) -> ::core::result::Result<#ret_ty, ::bstack_raii::ReplaceError<#ret_ty>> {
                    #body
                }
            }
        }
        // pod/weak/embed do not get `replace_<field>`.
        _ => quote!(),
    }
}

/// The `&BStack`-based body shared by owned/ref `replace_<field>` (both have an
/// infallible reconstruction). The swap never loses the *new* value: the old
/// offset is read **before** `value` is consumed (a read failure hands `value`
/// straight back), and a failed commit rebuilds the new value from its known
/// range. `new_range` references `__value`; `recon_old` returns the old value on
/// success; `rebuild_new` references `__new_range` for the failed-commit handback.
#[allow(clippy::too_many_arguments)]
pub(crate) fn replace_stack_method(
    vis: &syn::Visibility,
    name: &Ident,
    handle_ty: &TokenStream,
    off: &TokenStream,
    new_range: &TokenStream,
    recon_old: &TokenStream,
    rebuild_new: &TokenStream,
    nullable: bool,
) -> TokenStream {
    if nullable {
        quote! {
            /// Install `value` and move the previous value out (`None` = the `0`
            /// null niche). On an I/O failure the *new* value is handed back through
            /// [`ReplaceError`](::bstack_raii::ReplaceError), never lost.
            #vis fn #name(
                &self,
                stack: &::bstack_raii::BStack,
                value: ::core::option::Option<#handle_ty>,
            ) -> ::core::result::Result<
                ::core::option::Option<#handle_ty>,
                ::bstack_raii::ReplaceError<::core::option::Option<#handle_ty>>,
            > {
                let __off = #off;
                let mut __b = [0u8; 8];
                if let ::std::result::Result::Err(__e) = stack.get_into(__off, &mut __b) {
                    return ::core::result::Result::Err(::bstack_raii::ReplaceError::recovered(__e, value));
                }
                let __old_off = u64::from_le_bytes(__b);
                let (__new, __new_range_opt): (u64, ::core::option::Option<::bstack_raii::BStackRange>) =
                    match value {
                        ::core::option::Option::Some(__value) => {
                            let __new_range: ::bstack_raii::BStackRange = #new_range;
                            (__new_range.start(), ::core::option::Option::Some(__new_range))
                        }
                        ::core::option::Option::None => (0u64, ::core::option::Option::None),
                    };
                if let ::std::result::Result::Err(__e) = stack.set(__off, __new.to_le_bytes()) {
                    let __handback: ::core::option::Option<#handle_ty> = match __new_range_opt {
                        ::core::option::Option::Some(__new_range) => ::core::option::Option::Some(#rebuild_new),
                        ::core::option::Option::None => ::core::option::Option::None,
                    };
                    return ::core::result::Result::Err(::bstack_raii::ReplaceError::recovered(__e, __handback));
                }
                if __old_off == 0 {
                    ::core::result::Result::Ok(::core::option::Option::None)
                } else {
                    ::core::result::Result::Ok(::core::option::Option::Some(#recon_old))
                }
            }
        }
    } else {
        quote! {
            /// Install `value` and move the previous value out to the caller. On an
            /// I/O failure the *new* value is handed back through
            /// [`ReplaceError`](::bstack_raii::ReplaceError), never lost.
            #vis fn #name(
                &self,
                stack: &::bstack_raii::BStack,
                value: #handle_ty,
            ) -> ::core::result::Result<#handle_ty, ::bstack_raii::ReplaceError<#handle_ty>> {
                let __off = #off;
                let mut __b = [0u8; 8];
                if let ::std::result::Result::Err(__e) = stack.get_into(__off, &mut __b) {
                    return ::core::result::Result::Err(::bstack_raii::ReplaceError::recovered(__e, value));
                }
                let __old_off = u64::from_le_bytes(__b);
                let __new_range: ::bstack_raii::BStackRange = { let __value = value; #new_range };
                let __new = __new_range.start();
                if let ::std::result::Result::Err(__e) = stack.set(__off, __new.to_le_bytes()) {
                    return ::core::result::Result::Err(
                        ::bstack_raii::ReplaceError::recovered(__e, #rebuild_new),
                    );
                }
                ::core::result::Result::Ok(#recon_old)
            }
        }
    }
}

/// Generated `#[bstack_mut]` mutators for a block-reference array field
/// (`#[bstack_owned/strong/ref] [T; N]`, nested and per-element `Option` allowed).
/// Arrays are fixed-size, so there is no push/pop — only in-place change:
///
/// * `replace_<field>_at(index, value) -> old` swaps one element by **flat**,
///   row-major `index`, moving the old element out (never dropped on the floor);
///   one crash-atomic 8-byte `set`.
/// * `replace_<field>(array) -> old_array` swaps the whole array as one crash-atomic
///   write of the inline `[u64; N]` region.
/// * `#[bstack_ref]` also gets `set_<field>_at` / `set_<field>` — a ref owns nothing,
///   so it overwrites without handing the old value back.
///
/// Every `replace_` upholds the crate's "never lose the *new* value" invariant: on
/// an I/O failure the value being installed is handed back through [`ReplaceError`].
/// All mutators take `&A` (owned/ref need only the stack, but one signature is
/// simpler and matches the array accessors, which already take `&A`).
#[allow(clippy::too_many_arguments)]
pub(crate) fn array_mut_methods(
    vis: &syn::Visibility,
    fname: &Ident,
    elem: &TokenStream,
    on_disk: &TokenStream,
    kind: Kind,
    dims: &[&syn::Expr],
    total: &TokenStream,
    size_elem: &TokenStream,
    elem_nullable: bool,
) -> Vec<TokenStream> {
    let base = quote!(self.0.start() + ::core::mem::offset_of!(#on_disk, #fname) as u64);
    let is_strong = kind == Kind::Strong;
    let is_ref = kind == Kind::Ref;
    let oob = quote!(::std::io::Error::new(
        ::std::io::ErrorKind::InvalidInput,
        "array index out of bounds",
    ));

    // The leaf handle type over the method's `'__m` / `__A`, and the value/return
    // type (`Option`-wrapped for a per-element `[Option<T>; N]`).
    let leaf_handle = match kind {
        Kind::Owned => quote!(::bstack_raii::BStackOwned<#elem>),
        Kind::Strong => quote!(::bstack_raii::BStackRc<'__m, #elem, __A>),
        Kind::Ref => quote!(::bstack_raii::BStackRef<#elem>),
        _ => unreachable!(),
    };
    let elem_leaf = if elem_nullable {
        quote!(::core::option::Option<#leaf_handle>)
    } else {
        leaf_handle.clone()
    };
    let whole_ty = nested_ty(dims, &elem_leaf);

    // Consume a leaf handle `v` into `(offset: u64, ctrl: Option<BStackRange>)`
    // (`ctrl` is `None` except for a strong ref, whose control range is kept so the
    // new value can be rebuilt on a failed commit).
    let consume = |v: TokenStream| match kind {
        Kind::Owned => quote!({
            let __h = (#v).into_inner();
            (
                ::bstack_raii::BStackBlock::range(&__h).start(),
                ::core::option::Option::<::bstack_raii::BStackRange>::None,
            )
        }),
        Kind::Ref => quote!((
            (#v).into_range().start(),
            ::core::option::Option::<::bstack_raii::BStackRange>::None,
        )),
        Kind::Strong => quote!({
            let (__d, __c) = (#v).into_raw();
            (__d.into_range().start(), __c)
        }),
        _ => unreachable!(),
    };
    // Rebuild a leaf handle from `(off, ctrl)` for the new-value handback.
    let rebuild = |off: TokenStream, ctrl: TokenStream| match kind {
        Kind::Owned => quote!(unsafe {
            ::bstack_raii::BStackOwned::from_raw(
                <#elem as ::bstack_raii::BStackBlock>::from_range(
                    ::bstack_raii::BStackRange::new(#off, #size_elem)))
        }),
        Kind::Ref => quote!(unsafe {
            ::bstack_raii::BStackRef::<#elem>::from_range(
                ::bstack_raii::BStackRange::new(#off, #size_elem))
        }),
        Kind::Strong => quote!(unsafe {
            ::bstack_raii::BStackRc::from_raw(
                ::bstack_raii::BStackRef::<#elem>::from_range(
                    ::bstack_raii::BStackRange::new(#off, #size_elem)),
                #ctrl,
                allocator,
            )
        }),
        _ => unreachable!(),
    };
    // Reconstruct the OLD leaf from its offset (moves it out). Owned/ref are
    // infallible; strong reads the control block and may early-return `lost` — so it
    // must be used only where the enclosing fn returns `Result<_, ReplaceError<_>>`.
    let recon_old = |off: TokenStream| match kind {
        Kind::Owned => quote!(unsafe {
            ::bstack_raii::BStackOwned::from_raw(
                <#elem as ::bstack_raii::BStackBlock>::from_range(
                    ::bstack_raii::BStackRange::new(#off, #size_elem)))
        }),
        Kind::Ref => quote!(unsafe {
            ::bstack_raii::BStackRef::<#elem>::from_range(
                ::bstack_raii::BStackRange::new(#off, #size_elem))
        }),
        Kind::Strong => quote!({
            let __old_data = unsafe {
                ::bstack_raii::BStackRef::<#elem>::from_range(
                    ::bstack_raii::BStackRange::new(#off, #size_elem))
            };
            match <#elem as ::bstack_raii::BStackShared>::strong_parts(__old_data, allocator) {
                ::std::result::Result::Ok((__od2, __oc2)) =>
                    unsafe { ::bstack_raii::BStackRc::from_raw(__od2, __oc2, allocator) },
                ::std::result::Result::Err(__e) =>
                    return ::core::result::Result::Err(::bstack_raii::ReplaceError::lost(__e)),
            }
        }),
        _ => unreachable!(),
    };
    // Ctrl-hold pattern: bind the control range only for strong (avoids an unused
    // binding for owned/ref, whose `rebuild` ignores it).
    let cpat = if is_strong { quote!(__c) } else { quote!(_) };

    let mut out: Vec<TokenStream> = Vec::new();

    // ---- element `replace_<field>_at(index, value) -> old` ----
    let replace_at = format_ident!("replace_{}_at", fname);
    let elem_body = if elem_nullable {
        let consume_some = consume(quote!(__v));
        let rb = rebuild(quote!(__o), quote!(__c));
        let old = recon_old(quote!(__old_off));
        quote! {
            let mut __b = [0u8; 8];
            if let ::std::result::Result::Err(__e) = __stack.get_into(__off, &mut __b) {
                return ::core::result::Result::Err(::bstack_raii::ReplaceError::recovered(__e, value));
            }
            let __old_off = u64::from_le_bytes(__b);
            let (__new, __back): (
                u64,
                ::core::option::Option<(u64, ::core::option::Option<::bstack_raii::BStackRange>)>,
            ) = match value {
                ::core::option::Option::Some(__v) => {
                    let (__o, __c) = #consume_some;
                    (__o, ::core::option::Option::Some((__o, __c)))
                }
                ::core::option::Option::None => (0u64, ::core::option::Option::None),
            };
            if let ::std::result::Result::Err(__e) = __stack.set(__off, __new.to_le_bytes()) {
                let __hb: #elem_leaf = match __back {
                    ::core::option::Option::Some((__o, #cpat)) => ::core::option::Option::Some(#rb),
                    ::core::option::Option::None => ::core::option::Option::None,
                };
                return ::core::result::Result::Err(::bstack_raii::ReplaceError::recovered(__e, __hb));
            }
            if __old_off == 0 {
                ::core::result::Result::Ok(::core::option::Option::None)
            } else {
                ::core::result::Result::Ok(::core::option::Option::Some(#old))
            }
        }
    } else {
        let consume_v = consume(quote!(value));
        let rb = rebuild(quote!(__new), quote!(__c));
        let old = recon_old(quote!(__old_off));
        quote! {
            let mut __b = [0u8; 8];
            if let ::std::result::Result::Err(__e) = __stack.get_into(__off, &mut __b) {
                return ::core::result::Result::Err(::bstack_raii::ReplaceError::recovered(__e, value));
            }
            let __old_off = u64::from_le_bytes(__b);
            let (__new, #cpat): (u64, ::core::option::Option<::bstack_raii::BStackRange>) = #consume_v;
            if let ::std::result::Result::Err(__e) = __stack.set(__off, __new.to_le_bytes()) {
                return ::core::result::Result::Err(::bstack_raii::ReplaceError::recovered(__e, #rb));
            }
            ::core::result::Result::Ok(#old)
        }
    };
    out.push(quote! {
        /// Swap the element at the row-major flat `index`, moving the old element
        /// out. One crash-atomic 8-byte `set`; on I/O failure the *new* value is
        /// handed back through [`ReplaceError`](::bstack_raii::ReplaceError).
        #vis fn #replace_at<'__m, __A: ::bstack_raii::BStackRaiiAllocator>(
            &self,
            allocator: &'__m __A,
            index: usize,
            value: #elem_leaf,
        ) -> ::core::result::Result<#elem_leaf, ::bstack_raii::ReplaceError<#elem_leaf>> {
            if index >= (#total) {
                return ::core::result::Result::Err(
                    ::bstack_raii::ReplaceError::recovered(#oob, value));
            }
            let __stack = allocator.stack();
            let __off = #base + (index as u64) * 8;
            #elem_body
        }
    });

    // ---- whole-array `replace_<field>(array) -> old_array` ----
    // Consume the nested `value` into `__news[k]` (+ `__ncs[k]` for strong).
    let decl_ncs = if is_strong {
        quote!(let mut __ncs =
            [::core::option::Option::<::bstack_raii::BStackRange>::None; #total];)
    } else {
        quote!()
    };
    let consume_write = |k: &Ident, leaf: &Ident| {
        let store_ctrl = if is_strong {
            quote!(__ncs[#k] = __c;)
        } else {
            quote!()
        };
        if elem_nullable {
            let cs = consume(quote!(__v));
            quote! {
                match #leaf {
                    ::core::option::Option::Some(__v) => {
                        let (__o, #cpat) = #cs;
                        __news[#k] = __o;
                        #store_ctrl
                    }
                    ::core::option::Option::None => { __news[#k] = 0u64; }
                }
            }
        } else {
            let cs = consume(quote!(#leaf));
            quote! {
                let (__o, #cpat) = #cs;
                __news[#k] = __o;
                #store_ctrl
            }
        }
    };
    let consume_all = nested_consume(dims, &quote!(value), &consume_write);
    // Rebuild the new array (handback) from `__news` / `__ncs`.
    let new_read = |k: &Ident| {
        let rb = rebuild(quote!(__news[#k]), quote!(__ncs[#k]));
        if elem_nullable {
            quote!(if __news[#k] == 0u64 {
                ::core::option::Option::None
            } else {
                ::core::option::Option::Some(#rb)
            })
        } else {
            rb
        }
    };
    let new_nested = nested_build(dims, &elem_leaf, &new_read);
    // Rebuild the old array from the previously-read `__oldoffs`.
    let old_read = |k: &Ident| {
        let ro = recon_old(quote!(__oldoffs[#k]));
        if elem_nullable {
            quote!(if __oldoffs[#k] == 0u64 {
                ::core::option::Option::None
            } else {
                ::core::option::Option::Some(#ro)
            })
        } else {
            ro
        }
    };
    let old_nested = nested_build(dims, &elem_leaf, &old_read);
    let replace_whole = format_ident!("replace_{}", fname);
    out.push(quote! {
        /// Swap the whole array, moving the old array out. One crash-atomic write of
        /// the inline `[u64; N]` slot region; on I/O failure the *new* array is
        /// handed back through [`ReplaceError`](::bstack_raii::ReplaceError).
        #vis fn #replace_whole<'__m, __A: ::bstack_raii::BStackRaiiAllocator>(
            &self,
            allocator: &'__m __A,
            value: #whole_ty,
        ) -> ::core::result::Result<#whole_ty, ::bstack_raii::ReplaceError<#whole_ty>> {
            let __stack = allocator.stack();
            let __base = #base;
            let mut __buf = ::std::vec![0u8; ::core::mem::size_of::<#on_disk>()];
            let __r = unsafe { ::bstack_raii::BStackRef::<Self>::from_range(self.0) };
            let __oldoffs: [u64; #total] = match __r.read_on_disk(__stack, &mut __buf) {
                ::std::result::Result::Ok(__od) => __od.#fname,
                ::std::result::Result::Err(__e) =>
                    return ::core::result::Result::Err(
                        ::bstack_raii::ReplaceError::recovered(__e, value)),
            };
            let mut __news = [0u64; #total];
            #decl_ncs
            #consume_all
            if let ::std::result::Result::Err(__e) =
                __stack.set(__base, ::bstack_raii::bytemuck::bytes_of(&__news))
            {
                let __hb: #whole_ty = #new_nested;
                return ::core::result::Result::Err(
                    ::bstack_raii::ReplaceError::recovered(__e, __hb));
            }
            let __old: #whole_ty = #old_nested;
            ::core::result::Result::Ok(__old)
        }
    });

    // ---- `set_` mutators for `#[bstack_ref]` (a ref owns nothing) ----
    if is_ref {
        let set_at = format_ident!("set_{}_at", fname);
        let new_off = if elem_nullable {
            quote!(match value {
                ::core::option::Option::Some(__v) => __v.into_range().start(),
                ::core::option::Option::None => 0u64,
            })
        } else {
            quote!(value.into_range().start())
        };
        out.push(quote! {
            /// Repoint the element at the row-major flat `index` (a ref owns nothing,
            /// so the old ref is simply overwritten). One crash-atomic 8-byte `set`.
            #vis fn #set_at<'__m, __A: ::bstack_raii::BStackRaiiAllocator>(
                &self,
                allocator: &'__m __A,
                index: usize,
                value: #elem_leaf,
            ) -> ::std::io::Result<()> {
                if index >= (#total) {
                    return ::std::result::Result::Err(#oob);
                }
                let __off = #base + (index as u64) * 8;
                let __new: u64 = #new_off;
                allocator.stack().set(__off, __new.to_le_bytes())
            }
        });

        let set_whole = format_ident!("set_{}", fname);
        let set_write = |k: &Ident, leaf: &Ident| {
            if elem_nullable {
                quote! {
                    __news[#k] = match #leaf {
                        ::core::option::Option::Some(__v) => __v.into_range().start(),
                        ::core::option::Option::None => 0u64,
                    };
                }
            } else {
                quote!(__news[#k] = #leaf.into_range().start();)
            }
        };
        let set_consume = nested_consume(dims, &quote!(value), &set_write);
        out.push(quote! {
            /// Repoint the whole array (a ref owns nothing, so the old refs are
            /// overwritten). One crash-atomic write of the inline `[u64; N]` region.
            #vis fn #set_whole<'__m, __A: ::bstack_raii::BStackRaiiAllocator>(
                &self,
                allocator: &'__m __A,
                value: #whole_ty,
            ) -> ::std::io::Result<()> {
                let mut __news = [0u64; #total];
                #set_consume
                allocator.stack().set(#base, ::bstack_raii::bytemuck::bytes_of(&__news))
            }
        });
    }

    out
}

/// Generated `#[bstack_mut]` mutators for a scalar `Foreign<T>` /
/// `Option<Foreign<T>>` field. Unlike teardown / clone, the swap is **purely local**
/// — one crash-atomic 16-byte `ForeignRepr` write — because the cross-file
/// responsibility (free / decrement in the target's own file) travels with the
/// returned RAII dual, discharged later by the caller's `bstack_drop(&home)`:
///
/// * `#[bstack_owned/strong/weak]` — `replace_<field>(&alloc, new) -> old`, taking /
///   returning `ForeignOwned` / `ForeignRc` / `ForeignWeak` (never a bare `set_`,
///   which would strand the old cross-file target).
/// * `#[bstack_ref]` — `set_<field>` (a foreign ref owns nothing) and
///   `replace_<field>`, both trafficking in plain `Foreign`.
///
/// Every reconstruction is an infallible `from_repr` wrap, so a failed commit always
/// hands the *new* value back through [`ReplaceError`] (there is no `lost` case).
pub(crate) fn foreign_mut_methods(
    vis: &syn::Visibility,
    fname: &Ident,
    ftarget: &TokenStream,
    on_disk: &TokenStream,
    kind: Kind,
    nullable: bool,
) -> Vec<TokenStream> {
    let off = quote!(self.0.start() + ::core::mem::offset_of!(#on_disk, #fname) as u64);
    // The RAII dual over the method's `'__m` (a `SELF` pointer stays bound to the
    // allocator borrow, mirroring `bstack_move!`).
    let dual = match kind {
        Kind::Owned => quote!(::bstack_raii::ForeignOwned<'__m, #ftarget>),
        Kind::Strong => quote!(::bstack_raii::ForeignRc<'__m, #ftarget>),
        Kind::Weak => quote!(::bstack_raii::ForeignWeak<'__m, #ftarget>),
        Kind::Ref => quote!(::bstack_raii::Foreign<'__m, #ftarget>),
        _ => unreachable!(),
    };
    let val_ty = if nullable {
        quote!(::core::option::Option<#dual>)
    } else {
        dual.clone()
    };
    // Consume a dual `v` (by value) into its stored `ForeignRepr`.
    let consume = |v: TokenStream| match kind {
        Kind::Owned | Kind::Strong | Kind::Weak => quote!((#v).into_foreign().repr()),
        Kind::Ref => quote!((#v).repr()),
        _ => unreachable!(),
    };
    // Rebuild the dual from a `ForeignRepr` (infallible). `unsafe`: the repr was
    // stored into this file, and the returned handle is bound to `'__m`.
    let rebuild = |r: TokenStream| match kind {
        Kind::Owned => quote!(unsafe {
            ::bstack_raii::ForeignOwned::from_foreign(
                ::bstack_raii::Foreign::<#ftarget>::from_repr(#r))
        }),
        Kind::Strong => quote!(unsafe {
            ::bstack_raii::ForeignRc::from_foreign(
                ::bstack_raii::Foreign::<#ftarget>::from_repr(#r))
        }),
        Kind::Weak => quote!(unsafe {
            ::bstack_raii::ForeignWeak::from_foreign(
                ::bstack_raii::Foreign::<#ftarget>::from_repr(#r))
        }),
        Kind::Ref => quote!(unsafe { ::bstack_raii::Foreign::<#ftarget>::from_repr(#r) }),
        _ => unreachable!(),
    };

    let mut out: Vec<TokenStream> = Vec::new();

    // ---- replace_<field>(&alloc, new) -> old ----
    let replace = format_ident!("replace_{}", fname);
    let read_old = quote! {
        let __stack = allocator.stack();
        let __off = #off;
        let mut __b = [0u8; 16];
        if let ::std::result::Result::Err(__e) = __stack.get_into(__off, &mut __b) {
            return ::core::result::Result::Err(::bstack_raii::ReplaceError::recovered(__e, value));
        }
        let __old: ::bstack_raii::ForeignRepr =
            ::bstack_raii::bytemuck::pod_read_unaligned(&__b);
    };
    let replace_body = if nullable {
        let consume_some = consume(quote!(__v));
        let rb_new = rebuild(quote!(__r));
        let rb_old = rebuild(quote!(__old));
        quote! {
            #read_old
            let (__new, __back): (
                ::bstack_raii::ForeignRepr,
                ::core::option::Option<::bstack_raii::ForeignRepr>,
            ) = match value {
                ::core::option::Option::Some(__v) => {
                    let __r = #consume_some;
                    (__r, ::core::option::Option::Some(__r))
                }
                ::core::option::Option::None =>
                    (::bstack_raii::ForeignRepr::new(0, 0), ::core::option::Option::None),
            };
            if let ::std::result::Result::Err(__e) =
                __stack.set(__off, ::bstack_raii::bytemuck::bytes_of(&__new))
            {
                let __hb: #val_ty = match __back {
                    ::core::option::Option::Some(__r) => ::core::option::Option::Some(#rb_new),
                    ::core::option::Option::None => ::core::option::Option::None,
                };
                return ::core::result::Result::Err(::bstack_raii::ReplaceError::recovered(__e, __hb));
            }
            if __old.offset() == 0 {
                ::core::result::Result::Ok(::core::option::Option::None)
            } else {
                ::core::result::Result::Ok(::core::option::Option::Some(#rb_old))
            }
        }
    } else {
        let consume_v = consume(quote!(value));
        let rb_new = rebuild(quote!(__new));
        let rb_old = rebuild(quote!(__old));
        quote! {
            #read_old
            let __new: ::bstack_raii::ForeignRepr = #consume_v;
            if let ::std::result::Result::Err(__e) =
                __stack.set(__off, ::bstack_raii::bytemuck::bytes_of(&__new))
            {
                return ::core::result::Result::Err(::bstack_raii::ReplaceError::recovered(__e, #rb_new));
            }
            ::core::result::Result::Ok(#rb_old)
        }
    };
    out.push(quote! {
        /// Install `value` and move the previous cross-file target out as its RAII
        /// handle (free / decrement it with `bstack_drop(&home)`, or re-store it).
        /// One crash-atomic 16-byte `set`; on I/O failure the *new* value is handed
        /// back through [`ReplaceError`](::bstack_raii::ReplaceError).
        #vis fn #replace<'__m, __A: ::bstack_raii::BStackRaiiAllocator>(
            &self,
            allocator: &'__m __A,
            value: #val_ty,
        ) -> ::core::result::Result<#val_ty, ::bstack_raii::ReplaceError<#val_ty>> {
            #replace_body
        }
    });

    // ---- set_<field> (foreign `ref` only — owns nothing) ----
    if kind == Kind::Ref {
        let setter = format_ident!("set_{}", fname);
        let new_repr = if nullable {
            quote!(match value {
                ::core::option::Option::Some(__v) => __v.repr(),
                ::core::option::Option::None => ::bstack_raii::ForeignRepr::new(0, 0),
            })
        } else {
            quote!(value.repr())
        };
        out.push(quote! {
            /// Repoint this foreign `#[bstack_ref]` (a ref owns nothing, so the old
            /// pointer is simply overwritten). One crash-atomic 16-byte `set`.
            #vis fn #setter<'__m, __A: ::bstack_raii::BStackRaiiAllocator>(
                &self,
                allocator: &'__m __A,
                value: #val_ty,
            ) -> ::std::io::Result<()> {
                let __new: ::bstack_raii::ForeignRepr = #new_repr;
                allocator
                    .stack()
                    .set(#off, ::bstack_raii::bytemuck::bytes_of(&__new))
            }
        });
    }

    out
}

/// Generate `(param, prep, init)` for one constructor field. Not called for
/// `#[bstack_weak]` fields. `nullable` fields take an `Option<Handle>` (None => 0).
pub(crate) fn ctor_field(
    fname: &Ident,
    inner_ty: &Type,
    kind: Kind,
    nullable: bool,
) -> (TokenStream, TokenStream, TokenStream) {
    // The prep body that turns a consumed handle into its `u64` offset.
    let (handle_ty, to_offset): (TokenStream, TokenStream) = match kind {
        Kind::Pod => {
            return (
                quote!(#fname: #inner_ty,),
                quote!(),
                quote!(#fname: #fname,),
            );
        }
        Kind::Owned => (
            quote!(::bstack_raii::BStackOwned<#inner_ty>),
            quote!({
                let __h = __handle.into_inner();
                ::bstack_raii::BStackBlock::range(&__h).start()
            }),
        ),
        Kind::Strong => (
            quote!(::bstack_raii::BStackRc<'__ctor, #inner_ty, __A>),
            quote!({
                let (__d, _) = __handle.into_raw();
                __d.into_range().start()
            }),
        ),
        Kind::Ref => (
            quote!(::bstack_raii::BStackRef<#inner_ty>),
            quote!(__handle.into_range().start()),
        ),
        Kind::Weak => unreachable!("weak fields are wired via set_<field>, not the constructor"),
        Kind::Embed => unreachable!("#[embed] fields are handled before ctor_field"),
    };
    if nullable {
        (
            quote!(#fname: ::core::option::Option<#handle_ty>,),
            quote! {
                let #fname: u64 = match #fname {
                    ::core::option::Option::Some(__handle) => #to_offset,
                    ::core::option::Option::None => 0u64,
                };
            },
            quote!(#fname: #fname,),
        )
    } else {
        (
            quote!(#fname: #handle_ty,),
            quote! { let #fname: u64 = { let __handle = #fname; #to_offset }; },
            quote!(#fname: #fname,),
        )
    }
}

/// Build one `bstack_move!` field: its type in the result tuple and the
/// expression that reconstructs it from the captured offset `cap`.
pub(crate) fn move_field(
    cap: &Ident,
    inner_ty: &Type,
    kind: Kind,
    nullable: bool,
) -> (TokenStream, TokenStream) {
    let size_od =
        quote!(::core::mem::size_of::<<#inner_ty as ::bstack_raii::BStackBlock>::OnDisk>() as u64);
    match kind {
        Kind::Pod => (quote!(#inner_ty), quote!(#cap)),
        Kind::Embed => unreachable!("#[embed] fields are handled before move_field"),
        // Weak is inherently nullable and stores the control offset.
        Kind::Weak => {
            let ty = quote! {
                ::core::option::Option<::bstack_raii::BStackWeak<'__mv, #inner_ty, __A>>
            };
            let recon = quote! {
                if #cap == 0 {
                    ::core::option::Option::None
                } else {
                    let __ctrl = unsafe {
                        ::bstack_raii::BStackRef::<
                            <#inner_ty as ::bstack_raii::BStackWeakable>::Control
                        >::from_range(::bstack_raii::BStackRange::new(
                            #cap,
                            ::core::mem::size_of::<
                                <#inner_ty as ::bstack_raii::BStackWeakable>::Control
                            >() as u64,
                        ))
                    };
                    ::core::option::Option::Some(
                        unsafe { ::bstack_raii::BStackWeak::from_raw(__ctrl, __alloc) }
                    )
                }
            };
            (ty, recon)
        }
        Kind::Owned => wrap_move(
            quote!(::bstack_raii::BStackOwned<#inner_ty>),
            quote! {
                unsafe {
                    ::bstack_raii::BStackOwned::from_raw(
                        <#inner_ty as ::bstack_raii::BStackBlock>::from_range(
                            ::bstack_raii::BStackRange::new(#cap, #size_od),
                        ),
                    )
                }
            },
            cap,
            nullable,
        ),
        Kind::Ref => wrap_move(
            quote!(::bstack_raii::BStackRef<#inner_ty>),
            quote! {
                unsafe {
                    ::bstack_raii::BStackRef::<#inner_ty>::from_range(
                        ::bstack_raii::BStackRange::new(#cap, #size_od),
                    )
                }
            },
            cap,
            nullable,
        ),
        Kind::Strong => wrap_move(
            quote!(::bstack_raii::BStackRc<'__mv, #inner_ty, __A>),
            quote! {
                {
                    let __data = unsafe {
                        ::bstack_raii::BStackRef::<#inner_ty>::from_range(
                            ::bstack_raii::BStackRange::new(#cap, #size_od),
                        )
                    };
                    let (__d, __c) =
                        <#inner_ty as ::bstack_raii::BStackShared>::strong_parts(__data, __alloc)?;
                    unsafe { ::bstack_raii::BStackRc::from_raw(__d, __c, __alloc) }
                }
            },
            cap,
            nullable,
        ),
    }
}

/// Wrap a move field's type/expr in `Option` when the field is nullable.
pub(crate) fn wrap_move(
    ty: TokenStream,
    build: TokenStream,
    cap: &Ident,
    nullable: bool,
) -> (TokenStream, TokenStream) {
    if nullable {
        (
            quote!(::core::option::Option<#ty>),
            quote! {
                if #cap == 0 {
                    ::core::option::Option::None
                } else {
                    ::core::option::Option::Some(#build)
                }
            },
        )
    } else {
        (ty, build)
    }
}

/// Generate the `set_<field>` method for a `#[bstack_weak]` field: point it at a
/// weak target (consumed), releasing whatever it held before.
pub(crate) fn weak_setter(
    vis: &syn::Visibility,
    fname: &Ident,
    fty: &Type,
    on_disk: &TokenStream,
) -> TokenStream {
    let setter = format_ident!("set_{}", fname);
    quote! {
        #vis fn #setter<'__s, __A: ::bstack_raii::BStackRaiiAllocator>(
            &self,
            allocator: &'__s __A,
            weak: ::bstack_raii::BStackWeak<'__s, #fty, __A>,
        ) -> ::std::io::Result<()> {
            let __field = self.0.start() + ::core::mem::offset_of!(#on_disk, #fname) as u64;
            ::bstack_raii::set_weak_field(allocator, __field, weak)
        }
    }
}

/// Assemble the `new` constructor.
#[allow(clippy::too_many_arguments)]
pub(crate) fn constructor(
    vis: &syn::Visibility,
    on_disk: &TokenStream,
    // The `XOnDisk` name for a struct *literal* — bare, or a turbofish
    // `XOnDisk::<T>` when generic (`XOnDisk<T> { .. }` in expression position would
    // parse as a comparison, and `<T>::OnDisk` fields don't infer `T`). `on_disk`
    // above is the plain *type* (`XOnDisk<T>`), for `size_of`.
    on_disk_ctor: &TokenStream,
    mode: Mode,
    ctrl_eightcc: &TokenStream,
    params: &[TokenStream],
    preps: &[TokenStream],
    inits: &[TokenStream],
    // Steps run *after* the block's OnDisk is written (with `__data` = the block
    // range in scope): `#[embed]` fields copy each child block into its now-written
    // inline slot via `BStack::copy` and free the child shell.
    post: &[TokenStream],
) -> TokenStream {
    let header = quote! {
        __bstack_header: ::bstack_raii::BlockHeader {
            size: ::core::mem::size_of::<#on_disk>() as u64,
            tag: <Self as ::bstack_raii::BStackCast>::eightcc(),
        },
    };
    let size = quote!(::core::mem::size_of::<#on_disk>() as u64);

    match mode {
        // Plain and `rc` are already a single atomic write: the injected refcount
        // is baked into the OnDisk image, so one `alloc` + one `write_range` fully
        // constructs the block (a crash before the write just orphans the block).
        Mode::Plain | Mode::Rc => {
            let injected = if let Mode::Rc = mode {
                quote!(__bstack_refcount: 1u64,)
            } else {
                quote!()
            };
            let ret = if let Mode::Rc = mode {
                quote!(::bstack_raii::BStackRc<'__ctor, Self, __A>)
            } else {
                quote!(::bstack_raii::BStackOwned<Self>)
            };
            let finish = if let Mode::Rc = mode {
                quote! {
                    ::std::result::Result::Ok(unsafe {
                        ::bstack_raii::BStackRc::from_raw(
                            ::bstack_raii::BStackRef::from_range(__data),
                            ::core::option::Option::None,
                            allocator,
                        )
                    })
                }
            } else {
                quote! {
                    ::std::result::Result::Ok(unsafe {
                        ::bstack_raii::BStackOwned::from_raw(
                            <Self as ::bstack_raii::BStackBlock>::from_range(__data),
                        )
                    })
                }
            };
            quote! {
                // A block with many fields yields a many-arg `new`; that is expected.
                #[allow(clippy::too_many_arguments)]
                #vis fn new<'__ctor, __A: ::bstack_raii::BStackRaiiAllocator>(
                    allocator: &'__ctor __A,
                    #(#params)*
                ) -> ::std::io::Result<#ret> {
                    #(#preps)*
                    let __on_disk = #on_disk_ctor {
                        #header
                        #injected
                        #(#inits)*
                    };
                    let mut __slice = allocator.alloc(#size)?;
                    let __data = __slice.as_range();
                    if let ::std::result::Result::Err(__e) =
                        __slice.write_range(0, ::bstack_raii::bytemuck::bytes_of(&__on_disk))
                    {
                        let _ = allocator.dealloc(__slice);
                        return ::std::result::Result::Err(__e);
                    }
                    #(#post)*
                    #finish
                }
            }
        }
        // `(rc, weak)` needs two blocks (data + control). Allocate both up front,
        // bake the real control back-pointer into the data image, and commit both
        // images in ONE `set_batched` — so the block is created atomically, with
        // no separate back-pointer write and no transient half-wired state (a
        // crash before the commit just orphans the two fresh blocks).
        Mode::RcWeak => {
            let ctrl_size = quote! {
                ::core::mem::size_of::<<Self as ::bstack_raii::BStackWeakable>::Control>() as u64
            };
            quote! {
                // A block with many fields yields a many-arg `new`; that is expected.
                #[allow(clippy::too_many_arguments)]
                #vis fn new<'__ctor, __A: ::bstack_raii::BStackRaiiAllocator>(
                    allocator: &'__ctor __A,
                    #(#params)*
                ) -> ::std::io::Result<::bstack_raii::BStackRc<'__ctor, Self, __A>> {
                    #(#preps)*
                    // Allocate data + control up front (atomically when the
                    // allocator supports bulk); both are orphans until the commit.
                    let __blocks = ::bstack_raii::BStackRaiiAllocator::alloc_many(allocator, &[#size, #ctrl_size])?;
                    let __data = __blocks[0];
                    let __ctrl = __blocks[1];
                    let __on_disk = #on_disk_ctor {
                        #header
                        __bstack_ctrl: __ctrl.start(),
                        #(#inits)*
                    };
                    let __ctrl_payload = ::bstack_raii::build_control_payload(
                        #ctrl_eightcc,
                        __data.start(),
                        #ctrl_size,
                    );
                    let __writes: [(u64, ::std::vec::Vec<u8>); 2] = [
                        (
                            __data.start(),
                            ::bstack_raii::bytemuck::bytes_of(&__on_disk).to_vec(),
                        ),
                        (__ctrl.start(), __ctrl_payload),
                    ];
                    if let ::std::result::Result::Err(__e) =
                        allocator.stack().set_batched(__writes)
                    {
                        let _ = ::bstack_raii::BStackRaiiAllocator::free_many(allocator, [__data, __ctrl]);
                        return ::std::result::Result::Err(__e);
                    }
                    #(#post)*
                    ::std::result::Result::Ok(unsafe {
                        ::bstack_raii::BStackRc::from_raw(
                            ::bstack_raii::BStackRef::from_range(__data),
                            ::core::option::Option::Some(__ctrl),
                            allocator,
                        )
                    })
                }
            }
        }
    }
}

/// Build an owned/strong teardown statement: resolve the child field's `u64`
/// offset into a typed `BStackRef<#inner_ty>` bound to `__child`, then run
/// `body`. A `nullable` field guards on a non-zero offset.
pub(crate) fn child_range_stmt(
    fname: &Ident,
    inner_ty: &Type,
    nullable: bool,
    body: TokenStream,
) -> TokenStream {
    let core = quote! {
        let __range = ::bstack_raii::BStackRange::new(
            __off,
            ::core::mem::size_of::<<#inner_ty as ::bstack_raii::BStackBlock>::OnDisk>() as u64,
        );
        let __child = unsafe { ::bstack_raii::BStackRef::<#inner_ty>::from_range(__range) };
        #body
    };
    if nullable {
        quote! { { let __off = __on_disk.#fname; if __off != 0 { #core } } }
    } else {
        quote! { { let __off = __on_disk.#fname; #core } }
    }
}

/// Teardown statement for a `#[bstack_weak]` field, which stores the child's
/// control-block offset (`0` = unset).
pub(crate) fn weak_drop_stmt(fname: &Ident, inner_ty: &Type) -> TokenStream {
    quote! {
        {
            let __off = __on_disk.#fname;
            if __off != 0 {
                let __ctrl = unsafe {
                    ::bstack_raii::BStackRef::<
                        <#inner_ty as ::bstack_raii::BStackWeakable>::Control
                    >::from_range(::bstack_raii::BStackRange::new(
                        __off,
                        ::core::mem::size_of::<
                            <#inner_ty as ::bstack_raii::BStackWeakable>::Control
                        >() as u64,
                    ))
                };
                ::bstack_raii::WeakRef::<#inner_ty>(__ctrl).bstack_drop(allocator)?;
            }
        }
    }
}

/// A `TryCloneIn` statement for a scalar reference/POD field, the mirror of the
/// teardown dispatch run in reverse. Reads/patches the mutable `__od` OnDisk copy
/// and appends allocations / refcount bumps to `__plan`. `None` = nothing to do:
/// POD and `#[bstack_ref]` fields are byte-copied verbatim (a ref clone aliases
/// the same borrowed target — see the borrow-rules TODO). A `0` offset (a null
/// `Option` field, or an unset weak) is left copied as-is.
pub(crate) fn clone_field_stmt(fname: &Ident, inner_ty: &Type, kind: Kind) -> Option<TokenStream> {
    match kind {
        // Deep-clone the owned child into a fresh block, repoint the field.
        Kind::Owned => Some(quote! {
            {
                let __coff: u64 = __od.#fname;
                if __coff != 0 {
                    let __child = <#inner_ty as ::bstack_raii::BStackBlock>::from_range(
                        ::bstack_raii::BStackRange::new(
                            __coff,
                            ::core::mem::size_of::<
                                <#inner_ty as ::bstack_raii::BStackBlock>::OnDisk
                            >() as u64,
                        ),
                    );
                    let __new = __child.__bstack_clone_into(allocator, __plan)?;
                    __od.#fname = __new.start();
                }
            }
        }),
        // Shared: keep the copied data offset, bump the target's strong count.
        Kind::Strong => Some(quote! {
            {
                let __coff: u64 = __od.#fname;
                if __coff != 0 {
                    let __child = unsafe {
                        ::bstack_raii::BStackRef::<#inner_ty>::from_range(
                            ::bstack_raii::BStackRange::new(
                                __coff,
                                ::core::mem::size_of::<
                                    <#inner_ty as ::bstack_raii::BStackBlock>::OnDisk
                                >() as u64,
                            ),
                        )
                    };
                    __plan.bump_strong(__child, allocator)?;
                }
            }
        }),
        // Weak: keep the copied control offset, bump the target's weak count.
        Kind::Weak => Some(quote! {
            {
                let __coff: u64 = __od.#fname;
                if __coff != 0 {
                    __plan.bump_weak(__coff);
                }
            }
        }),
        // `#[embed]` is folded in its own branch (it `continue`s before this).
        Kind::Ref | Kind::Pod | Kind::Embed => None,
    }
}

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
                            return Err(Error::new_spanned(other, "expected a string literal"));
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
                            "expected `repr(u8|u16|u32|u64|i8|i16|i32|i64|aligned)`",
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
                                "`repr(usize)` / `repr(isize)` are not allowed — bstack offsets \
                                 are 64-bit, so pick an explicit width (e.g. `repr(u64)`)",
                            ));
                        }
                        _ => {
                            return Err(Error::new_spanned(
                                &r,
                                "expected `u8|u16|u32|u64|i8|i16|i32|i64|aligned`",
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
                                    "expected `overlong_tag`, `coerced_ref`, or `deprecated`",
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
                "`weak` requires `rc` (use `rc, weak`)",
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
    "expected `rc`, `weak`, `tag = \"...\"`, `ctrl_tag = \"...\"`, `allow(...)`, or (enums) `repr(...)`"
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
                "at most one bstack ownership annotation is allowed here",
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
                "expected an integer literal discriminant",
            )),
        },
        other => Err(Error::new_spanned(
            other,
            "expected an integer literal discriminant",
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
