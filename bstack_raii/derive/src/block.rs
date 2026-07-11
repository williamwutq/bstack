//! Implementation of the `#[bstack_block]` attribute macro.
//!
//! Given an ergonomic `struct X { .. }`, it emits:
//! * `struct X(BStackRange)` — the typed, without-allocator handle.
//! * `struct XOnDisk` — `#[repr(C, packed)]`, `Pod`, the on-disk payload.
//! * `impl BStackCast / BStackBlock / BStackDrop for X`.
//! * For `rc` / `(rc, weak)`: the injected refcount / `ctrl` field, an
//!   `impl BStackShared`, and (for `rc, weak`) the `XOnDiskRef` control block
//!   plus `impl BStackWeakable`.
//! * Field accessors, and (unless the block has a `#[bstack_weak]` field) a
//!   `new` constructor that allocates and wires the block.

use proc_macro2::{Span, TokenStream};
use quote::{format_ident, quote};
use syn::parse::Parser;
use syn::punctuated::Punctuated;
use syn::{
    Error, Expr, ExprLit, Fields, GenericArgument, Ident, ItemStruct, Lit, Meta, PathArguments,
    Token, Type,
};

/// The block mode from the attribute arguments.
#[derive(Clone, Copy, PartialEq)]
enum Mode {
    /// `#[bstack_block]`
    Plain,
    /// `#[bstack_block(rc)]`
    Rc,
    /// `#[bstack_block(rc, weak)]`
    RcWeak,
}

/// One field's ownership classification.
#[derive(Clone, Copy, PartialEq)]
enum Kind {
    Owned,
    Strong,
    Weak,
    Ref,
    /// POD field stored inline.
    Pod,
}

pub fn expand(attr: TokenStream, input: ItemStruct) -> syn::Result<TokenStream> {
    let attr = parse_attr(attr)?;
    let mode = attr.mode;

    if !input.generics.params.is_empty() {
        return Err(Error::new_spanned(
            &input.generics,
            "#[bstack_block] does not support generic block types",
        ));
    }

    let fields = match &input.fields {
        Fields::Named(named) => &named.named,
        _ => {
            return Err(Error::new_spanned(
                &input.fields,
                "#[bstack_block] requires a struct with named fields",
            ));
        }
    };

    let name = &input.ident;
    let vis = &input.vis;
    let on_disk = format_ident!("{}OnDisk", name);
    let control = format_ident!("{}OnDiskRef", name);

    // On-disk fields: header, then the injected refcount/ctrl (if any), then user
    // fields lowered per annotation.
    let mut on_disk_fields = Vec::new();
    match mode {
        Mode::Plain => {}
        Mode::Rc => on_disk_fields.push(quote!(__bstack_refcount: u64,)),
        Mode::RcWeak => on_disk_fields.push(quote!(__bstack_ctrl: u64,)),
    }

    let mut drop_stmts = Vec::new();
    let mut pod_types: Vec<&Type> = Vec::new();
    let mut accessors = Vec::new();
    let mut setters = Vec::new();
    let mut ctor_params = Vec::new();
    let mut ctor_preps = Vec::new();
    let mut ctor_inits = Vec::new();
    // `bstack_move!` support (owned/ref/pod fields only, plain blocks only).
    let mut mv_caps = Vec::new();
    let mut mv_types = Vec::new();
    let mut mv_recon = Vec::new();
    // Whether any field was written `&T` (coerced to owned `T`, with a warning).
    let mut ref_coerced = false;

    for field in fields {
        let fname = field.ident.as_ref().expect("named field");
        let kind = classify(field)?;

        // Ergonomic: `&T` is coerced to owned `T` (and `&str` to `String`), with
        // a warning. `eff_ty` is the type after stripping a leading `&`.
        let eff_ty: &Type = match &field.ty {
            Type::Reference(r) => &r.elem,
            other => other,
        };
        if matches!(&field.ty, Type::Reference(_)) {
            ref_coerced = true;
        }

        // `Vec<T>` / `String` fields (and `&str` → `String`): a fixed-size
        // descriptor offset on disk, a `BStackVec` at runtime. Handled here.
        let vinfo = if is_str(eff_ty) {
            Some(VecInfo {
                elem: quote!(u8),
                is_string: true,
            })
        } else {
            vec_field(eff_ty)
        };
        if let Some(vinfo) = vinfo {
            if kind != Kind::Owned {
                return Err(Error::new_spanned(
                    &field.ty,
                    "`Vec<T>` / `String` fields must be annotated `#[bstack_owned]`",
                ));
            }
            let elem = &vinfo.elem;
            on_disk_fields.push(quote!(#fname: u64,));
            drop_stmts.push(vec_drop_stmt(fname, elem));
            accessors.push(vec_accessor(vis, fname, elem, &on_disk));
            let (param, prep, init) = vec_ctor(fname, &vinfo);
            ctor_params.push(param);
            ctor_preps.push(prep);
            ctor_inits.push(init);
            let cap = format_ident!("__cap_{}", fname);
            mv_caps.push(quote!(let #cap = __od.#fname;));
            let (mv_ty, mv_rc) = vec_move(&cap, elem);
            mv_types.push(mv_ty);
            mv_recon.push(mv_rc);
            continue;
        }

        // `Option<Inner>` makes a reference field nullable: `0` on disk == `None`
        // (no allocation ever lives at offset 0). The annotation applies to Inner.
        let (inner_ty, nullable) = match option_inner(eff_ty) {
            Some(inner) => (inner, true),
            None => (eff_ty, false),
        };
        if nullable && kind == Kind::Pod {
            return Err(Error::new_spanned(
                &field.ty,
                "Option is only supported on #[bstack_owned] / #[bstack_strong] / \
                 #[bstack_weak] / #[bstack_ref] fields",
            ));
        }

        // On-disk lowering.
        match kind {
            Kind::Pod => {
                on_disk_fields.push(quote!(#fname: #inner_ty,));
                pod_types.push(inner_ty);
            }
            _ => on_disk_fields.push(quote!(#fname: u64,)),
        }

        // Teardown.
        match kind {
            Kind::Owned => drop_stmts.push(child_range_stmt(
                fname,
                inner_ty,
                nullable,
                quote!(::bstack_raii::OwnedRef(__child).bstack_drop(allocator)?;),
            )),
            Kind::Strong => drop_stmts.push(child_range_stmt(
                fname,
                inner_ty,
                nullable,
                quote!(<#inner_ty as ::bstack_raii::BStackShared>::drop_strong_ref(__child, allocator)?;),
            )),
            Kind::Weak => drop_stmts.push(weak_drop_stmt(fname, inner_ty)),
            Kind::Ref | Kind::Pod => {}
        }

        // Accessor.
        accessors.push(accessor(vis, fname, inner_ty, &on_disk, kind, nullable));

        // Constructor. Weak fields are not parameters — they start null and are
        // wired afterwards via the generated `set_<field>`.
        if kind == Kind::Weak {
            ctor_inits.push(quote!(#fname: 0u64,));
            setters.push(weak_setter(vis, fname, inner_ty, &on_disk));
        } else {
            let (param, prep, init) = ctor_field(fname, inner_ty, kind, nullable);
            ctor_params.push(param);
            ctor_preps.push(prep);
            ctor_inits.push(init);
        }

        // `bstack_move!` pieces: capture the field before the parent is freed,
        // then reconstruct the transferred handle after.
        let cap = format_ident!("__cap_{}", fname);
        mv_caps.push(quote!(let #cap = __od.#fname;));
        let (mv_ty, mv_rc) = move_field(&cap, inner_ty, kind, nullable);
        mv_types.push(mv_ty);
        mv_recon.push(mv_rc);
    }

    // EightCC tags: readable prefix over a hash of `crate ++ type_name`. The
    // control tag uses the same hash with the prefix lowercased.
    let type_name = name.to_string();
    let crate_name = std::env::var("CARGO_PKG_NAME").unwrap_or_default();
    let hash = fnv1a64(&format!("{crate_name}\0{type_name}"));
    let data_prefix = attr.tag.as_ref().map_or_else(
        || auto_prefix(&type_name),
        |t| t.bytes().collect::<Vec<u8>>(),
    );
    let ctrl_prefix = attr.ctrl_tag.as_ref().map_or_else(
        || {
            data_prefix
                .iter()
                .map(u8::to_ascii_lowercase)
                .collect::<Vec<u8>>()
        },
        |t| t.bytes().collect::<Vec<u8>>(),
    );
    let data_tag = build_tag(hash, &data_prefix);
    let ctrl_tag = build_tag(hash, &ctrl_prefix);
    let data_eightcc = eightcc_expr(&data_tag.bytes);
    let ctrl_eightcc = eightcc_expr(&ctrl_tag.bytes);

    // The warnings use the `deprecated` mechanism, so a real `#[allow(deprecated)]`
    // on the struct also silences them (in addition to the `allow(...)` args).
    let allow_deprecated = input.attrs.iter().any(is_allow_deprecated);
    let allow_overlong = attr.allow_overlong || allow_deprecated;
    let allow_coerced_ref = attr.allow_coerced_ref || allow_deprecated;

    // Overlong `tag =` / `ctrl_tag =` overrides warn (unless silenced) + truncate.
    let overlong_warning = if (data_tag.truncated || ctrl_tag.truncated) && !allow_overlong {
        let warn_fn = format_ident!("__bstack_tag_overlong_{}", name);
        let msg = format!(
            "#[bstack_block] on `{type_name}`: a tag override longer than 8 bytes was truncated; \
             add `allow(overlong_tag)` to silence"
        );
        quote! {
            #[doc(hidden)]
            #[allow(dead_code, non_snake_case)]
            fn #warn_fn() {
                #[deprecated(note = #msg)]
                fn overlong_tag() {}
                overlong_tag();
            }
        }
    } else {
        quote!()
    };

    // A `&T` field is coerced to owned `T` (`&str` to `String`); warn once.
    let ref_warning = if ref_coerced && !allow_coerced_ref {
        let warn_fn = format_ident!("__bstack_ref_coerced_{}", name);
        let msg = format!(
            "#[bstack_block] on `{type_name}`: a `&T` field was coerced to owned `T` \
             (and `&str` to `String`); write the owned type directly, or add \
             `allow(coerced_ref)` to silence"
        );
        quote! {
            #[doc(hidden)]
            #[allow(dead_code, non_snake_case)]
            fn #warn_fn() {
                #[deprecated(note = #msg)]
                fn ref_coerced() {}
                ref_coerced();
            }
        }
    } else {
        quote!()
    };

    // BStackShared / BStackWeakable / control block for the refcounted modes.
    let shared_impl = match mode {
        Mode::Plain => quote!(),
        Mode::Rc => quote! {
            impl ::bstack_raii::BStackShared for #name {
                fn drop_strong_ref<__A: ::bstack_raii::BStackOwnedSliceAllocator>(
                    data: ::bstack_raii::BStackRef<Self>,
                    allocator: &__A,
                ) -> ::std::io::Result<()> {
                    use ::bstack_raii::BStackDrop as _;
                    ::bstack_raii::StrongRef(data).bstack_drop(allocator)
                }
                fn strong_parts<__A: ::bstack_raii::BStackOwnedSliceAllocator>(
                    data: ::bstack_raii::BStackRef<Self>,
                    _allocator: &__A,
                ) -> ::std::io::Result<(
                    ::bstack_raii::BStackRef<Self>,
                    ::core::option::Option<::bstack_raii::BStackRange>,
                )> {
                    ::std::result::Result::Ok((data, ::core::option::Option::None))
                }
            }
        },
        Mode::RcWeak => quote! {
            impl ::bstack_raii::BStackShared for #name {
                fn drop_strong_ref<__A: ::bstack_raii::BStackOwnedSliceAllocator>(
                    data: ::bstack_raii::BStackRef<Self>,
                    allocator: &__A,
                ) -> ::std::io::Result<()> {
                    use ::bstack_raii::BStackDrop as _;
                    ::bstack_raii::StrongWeakRef::from_disk(data, allocator)?
                        .bstack_drop(allocator)
                }
                fn strong_parts<__A: ::bstack_raii::BStackOwnedSliceAllocator>(
                    data: ::bstack_raii::BStackRef<Self>,
                    allocator: &__A,
                ) -> ::std::io::Result<(
                    ::bstack_raii::BStackRef<Self>,
                    ::core::option::Option<::bstack_raii::BStackRange>,
                )> {
                    let __swr = ::bstack_raii::StrongWeakRef::from_disk(data, allocator)?;
                    ::std::result::Result::Ok((
                        __swr.0,
                        ::core::option::Option::Some(__swr.1.into_range()),
                    ))
                }
            }
        },
    };

    let weakable_items = if mode == Mode::RcWeak {
        quote! {
            #[repr(C, packed)]
            #[derive(::core::clone::Clone, ::core::marker::Copy)]
            #vis struct #control {
                __bstack_header: ::bstack_raii::BlockHeader,
                __bstack_strong: u64,
                __bstack_weak: u64,
                __bstack_x: u64,
            }
            unsafe impl ::bstack_raii::Zeroable for #control {}
            unsafe impl ::bstack_raii::Pod for #control {}

            impl ::bstack_raii::BStackWeakable for #name {
                type Control = #control;
            }
        }
    } else {
        quote!()
    };

    let constructor = constructor(
        vis,
        &on_disk,
        mode,
        &ctrl_eightcc,
        &ctor_params,
        &ctor_preps,
        &ctor_inits,
    );

    // The field destructure is generated for every mode: plain blocks use it via
    // `BStackOwned` (infallible), rc / rc,weak via `BStackRc::try_move`.
    let move_impl = {
        quote! {
            // Implemented on the block type (local downstream) so the orphan rule
            // is satisfied; `bstack_move!` selects it from the argument's type.
            impl ::bstack_raii::BStackMove for #name {
                type Fields<'__mv, __A: ::bstack_raii::BStackOwnedSliceAllocator> =
                    ( #(#mv_types,)* );
                fn bstack_move<'__mv, __A: ::bstack_raii::BStackOwnedSliceAllocator>(
                    owned: ::bstack_raii::BStackOwned<'__mv, Self, __A>,
                ) -> ::std::io::Result<Self::Fields<'__mv, __A>> {
                    // Take the inner handle out (defusing the owned Drop) and read
                    // the payload before freeing anything.
                    let (__inner, __alloc) = owned.into_raw_parts();
                    let __stack = __alloc.stack();
                    let __range = ::bstack_raii::BStackBlock::range(&__inner);
                    let mut __buf = [0u8; ::core::mem::size_of::<#on_disk>()];
                    let __r = unsafe { ::bstack_raii::BStackRef::<#name>::from_range(__range) };
                    let __od: #on_disk = *__r.read_on_disk(__stack, &mut __buf)?;
                    #(#mv_caps)*
                    // Free the parent shell only; children stay live on disk.
                    unsafe { ::bstack_raii::dealloc_range(__alloc, __range)?; }
                    ::std::result::Result::Ok(( #(#mv_recon,)* ))
                }
            }
        }
    };

    Ok(quote! {
        #[derive(::core::clone::Clone, ::core::marker::Copy)]
        #vis struct #name(::bstack_raii::BStackRange);

        #[repr(C, packed)]
        #[derive(::core::clone::Clone, ::core::marker::Copy)]
        #vis struct #on_disk {
            __bstack_header: ::bstack_raii::BlockHeader,
            #(#on_disk_fields)*
        }

        // SAFETY: `#[repr(C, packed)]` guarantees no padding, and every field is
        // `Pod` (u64 for refs/injected counters, header is Pod, each inline field
        // is asserted `Pod` below), so all bit patterns are valid.
        unsafe impl ::bstack_raii::Zeroable for #on_disk {}
        unsafe impl ::bstack_raii::Pod for #on_disk {}

        const _: fn() = || {
            fn __assert_pod<__T: ::bstack_raii::Pod>() {}
            #( __assert_pod::<#pod_types>(); )*
        };

        impl ::bstack_raii::BStackCast for #name {
            fn eightcc() -> ::bstack_raii::EightCC {
                #data_eightcc
            }
        }

        impl ::bstack_raii::BStackBlock for #name {
            type OnDisk = #on_disk;
            fn from_range(range: ::bstack_raii::BStackRange) -> Self {
                #name(range)
            }
            fn range(&self) -> ::bstack_raii::BStackRange {
                self.0
            }
        }

        impl ::bstack_raii::BStackDrop for #name {
            fn bstack_drop<__A: ::bstack_raii::BStackOwnedSliceAllocator>(
                self,
                allocator: &__A,
            ) -> ::std::io::Result<()> {
                use ::bstack_raii::BStackDrop as _;
                let __stack = allocator.stack();
                let mut __buf = [0u8; ::core::mem::size_of::<#on_disk>()];
                let __r = unsafe { ::bstack_raii::BStackRef::<Self>::from_range(self.0) };
                let __on_disk: #on_disk = *__r.read_on_disk(__stack, &mut __buf)?;
                #(#drop_stmts)*
                unsafe { ::bstack_raii::dealloc_range(allocator, self.0) }
            }
        }

        impl #name {
            #(#accessors)*
            #(#setters)*

            /// Borrow this block as an untyped slice (infallible upcast).
            #vis fn as_slice<'__s>(
                &self,
                stack: &'__s ::bstack_raii::BStack,
            ) -> ::bstack_raii::BStackSlice<'__s> {
                unsafe {
                    ::bstack_raii::BStackSlice::from_raw_range(
                        stack,
                        ::bstack_raii::BStackBlock::range(self),
                    )
                }
            }

            #constructor
        }

        #shared_impl
        #weakable_items
        #move_impl
        #overlong_warning
        #ref_warning
    })
}

/// A `Vec<T>` / `String` field: its element type (tokens) and whether it's a
/// `String` (so the constructor takes `&str`).
struct VecInfo {
    elem: TokenStream,
    is_string: bool,
}

/// Whether `ty` is the `str` type.
fn is_str(ty: &Type) -> bool {
    matches!(ty, Type::Path(tp) if tp.path.segments.last().is_some_and(|s| s.ident == "str"))
}

/// Detect `Vec<T>` / `String` field types.
fn vec_field(ty: &Type) -> Option<VecInfo> {
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

/// Teardown for an owned `Vec<T>` / `String` field: free its `BStackVec` (data +
/// descriptor blocks).
fn vec_drop_stmt(fname: &Ident, _elem: &TokenStream) -> TokenStream {
    quote! {
        {
            use ::bstack_raii::BStackDrop as _;
            ::bstack_raii::VecRef::from_descriptor(__on_disk.#fname).bstack_drop(allocator)?;
        }
    }
}

/// Accessor for a `Vec<T>` / `String` field: resolve the descriptor offset to a
/// `BStackVec` handle. Takes the allocator (the vector's ops need it).
fn vec_accessor(
    vis: &syn::Visibility,
    fname: &Ident,
    elem: &TokenStream,
    on_disk: &Ident,
) -> TokenStream {
    quote! {
        #vis fn #fname<'__v, __A: ::bstack_raii::BStackOwnedSliceAllocator>(
            &self,
            allocator: &'__v __A,
        ) -> ::std::io::Result<::bstack_raii::BStackVec<'__v, #elem, __A>> {
            let __field = self.0.start() + ::core::mem::offset_of!(#on_disk, #fname) as u64;
            let mut __buf = [0u8; 8];
            allocator.stack().get_into(__field, &mut __buf)?;
            ::std::result::Result::Ok(unsafe {
                ::bstack_raii::BStackVec::from_descriptor(u64::from_le_bytes(__buf), allocator)
            })
        }
    }
}

/// Constructor `(param, prep, init)` for a `Vec<T>` / `String` field: build the
/// `BStackVec` from the passed data and store its descriptor offset.
fn vec_ctor(fname: &Ident, vinfo: &VecInfo) -> (TokenStream, TokenStream, TokenStream) {
    let elem = &vinfo.elem;
    let (param_ty, data): (TokenStream, TokenStream) = if vinfo.is_string {
        (quote!(&str), quote!(#fname.as_bytes()))
    } else {
        (quote!(&[#elem]), quote!(#fname))
    };
    (
        quote!(#fname: #param_ty,),
        quote! {
            let #fname: u64 =
                ::bstack_raii::BStackVec::<#elem, __A>::from_slice(allocator, #data)?
                    .descriptor()
                    .start();
        },
        quote!(#fname: #fname,),
    )
}

/// `bstack_move!` field for a `Vec<T>` / `String`: yield the `BStackVec` handle.
fn vec_move(cap: &Ident, elem: &TokenStream) -> (TokenStream, TokenStream) {
    (
        quote!(::bstack_raii::BStackVec<'__mv, #elem, __A>),
        quote!(unsafe { ::bstack_raii::BStackVec::from_descriptor(#cap, __alloc) }),
    )
}

/// Return `Some(Inner)` if `ty` is `Option<Inner>`.
fn option_inner(ty: &Type) -> Option<&Type> {
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

/// Generate the reader method for one field. `nullable` (an `Option<_>` field)
/// makes ref accessors return `Option<Handle>`, treating a `0` offset as `None`.
fn accessor(
    vis: &syn::Visibility,
    fname: &Ident,
    inner_ty: &Type,
    on_disk: &Ident,
    kind: Kind,
    nullable: bool,
) -> TokenStream {
    // Weak fields hold a control offset; the accessor attempts a live upgrade.
    if kind == Kind::Weak {
        return quote! {
            #vis fn #fname<'__u, __A: ::bstack_raii::BStackOwnedSliceAllocator>(
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
        let mut __buf = [0u8; ::core::mem::size_of::<#on_disk>()];
        let __r = unsafe { ::bstack_raii::BStackRef::<Self>::from_range(self.0) };
        let __od: #on_disk = *__r.read_on_disk(stack, &mut __buf)?;
    };
    if kind == Kind::Pod {
        return quote! {
            #vis fn #fname(&self, stack: &::bstack_raii::BStack) -> ::std::io::Result<#inner_ty> {
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
            #vis fn #fname(
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
            #vis fn #fname(&self, stack: &::bstack_raii::BStack) -> ::std::io::Result<#inner_ty> {
                #read
                ::std::result::Result::Ok(#resolve)
            }
        }
    }
}

/// Generate `(param, prep, init)` for one constructor field. Not called for
/// `#[bstack_weak]` fields. `nullable` fields take an `Option<Handle>` (None => 0).
fn ctor_field(
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
            quote!(::bstack_raii::BStackOwned<'__ctor, #inner_ty, __A>),
            quote!({
                let (__h, _) = __handle.into_raw_parts();
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
fn move_field(
    cap: &Ident,
    inner_ty: &Type,
    kind: Kind,
    nullable: bool,
) -> (TokenStream, TokenStream) {
    let size_od =
        quote!(::core::mem::size_of::<<#inner_ty as ::bstack_raii::BStackBlock>::OnDisk>() as u64);
    match kind {
        Kind::Pod => (quote!(#inner_ty), quote!(#cap)),
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
            quote!(::bstack_raii::BStackOwned<'__mv, #inner_ty, __A>),
            quote! {
                unsafe {
                    ::bstack_raii::BStackOwned::from_raw(
                        <#inner_ty as ::bstack_raii::BStackBlock>::from_range(
                            ::bstack_raii::BStackRange::new(#cap, #size_od),
                        ),
                        __alloc,
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
fn wrap_move(
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
fn weak_setter(vis: &syn::Visibility, fname: &Ident, fty: &Type, on_disk: &Ident) -> TokenStream {
    let setter = format_ident!("set_{}", fname);
    quote! {
        #vis fn #setter<'__s, __A: ::bstack_raii::BStackOwnedSliceAllocator>(
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
fn constructor(
    vis: &syn::Visibility,
    on_disk: &Ident,
    mode: Mode,
    ctrl_eightcc: &TokenStream,
    params: &[TokenStream],
    preps: &[TokenStream],
    inits: &[TokenStream],
) -> TokenStream {
    let injected = match mode {
        Mode::Plain => quote!(),
        Mode::Rc => quote!(__bstack_refcount: 1u64,),
        Mode::RcWeak => quote!(__bstack_ctrl: 0u64,),
    };
    let ret = match mode {
        Mode::Plain => quote!(::bstack_raii::BStackOwned<'__ctor, Self, __A>),
        _ => quote!(::bstack_raii::BStackRc<'__ctor, Self, __A>),
    };
    let finish = match mode {
        Mode::Plain => quote! {
            ::std::result::Result::Ok(unsafe {
                ::bstack_raii::BStackOwned::from_raw(
                    <Self as ::bstack_raii::BStackBlock>::from_range(__data),
                    allocator,
                )
            })
        },
        Mode::Rc => quote! {
            ::std::result::Result::Ok(unsafe {
                ::bstack_raii::BStackRc::from_raw(
                    ::bstack_raii::BStackRef::from_range(__data),
                    ::core::option::Option::None,
                    allocator,
                )
            })
        },
        Mode::RcWeak => {
            quote! {
                let __ctrl = match ::bstack_raii::alloc_control(
                    allocator,
                    #ctrl_eightcc,
                    __data,
                    ::core::mem::size_of::<<Self as ::bstack_raii::BStackWeakable>::Control>() as u64,
                ) {
                    ::std::result::Result::Ok(__c) => __c,
                    ::std::result::Result::Err(__e) => {
                        let _ = unsafe { ::bstack_raii::dealloc_range(allocator, __data) };
                        return ::std::result::Result::Err(__e);
                    }
                };
                ::std::result::Result::Ok(unsafe {
                    ::bstack_raii::BStackRc::from_raw(
                        ::bstack_raii::BStackRef::from_range(__data),
                        ::core::option::Option::Some(__ctrl),
                        allocator,
                    )
                })
            }
        }
    };

    quote! {
        #vis fn new<'__ctor, __A: ::bstack_raii::BStackOwnedSliceAllocator>(
            allocator: &'__ctor __A,
            #(#params)*
        ) -> ::std::io::Result<#ret> {
            #(#preps)*
            let __on_disk = #on_disk {
                __bstack_header: ::bstack_raii::BlockHeader {
                    size: ::core::mem::size_of::<#on_disk>() as u64,
                    tag: <Self as ::bstack_raii::BStackCast>::eightcc(),
                },
                #injected
                #(#inits)*
            };
            let mut __slice = allocator.alloc(::core::mem::size_of::<#on_disk>() as u64)?;
            let __data = __slice.as_range();
            if let ::std::result::Result::Err(__e) =
                __slice.write_range(0, ::bstack_raii::bytemuck::bytes_of(&__on_disk))
            {
                let _ = allocator.dealloc(__slice);
                return ::std::result::Result::Err(__e);
            }
            #finish
        }
    }
}

/// Build an owned/strong teardown statement: resolve the child field's `u64`
/// offset into a typed `BStackRef<#inner_ty>` bound to `__child`, then run
/// `body`. A `nullable` field guards on a non-zero offset.
fn child_range_stmt(
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
fn weak_drop_stmt(fname: &Ident, inner_ty: &Type) -> TokenStream {
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

/// Parsed `#[bstack_block(...)]` arguments.
struct Attr {
    mode: Mode,
    /// Explicit data-block tag prefix (`tag = "..."`).
    tag: Option<String>,
    /// Explicit control-block tag prefix (`ctrl_tag = "..."`).
    ctrl_tag: Option<String>,
    /// Suppress the overlong-tag warning (`allow(overlong_tag)`).
    allow_overlong: bool,
    /// Suppress the reference-coercion warning (`allow(coerced_ref)`).
    allow_coerced_ref: bool,
}

/// Parse `rc`, `weak`, `tag = "..."`, `ctrl_tag = "..."`, and
/// `allow(overlong_tag | coerced_ref | deprecated)` in any order.
fn parse_attr(attr: TokenStream) -> syn::Result<Attr> {
    let (mut rc, mut weak) = (false, false);
    let mut tag = None;
    let mut ctrl_tag = None;
    let mut allow_overlong = false;
    let mut allow_coerced_ref = false;

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
    })
}

fn ident_of(path: &syn::Path) -> Option<String> {
    path.get_ident().map(|i| i.to_string())
}

/// Whether a struct attribute is `#[allow(.., deprecated, ..)]`.
fn is_allow_deprecated(attr: &syn::Attribute) -> bool {
    attr.path().is_ident("allow")
        && attr
            .parse_args_with(Punctuated::<Ident, Token![,]>::parse_terminated)
            .is_ok_and(|lints| lints.iter().any(|l| l == "deprecated"))
}

fn unknown_opt() -> &'static str {
    "expected `rc`, `weak`, `tag = \"...\"`, `ctrl_tag = \"...\"`, or `allow(...)`"
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
struct Tag {
    bytes: [u8; 8],
    truncated: bool,
}

fn fnv1a64(s: &str) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in s.bytes() {
        h ^= b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

/// Compose a tag from a hash and a readable prefix. Prefix bytes overwrite the
/// (high-bit-set) hash bytes from the front; > 8 prefix bytes are truncated.
fn build_tag(hash: u64, prefix: &[u8]) -> Tag {
    let mut bytes = hash.to_le_bytes();
    for b in bytes.iter_mut() {
        *b |= 0x80;
    }
    let truncated = prefix.len() > 8;
    let n = prefix.len().min(8);
    bytes[..n].copy_from_slice(&prefix[..n]);
    Tag { bytes, truncated }
}

fn is_ascii_vowel(b: u8) -> bool {
    matches!(
        b.to_ascii_uppercase(),
        b'A' | b'E' | b'I' | b'O' | b'U' | b'Y'
    )
}

/// Split a type name into words on camel-case boundaries and separators.
fn split_words(name: &str) -> Vec<String> {
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
fn auto_prefix(name: &str) -> Vec<u8> {
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
fn eightcc_expr(bytes: &[u8; 8]) -> TokenStream {
    let bytes = bytes.iter();
    quote!(::bstack_raii::EightCC::new([#(#bytes),*]))
}

/// Classify a field by its ownership annotation.
fn classify(field: &syn::Field) -> syn::Result<Kind> {
    let mut found: Option<Kind> = None;
    for attr in &field.attrs {
        let Some(id) = attr.path().get_ident() else {
            continue;
        };
        let kind = match id.to_string().as_str() {
            "bstack_owned" => Kind::Owned,
            "bstack_strong" => Kind::Strong,
            "bstack_weak" => Kind::Weak,
            "bstack_ref" => Kind::Ref,
            _ => continue,
        };
        if found.is_some() {
            return Err(Error::new_spanned(
                attr,
                "a field may carry at most one bstack ownership annotation",
            ));
        }
        found = Some(kind);
    }
    Ok(found.unwrap_or(Kind::Pod))
}
