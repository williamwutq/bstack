//! Implementation of the `#[bstack_class]` attribute macro.
//!
//! `#[bstack_class]` is a **drop-in replacement** for `#[bstack_block]` (structs) and
//! `#[bstack_enum]` (enums) that emits everything they do **plus** a persisted RTTI
//! schema:
//!
//! * it delegates the whole block-machinery codegen to [`crate::block::expand`] /
//!   [`crate::enum_::expand_enum`] (identical handle / `OnDisk` / accessors /
//!   teardown / clone / `new`); then
//! * it emits a `fn() -> RttiType` **descriptor builder** for the type and registers
//!   it into `bstack_raii::rtti::RTTI_TYPES` via `linkme`, so `rtti::sync` can persist
//!   the type's self-describing schema.
//!
//! The descriptor is assembled **at runtime** (once, at sync time) from the generated
//! `XOnDisk` type — `size_of` / `offset_of!` give the on-disk size and each field's
//! (block-relative) offset, and `<T as BStackCast>::eightcc()` gives a referenced
//! block's tag — so the emitter never re-derives layout arithmetic the block macro
//! already owns. It only translates each field / variant **shape** into an
//! `rtti::Shape` node.
//!
//! ## Class variables (`#[bstack_static]`)
//!
//! A `#[bstack_static(EXPR)]` struct field is a **class variable**: stored once inline
//! in the type's schema record (a `CLASS` shape carrying the value bytes), never as a
//! per-instance field. It is stripped from the generated block, and its `EXPR` is
//! encoded (via `bytemuck`) at descriptor-build time. `#[bstack_mut]` on it marks it
//! mutable (a fixed-size, in-place-updatable slot); otherwise it is a constant. Class
//! variables must be POD (Sized) for now.
//!
//! ## Not yet covered
//!
//! Generics, `Foreign`, tuple struct-fields, and complex enum variant payloads (vec /
//! array / foreign / tuple) are clear compile errors rather than a wrong descriptor —
//! they belong to a later RTTI phase.

use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{Error, Fields, Ident, ItemEnum, ItemStruct, Type};

use crate::util::*;

/// `#[bstack_class] struct` — emit the `#[bstack_block]` machinery plus an RTTI
/// descriptor builder and its `linkme` registration.
pub fn expand_struct(attr: TokenStream, input: ItemStruct) -> syn::Result<TokenStream> {
    // RTTI needs a single, concrete `XOnDisk` to `size_of` / `offset_of!`.
    if !input.generics.params.is_empty() {
        return Err(Error::new_spanned(
            &input.generics,
            "[BSTACK0408] a generic #[bstack_class] is not supported: RTTI needs a single \
             concrete on-disk layout to describe. Use a concrete type, or #[bstack_block] \
             without RTTI.",
        ));
    }

    let parsed = parse_attr(attr.clone())?;
    let rc = parsed.mode != Mode::Plain;
    let weak = parsed.mode == Mode::RcWeak;

    let name = &input.ident;
    let on_disk = format_ident!("{}OnDisk", name);

    // One `RttiField` initializer per field, in declaration order. Instance fields
    // resolve their offset via `offset_of!(XOnDisk, f)`; `#[bstack_static]` fields
    // become a `CLASS` shape and are stripped from the generated block.
    let mut rtti_fields: Vec<TokenStream> = Vec::new();
    let mut static_pod_types: Vec<Type> = Vec::new();
    let mut has_static = false;

    match &input.fields {
        Fields::Named(named) => {
            for f in &named.named {
                let fname = f.ident.clone().expect("named field");
                let fname_str = fname.to_string();
                if let Some(expr) = bstack_static_expr(&f.attrs) {
                    has_static = true;
                    let expr = expr.map_err(|_| {
                        Error::new_spanned(
                            f,
                            format!(
                                "[BSTACK0010] field `{fname_str}`: `#[bstack_static]` needs an \
                                 initial value, e.g. `#[bstack_static(0u32)]`"
                            ),
                        )
                    })?;
                    if classify(f)? != Kind::Pod {
                        return Err(Error::new_spanned(
                            f,
                            format!(
                                "[BSTACK0010] field `{fname_str}`: a `#[bstack_static]` class \
                                 variable must be POD — it cannot also own a block \
                                 (`#[bstack_owned/strong/weak/ref]` / `#[embed]`)"
                            ),
                        ));
                    }
                    static_pod_types.push(f.ty.clone());
                    rtti_fields.push(class_field(
                        &fname_str,
                        &f.ty,
                        is_bstack_mut(&f.attrs),
                        &expr,
                    ));
                } else {
                    let kind = classify(f)?;
                    let shape = field_shape(&fname_str, f, &f.ty, kind)?;
                    rtti_fields.push(instance_field(&fname_str, &on_disk, &fname, shape));
                }
            }
        }
        Fields::Unnamed(unnamed) => {
            for (i, f) in unnamed.unnamed.iter().enumerate() {
                if is_bstack_static(&f.attrs) {
                    return Err(Error::new_spanned(
                        f,
                        "[BSTACK0010] `#[bstack_static]` class variables are only supported on \
                         named struct fields",
                    ));
                }
                let fname = format_ident!("field{i}");
                let kind = classify(f)?;
                let shape = field_shape(&fname.to_string(), f, &f.ty, kind)?;
                rtti_fields.push(instance_field(&i.to_string(), &on_disk, &fname, shape));
            }
        }
        Fields::Unit => {}
    }

    // The block machinery. If there are class variables, strip them from the struct
    // the block macro sees (they are not per-instance fields).
    let instance_input = if has_static {
        let mut cloned = input.clone();
        if let Fields::Named(named) = &mut cloned.fields {
            named.named = named
                .named
                .iter()
                .filter(|f| !is_bstack_static(&f.attrs))
                .cloned()
                .collect();
        }
        cloned
    } else {
        input.clone()
    };
    let block_ts = crate::block::expand(attr, instance_input)?;

    let name_str = name.to_string();
    let rtti_type = quote! {
        ::bstack_raii::rtti::RttiType {
            tag: <#name as ::bstack_raii::BStackCast>::eightcc(),
            name: ::std::string::String::from(#name_str),
            rc: #rc,
            weak: #weak,
            ondisk_size: ::core::mem::size_of::<#on_disk>() as u64,
            body: ::bstack_raii::rtti::RttiBody::Struct(::std::vec![
                #(#rtti_fields),*
            ]),
        }
    };
    let reg_ts = registration(name, rtti_type);
    let pod_assert = pod_assertion(&static_pod_types);

    Ok(quote! {
        #block_ts
        #pod_assert
        #reg_ts
    })
}

/// `#[bstack_class] enum` — emit the `#[bstack_enum]` machinery plus an RTTI
/// descriptor (discriminant table + per-variant shapes) and its registration.
pub fn expand_enum(attr: TokenStream, input: ItemEnum) -> syn::Result<TokenStream> {
    if !input.generics.params.is_empty() {
        return Err(Error::new_spanned(
            &input.generics,
            "[BSTACK0408] a generic #[bstack_class] is not supported: RTTI needs a single \
             concrete on-disk layout to describe. Use a concrete type, or #[bstack_enum] \
             without RTTI.",
        ));
    }

    let parsed = parse_attr(attr.clone())?;
    let rc = parsed.mode != Mode::Plain;
    let weak = parsed.mode == Mode::RcWeak;

    // The resolved discriminant width + per-variant values (declaration order).
    let disc = crate::layout::discriminants(&input.variants, &parsed.repr)?;
    let disc_ty = &disc.ty;

    let name = &input.ident;
    let on_disk = format_ident!("{}OnDisk", name);

    let mut variant_toks: Vec<TokenStream> = Vec::with_capacity(input.variants.len());
    for (variant, value) in input.variants.iter().zip(disc.values.iter()) {
        variant_toks.push(enum_variant(variant, *value)?);
    }

    let block_ts = crate::enum_::expand_enum(attr, input.clone())?;

    let name_str = name.to_string();
    let rtti_type = quote! {
        ::bstack_raii::rtti::RttiType {
            tag: <#name as ::bstack_raii::BStackCast>::eightcc(),
            name: ::std::string::String::from(#name_str),
            rc: #rc,
            weak: #weak,
            ondisk_size: ::core::mem::size_of::<#on_disk>() as u64,
            body: ::bstack_raii::rtti::RttiBody::Enum(::bstack_raii::rtti::RttiEnum {
                disc_width: ::core::mem::size_of::<#disc_ty>() as u8,
                disc_off: ::core::mem::offset_of!(#on_disk, __bstack_disc) as u16,
                payload_off: ::core::mem::offset_of!(#on_disk, __bstack_payload) as u16,
                variants: ::std::vec![
                    #(#variant_toks),*
                ],
            }),
        }
    };
    let reg_ts = registration(name, rtti_type);

    Ok(quote! {
        #block_ts
        #reg_ts
    })
}

/// One enum variant's `RttiVariant` initializer: its name, discriminant, and payload
/// fields (offsets relative to the payload area).
fn enum_variant(variant: &syn::Variant, value: i128) -> syn::Result<TokenStream> {
    let vname = variant.ident.to_string();
    let disc = value as i64;
    let kind = classify_attrs(&variant.attrs)?;

    let fields: Vec<TokenStream> = match &variant.fields {
        Fields::Unit => Vec::new(),
        _ if kind == Kind::Pod => {
            // POD aggregate: fields packed at cumulative `size_of` byte offsets,
            // declaration order, unaligned (see `emit::pod_aggregate_variant`).
            let mut preceding: Vec<&Type> = Vec::new();
            let mut out = Vec::with_capacity(variant.fields.len());
            for (i, f) in variant.fields.iter().enumerate() {
                let fname = f
                    .ident
                    .as_ref()
                    .map(|id| id.to_string())
                    .unwrap_or_else(|| i.to_string());
                let ty = &f.ty;
                let offset = if preceding.is_empty() {
                    quote!(0u32)
                } else {
                    let sizes = preceding
                        .iter()
                        .map(|t| quote!(::core::mem::size_of::<#t>()));
                    quote!((#(#sizes)+*) as u32)
                };
                out.push(quote! {
                    ::bstack_raii::rtti::RttiField {
                        name: ::std::string::String::from(#fname),
                        offset: #offset,
                        shape: ::bstack_raii::rtti::Shape::Pod {
                            width: ::core::mem::size_of::<#ty>() as u32,
                        },
                    }
                });
                preceding.push(ty);
            }
            out
        }
        _ => {
            // A reference-kind variant is a single-field tuple stored as one `u64`
            // offset (or an embedded child) at payload offset 0.
            let f = variant
                .fields
                .iter()
                .next()
                .expect("an annotated variant has exactly one field");
            let ty = &f.ty;
            let (inner, nullable) = match option_inner(ty) {
                Some(i) => (i, true),
                None => (ty, false),
            };
            if vec_info(inner).is_some()
                || matches!(inner, Type::Array(_) | Type::Tuple(_))
                || field_foreign_target(inner).is_some()
            {
                return Err(Error::new_spanned(
                    ty,
                    format!(
                        "[BSTACK0206] variant `{vname}`: a vec / array / foreign / tuple payload \
                         is not yet supported by #[bstack_class]"
                    ),
                ));
            }
            let variant_ident = kind_variant(kind).expect("reference kind");
            let base = quote!(::bstack_raii::rtti::Shape::#variant_ident(
                <#inner as ::bstack_raii::BStackCast>::eightcc()
            ));
            let shape = if nullable {
                quote!(::bstack_raii::rtti::Shape::Option(::std::boxed::Box::new(#base)))
            } else {
                base
            };
            vec![quote! {
                ::bstack_raii::rtti::RttiField {
                    name: ::std::string::String::from("0"),
                    offset: 0u32,
                    shape: #shape,
                }
            }]
        }
    };

    Ok(quote! {
        ::bstack_raii::rtti::RttiVariant {
            name: ::std::string::String::from(#vname),
            disc_value: #disc,
            fields: ::std::vec![
                #(#fields),*
            ],
        }
    })
}

/// The `Shape::<Variant>` ident for a reference ownership kind, `None` for POD.
fn kind_variant(kind: Kind) -> Option<TokenStream> {
    Some(match kind {
        Kind::Owned => quote!(Owned),
        Kind::Strong => quote!(Strong),
        Kind::Weak => quote!(Weak),
        Kind::Ref => quote!(Ref),
        Kind::Embed => quote!(Embed),
        Kind::Pod => return None,
    })
}

/// A per-instance field's `RttiField`: name, `offset_of!`-resolved offset, and shape.
fn instance_field(name: &str, on_disk: &Ident, fname: &Ident, shape: TokenStream) -> TokenStream {
    quote! {
        ::bstack_raii::rtti::RttiField {
            name: ::std::string::String::from(#name),
            offset: ::core::mem::offset_of!(#on_disk, #fname) as u32,
            shape: #shape,
        }
    }
}

/// A `#[bstack_static]` class variable's `RttiField`: a `CLASS` shape carrying its
/// (bytemuck-encoded) value; `offset` is unused (class vars are not per-instance).
fn class_field(name: &str, ty: &Type, mutable: bool, expr: &syn::Expr) -> TokenStream {
    quote! {
        ::bstack_raii::rtti::RttiField {
            name: ::std::string::String::from(#name),
            offset: 0u32,
            shape: ::bstack_raii::rtti::Shape::Class {
                mutable: #mutable,
                inner: ::std::boxed::Box::new(::bstack_raii::rtti::Shape::Pod {
                    width: ::core::mem::size_of::<#ty>() as u32,
                }),
                value: {
                    let __v: #ty = #expr;
                    ::bstack_raii::bytemuck::bytes_of(&__v).to_vec()
                },
            },
        }
    }
}

/// A `T: Pod` assertion for every `#[bstack_static]` type, so a non-POD class variable
/// gets a directed error rather than a raw `bytes_of` bound failure.
fn pod_assertion(types: &[Type]) -> TokenStream {
    if types.is_empty() {
        return quote!();
    }
    quote! {
        const _: fn() = || {
            fn __assert_static_pod<__T: ::bstack_raii::Pod>() {}
            #( __assert_static_pod::<#types>(); )*
        };
    }
}

/// The descriptor builder `fn` + its link-time registration. `#[linkme(crate = …)]`
/// points linkme at our re-export so a downstream crate needs no direct linkme
/// dependency (linkme otherwise hard-codes `::linkme`).
fn registration(name: &Ident, rtti_type: TokenStream) -> TokenStream {
    let build_fn = format_ident!("__bstack_rtti_build_{}", name);
    let reg_static = format_ident!("__BSTACK_RTTI_REG_{}", name);
    quote! {
        #[doc(hidden)]
        #[allow(non_snake_case)]
        fn #build_fn() -> ::bstack_raii::rtti::RttiType {
            #rtti_type
        }

        #[doc(hidden)]
        #[allow(non_upper_case_globals)]
        #[::bstack_raii::rtti::distributed_slice_reexport(::bstack_raii::rtti::RTTI_TYPES)]
        #[linkme(crate = ::bstack_raii::rtti::linkme)]
        static #reg_static: ::bstack_raii::rtti::RttiRegistration =
            ::bstack_raii::rtti::RttiRegistration { build: #build_fn };
    }
}

/// Translate one field's type + ownership kind into a token stream that builds its
/// `rtti::Shape` at runtime. Recurses through `Option` / `[T; N]` / `Vec` / `String`
/// down to a leaf (`Pod` or a block reference); the ownership `kind` applies to the
/// leaf.
fn field_shape(fname: &str, field: &syn::Field, ty: &Type, kind: Kind) -> syn::Result<TokenStream> {
    // `Foreign` is a later (cross-file) phase; its schema shape needs the target's
    // ownership kind, which the current `Shape::Foreign` does not yet carry.
    if field_foreign_target(ty).is_some() {
        return Err(Error::new_spanned(
            ty,
            format!(
                "[BSTACK0309] field `{fname}`: `Foreign` is not yet supported by #[bstack_class] \
                 (cross-file RTTI is a later phase)"
            ),
        ));
    }

    // `Option<Inner>` — nullable leaf / child.
    if let Some(inner) = option_inner(ty) {
        let inner_shape = leaf_or_container_shape(fname, ty, inner, kind)?;
        return Ok(
            quote!(::bstack_raii::rtti::Shape::Option(::std::boxed::Box::new(#inner_shape))),
        );
    }

    let _ = field;
    leaf_or_container_shape(fname, ty, ty, kind)
}

/// The non-`Option` part of the shape: a `Vec` / `String` region, an `[T; N]` array,
/// or a leaf. `orig` is the whole field type (for error spans); `ty` is the current
/// (possibly `Option`-peeled) type.
fn leaf_or_container_shape(
    fname: &str,
    orig: &Type,
    ty: &Type,
    kind: Kind,
) -> syn::Result<TokenStream> {
    // `Vec<T>` / `String` — a dynamically-sized region; the element carries the
    // field's ownership kind (POD bytes, or block offsets).
    if let Some(vi) = vec_info(ty) {
        if vi.is_string {
            return Ok(quote!(::bstack_raii::rtti::Shape::Vec(
                ::std::boxed::Box::new(::bstack_raii::rtti::Shape::Pod { width: 1 })
            )));
        }
        let elem = vec_inner(ty).expect("vec_info matched");
        let elem_shape = leaf_shape(fname, orig, elem, kind)?;
        return Ok(quote!(::bstack_raii::rtti::Shape::Vec(::std::boxed::Box::new(#elem_shape))));
    }

    // `[T; N]` (possibly nested `[[T; N]; M]`, possibly `[Option<T>; N]`).
    if matches!(ty, Type::Array(_)) {
        let (dims, leaf, nullable) = array_shape(ty)?;
        let mut acc = leaf_shape(fname, orig, leaf, kind)?;
        if nullable {
            acc = quote!(::bstack_raii::rtti::Shape::Option(::std::boxed::Box::new(#acc)));
        }
        for dim in dims.iter().rev() {
            acc = quote!(::bstack_raii::rtti::Shape::Array {
                n: (#dim) as u32,
                inner: ::std::boxed::Box::new(#acc),
            });
        }
        return Ok(acc);
    }

    leaf_shape(fname, orig, ty, kind)
}

/// A single leaf: a `Pod` value (its byte width) or a block reference (its target's
/// tag), selected by the ownership `kind`.
fn leaf_shape(fname: &str, orig: &Type, ty: &Type, kind: Kind) -> syn::Result<TokenStream> {
    if matches!(ty, Type::Tuple(_)) {
        return Err(Error::new_spanned(
            orig,
            format!(
                "[BSTACK0112] field `{fname}`: a tuple field is not yet supported by \
                 #[bstack_class]; wrap it in a named #[bstack_class] struct"
            ),
        ));
    }
    Ok(match kind {
        Kind::Pod => quote!(::bstack_raii::rtti::Shape::Pod {
            width: ::core::mem::size_of::<#ty>() as u32,
        }),
        other => {
            let variant = kind_variant(other).expect("non-POD kind");
            quote!(::bstack_raii::rtti::Shape::#variant(
                <#ty as ::bstack_raii::BStackCast>::eightcc()
            ))
        }
    })
}
