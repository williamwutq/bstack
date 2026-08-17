//! Implementation of the `#[bstack_class]` attribute macro.
//!
//! `#[bstack_class]` is a **drop-in replacement** for `#[bstack_block]` (structs)
//! that emits everything `#[bstack_block]` does **plus** a persisted RTTI schema:
//!
//! * it delegates the whole block-machinery codegen to [`crate::block::expand`]
//!   (identical handle / `OnDisk` / accessors / teardown / clone / `new`); then
//! * it emits a `fn() -> RttiType` **descriptor builder** for the type and
//!   registers it into `bstack_raii::rtti::RTTI_TYPES` via `linkme`, so
//!   `rtti::sync` can persist the type's self-describing schema to disk.
//!
//! The descriptor is assembled **at runtime** (once, at sync time) from the
//! generated `XOnDisk` type — `size_of` / `offset_of!` give the on-disk size and
//! each field's offset, and `<T as BStackCast>::eightcc()` gives a referenced
//! block's tag — so the emitter never has to re-derive layout arithmetic that the
//! block macro already owns. It only has to translate each field's **shape**
//! (`Pod` / `Owned` / `Strong` / `Weak` / `Ref` / `Embed`, wrapped in `Option` /
//! `[T; N]` / `Vec` / `String`) into an `rtti::Shape` node.
//!
//! Not yet covered (a clear compile error, not a wrong descriptor): enums,
//! generics, `Foreign`, tuple fields, and `#[bstack_static]` class variables.
//! Those either belong to a later RTTI phase or have an unresolved value story;
//! erroring keeps `#[bstack_class]` from ever persisting a schema it can't fully
//! describe.

use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{Error, Fields, Ident, ItemEnum, ItemStruct, Type};

use crate::util::*;

/// `#[bstack_class] enum` — not yet supported. Enum RTTI (discriminant / variant
/// dispatch) is a later phase; for now the enum block machinery is available under
/// `#[bstack_enum]`.
pub fn expand_enum(_attr: TokenStream, input: ItemEnum) -> syn::Result<TokenStream> {
    Err(Error::new_spanned(
        &input.ident,
        "[BSTACK0009] #[bstack_class] on an enum is not yet supported (enum RTTI is a \
         later phase). Use #[bstack_enum] for the block machinery in the meantime.",
    ))
}

/// `#[bstack_class] struct` — emit the `#[bstack_block]` machinery plus an RTTI
/// descriptor builder and its `linkme` registration.
pub fn expand_struct(attr: TokenStream, input: ItemStruct) -> syn::Result<TokenStream> {
    // RTTI needs a single, concrete `XOnDisk` to `size_of` / `offset_of!`; a
    // generic block has no such type. (Foreign / cross-file is a later phase too.)
    if !input.generics.params.is_empty() {
        return Err(Error::new_spanned(
            &input.generics,
            "[BSTACK0408] a generic #[bstack_class] is not supported: RTTI needs a single \
             concrete on-disk layout to describe. Use a concrete type, or #[bstack_block] \
             without RTTI.",
        ));
    }

    let parsed = parse_attr(attr.clone())?;
    let mode = parsed.mode;
    let rc = mode != Mode::Plain;
    let weak = mode == Mode::RcWeak;

    let name = &input.ident;
    let on_disk = format_ident!("{}OnDisk", name);

    // Normalize fields exactly as `block::expand` does, so field names / synthetic
    // tuple names (`field0`, …) match the generated `XOnDisk` field identifiers
    // that `offset_of!` will resolve against.
    let field_list: Vec<(Ident, &syn::Field)> = match &input.fields {
        Fields::Named(named) => named
            .named
            .iter()
            .map(|f| (f.ident.clone().expect("named field"), f))
            .collect(),
        Fields::Unnamed(unnamed) => unnamed
            .unnamed
            .iter()
            .enumerate()
            .map(|(i, f)| (format_ident!("field{i}"), f))
            .collect(),
        Fields::Unit => Vec::new(),
    };

    // Build one `RttiField { name, offset, shape }` initializer per field.
    let mut rtti_fields: Vec<TokenStream> = Vec::with_capacity(field_list.len());
    for (fname, field) in &field_list {
        let kind = classify(field)?;
        let shape = field_shape(&fname.to_string(), field, &field.ty, kind)?;
        let fname_str = fname.to_string();
        rtti_fields.push(quote! {
            ::bstack_raii::rtti::RttiField {
                name: ::std::string::String::from(#fname_str),
                offset: ::core::mem::offset_of!(#on_disk, #fname) as u32,
                shape: #shape,
            }
        });
    }

    // The block machinery, verbatim from `#[bstack_block]`.
    let block_ts = crate::block::expand(attr, input.clone())?;

    let name_str = name.to_string();
    let build_fn = format_ident!("__bstack_rtti_build_{}", name);
    let reg_static = format_ident!("__BSTACK_RTTI_REG_{}", name);

    // The descriptor builder + its link-time registration. `#[linkme(crate = …)]`
    // points linkme at our re-export so a downstream crate needs no direct linkme
    // dependency (linkme otherwise hard-codes `::linkme`).
    let rtti_ts = quote! {
        #[doc(hidden)]
        #[allow(non_snake_case)]
        fn #build_fn() -> ::bstack_raii::rtti::RttiType {
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
        }

        #[doc(hidden)]
        #[allow(non_upper_case_globals)]
        #[::bstack_raii::rtti::distributed_slice_reexport(::bstack_raii::rtti::RTTI_TYPES)]
        #[linkme(crate = ::bstack_raii::rtti::linkme)]
        static #reg_static: ::bstack_raii::rtti::RttiRegistration =
            ::bstack_raii::rtti::RttiRegistration { build: #build_fn };
    };

    Ok(quote! {
        #block_ts
        #rtti_ts
    })
}

/// Translate one field's type + ownership kind into a token stream that builds its
/// `rtti::Shape` at runtime. Recurses through `Option` / `[T; N]` / `Vec` /
/// `String` down to a leaf (`Pod` or a block reference); the ownership `kind`
/// applies to the leaf.
fn field_shape(fname: &str, field: &syn::Field, ty: &Type, kind: Kind) -> syn::Result<TokenStream> {
    // `#[bstack_static]` class variables have an unresolved value story — reject
    // rather than silently drop the field from the schema.
    if is_bstack_static(&field.attrs) {
        return Err(Error::new_spanned(
            ty,
            format!(
                "[BSTACK0010] field `{fname}`: `#[bstack_static]` class variables are not yet \
                 supported by #[bstack_class]"
            ),
        ));
    }
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
        return Ok(quote!(::bstack_raii::rtti::Shape::Option(::std::boxed::Box::new(#inner_shape))));
    }

    leaf_or_container_shape(fname, ty, ty, kind)
}

/// The non-`Option` part of the shape: a `Vec` / `String` region, an `[T; N]`
/// array, or a leaf. `orig` is the whole field type (for error spans); `ty` is the
/// current (possibly `Option`-peeled) type.
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
            // `String` is always POD bytes.
            return Ok(quote!(::bstack_raii::rtti::Shape::Vec(::std::boxed::Box::new(
                ::bstack_raii::rtti::Shape::Pod { width: 1 }
            ))));
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
        // Nest innermost-out so the outermost dimension is the outer `Array`.
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
    // A tuple leaf (POD aggregate) has no single-tag shape node yet.
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
        Kind::Owned => reference_shape(quote!(Owned), ty),
        Kind::Strong => reference_shape(quote!(Strong), ty),
        Kind::Weak => reference_shape(quote!(Weak), ty),
        Kind::Ref => reference_shape(quote!(Ref), ty),
        Kind::Embed => reference_shape(quote!(Embed), ty),
    })
}

/// A reference-kind leaf: `Shape::<Variant>(<Target as BStackCast>::eightcc())`.
fn reference_shape(variant: TokenStream, ty: &Type) -> TokenStream {
    quote!(::bstack_raii::rtti::Shape::#variant(
        <#ty as ::bstack_raii::BStackCast>::eightcc()
    ))
}
