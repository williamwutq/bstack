//! Implementation of the `#[bstack_enum]` attribute macro (orchestrator). The
//! shared classification / analysis / emit machinery lives in [`crate::common`].

use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{Error, Fields, Ident, Type};

use crate::emit::*;
use crate::layout;
use crate::util::*;

/// Implementation of the `#[bstack_enum]` attribute macro.
pub fn expand_enum(attr: TokenStream, input: syn::ItemEnum) -> syn::Result<TokenStream> {
    let attr = parse_attr(attr)?;
    let mode = attr.mode;

    // Generic enums (layout-preserving): a type parameter may appear only in a
    // *reference* variant (`#[bstack_owned/strong/weak/ref] V(T)`, and array / vec
    // forms) — each a bare `u64` offset (or `VecDesc`) in the payload, keeping the
    // fixed payload size independent of the parameter. A POD or `#[embed]` variant
    // stores the parameter inline, so its payload width would depend on it —
    // rejected. Const / lifetime parameters and non-plain modes are not supported.
    let type_params: Vec<&Ident> = input.generics.type_params().map(|tp| &tp.ident).collect();
    if !input.generics.params.is_empty() {
        for p in &input.generics.params {
            if !matches!(p, syn::GenericParam::Type(_)) {
                return Err(Error::new_spanned(
                    p,
                    "a generic #[bstack_enum] currently supports only type parameters (no \
                     lifetime or const generics)",
                ));
            }
        }
        if mode != Mode::Plain {
            return Err(Error::new_spanned(
                &input.generics,
                "a generic #[bstack_enum] currently supports plain mode only (not `rc` / \
                 `rc, weak`)",
            ));
        }
    }
    #[derive(Default)]
    struct EUsage {
        strong: bool,
        weak: bool,
        /// The param is the target of a `#[bstack_owned] V(Foreign<T>)` variant (an
        /// owned foreign deep-clone runs `try_clone_in`, needing `TryCloneIn`).
        foreign_owned: bool,
        /// The param is the target of *any* `Foreign<T>` variant; `Foreign<'a, T>`
        /// requires `T: 'static`.
        foreign: bool,
    }
    let mut eusage: Vec<(Ident, EUsage)> = type_params
        .iter()
        .map(|p| ((*p).clone(), EUsage::default()))
        .collect();
    for variant in &input.variants {
        let kind = classify_attrs(&variant.attrs)?;
        // A `#[bstack_mut]` mutator on an enum is *whole-value* (the payload's
        // meaning depends on the discriminant, so there is no per-variant field to
        // set). Put `#[bstack_mut]` on the enum itself — it is a no-op / error on a
        // variant.
        if is_bstack_mut(&variant.attrs) {
            return Err(Error::new_spanned(
                variant,
                "#[bstack_mut] on a `#[bstack_enum]` goes on the enum itself (a whole-value \
                 `set` / `replace`), not on a variant — a variant has no separately \
                 mutable field",
            ));
        }
        for f in &variant.fields {
            if !type_mentions_any(&f.ty, &type_params) {
                continue;
            }
            let ftargets = foreign_targets_in(&f.ty);
            if ftargets.is_empty() && (kind == Kind::Pod || kind == Kind::Embed) {
                return Err(Error::new_spanned(
                    &f.ty,
                    "a generic type parameter in a `#[bstack_enum]` variant must be a reference \
                     (`#[bstack_owned]` / `#[bstack_strong]` / `#[bstack_weak]` / `#[bstack_ref]`), \
                     not stored inline — a POD or `#[embed]` variant's payload width would depend \
                     on the parameter",
                ));
            }
            for (p, u) in eusage.iter_mut() {
                if !type_mentions_any(&f.ty, &[&*p]) {
                    continue;
                }
                let is_ftarget = ftargets.iter().any(|t| type_mentions_any(t, &[&*p]));
                if !ftargets.is_empty() && !is_ftarget {
                    return Err(Error::new_spanned(
                        &f.ty,
                        "a generic type parameter in a non-`Foreign` position of a `Foreign` \
                         variant is not supported; use concrete types for the non-foreign parts",
                    ));
                }
                u.strong |= kind == Kind::Strong;
                u.weak |= kind == Kind::Weak;
                u.foreign_owned |= is_ftarget && kind == Kind::Owned;
                u.foreign |= is_ftarget;
            }
        }
    }
    let mut aug_generics = input.generics.clone();
    for tp in aug_generics.type_params_mut() {
        tp.bounds
            .push(syn::parse_quote!(::bstack_raii::BStackBlock));
        if let Some((_, u)) = eusage.iter().find(|(p, _)| *p == tp.ident) {
            if u.strong {
                tp.bounds
                    .push(syn::parse_quote!(::bstack_raii::BStackShared));
            }
            if u.weak {
                tp.bounds
                    .push(syn::parse_quote!(::bstack_raii::BStackWeakable));
            }
            if u.foreign_owned {
                tp.bounds.push(syn::parse_quote!(::bstack_raii::TryCloneIn));
            }
            if u.foreign {
                tp.bounds.push(syn::parse_quote!('static));
            }
        }
    }
    let (enum_impl_g, enum_ty_g, enum_where) = aug_generics.split_for_impl();
    let (enum_decl_g, enum_decl_ty_g, enum_decl_where) = input.generics.split_for_impl();
    let (enum_phantom_field, enum_phantom_ctor): (TokenStream, TokenStream) =
        if type_params.is_empty() {
            (quote!(), quote!())
        } else {
            (
                quote!(, ::core::marker::PhantomData<fn() -> (#(#type_params,)*)>),
                quote!(, ::core::marker::PhantomData),
            )
        };

    // The on-disk discriminant width + per-variant literal patterns.
    let layout::Discriminants {
        ty: disc_ty,
        pats: disc_pats,
    } = layout::discriminants(&input.variants, &attr.repr)?;

    let name = &input.ident;
    let vis = &input.vis;
    let on_disk = format_ident!("{}OnDisk", name);
    let control = format_ident!("{}OnDiskRef", name);
    let payload_const = format_ident!("__bstack_payload_{}", name);
    // `EData` is the in-memory owned form of the enum's payload — the same type is
    // used to *construct* (`E::new`) and to receive a *destructured* variant
    // (`bstack_move!`), since both hold owned handles (they are duals).
    let data = format_ident!("{}Data", name);
    let view = format_ident!("{}View", name);

    let mut data_variants = Vec::new();
    let mut view_variants = Vec::new();
    let mut new_arms = Vec::new();
    // Whether any variant is `#[embed]` (its `new` folds the child in post-write).
    let mut enum_has_embed = false;
    let mut read_arms = Vec::new();
    let mut move_arms = Vec::new();
    let mut drop_arms = Vec::new();
    // `TryCloneIn` per-variant payload fix-ups (mirror of `drop_arms`). Variants
    // that need no fix-up (unit / POD aggregate / ref) emit none and fall to the
    // catch-all; the whole payload is byte-copied regardless.
    let mut clone_arms = Vec::new();
    let mut payload_sizes = Vec::new();
    let mut pod_types: Vec<Type> = Vec::new();
    // `#[embed]`ded variant children — asserted `BStackEmbeddable` (plain, self-contained)
    // so an `(rc)` / `(rc, weak)` child cannot be inlined and strand its control block.
    // Enum embed is always concrete (a generic embed variant is rejected in `EUsage`).
    let mut embed_types: Vec<TokenStream> = Vec::new();
    let mut needs_payload = false;
    // A strong/weak variant makes `EData` generic over `<'e, A>`; a weak variant
    // also makes `EView` generic (its read upgrades to a `BStackRc`).
    let mut has_shared = false;
    let mut has_weak = false;
    // A `Foreign` variant (scalar / vec / array / tuple) makes both `EData` and `EView`
    // carry the `'__e` lifetime so a `SELF` pointer is bound to the read / move borrow.
    let mut has_foreign = false;

    for (i, variant) in input.variants.iter().enumerate() {
        let disc = &disc_pats[i];
        let vname = &variant.ident;
        let kind = classify_attrs(&variant.attrs)?;

        // The child block's range recovered from a stored offset (owned / ref).
        let child_from_off = |ty: &Type| {
            quote! {
                <#ty as ::bstack_raii::BStackBlock>::from_range(::bstack_raii::BStackRange::new(
                    ::bstack_raii::get_u64(&__pl),
                    ::core::mem::size_of::<<#ty as ::bstack_raii::BStackBlock>::OnDisk>() as u64,
                ))
            }
        };
        // A `BStackRef<T>` over the child block, recovered from a stored offset.
        let child_ref = |ty: &Type| {
            quote! {
                ::bstack_raii::BStackRef::<#ty>::from_range(::bstack_raii::BStackRange::new(
                    ::bstack_raii::get_u64(&__pl),
                    ::core::mem::size_of::<<#ty as ::bstack_raii::BStackBlock>::OnDisk>() as u64,
                ))
            }
        };

        match &variant.fields {
            // Annotated single-field tuple `#[..] V(T)`: an owned / strong / weak /
            // ref child stored as a `u64` offset. (Unit, un-annotated single-POD,
            // multi-field tuple, and struct variants are POD aggregates, below.)
            // Annotated single-field `#[..] V(T)`, OR a POD `V(Vec<T>)` / `V(String)`
            // (which needs the vec machinery, not the byte-packed POD aggregate).
            Fields::Unnamed(f)
                if f.unnamed.len() == 1
                    && (kind != Kind::Pod
                        || vec_info(&f.unnamed.first().unwrap().ty).is_some()
                        || foreign_inner(&f.unnamed.first().unwrap().ty).is_some()) =>
            {
                needs_payload = true;
                let ty = &f.unnamed.first().unwrap().ty;

                // Annotated **vector** variant `#[..] V(Vec<T>)` / `V(Vec<[T; N]>)`:
                // a `VecDesc` (16 bytes) in the payload naming a data block — the
                // per-variant mirror of a `#[bstack_owned/strong/weak/ref] Vec<..>`
                // struct field. A `Vec<[T; N]>` stores its offsets FLAT (like the
                // struct case), reshaped to `Vec<[[T;..];..]>` on read.
                if vec_info(ty).is_some() {
                    check_container_nesting(ty)?;
                    if kind == Kind::Embed {
                        return Err(Error::new_spanned(
                            ty,
                            "cannot #[embed] a `Vec`; embed a `#[bstack_block]` type",
                        ));
                    }
                    needs_payload = true;
                    payload_sizes.push(quote!(::core::mem::size_of::<::bstack_raii::VecDesc>()));
                    let read_desc = quote!(::bstack_raii::bytemuck::pod_read_unaligned::<
                        ::bstack_raii::VecDesc,
                    >(&__pl[..16]));

                    // Annotated **foreign vector** variant `#[..] V(Vec<Foreign<T>>)`
                    // (+ `Vec<Option<Foreign<T>>>`): a `VecDesc` naming a `ForeignPtr`
                    // data block, the per-variant mirror of a `Vec<Foreign>` field.
                    if let Some(velem) = vec_inner(ty)
                        && let Some(ftarget) = foreign_inner(option_inner(velem).unwrap_or(velem))
                    {
                        match kind {
                            Kind::Owned | Kind::Strong | Kind::Weak | Kind::Ref => {}
                            Kind::Pod => {
                                return Err(Error::new_spanned(
                                    ty,
                                    "a `Vec<Foreign<T>>` enum variant needs an ownership \
                                     annotation (`#[bstack_owned/strong/weak/ref]`)",
                                ));
                            }
                            Kind::Embed => {
                                return Err(Error::new_spanned(
                                    ty,
                                    "`Foreign` is a pointer and cannot be `#[embed]`ed",
                                ));
                            }
                        }
                        reject_bad_foreign_target(ftarget, ty, "a `Foreign` vec variant")?;
                        has_foreign = true;
                        let elem_nullable = option_inner(velem).is_some();
                        let store =
                            quote!(::bstack_raii::BStackVec::<::bstack_raii::ForeignRepr, __A>);
                        // Variant type uses the enum's `'__e`; the move local uses `'__mv`
                        // (the move borrow), since `'__e` is not in scope in `bstack_move`.
                        let fty = if elem_nullable {
                            quote!(::core::option::Option<::bstack_raii::Foreign<'__e, #ftarget>>)
                        } else {
                            quote!(::bstack_raii::Foreign<'__e, #ftarget>)
                        };
                        let fty_mv = if elem_nullable {
                            quote!(::core::option::Option<::bstack_raii::Foreign<'__mv, #ftarget>>)
                        } else {
                            quote!(::bstack_raii::Foreign<'__mv, #ftarget>)
                        };
                        // SAFETY: each repr was stored into this file; the returned
                        // `Foreign`s are lifetime-bound by the read / move signature.
                        let from_ptr = if elem_nullable {
                            quote!(|__p: ::bstack_raii::ForeignRepr| if __p.offset() == 0 {
                                ::core::option::Option::None
                            } else {
                                ::core::option::Option::Some(
                                    unsafe { ::bstack_raii::Foreign::<#ftarget>::from_repr(__p) })
                            })
                        } else {
                            quote!(|__p: ::bstack_raii::ForeignRepr|
                                unsafe { ::bstack_raii::Foreign::<#ftarget>::from_repr(__p) })
                        };
                        let to_ptr = if elem_nullable {
                            quote!(|__f: #fty| match __f {
                                ::core::option::Option::Some(__ff) => __ff.repr(),
                                ::core::option::Option::None => ::bstack_raii::ForeignRepr::new(0, 0),
                            })
                        } else {
                            quote!(|__f: #fty| __f.repr())
                        };
                        data_variants.push(quote!(#vname(::std::vec::Vec<#fty>),));
                        view_variants.push(quote!(#vname(::std::vec::Vec<#fty>),));
                        new_arms.push(quote! {
                            #data::#vname(__list) => {
                                let __ptrs: ::std::vec::Vec<::bstack_raii::ForeignRepr> =
                                    __list.into_iter().map(#to_ptr).collect();
                                let __desc = #store::from_slice(allocator, &__ptrs)?.descriptor();
                                let mut __pl = [0u8; #payload_const];
                                __pl[..16].copy_from_slice(
                                    ::bstack_raii::bytemuck::bytes_of(&__desc));
                                (#disc, __pl)
                            }
                        });
                        read_arms.push(quote! {
                            #disc => #view::#vname(
                                #store::from_desc(#read_desc, allocator)
                                    .to_vec()?.into_iter().map(#from_ptr).collect()),
                        });
                        move_arms.push(quote! {
                            #disc => {
                                let __out: ::std::vec::Vec<#fty_mv> =
                                    #store::from_desc(#read_desc, __alloc)
                                        .to_vec()?.into_iter().map(#from_ptr).collect();
                                #store::from_desc(#read_desc, __alloc).bstack_drop()?;
                                #data::#vname(__out)
                            }
                        });
                        // Teardown: dispatch each element (non-ref), then free the data
                        // block (owned by the enum even for `ref`).
                        let elem_drop = foreign_elem_drop(kind, ftarget);
                        let drop_loop = if matches!(kind, Kind::Ref) {
                            quote!()
                        } else {
                            quote!(for __fp in #store::from_desc(#read_desc, allocator).to_vec()? {
                                #elem_drop
                            })
                        };
                        drop_arms.push(quote! {
                            #disc => {
                                #drop_loop
                                #store::from_desc(#read_desc, allocator).bstack_drop()?;
                            }
                        });
                        let elem_clone = foreign_elem_clone(kind, ftarget);
                        clone_arms.push(quote! {
                            #disc => {
                                let __src = #store::from_desc(#read_desc, allocator).to_vec()?;
                                let mut __new: ::std::vec::Vec<::bstack_raii::ForeignRepr> =
                                    ::std::vec::Vec::with_capacity(__src.len());
                                for __fp in __src {
                                    #elem_clone
                                    __new.push(__newfp);
                                }
                                let __newdesc = __plan.stage_bytevec(
                                    allocator, ::bstack_raii::bytemuck::cast_slice(&__new))?;
                                __pl[..16].copy_from_slice(
                                    ::bstack_raii::bytemuck::bytes_of(&__newdesc));
                            }
                        });
                        continue;
                    }

                    // A POD `V(Vec<Pod>)` / `V(String)` (un-annotated): a plain
                    // `BStackVec<elem>` (elem = the whole vec element type, itself
                    // `Pod` — arrays included — or `u8` for `String`). No block
                    // lifecycle, so clone is a verbatim byte copy.
                    if kind == Kind::Pod {
                        let elem: TokenStream = if vec_info(ty).is_some_and(|vi| vi.is_string) {
                            quote!(u8)
                        } else {
                            let vi = vec_inner(ty).unwrap();
                            quote!(#vi)
                        };
                        // The vec handle borrows the allocator → both enums need generics.
                        has_shared = true;
                        has_weak = true;
                        data_variants
                            .push(quote!(#vname(::bstack_raii::BStackVec<'__e, #elem, __A>),));
                        view_variants
                            .push(quote!(#vname(::bstack_raii::BStackVec<'__e, #elem, __A>),));
                        new_arms.push(quote! {
                            #data::#vname(__v) => {
                                let __desc = __v.descriptor();
                                let mut __pl = [0u8; #payload_const];
                                __pl[..16].copy_from_slice(
                                    ::bstack_raii::bytemuck::bytes_of(&__desc));
                                (#disc, __pl)
                            }
                        });
                        read_arms.push(quote! {
                            #disc => #view::#vname(
                                ::bstack_raii::BStackVec::<#elem, __A>::from_desc(
                                    #read_desc, allocator)),
                        });
                        move_arms.push(quote! {
                            #disc => #data::#vname(
                                ::bstack_raii::BStackVec::<#elem, __A>::from_desc(
                                    #read_desc, __alloc)),
                        });
                        drop_arms.push(quote! {
                            #disc => {
                                ::bstack_raii::BStackVec::<#elem, __A>::from_desc(
                                    #read_desc, allocator).bstack_drop()?;
                            }
                        });
                        clone_arms.push(quote! {
                            #disc => {
                                let __srcdesc = #read_desc;
                                let __newdesc = ::bstack_raii::BStackVec::<#elem, __A>::from_desc(
                                    __srcdesc, allocator).clone_data_into(__plan)?;
                                __pl[..16].copy_from_slice(
                                    ::bstack_raii::bytemuck::bytes_of(&__newdesc));
                            }
                        });
                        continue;
                    }
                    if vec_info(ty).is_some_and(|vi| vi.is_string) {
                        return Err(Error::new_spanned(
                            ty,
                            "`String` is always POD; drop the ownership annotation to store \
                             it as a POD `V(String)` variant",
                        ));
                    }
                    let is_weak = kind == Kind::Weak;
                    let vec_ty = match kind {
                        Kind::Owned => quote!(BStackBlockVec),
                        Kind::Strong => quote!(BStackStrongVec),
                        Kind::Weak => quote!(BStackWeakVec),
                        Kind::Ref => quote!(BStackRefVec),
                        _ => unreachable!(),
                    };

                    // Shared teardown / clone (offset-agnostic: a `Vec<[T;N]>` stores
                    // its offsets FLAT, so the per-offset lifecycle is identical to a
                    // scalar block vector). `#elem` is the leaf block type below.
                    let velem = vec_inner(ty).unwrap();
                    let (dims, elem, leaf_nullable) = if let Type::Array(_) = velem {
                        array_shape(velem)?
                    } else {
                        (Vec::new(), velem, false)
                    };
                    let is_array = !dims.is_empty();
                    let size_elem = quote!(::core::mem::size_of::<
                        <#elem as ::bstack_raii::BStackBlock>::OnDisk>() as u64);

                    // The data / view enums must carry `<'__e, __A>` only when a
                    // variant's stored handle actually borrows the allocator. Scalar
                    // vecs always do (the `BStack*Vec` handle); an array's owning
                    // handle does only for strong/weak, and its view only for weak.
                    if !is_array || matches!(kind, Kind::Strong | Kind::Weak) {
                        has_shared = true;
                    }
                    if !is_array || is_weak {
                        has_weak = true;
                    }

                    // ---- Teardown (shared) ----
                    drop_arms.push(quote! {
                        #disc => {
                            ::bstack_raii::#vec_ty::<#elem, __A>::from_desc(#read_desc, allocator)
                                .bstack_drop()?;
                        }
                    });
                    // ---- Clone (shared) ----
                    let clone_expr = match kind {
                        Kind::Owned => quote!(
                            ::bstack_raii::BStackBlockVec::<#elem, __A>::from_desc(__srcdesc, allocator)
                                .clone_into(__plan, |__er, __p| {
                                    <#elem as ::bstack_raii::BStackBlock>::from_range(__er)
                                        .__bstack_clone_into(allocator, __p)
                                })?),
                        Kind::Strong => quote!(
                            ::bstack_raii::BStackStrongVec::<#elem, __A>::from_desc(__srcdesc, allocator)
                                .clone_into(__plan)?),
                        Kind::Weak => quote!(
                            ::bstack_raii::BStackWeakVec::<#elem, __A>::from_desc(__srcdesc, allocator)
                                .clone_into(__plan)?),
                        Kind::Ref => quote!(
                            ::bstack_raii::BStackRefVec::<#elem, __A>::from_desc(__srcdesc, allocator)
                                .clone_into(__plan)?),
                        _ => unreachable!(),
                    };
                    clone_arms.push(quote! {
                        #disc => {
                            let __srcdesc = #read_desc;
                            let __newdesc = #clone_expr;
                            __pl[..16].copy_from_slice(
                                ::bstack_raii::bytemuck::bytes_of(&__newdesc));
                        }
                    });

                    if !is_array {
                        // ---- Scalar `Vec<T>`: data = view = the vec handle ----
                        data_variants
                            .push(quote!(#vname(::bstack_raii::#vec_ty<'__e, #elem, __A>),));
                        view_variants
                            .push(quote!(#vname(::bstack_raii::#vec_ty<'__e, #elem, __A>),));
                        new_arms.push(quote! {
                            #data::#vname(__v) => {
                                let __desc = __v.descriptor();
                                let mut __pl = [0u8; #payload_const];
                                __pl[..16].copy_from_slice(
                                    ::bstack_raii::bytemuck::bytes_of(&__desc));
                                (#disc, __pl)
                            }
                        });
                        read_arms.push(quote! {
                            #disc => #view::#vname(
                                ::bstack_raii::#vec_ty::<#elem, __A>::from_desc(
                                    #read_desc, allocator)),
                        });
                        move_arms.push(quote! {
                            #disc => #data::#vname(
                                ::bstack_raii::#vec_ty::<#elem, __A>::from_desc(
                                    #read_desc, __alloc)),
                        });
                        continue;
                    }

                    // ---- `Vec<[T; N]>` (array element): flat storage, reshaped ----
                    let total = dims_prod(&dims);
                    let ctrl_ty = quote!(<#elem as ::bstack_raii::BStackWeakable>::Control);
                    let ctrl_size = quote!(::core::mem::size_of::<
                        <#elem as ::bstack_raii::BStackWeakable>::Control>() as u64);

                    // View leaf + per-leaf read from a chunk `__grp[k]`.
                    let view_leaf = if is_weak {
                        quote!(::core::option::Option<
                            ::bstack_raii::BStackRc<'__e, #elem, __A>>)
                    } else if leaf_nullable {
                        quote!(::core::option::Option<#elem>)
                    } else {
                        quote!(#elem)
                    };
                    let view_read = |k: &Ident| {
                        if is_weak {
                            quote!({
                                let __o = __grp[#k];
                                if __o == 0 { ::core::option::Option::None } else {
                                    let __ctrl = unsafe {
                                        ::bstack_raii::BStackRef::<#ctrl_ty>::from_range(
                                            ::bstack_raii::BStackRange::new(__o, #ctrl_size)) };
                                    let __wk = unsafe {
                                        ::bstack_raii::BStackWeak::<#elem, __A>::from_raw(
                                            __ctrl, allocator) };
                                    let __up = __wk.upgrade()?;
                                    let _ = __wk.into_raw();
                                    __up
                                }
                            })
                        } else if leaf_nullable {
                            quote!({
                                let __o = __grp[#k];
                                if __o == 0 { ::core::option::Option::None } else {
                                    ::core::option::Option::Some(
                                        <#elem as ::bstack_raii::BStackBlock>::from_range(
                                            ::bstack_raii::BStackRange::new(__o, #size_elem)))
                                }
                            })
                        } else {
                            quote!(<#elem as ::bstack_raii::BStackBlock>::from_range(
                                ::bstack_raii::BStackRange::new(__grp[#k], #size_elem)))
                        }
                    };
                    let view_build = nested_build(&dims, &view_leaf, &view_read);
                    let view_ret = nested_ty(&dims, &view_leaf);
                    view_variants.push(quote!(#vname(::std::vec::Vec<#view_ret>),));

                    // Owning-handle leaf + per-leaf reconstruction for `new`/`move`.
                    let own_leaf_base = match kind {
                        Kind::Owned => quote!(::bstack_raii::BStackOwned<#elem>),
                        Kind::Strong => quote!(::bstack_raii::BStackRc<'__e, #elem, __A>),
                        Kind::Weak => quote!(::bstack_raii::BStackWeak<'__e, #elem, __A>),
                        Kind::Ref => quote!(::bstack_raii::BStackRef<#elem>),
                        _ => unreachable!(),
                    };
                    let own_leaf = if leaf_nullable {
                        quote!(::core::option::Option<#own_leaf_base>)
                    } else {
                        own_leaf_base.clone()
                    };
                    let data_ty = nested_ty(&dims, &own_leaf);
                    data_variants.push(quote!(#vname(::std::vec::Vec<#data_ty>),));

                    // `new`: flatten `Vec<[[Handle;..];..]>` → flat offsets.
                    let off_of = |h: &Ident| match kind {
                        Kind::Owned => quote!({
                            let __h = #h.into_inner();
                            ::bstack_raii::BStackBlock::range(&__h).start()
                        }),
                        Kind::Strong => quote!({
                            let (__d, _c) = #h.into_raw();
                            __d.into_range().start()
                        }),
                        Kind::Weak => quote!(#h.into_raw().into_range().start()),
                        Kind::Ref => quote!(#h.into_range().start()),
                        _ => unreachable!(),
                    };
                    let leaf_write = |_k: &Ident, leaf: &Ident| {
                        if leaf_nullable {
                            let hh = format_ident!("__h");
                            let off = off_of(&hh);
                            quote!(__flat.push(match #leaf {
                                ::core::option::Option::Some(#hh) => #off,
                                ::core::option::Option::None => 0u64,
                            });)
                        } else {
                            let off = off_of(leaf);
                            quote!(__flat.push(#off);)
                        }
                    };
                    let consume_one = nested_consume(&dims, &quote!(__a), &leaf_write);
                    new_arms.push(quote! {
                        #data::#vname(__list) => {
                            let mut __flat: ::std::vec::Vec<u64> = ::std::vec::Vec::new();
                            for __a in __list {
                                #consume_one
                            }
                            let __desc = ::bstack_raii::BStackVec::<u64, __A>::from_slice(
                                allocator, &__flat)?.descriptor();
                            let mut __pl = [0u8; #payload_const];
                            __pl[..16].copy_from_slice(::bstack_raii::bytemuck::bytes_of(&__desc));
                            (#disc, __pl)
                        }
                    });

                    // `read`: flat offsets → chunk → reshape to `Vec<[[View;..];..]>`.
                    read_arms.push(quote! {
                        #disc => {
                            let __flat = ::bstack_raii::BStackVec::<u64, __A>::from_desc(
                                #read_desc, allocator).to_vec()?;
                            let mut __out = ::std::vec::Vec::with_capacity(__flat.len() / (#total));
                            for __grp in __flat.chunks(#total) {
                                __out.push(#view_build);
                            }
                            #view::#vname(__out)
                        }
                    });

                    // `move`: reshape to nested owning handles, then free the flat
                    // offset-array block (children now owned by the handles).
                    let move_read = |k: &Ident| {
                        let one = match kind {
                            Kind::Owned => quote!(unsafe {
                                ::bstack_raii::BStackOwned::from_raw(
                                    <#elem as ::bstack_raii::BStackBlock>::from_range(
                                        ::bstack_raii::BStackRange::new(__o, #size_elem)))
                            }),
                            Kind::Ref => quote!(unsafe {
                                ::bstack_raii::BStackRef::<#elem>::from_range(
                                    ::bstack_raii::BStackRange::new(__o, #size_elem))
                            }),
                            Kind::Strong => quote!({
                                let __data = unsafe {
                                    ::bstack_raii::BStackRef::<#elem>::from_range(
                                        ::bstack_raii::BStackRange::new(__o, #size_elem)) };
                                let (__d, __c) =
                                    <#elem as ::bstack_raii::BStackShared>::strong_parts(__data, __alloc)?;
                                unsafe { ::bstack_raii::BStackRc::from_raw(__d, __c, __alloc) }
                            }),
                            Kind::Weak => quote!({
                                let __ctrl = unsafe {
                                    ::bstack_raii::BStackRef::<#ctrl_ty>::from_range(
                                        ::bstack_raii::BStackRange::new(__o, #ctrl_size)) };
                                unsafe { ::bstack_raii::BStackWeak::from_raw(__ctrl, __alloc) }
                            }),
                            _ => unreachable!(),
                        };
                        if leaf_nullable {
                            quote!({
                                let __o = __grp[#k];
                                if __o == 0 { ::core::option::Option::None }
                                else { ::core::option::Option::Some(#one) }
                            })
                        } else {
                            quote!({ let __o = __grp[#k]; #one })
                        }
                    };
                    // The move fn's lifetime is `'__mv`, not the data enum's `'__e`,
                    // so `try_from::<[Handle; N]>` inside the reshape must name `'__mv`.
                    let own_leaf_base_mv = match kind {
                        Kind::Owned => quote!(::bstack_raii::BStackOwned<#elem>),
                        Kind::Strong => quote!(::bstack_raii::BStackRc<'__mv, #elem, __A>),
                        Kind::Weak => quote!(::bstack_raii::BStackWeak<'__mv, #elem, __A>),
                        Kind::Ref => quote!(::bstack_raii::BStackRef<#elem>),
                        _ => unreachable!(),
                    };
                    let own_leaf_mv = if leaf_nullable {
                        quote!(::core::option::Option<#own_leaf_base_mv>)
                    } else {
                        own_leaf_base_mv.clone()
                    };
                    let move_build = nested_build(&dims, &own_leaf_mv, &move_read);
                    move_arms.push(quote! {
                        #disc => {
                            let __flat = ::bstack_raii::BStackVec::<u64, __A>::from_desc(
                                #read_desc, __alloc).to_vec()?;
                            let mut __out = ::std::vec::Vec::with_capacity(__flat.len() / (#total));
                            for __grp in __flat.chunks(#total) {
                                __out.push(#move_build);
                            }
                            ::bstack_raii::BStackVec::<u64, __A>::from_desc(#read_desc, __alloc)
                                .bstack_drop()?;
                            #data::#vname(__out)
                        }
                    });
                    continue;
                }

                // Annotated **array** variant `#[..] V([T; N])`: N block references
                // stored inline in the payload as `[u64; N]` (N*8 bytes), the
                // per-element mirror of a `#[bstack_owned/strong/weak/ref] V(T)`.
                // Annotated **array** variant `#[..] V([T; N])` — possibly nested
                // `[[..]; ..]`, and (for owned/strong/ref) possibly with `Option<T>`
                // leaves. Block refs are stored **flat** in the payload as
                // `[u64; TOTAL]` (TOTAL*8 bytes), the per-element mirror of a
                // `#[bstack_owned/strong/weak/ref] V(T)`; `#[embed]` stores each
                // child's whole on-disk form verbatim.
                if let Type::Array(_) = ty {
                    let (dims, elem, elem_nullable) = array_shape(ty)?;
                    let total = dims_prod(&dims);

                    // Annotated **foreign array** variant `#[..] V([Foreign<T>; N])`
                    // (nested / per-element `Option`): a flat `[ForeignPtr; TOTAL]`
                    // (TOTAL*16 bytes) stored INLINE in the payload — the per-variant
                    // mirror of a `[Foreign<T>; N]` struct field.
                    if let Some(ftarget) = foreign_inner(elem) {
                        match kind {
                            Kind::Owned | Kind::Strong | Kind::Weak | Kind::Ref => {}
                            Kind::Pod => {
                                return Err(Error::new_spanned(
                                    ty,
                                    "a `[Foreign<T>; N]` enum variant needs an ownership \
                                     annotation (`#[bstack_owned/strong/weak/ref]`)",
                                ));
                            }
                            Kind::Embed => {
                                return Err(Error::new_spanned(
                                    ty,
                                    "`Foreign` is a pointer and cannot be `#[embed]`ed",
                                ));
                            }
                        }
                        reject_bad_foreign_target(ftarget, ty, "a `Foreign` array variant")?;
                        has_foreign = true;
                        payload_sizes.push(quote!((#total) * 16));
                        // Variant type uses the enum's `'__e`; the move build uses `'__mv`.
                        let fty = if elem_nullable {
                            quote!(::core::option::Option<::bstack_raii::Foreign<'__e, #ftarget>>)
                        } else {
                            quote!(::bstack_raii::Foreign<'__e, #ftarget>)
                        };
                        let fty_mv = if elem_nullable {
                            quote!(::core::option::Option<::bstack_raii::Foreign<'__mv, #ftarget>>)
                        } else {
                            quote!(::bstack_raii::Foreign<'__mv, #ftarget>)
                        };
                        let nested = nested_ty(&dims, &fty);
                        data_variants.push(quote!(#vname(#nested),));
                        view_variants.push(quote!(#vname(#nested),));

                        // new: flatten nested handles → `[ForeignPtr; TOTAL]` in `__pl`.
                        let leaf_write = |k: &Ident, leaf: &Ident| {
                            let to_fp = if elem_nullable {
                                quote!(match #leaf {
                                    ::core::option::Option::Some(__f) => __f.repr(),
                                    ::core::option::Option::None =>
                                        ::bstack_raii::ForeignRepr::new(0, 0),
                                })
                            } else {
                                quote!(#leaf.repr())
                            };
                            quote!(__pl[(#k) * 16..(#k) * 16 + 16].copy_from_slice(
                                ::bstack_raii::bytemuck::bytes_of(&(#to_fp)));)
                        };
                        let flatten = nested_consume(&dims, &quote!(__list), &leaf_write);
                        new_arms.push(quote! {
                            #data::#vname(__list) => {
                                let mut __pl = [0u8; #payload_const];
                                #flatten
                                (#disc, __pl)
                            }
                        });

                        // read / move: reshape `[ForeignRepr; TOTAL]` → nested handles.
                        // SAFETY: each repr was stored into this file; the returned
                        // `Foreign`s are lifetime-bound by the read (`'__e`) / move
                        // (`'__mv`) signature via the variant type.
                        let leaf_read = |k: &Ident| {
                            let fp = quote!(::bstack_raii::bytemuck::pod_read_unaligned::<
                                ::bstack_raii::ForeignRepr,
                            >(&__pl[(#k) * 16..(#k) * 16 + 16]));
                            if elem_nullable {
                                quote!({
                                    let __p = #fp;
                                    if __p.offset() == 0 {
                                        ::core::option::Option::None
                                    } else {
                                        ::core::option::Option::Some(
                                            unsafe { ::bstack_raii::Foreign::<#ftarget>::from_repr(__p) })
                                    }
                                })
                            } else {
                                quote!(unsafe { ::bstack_raii::Foreign::<#ftarget>::from_repr(#fp) })
                            }
                        };
                        let build_e = nested_build(&dims, &fty, &leaf_read);
                        let build_mv = nested_build(&dims, &fty_mv, &leaf_read);
                        read_arms.push(quote!(#disc => #view::#vname(#build_e),));
                        move_arms.push(quote!(#disc => #data::#vname(#build_mv),));

                        // Teardown / clone: iterate the flat slots (inline — no block).
                        if !matches!(kind, Kind::Ref) {
                            let elem_drop = foreign_elem_drop(kind, ftarget);
                            drop_arms.push(quote! {
                                #disc => {
                                    for __k in 0usize..(#total) {
                                        let __fp = ::bstack_raii::bytemuck::pod_read_unaligned::<
                                            ::bstack_raii::ForeignRepr,
                                        >(&__pl[__k * 16..__k * 16 + 16]);
                                        #elem_drop
                                    }
                                }
                            });
                            let elem_clone = foreign_elem_clone(kind, ftarget);
                            clone_arms.push(quote! {
                                #disc => {
                                    for __k in 0usize..(#total) {
                                        let __fp = ::bstack_raii::bytemuck::pod_read_unaligned::<
                                            ::bstack_raii::ForeignRepr,
                                        >(&__pl[__k * 16..__k * 16 + 16]);
                                        #elem_clone
                                        __pl[__k * 16..__k * 16 + 16].copy_from_slice(
                                            ::bstack_raii::bytemuck::bytes_of(&__newfp));
                                    }
                                }
                            });
                        }
                        continue;
                    }

                    // Flat byte read/write of leaf `#k`'s `u64` in the payload.
                    let pl_off = |k: &Ident| quote!(::bstack_raii::get_u64(&__pl[(#k) * 8..]));
                    let pl_put = |k: &Ident, off: TokenStream| {
                        quote!(__pl[(#k) * 8..(#k) * 8 + 8]
                            .copy_from_slice(&(#off).to_le_bytes());)
                    };

                    // ---- #[embed] array: verbatim child on-disk forms ----
                    if kind == Kind::Embed {
                        if elem_nullable {
                            return Err(Error::new_spanned(
                                ty,
                                "#[embed] does not support `Option`",
                            ));
                        }
                        enum_has_embed = true;
                        let child = elem;
                        embed_types.push(quote!(#child));
                        let co = quote!(<#child as ::bstack_raii::BStackBlock>::OnDisk);
                        payload_sizes.push(quote!((#total) * ::core::mem::size_of::<#co>()));

                        let data_leaf = quote!(::bstack_raii::BStackOwned<#child>);
                        let data_ty_nested = nested_ty(&dims, &data_leaf);
                        data_variants.push(quote!(#vname(#data_ty_nested),));
                        let view_ty_nested = nested_ty(&dims, &quote!(#child));
                        view_variants.push(quote!(#vname(#view_ty_nested),));

                        // new: push one copy entry per flat slot; payload stays zeroed.
                        let cap_write = |k: &Ident, leaf: &Ident| {
                            quote! {
                                let __h = #leaf.into_inner();
                                let __cr = ::bstack_raii::BStackBlock::range(&__h);
                                __embed_copy.push((
                                    __cr,
                                    ::core::mem::size_of::<#co>() as u64,
                                    (#k as u64) * ::core::mem::size_of::<#co>() as u64,
                                ));
                            }
                        };
                        let consume = nested_consume(&dims, &quote!(__arr), &cap_write);
                        new_arms.push(quote! {
                            #data::#vname(__arr) => {
                                #consume
                                (#disc, [0u8; #payload_const])
                            }
                        });

                        // read (view): nested child handles into the payload slots.
                        let read_leaf = |k: &Ident| {
                            quote!(<#child as ::bstack_raii::BStackBlock>::from_range(
                                ::bstack_raii::BStackRange::new(
                                    __base + (#k as u64) * __step, __step)))
                        };
                        let read_body = nested_build(&dims, &quote!(#child), &read_leaf);
                        read_arms.push(quote! {
                            #disc => {
                                let __base = self.0.start()
                                    + ::core::mem::offset_of!(#on_disk, __bstack_payload) as u64;
                                let __step = ::core::mem::size_of::<#co>() as u64;
                                #view::#vname(#read_body)
                            }
                        });

                        // move: re-home each embedded child to a fresh allocation.
                        let mv_read = |k: &Ident| {
                            quote! {{
                                let __start = (#k) * ::core::mem::size_of::<#co>();
                                let mut __slice =
                                    __alloc.alloc(::core::mem::size_of::<#co>() as u64)?;
                                let __r = __slice.as_range();
                                if let ::std::result::Result::Err(__e) = __slice.write_range(
                                    0, &__pl[__start..__start + ::core::mem::size_of::<#co>()])
                                {
                                    let _ = __alloc.dealloc(__slice);
                                    return ::std::result::Result::Err(__e);
                                }
                                unsafe { ::bstack_raii::BStackOwned::from_raw(
                                    <#child as ::bstack_raii::BStackBlock>::from_range(__r)) }
                            }}
                        };
                        let mv_body = nested_build(&dims, &data_leaf, &mv_read);
                        move_arms.push(quote!(#disc => #data::#vname(#mv_body),));

                        // teardown: free each embedded child's children in place.
                        drop_arms.push(quote! {
                            #disc => {
                                let __base = __range.start()
                                    + ::core::mem::offset_of!(#on_disk, __bstack_payload) as u64;
                                let __step = ::core::mem::size_of::<#co>() as u64;
                                for __k in 0usize..(#total) {
                                    let __embed = ::bstack_raii::BStackRange::new(
                                        __base + (__k as u64) * __step, __step);
                                    <#child>::__bstack_drop_children(__embed, allocator)?;
                                }
                            }
                        });

                        // clone: fold each embedded child inline; patch payload bytes.
                        clone_arms.push(quote! {
                            #disc => {
                                let __base = self.0.start()
                                    + ::core::mem::offset_of!(#on_disk, __bstack_payload) as u64;
                                let __step = ::core::mem::size_of::<#co>() as u64;
                                for __k in 0usize..(#total) {
                                    let __child = <#child as ::bstack_raii::BStackBlock>::from_range(
                                        ::bstack_raii::BStackRange::new(
                                            __base + (__k as u64) * __step, __step));
                                    let __fixed =
                                        __child.__bstack_clone_children_inplace(allocator, __plan)?;
                                    let __start = (__k) * ::core::mem::size_of::<#co>();
                                    __pl[__start..__start + ::core::mem::size_of::<#co>()]
                                        .copy_from_slice(::bstack_raii::bytemuck::bytes_of(&__fixed));
                                }
                            }
                        });
                        continue;
                    }

                    // ---- owned / strong / weak / ref: flat `[u64; TOTAL]` ----
                    let elem_size = quote!(::core::mem::size_of::<
                        <#elem as ::bstack_raii::BStackBlock>::OnDisk>() as u64);
                    payload_sizes.push(quote!((#total) * 8));

                    if kind == Kind::Weak {
                        has_shared = true;
                        has_weak = true;
                        let ctrl_ty = quote!(<#elem as ::bstack_raii::BStackWeakable>::Control);
                        let ctrl_size = quote!(::core::mem::size_of::<#ctrl_ty>() as u64);

                        let data_leaf = quote!(::bstack_raii::BStackWeak<'__e, #elem, __A>);
                        let data_ty_nested = nested_ty(&dims, &data_leaf);
                        data_variants.push(quote!(#vname(#data_ty_nested),));
                        let view_leaf = quote!(::core::option::Option<::bstack_raii::BStackRc<'__e, #elem, __A>>);
                        let view_ty_nested = nested_ty(&dims, &view_leaf);
                        view_variants.push(quote!(#vname(#view_ty_nested),));

                        // new: consume nested weaks → control offsets.
                        let cap_write = |k: &Ident, leaf: &Ident| {
                            pl_put(k, quote!(#leaf.into_raw().into_range().start()))
                        };
                        let consume = nested_consume(&dims, &quote!(__arr), &cap_write);
                        new_arms.push(quote! {
                            #data::#vname(__arr) => {
                                let mut __pl = [0u8; #payload_const];
                                #consume
                                (#disc, __pl)
                            }
                        });

                        // read: upgrade each control offset (fallible).
                        let view_leaf_e = view_leaf.clone();
                        let read_leaf = |k: &Ident| {
                            let off = pl_off(k);
                            quote! {{
                                let __off = #off;
                                let __ctrl = unsafe {
                                    ::bstack_raii::BStackRef::<#ctrl_ty>::from_range(
                                        ::bstack_raii::BStackRange::new(__off, #ctrl_size)) };
                                let __wk = unsafe {
                                    ::bstack_raii::BStackWeak::<#elem, __A>::from_raw(__ctrl, allocator) };
                                let __up = __wk.upgrade()?;
                                let _ = __wk.into_raw();
                                __up
                            }}
                        };
                        let read_body = nested_build(&dims, &view_leaf_e, &read_leaf);
                        read_arms.push(quote!(#disc => #view::#vname(#read_body),));

                        // move: nested `BStackWeak<'__mv>` from control offsets.
                        let mv_leaf = quote!(::bstack_raii::BStackWeak<'__mv, #elem, __A>);
                        let mv_read = |k: &Ident| {
                            let off = pl_off(k);
                            quote! {{
                                let __ctrl = unsafe {
                                    ::bstack_raii::BStackRef::<#ctrl_ty>::from_range(
                                        ::bstack_raii::BStackRange::new(#off, #ctrl_size)) };
                                unsafe { ::bstack_raii::BStackWeak::from_raw(__ctrl, __alloc) }
                            }}
                        };
                        let mv_body = nested_build(&dims, &mv_leaf, &mv_read);
                        move_arms.push(quote!(#disc => #data::#vname(#mv_body),));

                        // teardown: release each weak.
                        drop_arms.push(quote! {
                            #disc => {
                                for __k in 0usize..(#total) {
                                    let __off = ::bstack_raii::get_u64(&__pl[__k * 8..]);
                                    let __ctrl = unsafe {
                                        ::bstack_raii::BStackRef::<#ctrl_ty>::from_range(
                                            ::bstack_raii::BStackRange::new(__off, #ctrl_size)) };
                                    ::bstack_raii::WeakRef::<#elem>(__ctrl).bstack_drop(allocator)?;
                                }
                            }
                        });
                        // clone: bump each weak.
                        clone_arms.push(quote! {
                            #disc => {
                                for __k in 0usize..(#total) {
                                    let __off = ::bstack_raii::get_u64(&__pl[__k * 8..]);
                                    __plan.bump_weak(__off);
                                }
                            }
                        });
                        continue;
                    }

                    // owned / strong / ref
                    if kind == Kind::Strong {
                        has_shared = true;
                    }
                    let view_leaf = if elem_nullable {
                        quote!(::core::option::Option<#elem>)
                    } else {
                        quote!(#elem)
                    };
                    let view_ty_nested = nested_ty(&dims, &view_leaf);
                    view_variants.push(quote!(#vname(#view_ty_nested),));

                    let data_leaf = match kind {
                        Kind::Owned => quote!(::bstack_raii::BStackOwned<#elem>),
                        Kind::Strong => quote!(::bstack_raii::BStackRc<'__e, #elem, __A>),
                        Kind::Ref => quote!(::bstack_raii::BStackRef<#elem>),
                        _ => unreachable!(),
                    };
                    let data_leaf_full = if elem_nullable {
                        quote!(::core::option::Option<#data_leaf>)
                    } else {
                        data_leaf.clone()
                    };
                    let data_ty_nested = nested_ty(&dims, &data_leaf_full);
                    data_variants.push(quote!(#vname(#data_ty_nested),));

                    let off_of = |h: &Ident| match kind {
                        Kind::Owned => quote!({
                            let __h = #h.into_inner();
                            ::bstack_raii::BStackBlock::range(&__h).start()
                        }),
                        Kind::Strong => quote!({
                            let (__d, _c) = #h.into_raw();
                            __d.into_range().start()
                        }),
                        Kind::Ref => quote!(#h.into_range().start()),
                        _ => unreachable!(),
                    };
                    // new: consume nested handles → offsets.
                    let cap_write = |k: &Ident, leaf: &Ident| {
                        if elem_nullable {
                            let hh = format_ident!("__handle");
                            let off = off_of(&hh);
                            let put = pl_put(k, quote!(__off));
                            quote! {{
                                let __off: u64 = match #leaf {
                                    ::core::option::Option::Some(#hh) => #off,
                                    ::core::option::Option::None => 0u64,
                                };
                                #put
                            }}
                        } else {
                            pl_put(k, off_of(leaf))
                        }
                    };
                    let consume = nested_consume(&dims, &quote!(__arr), &cap_write);
                    new_arms.push(quote! {
                        #data::#vname(__arr) => {
                            let mut __pl = [0u8; #payload_const];
                            #consume
                            (#disc, __pl)
                        }
                    });

                    // read (view): nested block views.
                    let read_leaf = |k: &Ident| {
                        let off = pl_off(k);
                        let build = quote!(<#elem as ::bstack_raii::BStackBlock>::from_range(
                            ::bstack_raii::BStackRange::new(__off, #elem_size)));
                        if elem_nullable {
                            quote! {{
                                let __off = #off;
                                if __off == 0 { ::core::option::Option::None }
                                else { ::core::option::Option::Some(#build) }
                            }}
                        } else {
                            quote!({ let __off = #off; #build })
                        }
                    };
                    let read_body = nested_build(&dims, &view_leaf, &read_leaf);
                    read_arms.push(quote!(#disc => #view::#vname(#read_body),));

                    // move.
                    let mv_leaf_base = match kind {
                        Kind::Owned => quote!(::bstack_raii::BStackOwned<#elem>),
                        Kind::Ref => quote!(::bstack_raii::BStackRef<#elem>),
                        Kind::Strong => quote!(::bstack_raii::BStackRc<'__mv, #elem, __A>),
                        _ => unreachable!(),
                    };
                    let mv_leaf_ty = if elem_nullable {
                        quote!(::core::option::Option<#mv_leaf_base>)
                    } else {
                        mv_leaf_base.clone()
                    };
                    let build_one = match kind {
                        Kind::Owned => quote!(unsafe {
                            ::bstack_raii::BStackOwned::from_raw(
                                <#elem as ::bstack_raii::BStackBlock>::from_range(
                                    ::bstack_raii::BStackRange::new(__off, #elem_size)))
                        }),
                        Kind::Ref => quote!(unsafe {
                            ::bstack_raii::BStackRef::<#elem>::from_range(
                                ::bstack_raii::BStackRange::new(__off, #elem_size))
                        }),
                        Kind::Strong => quote!({
                            let __data = unsafe {
                                ::bstack_raii::BStackRef::<#elem>::from_range(
                                    ::bstack_raii::BStackRange::new(__off, #elem_size)) };
                            let (__d, __c) =
                                <#elem as ::bstack_raii::BStackShared>::strong_parts(__data, __alloc)?;
                            unsafe { ::bstack_raii::BStackRc::from_raw(__d, __c, __alloc) }
                        }),
                        _ => unreachable!(),
                    };
                    let mv_read = |k: &Ident| {
                        let off = pl_off(k);
                        if elem_nullable {
                            quote! {{
                                let __off = #off;
                                if __off == 0 { ::core::option::Option::None }
                                else { ::core::option::Option::Some(#build_one) }
                            }}
                        } else {
                            quote!({ let __off = #off; #build_one })
                        }
                    };
                    let mv_body = nested_build(&dims, &mv_leaf_ty, &mv_read);
                    move_arms.push(quote!(#disc => #data::#vname(#mv_body),));

                    // teardown (owned/strong; ref owns nothing).
                    if kind != Kind::Ref {
                        let per = match kind {
                            Kind::Owned => quote! {
                                ::bstack_raii::OwnedRef(__child).bstack_drop(allocator)?;
                            },
                            Kind::Strong => quote! {
                                <#elem as ::bstack_raii::BStackShared>::drop_strong_ref(
                                    __child, allocator)?;
                            },
                            _ => unreachable!(),
                        };
                        drop_arms.push(quote! {
                            #disc => {
                                for __k in 0usize..(#total) {
                                    let __off = ::bstack_raii::get_u64(&__pl[__k * 8..]);
                                    if __off != 0 {
                                        let __child = unsafe {
                                            ::bstack_raii::BStackRef::<#elem>::from_range(
                                                ::bstack_raii::BStackRange::new(__off, #elem_size)) };
                                        #per
                                    }
                                }
                            }
                        });
                    }

                    // clone: owned deep-clones each; strong bumps each; ref aliases.
                    match kind {
                        Kind::Owned => clone_arms.push(quote! {
                            #disc => {
                                for __k in 0usize..(#total) {
                                    let __off = ::bstack_raii::get_u64(&__pl[__k * 8..]);
                                    if __off != 0 {
                                        let __child =
                                            <#elem as ::bstack_raii::BStackBlock>::from_range(
                                                ::bstack_raii::BStackRange::new(__off, #elem_size));
                                        let __new = __child.__bstack_clone_into(allocator, __plan)?;
                                        __pl[__k * 8..__k * 8 + 8]
                                            .copy_from_slice(&__new.start().to_le_bytes());
                                    }
                                }
                            }
                        }),
                        Kind::Strong => clone_arms.push(quote! {
                            #disc => {
                                for __k in 0usize..(#total) {
                                    let __off = ::bstack_raii::get_u64(&__pl[__k * 8..]);
                                    if __off != 0 {
                                        let __data = unsafe {
                                            ::bstack_raii::BStackRef::<#elem>::from_range(
                                                ::bstack_raii::BStackRange::new(__off, #elem_size)) };
                                        __plan.bump_strong(__data, allocator)?;
                                    }
                                }
                            }
                        }),
                        // Ref: aliased — payload offsets copied verbatim.
                        _ => {}
                    }
                    continue;
                }

                // Annotated **foreign** variant `#[..] V(Foreign<T>)`: a cross-file
                // wide pointer stored as a 16-byte `ForeignPtr` in the payload. The
                // annotation names the target's ownership in its own file (teardown /
                // clone dispatch cross-file, like a scalar `Foreign` struct field).
                // Concrete target only for now; container-in-variant is not handled.
                if let Some(ftarget) = foreign_inner(ty) {
                    match kind {
                        Kind::Owned | Kind::Strong | Kind::Weak | Kind::Ref => {}
                        Kind::Pod => {
                            return Err(Error::new_spanned(
                                ty,
                                "a `Foreign` enum variant needs an ownership annotation \
                                 (`#[bstack_owned/strong/weak/ref]`) naming the target's kind",
                            ));
                        }
                        Kind::Embed => {
                            return Err(Error::new_spanned(
                                ty,
                                "`Foreign` is a pointer and cannot be `#[embed]`ed",
                            ));
                        }
                    }
                    // Generic foreign targets are allowed (bounds inferred above).
                    reject_bad_foreign_target(ftarget, ty, "a `Foreign` enum variant")?;

                    has_foreign = true;
                    payload_sizes.push(quote!(16usize));
                    // `'__e` is the enum's read / move borrow (see `has_foreign`), so a
                    // `SELF` pointer in this variant cannot escape it.
                    let fty = quote!(::bstack_raii::Foreign<'__e, #ftarget>);
                    let read_fp = quote!(::bstack_raii::bytemuck::pod_read_unaligned::<
                        ::bstack_raii::ForeignRepr,
                    >(&__pl[..16]));
                    data_variants.push(quote!(#vname(#fty),));
                    view_variants.push(quote!(#vname(#fty),));
                    new_arms.push(quote! {
                        #data::#vname(__f) => {
                            let mut __pl = [0u8; #payload_const];
                            __pl[..16].copy_from_slice(
                                ::bstack_raii::bytemuck::bytes_of(&__f.repr()));
                            (#disc, __pl)
                        }
                    });
                    // SAFETY: the repr was stored into this file; bound to `'__e`.
                    read_arms.push(quote!(#disc => #view::#vname(
                        unsafe { ::bstack_raii::Foreign::from_repr(#read_fp) }),));
                    move_arms.push(quote!(#disc => #data::#vname(
                        unsafe { ::bstack_raii::Foreign::from_repr(#read_fp) }),));
                    // Teardown / clone dispatch (a `#[bstack_ref]` owns nothing → none;
                    // its `ForeignPtr` is byte-copied by the payload catch-all).
                    if !matches!(kind, Kind::Ref) {
                        let elem_drop = foreign_elem_drop(kind, ftarget);
                        drop_arms.push(quote! {
                            #disc => {
                                let __fp: ::bstack_raii::ForeignRepr = #read_fp;
                                #elem_drop
                            }
                        });
                        let elem_clone = foreign_elem_clone(kind, ftarget);
                        clone_arms.push(quote! {
                            #disc => {
                                let __fp: ::bstack_raii::ForeignRepr = #read_fp;
                                #elem_clone
                                __pl[..16].copy_from_slice(
                                    ::bstack_raii::bytemuck::bytes_of(&__newfp));
                            }
                        });
                    }
                    continue;
                }

                // Annotated **foreign tuple** variant `#[..] V((A, Foreign<T>, ..))`:
                // POD elements packed inline, each foreign element a 16-byte
                // `ForeignPtr`, all at cumulative byte offsets in the payload (the
                // per-variant mirror of a `#[ann] (A, Foreign<T>)` struct field). The
                // annotation names the foreign elements' ownership.
                if let Type::Tuple(tup) = ty
                    && tup
                        .elems
                        .iter()
                        .any(|e| foreign_inner(option_inner(e).unwrap_or(e)).is_some())
                {
                    if kind == Kind::Embed {
                        return Err(Error::new_spanned(ty, "cannot #[embed] a tuple"));
                    }
                    let nelem = tup.elems.len();
                    let mut is_foreign = Vec::with_capacity(nelem);
                    let mut ftargets: Vec<Option<&Type>> = Vec::with_capacity(nelem);
                    let mut nulls = Vec::with_capacity(nelem);
                    for e in &tup.elems {
                        let inner = option_inner(e).unwrap_or(e);
                        if let Some(ft) = foreign_inner(inner) {
                            reject_bad_foreign_target(ft, ty, "a `Foreign` tuple element")?;
                            is_foreign.push(true);
                            ftargets.push(Some(ft));
                            nulls.push(option_inner(e).is_some());
                        } else {
                            is_foreign.push(false);
                            ftargets.push(None);
                            nulls.push(false);
                            pod_types.push(e.clone());
                        }
                    }

                    // Element byte offsets + the total payload size.
                    let mut offsets = Vec::with_capacity(nelem);
                    let mut acc = quote!(0usize);
                    let mut sizes = Vec::with_capacity(nelem);
                    for (&frn, e) in is_foreign.iter().zip(&tup.elems) {
                        offsets.push(acc.clone());
                        let sz = if frn {
                            quote!(16usize)
                        } else {
                            quote!(::core::mem::size_of::<#e>())
                        };
                        sizes.push(sz.clone());
                        acc = quote!(#acc + #sz);
                    }
                    payload_sizes.push(acc);

                    has_foreign = true;
                    // Public tuple type: `Foreign` → `::bstack_raii::Foreign<'__e, _>` so a
                    // `SELF` element is bound to the enum's read / move borrow (+ Option).
                    let pub_elems: Vec<TokenStream> = (0..nelem)
                        .map(|i| {
                            if is_foreign[i] {
                                let ft = ftargets[i].unwrap();
                                if nulls[i] {
                                    quote!(::core::option::Option<::bstack_raii::Foreign<'__e, #ft>>)
                                } else {
                                    quote!(::bstack_raii::Foreign<'__e, #ft>)
                                }
                            } else {
                                let e = &tup.elems[i];
                                quote!(#e)
                            }
                        })
                        .collect();
                    let pub_tuple_ty = quote!(( #(#pub_elems,)* ));
                    data_variants.push(quote!(#vname(#pub_tuple_ty),));
                    view_variants.push(quote!(#vname(#pub_tuple_ty),));

                    // new: destructure the tuple, write each element into the payload.
                    let binds: Vec<Ident> = (0..nelem).map(|i| format_ident!("__f{}", i)).collect();
                    let writes: Vec<TokenStream> = (0..nelem)
                        .map(|i| {
                            let b = &binds[i];
                            let off = &offsets[i];
                            let sz = &sizes[i];
                            if is_foreign[i] {
                                let to_fp = if nulls[i] {
                                    quote!(match #b {
                                        ::core::option::Option::Some(__x) => __x.repr(),
                                        ::core::option::Option::None =>
                                            ::bstack_raii::ForeignRepr::new(0, 0),
                                    })
                                } else {
                                    quote!(#b.repr())
                                };
                                quote!(__pl[(#off)..(#off) + 16].copy_from_slice(
                                    ::bstack_raii::bytemuck::bytes_of(&(#to_fp)));)
                            } else {
                                quote!(__pl[(#off)..(#off) + #sz].copy_from_slice(
                                    ::bstack_raii::bytemuck::bytes_of(&#b));)
                            }
                        })
                        .collect();
                    new_arms.push(quote! {
                        #data::#vname(( #(#binds,)* )) => {
                            let mut __pl = [0u8; #payload_const];
                            #(#writes)*
                            (#disc, __pl)
                        }
                    });

                    // read / move: rebuild the tuple from the payload.
                    let reads: Vec<TokenStream> = (0..nelem)
                        .map(|i| {
                            let off = &offsets[i];
                            let sz = &sizes[i];
                            if is_foreign[i] {
                                let ft = ftargets[i].unwrap();
                                let fp = quote!(::bstack_raii::bytemuck::pod_read_unaligned::<
                                    ::bstack_raii::ForeignRepr,
                                >(&__pl[(#off)..(#off) + 16]));
                                // SAFETY: repr stored into this file; bound by the
                                // variant type (read `'__e` / move `'__mv`).
                                if nulls[i] {
                                    quote!({
                                        let __p = #fp;
                                        if __p.offset() == 0 {
                                            ::core::option::Option::None
                                        } else {
                                            ::core::option::Option::Some(
                                                unsafe { ::bstack_raii::Foreign::<#ft>::from_repr(__p) })
                                        }
                                    })
                                } else {
                                    quote!(unsafe { ::bstack_raii::Foreign::<#ft>::from_repr(#fp) })
                                }
                            } else {
                                let e = &tup.elems[i];
                                quote!(::bstack_raii::bytemuck::pod_read_unaligned::<#e>(
                                    &__pl[(#off)..(#off) + #sz]))
                            }
                        })
                        .collect();
                    read_arms.push(quote!(#disc => #view::#vname(( #(#reads,)* )),));
                    move_arms.push(quote!(#disc => #data::#vname(( #(#reads,)* )),));

                    // Teardown / clone: dispatch each foreign element (ref = none).
                    if !matches!(kind, Kind::Ref) {
                        let mut drops = Vec::new();
                        let mut clones = Vec::new();
                        for i in 0..nelem {
                            if !is_foreign[i] {
                                continue;
                            }
                            let off = &offsets[i];
                            let ft = ftargets[i].unwrap();
                            let read_fp = quote!(::bstack_raii::bytemuck::pod_read_unaligned::<
                                ::bstack_raii::ForeignRepr,
                            >(&__pl[(#off)..(#off) + 16]));
                            let ed = foreign_elem_drop(kind, ft);
                            drops.push(
                                quote! { { let __fp: ::bstack_raii::ForeignRepr = #read_fp; #ed } },
                            );
                            let ec = foreign_elem_clone(kind, ft);
                            clones.push(quote! {
                                {
                                    let __fp: ::bstack_raii::ForeignRepr = #read_fp;
                                    #ec
                                    __pl[(#off)..(#off) + 16].copy_from_slice(
                                        ::bstack_raii::bytemuck::bytes_of(&__newfp));
                                }
                            });
                        }
                        drop_arms.push(quote!(#disc => { #(#drops)* }));
                        clone_arms.push(quote!(#disc => { #(#clones)* }));
                    }
                    continue;
                }

                match kind {
                    Kind::Pod => unreachable!("guarded out above"),
                    Kind::Owned => {
                        payload_sizes.push(quote!(8usize));
                        let child = child_from_off(ty);
                        data_variants.push(quote!(#vname(::bstack_raii::BStackOwned<#ty>),));
                        view_variants.push(quote!(#vname(#ty),));
                        new_arms.push(quote! {
                            #data::#vname(__v) => {
                                let __h = __v.into_inner();
                                let __off = ::bstack_raii::BStackBlock::range(&__h).start();
                                let mut __pl = [0u8; #payload_const];
                                __pl[..8].copy_from_slice(&__off.to_le_bytes());
                                (#disc, __pl)
                            }
                        });
                        read_arms.push(quote!(#disc => #view::#vname(#child),));
                        move_arms.push(quote! {
                            #disc => #data::#vname(unsafe {
                                ::bstack_raii::BStackOwned::from_raw(#child)
                            }),
                        });
                        drop_arms.push(quote! {
                            #disc => {
                                let __child = unsafe {
                                    ::bstack_raii::BStackRef::<#ty>::from_range(
                                        ::bstack_raii::BStackRange::new(
                                            ::bstack_raii::get_u64(&__pl),
                                            ::core::mem::size_of::<
                                                <#ty as ::bstack_raii::BStackBlock>::OnDisk
                                            >() as u64,
                                        ),
                                    )
                                };
                                ::bstack_raii::OwnedRef(__child).bstack_drop(allocator)?;
                            }
                        });
                        clone_arms.push(quote! {
                            #disc => {
                                let __off = ::bstack_raii::get_u64(&__pl);
                                let __child = <#ty as ::bstack_raii::BStackBlock>::from_range(
                                    ::bstack_raii::BStackRange::new(
                                        __off,
                                        ::core::mem::size_of::<
                                            <#ty as ::bstack_raii::BStackBlock>::OnDisk
                                        >() as u64,
                                    ),
                                );
                                let __new = __child.__bstack_clone_into(allocator, __plan)?;
                                __pl[..8].copy_from_slice(&__new.start().to_le_bytes());
                            }
                        });
                    }
                    Kind::Ref => {
                        payload_sizes.push(quote!(8usize));
                        let child = child_from_off(ty);
                        let cref = child_ref(ty);
                        data_variants.push(quote!(#vname(::bstack_raii::BStackRef<#ty>),));
                        view_variants.push(quote!(#vname(#ty),));
                        new_arms.push(quote! {
                            #data::#vname(__v) => {
                                let mut __pl = [0u8; #payload_const];
                                __pl[..8].copy_from_slice(&__v.into_range().start().to_le_bytes());
                                (#disc, __pl)
                            }
                        });
                        read_arms.push(quote!(#disc => #view::#vname(#child),));
                        move_arms.push(quote!(#disc => #data::#vname(unsafe { #cref }),));
                        // A raw reference owns nothing: no teardown.
                    }
                    Kind::Strong => {
                        has_shared = true;
                        payload_sizes.push(quote!(8usize));
                        let child = child_from_off(ty);
                        // A strong variant stores the child's DATA offset and holds
                        // one strong reference (like a `#[bstack_strong]` field).
                        let cref = child_ref(ty);
                        data_variants
                            .push(quote!(#vname(::bstack_raii::BStackRc<'__e, #ty, __A>),));
                        view_variants.push(quote!(#vname(#ty),));
                        new_arms.push(quote! {
                            #data::#vname(__v) => {
                                let (__data, _ctrl) = __v.into_raw();
                                let mut __pl = [0u8; #payload_const];
                                __pl[..8].copy_from_slice(&__data.into_range().start().to_le_bytes());
                                (#disc, __pl)
                            }
                        });
                        read_arms.push(quote!(#disc => #view::#vname(#child),));
                        // Move: rebuild a `BStackRc` (transferring the strong ref)
                        // via `strong_parts` — exactly like a `#[bstack_strong]` field.
                        move_arms.push(quote! {
                            #disc => {
                                let __data = unsafe { #cref };
                                let (__d, __c) =
                                    <#ty as ::bstack_raii::BStackShared>::strong_parts(__data, __alloc)?;
                                #data::#vname(unsafe {
                                    ::bstack_raii::BStackRc::from_raw(__d, __c, __alloc)
                                })
                            }
                        });
                        drop_arms.push(quote! {
                            #disc => {
                                let __data = unsafe { #cref };
                                <#ty as ::bstack_raii::BStackShared>::drop_strong_ref(
                                    __data, allocator,
                                )?;
                            }
                        });
                        clone_arms.push(quote! {
                            #disc => {
                                let __data = unsafe { #cref };
                                __plan.bump_strong(__data, allocator)?;
                            }
                        });
                    }
                    Kind::Weak => {
                        has_shared = true;
                        has_weak = true;
                        payload_sizes.push(quote!(8usize));
                        let ctrl_ref = quote! {
                            unsafe {
                                ::bstack_raii::BStackRef::<
                                    <#ty as ::bstack_raii::BStackWeakable>::Control
                                >::from_range(::bstack_raii::BStackRange::new(
                                    ::bstack_raii::get_u64(&__pl),
                                    ::core::mem::size_of::<
                                        <#ty as ::bstack_raii::BStackWeakable>::Control
                                    >() as u64,
                                ))
                            }
                        };
                        // A weak variant stores the child's CONTROL offset and holds
                        // one weak reference (like a `#[bstack_weak]` field).
                        data_variants
                            .push(quote!(#vname(::bstack_raii::BStackWeak<'__e, #ty, __A>),));
                        view_variants.push(quote! {
                            #vname(::core::option::Option<
                                ::bstack_raii::BStackRc<'__e, #ty, __A>
                            >),
                        });
                        new_arms.push(quote! {
                            #data::#vname(__v) => {
                                let __ctrl = __v.into_raw();
                                let mut __pl = [0u8; #payload_const];
                                __pl[..8].copy_from_slice(&__ctrl.into_range().start().to_le_bytes());
                                (#disc, __pl)
                            }
                        });
                        read_arms.push(quote! {
                            #disc => {
                                // Borrow a weak over the stored control ref just long
                                // enough to upgrade; consume it via `into_raw` so the
                                // variant's own weak count is untouched.
                                let __w = unsafe {
                                    ::bstack_raii::BStackWeak::<#ty, __A>::from_raw(#ctrl_ref, allocator)
                                };
                                let __up = __w.upgrade()?;
                                let _ = __w.into_raw();
                                #view::#vname(__up)
                            }
                        });
                        // Move: hand out the `BStackWeak` (transferring the weak ref),
                        // like moving out a `#[bstack_weak]` field.
                        move_arms.push(quote! {
                            #disc => #data::#vname(unsafe {
                                ::bstack_raii::BStackWeak::from_raw(#ctrl_ref, __alloc)
                            }),
                        });
                        drop_arms.push(quote! {
                            #disc => {
                                ::bstack_raii::WeakRef::<#ty>(#ctrl_ref).bstack_drop(allocator)?;
                            }
                        });
                        clone_arms.push(quote! {
                            #disc => {
                                let __ctrl_off = ::bstack_raii::get_u64(&__pl);
                                __plan.bump_weak(__ctrl_off);
                            }
                        });
                    }
                    // `#[embed] V(Child)`: the child's whole on-disk form is stored
                    // INLINE in the payload (header and all).
                    Kind::Embed => {
                        embed_types.push(quote!(#ty));
                        let co = quote!(<#ty as ::bstack_raii::BStackBlock>::OnDisk);
                        payload_sizes.push(quote!(::core::mem::size_of::<#co>()));
                        data_variants.push(quote!(#vname(::bstack_raii::BStackOwned<#ty>),));
                        view_variants.push(quote!(#vname(#ty),));
                        // new: capture the child's block range; the payload is a
                        // zeroed placeholder, and a post-write step `BStack::copy`s
                        // the child into it (then frees the shell) — no materialising.
                        enum_has_embed = true;
                        new_arms.push(quote! {
                            #data::#vname(__v) => {
                                let __h = __v.into_inner();
                                let __cr = ::bstack_raii::BStackBlock::range(&__h);
                                __embed_copy.push((
                                    __cr,
                                    ::core::mem::size_of::<#co>() as u64,
                                    0u64,
                                ));
                                (#disc, [0u8; #payload_const])
                            }
                        });
                        // read (view): a child handle at the embedded payload offset.
                        read_arms.push(quote! {
                            #disc => #view::#vname(
                                <#ty as ::bstack_raii::BStackBlock>::from_range(
                                    ::bstack_raii::BStackRange::new(
                                        self.0.start()
                                            + ::core::mem::offset_of!(#on_disk, __bstack_payload)
                                                as u64,
                                        ::core::mem::size_of::<#co>() as u64,
                                    ),
                                )
                            ),
                        });
                        // move: re-home the embedded child to a fresh allocation.
                        move_arms.push(quote! {
                            #disc => {
                                let mut __slice =
                                    __alloc.alloc(::core::mem::size_of::<#co>() as u64)?;
                                let __r = __slice.as_range();
                                if let ::std::result::Result::Err(__e) = __slice
                                    .write_range(0, &__pl[..::core::mem::size_of::<#co>()])
                                {
                                    let _ = __alloc.dealloc(__slice);
                                    return ::std::result::Result::Err(__e);
                                }
                                #data::#vname(unsafe {
                                    ::bstack_raii::BStackOwned::from_raw(
                                        <#ty as ::bstack_raii::BStackBlock>::from_range(__r),
                                    )
                                })
                            }
                        });
                        // teardown: free the embedded child's children in place.
                        drop_arms.push(quote! {
                            #disc => {
                                let __embed = ::bstack_raii::BStackRange::new(
                                    __range.start()
                                        + ::core::mem::offset_of!(#on_disk, __bstack_payload) as u64,
                                    ::core::mem::size_of::<#co>() as u64,
                                );
                                <#ty>::__bstack_drop_children(__embed, allocator)?;
                            }
                        });
                        clone_arms.push(quote! {
                            #disc => {
                                let __child = <#ty as ::bstack_raii::BStackBlock>::from_range(
                                    ::bstack_raii::BStackRange::new(
                                        self.0.start()
                                            + ::core::mem::offset_of!(#on_disk, __bstack_payload)
                                                as u64,
                                        ::core::mem::size_of::<#co>() as u64,
                                    ),
                                );
                                let __fixed =
                                    __child.__bstack_clone_children_inplace(allocator, __plan)?;
                                __pl[..::core::mem::size_of::<#co>()]
                                    .copy_from_slice(::bstack_raii::bytemuck::bytes_of(&__fixed));
                            }
                        });
                    }
                }
            }
            // A POD aggregate: unit, an all-POD tuple `V(A, B, ..)`, or an all-POD
            // struct `V { x: A, .. }`. The fields are packed sequentially into the
            // payload (declaration order). This is sound because the payload is
            // read/written **unaligned**, so field alignment is irrelevant — the
            // packed byte sequence of POD fields is itself just POD bytes.
            _ => {
                if kind != Kind::Pod {
                    return Err(Error::new_spanned(
                        variant,
                        "an ownership annotation is only allowed on a single-field tuple \
                         variant, e.g. `#[bstack_owned] V(T)`",
                    ));
                }
                let named = matches!(&variant.fields, Fields::Named(_));
                let mut binds = Vec::new();
                let mut tys: Vec<Type> = Vec::new();
                let mut fnames = Vec::new();
                for (j, f) in variant.fields.iter().enumerate() {
                    pod_types.push(f.ty.clone());
                    tys.push(f.ty.clone());
                    binds.push(format_ident!("__f{}", j));
                    if let Some(id) = &f.ident {
                        fnames.push(id.clone());
                    }
                }

                // Cumulative byte offsets of each field within the payload.
                let mut offsets = Vec::new();
                let mut acc = quote!(0usize);
                for ty in &tys {
                    offsets.push(acc.clone());
                    acc = quote!(#acc + ::core::mem::size_of::<#ty>());
                }
                let payload_size = if tys.is_empty() { quote!(0usize) } else { acc };

                let writes = binds.iter().zip(&offsets).zip(&tys).map(|((b, off), ty)| {
                    quote! {
                        __pl[(#off)..(#off) + ::core::mem::size_of::<#ty>()]
                            .copy_from_slice(::bstack_raii::bytemuck::bytes_of(&#b));
                    }
                });
                let reads: Vec<TokenStream> = offsets
                    .iter()
                    .zip(&tys)
                    .map(|(off, ty)| {
                        quote! {
                            ::bstack_raii::bytemuck::pod_read_unaligned::<#ty>(
                                &__pl[(#off)..(#off) + ::core::mem::size_of::<#ty>()],
                            )
                        }
                    })
                    .collect();

                // The variant's in-memory shape (`V`, `V(A, B)`, or `V { x: A, .. }`),
                // its destructuring pattern, and its reconstruction from `reads`.
                let (decl, pat, cons) = if tys.is_empty() {
                    (quote!(#vname,), quote!(#vname), quote!(#vname))
                } else if named {
                    (
                        quote!(#vname { #(#fnames: #tys),* },),
                        quote!(#vname { #(#fnames: #binds),* }),
                        quote!(#vname { #(#fnames: #reads),* }),
                    )
                } else {
                    (
                        quote!(#vname(#(#tys),*),),
                        quote!(#vname(#(#binds),*)),
                        quote!(#vname(#(#reads),*)),
                    )
                };
                if !tys.is_empty() {
                    needs_payload = true;
                }

                data_variants.push(decl.clone());
                view_variants.push(decl);
                payload_sizes.push(payload_size);
                new_arms.push(quote! {
                    #data::#pat => {
                        let mut __pl = [0u8; #payload_const];
                        #(#writes)*
                        (#disc, __pl)
                    }
                });
                read_arms.push(quote!(#disc => #view::#cons,));
                move_arms.push(quote!(#disc => #data::#cons,));
                // POD: no teardown.
            }
        }
    }

    // EightCC tag: readable prefix over a hash of `crate ++ type_name` (as structs).
    let type_name = name.to_string();
    let crate_name = std::env::var("CARGO_PKG_NAME").unwrap_or_default();
    let hash = fnv1a64(&format!("{crate_name}\0{type_name}"));
    let prefix = attr.tag.as_ref().map_or_else(
        || auto_prefix(&type_name),
        |t| t.bytes().collect::<Vec<u8>>(),
    );
    let tag = build_tag(hash, &prefix);
    let eightcc = eightcc_expr(&tag.bytes);
    // For a generic enum, fold each type argument's tag into the discriminant so
    // distinct instantiations get distinct tags (mixed at runtime in `eightcc()`).
    let eightcc = if type_params.is_empty() {
        eightcc
    } else {
        let mixes = type_params
            .iter()
            .map(|p| quote!(.mix(<#p as ::bstack_raii::BStackCast>::eightcc())));
        quote!(#eightcc #(#mixes)*)
    };

    // Control-block tag (rc, weak): the data tag with its prefix lowercased, or a
    // `ctrl_tag` override.
    let ctrl_prefix = attr.ctrl_tag.as_ref().map_or_else(
        || {
            prefix
                .iter()
                .map(u8::to_ascii_lowercase)
                .collect::<Vec<u8>>()
        },
        |t| t.bytes().collect::<Vec<u8>>(),
    );
    let ctrl_tag = build_tag(hash, &ctrl_prefix);
    let ctrl_eightcc = eightcc_expr(&ctrl_tag.bytes);

    // Refcount / control machinery, mirroring the struct rc modes: an injected
    // field after the header, `BStackShared` (rc / rc,weak), and (rc, weak) a
    // control block + `BStackWeakable`. `new` returns `BStackRc` for rc modes.
    let injected_ondisk = match mode {
        Mode::Plain => quote!(),
        Mode::Rc => quote!(__bstack_refcount: u64,),
        Mode::RcWeak => quote!(__bstack_ctrl: u64,),
    };
    let new_ret = match mode {
        Mode::Plain => quote!(::bstack_raii::BStackOwned<Self>),
        _ => quote!(::bstack_raii::BStackRc<'__e, Self, __A>),
    };
    // `EData` type name — `new`'s `data` parameter and `bstack_move!` output.
    // The companion `EData` / `EView` enums are generic over `<'e, A>` (when a
    // strong/weak variant needs it) AND over the enum's own type parameters. These
    // helpers assemble a use (`EData<'e, A, T>`) or a decl (`<'e, A, T: Bound>`)
    // with the right subset present.
    let etp_args: Vec<TokenStream> = type_params.iter().map(|p| quote!(#p)).collect();
    let etp_decl: Vec<TokenStream> = aug_generics.type_params().map(|tp| quote!(#tp)).collect();
    // `want_lt` and `want_a` are decoupled: a strong/weak variant needs *both* the
    // lifetime and `__A` (`BStackRc<'__e, T, __A>`), but a `Foreign` variant needs only
    // the lifetime (`Foreign<'__e, T>` binds a `SELF` pointer to the read/move borrow,
    // carrying no allocator) — so an enum with a `Foreign` variant but no shared/weak
    // one gets `'__e` without an unused `__A` param.
    let comp_ty = |id: &Ident, lt: &TokenStream, want_lt: bool, want_a: bool| -> TokenStream {
        let mut args: Vec<TokenStream> = Vec::new();
        if want_lt {
            args.push(lt.clone());
        }
        if want_a {
            args.push(quote!(__A));
        }
        args.extend(etp_args.iter().cloned());
        if args.is_empty() {
            quote!(#id)
        } else {
            quote!(#id < #(#args),* >)
        }
    };
    let comp_decl = |lt: &TokenStream, want_lt: bool, want_a: bool| -> TokenStream {
        let mut parts: Vec<TokenStream> = Vec::new();
        if want_lt {
            parts.push(lt.clone());
        }
        if want_a {
            parts.push(quote!(__A: ::bstack_raii::BStackRaiiAllocator));
        }
        parts.extend(etp_decl.iter().cloned());
        if parts.is_empty() {
            quote!()
        } else {
            quote!(< #(#parts),* >)
        }
    };
    let data_ty = comp_ty(&data, &quote!('__e), has_shared || has_foreign, has_shared);
    // The `new` constructor. Plain / `rc` are one atomic write (the injected
    // refcount is baked into the image); `(rc, weak)` allocates data + control and
    // commits both images in one `set_batched`, with the control back-pointer
    // baked into the data image — no separate back-pointer write, no half-wired
    // transient state.
    let enum_header = quote! {
        __bstack_header: ::bstack_raii::BlockHeader {
            size: ::core::mem::size_of::<#on_disk>() as u64,
            tag: <Self as ::bstack_raii::BStackCast>::eightcc(),
        },
    };
    let enum_size = quote!(::core::mem::size_of::<#on_disk>() as u64);
    // For an `#[embed]` variant, `new` declares a captured child range (set by the
    // active variant's arm) and, after writing the OnDisk with a zeroed payload,
    // `BStack::copy`s the child into the payload and frees its shell.
    let (embed_decl, embed_post) = if enum_has_embed {
        (
            // Each entry: (child source range, byte size, destination offset WITHIN
            // the payload). A scalar `#[embed] V(Child)` pushes one entry at offset
            // 0; an `#[embed] V([Child; N])` (or nested) pushes one per flat slot.
            quote!(let mut __embed_copy:
                ::std::vec::Vec<(::bstack_raii::BStackRange, u64, u64)> =
                ::std::vec::Vec::new();),
            quote! {
                for (__cr, __sz, __doff) in __embed_copy {
                    allocator.stack().copy(
                        __cr.start(),
                        __data.start()
                            + ::core::mem::offset_of!(#on_disk, __bstack_payload) as u64
                            + __doff,
                        __sz,
                    )?;
                    unsafe { ::bstack_raii::dealloc_range(allocator, __cr)?; }
                }
            },
        )
    } else {
        (quote!(), quote!())
    };
    let enum_new = match mode {
        Mode::Plain | Mode::Rc => {
            let injected_init = if let Mode::Rc = mode {
                quote!(__bstack_refcount: 1u64,)
            } else {
                quote!()
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
                #vis fn new<'__e, __A: ::bstack_raii::BStackRaiiAllocator>(
                    allocator: &'__e __A,
                    data: #data_ty,
                ) -> ::std::io::Result<#new_ret> {
                    #embed_decl
                    let (__disc, __payload): (#disc_ty, [u8; #payload_const]) = match data {
                        #(#new_arms)*
                    };
                    let __on_disk = #on_disk {
                        #enum_header
                        #injected_init
                        __bstack_disc: __disc,
                        __bstack_payload: __payload,
                    };
                    let mut __slice = allocator.alloc(#enum_size)?;
                    let __data = __slice.as_range();
                    if let ::std::result::Result::Err(__e) =
                        __slice.write_range(0, ::bstack_raii::bytemuck::bytes_of(&__on_disk))
                    {
                        let _ = allocator.dealloc(__slice);
                        return ::std::result::Result::Err(__e);
                    }
                    #embed_post
                    #finish
                }
            }
        }
        Mode::RcWeak => {
            let ctrl_size = quote! {
                ::core::mem::size_of::<<Self as ::bstack_raii::BStackWeakable>::Control>() as u64
            };
            quote! {
                #vis fn new<'__e, __A: ::bstack_raii::BStackRaiiAllocator>(
                    allocator: &'__e __A,
                    data: #data_ty,
                ) -> ::std::io::Result<::bstack_raii::BStackRc<'__e, Self, __A>> {
                    #embed_decl
                    let (__disc, __payload): (#disc_ty, [u8; #payload_const]) = match data {
                        #(#new_arms)*
                    };
                    let __blocks = ::bstack_raii::BStackRaiiAllocator::alloc_many(allocator, &[#enum_size, #ctrl_size])?;
                    let __data = __blocks[0];
                    let __ctrl = __blocks[1];
                    let __on_disk = #on_disk {
                        #enum_header
                        __bstack_ctrl: __ctrl.start(),
                        __bstack_disc: __disc,
                        __bstack_payload: __payload,
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
                    #embed_post
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
    };
    let shared_impl = shared_impl(mode, name);
    // A plain enum is self-contained, so it may be `#[embed]`ded; an `rc` enum is not.
    let embeddable_impl = if mode == Mode::Plain {
        quote! {
            impl #enum_impl_g ::bstack_raii::__private::BStackEmbeddable for #name #enum_ty_g #enum_where {}
        }
    } else {
        quote!()
    };
    let weakable_items = weakable_items(mode, name, &control, vis);

    let allow_deprecated = input.attrs.iter().any(is_allow_deprecated);
    let ctrl_truncated = mode == Mode::RcWeak && ctrl_tag.truncated;
    let overlong_warning = if (tag.truncated || ctrl_truncated)
        && !(attr.allow_overlong || allow_deprecated)
    {
        let warn_fn = format_ident!("__bstack_tag_overlong_{}", name);
        let msg = format!(
            "#[bstack_enum] on `{type_name}`: a tag override longer than 8 bytes was truncated; \
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

    // `read` / `bstack_drop` only need the payload bytes when a variant carries one.
    let read_payload = if needs_payload {
        quote!(let __pl = __od.__bstack_payload;)
    } else {
        quote!()
    };
    // Body of `__bstack_drop_children(__range, allocator)`: free the active
    // variant's owned child (if any). `__range` is this block's range (its own,
    // or — when `#[embed]`ded — its slot in the parent).
    let drop_children_body = if drop_arms.is_empty() {
        quote!()
    } else {
        quote! {
            let __stack = allocator.stack();
            let mut __buf = [0u8; ::core::mem::size_of::<#on_disk>()];
            let __r = unsafe { ::bstack_raii::BStackRef::<Self>::from_range(__range) };
            let __od: #on_disk = *__r.read_on_disk(__stack, &mut __buf)?;
            let __disc = __od.__bstack_disc;
            let __pl = __od.__bstack_payload;
            match __disc {
                #(#drop_arms)*
                _ => {}
            }
        }
    };

    // Body of `__bstack_clone_into`: real deep clone for a plain enum, a runtime
    // error for `rc` / `rc, weak` (its injected refcount / control block would
    // need re-initialization). The whole OnDisk is byte-copied; the active
    // variant's payload is then fixed up (owned → deep clone + repoint, strong /
    // weak → refcount bump, ref → alias, embed → not yet supported).
    let (clone_children_body, clone_into_body) = if mode != Mode::Plain {
        let err = quote! {
            ::std::result::Result::Err(::std::io::Error::new(
                ::std::io::ErrorKind::Unsupported,
                "TryCloneIn: a reference-counted (`rc` / `rc, weak`) enum block is shared, \
                 not deep-cloned — duplicate its handle with `BStackRc::try_clone` \
                 (see the `TryClone` trait)",
            ))
        };
        (err.clone(), err)
    } else {
        let dispatch = if clone_arms.is_empty() {
            quote!()
        } else {
            quote! {
                let __disc = __od.__bstack_disc;
                let mut __pl = __od.__bstack_payload;
                match __disc {
                    #(#clone_arms)*
                    _ => {}
                }
                __od.__bstack_payload = __pl;
            }
        };
        let children = quote! {
            let __stack = allocator.stack();
            let __src = self.0;
            let mut __buf = [0u8; ::core::mem::size_of::<#on_disk>()];
            let __r = unsafe { ::bstack_raii::BStackRef::<Self>::from_range(__src) };
            #[allow(unused_mut)]
            let mut __od: #on_disk = *__r.read_on_disk(__stack, &mut __buf)?;
            #dispatch
            ::std::result::Result::Ok(__od)
        };
        let into = quote! {
            let __od = self.__bstack_clone_children_inplace(allocator, __plan)?;
            let __dst = __plan.alloc_raw(allocator, ::core::mem::size_of::<#on_disk>() as u64)?;
            __plan.write(__dst.start(), ::bstack_raii::bytemuck::bytes_of(&__od).to_vec());
            ::std::result::Result::Ok(__dst)
        };
        (children, into)
    };
    // The public `TryCloneIn` entry point, for plain enums only.
    let enum_clone_trait = if mode == Mode::Plain {
        quote! {
            impl #enum_impl_g ::bstack_raii::TryCloneIn for #name #enum_ty_g #enum_where {
                fn try_clone_in<__A: ::bstack_raii::BStackRaiiAllocator>(
                    &self,
                    allocator: &__A,
                ) -> ::std::io::Result<::bstack_raii::BStackOwned<Self>> {
                    use ::bstack_raii::BStackBlock as _;
                    // The clone strategy (single-pass intention-first, or two-pass
                    // atomic bulk on a `BStackBulkAllocator`) is chosen inside
                    // `run_clone`, which may run this descent twice (measure + build).
                    let __dst = ::bstack_raii::ClonePlan::run_clone(allocator, |__plan| {
                        self.__bstack_clone_into(allocator, __plan)
                    })?;
                    ::std::result::Result::Ok(unsafe {
                        ::bstack_raii::BStackOwned::from_raw(
                            <Self as ::bstack_raii::BStackBlock>::from_range(__dst),
                        )
                    })
                }
            }
        }
    } else {
        quote!()
    };

    // `EData` is generic over `<'e, A>` when a variant holds a strong/weak
    // reference; `EView` only when a weak variant makes `read` upgrade; both are
    // also generic over the enum's own type parameters.
    let data_generics = comp_decl(&quote!('__e), has_shared || has_foreign, has_shared);
    let view_generics = comp_decl(&quote!('__e), has_weak || has_foreign, has_weak);
    let view_ty = comp_ty(&view, &quote!('__e), has_weak || has_foreign, has_weak);
    // `bstack_move!` yields the same `EData` (owned handles); `Fields` just names
    // it with the move lifetime.
    let move_fields_ty = comp_ty(&data, &quote!('__mv), has_shared || has_foreign, has_shared);
    // `bstack_move!` frees the enum shell, then rebuilds the active variant's
    // payload as an owned handle.
    let move_payload = if needs_payload {
        quote!(let __pl = __od.__bstack_payload;)
    } else {
        quote!()
    };

    // Whole-value `#[bstack_mut]` on the enum itself: `set` when no variant owns
    // anything (a wholesale overwrite is safe), else `replace` (hand the old value
    // out). Both are one crash-atomic `set` of the same-size record. Only for a
    // plain enum (an `rc` / `rc, weak` block's injected refcount / control must not
    // be clobbered) and not with `#[embed]` variants (their post-write copy / re-home
    // makes in-place replacement a separate problem).
    let enum_mut_methods = if is_bstack_mut(&input.attrs) {
        if mode != Mode::Plain {
            return Err(Error::new_spanned(
                &input.ident,
                "#[bstack_mut] on a `#[bstack_enum]` is only supported for a plain enum — a \
                 shared (`rc` / `rc, weak`) enum's refcount / control block can't be \
                 overwritten in place; rebuild the value instead",
            ));
        }
        if enum_has_embed {
            return Err(Error::new_spanned(
                &input.ident,
                "#[bstack_mut] is not yet supported on a `#[bstack_enum]` with an `#[embed]` \
                 variant",
            ));
        }
        let build_image = quote! {
            let (__disc, __payload): (#disc_ty, [u8; #payload_const]) = match data {
                #(#new_arms)*
            };
            let __on_disk = #on_disk {
                #enum_header
                __bstack_disc: __disc,
                __bstack_payload: __payload,
            };
        };
        if drop_arms.is_empty() {
            // No variant owns anything (pure POD / ref / foreign-ref): a wholesale
            // overwrite frees nothing and needs no hand-back.
            quote! {
                /// Overwrite the whole enum value (variant + payload) in place, as
                /// one crash-atomic `set`. Available because no variant owns
                /// anything, so nothing is stranded.
                #[allow(unused_variables)]
                #vis fn set<'__e, __A: ::bstack_raii::BStackRaiiAllocator>(
                    &self,
                    allocator: &'__e __A,
                    data: #data_ty,
                ) -> ::std::io::Result<()> {
                    #build_image
                    allocator
                        .stack()
                        .set(self.0.start(), ::bstack_raii::bytemuck::bytes_of(&__on_disk))
                }
            }
        } else {
            // Some variant owns children: install the new value and move the old
            // one out (`bstack_move!`'s per-variant reconstruction), upholding the
            // `ReplaceError` "never lose the new value" contract.
            quote! {
                /// Replace the whole enum value (variant + payload), moving the old
                /// value out. One crash-atomic `set`; on I/O failure the *new* value
                /// is handed back through [`ReplaceError`](::bstack_raii::ReplaceError).
                #[allow(unused_variables)]
                #vis fn replace<'__e, __A: ::bstack_raii::BStackRaiiAllocator>(
                    &self,
                    allocator: &'__e __A,
                    data: #data_ty,
                ) -> ::core::result::Result<#data_ty, ::bstack_raii::ReplaceError<#data_ty>> {
                    let __alloc = allocator;
                    // Reconstruct an owned `EData` from a (disc, payload) pair — the
                    // same per-variant logic as `bstack_move!`, transferring children
                    // out (nothing freed).
                    let __reconstruct = |__disc: #disc_ty, __pl: [u8; #payload_const]|
                        -> ::std::io::Result<#data_ty>
                    {
                        ::std::result::Result::Ok(match __disc {
                            #(#move_arms)*
                            _ => return ::std::result::Result::Err(::std::io::Error::new(
                                ::std::io::ErrorKind::InvalidData,
                                "bstack_enum: invalid discriminant",
                            )),
                        })
                    };
                    // 1. Read the old disc + payload before consuming `data`.
                    let mut __buf = [0u8; ::core::mem::size_of::<#on_disk>()];
                    let __r = unsafe { ::bstack_raii::BStackRef::<Self>::from_range(self.0) };
                    let (__old_disc, __old_pl) =
                        match __r.read_on_disk(allocator.stack(), &mut __buf) {
                            ::std::result::Result::Ok(__od) =>
                                (__od.__bstack_disc, __od.__bstack_payload),
                            ::std::result::Result::Err(__e) =>
                                return ::core::result::Result::Err(
                                    ::bstack_raii::ReplaceError::recovered(__e, data)),
                        };
                    // 2. Consume the new value into its on-disk image.
                    #build_image
                    // 3. Commit (single atomic write of the same-size record).
                    if let ::std::result::Result::Err(__e) = allocator
                        .stack()
                        .set(self.0.start(), ::bstack_raii::bytemuck::bytes_of(&__on_disk))
                    {
                        return ::core::result::Result::Err(
                            match __reconstruct(__disc, __payload) {
                                ::std::result::Result::Ok(__hb) =>
                                    ::bstack_raii::ReplaceError::recovered(__e, __hb),
                                ::std::result::Result::Err(_) =>
                                    ::bstack_raii::ReplaceError::lost(__e),
                            },
                        );
                    }
                    // 4. Reconstruct + return the old value (now overwritten on disk).
                    match __reconstruct(__old_disc, __old_pl) {
                        ::std::result::Result::Ok(__old) => ::core::result::Result::Ok(__old),
                        ::std::result::Result::Err(__e) =>
                            ::core::result::Result::Err(::bstack_raii::ReplaceError::lost(__e)),
                    }
                }
            }
        }
    } else {
        quote!()
    };

    let enum_handle_def = if type_params.is_empty() {
        quote! {
            #[derive(::core::clone::Clone, ::core::marker::Copy)]
            #vis struct #name(::bstack_raii::BStackRange);
        }
    } else {
        quote! {
            #vis struct #name #enum_decl_g(
                ::bstack_raii::BStackRange #enum_phantom_field
            ) #enum_decl_where;
            impl #enum_decl_g ::core::clone::Clone for #name #enum_decl_ty_g #enum_decl_where {
                fn clone(&self) -> Self { *self }
            }
            impl #enum_decl_g ::core::marker::Copy for #name #enum_decl_ty_g #enum_decl_where {}
        }
    };
    // The payload area size const (max over all variants), folded at const-eval.
    let payload_const_def = layout::payload_const_def(vis, &payload_const, &payload_sizes);
    Ok(quote! {
        #enum_handle_def

        #payload_const_def

        impl #enum_impl_g #name #enum_ty_g #enum_where {
            /// The payload area size (bytes) — the max over all variants.
            #[doc(hidden)]
            pub const __PAYLOAD: usize = #payload_const;
        }

        #[repr(C, packed)]
        #[derive(::core::clone::Clone, ::core::marker::Copy)]
        #vis struct #on_disk {
            __bstack_header: ::bstack_raii::BlockHeader,
            #injected_ondisk
            __bstack_disc: #disc_ty,
            __bstack_payload: [u8; #payload_const],
        }
        unsafe impl ::bstack_raii::Zeroable for #on_disk {}
        unsafe impl ::bstack_raii::Pod for #on_disk {}

        const _: fn() = || {
            fn __assert_pod<__T: ::bstack_raii::Pod>() {}
            #( __assert_pod::<#pod_types>(); )*
        };

        const _: fn() = || {
            fn __assert_embeddable<__T: ::bstack_raii::__private::BStackEmbeddable>() {}
            #( __assert_embeddable::<#embed_types>(); )*
        };

        /// The in-memory owned form of the enum's payload — POD by value and
        /// each child/reference as an owned handle (owned → `BStackOwned`,
        /// strong → `BStackRc`, weak → `BStackWeak`, ref → `BStackRef`).
        ///
        /// The *same* type is passed to [`new`](#name::new) to construct a variant
        /// and returned by `bstack_move!` to destructure one (they are duals).
        #vis enum #data #data_generics {
            #(#data_variants)*
        }

        /// The result of [`read`](#name::read): the current variant, with POD
        /// values by value, owned/ref children as borrowed handles, and a weak
        /// variant upgraded to `Option<BStackRc>`.
        #vis enum #view #view_generics {
            #(#view_variants)*
        }

        impl #enum_impl_g ::bstack_raii::BStackCast for #name #enum_ty_g #enum_where {
            fn eightcc() -> ::bstack_raii::EightCC {
                #eightcc
            }
        }

        impl #enum_impl_g ::bstack_raii::BStackBlock for #name #enum_ty_g #enum_where {
            type OnDisk = #on_disk;
            fn from_range(range: ::bstack_raii::BStackRange) -> Self {
                #name(range #enum_phantom_ctor)
            }
            fn range(&self) -> ::bstack_raii::BStackRange {
                self.0
            }

            /// Read this enum's OnDisk and return a deep-cloned copy: the active
            /// variant's payload fixed up (owned child cloned into `__plan`,
            /// strong/weak bumped, ref aliased, embedded child folded in place),
            /// without allocating a block for `self`. Overrides the childless
            /// `BStackBlock` default. Used to fold an `#[embed]`ded enum inline,
            /// and by `__bstack_clone_into`.
            #[doc(hidden)]
            #[allow(unused_variables, unused_imports)]
            fn __bstack_clone_children_inplace<__A: ::bstack_raii::BStackRaiiAllocator>(
                &self,
                allocator: &__A,
                __plan: &mut ::bstack_raii::ClonePlan,
            ) -> ::std::io::Result<#on_disk> {
                use ::bstack_raii::BStackBlock as _;
                #clone_children_body
            }

            /// Deep-clone this enum into a `ClonePlan`: allocate a fresh block and
            /// stage its fixed-up payload. Returns the new block's range. Also lets
            /// an owned enum child of a struct be recursed into.
            #[doc(hidden)]
            #[allow(unused_variables)]
            fn __bstack_clone_into<__A: ::bstack_raii::BStackRaiiAllocator>(
                &self,
                allocator: &__A,
                __plan: &mut ::bstack_raii::ClonePlan,
            ) -> ::std::io::Result<::bstack_raii::BStackRange> {
                #clone_into_body
            }

            /// Free the active variant's owned child (recursively) given this
            /// block's range, **without** freeing the block itself — used when the
            /// enum is `#[embed]`ded, and by `bstack_drop`. Overrides the childless
            /// `BStackBlock` default.
            #[doc(hidden)]
            #[allow(unused_imports)]
            fn __bstack_drop_children<__A: ::bstack_raii::BStackRaiiAllocator>(
                __range: ::bstack_raii::BStackRange,
                allocator: &__A,
            ) -> ::std::io::Result<()> {
                use ::bstack_raii::BStackDrop as _;
                use ::bstack_raii::BStackBlock as _;
                #drop_children_body
                ::std::result::Result::Ok(())
            }
        }

        impl #enum_impl_g ::bstack_raii::BStackDrop for #name #enum_ty_g #enum_where {
            fn bstack_drop<__A: ::bstack_raii::BStackRaiiAllocator>(
                self,
                allocator: &__A,
            ) -> ::std::io::Result<()> {
                <Self as ::bstack_raii::BStackBlock>::__bstack_drop_children(self.0, allocator)?;
                unsafe { ::bstack_raii::dealloc_range(allocator, self.0) }
            }
        }

        impl #enum_impl_g #name #enum_ty_g #enum_where {
            /// Allocate a new enum block holding `data`'s variant + payload.
            #enum_new

            /// Read the current variant. Takes the allocator (a weak variant's
            /// read upgrades through it; other variants just read the block).
            #vis fn read<'__e, __A: ::bstack_raii::BStackRaiiAllocator>(
                &self,
                allocator: &'__e __A,
            ) -> ::std::io::Result<#view_ty> {
                let mut __buf = [0u8; ::core::mem::size_of::<#on_disk>()];
                let __r = unsafe { ::bstack_raii::BStackRef::<Self>::from_range(self.0) };
                let __od: #on_disk = *__r.read_on_disk(allocator.stack(), &mut __buf)?;
                let __disc = __od.__bstack_disc;
                #read_payload
                ::std::result::Result::Ok(match __disc {
                    #(#read_arms)*
                    _ => {
                        return ::std::result::Result::Err(::std::io::Error::new(
                            ::std::io::ErrorKind::InvalidData,
                            "bstack_enum: invalid discriminant",
                        ));
                    }
                })
            }

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

            #enum_mut_methods
        }

        #shared_impl
        #embeddable_impl
        #weakable_items

        impl #enum_impl_g ::bstack_raii::BStackMove for #name #enum_ty_g #enum_where {
            type Fields<'__mv, __A: ::bstack_raii::BStackRaiiAllocator> = #move_fields_ty;
            fn bstack_move<'__mv, __A: ::bstack_raii::BStackRaiiAllocator>(
                owned: ::bstack_raii::BStackOwned<Self>,
                __alloc: &'__mv __A,
            ) -> ::std::io::Result<Self::Fields<'__mv, __A>> {
                let __inner = owned.into_inner();
                let __range = ::bstack_raii::BStackBlock::range(&__inner);
                let mut __buf = [0u8; ::core::mem::size_of::<#on_disk>()];
                let __r = unsafe { ::bstack_raii::BStackRef::<Self>::from_range(__range) };
                let __od: #on_disk = *__r.read_on_disk(__alloc.stack(), &mut __buf)?;
                let __disc = __od.__bstack_disc;
                #move_payload
                let __result = match __disc {
                    #(#move_arms)*
                    _ => {
                        return ::std::result::Result::Err(::std::io::Error::new(
                            ::std::io::ErrorKind::InvalidData,
                            "bstack_enum: invalid discriminant",
                        ));
                    }
                };
                // Free the enum shell only; the moved-out payload stays live.
                unsafe { ::bstack_raii::dealloc_range(__alloc, __range)?; }
                ::std::result::Result::Ok(__result)
            }
        }

        #enum_clone_trait
        #overlong_warning
    })
}
