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
use syn::{Error, Fields, Ident, ItemStruct, Type};

use crate::common::*;

pub fn expand(attr: TokenStream, input: ItemStruct) -> syn::Result<TokenStream> {
    let attr = parse_attr(attr)?;
    let mode = attr.mode;

    if attr.repr.is_some() {
        return Err(Error::new(
            Span::call_site(),
            "`repr(..)` selects an enum discriminant width; it is only for #[bstack_enum]",
        ));
    }

    // A generic block is supported only in the **layout-preserving** case: every
    // type parameter must be used ONLY in `#[bstack_ref]` fields (a bare `u64`
    // offset on disk), so `XOnDisk` stays independent of the parameters and
    // teardown/clone need no recursion into them. The per-field check is below,
    // once the fields are parsed; here we gate the coarse constraints.
    let type_params: Vec<&Ident> = input.generics.type_params().map(|tp| &tp.ident).collect();
    // Const parameters `const N: usize` are supported as array lengths (`[T; N]`);
    // a direct const-param length is legal on stable, unlike an arbitrary const
    // expression. Lifetimes are still rejected.
    let const_params: Vec<&Ident> = input.generics.const_params().map(|cp| &cp.ident).collect();
    if !input.generics.params.is_empty() {
        for p in &input.generics.params {
            if matches!(p, syn::GenericParam::Lifetime(_)) {
                return Err(Error::new_spanned(
                    p,
                    "a generic #[bstack_block] currently supports type and const parameters, \
                     not lifetimes",
                ));
            }
        }
        if mode != Mode::Plain {
            return Err(Error::new_spanned(
                &input.generics,
                "a generic #[bstack_block] currently supports plain mode only (not `rc` / \
                 `rc, weak`)",
            ));
        }
    }

    // Normalize fields to `(name, field)`: named fields keep their name, a tuple
    // struct's positional fields get synthetic `field0` / `field1` / … names (so
    // they access as `x.field0(stack)` and reuse the whole field machinery), and a
    // unit struct has none — yielding a valid **header-only** block.
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

    let name = &input.ident;
    let vis = &input.vis;
    let on_disk = format_ident!("{}OnDisk", name);
    let control = format_ident!("{}OnDiskRef", name);

    // Per-parameter usage across fields — driving both the trait bound and whether
    // the parameter is stored INLINE (making `XOnDisk`, and its `size_of` /
    // `offset_of`, depend on it). A parameter is either a **POD** value (`T: Pod`,
    // stored by value) or a **block reference / embed** (`T: BStackBlock`, plus
    // `BStackShared` / `BStackWeakable` for strong / weak elements). `ref` / `owned`
    // / `strong` / `weak` lower to a bare `u64` offset (not in `XOnDisk`); `#[embed]`
    // and POD store the type inline (in `XOnDisk`).
    #[derive(Default)]
    struct Usage {
        pod: bool,
        blockish: bool,
        strong: bool,
        weak: bool,
        in_ondisk: bool,
        /// The parameter is the target of a `#[bstack_owned] Foreign<T>` (scalar or in
        /// a container). Unlike a plain owned child (cloned via `__bstack_clone_into`,
        /// needing only `BStackBlock`), an owned foreign deep-clone runs a self-
        /// contained `try_clone_in` on the target's file, so it needs `TryCloneIn`.
        foreign_owned: bool,
        /// The parameter is the target of *any* `Foreign<T>` (any kind). `Foreign<'a, T>`
        /// requires `T: 'static`, so such a parameter needs the `'static` bound even
        /// though a foreign target is never stored inline (so `in_ondisk` is not set).
        foreign: bool,
        /// The parameter is `#[embed]`ded (inlined). `#[embed]` targets must be
        /// self-contained (`BStackEmbeddable`) — never `(rc)` / `(rc, weak)`.
        embed: bool,
    }
    let mut usage: Vec<(Ident, Usage)> = type_params
        .iter()
        .map(|p| ((*p).clone(), Usage::default()))
        .collect();
    for (_, field) in &field_list {
        let kind = classify(field)?;
        if !type_mentions_any(&field.ty, &type_params) {
            continue;
        }
        // A foreign field lowers to a `ForeignPtr` (its target `T` is never stored
        // inline), so a target parameter is a *block reference*, not a POD/embed —
        // regardless of the container it sits in. Detect every foreign target up front
        // so the bounds are `BStackBlock` (+ `TryCloneIn` for owned, and the usual
        // strong/weak) rather than the `Pod`/`in_ondisk` a bare field would imply.
        let ftargets = foreign_targets_in(&field.ty);
        for (p, u) in usage.iter_mut() {
            if !type_mentions_any(&field.ty, &[&*p]) {
                continue;
            }
            if ftargets.iter().any(|t| type_mentions_any(t, &[&*p])) {
                // The parameter is a foreign *target*: a block reference in its own
                // file. Kind names the ownership of that target.
                u.blockish = true;
                u.foreign = true;
                match kind {
                    Kind::Owned => u.foreign_owned = true,
                    Kind::Strong => u.strong = true,
                    Kind::Weak => u.weak = true,
                    _ => {}
                }
                continue;
            }
            // The param is in this field but NOT as a foreign target. If the field
            // *also* holds a `Foreign`, the param sits in a non-foreign position of it
            // (e.g. a POD element of a foreign tuple), which the per-field lowering
            // can't classify generically — require concrete types there.
            if !ftargets.is_empty() {
                return Err(Error::new_spanned(
                    &field.ty,
                    "a generic type parameter in a non-`Foreign` position of a field that also \
                     holds a `Foreign` is not supported; use concrete types for the non-foreign parts",
                ));
            }
            match kind {
                Kind::Pod => {
                    u.pod = true;
                    u.in_ondisk = true;
                }
                Kind::Embed => {
                    u.blockish = true;
                    u.in_ondisk = true;
                    u.embed = true;
                }
                Kind::Ref | Kind::Owned => u.blockish = true,
                Kind::Strong => {
                    u.blockish = true;
                    u.strong = true;
                }
                Kind::Weak => {
                    u.blockish = true;
                    u.weak = true;
                }
            }
        }
    }
    for (p, u) in &usage {
        if u.pod && u.blockish {
            return Err(Error::new_spanned(
                p,
                "a generic type parameter cannot be used both as a POD field and as a \
                 reference / embed field — a `Pod` value and a `#[bstack_block]` reference are \
                 different kinds of thing, with incompatible bounds",
            ));
        }
    }
    // Generics threaded into the generated impls (with the computed bounds), plus
    // the handle's phantom marker over them. `impl_g`/`ty_g`/`where_g` carry the
    // bounds (for the bstack trait impls); `decl_g`/`decl_ty_g`/`decl_where` are
    // the user's own (for the handle type + its `Clone`/`Copy`, which hold
    // regardless of `T`).
    let mut aug_generics = input.generics.clone();
    for tp in aug_generics.type_params_mut() {
        let u = usage.iter().find(|(p, _)| *p == tp.ident).map(|(_, u)| u);
        if u.is_some_and(|u| u.pod) {
            tp.bounds.push(syn::parse_quote!(::bstack_raii::Pod));
        } else {
            tp.bounds
                .push(syn::parse_quote!(::bstack_raii::BStackBlock));
            if let Some(u) = u {
                if u.strong {
                    tp.bounds
                        .push(syn::parse_quote!(::bstack_raii::BStackShared));
                }
                if u.weak {
                    tp.bounds
                        .push(syn::parse_quote!(::bstack_raii::BStackWeakable));
                }
                if u.foreign_owned {
                    // An owned foreign target is deep-cloned via its own `try_clone_in`.
                    tp.bounds.push(syn::parse_quote!(::bstack_raii::TryCloneIn));
                }
                if u.embed {
                    // An `#[embed]`ded param must be a plain, self-contained block —
                    // never `(rc)` / `(rc, weak)` (its control block would be stranded).
                    tp.bounds.push(syn::parse_quote!(
                        ::bstack_raii::__private::BStackEmbeddable
                    ));
                }
            }
        }
        // A parameter stored inline makes `XOnDisk: Pod` depend on it, and
        // `bytemuck::Pod` requires `'static`. A stored parameter is a `Pod` value or
        // a block handle (a `BStackRange` newtype) — both `'static` — so the block's
        // own impls need the bound too, to use `Self::OnDisk: Pod`.
        if u.is_some_and(|u| u.in_ondisk || u.foreign) {
            tp.bounds.push(syn::parse_quote!('static));
        }
    }
    let (impl_g, ty_g, where_g) = aug_generics.split_for_impl();
    let (decl_g, decl_ty_g, decl_where) = input.generics.split_for_impl();

    // `XOnDisk` is generic over exactly the parameters stored inline (embed / POD).
    // For a block with none (ref/owned/strong/weak-only, or non-generic), it stays
    // a plain non-generic struct and `on_disk_ty` is just its name.
    let ondisk_idents: Vec<Ident> = usage
        .iter()
        .filter(|(_, u)| u.in_ondisk)
        .map(|(p, _)| p.clone())
        .collect();
    // A const parameter appears only as an array length (`[T; N]`), which always
    // sizes the `OnDisk`, so any const parameter used in a field is an `OnDisk`
    // parameter.
    let mut ondisk_const_idents: Vec<Ident> = Vec::new();
    for cp in &const_params {
        if field_list
            .iter()
            .any(|(_, f)| type_mentions_any(&f.ty, &[*cp]))
        {
            ondisk_const_idents.push((*cp).clone());
        }
    }
    let ondisk_empty = ondisk_idents.is_empty() && ondisk_const_idents.is_empty();
    let ondisk_generics: syn::Generics = {
        let mut g = syn::Generics::default();
        // Preserve declaration order (Rust requires types before consts). Inherits
        // the `Pod`/`BStackBlock` + `'static` bounds from `aug_generics`.
        for p in &aug_generics.params {
            let keep = match p {
                syn::GenericParam::Type(tp) => ondisk_idents.contains(&tp.ident),
                syn::GenericParam::Const(cp) => ondisk_const_idents.contains(&cp.ident),
                syn::GenericParam::Lifetime(_) => false,
            };
            if keep {
                g.params.push(p.clone());
            }
        }
        g
    };
    let (od_impl_g, od_ty_g, od_where) = ondisk_generics.split_for_impl();
    let on_disk_ty = if ondisk_empty {
        quote!(#on_disk)
    } else {
        quote!(#on_disk #od_ty_g)
    };
    // For a struct *literal* `XOnDisk { .. }`: bare when non-generic (or when the
    // fields determine the parameters, as for a POD field), but an `#[embed]`
    // field is `<T>::OnDisk`, which does NOT determine `T` — so use a turbofish
    // `XOnDisk::<T, N> { .. }` whenever generic.
    let on_disk_ctor = if ondisk_empty {
        quote!(#on_disk)
    } else {
        quote!(#on_disk::#od_ty_g)
    };
    let (phantom_field, phantom_ctor): (TokenStream, TokenStream) =
        if type_params.is_empty() && const_params.is_empty() {
            (quote!(), quote!())
        } else {
            // Const parameters are held via `[(); N]` so they count as "used".
            let const_markers = const_params.iter().map(|c| quote!([(); #c]));
            (
                quote!(, ::core::marker::PhantomData<
                fn() -> (#(#type_params,)* #(#const_markers,)*)>),
                quote!(, ::core::marker::PhantomData),
            )
        };

    // On-disk fields: header, then the injected refcount/ctrl (if any), then user
    // fields lowered per annotation.
    let mut on_disk_fields = Vec::new();
    match mode {
        Mode::Plain => {}
        Mode::Rc => on_disk_fields.push(quote!(__bstack_refcount: u64,)),
        Mode::RcWeak => on_disk_fields.push(quote!(__bstack_ctrl: u64,)),
    }

    let mut drop_stmts = Vec::new();
    // `TryCloneIn` deep-clone statements for user fields, mirroring `drop_stmts`
    // in reverse (owned → recurse, strong/weak → refcount bump, embed → fold
    // inline, vec → per-element; POD / ref are byte-copied so emit nothing).
    let mut clone_stmts = Vec::new();
    let mut pod_types: Vec<&Type> = Vec::new();
    let mut accessors = Vec::new();
    let mut setters = Vec::new();
    let mut ctor_params = Vec::new();
    let mut ctor_preps = Vec::new();
    let mut ctor_inits = Vec::new();
    // Post-write construction steps (`#[embed]` `BStack::copy`s the child into its
    // inline slot after the block's OnDisk is written).
    let mut ctor_post: Vec<TokenStream> = Vec::new();
    // `bstack_move!` support (owned/ref/pod fields only, plain blocks only).
    let mut mv_caps = Vec::new();
    let mut mv_types = Vec::new();
    let mut mv_recon = Vec::new();
    // Generated `#[repr(C, packed)]` Pod wrappers for POD tuple fields.
    let mut wrapper_defs = Vec::new();
    // Whether any field was written `&T` (coerced to owned `T`, with a warning).
    let mut ref_coerced = false;

    for (fname, field) in &field_list {
        let kind = classify(field)?;
        // The public accessor name (`get_<field>`); `#fname` itself stays the
        // on-disk field / struct-literal name throughout.
        let getter = format_ident!("get_{}", fname);

        // Ergonomic: `&T` is coerced to owned `T` (and `&str` to `String`), with
        // a warning. `eff_ty` is the type after stripping a leading `&`.
        let eff_ty: &Type = match &field.ty {
            Type::Reference(r) => &r.elem,
            other => other,
        };
        if matches!(&field.ty, Type::Reference(_)) {
            ref_coerced = true;
        }

        // Peek through `Option<Inner>` (which makes a *reference* field nullable,
        // `0` on disk == `None`) so the inner type — which may itself be a `Vec` /
        // `String` — is what we classify. A POD field keeps the whole type (below).
        let (opt_inner, nullable) = match option_inner(eff_ty) {
            Some(inner) => (inner, true),
            None => (eff_ty, false),
        };

        // Reject unsupported `Vec` / `Option` nesting (`Vec<Vec<T>>`,
        // `Option<Option<T>>`, and every mix like `Vec<Option<Vec<T>>>`) with a
        // directed error, scanning outermost-first so the message names the first
        // offending construct. Valid mixes (`Option<Vec<Option<T>>>`, …) pass.
        check_container_nesting(eff_ty)?;

        // `Foreign` is supported only in a handful of shapes — a scalar `Foreign<T>` /
        // `Option<Foreign<T>>`, a `Vec<Foreign<T>>` / `Vec<Option<Foreign<T>>>`, or a
        // `[Foreign<T>; N]` (nested / per-element `Option`). If it appears anywhere
        // else (inside a tuple, a POD aggregate, a `Vec` of tuples, …) `field_foreign_target`
        // can't reach it, yet the type still mentions it — reject with a directed message
        // rather than leaking a bare, unresolved `Foreign` type into the output.
        if tokens_mention(quote!(#eff_ty), &[&format_ident!("Foreign")])
            && field_foreign_target(eff_ty).is_none()
        {
            return Err(Error::new_spanned(
                &field.ty,
                "`Foreign` is nested in an unsupported position (e.g. inside a tuple or another \
                 POD aggregate). It is supported as a scalar `Foreign<T>` / `Option<Foreign<T>>`, \
                 a `Vec<Foreign<T>>` / `Vec<Option<Foreign<T>>>`, or a `[Foreign<T>; N]` — \
                 anywhere else, wrap the `Foreign` inside a `#[bstack_block]` struct and use that.",
            ));
        }

        // `#[bstack_mut]` is honored on scalar POD / block references (`set_` /
        // `replace_`), POD tuples (`set_`), block-reference arrays (element
        // `replace_<f>_at` + whole-array `replace_<f>`, plus `set_` for `ref`), and a
        // scalar `Foreign<T>` / `Option<Foreign<T>>` (`replace_`, plus `set_` for a
        // foreign `ref`). A `Vec` is *always* mutable in place through its `get_<f>()`
        // handle (which writes its descriptor back), so the annotation is a redundant
        // no-op there. A `Foreign` in a *container / tuple* (`Vec<Foreign>`,
        // `[Foreign; N]`, a foreign tuple) has no mutator yet — reject it rather than
        // silently ignoring it.
        if is_bstack_mut(&field.attrs)
            && tokens_mention(quote!(#eff_ty), &[&format_ident!("Foreign")])
            && foreign_inner(opt_inner).is_none()
        {
            return Err(Error::new_spanned(
                field,
                "#[bstack_mut] is not yet supported on `Foreign` inside a container or \
                 tuple (`Vec<Foreign>`, `[Foreign; N]`, a foreign tuple) — only a scalar \
                 `Foreign<T>` / `Option<Foreign<T>>` field is mutable",
            ));
        }

        // `Foreign<T>`: a cross-file wide pointer, stored inline as a 16-byte
        // `ForeignPtr` `(file_id, offset)` and resolved through the registry (length
        // recovered from `size_of::<T::OnDisk>()`, so it is not stored). The field's
        // annotation (`#[bstack_owned/strong/weak/ref]`, or none) selects the
        // target's ownership *in its own file*. `#[embed]` is meaningless for a
        // pointer and rejected. Nullable via `Option<Foreign<T>>` (`offset == 0`
        // niche). Cross-file teardown (free/decrement/release the target on the
        // other side) and deep clone are DEFERRED — the field is byte-copied on
        // clone (an alias) and freed by nobody on teardown, whatever the annotation;
        // the annotation is recorded for the eventual per-kind dispatch.
        if let Some(ftarget) = foreign_inner(opt_inner) {
            // A `Foreign` points at a *block* in another file: it must carry an
            // ownership annotation, and its target must be a bstack block — not a
            // pointer, a container, or a tuple (see `validate_foreign_target`).
            // Nullable at the *field* level via `Option<Foreign<T>>` (handled below).
            validate_foreign_target(
                kind,
                ftarget,
                &field.ty,
                "`Foreign<T>`",
                format_ident!("__bstack_foreign_target_{}", fname),
                !type_mentions_any(ftarget, &type_params),
                &mut wrapper_defs,
            )?;
            on_disk_fields.push(quote!(#fname: ::bstack_raii::ForeignRepr,));
            let field_ty = quote!(::bstack_raii::Foreign<#ftarget>);
            let cap = format_ident!("__cap_{}", fname);
            mv_caps.push(quote!(let #cap = __od.#fname;));

            // `bstack_move!` hands an owning foreign field back as its RAII dual
            // (`ForeignOwned` / `ForeignRc` / `ForeignWeak`, each with `bstack_drop` +
            // `into_foreign`); a `#[bstack_ref]` yields a plain `Foreign` (owns nothing).
            let (mv_leaf_ty, mv_leaf_expr) = match kind {
                Kind::Owned => (
                    quote!(::bstack_raii::ForeignOwned<'__mv, #ftarget>),
                    quote!(unsafe {
                        ::bstack_raii::ForeignOwned::from_foreign(
                            ::bstack_raii::Foreign::from_repr(#cap))
                    }),
                ),
                Kind::Strong => (
                    quote!(::bstack_raii::ForeignRc<'__mv, #ftarget>),
                    quote!(unsafe {
                        ::bstack_raii::ForeignRc::from_foreign(
                            ::bstack_raii::Foreign::from_repr(#cap))
                    }),
                ),
                Kind::Weak => (
                    quote!(::bstack_raii::ForeignWeak<'__mv, #ftarget>),
                    quote!(unsafe {
                        ::bstack_raii::ForeignWeak::from_foreign(
                            ::bstack_raii::Foreign::from_repr(#cap))
                    }),
                ),
                _ => (
                    quote!(::bstack_raii::Foreign<'__mv, #ftarget>),
                    quote!(unsafe { ::bstack_raii::Foreign::from_repr(#cap) }),
                ),
            };

            if nullable {
                // Niche: a stored `offset == 0` is `None` (no target sits at 0).
                accessors.push(quote! {
                    #vis fn #getter<'__f>(
                        &self,
                        stack: &'__f ::bstack_raii::BStack,
                    ) -> ::std::io::Result<
                        ::core::option::Option<::bstack_raii::Foreign<'__f, #ftarget>>,
                    > {
                        let mut __buf = ::std::vec![0u8; ::core::mem::size_of::<#on_disk_ty>()];
                        let __r = unsafe { ::bstack_raii::BStackRef::<Self>::from_range(self.0) };
                        let __od: #on_disk_ty = *__r.read_on_disk(stack, &mut __buf)?;
                        let __p = __od.#fname;
                        ::std::result::Result::Ok(if __p.offset() == 0 {
                            ::core::option::Option::None
                        } else {
                            // SAFETY: `__p` was stored into this file; the returned
                            // `Foreign`'s lifetime is bound to `stack` by the signature.
                            ::core::option::Option::Some(unsafe {
                                ::bstack_raii::Foreign::from_repr(__p)
                            })
                        })
                    }
                });
                ctor_params.push(quote!(#fname: ::core::option::Option<#field_ty>,));
                ctor_preps.push(quote! {
                    let #fname: ::bstack_raii::ForeignRepr = match #fname {
                        ::core::option::Option::Some(__f) => __f.repr(),
                        ::core::option::Option::None => ::bstack_raii::ForeignRepr::new(0, 0),
                    };
                });
                ctor_inits.push(quote!(#fname: #fname,));
                mv_types.push(quote!(::core::option::Option<#mv_leaf_ty>));
                mv_recon.push(quote! {
                    if #cap.offset() == 0 {
                        ::core::option::Option::None
                    } else {
                        // SAFETY: `#cap` was stored into this file; the handle is bound
                        // to `'__mv` and owns the target per the field annotation.
                        ::core::option::Option::Some(#mv_leaf_expr)
                    }
                });
            } else {
                accessors.push(quote! {
                    #vis fn #getter<'__f>(
                        &self,
                        stack: &'__f ::bstack_raii::BStack,
                    ) -> ::std::io::Result<::bstack_raii::Foreign<'__f, #ftarget>> {
                        let mut __buf = ::std::vec![0u8; ::core::mem::size_of::<#on_disk_ty>()];
                        let __r = unsafe { ::bstack_raii::BStackRef::<Self>::from_range(self.0) };
                        let __od: #on_disk_ty = *__r.read_on_disk(stack, &mut __buf)?;
                        // SAFETY: stored into this file; bound to `stack` by the signature.
                        ::std::result::Result::Ok(unsafe {
                            ::bstack_raii::Foreign::from_repr(__od.#fname)
                        })
                    }
                });
                ctor_params.push(quote!(#fname: #field_ty,));
                ctor_preps.push(quote!(let #fname: ::bstack_raii::ForeignRepr = #fname.repr();));
                ctor_inits.push(quote!(#fname: #fname,));
                mv_types.push(quote!(#mv_leaf_ty));
                mv_recon.push(quote!(#mv_leaf_expr));
            }

            // Teardown: an owning foreign pointer frees / decrements / releases its
            // target *in the target's own file*. The kind picks a helper; all run the
            // ordinary generic teardown against whichever allocator addresses the
            // target — the local `allocator` for a `SELF` pointer, or a
            // `ForeignHostAllocator` (over the live host) for a cross-file one. Frees
            // are tagged (via `wal_file_id`) with the target's file so the home WAL
            // reclaims them there. `#[bstack_ref]` owns nothing → no teardown.
            let foreign_drop_helper = match kind {
                Kind::Owned => Some(quote!(::bstack_raii::__private::foreign_drop_owned)),
                Kind::Strong => Some(quote!(::bstack_raii::__private::foreign_drop_strong)),
                Kind::Weak => Some(quote!(::bstack_raii::__private::foreign_drop_weak)),
                // Ref: non-owning. Pod / Embed: already rejected above.
                Kind::Ref | Kind::Pod | Kind::Embed => None,
            };
            if let Some(helper) = foreign_drop_helper {
                drop_stmts.push(quote! {
                    {
                        let __fp: ::bstack_raii::ForeignRepr = __on_disk.#fname;
                        // A `0` offset is the null / unset niche (nullable field, or a
                        // never-set pointer) — nothing to free.
                        let __off = __fp.offset();
                        if __off != 0 {
                            let __fid = __fp.file_id();
                            if __fid == 0 {
                                // `SELF`: the target is in this same file.
                                unsafe { #helper::<#ftarget, _>(allocator, __off)?; }
                            } else if let ::core::option::Option::Some(__id) =
                                ::bstack_raii::registry::FileId::from_u64(__fid)
                            {
                                // Foreign: adapt the live host to an allocator and run
                                // the same teardown against the other file. If that
                                // file isn't currently attached, the target is
                                // unreachable and leaks (permitted).
                                if let ::core::option::Option::Some(__host) =
                                    ::bstack_raii::registry::host_arc(__id)
                                {
                                    let __adapter =
                                        ::bstack_raii::ForeignHostAllocator::new(__host, __id);
                                    unsafe { #helper::<#ftarget, _>(&__adapter, __off)?; }
                                }
                            }
                            // A malformed id (does not fit the `FileId` space) is
                            // unreachable → leak (skip), not an error.
                        }
                    }
                });
            }

            // Deep clone: per-kind, acting on the target *in its own file*. `owned`
            // deep-copies the target (a fresh block, the pointer repointed); `strong`
            // / `weak` share it and bump its count; `ref` aliases (byte-copied — no
            // clone_stmt). A `SELF` pointer folds into the *home* plan (atomic with the
            // home commit); a foreign one acts eagerly via the adapter (best-effort,
            // over-provisioning ⇒ leak, never under ⇒ double-free). A detached target
            // file makes the clone error (aliasing an owner would double-free later).
            let target_od_size = quote! {
                ::core::mem::size_of::<<#ftarget as ::bstack_raii::BStackBlock>::OnDisk>() as u64
            };
            let foreign_clone_stmt = match kind {
                Kind::Owned => Some(quote! {
                    {
                        let __fp: ::bstack_raii::ForeignRepr = __od.#fname;
                        let __off = __fp.offset();
                        if __off != 0 {
                            let __fid = __fp.file_id();
                            if __fid == 0 {
                                // SELF: deep-clone into the home plan (one atomic commit).
                                let __child = <#ftarget as ::bstack_raii::BStackBlock>::from_range(
                                    ::bstack_raii::BStackRange::new(__off, #target_od_size),
                                );
                                let __new = __child.__bstack_clone_into(allocator, __plan)?;
                                __od.#fname = ::bstack_raii::ForeignRepr::new(0, __new.start());
                            } else if __plan.is_measuring() {
                                // Foreign deep-clone is eager cross-file work; the
                                // measure pass (home-file sizes only) skips it, so it
                                // runs exactly once in the build pass.
                            } else if let ::core::option::Option::Some(__id) =
                                ::bstack_raii::registry::FileId::from_u64(__fid)
                            {
                                let __host = ::bstack_raii::registry::host_arc(__id)
                                    .ok_or_else(|| ::std::io::Error::new(
                                        ::std::io::ErrorKind::NotFound,
                                        "cannot deep-clone `#[bstack_owned] Foreign<T>`: \
                                         target file not attached",
                                    ))?;
                                let __adapter =
                                    ::bstack_raii::ForeignHostAllocator::new(__host, __id);
                                let __new_off = unsafe {
                                    ::bstack_raii::__private::foreign_clone_owned::<#ftarget, _>(
                                        &__adapter, __off,
                                    )?
                                };
                                __od.#fname = ::bstack_raii::ForeignRepr::new(__fid, __new_off);
                            } else {
                                return ::std::result::Result::Err(::std::io::Error::new(
                                    ::std::io::ErrorKind::InvalidData,
                                    "cannot clone `Foreign<T>`: malformed file id",
                                ));
                            }
                        }
                    }
                }),
                Kind::Strong => Some(quote! {
                    {
                        let __fp: ::bstack_raii::ForeignRepr = __od.#fname;
                        let __off = __fp.offset();
                        if __off != 0 {
                            let __fid = __fp.file_id();
                            if __fid == 0 {
                                // SELF: bump the strong count via the home plan (atomic).
                                let __data = unsafe {
                                    ::bstack_raii::BStackRef::<#ftarget>::from_range(
                                        ::bstack_raii::BStackRange::new(__off, #target_od_size),
                                    )
                                };
                                __plan.bump_strong(__data, allocator)?;
                            } else if __plan.is_measuring() {
                                // Foreign refcount bump is eager cross-file work; done
                                // once, in the build pass (measure skips it).
                            } else if let ::core::option::Option::Some(__id) =
                                ::bstack_raii::registry::FileId::from_u64(__fid)
                            {
                                let __host = ::bstack_raii::registry::host_arc(__id)
                                    .ok_or_else(|| ::std::io::Error::new(
                                        ::std::io::ErrorKind::NotFound,
                                        "cannot clone `#[bstack_strong] Foreign<T>`: \
                                         target file not attached",
                                    ))?;
                                let __adapter =
                                    ::bstack_raii::ForeignHostAllocator::new(__host, __id);
                                unsafe {
                                    ::bstack_raii::__private::foreign_clone_strong::<#ftarget, _>(
                                        &__adapter, __off,
                                    )?;
                                }
                                // The pointer is unchanged (shares the same target).
                            } else {
                                return ::std::result::Result::Err(::std::io::Error::new(
                                    ::std::io::ErrorKind::InvalidData,
                                    "cannot clone `Foreign<T>`: malformed file id",
                                ));
                            }
                        }
                    }
                }),
                Kind::Weak => Some(quote! {
                    {
                        let __fp: ::bstack_raii::ForeignRepr = __od.#fname;
                        // For a weak pointer, the offset is the target's control block.
                        let __off = __fp.offset();
                        if __off != 0 {
                            let __fid = __fp.file_id();
                            if __fid == 0 {
                                // SELF: bump the weak count via the home plan (atomic).
                                __plan.bump_weak(__off);
                            } else if __plan.is_measuring() {
                                // Foreign refcount bump is eager cross-file work; done
                                // once, in the build pass (measure skips it).
                            } else if let ::core::option::Option::Some(__id) =
                                ::bstack_raii::registry::FileId::from_u64(__fid)
                            {
                                let __host = ::bstack_raii::registry::host_arc(__id)
                                    .ok_or_else(|| ::std::io::Error::new(
                                        ::std::io::ErrorKind::NotFound,
                                        "cannot clone `#[bstack_weak] Foreign<T>`: \
                                         target file not attached",
                                    ))?;
                                let __adapter =
                                    ::bstack_raii::ForeignHostAllocator::new(__host, __id);
                                unsafe {
                                    ::bstack_raii::__private::foreign_clone_weak::<#ftarget, _>(
                                        &__adapter, __off,
                                    )?;
                                }
                            } else {
                                return ::std::result::Result::Err(::std::io::Error::new(
                                    ::std::io::ErrorKind::InvalidData,
                                    "cannot clone `Foreign<T>`: malformed file id",
                                ));
                            }
                        }
                    }
                }),
                // Ref aliases (byte-copied verbatim); Pod / Embed already rejected.
                Kind::Ref | Kind::Pod | Kind::Embed => None,
            };
            if let Some(cs) = foreign_clone_stmt {
                clone_stmts.push(cs);
            }

            // `#[bstack_mut]`: `replace_<f>` (owned/strong/weak — moves the old
            // cross-file target out as its RAII dual) and, for a foreign `ref`, also
            // `set_<f>`. One crash-atomic 16-byte `ForeignRepr` write; the swap is
            // purely local (no registry / host access), the cross-file free/decrement
            // travelling with the returned handle.
            if is_bstack_mut(&field.attrs) {
                for m in
                    foreign_mut_methods(vis, fname, &quote!(#ftarget), &on_disk_ty, kind, nullable)
                {
                    accessors.push(m);
                }
            }
            continue;
        }

        // `Vec<T>` / `String` (and `&str` → `String`): an inline descriptor on
        // disk, a `BStackVec` at runtime. A nullable vec uses the `data_off == 0`
        // niche. Handled here.
        let vinfo = if is_str(opt_inner) {
            Some(VecInfo {
                elem: quote!(u8),
                is_string: true,
            })
        } else {
            vec_field(opt_inner)
        };
        if let Some(vinfo) = vinfo {
            let elem = &vinfo.elem;
            // The descriptor lives inline in the field (no descriptor block).
            on_disk_fields.push(quote!(#fname: ::bstack_raii::VecDesc,));
            let cap = format_ident!("__cap_{}", fname);
            mv_caps.push(quote!(let #cap = __od.#fname;));

            // `String` is always POD bytes; a block annotation on it is meaningless.
            if vinfo.is_string && kind != Kind::Pod {
                return Err(Error::new_spanned(
                    &field.ty,
                    "`String` is always POD; remove the ownership annotation",
                ));
            }

            // `#[ann] Vec<Foreign<T>>` — a growable vector of cross-file wide
            // pointers, each an owning foreign reference per the annotation. Stored as
            // a POD-style vector of `ForeignPtr` (16 B each); construction / access map
            // to `Foreign<T>`, and teardown / clone dispatch each element cross-file
            // exactly like a scalar `Foreign` field. A null/unset element is a
            // `Foreign` whose offset is `0` (skipped by teardown / clone).
            if let Some(velem) = vec_inner(opt_inner)
                && let Some(ftarget) = foreign_inner(option_inner(velem).unwrap_or(velem))
            {
                // `Vec<Option<Foreign<T>>>`: a per-element-nullable vector (a stored
                // offset of `0` reads as `None`); `Vec<Foreign<T>>` is the plain form.
                let elem_nullable = option_inner(velem).is_some();
                validate_foreign_target(
                    kind,
                    ftarget,
                    &field.ty,
                    "`Vec<Foreign<T>>`",
                    format_ident!("__bstack_foreign_vec_target_{}", fname),
                    !type_mentions_any(ftarget, &type_params),
                    &mut wrapper_defs,
                )?;

                let store = quote!(::bstack_raii::BStackVec::<::bstack_raii::ForeignRepr, __A>);
                let field_loc =
                    quote!(self.0.start() + ::core::mem::offset_of!(#on_disk_ty, #fname) as u64);
                let field_ty = if elem_nullable {
                    quote!(::core::option::Option<::bstack_raii::Foreign<#ftarget>>)
                } else {
                    quote!(::bstack_raii::Foreign<#ftarget>)
                };
                // The accessor binds each returned `Foreign`'s lifetime to `'__v` (the
                // allocator borrow it read through), so a `SELF` element cannot escape it.
                let acc_elem_ty = if elem_nullable {
                    quote!(::core::option::Option<::bstack_raii::Foreign<'__v, #ftarget>>)
                } else {
                    quote!(::bstack_raii::Foreign<'__v, #ftarget>)
                };
                // Map a stored `ForeignRepr` ↔ the element type (offset 0 ⇒ `None` when
                // the element is `Option`-wrapped). SAFETY: each repr was stored into
                // this file; the returned `Foreign`s are `'__v`-bound by the accessor.
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
                    quote!(|__f: #field_ty| match __f {
                        ::core::option::Option::Some(__ff) => __ff.repr(),
                        ::core::option::Option::None => ::bstack_raii::ForeignRepr::new(0, 0),
                    })
                } else {
                    quote!(|__f: #field_ty| __f.repr())
                };

                // ---- Accessor: `Vec<Foreign<T>>` / `Vec<Option<Foreign<T>>>` (or `Option<..>`) ----
                let (acc_ret, acc_body) = if nullable {
                    (
                        quote!(::core::option::Option<::std::vec::Vec<#acc_elem_ty>>),
                        quote!(match unsafe { #store::from_field_opt(#field_loc, allocator) }? {
                            ::core::option::Option::Some(__v) => ::core::option::Option::Some(
                                __v.to_vec()?
                                    .into_iter()
                                    .map(#from_ptr)
                                    .collect()),
                            ::core::option::Option::None => ::core::option::Option::None,
                        }),
                    )
                } else {
                    (
                        quote!(::std::vec::Vec<#acc_elem_ty>),
                        quote!(unsafe { #store::from_field(#field_loc, allocator)? }
                            .to_vec()?
                            .into_iter()
                            .map(#from_ptr)
                            .collect()),
                    )
                };
                accessors.push(quote! {
                    #vis fn #getter<'__v, __A: ::bstack_raii::BStackRaiiAllocator>(
                        &self,
                        allocator: &'__v __A,
                    ) -> ::std::io::Result<#acc_ret> {
                        ::std::result::Result::Ok(#acc_body)
                    }
                });

                // ---- Constructor: `Vec<Foreign<T>>` → a `ForeignPtr` data block ----
                let build = quote! {
                    let __ptrs: ::std::vec::Vec<::bstack_raii::ForeignRepr> =
                        __list.into_iter().map(#to_ptr).collect();
                    #store::from_slice(allocator, &__ptrs)?.descriptor()
                };
                let (param, prep) = if nullable {
                    (
                        quote!(#fname: ::core::option::Option<::std::vec::Vec<#field_ty>>,),
                        quote! {
                            let #fname: ::bstack_raii::VecDesc = match #fname {
                                ::core::option::Option::Some(__list) => { #build }
                                ::core::option::Option::None => ::core::default::Default::default(),
                            };
                        },
                    )
                } else {
                    (
                        quote!(#fname: ::std::vec::Vec<#field_ty>,),
                        quote! {
                            let #fname: ::bstack_raii::VecDesc = { let __list = #fname; #build };
                        },
                    )
                };
                ctor_params.push(param);
                ctor_preps.push(prep);
                ctor_inits.push(quote!(#fname: #fname,));

                // ---- Teardown: dispatch each element, then free the data block ----
                let elem_drop = foreign_elem_drop(kind, ftarget);
                let drop_loop = if matches!(kind, Kind::Ref) {
                    quote!()
                } else {
                    quote! {
                        for __fp in #store::from_desc(__desc, allocator).to_vec()? { #elem_drop }
                    }
                };
                drop_stmts.push(quote! {
                    {
                        let __desc: ::bstack_raii::VecDesc = __on_disk.#fname;
                        if __desc.data_off != 0 {
                            #drop_loop
                            #store::from_desc(__desc, allocator).bstack_drop()?;
                        }
                    }
                });

                // ---- Clone: dispatch each element into a fresh `ForeignPtr` block ----
                let elem_clone = foreign_elem_clone(kind, ftarget);
                clone_stmts.push(quote! {
                    {
                        let __srcdesc: ::bstack_raii::VecDesc = __od.#fname;
                        if __srcdesc.data_off != 0 {
                            let __src = #store::from_desc(__srcdesc, allocator).to_vec()?;
                            let mut __new: ::std::vec::Vec<::bstack_raii::ForeignRepr> =
                                ::std::vec::Vec::with_capacity(__src.len());
                            for __fp in __src {
                                #elem_clone
                                __new.push(__newfp);
                            }
                            __od.#fname = __plan.stage_bytevec(
                                allocator, ::bstack_raii::bytemuck::cast_slice(&__new))?;
                        }
                    }
                });

                // ---- Move: the raw `ForeignPtr` vector handle ----
                let (mvt, mvr) = wrap_vec_move(
                    quote!(::bstack_raii::BStackVec<'__mv, ::bstack_raii::ForeignRepr, __A>),
                    quote!(::bstack_raii::BStackVec::from_desc(#cap, __alloc)),
                    &cap,
                    nullable,
                );
                mv_types.push(mvt);
                mv_recon.push(mvr);
                continue;
            }

            // `#[bstack_owned/strong/weak/ref] Vec<[T; N]>` — a vector whose
            // elements are fixed-size arrays of block references (nested `[[T;N];M]`
            // and per-element `[Option<T>; N]` allowed). The offsets are stored
            // **flat** as a `BStackVec<u64>` (one per leaf, row-major), exactly like
            // a scalar block-element vector — so per-offset teardown / clone are the
            // same; only the accessor (reshape to `[[T;..];..]`) and constructor
            // (flatten) differ. POD `Vec<[Pod; N]>` rides the normal POD vec path.
            if kind != Kind::Pod
                && let Some(velem) = vec_inner(opt_inner)
                && let Type::Array(_) = velem
            {
                let (dims, elem_ty, leaf_nullable) = array_shape(velem)?;
                reject_nested_const_dims(&dims, &const_params, &field.ty)?;
                let total = dims_prod(&dims);
                let elem_ts = quote!(#elem_ty);
                let size_elem = quote!(::core::mem::size_of::<
                    <#elem_ty as ::bstack_raii::BStackBlock>::OnDisk>() as u64);
                let store = quote!(::bstack_raii::BStackVec::<u64, __A>);
                let field_loc =
                    quote!(self.0.start() + ::core::mem::offset_of!(#on_disk_ty, #fname) as u64);
                let is_weak = kind == Kind::Weak;
                let (ctrl_ty, ctrl_size) = (
                    quote!(<#elem_ty as ::bstack_raii::BStackWeakable>::Control),
                    quote!(::core::mem::size_of::<
                        <#elem_ty as ::bstack_raii::BStackWeakable>::Control>() as u64),
                );
                let vec_ty = match kind {
                    Kind::Owned => quote!(BStackBlockVec),
                    Kind::Strong => quote!(BStackStrongVec),
                    Kind::Weak => quote!(BStackWeakVec),
                    Kind::Ref => quote!(BStackRefVec),
                    _ => unreachable!(),
                };

                // ---- Accessor: materialize `Vec<[[View; ..]; ..]>` ----
                let view_leaf = if is_weak {
                    quote!(::core::option::Option<::bstack_raii::BStackRc<'__v, #elem_ty, __A>>)
                } else if leaf_nullable {
                    quote!(::core::option::Option<#elem_ty>)
                } else {
                    quote!(#elem_ty)
                };
                let view_ret = nested_ty(&dims, &view_leaf);
                let view_read = |k: &Ident| {
                    if is_weak {
                        quote!({
                            let __o = __grp[#k];
                            if __o == 0 {
                                ::core::option::Option::None
                            } else {
                                let __ctrl = unsafe {
                                    ::bstack_raii::BStackRef::<#ctrl_ty>::from_range(
                                        ::bstack_raii::BStackRange::new(__o, #ctrl_size)) };
                                let __wk = unsafe {
                                    ::bstack_raii::BStackWeak::<#elem_ty, __A>::from_raw(
                                        __ctrl, allocator) };
                                let __up = __wk.upgrade()?;
                                let _ = __wk.into_raw();
                                __up
                            }
                        })
                    } else if leaf_nullable {
                        quote!({
                            let __o = __grp[#k];
                            if __o == 0 {
                                ::core::option::Option::None
                            } else {
                                ::core::option::Option::Some(
                                    <#elem_ty as ::bstack_raii::BStackBlock>::from_range(
                                        ::bstack_raii::BStackRange::new(__o, #size_elem)))
                            }
                        })
                    } else {
                        quote!(<#elem_ty as ::bstack_raii::BStackBlock>::from_range(
                            ::bstack_raii::BStackRange::new(__grp[#k], #size_elem)))
                    }
                };
                let build_body = nested_build(&dims, &view_leaf, &view_read);
                let reshape = quote! {
                    let mut __out = ::std::vec::Vec::with_capacity(__flat.len() / (#total));
                    for __grp in __flat.chunks(#total) {
                        __out.push(#build_body);
                    }
                    __out
                };
                let (acc_ret, acc_map): (TokenStream, TokenStream) = if nullable {
                    (
                        quote!(::core::option::Option<::std::vec::Vec<#view_ret>>),
                        quote!(match unsafe { #store::from_field_opt(#field_loc, allocator) }? {
                            ::core::option::Option::Some(__v) => {
                                let __flat = __v.to_vec()?;
                                ::core::option::Option::Some({ #reshape })
                            }
                            ::core::option::Option::None => ::core::option::Option::None,
                        }),
                    )
                } else {
                    (
                        quote!(::std::vec::Vec<#view_ret>),
                        quote!({
                            let __flat = unsafe { #store::from_field(#field_loc, allocator)? }.to_vec()?;
                            #reshape
                        }),
                    )
                };
                accessors.push(quote! {
                    #vis fn #getter<'__v, __A: ::bstack_raii::BStackRaiiAllocator>(
                        &self,
                        allocator: &'__v __A,
                    ) -> ::std::io::Result<#acc_ret> {
                        ::std::result::Result::Ok(#acc_map)
                    }
                });

                // ---- Constructor: flatten `Vec<[[Handle; ..]; ..]>` → flat offsets ----
                let handle_base = match kind {
                    Kind::Owned => quote!(::bstack_raii::BStackOwned<#elem_ty>),
                    Kind::Strong => quote!(::bstack_raii::BStackRc<'__ctor, #elem_ty, __A>),
                    Kind::Weak => quote!(::bstack_raii::BStackWeak<'__ctor, #elem_ty, __A>),
                    Kind::Ref => quote!(::bstack_raii::BStackRef<#elem_ty>),
                    _ => unreachable!(),
                };
                let handle_leaf = if leaf_nullable {
                    quote!(::core::option::Option<#handle_base>)
                } else {
                    handle_base.clone()
                };
                let param_ty = nested_ty(&dims, &handle_leaf);
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
                let build_flat = quote! {
                    let mut __flat: ::std::vec::Vec<u64> = ::std::vec::Vec::new();
                    for __a in __list {
                        #consume_one
                    }
                    #store::from_slice(allocator, &__flat)?.descriptor()
                };
                let (param, prep) = if nullable {
                    (
                        quote!(#fname: ::core::option::Option<::std::vec::Vec<#param_ty>>,),
                        quote! {
                            let #fname: ::bstack_raii::VecDesc = match #fname {
                                ::core::option::Option::Some(__list) => { #build_flat }
                                ::core::option::Option::None => ::core::default::Default::default(),
                            };
                        },
                    )
                } else {
                    (
                        quote!(#fname: ::std::vec::Vec<#param_ty>,),
                        quote! {
                            let #fname: ::bstack_raii::VecDesc = {
                                let __list = #fname;
                                #build_flat
                            };
                        },
                    )
                };
                ctor_params.push(param);
                ctor_preps.push(prep);
                ctor_inits.push(quote!(#fname: #fname,));

                // ---- Teardown: free each child per kind, then the offset block ----
                let free_child = match kind {
                    Kind::Owned => quote!(::bstack_raii::OwnedRef(unsafe {
                        ::bstack_raii::BStackRef::<#elem_ty>::from_range(
                            ::bstack_raii::BStackRange::new(__off, #size_elem))
                    }).bstack_drop(allocator)?;),
                    Kind::Strong => {
                        quote!(<#elem_ty as ::bstack_raii::BStackShared>::drop_strong_ref(
                        unsafe { ::bstack_raii::BStackRef::<#elem_ty>::from_range(
                            ::bstack_raii::BStackRange::new(__off, #size_elem)) },
                        allocator)?;)
                    }
                    Kind::Weak => quote!(::bstack_raii::WeakRef::<#elem_ty>(unsafe {
                        ::bstack_raii::BStackRef::<#ctrl_ty>::from_range(
                            ::bstack_raii::BStackRange::new(__off, #ctrl_size))
                    }).bstack_drop(allocator)?;),
                    Kind::Ref => quote!(),
                    _ => unreachable!(),
                };
                let free_children = if matches!(kind, Kind::Ref) {
                    quote!()
                } else {
                    quote! {
                        for __off in #store::from_desc(__desc, allocator).to_vec()? {
                            if __off != 0 { #free_child }
                        }
                    }
                };
                drop_stmts.push(quote! {
                    {
                        let __desc: ::bstack_raii::VecDesc = __on_disk.#fname;
                        if __desc.data_off != 0 {
                            #free_children
                            #store::from_desc(__desc, allocator).bstack_drop()?;
                        }
                    }
                });

                // ---- Clone: owned deep-clones offsets; strong/weak bump + copy;
                //      ref copies verbatim (all staged into the plan) ----
                let clone_body = match kind {
                    Kind::Owned => quote! {
                        let __flat = #store::from_desc(__srcdesc, allocator).to_vec()?;
                        let mut __new: ::std::vec::Vec<u64> =
                            ::std::vec::Vec::with_capacity(__flat.len());
                        for __off in __flat {
                            if __off != 0 {
                                __new.push(
                                    <#elem_ty as ::bstack_raii::BStackBlock>::from_range(
                                        ::bstack_raii::BStackRange::new(__off, #size_elem))
                                        .__bstack_clone_into(allocator, __plan)?.start());
                            } else {
                                __new.push(0u64);
                            }
                        }
                        __od.#fname = __plan.stage_bytevec(
                            allocator, ::bstack_raii::bytemuck::cast_slice(&__new))?;
                    },
                    Kind::Strong => quote! {
                        for __off in #store::from_desc(__srcdesc, allocator).to_vec()? {
                            if __off != 0 {
                                __plan.bump_strong(unsafe {
                                    ::bstack_raii::BStackRef::<#elem_ty>::from_range(
                                        ::bstack_raii::BStackRange::new(__off, #size_elem))
                                }, allocator)?;
                            }
                        }
                        __od.#fname = #store::from_desc(__srcdesc, allocator)
                            .clone_data_into(__plan)?;
                    },
                    Kind::Weak => quote! {
                        for __off in #store::from_desc(__srcdesc, allocator).to_vec()? {
                            if __off != 0 { __plan.bump_weak(__off); }
                        }
                        __od.#fname = #store::from_desc(__srcdesc, allocator)
                            .clone_data_into(__plan)?;
                    },
                    Kind::Ref => quote! {
                        __od.#fname = #store::from_desc(__srcdesc, allocator)
                            .clone_data_into(__plan)?;
                    },
                    _ => unreachable!(),
                };
                clone_stmts.push(quote! {
                    {
                        let __srcdesc: ::bstack_raii::VecDesc = __od.#fname;
                        if __srcdesc.data_off != 0 {
                            #clone_body
                        }
                    }
                });

                // ---- Move: yield the flat block-vector handle (loses `[T; N]` shape) ----
                let (mvt, mvr) = block_vec_move(&cap, &elem_ts, vec_ty, nullable);
                mv_types.push(mvt);
                mv_recon.push(mvr);
                continue;
            }

            // The annotation states the *elements'* relationship (the descriptor
            // + array is always owned by this struct regardless). No annotation =>
            // POD elements (byte storage, requiring `T: Pod`).
            let (drop_s, acc, ctor, mv) = match kind {
                Kind::Embed => {
                    return Err(Error::new_spanned(
                        &field.ty,
                        "cannot #[embed] a `Vec` / `String`; embed a `#[bstack_block]` type",
                    ));
                }
                Kind::Pod => (
                    vec_drop_stmt(fname, elem, nullable),
                    vec_accessor(vis, fname, elem, &on_disk_ty, nullable),
                    vec_ctor(fname, &vinfo, nullable),
                    vec_move(&cap, elem, nullable),
                ),
                Kind::Owned => (
                    block_vec_drop_stmt(fname, quote!(BStackBlockVec), elem, nullable),
                    block_vec_accessor(
                        vis,
                        fname,
                        elem,
                        &on_disk_ty,
                        quote!(BStackBlockVec),
                        nullable,
                    ),
                    block_vec_ctor(
                        fname,
                        elem,
                        quote!(BStackBlockVec),
                        quote!(::bstack_raii::BStackOwned<#elem>),
                        nullable,
                    ),
                    block_vec_move(&cap, elem, quote!(BStackBlockVec), nullable),
                ),
                Kind::Strong => (
                    block_vec_drop_stmt(fname, quote!(BStackStrongVec), elem, nullable),
                    block_vec_accessor(
                        vis,
                        fname,
                        elem,
                        &on_disk_ty,
                        quote!(BStackStrongVec),
                        nullable,
                    ),
                    block_vec_ctor(
                        fname,
                        elem,
                        quote!(BStackStrongVec),
                        quote!(::bstack_raii::BStackRc<'__ctor, #elem, __A>),
                        nullable,
                    ),
                    block_vec_move(&cap, elem, quote!(BStackStrongVec), nullable),
                ),
                Kind::Weak => (
                    block_vec_drop_stmt(fname, quote!(BStackWeakVec), elem, nullable),
                    block_vec_accessor(
                        vis,
                        fname,
                        elem,
                        &on_disk_ty,
                        quote!(BStackWeakVec),
                        nullable,
                    ),
                    block_vec_ctor(
                        fname,
                        elem,
                        quote!(BStackWeakVec),
                        quote!(::bstack_raii::BStackWeak<'__ctor, #elem, __A>),
                        nullable,
                    ),
                    block_vec_move(&cap, elem, quote!(BStackWeakVec), nullable),
                ),
                Kind::Ref => (
                    block_vec_drop_stmt(fname, quote!(BStackRefVec), elem, nullable),
                    block_vec_accessor(
                        vis,
                        fname,
                        elem,
                        &on_disk_ty,
                        quote!(BStackRefVec),
                        nullable,
                    ),
                    block_vec_ctor(
                        fname,
                        elem,
                        quote!(BStackRefVec),
                        quote!(::bstack_raii::BStackRef<#elem>),
                        nullable,
                    ),
                    block_vec_move(&cap, elem, quote!(BStackRefVec), nullable),
                ),
            };
            drop_stmts.push(drop_s);
            clone_stmts.push(vec_clone_stmt(fname, kind, elem));
            accessors.push(acc);
            let (param, prep, init) = ctor;
            ctor_params.push(param);
            ctor_preps.push(prep);
            ctor_inits.push(init);
            let (mv_ty, mv_rc) = mv;
            mv_types.push(mv_ty);
            mv_recon.push(mv_rc);
            continue;
        }

        // Inline array of vectors `[Vec<T>; N]` — possibly nested `[[Vec<T>;N];M]`
        // and/or per-element `[Option<Vec<T>>; N]`: N independent inline `VecDesc`s,
        // each owning its own data block. Detected as an array whose leaf is a
        // `Vec` / `String`. A POD `[Vec<Pod>; N]` is intercepted here too — the
        // `VecDesc`s are Pod bytes, but the data blocks need a real lifecycle, so it
        // must NOT fall through to the plain POD path. The element annotation names
        // the inner vectors' element ownership, exactly like a scalar `Vec<T>`.
        if let Type::Array(_) = opt_inner {
            let (dims, leaf, leaf_nullable) = array_shape(opt_inner)?;
            reject_nested_const_dims(&dims, &const_params, &field.ty)?;
            let leaf_vinfo = if is_str(leaf) {
                Some(VecInfo {
                    elem: quote!(u8),
                    is_string: true,
                })
            } else {
                vec_field(leaf)
            };
            if let Some(leaf_vinfo) = leaf_vinfo {
                // Validate the leaf vector's own element nesting (`Vec<Vec<..>>`).
                check_container_nesting(leaf)?;
                if nullable {
                    return Err(Error::new_spanned(
                        &field.ty,
                        "a whole-array `Option<[Vec<T>; N]>` is not supported; use \
                         `[Option<Vec<T>>; N]` for per-element nullability",
                    ));
                }
                if leaf_vinfo.is_string && kind != Kind::Pod {
                    return Err(Error::new_spanned(
                        &field.ty,
                        "`String` is always POD; remove the ownership annotation",
                    ));
                }
                if kind == Kind::Embed {
                    return Err(Error::new_spanned(
                        &field.ty,
                        "cannot #[embed] a `Vec` / `String`; embed a `#[bstack_block]` type",
                    ));
                }
                let total = dims_prod(&dims);
                let elem = &leaf_vinfo.elem;
                let is_string = leaf_vinfo.is_string;
                let vec_ty = match kind {
                    Kind::Pod => quote!(BStackVec),
                    Kind::Owned => quote!(BStackBlockVec),
                    Kind::Strong => quote!(BStackStrongVec),
                    Kind::Weak => quote!(BStackWeakVec),
                    Kind::Ref => quote!(BStackRefVec),
                    Kind::Embed => unreachable!(),
                };
                on_disk_fields.push(quote!(#fname: [::bstack_raii::VecDesc; #total],));

                // Accessor: nested `[[VecHandle; ..]; ..]` (or `Option` per slot),
                // reading each `VecDesc` from the on-disk field once.
                let handle_lt = quote!(::bstack_raii::#vec_ty<'__v, #elem, __A>);
                let acc_leaf = if leaf_nullable {
                    quote!(::core::option::Option<#handle_lt>)
                } else {
                    handle_lt.clone()
                };
                let acc_ret = nested_ty(&dims, &acc_leaf);
                // Each slot resolves through `from_field` so descriptor updates
                // (growth / realloc) persist back to its OWN inline `VecDesc` — like
                // a scalar `Vec` field, but at `base + k * size_of::<VecDesc>()`.
                let acc_read = |k: &Ident| {
                    let slot = quote!(
                        __base + (#k as u64) * (::core::mem::size_of::<::bstack_raii::VecDesc>() as u64));
                    if leaf_nullable {
                        quote!(unsafe { ::bstack_raii::#vec_ty::from_field_opt(#slot, allocator) }?)
                    } else {
                        quote!(unsafe { ::bstack_raii::#vec_ty::from_field(#slot, allocator) }?)
                    }
                };
                let acc_body = nested_build(&dims, &acc_leaf, &acc_read);
                accessors.push(quote! {
                    #vis fn #getter<'__v, __A: ::bstack_raii::BStackRaiiAllocator>(
                        &self,
                        allocator: &'__v __A,
                    ) -> ::std::io::Result<#acc_ret> {
                        let __base =
                            self.0.start() + ::core::mem::offset_of!(#on_disk_ty, #fname) as u64;
                        ::std::result::Result::Ok(#acc_body)
                    }
                });

                // Constructor: allocate a data block per slot, store its descriptor.
                // POD slots take `&[T]` / `&str` (`from_slice`); block slots take
                // `Vec<Handle>` (`from_handles`).
                let handle_ctor = match kind {
                    Kind::Owned => quote!(::bstack_raii::BStackOwned<#elem>),
                    Kind::Strong => quote!(::bstack_raii::BStackRc<'__ctor, #elem, __A>),
                    Kind::Weak => quote!(::bstack_raii::BStackWeak<'__ctor, #elem, __A>),
                    Kind::Ref => quote!(::bstack_raii::BStackRef<#elem>),
                    _ => quote!(),
                };
                let ctor_leaf = match kind {
                    Kind::Pod if is_string => quote!(&str),
                    Kind::Pod => quote!(&[#elem]),
                    _ => quote!(::std::vec::Vec<#handle_ctor>),
                };
                let param_leaf = if leaf_nullable {
                    quote!(::core::option::Option<#ctor_leaf>)
                } else {
                    ctor_leaf.clone()
                };
                let ctor_param_ty = nested_ty(&dims, &param_leaf);
                ctor_params.push(quote!(#fname: #ctor_param_ty,));
                let desc_of = |b: &Ident| -> TokenStream {
                    match kind {
                        Kind::Pod => {
                            let data = if is_string {
                                quote!(#b.as_bytes())
                            } else {
                                quote!(#b)
                            };
                            quote!(::bstack_raii::BStackVec::<#elem, __A>::from_slice(
                                allocator, #data)?.descriptor())
                        }
                        _ => quote!(::bstack_raii::#vec_ty::<#elem, __A>::from_handles(
                            allocator, #b)?.descriptor()),
                    }
                };
                let ctor_write = |k: &Ident, leaf: &Ident| {
                    if leaf_nullable {
                        let inner = format_ident!("__vd");
                        let d = desc_of(&inner);
                        quote!({
                            __slots[#k] = match #leaf {
                                ::core::option::Option::Some(#inner) => #d,
                                ::core::option::Option::None =>
                                    ::core::default::Default::default(),
                            };
                        })
                    } else {
                        let d = desc_of(leaf);
                        quote!(__slots[#k] = #d;)
                    }
                };
                let flatten = nested_consume(&dims, &quote!(#fname), &ctor_write);
                ctor_preps.push(quote! {
                    let #fname: [::bstack_raii::VecDesc; #total] = {
                        let mut __slots =
                            [<::bstack_raii::VecDesc as ::core::default::Default>::default();
                                #total];
                        #flatten
                        __slots
                    };
                });
                ctor_inits.push(quote!(#fname: #fname,));

                // Teardown: free each vector's data block.
                drop_stmts.push(quote! {
                    {
                        let __descs: [::bstack_raii::VecDesc; #total] = __on_disk.#fname;
                        for __k in 0usize..(#total) {
                            if __descs[__k].data_off != 0 {
                                ::bstack_raii::#vec_ty::<#elem, __A>::from_desc(
                                    __descs[__k], allocator).bstack_drop()?;
                            }
                        }
                    }
                });

                // Clone: deep-clone each vector's data block per the element
                // relationship, repointing the slot descriptor (a `0` niche is kept).
                let clone_expr = match kind {
                    Kind::Pod => quote!(::bstack_raii::BStackVec::<#elem, __A>::from_desc(
                        __sd, allocator).clone_data_into(__plan)?),
                    Kind::Owned => quote!(::bstack_raii::BStackBlockVec::<#elem, __A>::from_desc(
                        __sd, allocator).clone_into(__plan, |__er, __p| {
                            <#elem as ::bstack_raii::BStackBlock>::from_range(__er)
                                .__bstack_clone_into(allocator, __p)
                        })?),
                    Kind::Strong => quote!(::bstack_raii::BStackStrongVec::<#elem, __A>::from_desc(
                        __sd, allocator).clone_into(__plan)?),
                    Kind::Weak => quote!(::bstack_raii::BStackWeakVec::<#elem, __A>::from_desc(
                        __sd, allocator).clone_into(__plan)?),
                    Kind::Ref => quote!(::bstack_raii::BStackRefVec::<#elem, __A>::from_desc(
                        __sd, allocator).clone_into(__plan)?),
                    Kind::Embed => unreachable!(),
                };
                clone_stmts.push(quote! {
                    {
                        let mut __descs: [::bstack_raii::VecDesc; #total] = __od.#fname;
                        for __k in 0usize..(#total) {
                            let __sd: ::bstack_raii::VecDesc = __descs[__k];
                            if __sd.data_off != 0 {
                                __descs[__k] = #clone_expr;
                            }
                        }
                        __od.#fname = __descs;
                    }
                });

                // Move: nested `[[VecHandle; ..]; ..]` from the captured descriptors.
                let cap = format_ident!("__cap_{}", fname);
                mv_caps.push(quote!(let #cap = __od.#fname;));
                let mv_handle = quote!(::bstack_raii::#vec_ty<'__mv, #elem, __A>);
                let mv_leaf = if leaf_nullable {
                    quote!(::core::option::Option<#mv_handle>)
                } else {
                    mv_handle.clone()
                };
                mv_types.push(nested_ty(&dims, &mv_leaf));
                let mv_read = |k: &Ident| {
                    if leaf_nullable {
                        quote!({
                            let __d = #cap[#k];
                            if __d.data_off != 0 {
                                ::core::option::Option::Some(
                                    ::bstack_raii::#vec_ty::from_desc(__d, __alloc))
                            } else {
                                ::core::option::Option::None
                            }
                        })
                    } else {
                        quote!(::bstack_raii::#vec_ty::from_desc(#cap[#k], __alloc))
                    }
                };
                mv_recon.push(nested_build(&dims, &mv_leaf, &mv_read));
                continue;
            }
        }

        // `#[ann] [Foreign<T>; N]` — an inline array of cross-file wide pointers
        // (possibly nested `[[Foreign<T>; A]; B]` / per-element `[Option<Foreign<T>>; N]`).
        // Stored flat as `[ForeignPtr; TOTAL]` inline (16 B each, no data block); each
        // slot's teardown / clone dispatches cross-file exactly like a scalar `Foreign`.
        // A null/unset slot is a `Foreign` whose offset is `0`. Must be annotated.
        if let Type::Array(_) = opt_inner {
            let (adims, aleaf, aleaf_nullable) = array_shape(opt_inner)?;
            if let Some(ftarget) = foreign_inner(aleaf) {
                reject_nested_const_dims(&adims, &const_params, &field.ty)?;
                validate_foreign_target(
                    kind,
                    ftarget,
                    &field.ty,
                    "`[Foreign<T>; N]`",
                    format_ident!("__bstack_foreign_arr_target_{}", fname),
                    !type_mentions_any(ftarget, &type_params),
                    &mut wrapper_defs,
                )?;
                if nullable {
                    return Err(Error::new_spanned(
                        &field.ty,
                        "a whole-array `Option<[Foreign<T>; N]>` is not supported; a null foreign \
                         element is a `Foreign` with offset 0, or use `[Option<Foreign<T>>; N]`",
                    ));
                }
                let total = dims_prod(&adims);
                let field_ty = quote!(::bstack_raii::Foreign<#ftarget>);
                on_disk_fields.push(quote!(#fname: [::bstack_raii::ForeignRepr; #total],));

                // ---- Accessor: nested `[[Foreign<T>; ..]; ..]` (Option per slot) ----
                // Each returned `Foreign`'s lifetime is bound to `'__f` (the `stack`
                // borrow), so a `SELF` slot cannot escape the file it was read from.
                let leaf_ty = if aleaf_nullable {
                    quote!(::core::option::Option<::bstack_raii::Foreign<'__f, #ftarget>>)
                } else {
                    quote!(::bstack_raii::Foreign<'__f, #ftarget>)
                };
                let acc_ret = nested_ty(&adims, &leaf_ty);
                let acc_read = |k: &Ident| {
                    // SAFETY: each slot repr was stored into this file; bound to `'__f`.
                    if aleaf_nullable {
                        quote!({
                            let __p = __arr[#k];
                            if __p.offset() == 0 {
                                ::core::option::Option::None
                            } else {
                                ::core::option::Option::Some(
                                    unsafe { ::bstack_raii::Foreign::<#ftarget>::from_repr(__p) })
                            }
                        })
                    } else {
                        quote!(unsafe { ::bstack_raii::Foreign::<#ftarget>::from_repr(__arr[#k]) })
                    }
                };
                let acc_body = nested_build(&adims, &leaf_ty, &acc_read);
                accessors.push(quote! {
                    #vis fn #getter<'__f>(
                        &self,
                        stack: &'__f ::bstack_raii::BStack,
                    ) -> ::std::io::Result<#acc_ret> {
                        let mut __buf = ::std::vec![0u8; ::core::mem::size_of::<#on_disk_ty>()];
                        let __r = unsafe { ::bstack_raii::BStackRef::<Self>::from_range(self.0) };
                        let __od: #on_disk_ty = *__r.read_on_disk(stack, &mut __buf)?;
                        let __arr: [::bstack_raii::ForeignRepr; #total] = __od.#fname;
                        ::std::result::Result::Ok(#acc_body)
                    }
                });

                // ---- Constructor: nested `[[Foreign<T>; ..]; ..]` → flat `[ForeignPtr; TOTAL]` ----
                let param_leaf = if aleaf_nullable {
                    quote!(::core::option::Option<#field_ty>)
                } else {
                    field_ty.clone()
                };
                let param_ty = nested_ty(&adims, &param_leaf);
                ctor_params.push(quote!(#fname: #param_ty,));
                let ctor_write = |k: &Ident, leaf: &Ident| {
                    if aleaf_nullable {
                        quote!(__slots[#k] = match #leaf {
                            ::core::option::Option::Some(__f) => __f.repr(),
                            ::core::option::Option::None => ::bstack_raii::ForeignRepr::new(0, 0),
                        };)
                    } else {
                        quote!(__slots[#k] = #leaf.repr();)
                    }
                };
                let flatten = nested_consume(&adims, &quote!(#fname), &ctor_write);
                ctor_preps.push(quote! {
                    let #fname: [::bstack_raii::ForeignRepr; #total] = {
                        let mut __slots = [::bstack_raii::ForeignRepr::new(0, 0); #total];
                        #flatten
                        __slots
                    };
                });
                ctor_inits.push(quote!(#fname: #fname,));

                // ---- Teardown: dispatch each slot (inline; nothing else to free) ----
                let elem_drop = foreign_elem_drop(kind, ftarget);
                let drop_body = if matches!(kind, Kind::Ref) {
                    quote!()
                } else {
                    quote! {
                        let __arr: [::bstack_raii::ForeignRepr; #total] = __on_disk.#fname;
                        for __k in 0usize..(#total) {
                            let __fp = __arr[__k];
                            #elem_drop
                        }
                    }
                };
                drop_stmts.push(quote! { { #drop_body } });

                // ---- Clone: dispatch each slot into a fresh `[ForeignPtr; TOTAL]` ----
                let elem_clone = foreign_elem_clone(kind, ftarget);
                clone_stmts.push(quote! {
                    {
                        let __arr: [::bstack_raii::ForeignRepr; #total] = __od.#fname;
                        let mut __narr: [::bstack_raii::ForeignRepr; #total] = __arr;
                        for __k in 0usize..(#total) {
                            let __fp = __arr[__k];
                            #elem_clone
                            __narr[__k] = __newfp;
                        }
                        __od.#fname = __narr;
                    }
                });

                // ---- Move: materialize the nested `[[Foreign<T>; ..]; ..]` values ----
                let cap = format_ident!("__cap_{}", fname);
                mv_caps.push(quote!(let #cap = __od.#fname;));
                let mv_leaf = if aleaf_nullable {
                    quote!(::core::option::Option<::bstack_raii::Foreign<'__mv, #ftarget>>)
                } else {
                    quote!(::bstack_raii::Foreign<'__mv, #ftarget>)
                };
                mv_types.push(nested_ty(&adims, &mv_leaf));
                let mv_read = |k: &Ident| {
                    // SAFETY: each slot repr was stored into this file; bound to `'__mv`.
                    if aleaf_nullable {
                        quote!({
                            let __p = #cap[#k];
                            if __p.offset() == 0 {
                                ::core::option::Option::None
                            } else {
                                ::core::option::Option::Some(
                                    unsafe { ::bstack_raii::Foreign::<#ftarget>::from_repr(__p) })
                            }
                        })
                    } else {
                        quote!(unsafe { ::bstack_raii::Foreign::<#ftarget>::from_repr(#cap[#k]) })
                    }
                };
                mv_recon.push(nested_build(&adims, &mv_leaf, &mv_read));
                continue;
            }
        }

        // Inline fixed-size array `[T; N]` — possibly *nested* `[[..]; ..]` — of
        // block references. (A POD array falls through to the POD path below: an
        // array of `Pod` is `Pod`.) Stored **flat** as `[u64; N0*..*Nk]` inline
        // (no data block), one offset per leaf, with per-element ownership; the
        // accessor / ctor / move traffic in the nested `[[Handle; ..]; ..]` shape.
        if kind != Kind::Pod
            && let Type::Array(_) = opt_inner
        {
            if nullable {
                return Err(Error::new_spanned(
                    &field.ty,
                    "a whole-array `Option<[T; N]>` is not supported; use `[Option<T>; N]` \
                     for per-element nullability",
                ));
            }
            let (dims, elem, elem_nullable) = array_shape(opt_inner)?;
            reject_nested_const_dims(&dims, &const_params, &field.ty)?;
            let total = dims_prod(&dims);

            // `#[embed] [Child; N]` (or nested): N verbatim child on-disk forms
            // inline (`[<Child as BStackBlock>::OnDisk; TOTAL]`, flat). Construction
            // folds each `BStackOwned<Child>` in (read OnDisk, copy, free shell).
            if kind == Kind::Embed {
                if elem_nullable {
                    return Err(Error::new_spanned(
                        &field.ty,
                        "#[embed] does not support `Option`",
                    ));
                }
                if is_bstack_mut(&field.attrs) {
                    return Err(Error::new_spanned(
                        field,
                        "#[bstack_mut] is not yet supported on #[embed] fields",
                    ));
                }
                let child = elem;
                // Guard: `#[embed]` target must be a plain, self-contained block.
                if !type_mentions_any(child, &type_params) {
                    wrapper_defs.push(quote! {
                        const _: fn() = || {
                            fn __assert_embeddable<__T: ::bstack_raii::__private::BStackEmbeddable>() {}
                            __assert_embeddable::<#child>();
                        };
                    });
                }
                let child_od = quote!(<#child as ::bstack_raii::BStackBlock>::OnDisk);
                on_disk_fields.push(quote!(#fname: [#child_od; #total],));

                // Teardown: free each embedded child's children in place.
                drop_stmts.push(quote! {
                    {
                        let __base =
                            __range.start() + ::core::mem::offset_of!(#on_disk_ty, #fname) as u64;
                        let __step = ::core::mem::size_of::<#child_od>() as u64;
                        for __k in 0usize..(#total) {
                            let __embed = ::bstack_raii::BStackRange::new(
                                __base + (__k as u64) * __step, __step);
                            <#child>::__bstack_drop_children(__embed, allocator)?;
                        }
                    }
                });

                // Accessor: nested `[[Child; ..]; ..]`, each a handle into its slot.
                let acc_ret = nested_ty(&dims, &quote!(#child));
                let acc_read = |k: &Ident| {
                    quote!(<#child as ::bstack_raii::BStackBlock>::from_range(
                        ::bstack_raii::BStackRange::new(__base + (#k as u64) * __step, __step)))
                };
                let acc_body = nested_build(&dims, &quote!(#child), &acc_read);
                accessors.push(quote! {
                    #vis fn #getter(&self) -> #acc_ret {
                        let __base =
                            self.0.start() + ::core::mem::offset_of!(#on_disk_ty, #fname) as u64;
                        let __step = ::core::mem::size_of::<#child_od>() as u64;
                        #acc_body
                    }
                });

                // Constructor: flatten the nested owned array to `[BStackRange; TOTAL]`
                // (each child's source block), zero the slots, and `copy` each child
                // into place post-write (then free the shell) — no materialising.
                let src_id = format_ident!("__embed_src_{}", fname);
                let param_ty = nested_ty(&dims, &quote!(::bstack_raii::BStackOwned<#child>));
                ctor_params.push(quote!(#fname: #param_ty,));
                let cap_write = |k: &Ident, leaf: &Ident| {
                    quote! {
                        #src_id[#k] = {
                            let __h = #leaf.into_inner();
                            ::bstack_raii::BStackBlock::range(&__h)
                        };
                    }
                };
                let flatten = nested_consume(&dims, &quote!(#fname), &cap_write);
                ctor_preps.push(quote! {
                    let #src_id: [::bstack_raii::BStackRange; #total] = {
                        let mut #src_id = [::bstack_raii::BStackRange::new(0, 0); #total];
                        #flatten
                        #src_id
                    };
                });
                ctor_inits.push(
                    quote!(#fname: [<#child_od as ::bstack_raii::Zeroable>::zeroed(); #total],),
                );
                ctor_post.push(quote! {
                    {
                        let __base =
                            __data.start() + ::core::mem::offset_of!(#on_disk_ty, #fname) as u64;
                        let __step = ::core::mem::size_of::<#child_od>() as u64;
                        for __k in 0usize..(#total) {
                            let __src = #src_id[__k];
                            allocator.stack().copy(
                                __src.start(), __base + (__k as u64) * __step, __step)?;
                            unsafe { ::bstack_raii::dealloc_range(allocator, __src)?; }
                        }
                    }
                });

                // Move: re-home each embedded child to a fresh standalone allocation.
                let cap = format_ident!("__cap_{}", fname);
                mv_caps.push(quote!(let #cap = __od.#fname;));
                mv_types.push(nested_ty(
                    &dims,
                    &quote!(::bstack_raii::BStackOwned<#child>),
                ));
                let mv_read = |k: &Ident| {
                    quote! {{
                        let __cod = #cap[#k];
                        let mut __slice =
                            __alloc.alloc(::core::mem::size_of::<#child_od>() as u64)?;
                        let __r = __slice.as_range();
                        if let ::std::result::Result::Err(__e) =
                            __slice.write_range(0, ::bstack_raii::bytemuck::bytes_of(&__cod))
                        {
                            let _ = __alloc.dealloc(__slice);
                            return ::std::result::Result::Err(__e);
                        }
                        unsafe {
                            ::bstack_raii::BStackOwned::from_raw(
                                <#child as ::bstack_raii::BStackBlock>::from_range(__r))
                        }
                    }}
                };
                mv_recon.push(nested_build(
                    &dims,
                    &quote!(::bstack_raii::BStackOwned<#child>),
                    &mv_read,
                ));

                // Clone: fold each embedded child's clone inline (flat; copy the
                // array out, mutate, write back — packed fields can't be `&mut`'d).
                clone_stmts.push(quote! {
                    {
                        let __base =
                            __src.start() + ::core::mem::offset_of!(#on_disk_ty, #fname) as u64;
                        let __step = ::core::mem::size_of::<#child_od>() as u64;
                        let mut __arr: [#child_od; #total] = __od.#fname;
                        for __k in 0usize..(#total) {
                            let __child = <#child as ::bstack_raii::BStackBlock>::from_range(
                                ::bstack_raii::BStackRange::new(
                                    __base + (__k as u64) * __step, __step));
                            __arr[__k] =
                                __child.__bstack_clone_children_inplace(allocator, __plan)?;
                        }
                        __od.#fname = __arr;
                    }
                });
                continue;
            }

            on_disk_fields.push(quote!(#fname: [u64; #total],));
            let size_elem = quote! {
                ::core::mem::size_of::<<#elem as ::bstack_raii::BStackBlock>::OnDisk>() as u64
            };

            // A weak array stores control offsets (`0` = unset), is not a ctor
            // parameter (starts null, wired per flat index via a setter), and its
            // accessor upgrades each element (address-based).
            if kind == Kind::Weak {
                let ctrl_ty = quote!(<#elem as ::bstack_raii::BStackWeakable>::Control);
                let ctrl_size = quote!(::core::mem::size_of::<#ctrl_ty>() as u64);
                ctor_inits.push(quote!(#fname: [0u64; #total],));

                let setter = format_ident!("set_{}", fname);
                setters.push(quote! {
                    #vis fn #setter<'__s, __A: ::bstack_raii::BStackRaiiAllocator>(
                        &self,
                        allocator: &'__s __A,
                        index: usize,
                        weak: ::bstack_raii::BStackWeak<'__s, #elem, __A>,
                    ) -> ::std::io::Result<()> {
                        let __field = self.0.start()
                            + ::core::mem::offset_of!(#on_disk_ty, #fname) as u64
                            + (index as u64) * 8;
                        ::bstack_raii::set_weak_field(allocator, __field, weak)
                    }
                });

                let leaf_ty =
                    quote!(::core::option::Option<::bstack_raii::BStackRc<'__u, #elem, __A>>);
                let acc_ret = nested_ty(&dims, &leaf_ty);
                let acc_read = |k: &Ident| {
                    quote!(::bstack_raii::upgrade_weak_field(
                        allocator, __base + (#k as u64) * 8)?)
                };
                let acc_body = nested_build(&dims, &leaf_ty, &acc_read);
                accessors.push(quote! {
                    #vis fn #getter<'__u, __A: ::bstack_raii::BStackRaiiAllocator>(
                        &self,
                        allocator: &'__u __A,
                    ) -> ::std::io::Result<#acc_ret> {
                        let __base =
                            self.0.start() + ::core::mem::offset_of!(#on_disk_ty, #fname) as u64;
                        ::std::result::Result::Ok(#acc_body)
                    }
                });

                // Teardown: release each non-null weak reference.
                drop_stmts.push(quote! {
                    {
                        let __offs: [u64; #total] = __on_disk.#fname;
                        for __off in __offs {
                            if __off != 0 {
                                let __ctrl = unsafe {
                                    ::bstack_raii::BStackRef::<#ctrl_ty>::from_range(
                                        ::bstack_raii::BStackRange::new(__off, #ctrl_size))
                                };
                                ::bstack_raii::WeakRef::<#elem>(__ctrl).bstack_drop(allocator)?;
                            }
                        }
                    }
                });

                // Clone: bump each non-null weak count (offsets kept — a weak clone
                // aliases the same control block).
                clone_stmts.push(quote! {
                    {
                        let __offs: [u64; #total] = __od.#fname;
                        for __off in __offs {
                            if __off != 0 {
                                __plan.bump_weak(__off);
                            }
                        }
                    }
                });

                // Move: nested `[[Option<BStackWeak>; ..]; ..]` from flat offsets.
                let cap = format_ident!("__cap_{}", fname);
                mv_caps.push(quote!(let #cap = __od.#fname;));
                let mv_leaf_ty =
                    quote!(::core::option::Option<::bstack_raii::BStackWeak<'__mv, #elem, __A>>);
                mv_types.push(nested_ty(&dims, &mv_leaf_ty));
                let mv_read = |k: &Ident| {
                    quote! {{
                        let __off = #cap[#k];
                        if __off == 0 {
                            ::core::option::Option::None
                        } else {
                            let __ctrl = unsafe {
                                ::bstack_raii::BStackRef::<#ctrl_ty>::from_range(
                                    ::bstack_raii::BStackRange::new(__off, #ctrl_size))
                            };
                            ::core::option::Option::Some(unsafe {
                                ::bstack_raii::BStackWeak::from_raw(__ctrl, __alloc)
                            })
                        }
                    }}
                };
                mv_recon.push(nested_build(&dims, &mv_leaf_ty, &mv_read));
                continue;
            }

            // Owned / strong / ref: nested `[[Handle; ..]; ..]`, value-based from
            // the flat offsets. A `0` slot is `None` for an `Option`-element array.
            let leaf_view = if elem_nullable {
                quote!(::core::option::Option<#elem>)
            } else {
                quote!(#elem)
            };
            let acc_ret = nested_ty(&dims, &leaf_view);
            let acc_read = |k: &Ident| {
                if elem_nullable {
                    quote!({
                        let __o = __offs[#k];
                        if __o == 0 {
                            ::core::option::Option::None
                        } else {
                            ::core::option::Option::Some(
                                <#elem as ::bstack_raii::BStackBlock>::from_range(
                                    ::bstack_raii::BStackRange::new(__o, #size_elem)))
                        }
                    })
                } else {
                    quote!(<#elem as ::bstack_raii::BStackBlock>::from_range(
                        ::bstack_raii::BStackRange::new(__offs[#k], #size_elem)))
                }
            };
            let acc_body = nested_build(&dims, &leaf_view, &acc_read);
            accessors.push(quote! {
                #vis fn #getter(
                    &self,
                    stack: &::bstack_raii::BStack,
                ) -> ::std::io::Result<#acc_ret> {
                    let mut __buf = ::std::vec![0u8; ::core::mem::size_of::<#on_disk_ty>()];
                    let __r = unsafe { ::bstack_raii::BStackRef::<Self>::from_range(self.0) };
                    let __od: #on_disk_ty = *__r.read_on_disk(stack, &mut __buf)?;
                    let __offs: [u64; #total] = __od.#fname;
                    ::std::result::Result::Ok(#acc_body)
                }
            });

            // Constructor: nested `[[Handle; ..]; ..]` → flat `[u64; TOTAL]`.
            let handle_ty = match kind {
                Kind::Owned => quote!(::bstack_raii::BStackOwned<#elem>),
                Kind::Strong => quote!(::bstack_raii::BStackRc<'__ctor, #elem, __A>),
                Kind::Ref => quote!(::bstack_raii::BStackRef<#elem>),
                _ => unreachable!(),
            };
            let off_of = |h: &Ident| match kind {
                Kind::Owned => quote!({
                    let __h = #h.into_inner();
                    ::bstack_raii::BStackBlock::range(&__h).start()
                }),
                Kind::Strong => quote!({
                    let (__d, _) = #h.into_raw();
                    __d.into_range().start()
                }),
                Kind::Ref => quote!(#h.into_range().start()),
                _ => unreachable!(),
            };
            let ctor_leaf_ty = if elem_nullable {
                quote!(::core::option::Option<#handle_ty>)
            } else {
                quote!(#handle_ty)
            };
            let ctor_param_ty = nested_ty(&dims, &ctor_leaf_ty);
            ctor_params.push(quote!(#fname: #ctor_param_ty,));
            let ctor_write = |k: &Ident, leaf: &Ident| {
                if elem_nullable {
                    let h = format_ident!("__handle");
                    let off = off_of(&h);
                    quote! {
                        __a[#k] = match #leaf {
                            ::core::option::Option::Some(#h) => #off,
                            ::core::option::Option::None => 0u64,
                        };
                    }
                } else {
                    let off = off_of(leaf);
                    quote!(__a[#k] = #off;)
                }
            };
            let flatten = nested_consume(&dims, &quote!(#fname), &ctor_write);
            ctor_preps.push(quote! {
                let #fname: [u64; #total] = {
                    let mut __a = [0u64; #total];
                    #flatten
                    __a
                };
            });
            ctor_inits.push(quote!(#fname: #fname,));

            // Teardown: free / release each non-null element (a ref owns nothing).
            let per_teardown = match kind {
                Kind::Owned => quote! {
                    ::bstack_raii::OwnedRef(unsafe {
                        ::bstack_raii::BStackRef::<#elem>::from_range(
                            ::bstack_raii::BStackRange::new(__off, #size_elem))
                    }).bstack_drop(allocator)?;
                },
                Kind::Strong => quote! {
                    <#elem as ::bstack_raii::BStackShared>::drop_strong_ref(unsafe {
                        ::bstack_raii::BStackRef::<#elem>::from_range(
                            ::bstack_raii::BStackRange::new(__off, #size_elem))
                    }, allocator)?;
                },
                _ => quote!(),
            };
            if kind != Kind::Ref {
                drop_stmts.push(quote! {
                    {
                        let __offs: [u64; #total] = __on_disk.#fname;
                        for __off in __offs {
                            if __off != 0 { #per_teardown }
                        }
                    }
                });
            }

            // Clone: owned deep-clones each; strong bumps each; ref aliases.
            match kind {
                Kind::Owned => clone_stmts.push(quote! {
                    {
                        let mut __arr: [u64; #total] = __od.#fname;
                        for __k in 0usize..(#total) {
                            let __off = __arr[__k];
                            if __off != 0 {
                                let __child = <#elem as ::bstack_raii::BStackBlock>::from_range(
                                    ::bstack_raii::BStackRange::new(__off, #size_elem));
                                __arr[__k] =
                                    __child.__bstack_clone_into(allocator, __plan)?.start();
                            }
                        }
                        __od.#fname = __arr;
                    }
                }),
                Kind::Strong => clone_stmts.push(quote! {
                    {
                        let __offs: [u64; #total] = __od.#fname;
                        for __off in __offs {
                            if __off != 0 {
                                let __child = unsafe {
                                    ::bstack_raii::BStackRef::<#elem>::from_range(
                                        ::bstack_raii::BStackRange::new(__off, #size_elem))
                                };
                                __plan.bump_strong(__child, allocator)?;
                            }
                        }
                    }
                }),
                // Ref: aliased — the copied `[u64; TOTAL]` is kept verbatim.
                _ => {}
            }

            // Move: nested `[[Handle; ..]; ..]` from flat offsets.
            let cap = format_ident!("__cap_{}", fname);
            mv_caps.push(quote!(let #cap = __od.#fname;));
            let (mv_leaf, build_one): (TokenStream, TokenStream) = match kind {
                Kind::Owned => (
                    quote!(::bstack_raii::BStackOwned<#elem>),
                    quote!(unsafe {
                        ::bstack_raii::BStackOwned::from_raw(
                            <#elem as ::bstack_raii::BStackBlock>::from_range(
                                ::bstack_raii::BStackRange::new(__off, #size_elem)))
                    }),
                ),
                Kind::Ref => (
                    quote!(::bstack_raii::BStackRef<#elem>),
                    quote!(unsafe {
                        ::bstack_raii::BStackRef::<#elem>::from_range(
                            ::bstack_raii::BStackRange::new(__off, #size_elem))
                    }),
                ),
                Kind::Strong => (
                    quote!(::bstack_raii::BStackRc<'__mv, #elem, __A>),
                    quote!({
                        let __data = unsafe {
                            ::bstack_raii::BStackRef::<#elem>::from_range(
                                ::bstack_raii::BStackRange::new(__off, #size_elem))
                        };
                        let (__d, __c) =
                            <#elem as ::bstack_raii::BStackShared>::strong_parts(__data, __alloc)?;
                        unsafe { ::bstack_raii::BStackRc::from_raw(__d, __c, __alloc) }
                    }),
                ),
                _ => unreachable!(),
            };
            let mv_leaf_ty = if elem_nullable {
                quote!(::core::option::Option<#mv_leaf>)
            } else {
                mv_leaf.clone()
            };
            mv_types.push(nested_ty(&dims, &mv_leaf_ty));
            let mv_read = |k: &Ident| {
                if elem_nullable {
                    quote! {{
                        let __off = #cap[#k];
                        if __off == 0 {
                            ::core::option::Option::None
                        } else {
                            ::core::option::Option::Some(#build_one)
                        }
                    }}
                } else {
                    quote! {{
                        let __off = #cap[#k];
                        #build_one
                    }}
                }
            };
            mv_recon.push(nested_build(&dims, &mv_leaf_ty, &mv_read));

            // `#[bstack_mut]`: element `replace_<f>_at` + whole-array `replace_<f>`
            // (and `set_` for `ref`). Weak arrays already have a `set_<f>` element
            // setter unconditionally; embed arrays are rejected above.
            if is_bstack_mut(&field.attrs) {
                for m in array_mut_methods(
                    vis,
                    fname,
                    &quote!(#elem),
                    &on_disk_ty,
                    kind,
                    &dims,
                    &total,
                    &size_elem,
                    elem_nullable,
                ) {
                    accessors.push(m);
                }
            }
            continue;
        }

        // Decide the stored type + nullability now that vectors are handled.
        //
        // * A **reference** kind (owned/strong/weak/ref) lowers to a `u64` offset;
        //   `Option<T>` makes it nullable.
        // * A **POD** field stores its *whole* type inline. That includes an
        //   `Option<A>` — stored via the bytemuck `PodInOption` niche (so
        //   `Option<A>: Pod` iff `A: PodInOption`) — so no annotation is needed and
        //   the accessor/`bstack_move!` hand back the `Option<A>` by value.
        let (inner_ty, nullable) = if kind == Kind::Pod {
            (eff_ty, false)
        } else {
            (opt_inner, nullable)
        };

        // A **tuple with ≥1 `Foreign` element**: `#[ann] (A, Foreign<T>, Option<Foreign<U>>, ..)`.
        // POD elements store inline; each foreign element stores as a `ForeignPtr` (so
        // the packed wrapper stays `Pod`). The field annotation names the ownership of
        // *all* the foreign elements — they are freed / decremented / deep-cloned in
        // their own files at teardown / clone. `Option<Foreign<T>>` elements use the
        // offset-0 niche. (Concrete element types only for now — no generic params.)
        if let Type::Tuple(tup) = inner_ty
            && tup
                .elems
                .iter()
                .any(|e| foreign_inner(option_inner(e).unwrap_or(e)).is_some())
        {
            // Generic foreign *targets* are allowed (bounds are inferred above); a
            // generic param in a POD element was already rejected in the usage pass.
            match kind {
                Kind::Owned | Kind::Strong | Kind::Weak | Kind::Ref => {}
                Kind::Pod => {
                    return Err(Error::new_spanned(
                        &field.ty,
                        "a tuple containing a `Foreign` needs an ownership annotation \
                         (`#[bstack_owned/strong/weak/ref]`) naming the foreign elements' kind",
                    ));
                }
                Kind::Embed => {
                    return Err(Error::new_spanned(&field.ty, "cannot #[embed] a tuple"));
                }
            }
            if nullable {
                return Err(Error::new_spanned(
                    &field.ty,
                    "a whole-tuple `Option<(..)>` is not supported; make the individual \
                     elements nullable instead",
                ));
            }

            // Per-element: is it foreign (and null-wrapped), and its target.
            let mut is_foreign = Vec::with_capacity(tup.elems.len());
            let mut ftargets: Vec<Option<&Type>> = Vec::with_capacity(tup.elems.len());
            let mut nulls = Vec::with_capacity(tup.elems.len());
            for e in &tup.elems {
                let inner = option_inner(e).unwrap_or(e);
                if let Some(ft) = foreign_inner(inner) {
                    reject_bad_foreign_target(ft, &field.ty, "a `Foreign` tuple element")?;
                    is_foreign.push(true);
                    ftargets.push(Some(ft));
                    nulls.push(option_inner(e).is_some());
                } else {
                    is_foreign.push(false);
                    ftargets.push(None);
                    nulls.push(false);
                    pod_types.push(e);
                }
            }

            let n = tup.elems.len();
            let idx: Vec<syn::Index> = (0..n).map(syn::Index::from).collect();
            // The PUBLIC tuple type (accessor / ctor / move): `Foreign` is a token, so
            // rewrite each foreign element to the real `::bstack_raii::Foreign<T>` (the
            // user's bare `Foreign` isn't in scope in the generated impls).
            // Build the public tuple type with a given lifetime on each `Foreign`
            // element (`None` ⇒ elided, for the by-value ctor param). The accessor binds
            // `'__f` (its `stack` borrow) and the move binds `'__mv`, so a `SELF` element
            // cannot escape the file / block it came from.
            let mk_tuple_ty = |lt: Option<&syn::Lifetime>| -> TokenStream {
                let elems: Vec<TokenStream> = (0..n)
                    .map(|i| {
                        if is_foreign[i] {
                            let ft = ftargets[i].unwrap();
                            let f = match lt {
                                Some(l) => quote!(::bstack_raii::Foreign<#l, #ft>),
                                None => quote!(::bstack_raii::Foreign<#ft>),
                            };
                            if nulls[i] {
                                quote!(::core::option::Option<#f>)
                            } else {
                                f
                            }
                        } else {
                            let e = &tup.elems[i];
                            quote!(#e)
                        }
                    })
                    .collect();
                quote!(( #(#elems,)* ))
            };
            let lt_f = syn::Lifetime::new("'__f", Span::call_site());
            let lt_mv = syn::Lifetime::new("'__mv", Span::call_site());
            let pub_tuple_ty = mk_tuple_ty(None);
            let acc_tuple_ty = mk_tuple_ty(Some(&lt_f));
            let mv_tuple_ty = mk_tuple_ty(Some(&lt_mv));
            let wrapper = format_ident!("__BstackFTup_{}_{}", name, fname);
            // Wrapper element types: POD verbatim, foreign → `ForeignPtr`.
            let welem: Vec<TokenStream> = tup
                .elems
                .iter()
                .enumerate()
                .map(|(i, e)| {
                    if is_foreign[i] {
                        quote!(::bstack_raii::ForeignRepr)
                    } else {
                        quote!(#e)
                    }
                })
                .collect();
            wrapper_defs.push(quote! {
                #[repr(C, packed)]
                #[derive(::core::clone::Clone, ::core::marker::Copy)]
                #[doc(hidden)]
                #vis struct #wrapper( #(#welem),* );
                // SAFETY: `#[repr(C, packed)]` => no padding; every element is `Pod`
                // (POD elements asserted via `pod_types`; `ForeignPtr` is `Pod`).
                unsafe impl ::bstack_raii::Zeroable for #wrapper {}
                unsafe impl ::bstack_raii::Pod for #wrapper {}
            });
            on_disk_fields.push(quote!(#fname: #wrapper,));

            // Accessor: rebuild the tuple, mapping each `ForeignRepr` back to a `Foreign`.
            // SAFETY: each element repr was stored into this file; the returned
            // `Foreign`s are `'__f`-bound to `stack` by the accessor signature.
            let acc_elems: Vec<TokenStream> = (0..n)
                .map(|i| {
                    let ix = &idx[i];
                    if is_foreign[i] {
                        let ft = ftargets[i].unwrap();
                        if nulls[i] {
                            quote!(if __w.#ix.offset() == 0 {
                                ::core::option::Option::None
                            } else {
                                ::core::option::Option::Some(
                                    unsafe { ::bstack_raii::Foreign::<#ft>::from_repr(__w.#ix) })
                            })
                        } else {
                            quote!(unsafe { ::bstack_raii::Foreign::<#ft>::from_repr(__w.#ix) })
                        }
                    } else {
                        quote!(__w.#ix)
                    }
                })
                .collect();
            accessors.push(quote! {
                #vis fn #getter<'__f>(
                    &self,
                    stack: &'__f ::bstack_raii::BStack,
                ) -> ::std::io::Result<#acc_tuple_ty> {
                    let mut __buf = ::std::vec![0u8; ::core::mem::size_of::<#on_disk_ty>()];
                    let __r = unsafe { ::bstack_raii::BStackRef::<Self>::from_range(self.0) };
                    let __od: #on_disk_ty = *__r.read_on_disk(stack, &mut __buf)?;
                    let __w = __od.#fname;
                    ::std::result::Result::Ok(( #(#acc_elems,)* ))
                }
            });

            // Constructor: map each foreign element to a `ForeignPtr`, POD verbatim.
            let ctor_elems: Vec<TokenStream> = (0..n)
                .map(|i| {
                    let ix = &idx[i];
                    if is_foreign[i] {
                        if nulls[i] {
                            quote!(match #fname.#ix {
                                ::core::option::Option::Some(__f) => __f.repr(),
                                ::core::option::Option::None => ::bstack_raii::ForeignRepr::new(0, 0),
                            })
                        } else {
                            quote!(#fname.#ix.repr())
                        }
                    } else {
                        quote!(#fname.#ix)
                    }
                })
                .collect();
            ctor_params.push(quote!(#fname: #pub_tuple_ty,));
            ctor_preps.push(quote!(let #fname: #wrapper = #wrapper( #(#ctor_elems),* );));
            ctor_inits.push(quote!(#fname: #fname,));

            // Teardown / clone: dispatch each foreign element from the on-disk wrapper.
            let mut tup_drops = Vec::new();
            let mut tup_clones = Vec::new();
            for i in 0..n {
                if !is_foreign[i] {
                    continue;
                }
                let ix = &idx[i];
                let ft = ftargets[i].unwrap();
                let elem_drop = foreign_elem_drop(kind, ft);
                tup_drops.push(quote! {
                    {
                        let __fp: ::bstack_raii::ForeignRepr = __w.#ix;
                        #elem_drop
                    }
                });
                let elem_clone = foreign_elem_clone(kind, ft);
                tup_clones.push(quote! {
                    {
                        let __fp: ::bstack_raii::ForeignRepr = __w.#ix;
                        #elem_clone
                        __w.#ix = __newfp;
                    }
                });
            }
            if !matches!(kind, Kind::Ref) {
                drop_stmts.push(quote! {
                    {
                        let __w = __on_disk.#fname;
                        #(#tup_drops)*
                    }
                });
                clone_stmts.push(quote! {
                    {
                        let mut __w = __od.#fname;
                        #(#tup_clones)*
                        __od.#fname = __w;
                    }
                });
            }

            // Move: rebuild the tuple (same mapping as the accessor), `'__mv`-bound.
            // SAFETY: each element repr was stored into this file; bound to `'__mv`.
            let cap = format_ident!("__cap_{}", fname);
            mv_caps.push(quote!(let #cap = __od.#fname;));
            mv_types.push(quote!(#mv_tuple_ty));
            let mv_elems: Vec<TokenStream> = (0..n)
                .map(|i| {
                    let ix = &idx[i];
                    if is_foreign[i] {
                        let ft = ftargets[i].unwrap();
                        if nulls[i] {
                            quote!(if #cap.#ix.offset() == 0 {
                                ::core::option::Option::None
                            } else {
                                ::core::option::Option::Some(
                                    unsafe { ::bstack_raii::Foreign::<#ft>::from_repr(#cap.#ix) })
                            })
                        } else {
                            quote!(unsafe { ::bstack_raii::Foreign::<#ft>::from_repr(#cap.#ix) })
                        }
                    } else {
                        quote!(#cap.#ix)
                    }
                })
                .collect();
            mv_recon.push(quote!(( #(#mv_elems,)* )));
            continue;
        }

        // A POD **tuple** field `a: (A, B, ..)`: a Rust tuple is not `Pod`, but a
        // packed struct of its (POD) elements is — alignment is irrelevant on disk
        // — so store it through a generated wrapper and rebuild the tuple on read.
        // `bstack_move!` hands back the tuple as one element (not flattened).
        if kind == Kind::Pod
            && let Type::Tuple(tup) = inner_ty
        {
            let elems: Vec<&Type> = tup.elems.iter().collect();
            let wrapper = format_ident!("__BstackTup_{}_{}", name, fname);
            let idx: Vec<syn::Index> = (0..elems.len()).map(syn::Index::from).collect();
            wrapper_defs.push(quote! {
                #[repr(C, packed)]
                #[derive(::core::clone::Clone, ::core::marker::Copy)]
                #[doc(hidden)]
                #vis struct #wrapper( #(#elems),* );
                // SAFETY: `#[repr(C, packed)]` => no padding; every element is
                // `Pod` (asserted below), so all bit patterns are valid.
                unsafe impl ::bstack_raii::Zeroable for #wrapper {}
                unsafe impl ::bstack_raii::Pod for #wrapper {}
            });
            pod_types.extend(elems.iter().copied());
            on_disk_fields.push(quote!(#fname: #wrapper,));
            accessors.push(quote! {
                #vis fn #getter(
                    &self,
                    stack: &::bstack_raii::BStack,
                ) -> ::std::io::Result<#inner_ty> {
                    let mut __buf = ::std::vec![0u8; ::core::mem::size_of::<#on_disk_ty>()];
                    let __r = unsafe { ::bstack_raii::BStackRef::<Self>::from_range(self.0) };
                    let __od: #on_disk_ty = *__r.read_on_disk(stack, &mut __buf)?;
                    let __w = __od.#fname;
                    ::std::result::Result::Ok(( #(__w.#idx,)* ))
                }
            });
            ctor_params.push(quote!(#fname: #inner_ty,));
            ctor_preps.push(quote!(let #fname: #wrapper = #wrapper( #(#fname.#idx),* );));
            ctor_inits.push(quote!(#fname: #fname,));
            let cap = format_ident!("__cap_{}", fname);
            mv_caps.push(quote!(let #cap = __od.#fname;));
            mv_types.push(quote!(#inner_ty));
            mv_recon.push(quote!(( #(#cap.#idx,)* )));
            // `#[bstack_mut]`: overwrite the whole inline POD tuple, one atomic `set`
            // (a POD tuple owns no children, so nothing is freed — like a POD scalar).
            if is_bstack_mut(&field.attrs) {
                let setter = format_ident!("set_{}", fname);
                let idx2 = idx.clone();
                accessors.push(quote! {
                    /// Overwrite this POD tuple field, as one crash-atomic `set`.
                    #vis fn #setter(
                        &self,
                        stack: &::bstack_raii::BStack,
                        value: #inner_ty,
                    ) -> ::std::io::Result<()> {
                        let __w: #wrapper = #wrapper( #(value.#idx2),* );
                        let __off = self.0.start()
                            + ::core::mem::offset_of!(#on_disk_ty, #fname) as u64;
                        stack.set(__off, ::bstack_raii::bytemuck::bytes_of(&__w))
                    }
                });
            }
            continue;
        }

        // `#[embed] child: Block`: store the child's whole on-disk form INLINE
        // (`<Child as BStackBlock>::OnDisk`, header and all) instead of a `u64`
        // offset — an exclusively-owned inline block.
        if kind == Kind::Embed {
            if let Type::Tuple(_) = inner_ty {
                return Err(Error::new_spanned(
                    &field.ty,
                    "cannot #[embed] a tuple — embed a `#[bstack_block]` / `#[bstack_enum]` type",
                ));
            }
            if nullable {
                return Err(Error::new_spanned(
                    &field.ty,
                    "#[embed] does not support `Option`",
                ));
            }
            // `#[embed]` fields `continue` before the scalar mutator injection, so a
            // `#[bstack_mut]` here would be silently ignored — reject it explicitly.
            if is_bstack_mut(&field.attrs) {
                return Err(Error::new_spanned(
                    field,
                    "#[bstack_mut] is not yet supported on #[embed] fields",
                ));
            }
            let child = inner_ty;
            // Guard: an `#[embed]` target must be a plain, self-contained block
            // (`BStackEmbeddable`) — never `(rc)` / `(rc, weak)`, whose refcount /
            // separate control block embedding would strand. A concrete target gets a
            // direct assertion here; a generic one is bounded via `Usage` above.
            if !type_mentions_any(child, &type_params) {
                wrapper_defs.push(quote! {
                    const _: fn() = || {
                        fn __assert_embeddable<__T: ::bstack_raii::__private::BStackEmbeddable>() {}
                        __assert_embeddable::<#child>();
                    };
                });
            }
            let child_od = quote!(<#child as ::bstack_raii::BStackBlock>::OnDisk);
            on_disk_fields.push(quote!(#fname: #child_od,));

            // Teardown: free the embedded child's own children *in place* (its
            // storage is part of this block, so no separate dealloc). `__range` is
            // this block's range, bound by `__bstack_drop_children`.
            drop_stmts.push(quote! {
                {
                    let __embed = ::bstack_raii::BStackRange::new(
                        __range.start() + ::core::mem::offset_of!(#on_disk_ty, #fname) as u64,
                        ::core::mem::size_of::<#child_od>() as u64,
                    );
                    <#child>::__bstack_drop_children(__embed, allocator)?;
                }
            });

            // Accessor: a child handle at the embedded offset (pure offset math).
            accessors.push(quote! {
                #vis fn #getter(&self) -> #child {
                    <#child as ::bstack_raii::BStackBlock>::from_range(
                        ::bstack_raii::BStackRange::new(
                            self.0.start() + ::core::mem::offset_of!(#on_disk_ty, #fname) as u64,
                            ::core::mem::size_of::<#child_od>() as u64,
                        ),
                    )
                }
            });

            // Constructor: capture the child's block range; the OnDisk slot is a
            // zeroed placeholder, and a post-write step `BStack::copy`s the child
            // into it (then frees the child shell) — no materialising the OnDisk.
            let src_id = format_ident!("__embed_src_{}", fname);
            ctor_params.push(quote!(#fname: ::bstack_raii::BStackOwned<#child>,));
            ctor_preps.push(quote! {
                let #src_id = {
                    let __h = #fname.into_inner();
                    ::bstack_raii::BStackBlock::range(&__h)
                };
            });
            ctor_inits.push(quote!(#fname: <#child_od as ::bstack_raii::Zeroable>::zeroed(),));
            ctor_post.push(quote! {
                {
                    allocator.stack().copy(
                        #src_id.start(),
                        __data.start() + ::core::mem::offset_of!(#on_disk_ty, #fname) as u64,
                        ::core::mem::size_of::<#child_od>() as u64,
                    )?;
                    unsafe { ::bstack_raii::dealloc_range(allocator, #src_id)?; }
                }
            });

            // Move: re-home the embedded child to a fresh standalone allocation.
            let cap = format_ident!("__cap_{}", fname);
            mv_caps.push(quote!(let #cap = __od.#fname;));
            mv_types.push(quote!(::bstack_raii::BStackOwned<#child>));
            mv_recon.push(quote! {
                {
                    let mut __slice =
                        __alloc.alloc(::core::mem::size_of::<#child_od>() as u64)?;
                    let __r = __slice.as_range();
                    if let ::std::result::Result::Err(__e) =
                        __slice.write_range(0, ::bstack_raii::bytemuck::bytes_of(&#cap))
                    {
                        let _ = __alloc.dealloc(__slice);
                        return ::std::result::Result::Err(__e);
                    }
                    unsafe {
                        ::bstack_raii::BStackOwned::from_raw(
                            <#child as ::bstack_raii::BStackBlock>::from_range(__r),
                        )
                    }
                }
            });
            // Clone: fold the embedded child's clone inline — deep-clone its own
            // children into the plan and store the fixed-up child OnDisk in place
            // (no separate child allocation, mirroring the in-place teardown).
            clone_stmts.push(quote! {
                {
                    let __child = <#child as ::bstack_raii::BStackBlock>::from_range(
                        ::bstack_raii::BStackRange::new(
                            __src.start() + ::core::mem::offset_of!(#on_disk_ty, #fname) as u64,
                            ::core::mem::size_of::<#child_od>() as u64,
                        ),
                    );
                    __od.#fname =
                        __child.__bstack_clone_children_inplace(allocator, __plan)?;
                }
            });
            continue;
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
            // `#[embed]` is fully handled above (it `continue`s).
            Kind::Ref | Kind::Pod | Kind::Embed => {}
        }

        // Deep clone (mirror of teardown; POD / ref are copied verbatim).
        if let Some(cs) = clone_field_stmt(fname, inner_ty, kind) {
            clone_stmts.push(cs);
        }

        // Accessor: the `get_<field>` reader, the unsafe `raw_<field>_slice` place,
        // and — for `#[bstack_mut]` fields — a `set_<field>` (POD/ref) and/or
        // `replace_<field>` (owned/strong/ref).
        accessors.push(accessor(vis, fname, inner_ty, &on_disk_ty, kind, nullable));
        accessors.push(raw_slice_accessor(vis, fname, inner_ty, &on_disk_ty, kind));
        if is_bstack_mut(&field.attrs) {
            match kind {
                // POD: overwrite in place.
                Kind::Pod => {
                    accessors.push(set_accessor(
                        vis,
                        fname,
                        inner_ty,
                        &on_disk_ty,
                        kind,
                        nullable,
                    ));
                }
                // Ref is the only kind with BOTH: `set_` (overwrite; a ref owns
                // nothing) and `replace_` (swap, handing the old ref back).
                Kind::Ref => {
                    accessors.push(set_accessor(
                        vis,
                        fname,
                        inner_ty,
                        &on_disk_ty,
                        kind,
                        nullable,
                    ));
                    accessors.push(replace_accessor(
                        vis,
                        fname,
                        inner_ty,
                        &on_disk_ty,
                        kind,
                        nullable,
                    ));
                }
                // Owned / strong: only `replace_` — a plain `set_` would strand the
                // old owned block / strong count; `replace_` moves it out instead.
                Kind::Owned | Kind::Strong => {
                    accessors.push(replace_accessor(
                        vis,
                        fname,
                        inner_ty,
                        &on_disk_ty,
                        kind,
                        nullable,
                    ));
                }
                // Weak fields already have a `set_<field>` (the weak setter).
                Kind::Weak => {}
                Kind::Embed => {
                    return Err(Error::new_spanned(
                        field,
                        "#[bstack_mut] is not yet supported on #[embed] fields",
                    ));
                }
            }
        }

        // Constructor. Weak fields are not parameters — they start null and are
        // wired afterwards via the generated `set_<field>`.
        if kind == Kind::Weak {
            ctor_inits.push(quote!(#fname: 0u64,));
            setters.push(weak_setter(vis, fname, inner_ty, &on_disk_ty));
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
    // For a generic block, fold each type argument's tag into the discriminant
    // so distinct instantiations get distinct tags (the `eightcc()` body — always
    // called at runtime — mixes them; the readable prefix stays the outer name's).
    let data_eightcc = if type_params.is_empty() && const_params.is_empty() {
        data_eightcc
    } else {
        // A block parameter has its own `eightcc`; a POD one does not, so fold in
        // its byte size instead (distinct-size instantiations get distinct tags;
        // same-size POD types are bit-compatible on disk, so sharing one is sound).
        let mixes = usage.iter().map(|(p, u)| {
            if u.pod {
                quote!(.mix(::bstack_raii::EightCC::new(
                    (::core::mem::size_of::<#p>() as u64).to_le_bytes())))
            } else {
                quote!(.mix(<#p as ::bstack_raii::BStackCast>::eightcc()))
            }
        });
        // A const parameter changes the array width (the layout), so fold its value
        // in — distinct `N` gives distinct tags.
        let const_mixes = const_params
            .iter()
            .map(|c| quote!(.mix(::bstack_raii::EightCC::new((#c as u64).to_le_bytes()))));
        quote!(#data_eightcc #(#mixes)* #(#const_mixes)*)
    };

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
                fn drop_strong_ref<__A: ::bstack_raii::BStackRaiiAllocator>(
                    data: ::bstack_raii::BStackRef<Self>,
                    allocator: &__A,
                ) -> ::std::io::Result<()> {
                    use ::bstack_raii::BStackDrop as _;
                    ::bstack_raii::StrongRef(data).bstack_drop(allocator)
                }
                fn strong_parts<__A: ::bstack_raii::BStackRaiiAllocator>(
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
                fn drop_strong_ref<__A: ::bstack_raii::BStackRaiiAllocator>(
                    data: ::bstack_raii::BStackRef<Self>,
                    allocator: &__A,
                ) -> ::std::io::Result<()> {
                    use ::bstack_raii::BStackDrop as _;
                    ::bstack_raii::StrongWeakRef::from_disk(data, allocator)?
                        .bstack_drop(allocator)
                }
                fn strong_parts<__A: ::bstack_raii::BStackRaiiAllocator>(
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

    // A plain block is self-contained (no separate control block), so it may be
    // `#[embed]`ded; `(rc)` / `(rc, weak)` blocks are deliberately not `BStackEmbeddable`.
    let embeddable_impl = if mode == Mode::Plain {
        quote! {
            impl #impl_g ::bstack_raii::__private::BStackEmbeddable for #name #ty_g #where_g {}
        }
    } else {
        quote!()
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
        &on_disk_ty,
        &on_disk_ctor,
        mode,
        &ctrl_eightcc,
        &ctor_params,
        &ctor_preps,
        &ctor_inits,
        &ctor_post,
    );

    // The field destructure is generated for every mode: plain blocks use it via
    // `BStackOwned` (infallible), rc / rc,weak via `BStackRc::try_move`.
    let move_impl = {
        quote! {
            // Implemented on the block type (local downstream) so the orphan rule
            // is satisfied; `bstack_move!` selects it from the argument's type.
            impl #impl_g ::bstack_raii::BStackMove for #name #ty_g #where_g {
                type Fields<'__mv, __A: ::bstack_raii::BStackRaiiAllocator> =
                    ( #(#mv_types,)* );
                fn bstack_move<'__mv, __A: ::bstack_raii::BStackRaiiAllocator>(
                    owned: ::bstack_raii::BStackOwned<Self>,
                    __alloc: &'__mv __A,
                ) -> ::std::io::Result<Self::Fields<'__mv, __A>> {
                    // Unwrap the ownership marker and read the payload before
                    // freeing anything.
                    let __inner = owned.into_inner();
                    let __stack = __alloc.stack();
                    let __range = ::bstack_raii::BStackBlock::range(&__inner);
                    let mut __buf = ::std::vec![0u8; ::core::mem::size_of::<#on_disk_ty>()];
                    let __r = unsafe { ::bstack_raii::BStackRef::<Self>::from_range(__range) };
                    let __od: #on_disk_ty = *__r.read_on_disk(__stack, &mut __buf)?;
                    #(#mv_caps)*
                    // Free the parent shell only; children stay live on disk.
                    unsafe { ::bstack_raii::dealloc_range(__alloc, __range)?; }
                    ::std::result::Result::Ok(( #(#mv_recon,)* ))
                }
            }
        }
    };

    // Deep clone. `__bstack_clone_children_inplace` reads this block's OnDisk and
    // returns a fixed-up copy — owned children deep-cloned into `__plan`, shared
    // children's refcounts bumped, embedded children cloned in place — without
    // allocating a block for `self` (so an `#[embed]` parent can fold it inline).
    // `__bstack_clone_into` layers on the destination allocation + staged write.
    // Both are generated for every block (so an owned/embedded child of any kind
    // can be recursed into) but do real work only for a plain block; a shared
    // (`rc` / `rc, weak`) block returns an error — its clone is a handle
    // duplication via `BStackRc::try_clone`, never a deep copy. The public
    // `TryCloneIn` entry point is generated for plain blocks only.
    let (clone_children_body, clone_into_body) = if mode != Mode::Plain {
        // Reachable only by owning / embedding a shared block (a misuse: shared
        // blocks are referenced, not owned) and deep-cloning the owner.
        let err = quote! {
            ::std::result::Result::Err(::std::io::Error::new(
                ::std::io::ErrorKind::Unsupported,
                "TryCloneIn: a reference-counted (`rc` / `rc, weak`) block is shared, \
                 not deep-cloned — duplicate its handle with `BStackRc::try_clone` \
                 (see the `TryClone` trait)",
            ))
        };
        (err.clone(), err)
    } else {
        let children = quote! {
            let __stack = allocator.stack();
            let __src = ::bstack_raii::BStackBlock::range(self);
            let mut __buf = ::std::vec![0u8; ::core::mem::size_of::<#on_disk_ty>()];
            let __r = unsafe { ::bstack_raii::BStackRef::<Self>::from_range(__src) };
            #[allow(unused_mut)]
            let mut __od: #on_disk_ty = *__r.read_on_disk(__stack, &mut __buf)?;
            #(#clone_stmts)*
            ::std::result::Result::Ok(__od)
        };
        let into = quote! {
            let __od = self.__bstack_clone_children_inplace(allocator, __plan)?;
            let __dst = __plan.alloc_raw(
                allocator,
                ::core::mem::size_of::<#on_disk_ty>() as u64,
            )?;
            __plan.write(
                __dst.start(),
                ::bstack_raii::bytemuck::bytes_of(&__od).to_vec(),
            );
            ::std::result::Result::Ok(__dst)
        };
        (children, into)
    };
    // The two clone hooks are `BStackBlock` **trait** methods (overriding the
    // childless defaults) so a generic parent can recurse into a `#[bstack_owned]`
    // type parameter. Emitted into the `impl BStackBlock for X` block below.
    let clone_trait_methods = quote! {
        #[doc(hidden)]
        #[allow(unused_variables, unused_imports)]
        fn __bstack_clone_children_inplace<__A: ::bstack_raii::BStackRaiiAllocator>(
            &self,
            allocator: &__A,
            __plan: &mut ::bstack_raii::ClonePlan,
        ) -> ::std::io::Result<#on_disk_ty> {
            // Bring the trait into scope so a child's (possibly generic) clone hook
            // resolves via method syntax.
            use ::bstack_raii::BStackBlock as _;
            #clone_children_body
        }
        #[doc(hidden)]
        #[allow(unused_variables)]
        fn __bstack_clone_into<__A: ::bstack_raii::BStackRaiiAllocator>(
            &self,
            allocator: &__A,
            __plan: &mut ::bstack_raii::ClonePlan,
        ) -> ::std::io::Result<::bstack_raii::BStackRange> {
            #clone_into_body
        }
    };
    let clone_impl = if mode == Mode::Plain {
        quote! {
            impl #impl_g ::bstack_raii::TryCloneIn for #name #ty_g #where_g {
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

    // The handle: a `BStackRange` newtype (plus a phantom over the type
    // parameters when generic). `Clone`/`Copy` hold regardless of `T` — for the
    // generic case they're hand-written (no `T: Copy` bound) rather than derived.
    let handle_def = if type_params.is_empty() && const_params.is_empty() {
        quote! {
            #[derive(::core::clone::Clone, ::core::marker::Copy)]
            #vis struct #name(::bstack_raii::BStackRange);
        }
    } else {
        quote! {
            #vis struct #name #decl_g(::bstack_raii::BStackRange #phantom_field) #decl_where;
            impl #decl_g ::core::clone::Clone for #name #decl_ty_g #decl_where {
                fn clone(&self) -> Self { *self }
            }
            impl #decl_g ::core::marker::Copy for #name #decl_ty_g #decl_where {}
        }
    };

    // A generic `OnDisk` (embed / POD parameters) can't `#[derive(Copy)]` — the
    // derived `T: Copy` bound isn't implied by `T: BStackBlock`, though the fields
    // (`<T>::OnDisk` / `T: Pod`) always are — so hand-write `Clone`/`Copy` with the
    // `OnDisk`'s own bounds. A non-generic `OnDisk` keeps the derive.
    let (on_disk_derive, on_disk_clonecopy): (TokenStream, TokenStream) = if ondisk_empty {
        (
            quote!(#[derive(::core::clone::Clone, ::core::marker::Copy)]),
            quote!(),
        )
    } else {
        (
            quote!(),
            quote! {
                impl #od_impl_g ::core::clone::Clone for #on_disk_ty #od_where {
                    fn clone(&self) -> Self { *self }
                }
                impl #od_impl_g ::core::marker::Copy for #on_disk_ty #od_where {}
            },
        )
    };
    // The `Pod` assertion can only name concrete field types — a generic parameter
    // in a POD field carries a `T: Pod` bound instead (and the `OnDisk`'s own `Pod`
    // impl checks the composite).
    let all_param_idents: Vec<&Ident> = type_params
        .iter()
        .copied()
        .chain(const_params.iter().copied())
        .collect();
    let concrete_pod_types: Vec<&Type> = pod_types
        .iter()
        .filter(|t| !type_mentions_any(t, &all_param_idents))
        .copied()
        .collect();

    Ok(quote! {
        #handle_def

        // Packed Pod wrappers for any POD tuple fields.
        #(#wrapper_defs)*

        #[repr(C, packed)]
        #on_disk_derive
        #vis struct #on_disk #od_impl_g #od_where {
            __bstack_header: ::bstack_raii::BlockHeader,
            #(#on_disk_fields)*
        }
        #on_disk_clonecopy

        // SAFETY: `#[repr(C, packed)]` guarantees no padding, and every field is
        // `Pod` (u64 for refs/injected counters, header is Pod, each inline field
        // is asserted `Pod` below, and a generic inline field is `Pod` by its
        // parameter's bound), so all bit patterns are valid.
        unsafe impl #od_impl_g ::bstack_raii::Zeroable for #on_disk_ty #od_where {}
        unsafe impl #od_impl_g ::bstack_raii::Pod for #on_disk_ty #od_where {}

        const _: fn() = || {
            fn __assert_pod<__T: ::bstack_raii::Pod>() {}
            #( __assert_pod::<#concrete_pod_types>(); )*
        };

        impl #impl_g ::bstack_raii::BStackCast for #name #ty_g #where_g {
            fn eightcc() -> ::bstack_raii::EightCC {
                #data_eightcc
            }
        }

        impl #impl_g ::bstack_raii::BStackBlock for #name #ty_g #where_g {
            type OnDisk = #on_disk_ty;
            fn from_range(range: ::bstack_raii::BStackRange) -> Self {
                #name(range #phantom_ctor)
            }
            fn range(&self) -> ::bstack_raii::BStackRange {
                self.0
            }

            #clone_trait_methods

            /// Free this block's owned children (recursively) given its range,
            /// **without** freeing the block itself — used when the block is
            /// `#[embed]`ded (its storage is part of its parent), and by
            /// `bstack_drop` before the self-dealloc. Overrides the childless
            /// `BStackBlock` default.
            #[doc(hidden)]
            #[allow(unused_imports)]
            fn __bstack_drop_children<__A: ::bstack_raii::BStackRaiiAllocator>(
                __range: ::bstack_raii::BStackRange,
                allocator: &__A,
            ) -> ::std::io::Result<()> {
                use ::bstack_raii::BStackDrop as _;
                // Bring the trait into scope so a child's (possibly generic) teardown
                // hook resolves.
                use ::bstack_raii::BStackBlock as _;
                let __stack = allocator.stack();
                let mut __buf = ::std::vec![0u8; ::core::mem::size_of::<#on_disk_ty>()];
                let __r = unsafe { ::bstack_raii::BStackRef::<Self>::from_range(__range) };
                let __on_disk: #on_disk_ty = *__r.read_on_disk(__stack, &mut __buf)?;
                #(#drop_stmts)*
                ::std::result::Result::Ok(())
            }
        }

        impl #impl_g ::bstack_raii::BStackDrop for #name #ty_g #where_g {
            fn bstack_drop<__A: ::bstack_raii::BStackRaiiAllocator>(
                self,
                allocator: &__A,
            ) -> ::std::io::Result<()> {
                <Self as ::bstack_raii::BStackBlock>::__bstack_drop_children(self.0, allocator)?;
                unsafe { ::bstack_raii::dealloc_range(allocator, self.0) }
            }
        }

        impl #impl_g #name #ty_g #where_g {
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
        #embeddable_impl
        #weakable_items
        #move_impl
        #clone_impl
        #overlong_warning
        #ref_warning
    })
}
