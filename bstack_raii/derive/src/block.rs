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
    /// `#[embed]`: an exclusively-owned child block stored **inline** (its whole
    /// on-disk form, header and all), not as a `u64` offset.
    Embed,
    /// POD field stored inline.
    Pod,
}

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
    if !input.generics.params.is_empty() {
        for p in &input.generics.params {
            if !matches!(p, syn::GenericParam::Type(_)) {
                return Err(Error::new_spanned(
                    p,
                    "a generic #[bstack_block] currently supports only type parameters (no \
                     lifetime or const generics)",
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

    // Layout-preserving check + per-parameter bound: a type parameter may appear
    // only in a `#[bstack_ref]` / `#[bstack_strong]` / `#[bstack_weak]` field —
    // each a bare `u64` offset on disk whose teardown/clone recurse through
    // *traits* (`BStackShared`) or library helpers, never the generated inherent
    // methods. Every such parameter is bounded `BStackBlock`, plus `BStackShared`
    // if it is ever a `#[bstack_strong]` element and `BStackWeakable` if ever a
    // `#[bstack_weak]` element. (Owned needs a not-yet-done clone-recursion
    // change; embed/POD change the on-disk layout — both rejected.)
    let mut param_extra: Vec<(Ident, bool, bool)> =
        type_params.iter().map(|p| ((*p).clone(), false, false)).collect();
    for (_, field) in &field_list {
        let kind = classify(field)?;
        if !type_mentions_any(&field.ty, &type_params) {
            continue;
        }
        match kind {
            Kind::Ref => {}
            Kind::Strong | Kind::Weak => {
                for (p, is_strong, is_weak) in param_extra.iter_mut() {
                    if type_mentions_any(&field.ty, &[p]) {
                        *is_strong |= kind == Kind::Strong;
                        *is_weak |= kind == Kind::Weak;
                    }
                }
            }
            _ => {
                return Err(Error::new_spanned(
                    &field.ty,
                    "a generic type parameter may currently be used only in a `#[bstack_ref]` / \
                     `#[bstack_strong]` / `#[bstack_weak]` field — each a bare `u64` offset that \
                     keeps the block's on-disk layout independent of the type. An owned use needs \
                     a clone-recursion change, and embed/POD change the layout — not yet \
                     supported.",
                ));
            }
        }
    }
    // Generics threaded into the generated impls (with the computed bounds added
    // to every parameter), plus the handle's phantom marker over them.
    // `impl_g`/`ty_g`/`where_g` carry the bounds (for the bstack trait impls);
    // `decl_g`/`decl_ty_g`/`decl_where` are the user's own (for the handle type
    // and its `Clone`/`Copy`, which hold regardless of `T`).
    let mut aug_generics = input.generics.clone();
    for tp in aug_generics.type_params_mut() {
        tp.bounds.push(syn::parse_quote!(::bstack_raii::BStackBlock));
        if let Some((_, is_strong, is_weak)) = param_extra.iter().find(|(p, ..)| *p == tp.ident) {
            if *is_strong {
                tp.bounds.push(syn::parse_quote!(::bstack_raii::BStackShared));
            }
            if *is_weak {
                tp.bounds.push(syn::parse_quote!(::bstack_raii::BStackWeakable));
            }
        }
    }
    let (impl_g, ty_g, where_g) = aug_generics.split_for_impl();
    let (decl_g, decl_ty_g, decl_where) = input.generics.split_for_impl();
    let (phantom_field, phantom_ctor): (TokenStream, TokenStream) = if type_params.is_empty() {
        (quote!(), quote!())
    } else {
        (
            quote!(, ::core::marker::PhantomData<fn() -> (#(#type_params,)*)>),
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
                let total = dims_prod(&dims);
                let elem_ts = quote!(#elem_ty);
                let size_elem = quote!(::core::mem::size_of::<
                    <#elem_ty as ::bstack_raii::BStackBlock>::OnDisk>() as u64);
                let store = quote!(::bstack_raii::BStackVec::<u64, __A>);
                let field_loc =
                    quote!(self.0.start() + ::core::mem::offset_of!(#on_disk, #fname) as u64);
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
                    #vis fn #fname<'__v, __A: ::bstack_raii::BStackOwnedSliceAllocator>(
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
                    Kind::Strong => quote!(<#elem_ty as ::bstack_raii::BStackShared>::drop_strong_ref(
                        unsafe { ::bstack_raii::BStackRef::<#elem_ty>::from_range(
                            ::bstack_raii::BStackRange::new(__off, #size_elem)) },
                        allocator)?;),
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
                    vec_accessor(vis, fname, elem, &on_disk, nullable),
                    vec_ctor(fname, &vinfo, nullable),
                    vec_move(&cap, elem, nullable),
                ),
                Kind::Owned => (
                    block_vec_drop_stmt(fname, quote!(BStackBlockVec), elem, nullable),
                    block_vec_accessor(vis, fname, elem, &on_disk, quote!(BStackBlockVec), nullable),
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
                    block_vec_accessor(vis, fname, elem, &on_disk, quote!(BStackStrongVec), nullable),
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
                    block_vec_accessor(vis, fname, elem, &on_disk, quote!(BStackWeakVec), nullable),
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
                    block_vec_accessor(vis, fname, elem, &on_disk, quote!(BStackRefVec), nullable),
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
                    #vis fn #fname<'__v, __A: ::bstack_raii::BStackOwnedSliceAllocator>(
                        &self,
                        allocator: &'__v __A,
                    ) -> ::std::io::Result<#acc_ret> {
                        let __base =
                            self.0.start() + ::core::mem::offset_of!(#on_disk, #fname) as u64;
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

        // Inline fixed-size array `[T; N]` — possibly *nested* `[[..]; ..]` — of
        // block references. (A POD array falls through to the POD path below: an
        // array of `Pod` is `Pod`.) Stored **flat** as `[u64; N0*..*Nk]` inline
        // (no data block), one offset per leaf, with per-element ownership; the
        // accessor / ctor / move traffic in the nested `[[Handle; ..]; ..]` shape.
        if kind != Kind::Pod && let Type::Array(_) = opt_inner {
            if nullable {
                return Err(Error::new_spanned(
                    &field.ty,
                    "a whole-array `Option<[T; N]>` is not supported; use `[Option<T>; N]` \
                     for per-element nullability",
                ));
            }
            let (dims, elem, elem_nullable) = array_shape(opt_inner)?;
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
                let child = elem;
                let child_od = quote!(<#child as ::bstack_raii::BStackBlock>::OnDisk);
                on_disk_fields.push(quote!(#fname: [#child_od; #total],));

                // Teardown: free each embedded child's children in place.
                drop_stmts.push(quote! {
                    {
                        let __base =
                            __range.start() + ::core::mem::offset_of!(#on_disk, #fname) as u64;
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
                    #vis fn #fname(&self) -> #acc_ret {
                        let __base =
                            self.0.start() + ::core::mem::offset_of!(#on_disk, #fname) as u64;
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
                            __data.start() + ::core::mem::offset_of!(#on_disk, #fname) as u64;
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
                mv_types.push(nested_ty(&dims, &quote!(::bstack_raii::BStackOwned<#child>)));
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
                            __src.start() + ::core::mem::offset_of!(#on_disk, #fname) as u64;
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
                    #vis fn #setter<'__s, __A: ::bstack_raii::BStackOwnedSliceAllocator>(
                        &self,
                        allocator: &'__s __A,
                        index: usize,
                        weak: ::bstack_raii::BStackWeak<'__s, #elem, __A>,
                    ) -> ::std::io::Result<()> {
                        let __field = self.0.start()
                            + ::core::mem::offset_of!(#on_disk, #fname) as u64
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
                    #vis fn #fname<'__u, __A: ::bstack_raii::BStackOwnedSliceAllocator>(
                        &self,
                        allocator: &'__u __A,
                    ) -> ::std::io::Result<#acc_ret> {
                        let __base =
                            self.0.start() + ::core::mem::offset_of!(#on_disk, #fname) as u64;
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
                #vis fn #fname(
                    &self,
                    stack: &::bstack_raii::BStack,
                ) -> ::std::io::Result<#acc_ret> {
                    let mut __buf = [0u8; ::core::mem::size_of::<#on_disk>()];
                    let __r = unsafe { ::bstack_raii::BStackRef::<Self>::from_range(self.0) };
                    let __od: #on_disk = *__r.read_on_disk(stack, &mut __buf)?;
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

        // A POD **tuple** field `a: (A, B, ..)`: a Rust tuple is not `Pod`, but a
        // packed struct of its (POD) elements is — alignment is irrelevant on disk
        // — so store it through a generated wrapper and rebuild the tuple on read.
        // `bstack_move!` hands back the tuple as one element (not flattened).
        if kind == Kind::Pod && let Type::Tuple(tup) = inner_ty {
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
                #vis fn #fname(
                    &self,
                    stack: &::bstack_raii::BStack,
                ) -> ::std::io::Result<#inner_ty> {
                    let mut __buf = [0u8; ::core::mem::size_of::<#on_disk>()];
                    let __r = unsafe { ::bstack_raii::BStackRef::<Self>::from_range(self.0) };
                    let __od: #on_disk = *__r.read_on_disk(stack, &mut __buf)?;
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
            let child = inner_ty;
            let child_od = quote!(<#child as ::bstack_raii::BStackBlock>::OnDisk);
            on_disk_fields.push(quote!(#fname: #child_od,));

            // Teardown: free the embedded child's own children *in place* (its
            // storage is part of this block, so no separate dealloc). `__range` is
            // this block's range, bound by `__bstack_drop_children`.
            drop_stmts.push(quote! {
                {
                    let __embed = ::bstack_raii::BStackRange::new(
                        __range.start() + ::core::mem::offset_of!(#on_disk, #fname) as u64,
                        ::core::mem::size_of::<#child_od>() as u64,
                    );
                    <#child>::__bstack_drop_children(__embed, allocator)?;
                }
            });

            // Accessor: a child handle at the embedded offset (pure offset math).
            accessors.push(quote! {
                #vis fn #fname(&self) -> #child {
                    <#child as ::bstack_raii::BStackBlock>::from_range(
                        ::bstack_raii::BStackRange::new(
                            self.0.start() + ::core::mem::offset_of!(#on_disk, #fname) as u64,
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
                        __data.start() + ::core::mem::offset_of!(#on_disk, #fname) as u64,
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
                            __src.start() + ::core::mem::offset_of!(#on_disk, #fname) as u64,
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
    // For a generic block, fold each type argument's tag into the discriminant
    // so distinct instantiations get distinct tags (the `eightcc()` body — always
    // called at runtime — mixes them; the readable prefix stays the outer name's).
    let data_eightcc = if type_params.is_empty() {
        data_eightcc
    } else {
        let mixes = type_params
            .iter()
            .map(|p| quote!(.mix(<#p as ::bstack_raii::BStackCast>::eightcc())));
        quote!(#data_eightcc #(#mixes)*)
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
        &ctor_post,
    );

    // The field destructure is generated for every mode: plain blocks use it via
    // `BStackOwned` (infallible), rc / rc,weak via `BStackRc::try_move`.
    let move_impl = {
        quote! {
            // Implemented on the block type (local downstream) so the orphan rule
            // is satisfied; `bstack_move!` selects it from the argument's type.
            impl #impl_g ::bstack_raii::BStackMove for #name #ty_g #where_g {
                type Fields<'__mv, __A: ::bstack_raii::BStackOwnedSliceAllocator> =
                    ( #(#mv_types,)* );
                fn bstack_move<'__mv, __A: ::bstack_raii::BStackOwnedSliceAllocator>(
                    owned: ::bstack_raii::BStackOwned<Self>,
                    __alloc: &'__mv __A,
                ) -> ::std::io::Result<Self::Fields<'__mv, __A>> {
                    // Unwrap the ownership marker and read the payload before
                    // freeing anything.
                    let __inner = owned.into_inner();
                    let __stack = __alloc.stack();
                    let __range = ::bstack_raii::BStackBlock::range(&__inner);
                    let mut __buf = [0u8; ::core::mem::size_of::<#on_disk>()];
                    let __r = unsafe { ::bstack_raii::BStackRef::<Self>::from_range(__range) };
                    let __od: #on_disk = *__r.read_on_disk(__stack, &mut __buf)?;
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
            let mut __buf = [0u8; ::core::mem::size_of::<#on_disk>()];
            let __r = unsafe { ::bstack_raii::BStackRef::<Self>::from_range(__src) };
            #[allow(unused_mut)]
            let mut __od: #on_disk = *__r.read_on_disk(__stack, &mut __buf)?;
            #(#clone_stmts)*
            ::std::result::Result::Ok(__od)
        };
        let into = quote! {
            let __od = self.__bstack_clone_children_inplace(allocator, __plan)?;
            let __dst = __plan.alloc_raw(
                allocator,
                ::core::mem::size_of::<#on_disk>() as u64,
            )?;
            __plan.write(
                __dst.start(),
                ::bstack_raii::bytemuck::bytes_of(&__od).to_vec(),
            );
            ::std::result::Result::Ok(__dst)
        };
        (children, into)
    };
    let clone_into_method = quote! {
        impl #impl_g #name #ty_g #where_g {
            /// Read this block's OnDisk and return a deep-cloned copy: owned
            /// children cloned into `__plan`, shared children's refcounts bumped,
            /// embedded children folded in place. Does **not** allocate a block for
            /// `self` — used to fold an `#[embed]`ded child inline into its parent's
            /// clone, and by `__bstack_clone_into` before the self-allocation.
            #[doc(hidden)]
            #[allow(unused_variables)]
            #vis fn __bstack_clone_children_inplace<__A: ::bstack_raii::BStackOwnedSliceAllocator>(
                &self,
                allocator: &__A,
                __plan: &mut ::bstack_raii::ClonePlan,
            ) -> ::std::io::Result<#on_disk> {
                #clone_children_body
            }

            /// Deep-clone this block's subtree into `__plan`: allocate a fresh
            /// destination block, recurse into owned children (bumping shared
            /// children's refcounts), and stage the destination payload —
            /// returning the new block's range. Writes are staged, not committed;
            /// the caller commits `__plan` once.
            #[doc(hidden)]
            #[allow(unused_variables)]
            #vis fn __bstack_clone_into<__A: ::bstack_raii::BStackOwnedSliceAllocator>(
                &self,
                allocator: &__A,
                __plan: &mut ::bstack_raii::ClonePlan,
            ) -> ::std::io::Result<::bstack_raii::BStackRange> {
                #clone_into_body
            }
        }
    };
    let clone_impl = if mode == Mode::Plain {
        quote! {
            #clone_into_method

            impl #impl_g ::bstack_raii::TryCloneIn for #name #ty_g #where_g {
                fn try_clone_in<__A: ::bstack_raii::BStackOwnedSliceAllocator>(
                    &self,
                    allocator: &__A,
                ) -> ::std::io::Result<::bstack_raii::BStackOwned<Self>> {
                    let mut __plan = ::bstack_raii::ClonePlan::new();
                    let __dst = match self.__bstack_clone_into(allocator, &mut __plan) {
                        ::std::result::Result::Ok(__d) => __d,
                        ::std::result::Result::Err(__e) => {
                            __plan.rollback(allocator);
                            return ::std::result::Result::Err(__e);
                        }
                    };
                    __plan.commit(allocator)?;
                    ::std::result::Result::Ok(unsafe {
                        ::bstack_raii::BStackOwned::from_raw(
                            <Self as ::bstack_raii::BStackBlock>::from_range(__dst),
                        )
                    })
                }
            }
        }
    } else {
        clone_into_method
    };

    // The handle: a `BStackRange` newtype (plus a phantom over the type
    // parameters when generic). `Clone`/`Copy` hold regardless of `T` — for the
    // generic case they're hand-written (no `T: Copy` bound) rather than derived.
    let handle_def = if type_params.is_empty() {
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

    Ok(quote! {
        #handle_def

        // Packed Pod wrappers for any POD tuple fields.
        #(#wrapper_defs)*

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

        impl #impl_g ::bstack_raii::BStackCast for #name #ty_g #where_g {
            fn eightcc() -> ::bstack_raii::EightCC {
                #data_eightcc
            }
        }

        impl #impl_g ::bstack_raii::BStackBlock for #name #ty_g #where_g {
            type OnDisk = #on_disk;
            fn from_range(range: ::bstack_raii::BStackRange) -> Self {
                #name(range #phantom_ctor)
            }
            fn range(&self) -> ::bstack_raii::BStackRange {
                self.0
            }
        }

        impl #impl_g #name #ty_g #where_g {
            /// Free this block's owned children (recursively) given its range,
            /// **without** freeing the block itself — used when the block is
            /// `#[embed]`ded (its storage is part of its parent), and by
            /// `bstack_drop` before the self-dealloc.
            #[doc(hidden)]
            #vis fn __bstack_drop_children<__A: ::bstack_raii::BStackOwnedSliceAllocator>(
                __range: ::bstack_raii::BStackRange,
                allocator: &__A,
            ) -> ::std::io::Result<()> {
                use ::bstack_raii::BStackDrop as _;
                let __stack = allocator.stack();
                let mut __buf = [0u8; ::core::mem::size_of::<#on_disk>()];
                let __r = unsafe { ::bstack_raii::BStackRef::<Self>::from_range(__range) };
                let __on_disk: #on_disk = *__r.read_on_disk(__stack, &mut __buf)?;
                #(#drop_stmts)*
                ::std::result::Result::Ok(())
            }
        }

        impl #impl_g ::bstack_raii::BStackDrop for #name #ty_g #where_g {
            fn bstack_drop<__A: ::bstack_raii::BStackOwnedSliceAllocator>(
                self,
                allocator: &__A,
            ) -> ::std::io::Result<()> {
                Self::__bstack_drop_children(self.0, allocator)?;
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
        #weakable_items
        #move_impl
        #clone_impl
        #overlong_warning
        #ref_warning
    })
}

/// A `Vec<T>` / `String` field: its element type (tokens) and whether it's a
/// `String` (so the constructor takes `&str`). Whether the elements are POD
/// (byte storage) or blocks (offset storage) is decided by the field's ownership
/// annotation, not by inspecting the element type.
struct VecInfo {
    elem: TokenStream,
    is_string: bool,
}

/// Whether `ty` is the `str` type.
fn is_str(ty: &Type) -> bool {
    matches!(ty, Type::Path(tp) if tp.path.segments.last().is_some_and(|s| s.ident == "str"))
}

/// Whether `ty` mentions any of the given (generic type-parameter) identifiers
/// anywhere in its token tree. Used to enforce that a generic parameter is only
/// ever used in a `#[bstack_ref]` field.
fn type_mentions_any(ty: &Type, params: &[&Ident]) -> bool {
    fn walk(ts: TokenStream, params: &[&Ident]) -> bool {
        ts.into_iter().any(|t| match t {
            proc_macro2::TokenTree::Ident(id) => params.iter().any(|p| **p == id),
            proc_macro2::TokenTree::Group(g) => walk(g.stream(), params),
            _ => false,
        })
    }
    walk(quote!(#ty), params)
}

/// The element type `T` of a `Vec<T>`, if `ty` is a `Vec`. Used to reject
/// nested `Vec<Vec<T>>` / `Vec<String>` with a directed error.
fn vec_inner(ty: &Type) -> Option<&Type> {
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
fn err_double_option(ty: &Type) -> Error {
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
fn err_vec_in_vec(ty: &Type) -> Error {
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
fn err_tuple_in_vec(ty: &Type) -> Error {
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
fn check_container_nesting(ty: &Type) -> syn::Result<()> {
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
fn check_bare(ty: &Type) -> syn::Result<()> {
    if let Some(elem) = vec_inner(ty) {
        return check_vec_elem(elem);
    }
    Ok(())
}

/// A `Vec` element: a leaf, optionally an array `[..; N]` of leaves, optionally
/// wrapped in exactly one `Option`. A `Vec` / `String` in leaf position is
/// `Vec<Vec>` (`Vec<[Vec<T>; N]>` included — arrays are peeled first); an
/// `Option<Option>` is a double option; an `Option<Vec>` is again a nested `Vec`.
fn check_vec_elem(ty: &Type) -> syn::Result<()> {
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

/// Teardown for a POD `Vec<T>` / `String` field: free the vector's data block
/// (the inline descriptor is freed with the enclosing struct's block). A nullable
/// field frees nothing when the descriptor is the `0` niche.
fn vec_drop_stmt(fname: &Ident, elem: &TokenStream, nullable: bool) -> TokenStream {
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
fn vec_accessor(
    vis: &syn::Visibility,
    fname: &Ident,
    elem: &TokenStream,
    on_disk: &Ident,
    nullable: bool,
) -> TokenStream {
    let field = quote!(self.0.start() + ::core::mem::offset_of!(#on_disk, #fname) as u64);
    if nullable {
        quote! {
            #vis fn #fname<'__v, __A: ::bstack_raii::BStackOwnedSliceAllocator>(
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
            #vis fn #fname<'__v, __A: ::bstack_raii::BStackOwnedSliceAllocator>(
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
fn vec_ctor(fname: &Ident, vinfo: &VecInfo, nullable: bool) -> (TokenStream, TokenStream, TokenStream) {
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
fn vec_move(cap: &Ident, elem: &TokenStream, nullable: bool) -> (TokenStream, TokenStream) {
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
fn vec_clone_stmt(fname: &Ident, kind: Kind, elem: &TokenStream) -> TokenStream {
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
fn wrap_vec_move(
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
fn block_vec_drop_stmt(
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
fn block_vec_accessor(
    vis: &syn::Visibility,
    fname: &Ident,
    elem: &TokenStream,
    on_disk: &Ident,
    vec_ty: TokenStream,
    nullable: bool,
) -> TokenStream {
    let field = quote!(self.0.start() + ::core::mem::offset_of!(#on_disk, #fname) as u64);
    if nullable {
        quote! {
            #vis fn #fname<'__v, __A: ::bstack_raii::BStackOwnedSliceAllocator>(
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
            #vis fn #fname<'__v, __A: ::bstack_raii::BStackOwnedSliceAllocator>(
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
fn block_vec_ctor(
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
fn block_vec_move(
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
fn array_shape(ty: &Type) -> syn::Result<(Vec<&Expr>, &Type, bool)> {
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
fn dims_prod(dims: &[&Expr]) -> TokenStream {
    if dims.is_empty() {
        return quote!(1usize);
    }
    let first = dims[0];
    let mut t = quote!((#first));
    for d in &dims[1..] {
        t = quote!(#t * (#d));
    }
    t
}

/// The nested handle-array type `[[.. [leaf; N0]..]; Nk]` for the given
/// dimensions (outer→inner) around a leaf token type.
fn nested_ty(dims: &[&Expr], leaf: &TokenStream) -> TokenStream {
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
fn nested_build(
    dims: &[&Expr],
    leaf_ty: &TokenStream,
    leaf_read: &dyn Fn(&Ident) -> TokenStream,
) -> TokenStream {
    let k = format_ident!("__k");
    let body = nested_build_inner(dims, leaf_ty, 0, &k, leaf_read);
    quote!({ let mut #k = 0usize; #body })
}

fn nested_build_inner(
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
fn nested_consume(
    dims: &[&Expr],
    val: &TokenStream,
    leaf_write: &dyn Fn(&Ident, &Ident) -> TokenStream,
) -> TokenStream {
    let k = format_ident!("__k");
    let body = nested_consume_inner(dims, val, 0, &k, leaf_write);
    quote!({ let mut #k = 0usize; #body })
}

fn nested_consume_inner(
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
#[allow(clippy::too_many_arguments)]
fn constructor(
    vis: &syn::Visibility,
    on_disk: &Ident,
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
                #vis fn new<'__ctor, __A: ::bstack_raii::BStackOwnedSliceAllocator>(
                    allocator: &'__ctor __A,
                    #(#params)*
                ) -> ::std::io::Result<#ret> {
                    #(#preps)*
                    let __on_disk = #on_disk {
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
                #vis fn new<'__ctor, __A: ::bstack_raii::BStackOwnedSliceAllocator>(
                    allocator: &'__ctor __A,
                    #(#params)*
                ) -> ::std::io::Result<::bstack_raii::BStackRc<'__ctor, Self, __A>> {
                    #(#preps)*
                    // Allocate data + control up front (atomically when the
                    // allocator supports bulk); both are orphans until the commit.
                    let __blocks = ::bstack_raii::alloc_many(allocator, &[#size, #ctrl_size])?;
                    let __data = __blocks[0];
                    let __ctrl = __blocks[1];
                    let __on_disk = #on_disk {
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
                        let _ = ::bstack_raii::free_many(allocator, [__data, __ctrl]);
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

/// A `TryCloneIn` statement for a scalar reference/POD field, the mirror of the
/// teardown dispatch run in reverse. Reads/patches the mutable `__od` OnDisk copy
/// and appends allocations / refcount bumps to `__plan`. `None` = nothing to do:
/// POD and `#[bstack_ref]` fields are byte-copied verbatim (a ref clone aliases
/// the same borrowed target — see the borrow-rules TODO). A `0` offset (a null
/// `Option` field, or an unset weak) is left copied as-is.
fn clone_field_stmt(fname: &Ident, inner_ty: &Type, kind: Kind) -> Option<TokenStream> {
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
    /// `#[bstack_enum(repr(..))]`: the discriminant integer type name (e.g.
    /// `"u16"`), with `aligned` normalized to `"u64"`. Enum-only.
    repr: Option<String>,
}

/// Parse `rc`, `weak`, `tag = "..."`, `ctrl_tag = "..."`,
/// `allow(overlong_tag | coerced_ref | deprecated)`, and (enums)
/// `repr(u8|u16|u32|u64|i8|i16|i32|i64|aligned)` in any order.
fn parse_attr(attr: TokenStream) -> syn::Result<Attr> {
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

/// Classify by ownership annotation among a set of attributes (a field's or an
/// enum variant's). No annotation => `Pod`.
fn classify_attrs(attrs: &[syn::Attribute]) -> syn::Result<Kind> {
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
fn classify(field: &syn::Field) -> syn::Result<Kind> {
    classify_attrs(&field.attrs)
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
fn parse_disc_expr(expr: &Expr) -> syn::Result<i128> {
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
fn int_bounds(ty: &str) -> (i128, i128) {
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
fn infer_disc_ty(min: i128, max: i128) -> &'static str {
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

/// Implementation of the `#[bstack_enum]` attribute macro.
pub fn expand_enum(attr: TokenStream, input: syn::ItemEnum) -> syn::Result<TokenStream> {
    let attr = parse_attr(attr)?;
    let mode = attr.mode;
    if !input.generics.params.is_empty() {
        return Err(Error::new_spanned(
            &input.generics,
            "#[bstack_enum] does not support generic enums",
        ));
    }

    // The on-disk discriminant. Each variant's value follows Rust's rules
    // (explicit `= N`, else previous + 1); the width is an explicit `repr(..)`
    // or, absent that, the smallest integer type that fits every value.
    let disc_values: Vec<i128> = {
        let mut next: i128 = 0;
        let mut out: Vec<i128> = Vec::with_capacity(input.variants.len());
        for v in &input.variants {
            let d = match &v.discriminant {
                Some((_, expr)) => parse_disc_expr(expr)?,
                None => next,
            };
            // The macro replaces the enum, so rustc's E0081 never fires; a
            // duplicate would only surface as an `unreachable_patterns` warning on
            // the generated match (and read the wrong variant). Reject it clearly.
            if out.contains(&d) {
                return Err(Error::new_spanned(
                    v,
                    format!("discriminant value `{d}` assigned more than once"),
                ));
            }
            out.push(d);
            next = d
                .checked_add(1)
                .ok_or_else(|| Error::new_spanned(v, "#[bstack_enum] discriminant overflow"))?;
        }
        out
    };
    let dmin = disc_values.iter().copied().min().unwrap_or(0);
    let dmax = disc_values.iter().copied().max().unwrap_or(0);
    let disc_ty_name: String = match &attr.repr {
        Some(r) => {
            let (lo, hi) = int_bounds(r);
            if dmin < lo || dmax > hi {
                return Err(Error::new_spanned(
                    &input.variants,
                    format!(
                        "a discriminant value is out of range for `repr({r})` \
                         (values span {dmin}..={dmax})"
                    ),
                ));
            }
            r.clone()
        }
        None => infer_disc_ty(dmin, dmax).to_string(),
    };
    let disc_ty: TokenStream = disc_ty_name.parse().expect("valid integer type name");
    // Typed literal patterns for the match arms / stored value (e.g. `300u16`).
    let disc_pats: Vec<TokenStream> = disc_values
        .iter()
        .map(|v| {
            format!("{v}{disc_ty_name}")
                .parse()
                .expect("valid integer literal")
        })
        .collect();

    let name = &input.ident;
    let vis = &input.vis;
    let on_disk = format_ident!("{}OnDisk", name);
    let control = format_ident!("{}OnDiskRef", name);
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
    let mut needs_payload = false;
    // A strong/weak variant makes `EData` generic over `<'e, A>`; a weak variant
    // also makes `EView` generic (its read upgrades to a `BStackRc`).
    let mut has_shared = false;
    let mut has_weak = false;

    for (i, variant) in input.variants.iter().enumerate() {
        let disc = &disc_pats[i];
        let vname = &variant.ident;
        let kind = classify_attrs(&variant.attrs)?;

        // The child block's range recovered from a stored offset (owned / ref).
        let child_from_off = |ty: &Type| {
            quote! {
                <#ty as ::bstack_raii::BStackBlock>::from_range(::bstack_raii::BStackRange::new(
                    u64::from_le_bytes(__pl[..8].try_into().unwrap()),
                    ::core::mem::size_of::<<#ty as ::bstack_raii::BStackBlock>::OnDisk>() as u64,
                ))
            }
        };
        // A `BStackRef<T>` over the child block, recovered from a stored offset.
        let child_ref = |ty: &Type| {
            quote! {
                ::bstack_raii::BStackRef::<#ty>::from_range(::bstack_raii::BStackRange::new(
                    u64::from_le_bytes(__pl[..8].try_into().unwrap()),
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
                        || vec_field(&f.unnamed.first().unwrap().ty).is_some()) =>
            {
                needs_payload = true;
                let ty = &f.unnamed.first().unwrap().ty;

                // Annotated **vector** variant `#[..] V(Vec<T>)` / `V(Vec<[T; N]>)`:
                // a `VecDesc` (16 bytes) in the payload naming a data block — the
                // per-variant mirror of a `#[bstack_owned/strong/weak/ref] Vec<..>`
                // struct field. A `Vec<[T; N]>` stores its offsets FLAT (like the
                // struct case), reshaped to `Vec<[[T;..];..]>` on read.
                if vec_field(ty).is_some() {
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
                        ::bstack_raii::VecDesc>(&__pl[..16]));

                    // A POD `V(Vec<Pod>)` / `V(String)` (un-annotated): a plain
                    // `BStackVec<elem>` (elem = the whole vec element type, itself
                    // `Pod` — arrays included — or `u8` for `String`). No block
                    // lifecycle, so clone is a verbatim byte copy.
                    if kind == Kind::Pod {
                        let elem: TokenStream = if vec_field(ty).is_some_and(|vi| vi.is_string) {
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
                                let mut __pl = [0u8; Self::__PAYLOAD];
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
                    if vec_field(ty).is_some_and(|vi| vi.is_string) {
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
                                let mut __pl = [0u8; Self::__PAYLOAD];
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
                            let mut __pl = [0u8; Self::__PAYLOAD];
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
                    // Flat byte read/write of leaf `#k`'s `u64` in the payload.
                    let pl_off = |k: &Ident| {
                        quote!(u64::from_le_bytes(
                            __pl[(#k) * 8..(#k) * 8 + 8].try_into().unwrap()))
                    };
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
                                (#disc, [0u8; Self::__PAYLOAD])
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
                        let view_leaf =
                            quote!(::core::option::Option<::bstack_raii::BStackRc<'__e, #elem, __A>>);
                        let view_ty_nested = nested_ty(&dims, &view_leaf);
                        view_variants.push(quote!(#vname(#view_ty_nested),));

                        // new: consume nested weaks → control offsets.
                        let cap_write = |k: &Ident, leaf: &Ident| {
                            pl_put(k, quote!(#leaf.into_raw().into_range().start()))
                        };
                        let consume = nested_consume(&dims, &quote!(__arr), &cap_write);
                        new_arms.push(quote! {
                            #data::#vname(__arr) => {
                                let mut __pl = [0u8; Self::__PAYLOAD];
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
                                    let __off = u64::from_le_bytes(
                                        __pl[__k * 8..__k * 8 + 8].try_into().unwrap());
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
                                    let __off = u64::from_le_bytes(
                                        __pl[__k * 8..__k * 8 + 8].try_into().unwrap());
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
                            let mut __pl = [0u8; Self::__PAYLOAD];
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
                                    let __off = u64::from_le_bytes(
                                        __pl[__k * 8..__k * 8 + 8].try_into().unwrap());
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
                                    let __off = u64::from_le_bytes(
                                        __pl[__k * 8..__k * 8 + 8].try_into().unwrap());
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
                                    let __off = u64::from_le_bytes(
                                        __pl[__k * 8..__k * 8 + 8].try_into().unwrap());
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
                                let mut __pl = [0u8; Self::__PAYLOAD];
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
                                            u64::from_le_bytes(__pl[..8].try_into().unwrap()),
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
                                let __off = u64::from_le_bytes(__pl[..8].try_into().unwrap());
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
                                let mut __pl = [0u8; Self::__PAYLOAD];
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
                                let mut __pl = [0u8; Self::__PAYLOAD];
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
                                    u64::from_le_bytes(__pl[..8].try_into().unwrap()),
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
                                let mut __pl = [0u8; Self::__PAYLOAD];
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
                                let __ctrl_off = u64::from_le_bytes(__pl[..8].try_into().unwrap());
                                __plan.bump_weak(__ctrl_off);
                            }
                        });
                    }
                    // `#[embed] V(Child)`: the child's whole on-disk form is stored
                    // INLINE in the payload (header and all).
                    Kind::Embed => {
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
                                (#disc, [0u8; Self::__PAYLOAD])
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
                        let mut __pl = [0u8; Self::__PAYLOAD];
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
    let prefix = attr
        .tag
        .as_ref()
        .map_or_else(|| auto_prefix(&type_name), |t| t.bytes().collect::<Vec<u8>>());
    let tag = build_tag(hash, &prefix);
    let eightcc = eightcc_expr(&tag.bytes);

    // Control-block tag (rc, weak): the data tag with its prefix lowercased, or a
    // `ctrl_tag` override.
    let ctrl_prefix = attr.ctrl_tag.as_ref().map_or_else(
        || prefix.iter().map(u8::to_ascii_lowercase).collect::<Vec<u8>>(),
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
    let data_ty = if has_shared {
        quote!(#data<'__e, __A>)
    } else {
        quote!(#data)
    };
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
                #vis fn new<'__e, __A: ::bstack_raii::BStackOwnedSliceAllocator>(
                    allocator: &'__e __A,
                    data: #data_ty,
                ) -> ::std::io::Result<#new_ret> {
                    #embed_decl
                    let (__disc, __payload): (#disc_ty, [u8; Self::__PAYLOAD]) = match data {
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
                #vis fn new<'__e, __A: ::bstack_raii::BStackOwnedSliceAllocator>(
                    allocator: &'__e __A,
                    data: #data_ty,
                ) -> ::std::io::Result<::bstack_raii::BStackRc<'__e, Self, __A>> {
                    #embed_decl
                    let (__disc, __payload): (#disc_ty, [u8; Self::__PAYLOAD]) = match data {
                        #(#new_arms)*
                    };
                    let __blocks = ::bstack_raii::alloc_many(allocator, &[#enum_size, #ctrl_size])?;
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
                        let _ = ::bstack_raii::free_many(allocator, [__data, __ctrl]);
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
            impl ::bstack_raii::TryCloneIn for #name {
                fn try_clone_in<__A: ::bstack_raii::BStackOwnedSliceAllocator>(
                    &self,
                    allocator: &__A,
                ) -> ::std::io::Result<::bstack_raii::BStackOwned<Self>> {
                    let mut __plan = ::bstack_raii::ClonePlan::new();
                    let __dst = match self.__bstack_clone_into(allocator, &mut __plan) {
                        ::std::result::Result::Ok(__d) => __d,
                        ::std::result::Result::Err(__e) => {
                            __plan.rollback(allocator);
                            return ::std::result::Result::Err(__e);
                        }
                    };
                    __plan.commit(allocator)?;
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
    // reference; `EView` only when a weak variant makes `read` upgrade.
    let data_generics = if has_shared {
        quote!(<'__e, __A: ::bstack_raii::BStackOwnedSliceAllocator>)
    } else {
        quote!()
    };
    let view_generics = if has_weak {
        quote!(<'__e, __A: ::bstack_raii::BStackOwnedSliceAllocator>)
    } else {
        quote!()
    };
    let view_ty = if has_weak {
        quote!(#view<'__e, __A>)
    } else {
        quote!(#view)
    };
    // `bstack_move!` yields the same `EData` (owned handles); `Fields` just names
    // it with the move lifetime.
    let move_fields_ty = if has_shared {
        quote!(#data<'__mv, __A>)
    } else {
        quote!(#data)
    };
    // `bstack_move!` frees the enum shell, then rebuilds the active variant's
    // payload as an owned handle.
    let move_payload = if needs_payload {
        quote!(let __pl = __od.__bstack_payload;)
    } else {
        quote!()
    };

    Ok(quote! {
        #[derive(::core::clone::Clone, ::core::marker::Copy)]
        #vis struct #name(::bstack_raii::BStackRange);

        impl #name {
            /// The payload area size (bytes) — the max over all variants.
            #[doc(hidden)]
            pub const __PAYLOAD: usize = {
                let __s = [0usize #(, #payload_sizes)*];
                let mut __m = 0usize;
                let mut __i = 0usize;
                while __i < __s.len() {
                    if __s[__i] > __m {
                        __m = __s[__i];
                    }
                    __i += 1;
                }
                __m
            };
        }

        #[repr(C, packed)]
        #[derive(::core::clone::Clone, ::core::marker::Copy)]
        #vis struct #on_disk {
            __bstack_header: ::bstack_raii::BlockHeader,
            #injected_ondisk
            __bstack_disc: #disc_ty,
            __bstack_payload: [u8; #name::__PAYLOAD],
        }
        unsafe impl ::bstack_raii::Zeroable for #on_disk {}
        unsafe impl ::bstack_raii::Pod for #on_disk {}

        const _: fn() = || {
            fn __assert_pod<__T: ::bstack_raii::Pod>() {}
            #( __assert_pod::<#pod_types>(); )*
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

        impl ::bstack_raii::BStackCast for #name {
            fn eightcc() -> ::bstack_raii::EightCC {
                #eightcc
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

        impl #name {
            /// Free the active variant's owned child (recursively) given this
            /// block's range, **without** freeing the block itself — used when the
            /// enum is `#[embed]`ded, and by `bstack_drop` before the self-dealloc.
            #[doc(hidden)]
            #vis fn __bstack_drop_children<__A: ::bstack_raii::BStackOwnedSliceAllocator>(
                __range: ::bstack_raii::BStackRange,
                allocator: &__A,
            ) -> ::std::io::Result<()> {
                use ::bstack_raii::BStackDrop as _;
                #drop_children_body
                ::std::result::Result::Ok(())
            }

            /// Read this enum's OnDisk and return a deep-cloned copy: the active
            /// variant's payload fixed up (owned child cloned into `__plan`,
            /// strong/weak bumped, ref aliased, embedded child folded in place),
            /// without allocating a block for `self`. Used to fold an
            /// `#[embed]`ded enum inline, and by `__bstack_clone_into`.
            #[doc(hidden)]
            #[allow(unused_variables)]
            #vis fn __bstack_clone_children_inplace<__A: ::bstack_raii::BStackOwnedSliceAllocator>(
                &self,
                allocator: &__A,
                __plan: &mut ::bstack_raii::ClonePlan,
            ) -> ::std::io::Result<#on_disk> {
                #clone_children_body
            }

            /// Deep-clone this enum into a `ClonePlan`: allocate a fresh block and
            /// stage its fixed-up payload. Returns the new block's range. Also lets
            /// an owned enum child of a struct be recursed into.
            #[doc(hidden)]
            #[allow(unused_variables)]
            #vis fn __bstack_clone_into<__A: ::bstack_raii::BStackOwnedSliceAllocator>(
                &self,
                allocator: &__A,
                __plan: &mut ::bstack_raii::ClonePlan,
            ) -> ::std::io::Result<::bstack_raii::BStackRange> {
                #clone_into_body
            }
        }

        impl ::bstack_raii::BStackDrop for #name {
            fn bstack_drop<__A: ::bstack_raii::BStackOwnedSliceAllocator>(
                self,
                allocator: &__A,
            ) -> ::std::io::Result<()> {
                Self::__bstack_drop_children(self.0, allocator)?;
                unsafe { ::bstack_raii::dealloc_range(allocator, self.0) }
            }
        }

        impl #name {
            /// Allocate a new enum block holding `data`'s variant + payload.
            #enum_new

            /// Read the current variant. Takes the allocator (a weak variant's
            /// read upgrades through it; other variants just read the block).
            #vis fn read<'__e, __A: ::bstack_raii::BStackOwnedSliceAllocator>(
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
        }

        #shared_impl
        #weakable_items

        impl ::bstack_raii::BStackMove for #name {
            type Fields<'__mv, __A: ::bstack_raii::BStackOwnedSliceAllocator> = #move_fields_ty;
            fn bstack_move<'__mv, __A: ::bstack_raii::BStackOwnedSliceAllocator>(
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
