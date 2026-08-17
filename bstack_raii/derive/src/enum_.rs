//! Implementation of the `#[bstack_enum]` attribute macro (orchestrator). The
//! shared classification / analysis / emit machinery lives in [`crate::common`].

use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{Error, Fields, Ident};

use crate::emit::*;
use crate::layout;
use crate::model::VariantParts;
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
                    "[BSTACK0403] a generic #[bstack_enum] currently supports only type parameters (no \
                     lifetime or const generics)",
                ));
            }
        }
        if mode != Mode::Plain {
            return Err(Error::new_spanned(
                &input.generics,
                "[BSTACK0402] a generic #[bstack_enum] currently supports plain mode only (not `rc` / \
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
                "[BSTACK0603] #[bstack_mut] on a `#[bstack_enum]` goes on the enum itself (a whole-value \
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
                    "[BSTACK0406] a generic type parameter in a `#[bstack_enum]` variant must be a reference \
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
                        "[BSTACK0405] a generic type parameter in a non-`Foreign` position of a `Foreign` \
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

    // Every variant's contributions accumulate here (the enum's Slot IR bundle):
    // the `EData` / `EView` variant decls, the `new` / `read` / `move` / `drop` /
    // `clone` match arms, per-variant payload sizes, POD / embed assertion types, and
    // the flags that shape the companion enums' generics (a strong/weak variant makes
    // `EData` `<'e, A>`; a weak one also makes `EView`; a `Foreign` variant makes both
    // carry `'__e`; `#[embed]` folds the child in post-write; `needs_payload` drives
    // the payload read). Enum embed is always concrete (a generic embed variant is
    // rejected in `EUsage`), so `embed_types` is asserted `BStackEmbeddable`.
    let mut vp = VariantParts::default();

    for (i, variant) in input.variants.iter().enumerate() {
        let disc = &disc_pats[i];
        let vname = &variant.ident;
        let kind = classify_attrs(&variant.attrs)?;

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
                vp.needs_payload = true;
                let ty = &f.unnamed.first().unwrap().ty;

                // Annotated **vector** variant `#[..] V(Vec<T>)` / `V(Vec<[T; N]>)`:
                // a `VecDesc` (16 bytes) in the payload naming a data block — the
                // per-variant mirror of a `#[bstack_owned/strong/weak/ref] Vec<..>`
                // struct field. A `Vec<[T; N]>` stores its offsets FLAT (like the
                // struct case), reshaped to `Vec<[[T;..];..]>` on read.
                if let Some(p) = vec_variant(ty, vname, disc, kind, &data, &view, &payload_const)? {
                    vp.merge(p);
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
                if let Some(p) = array_variant(
                    ty,
                    vname,
                    disc,
                    kind,
                    &data,
                    &view,
                    &on_disk,
                    &payload_const,
                )? {
                    vp.merge(p);
                    continue;
                }

                // Annotated **foreign** variant `#[..] V(Foreign<T>)`: a cross-file
                // wide pointer stored as a 16-byte `ForeignPtr` in the payload. The
                // annotation names the target's ownership in its own file (teardown /
                // clone dispatch cross-file, like a scalar `Foreign` struct field).
                // Concrete target only for now; container-in-variant is not handled.
                if let Some(p) =
                    foreign_variant(ty, vname, disc, kind, &data, &view, &payload_const)?
                {
                    vp.merge(p);
                    continue;
                }

                // Annotated **foreign tuple** variant `#[..] V((A, Foreign<T>, ..))`:
                // POD elements packed inline, each foreign element a 16-byte
                // `ForeignPtr`, all at cumulative byte offsets in the payload (the
                // per-variant mirror of a `#[ann] (A, Foreign<T>)` struct field). The
                // annotation names the foreign elements' ownership.
                if let Some(p) =
                    foreign_tuple_variant(ty, vname, disc, kind, &data, &view, &payload_const)?
                {
                    vp.merge(p);
                    continue;
                }

                vp.merge(single_block_variant(
                    ty,
                    vname,
                    disc,
                    kind,
                    &data,
                    &view,
                    &on_disk,
                    &payload_const,
                )?);
            }
            // A POD aggregate: unit, an all-POD tuple `V(A, B, ..)`, or an all-POD
            // struct `V { x: A, .. }`. The fields are packed sequentially into the
            // payload (declaration order). This is sound because the payload is
            // read/written **unaligned**, so field alignment is irrelevant — the
            // packed byte sequence of POD fields is itself just POD bytes.
            _ => {
                vp.merge(pod_aggregate_variant(
                    variant,
                    vname,
                    disc,
                    kind,
                    &data,
                    &view,
                    &payload_const,
                )?);
            }
        }
    }

    // Unpack the accumulated parts into the names the assembly below reads.
    let VariantParts {
        data_variants,
        view_variants,
        new_arms,
        read_arms,
        move_arms,
        drop_arms,
        clone_arms,
        payload_sizes,
        pod_types,
        embed_types,
        has_embed: enum_has_embed,
        needs_payload,
        has_shared,
        has_weak,
        has_foreign,
    } = vp;

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
                "[BSTACK0604] #[bstack_mut] on a `#[bstack_enum]` is only supported for a plain enum — a \
                 shared (`rc` / `rc, weak`) enum's refcount / control block can't be \
                 overwritten in place; rebuild the value instead",
            ));
        }
        if enum_has_embed {
            return Err(Error::new_spanned(
                &input.ident,
                "[BSTACK0605] #[bstack_mut] is not yet supported on a `#[bstack_enum]` with an `#[embed]` \
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
