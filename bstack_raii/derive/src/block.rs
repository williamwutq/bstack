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

use crate::emit::*;
use crate::util::*;

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
    let mut pod_types: Vec<Type> = Vec::new();
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

        // `Foreign<T>` scalar / `Option<Foreign<T>>`: a 16-byte `ForeignRepr` slot
        // resolved through the registry. Lowered by `emit::foreign_field` (the inline
        // slot, lifetime-bound accessor, ctor wiring, per-kind cross-file teardown /
        // deep-clone, `bstack_move!` RAII duals, and `#[bstack_mut]` mutators).
        if let Some(ftarget) = foreign_inner(opt_inner) {
            let fp = foreign_field(
                vis,
                fname,
                field,
                ftarget,
                kind,
                nullable,
                &on_disk_ty,
                &type_params,
            )?;
            on_disk_fields.extend(fp.on_disk_fields);
            accessors.extend(fp.accessors);
            ctor_params.extend(fp.ctor_params);
            ctor_preps.extend(fp.ctor_preps);
            ctor_inits.extend(fp.ctor_inits);
            drop_stmts.extend(fp.drop_stmts);
            clone_stmts.extend(fp.clone_stmts);
            mv_caps.extend(fp.mv_caps);
            mv_types.extend(fp.mv_types);
            mv_recon.extend(fp.mv_recon);
            wrapper_defs.extend(fp.wrapper_defs);
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
            vec_info(opt_inner)
        };
        if let Some(vinfo) = vinfo {
            let fp = vec_field(
                vis,
                fname,
                field,
                opt_inner,
                vinfo,
                kind,
                nullable,
                &on_disk_ty,
                &type_params,
                &const_params,
            )?;
            on_disk_fields.extend(fp.on_disk_fields);
            accessors.extend(fp.accessors);
            ctor_params.extend(fp.ctor_params);
            ctor_preps.extend(fp.ctor_preps);
            ctor_inits.extend(fp.ctor_inits);
            drop_stmts.extend(fp.drop_stmts);
            clone_stmts.extend(fp.clone_stmts);
            mv_caps.extend(fp.mv_caps);
            mv_types.extend(fp.mv_types);
            mv_recon.extend(fp.mv_recon);
            wrapper_defs.extend(fp.wrapper_defs);
            continue;
        }

        // Inline array of vectors `[Vec<T>; N]` — possibly nested `[[Vec<T>;N];M]`
        // and/or per-element `[Option<Vec<T>>; N]`: N independent inline `VecDesc`s,
        // each owning its own data block. Detected as an array whose leaf is a
        // `Vec` / `String`. A POD `[Vec<Pod>; N]` is intercepted here too — the
        // `VecDesc`s are Pod bytes, but the data blocks need a real lifecycle, so it
        // must NOT fall through to the plain POD path. The element annotation names
        // the inner vectors' element ownership, exactly like a scalar `Vec<T>`.
        if let Some(fp) =
            vec_array_field(vis, fname, field, opt_inner, kind, nullable, &on_disk_ty, &const_params)?
        {
            on_disk_fields.extend(fp.on_disk_fields);
            accessors.extend(fp.accessors);
            ctor_params.extend(fp.ctor_params);
            ctor_preps.extend(fp.ctor_preps);
            ctor_inits.extend(fp.ctor_inits);
            drop_stmts.extend(fp.drop_stmts);
            clone_stmts.extend(fp.clone_stmts);
            mv_caps.extend(fp.mv_caps);
            mv_types.extend(fp.mv_types);
            mv_recon.extend(fp.mv_recon);
            continue;
        }

        // `#[ann] [Foreign<T>; N]` — an inline array of cross-file wide pointers
        // (possibly nested `[[Foreign<T>; A]; B]` / per-element `[Option<Foreign<T>>; N]`).
        // Stored flat as `[ForeignPtr; TOTAL]` inline (16 B each, no data block); each
        // slot's teardown / clone dispatches cross-file exactly like a scalar `Foreign`.
        // A null/unset slot is a `Foreign` whose offset is `0`. Must be annotated.
        if let Some(fp) = foreign_array_field(
            vis,
            fname,
            field,
            opt_inner,
            kind,
            nullable,
            &on_disk_ty,
            &type_params,
            &const_params,
        )? {
            on_disk_fields.extend(fp.on_disk_fields);
            accessors.extend(fp.accessors);
            ctor_params.extend(fp.ctor_params);
            ctor_preps.extend(fp.ctor_preps);
            ctor_inits.extend(fp.ctor_inits);
            drop_stmts.extend(fp.drop_stmts);
            clone_stmts.extend(fp.clone_stmts);
            mv_caps.extend(fp.mv_caps);
            mv_types.extend(fp.mv_types);
            mv_recon.extend(fp.mv_recon);
            wrapper_defs.extend(fp.wrapper_defs);
            continue;
        }

        // Inline fixed-size array `[T; N]` — possibly *nested* `[[..]; ..]` — of
        // block references. (A POD array falls through to the POD path below: an
        // array of `Pod` is `Pod`.) Stored **flat** as `[u64; N0*..*Nk]` inline
        // (no data block), one offset per leaf, with per-element ownership; the
        // accessor / ctor / move traffic in the nested `[[Handle; ..]; ..]` shape.
        if let Some(fp) = block_array_field(
            vis,
            fname,
            field,
            opt_inner,
            kind,
            nullable,
            &on_disk_ty,
            &type_params,
            &const_params,
        )? {
            on_disk_fields.extend(fp.on_disk_fields);
            accessors.extend(fp.accessors);
            setters.extend(fp.setters);
            ctor_params.extend(fp.ctor_params);
            ctor_preps.extend(fp.ctor_preps);
            ctor_inits.extend(fp.ctor_inits);
            ctor_post.extend(fp.ctor_post);
            drop_stmts.extend(fp.drop_stmts);
            clone_stmts.extend(fp.clone_stmts);
            mv_caps.extend(fp.mv_caps);
            mv_types.extend(fp.mv_types);
            mv_recon.extend(fp.mv_recon);
            wrapper_defs.extend(fp.wrapper_defs);
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
        if let Some(fp) =
            foreign_tuple_field(vis, name, fname, field, inner_ty, kind, nullable, &on_disk_ty)?
        {
            on_disk_fields.extend(fp.on_disk_fields);
            accessors.extend(fp.accessors);
            ctor_params.extend(fp.ctor_params);
            ctor_preps.extend(fp.ctor_preps);
            ctor_inits.extend(fp.ctor_inits);
            drop_stmts.extend(fp.drop_stmts);
            clone_stmts.extend(fp.clone_stmts);
            mv_caps.extend(fp.mv_caps);
            mv_types.extend(fp.mv_types);
            mv_recon.extend(fp.mv_recon);
            pod_types.extend(fp.pod_types);
            wrapper_defs.extend(fp.wrapper_defs);
            continue;
        }

        // A POD **tuple** field `a: (A, B, ..)`: a Rust tuple is not `Pod`, but a
        // packed struct of its (POD) elements is — alignment is irrelevant on disk
        // — so store it through a generated wrapper and rebuild the tuple on read.
        // `bstack_move!` hands back the tuple as one element (not flattened).
        if let Some(fp) = pod_tuple_field(vis, name, fname, field, inner_ty, kind, &on_disk_ty)? {
            on_disk_fields.extend(fp.on_disk_fields);
            accessors.extend(fp.accessors);
            ctor_params.extend(fp.ctor_params);
            ctor_preps.extend(fp.ctor_preps);
            ctor_inits.extend(fp.ctor_inits);
            mv_caps.extend(fp.mv_caps);
            mv_types.extend(fp.mv_types);
            mv_recon.extend(fp.mv_recon);
            pod_types.extend(fp.pod_types);
            wrapper_defs.extend(fp.wrapper_defs);
            continue;
        }

        // `#[embed] child: Block`: store the child's whole on-disk form INLINE
        // (`<Child as BStackBlock>::OnDisk`, header and all) instead of a `u64`
        // offset — an exclusively-owned inline block.
        if let Some(fp) =
            embed_field(vis, fname, field, inner_ty, kind, nullable, &on_disk_ty, &type_params)?
        {
            on_disk_fields.extend(fp.on_disk_fields);
            accessors.extend(fp.accessors);
            ctor_params.extend(fp.ctor_params);
            ctor_preps.extend(fp.ctor_preps);
            ctor_inits.extend(fp.ctor_inits);
            ctor_post.extend(fp.ctor_post);
            drop_stmts.extend(fp.drop_stmts);
            clone_stmts.extend(fp.clone_stmts);
            mv_caps.extend(fp.mv_caps);
            mv_types.extend(fp.mv_types);
            mv_recon.extend(fp.mv_recon);
            wrapper_defs.extend(fp.wrapper_defs);
            continue;
        }

        // Scalar fall-through (POD inline, or a single owned/strong/weak/ref block
        // reference): reader / mutators / ctor / teardown / clone / move.
        let fp = scalar_field(vis, fname, field, inner_ty, kind, nullable, &on_disk_ty)?;
        on_disk_fields.extend(fp.on_disk_fields);
        accessors.extend(fp.accessors);
        setters.extend(fp.setters);
        ctor_params.extend(fp.ctor_params);
        ctor_preps.extend(fp.ctor_preps);
        ctor_inits.extend(fp.ctor_inits);
        drop_stmts.extend(fp.drop_stmts);
        clone_stmts.extend(fp.clone_stmts);
        mv_caps.extend(fp.mv_caps);
        mv_types.extend(fp.mv_types);
        mv_recon.extend(fp.mv_recon);
        pod_types.extend(fp.pod_types);
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
    let shared_impl = shared_impl(mode, name);

    // A plain block is self-contained (no separate control block), so it may be
    // `#[embed]`ded; `(rc)` / `(rc, weak)` blocks are deliberately not `BStackEmbeddable`.
    let embeddable_impl = if mode == Mode::Plain {
        quote! {
            impl #impl_g ::bstack_raii::__private::BStackEmbeddable for #name #ty_g #where_g {}
        }
    } else {
        quote!()
    };

    let weakable_items = weakable_items(mode, name, &control, vis);

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
