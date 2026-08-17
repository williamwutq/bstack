//! The per-field / per-variant **code emitters**: readers/accessors, constructors,
//! setters/`replace_` mutators (scalar, array, foreign), teardown/clone statements,
//! `bstack_move!` pieces, and the vector/`block_vec` machinery. Built on the
//! analysis primitives in [`crate::util`].

use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{Ident, Type};

use crate::util::*;

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

