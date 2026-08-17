//! The per-field / per-variant **code emitters**: readers/accessors, constructors,
//! setters/`replace_` mutators (scalar, array, foreign), teardown/clone statements,
//! `bstack_move!` pieces, and the vector/`block_vec` machinery. Built on the
//! analysis primitives in [`crate::util`].

use proc_macro2::{Span, TokenStream};
use quote::{format_ident, quote};
use syn::{Error, Ident, Type};

use crate::model::{FieldParts, VariantParts};
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
pub(crate) fn vec_move(
    cap: &Ident,
    elem: &TokenStream,
    nullable: bool,
) -> (TokenStream, TokenStream) {
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

/// `impl BStackShared for #name` for `rc` / `(rc, weak)` blocks — identical for
/// structs and enums (it depends only on the block name and mode). Plain blocks
/// own no strong count, so they get nothing.
pub(crate) fn shared_impl(mode: Mode, name: &Ident) -> TokenStream {
    match mode {
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
    }
}

/// The `(rc, weak)` control-block struct (`#control`) + its `impl BStackWeakable`
/// — identical for structs and enums. Empty for other modes.
pub(crate) fn weakable_items(
    mode: Mode,
    name: &Ident,
    control: &Ident,
    vis: &syn::Visibility,
) -> TokenStream {
    if mode == Mode::RcWeak {
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
    }
}

/// Lower a scalar `Foreign<T>` / `Option<Foreign<T>>` field to its [`FieldParts`]:
/// the inline `ForeignRepr` slot, the lifetime-bound accessor, ctor wiring,
/// per-kind cross-file teardown / deep-clone, `bstack_move!` RAII-dual pieces, and
/// (for `#[bstack_mut]`) the `replace_` / `set_` mutators.
#[allow(clippy::too_many_arguments)]
pub(crate) fn foreign_field(
    vis: &syn::Visibility,
    fname: &Ident,
    field: &syn::Field,
    ftarget: &Type,
    kind: Kind,
    nullable: bool,
    on_disk_ty: &TokenStream,
    type_params: &[&Ident],
) -> syn::Result<FieldParts> {
    let mut parts = FieldParts::default();
    let getter = format_ident!("get_{}", fname);
    validate_foreign_target(
        kind,
        ftarget,
        &field.ty,
        "`Foreign<T>`",
        format_ident!("__bstack_foreign_target_{}", fname),
        !type_mentions_any(ftarget, type_params),
        &mut parts.wrapper_defs,
    )?;
    parts
        .on_disk_fields
        .push(quote!(#fname: ::bstack_raii::ForeignRepr,));
    let field_ty = quote!(::bstack_raii::Foreign<#ftarget>);
    let cap = format_ident!("__cap_{}", fname);
    parts.mv_caps.push(quote!(let #cap = __od.#fname;));

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
        parts.accessors.push(quote! {
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
        parts
            .ctor_params
            .push(quote!(#fname: ::core::option::Option<#field_ty>,));
        parts.ctor_preps.push(quote! {
            let #fname: ::bstack_raii::ForeignRepr = match #fname {
                ::core::option::Option::Some(__f) => __f.repr(),
                ::core::option::Option::None => ::bstack_raii::ForeignRepr::new(0, 0),
            };
        });
        parts.ctor_inits.push(quote!(#fname: #fname,));
        parts
            .mv_types
            .push(quote!(::core::option::Option<#mv_leaf_ty>));
        parts.mv_recon.push(quote! {
            if #cap.offset() == 0 {
                ::core::option::Option::None
            } else {
                // SAFETY: `#cap` was stored into this file; the handle is bound
                // to `'__mv` and owns the target per the field annotation.
                ::core::option::Option::Some(#mv_leaf_expr)
            }
        });
    } else {
        parts.accessors.push(quote! {
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
        parts.ctor_params.push(quote!(#fname: #field_ty,));
        parts
            .ctor_preps
            .push(quote!(let #fname: ::bstack_raii::ForeignRepr = #fname.repr();));
        parts.ctor_inits.push(quote!(#fname: #fname,));
        parts.mv_types.push(quote!(#mv_leaf_ty));
        parts.mv_recon.push(quote!(#mv_leaf_expr));
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
        parts.drop_stmts.push(quote! {
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
        parts.clone_stmts.push(cs);
    }

    // `#[bstack_mut]`: `replace_<f>` (owned/strong/weak — moves the old
    // cross-file target out as its RAII dual) and, for a foreign `ref`, also
    // `set_<f>`. One crash-atomic 16-byte `ForeignRepr` write; the swap is
    // purely local (no registry / host access), the cross-file free/decrement
    // travelling with the returned handle.
    if is_bstack_mut(&field.attrs) {
        for m in foreign_mut_methods(vis, fname, &quote!(#ftarget), on_disk_ty, kind, nullable) {
            parts.accessors.push(m);
        }
    }
    Ok(parts)
}

/// Lower a `Vec<T>` / `String` (and their `Option`, `Vec<Foreign>`, `Vec<[T; N]>`
/// forms) field to its [`FieldParts`]: the inline `VecDesc` slot, the `BStackVec`-
/// family accessor, ctor wiring, per-kind teardown / deep-clone, and `bstack_move!`
/// pieces. `vinfo` is the element/`is_string` classification from `util::vec_info`.
#[allow(clippy::too_many_arguments)]
pub(crate) fn vec_field(
    vis: &syn::Visibility,
    fname: &Ident,
    field: &syn::Field,
    opt_inner: &Type,
    vinfo: VecInfo,
    kind: Kind,
    nullable: bool,
    on_disk_ty: &TokenStream,
    type_params: &[&Ident],
    const_params: &[&Ident],
) -> syn::Result<FieldParts> {
    let mut parts = FieldParts::default();
    let getter = format_ident!("get_{}", fname);
    let elem = &vinfo.elem;
    // The descriptor lives inline in the field (no descriptor block).
    parts
        .on_disk_fields
        .push(quote!(#fname: ::bstack_raii::VecDesc,));
    let cap = format_ident!("__cap_{}", fname);
    parts.mv_caps.push(quote!(let #cap = __od.#fname;));

    // `String` is always POD bytes; a block annotation on it is meaningless.
    if vinfo.is_string && kind != Kind::Pod {
        return Err(Error::new_spanned(
            &field.ty,
            "[BSTACK0107] `String` is always POD; remove the ownership annotation",
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
            !type_mentions_any(ftarget, type_params),
            &mut parts.wrapper_defs,
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
        parts.accessors.push(quote! {
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
        parts.ctor_params.push(param);
        parts.ctor_preps.push(prep);
        parts.ctor_inits.push(quote!(#fname: #fname,));

        // ---- Teardown: dispatch each element, then free the data block ----
        let elem_drop = foreign_elem_drop(kind, ftarget);
        let drop_loop = if matches!(kind, Kind::Ref) {
            quote!()
        } else {
            quote! {
                for __fp in #store::from_desc(__desc, allocator).to_vec()? { #elem_drop }
            }
        };
        parts.drop_stmts.push(quote! {
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
        parts.clone_stmts.push(quote! {
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
        parts.mv_types.push(mvt);
        parts.mv_recon.push(mvr);
        return Ok(parts);
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
        reject_nested_const_dims(&dims, const_params, &field.ty)?;
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
        parts.accessors.push(quote! {
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
        parts.ctor_params.push(param);
        parts.ctor_preps.push(prep);
        parts.ctor_inits.push(quote!(#fname: #fname,));

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
        parts.drop_stmts.push(quote! {
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
        parts.clone_stmts.push(quote! {
            {
                let __srcdesc: ::bstack_raii::VecDesc = __od.#fname;
                if __srcdesc.data_off != 0 {
                    #clone_body
                }
            }
        });

        // ---- Move: yield the flat block-vector handle (loses `[T; N]` shape) ----
        let (mvt, mvr) = block_vec_move(&cap, &elem_ts, vec_ty, nullable);
        parts.mv_types.push(mvt);
        parts.mv_recon.push(mvr);
        return Ok(parts);
    }

    // The annotation states the *elements'* relationship (the descriptor
    // + array is always owned by this struct regardless). No annotation =>
    // POD elements (byte storage, requiring `T: Pod`).
    let (drop_s, acc, ctor, mv) = match kind {
        Kind::Embed => {
            return Err(Error::new_spanned(
                &field.ty,
                "[BSTACK0501] cannot #[embed] a `Vec` / `String`; embed a `#[bstack_block]` type",
            ));
        }
        Kind::Pod => (
            vec_drop_stmt(fname, elem, nullable),
            vec_accessor(vis, fname, elem, on_disk_ty, nullable),
            vec_ctor(fname, &vinfo, nullable),
            vec_move(&cap, elem, nullable),
        ),
        Kind::Owned => (
            block_vec_drop_stmt(fname, quote!(BStackBlockVec), elem, nullable),
            block_vec_accessor(
                vis,
                fname,
                elem,
                on_disk_ty,
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
                on_disk_ty,
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
                on_disk_ty,
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
            block_vec_accessor(vis, fname, elem, on_disk_ty, quote!(BStackRefVec), nullable),
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
    parts.drop_stmts.push(drop_s);
    parts.clone_stmts.push(vec_clone_stmt(fname, kind, elem));
    parts.accessors.push(acc);
    let (param, prep, init) = ctor;
    parts.ctor_params.push(param);
    parts.ctor_preps.push(prep);
    parts.ctor_inits.push(init);
    let (mv_ty, mv_rc) = mv;
    parts.mv_types.push(mv_ty);
    parts.mv_recon.push(mv_rc);
    Ok(parts)
}

/// Lower an inline **array of vectors** `[Vec<T>; N]` (nested / per-element
/// `Option` included) to its [`FieldParts`] — N independent inline `VecDesc`s,
/// each owning its own data block. Returns `Ok(None)` if the field isn't an array
/// whose leaf is a `Vec` / `String` (so the caller falls through to the next shape).
#[allow(clippy::too_many_arguments)]
pub(crate) fn vec_array_field(
    vis: &syn::Visibility,
    fname: &Ident,
    field: &syn::Field,
    opt_inner: &Type,
    kind: Kind,
    nullable: bool,
    on_disk_ty: &TokenStream,
    const_params: &[&Ident],
) -> syn::Result<Option<FieldParts>> {
    let Type::Array(_) = opt_inner else {
        return Ok(None);
    };
    let mut parts = FieldParts::default();
    let getter = format_ident!("get_{}", fname);
    let (dims, leaf, leaf_nullable) = array_shape(opt_inner)?;
    reject_nested_const_dims(&dims, const_params, &field.ty)?;
    let leaf_vinfo = if is_str(leaf) {
        Some(VecInfo {
            elem: quote!(u8),
            is_string: true,
        })
    } else {
        vec_info(leaf)
    };
    if let Some(leaf_vinfo) = leaf_vinfo {
        // Validate the leaf vector's own element nesting (`Vec<Vec<..>>`).
        check_container_nesting(leaf)?;
        if nullable {
            return Err(Error::new_spanned(
                &field.ty,
                "[BSTACK0110] a whole-array `Option<[Vec<T>; N]>` is not supported; use \
                         `[Option<Vec<T>>; N]` for per-element nullability",
            ));
        }
        if leaf_vinfo.is_string && kind != Kind::Pod {
            return Err(Error::new_spanned(
                &field.ty,
                "[BSTACK0107] `String` is always POD; remove the ownership annotation",
            ));
        }
        if kind == Kind::Embed {
            return Err(Error::new_spanned(
                &field.ty,
                "[BSTACK0501] cannot #[embed] a `Vec` / `String`; embed a `#[bstack_block]` type",
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
        parts
            .on_disk_fields
            .push(quote!(#fname: [::bstack_raii::VecDesc; #total],));

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
        parts.accessors.push(quote! {
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
        parts.ctor_params.push(quote!(#fname: #ctor_param_ty,));
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
        parts.ctor_preps.push(quote! {
            let #fname: [::bstack_raii::VecDesc; #total] = {
                let mut __slots =
                    [<::bstack_raii::VecDesc as ::core::default::Default>::default();
                        #total];
                #flatten
                __slots
            };
        });
        parts.ctor_inits.push(quote!(#fname: #fname,));

        // Teardown: free each vector's data block.
        parts.drop_stmts.push(quote! {
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
        parts.clone_stmts.push(quote! {
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
        parts.mv_caps.push(quote!(let #cap = __od.#fname;));
        let mv_handle = quote!(::bstack_raii::#vec_ty<'__mv, #elem, __A>);
        let mv_leaf = if leaf_nullable {
            quote!(::core::option::Option<#mv_handle>)
        } else {
            mv_handle.clone()
        };
        parts.mv_types.push(nested_ty(&dims, &mv_leaf));
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
        parts.mv_recon.push(nested_build(&dims, &mv_leaf, &mv_read));
        return Ok(Some(parts));
    }
    Ok(None)
}

/// Lower an inline **array of `Foreign`** `[Foreign<T>; N]` (nested / per-element
/// `Option`) to its [`FieldParts`] — a flat `[ForeignRepr; TOTAL]` inline, each
/// slot's teardown / deep-clone dispatching cross-file like a scalar `Foreign`.
/// Returns `Ok(None)` if the field isn't an array whose leaf is a `Foreign`.
#[allow(clippy::too_many_arguments)]
pub(crate) fn foreign_array_field(
    vis: &syn::Visibility,
    fname: &Ident,
    field: &syn::Field,
    opt_inner: &Type,
    kind: Kind,
    nullable: bool,
    on_disk_ty: &TokenStream,
    type_params: &[&Ident],
    const_params: &[&Ident],
) -> syn::Result<Option<FieldParts>> {
    let Type::Array(_) = opt_inner else {
        return Ok(None);
    };
    let mut parts = FieldParts::default();
    let getter = format_ident!("get_{}", fname);
    let (adims, aleaf, aleaf_nullable) = array_shape(opt_inner)?;
    if let Some(ftarget) = foreign_inner(aleaf) {
        reject_nested_const_dims(&adims, const_params, &field.ty)?;
        validate_foreign_target(
            kind,
            ftarget,
            &field.ty,
            "`[Foreign<T>; N]`",
            format_ident!("__bstack_foreign_arr_target_{}", fname),
            !type_mentions_any(ftarget, type_params),
            &mut parts.wrapper_defs,
        )?;
        if nullable {
            return Err(Error::new_spanned(
                &field.ty,
                "[BSTACK0111] a whole-array `Option<[Foreign<T>; N]>` is not supported; a null foreign \
                         element is a `Foreign` with offset 0, or use `[Option<Foreign<T>>; N]`",
            ));
        }
        let total = dims_prod(&adims);
        let field_ty = quote!(::bstack_raii::Foreign<#ftarget>);
        parts
            .on_disk_fields
            .push(quote!(#fname: [::bstack_raii::ForeignRepr; #total],));

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
        parts.accessors.push(quote! {
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
        parts.ctor_params.push(quote!(#fname: #param_ty,));
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
        parts.ctor_preps.push(quote! {
            let #fname: [::bstack_raii::ForeignRepr; #total] = {
                let mut __slots = [::bstack_raii::ForeignRepr::new(0, 0); #total];
                #flatten
                __slots
            };
        });
        parts.ctor_inits.push(quote!(#fname: #fname,));

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
        parts.drop_stmts.push(quote! { { #drop_body } });

        // ---- Clone: dispatch each slot into a fresh `[ForeignPtr; TOTAL]` ----
        let elem_clone = foreign_elem_clone(kind, ftarget);
        parts.clone_stmts.push(quote! {
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
        parts.mv_caps.push(quote!(let #cap = __od.#fname;));
        let mv_leaf = if aleaf_nullable {
            quote!(::core::option::Option<::bstack_raii::Foreign<'__mv, #ftarget>>)
        } else {
            quote!(::bstack_raii::Foreign<'__mv, #ftarget>)
        };
        parts.mv_types.push(nested_ty(&adims, &mv_leaf));
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
        parts
            .mv_recon
            .push(nested_build(&adims, &mv_leaf, &mv_read));
        return Ok(Some(parts));
    }
    Ok(None)
}

/// Lower an inline **array of block references** `[T; N]` (nested `[[..]; ..]`,
/// per-element `Option`, and the `#[embed]` / weak variants) to its [`FieldParts`]
/// — a flat `[u64; N0*..*Nk]` (or inline embed) with per-element ownership. Returns
/// `Ok(None)` for a POD array or a non-array (fall through to the POD scalar path).
#[allow(clippy::too_many_arguments)]
pub(crate) fn block_array_field(
    vis: &syn::Visibility,
    fname: &Ident,
    field: &syn::Field,
    opt_inner: &Type,
    kind: Kind,
    nullable: bool,
    on_disk_ty: &TokenStream,
    type_params: &[&Ident],
    const_params: &[&Ident],
) -> syn::Result<Option<FieldParts>> {
    if kind == Kind::Pod {
        return Ok(None);
    }
    let Type::Array(_) = opt_inner else {
        return Ok(None);
    };
    let mut parts = FieldParts::default();
    let getter = format_ident!("get_{}", fname);
    if nullable {
        return Err(Error::new_spanned(
            &field.ty,
            "[BSTACK0105] a whole-array `Option<[T; N]>` is not supported; use `[Option<T>; N]` \
                     for per-element nullability",
        ));
    }
    let (dims, elem, elem_nullable) = array_shape(opt_inner)?;
    reject_nested_const_dims(&dims, const_params, &field.ty)?;
    let total = dims_prod(&dims);

    // `#[embed] [Child; N]` (or nested): N verbatim child on-disk forms
    // inline (`[<Child as BStackBlock>::OnDisk; TOTAL]`, flat). Construction
    // folds each `BStackOwned<Child>` in (read OnDisk, copy, free shell).
    if kind == Kind::Embed {
        if elem_nullable {
            return Err(Error::new_spanned(
                &field.ty,
                "[BSTACK0503] #[embed] does not support `Option`",
            ));
        }
        if is_bstack_mut(&field.attrs) {
            return Err(Error::new_spanned(
                field,
                "[BSTACK0601] #[bstack_mut] is not yet supported on #[embed] fields",
            ));
        }
        let child = elem;
        // Guard: `#[embed]` target must be a plain, self-contained block.
        if !type_mentions_any(child, type_params) {
            parts.wrapper_defs.push(quote! {
                const _: fn() = || {
                    fn __assert_embeddable<__T: ::bstack_raii::__private::BStackEmbeddable>() {}
                    __assert_embeddable::<#child>();
                };
            });
        }
        let child_od = quote!(<#child as ::bstack_raii::BStackBlock>::OnDisk);
        parts
            .on_disk_fields
            .push(quote!(#fname: [#child_od; #total],));

        // Teardown: free each embedded child's children in place.
        parts.drop_stmts.push(quote! {
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
        parts.accessors.push(quote! {
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
        parts.ctor_params.push(quote!(#fname: #param_ty,));
        let cap_write = |k: &Ident, leaf: &Ident| {
            quote! {
                #src_id[#k] = {
                    let __h = #leaf.into_inner();
                    ::bstack_raii::BStackBlock::range(&__h)
                };
            }
        };
        let flatten = nested_consume(&dims, &quote!(#fname), &cap_write);
        parts.ctor_preps.push(quote! {
            let #src_id: [::bstack_raii::BStackRange; #total] = {
                let mut #src_id = [::bstack_raii::BStackRange::new(0, 0); #total];
                #flatten
                #src_id
            };
        });
        parts
            .ctor_inits
            .push(quote!(#fname: [<#child_od as ::bstack_raii::Zeroable>::zeroed(); #total],));
        parts.ctor_post.push(quote! {
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
        parts.mv_caps.push(quote!(let #cap = __od.#fname;));
        parts.mv_types.push(nested_ty(
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
        parts.mv_recon.push(nested_build(
            &dims,
            &quote!(::bstack_raii::BStackOwned<#child>),
            &mv_read,
        ));

        // Clone: fold each embedded child's clone inline (flat; copy the
        // array out, mutate, write back — packed fields can't be `&mut`'d).
        parts.clone_stmts.push(quote! {
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
        return Ok(Some(parts));
    }

    parts.on_disk_fields.push(quote!(#fname: [u64; #total],));
    let size_elem = quote! {
        ::core::mem::size_of::<<#elem as ::bstack_raii::BStackBlock>::OnDisk>() as u64
    };

    // A weak array stores control offsets (`0` = unset), is not a ctor
    // parameter (starts null, wired per flat index via a setter), and its
    // accessor upgrades each element (address-based).
    if kind == Kind::Weak {
        let ctrl_ty = quote!(<#elem as ::bstack_raii::BStackWeakable>::Control);
        let ctrl_size = quote!(::core::mem::size_of::<#ctrl_ty>() as u64);
        parts.ctor_inits.push(quote!(#fname: [0u64; #total],));

        let setter = format_ident!("set_{}", fname);
        parts.setters.push(quote! {
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

        let leaf_ty = quote!(::core::option::Option<::bstack_raii::BStackRc<'__u, #elem, __A>>);
        let acc_ret = nested_ty(&dims, &leaf_ty);
        let acc_read = |k: &Ident| {
            quote!(::bstack_raii::upgrade_weak_field(
                        allocator, __base + (#k as u64) * 8)?)
        };
        let acc_body = nested_build(&dims, &leaf_ty, &acc_read);
        parts.accessors.push(quote! {
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
        parts.drop_stmts.push(quote! {
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
        parts.clone_stmts.push(quote! {
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
        parts.mv_caps.push(quote!(let #cap = __od.#fname;));
        let mv_leaf_ty =
            quote!(::core::option::Option<::bstack_raii::BStackWeak<'__mv, #elem, __A>>);
        parts.mv_types.push(nested_ty(&dims, &mv_leaf_ty));
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
        parts
            .mv_recon
            .push(nested_build(&dims, &mv_leaf_ty, &mv_read));
        return Ok(Some(parts));
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
    parts.accessors.push(quote! {
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
    parts.ctor_params.push(quote!(#fname: #ctor_param_ty,));
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
    parts.ctor_preps.push(quote! {
        let #fname: [u64; #total] = {
            let mut __a = [0u64; #total];
            #flatten
            __a
        };
    });
    parts.ctor_inits.push(quote!(#fname: #fname,));

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
        parts.drop_stmts.push(quote! {
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
        Kind::Owned => parts.clone_stmts.push(quote! {
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
        Kind::Strong => parts.clone_stmts.push(quote! {
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
    parts.mv_caps.push(quote!(let #cap = __od.#fname;));
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
    parts.mv_types.push(nested_ty(&dims, &mv_leaf_ty));
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
    parts
        .mv_recon
        .push(nested_build(&dims, &mv_leaf_ty, &mv_read));

    // `#[bstack_mut]`: element `replace_<f>_at` + whole-array `replace_<f>`
    // (and `set_` for `ref`). Weak arrays already have a `set_<f>` element
    // setter unconditionally; embed arrays are rejected above.
    if is_bstack_mut(&field.attrs) {
        for m in array_mut_methods(
            vis,
            fname,
            &quote!(#elem),
            on_disk_ty,
            kind,
            &dims,
            &total,
            &size_elem,
            elem_nullable,
        ) {
            parts.accessors.push(m);
        }
    }
    Ok(Some(parts))
}

/// Lower a **tuple with ≥1 `Foreign` element** `(A, Foreign<T>, ..)` to its
/// [`FieldParts`]: POD elements packed inline, each foreign element a `ForeignRepr`,
/// all at cumulative payload offsets. Returns `Ok(None)` if the field isn't a tuple
/// containing a `Foreign` (fall through to the POD-tuple / scalar path).
#[allow(clippy::too_many_arguments)]
pub(crate) fn foreign_tuple_field(
    vis: &syn::Visibility,
    name: &Ident,
    fname: &Ident,
    field: &syn::Field,
    inner_ty: &Type,
    kind: Kind,
    nullable: bool,
    on_disk_ty: &TokenStream,
) -> syn::Result<Option<FieldParts>> {
    let Type::Tuple(tup) = inner_ty else {
        return Ok(None);
    };
    if !tup
        .elems
        .iter()
        .any(|e| foreign_inner(option_inner(e).unwrap_or(e)).is_some())
    {
        return Ok(None);
    }
    let mut parts = FieldParts::default();
    let getter = format_ident!("get_{}", fname);
    // Generic foreign *targets* are allowed (bounds are inferred above); a
    // generic param in a POD element was already rejected in the usage pass.
    match kind {
        Kind::Owned | Kind::Strong | Kind::Weak | Kind::Ref => {}
        Kind::Pod => {
            return Err(Error::new_spanned(
                &field.ty,
                "[BSTACK0302] a tuple containing a `Foreign` needs an ownership annotation \
                         (`#[bstack_owned/strong/weak/ref]`) naming the foreign elements' kind",
            ));
        }
        Kind::Embed => {
            return Err(Error::new_spanned(
                &field.ty,
                "[BSTACK0502] cannot #[embed] a tuple",
            ));
        }
    }
    if nullable {
        return Err(Error::new_spanned(
            &field.ty,
            "[BSTACK0106] a whole-tuple `Option<(..)>` is not supported; make the individual \
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
            parts.pod_types.push(e.clone());
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
    parts.wrapper_defs.push(quote! {
        #[repr(C, packed)]
        #[derive(::core::clone::Clone, ::core::marker::Copy)]
        #[doc(hidden)]
        #vis struct #wrapper( #(#welem),* );
        // SAFETY: `#[repr(C, packed)]` => no padding; every element is `Pod`
        // (POD elements asserted via `pod_types`; `ForeignPtr` is `Pod`).
        unsafe impl ::bstack_raii::Zeroable for #wrapper {}
        unsafe impl ::bstack_raii::Pod for #wrapper {}
    });
    parts.on_disk_fields.push(quote!(#fname: #wrapper,));

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
    parts.accessors.push(quote! {
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
    parts.ctor_params.push(quote!(#fname: #pub_tuple_ty,));
    parts
        .ctor_preps
        .push(quote!(let #fname: #wrapper = #wrapper( #(#ctor_elems),* );));
    parts.ctor_inits.push(quote!(#fname: #fname,));

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
        parts.drop_stmts.push(quote! {
            {
                let __w = __on_disk.#fname;
                #(#tup_drops)*
            }
        });
        parts.clone_stmts.push(quote! {
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
    parts.mv_caps.push(quote!(let #cap = __od.#fname;));
    parts.mv_types.push(quote!(#mv_tuple_ty));
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
    parts.mv_recon.push(quote!(( #(#mv_elems,)* )));
    Ok(Some(parts))
}

/// Lower a **POD tuple** field `(A, B, ..)` to its [`FieldParts`]: a generated
/// `#[repr(C, packed)]` `Pod` wrapper stored inline, rebuilt into the tuple on read
/// (+ a `set_` mutator for `#[bstack_mut]`). Returns `Ok(None)` unless the field is a
/// POD (un-annotated) tuple.
pub(crate) fn pod_tuple_field(
    vis: &syn::Visibility,
    name: &Ident,
    fname: &Ident,
    field: &syn::Field,
    inner_ty: &Type,
    kind: Kind,
    on_disk_ty: &TokenStream,
) -> syn::Result<Option<FieldParts>> {
    if kind != Kind::Pod {
        return Ok(None);
    }
    let Type::Tuple(tup) = inner_ty else {
        return Ok(None);
    };
    let mut parts = FieldParts::default();
    let getter = format_ident!("get_{}", fname);
    let elems: Vec<&Type> = tup.elems.iter().collect();
    let wrapper = format_ident!("__BstackTup_{}_{}", name, fname);
    let idx: Vec<syn::Index> = (0..elems.len()).map(syn::Index::from).collect();
    parts.wrapper_defs.push(quote! {
        #[repr(C, packed)]
        #[derive(::core::clone::Clone, ::core::marker::Copy)]
        #[doc(hidden)]
        #vis struct #wrapper( #(#elems),* );
        // SAFETY: `#[repr(C, packed)]` => no padding; every element is
        // `Pod` (asserted below), so all bit patterns are valid.
        unsafe impl ::bstack_raii::Zeroable for #wrapper {}
        unsafe impl ::bstack_raii::Pod for #wrapper {}
    });
    parts.pod_types.extend(elems.iter().map(|t| (*t).clone()));
    parts.on_disk_fields.push(quote!(#fname: #wrapper,));
    parts.accessors.push(quote! {
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
    parts.ctor_params.push(quote!(#fname: #inner_ty,));
    parts
        .ctor_preps
        .push(quote!(let #fname: #wrapper = #wrapper( #(#fname.#idx),* );));
    parts.ctor_inits.push(quote!(#fname: #fname,));
    let cap = format_ident!("__cap_{}", fname);
    parts.mv_caps.push(quote!(let #cap = __od.#fname;));
    parts.mv_types.push(quote!(#inner_ty));
    parts.mv_recon.push(quote!(( #(#cap.#idx,)* )));
    // `#[bstack_mut]`: overwrite the whole inline POD tuple, one atomic `set`
    // (a POD tuple owns no children, so nothing is freed — like a POD scalar).
    if is_bstack_mut(&field.attrs) {
        let setter = format_ident!("set_{}", fname);
        let idx2 = idx.clone();
        parts.accessors.push(quote! {
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
    Ok(Some(parts))
}

/// Lower an `#[embed] child: Block` field to its [`FieldParts`]: the child's whole
/// on-disk form stored INLINE (`<Child as BStackBlock>::OnDisk`), copied in
/// post-write and freed/cloned in place. Returns `Ok(None)` unless the field is
/// `#[embed]`.
#[allow(clippy::too_many_arguments)]
pub(crate) fn embed_field(
    vis: &syn::Visibility,
    fname: &Ident,
    field: &syn::Field,
    inner_ty: &Type,
    kind: Kind,
    nullable: bool,
    on_disk_ty: &TokenStream,
    type_params: &[&Ident],
) -> syn::Result<Option<FieldParts>> {
    if kind != Kind::Embed {
        return Ok(None);
    }
    let mut parts = FieldParts::default();
    let getter = format_ident!("get_{}", fname);
    if let Type::Tuple(_) = inner_ty {
        return Err(Error::new_spanned(
            &field.ty,
            "[BSTACK0502] cannot #[embed] a tuple — embed a `#[bstack_block]` / `#[bstack_enum]` type",
        ));
    }
    if nullable {
        return Err(Error::new_spanned(
            &field.ty,
            "[BSTACK0503] #[embed] does not support `Option`",
        ));
    }
    // `#[embed]` fields `continue` before the scalar mutator injection, so a
    // `#[bstack_mut]` here would be silently ignored — reject it explicitly.
    if is_bstack_mut(&field.attrs) {
        return Err(Error::new_spanned(
            field,
            "[BSTACK0601] #[bstack_mut] is not yet supported on #[embed] fields",
        ));
    }
    let child = inner_ty;
    // Guard: an `#[embed]` target must be a plain, self-contained block
    // (`BStackEmbeddable`) — never `(rc)` / `(rc, weak)`, whose refcount /
    // separate control block embedding would strand. A concrete target gets a
    // direct assertion here; a generic one is bounded via `Usage` above.
    if !type_mentions_any(child, type_params) {
        parts.wrapper_defs.push(quote! {
            const _: fn() = || {
                fn __assert_embeddable<__T: ::bstack_raii::__private::BStackEmbeddable>() {}
                __assert_embeddable::<#child>();
            };
        });
    }
    let child_od = quote!(<#child as ::bstack_raii::BStackBlock>::OnDisk);
    parts.on_disk_fields.push(quote!(#fname: #child_od,));

    // Teardown: free the embedded child's own children *in place* (its
    // storage is part of this block, so no separate dealloc). `__range` is
    // this block's range, bound by `__bstack_drop_children`.
    parts.drop_stmts.push(quote! {
        {
            let __embed = ::bstack_raii::BStackRange::new(
                __range.start() + ::core::mem::offset_of!(#on_disk_ty, #fname) as u64,
                ::core::mem::size_of::<#child_od>() as u64,
            );
            <#child>::__bstack_drop_children(__embed, allocator)?;
        }
    });

    // Accessor: a child handle at the embedded offset (pure offset math).
    parts.accessors.push(quote! {
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
    parts
        .ctor_params
        .push(quote!(#fname: ::bstack_raii::BStackOwned<#child>,));
    parts.ctor_preps.push(quote! {
        let #src_id = {
            let __h = #fname.into_inner();
            ::bstack_raii::BStackBlock::range(&__h)
        };
    });
    parts
        .ctor_inits
        .push(quote!(#fname: <#child_od as ::bstack_raii::Zeroable>::zeroed(),));
    parts.ctor_post.push(quote! {
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
    parts.mv_caps.push(quote!(let #cap = __od.#fname;));
    parts
        .mv_types
        .push(quote!(::bstack_raii::BStackOwned<#child>));
    parts.mv_recon.push(quote! {
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
    parts.clone_stmts.push(quote! {
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
    Ok(Some(parts))
}

/// Lower a **scalar** field (the fall-through: POD inline, or a single
/// `#[bstack_owned/strong/weak/ref]` / `#[embed]`... no — embed handled earlier)
/// block reference) to its [`FieldParts`]: on-disk slot, `get_`/`raw_`_slice
/// accessor, `#[bstack_mut]` `set_`/`replace_` mutators, ctor wiring (or a weak
/// setter), teardown, clone, and `bstack_move!` pieces. Always applies.
pub(crate) fn scalar_field(
    vis: &syn::Visibility,
    fname: &Ident,
    field: &syn::Field,
    inner_ty: &Type,
    kind: Kind,
    nullable: bool,
    on_disk_ty: &TokenStream,
) -> syn::Result<FieldParts> {
    let mut parts = FieldParts::default();
    // On-disk lowering.
    match kind {
        Kind::Pod => {
            parts.on_disk_fields.push(quote!(#fname: #inner_ty,));
            parts.pod_types.push(inner_ty.clone());
        }
        _ => parts.on_disk_fields.push(quote!(#fname: u64,)),
    }

    // Teardown.
    match kind {
            Kind::Owned => parts.drop_stmts.push(child_range_stmt(
                fname,
                inner_ty,
                nullable,
                quote!(::bstack_raii::OwnedRef(__child).bstack_drop(allocator)?;),
            )),
            Kind::Strong => parts.drop_stmts.push(child_range_stmt(
                fname,
                inner_ty,
                nullable,
                quote!(<#inner_ty as ::bstack_raii::BStackShared>::drop_strong_ref(__child, allocator)?;),
            )),
            Kind::Weak => parts.drop_stmts.push(weak_drop_stmt(fname, inner_ty)),
            // `#[embed]` is fully handled above (it `continue`s).
            Kind::Ref | Kind::Pod | Kind::Embed => {}
        }

    // Deep clone (mirror of teardown; POD / ref are copied verbatim).
    if let Some(cs) = clone_field_stmt(fname, inner_ty, kind) {
        parts.clone_stmts.push(cs);
    }

    // Accessor: the `get_<field>` reader, the unsafe `raw_<field>_slice` place,
    // and — for `#[bstack_mut]` fields — a `set_<field>` (POD/ref) and/or
    // `replace_<field>` (owned/strong/ref).
    parts
        .accessors
        .push(accessor(vis, fname, inner_ty, on_disk_ty, kind, nullable));
    parts
        .accessors
        .push(raw_slice_accessor(vis, fname, inner_ty, on_disk_ty, kind));
    if is_bstack_mut(&field.attrs) {
        match kind {
            // POD: overwrite in place.
            Kind::Pod => {
                parts.accessors.push(set_accessor(
                    vis, fname, inner_ty, on_disk_ty, kind, nullable,
                ));
            }
            // Ref is the only kind with BOTH: `set_` (overwrite; a ref owns
            // nothing) and `replace_` (swap, handing the old ref back).
            Kind::Ref => {
                parts.accessors.push(set_accessor(
                    vis, fname, inner_ty, on_disk_ty, kind, nullable,
                ));
                parts.accessors.push(replace_accessor(
                    vis, fname, inner_ty, on_disk_ty, kind, nullable,
                ));
            }
            // Owned / strong: only `replace_` — a plain `set_` would strand the
            // old owned block / strong count; `replace_` moves it out instead.
            Kind::Owned | Kind::Strong => {
                parts.accessors.push(replace_accessor(
                    vis, fname, inner_ty, on_disk_ty, kind, nullable,
                ));
            }
            // Weak fields already have a `set_<field>` (the weak setter).
            Kind::Weak => {}
            Kind::Embed => {
                return Err(Error::new_spanned(
                    field,
                    "[BSTACK0601] #[bstack_mut] is not yet supported on #[embed] fields",
                ));
            }
        }
    }

    // Constructor. Weak fields are not parameters — they start null and are
    // wired afterwards via the generated `set_<field>`.
    if kind == Kind::Weak {
        parts.ctor_inits.push(quote!(#fname: 0u64,));
        parts
            .setters
            .push(weak_setter(vis, fname, inner_ty, on_disk_ty));
    } else {
        let (param, prep, init) = ctor_field(fname, inner_ty, kind, nullable);
        parts.ctor_params.push(param);
        parts.ctor_preps.push(prep);
        parts.ctor_inits.push(init);
    }

    // `bstack_move!` pieces: capture the field before the parent is freed,
    // then reconstruct the transferred handle after.
    let cap = format_ident!("__cap_{}", fname);
    parts.mv_caps.push(quote!(let #cap = __od.#fname;));
    let (mv_ty, mv_rc) = move_field(&cap, inner_ty, kind, nullable);
    parts.mv_types.push(mv_ty);
    parts.mv_recon.push(mv_rc);
    Ok(parts)
}

/// A **POD-aggregate variant**: unit `V`, an all-POD tuple `V(A, B, ..)`, or an
/// all-POD struct `V { x: A, .. }`. The fields are packed sequentially into the
/// payload (declaration order). This is sound because the payload is read/written
/// **unaligned**, so field alignment is irrelevant — the packed byte sequence of POD
/// fields is itself just POD bytes. The loop's catch-all; always applies.
pub(crate) fn pod_aggregate_variant(
    variant: &syn::Variant,
    vname: &Ident,
    disc: &TokenStream,
    kind: Kind,
    data: &Ident,
    view: &Ident,
    payload_const: &Ident,
) -> syn::Result<VariantParts> {
    let mut parts = VariantParts::default();
    if kind != Kind::Pod {
        return Err(Error::new_spanned(
            variant,
            "[BSTACK0205] an ownership annotation is only allowed on a single-field tuple \
             variant, e.g. `#[bstack_owned] V(T)`",
        ));
    }
    let named = matches!(&variant.fields, syn::Fields::Named(_));
    let mut binds = Vec::new();
    let mut tys: Vec<Type> = Vec::new();
    let mut fnames = Vec::new();
    for (j, f) in variant.fields.iter().enumerate() {
        parts.pod_types.push(f.ty.clone());
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
        parts.needs_payload = true;
    }

    parts.data_variants.push(decl.clone());
    parts.view_variants.push(decl);
    parts.payload_sizes.push(payload_size);
    parts.new_arms.push(quote! {
        #data::#pat => {
            let mut __pl = [0u8; #payload_const];
            #(#writes)*
            (#disc, __pl)
        }
    });
    parts.read_arms.push(quote!(#disc => #view::#cons,));
    parts.move_arms.push(quote!(#disc => #data::#cons,));
    // POD: no teardown.
    Ok(parts)
}

/// A **single-field block-reference variant**: `#[bstack_owned/strong/weak/ref] V(T)`
/// or `#[embed] V(Child)`, where the child is stored as a `u64` offset (owned / ref /
/// strong / weak) or its whole on-disk form inline (`#[embed]`). The trailing case of
/// the annotated single-field arm (vec / array / foreign shapes handled before it).
#[allow(clippy::too_many_arguments)]
pub(crate) fn single_block_variant(
    ty: &Type,
    vname: &Ident,
    disc: &TokenStream,
    kind: Kind,
    data: &Ident,
    view: &Ident,
    on_disk: &Ident,
    payload_const: &Ident,
) -> syn::Result<VariantParts> {
    let mut parts = VariantParts::default();
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
    match kind {
        Kind::Pod => unreachable!("guarded out above"),
        Kind::Owned => {
            parts.payload_sizes.push(quote!(8usize));
            let child = child_from_off(ty);
            parts
                .data_variants
                .push(quote!(#vname(::bstack_raii::BStackOwned<#ty>),));
            parts.view_variants.push(quote!(#vname(#ty),));
            parts.new_arms.push(quote! {
                #data::#vname(__v) => {
                    let __h = __v.into_inner();
                    let __off = ::bstack_raii::BStackBlock::range(&__h).start();
                    let mut __pl = [0u8; #payload_const];
                    __pl[..8].copy_from_slice(&__off.to_le_bytes());
                    (#disc, __pl)
                }
            });
            parts
                .read_arms
                .push(quote!(#disc => #view::#vname(#child),));
            parts.move_arms.push(quote! {
                #disc => #data::#vname(unsafe {
                    ::bstack_raii::BStackOwned::from_raw(#child)
                }),
            });
            parts.drop_arms.push(quote! {
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
            parts.clone_arms.push(quote! {
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
            parts.payload_sizes.push(quote!(8usize));
            let child = child_from_off(ty);
            let cref = child_ref(ty);
            parts
                .data_variants
                .push(quote!(#vname(::bstack_raii::BStackRef<#ty>),));
            parts.view_variants.push(quote!(#vname(#ty),));
            parts.new_arms.push(quote! {
                #data::#vname(__v) => {
                    let mut __pl = [0u8; #payload_const];
                    __pl[..8].copy_from_slice(&__v.into_range().start().to_le_bytes());
                    (#disc, __pl)
                }
            });
            parts
                .read_arms
                .push(quote!(#disc => #view::#vname(#child),));
            parts
                .move_arms
                .push(quote!(#disc => #data::#vname(unsafe { #cref }),));
            // A raw reference owns nothing: no teardown.
        }
        Kind::Strong => {
            parts.has_shared = true;
            parts.payload_sizes.push(quote!(8usize));
            let child = child_from_off(ty);
            // A strong variant stores the child's DATA offset and holds
            // one strong reference (like a `#[bstack_strong]` field).
            let cref = child_ref(ty);
            parts
                .data_variants
                .push(quote!(#vname(::bstack_raii::BStackRc<'__e, #ty, __A>),));
            parts.view_variants.push(quote!(#vname(#ty),));
            parts.new_arms.push(quote! {
                #data::#vname(__v) => {
                    let (__data, _ctrl) = __v.into_raw();
                    let mut __pl = [0u8; #payload_const];
                    __pl[..8].copy_from_slice(&__data.into_range().start().to_le_bytes());
                    (#disc, __pl)
                }
            });
            parts
                .read_arms
                .push(quote!(#disc => #view::#vname(#child),));
            // Move: rebuild a `BStackRc` (transferring the strong ref)
            // via `strong_parts` — exactly like a `#[bstack_strong]` field.
            parts.move_arms.push(quote! {
                #disc => {
                    let __data = unsafe { #cref };
                    let (__d, __c) =
                        <#ty as ::bstack_raii::BStackShared>::strong_parts(__data, __alloc)?;
                    #data::#vname(unsafe {
                        ::bstack_raii::BStackRc::from_raw(__d, __c, __alloc)
                    })
                }
            });
            parts.drop_arms.push(quote! {
                #disc => {
                    let __data = unsafe { #cref };
                    <#ty as ::bstack_raii::BStackShared>::drop_strong_ref(
                        __data, allocator,
                    )?;
                }
            });
            parts.clone_arms.push(quote! {
                #disc => {
                    let __data = unsafe { #cref };
                    __plan.bump_strong(__data, allocator)?;
                }
            });
        }
        Kind::Weak => {
            parts.has_shared = true;
            parts.has_weak = true;
            parts.payload_sizes.push(quote!(8usize));
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
            parts
                .data_variants
                .push(quote!(#vname(::bstack_raii::BStackWeak<'__e, #ty, __A>),));
            parts.view_variants.push(quote! {
                #vname(::core::option::Option<
                    ::bstack_raii::BStackRc<'__e, #ty, __A>
                >),
            });
            parts.new_arms.push(quote! {
                #data::#vname(__v) => {
                    let __ctrl = __v.into_raw();
                    let mut __pl = [0u8; #payload_const];
                    __pl[..8].copy_from_slice(&__ctrl.into_range().start().to_le_bytes());
                    (#disc, __pl)
                }
            });
            parts.read_arms.push(quote! {
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
            parts.move_arms.push(quote! {
                #disc => #data::#vname(unsafe {
                    ::bstack_raii::BStackWeak::from_raw(#ctrl_ref, __alloc)
                }),
            });
            parts.drop_arms.push(quote! {
                #disc => {
                    ::bstack_raii::WeakRef::<#ty>(#ctrl_ref).bstack_drop(allocator)?;
                }
            });
            parts.clone_arms.push(quote! {
                #disc => {
                    let __ctrl_off = ::bstack_raii::get_u64(&__pl);
                    __plan.bump_weak(__ctrl_off);
                }
            });
        }
        // `#[embed] V(Child)`: the child's whole on-disk form is stored
        // INLINE in the payload (header and all).
        Kind::Embed => {
            parts.embed_types.push(quote!(#ty));
            let co = quote!(<#ty as ::bstack_raii::BStackBlock>::OnDisk);
            parts
                .payload_sizes
                .push(quote!(::core::mem::size_of::<#co>()));
            parts
                .data_variants
                .push(quote!(#vname(::bstack_raii::BStackOwned<#ty>),));
            parts.view_variants.push(quote!(#vname(#ty),));
            // new: capture the child's block range; the payload is a
            // zeroed placeholder, and a post-write step `BStack::copy`s
            // the child into it (then frees the shell) — no materialising.
            parts.has_embed = true;
            parts.new_arms.push(quote! {
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
            parts.read_arms.push(quote! {
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
            parts.move_arms.push(quote! {
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
            parts.drop_arms.push(quote! {
                #disc => {
                    let __embed = ::bstack_raii::BStackRange::new(
                        __range.start()
                            + ::core::mem::offset_of!(#on_disk, __bstack_payload) as u64,
                        ::core::mem::size_of::<#co>() as u64,
                    );
                    <#ty>::__bstack_drop_children(__embed, allocator)?;
                }
            });
            parts.clone_arms.push(quote! {
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
    Ok(parts)
}

/// An annotated **scalar `Foreign` variant** `#[bstack_owned/strong/weak/ref] V(Foreign<T>)`:
/// a cross-file wide pointer stored as a 16-byte `ForeignPtr` in the payload, the
/// annotation naming the target's ownership in its own file (teardown / clone dispatch
/// cross-file, like a scalar `Foreign` struct field). `None` = not a `Foreign` (fall
/// through). Concrete target only for now; container-in-variant is not handled.
pub(crate) fn foreign_variant(
    ty: &Type,
    vname: &Ident,
    disc: &TokenStream,
    kind: Kind,
    data: &Ident,
    view: &Ident,
    payload_const: &Ident,
) -> syn::Result<Option<VariantParts>> {
    let Some(ftarget) = foreign_inner(ty) else {
        return Ok(None);
    };
    let mut parts = VariantParts::default();
    match kind {
        Kind::Owned | Kind::Strong | Kind::Weak | Kind::Ref => {}
        Kind::Pod => {
            return Err(Error::new_spanned(
                ty,
                "[BSTACK0302] a `Foreign` enum variant needs an ownership annotation \
                                 (`#[bstack_owned/strong/weak/ref]`) naming the target's kind",
            ));
        }
        Kind::Embed => {
            return Err(Error::new_spanned(
                ty,
                "[BSTACK0301] `Foreign` is a pointer and cannot be `#[embed]`ed",
            ));
        }
    }
    // Generic foreign targets are allowed (bounds inferred above).
    reject_bad_foreign_target(ftarget, ty, "a `Foreign` enum variant")?;

    parts.has_foreign = true;
    parts.payload_sizes.push(quote!(16usize));
    // `'__e` is the enum's read / move borrow (see `has_foreign`), so a
    // `SELF` pointer in this variant cannot escape it.
    let fty = quote!(::bstack_raii::Foreign<'__e, #ftarget>);
    let read_fp = quote!(::bstack_raii::bytemuck::pod_read_unaligned::<
        ::bstack_raii::ForeignRepr,
    >(&__pl[..16]));
    parts.data_variants.push(quote!(#vname(#fty),));
    parts.view_variants.push(quote!(#vname(#fty),));
    parts.new_arms.push(quote! {
        #data::#vname(__f) => {
            let mut __pl = [0u8; #payload_const];
            __pl[..16].copy_from_slice(
                ::bstack_raii::bytemuck::bytes_of(&__f.repr()));
            (#disc, __pl)
        }
    });
    // SAFETY: the repr was stored into this file; bound to `'__e`.
    parts.read_arms.push(quote!(#disc => #view::#vname(
                        unsafe { ::bstack_raii::Foreign::from_repr(#read_fp) }),));
    parts.move_arms.push(quote!(#disc => #data::#vname(
                        unsafe { ::bstack_raii::Foreign::from_repr(#read_fp) }),));
    // Teardown / clone dispatch (a `#[bstack_ref]` owns nothing → none;
    // its `ForeignPtr` is byte-copied by the payload catch-all).
    if !matches!(kind, Kind::Ref) {
        let elem_drop = foreign_elem_drop(kind, ftarget);
        parts.drop_arms.push(quote! {
            #disc => {
                let __fp: ::bstack_raii::ForeignRepr = #read_fp;
                #elem_drop
            }
        });
        let elem_clone = foreign_elem_clone(kind, ftarget);
        parts.clone_arms.push(quote! {
            #disc => {
                let __fp: ::bstack_raii::ForeignRepr = #read_fp;
                #elem_clone
                __pl[..16].copy_from_slice(
                    ::bstack_raii::bytemuck::bytes_of(&__newfp));
            }
        });
    }
    Ok(Some(parts))
}

/// An annotated **foreign-tuple variant** `#[bstack_owned/strong/weak/ref] V((A, Foreign<T>, ..))`:
/// POD elements packed inline, each foreign element a 16-byte `ForeignPtr`, all at
/// cumulative byte offsets in the payload (the per-variant mirror of a `#[ann]
/// (A, Foreign<T>)` struct field). The annotation names the foreign elements'
/// ownership. `None` = not a tuple with a `Foreign` element (fall through).
pub(crate) fn foreign_tuple_variant(
    ty: &Type,
    vname: &Ident,
    disc: &TokenStream,
    kind: Kind,
    data: &Ident,
    view: &Ident,
    payload_const: &Ident,
) -> syn::Result<Option<VariantParts>> {
    let Type::Tuple(tup) = ty else {
        return Ok(None);
    };
    if !tup
        .elems
        .iter()
        .any(|e| foreign_inner(option_inner(e).unwrap_or(e)).is_some())
    {
        return Ok(None);
    }
    let mut parts = VariantParts::default();
    if kind == Kind::Embed {
        return Err(Error::new_spanned(
            ty,
            "[BSTACK0502] cannot #[embed] a tuple",
        ));
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
            parts.pod_types.push(e.clone());
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
    parts.payload_sizes.push(acc);

    parts.has_foreign = true;
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
    parts.data_variants.push(quote!(#vname(#pub_tuple_ty),));
    parts.view_variants.push(quote!(#vname(#pub_tuple_ty),));

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
    parts.new_arms.push(quote! {
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
    parts
        .read_arms
        .push(quote!(#disc => #view::#vname(( #(#reads,)* )),));
    parts
        .move_arms
        .push(quote!(#disc => #data::#vname(( #(#reads,)* )),));

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
            drops.push(quote! { { let __fp: ::bstack_raii::ForeignRepr = #read_fp; #ed } });
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
        parts.drop_arms.push(quote!(#disc => { #(#drops)* }));
        parts.clone_arms.push(quote!(#disc => { #(#clones)* }));
    }
    Ok(Some(parts))
}

/// An annotated **array variant** `#[bstack_owned/strong/weak/ref] V([T; N])` (possibly
/// nested `[[..]; ..]`, and for owned/strong/ref possibly `Option<T>` leaves), or a
/// foreign array `V([Foreign<T>; N])`, or an `#[embed] V([Child; N])`. Block refs are
/// stored **flat** in the payload as `[u64; TOTAL]` (foreign: `[ForeignPtr; TOTAL]`;
/// embed: each child's whole on-disk form). `None` = not an array (fall through).
#[allow(clippy::too_many_arguments)]
pub(crate) fn array_variant(
    ty: &Type,
    vname: &Ident,
    disc: &TokenStream,
    kind: Kind,
    data: &Ident,
    view: &Ident,
    on_disk: &Ident,
    payload_const: &Ident,
) -> syn::Result<Option<VariantParts>> {
    let Type::Array(_) = ty else {
        return Ok(None);
    };
    let mut parts = VariantParts::default();
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
                    "[BSTACK0302] a `[Foreign<T>; N]` enum variant needs an ownership \
                                     annotation (`#[bstack_owned/strong/weak/ref]`)",
                ));
            }
            Kind::Embed => {
                return Err(Error::new_spanned(
                    ty,
                    "[BSTACK0301] `Foreign` is a pointer and cannot be `#[embed]`ed",
                ));
            }
        }
        reject_bad_foreign_target(ftarget, ty, "a `Foreign` array variant")?;
        parts.has_foreign = true;
        parts.payload_sizes.push(quote!((#total) * 16));
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
        parts.data_variants.push(quote!(#vname(#nested),));
        parts.view_variants.push(quote!(#vname(#nested),));

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
        parts.new_arms.push(quote! {
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
        parts
            .read_arms
            .push(quote!(#disc => #view::#vname(#build_e),));
        parts
            .move_arms
            .push(quote!(#disc => #data::#vname(#build_mv),));

        // Teardown / clone: iterate the flat slots (inline — no block).
        if !matches!(kind, Kind::Ref) {
            let elem_drop = foreign_elem_drop(kind, ftarget);
            parts.drop_arms.push(quote! {
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
            parts.clone_arms.push(quote! {
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
        return Ok(Some(parts));
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
                "[BSTACK0503] #[embed] does not support `Option`",
            ));
        }
        parts.has_embed = true;
        let child = elem;
        parts.embed_types.push(quote!(#child));
        let co = quote!(<#child as ::bstack_raii::BStackBlock>::OnDisk);
        parts
            .payload_sizes
            .push(quote!((#total) * ::core::mem::size_of::<#co>()));

        let data_leaf = quote!(::bstack_raii::BStackOwned<#child>);
        let data_ty_nested = nested_ty(&dims, &data_leaf);
        parts.data_variants.push(quote!(#vname(#data_ty_nested),));
        let view_ty_nested = nested_ty(&dims, &quote!(#child));
        parts.view_variants.push(quote!(#vname(#view_ty_nested),));

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
        parts.new_arms.push(quote! {
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
        parts.read_arms.push(quote! {
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
        parts
            .move_arms
            .push(quote!(#disc => #data::#vname(#mv_body),));

        // teardown: free each embedded child's children in place.
        parts.drop_arms.push(quote! {
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
        parts.clone_arms.push(quote! {
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
        return Ok(Some(parts));
    }

    // ---- owned / strong / weak / ref: flat `[u64; TOTAL]` ----
    let elem_size = quote!(::core::mem::size_of::<
                        <#elem as ::bstack_raii::BStackBlock>::OnDisk>() as u64);
    parts.payload_sizes.push(quote!((#total) * 8));

    if kind == Kind::Weak {
        parts.has_shared = true;
        parts.has_weak = true;
        let ctrl_ty = quote!(<#elem as ::bstack_raii::BStackWeakable>::Control);
        let ctrl_size = quote!(::core::mem::size_of::<#ctrl_ty>() as u64);

        let data_leaf = quote!(::bstack_raii::BStackWeak<'__e, #elem, __A>);
        let data_ty_nested = nested_ty(&dims, &data_leaf);
        parts.data_variants.push(quote!(#vname(#data_ty_nested),));
        let view_leaf = quote!(::core::option::Option<::bstack_raii::BStackRc<'__e, #elem, __A>>);
        let view_ty_nested = nested_ty(&dims, &view_leaf);
        parts.view_variants.push(quote!(#vname(#view_ty_nested),));

        // new: consume nested weaks → control offsets.
        let cap_write =
            |k: &Ident, leaf: &Ident| pl_put(k, quote!(#leaf.into_raw().into_range().start()));
        let consume = nested_consume(&dims, &quote!(__arr), &cap_write);
        parts.new_arms.push(quote! {
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
        parts
            .read_arms
            .push(quote!(#disc => #view::#vname(#read_body),));

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
        parts
            .move_arms
            .push(quote!(#disc => #data::#vname(#mv_body),));

        // teardown: release each weak.
        parts.drop_arms.push(quote! {
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
        parts.clone_arms.push(quote! {
            #disc => {
                for __k in 0usize..(#total) {
                    let __off = ::bstack_raii::get_u64(&__pl[__k * 8..]);
                    __plan.bump_weak(__off);
                }
            }
        });
        return Ok(Some(parts));
    }

    // owned / strong / ref
    if kind == Kind::Strong {
        parts.has_shared = true;
    }
    let view_leaf = if elem_nullable {
        quote!(::core::option::Option<#elem>)
    } else {
        quote!(#elem)
    };
    let view_ty_nested = nested_ty(&dims, &view_leaf);
    parts.view_variants.push(quote!(#vname(#view_ty_nested),));

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
    parts.data_variants.push(quote!(#vname(#data_ty_nested),));

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
    parts.new_arms.push(quote! {
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
    parts
        .read_arms
        .push(quote!(#disc => #view::#vname(#read_body),));

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
    parts
        .move_arms
        .push(quote!(#disc => #data::#vname(#mv_body),));

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
        parts.drop_arms.push(quote! {
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
        Kind::Owned => parts.clone_arms.push(quote! {
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
        Kind::Strong => parts.clone_arms.push(quote! {
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
    Ok(Some(parts))
}

/// An annotated **vector variant** `#[bstack_owned/strong/weak/ref] V(Vec<T>)` /
/// `V(Vec<[T; N]>)`, a POD `V(Vec<Pod>)` / `V(String)`, or a foreign `V(Vec<Foreign<T>>)`:
/// a `VecDesc` (16 bytes) in the payload naming a data block — the per-variant mirror
/// of a `#[ann] Vec<..>` struct field. A `Vec<[T; N]>` stores its offsets FLAT (like
/// the struct case), reshaped to `Vec<[[T;..];..]>` on read. `None` = not a `Vec` /
/// `String` (fall through).
pub(crate) fn vec_variant(
    ty: &Type,
    vname: &Ident,
    disc: &TokenStream,
    kind: Kind,
    data: &Ident,
    view: &Ident,
    payload_const: &Ident,
) -> syn::Result<Option<VariantParts>> {
    if vec_info(ty).is_none() {
        return Ok(None);
    }
    let mut parts = VariantParts::default();
    check_container_nesting(ty)?;
    if kind == Kind::Embed {
        return Err(Error::new_spanned(
            ty,
            "[BSTACK0501] cannot #[embed] a `Vec`; embed a `#[bstack_block]` type",
        ));
    }
    parts.needs_payload = true;
    parts
        .payload_sizes
        .push(quote!(::core::mem::size_of::<::bstack_raii::VecDesc>()));
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
                    "[BSTACK0302] a `Vec<Foreign<T>>` enum variant needs an ownership \
                                     annotation (`#[bstack_owned/strong/weak/ref]`)",
                ));
            }
            Kind::Embed => {
                return Err(Error::new_spanned(
                    ty,
                    "[BSTACK0301] `Foreign` is a pointer and cannot be `#[embed]`ed",
                ));
            }
        }
        reject_bad_foreign_target(ftarget, ty, "a `Foreign` vec variant")?;
        parts.has_foreign = true;
        let elem_nullable = option_inner(velem).is_some();
        let store = quote!(::bstack_raii::BStackVec::<::bstack_raii::ForeignRepr, __A>);
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
        parts
            .data_variants
            .push(quote!(#vname(::std::vec::Vec<#fty>),));
        parts
            .view_variants
            .push(quote!(#vname(::std::vec::Vec<#fty>),));
        parts.new_arms.push(quote! {
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
        parts.read_arms.push(quote! {
            #disc => #view::#vname(
                #store::from_desc(#read_desc, allocator)
                    .to_vec()?.into_iter().map(#from_ptr).collect()),
        });
        parts.move_arms.push(quote! {
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
        parts.drop_arms.push(quote! {
            #disc => {
                #drop_loop
                #store::from_desc(#read_desc, allocator).bstack_drop()?;
            }
        });
        let elem_clone = foreign_elem_clone(kind, ftarget);
        parts.clone_arms.push(quote! {
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
        return Ok(Some(parts));
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
        parts.has_shared = true;
        parts.has_weak = true;
        parts
            .data_variants
            .push(quote!(#vname(::bstack_raii::BStackVec<'__e, #elem, __A>),));
        parts
            .view_variants
            .push(quote!(#vname(::bstack_raii::BStackVec<'__e, #elem, __A>),));
        parts.new_arms.push(quote! {
            #data::#vname(__v) => {
                let __desc = __v.descriptor();
                let mut __pl = [0u8; #payload_const];
                __pl[..16].copy_from_slice(
                    ::bstack_raii::bytemuck::bytes_of(&__desc));
                (#disc, __pl)
            }
        });
        parts.read_arms.push(quote! {
            #disc => #view::#vname(
                ::bstack_raii::BStackVec::<#elem, __A>::from_desc(
                    #read_desc, allocator)),
        });
        parts.move_arms.push(quote! {
            #disc => #data::#vname(
                ::bstack_raii::BStackVec::<#elem, __A>::from_desc(
                    #read_desc, __alloc)),
        });
        parts.drop_arms.push(quote! {
            #disc => {
                ::bstack_raii::BStackVec::<#elem, __A>::from_desc(
                    #read_desc, allocator).bstack_drop()?;
            }
        });
        parts.clone_arms.push(quote! {
            #disc => {
                let __srcdesc = #read_desc;
                let __newdesc = ::bstack_raii::BStackVec::<#elem, __A>::from_desc(
                    __srcdesc, allocator).clone_data_into(__plan)?;
                __pl[..16].copy_from_slice(
                    ::bstack_raii::bytemuck::bytes_of(&__newdesc));
            }
        });
        return Ok(Some(parts));
    }
    if vec_info(ty).is_some_and(|vi| vi.is_string) {
        return Err(Error::new_spanned(
            ty,
            "[BSTACK0107] `String` is always POD; drop the ownership annotation to store \
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
        parts.has_shared = true;
    }
    if !is_array || is_weak {
        parts.has_weak = true;
    }

    // ---- Teardown (shared) ----
    parts.drop_arms.push(quote! {
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
    parts.clone_arms.push(quote! {
        #disc => {
            let __srcdesc = #read_desc;
            let __newdesc = #clone_expr;
            __pl[..16].copy_from_slice(
                ::bstack_raii::bytemuck::bytes_of(&__newdesc));
        }
    });

    if !is_array {
        // ---- Scalar `Vec<T>`: data = view = the vec handle ----
        parts
            .data_variants
            .push(quote!(#vname(::bstack_raii::#vec_ty<'__e, #elem, __A>),));
        parts
            .view_variants
            .push(quote!(#vname(::bstack_raii::#vec_ty<'__e, #elem, __A>),));
        parts.new_arms.push(quote! {
            #data::#vname(__v) => {
                let __desc = __v.descriptor();
                let mut __pl = [0u8; #payload_const];
                __pl[..16].copy_from_slice(
                    ::bstack_raii::bytemuck::bytes_of(&__desc));
                (#disc, __pl)
            }
        });
        parts.read_arms.push(quote! {
            #disc => #view::#vname(
                ::bstack_raii::#vec_ty::<#elem, __A>::from_desc(
                    #read_desc, allocator)),
        });
        parts.move_arms.push(quote! {
            #disc => #data::#vname(
                ::bstack_raii::#vec_ty::<#elem, __A>::from_desc(
                    #read_desc, __alloc)),
        });
        return Ok(Some(parts));
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
    parts
        .view_variants
        .push(quote!(#vname(::std::vec::Vec<#view_ret>),));

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
    parts
        .data_variants
        .push(quote!(#vname(::std::vec::Vec<#data_ty>),));

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
    parts.new_arms.push(quote! {
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
    parts.read_arms.push(quote! {
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
    parts.move_arms.push(quote! {
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
    Ok(Some(parts))
}
