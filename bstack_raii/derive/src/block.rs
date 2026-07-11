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
use syn::{Error, Fields, Ident, ItemStruct, Token, Type};

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
    let mode = parse_mode(attr)?;

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

    for field in fields {
        let fname = field.ident.as_ref().expect("named field");
        let fty = &field.ty;
        let kind = classify(field)?;

        // On-disk lowering + teardown.
        match kind {
            Kind::Pod => {
                on_disk_fields.push(quote!(#fname: #fty,));
                pod_types.push(fty);
            }
            _ => on_disk_fields.push(quote!(#fname: u64,)),
        }
        match kind {
            Kind::Owned => drop_stmts.push(child_range_stmt(
                fname,
                fty,
                quote! {
                    ::bstack_raii::OwnedRef(__child).bstack_drop(allocator)?;
                },
            )),
            Kind::Strong => drop_stmts.push(child_range_stmt(
                fname,
                fty,
                quote! {
                    <#fty as ::bstack_raii::BStackShared>::drop_strong_ref(__child, allocator)?;
                },
            )),
            // Weak fields store the child's *control-block* offset (sound even if
            // the target's data is already freed), and may be null (0 = unset).
            Kind::Weak => drop_stmts.push(quote! {
                {
                    let __off = __on_disk.#fname;
                    if __off != 0 {
                        let __ctrl = unsafe {
                            ::bstack_raii::BStackRef::<
                                <#fty as ::bstack_raii::BStackWeakable>::Control
                            >::from_range(::bstack_raii::BStackRange::new(
                                __off,
                                ::core::mem::size_of::<
                                    <#fty as ::bstack_raii::BStackWeakable>::Control
                                >() as u64,
                            ))
                        };
                        ::bstack_raii::WeakRef::<#fty>(__ctrl).bstack_drop(allocator)?;
                    }
                }
            }),
            Kind::Ref | Kind::Pod => {}
        }

        // Accessor.
        accessors.push(accessor(vis, fname, fty, &on_disk, kind));

        // Constructor pieces. Weak fields are not constructor parameters — they
        // start null and are wired afterwards via the generated `set_<field>`.
        if kind == Kind::Weak {
            ctor_inits.push(quote!(#fname: 0u64,));
            setters.push(weak_setter(vis, fname, fty, &on_disk));
        } else {
            let (param, prep, init) = ctor_field(fname, fty, kind);
            ctor_params.push(param);
            ctor_preps.push(prep);
            ctor_inits.push(init);
        }

        // `bstack_move!` pieces: capture the field before the parent is freed,
        // then reconstruct the transferred handle after.
        let cap = format_ident!("__cap_{}", fname);
        mv_caps.push(quote!(let #cap = __od.#fname;));
        match kind {
            Kind::Owned => {
                mv_types.push(quote!(::bstack_raii::BStackOwned<'__mv, #fty, __A>));
                mv_recon.push(quote! {
                    unsafe {
                        ::bstack_raii::BStackOwned::from_raw(
                            <#fty as ::bstack_raii::BStackBlock>::from_range(
                                ::bstack_raii::BStackRange::new(
                                    #cap,
                                    ::core::mem::size_of::<
                                        <#fty as ::bstack_raii::BStackBlock>::OnDisk
                                    >() as u64,
                                ),
                            ),
                            __alloc,
                        )
                    }
                });
            }
            Kind::Ref => {
                mv_types.push(quote!(::bstack_raii::BStackRef<#fty>));
                mv_recon.push(quote! {
                    unsafe {
                        ::bstack_raii::BStackRef::<#fty>::from_range(
                            ::bstack_raii::BStackRange::new(
                                #cap,
                                ::core::mem::size_of::<
                                    <#fty as ::bstack_raii::BStackBlock>::OnDisk
                                >() as u64,
                            ),
                        )
                    }
                });
            }
            Kind::Pod => {
                mv_types.push(quote!(#fty));
                mv_recon.push(quote!(#cap));
            }
            Kind::Strong => {
                // Rebuild a BStackRc, dispatching through BStackShared so the
                // child's kind (rc vs rc,weak) picks up the control block if any.
                mv_types.push(quote!(::bstack_raii::BStackRc<'__mv, #fty, __A>));
                mv_recon.push(quote! {
                    {
                        let __data = unsafe {
                            ::bstack_raii::BStackRef::<#fty>::from_range(
                                ::bstack_raii::BStackRange::new(
                                    #cap,
                                    ::core::mem::size_of::<
                                        <#fty as ::bstack_raii::BStackBlock>::OnDisk
                                    >() as u64,
                                ),
                            )
                        };
                        let (__d, __c) =
                            <#fty as ::bstack_raii::BStackShared>::strong_parts(__data, __alloc)?;
                        unsafe { ::bstack_raii::BStackRc::from_raw(__d, __c, __alloc) }
                    }
                });
            }
            Kind::Weak => {
                // The field holds the child's control offset directly; rebuild a
                // BStackWeak, or None if the field was never set (0).
                mv_types.push(quote! {
                    ::core::option::Option<::bstack_raii::BStackWeak<'__mv, #fty, __A>>
                });
                mv_recon.push(quote! {
                    if #cap == 0 {
                        ::core::option::Option::None
                    } else {
                        let __ctrl = unsafe {
                            ::bstack_raii::BStackRef::<
                                <#fty as ::bstack_raii::BStackWeakable>::Control
                            >::from_range(::bstack_raii::BStackRange::new(
                                #cap,
                                ::core::mem::size_of::<
                                    <#fty as ::bstack_raii::BStackWeakable>::Control
                                >() as u64,
                            ))
                        };
                        ::core::option::Option::Some(
                            unsafe { ::bstack_raii::BStackWeak::from_raw(__ctrl, __alloc) }
                        )
                    }
                });
            }
        }
    }

    let tag = name.to_string();

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
        name,
        vis,
        &on_disk,
        mode,
        &ctor_params,
        &ctor_preps,
        &ctor_inits,
    );

    // `bstack_move!` is defined for plain blocks (not rc / rc,weak themselves);
    // their fields may be any kind, including strong/weak.
    let move_impl = if mode == Mode::Plain {
        quote! {
            impl<'__mv, __A: ::bstack_raii::BStackOwnedSliceAllocator>
                ::bstack_raii::BStackMove for ::bstack_raii::BStackOwned<'__mv, #name, __A>
            {
                type Fields = ( #(#mv_types,)* );
                fn bstack_move(self) -> ::std::io::Result<Self::Fields> {
                    // Take the inner handle out (defusing the owned Drop) and read
                    // the payload before freeing anything.
                    let (__inner, __alloc) = self.into_raw_parts();
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
    } else {
        quote!()
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
                ::bstack_raii::EightCC::from_name(#tag)
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
            #constructor
        }

        #shared_impl
        #weakable_items
        #move_impl
    })
}

/// Generate the reader method for one field.
fn accessor(
    vis: &syn::Visibility,
    fname: &Ident,
    fty: &Type,
    on_disk: &Ident,
    kind: Kind,
) -> TokenStream {
    // Weak fields hold a control offset; the accessor attempts a live upgrade.
    if kind == Kind::Weak {
        return quote! {
            #vis fn #fname<'__u, __A: ::bstack_raii::BStackOwnedSliceAllocator>(
                &self,
                allocator: &'__u __A,
            ) -> ::std::io::Result<
                ::core::option::Option<::bstack_raii::BStackRc<'__u, #fty, __A>>
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
        quote! {
            #vis fn #fname(&self, stack: &::bstack_raii::BStack) -> ::std::io::Result<#fty> {
                #read
                ::std::result::Result::Ok(__od.#fname)
            }
        }
    } else {
        // Owned/strong/ref field: resolve the stored data offset to the handle.
        quote! {
            #vis fn #fname(&self, stack: &::bstack_raii::BStack) -> ::std::io::Result<#fty> {
                #read
                let __range = ::bstack_raii::BStackRange::new(
                    __od.#fname,
                    ::core::mem::size_of::<<#fty as ::bstack_raii::BStackBlock>::OnDisk>() as u64,
                );
                ::std::result::Result::Ok(<#fty as ::bstack_raii::BStackBlock>::from_range(__range))
            }
        }
    }
}

/// Generate `(param, prep, init)` for one constructor field. Not called for
/// `#[bstack_weak]` fields.
fn ctor_field(fname: &Ident, fty: &Type, kind: Kind) -> (TokenStream, TokenStream, TokenStream) {
    match kind {
        Kind::Pod => (quote!(#fname: #fty,), quote!(), quote!(#fname: #fname,)),
        Kind::Owned => (
            quote!(#fname: ::bstack_raii::BStackOwned<'__ctor, #fty, __A>,),
            quote! {
                let #fname: u64 = {
                    let (__h, _) = #fname.into_raw_parts();
                    ::bstack_raii::BStackBlock::range(&__h).start()
                };
            },
            quote!(#fname: #fname,),
        ),
        Kind::Strong => (
            quote!(#fname: ::bstack_raii::BStackRc<'__ctor, #fty, __A>,),
            quote! {
                let #fname: u64 = {
                    let (__d, _) = #fname.into_raw();
                    __d.into_range().start()
                };
            },
            quote!(#fname: #fname,),
        ),
        Kind::Ref => (
            quote!(#fname: ::bstack_raii::BStackRef<#fty>,),
            quote!(),
            quote!(#fname: #fname.into_range().start(),),
        ),
        Kind::Weak => unreachable!("weak fields are wired via set_<field>, not the constructor"),
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
    name: &Ident,
    vis: &syn::Visibility,
    on_disk: &Ident,
    mode: Mode,
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
            let ctrl_tag = format!("{name}Ref");
            quote! {
                let __ctrl = match ::bstack_raii::alloc_control(
                    allocator,
                    ::bstack_raii::EightCC::from_name(#ctrl_tag),
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

/// Build a teardown statement that resolves a child field's `u64` offset into a
/// typed `BStackRef<#fty>` bound to `__child`, then runs `body`.
fn child_range_stmt(fname: &Ident, fty: &Type, body: TokenStream) -> TokenStream {
    quote! {
        {
            let __off = __on_disk.#fname;
            let __range = ::bstack_raii::BStackRange::new(
                __off,
                ::core::mem::size_of::<<#fty as ::bstack_raii::BStackBlock>::OnDisk>() as u64,
            );
            let __child = unsafe { ::bstack_raii::BStackRef::<#fty>::from_range(__range) };
            #body
        }
    }
}

/// Parse the attribute arguments into a [`Mode`]: ``, `rc`, or `rc, weak`.
fn parse_mode(attr: TokenStream) -> syn::Result<Mode> {
    if attr.is_empty() {
        return Ok(Mode::Plain);
    }
    let parser = Punctuated::<Ident, Token![,]>::parse_terminated;
    let idents: Vec<String> = parser
        .parse2(attr)?
        .into_iter()
        .map(|i| i.to_string())
        .collect();
    match idents.as_slice() {
        [rc] if rc == "rc" => Ok(Mode::Rc),
        [rc, weak] if rc == "rc" && weak == "weak" => Ok(Mode::RcWeak),
        _ => Err(Error::new(
            Span::call_site(),
            "expected `#[bstack_block]`, `#[bstack_block(rc)]`, or `#[bstack_block(rc, weak)]`",
        )),
    }
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
