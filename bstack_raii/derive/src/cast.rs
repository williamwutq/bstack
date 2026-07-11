//! Implementation of the `bstack_cast!(expr as Target)` macro.
//!
//! A function-like macro can't observe the surrounding `let x: T = …`
//! annotation, so the target type is given explicitly with `as`, and the
//! direction is chosen from the target's tokens:
//!
//! * `expr as BStackOwnedSlice`     — owned upcast     → `BStackOwned::into_slice`
//! * `expr as BStackOwned<X, _>`    — owned downcast   → `BStackCastInto::cast_into::<X>`
//! * `slice as X` (a block type)    — borrowed downcast → `BStackCastAs::cast_as::<X>`
//!
//! The borrowed upcast (`X` → `BStackSlice`) needs a stack, so it is the
//! generated `handle.as_slice(stack)` method rather than a `bstack_cast!` form.

use proc_macro2::TokenStream;
use quote::quote;
use syn::{Error, ExprCast, GenericArgument, PathArguments, Type};

pub fn expand(input: TokenStream) -> syn::Result<TokenStream> {
    let cast: ExprCast = syn::parse2(input).map_err(|_| {
        Error::new(
            proc_macro2::Span::call_site(),
            "bstack_cast! expects `expr as Target` (e.g. `bstack_cast!(slice as BStackOwned<X, _>)`)",
        )
    })?;
    let expr = &cast.expr;
    let ty = &*cast.ty;

    let Type::Path(tp) = ty else {
        return Err(Error::new_spanned(
            ty,
            "bstack_cast!: target must be a type path",
        ));
    };
    let seg = tp.path.segments.last().expect("non-empty path");

    let tokens = match seg.ident.to_string().as_str() {
        "BStackOwnedSlice" => quote!((#expr).into_slice()),
        "BStackSlice" => {
            return Err(Error::new_spanned(
                ty,
                "bstack_cast! can't build a borrowed slice (it needs a stack); \
                 use `handle.as_slice(stack)` instead",
            ));
        }
        "BStackOwned" => {
            let inner = first_type_arg(seg)?;
            quote!(::bstack_raii::BStackCastInto::cast_into::<#inner>(#expr))
        }
        // A concrete block type: borrowed downcast off a `BStackSlice`.
        _ => quote!(::bstack_raii::BStackCastAs::cast_as::<#ty>(&#expr)),
    };
    Ok(tokens)
}

/// Extract `X` from a `BStackOwned<X, ..>` target.
fn first_type_arg(seg: &syn::PathSegment) -> syn::Result<&Type> {
    if let PathArguments::AngleBracketed(ab) = &seg.arguments {
        for arg in &ab.args {
            if let GenericArgument::Type(t) = arg {
                return Ok(t);
            }
        }
    }
    Err(Error::new_spanned(
        seg,
        "expected `BStackOwned<BlockType, _>` for an owned downcast",
    ))
}
