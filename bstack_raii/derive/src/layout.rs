//! On-disk **layout** computation for `#[bstack_enum]`: the discriminant width +
//! per-variant literal patterns, and the payload-area sizing (max over variants).
//! (A struct's layout is expressed entirely through generated `offset_of!` /
//! `size_of` in [`crate::emit`], so it needs nothing here yet.)

use proc_macro2::TokenStream;
use quote::quote;
use syn::punctuated::Punctuated;
use syn::{Error, Ident, Token, Variant, Visibility};

use crate::util::{infer_disc_ty, int_bounds, parse_disc_expr};

/// The enum's on-disk discriminant: its integer type (tokens) and the typed literal
/// pattern for each variant (e.g. `300u16`), in declaration order.
pub(crate) struct Discriminants {
    pub ty: TokenStream,
    pub pats: Vec<TokenStream>,
}

/// Assign each variant its discriminant (explicit `= N`, else previous + 1),
/// reject duplicates, then pick the width: an explicit `repr(..)` (range-checked)
/// or the smallest integer type that fits every value.
pub(crate) fn discriminants(
    variants: &Punctuated<Variant, Token![,]>,
    repr: &Option<String>,
) -> syn::Result<Discriminants> {
    let disc_values: Vec<i128> = {
        let mut next: i128 = 0;
        let mut out: Vec<i128> = Vec::with_capacity(variants.len());
        for v in variants {
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
                    format!("[BSTACK0201] discriminant value `{d}` assigned more than once"),
                ));
            }
            out.push(d);
            next = d.checked_add(1).ok_or_else(|| {
                Error::new_spanned(v, "[BSTACK0202] #[bstack_enum] discriminant overflow")
            })?;
        }
        out
    };
    let dmin = disc_values.iter().copied().min().unwrap_or(0);
    let dmax = disc_values.iter().copied().max().unwrap_or(0);
    let disc_ty_name: String = match repr {
        Some(r) => {
            let (lo, hi) = int_bounds(r);
            if dmin < lo || dmax > hi {
                return Err(Error::new_spanned(
                    variants,
                    format!(
                        "[BSTACK0203] a discriminant value is out of range for `repr({r})` \
                         (values span {dmin}..={dmax})"
                    ),
                ));
            }
            r.clone()
        }
        None => infer_disc_ty(dmin, dmax).to_string(),
    };
    let ty: TokenStream = disc_ty_name.parse().expect("valid integer type name");
    // Typed literal patterns for the match arms / stored value (e.g. `300u16`).
    let pats: Vec<TokenStream> = disc_values
        .iter()
        .map(|v| {
            format!("{v}{disc_ty_name}")
                .parse()
                .expect("valid integer literal")
        })
        .collect();
    Ok(Discriminants { ty, pats })
}

/// The `const <name>: usize = max(payload_sizes)` definition — the payload area is
/// sized to the largest variant, folded at const-eval so it stays a single ABI
/// constant.
pub(crate) fn payload_const_def(
    vis: &Visibility,
    name: &Ident,
    payload_sizes: &[TokenStream],
) -> TokenStream {
    quote! {
        #[doc(hidden)]
        #[allow(non_upper_case_globals)]
        #vis const #name: usize = {
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
}
