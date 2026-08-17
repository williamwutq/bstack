//! The struct/enum lowering **IR**: the bundle of code pieces one field (or, later,
//! one enum variant) contributes to the generated block.
//!
//! The orchestrators ([`crate::block`], [`crate::enum_`]) walk the fields, ask an
//! [`crate::emit`] function to lower each into a [`FieldParts`], and merge the parts
//! into the whole. This replaces the old "push into a dozen loose `Vec`s inline"
//! monolith: each shape's lowering becomes a self-contained function returning its
//! `FieldParts`.

use proc_macro2::TokenStream;
use syn::Type;

/// Everything one field contributes to the generated block, grouped by the section
/// it lands in. A shape-lowering `emit::*` function fills the relevant fields and
/// leaves the rest empty; the orchestrator [`merge`](FieldParts::merge)s them.
#[derive(Default)]
#[allow(dead_code)] // fields/merge fill in as more branches are extracted
pub(crate) struct FieldParts {
    /// `XOnDisk` struct field(s) (`name: Ty,`).
    pub on_disk_fields: Vec<TokenStream>,
    /// Reader / mutator methods on the handle (`get_<f>` / `set_<f>` / `replace_<f>` …).
    pub accessors: Vec<TokenStream>,
    /// Post-construction setters (weak-field wiring).
    pub setters: Vec<TokenStream>,
    /// `new` constructor: parameters, prep statements, and struct-literal inits.
    pub ctor_params: Vec<TokenStream>,
    pub ctor_preps: Vec<TokenStream>,
    pub ctor_inits: Vec<TokenStream>,
    /// `new` steps run *after* the block image is written (`#[embed]` copy-in).
    pub ctor_post: Vec<TokenStream>,
    /// Teardown (`__bstack_drop_children`) statements.
    pub drop_stmts: Vec<TokenStream>,
    /// Deep-clone (`__bstack_clone_children_inplace`) statements.
    pub clone_stmts: Vec<TokenStream>,
    /// `bstack_move!` pieces: capture, moved-out field type, reconstruction expr.
    pub mv_caps: Vec<TokenStream>,
    pub mv_types: Vec<TokenStream>,
    pub mv_recon: Vec<TokenStream>,
    /// Helper item definitions the field needs (POD tuple wrappers, embed assertions).
    pub wrapper_defs: Vec<TokenStream>,
    /// POD element types to `__assert_pod` on.
    pub pod_types: Vec<Type>,
    /// A field coerced `&T` → `T` (drives the one-shot coercion warning).
    pub ref_coerced: bool,
}

impl FieldParts {
    #[allow(dead_code)]
    /// Fold another field's parts into this one.
    pub fn merge(&mut self, other: FieldParts) {
        self.on_disk_fields.extend(other.on_disk_fields);
        self.accessors.extend(other.accessors);
        self.setters.extend(other.setters);
        self.ctor_params.extend(other.ctor_params);
        self.ctor_preps.extend(other.ctor_preps);
        self.ctor_inits.extend(other.ctor_inits);
        self.ctor_post.extend(other.ctor_post);
        self.drop_stmts.extend(other.drop_stmts);
        self.clone_stmts.extend(other.clone_stmts);
        self.mv_caps.extend(other.mv_caps);
        self.mv_types.extend(other.mv_types);
        self.mv_recon.extend(other.mv_recon);
        self.wrapper_defs.extend(other.wrapper_defs);
        self.pod_types.extend(other.pod_types);
        self.ref_coerced |= other.ref_coerced;
    }
}
