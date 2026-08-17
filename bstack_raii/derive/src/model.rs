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

/// Everything one enum **variant** contributes to the generated block, grouped by
/// the match arm / helper enum it lands in. The parallel of [`FieldParts`] for the
/// `#[bstack_enum]` path: a shape-lowering `emit::*_variant` fills the relevant
/// fields, and [`expand_enum`](crate::enum_::expand_enum) [`merge`](VariantParts::merge)s
/// them across all variants.
///
/// Storage differs from a struct field (a variant packs into the shared payload area
/// at discriminant-dispatched offsets, not its own `OnDisk` field), so this is a
/// distinct bundle — but the *leaf* logic (`foreign_elem_*`, nested arrays, per-kind
/// reconstruction) is the same machinery the field emitters use.
#[derive(Default)]
pub(crate) struct VariantParts {
    /// `EData` enum variant decl(s).
    pub data_variants: Vec<TokenStream>,
    /// `EView` enum variant decl(s).
    pub view_variants: Vec<TokenStream>,
    /// `new` match arm (`EData::V(..) => (disc, payload)`).
    pub new_arms: Vec<TokenStream>,
    /// `read` match arm (`disc => EView::V(..)`).
    pub read_arms: Vec<TokenStream>,
    /// `bstack_move!` / `replace` match arm (`disc => EData::V(..)`).
    pub move_arms: Vec<TokenStream>,
    /// Teardown (`__bstack_drop_children`) match arm.
    pub drop_arms: Vec<TokenStream>,
    /// Deep-clone (`__bstack_clone_children_inplace`) match arm.
    pub clone_arms: Vec<TokenStream>,
    /// This variant's payload size (folded into the max-over-variants const).
    pub payload_sizes: Vec<TokenStream>,
    /// POD element types to `__assert_pod` on.
    pub pod_types: Vec<Type>,
    /// `#[embed]`ded child types to `__assert_embeddable` on.
    pub embed_types: Vec<TokenStream>,
    /// This variant is `#[embed]` (its `new` folds the child in post-write).
    pub has_embed: bool,
    /// This variant carries a payload (drives the `read` / `move` payload read).
    pub needs_payload: bool,
    /// This variant holds a strong reference (`EData` becomes `<'e, A>`).
    pub has_shared: bool,
    /// This variant holds a weak reference (`EView` also becomes `<'e, A>`).
    pub has_weak: bool,
    /// This variant holds a `Foreign` (both enums carry the `'__e` lifetime).
    pub has_foreign: bool,
}

impl VariantParts {
    /// Fold another variant's parts into this accumulator.
    pub fn merge(&mut self, other: VariantParts) {
        self.data_variants.extend(other.data_variants);
        self.view_variants.extend(other.view_variants);
        self.new_arms.extend(other.new_arms);
        self.read_arms.extend(other.read_arms);
        self.move_arms.extend(other.move_arms);
        self.drop_arms.extend(other.drop_arms);
        self.clone_arms.extend(other.clone_arms);
        self.payload_sizes.extend(other.payload_sizes);
        self.pod_types.extend(other.pod_types);
        self.embed_types.extend(other.embed_types);
        self.has_embed |= other.has_embed;
        self.needs_payload |= other.needs_payload;
        self.has_shared |= other.has_shared;
        self.has_weak |= other.has_weak;
        self.has_foreign |= other.has_foreign;
    }
}

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
