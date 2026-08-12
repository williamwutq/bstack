//! # Cross-file ownership with `Foreign<T>` (`bstack_raii`)
//!
//! A `bstack_raii` reference normally points inside *one* file. A `Foreign<T>`
//! is a **wide pointer that crosses the file boundary**: it names both a target
//! file (through a process-wide registry) and an offset within it. An owning
//! foreign field frees — or a deep clone duplicates — its target *in the
//! target's own file*, transparently.
//!
//! This models a common sharding layout. A small **catalog** file holds one
//! lightweight `Card` per document; the heavy `Document` bodies live in a
//! separate **store** file. Each card owns its document across the boundary, so
//! deleting a card reclaims its body in the store — no dangling record, no leak.
//!
//! Run with: `cargo run --example crossfile`

use std::io;
use std::path::Path;
use std::sync::Arc;

use bstack::FirstFitBStackAllocator;
use bstack_raii::{
    BStack, BStackAllocator, BStackBlock, BStackDrop, Foreign, TryCloneIn, bstack_block, registry,
};

/// A heavy record, living in the *store* file.
#[bstack_block]
struct Document {
    size: u64,
    checksum: u64,
}

/// A lightweight catalog entry, living in the *catalog* file. It **owns** its
/// `Document` across the file boundary: the document is freed in the store when
/// this card is torn down.
#[bstack_block]
struct Card {
    title: String,
    #[bstack_owned]
    body: Foreign<Document>,
}

/// A catalog entry that owns *several* store documents at once — a
/// `Vec<Foreign<T>>` is a growable list of cross-file pointers.
#[bstack_block]
struct Bundle {
    name: String,
    #[bstack_owned]
    parts: Vec<Foreign<Document>>,
}

fn main() -> io::Result<()> {
    let dir = std::env::temp_dir();
    let registry_path = dir.join("bstack_raii_registry.bstack");
    let store_path = dir.join("bstack_raii_store.bstack");
    let catalog_path = dir.join("bstack_raii_catalog.bstack");
    for p in [&registry_path, &store_path, &catalog_path] {
        let _ = std::fs::remove_file(p);
    }

    // The registry maps each file's persistent path <-> a small numeric id, so a
    // `Foreign` can name its target file compactly and stably. Bring it up once.
    registry::init(&registry_path)?;

    // The catalog file (our "home") stays a plain local allocator we build with.
    let catalog = FirstFitBStackAllocator::new(BStack::open(&catalog_path)?)?;

    // The store file's allocator is shared through an `Arc`: one clone is handed
    // to the registry as the live host, and we keep one to build documents with
    // and to inspect afterward.
    let store = Arc::new(FirstFitBStackAllocator::new(BStack::open(&store_path)?)?);
    let store_id = registry::get()
        .unwrap()
        .attach(&store_path, store.clone())?;
    println!("store attached as file id {}", store_id.get());

    // Populate the store file, remembering each document's offset.
    let doc = Document::new(&*store, 4096, 0xDEAD_BEEF)?;
    let doc_off = doc.handle().range().start();

    let extra: Vec<u64> = (0u64..3)
        .map(|i| {
            let d = Document::new(&*store, 1000 + i, i).unwrap();
            d.handle().range().start()
        })
        .collect();

    // --- a single owned foreign pointer -------------------------------------

    // A card in the catalog file that owns `doc` over in the store file.
    let card = Card::new(
        &catalog,
        "annual-report",
        Foreign::<Document>::new(store_id, doc_off),
    )?;

    // Resolve the foreign pointer and read the far-side document. `with` takes
    // the *local* allocator (used only for a same-file `Foreign`) and a closure
    // run against the target and its file's stack. It returns `Ok(None)` for a
    // null pointer (not this field — it's `#[bstack_owned]`, never null) and
    // `Err` if the target file isn't currently attached — propagate that with
    // `?`, same as any other I/O failure.
    let (size, sum) = card
        .handle()
        .get_body(catalog.stack())?
        .with(&catalog, |d, fs| {
            (d.get_size(fs).unwrap(), d.get_checksum(fs).unwrap())
        })?
        .expect("owned Foreign is never null");
    println!("card 'annual-report' -> document size {size}, checksum {sum:#x}");

    // Deep-clone the card. The clone gets its *own* fresh copy of the document,
    // allocated in the store file — cross-file `TryCloneIn` follows the pointer.
    let card_copy = card.try_clone_in(&catalog)?;
    let copy_off = card_copy.handle().get_body(catalog.stack())?.offset();
    println!(
        "cloned card -> independent document at store offset {copy_off} (original at {doc_off})"
    );
    assert_ne!(
        copy_off, doc_off,
        "the clone must not alias the original document"
    );

    // --- a vector of owned foreign pointers ---------------------------------

    let ptrs: Vec<Foreign<Document>> = extra
        .iter()
        .map(|&off| Foreign::<Document>::new(store_id, off))
        .collect();
    let bundle = Bundle::new(&catalog, "q3-batch", ptrs)?;
    let bundle_sizes: Vec<u64> = bundle
        .handle()
        .get_parts(&catalog)?
        .into_iter()
        .map(|f| {
            f.with(&catalog, |d, fs| d.get_size(fs).unwrap())
                .unwrap()
                .expect("owned Foreign is never null")
        })
        .collect();
    println!(
        "bundle 'q3-batch' -> {} documents, sizes {bundle_sizes:?}",
        bundle_sizes.len()
    );

    // --- cross-file teardown reclaims the store -----------------------------

    let frontier = store.stack().len()?; // the store's high-water mark right now

    // Tear everything down. Each owned foreign pointer frees its target back in
    // the store file — the catalog never touches the store's bytes directly.
    card.bstack_drop(&catalog)?;
    card_copy.bstack_drop(&catalog)?;
    bundle.bstack_drop(&catalog)?;

    // Prove the store space was actually reclaimed (not merely unlinked): a fresh
    // document lands *inside* the old frontier, reusing a freed slot instead of
    // extending the file — a leak-only teardown would have bumped past `frontier`.
    let probe = Document::new(&*store, 1, 1)?;
    let probe_off = probe.handle().range().start();
    println!(
        "after teardown, a new store document reuses freed offset {probe_off} (frontier was {frontier})"
    );
    assert!(
        probe_off < frontier,
        "cross-file teardown should have freed the documents' store space for reuse"
    );
    probe.bstack_drop(&*store)?;
    println!("every document was reclaimed across the file boundary");

    registry::detach(store_id);
    for p in [&registry_path, &store_path, &catalog_path] {
        let _ = std::fs::remove_file(p as &Path);
    }
    Ok(())
}
