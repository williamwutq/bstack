//! stdlib collections — public-API behavior, migrated from `src/tests.rs`. Each
//! collection (`BStackCow/Box/List/Deque/HashMap/BTreeMap/String/Bloom/HashSet/
//! BTreeSet/Heap`) plus their composition into blocks/enums, generics, and
//! embed-of-a-collection. (The cross-file `Foreign<Collection>` test stays a unit
//! test — it drives the process-global registry.)
#![allow(unused_imports)]

use bstack::{BStack, BStackAllocator, FirstFitBStackAllocator};
use bstack_raii::{
    BStackBTreeMap, BStackBTreeSet, BStackBinaryHeap, BStackBlock, BStackBox, BStackCast,
    BStackCountingBloomFilter, BStackCow, BStackDeque, BStackDrop, BStackHashMap, BStackHashSet,
    BStackLinkedList, BStackOwned, BStackRaiiAllocator, BStackRc, BStackRef, BStackString,
    TryClone, TryCloneIn, bstack_block, bstack_enum, bstack_move,
};

use crate::common::{TempStack, assert_teardown_reclaims};

// Shared fixtures the migrated tests use (ported from src/tests.rs).
#[bstack_block]
struct MacroLeaf {
    val: u32,
}
#[bstack_block]
struct MacroParent {
    #[bstack_owned]
    child: MacroLeaf,
    tag: u32,
}
#[bstack_block(rc, weak)]
struct MacroStrongChild {
    val: u32,
}

// --------------------------------------------------------------------------
// stdlib: BStackCow<T> — clone-on-write ownership of a block
// --------------------------------------------------------------------------

#[test]
fn stdlib_cow_borrowed_into_owned_deep_copies() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    // A block owned elsewhere; the Cow only borrows it.
    let base = MacroLeaf::new(&alloc, 7).unwrap();
    let base_start = base.handle().range().start();
    let cow =
        BStackCow::borrowed(unsafe { BStackRef::<MacroLeaf>::from_range(base.handle().range()) });

    assert!(cow.is_borrowed());
    // Reads go through the borrowed block, at its address.
    assert_eq!(cow.handle().get_val(stack).unwrap(), 7);
    assert_eq!(cow.range().start(), base_start);

    // into_owned deep-copies: a fresh block at a different address, same value.
    let owned = cow.into_owned(&alloc).unwrap();
    assert_ne!(owned.handle().range().start(), base_start);
    assert_eq!(owned.handle().get_val(stack).unwrap(), 7);
    owned.bstack_drop(&alloc).unwrap();

    // The borrowed source is untouched.
    assert_eq!(base.handle().get_val(stack).unwrap(), 7);
    base.bstack_drop(&alloc).unwrap();
}

#[test]
fn stdlib_cow_owned_into_owned_is_free() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    let src = MacroLeaf::new(&alloc, 5).unwrap();
    let start = src.handle().range().start();
    let cow = BStackCow::owned(src);
    assert!(cow.is_owned());

    // Already owned: into_owned hands back the *same* block, no copy.
    let owned = cow.into_owned(&alloc).unwrap();
    assert_eq!(owned.handle().range().start(), start);
    assert_eq!(owned.handle().get_val(stack).unwrap(), 5);
    owned.bstack_drop(&alloc).unwrap();
}

#[test]
fn stdlib_cow_to_mut_copies_then_owns() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    let base = MacroLeaf::new(&alloc, 9).unwrap();
    let base_start = base.handle().range().start();
    let mut cow =
        BStackCow::borrowed(unsafe { BStackRef::<MacroLeaf>::from_range(base.handle().range()) });

    // First write forces a private copy and flips to Owned.
    {
        let m = cow.to_mut(&alloc).unwrap();
        assert_ne!(m.handle().range().start(), base_start);
        assert_eq!(m.handle().get_val(stack).unwrap(), 9);
    }
    assert!(cow.is_owned());

    // A second to_mut is a no-op: still the same owned copy.
    let owned_start = cow.range().start();
    let _ = cow.to_mut(&alloc).unwrap();
    assert_eq!(cow.range().start(), owned_start);

    // Dropping the Cow frees only the copy; the borrowed source survives.
    cow.bstack_drop(&alloc).unwrap();
    assert_eq!(base.handle().get_val(stack).unwrap(), 9);
    base.bstack_drop(&alloc).unwrap();
}

#[test]
fn stdlib_cow_borrowed_drop_frees_nothing() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    let base = MacroLeaf::new(&alloc, 3).unwrap();
    let cow =
        BStackCow::borrowed(unsafe { BStackRef::<MacroLeaf>::from_range(base.handle().range()) });

    // Dropping a borrowed Cow has no claim on the target.
    cow.bstack_drop(&alloc).unwrap();
    assert_eq!(base.handle().get_val(stack).unwrap(), 3);
    base.bstack_drop(&alloc).unwrap();
}

// --------------------------------------------------------------------------
// stdlib: BStackBox<T> — an owned single-value block for Pod T
// --------------------------------------------------------------------------

#[repr(C)]
#[derive(Clone, Copy, PartialEq, Debug, bytemuck::Pod, bytemuck::Zeroable)]
struct Point3 {
    x: i32,
    y: i32,
    z: i32,
}

// A block that owns a box as a child, proving BStackBox composes as a field.
#[bstack_block]
struct BoxHolder {
    #[bstack_owned]
    boxed: BStackBox<u64>,
    tag: u32,
}

#[test]
fn stdlib_box_roundtrip_and_set() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    // A bare scalar owned as its own block — no macro struct needed.
    let b = BStackBox::new(&alloc, 42u64).unwrap();
    assert_eq!(b.handle().get(stack).unwrap(), 42);

    // In-place overwrite.
    b.handle().set(&alloc, 99).unwrap();
    assert_eq!(b.handle().get(stack).unwrap(), 99);

    // A plain POD struct payload works too (the point of the Pod bound).
    let p = BStackBox::new(&alloc, Point3 { x: 1, y: 2, z: 3 }).unwrap();
    assert_eq!(p.handle().get(stack).unwrap(), Point3 { x: 1, y: 2, z: 3 });

    b.bstack_drop(&alloc).unwrap();
    p.bstack_drop(&alloc).unwrap();
}

#[test]
fn stdlib_box_clone_is_a_byte_copy() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    let b = BStackBox::new(&alloc, 7u32).unwrap();
    let clone = b.try_clone_in(&alloc).unwrap();

    // Fresh, independent block, same value.
    assert_ne!(clone.handle().range().start(), b.handle().range().start());
    assert_eq!(clone.handle().get(stack).unwrap(), 7);

    // Mutating the clone leaves the original untouched.
    clone.handle().set(&alloc, 8).unwrap();
    assert_eq!(b.handle().get(stack).unwrap(), 7);

    b.bstack_drop(&alloc).unwrap();
    clone.bstack_drop(&alloc).unwrap();
}

#[test]
fn stdlib_box_move_yields_the_value() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();

    let b = BStackBox::new(&alloc, 123u64).unwrap();
    let start = b.handle().range().start();
    let value = bstack_move!(b, &alloc).unwrap();
    assert_eq!(value, 123);

    // The shell was freed: its slot is reused by the next allocation.
    let b2 = BStackBox::new(&alloc, 5u64).unwrap();
    assert_eq!(b2.handle().range().start(), start);
    b2.bstack_drop(&alloc).unwrap();
}

#[test]
fn stdlib_box_distinct_tags_by_size() {
    // Boxes of differently-sized payloads get distinct tags.
    assert_ne!(
        <BStackBox<u32> as BStackCast>::eightcc(),
        <BStackBox<u64> as BStackCast>::eightcc(),
    );
    // Same size => same tag (the generic-POD tag scheme distinguishes by size).
    assert_eq!(
        <BStackBox<u32> as BStackCast>::eightcc(),
        <BStackBox<i32> as BStackCast>::eightcc(),
    );
}

#[test]
fn stdlib_box_composes_as_owned_field() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    let inner = BStackBox::new(&alloc, 500u64).unwrap();
    let holder = BoxHolder::new(&alloc, inner, 9).unwrap();
    assert_eq!(
        holder
            .handle()
            .get_boxed(stack)
            .unwrap()
            .get(stack)
            .unwrap(),
        500
    );
    assert_eq!(holder.handle().get_tag(stack).unwrap(), 9);

    // Deep-cloning the parent recurses into the child box (fresh child block).
    let clone = holder.try_clone_in(&alloc).unwrap();
    assert_ne!(
        clone.handle().get_boxed(stack).unwrap().range().start(),
        holder.handle().get_boxed(stack).unwrap().range().start(),
    );
    assert_eq!(
        clone.handle().get_boxed(stack).unwrap().get(stack).unwrap(),
        500
    );

    clone.bstack_drop(&alloc).unwrap();
    holder.bstack_drop(&alloc).unwrap();
}

#[test]
fn stdlib_box_in_cow() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    // A borrowed Cow over a box; first write deep-copies the box.
    let base = BStackBox::new(&alloc, 11u64).unwrap();
    let mut cow = BStackCow::borrowed(unsafe {
        BStackRef::<BStackBox<u64>>::from_range(base.handle().range())
    });
    assert_eq!(cow.handle().get(stack).unwrap(), 11);

    let owned = cow.to_mut(&alloc).unwrap();
    owned.handle().set(&alloc, 22).unwrap();
    assert_ne!(cow.range().start(), base.handle().range().start());
    assert_eq!(base.handle().get(stack).unwrap(), 11); // source untouched

    cow.bstack_drop(&alloc).unwrap();
    base.bstack_drop(&alloc).unwrap();
}

// --------------------------------------------------------------------------
// stdlib: BStackLinkedList<T> — owned doubly-linked list of block values
// --------------------------------------------------------------------------

fn list_values(list: &BStackLinkedList<MacroLeaf>, stack: &BStack) -> Vec<u32> {
    list.to_vec(stack)
        .unwrap()
        .iter()
        .map(|h| h.get_val(stack).unwrap())
        .collect()
}

#[test]
fn stdlib_list_push_back_pop_front() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    let list = BStackLinkedList::<MacroLeaf>::new(&alloc).unwrap();
    assert!(list.is_empty(stack).unwrap());

    for v in [1u32, 2, 3] {
        list.push_back(&alloc, MacroLeaf::new(&alloc, v).unwrap())
            .unwrap();
    }
    assert_eq!(list.len(stack).unwrap(), 3);
    assert_eq!(list_values(&list, stack), vec![1, 2, 3]);
    assert_eq!(
        list.front(stack).unwrap().unwrap().get_val(stack).unwrap(),
        1
    );
    assert_eq!(
        list.back(stack).unwrap().unwrap().get_val(stack).unwrap(),
        3
    );

    // FIFO drain from the front.
    let a = list.pop_front(&alloc).unwrap().unwrap();
    assert_eq!(a.handle().get_val(stack).unwrap(), 1);
    a.bstack_drop(&alloc).unwrap();
    assert_eq!(list.len(stack).unwrap(), 2);
    assert_eq!(list_values(&list, stack), vec![2, 3]);

    list.bstack_drop(&alloc).unwrap(); // frees remaining nodes + values
}

#[test]
fn stdlib_list_both_ends() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    let list = BStackLinkedList::<MacroLeaf>::new(&alloc).unwrap();
    list.push_front(&alloc, MacroLeaf::new(&alloc, 2).unwrap())
        .unwrap();
    list.push_front(&alloc, MacroLeaf::new(&alloc, 1).unwrap())
        .unwrap();
    list.push_back(&alloc, MacroLeaf::new(&alloc, 3).unwrap())
        .unwrap();
    assert_eq!(list_values(&list, stack), vec![1, 2, 3]);

    let back = list.pop_back(&alloc).unwrap().unwrap();
    assert_eq!(back.handle().get_val(stack).unwrap(), 3);
    back.bstack_drop(&alloc).unwrap();

    let front = list.pop_front(&alloc).unwrap().unwrap();
    assert_eq!(front.handle().get_val(stack).unwrap(), 1);
    front.bstack_drop(&alloc).unwrap();

    assert_eq!(list_values(&list, stack), vec![2]);
    assert!(list.pop_back(&alloc).unwrap().is_some());
    assert!(list.is_empty(stack).unwrap());
    assert!(list.pop_front(&alloc).unwrap().is_none());

    list.bstack_drop(&alloc).unwrap();
}

#[test]
fn stdlib_list_drop_is_recursive() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();

    // A value type that itself owns a child, to prove teardown recurses through
    // the node's single value ref into the value's own children (a non-recursive
    // teardown would leak the MacroLeaf grandchild).
    assert_teardown_reclaims(&alloc, || {
        let leaf = MacroLeaf::new(&alloc, 10).unwrap();
        let parent = MacroParent::new(&alloc, leaf, 1).unwrap();
        let list = BStackLinkedList::<MacroParent>::new(&alloc).unwrap();
        list.push_back(&alloc, parent).unwrap();
        list
    });
}

#[test]
fn stdlib_list_deep_clone_is_independent() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    let list = BStackLinkedList::<MacroLeaf>::new(&alloc).unwrap();
    for v in [1u32, 2, 3] {
        list.push_back(&alloc, MacroLeaf::new(&alloc, v).unwrap())
            .unwrap();
    }

    let clone = list.try_clone_in(&alloc).unwrap();
    assert_eq!(list_values(&clone, stack), vec![1, 2, 3]);

    // The clone's values are fresh blocks, not aliases of the source's.
    assert_ne!(
        clone.front(stack).unwrap().unwrap().range().start(),
        list.front(stack).unwrap().unwrap().range().start(),
    );

    // Mutating the clone leaves the original intact.
    let popped = clone.pop_back(&alloc).unwrap().unwrap();
    popped.bstack_drop(&alloc).unwrap();
    assert_eq!(clone.len(stack).unwrap(), 2);
    assert_eq!(list.len(stack).unwrap(), 3);
    assert_eq!(list_values(&list, stack), vec![1, 2, 3]);

    clone.bstack_drop(&alloc).unwrap();
    list.bstack_drop(&alloc).unwrap();
}

/// Many threads hammering one shared list. Phase 1 is concurrent `push_back`
/// only; phase 2 is concurrent `pop_front` only. If the relink/len RMW were not
/// atomic under contention, lost updates would corrupt the chain (a wrong length,
/// a broken `next` walk, or duplicate/missing values). The `inplace_gen`-based
/// [`bstack_raii::BStackLinkedList`] mutators need no external lock around them.
#[test]
fn stdlib_list_concurrent_push_pop() {
    // Kept modest: each op is a durable `inplace_gen` commit (an fsync), so the
    // cost is in the op count, not the thread count — the contention that would
    // expose a non-atomic RMW comes from the parallel threads, not from more
    // iterations.
    const THREADS: u32 = 8;
    const ITERS: u32 = 8;

    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let list = BStackLinkedList::<MacroLeaf>::new(&alloc).unwrap();
    let total = (THREADS * ITERS) as u64;

    // Phase 1 — concurrent pushes of distinct values.
    std::thread::scope(|s| {
        for t in 0..THREADS {
            let list = &list;
            let alloc = &alloc;
            s.spawn(move || {
                for i in 0..ITERS {
                    let leaf = MacroLeaf::new(alloc, t * ITERS + i).unwrap();
                    list.push_back(alloc, leaf).unwrap();
                }
            });
        }
    });

    assert_eq!(list.len(alloc.stack()).unwrap(), total);
    // Chain integrity: walking `next` yields exactly the distinct values 0..total.
    let mut seen: Vec<u32> = list
        .to_vec(alloc.stack())
        .unwrap()
        .iter()
        .map(|h| h.get_val(alloc.stack()).unwrap())
        .collect();
    seen.sort_unstable();
    assert_eq!(seen.len() as u64, total);
    seen.dedup();
    assert_eq!(seen.len() as u64, total, "no lost/duplicated nodes");
    assert_eq!(seen.first().copied(), Some(0));
    assert_eq!(seen.last().copied(), Some(total as u32 - 1));

    // Phase 2 — concurrent pops (exactly `total` across threads, so none miss).
    std::thread::scope(|s| {
        for _ in 0..THREADS {
            let list = &list;
            let alloc = &alloc;
            s.spawn(move || {
                for _ in 0..ITERS {
                    let v = list.pop_front(alloc).unwrap().expect("list non-empty");
                    v.bstack_drop(alloc).unwrap();
                }
            });
        }
    });

    assert_eq!(list.len(alloc.stack()).unwrap(), 0);
    assert!(list.front(alloc.stack()).unwrap().is_none());
    list.bstack_drop(&alloc).unwrap();
}

// --------------------------------------------------------------------------
// stdlib: BStackDeque<T> — owned double-ended queue over a contiguous ring
// --------------------------------------------------------------------------

fn deque_values(dq: &BStackDeque<MacroLeaf>, stack: &BStack) -> Vec<u32> {
    dq.to_vec(stack)
        .unwrap()
        .iter()
        .map(|h| h.get_val(stack).unwrap())
        .collect()
}

#[test]
fn stdlib_deque_push_back_grows() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    let dq = BStackDeque::<MacroLeaf>::new(&alloc).unwrap();
    assert!(dq.is_empty(stack).unwrap());

    // Push past the initial capacity to force at least one growth.
    for v in 0..10u32 {
        dq.push_back(&alloc, MacroLeaf::new(&alloc, v).unwrap())
            .unwrap();
    }
    assert_eq!(dq.len(stack).unwrap(), 10);
    assert!(dq.capacity(stack).unwrap() >= 10);
    assert_eq!(deque_values(&dq, stack), (0..10).collect::<Vec<_>>());
    assert_eq!(dq.front(stack).unwrap().unwrap().get_val(stack).unwrap(), 0);
    assert_eq!(dq.back(stack).unwrap().unwrap().get_val(stack).unwrap(), 9);

    // FIFO drain from the front.
    for v in 0..10u32 {
        let x = dq.pop_front(&alloc).unwrap().unwrap();
        assert_eq!(x.handle().get_val(stack).unwrap(), v);
        x.bstack_drop(&alloc).unwrap();
    }
    assert!(dq.is_empty(stack).unwrap());
    assert!(dq.pop_front(&alloc).unwrap().is_none());
    dq.bstack_drop(&alloc).unwrap();
}

#[test]
fn stdlib_deque_wraparound_no_growth() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    // Fixed capacity 4; exercise circular indexing without any growth.
    let dq = BStackDeque::<MacroLeaf>::with_capacity(&alloc, 4).unwrap();
    for v in [1u32, 2, 3, 4] {
        dq.push_back(&alloc, MacroLeaf::new(&alloc, v).unwrap())
            .unwrap();
    }
    // Drop the front two: head advances into the ring.
    for _ in 0..2 {
        dq.pop_front(&alloc)
            .unwrap()
            .unwrap()
            .bstack_drop(&alloc)
            .unwrap();
    }
    // Two more push_backs wrap around the physical slots 0,1.
    dq.push_back(&alloc, MacroLeaf::new(&alloc, 5).unwrap())
        .unwrap();
    dq.push_back(&alloc, MacroLeaf::new(&alloc, 6).unwrap())
        .unwrap();
    assert_eq!(dq.capacity(stack).unwrap(), 4); // never grew
    assert_eq!(deque_values(&dq, stack), vec![3, 4, 5, 6]);

    dq.bstack_drop(&alloc).unwrap();
}

#[test]
fn stdlib_deque_both_ends() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    let dq = BStackDeque::<MacroLeaf>::new(&alloc).unwrap();
    dq.push_back(&alloc, MacroLeaf::new(&alloc, 2).unwrap())
        .unwrap();
    dq.push_front(&alloc, MacroLeaf::new(&alloc, 1).unwrap())
        .unwrap();
    dq.push_back(&alloc, MacroLeaf::new(&alloc, 3).unwrap())
        .unwrap();
    assert_eq!(deque_values(&dq, stack), vec![1, 2, 3]);

    let back = dq.pop_back(&alloc).unwrap().unwrap();
    assert_eq!(back.handle().get_val(stack).unwrap(), 3);
    back.bstack_drop(&alloc).unwrap();

    let front = dq.pop_front(&alloc).unwrap().unwrap();
    assert_eq!(front.handle().get_val(stack).unwrap(), 1);
    front.bstack_drop(&alloc).unwrap();

    assert_eq!(deque_values(&dq, stack), vec![2]);
    dq.bstack_drop(&alloc).unwrap();
}

#[test]
fn stdlib_deque_drop_is_recursive() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();

    // Full recursion through the ring must free the MacroLeaf grandchild too.
    assert_teardown_reclaims(&alloc, || {
        let leaf = MacroLeaf::new(&alloc, 10).unwrap();
        let parent = MacroParent::new(&alloc, leaf, 1).unwrap();
        let dq = BStackDeque::<MacroParent>::new(&alloc).unwrap();
        dq.push_back(&alloc, parent).unwrap();
        dq
    });
}

#[test]
fn stdlib_deque_deep_clone_is_independent() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    let dq = BStackDeque::<MacroLeaf>::new(&alloc).unwrap();
    for v in [1u32, 2, 3, 4, 5] {
        dq.push_back(&alloc, MacroLeaf::new(&alloc, v).unwrap())
            .unwrap();
    }

    let clone = dq.try_clone_in(&alloc).unwrap();
    assert_eq!(deque_values(&clone, stack), vec![1, 2, 3, 4, 5]);
    // Clone is compacted to exactly `len` slots.
    assert_eq!(clone.capacity(stack).unwrap(), 5);
    // Fresh element blocks, not aliases.
    assert_ne!(
        clone.front(stack).unwrap().unwrap().range().start(),
        dq.front(stack).unwrap().unwrap().range().start(),
    );

    // Mutating the clone leaves the original intact.
    clone
        .pop_back(&alloc)
        .unwrap()
        .unwrap()
        .bstack_drop(&alloc)
        .unwrap();
    assert_eq!(clone.len(stack).unwrap(), 4);
    assert_eq!(deque_values(&dq, stack), vec![1, 2, 3, 4, 5]);

    clone.bstack_drop(&alloc).unwrap();
    dq.bstack_drop(&alloc).unwrap();
}

/// Many threads hammering one shared deque, including concurrent growth. Phase 1
/// is concurrent `push_back`; phase 2 is concurrent `pop_front`. A non-atomic
/// slot/metadata RMW (or a racy growth) would drop or duplicate elements.
#[test]
fn stdlib_deque_concurrent_push_pop() {
    const THREADS: u32 = 8;
    const ITERS: u32 = 8;

    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let dq = BStackDeque::<MacroLeaf>::new(&alloc).unwrap();
    let total = (THREADS * ITERS) as u64;

    std::thread::scope(|s| {
        for t in 0..THREADS {
            let dq = &dq;
            let alloc = &alloc;
            s.spawn(move || {
                for i in 0..ITERS {
                    let leaf = MacroLeaf::new(alloc, t * ITERS + i).unwrap();
                    dq.push_back(alloc, leaf).unwrap();
                }
            });
        }
    });

    assert_eq!(dq.len(alloc.stack()).unwrap(), total);
    let mut seen = deque_values(&dq, alloc.stack());
    seen.sort_unstable();
    assert_eq!(seen.len() as u64, total);
    seen.dedup();
    assert_eq!(seen.len() as u64, total, "no lost/duplicated elements");

    std::thread::scope(|s| {
        for _ in 0..THREADS {
            let dq = &dq;
            let alloc = &alloc;
            s.spawn(move || {
                for _ in 0..ITERS {
                    let v = dq.pop_front(alloc).unwrap().expect("deque non-empty");
                    v.bstack_drop(alloc).unwrap();
                }
            });
        }
    });

    assert_eq!(dq.len(alloc.stack()).unwrap(), 0);
    dq.bstack_drop(&alloc).unwrap();
}

// --------------------------------------------------------------------------
// stdlib: BStackHashMap<K, V> — owned open-addressing hash map
// --------------------------------------------------------------------------

#[test]
fn stdlib_map_insert_get_remove() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    let map = BStackHashMap::<u32, MacroLeaf>::new(&alloc).unwrap();
    assert!(map.is_empty(stack).unwrap());
    assert!(map.get(stack, &7).unwrap().is_none());

    // Insert of a new key returns no previous value.
    assert!(
        map.insert(&alloc, 7, MacroLeaf::new(&alloc, 700).unwrap())
            .unwrap()
            .is_none()
    );
    assert!(
        map.insert(&alloc, 9, MacroLeaf::new(&alloc, 900).unwrap())
            .unwrap()
            .is_none()
    );
    assert_eq!(map.len(stack).unwrap(), 2);
    assert_eq!(
        map.get(stack, &7).unwrap().unwrap().get_val(stack).unwrap(),
        700
    );
    assert_eq!(
        map.get(stack, &9).unwrap().unwrap().get_val(stack).unwrap(),
        900
    );
    assert!(map.contains_key(stack, &7).unwrap());
    assert!(!map.contains_key(stack, &8).unwrap());

    // Overwrite returns the previous value (owned) and does not change len.
    let old = map
        .insert(&alloc, 7, MacroLeaf::new(&alloc, 701).unwrap())
        .unwrap()
        .unwrap();
    assert_eq!(old.handle().get_val(stack).unwrap(), 700);
    old.bstack_drop(&alloc).unwrap();
    assert_eq!(map.len(stack).unwrap(), 2);
    assert_eq!(
        map.get(stack, &7).unwrap().unwrap().get_val(stack).unwrap(),
        701
    );

    // Remove returns the value (owned); the key is then absent.
    let removed = map.remove(&alloc, &9).unwrap().unwrap();
    assert_eq!(removed.handle().get_val(stack).unwrap(), 900);
    removed.bstack_drop(&alloc).unwrap();
    assert!(map.get(stack, &9).unwrap().is_none());
    assert!(map.remove(&alloc, &9).unwrap().is_none());
    assert_eq!(map.len(stack).unwrap(), 1);

    map.bstack_drop(&alloc).unwrap();
}

#[test]
fn stdlib_map_grows_and_keeps_all() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    let map = BStackHashMap::<u32, MacroLeaf>::new(&alloc).unwrap();
    // Enough entries to force several rehashes (cap 4 -> ... ).
    for k in 0..100u32 {
        assert!(
            map.insert(&alloc, k, MacroLeaf::new(&alloc, k * 10).unwrap())
                .unwrap()
                .is_none()
        );
    }
    assert_eq!(map.len(stack).unwrap(), 100);
    // Every key survives the rehashes with its value.
    for k in 0..100u32 {
        assert_eq!(
            map.get(stack, &k).unwrap().unwrap().get_val(stack).unwrap(),
            k * 10
        );
    }

    // Remove the evens; odds remain (exercises tombstones + probing past them).
    for k in (0..100u32).step_by(2) {
        map.remove(&alloc, &k)
            .unwrap()
            .unwrap()
            .bstack_drop(&alloc)
            .unwrap();
    }
    assert_eq!(map.len(stack).unwrap(), 50);
    for k in 0..100u32 {
        let got = map.get(stack, &k).unwrap();
        if k % 2 == 0 {
            assert!(got.is_none());
        } else {
            assert_eq!(got.unwrap().get_val(stack).unwrap(), k * 10);
        }
    }

    map.bstack_drop(&alloc).unwrap();
}

#[test]
fn stdlib_map_pod_struct_key() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    // A composite Pod key (see Point3, defined for the box tests).
    let map = BStackHashMap::<Point3, MacroLeaf>::new(&alloc).unwrap();
    let a = Point3 { x: 1, y: 2, z: 3 };
    let b = Point3 { x: 1, y: 2, z: 4 };
    map.insert(&alloc, a, MacroLeaf::new(&alloc, 11).unwrap())
        .unwrap();
    map.insert(&alloc, b, MacroLeaf::new(&alloc, 22).unwrap())
        .unwrap();
    assert_eq!(
        map.get(stack, &a).unwrap().unwrap().get_val(stack).unwrap(),
        11
    );
    assert_eq!(
        map.get(stack, &b).unwrap().unwrap().get_val(stack).unwrap(),
        22
    );
    assert!(
        map.get(stack, &Point3 { x: 9, y: 9, z: 9 })
            .unwrap()
            .is_none()
    );

    map.bstack_drop(&alloc).unwrap();
}

#[test]
fn stdlib_map_distinct_tags() {
    assert_ne!(
        <BStackHashMap<u32, MacroLeaf> as BStackCast>::eightcc(),
        <BStackHashMap<u64, MacroLeaf> as BStackCast>::eightcc(),
    );
    assert_ne!(
        <BStackHashMap<u32, MacroLeaf> as BStackCast>::eightcc(),
        <BStackHashMap<u32, MacroStrongChild> as BStackCast>::eightcc(),
    );
}

#[test]
fn stdlib_map_drop_is_recursive() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();

    // Full recursion through a stored value must free the MacroLeaf grandchild.
    assert_teardown_reclaims(&alloc, || {
        let leaf = MacroLeaf::new(&alloc, 10).unwrap();
        let parent = MacroParent::new(&alloc, leaf, 1).unwrap();
        let map = BStackHashMap::<u32, MacroParent>::new(&alloc).unwrap();
        map.insert(&alloc, 42, parent).unwrap();
        map
    });
}

#[test]
fn stdlib_map_deep_clone_is_independent() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    let map = BStackHashMap::<u32, MacroLeaf>::new(&alloc).unwrap();
    for k in 0..8u32 {
        map.insert(&alloc, k, MacroLeaf::new(&alloc, k + 100).unwrap())
            .unwrap();
    }

    let clone = map.try_clone_in(&alloc).unwrap();
    for k in 0..8u32 {
        assert_eq!(
            clone
                .get(stack, &k)
                .unwrap()
                .unwrap()
                .get_val(stack)
                .unwrap(),
            k + 100
        );
    }
    // Clone's value blocks are fresh, not aliases.
    assert_ne!(
        clone.get(stack, &3).unwrap().unwrap().range().start(),
        map.get(stack, &3).unwrap().unwrap().range().start(),
    );

    // Mutating the clone leaves the original intact.
    clone
        .remove(&alloc, &3)
        .unwrap()
        .unwrap()
        .bstack_drop(&alloc)
        .unwrap();
    assert!(clone.get(stack, &3).unwrap().is_none());
    assert_eq!(
        map.get(stack, &3).unwrap().unwrap().get_val(stack).unwrap(),
        103
    );

    clone.bstack_drop(&alloc).unwrap();
    map.bstack_drop(&alloc).unwrap();
}

// --------------------------------------------------------------------------
// stdlib: BStackBTreeMap<K, V> — owned ordered map (copy-on-write B-tree)
// --------------------------------------------------------------------------

fn tree_pairs(tree: &BStackBTreeMap<u32, MacroLeaf>, stack: &BStack) -> Vec<(u32, u32)> {
    tree.to_vec(stack)
        .unwrap()
        .iter()
        .map(|(k, v)| (*k, v.get_val(stack).unwrap()))
        .collect()
}

#[test]
fn stdlib_tree_insert_get_ordered() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    let tree = BStackBTreeMap::<u32, MacroLeaf>::new(&alloc).unwrap();
    assert!(tree.is_empty(stack).unwrap());
    assert!(tree.get(stack, &5).unwrap().is_none());
    assert!(tree.first(stack).unwrap().is_none());

    // Insert 0..50 in a scrambled (but bijective) order to exercise splits.
    for i in 0..50u32 {
        let k = (i * 17) % 50;
        assert!(
            tree.insert(&alloc, k, MacroLeaf::new(&alloc, k * 10).unwrap())
                .unwrap()
                .is_none()
        );
    }
    assert_eq!(tree.len(stack).unwrap(), 50);

    // Every key present with its value.
    for k in 0..50u32 {
        assert_eq!(
            tree.get(stack, &k)
                .unwrap()
                .unwrap()
                .get_val(stack)
                .unwrap(),
            k * 10
        );
    }
    assert!(tree.get(stack, &999).unwrap().is_none());

    // Ordered iteration is sorted; first/last are the extremes.
    let pairs = tree_pairs(&tree, stack);
    let expected: Vec<(u32, u32)> = (0..50u32).map(|k| (k, k * 10)).collect();
    assert_eq!(pairs, expected);
    assert_eq!(tree.first(stack).unwrap().unwrap().0, 0);
    assert_eq!(tree.last(stack).unwrap().unwrap().0, 49);

    // Overwrite returns the previous value; len unchanged; order preserved.
    let old = tree
        .insert(&alloc, 25, MacroLeaf::new(&alloc, 9999).unwrap())
        .unwrap()
        .unwrap();
    assert_eq!(old.handle().get_val(stack).unwrap(), 250);
    old.bstack_drop(&alloc).unwrap();
    assert_eq!(tree.len(stack).unwrap(), 50);
    assert_eq!(
        tree.get(stack, &25)
            .unwrap()
            .unwrap()
            .get_val(stack)
            .unwrap(),
        9999
    );

    tree.bstack_drop(&alloc).unwrap();
}

// A 64-byte Pod key: a B-tree node is 280 + 15*64 = 1240 bytes, past the 1024
// inline `Scratch` buffer, so `get` exercises the heap-spill fallback.
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, bytemuck::Pod, bytemuck::Zeroable)]
struct BigKey([u64; 8]);

#[test]
fn stdlib_tree_large_key_spills() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    let tree = BStackBTreeMap::<BigKey, MacroLeaf>::new(&alloc).unwrap();
    for i in 0..20u64 {
        let mut k = [0u64; 8];
        k[0] = i;
        tree.insert(&alloc, BigKey(k), MacroLeaf::new(&alloc, i as u32).unwrap())
            .unwrap();
    }
    for i in 0..20u64 {
        let mut k = [0u64; 8];
        k[0] = i;
        assert_eq!(
            tree.get(stack, &BigKey(k))
                .unwrap()
                .unwrap()
                .get_val(stack)
                .unwrap(),
            i as u32
        );
    }
    tree.bstack_drop(&alloc).unwrap();
}

#[test]
fn stdlib_tree_distinct_tags() {
    assert_ne!(
        <BStackBTreeMap<u32, MacroLeaf> as BStackCast>::eightcc(),
        <BStackBTreeMap<u64, MacroLeaf> as BStackCast>::eightcc(),
    );
    assert_ne!(
        <BStackBTreeMap<u32, MacroLeaf> as BStackCast>::eightcc(),
        <BStackBTreeMap<u32, MacroStrongChild> as BStackCast>::eightcc(),
    );
}

#[test]
fn stdlib_tree_drop_is_recursive() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();

    // Full recursion through a stored value must free the MacroLeaf grandchild.
    assert_teardown_reclaims(&alloc, || {
        let leaf = MacroLeaf::new(&alloc, 10).unwrap();
        let parent = MacroParent::new(&alloc, leaf, 1).unwrap();
        let tree = BStackBTreeMap::<u32, MacroParent>::new(&alloc).unwrap();
        tree.insert(&alloc, 42, parent).unwrap();
        tree
    });
}

#[test]
fn stdlib_tree_deep_clone_is_independent() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    let tree = BStackBTreeMap::<u32, MacroLeaf>::new(&alloc).unwrap();
    for k in 0..30u32 {
        tree.insert(&alloc, k, MacroLeaf::new(&alloc, k + 100).unwrap())
            .unwrap();
    }

    let clone = tree.try_clone_in(&alloc).unwrap();
    for k in 0..30u32 {
        assert_eq!(
            clone
                .get(stack, &k)
                .unwrap()
                .unwrap()
                .get_val(stack)
                .unwrap(),
            k + 100
        );
    }
    // Fresh value blocks, not aliases.
    assert_ne!(
        clone.get(stack, &10).unwrap().unwrap().range().start(),
        tree.get(stack, &10).unwrap().unwrap().range().start(),
    );

    // Overwriting in the clone leaves the original intact.
    tree.insert(&alloc, 10, MacroLeaf::new(&alloc, 7).unwrap())
        .unwrap()
        .unwrap()
        .bstack_drop(&alloc)
        .unwrap();
    // (`clone` and `tree` share no nodes: the clone deep-copied every node.)
    assert_eq!(
        clone
            .get(stack, &10)
            .unwrap()
            .unwrap()
            .get_val(stack)
            .unwrap(),
        110
    );
    assert_eq!(
        tree.get(stack, &10)
            .unwrap()
            .unwrap()
            .get_val(stack)
            .unwrap(),
        7
    );

    clone.bstack_drop(&alloc).unwrap();
    tree.bstack_drop(&alloc).unwrap();
}

/// Many threads reading one shared tree concurrently (the B-tree is
/// single-writer / multi-reader: no writes race here, only lookups).
#[test]
fn stdlib_tree_concurrent_readers() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();

    let tree = BStackBTreeMap::<u32, MacroLeaf>::new(&alloc).unwrap();
    for k in 0..64u32 {
        tree.insert(&alloc, k, MacroLeaf::new(&alloc, k * 2).unwrap())
            .unwrap();
    }

    std::thread::scope(|s| {
        for _ in 0..8 {
            let tree = &tree;
            let alloc = &alloc;
            s.spawn(move || {
                for k in 0..64u32 {
                    assert_eq!(
                        tree.get(alloc.stack(), &k)
                            .unwrap()
                            .unwrap()
                            .get_val(alloc.stack())
                            .unwrap(),
                        k * 2
                    );
                }
            });
        }
    });

    tree.bstack_drop(&alloc).unwrap();
}

/// Many threads inserting distinct keys into one shared map, driving concurrent
/// growth/rehash. A non-atomic probe/write or a racy rehash would drop entries.
#[test]
fn stdlib_map_concurrent_insert() {
    const THREADS: u32 = 8;
    const ITERS: u32 = 8;

    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let map = BStackHashMap::<u32, MacroLeaf>::new(&alloc).unwrap();
    let total = (THREADS * ITERS) as u64;

    std::thread::scope(|s| {
        for t in 0..THREADS {
            let map = &map;
            let alloc = &alloc;
            s.spawn(move || {
                for i in 0..ITERS {
                    let k = t * ITERS + i;
                    map.insert(alloc, k, MacroLeaf::new(alloc, k).unwrap())
                        .unwrap();
                }
            });
        }
    });

    assert_eq!(map.len(alloc.stack()).unwrap(), total);
    for k in 0..(THREADS * ITERS) {
        assert_eq!(
            map.get(alloc.stack(), &k)
                .unwrap()
                .unwrap()
                .get_val(alloc.stack())
                .unwrap(),
            k
        );
    }

    map.bstack_drop(&alloc).unwrap();
}

// --------------------------------------------------------------------------
// stdlib: BStackString — standalone owned UTF-8 string
// --------------------------------------------------------------------------

#[test]
fn stdlib_string_roundtrip_set_push() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    let s = BStackString::new(&alloc, "hello").unwrap();
    assert_eq!(s.handle().len(stack).unwrap(), 5);
    assert_eq!(s.handle().to_string(stack).unwrap(), "hello");

    // Replace with something longer, then shorter.
    s.handle().set(&alloc, "hello, world").unwrap();
    assert_eq!(s.handle().to_string(stack).unwrap(), "hello, world");
    s.handle().set(&alloc, "hi").unwrap();
    assert_eq!(s.handle().to_string(stack).unwrap(), "hi");

    // Append.
    s.handle().push_str(&alloc, " there").unwrap();
    assert_eq!(s.handle().to_string(stack).unwrap(), "hi there");

    // Empty string has no bytes block.
    let e = BStackString::new(&alloc, "").unwrap();
    assert!(e.handle().is_empty(stack).unwrap());
    assert_eq!(e.handle().to_string(stack).unwrap(), "");

    s.bstack_drop(&alloc).unwrap();
    e.bstack_drop(&alloc).unwrap();
}

#[test]
fn stdlib_string_unicode() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    let text = "héllo — 世界 🦀";
    let s = BStackString::new(&alloc, text).unwrap();
    assert_eq!(s.handle().len(stack).unwrap(), text.len() as u64); // byte length
    assert_eq!(s.handle().to_string(stack).unwrap(), text);
    s.bstack_drop(&alloc).unwrap();
}

#[test]
fn stdlib_string_deep_clone_is_independent() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    let s = BStackString::new(&alloc, "original").unwrap();
    let clone = s.try_clone_in(&alloc).unwrap();
    assert_eq!(clone.handle().to_string(stack).unwrap(), "original");

    // Mutating the clone leaves the original intact.
    clone.handle().set(&alloc, "changed").unwrap();
    assert_eq!(clone.handle().to_string(stack).unwrap(), "changed");
    assert_eq!(s.handle().to_string(stack).unwrap(), "original");

    clone.bstack_drop(&alloc).unwrap();
    s.bstack_drop(&alloc).unwrap();
}

#[test]
fn stdlib_string_as_map_value() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    // The headline use: strings as owned map values.
    let map = BStackHashMap::<u32, BStackString>::new(&alloc).unwrap();
    map.insert(&alloc, 1, BStackString::new(&alloc, "one").unwrap())
        .unwrap();
    map.insert(&alloc, 2, BStackString::new(&alloc, "two").unwrap())
        .unwrap();
    assert_eq!(
        map.get(stack, &1)
            .unwrap()
            .unwrap()
            .to_string(stack)
            .unwrap(),
        "one"
    );
    assert_eq!(
        map.get(stack, &2)
            .unwrap()
            .unwrap()
            .to_string(stack)
            .unwrap(),
        "two"
    );

    // Overwrite returns the old string (owned), which we free.
    let old = map
        .insert(&alloc, 1, BStackString::new(&alloc, "uno").unwrap())
        .unwrap()
        .unwrap();
    assert_eq!(old.handle().to_string(stack).unwrap(), "one");
    old.bstack_drop(&alloc).unwrap();
    assert_eq!(
        map.get(stack, &1)
            .unwrap()
            .unwrap()
            .to_string(stack)
            .unwrap(),
        "uno"
    );

    // Dropping the map recursively frees every string value (and its bytes block).
    map.bstack_drop(&alloc).unwrap();
}

// --------------------------------------------------------------------------
// stdlib: BStackCountingBloomFilter<K> — probabilistic set
// --------------------------------------------------------------------------

#[test]
fn stdlib_bloom_no_false_negatives() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    let bloom = BStackCountingBloomFilter::<u32>::with_capacity(&alloc, 1000, 0.001).unwrap();
    assert!(bloom.is_empty(stack).unwrap());
    assert!(!bloom.contains(stack, &7).unwrap()); // fresh: everything absent

    for k in 0..50u32 {
        bloom.insert(&alloc, &k).unwrap();
    }
    assert_eq!(bloom.count(stack).unwrap(), 50);

    // No false negatives: every inserted key reports present.
    for k in 0..50u32 {
        assert!(bloom.contains(stack, &k).unwrap());
    }

    // Disjoint keys are (almost all) absent — allow a few false positives.
    let absent = (1_000..1_050u32)
        .filter(|k| !bloom.contains(stack, k).unwrap())
        .count();
    assert!(
        absent >= 45,
        "too many false positives: {}/50 absent",
        absent
    );

    // A positive FP estimate in (0, 1).
    let fp = bloom.estimated_fp_rate(stack).unwrap();
    assert!(fp > 0.0 && fp < 1.0);

    bloom.bstack_drop(&alloc).unwrap();
}

#[test]
fn stdlib_bloom_remove_and_clear() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    // A single inserted key: removing it drives its counters back to zero (no
    // sharing), so `contains` becomes definitively false.
    let bloom = BStackCountingBloomFilter::<u32>::with_capacity(&alloc, 100, 0.01).unwrap();
    bloom.insert(&alloc, &42).unwrap();
    assert!(bloom.contains(stack, &42).unwrap());
    assert_eq!(bloom.count(stack).unwrap(), 1);
    bloom.remove(&alloc, &42).unwrap();
    assert!(!bloom.contains(stack, &42).unwrap());
    assert_eq!(bloom.count(stack).unwrap(), 0);

    // clear() zeroes everything.
    for k in 0..20u32 {
        bloom.insert(&alloc, &k).unwrap();
    }
    assert_eq!(bloom.count(stack).unwrap(), 20);
    bloom.clear(&alloc).unwrap();
    assert_eq!(bloom.count(stack).unwrap(), 0);
    for k in 0..20u32 {
        assert!(!bloom.contains(stack, &k).unwrap());
    }

    bloom.bstack_drop(&alloc).unwrap();
}

#[test]
fn stdlib_bloom_deep_clone_is_independent() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    let bloom = BStackCountingBloomFilter::<u32>::with_capacity(&alloc, 100, 0.01).unwrap();
    for k in 0..10u32 {
        bloom.insert(&alloc, &k).unwrap();
    }
    let clone = bloom.try_clone_in(&alloc).unwrap();
    for k in 0..10u32 {
        assert!(clone.contains(stack, &k).unwrap());
    }
    // Clearing the clone leaves the original intact.
    clone.clear(&alloc).unwrap();
    assert_eq!(clone.count(stack).unwrap(), 0);
    assert_eq!(bloom.count(stack).unwrap(), 10);
    assert!(bloom.contains(stack, &5).unwrap());

    clone.bstack_drop(&alloc).unwrap();
    bloom.bstack_drop(&alloc).unwrap();
}

#[test]
fn stdlib_bloom_distinct_tags() {
    assert_ne!(
        <BStackCountingBloomFilter<u32> as BStackCast>::eightcc(),
        <BStackCountingBloomFilter<u64> as BStackCast>::eightcc(),
    );
}

#[test]
fn stdlib_bloom_guards_a_map() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    // The headline pattern: a bloom filter in front of a map skips the disk probe
    // for keys that are definitely absent.
    let bloom = BStackCountingBloomFilter::<u32>::with_capacity(&alloc, 1000, 0.001).unwrap();
    let map = BStackHashMap::<u32, MacroLeaf>::new(&alloc).unwrap();
    for k in 0..40u32 {
        map.insert(&alloc, k, MacroLeaf::new(&alloc, k * 3).unwrap())
            .unwrap();
        bloom.insert(&alloc, &k).unwrap();
    }

    let lookup = |k: u32| -> Option<u32> {
        // Fast-reject via the filter before touching the map.
        if !bloom.contains(stack, &k).unwrap() {
            return None;
        }
        map.get(stack, &k)
            .unwrap()
            .map(|v| v.get_val(stack).unwrap())
    };

    for k in 0..40u32 {
        assert_eq!(lookup(k), Some(k * 3));
    }
    // Absent keys: the filter short-circuits (and the map agrees).
    for k in 500..540u32 {
        assert_eq!(lookup(k), None);
    }

    bloom.bstack_drop(&alloc).unwrap();
    map.bstack_drop(&alloc).unwrap();
}

// --------------------------------------------------------------------------
// stdlib: BStackHashSet<K> — set with an embedded bloom filter
// --------------------------------------------------------------------------

#[test]
fn stdlib_hashset_basic() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    let set = BStackHashSet::<u32>::new(&alloc).unwrap();
    assert!(set.is_empty(stack).unwrap());
    assert!(!set.contains(stack, &5).unwrap());

    // Insert many to force table growth; insert reports newness.
    for k in 0..100u32 {
        assert!(set.insert(&alloc, k).unwrap());
    }
    assert_eq!(set.len(stack).unwrap(), 100);
    // Duplicate insert is a no-op returning false.
    assert!(!set.insert(&alloc, 50).unwrap());
    assert_eq!(set.len(stack).unwrap(), 100);

    // No false negatives: every inserted key is present.
    for k in 0..100u32 {
        assert!(set.contains(stack, &k).unwrap());
    }
    assert!(!set.contains(stack, &10_000).unwrap());

    // Remove and re-check.
    assert!(set.remove(&alloc, &50).unwrap());
    assert!(!set.remove(&alloc, &50).unwrap()); // already gone
    assert!(!set.contains(stack, &50).unwrap());
    assert_eq!(set.len(stack).unwrap(), 99);

    set.bstack_drop(&alloc).unwrap();
}

#[test]
fn stdlib_hashset_deep_clone_is_independent() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    let set = BStackHashSet::<u32>::new(&alloc).unwrap();
    for k in 0..20u32 {
        set.insert(&alloc, k).unwrap();
    }
    let clone = set.try_clone_in(&alloc).unwrap();
    for k in 0..20u32 {
        assert!(clone.contains(stack, &k).unwrap());
    }
    // Mutating the clone (incl. its embedded bloom) leaves the original intact.
    clone.remove(&alloc, &5).unwrap();
    assert!(!clone.contains(stack, &5).unwrap());
    assert!(set.contains(stack, &5).unwrap());
    assert_eq!(set.len(stack).unwrap(), 20);
    assert_eq!(clone.len(stack).unwrap(), 19);

    clone.bstack_drop(&alloc).unwrap();
    set.bstack_drop(&alloc).unwrap();
}

#[test]
fn stdlib_hashset_distinct_tags() {
    assert_ne!(
        <BStackHashSet<u32> as BStackCast>::eightcc(),
        <BStackHashSet<u64> as BStackCast>::eightcc(),
    );
}

// --------------------------------------------------------------------------
// stdlib: BStackBTreeSet<K> — ordered set with an embedded bloom filter
// --------------------------------------------------------------------------

#[test]
fn stdlib_btreeset_ordered() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    let set = BStackBTreeSet::<u32>::new(&alloc).unwrap();
    assert!(set.first(stack).unwrap().is_none());

    // Scrambled but bijective insertion order to force node splits.
    for i in 0..60u32 {
        let k = (i * 23) % 60;
        assert!(set.insert(&alloc, k).unwrap());
    }
    assert_eq!(set.len(stack).unwrap(), 60);
    assert!(!set.insert(&alloc, 30).unwrap()); // duplicate
    assert_eq!(set.len(stack).unwrap(), 60);

    // No false negatives; ordered iteration is sorted.
    for k in 0..60u32 {
        assert!(set.contains(stack, &k).unwrap());
    }
    assert!(!set.contains(stack, &999).unwrap());
    assert_eq!(set.to_vec(stack).unwrap(), (0..60u32).collect::<Vec<_>>());
    assert_eq!(set.first(stack).unwrap().unwrap(), 0);
    assert_eq!(set.last(stack).unwrap().unwrap(), 59);

    set.bstack_drop(&alloc).unwrap();
}

#[test]
fn stdlib_btreeset_deep_clone_is_independent() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    let set = BStackBTreeSet::<u32>::new(&alloc).unwrap();
    for k in 0..30u32 {
        set.insert(&alloc, k).unwrap();
    }
    let clone = set.try_clone_in(&alloc).unwrap();
    for k in 0..30u32 {
        assert!(clone.contains(stack, &k).unwrap());
    }
    // Inserting into the clone leaves the original unchanged.
    assert!(clone.insert(&alloc, 100).unwrap());
    assert!(clone.contains(stack, &100).unwrap());
    assert!(!set.contains(stack, &100).unwrap());
    assert_eq!(set.len(stack).unwrap(), 30);

    clone.bstack_drop(&alloc).unwrap();
    set.bstack_drop(&alloc).unwrap();
}

#[test]
fn stdlib_btreeset_distinct_tags() {
    assert_ne!(
        <BStackBTreeSet<u32> as BStackCast>::eightcc(),
        <BStackBTreeSet<u64> as BStackCast>::eightcc(),
    );
}

#[test]
fn stdlib_tree_remove_rebalances() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    let tree = BStackBTreeMap::<u32, MacroLeaf>::new(&alloc).unwrap();
    for k in 0..200u32 {
        tree.insert(&alloc, k, MacroLeaf::new(&alloc, k * 10).unwrap())
            .unwrap();
    }
    assert_eq!(tree.len(stack).unwrap(), 200);

    // Remove-absent returns None.
    assert!(tree.remove(&alloc, &1000).unwrap().is_none());

    // Remove every even key (heavy borrow/merge across a multi-level tree).
    for k in (0..200u32).step_by(2) {
        let v = tree.remove(&alloc, &k).unwrap().unwrap();
        assert_eq!(v.handle().get_val(stack).unwrap(), k * 10);
        v.bstack_drop(&alloc).unwrap();
    }
    assert_eq!(tree.len(stack).unwrap(), 100);

    // Evens gone, odds intact, iteration still sorted.
    for k in 0..200u32 {
        let g = tree.get(stack, &k).unwrap();
        if k % 2 == 0 {
            assert!(g.is_none());
        } else {
            assert_eq!(g.unwrap().get_val(stack).unwrap(), k * 10);
        }
    }
    let keys: Vec<u32> = tree
        .to_vec(stack)
        .unwrap()
        .iter()
        .map(|(k, _)| *k)
        .collect();
    assert_eq!(keys, (0..200u32).filter(|k| k % 2 == 1).collect::<Vec<_>>());

    // Drain the rest → empty (root collapses to 0).
    for k in (1..200u32).step_by(2) {
        tree.remove(&alloc, &k)
            .unwrap()
            .unwrap()
            .bstack_drop(&alloc)
            .unwrap();
    }
    assert_eq!(tree.len(stack).unwrap(), 0);
    assert!(tree.is_empty(stack).unwrap());
    assert!(tree.first(stack).unwrap().is_none());

    // Reinsert after collapse works.
    tree.insert(&alloc, 42, MacroLeaf::new(&alloc, 420).unwrap())
        .unwrap();
    assert_eq!(
        tree.get(stack, &42)
            .unwrap()
            .unwrap()
            .get_val(stack)
            .unwrap(),
        420
    );

    tree.bstack_drop(&alloc).unwrap();
}

#[test]
fn stdlib_btreeset_remove_rebalances() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    let set = BStackBTreeSet::<u32>::new(&alloc).unwrap();
    for k in 0..60u32 {
        set.insert(&alloc, k).unwrap();
    }
    assert_eq!(set.len(stack).unwrap(), 60);
    assert!(!set.remove(&alloc, &999).unwrap()); // absent

    // Remove every even key (triggers borrow/merge across levels).
    for k in (0..60u32).step_by(2) {
        assert!(set.remove(&alloc, &k).unwrap());
        assert!(!set.remove(&alloc, &k).unwrap()); // already gone
    }
    assert_eq!(set.len(stack).unwrap(), 30);
    for k in 0..60u32 {
        assert_eq!(set.contains(stack, &k).unwrap(), k % 2 == 1);
    }
    assert_eq!(
        set.to_vec(stack).unwrap(),
        (0..60u32).filter(|k| k % 2 == 1).collect::<Vec<_>>()
    );

    // Drain the rest → empty, then reinsert.
    for k in (1..60u32).step_by(2) {
        assert!(set.remove(&alloc, &k).unwrap());
    }
    assert_eq!(set.len(stack).unwrap(), 0);
    assert!(set.first(stack).unwrap().is_none());
    assert!(set.insert(&alloc, 7).unwrap());
    assert!(set.contains(stack, &7).unwrap());

    set.bstack_drop(&alloc).unwrap();
}

#[test]
fn stdlib_string_extra_methods() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    let s = BStackString::new(&alloc, "hello").unwrap();
    s.handle().push(&alloc, '!').unwrap();
    assert_eq!(s.handle().to_string(stack).unwrap(), "hello!");
    assert!(s.handle().starts_with(stack, "hell").unwrap());
    assert!(s.handle().ends_with(stack, "o!").unwrap());
    assert!(s.handle().contains(stack, "ell").unwrap());
    assert!(s.handle().eq_str(stack, "hello!").unwrap());
    assert!(!s.handle().eq_str(stack, "nope").unwrap());

    // truncate to a boundary; non-boundary is an error.
    s.handle().truncate(&alloc, 5).unwrap();
    assert_eq!(s.handle().to_string(stack).unwrap(), "hello");
    let u = BStackString::new(&alloc, "héllo").unwrap(); // 'é' is 2 bytes at [1,2]
    assert_eq!(u.handle().char_count(stack).unwrap(), 5);
    assert_eq!(u.handle().len(stack).unwrap(), 6);
    assert!(u.handle().truncate(&alloc, 2).is_err()); // splits 'é'
    u.bstack_drop(&alloc).unwrap();

    // clear empties it.
    s.handle().clear(&alloc).unwrap();
    assert!(s.handle().is_empty(stack).unwrap());
    assert_eq!(s.handle().char_count(stack).unwrap(), 0);

    s.bstack_drop(&alloc).unwrap();
}

// --------------------------------------------------------------------------
// stdlib: BStackBinaryHeap<K, V> — priority queue (binary min-heap)
// --------------------------------------------------------------------------

#[test]
fn stdlib_heap_pop_ascending() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    let heap = BStackBinaryHeap::<u32, MacroLeaf>::new(&alloc).unwrap();
    assert!(heap.is_empty(stack).unwrap());
    assert!(heap.peek(stack).unwrap().is_none());
    assert!(heap.pop(&alloc).unwrap().is_none());

    // Push 0..50 in a scrambled (bijective) order; forces growth.
    for i in 0..50u32 {
        let k = (i * 17) % 50;
        heap.push(&alloc, k, MacroLeaf::new(&alloc, k * 10).unwrap())
            .unwrap();
    }
    assert_eq!(heap.len(stack).unwrap(), 50);
    assert_eq!(heap.peek(stack).unwrap().unwrap().0, 0); // min on top

    // Pop drains in ascending key order, with the right values.
    for expected in 0..50u32 {
        let (k, v) = heap.pop(&alloc).unwrap().unwrap();
        assert_eq!(k, expected);
        assert_eq!(v.handle().get_val(stack).unwrap(), expected * 10);
        v.bstack_drop(&alloc).unwrap();
    }
    assert!(heap.is_empty(stack).unwrap());
    assert!(heap.pop(&alloc).unwrap().is_none());

    heap.bstack_drop(&alloc).unwrap();
}

#[test]
fn stdlib_heap_duplicate_keys() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();

    let heap = BStackBinaryHeap::<u32, MacroLeaf>::new(&alloc).unwrap();
    for (k, v) in [(3u32, 30u32), (1, 10), (3, 31), (1, 11), (2, 20)] {
        heap.push(&alloc, k, MacroLeaf::new(&alloc, v).unwrap())
            .unwrap();
    }
    // Keys come out sorted (values within equal keys unspecified).
    let mut keys = Vec::new();
    while let Some((k, v)) = heap.pop(&alloc).unwrap() {
        keys.push(k);
        v.bstack_drop(&alloc).unwrap();
    }
    assert_eq!(keys, vec![1, 1, 2, 3, 3]);

    heap.bstack_drop(&alloc).unwrap();
}

#[test]
fn stdlib_heap_drop_is_recursive() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();

    // Full recursion through a stored value must free the MacroLeaf grandchild.
    assert_teardown_reclaims(&alloc, || {
        let leaf = MacroLeaf::new(&alloc, 10).unwrap();
        let parent = MacroParent::new(&alloc, leaf, 1).unwrap();
        let heap = BStackBinaryHeap::<u32, MacroParent>::new(&alloc).unwrap();
        heap.push(&alloc, 5, parent).unwrap();
        heap
    });
}

#[test]
fn stdlib_heap_deep_clone_is_independent() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    let heap = BStackBinaryHeap::<u32, MacroLeaf>::new(&alloc).unwrap();
    for k in [5u32, 1, 4, 2, 3] {
        heap.push(&alloc, k, MacroLeaf::new(&alloc, k * 10).unwrap())
            .unwrap();
    }
    let clone = heap.try_clone_in(&alloc).unwrap();
    // Fresh value blocks.
    assert_ne!(
        clone.peek(stack).unwrap().unwrap().1.range().start(),
        heap.peek(stack).unwrap().unwrap().1.range().start(),
    );
    // Draining the clone leaves the original intact.
    for expected in 1..=5u32 {
        let (k, v) = clone.pop(&alloc).unwrap().unwrap();
        assert_eq!(k, expected);
        v.bstack_drop(&alloc).unwrap();
    }
    assert_eq!(heap.len(stack).unwrap(), 5);
    assert_eq!(heap.peek(stack).unwrap().unwrap().0, 1);

    clone.bstack_drop(&alloc).unwrap();
    heap.bstack_drop(&alloc).unwrap();
}

#[test]
fn stdlib_heap_distinct_tags() {
    assert_ne!(
        <BStackBinaryHeap<u32, MacroLeaf> as BStackCast>::eightcc(),
        <BStackBinaryHeap<u64, MacroLeaf> as BStackCast>::eightcc(),
    );
}

// --------------------------------------------------------------------------
// stdlib: iterators
// --------------------------------------------------------------------------

#[test]
fn stdlib_deque_iter() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();
    let dq = BStackDeque::<MacroLeaf>::new(&alloc).unwrap();
    for v in 0..10u32 {
        dq.push_back(&alloc, MacroLeaf::new(&alloc, v).unwrap())
            .unwrap();
    }
    let mut got = Vec::new();
    for r in dq.iter(stack).unwrap() {
        got.push(r.unwrap().get_val(stack).unwrap());
    }
    assert_eq!(got, (0..10u32).collect::<Vec<_>>());
    dq.bstack_drop(&alloc).unwrap();
}

#[test]
fn stdlib_list_iter() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();
    let list = BStackLinkedList::<MacroLeaf>::new(&alloc).unwrap();
    for v in 0..6u32 {
        list.push_back(&alloc, MacroLeaf::new(&alloc, v).unwrap())
            .unwrap();
    }
    let mut got = Vec::new();
    for r in list.iter(stack).unwrap() {
        got.push(r.unwrap().get_val(stack).unwrap());
    }
    assert_eq!(got, (0..6u32).collect::<Vec<_>>());
    list.bstack_drop(&alloc).unwrap();
}

#[test]
fn stdlib_map_iter() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();
    let map = BStackHashMap::<u32, MacroLeaf>::new(&alloc).unwrap();
    for k in 0..20u32 {
        map.insert(&alloc, k, MacroLeaf::new(&alloc, k * 10).unwrap())
            .unwrap();
    }
    let mut got = Vec::new();
    for r in map.iter(stack).unwrap() {
        let (k, v) = r.unwrap();
        got.push((k, v.get_val(stack).unwrap()));
    }
    got.sort_unstable(); // unordered iteration
    assert_eq!(got, (0..20u32).map(|k| (k, k * 10)).collect::<Vec<_>>());
    map.bstack_drop(&alloc).unwrap();
}

#[test]
fn stdlib_hashset_iter() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();
    let set = BStackHashSet::<u32>::new(&alloc).unwrap();
    for k in 0..20u32 {
        set.insert(&alloc, k).unwrap();
    }
    let mut got = Vec::new();
    for r in set.iter(stack).unwrap() {
        got.push(r.unwrap());
    }
    got.sort_unstable();
    assert_eq!(got, (0..20u32).collect::<Vec<_>>());
    set.bstack_drop(&alloc).unwrap();
}

#[test]
fn stdlib_tree_iter_and_range() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();
    let tree = BStackBTreeMap::<u32, MacroLeaf>::new(&alloc).unwrap();
    for i in 0..40u32 {
        let k = (i * 13) % 40; // scrambled permutation
        tree.insert(&alloc, k, MacroLeaf::new(&alloc, k * 10).unwrap())
            .unwrap();
    }
    // Full iteration is sorted.
    let mut got = Vec::new();
    for r in tree.iter(stack).unwrap() {
        let (k, v) = r.unwrap();
        got.push((k, v.get_val(stack).unwrap()));
    }
    assert_eq!(got, (0..40u32).map(|k| (k, k * 10)).collect::<Vec<_>>());

    // range(10, 20) is the inclusive sub-slice, still sorted.
    let mut ranged = Vec::new();
    for r in tree.range(stack, 10, 20).unwrap() {
        ranged.push(r.unwrap().0);
    }
    assert_eq!(ranged, (10..=20u32).collect::<Vec<_>>());

    // A range whose lo falls between keys and hi past the end.
    let mut r2 = Vec::new();
    for r in tree.range(stack, 37, 999).unwrap() {
        r2.push(r.unwrap().0);
    }
    assert_eq!(r2, vec![37, 38, 39]);

    tree.bstack_drop(&alloc).unwrap();
}

#[test]
fn stdlib_btreeset_iter_and_range() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();
    let set = BStackBTreeSet::<u32>::new(&alloc).unwrap();
    for i in 0..40u32 {
        set.insert(&alloc, (i * 13) % 40).unwrap();
    }
    let mut got = Vec::new();
    for r in set.iter(stack).unwrap() {
        got.push(r.unwrap());
    }
    assert_eq!(got, (0..40u32).collect::<Vec<_>>());

    let mut ranged = Vec::new();
    for r in set.range(stack, 15, 18).unwrap() {
        ranged.push(r.unwrap());
    }
    assert_eq!(ranged, vec![15, 16, 17, 18]);

    set.bstack_drop(&alloc).unwrap();
}

// --------------------------------------------------------------------------
// stdlib: entry API (get_or_insert_with / get_or_insert)
// --------------------------------------------------------------------------

#[test]
fn stdlib_map_entry() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();
    let map = BStackHashMap::<u32, MacroLeaf>::new(&alloc).unwrap();

    // Absent: inserts, f produced the value.
    let (v, inserted) = map
        .get_or_insert_with(&alloc, 7, || MacroLeaf::new(&alloc, 70))
        .unwrap();
    assert!(inserted);
    assert_eq!(v.get_val(stack).unwrap(), 70);

    // Present: single probe, f NOT called, value unchanged.
    map.insert(&alloc, 5, MacroLeaf::new(&alloc, 50).unwrap())
        .unwrap();
    let called = std::cell::Cell::new(false);
    let (v, inserted) = map
        .get_or_insert_with(&alloc, 5, || {
            called.set(true);
            MacroLeaf::new(&alloc, 999)
        })
        .unwrap();
    assert!(!inserted);
    assert!(!called.get(), "f must not run on a hit");
    assert_eq!(v.get_val(stack).unwrap(), 50);

    // Eager get_or_insert frees the unused default on a hit.
    let (v, inserted) = map
        .get_or_insert(&alloc, 5, MacroLeaf::new(&alloc, 111).unwrap())
        .unwrap();
    assert!(!inserted);
    assert_eq!(v.get_val(stack).unwrap(), 50);

    map.bstack_drop(&alloc).unwrap();
}

#[test]
fn stdlib_map_entry_counter() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();
    // The headline entry pattern: a counter map with in-place value mutation.
    let counts = BStackHashMap::<u32, BStackBox<u64>>::new(&alloc).unwrap();
    for k in [1u32, 1, 2, 1, 2, 1] {
        let (v, inserted) = counts
            .get_or_insert_with(&alloc, k, || BStackBox::new(&alloc, 1u64))
            .unwrap();
        if !inserted {
            let cur = v.get(stack).unwrap();
            v.set(&alloc, cur + 1).unwrap();
        }
    }
    assert_eq!(
        counts.get(stack, &1).unwrap().unwrap().get(stack).unwrap(),
        4
    );
    assert_eq!(
        counts.get(stack, &2).unwrap().unwrap().get(stack).unwrap(),
        2
    );
    counts.bstack_drop(&alloc).unwrap();
}

#[test]
fn stdlib_tree_entry() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();
    let tree = BStackBTreeMap::<u32, MacroLeaf>::new(&alloc).unwrap();

    let (v, inserted) = tree
        .get_or_insert_with(&alloc, 7, || MacroLeaf::new(&alloc, 70))
        .unwrap();
    assert!(inserted);
    assert_eq!(v.get_val(stack).unwrap(), 70);

    let called = std::cell::Cell::new(false);
    let (v, inserted) = tree
        .get_or_insert_with(&alloc, 7, || {
            called.set(true);
            MacroLeaf::new(&alloc, 999)
        })
        .unwrap();
    assert!(!inserted);
    assert!(!called.get());
    assert_eq!(v.get_val(stack).unwrap(), 70);

    tree.bstack_drop(&alloc).unwrap();
}
// --------------------------------------------------------------------------
// Stdlib collections composed into `#[bstack_block]` / `#[bstack_enum]` types.
// Collections are `BStackBlock + TryCloneIn` and override the `__bstack_*`
// hooks, so they should compose anywhere a scalar block can; these tests verify
// the compositions actually build, read back, deep-clone (independent copies),
// and tear down (leak-free) — on both the sequential (FirstFit) and the
// bulk/WAL (GhostTree) allocators, plus cross-file `Foreign<Collection>`
// clone/teardown.
// --------------------------------------------------------------------------

fn deque_offsets(dq: &BStackDeque<MacroLeaf>, stack: &BStack) -> Vec<u64> {
    dq.to_vec(stack)
        .unwrap()
        .into_iter()
        .map(|h| h.range().start())
        .collect()
}

// A block whose fields ARE stdlib collections: an owned deque and a nullable
// owned map. Each lowers to a `u64` offset on disk, exactly like a scalar owned
// child — the collection's own `__bstack_*` hooks do the recursive work.
#[bstack_block]
struct CollectionFields {
    tag: u32,
    #[bstack_owned]
    dq: BStackDeque<MacroLeaf>,
    #[bstack_owned]
    maybe_map: Option<BStackHashMap<u32, MacroLeaf>>,
}

fn build_collection_fields<A: BStackRaiiAllocator>(alloc: &A) -> BStackOwned<CollectionFields> {
    let dq = BStackDeque::<MacroLeaf>::new(alloc).unwrap();
    for v in [10u32, 20, 30] {
        dq.push_back(alloc, MacroLeaf::new(alloc, v).unwrap())
            .unwrap();
    }
    let map = BStackHashMap::<u32, MacroLeaf>::new(alloc).unwrap();
    map.insert(alloc, 1, MacroLeaf::new(alloc, 100).unwrap())
        .unwrap();
    map.insert(alloc, 2, MacroLeaf::new(alloc, 200).unwrap())
        .unwrap();
    CollectionFields::new(alloc, 7, dq, Some(map)).unwrap()
}

#[test]
fn stdlib_in_block_fields_build_read_clone_teardown() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    let h = build_collection_fields(&alloc);
    assert_eq!(h.handle().get_tag(stack).unwrap(), 7);

    // Read the collections back through the generated accessors.
    let got_dq = h.handle().get_dq(stack).unwrap();
    assert_eq!(deque_values(&got_dq, stack), vec![10, 20, 30]);
    let got_map = h.handle().get_maybe_map(stack).unwrap().expect("Some map");
    assert_eq!(got_map.len(stack).unwrap(), 2);
    assert_eq!(
        got_map
            .get(stack, &1)
            .unwrap()
            .unwrap()
            .get_val(stack)
            .unwrap(),
        100
    );
    assert_eq!(
        got_map
            .get(stack, &2)
            .unwrap()
            .unwrap()
            .get_val(stack)
            .unwrap(),
        200
    );

    // A `None` map round-trips through the offset-0 niche.
    let dq0 = BStackDeque::<MacroLeaf>::new(&alloc).unwrap();
    let h_none = CollectionFields::new(&alloc, 0, dq0, None).unwrap();
    assert!(h_none.handle().get_maybe_map(stack).unwrap().is_none());
    h_none.bstack_drop(&alloc).unwrap();

    // Deep clone: the clone carries independent copies of BOTH collections.
    let c = h.handle().try_clone_in(&alloc).unwrap();
    let cdq = c.handle().get_dq(stack).unwrap();
    let cmap = c.handle().get_maybe_map(stack).unwrap().unwrap();
    assert_ne!(cdq.range().start(), got_dq.range().start());
    assert_ne!(c.handle().range().start(), h.handle().range().start());
    assert_eq!(deque_values(&cdq, stack), vec![10, 20, 30]);
    // Every deque element is a fresh block, not an alias (the deep clone
    // recursed into the collection's ring, not just the handle).
    assert_eq!(deque_offsets(&got_dq, stack).len(), 3);
    assert!(
        deque_offsets(&got_dq, stack)
            .iter()
            .zip(deque_offsets(&cdq, stack).iter())
            .all(|(a, b)| a != b),
        "clone must deep-copy the deque's elements"
    );
    assert_eq!(
        cmap.get(stack, &1)
            .unwrap()
            .unwrap()
            .get_val(stack)
            .unwrap(),
        100
    );
    assert_eq!(
        cmap.get(stack, &2)
            .unwrap()
            .unwrap()
            .get_val(stack)
            .unwrap(),
        200
    );

    // Freeing the clone frees only the clone's subtree; the original is intact.
    c.bstack_drop(&alloc).unwrap();
    assert_eq!(deque_values(&got_dq, stack), vec![10, 20, 30]);
    assert_eq!(got_map.len(stack).unwrap(), 2);
    h.bstack_drop(&alloc).unwrap();

    // Teardown must reclaim the whole structure — deque handle, ring, elements,
    // map handle, bucket blocks, and value blocks — with no leak. Build + tear
    // twice and assert the stack returns exactly to baseline.
    assert_teardown_reclaims(&alloc, || build_collection_fields(&alloc));
}

#[test]
fn stdlib_in_block_fields_on_bulk_allocator() {
    // The same composition on GhostTree (`atomic_bulk() == true`): teardown frees
    // the whole multi-block subtree with one atomic `dealloc_bulk`, and the
    // two-pass (measure -> build) clone must measure every block the collections
    // own — ring, bucket arrays, elements — so the clone is exact-sized.
    let tmp = TempStack::new();
    let alloc = tmp.ghost_allocator();
    let stack = alloc.stack();

    let h = build_collection_fields(&alloc);
    let c = h.handle().try_clone_in(&alloc).unwrap();

    assert_eq!(
        deque_values(&c.handle().get_dq(stack).unwrap(), stack),
        vec![10, 20, 30]
    );
    assert_eq!(
        c.handle()
            .get_maybe_map(stack)
            .unwrap()
            .unwrap()
            .get(stack, &2)
            .unwrap()
            .unwrap()
            .get_val(stack)
            .unwrap(),
        200
    );

    // Warm, baseline, then build + tear the identical structure twice.
    build_collection_fields(&alloc).bstack_drop(&alloc).unwrap();
    let base = alloc.stack().len().unwrap();
    build_collection_fields(&alloc).bstack_drop(&alloc).unwrap();
    assert_eq!(
        alloc.stack().len().unwrap(),
        base,
        "bulk teardown leaked a collection's block"
    );

    h.bstack_drop(&alloc).unwrap();
    c.bstack_drop(&alloc).unwrap();
}

// A block whose Vec / array ELEMENTS are collections — the container's own
// machinery (`BStackBlockVec`, the fixed-array path) recurses per element.
#[bstack_block]
struct CollectionContainer {
    tag: u32,
    #[bstack_owned]
    deques: Vec<BStackDeque<MacroLeaf>>,
    #[bstack_owned]
    fixed: [BStackDeque<MacroLeaf>; 2],
}

#[test]
fn stdlib_collections_in_vec_and_array_elements() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    let make_dq = |vals: &[u32]| {
        let dq = BStackDeque::<MacroLeaf>::new(&alloc).unwrap();
        for v in vals {
            dq.push_back(&alloc, MacroLeaf::new(&alloc, *v).unwrap())
                .unwrap();
        }
        dq
    };

    let h = CollectionContainer::new(
        &alloc,
        7,
        vec![make_dq(&[1, 2]), make_dq(&[3, 4, 5])],
        [make_dq(&[6]), make_dq(&[7, 8])],
    )
    .unwrap();

    // Read the Vec<..> and [..; 2] back through the accessors.
    let got_vec = h.handle().get_deques(&alloc).unwrap().to_vec().unwrap();
    assert_eq!(got_vec.len(), 2);
    assert_eq!(deque_values(&got_vec[0], stack), vec![1, 2]);
    assert_eq!(deque_values(&got_vec[1], stack), vec![3, 4, 5]);
    let got_arr = h.handle().get_fixed(stack).unwrap();
    assert_eq!(deque_values(&got_arr[0], stack), vec![6]);
    assert_eq!(deque_values(&got_arr[1], stack), vec![7, 8]);

    // Deep clone recurses through each element of each container.
    let c = h.handle().try_clone_in(&alloc).unwrap();
    let cvec = c.handle().get_deques(&alloc).unwrap().to_vec().unwrap();
    let carr = c.handle().get_fixed(stack).unwrap();
    for i in 0..2 {
        assert_ne!(
            cvec[i].range().start(),
            got_vec[i].range().start(),
            "cloned Vec element must be a fresh block"
        );
        assert_ne!(
            carr[i].range().start(),
            got_arr[i].range().start(),
            "cloned array element must be a fresh block"
        );
    }
    assert_eq!(deque_values(&cvec[0], stack), vec![1, 2]);
    assert_eq!(deque_values(&cvec[1], stack), vec![3, 4, 5]);
    assert_eq!(deque_values(&carr[0], stack), vec![6]);
    assert_eq!(deque_values(&carr[1], stack), vec![7, 8]);

    // Independent teardown + full reclamation (a leak here means a nested
    // element was never freed).
    c.bstack_drop(&alloc).unwrap();
    assert_eq!(deque_values(&got_vec[0], stack), vec![1, 2]);
    assert_eq!(deque_values(&got_arr[1], stack), vec![7, 8]);
    assert_teardown_reclaims(&alloc, || {
        CollectionContainer::new(
            &alloc,
            0,
            vec![make_dq(&[1, 2]), make_dq(&[3])],
            [make_dq(&[4]), make_dq(&[5, 6])],
        )
        .unwrap()
    });
}

// A collection as an OWNED ENUM VARIANT payload — the enum's per-variant
// teardown / clone dispatch calls the collection's `__bstack_*` hooks.
#[bstack_enum]
enum CollectionEnum {
    Empty,
    #[bstack_owned]
    Deq(BStackDeque<MacroLeaf>),
    #[bstack_owned]
    Map(BStackHashMap<u32, MacroLeaf>),
}

#[test]
fn stdlib_collection_in_enum_variant() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    // Build a `Deq` variant, read the deque back through the view, clone it
    // (independent), and tear it down — reclaiming deque + ring + elements.
    let dq = BStackDeque::<MacroLeaf>::new(&alloc).unwrap();
    dq.push_back(&alloc, MacroLeaf::new(&alloc, 1).unwrap())
        .unwrap();
    dq.push_back(&alloc, MacroLeaf::new(&alloc, 2).unwrap())
        .unwrap();
    let e = CollectionEnum::new(&alloc, CollectionEnumData::Deq(dq)).unwrap();
    let got = match e.handle().read(&alloc).unwrap() {
        CollectionEnumView::Deq(d) => d,
        _ => panic!("expected Deq variant"),
    };
    assert_eq!(deque_values(&got, stack), vec![1, 2]);

    let c = e.handle().try_clone_in(&alloc).unwrap();
    let cgot = match c.handle().read(&alloc).unwrap() {
        CollectionEnumView::Deq(d) => d,
        _ => panic!("expected Deq variant"),
    };
    assert_ne!(cgot.range().start(), got.range().start());
    assert_eq!(deque_values(&cgot, stack), vec![1, 2]);
    assert!(
        deque_offsets(&got, stack)
            .iter()
            .zip(deque_offsets(&cgot, stack).iter())
            .all(|(a, b)| a != b),
        "cloned enum deque must deep-copy its elements"
    );

    // `bstack_move!` hands the owned collection back as a `BStackOwned`.
    let mv = bstack_move!(e, &alloc).unwrap();
    match mv {
        CollectionEnumData::Deq(dq) => {
            assert_eq!(deque_values(&dq, stack), vec![1, 2]);
            dq.bstack_drop(&alloc).unwrap();
        }
        _ => panic!("expected Deq variant from bstack_move"),
    }
    c.bstack_drop(&alloc).unwrap();

    // A `Map` variant round-trips, reads back, clones independently, and
    // reclaims too.
    let map = BStackHashMap::<u32, MacroLeaf>::new(&alloc).unwrap();
    map.insert(&alloc, 1, MacroLeaf::new(&alloc, 10).unwrap())
        .unwrap();
    map.insert(&alloc, 2, MacroLeaf::new(&alloc, 20).unwrap())
        .unwrap();
    let em = CollectionEnum::new(&alloc, CollectionEnumData::Map(map)).unwrap();
    let mgot = match em.handle().read(&alloc).unwrap() {
        CollectionEnumView::Map(m) => m,
        _ => panic!("expected Map variant"),
    };
    assert_eq!(mgot.len(stack).unwrap(), 2);
    assert_eq!(
        mgot.get(stack, &2)
            .unwrap()
            .unwrap()
            .get_val(stack)
            .unwrap(),
        20
    );
    let mclone = em.handle().try_clone_in(&alloc).unwrap();
    let mc = match mclone.handle().read(&alloc).unwrap() {
        CollectionEnumView::Map(m) => m,
        _ => panic!("expected Map variant from clone"),
    };
    assert_ne!(mc.range().start(), mgot.range().start());
    assert_eq!(mc.len(stack).unwrap(), 2);
    em.bstack_drop(&alloc).unwrap();
    mclone.bstack_drop(&alloc).unwrap();

    assert_teardown_reclaims(&alloc, || {
        let map = BStackHashMap::<u32, MacroLeaf>::new(&alloc).unwrap();
        map.insert(&alloc, 1, MacroLeaf::new(&alloc, 10).unwrap())
            .unwrap();
        CollectionEnum::new(&alloc, CollectionEnumData::Map(map)).unwrap()
    });

    // The `Empty` variant still works alongside the collection variants.
    let empty = CollectionEnum::new(&alloc, CollectionEnumData::Empty).unwrap();
    assert!(matches!(
        empty.handle().read(&alloc).unwrap(),
        CollectionEnumView::Empty
    ));
    empty.bstack_drop(&alloc).unwrap();
}
// composition.
// --------------------------------------------------------------------------

#[bstack_block]
struct GenericCollectionHolder<T> {
    tag: u32,
    #[bstack_owned]
    dq: BStackDeque<T>,
    #[bstack_owned]
    map: Option<BStackHashMap<u32, T>>,
}

#[bstack_block]
struct GenericCollectionVec<T> {
    #[bstack_owned]
    deques: Vec<BStackDeque<T>>,
}

#[test]
fn generic_collection_bound_inference_and_composition() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    // Concrete instantiation with `T = MacroLeaf`.
    let dq = BStackDeque::<MacroLeaf>::new(&alloc).unwrap();
    for v in [1u32, 2, 3] {
        dq.push_back(&alloc, MacroLeaf::new(&alloc, v).unwrap())
            .unwrap();
    }
    let map = BStackHashMap::<u32, MacroLeaf>::new(&alloc).unwrap();
    map.insert(&alloc, 1, MacroLeaf::new(&alloc, 10).unwrap())
        .unwrap();
    let h = GenericCollectionHolder::<MacroLeaf>::new(&alloc, 7, dq, Some(map)).unwrap();

    // Accessors resolve through the generic collection field types.
    assert_eq!(h.handle().get_tag(stack).unwrap(), 7);
    let got_dq = h.handle().get_dq(stack).unwrap();
    assert_eq!(deque_values(&got_dq, stack), vec![1, 2, 3]);
    let got_map = h.handle().get_map(stack).unwrap().expect("Some");
    assert_eq!(got_map.len(stack).unwrap(), 1);
    assert_eq!(
        got_map
            .get(stack, &1)
            .unwrap()
            .unwrap()
            .get_val(stack)
            .unwrap(),
        10
    );

    // Deep clone recurses through the generic collection fields.
    let c = h.handle().try_clone_in(&alloc).unwrap();
    assert_ne!(c.handle().range().start(), h.handle().range().start());
    let cdq = c.handle().get_dq(stack).unwrap();
    assert_ne!(cdq.range().start(), got_dq.range().start());
    assert_eq!(deque_values(&cdq, stack), vec![1, 2, 3]);
    assert_eq!(
        c.handle()
            .get_map(stack)
            .unwrap()
            .unwrap()
            .get(stack, &1)
            .unwrap()
            .unwrap()
            .get_val(stack)
            .unwrap(),
        10
    );

    // Independent teardown of both, then leak-check the whole composition.
    c.bstack_drop(&alloc).unwrap();
    assert_eq!(deque_values(&got_dq, stack), vec![1, 2, 3]);
    h.bstack_drop(&alloc).unwrap();
    assert_teardown_reclaims(&alloc, || {
        let dq = BStackDeque::<MacroLeaf>::new(&alloc).unwrap();
        dq.push_back(&alloc, MacroLeaf::new(&alloc, 5).unwrap())
            .unwrap();
        let map = BStackHashMap::<u32, MacroLeaf>::new(&alloc).unwrap();
        map.insert(&alloc, 1, MacroLeaf::new(&alloc, 50).unwrap())
            .unwrap();
        GenericCollectionHolder::<MacroLeaf>::new(&alloc, 0, dq, Some(map)).unwrap()
    });

    // A second instantiation `T = MacroLeaf` with different leaf values works,
    // and the Vec-of-generic-collection form compiles + tears down too.
    let vh = GenericCollectionVec::<MacroLeaf>::new(
        &alloc,
        vec![
            BStackDeque::<MacroLeaf>::new(&alloc).unwrap(),
            BStackDeque::<MacroLeaf>::new(&alloc).unwrap(),
        ],
    )
    .unwrap();
    assert_eq!(vh.handle().get_deques(&alloc).unwrap().len().unwrap(), 2);
    vh.bstack_drop(&alloc).unwrap();
    assert_teardown_reclaims(&alloc, || {
        GenericCollectionVec::<MacroLeaf>::new(
            &alloc,
            vec![BStackDeque::<MacroLeaf>::new(&alloc).unwrap()],
        )
        .unwrap()
    });
}

// --------------------------------------------------------------------------
// `#[embed]` of a collection: only the collection's fixed descriptor is folded
// inline (the ring / element blocks stay out-of-line). Teardown and move both
// reason purely from the descriptor's absolute offsets, so they should work;
// deep clone folds the child's `OnDisk` in place via
// `__bstack_clone_children_inplace` — which the hand-written collection impls
// do NOT override (they override only `__bstack_drop_children` /
// `__bstack_clone_into`), so the clone may alias the source's out-of-line ring
// and elements instead of deep-copying them.
// --------------------------------------------------------------------------

#[bstack_block]
struct EmbCollectionHolder {
    #[embed]
    dq: BStackDeque<MacroLeaf>,
    tag: u32,
}

#[bstack_block]
struct EmbListHolder {
    #[embed]
    l: BStackLinkedList<MacroLeaf>,
    tag: u32,
}

fn build_emb_collection_holder(
    alloc: &FirstFitBStackAllocator,
) -> BStackOwned<EmbCollectionHolder> {
    let dq = BStackDeque::<MacroLeaf>::new(alloc).unwrap();
    dq.push_back(alloc, MacroLeaf::new(alloc, 1).unwrap())
        .unwrap();
    dq.push_back(alloc, MacroLeaf::new(alloc, 2).unwrap())
        .unwrap();
    EmbCollectionHolder::new(alloc, dq, 7).unwrap()
}

#[test]
fn embed_collection_build_read_teardown() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    // Build: the deque descriptor is folded inline; ring + elements stay out
    // of line and keep their absolute offsets.
    let h = build_emb_collection_holder(&alloc);
    assert_eq!(h.handle().get_tag(stack).unwrap(), 7);

    // Read: the accessor yields a deque handle into the inline region.
    let got = h.handle().get_dq();
    assert_eq!(deque_values(&got, stack), vec![1, 2]);

    // Teardown: frees the ring + both elements *in place* via
    // `__bstack_drop_children`, then the holder shell — no leak.
    h.bstack_drop(&alloc).unwrap();
    assert_teardown_reclaims(&alloc, || build_emb_collection_holder(&alloc));
}

#[test]
fn embed_collection_move_rehomes() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    // Warm the persistent WAL anchor with one full cycle, then baseline (a
    // WAL-backed teardown's anchor is a one-time per-file allocation).
    {
        let (m, _) = bstack_move!(build_emb_collection_holder(&alloc), &alloc).unwrap();
        m.bstack_drop(&alloc).unwrap();
    }
    let base = alloc.stack().len().unwrap();

    // bstack_move! re-homes the descriptor to a fresh standalone deque block;
    // the parent shell is freed, so nothing is double-freed later.
    let (moved, tag) = bstack_move!(build_emb_collection_holder(&alloc), &alloc).unwrap();
    assert_eq!(tag, 7);
    assert_eq!(deque_values(&moved, stack), vec![1, 2]);
    // The re-homed deque owns its ring + elements; dropping it reclaims all of
    // them (build -> move -> drop the moved child must return to baseline).
    moved.bstack_drop(&alloc).unwrap();
    assert_eq!(
        alloc.stack().len().unwrap(),
        base,
        "move re-homed the embedded deque but leaked its ring or elements"
    );
}

#[test]
fn embed_collection_clone_is_independent() {
    let tmp = TempStack::new();
    let alloc = tmp.allocator();
    let stack = alloc.stack();

    let h = build_emb_collection_holder(&alloc);
    let got = h.handle().get_dq();

    // A deep clone of the holder must carry an INDEPENDENT copy of the embedded
    // deque — a fresh ring and fresh element blocks, not aliases of the
    // original's out-of-line storage.
    let c = h.handle().try_clone_in(&alloc).unwrap();
    let cdq = c.handle().get_dq();
    assert_ne!(
        c.handle().range().start(),
        h.handle().range().start(),
        "cloned holder must be a fresh block"
    );
    assert_ne!(
        cdq.range().start(),
        got.range().start(),
        "embedded deque must not alias the source descriptor"
    );
    assert_eq!(deque_values(&cdq, stack), vec![1, 2]);
    assert!(
        deque_offsets(&got, stack)
            .iter()
            .zip(deque_offsets(&cdq, stack).iter())
            .all(|(a, b)| a != b),
        "embedded collection clone must deep-copy its ring elements"
    );

    // Tearing the clone down must not touch the original's ring / elements.
    c.bstack_drop(&alloc).unwrap();
    assert_eq!(deque_values(&got, stack), vec![1, 2]);
    h.bstack_drop(&alloc).unwrap();
}

#[test]
fn embed_list_clone_is_independent() {
    // A second embedded collection (nodes AND element blocks out-of-line) confirms the
    // clone fix generalizes beyond deque: deep-cloning an embedded collection deep-copies
    // its out-of-line storage, so dropping the source and then the clone returns exactly
    // to baseline — an aliasing (verbatim-descriptor) clone would double-free the shared
    // nodes/elements on the second drop.
    let tmp = TempStack::new();
    let alloc = tmp.allocator();

    // Warm the WAL anchor for a stable baseline.
    {
        let g = MacroLeaf::new(&alloc, 0).unwrap();
        g.bstack_drop(&alloc).unwrap();
    }
    let base = alloc.stack().len().unwrap();

    let h = {
        let l = BStackLinkedList::<MacroLeaf>::new(&alloc).unwrap();
        l.push_back(&alloc, MacroLeaf::new(&alloc, 1).unwrap())
            .unwrap();
        l.push_back(&alloc, MacroLeaf::new(&alloc, 2).unwrap())
            .unwrap();
        EmbListHolder::new(&alloc, l, 9).unwrap()
    };
    let c = h.handle().try_clone_in(&alloc).unwrap();

    // Independent copies: dropping both returns to baseline (aliasing ⇒ double-free).
    h.bstack_drop(&alloc).unwrap();
    c.bstack_drop(&alloc).unwrap();
    assert_eq!(
        alloc.stack().len().unwrap(),
        base,
        "embedded list clone aliased the source's out-of-line nodes/elements"
    );
}
