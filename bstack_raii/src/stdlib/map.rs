//! [`BStackHashMap<K, V>`]: an owned open-addressing hash map.
//!
//! The on-disk answer to [`std::collections::HashMap`], and the way to look a
//! value up by key without a linear scan. Keys are **`Pod`** (`K: Pod`), stored
//! inline in the bucket and hashed by their raw bytes; values are blocks
//! (`V: BStackBlock`) the map owns, referenced by a single `u64` per bucket.
//!
//! # Layout
//!
//! The fixed handle block ([`MapOnDisk`]) holds a pointer to a **contiguous
//! bucket block**, the bucket count `cap` (a power of two), the live-entry count
//! `len`, and `used` (occupied + tombstone slots, which drives growth). Each
//! bucket is `state: u64` (`EMPTY` / `OCCUPIED` / `TOMBSTONE`), then the inline
//! key `K`, then a `u64` value reference — a stride of `16 + size_of::<K>()`
//! bytes. Probing is linear (`cap` a power of two, `mask = cap - 1`), so the
//! whole probe sequence is a contiguous scan, not a pointer chase.
//!
//! # Atomicity
//!
//! Each `insert` / `remove` is atomic per call and external-lock-free: the entire
//! probe *and* the resulting bucket + metadata writes run inside one
//! [`bstack::BStack::inplace_gen`] (see [`probe_commit`]), so a concurrent writer
//! never observes a torn table and a crash never corrupts it. **Growth / rehash**
//! is likewise atomic: a bigger bucket block is allocated first (an orphan), then
//! one `inplace_gen` snapshots every live bucket, rebuilds the table into the new
//! block (dropping tombstones), and swaps the descriptor — all under bstack's
//! write lock, composing with concurrent inserts (an insert that can't place
//! grows and retries). A crash mid-rehash leaks a bucket block, never tears the
//! map.
//!
//! `get` / `contains_key` are plain probes (no write lock); each bucket read is
//! atomic, but the probe is not linearized against a concurrent mutation, so a
//! borrowed value handle it returns is valid only while that entry is not removed.

use core::cell::Cell;
use core::marker::PhantomData;
use core::mem::size_of;
use std::io;

use bstack::{BStack, BStackGenOp, BStackOwnedSliceAllocator, BStackRange};
use bytemuck::{Pod, Zeroable};

use super::util::{alloc_image, get_u64};
use crate::block::{BStackBlock, BStackCast};
use crate::clone::{ClonePlan, TryCloneIn};
use crate::layout::{BlockHeader, EightCC, HEADER_SIZE};
use crate::owned::BStackOwned;
use crate::teardown::{AutoDrop, BStackDrop, dealloc_range};

/// The on-disk image of a [`BStackHashMap`]: header, bucket-block pointer (`0` =
/// none), bucket count `cap`, live-entry count `len`, and `used` (occupied +
/// tombstone). `#[repr(C)]`, only `u64` fields after the header — non-generic.
#[repr(C)]
#[derive(Clone, Copy, Pod, Zeroable)]
pub struct MapOnDisk {
    /// The 16-byte block header (size + type tag).
    pub header: BlockHeader,
    /// Offset of the bucket block, or `0` when unallocated.
    pub table: u64,
    /// Number of buckets (a power of two).
    pub cap: u64,
    /// Number of live entries.
    pub len: u64,
    /// Occupied + tombstone slots (drives growth).
    pub used: u64,
}

// Field offsets within the handle block. `table..used` are contiguous so all
// four load in one 32-byte read.
const TABLE_OFF: u64 = HEADER_SIZE; // 16
const CAP_OFF: u64 = HEADER_SIZE + 8; // 24
const LEN_OFF: u64 = HEADER_SIZE + 16; // 32
const USED_OFF: u64 = HEADER_SIZE + 24; // 40

const MAP_SIZE: u64 = size_of::<MapOnDisk>() as u64;
/// Bucket count of a freshly grown empty table (a power of two).
const MIN_CAP: u64 = 4;

// Bucket states.
const EMPTY: u64 = 0;
const OCCUPIED: u64 = 1;
const TOMBSTONE: u64 = 2;

/// Read a little-endian `u64` from the first 8 bytes of `b`.
fn u64le(b: &[u8]) -> u64 {
    u64::from_le_bytes(b[..8].try_into().unwrap())
}

/// 64-bit FNV-1a over `bytes`. Deterministic (so it is stable on disk).
fn fnv1a(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in bytes {
        h ^= b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

/// A snapshot of the four handle metadata fields, read inside a generator.
struct Meta {
    table: u64,
    cap: u64,
    len: u64,
    used: u64,
}

/// A probe step returned by the `inspect` closure of [`probe_commit`].
enum ProbeStep {
    /// This bucket isn't the target — keep probing.
    Continue,
    /// Stop here and commit these writes (empty = commit nothing).
    Stop(Vec<(u64, Vec<u8>)>),
}

/// Run an atomic, external-lock-free probe over the bucket table under one
/// [`BStack::inplace_gen`].
///
/// Reads the handle metadata, then linearly probes buckets from `hash & (cap-1)`,
/// reading the full `stride`-byte bucket each step and handing it to `inspect`.
/// The first `inspect` returning [`ProbeStep::Stop`] commits its writes and ends;
/// if all `cap` buckets are probed without a stop, `exhausted` produces the final
/// writes. Every read and write rides the one generator, so the probe sees a
/// consistent snapshot and the writes land as one crash-atomic batch.
fn probe_commit<A, I, E>(
    allocator: &A,
    handle: u64,
    stride: u64,
    hash: u64,
    mut inspect: I,
    exhausted: E,
) -> io::Result<()>
where
    A: BStackOwnedSliceAllocator,
    I: FnMut(&Meta, u64, &[u8]) -> ProbeStep,
    E: FnOnce(&Meta) -> Vec<(u64, Vec<u8>)>,
{
    let mut meta_buf = [0u8; 32];
    let mut bucket_buf = vec![0u8; stride as usize];
    let mut writes: Vec<(u64, Vec<u8>)> = Vec::new();

    let mut meta_issued = false;
    let mut meta: Option<Meta> = None;
    let mut mask = 0u64;
    let mut cur = 0u64;
    let mut idx_at_read = 0u64;
    let mut probe_pending = false;
    let mut probed = 0u64;
    let mut decided = false;
    let mut exhausted = Some(exhausted);
    let mut w = 0usize;

    allocator.stack().inplace_gen(|_feedback| {
        // 1. Read the 32-byte metadata block.
        if !meta_issued {
            meta_issued = true;
            // SAFETY: `meta_buf` outlives the call; used by this one read.
            let b: &mut [u8] = unsafe { core::mem::transmute::<&mut [u8], _>(&mut meta_buf[..]) };
            return Some(BStackGenOp::Read {
                offset: handle + TABLE_OFF,
                buf: b,
            });
        }
        // 2. Parse it once.
        if meta.is_none() {
            let m = Meta {
                table: u64le(&meta_buf[0..8]),
                cap: u64le(&meta_buf[8..16]),
                len: u64le(&meta_buf[16..24]),
                used: u64le(&meta_buf[24..32]),
            };
            mask = m.cap.wrapping_sub(1);
            cur = if m.cap == 0 { 0 } else { hash & mask };
            meta = Some(m);
        }
        let m = meta.as_ref().unwrap();

        // 3a. Inspect a completed bucket read.
        if probe_pending {
            probe_pending = false;
            if let ProbeStep::Stop(ws) = inspect(m, idx_at_read, &bucket_buf) {
                writes = ws;
                decided = true;
            }
        }
        // 3b. Issue the next probe, or finish by exhaustion.
        if !decided {
            if m.cap == 0 || probed >= m.cap {
                writes = (exhausted.take().unwrap())(m);
                decided = true;
            } else {
                idx_at_read = cur;
                probe_pending = true;
                probed += 1;
                cur = (cur + 1) & mask;
                let off = m.table + idx_at_read * stride;
                // SAFETY: `bucket_buf` outlives the call; each read completes
                // (and is inspected) before the next is issued.
                let b: &mut [u8] =
                    unsafe { core::mem::transmute::<&mut [u8], _>(&mut bucket_buf[..]) };
                return Some(BStackGenOp::Read { offset: off, buf: b });
            }
        }
        // 4. Commit the chosen writes together.
        if w < writes.len() {
            let i = w;
            w += 1;
            let (off, ref bytes) = writes[i];
            // SAFETY: `writes` outlives the call and is not mutated after this point.
            let d: &[u8] = unsafe { core::mem::transmute::<&[u8], _>(bytes.as_slice()) };
            return Some(BStackGenOp::Write { offset: off, data: d });
        }
        None
    })
}

/// Build the writes that place a *new* entry (state, key, value) at bucket
/// `target`, bumping `len` (and `used` when the slot was previously `EMPTY`).
fn new_bucket_writes(
    handle: u64,
    stride: u64,
    ksz: usize,
    m: &Meta,
    target: u64,
    slot_was_empty: bool,
    key_bytes: &[u8],
    val_ref: u64,
) -> Vec<(u64, Vec<u8>)> {
    let mut img = Vec::with_capacity(16 + ksz);
    img.extend_from_slice(&OCCUPIED.to_le_bytes());
    img.extend_from_slice(key_bytes);
    img.extend_from_slice(&val_ref.to_le_bytes());

    let mut w = vec![
        (m.table + target * stride, img),
        (handle + LEN_OFF, (m.len + 1).to_le_bytes().to_vec()),
    ];
    if slot_was_empty {
        w.push((handle + USED_OFF, (m.used + 1).to_le_bytes().to_vec()));
    }
    w
}

/// An owned open-addressing hash map from a `Pod` key to a block value.
///
/// A typed handle (a newtype over a [`BStackRange`]); [`new`](Self::new) returns
/// a bare [`BStackOwned<BStackHashMap<K, V>>`] that frees nothing on scope exit —
/// free it with [`bstack_drop`](BStackDrop::bstack_drop) or wrap it
/// ([`AutoDrop`] / [`crate::BStackCow`]).
///
/// The map owns its values' blocks: [`insert`](Self::insert) takes a
/// [`BStackOwned<V>`], [`remove`](Self::remove) hands one back (as does an
/// overwriting `insert`, returning the replaced value), and teardown recursively
/// frees every value and the bucket block.
pub struct BStackHashMap<K: Pod, V: BStackBlock> {
    range: BStackRange,
    _marker: PhantomData<fn() -> (K, V)>,
}

impl<K: Pod, V: BStackBlock> BStackHashMap<K, V> {
    /// Bytes of an inline key.
    fn ksize() -> usize {
        size_of::<K>()
    }

    /// Bytes of one bucket: `state (8) + key (ksize) + value ref (8)`.
    fn stride() -> u64 {
        16 + Self::ksize() as u64
    }

    /// On-disk size of one `V` value block.
    fn value_size() -> u64 {
        size_of::<<V as BStackBlock>::OnDisk>() as u64
    }

    /// A `V`-value handle over the block at `off`.
    fn value_at(off: u64) -> V {
        <V as BStackBlock>::from_range(BStackRange::new(off, Self::value_size()))
    }

    /// Allocate an empty map (no bucket block until the first insert).
    pub fn new<A: BStackOwnedSliceAllocator>(allocator: &A) -> io::Result<BStackOwned<Self>> {
        let od = MapOnDisk {
            header: BlockHeader {
                size: MAP_SIZE,
                tag: Self::eightcc(),
            },
            table: 0,
            cap: 0,
            len: 0,
            used: 0,
        };
        let range = alloc_image(allocator, bytemuck::bytes_of(&od))?;
        // SAFETY: a freshly allocated block owned by no other handle.
        Ok(unsafe { BStackOwned::from_raw(Self::from_range(range)) })
    }

    /// Number of live entries.
    pub fn len(&self, stack: &BStack) -> io::Result<u64> {
        get_u64(stack, self.range.start() + LEN_OFF)
    }

    /// Whether the map has no entries.
    pub fn is_empty(&self, stack: &BStack) -> io::Result<bool> {
        Ok(self.len(stack)? == 0)
    }

    /// Insert `key -> value`, taking ownership of the value block. Returns the
    /// previously-mapped value (owned) if `key` was already present, else `None`.
    ///
    /// Atomic and external-lock-free: grows the table first if the load factor
    /// would be exceeded, then probes and commits the bucket + metadata writes in
    /// one [`probe_commit`].
    pub fn insert<A: BStackOwnedSliceAllocator>(
        &self,
        allocator: &A,
        key: K,
        value: BStackOwned<V>,
    ) -> io::Result<Option<BStackOwned<V>>> {
        let handle = self.range.start();
        let stride = Self::stride();
        let ksz = Self::ksize();
        let key_bytes = bytemuck::bytes_of(&key).to_vec();
        let val_ref = value.into_inner().range().start();
        let hash = fnv1a(&key_bytes);

        loop {
            // Proactively keep the load factor under 3/4 (also clears tombstones).
            let cap = get_u64(allocator.stack(), handle + CAP_OFF)?;
            let used = get_u64(allocator.stack(), handle + USED_OFF)?;
            if cap == 0 || (used + 1) * 4 > cap * 3 {
                self.grow(allocator)?;
                continue;
            }

            let first_tomb: Cell<Option<u64>> = Cell::new(None);
            let is_new = Cell::new(false);
            let need_grow = Cell::new(false);
            let old_value = Cell::new(0u64);

            probe_commit(
                allocator,
                handle,
                stride,
                hash,
                |m, idx, buf| {
                    let state = u64le(&buf[0..8]);
                    if state == EMPTY {
                        let target = first_tomb.get().unwrap_or(idx);
                        let slot_was_empty = first_tomb.get().is_none();
                        is_new.set(true);
                        ProbeStep::Stop(new_bucket_writes(
                            handle,
                            stride,
                            ksz,
                            m,
                            target,
                            slot_was_empty,
                            &key_bytes,
                            val_ref,
                        ))
                    } else if state == OCCUPIED && buf[8..8 + ksz] == key_bytes[..] {
                        // Overwrite: replace the value ref, hand back the old one.
                        old_value.set(u64le(&buf[8 + ksz..8 + ksz + 8]));
                        is_new.set(false);
                        let value_off = m.table + idx * stride + 8 + ksz as u64;
                        ProbeStep::Stop(vec![(value_off, val_ref.to_le_bytes().to_vec())])
                    } else {
                        if state == TOMBSTONE && first_tomb.get().is_none() {
                            first_tomb.set(Some(idx));
                        }
                        ProbeStep::Continue
                    }
                },
                |m| {
                    if let Some(t) = first_tomb.get() {
                        is_new.set(true);
                        new_bucket_writes(handle, stride, ksz, m, t, false, &key_bytes, val_ref)
                    } else {
                        need_grow.set(true);
                        Vec::new()
                    }
                },
            )?;

            if need_grow.get() {
                self.grow(allocator)?;
                continue;
            }
            return if is_new.get() {
                Ok(None)
            } else {
                // SAFETY: the replaced value block's ownership transfers to the caller.
                Ok(Some(unsafe { BStackOwned::from_raw(Self::value_at(old_value.get())) }))
            };
        }
    }

    /// Remove `key`, returning its value (owned) if present, else `None`. The
    /// bucket becomes a tombstone; the value block's ownership transfers out.
    pub fn remove<A: BStackOwnedSliceAllocator>(
        &self,
        allocator: &A,
        key: &K,
    ) -> io::Result<Option<BStackOwned<V>>> {
        let handle = self.range.start();
        let stride = Self::stride();
        let ksz = Self::ksize();
        let key_bytes = bytemuck::bytes_of(key).to_vec();
        let hash = fnv1a(&key_bytes);

        let found = Cell::new(false);
        let old_value = Cell::new(0u64);

        probe_commit(
            allocator,
            handle,
            stride,
            hash,
            |m, idx, buf| {
                let state = u64le(&buf[0..8]);
                if state == EMPTY {
                    ProbeStep::Stop(Vec::new()) // absent: commit nothing
                } else if state == OCCUPIED && buf[8..8 + ksz] == key_bytes[..] {
                    found.set(true);
                    old_value.set(u64le(&buf[8 + ksz..8 + ksz + 8]));
                    ProbeStep::Stop(vec![
                        (m.table + idx * stride, TOMBSTONE.to_le_bytes().to_vec()),
                        (handle + LEN_OFF, (m.len - 1).to_le_bytes().to_vec()),
                    ])
                } else {
                    ProbeStep::Continue
                }
            },
            |_m| Vec::new(),
        )?;

        if found.get() {
            // SAFETY: the removed value block's ownership transfers to the caller.
            Ok(Some(unsafe { BStackOwned::from_raw(Self::value_at(old_value.get())) }))
        } else {
            Ok(None)
        }
    }

    /// A **borrowed** handle to the value mapped by `key` (no ownership; valid
    /// only while the entry is not removed), or `None` if absent.
    ///
    /// A plain probe: correct with external synchronization or a single writer,
    /// but not linearized against a concurrent mutation.
    pub fn get(&self, stack: &BStack, key: &K) -> io::Result<Option<V>> {
        let handle = self.range.start();
        let stride = Self::stride();
        let ksz = Self::ksize();
        let table = get_u64(stack, handle + TABLE_OFF)?;
        let cap = get_u64(stack, handle + CAP_OFF)?;
        if cap == 0 {
            return Ok(None);
        }
        let key_bytes = bytemuck::bytes_of(key);
        let mask = cap - 1;
        let mut idx = fnv1a(key_bytes) & mask;
        let mut kb = vec![0u8; ksz];
        for _ in 0..cap {
            let bucket = table + idx * stride;
            let state = get_u64(stack, bucket)?;
            if state == EMPTY {
                return Ok(None);
            }
            if state == OCCUPIED {
                stack.get_into(bucket + 8, &mut kb)?;
                if kb == key_bytes {
                    let vref = get_u64(stack, bucket + 8 + ksz as u64)?;
                    return Ok(Some(Self::value_at(vref)));
                }
            }
            idx = (idx + 1) & mask;
        }
        Ok(None)
    }

    /// Whether `key` is present.
    pub fn contains_key(&self, stack: &BStack, key: &K) -> io::Result<bool> {
        Ok(self.get(stack, key)?.is_some())
    }

    /// Grow the table to at least double its capacity, rehashing every live entry
    /// (and dropping tombstones) atomically. A no-op (beyond a freed spare block)
    /// if another thread already grew it.
    fn grow<A: BStackOwnedSliceAllocator>(&self, allocator: &A) -> io::Result<()> {
        let handle = self.range.start();
        let stride = Self::stride();
        let ksz = Self::ksize();
        let cap0 = get_u64(allocator.stack(), handle + CAP_OFF)?;
        let newcap = if cap0 == 0 { MIN_CAP } else { cap0 * 2 };
        // Allocate the new bucket block up front (an orphan until the swap).
        let newtable = allocator.alloc(newcap * stride)?.as_range().start();

        let mut meta_buf = [0u8; 32];
        let mut old_buf = vec![0u8; (cap0 * stride) as usize];
        let mut new_image: Vec<u8> = Vec::new();
        let grown = Cell::new(false);
        let old_table = Cell::new(0u64);
        let old_cap = Cell::new(0u64);

        let mut meta_issued = false;
        let mut meta: Option<Meta> = None;
        let mut abort = false;
        let mut read_i = 0u64;
        let mut built = false;
        let mut writes: Vec<(u64, Vec<u8>)> = Vec::new();
        let mut w = 0usize;

        allocator.stack().inplace_gen(|_feedback| {
            if !meta_issued {
                meta_issued = true;
                // SAFETY: `meta_buf` outlives the call.
                let b: &mut [u8] =
                    unsafe { core::mem::transmute::<&mut [u8], _>(&mut meta_buf[..]) };
                return Some(BStackGenOp::Read {
                    offset: handle + TABLE_OFF,
                    buf: b,
                });
            }
            if meta.is_none() {
                let m = Meta {
                    table: u64le(&meta_buf[0..8]),
                    cap: u64le(&meta_buf[8..16]),
                    len: u64le(&meta_buf[16..24]),
                    used: u64le(&meta_buf[24..32]),
                };
                // Abort if someone already grew to at least this size.
                if newcap <= m.cap {
                    abort = true;
                }
                meta = Some(m);
            }
            if abort {
                return None; // commit nothing
            }
            let m = meta.as_ref().unwrap();

            // Snapshot every old bucket.
            if read_i < m.cap {
                let i = read_i;
                read_i += 1;
                let lo = (i * stride) as usize;
                let hi = lo + stride as usize;
                // SAFETY: `old_buf` outlives the call; each slice read once.
                let b: &mut [u8] =
                    unsafe { core::mem::transmute::<&mut [u8], _>(&mut old_buf[lo..hi]) };
                return Some(BStackGenOp::Read {
                    offset: m.table + i * stride,
                    buf: b,
                });
            }

            // Rebuild the table into the new block (dropping tombstones).
            if !built {
                built = true;
                grown.set(true);
                old_table.set(m.table);
                old_cap.set(m.cap);

                new_image = vec![0u8; (newcap * stride) as usize]; // all EMPTY
                let newmask = newcap - 1;
                for j in 0..m.cap {
                    let lo = (j * stride) as usize;
                    if u64le(&old_buf[lo..lo + 8]) != OCCUPIED {
                        continue;
                    }
                    let kb = &old_buf[lo + 8..lo + 8 + ksz];
                    let vref = &old_buf[lo + 8 + ksz..lo + 16 + ksz];
                    let mut idx = fnv1a(kb) & newmask;
                    loop {
                        let nlo = (idx * stride) as usize;
                        if u64le(&new_image[nlo..nlo + 8]) == EMPTY {
                            new_image[nlo..nlo + 8].copy_from_slice(&OCCUPIED.to_le_bytes());
                            new_image[nlo + 8..nlo + 8 + ksz].copy_from_slice(kb);
                            new_image[nlo + 8 + ksz..nlo + 16 + ksz].copy_from_slice(vref);
                            break;
                        }
                        idx = (idx + 1) & newmask;
                    }
                }
                writes.push((newtable, std::mem::take(&mut new_image)));
                writes.push((handle + TABLE_OFF, newtable.to_le_bytes().to_vec()));
                writes.push((handle + CAP_OFF, newcap.to_le_bytes().to_vec()));
                // Tombstones dropped: used == len now.
                writes.push((handle + USED_OFF, m.len.to_le_bytes().to_vec()));
            }
            if w < writes.len() {
                let i = w;
                w += 1;
                let (off, ref bytes) = writes[i];
                // SAFETY: `writes` outlives the call and is not mutated after build.
                let d: &[u8] = unsafe { core::mem::transmute::<&[u8], _>(bytes.as_slice()) };
                return Some(BStackGenOp::Write { offset: off, data: d });
            }
            None
        })?;

        if grown.get() {
            if old_cap.get() > 0 {
                // SAFETY: the descriptor no longer points at the old table.
                let _ = unsafe {
                    dealloc_range(
                        allocator,
                        BStackRange::new(old_table.get(), old_cap.get() * stride),
                    )
                };
            }
        } else {
            // SAFETY: `newtable` was never linked into the descriptor.
            let _ = unsafe { dealloc_range(allocator, BStackRange::new(newtable, newcap * stride)) };
        }
        Ok(())
    }

    /// Attach an allocator to make an auto-freeing [`AutoDrop`] guard.
    pub fn auto<A: BStackOwnedSliceAllocator>(self, allocator: &A) -> AutoDrop<'_, Self, A> {
        // SAFETY: sole ownership was asserted when the map was created.
        unsafe { AutoDrop::from_raw(self, allocator) }
    }
}

impl<K: Pod, V: BStackBlock> BStackCast for BStackHashMap<K, V> {
    /// A `"Map"` prefix over hash bytes perturbed by the key size and the value
    /// type's tag, so maps of different key/value types never share a
    /// discriminant despite the identical handle layout.
    fn eightcc() -> EightCC {
        const BASE: EightCC = EightCC::new([b'M', b'a', b'p', 0x80, 0x81, 0x82, 0x83, 0x84]);
        BASE.mix(EightCC::new((size_of::<K>() as u64).to_le_bytes()))
            .mix(<V as BStackCast>::eightcc())
    }
}

impl<K: Pod, V: BStackBlock> BStackBlock for BStackHashMap<K, V> {
    type OnDisk = MapOnDisk;

    fn from_range(range: BStackRange) -> Self {
        BStackHashMap {
            range,
            _marker: PhantomData,
        }
    }

    fn range(&self) -> BStackRange {
        self.range
    }

    /// Recursively free every value block and the bucket block, **without**
    /// freeing the handle block itself.
    fn __bstack_drop_children<A: BStackOwnedSliceAllocator>(
        range: BStackRange,
        allocator: &A,
    ) -> io::Result<()> {
        let stride = Self::stride();
        let ksz = Self::ksize() as u64;
        let handle = range.start();
        let table = get_u64(allocator.stack(), handle + TABLE_OFF)?;
        let cap = get_u64(allocator.stack(), handle + CAP_OFF)?;
        for j in 0..cap {
            let bucket = table + j * stride;
            if get_u64(allocator.stack(), bucket)? == OCCUPIED {
                let vref = get_u64(allocator.stack(), bucket + 8 + ksz)?;
                if vref != 0 {
                    // SAFETY: the map solely owns each value block.
                    let owned = unsafe { BStackOwned::from_raw(Self::value_at(vref)) };
                    owned.bstack_drop(allocator)?;
                }
            }
        }
        if table != 0 {
            // SAFETY: the map solely owns its bucket block.
            unsafe { dealloc_range(allocator, BStackRange::new(table, cap * stride))? };
        }
        Ok(())
    }

    /// Deep-clone the map into `plan`: copy the bucket block verbatim (keeping
    /// every key's position), deep-clone each occupied value (via `V`'s clone
    /// hook) and swap in the clone's ref; stage the handle — all in the parent
    /// plan's single atomic commit.
    fn __bstack_clone_into<A: BStackOwnedSliceAllocator>(
        &self,
        allocator: &A,
        plan: &mut ClonePlan,
    ) -> io::Result<BStackRange> {
        let stride = Self::stride();
        let ksz = Self::ksize();
        let handle = self.range.start();
        let table = get_u64(allocator.stack(), handle + TABLE_OFF)?;
        let cap = get_u64(allocator.stack(), handle + CAP_OFF)?;
        let len = get_u64(allocator.stack(), handle + LEN_OFF)?;
        let used = get_u64(allocator.stack(), handle + USED_OFF)?;

        let (new_table, new_cap, new_used) = if cap == 0 {
            (0, 0, 0)
        } else {
            // Copy the whole bucket block, then deep-clone the occupied values.
            let mut image = vec![0u8; (cap * stride) as usize];
            allocator.stack().get_into(table, &mut image)?;
            for j in 0..cap as usize {
                let lo = j * stride as usize;
                if u64le(&image[lo..lo + 8]) != OCCUPIED {
                    continue;
                }
                let vref = u64le(&image[lo + 8 + ksz..lo + 16 + ksz]);
                let cloned = Self::value_at(vref)
                    .__bstack_clone_into(allocator, plan)?
                    .start();
                image[lo + 8 + ksz..lo + 16 + ksz].copy_from_slice(&cloned.to_le_bytes());
            }
            let dst = plan.alloc_raw(allocator, cap * stride)?;
            plan.write(dst.start(), image);
            (dst.start(), cap, used)
        };

        let handle_dst = plan.alloc_raw(allocator, MAP_SIZE)?;
        let od = MapOnDisk {
            header: BlockHeader {
                size: MAP_SIZE,
                tag: Self::eightcc(),
            },
            table: new_table,
            cap: new_cap,
            len,
            used: new_used,
        };
        plan.write(handle_dst.start(), bytemuck::bytes_of(&od).to_vec());
        Ok(handle_dst)
    }
}

impl<K: Pod, V: BStackBlock> BStackDrop for BStackHashMap<K, V> {
    fn bstack_drop<A: BStackOwnedSliceAllocator>(self, allocator: &A) -> io::Result<()> {
        Self::__bstack_drop_children(self.range, allocator)?;
        // SAFETY: sole ownership of the handle block was asserted at construction.
        unsafe { dealloc_range(allocator, self.range) }
    }
}

impl<K: Pod, V: BStackBlock> TryCloneIn for BStackHashMap<K, V> {
    fn try_clone_in<A: BStackOwnedSliceAllocator>(
        &self,
        allocator: &A,
    ) -> io::Result<BStackOwned<Self>> {
        let mut plan = ClonePlan::new();
        let dst = match self.__bstack_clone_into(allocator, &mut plan) {
            Ok(range) => range,
            Err(e) => {
                plan.rollback(allocator);
                return Err(e);
            }
        };
        plan.commit(allocator)?;
        // SAFETY: `dst` is a fresh block owned by nobody else.
        Ok(unsafe { BStackOwned::from_raw(Self::from_range(dst)) })
    }
}
