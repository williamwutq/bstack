//! [`BStackHashSet<K>`]: an owned open-addressing set of `Pod` keys, with an
//! embedded counting Bloom filter front.
//!
//! The set analogue of [`crate::BStackHashMap`] — the same linear-probe table and
//! [`probe_commit`] engine, but each bucket is just `state + key` (no value
//! column), so it is denser and never touches owned value blocks. Keys are `Pod`,
//! hashed by their raw bytes.
//!
//! # Bloom filter in front
//!
//! Every set embeds a [`crate::BStackCountingBloomFilter`] as a cheap fast-reject
//! guard: [`contains`](BStackHashSet::contains) checks the filter first and skips
//! the table probe entirely for keys it reports absent. The filter is maintained
//! as a strict **over-approximation** of the table (every key in the table is in
//! the filter), so a bloom "absent" is authoritative and there are never false
//! negatives. Consistency across the two blocks is by ordering:
//!
//! * `insert` adds to the filter *before* the table (and undoes the filter bump
//!   if the key turned out to be a duplicate);
//! * `remove` deletes from the table *before* decrementing the filter, and only
//!   decrements for a key that was actually present.
//!
//! A crash between the two steps can only leave the filter *more* permissive
//! (extra false positives), never less — the table stays the source of truth.
//! Because a set op spans two blocks it is **not** a single atomic commit; treat
//! the set as single-writer for the filter's accuracy (a concurrent writer may
//! over-count the filter — more false positives, never a false negative). The
//! filter is fixed-size, so a set far larger than its configured capacity keeps
//! working, just with a higher (still sound) false-positive rate.

use core::cell::Cell;
use core::marker::PhantomData;
use core::mem::size_of;
use std::io;

use bstack::{BStack, BStackGenOp, BStackOwnedSliceAllocator, BStackRange};
use crate::wal::BStackWalAnchor;
use bytemuck::{Pod, Zeroable};

use super::bloom::{BStackCountingBloomFilter, BloomOnDisk};
use super::hash::fnv1a;
use super::util::{Meta, ProbeStep, Scratch, alloc_image, probe_commit, read_fields, read_u64};
use crate::block::{BStackBlock, BStackCast};
use crate::clone::{ClonePlan, TryCloneIn};
use crate::layout::{BlockHeader, EightCC, HEADER_SIZE, get_u64};
use crate::owned::BStackOwned;
use crate::teardown::{AutoDrop, BStackDrop, dealloc_range};

/// The on-disk image of a [`BStackHashSet`]: header, bucket-block pointer,
/// bucket count `cap`, key count `len`, `used` (occupied + tombstone), and the
/// embedded Bloom filter's handle offset. `#[repr(C)]`, `u64` fields only.
#[repr(C)]
#[derive(Clone, Copy, Pod, Zeroable)]
pub struct HashSetOnDisk {
    /// The 16-byte block header (size + type tag).
    pub header: BlockHeader,
    /// Offset of the bucket block, or `0` when unallocated.
    pub table: u64,
    /// Number of buckets (a power of two).
    pub cap: u64,
    /// Number of live keys.
    pub len: u64,
    /// Occupied + tombstone slots (drives growth).
    pub used: u64,
    /// Offset of the embedded counting Bloom filter's handle block.
    pub bloom: u64,
}

const TABLE_OFF: u64 = HEADER_SIZE; // 16
const CAP_OFF: u64 = HEADER_SIZE + 8; // 24
const LEN_OFF: u64 = HEADER_SIZE + 16; // 32
const USED_OFF: u64 = HEADER_SIZE + 24; // 40
const BLOOM_OFF: u64 = HEADER_SIZE + 32; // 48
const SET_SIZE: u64 = size_of::<HashSetOnDisk>() as u64;
const BLOOM_SIZE: u64 = size_of::<BloomOnDisk>() as u64;
const MIN_CAP: u64 = 4;

const EMPTY: u64 = 0;
const OCCUPIED: u64 = 1;
const TOMBSTONE: u64 = 2;

// Default Bloom sizing when the caller does not specify one.
const DEFAULT_ITEMS: u64 = 1024;
const DEFAULT_FP: f64 = 0.01;

/// Build the writes that place a key at bucket `target`, bumping `len` (and
/// `used` when the slot was previously `EMPTY`).
fn place_writes(
    handle: u64,
    stride: u64,
    m: &Meta,
    target: u64,
    slot_was_empty: bool,
    key_bytes: &[u8],
) -> Vec<(u64, Vec<u8>)> {
    let mut img = Vec::with_capacity(8 + key_bytes.len());
    img.extend_from_slice(&OCCUPIED.to_le_bytes());
    img.extend_from_slice(key_bytes);
    let mut w = vec![
        (m.table + target * stride, img),
        (handle + LEN_OFF, (m.len + 1).to_le_bytes().to_vec()),
    ];
    if slot_was_empty {
        w.push((handle + USED_OFF, (m.used + 1).to_le_bytes().to_vec()));
    }
    w
}

/// An owned open-addressing set of `Pod` keys with an embedded Bloom filter.
pub struct BStackHashSet<K: Pod> {
    range: BStackRange,
    _marker: PhantomData<fn() -> K>,
}

impl<K: Pod> BStackHashSet<K> {
    fn ksize() -> usize {
        size_of::<K>()
    }
    fn stride() -> u64 {
        8 + Self::ksize() as u64
    }

    /// The embedded Bloom filter (its handle offset is fixed after construction).
    fn bloom(&self, stack: &BStack) -> io::Result<BStackCountingBloomFilter<K>> {
        let off = read_u64(stack, self.range.start() + BLOOM_OFF)?;
        Ok(<BStackCountingBloomFilter<K> as BStackBlock>::from_range(
            BStackRange::new(off, BLOOM_SIZE),
        ))
    }

    /// Allocate an empty set with a default-sized Bloom filter.
    pub fn new<A: BStackWalAnchor>(allocator: &A) -> io::Result<BStackOwned<Self>> {
        Self::with_capacity(allocator, DEFAULT_ITEMS, DEFAULT_FP)
    }

    /// Allocate an empty set whose Bloom filter is sized for `expected_items` at
    /// false-positive rate `fp_rate`.
    pub fn with_capacity<A: BStackWalAnchor>(
        allocator: &A,
        expected_items: u64,
        fp_rate: f64,
    ) -> io::Result<BStackOwned<Self>> {
        // The Bloom filter is a child block; take its handle offset.
        let bloom =
            BStackCountingBloomFilter::<K>::with_capacity(allocator, expected_items, fp_rate)?;
        let bloom_off = bloom.into_inner().range().start();
        let od = HashSetOnDisk {
            header: BlockHeader {
                size: SET_SIZE,
                tag: Self::eightcc(),
            },
            table: 0,
            cap: 0,
            len: 0,
            used: 0,
            bloom: bloom_off,
        };
        match alloc_image(allocator, bytemuck::bytes_of(&od)) {
            // SAFETY: a freshly allocated block owned by no other handle.
            Ok(range) => Ok(unsafe { BStackOwned::from_raw(Self::from_range(range)) }),
            Err(e) => {
                // SAFETY: the Bloom child was just allocated, referenced by nobody.
                let bloom = <BStackCountingBloomFilter<K> as BStackBlock>::from_range(
                    BStackRange::new(bloom_off, BLOOM_SIZE),
                );
                let _ = unsafe { BStackOwned::from_raw(bloom) }.bstack_drop(allocator);
                Err(e)
            }
        }
    }

    /// Number of keys.
    pub fn len(&self, stack: &BStack) -> io::Result<u64> {
        read_u64(stack, self.range.start() + LEN_OFF)
    }

    /// Whether the set is empty.
    pub fn is_empty(&self, stack: &BStack) -> io::Result<bool> {
        Ok(self.len(stack)? == 0)
    }

    /// Insert `key`; returns `true` if it was newly added, `false` if already
    /// present.
    pub fn insert<A: BStackWalAnchor>(&self, allocator: &A, key: K) -> io::Result<bool> {
        let key_bytes = bytemuck::bytes_of(&key).to_vec();
        let hash = fnv1a(&key_bytes);
        let bloom = self.bloom(allocator.stack())?;
        // Add to the filter first so it always over-approximates the table.
        bloom.insert(allocator, &key)?;
        let was_new = self.table_insert(allocator, &key_bytes, hash)?;
        if !was_new {
            // Duplicate: undo the filter bump so counts don't drift upward.
            bloom.remove(allocator, &key)?;
        }
        Ok(was_new)
    }

    /// Remove `key`; returns `true` if it was present.
    pub fn remove<A: BStackWalAnchor>(&self, allocator: &A, key: &K) -> io::Result<bool> {
        let key_bytes = bytemuck::bytes_of(key).to_vec();
        let hash = fnv1a(&key_bytes);
        // Remove from the table first; only then decrement the filter (and only
        // for a key that was actually present, per the counting-Bloom contract).
        let was_present = self.table_remove(allocator, &key_bytes, hash)?;
        if was_present {
            self.bloom(allocator.stack())?.remove(allocator, key)?;
        }
        Ok(was_present)
    }

    /// Whether `key` is present. Fast-rejects via the Bloom filter before probing.
    pub fn contains(&self, stack: &BStack, key: &K) -> io::Result<bool> {
        if !self.bloom(stack)?.contains(stack, key)? {
            return Ok(false);
        }
        let key_bytes = bytemuck::bytes_of(key);
        self.table_contains(stack, key_bytes, fnv1a(key_bytes))
    }

    /// A lazy iterator over all keys in **unspecified** order, yielding
    /// `io::Result`. A read snapshot: do not mutate the set while iterating.
    pub fn iter<'a>(&self, stack: &'a BStack) -> io::Result<HashSetIter<'a, K>> {
        let [table, cap] = read_fields::<2>(stack, self.range.start() + TABLE_OFF)?;
        Ok(HashSetIter {
            stack,
            table,
            cap,
            stride: Self::stride(),
            ksz: Self::ksize(),
            idx: 0,
            scratch: Scratch::new(),
            _marker: PhantomData,
        })
    }

    /// Place `key` in the table if absent; returns whether it was newly added.
    fn table_insert<A: BStackWalAnchor>(
        &self,
        allocator: &A,
        key_bytes: &[u8],
        hash: u64,
    ) -> io::Result<bool> {
        let handle = self.range.start();
        let stride = Self::stride();
        let ksz = Self::ksize();
        loop {
            let [cap, _len, used] = read_fields::<3>(allocator.stack(), handle + CAP_OFF)?;
            if cap == 0 || (used + 1) * 4 > cap * 3 {
                self.grow(allocator)?;
                continue;
            }
            let first_tomb: Cell<Option<u64>> = Cell::new(None);
            let is_new = Cell::new(false);
            let need_grow = Cell::new(false);

            probe_commit(
                allocator,
                handle,
                stride,
                hash,
                |m, idx, buf| {
                    let state = get_u64(&buf[0..8]);
                    if state == EMPTY {
                        let target = first_tomb.get().unwrap_or(idx);
                        let slot_was_empty = first_tomb.get().is_none();
                        is_new.set(true);
                        ProbeStep::Stop(place_writes(
                            handle,
                            stride,
                            m,
                            target,
                            slot_was_empty,
                            key_bytes,
                        ))
                    } else if state == OCCUPIED && buf[8..8 + ksz] == *key_bytes {
                        ProbeStep::Stop(Vec::new()) // already present
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
                        place_writes(handle, stride, m, t, false, key_bytes)
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
            return Ok(is_new.get());
        }
    }

    /// Tombstone `key` in the table if present; returns whether it was.
    fn table_remove<A: BStackWalAnchor>(
        &self,
        allocator: &A,
        key_bytes: &[u8],
        hash: u64,
    ) -> io::Result<bool> {
        let handle = self.range.start();
        let stride = Self::stride();
        let ksz = Self::ksize();
        let found = Cell::new(false);
        probe_commit(
            allocator,
            handle,
            stride,
            hash,
            |m, idx, buf| {
                let state = get_u64(&buf[0..8]);
                if state == EMPTY {
                    ProbeStep::Stop(Vec::new())
                } else if state == OCCUPIED && buf[8..8 + ksz] == *key_bytes {
                    found.set(true);
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
        Ok(found.get())
    }

    /// Exact table membership probe (no Bloom fast-reject).
    fn table_contains(&self, stack: &BStack, key_bytes: &[u8], hash: u64) -> io::Result<bool> {
        let handle = self.range.start();
        let stride = Self::stride();
        let ksz = Self::ksize();
        let [table, cap] = read_fields::<2>(stack, handle + TABLE_OFF)?;
        if cap == 0 {
            return Ok(false);
        }
        let mask = cap - 1;
        let mut idx = hash & mask;
        let mut scratch = Scratch::new();
        for _ in 0..cap {
            let bucket = table + idx * stride;
            let buf = scratch.buf(stride as usize);
            stack.get_into(bucket, buf)?;
            let state = get_u64(&buf[0..8]);
            if state == EMPTY {
                return Ok(false);
            }
            if state == OCCUPIED && buf[8..8 + ksz] == *key_bytes {
                return Ok(true);
            }
            idx = (idx + 1) & mask;
        }
        Ok(false)
    }

    /// Grow the table to at least double its capacity, rehashing every live key
    /// (and dropping tombstones) atomically.
    fn grow<A: BStackWalAnchor>(&self, allocator: &A) -> io::Result<()> {
        let handle = self.range.start();
        let stride = Self::stride();
        let ksz = Self::ksize();
        let cap0 = read_u64(allocator.stack(), handle + CAP_OFF)?;
        let newcap = if cap0 == 0 { MIN_CAP } else { cap0 * 2 };
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
                    table: get_u64(&meta_buf[0..8]),
                    cap: get_u64(&meta_buf[8..16]),
                    len: get_u64(&meta_buf[16..24]),
                    used: get_u64(&meta_buf[24..32]),
                };
                if newcap <= m.cap {
                    abort = true;
                }
                meta = Some(m);
            }
            if abort {
                return None;
            }
            let m = meta.as_ref().unwrap();

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

            if !built {
                built = true;
                grown.set(true);
                old_table.set(m.table);
                old_cap.set(m.cap);

                new_image = vec![0u8; (newcap * stride) as usize];
                let newmask = newcap - 1;
                for j in 0..m.cap {
                    let lo = (j * stride) as usize;
                    if get_u64(&old_buf[lo..lo + 8]) != OCCUPIED {
                        continue;
                    }
                    let kb = &old_buf[lo + 8..lo + 8 + ksz];
                    let mut idx = fnv1a(kb) & newmask;
                    loop {
                        let nlo = (idx * stride) as usize;
                        if get_u64(&new_image[nlo..nlo + 8]) == EMPTY {
                            new_image[nlo..nlo + 8].copy_from_slice(&OCCUPIED.to_le_bytes());
                            new_image[nlo + 8..nlo + 8 + ksz].copy_from_slice(kb);
                            break;
                        }
                        idx = (idx + 1) & newmask;
                    }
                }
                writes.push((newtable, std::mem::take(&mut new_image)));
                writes.push((handle + TABLE_OFF, newtable.to_le_bytes().to_vec()));
                writes.push((handle + CAP_OFF, newcap.to_le_bytes().to_vec()));
                writes.push((handle + USED_OFF, m.len.to_le_bytes().to_vec()));
            }
            if w < writes.len() {
                let i = w;
                w += 1;
                let (off, ref bytes) = writes[i];
                // SAFETY: `writes` outlives the call and is not mutated after build.
                let d: &[u8] = unsafe { core::mem::transmute::<&[u8], _>(bytes.as_slice()) };
                return Some(BStackGenOp::Write {
                    offset: off,
                    data: d,
                });
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
            let _ =
                unsafe { dealloc_range(allocator, BStackRange::new(newtable, newcap * stride)) };
        }
        Ok(())
    }

    /// Attach an allocator to make an auto-freeing [`AutoDrop`] guard.
    pub fn auto<A: BStackWalAnchor>(self, allocator: &A) -> AutoDrop<'_, Self, A> {
        // SAFETY: sole ownership was asserted when the set was created.
        unsafe { AutoDrop::from_raw(self, allocator) }
    }
}

impl<K: Pod> BStackCast for BStackHashSet<K> {
    /// An `"HSt"` prefix perturbed by the key size.
    fn eightcc() -> EightCC {
        const BASE: EightCC = EightCC::new([b'H', b'S', b't', 0x80, 0x81, 0x82, 0x83, 0x84]);
        BASE.mix(EightCC::new((size_of::<K>() as u64).to_le_bytes()))
    }
}

impl<K: Pod> BStackBlock for BStackHashSet<K> {
    type OnDisk = HashSetOnDisk;

    fn from_range(range: BStackRange) -> Self {
        BStackHashSet {
            range,
            _marker: PhantomData,
        }
    }

    fn range(&self) -> BStackRange {
        self.range
    }

    /// Free the bucket block and the embedded Bloom filter, **without** freeing
    /// the handle block itself.
    fn __bstack_drop_children<A: BStackWalAnchor>(
        range: BStackRange,
        allocator: &A,
    ) -> io::Result<()> {
        let handle = range.start();
        let [table, cap, _len, _used, bloom_off] =
            read_fields::<5>(allocator.stack(), handle + TABLE_OFF)?;
        if table != 0 {
            // SAFETY: the set solely owns its bucket block.
            unsafe { dealloc_range(allocator, BStackRange::new(table, cap * Self::stride()))? };
        }
        if bloom_off != 0 {
            // SAFETY: the set solely owns its embedded Bloom filter.
            let bloom = <BStackCountingBloomFilter<K> as BStackBlock>::from_range(
                BStackRange::new(bloom_off, BLOOM_SIZE),
            );
            unsafe { BStackOwned::from_raw(bloom) }.bstack_drop(allocator)?;
        }
        Ok(())
    }

    /// Deep-clone: copy the bucket block, deep-clone the Bloom filter, and stage
    /// the handle, in the parent plan's single atomic commit.
    fn __bstack_clone_into<A: BStackWalAnchor>(
        &self,
        allocator: &A,
        plan: &mut ClonePlan,
    ) -> io::Result<BStackRange> {
        let handle = self.range.start();
        let stride = Self::stride();
        let [table, cap, len, used, bloom_off] =
            read_fields::<5>(allocator.stack(), handle + TABLE_OFF)?;

        let new_table = if cap != 0 {
            let mut image = vec![0u8; (cap * stride) as usize];
            allocator.stack().get_into(table, &mut image)?;
            let dst = plan.alloc_raw(allocator, cap * stride)?;
            plan.write(dst.start(), image);
            dst.start()
        } else {
            0
        };

        let bloom = <BStackCountingBloomFilter<K> as BStackBlock>::from_range(BStackRange::new(
            bloom_off, BLOOM_SIZE,
        ));
        let new_bloom = bloom.__bstack_clone_into(allocator, plan)?.start();

        let handle_dst = plan.alloc_raw(allocator, SET_SIZE)?;
        let od = HashSetOnDisk {
            header: BlockHeader {
                size: SET_SIZE,
                tag: Self::eightcc(),
            },
            table: new_table,
            cap,
            len,
            used,
            bloom: new_bloom,
        };
        plan.write(handle_dst.start(), bytemuck::bytes_of(&od).to_vec());
        Ok(handle_dst)
    }
}

impl<K: Pod> BStackDrop for BStackHashSet<K> {
    fn bstack_drop<A: BStackWalAnchor>(self, allocator: &A) -> io::Result<()> {
        Self::__bstack_drop_children(self.range, allocator)?;
        // SAFETY: sole ownership of the handle block was asserted at construction.
        unsafe { dealloc_range(allocator, self.range) }
    }
}

impl<K: Pod> TryCloneIn for BStackHashSet<K> {
    fn try_clone_in<A: BStackWalAnchor>(
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

/// An unordered iterator over a [`BStackHashSet`]'s keys, yielding
/// `io::Result<K>`. Created by [`BStackHashSet::iter`]; scans the buckets.
pub struct HashSetIter<'a, K: Pod> {
    stack: &'a BStack,
    table: u64,
    cap: u64,
    stride: u64,
    ksz: usize,
    idx: u64,
    scratch: Scratch,
    _marker: PhantomData<fn() -> K>,
}

impl<'a, K: Pod> Iterator for HashSetIter<'a, K> {
    type Item = io::Result<K>;

    fn next(&mut self) -> Option<Self::Item> {
        while self.idx < self.cap {
            let i = self.idx;
            self.idx += 1;
            let buf = self.scratch.buf(self.stride as usize);
            if let Err(e) = self.stack.get_into(self.table + i * self.stride, buf) {
                self.idx = self.cap;
                return Some(Err(e));
            }
            if get_u64(&buf[0..8]) == OCCUPIED {
                return Some(Ok(bytemuck::pod_read_unaligned::<K>(&buf[8..8 + self.ksz])));
            }
        }
        None
    }
}
