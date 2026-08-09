//! [`BStackCountingBloomFilter<K>`]: an owned counting Bloom filter.
//!
//! A probabilistic set: [`contains`](BStackCountingBloomFilter::contains) never
//! yields a false negative (a key that was inserted always reports present) but
//! may yield a false positive (report present for a key that was not). The
//! **counting** variant uses small integer counters instead of single bits, so it
//! also supports [`remove`](BStackCountingBloomFilter::remove) — the classic use
//! being a cheap in-memory-ish guard *in front of* an expensive
//! [`crate::BStackHashMap`] / [`crate::BStackBTreeMap`] lookup, to skip the disk
//! probe for keys that are definitely absent.
//!
//! # Layout — one contiguous block, no pointers
//!
//! The fixed handle ([`BloomOnDisk`]) records the counter-array pointer, the
//! counter count `m`, the number of hash functions `k`, and the inserted-item
//! count `n`. The counters themselves are one contiguous `[u8; m]` block (byte
//! counters — trivially addressable and saturating at 255; ~8× a bit filter, a
//! deliberate simplicity-for-space trade). The `k` indices come from **double
//! hashing** ([`super::hash::double_hash`]) so a single key yields `k`
//! well-distributed positions with no per-`k` hashing cost. Keys are `Pod`,
//! hashed by their raw bytes.
//!
//! # Atomicity
//!
//! `insert` / `remove` read every touched counter and `n`, then write the
//! adjusted values, all inside one [`bstack::BStack::inplace_gen`] — so each is
//! atomic per call and external-lock-free (a concurrent writer never loses an
//! increment, which would otherwise let a `remove` wrongly zero a shared
//! counter). The filter is fixed-size (no growth), so `data`/`m`/`k` never change
//! after construction and need no synchronization. `contains` is a plain read.
//!
//! **Caveat (inherent to counting Bloom filters):** only `remove` keys that were
//! actually inserted. Removing an absent key may decrement counters shared with
//! present keys and introduce false negatives.

use core::marker::PhantomData;
use core::mem::size_of;
use std::io;

use bstack::{BStack, BStackGenOp, BStackOwnedSliceAllocator, BStackRange};
use crate::wal::BStackWalAnchor;
use bytemuck::{Pod, Zeroable};

use super::hash::double_hash;
use super::util::{alloc_image, read_fields, read_u64};
use crate::block::{BStackBlock, BStackCast};
use crate::clone::{ClonePlan, TryCloneIn};
use crate::layout::{BlockHeader, EightCC, HEADER_SIZE};
use crate::owned::BStackOwned;
use crate::teardown::{AutoDrop, BStackDrop, dealloc_range};

/// The on-disk image of a [`BStackCountingBloomFilter`]: header, counter-array
/// pointer (`0` = none), counter count `m`, hash count `k`, and inserted-item
/// count `n`. `#[repr(C)]`, `u64` fields only — fixed-size, non-generic.
#[repr(C)]
#[derive(Clone, Copy, Pod, Zeroable)]
pub struct BloomOnDisk {
    /// The 16-byte block header (size + type tag).
    pub header: BlockHeader,
    /// Offset of the `[u8; m]` counter block, or `0` when unallocated.
    pub data: u64,
    /// Number of counters.
    pub m: u64,
    /// Number of hash functions.
    pub k: u64,
    /// Number of items inserted (minus removed).
    pub n: u64,
}

const DATA_OFF: u64 = HEADER_SIZE; // 16
const M_OFF: u64 = HEADER_SIZE + 8; // 24
const K_OFF: u64 = HEADER_SIZE + 16; // 32
const N_OFF: u64 = HEADER_SIZE + 24; // 40
const BLOOM_SIZE: u64 = size_of::<BloomOnDisk>() as u64;

/// An owned counting Bloom filter over `Pod` keys.
///
/// A typed handle (a newtype over a [`BStackRange`]); [`new`](Self::new) /
/// [`with_capacity`](Self::with_capacity) return a bare
/// [`BStackOwned<BStackCountingBloomFilter<K>>`] that frees nothing on scope exit
/// — free it with [`bstack_drop`](BStackDrop::bstack_drop) or wrap it
/// ([`AutoDrop`] / [`crate::BStackCow`]).
pub struct BStackCountingBloomFilter<K: Pod> {
    range: BStackRange,
    _marker: PhantomData<fn() -> K>,
}

impl<K: Pod> BStackCountingBloomFilter<K> {
    /// The `k` counter indices for `key_bytes` (with possible repeats).
    fn indices(m: u64, k: u64, key_bytes: &[u8]) -> Vec<u64> {
        let (h1, h2) = double_hash(key_bytes);
        (0..k)
            .map(|i| h1.wrapping_add(i.wrapping_mul(h2)) % m)
            .collect()
    }

    /// Collapse indices to distinct `(index, multiplicity)`, so a counter hit by
    /// two of the `k` hashes is adjusted by two in one write.
    fn aggregate(mut idxs: Vec<u64>) -> Vec<(u64, u32)> {
        idxs.sort_unstable();
        let mut out: Vec<(u64, u32)> = Vec::new();
        for x in idxs {
            match out.last_mut() {
                Some(last) if last.0 == x => last.1 += 1,
                _ => out.push((x, 1)),
            }
        }
        out
    }

    /// Allocate a filter with `m` counters and `k` hash functions (both forced to
    /// at least 1). Prefer [`with_capacity`](Self::with_capacity) to size these.
    pub fn new<A: BStackWalAnchor>(
        allocator: &A,
        m: u64,
        k: u64,
    ) -> io::Result<BStackOwned<Self>> {
        let m = m.max(1);
        let k = k.max(1);
        // Allocate and zero the counter block (an orphan until the handle links it).
        let data = {
            let mut slice = allocator.alloc(m)?;
            if let Err(e) = slice.write_range(0, vec![0u8; m as usize]) {
                let _ = allocator.dealloc(slice);
                return Err(e);
            }
            slice.as_range().start()
        };
        let od = BloomOnDisk {
            header: BlockHeader {
                size: BLOOM_SIZE,
                tag: Self::eightcc(),
            },
            data,
            m,
            k,
            n: 0,
        };
        match alloc_image(allocator, bytemuck::bytes_of(&od)) {
            // SAFETY: a freshly allocated block owned by no other handle.
            Ok(range) => Ok(unsafe { BStackOwned::from_raw(Self::from_range(range)) }),
            Err(e) => {
                // SAFETY: the counter block was just allocated, referenced by nobody.
                let _ = unsafe { dealloc_range(allocator, BStackRange::new(data, m)) };
                Err(e)
            }
        }
    }

    /// Allocate a filter sized for `expected_items` at target false-positive rate
    /// `fp_rate`, using the standard optimal `m = -n·ln p / (ln 2)²` and
    /// `k = (m/n)·ln 2`.
    pub fn with_capacity<A: BStackWalAnchor>(
        allocator: &A,
        expected_items: u64,
        fp_rate: f64,
    ) -> io::Result<BStackOwned<Self>> {
        let n = expected_items.max(1) as f64;
        let p = fp_rate.clamp(1e-9, 0.5);
        let ln2 = core::f64::consts::LN_2;
        let m = (-(n * p.ln()) / (ln2 * ln2)).ceil().max(1.0) as u64;
        let k = ((m as f64 / n) * ln2).round().clamp(1.0, 32.0) as u64;
        Self::new(allocator, m, k)
    }

    /// Number of items inserted (minus removed).
    pub fn count(&self, stack: &BStack) -> io::Result<u64> {
        read_u64(stack, self.range.start() + N_OFF)
    }

    /// Whether no items are currently present.
    pub fn is_empty(&self, stack: &BStack) -> io::Result<bool> {
        Ok(self.count(stack)? == 0)
    }

    /// The current estimated false-positive probability, `(1 - e^{-k n / m})^k`.
    pub fn estimated_fp_rate(&self, stack: &BStack) -> io::Result<f64> {
        let handle = self.range.start();
        let [m, k, n] = read_fields::<3>(stack, handle + M_OFF)?;
        let (m, k, n) = (m as f64, k as f64, n as f64);
        Ok((1.0 - (-k * n / m).exp()).powf(k))
    }

    /// Insert `key`, bumping each of its `k` counters (saturating at 255).
    pub fn insert<A: BStackWalAnchor>(&self, allocator: &A, key: &K) -> io::Result<()> {
        self.adjust(allocator, key, true)
    }

    /// Remove `key`, decrementing each of its `k` counters (saturating at 0).
    ///
    /// Only call this for a key that was actually inserted (see the module docs) —
    /// removing an absent key can introduce false negatives.
    pub fn remove<A: BStackWalAnchor>(&self, allocator: &A, key: &K) -> io::Result<()> {
        self.adjust(allocator, key, false)
    }

    /// Whether `key` is *possibly* present: `true` if all `k` counters are
    /// non-zero (may be a false positive), `false` if any is zero (definitely
    /// absent). A plain read.
    pub fn contains(&self, stack: &BStack, key: &K) -> io::Result<bool> {
        let handle = self.range.start();
        let [data, m, k] = read_fields::<3>(stack, handle + DATA_OFF)?;
        let key_bytes = bytemuck::bytes_of(key);
        for idx in Self::indices(m, k, key_bytes) {
            let mut b = [0u8; 1];
            stack.get_into(data + idx, &mut b)?;
            if b[0] == 0 {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Reset every counter and the item count to zero.
    pub fn clear<A: BStackWalAnchor>(&self, allocator: &A) -> io::Result<()> {
        let handle = self.range.start();
        let [data, m] = read_fields::<2>(allocator.stack(), handle + DATA_OFF)?;
        allocator.stack().set_batched([
            (data, vec![0u8; m as usize]),
            (handle + N_OFF, 0u64.to_le_bytes().to_vec()),
        ])
    }

    /// Atomically adjust the counters for `key` (and `n`) up or down, reading and
    /// writing every touched counter in one `inplace_gen` (external-lock-free).
    fn adjust<A: BStackWalAnchor>(
        &self,
        allocator: &A,
        key: &K,
        add: bool,
    ) -> io::Result<()> {
        let handle = self.range.start();
        let [data, m, k] = read_fields::<3>(allocator.stack(), handle + DATA_OFF)?;
        let agg = Self::aggregate(Self::indices(m, k, bytemuck::bytes_of(key)));
        let cn = agg.len();

        // Buffers that must outlive the whole `inplace_gen` call.
        let mut read_c = vec![0u8; cn];
        let mut n_buf = [0u8; 8];
        let mut new_c = vec![0u8; cn];
        let mut new_n = [0u8; 8];

        let mut rc = 0usize;
        let mut n_read = false;
        let mut computed = false;
        let mut wc = 0usize;
        let mut n_written = false;

        allocator.stack().inplace_gen(|_feedback| {
            // Read each distinct counter (one byte).
            if rc < cn {
                let i = rc;
                rc += 1;
                // SAFETY: `read_c` outlives the call; each byte read once.
                let b: &mut [u8] =
                    unsafe { core::mem::transmute::<&mut [u8], _>(&mut read_c[i..i + 1]) };
                return Some(BStackGenOp::Read {
                    offset: data + agg[i].0,
                    buf: b,
                });
            }
            // Read `n`.
            if !n_read {
                n_read = true;
                // SAFETY: `n_buf` outlives the call.
                let b: &mut [u8] = unsafe { core::mem::transmute::<&mut [u8], _>(&mut n_buf[..]) };
                return Some(BStackGenOp::Read {
                    offset: handle + N_OFF,
                    buf: b,
                });
            }
            // Compute the adjusted counters and item count.
            if !computed {
                computed = true;
                for i in 0..cn {
                    let mult = agg[i].1.min(255) as u8;
                    new_c[i] = if add {
                        read_c[i].saturating_add(mult)
                    } else {
                        read_c[i].saturating_sub(mult)
                    };
                }
                let n = u64::from_le_bytes(n_buf);
                let nn = if add {
                    n.saturating_add(1)
                } else {
                    n.saturating_sub(1)
                };
                new_n = nn.to_le_bytes();
            }
            // Write the adjusted counters.
            if wc < cn {
                let i = wc;
                wc += 1;
                // SAFETY: `new_c` outlives the call and is not mutated after compute.
                let d: &[u8] = unsafe { core::mem::transmute::<&[u8], _>(&new_c[i..i + 1]) };
                return Some(BStackGenOp::Write {
                    offset: data + agg[i].0,
                    data: d,
                });
            }
            // Write `n`.
            if !n_written {
                n_written = true;
                // SAFETY: `new_n` outlives the call.
                let d: &[u8] = unsafe { core::mem::transmute::<&[u8], _>(&new_n[..]) };
                return Some(BStackGenOp::Write {
                    offset: handle + N_OFF,
                    data: d,
                });
            }
            None
        })
    }

    /// Attach an allocator to make an auto-freeing [`AutoDrop`] guard.
    pub fn auto<A: BStackWalAnchor>(self, allocator: &A) -> AutoDrop<'_, Self, A> {
        // SAFETY: sole ownership was asserted when the filter was created.
        unsafe { AutoDrop::from_raw(self, allocator) }
    }
}

impl<K: Pod> BStackCast for BStackCountingBloomFilter<K> {
    /// A `"Blm"` prefix perturbed by the key size.
    fn eightcc() -> EightCC {
        const BASE: EightCC = EightCC::new([b'B', b'l', b'm', 0x80, 0x81, 0x82, 0x83, 0x84]);
        BASE.mix(EightCC::new((size_of::<K>() as u64).to_le_bytes()))
    }
}

impl<K: Pod> BStackBlock for BStackCountingBloomFilter<K> {
    type OnDisk = BloomOnDisk;

    fn from_range(range: BStackRange) -> Self {
        BStackCountingBloomFilter {
            range,
            _marker: PhantomData,
        }
    }

    fn range(&self) -> BStackRange {
        self.range
    }

    /// Free the counter block, **without** freeing the handle block itself.
    fn __bstack_drop_children<A: BStackWalAnchor>(
        range: BStackRange,
        allocator: &A,
    ) -> io::Result<()> {
        let [data, m] = read_fields::<2>(allocator.stack(), range.start() + DATA_OFF)?;
        if data != 0 {
            // SAFETY: the filter solely owns its counter block.
            unsafe { dealloc_range(allocator, BStackRange::new(data, m))? };
        }
        Ok(())
    }

    /// Deep-clone: copy the counter block and stage the handle, in the parent
    /// plan's single atomic commit.
    fn __bstack_clone_into<A: BStackWalAnchor>(
        &self,
        allocator: &A,
        plan: &mut ClonePlan,
    ) -> io::Result<BStackRange> {
        let handle = self.range.start();
        let [data, m, k, n] = read_fields::<4>(allocator.stack(), handle + DATA_OFF)?;

        let new_data = if m != 0 {
            let mut bytes = vec![0u8; m as usize];
            allocator.stack().get_into(data, &mut bytes)?;
            let dst = plan.alloc_raw(allocator, m)?;
            plan.write(dst.start(), bytes);
            dst.start()
        } else {
            0
        };

        let handle_dst = plan.alloc_raw(allocator, BLOOM_SIZE)?;
        let od = BloomOnDisk {
            header: BlockHeader {
                size: BLOOM_SIZE,
                tag: Self::eightcc(),
            },
            data: new_data,
            m,
            k,
            n,
        };
        plan.write(handle_dst.start(), bytemuck::bytes_of(&od).to_vec());
        Ok(handle_dst)
    }
}

impl<K: Pod> BStackDrop for BStackCountingBloomFilter<K> {
    fn bstack_drop<A: BStackWalAnchor>(self, allocator: &A) -> io::Result<()> {
        Self::__bstack_drop_children(self.range, allocator)?;
        // SAFETY: sole ownership of the handle block was asserted at construction.
        unsafe { dealloc_range(allocator, self.range) }
    }
}

impl<K: Pod> TryCloneIn for BStackCountingBloomFilter<K> {
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
