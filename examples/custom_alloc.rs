//! Custom allocator showcasing `Into<BStackOwnedSlice>` and the owned/borrowed split.
//!
//! Demonstrates:
//!
//! 1. **`Into<BStackOwnedSlice>`** — the `Allocated` associated type can carry
//!    extra metadata alongside the region handle.  The trait only requires
//!    `Into<BStackOwnedSlice<'a, Self>>`, so the custom type can be richer than
//!    a bare slice.
//!
//! 2. **Owned vs borrowed I/O** — `BStackOwnedSlice` exposes `read*` / `write*` /
//!    `zero*` convenience methods directly.  Read methods take `&self`; write and
//!    zero methods take `&mut self`, enforcing single-writer exclusivity in the
//!    type system without requiring an explicit `as_slice[_mut]()` call.
//!
//! 3. **`BStackOwnedSliceAllocator` convenience bound** — a compact replacement
//!    for the three-part allocator + error + type bound.
//!
//! Run:
//!
//! ```sh
//! cargo run --example custom_alloc --features alloc,set
//! ```

#[cfg(all(feature = "alloc", feature = "set"))]
use bstack::{
    BStack, BStackAllocator, BStackOwnedSlice, BStackOwnedSliceAllocator, LinearBStackAllocator,
};
#[cfg(all(feature = "alloc", feature = "set"))]
use std::io;
#[cfg(all(feature = "alloc", feature = "set"))]
use std::sync::atomic::{AtomicU64, Ordering};

// ── Custom error type ────────────────────────────────────────────────────────

#[cfg(all(feature = "alloc", feature = "set"))]
#[derive(Debug)]
enum BumpError {
    Io(io::Error),
    NotTail,
}

#[cfg(all(feature = "alloc", feature = "set"))]
impl std::fmt::Display for BumpError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BumpError::Io(e) => write!(f, "I/O error: {e}"),
            BumpError::NotTail => {
                f.write_str("SequenceBumpAllocator: realloc only supported for the tail allocation")
            }
        }
    }
}

#[cfg(all(feature = "alloc", feature = "set"))]
impl From<io::Error> for BumpError {
    fn from(e: io::Error) -> Self {
        BumpError::Io(e)
    }
}

// ── Custom handle: BStackOwnedSlice + in-memory sequence number ──────────────

/// A region handle that carries the allocation coordinate AND a sequence number.
///
/// This illustrates that `Allocated` can hold more than just the offset and
/// length: any per-allocation metadata lives here, and the allocator trait only
/// requires that it can be converted `Into<BStackOwnedSlice<'a, Self>>`.
#[cfg(all(feature = "alloc", feature = "set"))]
#[derive(Debug)]
struct StampedSlice<'a> {
    inner: BStackOwnedSlice<'a, SequenceBumpAllocator>,
    seq: u64,
}

#[cfg(all(feature = "alloc", feature = "set"))]
impl<'a> StampedSlice<'a> {
    fn start(&self) -> u64 {
        self.inner.start()
    }
    fn len(&self) -> u64 {
        self.inner.len()
    }
}

// Required: `Allocated<'a>: Into<BStackOwnedSlice<'a, Self>>`.
#[cfg(all(feature = "alloc", feature = "set"))]
impl<'a> From<StampedSlice<'a>> for BStackOwnedSlice<'a, SequenceBumpAllocator> {
    fn from(s: StampedSlice<'a>) -> Self {
        s.inner
    }
}

// ── Custom allocator ─────────────────────────────────────────────────────────

#[cfg(all(feature = "alloc", feature = "set"))]
struct SequenceBumpAllocator {
    stack: BStack,
    counter: AtomicU64,
}

#[cfg(all(feature = "alloc", feature = "set"))]
impl SequenceBumpAllocator {
    fn new(stack: BStack) -> Self {
        Self {
            stack,
            counter: AtomicU64::new(0),
        }
    }
}

#[cfg(all(feature = "alloc", feature = "set"))]
impl BStackAllocator for SequenceBumpAllocator {
    type Error = BumpError;
    type Allocated<'a> = StampedSlice<'a>;

    fn stack(&self) -> &BStack {
        &self.stack
    }

    fn into_stack(self) -> BStack {
        self.stack
    }

    fn alloc(&self, len: u64) -> Result<StampedSlice<'_>, BumpError> {
        let offset = self.stack.extend(len)?;
        let seq = self.counter.fetch_add(1, Ordering::Relaxed);
        // SAFETY: offset and len come from BStack::extend on self.stack.
        let inner = unsafe { BStackOwnedSlice::from_raw_parts(self, offset, len) };
        Ok(StampedSlice { inner, seq })
    }

    fn realloc<'a>(
        &'a self,
        handle: StampedSlice<'a>,
        new_len: u64,
    ) -> Result<StampedSlice<'a>, BumpError> {
        let start = handle.inner.start();
        let old_len = handle.inner.len();
        let end = handle.inner.end();
        let seq = handle.seq;
        // handle dropped (Drop is no-op); reconstruct below on success.
        if end != self.stack.len()? {
            return Err(BumpError::NotTail);
        }
        match new_len.cmp(&old_len) {
            std::cmp::Ordering::Greater => {
                self.stack.extend(new_len - old_len)?;
            }
            std::cmp::Ordering::Less => {
                self.stack.discard(old_len - new_len)?;
            }
            std::cmp::Ordering::Equal => {}
        }
        // SAFETY: start unchanged; tail adjusted to new_len.
        let inner = unsafe { BStackOwnedSlice::from_raw_parts(self, start, new_len) };
        Ok(StampedSlice { inner, seq })
    }

    fn dealloc(&self, handle: StampedSlice<'_>) -> Result<(), BumpError> {
        if handle.inner.end() == self.stack.len()? {
            self.stack.discard(handle.inner.len())?;
        }
        Ok(())
    }
}

// ── Generic helpers ───────────────────────────────────────────────────────────

/// Write `data` via `BStackOwnedSliceAllocator` (compact bound covering all
/// built-in allocators with `Error = io::Error` and `Allocated<'a> = BStackOwnedSlice<'a, A>`).
///
/// Does NOT accept `SequenceBumpAllocator` (custom Error and Allocated types).
#[cfg(all(feature = "alloc", feature = "set"))]
fn write_and_read<A: BStackOwnedSliceAllocator>(alloc: &A, data: &[u8]) -> io::Result<Vec<u8>> {
    let mut owned: BStackOwnedSlice<'_, A> = alloc.alloc(data.len() as u64)?;
    owned.write(data)?;
    owned.read()
}

/// Allocate `len` bytes and read them back via any `BStackAllocator` whose
/// `Allocated` converts `Into<BStackOwnedSlice>` — this covers both built-in
/// and custom allocators.
#[cfg(all(feature = "alloc", feature = "set"))]
fn alloc_read_back<A>(alloc: &A, len: u64) -> Result<Vec<u8>, A::Error>
where
    A: BStackAllocator,
    A::Error: From<io::Error>,
    for<'a> A::Allocated<'a>: Into<BStackOwnedSlice<'a, A>>,
{
    let handle: A::Allocated<'_> = alloc.alloc(len)?;
    // Convert the custom Allocated into a plain BStackOwnedSlice for I/O.
    let owned: BStackOwnedSlice<'_, A> = handle.into();
    owned.read().map_err(A::Error::from)
}

// ── Main ──────────────────────────────────────────────────────────────────────

#[cfg(all(feature = "alloc", feature = "set"))]
fn main() -> io::Result<()> {
    let path = "custom_alloc_example.bstack";
    let _ = std::fs::remove_file(path);

    // ── Custom Error + Allocated<'a> ─────────────────────────────────────────
    println!("=== SequenceBumpAllocator ===");
    {
        let alloc = SequenceBumpAllocator::new(BStack::open(path)?);

        let a = alloc.alloc(16).unwrap();
        println!("alloc  seq={} offset={} len={}", a.seq, a.start(), a.len());
        let b = alloc.alloc(8).unwrap();
        println!("alloc  seq={} offset={} len={}", b.seq, b.start(), b.len());

        // Non-tail realloc returns the custom NotTail variant.
        match alloc.realloc(a, 32).unwrap_err() {
            BumpError::NotTail => println!("realloc(a) → NotTail"),
            other => println!("realloc(a) → {other}"),
        }

        // Tail realloc preserves the sequence number.
        let b = alloc.realloc(b, 24).unwrap();
        println!("realloc(b) → seq={} len={}", b.seq, b.len());

        // Convert StampedSlice → BStackOwnedSlice via Into, then do I/O.
        // This is the core "Into<BStackOwnedSlice>" pattern: the rich handle
        // yields a plain owned slice for generic I/O code.
        let mut owned: BStackOwnedSlice<'_, SequenceBumpAllocator> = b.into();
        owned.write(b"stamped data!!\0")?;
        println!("write+read → {:?}", String::from_utf8_lossy(&owned.read()?));
        // Drop is a no-op: the allocation persists on disk until explicitly dealloc'd.
        // Must drop before into_stack() so the borrow of `alloc` ends.
        drop(owned);

        // alloc_read_back works for SequenceBumpAllocator via Into<BStackOwnedSlice>.
        let zeros = alloc_read_back(&alloc, 4).unwrap();
        println!("alloc_read_back(4) → {:?}", zeros);

        let len_before = alloc.len().unwrap();
        let c = alloc.alloc(8).unwrap();
        alloc.dealloc(c).unwrap();
        println!(
            "dealloc tail: {} → {} bytes",
            len_before,
            alloc.len().unwrap()
        );

        drop(alloc.into_stack());
    }

    // ── BStackOwnedSliceAllocator compact bound ──────────────────────────────
    println!("\n=== BStackOwnedSliceAllocator (LinearBStackAllocator) ===");
    {
        let _ = std::fs::remove_file(path);
        let alloc = LinearBStackAllocator::new(BStack::open(path)?);
        let data = write_and_read(&alloc, b"hello, BStackOwnedSliceAllocator")?;
        println!("{:?}", String::from_utf8_lossy(&data));
        drop(alloc.into_stack());
    }

    std::fs::remove_file(path).ok();
    Ok(())
}

#[cfg(not(all(feature = "alloc", feature = "set")))]
fn main() {
    println!("This example requires the 'alloc' and 'set' features.");
    println!("Run: cargo run --example custom_alloc --features alloc,set");
}
