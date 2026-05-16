#[cfg(all(feature = "alloc", feature = "set"))]
use bstack::{BStack, BStackAllocator, BStackSlice, BStackSliceAllocator, LinearBStackAllocator};
#[cfg(all(feature = "alloc", feature = "set"))]
use std::convert::Infallible;
#[cfg(all(feature = "alloc", feature = "set"))]
use std::fmt;
#[cfg(all(feature = "alloc", feature = "set"))]
use std::io;
#[cfg(all(feature = "alloc", feature = "set"))]
use std::sync::atomic::{AtomicU64, Ordering};

#[cfg(all(feature = "alloc", feature = "set"))]
#[derive(Debug)]
enum BumpError {
    Io(io::Error),
    NotTail,
}

#[cfg(all(feature = "alloc", feature = "set"))]
impl fmt::Display for BumpError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BumpError::Io(e) => write!(f, "I/O error: {e}"),
            BumpError::NotTail => f.write_str(
                "SequenceBumpAllocator: realloc is only supported for the tail allocation",
            ),
        }
    }
}

#[cfg(all(feature = "alloc", feature = "set"))]
impl From<io::Error> for BumpError {
    fn from(e: io::Error) -> Self {
        BumpError::Io(e)
    }
}

// Custom handle: BStackSlice + in-memory sequence number.
#[cfg(all(feature = "alloc", feature = "set"))]
#[derive(Copy, Clone, Debug)]
struct StampedSlice<'a> {
    slice: BStackSlice<'a, SequenceBumpAllocator>,
    seq: u64,
}

// Required: Allocated<'a>: TryInto<BStackSlice<'a, Self>>.
// Infallible error — we just unwrap the inner slice.
#[cfg(all(feature = "alloc", feature = "set"))]
impl<'a> TryFrom<StampedSlice<'a>> for BStackSlice<'a, SequenceBumpAllocator> {
    type Error = Infallible;

    fn try_from(s: StampedSlice<'a>) -> Result<Self, Infallible> {
        Ok(s.slice)
    }
}

// Bump allocator with custom associated types.
// Does NOT satisfy BStackSliceAllocator (uses custom Error + Allocated).
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
        // SAFETY: offset and len come from BStack::extend on self.stack.
        let slice = unsafe { BStackSlice::from_raw_parts(self, offset, len) };
        let seq = self.counter.fetch_add(1, Ordering::Relaxed);
        Ok(StampedSlice { slice, seq })
    }

    fn realloc<'a>(
        &'a self,
        handle: StampedSlice<'a>,
        new_len: u64,
    ) -> Result<StampedSlice<'a>, BumpError> {
        if handle.slice.end() != self.stack.len()? {
            return Err(BumpError::NotTail);
        }
        let old_len = handle.slice.len();
        match new_len.cmp(&old_len) {
            std::cmp::Ordering::Greater => { self.stack.extend(new_len - old_len)?; }
            std::cmp::Ordering::Less => { self.stack.discard(old_len - new_len)?; }
            std::cmp::Ordering::Equal => {}
        }
        // SAFETY: start unchanged; tail adjusted to new_len.
        let slice = unsafe { BStackSlice::from_raw_parts(self, handle.slice.start(), new_len) };
        Ok(StampedSlice { slice, seq: handle.seq })
    }

    fn dealloc(&self, handle: StampedSlice<'_>) -> Result<(), BumpError> {
        if handle.slice.end() == self.stack.len()? {
            self.stack.discard(handle.slice.len())?;
        }
        Ok(())
    }
}

// Generic helper using BStackSliceAllocator — compact bound covering all
// library allocators (Error = io::Error, Allocated<'a> = BStackSlice<'a, A>).
// Does NOT accept SequenceBumpAllocator (custom Error + Allocated types).
#[cfg(all(feature = "alloc", feature = "set"))]
fn write_and_read<A: BStackSliceAllocator>(alloc: &A, data: &[u8]) -> io::Result<Vec<u8>> {
    let slice: BStackSlice<'_, A> = alloc.alloc(data.len() as u64)?;
    slice.write(data)?;
    slice.read()
}

// Generic helper using raw BStackAllocator — accepts any allocator including
// custom ones, converting the handle via TryInto<BStackSlice>.
#[cfg(all(feature = "alloc", feature = "set"))]
fn alloc_read_back<A>(alloc: &A, len: u64) -> Result<Vec<u8>, A::Error>
where
    A: BStackAllocator,
    A::Error: From<io::Error>,
    for<'a> A::Allocated<'a>: TryInto<BStackSlice<'a, A>, Error = Infallible>,
{
    let handle: A::Allocated<'_> = alloc.alloc(len)?;
    let slice: BStackSlice<'_, A> = handle.try_into().unwrap();
    slice.read().map_err(A::Error::from)
}

#[cfg(all(feature = "alloc", feature = "set"))]
fn main() -> io::Result<()> {
    let path = "custom_alloc_example.bstack";
    let _ = std::fs::remove_file(path);

    // -- custom Error + Allocated<'a> -----------------------------------------
    println!("=== SequenceBumpAllocator ===");
    {
        let alloc = SequenceBumpAllocator::new(BStack::open(path)?);

        let a = alloc.alloc(16).unwrap();
        println!("alloc  seq={} offset={} len={}", a.seq, a.slice.start(), a.slice.len());
        let b = alloc.alloc(8).unwrap();
        println!("alloc  seq={} offset={} len={}", b.seq, b.slice.start(), b.slice.len());

        // Non-tail realloc returns the custom NotTail variant.
        match alloc.realloc(a, 32).unwrap_err() {
            BumpError::NotTail => println!("realloc(a) → NotTail"),
            other => println!("realloc(a) → {other}"),
        }

        // Tail realloc preserves the sequence number.
        let b = alloc.realloc(b, 24).unwrap();
        println!("realloc(b) → seq={} len={}", b.seq, b.slice.len());

        // TryInto converts the custom handle to a plain BStackSlice for I/O.
        let plain: BStackSlice<'_, SequenceBumpAllocator> = b.try_into().unwrap();
        plain.write(b"custom handle")?;
        println!("write+read → {:?}", String::from_utf8_lossy(&plain.read()?[..13]));

        // alloc_read_back uses the raw BStackAllocator bound with TryInto.
        let zeros = alloc_read_back(&alloc, 4).unwrap();
        println!("alloc_read_back(4) → {:?}", zeros);

        let len_before = alloc.len().unwrap();
        let c = alloc.alloc(8).unwrap();
        alloc.dealloc(c).unwrap();
        println!("dealloc tail: {} → {} bytes", len_before, alloc.len().unwrap());

        drop(alloc.into_stack());
    }

    // -- BStackSliceAllocator compact bound -----------------------------------
    println!("\n=== BStackSliceAllocator ===");
    {
        let _ = std::fs::remove_file(path);
        let alloc = LinearBStackAllocator::new(BStack::open(path)?);
        let data = write_and_read(&alloc, b"hello, BStackSliceAllocator")?;
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
