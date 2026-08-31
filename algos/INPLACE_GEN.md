# In-Place Generator Sequences — `inplace_gen`

This advisory covers `BStack::inplace_gen` (C: `bstack_inplace_gen`): when to
choose it over the neighbouring primitives, the generator protocol, the staging
overlay its reads and writes share, how the batch commits — and, its focus, the
**six mistakes** that account for nearly every bug written against it.

Four of those six commit a wrong state to disk; the other two are undefined
behaviour. Not one of them fails loudly: `inplace_gen` reports per-operation
errors *through* the generator rather than returning them, borrows write
payloads instead of copying them, and serves reads from pending edits rather
than the file. Each mistake is a direct consequence of one of those three
choices, so the mechanism is set out first and the mistakes follow from it.
A reader who only wants the checklist can start at *Common mistakes*.

`inplace_gen` runs a caller-driven sequence of **dependent reads interleaved
with in-place writes**, under one held write lock, committing every write as a
single crash-atomic unit when the sequence ends. Reach for it when *what* to
write, or *where*, is only known after reading, and when several such writes
must land together or not at all.

Requires the `set` and `atomic` features (`-DBSTACK_FEATURE_SET
-DBSTACK_FEATURE_ATOMIC` in C).

```rust
pub fn inplace_gen<'a, F>(&self, f: F) -> io::Result<()>
where F: FnMut(io::Result<()>) -> Option<BStackGenOp<'a>>;
```

```c
int bstack_inplace_gen(bstack_t *bs,
                       int (*gen)(bstack_gen_op_t *out_op, void *ctx),
                       void *ctx, int *prev_status);
```

---

## Choosing a primitive

| Primitive         | Shape                                                             | Lock  |
|-------------------|-------------------------------------------------------------------|-------|
| `set`             | one in-place write, offset and data known up front                | write |
| `set_batched`     | `k` in-place writes, all known up front, pairwise disjoint        | write |
| `process`         | read one contiguous window, permute it in memory, write it back   | write |
| `get_batched_gen` | dependent reads, no writes                                        | read  |
| `process_gen`     | dependent reads, then **one** mutating step of any kind           | write |
| `inplace_gen`     | dependent reads, then **many** in-place writes committed together | write |

`process_gen` is the sibling to compare against. It allows every op kind
(including the size-changing `Push`/`Pop`/`Discard`/`Atrunc`/`Splice`/`Sparse`
and `Swap`), but the first mutating op **ends** the sequence — one mutation per
call. `inplace_gen` inverts the trade: only in-place writes are allowed, but
they accumulate and commit as one unit.

`set_batched` is the degenerate case of `inplace_gen` with no reads: if every
offset is known before the call, use it — it is simpler and rejects overlap
rather than silently resolving it.

Both generator forms take the **write** lock before the first read, even for a
sequence that turns out to be read-only, because the closure may decide to
mutate only after seeing what it read. `get_batched_gen` is the read-only
alternative and takes only the read lock.

---

## The protocol

`f` is called in a loop. Each call returns `Some(op)` to issue an operation or
`None` to end and commit. Each call receives the **previous** op's
`io::Result<()>` (C: `*prev_status`, an errno or 0); the first call receives
`Ok(())` / `0`.

| Returned                    | Effect                                                             |
|-----------------------------|--------------------------------------------------------------------|
| `Read { offset, buf }`      | fills `buf` from the batch-so-far content; continues               |
| `Write { offset, data }`    | records a pending edit; continues                                  |
| `Len { out }`               | writes the payload size into `out`; continues                      |
| `Abort { source: Some(e) }` | ends; discards every pending edit; call returns `Err(e)`           |
| `Abort { source: None }`    | ends; discards every pending edit; call returns `Ok(())`           |
| `None`                      | ends; commits every pending edit                                   |
| any other op kind           | rejected with `InvalidInput` via feedback; not recorded; continues |

`Swap`, `Push`, `Pop`, `Discard`, `Atrunc`, `Splice`, and `Sparse` are the
rejected kinds. The multi-write journal pins `clen` as the start of its staging
region and the file size as its end, so nothing that moves either may be
compounded with a pending batch.

### Feedback, not early return

Every per-op failure is reported to the *next* `f` call, not returned from
`inplace_gen`:

- a `Read` whose range overflows or exceeds the payload size,
- a `Read` whose I/O fails (the buffer is left holding whatever it held before),
- a `Write` whose range overflows, exceeds the payload, or touches the locked
  prefix `[0, locked_len())`,
- a disallowed op kind.

A failing op is simply **not recorded**, and the sequence continues. The
generator inspects the feedback and decides: retry elsewhere, skip, or unwind
with `Abort`. Nothing about a per-op failure poisons the batch — a generator
that wants one to fail the whole call must respond with
`Abort { source: Some(e) }`.

The call itself returns `Err` only from lock acquisition, the initial size
probe, an `Abort` carrying an error, or the final commit. (In C, an allocation
failure while growing the overlay is also fatal and aborts the call.)

### The payload size is frozen

The size is sampled once, under the lock, before the first call to `f`, and
never changes during the sequence — every write is a same-length overwrite of
committed bytes. `Len` reports that snapshot.

> **Never call another `BStack` method from inside the generator.** The write
> lock is a `std::sync::RwLock` held for the whole call; `stack.len()` inside
> the closure re-enters it and deadlocks. Use the `Len` op, or sample what you
> need before the call.

Reads of the locked prefix `[0, locked_len())` are permitted — those bytes are
immutable. Writes touching it are rejected, matching `set`.

---

## Reads see the batch so far

A `Read` returns the payload as it *would* look with every pending edit
applied — committed bytes overlaid with the edits recorded so far — not the
on-disk bytes:

```
committed    h  e  l  l  o  w  o  r  l  d
pending      [  A  B  C  D  E  )                 Write { offset: 0, data: b"ABCDE" }
Read(0..10) → A  B  C  D  E  w  o  r  l  d
```

The file itself is untouched until the commit, so this "read your own writes"
view exists only in memory. A read whose range is fully covered by pending
edits performs **no disk I/O at all**. This is the intended behaviour and a
frequent source of bugs in equal measure — see mistake 3.

### Overlap resolves in favour of the later write

Unlike `set_batched`, which rejects overlapping writes, `inplace_gen` lets the
newer write win on the bytes it covers. For `a < b < c < d`:

```
             a       b       c       d
Write 1     [───────────────)                    data1 covers a..c
Write 2             [───────────────)            data2 covers b..d
overlay     [───────)───────────────)
              data1     data2
             a..b      b..d
```

Every containment case falls out of the same prefix/suffix split:

| Case             | Result                                                    |
|------------------|-----------------------------------------------------------|
| new encloses old | old edit dropped entirely                                 |
| old encloses new | old splits into a prefix and a suffix around the new edit |
| partial overlap  | only the non-covered end of the old edit survives         |
| no overlap       | both edits kept                                           |

The surviving fragments are **sub-slices of the original `data`**, which is why
a buffer handed to a `Write` stays borrowed for the rest of the call. The
overlay is maintained sorted and pairwise non-overlapping at all times, so what
reaches the journal is always a disjoint block set.

---

## Commit

When the generator returns `None`, the accumulated edits commit:

| Pending edits | Path                                                                |
|---------------|---------------------------------------------------------------------|
| 0             | no-op; the file is not touched                                      |
| 1             | ordinary single-write path (aligned-block write or the WIP journal) |
| ≥ 2           | multi-write journal                                                 |

The multi-write journal stages every `[s | e | data]` block past `clen`, syncs,
arms the `MultiWrite` sentinel (`wip_ptr` stays `0`), syncs, replays each block
in place, syncs, disarms, syncs, and truncates the staged tail. A crash leaves
either every block's old bytes or every block's new bytes. See the *Multi-write
journal* section of [`WIP.md`](WIP.md) for the full protocol and recovery rules.

`Abort` is the only way to unwind a batch. The pending edits only ever existed
in memory, so discarding them touches nothing.

---

## Writing a generator

The closure is `FnMut`, called once per op, so all sequencing state lives in
captured variables. The idiomatic shape is an explicit state enum driven by a
`loop`: `continue` for a state transition that issues no op, `return Some(op)`
for one that does.

```rust
use bstack::{BStackGenOp, bstack_unsafe_reborrow, bstack_unsafe_reborrow_mut};

enum St { ReadHead, Claim, WriteHead, Done }

// Every buffer an op borrows is declared *before* the call.
let mut head_buf = [0u8; 8];
let mut next_buf = [0u8; 8];
let mut tag_buf  = [0u8; 8];
let mut st = St::ReadHead;
let mut claimed = 0u64;

stack.inplace_gen(|feedback| {
    // Any failed read, or any write the validator rejected, unwinds the batch.
    // `Abort` — *not* `None`, which would commit whatever did stage.
    if let Err(e) = feedback {
        return Some(BStackGenOp::Abort { source: Some(e) });
    }
    loop {
        match st {
            St::ReadHead => {
                st = St::Claim;
                return Some(BStackGenOp::Read {
                    offset: HEAD_OFF,
                    // SAFETY: `head_buf` outlives the call; the op is consumed
                    // before the closure runs again.
                    buf: bstack_unsafe_reborrow_mut!(&mut head_buf[..]),
                });
            }
            St::Claim => {
                // The read has resolved, so `head_buf` is readable here.
                claimed = u64::from_le_bytes(head_buf);
                if claimed == SENTINEL {
                    return Some(BStackGenOp::Abort { source: None });
                }
                tag_buf = IN_USE.to_le_bytes();
                st = St::WriteHead;
                return Some(BStackGenOp::Write {
                    offset: claimed,
                    // SAFETY: `tag_buf` outlives the call and is never touched
                    // again — a Write's data stays borrowed until the commit.
                    data: bstack_unsafe_reborrow!(&tag_buf[..]),
                });
            }
            St::WriteHead => {
                next_buf = next_of(claimed).to_le_bytes();
                st = St::Done;
                return Some(BStackGenOp::Write {
                    offset: HEAD_OFF,
                    // SAFETY: as above.
                    data: bstack_unsafe_reborrow!(&next_buf[..]),
                });
            }
            St::Done => return None,   // commits both writes together
        }
    }
})?;
```

### Scratch buffers and the reborrow macros

An op needs a `&'a mut [u8]` (or `&'a [u8]`) whose lifetime outlives the whole
call, derived from one of the closure's own captures — which the borrow checker
rejects with `E0521: borrowed data escapes outside of closure`. This is a
limitation of the `FnMut` model, not a real hazard, and Polonius does not lift
it. `bstack_unsafe_reborrow!` / `bstack_unsafe_reborrow_mut!` package the
workaround: they change the borrow's lifetime and nothing else. The `unsafe` in
the name carries the marking, so no `unsafe` block is written at the call site —
but a `// SAFETY:` comment is still owed. Three obligations:

1. **The referent outlives the call.** A local declared *before* the call and
   never moved, reallocated, or dropped during it.
2. **No overlapping access while the callee holds the buffer.** For a `Read`
   this is automatic in the shape above — the op is consumed before the closure
   runs again, so inspecting the buffer at a later step is fine.
3. **A `Write` payload is frozen for the rest of the call.** Unlike
   `process_gen`, an `inplace_gen` `Write` is *not* applied immediately: the
   borrowed `data` is staged until the final commit and consulted by later
   `Read`s. Mutating it meanwhile is undefined behaviour outright, not merely a
   stale read.

See the `reborrow` module docs for the full contract.

---

## Common mistakes

The first two commit **partial batches** — the exact outcome the primitive
exists to prevent. The third commits a **whole batch computed from the wrong
bytes**. The next two are **undefined behaviour**. The last silently inverts the
generator's intent.

| Mistake                              | Symptom                                              |
|--------------------------------------|------------------------------------------------------|
| ignoring a failed `Read`             | the batch commits, computed from stale buffer bytes  |
| assuming every `Write` staged        | the batch commits, missing the rejected writes       |
| re-reading a region this batch wrote | the read returns the pending edit, not the pre-image |
| reborrowing out of a growing `Vec`   | use-after-free                                       |
| mutating a buffer between `Write`s   | every write commits the *last* content               |
| returning `None` to bail out         | the partial batch commits instead of unwinding       |

### 1. A failed `Read` is an I/O error, not a value

`Read` failures do **not** end the call and are **not** returned. They arrive as
the next call's `feedback`, and the buffer is left holding whatever it held
before — usually zeros on the first read, or the *previous* block's bytes on a
striding scan. A generator that never inspects `feedback` cannot tell the
difference between "the free-list head is 0" and "the read that was supposed to
fill it failed with `EIO`".

```rust
// WRONG — `head_buf` may never have been filled.
stack.inplace_gen(|_feedback| {
    match st {
        St::Consume => {
            let head = u64::from_le_bytes(head_buf);   // possibly stale
            ...
        }
    }
})
```

Every generator that issues a `Read` must branch on `feedback` before consuming
the buffer that read was meant to fill. Both range errors (`InvalidInput`) and
genuine I/O errors (`EIO`, a short read, a device error) arrive through the same
channel, so one check covers both.

The correct response depends on what has already staged. Before the first
`Write`, `None` and `Abort` are equivalent — nothing would commit either way.
After it, only `Abort` is correct; see mistake 6.

### 2. A rejected `Write` is silently not recorded

Validation failures are reported through `feedback` and the write is dropped.
The sequence keeps running, and unless the generator checks, it reaches `None`
and commits **the subset that happened to validate**. A write is rejected when
its range overflows `u64`, when it ends past the payload size, or when it starts
inside the locked prefix `[0, locked_len())`.

```rust
// WRONG — if the second write is rejected, the first still commits alone.
St::Stamp => Some(BStackGenOp::Write { offset: hdr, data: ... }),
St::Link  => Some(BStackGenOp::Write { offset: node, data: ... }),
St::Done  => None,
```

Two defences, and a batch that must be all-or-nothing wants both:

- **Check in advance.** Compute the bounds before the call — `locked_len()` and
  the `Len` op — and never issue a write that cannot validate.
- **Handle the rejection.** Treat a non-`Ok` `feedback` after a `Write` as fatal
  and `Abort`.

The locked prefix is where this bites hardest. `locked_len()` only ever grows,
and `inplace_gen` samples it once under the write lock, so it is fixed for the
duration of a call — but a `lock_up_to` from another thread can advance it
between the moment the generator's plan was computed and the moment the call
acquires the lock. A plan that was valid when built then has its low-offset
writes rejected one by one, and a generator that ignores `feedback` commits the
high-offset remainder: a header updated to point at a block whose tag was never
written. Sample `locked_len()` *inside* the same call that uses it, or bound the
whole batch above the locked region.

### 3. A `Read` returns the batch, not the disk

The overlay makes reads *read-your-writes*, which is what lets a generator make
each decision from the state its earlier writes established. The cost is that a
`Read` is no longer a window onto committed bytes, and two assumptions break.

**The pre-image is gone once the write is staged.** A generator that stages an
edit and later re-reads that range gets its own pending bytes back. If the old
contents are needed — for an undo record, a checksum over the prior state, a
comparison against what was there — read them *before* issuing the write that
covers them.

**A region this pass already rewrote no longer looks untouched.** This is the
form that bites hardest in a scan that rewrites as it goes, because the read
succeeds and returns well-formed data; nothing signals that the generator is
now parsing its own output.

```rust
// WRONG — on a scan that stages merged headers as it advances, this test
// cannot distinguish "already free on disk" from "freed by this very pass",
// so the scan re-consumes ground it has covered.
St::Probe    => Some(BStackGenOp::Read { offset: p, buf: ... }),
St::Classify => { if is_free(word) { /* fold it in again */ } }
```

The fix is to keep the scan's cursor ahead of its own edits — advance strictly
past every range written, so no read ever revisits one — or to track what has
been rewritten in memory and consult that rather than re-reading.

A partially covered read is a third surprise: the result is a **mix**, pending
bytes where edits exist and committed bytes in the gaps. Staging four bytes
inside an eight-byte record and then reading the record yields four new and four
old bytes. That is a coherent view of the batch-so-far, but not one a reader
expecting all-or-nothing will parse correctly.

Finally, a read reflecting the new bytes proves nothing about durability.
Nothing reaches the file until the commit: an `Abort`, or a crash mid-sequence,
erases every byte the reads were showing.

### 4. Reborrowing out of a `Vec` that is still growing

The natural way to accumulate writes is to push `(offset, bytes)` into a `Vec`
and hand out slices into it. But a `Vec` reallocates when it grows, and every
reborrow already yielded points into the **old, freed** buffer. `inplace_gen`
holds those slices until the commit, so the dangling reads happen at commit
time, far from the push that caused them — and often only under a specific
input size, when the capacity doubling happens to land mid-sequence.

```rust
// WRONG — this push may reallocate, dangling every slice already yielded.
let mut writes: Vec<(u64, [u8; 16])> = Vec::new();
...
writes.push((off, word));
Some(BStackGenOp::Write {
    offset: writes[i].0,
    data: bstack_unsafe_reborrow!(&writes[i].1[..]),
})
```

Four ways out, in order of preference:

- **Two phases.** Finish building the `Vec`, *then* stream reborrowed slices out
  of it. Once the collection phase is over the buffer never moves again, so an
  unbounded scan that discovers its own write set can still yield safely.
- **Indirection with a stable address.** `Vec<Box<[u8]>>` — each payload owns
  its own allocation, so growing or reordering the container moves the boxes,
  never the bytes a reborrow points at. This is the canonical fix: it keeps the
  push-as-you-go shape, costs one allocation per write, and needs no reasoning
  about capacity. The payload lives exactly as long as its `Box`, so the element
  must not be removed or overwritten for the rest of the call. Where that cannot
  be guaranteed — an entry replaced mid-sequence, a buffer shared with something
  outside the container, a container rebuilt during the call — reach for
  `Rc<[u8]>` instead and hold a clone for the duration; that discharges
  obligation 1 outright. `Rc` over `Arc`, since the closure never leaves the
  calling thread.
- **Don't heap-allocate.** A fixed-size array declared before the call cannot
  move — the reason a chase over 8-byte links wants a `[u8; 8]` local.
- **Reserve up front.** `Vec::with_capacity(n)` where `n` is a proven upper
  bound, and never exceed it. Fragile: an off-by-one in the bound is UB, not a
  panic.

Obligation 1 is the general rule this is a special case of: the *referent*, not
the handle, must outlive the call. A `Vec` local satisfies it; the heap
allocation it currently points at does not.

### 5. One buffer, many writes — all of them identical

`Write` data is borrowed, not copied. Reusing a single scratch buffer for
successive writes therefore does not do what it looks like: each pending edit
points at the same memory, and the commit reads all of them *after* the last
mutation. Every write lands with the final content.

```rust
// WRONG — both writes commit `b`'s bytes.
St::First => {
    buf = a.to_le_bytes();
    Some(BStackGenOp::Write { offset: o1, data: bstack_unsafe_reborrow!(&buf[..]) })
}
St::Second => {
    buf = b.to_le_bytes();      // silently rewrites the first pending edit too
    Some(BStackGenOp::Write { offset: o2, data: bstack_unsafe_reborrow!(&buf[..]) })
}
```

This is also undefined behaviour — mutating memory behind a live shared
reference — which is why it is worth stating separately from obligation 2: the
compiler will not catch it, and the observable symptom (duplicated content) does
not look like an aliasing bug.

A buffer handed to a `Write` is **frozen for the rest of the call**. Either give
each write its own storage, or keep the buffer genuinely constant. The constant
case is a real and useful pattern rather than a hazard: a scrub that hands the
*same* zero buffer to every write is correct precisely because it never mutates
it, and that is what makes its payload memory O(1) rather than O(writes).

### 6. `None` commits; only `Abort` unwinds

`None` means "the sequence is finished — commit it". It is not a way out. A
generator that hits an error and returns `None` commits exactly the partial
batch it was trying to avoid.

```rust
// WRONG — commits whatever staged before the failure.
if feedback.is_err() { return None; }

// RIGHT — discards every pending edit and fails the call.
if let Err(e) = feedback { return Some(BStackGenOp::Abort { source: Some(e) }); }
```

`Abort` discards the whole overlay. Its `source` sets the outcome
independently: `Some(e)` fails the call with `e`, `None` returns `Ok(())` having
written nothing — the right choice when the generator scans, finds no work to
do, and wants to leave the existing state untouched.

The mirror-image mistake is reaching for `Abort` where `None` was meant: an
error the generator can recover from (a rejected write it will reissue at a
different offset, a read it will retry elsewhere) does not need to tear down the
batch. That flexibility is the reason failures arrive as `feedback` rather than
as a return value.

### And two mechanical ones

- **`Write` does not end the sequence.** Unlike `process_gen`, where the first
  mutating op is the last, an `inplace_gen` generator that returns a `Write`
  and expects the call to be over spins forever. Only `None` and `Abort` end it.
- **No `BStack` method may be called from inside the closure.** The write lock
  is held for the whole call; re-entering it deadlocks. Use the `Len` op, or
  sample what you need before the call.

---

## The C form

`gen` fills `*out_op` and returns 1 to yield an op, or 0 to end and commit. The
previous op's status arrives through `*prev_status` before each call (0 on
success, an errno otherwise); pass `NULL` to ignore it. All sequencing state
lives in `ctx`, which typically also stashes the status so `gen` can read it.

```c
struct ig_ctx { int step; int prev_status; uint8_t *src; uint8_t *rbuf; };

static int gen(bstack_gen_op_t *out_op, void *userctx)
{
    struct ig_ctx *c = userctx;
    switch (c->step++) {
    case 0:
        out_op->kind = BSTACK_GEN_WRITE;
        out_op->u.write.offset = 0;
        out_op->u.write.data   = c->src;   /* "ABCDE" — must outlive the call */
        out_op->u.write.len    = 5;
        return 1;
    case 1:
        out_op->kind = BSTACK_GEN_READ;    /* sees "ABCDEworld" */
        out_op->u.read.offset = 0;
        out_op->u.read.buf    = c->rbuf;
        out_op->u.read.len    = 10;
        return 1;
    default:
        return 0;                          /* commit */
    }
}

struct ig_ctx c = { 0, 0, src, rbuf };
if (bstack_inplace_gen(bs, gen, &c, &c.prev_status) != 0) { /* handle errno */ }
```

`BSTACK_GEN_ABORT` with `u.abort.status == 0` ends successfully committing
nothing; a non-zero status makes the call fail with that errno. Only a return
of 0 ends the sequence — unlike `bstack_process_gen`, a `-1` return is not
special-cased.
