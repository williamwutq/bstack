# Lock-Free Linked Lists on `BStack`: Avoiding ABA in Push and Pop

This advisory covers external, on-disk singly linked lists built on `BStack`'s
`atomic` and `set` primitives, typically free lists, without an
allocator-level `Mutex`. It identifies which primitive compositions are safe,
which admit ABA, and why. The emphasis is on pop.

## Severity

| Pattern | Severity | Class |
|---|---|---|
| Pop: single `cas` on the head pointer | **Critical** | Silent corruption (ABA) ending in double allocation |
| Pop: `get_batched_gen` (read head, then read `head->next`) followed by a separate `cas` | **Critical** | Same ABA hazard. Harder to spot because the reads look atomic |
| Push: `peek`/`set`/`cas` retry loop | **Low** | Not unsafe. Strictly dominated by `cross_exchange`: more lock acquisitions, spurious retries, no compensating benefit |

The pop patterns corrupt state without ever returning an error. The push
pattern is a style and performance concern only. It is included so its shape
is not mistaken for a template that also applies to pop, where the same shape
is unsound.

## Impact

A "successfully" CAS'd but ABA-corrupted pop hands one on-disk block to two
live allocations, or drops a block out of the list entirely. `cas` returns
`Ok(true)`. No I/O error fires, no panic, no assertion. The fault surfaces
later, in an unrelated place: two allocations alias the same bytes, one
overwrites the other, and the eventual crash points at a different call site.
The bug requires a specific three-party interleaving, so it survives unit
tests and most stress tests, then appears under load on a cadence that defeats
reproduction.

## Anti-patterns

1. **Push via CAS retry loop.** `peek_into` reads the head, `set` writes the
   new node's `next`, and `cas(free_head, …)` attempts the swap, retrying on
   failure. A Treiber-stack-style push. Not unsafe, just inferior.
2. **Pop via single `cas`.** `peek_into(free_head)` reads the head,
   `peek_into(head->next)` reads its successor, and `cas(free_head, head,
   next)` attempts the swap, retrying on failure. The textbook Treiber-stack
   pop, and the textbook ABA bug.
3. **Pop via `get_batched_gen` plus `cas`.** Both reads run under one
   read-lock acquisition inside `get_batched_gen`, the lock is released, and a
   separate `cas` writes the result back into `free_head`. The batched reads
   are mutually consistent. The release-then-reacquire gap before the `cas` is
   not, and that gap is the entire problem.

## What not to do

- Never pop by reading `free_head`, reading `head->next`, and then
  conditionally writing `free_head` with `cas`, regardless of whether the
  reads are two separate `peek_into` calls or one `get_batched_gen` batch. The
  read step and the write step are always split across a lock release and a
  later reacquisition. That boundary is the bug.
- A `cas` returning `true` proves only that the bytes match now. It cannot
  distinguish "nothing happened" from "the value cycled back through a
  different history," which is what ABA is.
- Batching the dependent reads with `get_batched_gen` only makes the reads
  mutually consistent. The vulnerable gap was never between the two reads. It
  sits between the last read and the eventual write, and batching the reads
  does not touch it.
- For push, prefer `cross_exchange` to a CAS loop, not for safety but for
  cost: `cross_exchange` performs the splice in a single acquisition that
  cannot fail, while a CAS loop needs up to three acquisitions per attempt and
  may spin under contention for nothing in return.

## Vulnerable Code Example

### (a) Pop via single CAS

```rust
fn pop_single_cas(stack: &BStack) -> io::Result<Option<u64>> {
    loop {
        let mut head_buf = [0u8; 8];
        stack.peek_into(FREE_HEAD_OFFSET, &mut head_buf)?;   // read free_head; lock released after this call
        let head = u64::from_le_bytes(head_buf);
        if head == SENTINEL { return Ok(None); }

        let mut next_buf = [0u8; 8];
        stack.peek_into(head, &mut next_buf)?;              // read head->next; a separate acquisition

        if stack.cas(FREE_HEAD_OFFSET, head_buf, next_buf)? {
            return Ok(Some(head));                          // cas succeeded; looks sound, is not
        }
    }
}
```

### (b) Pop via `get_batched_gen` plus CAS: the same bug, better disguised

```rust
fn pop_batched_then_cas(stack: &BStack) -> io::Result<Option<u64>> {
    loop {
        let mut head_buf = [0u8; 8];
        let mut next_buf = [0u8; 8];
        let mut head_val = SENTINEL;
        let mut step = 0usize;

        stack.get_batched_gen(|| {                          // one read lock covers both reads; fine so far
            let item = match step {
                0 => Some((FREE_HEAD_OFFSET, &mut head_buf[..])),
                1 => {
                    head_val = u64::from_le_bytes(head_buf);
                    if head_val == SENTINEL { None }
                    else { Some((head_val, &mut next_buf[..])) }
                }
                _ => None,
            };
            step += 1;
            item
        })?;
        // the read lock is released here; anything can happen to the list before the next line

        if head_val == SENTINEL { return Ok(None); }

        if stack.cas(FREE_HEAD_OFFSET, head_val.to_le_bytes(), next_buf)? {
            return Ok(Some(head_val));                      // the gap is between here and the batch above
        }
    }
}
```

### (c) Push via CAS retry loop: sound, but pointless

```rust
fn push_cas_retry(stack: &BStack, b_addr: u64) -> io::Result<()> {
    loop {
        let mut head_buf = [0u8; 8];
        stack.peek_into(FREE_HEAD_OFFSET, &mut head_buf)?;          // H = free_head
        stack.set(b_addr, head_buf)?;                               // b->next = H; a separate acquisition
        if stack.cas(FREE_HEAD_OFFSET, head_buf, b_addr.to_le_bytes())? {
            return Ok(());                                          // free_head: H to b_addr
        }
    }
}
```
This is sound. `b` is private to the freeing thread until it is linked in, so
there is no stale-identity hazard: a failed CAS simply means a fresher `H` and
another attempt. It is shown only to contrast with (a) and (b): the same
"read, compute, CAS" shape that is sound for push is unsound for pop. It is
also strictly worse than the `cross_exchange` push below, which needs one
acquisition that always succeeds, against up to three acquisitions here plus
unbounded retries under contention.

## Why this is dangerous

(a) and (b) both reduce to the same sequence: read `free_head`, read
`head->next`, release the lock, reacquire it, then CAS `free_head`. The
release-and-reacquire gap is where ABA lives.

Interleaving, starting from `free_head` pointing through `H0`, `H1`, `H2`:

1. Thread A reads `free_head == H0` and `H0->next == H1`. Lock released.
2. Thread B pops `H0`, so `free_head` becomes `H1`. `H0` is now live.
3. Thread B pops `H1`, so `free_head` becomes `H2`. `H1` is now live.
4. Thread B frees `H0`: writes `H0->next = H2` and splices `H0` to the front.
   The list is now `free_head → H0 → H2 → …`.
5. Thread A's `cas(FREE_HEAD_OFFSET, H0, H1)` reads `free_head`, finds `H0`,
   the same bytes it captured in step 1, and writes `H1`.

`cas` returns `Ok(true)`. Thread A believes it popped `H0`, leaving
`free_head → H1 → …`. In fact:

- `H1` is still live, assigned in step 3. The next `alloc` hands it out again,
  so two allocations end up aliasing the same bytes.
- `H2` drops out of the reachable list. It leaks, because Thread A's write
  overwrote the `H0 → H2` link Thread B had just built with a stale `H1`
  pointer that no longer matches the list's structure.

Nothing errored. `cas` did exactly what it promises: it compared the bytes,
found them equal, and performed the exchange. Its underlying assumption is
that byte equality across two points in time implies nothing of consequence
happened in between. That assumption is exactly what ABA falsifies.
`free_head` did not stay `H0`; it *returned* to `H0` after a complete
pop/pop/push cycle that left the live/free partition unrecognizable. A
retry-on-failure policy would not help here, because this CAS never failed.

Batching the reads in (b) changes nothing structural. It makes `head_buf` and
`next_buf` mutually consistent at read time, but the vulnerable gap was never
between the two reads. It sits between the last read and the write, and
batching the reads leaves that gap exactly where it was.

## Recommended Implementation

Never split the dependent steps of push or pop across more than one lock
acquisition. Every step must require the same held lock, so the interleaving
above is excluded by construction rather than merely made unlikely.

### Push: one-shot splice via `cross_exchange`

`cross_exchange(a, b, n)` swaps two byte ranges under a single write lock: it
reads both, writes both, and does so atomically, with no failure mode and
nothing to retry. To push freed block `b` at payload offset `b_addr`:

1. **Plant a self-pointer placeholder.**
   `stack.set(b_addr, b_addr.to_le_bytes())` seeds `b->next` with an in-bounds
   value that no reader will ever observe.
2. **Splice.** `stack.cross_exchange(b_addr, FREE_HEAD_OFFSET, 8)`. Before the
   call, slot `b_addr` holds `b_addr` and `FREE_HEAD_OFFSET` holds the current
   head `H`. The swap runs under one write lock, so afterward
   `FREE_HEAD_OFFSET` holds `b_addr`, publishing `b` as the head, and
   `b_addr` holds `H`, completing `b->next = H`, in one indivisible step. The
   placeholder from step 1 is never observed.

One acquisition per push. It always succeeds and needs no retry loop: it is
strictly better than (c) on every axis.

### Pop: read, read, write under one held lock via `process_gen`

`process_gen` acquires the write lock before the first read and holds it,
unreleased, through every subsequent read and the single terminating `Write`
or `Swap`. Drive it through a three-step state machine:

- *Step 0*: `Read { offset: FREE_HEAD_OFFSET, buf: head_buf }`.
- *Step 1*: decode `head_buf`. If it equals `SENTINEL`, end with `None` (the
  list is empty). Otherwise issue `Read { offset: head, buf: next_buf }`.
- *Step 2*: `Write { offset: FREE_HEAD_OFFSET, data: next_buf }`, ending the
  sequence. The caller now exclusively owns `head`.

All three steps run under the single acquisition `process_gen` takes up
front. Reading `free_head`, reading its `next`, and writing `free_head` back
form one indivisible critical section. There is no release-and-reacquire
boundary, so there is no window for a competing pop/pop/push cycle, and the
byte comparison that CAS relies on, the one ABA defeats, becomes unnecessary:
nothing can change between the read and the write, because nothing can run in
between.

## Remediation Code Example

Minimal mutex-free push and pop. The full worked version, with a concurrency
demo and list-rendering helpers, is `examples/atomic_linked_list.rs`:

```rust
const SENTINEL: u64 = u64::MAX;
const FREE_HEAD_OFFSET: u64 = 0;

/// One acquisition. Cannot fail. Nothing to retry.
fn push(stack: &BStack, b_addr: u64) -> io::Result<()> {
    stack.set(b_addr, b_addr.to_le_bytes())?;
    stack.cross_exchange(b_addr, FREE_HEAD_OFFSET, 8)
}

/// Single read, read, write sequence under one held write lock. No CAS, no ABA window.
fn pop(stack: &BStack) -> io::Result<Option<u64>> {
    let mut head_buf = [0u8; 8];
    let mut next_buf = [0u8; 8];
    let mut step = 0usize;
    let mut popped: Option<u64> = None;

    stack.process_gen(|| {
        let op = match step {
            0 => Some(BStackGenOp::Read {
                offset: FREE_HEAD_OFFSET,
                buf: unsafe { core::mem::transmute::<&mut [u8], _>(&mut head_buf[..]) },
            }),
            1 => {
                let head = u64::from_le_bytes(head_buf);
                if head == SENTINEL {
                    None
                } else {
                    popped = Some(head);
                    Some(BStackGenOp::Read {
                        offset: head,
                        buf: unsafe { core::mem::transmute::<&mut [u8], _>(&mut next_buf[..]) },
                    })
                }
            }
            2 => Some(BStackGenOp::Write {
                offset: FREE_HEAD_OFFSET,
                data: unsafe { core::mem::transmute::<&[u8], _>(&next_buf[..]) },
            }),
            _ => None,
        };
        step += 1;
        op
    })?;

    Ok(popped)
}
```

(The `transmute` calls only extend the buffers' borrows to satisfy
`process_gen`'s `'a` bound. `head_buf` and `next_buf` are stack locals
captured by reference, and the call is synchronous, so they provably outlive
it. This is the same pattern used in `examples/atomic_linked_list.rs`.)

## Proof: ABA is structurally excluded

**Claim.** No execution of the recommended push/pop pair admits ABA.

**Definition.** An *ABA window* is an interval bounded by a lock release and a
later reacquisition of the same lock, in which a thread carries a value read
before the release into a decision committed after the reacquisition, while
other threads are free, inside that interval, to mutate the structure so the
value recurs without the structure being equivalent. Each anti-pattern above
contains exactly this interval: lock, read, unlock, then later lock, compare,
write, unlock.

**Lemma 1 (single acquisition).** Pop executes as one `process_gen` call,
which acquires `self.lock.write()` once on entry (`src/lib.rs:2655`) and
releases it only after its terminating write is synced. Push executes as one
`cross_exchange` call, which acquires that same `self.lock.write()` for its
full duration (`src/lib.rs:2430`). No other operation mutates `free_head` or
any node's `next` pointer (`README.md:1226-1233`). `RwLock` serializes
writers, so for the duration of any push or pop, no other push or pop can
execute any step.

**Lemma 2 (read validity at write time).** By Lemma 1, the interval between
the read of `free_head` in pop's step 0 and the write in step 2 contains no
step of any other push or pop. The byte sequence at offset `head`, read into
`next_buf` in step 1, is therefore unchanged at the moment step 2 writes it.
`next_buf` equals `head->next` both at read time and at write time, since the
interval between those two events contains no operation able to alter it.

**Conclusion.** ABA requires two participants inside one window: a thread
carrying a value across a release-and-reacquisition boundary, and a
concurrent sequence that returns the structure to a byte-identical but
non-equivalent state inside that boundary. By Lemma 1, the recommended pop
contains no such boundary: its read and write form a single critical section.
By Lemma 2, the value it commits cannot have changed between read and write.
The premise a CAS-based design depends on, that a freshly reread value might
differ from a previously read one, cannot arise here, since there is no
second read to differ from the first. ABA is excluded by the absence of its
structural precondition, not by a runtime check that detects and rejects it
after the fact.

This is the same distinction the crate draws between `get_batched_gen`, which
takes only the read lock and is documented as a multi-read primitive rather
than a read-modify-write one (`src/lib.rs:1993-2060`), and `process_gen`,
which always takes the write lock, even for sequences that end up read-only,
because the decision to write can only be made after seeing earlier reads
(`src/lib.rs:2624-2630`, `README.md:1247-1252`).
