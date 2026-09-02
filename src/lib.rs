//! A persistent, fsync-durable binary stack backed by a single file.
//!
//! # Overview
//!
//! [`BStack`] treats a file as a flat byte buffer that grows and shrinks from
//! the tail.  Every mutating operation — [`push`](BStack::push),
//! [`extend`](BStack::extend), [`pop`](BStack::pop), [`discard`](BStack::discard), (with the `set`
//! feature) [`set`](BStack::set), [`zero`](BStack::zero), and
//! [`repeat`](BStack::repeat), (with the `atomic` feature)
//! [`replace`](BStack::replace), and (with both `set` and `atomic`)
//! [`process`](BStack::process) — calls a *durable sync* before returning,
//! so the data survives a process crash or an unclean system shutdown.
//! Read-only operations — [`peek`](BStack::peek),
//! [`peek_into`](BStack::peek_into), [`get`](BStack::get), and
//! [`get_into`](BStack::get_into) — never modify the file and on Unix and
//! Windows can run concurrently with each other.
//! [`pop_into`](BStack::pop_into) is the buffer-passing counterpart of `pop`,
//! carrying the same durability and atomicity guarantees.
//! [`discard`](BStack::discard) is like `pop` but discards the removed bytes
//! without reading or returning them, avoiding any allocation or copy.
//!
//! The crate depends on **`libc`** (Unix) and **`windows-sys`** (Windows) for
//! platform-specific syscalls, and uses **no `unsafe` code beyond the required
//! FFI calls** and the two lifetime-extending reborrow helpers behind
//! [`bstack_unsafe_reborrow!`] / [`bstack_unsafe_reborrow_mut!`] (see the
//! [`reborrow`] module).
//!
//! # File format
//!
//! Every file begins with a fixed 32-byte header, then the concatenated payload
//! (push 0, push 1, …):
//!
//! ```text
//!   bytes      field
//!   ─────      ─────
//!    0 ..  8   magic[8]
//!    8 .. 16   clen      — committed payload length (u64 LE)
//!   16 .. 24   wip_ptr   — write-in-progress journal target (u64 LE; 0 when idle)
//!   24 .. 32   wip_aux   — write-in-progress journal mode (u64 LE)
//!   32 ..      payload   — push 0, push 1, … concatenated
//! ```
//!
//! * **`magic`** — 8 bytes: `BSTK` + major(1 B) + minor(1 B) + patch(1 B) + reserved(1 B).
//!   This version writes `BSTK\x00\x04\x04\x00` (0.4.4).  [`open`](BStack::open)
//!   accepts any file whose first 6 bytes match `BSTK\x00\x04` (any 0.4.x) and
//!   rejects anything with a different major or minor.
//! * **`clen`** — little-endian `u64` recording the *committed* payload length.
//!   It is updated atomically with each [`push`](BStack::push) or
//!   [`pop`](BStack::pop) and is used for crash recovery on the next
//!   [`open`](BStack::open).
//! * **`wip_ptr` / `wip_aux`** — two little-endian `u64` fields holding the
//!   write-in-progress journal that makes in-place mutations crash-atomic.
//!   `wip_ptr` is the physical offset an interrupted in-place write must be
//!   replayed into (`0` in the steady state); `wip_aux` names the journal mode
//!   (`Set` — verbatim replay of the staged tail; `Repeat` — repeat a staged
//!   pattern; `Copy` — replay a disjoint copy from its still-intact source, of
//!   which only the coordinate is staged; `SpliceGrow`/`SpliceShrink` — a
//!   length-changing tail replace, whose new committed length recovery derives
//!   from the file size and the recorded direction). Recovery interprets them on
//!   [`open`](BStack::open) — see *Crash
//!   recovery*. Legacy 0.1.x files (16-byte header) are upgraded in place by
//!   [`BStack::migrate`].
//!
//! All user-visible offsets are **logical** (0-based from the start of the
//! payload region, i.e. from file byte 32).
//!
//! # Crash recovery
//!
//! On [`open`](BStack::open), recovery first checks the write-in-progress journal
//! (`wip_ptr`); if disarmed, it reconciles the committed length against the file
//! size:
//!
//! | Condition | Cause | Recovery |
//! |-----------|-------|----------|
//! | `wip_ptr != 0`, `wip_aux = Set` | an in-place `set`/`swap`/`cas`/`copy`/`cross_exchange` crashed mid-commit | replay the staged tail verbatim into `[wip_ptr, …)`, disarm, truncate to `32 + clen` |
//! | `wip_ptr != 0`, `wip_aux = Repeat` | a `zero`/`repeat` crashed mid-fill | write `count` copies of the staged pattern into `[wip_ptr, …)`, disarm, truncate |
//! | `wip_ptr != 0`, `wip_aux = Copy` | a disjoint `copy` crashed mid-copy | replay `move_chunked(src → wip_ptr)` from the untouched source (the tail stages only `[src \| n]`), disarm, truncate |
//! | `wip_ptr != 0`, `wip_aux = SpliceGrow`/`SpliceShrink` | a length-changing `atrunc`/`splice`/`splice_into`/`replace` crashed mid-replace | derive `clen'` from the file size and direction, replay the staged new tail into `[wip_ptr, …)`, commit `clen'` while disarming, truncate |
//! | `wip_ptr != 0`, `wip_aux` unrecognized | a mode armed by a newer build | roll back: disarm, truncate to `32 + clen` |
//! | `wip_ptr == 0`, `wip_aux = MultiWrite` | a `set_batched`/`inplace_gen` multi-write batch crashed after all blocks were staged | replay each staged `[s \| e \| data]` block into `[s, e)`, disarm, truncate to `32 + clen` (a corrupt tail rolls back, applying nothing) |
//! | `wip_ptr == 0`, `file_size − 32 > clen` | partial tail write (push, or a crashed journal or multi-write stage) before the header update | truncate to `32 + clen` |
//! | `wip_ptr == 0`, `file_size − 32 < clen` | partial truncation (pop crashed before the header update) | set `clen = file_size − 32` |
//!
//! Each replay is idempotent — the staged tail is immutable and disjoint from
//! its target — so a crash during recovery itself is safe to re-run. After
//! recovery a `durable_sync` ensures the repaired state is on stable storage
//! before any caller can observe or modify the file.
//!
//! # Deferred replay
//!
//! A [`BStack`] stays usable after a write fails, with no reopen: the next write
//! repairs the file before doing anything else, and reads refuse until it has.
//!
//! A write that fails after its first mutating I/O can leave the states above —
//! an armed journal, or a stale tail past the committed length — on a handle
//! that stays open. The failed call returns its error unchanged and the repair
//! is deferred to the next write, through an in-memory `replay_needed` flag
//! under the `BStack` rwlock. While it is set:
//!
//! * **The next write replays first, silently**, before validating its own
//!   arguments — a stale tail would inflate the payload size every mutator
//!   derives from the file end — then proceeds normally. The flag clears only
//!   once the replay succeeds; a failing replay returns its own error.
//! * **Reads fail** with [`InterruptedWrite`], until a write replays or
//!   [`recover`](BStack::recover) is called to replay on its own. This covers
//!   every read that
//!   consults the file or the cached committed length — [`len`](BStack::len),
//!   and a zero-length [`get(n, n)`](BStack::get) too, since it still checks
//!   `n` against the payload size. Unaffected are the calls that return before
//!   taking the lock (`peek_into`/`get_into` on an empty buffer,
//!   `get_batched`/`get_batched_into` on no entries) and lock-free reads of the
//!   immutable locked prefix.
//!
//! # Durability
//!
//! **In-place same-length writes** — [`set`](BStack::set), [`zero`](BStack::zero),
//! [`repeat`](BStack::repeat), [`swap`](BStack::swap),
//! [`swap_into`](BStack::swap_into), [`cas`](BStack::cas), [`copy`](BStack::copy),
//! [`cross_exchange`](BStack::cross_exchange), [`process`](BStack::process),
//! [`set_batched`](BStack::set_batched), [`inplace_gen`](BStack::inplace_gen), and
//! the `crds` family — leave the payload length unchanged and are each
//! **crash-atomic**, committing by one of three strategies (recovered on the next
//! [`open`](BStack::open); see *Crash recovery*):
//!
//! * **Aligned-block write** — when the target lies within one power-fail-atomic
//!   block, a single `write` + `durable_sync` is already all-or-nothing; no
//!   journal is armed.
//! * **Write-in-progress journal** — otherwise: stage a backup past `clen` →
//!   `durable_sync` → arm `wip_ptr` → `durable_sync` → write in place →
//!   `durable_sync` → clear `wip_ptr` → `durable_sync` → `ftruncate` the backup.
//!   `zero`/`repeat` stage only `[count | pattern]`; `cross_exchange` stages one
//!   region and commits at a single atomic `wip_ptr` flip; moves and fills stream
//!   through a bounded buffer (O(1) memory).
//! * **Multi-write journal** — [`set_batched`](BStack::set_batched) and
//!   [`inplace_gen`](BStack::inplace_gen) commit several non-overlapping in-place
//!   writes as one unit: stage every `[s | e | data]` block past `clen` →
//!   `durable_sync` → arm the `MultiWrite` sentinel (`wip_ptr` stays `0`, so it
//!   never collides with a single-region journal) → `durable_sync` → replay each
//!   block in place → `durable_sync` → disarm → `ftruncate`. A batch that reduces
//!   to one write falls back to the single-write strategies above.
//!
//! Below, *commit* denotes whichever of those two strategies applies to the bytes
//! being written; anything before it is read/compare/callback work under the lock.
//!
//! | Operation | Syscall sequence |
//! |-----------|-----------------|
//! | `push` | `lseek(END)` → `write(data)` → `lseek(8)` → `write(clen)` → `durable_sync` |
//! | `extend` | `lseek(END)` → `set_len(new_end)` → `lseek(8)` → `write(clen)` → `durable_sync` |
//! | `extend_sparse`, `extend_sparse_batched` | `set_len(new_end)` → `write` each buffer into the grown region (gaps left zero) → `lseek(8)` → `write(clen)` → `durable_sync` |
//! | `pop`, `pop_into` | `lseek` → `read` → `ftruncate` → `lseek(8)` → `write(clen)` → `durable_sync` |
//! | `discard` | `ftruncate` → `lseek(8)` → `write(clen)` → `durable_sync` |
//! | `set` *(feature)* | *commit* `data` |
//! | `zero`, `repeat` *(feature)* | *commit* the repeated pattern (the journal stages only `[count \| pattern]`) |
//! | `atrunc` *(feature: atomic)* | dispatch on the tail-replace shape: pure truncation → `ftruncate` → *commit* `clen`; pure append → `set_len(new_end)` → `write(buf)` → `durable_sync` → *commit* `clen`; same-length → *commit* `buf` in place; length change → **splice journal** (stage the new tail past the payload → arm `SpliceGrow`/`SpliceShrink` → replay into place → atomically commit `clen'` + disarm → truncate, a `durable_sync` at each barrier) |
//! | `splice`, `splice_into` *(feature: atomic)* | `lseek(tail)` → `read(n)` → *(then as `atrunc`)* |
//! | `try_extend` *(feature: atomic)* | `lseek(END)` — conditional `push` sequence if size matches |
//! | `try_discard` *(feature: atomic)* | `lseek(END)` — conditional `discard` sequence if size matches |
//! | `try_extend_zeros` *(feature: atomic)* | `lseek(END)` — conditional `extend(n)` sequence if size matches |
//! | `try_extend_sparse`, `try_extend_sparse_batched` *(feature: atomic)* | `lseek(END)` — conditional `extend_sparse` / `extend_sparse_batched` sequence if size matches |
//! | `swap`, `swap_into` *(features: set+atomic)* | `read` old bytes → *commit* `buf` |
//! | `cas` *(features: set+atomic)* | `read` → compare — conditional *commit* of `new` |
//! | `process` *(features: set+atomic)* | `read(start..end)` → *(callback)* → *commit* the buffer |
//! | `process_gen` *(features: set+atomic)* | closure-driven reads, ending in at most one mutating step: `Write` *commits*; `Swap` uses the exchange journal (as `cross_exchange`); `Push`/`Pop`/`Discard`/`Atrunc`/`Splice`/`Sparse` behave as their standalone forms |
//! | `set_batched` *(features: set+atomic)* | validate + reject overlap → **multi-write journal**: stage every `[s \| e \| data]` block past `clen` → arm the `MultiWrite` sentinel (`wip_ptr` stays `0`) → replay each block in place → disarm → `ftruncate` (a `durable_sync` at each barrier); a lone effective write takes the ordinary single-write *commit* |
//! | `inplace_gen` *(features: set+atomic)* | closure-driven reads (each overlaid with the batch-so-far edits) interleaved with accumulated `Write`s (later overrides earlier on overlap); on `None` the pending edits commit together via the multi-write journal (as `set_batched`) |
//! | `replace` *(feature: atomic)* | `lseek(tail)` → `read(n)` → *(callback)* → *(then as `atrunc`)* |
//! | `cross_exchange` *(features: set+atomic)* | `read(a)`, `read(b)` → exchange journal: stage `a` → arm at `a` → write `b`→`a` → flip `wip_ptr` to `b` → write `a`→`b` → disarm → `ftruncate` (a `durable_sync` at each barrier) |
//! | `copy` *(features: set+atomic)* | same-location → no-op; single-block dest → *commit*; overlapping → stream source→tail→dest (`Set` journal); disjoint → **copy journal** (stage only `[src \| n]` → arm `Copy` → stream source→dest → disarm; recovery replays from the untouched source) |
//! | `eq_crds`, `ne_crds` *(features: set+atomic)* | `read(a)` → compare — conditional *commit* of `b_buf` |
//! | `masked_eq_crds`, `masked_ne_crds` *(features: set+atomic)* | `read(a)` → mask+compare — conditional *commit* of `b_buf` |
//! | `peek`, `peek_into`, `get`, `get_into`, `get_batched`, `get_batched_into`, `get_batched_gen` | `pread(2)` on Unix; `ReadFile`+`OVERLAPPED` on Windows; `lseek` → `read` elsewhere (no sync — read-only) |
//!
//! **`durable_sync` on macOS** issues `fcntl(F_FULLFSYNC)`, which flushes the
//! drive's hardware write cache.  Plain `fdatasync` is not sufficient on macOS
//! because the kernel may acknowledge it before the drive controller has
//! committed the data.  If `F_FULLFSYNC` is not supported by the device the
//! implementation falls back to `sync_data` (`fdatasync`).
//!
//! **`durable_sync` on other Unix** calls `sync_data` (`fdatasync`), which is
//! sufficient on Linux and BSD.
//!
//! **`durable_sync` on Windows** calls `sync_data`, which maps to
//! `FlushFileBuffers`.  This flushes the kernel write-back cache and waits for
//! the drive to acknowledge, providing equivalent durability to `fdatasync`.
//!
//! The debug-only `debug-no-sync` Cargo feature skips `durable_sync` entirely
//! (writes still happen, just unsynced) for faster fault-injection test
//! iteration. Not for production use.
//!
//! # Multi-process safety
//!
//! On Unix, [`open`](BStack::open) acquires an **exclusive advisory `flock`**
//! on the file (`LOCK_EX | LOCK_NB`).  If another process already holds the
//! lock, `open` returns immediately with [`io::ErrorKind::WouldBlock`] rather
//! than blocking indefinitely.  The lock is released automatically when the
//! [`BStack`] is dropped (the underlying file descriptor is closed).
//!
//! On Windows, [`open`](BStack::open) acquires an **exclusive `LockFileEx`**
//! lock (`LOCKFILE_EXCLUSIVE_LOCK | LOCKFILE_FAIL_IMMEDIATELY`) covering the
//! entire file range.  If another process already holds the lock, `open`
//! returns immediately with [`io::ErrorKind::WouldBlock`]
//! (`ERROR_LOCK_VIOLATION`).  The lock is released when the [`BStack`] is
//! dropped (the underlying file handle is closed).
//!
//! > **Note:** Both `flock` (Unix) and `LockFileEx` (Windows) are advisory
//! > and per-process.  They prevent well-behaved concurrent opens across
//! > processes but do not protect against processes that bypass the lock or
//! > against raw writes to the file.
//!
//! # Correct usage
//!
//! bstack files must only be opened through this crate or a compatible
//! implementation that understands the file format, the header protocol, and
//! the locking semantics.  Reading or writing the underlying file with raw
//! tools or syscalls while a [`BStack`] instance is live — or manually editing
//! the header fields — can silently corrupt the committed-length sentinel or
//! bypass the advisory lock.
//!
//! **The authors make no guarantees about the behaviour of this crate —
//! including freedom from data loss or logical corruption — when the file has
//! been accessed outside of this crate's controlled interface.**
//!
//! # Thread safety
//!
//! `BStack` wraps the file in a [`std::sync::RwLock`]. The committed payload
//! length is also cached in memory and kept in sync with the on-disk header
//! by every write-lock-held operation, so [`len`](BStack::len) and
//! [`is_empty`](BStack::is_empty) can be answered under the read lock without
//! any `File::metadata` syscall.
//!
//! | Operation | Lock (Unix / Windows) | Lock (other) |
//! |-----------|-----------------------|--------------|
//! | `push`, `extend`, `extend_sparse`, `extend_sparse_batched`, `pop`, `pop_into`, `discard` | write | write |
//! | `set`, `zero`, `repeat` *(feature)* | write | write |
//! | `atrunc`, `splice`, `splice_into`, `try_extend`, `try_extend_zeros`, `try_extend_sparse`, `try_extend_sparse_batched` *(feature: atomic)* | write | write |
//! | `try_discard(s, n > 0)` *(feature: atomic)* | write | write |
//! | `try_discard(s, 0)` *(feature: atomic)* | **read** | **read** |
//! | `get_batched`, `get_batched_into`, `get_batched_gen` *(feature: atomic)* | **read** | write |
//! | `swap`, `swap_into`, `cas` *(features: set+atomic)* | write | write |
//! | `cross_exchange`, `copy`, `process`, `process_gen`, `set_batched`, `inplace_gen` *(features: set+atomic)* | write | write |
//! | `eq_crds`, `ne_crds`, `masked_eq_crds`, `masked_ne_crds` *(features: set+atomic)* | write | write |
//! | `replace` *(feature: atomic)* | write | write |
//! | `peek`, `peek_into`, `get`, `get_into` | **read** | write |
//! | `len` | read | read |
//!
//! On Unix and Windows, `peek`, `peek_into`, `get`, and `get_into` use a
//! cursor-safe positional read (`pread(2)` on Unix; `ReadFile` with
//! `OVERLAPPED` on Windows) that does not modify the file-position cursor.
//! This allows multiple concurrent calls to any of these methods to run in
//! parallel while any ongoing `push`, `pop`, or `pop_into` still serialises
//! all writers via the write lock.  For [`get`](BStack::get) and
//! [`get_into`](BStack::get_into), reads that lie entirely within the
//! [locked region](#locked-region-lock_up_to) bypass the rwlock — see that
//! section for the concurrency model.
//!
//! On other platforms a seek is required, so `peek`, `peek_into`, `get`, and
//! `get_into` fall back to the write lock and all reads serialise.
//!
//! Unlike [`get_batched_gen`](BStack::get_batched_gen), which only ever takes
//! the **read** lock (Unix/Windows), [`process_gen`](BStack::process_gen) and
//! [`inplace_gen`](BStack::inplace_gen) *always* take the **write** lock — even
//! for sequences that turn out to be read-only and end in `None` — because the
//! closure may decide, only after seeing earlier reads, to mutate; the lock
//! therefore has to be acquired before the first read so the whole sequence
//! runs as one indivisible step.
//!
//! # Locked region (`lock_up_to`)
//!
//! [`BStack`] maintains an in-memory **monotonically growing partition
//! boundary** named the *locked region*.  Bytes in `[0, locked_len())` are
//! declared permanently immutable for the lifetime of the open file.
//!
//! The locked length starts at `0` on every [`open`](BStack::open) and is
//! **not persisted to disk** — the file format is unchanged.  Callers extend
//! the boundary by calling [`lock_up_to`](BStack::lock_up_to) (or open and
//! lock in one step with [`open_locked_up_to`](BStack::open_locked_up_to)).
//! It can only grow; attempts to shrink it return
//! [`io::ErrorKind::InvalidInput`].
//!
//! Opening with [`open_cached`](BStack::open_cached) (or
//! [`open_locked_up_to_cached`](BStack::open_locked_up_to_cached)) enables
//! an in-memory mirror of the locked region: each `lock_up_to` call reads the
//! newly locked bytes from disk into a heap buffer, and subsequent reads whose
//! range falls entirely within the cached region are served with no syscall.
//!
//! ## Effects
//!
//! * **`get`/`get_into` fast-path reads.**  When [`get`](BStack::get) or
//!   [`get_into`](BStack::get_into) are called with a range that lies entirely
//!   within the locked region, the internal `RwLock` is bypassed.
//!   - On **non-cached** stacks (Unix/Windows), reads are lock-free and use
//!     `pread(2)` (Unix) or `ReadFile` + `OVERLAPPED` (Windows).
//!   - On **cached** stacks (all platforms), reads are served from the
//!     in-memory buffer under a `Mutex` (so RwLock-free, but not lock-free).
//!     The `fstat` size check is skipped on this path — the locked length is a
//!     sufficient upper bound.
//!
//! * **Write protection.**  [`set`](BStack::set), [`zero`](BStack::zero),
//!   [`repeat`](BStack::repeat),
//!   [`swap`](BStack::swap), [`swap_into`](BStack::swap_into),
//!   [`cas`](BStack::cas), [`process`](BStack::process),
//!   [`cross_exchange`](BStack::cross_exchange), [`copy`](BStack::copy)
//!   (destination only), [`eq_crds`](BStack::eq_crds),
//!   [`ne_crds`](BStack::ne_crds), [`masked_eq_crds`](BStack::masked_eq_crds),
//!   and [`masked_ne_crds`](BStack::masked_ne_crds) (region B) return
//!   [`io::ErrorKind::InvalidInput`] when their write target range overlaps
//!   the locked region.  [`atrunc`](BStack::atrunc), [`splice`](BStack::splice),
//!   [`splice_into`](BStack::splice_into), and [`replace`](BStack::replace)
//!   return the same error when the operation would modify bytes inside it.
//!
//! * **Shrink protection.**  [`pop`](BStack::pop),
//!   [`pop_into`](BStack::pop_into), [`discard`](BStack::discard), and
//!   [`try_discard`](BStack::try_discard) return
//!   [`io::ErrorKind::InvalidInput`] when they would shrink the payload
//!   below the locked length.
//!
//! Callers that never invoke `lock_up_to` see no behavioural change — every
//! read and write path adds only a single uncontended `AtomicU64::load` and
//! a comparison.
//!
//! ## Concurrency model
//!
//! `lock_up_to(n)` acquires the exclusive write lock before publishing the
//! new boundary with a `Release` store.  Locked-region fast-path readers
//! `Acquire`-load `locked` before each call.  Two consequences follow:
//!
//! * A stale load is always safe.  If a reader sees an older (smaller)
//!   `locked` value, it falls through to the rwlock path; if it sees a
//!   newer value, the entire range it now reads is by definition immutable.
//!
//! * Locked-region checks on writers are evaluated **under the write lock**,
//!   so they cannot race against a concurrent `lock_up_to` extending the
//!   boundary across the write target.
//!
//! On cached stacks the cache `Mutex` is acquired and fully populated
//! *before* `locked` is advanced with the `Release` store.  A reader that
//! `Acquire`-loads `locked` and then locks the cache `Mutex` therefore always
//! sees a buffer whose valid range covers at least `[0, locked)`.
//!
//! ## Typical use
//!
//! ```no_run
//! use bstack::BStack;
//!
//! # fn main() -> std::io::Result<()> {
//! // A fixed 64-byte metadata block at the head of the file, read by many
//! // threads but never modified after first write.
//! let stack = BStack::open_locked_up_to("meta.bin", 64)?;
//! assert_eq!(stack.locked_len(), 64);
//!
//! // Reads of the metadata bypass the rwlock on Unix and Windows.
//! let header = stack.get(0, 64)?;
//! # let _ = header;
//! # Ok(())
//! # }
//! ```
//!
//! On cached stacks this locked-region fast path is available on all
//! platforms (served from the cache under a `Mutex`).
//!
//! # Standard I/O adapters
//!
//! ## Writing
//!
//! `BStack` implements [`std::io::Write`] (and so does `&BStack`, mirroring
//! [`std::io::Write` for `&File`]).  Each call to `write` is forwarded to
//! [`push`](BStack::push), so every write is atomically appended and durably
//! synced before returning.  `flush` is a no-op.
//!
//! ```no_run
//! use std::io::Write;
//! use bstack::BStack;
//!
//! # fn main() -> std::io::Result<()> {
//! let mut stack = BStack::open("log.bin")?;
//! stack.write_all(b"hello")?;
//! stack.write_all(b"world")?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Reading
//!
//! [`BStackReader`] wraps a `&BStack` with a cursor and implements
//! [`std::io::Read`] and [`std::io::Seek`].  Use [`BStack::reader`] or
//! [`BStack::reader_at`] to construct one.
//!
//! ```no_run
//! use std::io::{Read, Seek, SeekFrom};
//! use bstack::BStack;
//!
//! # fn main() -> std::io::Result<()> {
//! let stack = BStack::open("log.bin")?;
//! stack.push(b"hello world")?;
//!
//! let mut reader = stack.reader();
//! let mut buf = [0u8; 5];
//! reader.read_exact(&mut buf)?;  // b"hello"
//! reader.seek(SeekFrom::Start(6))?;
//! reader.read_exact(&mut buf)?;  // b"world"
//! # Ok(())
//! # }
//! ```
//!
//! # Trait implementations
//!
//! ## `BStack`
//!
//! | Trait | Semantics |
//! |-------|-----------|
//! | `Debug` | Shows `version` (semver string from the magic header, e.g. `"0.4.4"`) and `len` (`Option<u64>`, `None` on I/O failure). |
//! | `PartialEq` / `Eq` | **Pointer identity.** Two values are equal iff they are the same instance. No two distinct `BStack` values in one process can refer to the same file. |
//! | `Hash` | Hashes the instance address — consistent with pointer-identity `PartialEq`. |
//!
//! ## `BStackReader`
//!
//! | Trait | Semantics |
//! |-------|-----------|
//! | `PartialEq` / `Eq` | Equal when both the `BStack` pointer (identity) and the cursor `offset` match. |
//! | `Hash` | Hashes `(BStack pointer, offset)` — consistent with `PartialEq`. |
//! | `PartialOrd` / `Ord` | Ordered by `BStack` instance address, then by cursor `offset`. Groups all readers over the same stack and within that group orders by position. |
//!
//! # Feature flags
//!
//! * **`set`** — In-place overwrite of existing payload bytes without changing
//!   the file size ([`BStack::set`], [`BStack::zero`], [`BStack::repeat`]).
//!
//! * **`alloc`** — Region-based sub-allocation over a `BStack` payload.
//!   Adds the allocator traits, handle types ([`BStackRange`],
//!   [`BStackOwnedSlice`], [`BStackSlice`], [`BStackChunk`]),
//!   [`LinearBStackAllocator`], and [`DebugCheckingAllocator`].
//!   Combined with `set`, also enables [`BStackSliceWriter`],
//!   [`FirstFitBStackAllocator`], [`GhostTreeBstackAllocator`],
//!   [`SlabBStackAllocator`], [`CheckedSlabBStackAllocator`],
//!   [`SegregatedBStackAllocator`] (experimental), and [`BStackByteVec`].
//!
//! * **`atomic`** — Compound read-modify-write operations that hold the write
//!   lock across what would otherwise be separate calls.  Combined with `set`,
//!   also enables atomic swap, CAS, in-place batch writes, and cross-region
//!   operations.
//!
//! Enable with:
//!
//! ```toml
//! [dependencies]
//! bstack = { version = "0.4", features = ["set"] }
//! # or
//! bstack = { version = "0.4", features = ["alloc"] }
//! # or both
//! bstack = { version = "0.4", features = ["alloc", "set"] }
//! ```
//!
//! # Allocator (`alloc` feature)
//!
//! The `alloc` feature adds a region-management layer on top of [`BStack`].
//!
//! ## Key types
//!
//! * [`BStackAllocator`] — trait for types that own a [`BStack`] and manage
//!   contiguous byte regions within its payload.  Requires `stack()`,
//!   `into_stack()`, `alloc()`, and `realloc()`; provides a default no-op
//!   `dealloc()` and delegation helpers `len()` / `is_empty()`.
//!
//! * [`BStackBulkAllocator`] — extension trait for [`BStackAllocator`] that
//!   adds atomic bulk operations.  Both methods are required with no default; on error
//!   the backing store is left unchanged unless a crash occur.
//!
//! * [`BStackUninitAllocator`] — opt-in extension trait for [`BStackAllocator`]
//!   whose `alloc_uninit` / `realloc_uninit` skip zero-initialising newly
//!   allocated or grown bytes.  The returned bytes are **unspecified** (leftover
//!   from a prior allocation) but always valid to read, saving the zero-fill
//!   write for callers that overwrite the region before reading it.  Existing
//!   bytes are preserved exactly as `realloc`.  Implementing it is optional and
//!   signals that the allocator actually has a cheaper uninitialised path.
//!   Implemented by [`SlabBStackAllocator`], [`GhostTreeBstackAllocator`],
//!   [`CheckedSlabBStackAllocator`], [`SegregatedBStackAllocator`] and
//!   [`FirstFitBStackAllocator`], and forwarded by [`DebugCheckingAllocator`]
//!   when its inner allocator implements it.
//!   [`LinearBStackAllocator`] deliberately does not: a bump allocator only ever
//!   hands out freshly extended tail, whose zeroes cost no write I/O.
//!
//! * [`BStackAllocError`]`<'a, A>` — error returned by `realloc` / `dealloc`.
//!   Carries the failing `source` plus `handle: Option<A::Allocated<'a>>`, the
//!   surviving allocation handed back to the caller so a failed resize/free is
//!   not a silent leak.  [`BStackBulkAllocError`] is its `dealloc_bulk`
//!   counterpart, returning a `Vec` of the handles it did not free.
//!
//! * [`BStackRange`] — raw `(offset, len)` pair; `Copy`, no pointer, no I/O.
//!   Serialises to/from `[u8; 16]` for persistent bookkeeping.
//!
//! * [`BStackOwnedSlice`]`<'a, A>` — ownership handle returned by `alloc` /
//!   `realloc`.  Non-Copy, non-Clone; owns the allocation lifetime `'a`.
//!   Exposes `as_slice()` / `as_slice_mut()` to obtain a borrowed view, and also
//!   provides convenience `read*` / `write*` / `zero*` methods that delegate via
//!   those views. Passed by value to `realloc` and `dealloc`; Drop is a no-op.
//!
//! * [`BStackSlice`]`<'a>` — borrowed I/O view over a region.  Non-Copy;
//!   obtained from `BStackOwnedSlice::as_slice[_mut]()` or directly from
//!   `BStackSlice::from_raw_parts`.  Exposes `read`, `read_into`,
//!   `read_range_into`, `subslice`, `subslice_range`, `reader`, `reader_at`,
//!   and (with the `set` feature) `write`, `write_range`, `zero`, `zero_range`.
//!
//! * [`BStackSliceReader`]`<'a>` — cursor-based reader over a
//!   [`BStackSlice`], implementing [`io::Read`] and [`io::Seek`] in the
//!   slice's coordinate space.
//!
//! * [`LinearBStackAllocator`] — reference bump allocator that appends regions
//!   sequentially.  `realloc` is O(1) for the tail allocation and returns
//!   `Unsupported` for non-tail slices.  `dealloc` reclaims the tail via
//!   [`BStack::discard`] (or [`BStack::try_discard`] with `atomic`); non-tail
//!   deallocations are a no-op.  Every operation maps to exactly one [`BStack`]
//!   call and is crash-safe by inheritance.  `Send` in all configurations;
//!   also `Sync` with the `atomic` feature.  Implements [`BStackAllocator`]
//!   and [`BStackBulkAllocator`].
//!
//! * [`FirstFitBStackAllocator`] — A persistent first-fit free-list allocator
//!   that reuses freed regions to prevent unbounded file growth.  Requires both
//!   `alloc` and `set` features.  `Send` in all configurations; also `Sync`
//!   with the `atomic` feature, where an internal `Mutex` serializes free-list
//!   mutation and stack extension.
//!
//! * [`GhostTreeBstackAllocator`] — A pure-AVL general-purpose allocator with
//!   zero-overhead live allocations.  Free blocks store their AVL node inline,
//!   and the tree is keyed on `(size, address)` for best-fit allocation.
//!   Provides O(log n) allocation and deallocation with crash recovery through
//!   tree rebalancing on mount.  Requires both `alloc` and `set` features.
//!   `Send` in all configurations; `Send + Sync`
//!   with the `atomic` feature, where an internal `Mutex` serialises AVL tree
//!   mutations.
//!
//! * [`SlabBStackAllocator`] — Fixed-block slab allocator.
//!   All blocks are exactly `block_size` bytes with no per-block header or footer;
//!   freed blocks are tracked via an intrusive singly-linked free list stored in
//!   the first 8 bytes of each free block.  O(1) alloc and dealloc.
//!   Use [`SlabBStackAllocator::new`] to initialise an empty stack and
//!   [`SlabBStackAllocator::open`] to reopen an existing one.
//!   Requires both `alloc` and `set` features; with `atomic` additionally
//!   implements [`BStackBulkAllocator`] (`alloc_bulk`/`dealloc_bulk`).
//!
//! * [`CheckedSlabBStackAllocator`] — Crash-recoverable
//!   variant of [`SlabBStackAllocator`].  Prefixes every block with an 8-byte
//!   overhead field (zero when free, high bit set with a block count when in
//!   use) so leaked blocks are recoverable by a linear scan and double-frees
//!   are caught at runtime before the free list can be corrupted.  Constructor
//!   takes `data_size` (usable bytes per block, ≥ 8); the on-disk `block_size`
//!   is `data_size + 8`.  Use [`CheckedSlabBStackAllocator::new`] to initialise
//!   an empty stack and [`CheckedSlabBStackAllocator::open`] to reopen one
//!   ([`open`](CheckedSlabBStackAllocator::open) runs
//!   [`recover`](CheckedSlabBStackAllocator::recover) automatically).
//!   Requires both `alloc` and `set` features; with `atomic` additionally
//!   implements [`BStackBulkAllocator`] (`alloc_bulk`/`dealloc_bulk`).
//!
//! * [`SegregatedBStackAllocator`] — **experimental** segregated (binned)
//!   free-list allocator.  Generalises [`CheckedSlabBStackAllocator`] from one
//!   block size to 33 size classes sharing a single arena: 16 linear classes
//!   (16‥256 B, step 16), 16 geometric classes (320‥4096 B, 4 per octave), and
//!   one shared oversized bucket.  Each class is an independent intrusive free
//!   list; the class is computed from the request with register arithmetic (no
//!   tables), giving O(1) classed alloc/dealloc.  Every block carries the same
//!   8-byte overhead tag as the checked slab, so leaked blocks are reclaimable by
//!   a linear scan and double-frees are caught.  A single [`new`](SegregatedBStackAllocator::new)
//!   constructor initialises an empty stack or reopens one (running recovery
//!   automatically). Requires both `alloc` and `set`; `Send` in all
//!   configurations, `Send + Sync` with `atomic` (no allocator-level lock —
//!   free-list splices ride [`BStack::process_gen`]/[`BStack::inplace_gen`]), where
//!   it additionally implements [`BStackBulkAllocator`] (`alloc_bulk`/`dealloc_bulk`,
//!   work bounded by the classes touched, with oversized requests matched
//!   largest-first against the oversized free list).
//!   **Experimental:** the on-disk format and API may change, some resize paths
//!   differ between the `atomic` and non-`atomic` builds, and the deep in-use-leak
//!   GC is not yet implemented (the free-neighbour coalescer, `coalesce`, now is —
//!   `atomic` only).
//!
//! * [`DebugCheckingAllocator<A>`](DebugCheckingAllocator) — transparent debug
//!   wrapper.  Wraps any allocator whose `Allocated` type is [`BStackOwnedSlice`]
//!   and whose `Error` is [`io::Error`].  Tracks allocated and freed regions in
//!   memory and panics on overlapping allocations, double-frees, partial-frees,
//!   and multi-span frees.  When the inner allocator reports a lost handle
//!   (`handle: None` in [`BStackAllocError`]), the region is removed from
//!   tracking entirely — its fate is unknown, so neither "live" nor "freed" would
//!   be correct.  Intended for tests and debugging; O(n) per-operation overhead.
//!   Requires `alloc` only.
//!
//! * [`BStackByteVec`]`<'a, A>` — a growable byte (`u8`) vector backed by a
//!   [`BStack`] allocation (requires `alloc` + `set`).  Mirrors the core
//!   [`Vec<u8>`] API: `new`, `with_capacity`, `from_slice`, `push`,
//!   `extend_from_slice`, `pop`, `get`, `set`, `read_bytes`, `as_slice`,
//!   `truncate`, `clear`, `fill`, `reserve`, `reserve_exact`, `resize`,
//!   `shrink_to`, `shrink_to_fit`, and `iter`.  With the `atomic` feature it
//!   also gains the crash-atomic byte movers `insert`, `remove`, `swap_remove`,
//!   `extend_from_within`, and the cross-slice `extend_from_bstack_slice`,
//!   `copy_into_bstack_slice`, `append_from_owned`, and `move_tail_into`.
//!   The block stores a 16-byte header (`len`, `cap`) followed by the byte
//!   data; the header is re-read on every call for crash recoverability.
//!   `push` doubles capacity (minimum 4); `pop` decrements `len` then zeros
//!   the vacated slot; `truncate` writes `len` then zeros all removed slots.
//!
//! ## Lifetime model
//!
//! `BStackOwnedSlice<'a, A>` borrows the **allocator** for `'a`.
//! The borrow checker statically prevents calling
//! [`BStackAllocator::into_stack`] — which consumes the allocator by value —
//! while any owned slice is still in scope.  `BStackSlice<'a>` views obtained
//! via `as_slice[_mut]()` have a shorter lifetime tied to the borrow of the
//! owned slice, preventing them from outliving the handle that owns the region.
//!
//! What lifetimes cannot express is that a handle goes back to the allocator
//! that issued it: two allocators of the same type are the same type, so
//! `a2.dealloc(h1)` compiles.  That is not a soundness problem — handles are
//! `(offset, len)` coordinates into a file, not pointers — but it would corrupt
//! `a2`'s bookkeeping, so every allocator rejects a foreign handle at run time
//! with [`io::ErrorKind::InvalidInput`], returning the handle intact and its own
//! metadata untouched.  [`BStackOwnedSlice::is_from`] is the check, for custom
//! allocators that need it.
//!
//! ## Quick example
//!
//! ```skip
//! use bstack::{BStack, BStackAllocator, LinearBStackAllocator};
//!
//! # fn main() -> std::io::Result<()> {
//! let alloc = LinearBStackAllocator::new(BStack::open("data.bstack")?);
//!
//! let mut slice = alloc.alloc(128)?;      // reserve 128 zero bytes
//! let data = slice.read()?;    // read them back
//! alloc.dealloc(slice)?;                  // release (tail, so O(1))
//!
//! let stack = alloc.into_stack();         // reclaim the BStack
//! # Ok(())
//! # }
//! ```
//!
//! # Examples
//!
//! ```no_run
//! use bstack::BStack;
//!
//! # fn main() -> std::io::Result<()> {
//! let stack = BStack::open("log.bin")?;
//!
//! // push returns the logical byte offset where the payload starts.
//! let off0 = stack.push(b"hello")?;  // 0
//! let off1 = stack.push(b"world")?;  // 5
//!
//! assert_eq!(stack.len()?, 10);
//!
//! // peek reads from a logical offset to the end without removing anything.
//! assert_eq!(stack.peek(off1)?, b"world");
//!
//! // get reads an arbitrary half-open logical byte range.
//! assert_eq!(stack.get(3, 8)?, b"lowor");
//!
//! // pop removes bytes from the tail and returns them.
//! assert_eq!(stack.pop(5)?, b"world");
//! assert_eq!(stack.len()?, 5);
//! # Ok(())
//! # }
//! ```

// This crate-doc section is emitted only when the dev/test-only fault-injection
// machinery is actually compiled in (see the [`fault`] module).
#![cfg_attr(
    all(debug_assertions, feature = "fault-injection"),
    doc = "# Fault injection (`fault-injection` feature)",
    doc = "",
    doc = "This build has the dev/test-only `fault-injection` feature active, so",
    doc = "`BStack` I/O can be made to fail on demand. Implement [`FaultPolicy`] and arm",
    doc = "it with [`BStack::with_fault_policy`] (at construction) or",
    doc = "[`BStack::set_fault_policy`] (arm, re-arm, or disarm mid-test); every I/O",
    doc = "method then consults the policy once, **after** validating its arguments. This",
    doc = "exercises error-handling and rollback paths that a successful sequence of calls",
    doc = "can never reach. The whole mechanism is gated on `all(debug_assertions, feature",
    doc = "= \"fault-injection\")`, so a `--release` build carries none of it and its",
    doc = "performance is unaffected. See the [`fault`] module for details."
)]

/// Build an [`io::Error`](std::io::Error) from an [`ErrorKind`](std::io::ErrorKind)
/// variant and a message, without repeating `io::Error::new(io::ErrorKind::…, …)`.
///
/// * `$kind` — bare `ErrorKind` variant name (`InvalidData`, `NotFound`, …); expands
///   to `std::io::ErrorKind::$kind`.
/// * message — either a single expr (any `Into<Box<dyn Error + Send + Sync>>`: a
///   `&str`, `String`, or error value), or a format literal plus args (via
///   [`format!`]).
#[allow(unused)]
macro_rules! io_error {
    ($kind:ident, $fmt:literal, $($arg:tt)+) => {
        ::std::io::Error::new(
            ::std::io::ErrorKind::$kind,
            ::std::format!($fmt, $($arg)+),
        )
    };
    ($kind:ident, $msg:expr $(,)?) => {
        ::std::io::Error::new(::std::io::ErrorKind::$kind, $msg)
    };
}

mod io_core;
use io_core::*;

pub mod reborrow;

pub mod fault;
use fault::fault_point;
#[cfg(all(feature = "set", feature = "atomic"))]
use fault::fault_probe;
#[cfg(all(debug_assertions, feature = "fault-injection"))]
pub use fault::{FaultPolicy, FaultState};
#[cfg(all(test, feature = "alloc", feature = "set"))]
mod alloc_fuzz;
mod test;

#[cfg(feature = "alloc")]
mod alloc;
#[cfg(feature = "alloc")]
pub use alloc::{
    BStackAllocError, BStackAllocator, BStackBulkAllocError, BStackBulkAllocator, BStackChunk,
    BStackChunkIter, BStackInPlaceResizeAllocator, BStackJoinError, BStackOwnedSlice,
    BStackOwnedSliceAllocator, BStackRange, BStackSlice, BStackSliceError, BStackSliceReader,
    BStackUninitAllocator, DebugCheckingAllocator, LinearBStackAllocator,
};
#[cfg(all(feature = "guarded", feature = "atomic"))]
pub use alloc::{BStackAtomicGuardedSlice, BStackAtomicGuardedSliceSubview};
#[cfg(all(feature = "alloc", feature = "set"))]
pub use alloc::{
    BStackByteVec, BStackByteVecIter, BStackSliceWriter, CheckedSlabBStackAllocator,
    FirstFitBStackAllocator, GhostTreeBstackAllocator, SegregatedBStackAllocator,
    SlabBStackAllocator,
};
#[cfg(feature = "guarded")]
pub use alloc::{BStackGuardedSlice, BStackGuardedSliceSubview};

use std::fmt;
use std::fs::{File, OpenOptions};
use std::hash::{Hash, Hasher};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, RwLock};

#[cfg(unix)]
use std::os::unix::io::AsRawFd;
#[cfg(unix)]
use std::os::unix::io::RawFd;

#[cfg(windows)]
use std::os::windows::io::AsRawHandle;
#[cfg(windows)]
use windows_sys::Win32::Foundation::HANDLE;
#[cfg(windows)]
use windows_sys::Win32::Storage::FileSystem::{
    LOCKFILE_EXCLUSIVE_LOCK, LOCKFILE_FAIL_IMMEDIATELY, LockFileEx, ReadFile,
};
#[cfg(windows)]
use windows_sys::Win32::System::IO::OVERLAPPED;

/// On-disk **format** version encoded in the magic header. This is independent
/// of the crate version: it bumps only when the file format changes in a way an
/// older reader cannot handle. 0.4.0 introduces the 32-byte write-in-progress
/// journal header (see `algos/WIP.md`); bumping the minor here makes older binaries
/// reject the new files loudly instead of misreading them.
const FORMAT_MAJOR: u8 = 0;
const FORMAT_MINOR: u8 = 4;
const FORMAT_PATCH: u8 = 4;

/// Full magic for files written by this version
/// (`BSTK` + major + minor + patch + reserved(0)).
const MAGIC: [u8; 8] = [
    b'B',
    b'S',
    b'T',
    b'K',
    FORMAT_MAJOR,
    FORMAT_MINOR,
    FORMAT_PATCH,
    0,
];

/// Compatibility prefix checked on open: `BSTK` + format major + minor. A file
/// is accepted only when its first 6 bytes match — i.e. the same format
/// `major.minor`. The patch byte is informational and is not compared.
const MAGIC_PREFIX: [u8; 6] = [b'B', b'S', b'T', b'K', FORMAT_MAJOR, FORMAT_MINOR];

/// Magic prefix of the pre-0.4.0 (0.1.x) format that [`BStack::migrate`]
/// upgrades from: `BSTK` + major 0 + minor 1.
const LEGACY_MAGIC_PREFIX: [u8; 6] = [b'B', b'S', b'T', b'K', 0, 1];

/// Header size of the pre-0.4.0 (0.1.x) format: `magic[8] + committed_len[8]`.
const LEGACY_HEADER_SIZE: u64 = 16;

/// Compute `base + len`, mapping `u64` overflow to an `InvalidInput` error
/// carrying `msg`.
#[cfg(any(feature = "set", feature = "atomic"))]
pub(crate) fn checked_end(base: u64, len: u64, msg: &'static str) -> io::Result<u64> {
    base.checked_add(len)
        .ok_or_else(|| io_error!(InvalidInput, msg))
}

/// Reject an in-place write whose range `[offset, end)` starts inside the locked
/// prefix `[0, locked)`. `op` names the operation for the error message.
///
/// Shared by the single-range in-place mutators (`set`, `zero`, `swap`,
/// `swap_into`, `cas`); callers must load `locked` under the write lock so the
/// check cannot race a concurrent `lock_up_to`.
#[cfg(feature = "set")]
pub(crate) fn check_offset_unlocked(
    op: &str,
    offset: u64,
    end: u64,
    locked: u64,
) -> io::Result<()> {
    if offset < locked {
        return Err(io_error!(
            InvalidInput,
            format!("{op}: range [{offset}, {end}) overlaps locked region [0, {locked})")
        ));
    }
    Ok(())
}

/// Validate a batch of sparse-extend writes against a declared extension of
/// `length` bytes.
///
/// `blocks` holds `(relative_offset, data)` pairs (already stripped of empty
/// `data`), where each relative offset is measured from the current tail. Every
/// block must fit within the freshly grown region `[0, length)` — a block whose
/// range `[off, off + data.len())` runs past `length` (or overflows `u64`) is
/// rejected — and blocks must be pairwise non-overlapping. On success `blocks` is
/// left sorted by relative offset. `op` names the operation for error messages.
///
/// Shared by `extend_sparse_batched` and `try_extend_sparse_batched`. Kept
/// feature-agnostic (no dependency on the `set`/`atomic`-gated `checked_end`) so
/// the base-API batched form compiles with no features enabled.
fn validate_sparse_blocks(blocks: &mut [(u64, &[u8])], length: u64, op: &str) -> io::Result<()> {
    for (off, data) in blocks.iter() {
        let end = off.checked_add(data.len() as u64).ok_or_else(|| {
            io_error!(
                InvalidInput,
                "{op}: relative offset ({off}) + len ({}) overflows u64",
                data.len()
            )
        })?;
        if end > length {
            return Err(io_error!(
                InvalidInput,
                format!("{op}: write range [{off}, {end}) exceeds declared length ({length})")
            ));
        }
    }
    // Reject overlap: sort by offset, then check each block ends at or before the
    // next one begins (mirrors `set_batched`).
    blocks.sort_by_key(|(off, _)| *off);
    for pair in blocks.windows(2) {
        let (a_off, a_data) = pair[0];
        let (b_off, _) = pair[1];
        let a_end = a_off + a_data.len() as u64;
        if a_end > b_off {
            return Err(io_error!(
                InvalidInput,
                format!("{op}: write range [{a_off}, {a_end}) overlaps [{b_off}, ...)")
            ));
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------

/// The error a read carries while an interrupted write is pending replay.
///
/// Returned as the payload of an [`io::Error`] of kind
/// [`Other`](io::ErrorKind::Other) by every read that consults the file or the
/// cached committed length, until the next write replays the interruption (see
/// *Deferred replay* in the crate docs).  Match on it instead of the message:
///
/// ```
/// # use bstack::InterruptedWrite;
/// # fn f(err: &std::io::Error) -> bool {
/// err.get_ref().is_some_and(|e| e.is::<InterruptedWrite>())
/// # }
/// ```
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash)]
pub struct InterruptedWrite;

impl fmt::Display for InterruptedWrite {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(
            "bstack: an interrupted write is pending replay — the next write replays it \
             silently; reads are refused until then",
        )
    }
}

impl std::error::Error for InterruptedWrite {}

impl From<InterruptedWrite> for io::Error {
    #[inline]
    fn from(e: InterruptedWrite) -> Self {
        io::Error::other(e)
    }
}

/// A persistent, fsync-durable binary stack backed by a single file.
///
/// See the [crate-level documentation](crate) for the file format, durability
/// guarantees, crash recovery, multi-process safety, and thread-safety model.
pub struct BStack {
    /// The file handle, a cached copy of the on-disk header's committed payload
    /// length (`clen`), and the deferred-replay flag.
    ///
    /// The flag (the `.2` field) is `true` when a write failed after its first
    /// mutating I/O, so the file may still hold an armed journal or a stale tail
    /// (see *Deferred replay* in the crate docs). It is in-memory only — the
    /// on-disk journal is what recovery actually reads — and is cleared by the
    /// replay the next write performs.
    ///
    /// `clen` (the `.1` field) is seeded from the validated header at
    /// construction time (after recovery) and kept in sync by every
    /// write-lock-held operation that commits a new `clen` to the header, via
    /// `write_committed_len`. [`BStack::len`] and [`BStack::is_empty`] read it
    /// under the same lock used for the on-disk state, so no extra
    /// synchronisation is needed.
    lock: RwLock<(File, u64, bool)>,
    /// Monotonically growing partition boundary.  Bytes in `[0, locked)` are
    /// immutable and can be read without the rwlock on supported platforms.
    /// Not persisted — resets to 0 on every open.
    locked: AtomicU64,
    /// Copy of the raw file descriptor used for lock-free positional reads
    /// on the locked region.  The `File` inside `lock` retains ownership and
    /// will close the descriptor when `BStack` is dropped.
    #[cfg(unix)]
    fd: RawFd,
    /// Copy of the Windows HANDLE stored as `isize` so the field is
    /// `Send + Sync`.  Same lifetime guarantee as `fd` above.
    #[cfg(windows)]
    handle: isize,
    /// Whether in-memory caching of the locked region is enabled.
    /// Set once at construction; never mutated afterwards.
    cache_enabled: bool,
    /// In-memory mirror of `[0, locked)`.  Empty until the first `lock_up_to`
    /// call on a cached stack.  Capacity follows a power-of-two growth rule;
    /// `self.locked` is the count of valid bytes within the buffer.
    cache: Mutex<Vec<u8>>,
    /// Deterministic I/O-fault injection state, consulted at the API boundary of
    /// every instrumented method (see the [`fault`] module). Present only in
    /// builds with `debug_assertions` on and the `fault-injection` feature
    /// enabled; release builds carry neither the field nor its per-call branch.
    #[cfg(all(debug_assertions, feature = "fault-injection"))]
    fault: fault::FaultState,
}

// `BStack` is auto-`Send + Sync` on every platform: all fields
// (`RwLock<File>`, `AtomicU64`, and the `RawFd` / `isize` handle) already
// implement both traits.  The lock-free `pread` / `ReadFile`+`OVERLAPPED`
// paths are cursor-independent and safe to call from any thread, and the raw
// fd / handle remains valid for as long as `BStack` owns the `File`.

impl BStack {
    /// Write the 32-byte header into a brand-new (empty) file.
    fn init_header(file: &mut File) -> io::Result<()> {
        file.seek(SeekFrom::Start(0))?;
        file.write_all(&MAGIC)?;
        // committed_len[8] + wip_ptr[8] + wip_aux[8], all zero on a fresh file.
        file.write_all(&[0u8; (HEADER_SIZE - 8) as usize])
    }

    /// Read and validate the 32-byte header; return `(committed_len, wip_ptr,
    /// wip_aux)`.
    fn read_header(file: &mut File) -> io::Result<(u64, u64, u64)> {
        file.seek(SeekFrom::Start(0))?;
        let mut hdr = [0u8; HEADER_SIZE as usize];
        file.read_exact(&mut hdr)?;
        if hdr[0..6] != MAGIC_PREFIX {
            return Err(io_error!(
                InvalidData,
                "bstack: bad magic number — not a bstack file or incompatible version"
            ));
        }
        let committed_len = u64::from_le_bytes(hdr[8..16].try_into().unwrap());
        let wip_ptr = u64::from_le_bytes(hdr[16..24].try_into().unwrap());
        let wip_aux = u64::from_le_bytes(hdr[24..32].try_into().unwrap());
        Ok((committed_len, wip_ptr, wip_aux))
    }

    /// Replay or roll back an interrupted write, restoring the at-rest invariant
    /// (`file_size == HEADER_SIZE + clen`, journal disarmed).  Returns the
    /// committed length afterwards (unchanged except by a splice).
    ///
    /// `raw_size` is the current file size.  Shared by [`open`](Self::open) — a
    /// crash left the file armed — and by the deferred replay a write performs
    /// after an earlier write on the same handle failed midway.  Every replay is
    /// idempotent, so running it twice is harmless.
    #[inline]
    fn recover_file(file: &mut File, raw_size: u64) -> io::Result<u64> {
        let (committed_len, wip_ptr, wip_aux) = Self::read_header(file)?;
        let mut clen = committed_len;
        if wip_ptr != 0 {
            // An in-place write was in flight. Replay or roll it back, then
            // restore the at-rest invariant. A splice changes the committed
            // length, so adopt whatever recovery commits.
            clen = recover_wip(file, committed_len, wip_ptr, wip_aux, raw_size)?;
        } else if wip_aux == u64::from(WipAux::MultiWrite) {
            // A multi-write batch was in flight (armed with `wip_ptr == 0` and the
            // intent-complete sentinel). All blocks were fully staged before the
            // arm, so replay the sequence and disarm. The committed length is
            // unchanged.
            clen = recover_multi_write(file, committed_len, raw_size)?;
        } else {
            // No journal armed: reconcile the committed length against the file
            // size, using whichever is smaller (the committed value is the last
            // successfully synced boundary). This drops a stale tail from a
            // crashed push/extend or a crashed journal stage.
            let actual_data_len = raw_size - HEADER_SIZE;
            if actual_data_len != committed_len {
                let correct_len = committed_len.min(actual_data_len);
                file.set_len(HEADER_SIZE + correct_len)?;
                write_committed_len(file, &mut clen, correct_len)?;
                durable_sync(file)?;
            }
        }
        Ok(clen)
    }

    /// Flag the stack for replay when `r` failed: a mutating step that got past
    /// its first write may have left an armed journal or a stale tail behind.
    ///
    /// Wraps every mutating call made under the write lock. Read-only steps are
    /// not wrapped — a read that fails changes nothing on disk.
    #[inline]
    fn mark_replay<T>(replay: &mut bool, r: io::Result<T>) -> io::Result<T> {
        if r.is_err() {
            *replay = true;
        }
        r
    }

    /// Take the write lock for a **mutating** operation, first silently replaying
    /// any earlier write that failed midway (see *Deferred replay* in the crate
    /// docs).
    ///
    /// The replay must precede the operation's own validation: a pending one can
    /// leave a stale tail past the committed length, which would inflate the
    /// payload size every mutator derives from the file end.  A failed replay
    /// leaves the flag set, so the next write tries again.
    #[inline(always)]
    fn write_lock(&self) -> io::Result<std::sync::RwLockWriteGuard<'_, (File, u64, bool)>> {
        let mut guard = self.lock.write().unwrap();
        Self::replay_pending(&mut guard)?;
        Ok(guard)
    }

    /// Replay a pending interruption on a held write guard, adopting the
    /// committed length it commits and clearing the flag.  Returns whether one
    /// was pending.  A failed replay leaves the flag set, so the next write — or
    /// [`recover`](Self::recover) — tries again.
    #[inline]
    fn replay_pending(guard: &mut (File, u64, bool)) -> io::Result<bool> {
        if !guard.2 {
            return Ok(false);
        }
        let (file, clen, replay) = guard;
        let raw_size = file.metadata()?.len();
        *clen = Self::recover_file(file, raw_size)?;
        *replay = false;
        Ok(true)
    }

    /// Take the write lock for a **read-only** operation that needs `&mut File`
    /// (the non-positional-read platform fallbacks, and `lock_up_to`).
    ///
    /// A pending replay fails the call rather than being applied: only a write
    /// replays, so a read never returns bytes an interrupted write may still own.
    #[inline]
    fn write_lock_read(&self) -> io::Result<std::sync::RwLockWriteGuard<'_, (File, u64, bool)>> {
        let guard = self.lock.write().unwrap();
        if guard.2 {
            return Err(InterruptedWrite.into());
        }
        Ok(guard)
    }

    /// Take the read lock.  A pending replay fails the call — see
    /// [`write_lock_read`](Self::write_lock_read).
    #[inline]
    fn read_lock(&self) -> io::Result<std::sync::RwLockReadGuard<'_, (File, u64, bool)>> {
        let guard = self.lock.read().unwrap();
        if guard.2 {
            return Err(InterruptedWrite.into());
        }
        Ok(guard)
    }

    /// Open or create a stack file at `path`.
    ///
    /// On a **new** file the 32-byte header is written and durably synced
    /// before returning.
    ///
    /// On an **existing** file the header is validated and, if a previous crash
    /// left the file in an inconsistent state, the file is repaired and durably
    /// synced before returning (see *Crash recovery* in the crate docs).
    ///
    /// On Unix an **exclusive advisory `flock`** is acquired; if another
    /// process already holds the lock this function returns immediately with
    /// [`io::ErrorKind::WouldBlock`].
    ///
    /// # Errors
    ///
    /// * [`io::ErrorKind::WouldBlock`] — another process holds the exclusive
    ///   lock (Unix only).
    /// * [`io::ErrorKind::InvalidData`] — the file exists but its header magic
    ///   is wrong (not a bstack file, or created by an incompatible version),
    ///   or the file is too short to contain a valid header.
    /// * Any [`io::Error`] from [`OpenOptions::open`], `read`, `write`, or
    ///   `durable_sync`.
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;

        #[cfg(unix)]
        flock_exclusive(&file)?;

        #[cfg(windows)]
        lock_file_exclusive(&file)?;

        let raw_size = file.metadata()?.len();

        let mut clen = 0u64;
        if raw_size == 0 {
            Self::init_header(&mut file)?;
            durable_sync(&file)?;
        } else if raw_size < HEADER_SIZE {
            return Err(io_error!(
                InvalidData,
                format!(
                    "bstack: file is {raw_size} bytes — too small to contain the {HEADER_SIZE}-byte header"
                )
            ));
        } else {
            clen = Self::recover_file(&mut file, raw_size)?;
        }

        #[cfg(unix)]
        let fd = file.as_raw_fd();
        #[cfg(windows)]
        let handle = file.as_raw_handle() as isize;

        Ok(BStack {
            #[cfg(unix)]
            fd,
            #[cfg(windows)]
            handle,
            lock: RwLock::new((file, clen, false)),
            locked: AtomicU64::new(0),
            cache_enabled: false,
            cache: Mutex::new(Vec::new()),
            #[cfg(all(debug_assertions, feature = "fault-injection"))]
            fault: fault::FaultState::new(),
        })
    }

    /// Upgrade a legacy pre-0.4.0 (0.1.x, 16-byte header) file at `path` to the
    /// current 0.4.0 layout (32-byte header), in place.
    ///
    /// The file is rewritten into a sibling `"<path>.migrating"` — a fresh 0.4.0
    /// header followed by the old payload shifted from offset 16 to offset 32 —
    /// which is then atomically renamed onto the original (a crash leaves either
    /// the intact original or the finished new file, never neither). The
    /// committed length is preserved (clamped to the bytes actually present,
    /// mirroring [`open`](BStack::open)'s recovery).
    ///
    /// The caller must not hold the file open elsewhere. On success `path` is a
    /// valid 0.4.0 file ready for [`open`](BStack::open).
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::InvalidData`] if `path` is not a legacy 0.1.x
    /// file (wrong magic, or shorter than the 16-byte legacy header), and
    /// propagates any I/O error from reading, writing, syncing, removing, or
    /// renaming.
    pub fn migrate(path: impl AsRef<Path>) -> io::Result<()> {
        let path = path.as_ref();

        // Read and validate the legacy 16-byte header.
        let mut old = OpenOptions::new().read(true).open(path)?;
        let old_size = old.metadata()?.len();
        if old_size < LEGACY_HEADER_SIZE {
            return Err(io_error!(
                InvalidData,
                format!(
                    "bstack: file is {old_size} bytes — too small to be a legacy {LEGACY_HEADER_SIZE}-byte-header file"
                )
            ));
        }
        let mut hdr = [0u8; LEGACY_HEADER_SIZE as usize];
        old.read_exact(&mut hdr)?;
        if hdr[0..6] != LEGACY_MAGIC_PREFIX {
            return Err(io_error!(
                InvalidData,
                "bstack: not a legacy 0.1.x file — nothing to migrate"
            ));
        }
        // Committed length, clamped to the payload actually present.
        let clen =
            u64::from_le_bytes(hdr[8..16].try_into().unwrap()).min(old_size - LEGACY_HEADER_SIZE);

        // Sibling path "<path>.migrating", in the same directory so the final
        // rename stays within one filesystem.
        let mut tmp = path.as_os_str().to_owned();
        tmp.push(".migrating");
        let tmp = PathBuf::from(tmp);

        // Write the new file: 32-byte 0.4.0 header, then the old payload shifted
        // from offset 16 to offset 32.
        {
            let mut new = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(&tmp)?;
            new.write_all(&MAGIC)?; // magic[8]
            new.write_all(&clen.to_le_bytes())?; // committed_len[8]
            new.write_all(&[0u8; 16])?; // wip_ptr[8] | wip_aux[8] = 0
            old.seek(SeekFrom::Start(LEGACY_HEADER_SIZE))?;
            let mut src = (&mut old).take(clen);
            let copied = io::copy(&mut src, &mut new)?;
            if copied != clen {
                return Err(io_error!(
                    UnexpectedEof,
                    "bstack: legacy payload shorter than committed length during migration"
                ));
            }
            new.sync_all()?;
        }
        drop(old);

        // Atomically swap the sibling in for the original. `rename` replaces the
        // destination in a single step on both Unix (`rename(2)`) and Windows
        // (`MoveFileExW` with `MOVEFILE_REPLACE_EXISTING`), so a crash leaves
        // either the intact original or the completed 0.4.0 file at `path` —
        // never neither. (Removing the original first would open a window where
        // a crash leaves only the sibling.)
        std::fs::rename(&tmp, path)?;
        Ok(())
    }

    /// Replay a pending interrupted write now, instead of waiting for the next
    /// write to do it.
    ///
    /// Returns `true` if one was pending and has been replayed, `false` if the
    /// stack was already intact.  Reads refused with [`InterruptedWrite`] succeed
    /// again once this returns `Ok` — which is the point of the call: a caller
    /// whose write failed can carry on reading without having to issue another
    /// write or reopen the file.
    ///
    /// The repair is the one [`open`](Self::open) performs (see *Deferred replay*
    /// in the crate docs), and every write already runs it, so calling this is
    /// never required for correctness.
    ///
    /// # Errors
    ///
    /// Propagates any I/O error from the replay, leaving the stack flagged so a
    /// later call — or the next write — can retry.
    pub fn recover(&self) -> io::Result<bool> {
        let mut guard = self.lock.write().unwrap();
        fault_point!(self, "recover");
        Self::replay_pending(&mut guard)
    }

    /// Append `data` to the end of the file.
    ///
    /// Returns the **logical** byte offset at which `data` begins — i.e. the
    /// payload size immediately before the write.  An empty slice is valid; it
    /// writes nothing and returns the current end offset.
    ///
    /// # Atomicity
    ///
    /// Either the full payload is written, the header committed-length is
    /// updated, and the whole thing is durably synced, or the file is
    /// left unchanged (best-effort rollback via `ftruncate` + header reset).
    ///
    /// # Errors
    ///
    /// Returns any [`io::Error`] from `write_all`, `durable_sync`, or the
    /// fallback `set_len`.
    pub fn push(&self, data: impl AsRef<[u8]>) -> io::Result<u64> {
        let data = data.as_ref();
        let mut guard = self.write_lock()?;
        let (file, clen, replay) = &mut *guard;
        let file_end = file.seek(SeekFrom::End(0))?;
        let logical_offset = file_end - HEADER_SIZE;

        if data.is_empty() {
            return Ok(logical_offset);
        }

        fault_point!(self, "push");
        if let Err(e) = file.write_all(data) {
            // A failed rollback leaves a stale tail past the committed length:
            // defer it to the next write's replay.
            if file.set_len(file_end).is_err() {
                *replay = true;
            }
            return Err(e);
        }

        let new_len = logical_offset + data.len() as u64;
        Self::mark_replay(
            replay,
            commit_grow(file, clen, new_len, logical_offset, file_end),
        )?;
        Ok(logical_offset)
    }

    /// Append `n` zero bytes to the end of the file.
    ///
    /// Returns the **logical** byte offset at which the zeros begin — i.e. the
    /// payload size immediately before the write.  `n = 0` is valid; it writes
    /// nothing and returns the current end offset.
    ///
    /// # Atomicity
    ///
    /// Either the file is extended, the header committed-length is updated,
    /// and the whole thing is durably synced, or the file is left unchanged
    /// (best-effort rollback via `ftruncate` + header reset).
    ///
    /// # Errors
    ///
    /// Returns any [`io::Error`] from `set_len`, `durable_sync`, or the
    /// fallback `set_len`.
    pub fn extend(&self, n: u64) -> io::Result<u64> {
        let mut guard = self.write_lock()?;
        let (file, clen, replay) = &mut *guard;
        let file_end = file.seek(SeekFrom::End(0))?;
        let logical_offset = file_end - HEADER_SIZE;

        if n == 0 {
            return Ok(logical_offset);
        }

        fault_point!(self, "extend");
        let new_file_end = file_end + n;
        Self::mark_replay(replay, file.set_len(new_file_end))?;

        let new_len = logical_offset + n;
        Self::mark_replay(
            replay,
            commit_grow(file, clen, new_len, logical_offset, file_end),
        )?;
        Ok(logical_offset)
    }

    /// Sparsely grow the payload by `length` bytes, writing `buf` at the start of
    /// the freshly grown region and leaving the remaining `length - buf.len()`
    /// bytes zero.
    ///
    /// Returns the **logical** byte offset at which the growth begins — i.e. the
    /// payload size immediately before the call, the anchor `buf` is written at.
    ///
    /// This is a more efficient alternative to [`push`](Self::push) followed by
    /// [`extend`](Self::extend) (or a `push` of a large mostly-zero buffer) when
    /// you need a large zero-filled region with only a small prefix of real data:
    /// the whole `length` is realised with a single `set_len`, so the tail past
    /// `buf` costs no write I/O (it reads back as zero from the sparse file), and
    /// only `buf` and the header commit are written and synced.
    ///
    /// `length = 0` is valid only when `buf` is empty; it writes nothing and
    /// returns the current end offset. An empty `buf` with `length > 0` is
    /// equivalent to [`extend(length)`](Self::extend).
    ///
    /// # Atomicity
    ///
    /// Either the file is grown, `buf` written, the header committed-length
    /// updated, and the whole thing durably synced, or the file is left unchanged
    /// (best-effort rollback via `ftruncate` + header reset). No journal is
    /// needed: the entire grown region sits beyond the committed length, so a
    /// crash before the header commit rolls back by truncation.
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::InvalidInput`] if `buf.len()` exceeds `length`, or
    /// if the current payload size plus `length` overflows `u64`. Also propagates
    /// any I/O error from `set_len`, `write_all`, or `durable_sync`.
    pub fn extend_sparse(&self, buf: impl AsRef<[u8]>, length: u64) -> io::Result<u64> {
        let buf = buf.as_ref();
        if buf.len() as u64 > length {
            return Err(io_error!(
                InvalidInput,
                "extend_sparse: buffer length ({}) exceeds extension length ({length})",
                buf.len()
            ));
        }
        let mut guard = self.write_lock()?;
        let (file, clen, replay) = &mut *guard;
        let file_end = file.seek(SeekFrom::End(0))?;
        let logical_offset = file_end - HEADER_SIZE;

        if length == 0 {
            return Ok(logical_offset);
        }
        let new_len = logical_offset.checked_add(length).ok_or_else(|| {
            io_error!(
                InvalidInput,
                "extend_sparse: payload size + length overflows u64"
            )
        })?;
        fault_point!(self, "extend_sparse");
        let one = [(0u64, buf)];
        let blocks: &[(u64, &[u8])] = if buf.is_empty() { &[] } else { &one };
        Self::mark_replay(
            replay,
            commit_sparse_extend(file, clen, logical_offset, file_end, new_len, blocks),
        )?;
        Ok(logical_offset)
    }

    /// Sparsely grow the payload by `length` bytes, scattering several buffers
    /// into the freshly grown region and leaving the gaps between them zero.
    ///
    /// `writes` is any iterator of `(relative_offset, data)` pairs, where each
    /// relative offset is measured from the current tail (the returned offset).
    /// Each `data` is written at logical offset `tail + relative_offset`; the
    /// bytes not covered by any buffer read back as zero. Returns the **logical**
    /// byte offset at which the growth begins (the current payload size, the
    /// anchor every relative offset is measured from).
    ///
    /// Like [`extend_sparse`](Self::extend_sparse), the whole `length` is realised
    /// with a single `set_len`, so the zero gaps cost no write I/O and only the
    /// buffers and the header commit are written and synced. Empty `data` slices
    /// are ignored.
    ///
    /// The writes must be **pairwise non-overlapping** and each must fit within
    /// the grown region `[0, length)`; violations are rejected as invalid input.
    /// `length = 0` is valid only when every buffer is empty.
    ///
    /// # Atomicity
    ///
    /// Either every buffer lands, the header committed-length is updated, and the
    /// whole thing is durably synced, or the file is left unchanged (best-effort
    /// rollback via `ftruncate` + header reset). No journal is needed: the entire
    /// grown region sits beyond the committed length.
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::InvalidInput`] if any `relative_offset +
    /// data.len()` overflows `u64` or exceeds `length`, if two writes overlap, or
    /// if the current payload size plus `length` overflows `u64`. Also propagates
    /// any I/O error from `set_len`, `write_all`, or `durable_sync`.
    pub fn extend_sparse_batched<I, D>(&self, writes: I, length: u64) -> io::Result<u64>
    where
        I: IntoIterator<Item = (u64, D)>,
        D: AsRef<[u8]>,
    {
        // Materialise the inputs so their `AsRef` slices can be borrowed while we
        // validate and stage; drop empty writes (they touch nothing).
        let owned: Vec<(u64, D)> = writes.into_iter().collect();
        let mut blocks: Vec<(u64, &[u8])> = owned
            .iter()
            .map(|(off, d)| (*off, d.as_ref()))
            .filter(|(_, d)| !d.is_empty())
            .collect();
        validate_sparse_blocks(&mut blocks, length, "extend_sparse_batched")?;

        let mut guard = self.write_lock()?;
        let (file, clen, replay) = &mut *guard;
        let file_end = file.seek(SeekFrom::End(0))?;
        let logical_offset = file_end - HEADER_SIZE;

        if length == 0 {
            // Every block was validated to fit within `[0, 0)`, so `blocks` is empty.
            return Ok(logical_offset);
        }
        let new_len = logical_offset.checked_add(length).ok_or_else(|| {
            io_error!(
                InvalidInput,
                "extend_sparse_batched: payload size + length overflows u64"
            )
        })?;
        fault_point!(self, "extend_sparse_batched");
        Self::mark_replay(
            replay,
            commit_sparse_extend(file, clen, logical_offset, file_end, new_len, &blocks),
        )?;
        Ok(logical_offset)
    }

    /// Grow or shrink the payload to exactly `target` bytes and durable-sync.
    /// Any newly grown region is filled with zeros.
    ///
    /// Returns the payload size immediately before the resize. `target` equal
    /// to the current payload size is a valid no-op.
    ///
    /// # Atomicity
    ///
    /// Growth follows [`extend`](Self::extend)'s guarantees; shrinkage follows
    /// [`discard`](Self::discard)'s. Either the resize completes, the header
    /// committed-length is updated, and the whole thing is durably synced, or
    /// the file is left unchanged (best-effort rollback via `ftruncate` +
    /// header reset on a growth failure).
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::InvalidInput`] if shrinking would cut into the
    /// locked region `[0, locked_len())`. Propagates any I/O error from
    /// `set_len`, `write_committed_len`, or `durable_sync`.
    pub fn resize(&self, target: u64) -> io::Result<u64> {
        let mut guard = self.write_lock()?;
        let (file, clen, replay) = &mut *guard;
        let file_end = file.seek(SeekFrom::End(0))?;
        let data_size = file_end - HEADER_SIZE;

        if target == data_size {
            return Ok(data_size);
        }
        if target < data_size {
            let locked = self.locked.load(Ordering::Acquire);
            if target < locked {
                return Err(io_error!(
                    InvalidInput,
                    format!("resize({target}) would shrink payload below locked length ({locked})")
                ));
            }
            fault_point!(self, "resize");
            Self::mark_replay(replay, commit_shrink(file, clen, target))?;
            return Ok(data_size);
        }

        fault_point!(self, "resize");
        Self::mark_replay(replay, file.set_len(HEADER_SIZE + target))?;
        Self::mark_replay(replay, commit_grow(file, clen, target, data_size, file_end))?;
        Ok(data_size)
    }

    /// Grow the payload to at least `target` bytes, filling the new region
    /// with zeros, and durable-sync. A no-op if the payload is already
    /// `target` bytes or longer.
    ///
    /// Returns the payload size immediately before the call — the grow-only,
    /// unconditional counterpart of [`resize`](Self::resize).
    ///
    /// # Atomicity
    ///
    /// Same guarantees as [`extend`](Self::extend).
    ///
    /// # Errors
    ///
    /// Propagates any I/O error from `set_len`, `write_committed_len`, or
    /// `durable_sync`.
    pub fn ensure(&self, target: u64) -> io::Result<u64> {
        let mut guard = self.write_lock()?;
        let (file, clen, replay) = &mut *guard;
        let file_end = file.seek(SeekFrom::End(0))?;
        let data_size = file_end - HEADER_SIZE;

        if target <= data_size {
            return Ok(data_size);
        }

        fault_point!(self, "ensure");
        Self::mark_replay(replay, file.set_len(HEADER_SIZE + target))?;
        Self::mark_replay(replay, commit_grow(file, clen, target, data_size, file_end))?;
        Ok(data_size)
    }

    /// Grow the payload to at least `target` bytes, only if it is currently
    /// shorter, handing the freshly allocated tail to `f` for initialization
    /// before it is committed.
    ///
    /// If the payload is already `target` bytes or longer, `f` is not called
    /// and nothing changes. Otherwise `f` is called with a zero-filled
    /// `&mut [u8]` of length `target - old_len` — exactly the region
    /// [`ensure`](Self::ensure) would have appended — and whatever `f` leaves
    /// in that buffer is what lands on disk; the callback is the only way to
    /// populate the grown tail with anything but zeros.
    ///
    /// Returns the payload size immediately before the call.
    ///
    /// # Feature flag
    ///
    /// Only available when the `atomic` Cargo feature is enabled.
    ///
    /// # Atomicity
    ///
    /// Crash-atomic on the same terms as [`extend`](Self::extend): `f` runs in
    /// memory, under the write lock, before any of its output reaches disk, so
    /// a crash never observes a partially initialized tail. Either the grown
    /// region — with `f`'s edits applied — is committed and durably synced, or
    /// the file is left unchanged (best-effort rollback via `ftruncate` on a
    /// write failure).
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::OutOfMemory`] if `target - old_len` exceeds
    /// `isize::MAX` bytes — the maximum size of a single allocation.
    /// Propagates any I/O error from `set_len`, `write_all`, or
    /// `durable_sync`.
    #[cfg(feature = "atomic")]
    pub fn ensure_with<F>(&self, target: u64, f: F) -> io::Result<u64>
    where
        F: FnOnce(&mut [u8]),
    {
        let mut guard = self.write_lock()?;
        let (file, clen, replay) = &mut *guard;
        let file_end = file.seek(SeekFrom::End(0))?;
        let data_size = file_end - HEADER_SIZE;

        if target <= data_size {
            return Ok(data_size);
        }

        // A single allocation can never exceed `isize::MAX` bytes (Rust's own
        // allocator limit — `Vec` panics with "capacity overflow" past it),
        // which is stricter than `usize::MAX` and also covers 32-bit targets
        // where `usize` is narrower than `u64`.
        let growth = target - data_size;
        if growth > isize::MAX as u64 {
            return Err(io_error!(
                OutOfMemory,
                "ensure_with: growth too large to buffer on this platform"
            ));
        }
        let mut buf = vec![0u8; growth as usize];
        f(&mut buf);
        fault_point!(self, "ensure_with");
        if let Err(e) = file.write_all(&buf) {
            // A failed rollback leaves a stale tail past the committed length:
            // defer it to the next write's replay.
            if file.set_len(file_end).is_err() {
                *replay = true;
            }
            return Err(e);
        }
        Self::mark_replay(replay, commit_grow(file, clen, target, data_size, file_end))?;
        Ok(data_size)
    }

    /// Remove and return the last `n` bytes of the file.
    ///
    /// `n = 0` is valid: no bytes are removed and an empty `Vec` is returned.
    /// `n` may span across multiple previous [`push`](Self::push) boundaries.
    ///
    /// # Atomicity
    ///
    /// The bytes are read before the file is truncated.  The committed-length
    /// in the header is updated and durably synced after the truncation.
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::InvalidInput`] if `n` exceeds the current
    /// payload size.  Also propagates any I/O error from `read_exact`,
    /// `set_len`, `write_all`, or `durable_sync`.
    pub fn pop(&self, n: u64) -> io::Result<Vec<u8>> {
        let mut guard = self.write_lock()?;
        let (file, clen, replay) = &mut *guard;
        let raw_size = file.seek(SeekFrom::End(0))?;
        let data_size = raw_size - HEADER_SIZE;
        if n > data_size {
            return Err(io_error!(
                InvalidInput,
                format!("pop({n}) exceeds payload size ({data_size})")
            ));
        }
        let new_data_len = data_size - n;
        let locked = self.locked.load(Ordering::Acquire);
        if new_data_len < locked {
            return Err(io_error!(
                InvalidInput,
                format!("pop({n}) would shrink payload below locked length ({locked})")
            ));
        }
        let mut buf = vec![0u8; n as usize];
        fault_point!(self, "pop");
        read_at(file, new_data_len, &mut buf)?;
        Self::mark_replay(replay, commit_shrink(file, clen, new_data_len))?;
        Ok(buf)
    }

    /// Return a copy of every payload byte from `offset` to the end of the
    /// file.
    ///
    /// `offset` is a **logical** offset (as returned by [`push`](Self::push)).
    /// `offset == len()` is valid and returns an empty `Vec`.  The file is not
    /// modified.
    ///
    /// # Concurrency
    ///
    /// On Unix and Windows this uses a cursor-safe positional read (`pread(2)`
    /// on Unix; `ReadFile`+`OVERLAPPED` on Windows), so the method takes only
    /// the **read lock**, allowing multiple concurrent `peek` and `get` calls
    /// to run in parallel.
    ///
    /// On other platforms a seek is required; the method falls back to the
    /// write lock and concurrent reads serialise.
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::InvalidInput`] if `offset` exceeds the current
    /// payload size.
    ///
    /// Fails with [`InterruptedWrite`] while an earlier write is pending replay.
    pub fn peek(&self, offset: u64) -> io::Result<Vec<u8>> {
        #[cfg(any(unix, windows))]
        {
            let guard = self.read_lock()?;
            let file = &guard.0;
            let data_size = file.metadata()?.len().saturating_sub(HEADER_SIZE);
            if offset > data_size {
                return Err(io_error!(
                    InvalidInput,
                    format!("peek offset ({offset}) exceeds payload size ({data_size})")
                ));
            }
            fault_point!(self, "peek");
            pread_exact(file, HEADER_SIZE + offset, (data_size - offset) as usize)
        }
        #[cfg(not(any(unix, windows)))]
        {
            let mut guard = self.write_lock_read()?;
            let file = &mut guard.0;
            let raw_size = file.seek(SeekFrom::End(0))?;
            let data_size = raw_size.saturating_sub(HEADER_SIZE);
            if offset > data_size {
                return Err(io_error!(
                    InvalidInput,
                    format!("peek offset ({offset}) exceeds payload size ({data_size})")
                ));
            }
            fault_point!(self, "peek");
            file.seek(SeekFrom::Start(HEADER_SIZE + offset))?;
            let mut buf = vec![0u8; (data_size - offset) as usize];
            file.read_exact(&mut buf)?;
            Ok(buf)
        }
    }

    /// Return a copy of the bytes in the half-open logical range `[start, end)`.
    ///
    /// `start == end` is valid and returns an empty `Vec`.  The file is not
    /// modified.
    ///
    /// # Concurrency
    ///
    /// Same as [`peek`](Self::peek): on Unix and Windows the read lock is
    /// taken and concurrent `get`/`peek`/`len` calls may run in parallel.  On
    /// other platforms the write lock is taken and reads serialise.
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::InvalidInput`] if `end < start` or if `end`
    /// exceeds the current payload size.
    ///
    /// Fails with [`InterruptedWrite`] while an earlier write is pending replay.
    pub fn get(&self, start: u64, end: u64) -> io::Result<Vec<u8>> {
        if end < start {
            return Err(io_error!(
                InvalidInput,
                format!("get: end ({end}) < start ({start})")
            ));
        }
        // Fast-path: if the range lies entirely within the locked region,
        // serve from the in-memory cache (if enabled) or fall back to a
        // lock-free pread — locked bytes are immutable so no rwlock needed.
        #[cfg(any(unix, windows))]
        {
            let locked = self.locked.load(Ordering::Acquire);
            if end <= locked {
                if self.cache_enabled {
                    let len = (end - start) as usize;
                    let mut buf = vec![0u8; len];
                    let cache = self.cache.lock().unwrap();
                    buf.copy_from_slice(&cache[start as usize..end as usize]);
                    return Ok(buf);
                }
                #[cfg(unix)]
                {
                    let mut buf = vec![0u8; (end - start) as usize];
                    pread_exact_raw(self.fd, HEADER_SIZE + start, &mut buf)?;
                    return Ok(buf);
                }
                #[cfg(windows)]
                {
                    let mut buf = vec![0u8; (end - start) as usize];
                    pread_exact_raw_handle(self.handle, HEADER_SIZE + start, &mut buf)?;
                    return Ok(buf);
                }
            }
        }
        #[cfg(any(unix, windows))]
        {
            let guard = self.read_lock()?;
            let file = &guard.0;
            let data_size = file.metadata()?.len().saturating_sub(HEADER_SIZE);
            if end > data_size {
                return Err(io_error!(
                    InvalidInput,
                    format!("get: end ({end}) exceeds payload size ({data_size})")
                ));
            }
            fault_point!(self, "get");
            pread_exact(file, HEADER_SIZE + start, (end - start) as usize)
        }
        #[cfg(not(any(unix, windows)))]
        {
            let locked = self.locked.load(Ordering::Acquire);
            if end <= locked && self.cache_enabled {
                let cache = self.cache.lock().unwrap();
                return Ok(cache[start as usize..end as usize].to_vec());
            }
            let mut guard = self.write_lock_read()?;
            let file = &mut guard.0;
            let raw_size = file.seek(SeekFrom::End(0))?;
            let data_size = raw_size.saturating_sub(HEADER_SIZE);
            if end > data_size {
                return Err(io_error!(
                    InvalidInput,
                    format!("get: end ({end}) exceeds payload size ({data_size})")
                ));
            }
            fault_point!(self, "get");
            file.seek(SeekFrom::Start(HEADER_SIZE + start))?;
            let mut buf = vec![0u8; (end - start) as usize];
            file.read_exact(&mut buf)?;
            Ok(buf)
        }
    }

    /// Fill `buf` with bytes from logical `offset` to `offset + buf.len()`.
    ///
    /// Reads exactly `buf.len()` bytes from `offset` into the caller-supplied
    /// buffer.  An empty buffer is a valid no-op.  The file is not modified.
    ///
    /// Use this instead of [`peek`](Self::peek) when the destination buffer is
    /// already allocated and you want to avoid the extra heap allocation.
    ///
    /// # Concurrency
    ///
    /// Same as [`peek`](Self::peek): on Unix and Windows only the read lock is
    /// taken; on other platforms the write lock serialises all reads.
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::InvalidInput`] if `offset + buf.len()` overflows
    /// `u64` or exceeds the current payload size.
    ///
    /// Fails with [`InterruptedWrite`] while an earlier write is pending replay.
    pub fn peek_into(&self, offset: u64, buf: &mut [u8]) -> io::Result<()> {
        if buf.is_empty() {
            return Ok(());
        }
        let len = buf.len() as u64;
        let end = offset
            .checked_add(len)
            .ok_or_else(|| io_error!(InvalidInput, "peek_into: offset + len overflows u64"))?;
        #[cfg(any(unix, windows))]
        {
            let guard = self.read_lock()?;
            let file = &guard.0;
            let data_size = file.metadata()?.len().saturating_sub(HEADER_SIZE);
            if end > data_size {
                return Err(io_error!(
                    InvalidInput,
                    format!(
                        "peek_into: range [{offset}, {end}) exceeds payload size ({data_size})"
                    )
                ));
            }
            fault_point!(self, "peek_into");
            pread_exact_into(file, HEADER_SIZE + offset, buf)
        }
        #[cfg(not(any(unix, windows)))]
        {
            let mut guard = self.write_lock_read()?;
            let file = &mut guard.0;
            let data_size = file.seek(SeekFrom::End(0))?.saturating_sub(HEADER_SIZE);
            if end > data_size {
                return Err(io_error!(
                    InvalidInput,
                    format!(
                        "peek_into: range [{offset}, {end}) exceeds payload size ({data_size})"
                    )
                ));
            }
            fault_point!(self, "peek_into");
            file.seek(SeekFrom::Start(HEADER_SIZE + offset))?;
            file.read_exact(buf)
        }
    }

    /// Fill `buf` with bytes from the half-open logical range
    /// `[start, start + buf.len())`.
    ///
    /// An empty buffer is a valid no-op.  The file is not modified.
    ///
    /// Use this instead of [`get`](Self::get) when the destination buffer is
    /// already allocated and you want to avoid the extra heap allocation.
    ///
    /// # Concurrency
    ///
    /// Same as [`get`](Self::get): on Unix and Windows only the read lock is
    /// taken; on other platforms the write lock serialises all reads.
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::InvalidInput`] if `start + buf.len()` overflows
    /// `u64` or exceeds the current payload size.
    ///
    /// Fails with [`InterruptedWrite`] while an earlier write is pending replay.
    pub fn get_into(&self, start: u64, buf: &mut [u8]) -> io::Result<()> {
        if buf.is_empty() {
            return Ok(());
        }
        let len = buf.len() as u64;
        let end = start
            .checked_add(len)
            .ok_or_else(|| io_error!(InvalidInput, "get_into: start + len overflows u64"))?;
        // Fast-path: locked region is immutable — serve from cache or pread.
        #[cfg(any(unix, windows))]
        {
            let locked = self.locked.load(Ordering::Acquire);
            if end <= locked {
                if self.cache_enabled {
                    let cache = self.cache.lock().unwrap();
                    buf.copy_from_slice(&cache[start as usize..end as usize]);
                    return Ok(());
                }
                #[cfg(unix)]
                return pread_exact_raw(self.fd, HEADER_SIZE + start, buf);
                #[cfg(windows)]
                return pread_exact_raw_handle(self.handle, HEADER_SIZE + start, buf);
            }
        }
        #[cfg(any(unix, windows))]
        {
            let guard = self.read_lock()?;
            let file = &guard.0;
            let data_size = file.metadata()?.len().saturating_sub(HEADER_SIZE);
            if end > data_size {
                return Err(io_error!(
                    InvalidInput,
                    format!("get_into: end ({end}) exceeds payload size ({data_size})")
                ));
            }
            fault_point!(self, "get_into");
            pread_exact_into(file, HEADER_SIZE + start, buf)
        }
        #[cfg(not(any(unix, windows)))]
        {
            let locked = self.locked.load(Ordering::Acquire);
            if end <= locked && self.cache_enabled {
                let cache = self.cache.lock().unwrap();
                buf.copy_from_slice(&cache[start as usize..end as usize]);
                return Ok(());
            }
            let mut guard = self.write_lock_read()?;
            let file = &mut guard.0;
            let data_size = file.seek(SeekFrom::End(0))?.saturating_sub(HEADER_SIZE);
            if end > data_size {
                return Err(io_error!(
                    InvalidInput,
                    format!("get_into: end ({end}) exceeds payload size ({data_size})")
                ));
            }
            fault_point!(self, "get_into");
            file.seek(SeekFrom::Start(HEADER_SIZE + start))?;
            file.read_exact(buf)
        }
    }

    /// Remove the last `buf.len()` bytes from the file and write them into `buf`.
    ///
    /// An empty buffer is a valid no-op: no bytes are removed.
    ///
    /// Use this instead of [`pop`](Self::pop) when the destination buffer is
    /// already allocated and you want to avoid the extra heap allocation.
    ///
    /// # Atomicity
    ///
    /// Same guarantees as [`pop`](Self::pop).
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::InvalidInput`] if `buf.len()` exceeds the
    /// current payload size.  Also propagates any I/O error from `read_exact`,
    /// `set_len`, `write_all`, or `durable_sync`.
    pub fn pop_into(&self, buf: &mut [u8]) -> io::Result<()> {
        if buf.is_empty() {
            return Ok(());
        }
        let n = buf.len() as u64;
        let mut guard = self.write_lock()?;
        let (file, clen, replay) = &mut *guard;
        let raw_size = file.seek(SeekFrom::End(0))?;
        let data_size = raw_size - HEADER_SIZE;
        if n > data_size {
            return Err(io_error!(
                InvalidInput,
                format!("pop_into({n}) exceeds payload size ({data_size})")
            ));
        }
        let new_data_len = data_size - n;
        let locked = self.locked.load(Ordering::Acquire);
        if new_data_len < locked {
            return Err(io_error!(
                InvalidInput,
                format!("pop_into({n}) would shrink payload below locked length ({locked})")
            ));
        }
        fault_point!(self, "pop_into");
        read_at(file, new_data_len, buf)?;
        Self::mark_replay(replay, commit_shrink(file, clen, new_data_len))?;
        Ok(())
    }

    /// Remove (discard) the last `n` bytes from the file without returning them.
    ///
    /// Equivalent to [`pop`](Self::pop) but avoids allocating a buffer for the
    /// removed bytes.  `n = 0` is valid and is a no-op.
    ///
    /// # Atomicity
    ///
    /// Same guarantees as [`pop`](Self::pop).
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::InvalidInput`] if `n` exceeds the current
    /// payload size.  Also propagates any I/O error from `set_len`,
    /// `write_all`, or `durable_sync`.
    pub fn discard(&self, n: u64) -> io::Result<()> {
        if n == 0 {
            return Ok(());
        }
        let mut guard = self.write_lock()?;
        let (file, clen, replay) = &mut *guard;
        let raw_size = file.seek(SeekFrom::End(0))?;
        let data_size = raw_size - HEADER_SIZE;
        if n > data_size {
            return Err(io_error!(
                InvalidInput,
                format!("discard({n}) exceeds payload size ({data_size})")
            ));
        }
        let new_data_len = data_size - n;
        let locked = self.locked.load(Ordering::Acquire);
        if new_data_len < locked {
            return Err(io_error!(
                InvalidInput,
                format!("discard({n}) would shrink payload below locked length ({locked})")
            ));
        }
        fault_point!(self, "discard");
        Self::mark_replay(replay, commit_shrink(file, clen, new_data_len))?;
        Ok(())
    }

    /// Overwrite `data` bytes in place starting at logical `offset`.
    ///
    /// The file size is never changed: if `offset + data.len()` would exceed
    /// the current payload size the call is rejected.  An empty slice is a
    /// valid no-op.
    ///
    /// # Feature flag
    ///
    /// Only available when the `set` Cargo feature is enabled.
    ///
    /// # Durability & atomicity
    ///
    /// Crash-atomic: after a crash the slice holds either its old contents or the
    /// full new `data`, never a partial mix. A write confined to a single aligned
    /// storage block is committed with one durably-synced write; a larger write
    /// goes through the write-in-progress journal (stage → arm → commit → disarm),
    /// which recovery replays or rolls back on the next [`open`](Self::open). The
    /// overwritten bytes are durably synced before the call returns.
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::InvalidInput`] if `offset + data.len()`
    /// exceeds the current payload size, or if the addition overflows `u64`.
    /// Propagates any I/O error from `write_all`, `set_len`, or `durable_sync`.
    #[cfg(feature = "set")]
    pub fn set(&self, offset: u64, data: impl AsRef<[u8]>) -> io::Result<()> {
        let data = data.as_ref();
        if data.is_empty() {
            return Ok(());
        }
        let end = checked_end(offset, data.len() as u64, "set: offset + len overflows u64")?;
        let mut guard = self.write_lock()?;
        let (file, _, replay) = &mut *guard;
        // Load `locked` under the write lock — otherwise a concurrent
        // `lock_up_to` could extend the locked region between our check and
        // our write, letting us mutate a now-immutable byte.
        let locked = self.locked.load(Ordering::Acquire);
        check_offset_unlocked("set", offset, end, locked)?;
        let data_size = file.seek(SeekFrom::End(0))?.saturating_sub(HEADER_SIZE);
        if end > data_size {
            return Err(io_error!(
                InvalidInput,
                format!("set: write end ({end}) exceeds payload size ({data_size})")
            ));
        }
        fault_point!(self, "set");
        Self::mark_replay(replay, set_in_place(file, data_size, offset, data))
    }

    /// Overwrite `n` bytes with zeros in place starting at logical `offset`.
    ///
    /// The file size is never changed: if `offset + n` would exceed
    /// the current payload size the call is rejected.  `n = 0` is a
    /// valid no-op.
    ///
    /// # Feature flag
    ///
    /// Only available when the `set` Cargo feature is enabled.
    ///
    /// # Durability & atomicity
    ///
    /// Crash-atomic on the same terms as [`set`](Self::set): the zeroed slice
    /// survives a crash as either its old contents or all-zeros, never a mix.
    /// Small writes take the single-block atomic path; larger ones go through the
    /// write-in-progress journal. The overwritten bytes are durably synced before
    /// the call returns.
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::InvalidInput`] if `offset + n`
    /// exceeds the current payload size, or if the addition overflows `u64`.
    /// Propagates any I/O error from `write_all`, `set_len`, or `durable_sync`.
    #[cfg(feature = "set")]
    pub fn zero(&self, offset: u64, n: u64) -> io::Result<()> {
        if n == 0 {
            return Ok(());
        }
        let end = checked_end(offset, n, "zero: offset + n overflows u64")?;
        let mut guard = self.write_lock()?;
        let (file, _, replay) = &mut *guard;
        // Load `locked` under the write lock (see `set` for rationale).
        let locked = self.locked.load(Ordering::Acquire);
        check_offset_unlocked("zero", offset, end, locked)?;
        let data_size = file.seek(SeekFrom::End(0))?.saturating_sub(HEADER_SIZE);
        if end > data_size {
            return Err(io_error!(
                InvalidInput,
                format!("zero: write end ({end}) exceeds payload size ({data_size})")
            ));
        }
        // Zeroing is a repeat-fill of the single-byte pattern `[0x00]` `n` times:
        // the journal stages a fixed 9-byte `[k | s]` tail instead of `n` bytes.
        fault_point!(self, "zero");
        Self::mark_replay(replay, repeat_fill(file, data_size, offset, &[0u8], n))
    }

    /// Fill `count` copies of `pattern` in place starting at logical `offset` —
    /// i.e. overwrite `[offset, offset + count * pattern.len())` with the pattern
    /// repeated back to back.
    ///
    /// The file size is never changed: if the filled region would exceed the
    /// current payload size the call is rejected. An empty `pattern` or
    /// `count == 0` is a valid no-op.
    ///
    /// This is the general form of [`zero`](Self::zero) (which is `repeat` of the
    /// single byte `0x00`). Because only the pattern and count are journaled, a
    /// crash-safe fill of a large region costs a fixed-size journal rather than
    /// one proportional to the region — cheap for e.g. clearing or stamping a
    /// large area with a small repeating value.
    ///
    /// # Feature flag
    ///
    /// Only available when the `set` Cargo feature is enabled.
    ///
    /// # Durability & atomicity
    ///
    /// Crash-atomic on the same terms as [`set`](Self::set): after a crash the
    /// region holds either its old contents or the fully repeated pattern, never a
    /// mix. A fill confined to one aligned block takes the single-block atomic
    /// path; a larger one goes through the write-in-progress journal, which stages
    /// only `[count | pattern]` and replays it on the next [`open`](Self::open).
    /// The written bytes are durably synced before the call returns.
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::InvalidInput`] if `count * pattern.len()` or
    /// `offset + count * pattern.len()` overflows `u64`, or if the filled region
    /// exceeds the current payload size. Propagates any I/O error from `write_all`,
    /// `set_len`, or `durable_sync`.
    #[cfg(feature = "set")]
    pub fn repeat(&self, offset: u64, pattern: impl AsRef<[u8]>, count: u64) -> io::Result<()> {
        let pattern = pattern.as_ref();
        if pattern.is_empty() || count == 0 {
            return Ok(());
        }
        let total = (pattern.len() as u64).checked_mul(count).ok_or_else(|| {
            io_error!(InvalidInput, "repeat: count * pattern.len() overflows u64")
        })?;
        let end = checked_end(
            offset,
            total,
            "repeat: offset + count*pattern.len() overflows u64",
        )?;
        let mut guard = self.write_lock()?;
        let (file, _, replay) = &mut *guard;
        // Load `locked` under the write lock (see `set` for rationale).
        let locked = self.locked.load(Ordering::Acquire);
        check_offset_unlocked("repeat", offset, end, locked)?;
        let data_size = file.seek(SeekFrom::End(0))?.saturating_sub(HEADER_SIZE);
        if end > data_size {
            return Err(io_error!(
                InvalidInput,
                format!("repeat: write end ({end}) exceeds payload size ({data_size})")
            ));
        }
        fault_point!(self, "repeat");
        Self::mark_replay(replay, repeat_fill(file, data_size, offset, pattern, count))
    }
}

// ---------------------------------------------------------------------------
// Atomic compound operations

#[cfg(feature = "atomic")]
impl BStack {
    /// Cut `n` bytes off the tail then append `buf` as a single atomic operation.
    ///
    /// **Crash-atomic:** after a crash the tail holds either its old contents or
    /// the full replacement, never a mix. The commit dispatches on the shape of
    /// the replacement (see *Durability* in the crate docs and `algos/WIP.md`):
    ///
    /// * **Pure truncation** (`buf` empty): drop the tail and commit the smaller
    ///   `clen` — the truncation is the commit point.
    /// * **Pure append** (`n == 0`): the bytes land beyond the committed end,
    ///   uncommitted until the `clen` write, so a crash rolls back by truncation.
    /// * **Same length** (`buf.len() == n`): overwrite in place via the `Set`
    ///   write-in-progress journal (or a single-block atomic write).
    /// * **Length change** (`buf.len() != n`, both non-zero): the **splice
    ///   journal** (`SpliceGrow`/`SpliceShrink`) — stage the new tail past the
    ///   live payload, arm the direction, replay it into place, then commit the
    ///   new `clen` and disarm in one atomic header write. Recovery derives the
    ///   new length from the file size and rolls a crash forward, or rolls back
    ///   if the arm never landed.
    ///
    /// `n = 0` with an empty `buf` is a valid no-op.
    ///
    /// # Feature flag
    ///
    /// Only available when the `atomic` Cargo feature is enabled.
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::InvalidInput`] if `n` exceeds the current
    /// payload size.  Propagates any I/O error from `set_len`, `write_all`,
    /// or `durable_sync`.
    #[cfg(feature = "atomic")]
    pub fn atrunc(&self, n: u64, buf: impl AsRef<[u8]>) -> io::Result<()> {
        let buf = buf.as_ref();
        let buf_len = buf.len() as u64;
        if n == 0 && buf_len == 0 {
            return Ok(());
        }
        let mut guard = self.write_lock()?;
        let (file, clen, replay) = &mut *guard;
        let file_end = file.seek(SeekFrom::End(0))?;
        let data_size = file_end - HEADER_SIZE;
        if n > data_size {
            return Err(io_error!(
                InvalidInput,
                format!("atrunc: n ({n}) exceeds payload size ({data_size})")
            ));
        }
        let locked = self.locked.load(Ordering::Acquire);
        let new_tail_start = data_size - n;
        if new_tail_start < locked {
            return Err(io_error!(
                InvalidInput,
                format!("atrunc: operation would modify locked region [0, {locked})")
            ));
        }
        fault_point!(self, "atrunc");
        Self::mark_replay(
            replay,
            commit_tail_replace(file, clen, new_tail_start, n, buf, file_end),
        )
    }

    /// Pop `n` bytes off the tail then append `buf`, returning the removed bytes.
    ///
    /// The bytes are read before any mutation, so they are always available in
    /// the returned `Vec` even if the subsequent write fails.  The replacement
    /// commits with the same crash-atomic, shape-dispatched strategy as
    /// [`atrunc`](Self::atrunc).
    ///
    /// `n = 0` with an empty `buf` is a valid no-op and returns an empty `Vec`.
    ///
    /// # Feature flag
    ///
    /// Only available when the `atomic` Cargo feature is enabled.
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::InvalidInput`] if `n` exceeds the current
    /// payload size.  Propagates any I/O error from `read_exact`, `set_len`,
    /// `write_all`, or `durable_sync`.
    #[cfg(feature = "atomic")]
    pub fn splice(&self, n: u64, buf: impl AsRef<[u8]>) -> io::Result<Vec<u8>> {
        let buf = buf.as_ref();
        let buf_len = buf.len() as u64;
        if n == 0 && buf_len == 0 {
            return Ok(Vec::new());
        }
        let mut guard = self.write_lock()?;
        let (file, clen, replay) = &mut *guard;
        let file_end = file.seek(SeekFrom::End(0))?;
        let data_size = file_end - HEADER_SIZE;
        if n > data_size {
            return Err(io_error!(
                InvalidInput,
                format!("splice: n ({n}) exceeds payload size ({data_size})")
            ));
        }
        let locked = self.locked.load(Ordering::Acquire);
        let new_tail_start = data_size - n;
        if new_tail_start < locked {
            return Err(io_error!(
                InvalidInput,
                format!("splice: operation would modify locked region [0, {locked})")
            ));
        }
        fault_point!(self, "splice");
        // Read the bytes to remove before any mutation.
        let mut removed = vec![0u8; n as usize];
        read_at(file, new_tail_start, &mut removed)?;

        Self::mark_replay(
            replay,
            commit_tail_replace(file, clen, new_tail_start, n, buf, file_end),
        )?;
        Ok(removed)
    }

    /// Pop `old.len()` bytes off the tail into `old`, then append `new`.
    ///
    /// Buffer-reuse counterpart of [`splice`](Self::splice): avoids allocating
    /// a `Vec` for the removed bytes by writing them into the caller-supplied
    /// `old` slice.  The replacement commits with the same crash-atomic,
    /// shape-dispatched strategy as [`atrunc`](Self::atrunc).
    ///
    /// An empty `old` with an empty `new` is a valid no-op.
    ///
    /// # Feature flag
    ///
    /// Only available when the `atomic` Cargo feature is enabled.
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::InvalidInput`] if `old.len()` exceeds the
    /// current payload size.  Propagates any I/O error from `read_exact`,
    /// `set_len`, `write_all`, or `durable_sync`.
    #[cfg(feature = "atomic")]
    pub fn splice_into(&self, old: &mut [u8], new: impl AsRef<[u8]>) -> io::Result<()> {
        let new = new.as_ref();
        let n = old.len() as u64;
        let new_len = new.len() as u64;
        if n == 0 && new_len == 0 {
            return Ok(());
        }
        let mut guard = self.write_lock()?;
        let (file, clen, replay) = &mut *guard;
        let file_end = file.seek(SeekFrom::End(0))?;
        let data_size = file_end - HEADER_SIZE;
        if n > data_size {
            return Err(io_error!(
                InvalidInput,
                format!("splice_into: n ({n}) exceeds payload size ({data_size})")
            ));
        }
        let locked = self.locked.load(Ordering::Acquire);
        let new_tail_start = data_size - n;
        if new_tail_start < locked {
            return Err(io_error!(
                InvalidInput,
                format!("splice_into: operation would modify locked region [0, {locked})")
            ));
        }
        fault_point!(self, "splice_into");
        // Read the bytes to remove before any mutation.
        read_at(file, new_tail_start, old)?;

        Self::mark_replay(
            replay,
            commit_tail_replace(file, clen, new_tail_start, n, new, file_end),
        )
    }

    /// Append `buf` only if the current logical payload size equals `s`.
    ///
    /// Returns `Ok(true)` if the size matched and `buf` was appended (or `buf`
    /// is empty and no I/O was needed).  Returns `Ok(false)` without modifying
    /// the file if the size does not match.
    ///
    /// # Feature flag
    ///
    /// Only available when the `atomic` Cargo feature is enabled.
    ///
    /// # Errors
    ///
    /// Propagates any I/O error from `write_all`, `write_committed_len`, or
    /// `durable_sync`.
    #[cfg(feature = "atomic")]
    pub fn try_extend(&self, s: u64, buf: impl AsRef<[u8]>) -> io::Result<bool> {
        let buf = buf.as_ref();
        let mut guard = self.write_lock()?;
        let (file, clen, replay) = &mut *guard;
        let file_end = file.seek(SeekFrom::End(0))?;
        let data_size = file_end - HEADER_SIZE;
        if data_size != s {
            return Ok(false);
        }
        if buf.is_empty() {
            return Ok(true);
        }
        fault_point!(self, "try_extend");
        if let Err(e) = file.write_all(buf) {
            // A failed rollback leaves a stale tail past the committed length:
            // defer it to the next write's replay.
            if file.set_len(file_end).is_err() {
                *replay = true;
            }
            return Err(e);
        }
        let new_len = data_size + buf.len() as u64;
        Self::mark_replay(
            replay,
            commit_grow(file, clen, new_len, data_size, file_end),
        )?;
        Ok(true)
    }

    /// Append `n` zero bytes only if the current logical payload size equals `s`.
    ///
    /// Returns `Ok(true)` if the size matched and `n` zero bytes were appended
    /// (or `n = 0` and no I/O was needed).  Returns `Ok(false)` without
    /// modifying the file if the size does not match.
    ///
    /// # Feature flag
    ///
    /// Only available when the `atomic` Cargo feature is enabled.
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::InvalidInput`] if adding `n` to the current
    /// payload size would overflow `u64`.  Propagates any I/O error from
    /// `set_len`, `write_committed_len`, or `durable_sync`.
    #[cfg(feature = "atomic")]
    pub fn try_extend_zeros(&self, s: u64, n: u64) -> io::Result<bool> {
        let mut guard = self.write_lock()?;
        let (file, clen, replay) = &mut *guard;
        let file_end = file.seek(SeekFrom::End(0))?;
        let data_size = file_end - HEADER_SIZE;
        if data_size != s {
            return Ok(false);
        }
        if n == 0 {
            return Ok(true);
        }
        let new_len = checked_end(
            data_size,
            n,
            "try_extend_zeros: data_size + n overflows u64",
        )?;
        fault_point!(self, "try_extend_zeros");
        Self::mark_replay(replay, file.set_len(HEADER_SIZE + new_len))?;
        Self::mark_replay(
            replay,
            commit_grow(file, clen, new_len, data_size, file_end),
        )?;
        Ok(true)
    }

    /// Sparsely grow the payload by `length` bytes with `buf` at the start, only
    /// if the current logical payload size equals `s`.
    ///
    /// The size-guarded counterpart of [`extend_sparse`](Self::extend_sparse).
    /// Returns `Ok(true)` if the size matched and the growth was applied (or
    /// `length = 0` and no I/O was needed); returns `Ok(false)` without modifying
    /// the file if the size does not match. See [`extend_sparse`](Self::extend_sparse)
    /// for the sparse-write semantics and efficiency rationale.
    ///
    /// A malformed request (`buf.len()` exceeding `length`) is rejected with an
    /// error regardless of whether the size matches, so it always surfaces rather
    /// than being masked by a size mismatch.
    ///
    /// # Feature flag
    ///
    /// Only available when the `atomic` Cargo feature is enabled.
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::InvalidInput`] if `buf.len()` exceeds `length`, or
    /// if the current payload size plus `length` overflows `u64`. Propagates any
    /// I/O error from `set_len`, `write_all`, or `durable_sync`.
    #[cfg(feature = "atomic")]
    pub fn try_extend_sparse(
        &self,
        s: u64,
        buf: impl AsRef<[u8]>,
        length: u64,
    ) -> io::Result<bool> {
        let buf = buf.as_ref();
        if buf.len() as u64 > length {
            return Err(io_error!(
                InvalidInput,
                "try_extend_sparse: buffer length ({}) exceeds extension length ({length})",
                buf.len()
            ));
        }
        let mut guard = self.write_lock()?;
        let (file, clen, replay) = &mut *guard;
        let file_end = file.seek(SeekFrom::End(0))?;
        let data_size = file_end - HEADER_SIZE;
        if data_size != s {
            return Ok(false);
        }
        if length == 0 {
            return Ok(true);
        }
        let new_len = checked_end(
            data_size,
            length,
            "try_extend_sparse: data_size + length overflows u64",
        )?;
        fault_point!(self, "try_extend_sparse");
        let one = [(0u64, buf)];
        let blocks: &[(u64, &[u8])] = if buf.is_empty() { &[] } else { &one };
        Self::mark_replay(
            replay,
            commit_sparse_extend(file, clen, data_size, file_end, new_len, blocks),
        )?;
        Ok(true)
    }

    /// Sparsely grow the payload by `length` bytes, scattering several buffers
    /// into the grown region, only if the current logical payload size equals `s`.
    ///
    /// The size-guarded counterpart of
    /// [`extend_sparse_batched`](Self::extend_sparse_batched). Returns `Ok(true)`
    /// if the size matched and the growth was applied (or `length = 0` and no I/O
    /// was needed); returns `Ok(false)` without modifying the file if the size
    /// does not match. See [`extend_sparse_batched`](Self::extend_sparse_batched)
    /// for the scatter semantics.
    ///
    /// A malformed batch (overlapping writes, or a write past `length`) is
    /// rejected with an error regardless of whether the size matches, so it always
    /// surfaces rather than being masked by a size mismatch.
    ///
    /// # Feature flag
    ///
    /// Only available when the `atomic` Cargo feature is enabled.
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::InvalidInput`] if any `relative_offset +
    /// data.len()` overflows `u64` or exceeds `length`, if two writes overlap, or
    /// if the current payload size plus `length` overflows `u64`. Propagates any
    /// I/O error from `set_len`, `write_all`, or `durable_sync`.
    #[cfg(feature = "atomic")]
    pub fn try_extend_sparse_batched<I, D>(
        &self,
        s: u64,
        writes: I,
        length: u64,
    ) -> io::Result<bool>
    where
        I: IntoIterator<Item = (u64, D)>,
        D: AsRef<[u8]>,
    {
        let owned: Vec<(u64, D)> = writes.into_iter().collect();
        let mut blocks: Vec<(u64, &[u8])> = owned
            .iter()
            .map(|(off, d)| (*off, d.as_ref()))
            .filter(|(_, d)| !d.is_empty())
            .collect();
        validate_sparse_blocks(&mut blocks, length, "try_extend_sparse_batched")?;

        let mut guard = self.write_lock()?;
        let (file, clen, replay) = &mut *guard;
        let file_end = file.seek(SeekFrom::End(0))?;
        let data_size = file_end - HEADER_SIZE;
        if data_size != s {
            return Ok(false);
        }
        if length == 0 {
            return Ok(true);
        }
        let new_len = checked_end(
            data_size,
            length,
            "try_extend_sparse_batched: data_size + length overflows u64",
        )?;
        fault_point!(self, "try_extend_sparse_batched");
        Self::mark_replay(
            replay,
            commit_sparse_extend(file, clen, data_size, file_end, new_len, &blocks),
        )?;
        Ok(true)
    }

    /// Discard `n` bytes only if the current logical payload size equals `s`.
    ///
    /// Returns `Ok(true)` if the size matched and `n` bytes were removed (or
    /// `n = 0` and the size check passed without I/O).  Returns `Ok(false)`
    /// without modifying the file if the size does not match.
    ///
    /// When `n = 0` only the read lock is taken (no file mutation occurs).
    ///
    /// # Feature flag
    ///
    /// Only available when the `atomic` Cargo feature is enabled.
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::InvalidInput`] if `n` exceeds the current
    /// payload size.  Propagates any I/O error from `set_len`,
    /// `write_committed_len`, or `durable_sync`.
    #[cfg(feature = "atomic")]
    pub fn try_discard(&self, s: u64, n: u64) -> io::Result<bool> {
        if n == 0 {
            let guard = self.read_lock()?;
            let file = &guard.0;
            let data_size = file.metadata()?.len().saturating_sub(HEADER_SIZE);
            return Ok(data_size == s);
        }
        let mut guard = self.write_lock()?;
        let (file, clen, replay) = &mut *guard;
        let raw_size = file.seek(SeekFrom::End(0))?;
        let data_size = raw_size - HEADER_SIZE;
        if data_size != s {
            return Ok(false);
        }
        if n > data_size {
            return Err(io_error!(
                InvalidInput,
                format!("try_discard: n ({n}) exceeds payload size ({data_size})")
            ));
        }
        let new_data_len = data_size - n;
        let locked = self.locked.load(Ordering::Acquire);
        if new_data_len < locked {
            return Err(io_error!(
                InvalidInput,
                format!("try_discard: would shrink payload below locked length ({locked})")
            ));
        }
        fault_point!(self, "try_discard");
        Self::mark_replay(replay, commit_shrink(file, clen, new_data_len))?;
        Ok(true)
    }

    /// Read multiple logical ranges in a single lock acquisition.
    ///
    /// Takes any iterator whose items are [`Range<u64>`](std::ops::Range) and
    /// returns a [`Vec`] of owned byte buffers, one per input range, in the
    /// same order.  An empty iterator returns an empty `Vec`.  An empty range
    /// (`start == end`) produces an empty inner `Vec`.
    ///
    /// All reads happen under the same shared lock, so no write can interleave
    /// between them.  On Unix and Windows the shared read lock is taken once
    /// for all non-locked ranges; on other platforms the write lock serialises
    /// all reads.
    ///
    /// # Feature flag
    ///
    /// Only available when the `atomic` Cargo feature is enabled.
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::InvalidInput`] if any range has `end < start`
    /// or if any `end` exceeds the current payload size.
    ///
    /// Fails with [`InterruptedWrite`] while an earlier write is pending replay.
    #[cfg(feature = "atomic")]
    pub fn get_batched<I>(&self, ranges: I) -> io::Result<Vec<Vec<u8>>>
    where
        I: IntoIterator<Item = std::ops::Range<u64>>,
    {
        let ranges: Vec<std::ops::Range<u64>> = ranges.into_iter().collect();
        if ranges.is_empty() {
            return Ok(Vec::new());
        }
        for r in &ranges {
            if r.end < r.start {
                return Err(io_error!(
                    InvalidInput,
                    "get_batched: end ({}) < start ({})",
                    r.end,
                    r.start
                ));
            }
        }
        #[cfg(any(unix, windows))]
        {
            let guard = self.read_lock()?;
            let file = &guard.0;
            let data_size = file.metadata()?.len().saturating_sub(HEADER_SIZE);
            fault_point!(self, "get_batched");
            let mut results = Vec::with_capacity(ranges.len());
            for r in &ranges {
                if r.end > data_size {
                    return Err(io_error!(
                        InvalidInput,
                        "get_batched: end ({}) exceeds payload size ({data_size})",
                        r.end
                    ));
                }
                results.push(pread_exact(
                    file,
                    HEADER_SIZE + r.start,
                    (r.end - r.start) as usize,
                )?);
            }
            Ok(results)
        }
        #[cfg(not(any(unix, windows)))]
        {
            let mut guard = self.write_lock_read()?;
            let file = &mut guard.0;
            let data_size = file.seek(SeekFrom::End(0))?.saturating_sub(HEADER_SIZE);
            fault_point!(self, "get_batched");
            let mut results = Vec::with_capacity(ranges.len());
            for r in &ranges {
                if r.end > data_size {
                    return Err(io_error!(
                        InvalidInput,
                        "get_batched: end ({}) exceeds payload size ({data_size})",
                        r.end
                    ));
                }
                file.seek(SeekFrom::Start(HEADER_SIZE + r.start))?;
                let mut buf = vec![0u8; (r.end - r.start) as usize];
                file.read_exact(&mut buf)?;
                results.push(buf);
            }
            Ok(results)
        }
    }

    /// Read multiple logical ranges into caller-provided buffers in a single lock acquisition.
    ///
    /// Takes any iterator whose items are `(u64, &mut [u8])` — a start offset
    /// and a mutable buffer to fill.  The number of bytes read for each entry
    /// equals `buf.len()`.  An empty iterator returns immediately.
    ///
    /// All reads happen under the same shared lock, so no write can interleave
    /// between them.  On Unix and Windows the shared read lock is taken once
    /// for all reads; on other platforms the write lock serialises all reads.
    ///
    /// # Feature flag
    ///
    /// Only available when the `atomic` Cargo feature is enabled.
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::InvalidInput`] if any `offset + buf.len()`
    /// overflows `u64` or if any read would extend beyond the current payload
    /// size.
    ///
    /// Fails with [`InterruptedWrite`] while an earlier write is pending replay.
    #[cfg(feature = "atomic")]
    pub fn get_batched_into<'a, I>(&self, bufs: I) -> io::Result<()>
    where
        I: IntoIterator<Item = (u64, &'a mut [u8])>,
    {
        let bufs: Vec<(u64, &'a mut [u8])> = bufs.into_iter().collect();
        if bufs.is_empty() {
            return Ok(());
        }
        #[cfg(any(unix, windows))]
        {
            let guard = self.read_lock()?;
            let file = &guard.0;
            let data_size = file.metadata()?.len().saturating_sub(HEADER_SIZE);
            fault_point!(self, "get_batched_into");
            for (ptr, buf) in bufs {
                let end = ptr.checked_add(buf.len() as u64).ok_or_else(|| {
                    io_error!(
                        InvalidInput,
                        "get_batched_into: offset + buf.len() overflows u64"
                    )
                })?;
                if end > data_size {
                    return Err(io_error!(
                        InvalidInput,
                        format!("get_batched_into: end ({end}) exceeds payload size ({data_size})",)
                    ));
                }
                pread_exact_into(file, HEADER_SIZE + ptr, buf)?;
            }
            Ok(())
        }
        #[cfg(not(any(unix, windows)))]
        {
            let mut guard = self.write_lock_read()?;
            let file = &mut guard.0;
            let data_size = file.seek(SeekFrom::End(0))?.saturating_sub(HEADER_SIZE);
            fault_point!(self, "get_batched_into");
            for (ptr, buf) in bufs {
                let end = ptr.checked_add(buf.len() as u64).ok_or_else(|| {
                    io_error!(
                        InvalidInput,
                        "get_batched_into: offset + buf.len() overflows u64"
                    )
                })?;
                if end > data_size {
                    return Err(io_error!(
                        InvalidInput,
                        format!("get_batched_into: end ({end}) exceeds payload size ({data_size})",)
                    ));
                }
                file.seek(SeekFrom::Start(HEADER_SIZE + ptr))?;
                file.read_exact(buf)?;
            }
            Ok(())
        }
    }

    /// Read a dependent chain of logical ranges in a single lock acquisition.
    ///
    /// `gen` is called once per read step.  Each call returns `Some((offset,
    /// buf))` to request a read of `buf.len()` bytes starting at `offset` into
    /// `buf`, or `None` to stop.  When `gen` is called, the buffer supplied by
    /// the *previous* call has already been filled with its data — the call
    /// itself signals that the prior buffer is ready.
    ///
    /// All reads happen under the same shared lock (Unix/Windows: read lock;
    /// other platforms: write lock), so no write can interleave between steps.
    ///
    /// # Feature flag
    ///
    /// Only available when the `atomic` Cargo feature is enabled.
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::InvalidInput`] if any `offset + buf.len()`
    /// overflows `u64` or exceeds the current payload size.
    ///
    /// Fails with [`InterruptedWrite`] while an earlier write is pending replay.
    #[cfg(feature = "atomic")]
    pub fn get_batched_gen<'a, F>(&self, mut f: F) -> io::Result<()>
    where
        F: FnMut() -> Option<(u64, &'a mut [u8])>,
    {
        #[cfg(any(unix, windows))]
        {
            let guard = self.read_lock()?;
            let file = &guard.0;
            let data_size = file.metadata()?.len().saturating_sub(HEADER_SIZE);
            fault_point!(self, "get_batched_gen");
            while let Some((offset, buf)) = f() {
                let end = offset.checked_add(buf.len() as u64).ok_or_else(|| {
                    io_error!(
                        InvalidInput,
                        "get_batched_gen: offset + buf.len() overflows u64"
                    )
                })?;
                if end > data_size {
                    return Err(io_error!(
                        InvalidInput,
                        format!("get_batched_gen: end ({end}) exceeds payload size ({data_size})")
                    ));
                }
                // Per-step read fault: stands in for this read's I/O and ends
                // the whole call, as a genuine read failure here does.
                fault_point!(self, "get_batched_gen:read");
                pread_exact_into(file, HEADER_SIZE + offset, buf)?;
            }
            Ok(())
        }
        #[cfg(not(any(unix, windows)))]
        {
            let mut guard = self.write_lock_read()?;
            let file = &mut guard.0;
            let data_size = file.seek(SeekFrom::End(0))?.saturating_sub(HEADER_SIZE);
            fault_point!(self, "get_batched_gen");
            while let Some((offset, buf)) = f() {
                let end = offset.checked_add(buf.len() as u64).ok_or_else(|| {
                    io_error!(
                        InvalidInput,
                        "get_batched_gen: offset + buf.len() overflows u64"
                    )
                })?;
                if end > data_size {
                    return Err(io_error!(
                        InvalidInput,
                        format!("get_batched_gen: end ({end}) exceeds payload size ({data_size})")
                    ));
                }
                // Per-step read fault: stands in for this read's I/O and ends
                // the whole call, as a genuine read failure here does.
                fault_point!(self, "get_batched_gen:read");
                file.seek(SeekFrom::Start(HEADER_SIZE + offset))?;
                file.read_exact(buf)?;
            }
            Ok(())
        }
    }

    /// Pop `n` bytes off the tail, pass them read-only to a callback that
    /// returns the new tail bytes, then write the new tail.
    ///
    /// The read, callback invocation, and write all happen under the same write
    /// lock, so no other thread can observe the state between the pop and the
    /// push.  The callback may return a [`Vec<u8>`] of any length — the file
    /// will grow or shrink accordingly using the same crash-safe ordering
    /// strategy as [`atrunc`](Self::atrunc).
    ///
    /// `n = 0` is valid: the callback receives an empty slice and whatever it
    /// returns is appended.
    ///
    /// # Feature flag
    ///
    /// Only available when the `atomic` Cargo feature is enabled.
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::InvalidInput`] if `n` exceeds the current
    /// payload size.  Propagates any I/O error from `read_exact`, `set_len`,
    /// `write_all`, or `durable_sync`.
    #[cfg(feature = "atomic")]
    pub fn replace<F>(&self, n: u64, f: F) -> io::Result<()>
    where
        F: FnOnce(&[u8]) -> Vec<u8>,
    {
        let mut guard = self.write_lock()?;
        let (file, clen, replay) = &mut *guard;
        let file_end = file.seek(SeekFrom::End(0))?;
        let data_size = file_end - HEADER_SIZE;
        if n > data_size {
            return Err(io_error!(
                InvalidInput,
                format!("replace: n ({n}) exceeds payload size ({data_size})")
            ));
        }
        let locked = self.locked.load(Ordering::Acquire);
        let new_tail_start = data_size - n;
        if new_tail_start < locked {
            return Err(io_error!(
                InvalidInput,
                format!("replace: operation would modify locked region [0, {locked})")
            ));
        }
        fault_point!(self, "replace");
        let mut old_tail = vec![0u8; n as usize];
        read_at(file, new_tail_start, &mut old_tail)?;
        let new_tail = f(&old_tail);

        Self::mark_replay(
            replay,
            commit_tail_replace(file, clen, new_tail_start, n, &new_tail, file_end),
        )
    }
}

/// A request to read from, or write to, a region of the payload.
///
/// A value of this type describes a single request and carries no protocol of
/// its own — how a sequence of requests is assembled, interpreted, and
/// brought to an end is entirely up to the primitive that consumes them.
/// Different primitives may impose different rules over the same variants:
/// how many writes are permitted, whether a write ends the sequence, how the
/// sequence itself signals that it is done, and so on. The "ending the
/// sequence" notes below describe [`process_gen`](BStack::process_gen);
/// [`inplace_gen`](BStack::inplace_gen) instead accumulates multiple `Write`s
/// (none ends the sequence) and permits only `Read`, `Write`, `Len`, and
/// `Abort`.
///
/// # Variants
///
/// - `Read { offset, buf }` — read some number of bytes starting at logical
///   `offset` into the caller-supplied buffer `buf`.
/// - `Write { offset, data }` — write `data` starting at logical `offset`,
///   ending the sequence.
/// - `Swap { a_offset, b_offset, len }` — atomically exchange `len` bytes at
///   `a_offset` with `len` bytes at `b_offset`, ending the sequence.
/// - `Push { data }` — append `data` to the end of the file, growing the
///   payload, and ending the sequence.
/// - `Pop { buf }` — remove the last `buf.len()` bytes from the end of the
///   file into `buf`, shrinking the payload, and ending the sequence.
/// - `Discard { len }` — remove the last `len` bytes without reading them back,
///   shrinking the payload, and ending the sequence.
/// - `Atrunc { n, data }` — cut `n` bytes off the tail then append `data`
///   (no readback), ending the sequence.
/// - `Splice { old, new }` — pop `old.len()` bytes off the tail into `old` then
///   append `new`, ending the sequence.
/// - `Len { out }` — write the current logical payload size into `out`.
/// - `Abort { source }` — end the sequence discarding anything it accumulated,
///   failing the call with `source` if it is `Some`.
/// - `#[non_exhaustive]` — later versions may add further variants, for
///   richer write ownership or multi-write protocols, for instance, without
///   a breaking change.
///
/// # Feature flags
///
/// Only available when both the `set` and `atomic` Cargo features are enabled.
#[cfg(all(feature = "set", feature = "atomic"))]
#[non_exhaustive]
// Intentionally not `PartialEq`/`Eq`/`Hash`: each value is a transient,
// single-use request consumed immediately by `process_gen`'s loop, and for
// `Read` "equality" would mean comparing the destination buffer's stale
// pre-read contents — not a meaningful notion of identity.
#[derive(Debug)]
pub enum BStackGenOp<'a> {
    /// Read `buf.len()` bytes starting at logical `offset` into `buf`.
    Read {
        /// Logical offset to read from.
        offset: u64,
        /// Destination buffer; its length determines how many bytes are read.
        buf: &'a mut [u8],
    },
    /// Write `data` to logical `offset..offset + data.len()`, ending the
    /// sequence.
    Write {
        /// Logical offset to write to.
        offset: u64,
        /// Bytes to write.
        data: &'a [u8],
    },
    /// Atomically exchange `len` bytes at `a_offset` with `len` bytes at
    /// `b_offset`, ending the sequence.
    ///
    /// Both regions are read and then swapped under the same held write
    /// lock — the in-sequence equivalent of [`cross_exchange`](BStack::cross_exchange),
    /// useful when one or both offsets are only known once earlier `Read`s
    /// in the sequence have been resolved (e.g. "read the free-list head,
    /// then swap this block into that slot"). The regions must not overlap.
    Swap {
        /// Logical offset of the first region.
        a_offset: u64,
        /// Logical offset of the second region.
        b_offset: u64,
        /// Number of bytes to exchange.
        len: u64,
    },
    /// Append `data` to the end of the file, growing the payload by
    /// `data.len()` bytes, and ending the sequence.
    Push {
        /// Bytes to append.
        data: &'a [u8],
    },
    /// Remove the last `buf.len()` bytes from the end of the file into
    /// `buf`, shrinking the payload by `buf.len()` bytes, and ending the
    /// sequence.
    Pop {
        /// Destination buffer; its length determines how many bytes are
        /// popped.
        buf: &'a mut [u8],
    },
    /// Remove the last `len` bytes from the end of the file, shrinking the
    /// payload by `len` bytes, and ending the sequence.
    ///
    /// The dropped bytes are **not** read back — the in-sequence equivalent of
    /// [`discard`](BStack::discard), and the buffer-free counterpart of
    /// [`Pop`](Self::Pop) (mirroring a C `pop` with a `NULL` destination).
    /// Useful for truncating a tail whose size is only known once earlier
    /// `Read`s have been resolved, without allocating a throwaway buffer.
    Discard {
        /// Number of bytes to remove from the end of the file.
        len: u64,
    },
    /// Cut `n` bytes off the tail then append `data` as a single operation,
    /// ending the sequence.
    ///
    /// The removed bytes are **not** read back — the in-sequence equivalent of
    /// [`atrunc`](BStack::atrunc), and the buffer-free counterpart of
    /// [`Splice`](Self::Splice) (as [`Discard`](Self::Discard) is to
    /// [`Pop`](Self::Pop)). The net payload change is `data.len() − n`; useful
    /// for replacing a tail whose size is only known once earlier `Read`s have
    /// been resolved, without allocating a buffer for the discarded bytes.
    Atrunc {
        /// Number of bytes to cut off the tail before appending.
        n: u64,
        /// Bytes to append after the cut.
        data: &'a [u8],
    },
    /// Pop `old.len()` bytes off the tail into `old`, then append `new`, ending
    /// the sequence.
    ///
    /// The removed bytes are read into `old` before any mutation — the
    /// in-sequence equivalent of [`splice_into`](BStack::splice_into), and the
    /// readback counterpart of [`Atrunc`](Self::Atrunc) (as [`Pop`](Self::Pop)
    /// is to [`Discard`](Self::Discard)). The net payload change is
    /// `new.len() − old.len()`.
    Splice {
        /// Destination for the removed tail bytes; its length determines how
        /// many bytes are popped.
        old: &'a mut [u8],
        /// Bytes to append after the pop.
        new: &'a [u8],
    },
    /// Sparsely grow the payload by `length` bytes, scattering `writes` into the
    /// freshly grown region and leaving the gaps between them zero, then ending
    /// the sequence.
    ///
    /// The in-sequence equivalent of
    /// [`extend_sparse_batched`](BStack::extend_sparse_batched) (and, with a
    /// single write at relative offset `0`, of
    /// [`extend_sparse`](BStack::extend_sparse)). Each `(relative_offset, data)`
    /// pair writes `data` at logical offset `tail + relative_offset`, where `tail`
    /// is the payload size before the growth; the bytes not covered by any write
    /// read back as zero. The whole `length` is realised with a single `set_len`,
    /// so the zero gaps cost no write I/O. Useful when a large mostly-zero tail —
    /// or a set of blocks at offsets only known once earlier `Read`s have been
    /// resolved — must be appended without materialising the zeros.
    ///
    /// The writes must be **pairwise non-overlapping** and each must fit within
    /// the grown region `[0, length)`; empty `data` slices are ignored. `length =
    /// 0` is valid only when every write is empty.
    Sparse {
        /// `(relative_offset, data)` pairs, each relative offset measured from the
        /// current tail.
        writes: &'a [(u64, &'a [u8])],
        /// Total number of bytes to grow the payload by.
        length: u64,
    },
    /// Write the current logical payload size, in bytes, into `out`, then
    /// call `f` again — does not end the sequence.
    Len {
        /// Destination for the current payload size.
        out: &'a mut u64,
    },
    /// End the sequence **without applying anything it accumulated** — the
    /// counterpart of `None`, which ends it committing everything. `source`
    /// sets the outcome independently: `Some(e)` returns `Err(e)`, `None`
    /// returns `Ok(())`.
    Abort {
        /// The error the call fails with, or `None` to end successfully.
        source: Option<io::Error>,
    },
}

#[cfg(all(feature = "set", feature = "atomic"))]
impl BStack {
    /// Atomically read `buf.len()` bytes at `offset` and overwrite them with
    /// `buf`, returning the old contents.
    ///
    /// Both the read and the write happen under the same write lock, so no
    /// other thread can observe either the pre-swap or mid-swap state.  The
    /// file size is never changed.
    ///
    /// An empty `buf` is a valid no-op and returns an empty `Vec`.
    ///
    /// # Feature flags
    ///
    /// Only available when both the `set` and `atomic` Cargo features are
    /// enabled.
    ///
    /// # Crash atomicity
    ///
    /// Crash-atomic: after a crash the region holds either its old or its new
    /// contents, never a mix — the write commits via a single-block atomic write
    /// or the write-in-progress journal (see the crate-level *Durability* docs).
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::InvalidInput`] if `offset + buf.len()`
    /// overflows `u64` or exceeds the current payload size.  Propagates any
    /// I/O error from `read_exact`, `write_all`, or `durable_sync`.
    #[cfg(all(feature = "set", feature = "atomic"))]
    pub fn swap(&self, offset: u64, buf: impl AsRef<[u8]>) -> io::Result<Vec<u8>> {
        let buf = buf.as_ref();
        if buf.is_empty() {
            return Ok(Vec::new());
        }
        let end = checked_end(offset, buf.len() as u64, "swap: offset + len overflows u64")?;
        let mut guard = self.write_lock()?;
        let (file, _, replay) = &mut *guard;
        // Load `locked` under the write lock (see `set` for rationale).
        let locked = self.locked.load(Ordering::Acquire);
        check_offset_unlocked("swap", offset, end, locked)?;
        let data_size = file.seek(SeekFrom::End(0))?.saturating_sub(HEADER_SIZE);
        if end > data_size {
            return Err(io_error!(
                InvalidInput,
                format!("swap: range [{offset}, {end}) exceeds payload size ({data_size})")
            ));
        }
        fault_point!(self, "swap");
        let mut old = vec![0u8; buf.len()];
        read_at(file, offset, &mut old)?;
        Self::mark_replay(replay, set_in_place(file, data_size, offset, buf))?;
        Ok(old)
    }

    /// Atomically read `buf.len()` bytes at `offset` into `buf` while writing
    /// the original contents of `buf` into that position.
    ///
    /// On return, `buf` contains the bytes that were previously at `offset`,
    /// and the file contains what `buf` held on entry.  Buffer-reuse
    /// counterpart of [`swap`](Self::swap).
    ///
    /// An empty `buf` is a valid no-op.
    ///
    /// # Feature flags
    ///
    /// Only available when both the `set` and `atomic` Cargo features are
    /// enabled.
    ///
    /// # Crash atomicity
    ///
    /// Crash-atomic, on the same terms as [`swap`](Self::swap).
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::InvalidInput`] if `offset + buf.len()`
    /// overflows `u64` or exceeds the current payload size.  Propagates any
    /// I/O error from `read_exact`, `write_all`, or `durable_sync`.
    #[cfg(all(feature = "set", feature = "atomic"))]
    pub fn swap_into(&self, offset: u64, buf: &mut [u8]) -> io::Result<()> {
        if buf.is_empty() {
            return Ok(());
        }
        let end = checked_end(
            offset,
            buf.len() as u64,
            "swap_into: offset + len overflows u64",
        )?;
        let mut guard = self.write_lock()?;
        let (file, _, replay) = &mut *guard;
        // Load `locked` under the write lock (see `set` for rationale).
        let locked = self.locked.load(Ordering::Acquire);
        check_offset_unlocked("swap_into", offset, end, locked)?;
        let data_size = file.seek(SeekFrom::End(0))?.saturating_sub(HEADER_SIZE);
        if end > data_size {
            return Err(io_error!(
                InvalidInput,
                format!("swap_into: range [{offset}, {end}) exceeds payload size ({data_size})")
            ));
        }
        fault_point!(self, "swap_into");
        let mut tmp = vec![0u8; buf.len()];
        read_at(file, offset, &mut tmp)?;
        Self::mark_replay(replay, set_in_place(file, data_size, offset, buf))?;
        buf.copy_from_slice(&tmp);
        Ok(())
    }

    /// Compare-and-exchange: read `old.len()` bytes at `offset` and, if they
    /// equal `old`, overwrite them with `new`.
    ///
    /// Returns `Ok(true)` if the comparison succeeded and the exchange was
    /// performed.  Returns `Ok(false)` without modifying the file if
    /// `old.len() != new.len()` or if the current bytes do not match `old`.
    ///
    /// Both the compare and the exchange happen under the same write lock.
    ///
    /// # Feature flags
    ///
    /// Only available when both the `set` and `atomic` Cargo features are
    /// enabled.
    ///
    /// # Crash atomicity
    ///
    /// When the exchange is performed it is crash-atomic, on the same terms as
    /// [`set`](Self::set): after a crash the region holds either `old` or `new`,
    /// never a mix.
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::InvalidInput`] if `offset + old.len()`
    /// overflows `u64` or exceeds the current payload size.  Propagates any
    /// I/O error from `read_exact`, `write_all`, or `durable_sync`.
    #[cfg(all(feature = "set", feature = "atomic"))]
    pub fn cas(
        &self,
        offset: u64,
        old: impl AsRef<[u8]>,
        new: impl AsRef<[u8]>,
    ) -> io::Result<bool> {
        let old = old.as_ref();
        let new = new.as_ref();
        if old.len() != new.len() {
            return Ok(false);
        }
        if old.is_empty() {
            return Ok(true);
        }
        let end = checked_end(offset, old.len() as u64, "cas: offset + len overflows u64")?;
        let mut guard = self.write_lock()?;
        let (file, _, replay) = &mut *guard;
        // Load `locked` under the write lock (see `set` for rationale).
        let locked = self.locked.load(Ordering::Acquire);
        check_offset_unlocked("cas", offset, end, locked)?;
        let data_size = file.seek(SeekFrom::End(0))?.saturating_sub(HEADER_SIZE);
        if end > data_size {
            return Err(io_error!(
                InvalidInput,
                format!("cas: range [{offset}, {end}) exceeds payload size ({data_size})")
            ));
        }
        fault_point!(self, "cas");
        let mut current = vec![0u8; old.len()];
        read_at(file, offset, &mut current)?;
        if current != old {
            return Ok(false);
        }
        Self::mark_replay(replay, set_in_place(file, data_size, offset, new))?;
        Ok(true)
    }

    /// Atomically swap two equal-size, non-overlapping regions within the file.
    ///
    /// Bytes at `[a, a + n)` and `[b, b + n)` are exchanged under a single
    /// write lock, so no other thread can observe an intermediate state.
    /// The at-rest file size is never changed.  `n = 0` is a valid no-op (bounds
    /// are still checked).
    ///
    /// # Crash atomicity
    ///
    /// Crash-safe: after a crash the two regions hold either their original
    /// contents or the fully swapped contents, never a half-swap. Region A's
    /// bytes are staged in a tail backup and the swap commits at a single atomic
    /// `wip_ptr` flip; recovery on the next [`open`](Self::open) rolls the
    /// exchange back (before the flip) or forward (after it). During the operation
    /// the file grows by `n` bytes to hold the backup, which is dropped on
    /// completion.
    ///
    /// # Feature flags
    ///
    /// Only available when both the `set` and `atomic` Cargo features are
    /// enabled.
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::InvalidInput`] if either `a + n` or `b + n`
    /// overflows `u64`, if the regions overlap, if either region exceeds the
    /// current payload size, or if either region overlaps the locked prefix.
    /// Propagates any I/O error from `read_exact`, `write_all`, or
    /// `durable_sync`.
    #[cfg(all(feature = "set", feature = "atomic"))]
    pub fn cross_exchange(&self, a: u64, b: u64, n: u64) -> io::Result<()> {
        let a_end = checked_end(a, n, "cross_exchange: a + n overflows u64")?;
        let b_end = checked_end(b, n, "cross_exchange: b + n overflows u64")?;
        if n > 0 {
            let (lo, hi) = if a < b { (a, b) } else { (b, a) };
            if lo + n > hi {
                return Err(io_error!(
                    InvalidInput,
                    format!("cross_exchange: regions [{a}, {a_end}) and [{b}, {b_end}) overlap")
                ));
            }
        }
        let mut guard = self.write_lock()?;
        let (file, _, replay) = &mut *guard;
        let locked = self.locked.load(Ordering::Acquire);
        if a < locked {
            return Err(io_error!(
                InvalidInput,
                format!(
                    "cross_exchange: region [{a}, {a_end}) overlaps locked region [0, {locked})"
                )
            ));
        }
        if b < locked {
            return Err(io_error!(
                InvalidInput,
                format!(
                    "cross_exchange: region [{b}, {b_end}) overlaps locked region [0, {locked})"
                )
            ));
        }
        let data_size = file.seek(SeekFrom::End(0))?.saturating_sub(HEADER_SIZE);
        if a_end > data_size {
            return Err(io_error!(
                InvalidInput,
                format!("cross_exchange: region [{a}, {a_end}) exceeds payload size ({data_size})")
            ));
        }
        if b_end > data_size {
            return Err(io_error!(
                InvalidInput,
                format!("cross_exchange: region [{b}, {b_end}) exceeds payload size ({data_size})")
            ));
        }
        if n == 0 {
            return Ok(());
        }
        fault_point!(self, "cross_exchange");
        Self::mark_replay(replay, journaled_exchange(file, data_size, a, b, n))
    }

    /// Copy `n` bytes from `from..from+n` to `to..to+n` under a single write lock.
    ///
    /// Overlapping source and destination are handled correctly: the bytes route
    /// through the journal's tail region, disjoint from both.  `n = 0` is a valid
    /// no-op (bounds are still checked).  The file size is never changed.
    ///
    /// # Feature flags
    ///
    /// Only available when both the `set` and `atomic` Cargo features are
    /// enabled.
    ///
    /// # Durability & atomicity
    ///
    /// Crash-atomic: after a crash the destination holds either its old contents
    /// or the full copy, never a mix.  A destination within one aligned block
    /// takes the single-block atomic path.  A larger *overlapping* copy streams
    /// source→tail→dest through the write-in-progress journal in O(1) memory.  A
    /// larger *disjoint* copy uses the copy journal, which stages only the source
    /// coordinate (not the bytes) and replays directly from the untouched source —
    /// O(1) staging as well.  A copy onto the same location is a no-op.  All paths
    /// are replayed on the next [`open`](Self::open).
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::InvalidInput`] if either `from + n` or `to + n`
    /// overflows `u64`, if either region exceeds the current payload size, or
    /// if the destination region overlaps the locked prefix.
    /// Propagates any I/O error from `read_exact`, `write_all`, or
    /// `durable_sync`.
    #[cfg(all(feature = "set", feature = "atomic"))]
    pub fn copy(&self, from: u64, to: u64, n: u64) -> io::Result<()> {
        let from_end = checked_end(from, n, "copy: from + n overflows u64")?;
        let to_end = checked_end(to, n, "copy: to + n overflows u64")?;
        let mut guard = self.write_lock()?;
        let (file, _, replay) = &mut *guard;
        let locked = self.locked.load(Ordering::Acquire);
        if to < locked {
            return Err(io_error!(
                InvalidInput,
                format!("copy: destination [{to}, {to_end}) overlaps locked region [0, {locked})")
            ));
        }
        let data_size = file.seek(SeekFrom::End(0))?.saturating_sub(HEADER_SIZE);
        if from_end > data_size {
            return Err(io_error!(
                InvalidInput,
                format!("copy: source [{from}, {from_end}) exceeds payload size ({data_size})")
            ));
        }
        if to_end > data_size {
            return Err(io_error!(
                InvalidInput,
                format!("copy: destination [{to}, {to_end}) exceeds payload size ({data_size})")
            ));
        }
        if n == 0 {
            return Ok(());
        }
        // A copy onto its own location leaves every byte unchanged — a no-op once
        // the bounds above are validated.
        if from == to {
            return Ok(());
        }
        fault_point!(self, "copy");
        // Write-strategy hierarchy (see `algos/WIP.md`):
        //  * destination within one aligned block → single-block atomic write
        //    (read the source into a bounded buffer — `n` is at most one block
        //    here — and write it);
        //  * overlapping source/destination → route the bytes through the tail
        //    backup (source→tail→dest) so a replay never reads clobbered source
        //    bytes — `journaled_move`, O(1) memory but staging the full `n` bytes;
        //  * disjoint source/destination → copy journal: stage only the source
        //    coordinate `[src | n]`, since the untouched source lets recovery
        //    replay the copy directly — `journaled_copy`, O(1) staging.
        if is_atomic_write(to, n) {
            let mut buf = vec![0u8; n as usize];
            read_at(file, from, &mut buf)?;
            Self::mark_replay(replay, write_at(file, to, &buf))?;
            Self::mark_replay(replay, durable_sync(file))
        } else if from < to_end && to < from_end {
            Self::mark_replay(replay, journaled_move(file, data_size, from, to, n))
        } else {
            Self::mark_replay(replay, journaled_copy(file, data_size, from, to, n))
        }
    }

    /// Read bytes in the half-open logical range `[start, end)`, pass them to
    /// a callback that may mutate them in place, then write the modified bytes
    /// back.
    ///
    /// The read, callback invocation, and write all happen under the same write
    /// lock, so no other thread can observe an intermediate state.  The file
    /// size is never changed.
    ///
    /// `start == end` is a valid no-op: `f` is called with an empty slice and
    /// no I/O is performed beyond the initial size check.
    ///
    /// # Feature flags
    ///
    /// Only available when both the `set` and `atomic` Cargo features are
    /// enabled.
    ///
    /// # Crash atomicity
    ///
    /// Crash-atomic, on the same terms as [`set`](Self::set): after a crash the
    /// range holds either its pre-callback bytes or the callback's output, never a
    /// mix. (The callback runs in memory, under the lock, before the commit.)
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::InvalidInput`] if `end < start` or if `end`
    /// exceeds the current payload size.  Propagates any I/O error from
    /// `read_exact`, `write_all`, or `durable_sync`.
    #[cfg(all(feature = "set", feature = "atomic"))]
    pub fn process<F>(&self, start: u64, end: u64, f: F) -> io::Result<()>
    where
        F: FnOnce(&mut [u8]),
    {
        if end < start {
            return Err(io_error!(
                InvalidInput,
                format!("process: end ({end}) < start ({start})")
            ));
        }
        let n = end - start;
        let mut guard = self.write_lock()?;
        let (file, _, replay) = &mut *guard;
        let data_size = file.seek(SeekFrom::End(0))?.saturating_sub(HEADER_SIZE);
        if end > data_size {
            return Err(io_error!(
                InvalidInput,
                format!("process: end ({end}) exceeds payload size ({data_size})")
            ));
        }
        let locked = self.locked.load(Ordering::Acquire);
        if start < locked {
            return Err(io_error!(
                InvalidInput,
                format!("process: range [{start}, {end}) overlaps locked region [0, {locked})")
            ));
        }
        fault_point!(self, "process");
        let mut buf = vec![0u8; n as usize];
        if n > 0 {
            read_at(file, start, &mut buf)?;
        }
        f(&mut buf);
        if n > 0 {
            Self::mark_replay(replay, set_in_place(file, data_size, start, &buf))?;
        }
        Ok(())
    }

    /// Run a sequence of dependent reads, optionally followed by a single
    /// write, all under one held write lock.
    ///
    /// `f` is called in a loop and drives the sequence through [`BStackGenOp`]:
    ///
    /// - `Some(BStackGenOp::Read { offset, buf })` reads `offset..offset +
    ///   buf.len()` into `buf` and calls `f` again.  By the time `f` is called,
    ///   the buffer from the *previous* `Read` already holds its data, so each
    ///   step can use earlier results to decide the next one — e.g. "read the
    ///   head pointer, then read the node it points to".
    /// - `Some(BStackGenOp::Write { offset, data })` writes `data` to
    ///   `offset..offset + data.len()` and ends the sequence; `f` is not
    ///   called again.
    /// - `Some(BStackGenOp::Swap { a_offset, b_offset, len })` atomically
    ///   exchanges `len` bytes at `a_offset` with `len` bytes at `b_offset`
    ///   and ends the sequence — the in-sequence equivalent of
    ///   [`cross_exchange`](Self::cross_exchange), useful when a swap target
    ///   is only known once an earlier `Read` has resolved it (e.g. "read the
    ///   free-list head, then splice this block in as the new head").
    /// - `Some(BStackGenOp::Push { data })` appends `data` to the end of the
    ///   file, growing the payload, and ends the sequence — the in-sequence
    ///   equivalent of [`push`](Self::push).
    /// - `Some(BStackGenOp::Pop { buf })` removes the last `buf.len()` bytes
    ///   from the end of the file into `buf`, shrinking the payload, and ends
    ///   the sequence — the in-sequence equivalent of [`pop`](Self::pop).
    /// - `Some(BStackGenOp::Discard { len })` removes the last `len` bytes from
    ///   the end of the file without reading them back, shrinking the payload,
    ///   and ends the sequence — the in-sequence equivalent of
    ///   [`discard`](Self::discard) and the buffer-free counterpart of `Pop`.
    /// - `Some(BStackGenOp::Atrunc { n, data })` cuts `n` bytes off the tail
    ///   then appends `data` (without reading the removed bytes), changing the
    ///   payload by `data.len() − n`, and ends the sequence — the in-sequence
    ///   equivalent of [`atrunc`](Self::atrunc) and the buffer-free counterpart
    ///   of `Splice`.
    /// - `Some(BStackGenOp::Splice { old, new })` pops `old.len()` bytes off the
    ///   tail into `old` then appends `new`, changing the payload by
    ///   `new.len() − old.len()`, and ends the sequence — the in-sequence
    ///   equivalent of [`splice_into`](Self::splice_into).
    /// - `Some(BStackGenOp::Sparse { writes, length })` sparsely grows the payload
    ///   by `length` bytes, scattering the `(relative_offset, data)` `writes` into
    ///   the new region and leaving the gaps zero, and ends the sequence — the
    ///   in-sequence equivalent of
    ///   [`extend_sparse_batched`](Self::extend_sparse_batched).
    /// - `Some(BStackGenOp::Len { out })` writes the current logical payload
    ///   size into `out` and calls `f` again — the in-sequence equivalent of
    ///   [`len`](Self::len), useful when a later step's offset depends on the
    ///   payload size (e.g. "read the size, then read the last element").
    /// - `Some(BStackGenOp::Abort { source })` ends the sequence without writing
    ///   anything, returning `Err(e)` for `Some(e)` and `Ok(())` for `None` —
    ///   `None` with an optional error attached.
    /// - `None` ends the sequence without writing anything — useful when the
    ///   reads alone inform a decision, including the decision to change
    ///   nothing.
    ///
    /// `Write`, `Swap`, `Push`, `Pop`, `Discard`, `Atrunc`, `Splice`, and
    /// `Sparse` are the only mutating operations, exactly one is permitted per
    /// call, and any one of them ends the sequence immediately — `f` is not called
    /// again afterwards.
    ///
    /// Holding the write lock across every read and the final mutation means
    /// no other thread can observe or modify any region of the file in
    /// between — the guarantee that [`get_batched_gen`](Self::get_batched_gen)
    /// followed by a separate [`cas`](Self::cas) cannot provide, since the two
    /// separate lock acquisitions leave an ABA window.  The mutated region(s)
    /// need not overlap any region that was read.  `Push`, `Pop`, `Discard`,
    /// `Atrunc`, `Splice`, and `Sparse` are the steps that change the file size.
    ///
    /// Reads of the locked region `[0, locked_len())` are permitted, matching
    /// [`get`](Self::get) — locked bytes are immutable, so observing them
    /// mid-sequence is always safe.  `Write` and `Swap` ranges that touch the
    /// locked region are rejected, matching [`set`](Self::set) and
    /// [`cross_exchange`](Self::cross_exchange); an `Atrunc` or `Splice` whose
    /// cut point falls inside the locked region is likewise rejected, matching
    /// [`atrunc`](Self::atrunc) and [`splice_into`](Self::splice_into).
    ///
    /// # Feature flags
    ///
    /// Only available when both the `set` and `atomic` Cargo features are
    /// enabled.
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::InvalidInput`] if any `offset + len` overflows
    /// `u64`, if a read, write, or swap range exceeds the current payload
    /// size, if the two `Swap` regions overlap, if a write or swap range
    /// overlaps the locked region `[0, locked_len())`, if a `Pop`, `Discard`,
    /// `Atrunc`, or `Splice` removes more bytes than the current payload size,
    /// or if it would shrink the payload below (or cut into) the locked length.
    /// Propagates any I/O error from `read_exact`, `write_all`, `set_len`, or
    /// `durable_sync`.
    #[cfg(all(feature = "set", feature = "atomic"))]
    pub fn process_gen<'a, F>(&self, mut f: F) -> io::Result<()>
    where
        F: FnMut() -> Option<BStackGenOp<'a>>,
    {
        let mut guard = self.write_lock()?;
        let (file, clen, replay) = &mut *guard;
        let data_size = file.seek(SeekFrom::End(0))?.saturating_sub(HEADER_SIZE);
        let locked = self.locked.load(Ordering::Acquire);
        fault_point!(self, "process_gen");
        loop {
            match f() {
                Some(BStackGenOp::Read { offset, buf }) => {
                    let end = checked_end(
                        offset,
                        buf.len() as u64,
                        "process_gen: read offset + buf.len() overflows u64",
                    )?;
                    if end > data_size {
                        return Err(io_error!(
                            InvalidInput,
                            format!(
                                "process_gen: read range [{offset}, {end}) exceeds payload size ({data_size})"
                            )
                        ));
                    }
                    // Per-step read fault: stands in for this `Read`'s I/O, and
                    // like a genuine read failure here it ends the whole call —
                    // `process_gen` has no channel to report a step failure on.
                    // Consulted for every `Read` op, including ones the fast
                    // paths below serve without touching the disk, so the
                    // schedule does not shift with cache state.
                    fault_point!(self, "process_gen:read");
                    // Fast path: locked bytes are immutable, so they can be
                    // served from the cache or via a lock-free pread instead
                    // of going through the held file handle — mirroring how
                    // `get_into` treats reads of the locked region.
                    #[cfg(any(unix, windows))]
                    {
                        if end <= locked {
                            if self.cache_enabled {
                                let cache = self.cache.lock().unwrap();
                                buf.copy_from_slice(&cache[offset as usize..end as usize]);
                            } else {
                                #[cfg(unix)]
                                pread_exact_raw(self.fd, HEADER_SIZE + offset, buf)?;
                                #[cfg(windows)]
                                pread_exact_raw_handle(self.handle, HEADER_SIZE + offset, buf)?;
                            }
                        } else {
                            pread_exact_into(file, HEADER_SIZE + offset, buf)?;
                        }
                    }
                    #[cfg(not(any(unix, windows)))]
                    {
                        if end <= locked && self.cache_enabled {
                            let cache = self.cache.lock().unwrap();
                            buf.copy_from_slice(&cache[offset as usize..end as usize]);
                        } else {
                            read_at(file, offset, buf)?;
                        }
                    }
                }
                Some(BStackGenOp::Write { offset, data }) => {
                    let end = checked_end(
                        offset,
                        data.len() as u64,
                        "process_gen: write offset + data.len() overflows u64",
                    )?;
                    if offset < locked {
                        return Err(io_error!(
                            InvalidInput,
                            format!(
                                "process_gen: write range [{offset}, {end}) overlaps locked region [0, {locked})"
                            )
                        ));
                    }
                    if end > data_size {
                        return Err(io_error!(
                            InvalidInput,
                            format!(
                                "process_gen: write range [{offset}, {end}) exceeds payload size ({data_size})"
                            )
                        ));
                    }
                    if !data.is_empty() {
                        Self::mark_replay(replay, set_in_place(file, data_size, offset, data))?;
                    }
                    return Ok(());
                }
                Some(BStackGenOp::Swap {
                    a_offset,
                    b_offset,
                    len,
                }) => {
                    let a_end =
                        checked_end(a_offset, len, "process_gen: a_offset + len overflows u64")?;
                    let b_end =
                        checked_end(b_offset, len, "process_gen: b_offset + len overflows u64")?;
                    if len > 0 {
                        let (lo, hi) = if a_offset < b_offset {
                            (a_offset, b_offset)
                        } else {
                            (b_offset, a_offset)
                        };
                        if lo + len > hi {
                            return Err(io_error!(
                                InvalidInput,
                                format!(
                                    "process_gen: swap regions [{a_offset}, {a_end}) and [{b_offset}, {b_end}) overlap"
                                )
                            ));
                        }
                    }
                    if a_offset < locked {
                        return Err(io_error!(
                            InvalidInput,
                            format!(
                                "process_gen: swap region [{a_offset}, {a_end}) overlaps locked region [0, {locked})"
                            )
                        ));
                    }
                    if b_offset < locked {
                        return Err(io_error!(
                            InvalidInput,
                            format!(
                                "process_gen: swap region [{b_offset}, {b_end}) overlaps locked region [0, {locked})"
                            )
                        ));
                    }
                    if a_end > data_size {
                        return Err(io_error!(
                            InvalidInput,
                            format!(
                                "process_gen: swap region [{a_offset}, {a_end}) exceeds payload size ({data_size})"
                            )
                        ));
                    }
                    if b_end > data_size {
                        return Err(io_error!(
                            InvalidInput,
                            format!(
                                "process_gen: swap region [{b_offset}, {b_end}) exceeds payload size ({data_size})"
                            )
                        ));
                    }
                    if len > 0 {
                        Self::mark_replay(
                            replay,
                            journaled_exchange(file, data_size, a_offset, b_offset, len),
                        )?;
                    }
                    return Ok(());
                }
                Some(BStackGenOp::Push { data }) => {
                    if !data.is_empty() {
                        let file_end = file.seek(SeekFrom::End(0))?;
                        let logical_offset = file_end - HEADER_SIZE;
                        if let Err(e) = file.write_all(data) {
                            // A failed rollback leaves a stale tail past the committed length:
                            // defer it to the next write's replay.
                            if file.set_len(file_end).is_err() {
                                *replay = true;
                            }
                            return Err(e);
                        }
                        let new_len = logical_offset + data.len() as u64;
                        Self::mark_replay(
                            replay,
                            commit_grow(file, clen, new_len, logical_offset, file_end),
                        )?;
                    }
                    return Ok(());
                }
                Some(BStackGenOp::Pop { buf }) => {
                    let n = buf.len() as u64;
                    if n > data_size {
                        return Err(io_error!(
                            InvalidInput,
                            format!("process_gen: pop({n}) exceeds payload size ({data_size})")
                        ));
                    }
                    let new_data_len = data_size - n;
                    if new_data_len < locked {
                        return Err(io_error!(
                            InvalidInput,
                            format!(
                                "process_gen: pop({n}) would shrink payload below locked length ({locked})"
                            )
                        ));
                    }
                    if n > 0 {
                        read_at(file, new_data_len, buf)?;
                        Self::mark_replay(replay, commit_shrink(file, clen, new_data_len))?;
                    }
                    return Ok(());
                }
                Some(BStackGenOp::Discard { len }) => {
                    if len > data_size {
                        return Err(io_error!(
                            InvalidInput,
                            format!(
                                "process_gen: discard({len}) exceeds payload size ({data_size})"
                            )
                        ));
                    }
                    let new_data_len = data_size - len;
                    if new_data_len < locked {
                        return Err(io_error!(
                            InvalidInput,
                            format!(
                                "process_gen: discard({len}) would shrink payload below locked length ({locked})"
                            )
                        ));
                    }
                    if len > 0 {
                        Self::mark_replay(replay, commit_shrink(file, clen, new_data_len))?;
                    }
                    return Ok(());
                }
                Some(BStackGenOp::Atrunc { n, data }) => {
                    if n > data_size {
                        return Err(io_error!(
                            InvalidInput,
                            format!(
                                "process_gen: atrunc n ({n}) exceeds payload size ({data_size})"
                            )
                        ));
                    }
                    let new_tail_start = data_size - n;
                    if new_tail_start < locked {
                        return Err(io_error!(
                            InvalidInput,
                            format!("process_gen: atrunc would modify locked region [0, {locked})")
                        ));
                    }
                    if n != 0 || !data.is_empty() {
                        let file_end = HEADER_SIZE + data_size;
                        Self::mark_replay(
                            replay,
                            commit_tail_replace(file, clen, new_tail_start, n, data, file_end),
                        )?;
                    }
                    return Ok(());
                }
                Some(BStackGenOp::Splice { old, new }) => {
                    let n = old.len() as u64;
                    if n > data_size {
                        return Err(io_error!(
                            InvalidInput,
                            format!(
                                "process_gen: splice n ({n}) exceeds payload size ({data_size})"
                            )
                        ));
                    }
                    let new_tail_start = data_size - n;
                    if new_tail_start < locked {
                        return Err(io_error!(
                            InvalidInput,
                            format!("process_gen: splice would modify locked region [0, {locked})")
                        ));
                    }
                    if n != 0 || !new.is_empty() {
                        // Read the removed bytes before any mutation.
                        read_at(file, new_tail_start, old)?;
                        let file_end = HEADER_SIZE + data_size;
                        Self::mark_replay(
                            replay,
                            commit_tail_replace(file, clen, new_tail_start, n, new, file_end),
                        )?;
                    }
                    return Ok(());
                }
                Some(BStackGenOp::Sparse { writes, length }) => {
                    // Copy the borrowed blocks into a local list so they can be
                    // filtered and sorted for validation (the source slice is `&'a`).
                    let mut blocks: Vec<(u64, &[u8])> = writes
                        .iter()
                        .map(|(off, d)| (*off, *d))
                        .filter(|(_, d)| !d.is_empty())
                        .collect();
                    validate_sparse_blocks(&mut blocks, length, "process_gen: sparse")?;
                    if length != 0 {
                        let file_end = HEADER_SIZE + data_size;
                        let new_len = checked_end(
                            data_size,
                            length,
                            "process_gen: sparse data_size + length overflows u64",
                        )?;
                        Self::mark_replay(
                            replay,
                            commit_sparse_extend(file, clen, data_size, file_end, new_len, &blocks),
                        )?;
                    }
                    return Ok(());
                }
                Some(BStackGenOp::Len { out }) => {
                    *out = data_size;
                }
                Some(BStackGenOp::Abort { source }) => {
                    // Nothing has been mutated: every mutating op ends the
                    // sequence, so reaching here means only reads have run.
                    return source.map_or(Ok(()), Err);
                }
                None => return Ok(()),
            }
        }
    }

    /// Crash-atomically commit several non-overlapping in-place writes as a
    /// single unit.
    ///
    /// Takes any iterator of `(offset, data)` pairs and overwrites each
    /// `[offset, offset + data.len())` with `data`, all committing together:
    /// after a crash either every write is applied or none is, never a partial
    /// subset. Empty `data` slices are ignored. The file size is never changed —
    /// every write is an in-place overwrite of committed bytes.
    ///
    /// The writes must be **pairwise non-overlapping**; an overlapping pair is
    /// rejected as invalid input. (The generator form,
    /// [`inplace_gen`](Self::inplace_gen), instead resolves overlap in favour of
    /// the later write.)
    ///
    /// # Feature flags
    ///
    /// Only available when both the `set` and `atomic` Cargo features are
    /// enabled.
    ///
    /// # Durability & atomicity
    ///
    /// Crash-atomic across the whole batch via the multi-write journal (stage all
    /// blocks → arm → replay → disarm), which recovery replays or rolls back on
    /// the next [`open`](Self::open). A batch that reduces to a single non-empty
    /// write takes the ordinary single-write path (a single-block atomic write or
    /// the write-in-progress journal). The written bytes are durably synced
    /// before the call returns.
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::InvalidInput`] if any `offset + data.len()`
    /// overflows `u64` or exceeds the current payload size, if any write overlaps
    /// the locked region `[0, locked_len())`, or if two writes overlap.
    /// Propagates any I/O error from `write_all`, `set_len`, or `durable_sync`.
    #[cfg(all(feature = "set", feature = "atomic"))]
    pub fn set_batched<I, D>(&self, writes: I) -> io::Result<()>
    where
        I: IntoIterator<Item = (u64, D)>,
        D: AsRef<[u8]>,
    {
        // Materialise the inputs so their `AsRef` slices can be borrowed while we
        // validate, sort, and stage; drop empty writes (they touch nothing).
        let owned: Vec<(u64, D)> = writes.into_iter().collect();
        let mut blocks: Vec<(u64, &[u8])> = owned
            .iter()
            .map(|(off, d)| (*off, d.as_ref()))
            .filter(|(_, d)| !d.is_empty())
            .collect();
        if blocks.is_empty() {
            return Ok(());
        }
        let mut guard = self.write_lock()?;
        let (file, _, replay) = &mut *guard;
        // Load `locked` under the write lock (see `set` for rationale).
        let locked = self.locked.load(Ordering::Acquire);
        let data_size = file.seek(SeekFrom::End(0))?.saturating_sub(HEADER_SIZE);
        // Validate each block against the payload size and the locked prefix.
        for (off, data) in &blocks {
            let end = checked_end(
                *off,
                data.len() as u64,
                "set_batched: offset + len overflows u64",
            )?;
            if *off < locked {
                return Err(io_error!(
                    InvalidInput,
                    format!(
                        "set_batched: write range [{off}, {end}) overlaps locked region [0, {locked})"
                    )
                ));
            }
            if end > data_size {
                return Err(io_error!(
                    InvalidInput,
                    format!(
                        "set_batched: write range [{off}, {end}) exceeds payload size ({data_size})"
                    )
                ));
            }
        }
        fault_point!(self, "set_batched");
        // A lone write cannot overlap anything and is already atomic on its own,
        // so skip the overlap scan and the multi-write journal.
        if blocks.len() == 1 {
            let (off, data) = blocks[0];
            return Self::mark_replay(replay, set_in_place(file, data_size, off, data));
        }
        // Reject overlap: sort by offset, then check that each block ends at or
        // before the next one begins.
        blocks.sort_by_key(|(off, _)| *off);
        for pair in blocks.windows(2) {
            let (a_off, a_data) = pair[0];
            let (b_off, _) = pair[1];
            let a_end = a_off + a_data.len() as u64;
            if a_end > b_off {
                return Err(io_error!(
                    InvalidInput,
                    format!("set_batched: write range [{a_off}, {a_end}) overlaps [{b_off}, ...)")
                ));
            }
        }
        Self::mark_replay(replay, journaled_multi_set(file, data_size, &blocks))
    }

    /// Run a sequence of dependent reads interleaved with multiple in-place
    /// writes, committing every write as one crash-atomic unit when the sequence
    /// ends.
    ///
    /// Like [`process_gen`](Self::process_gen), `f` is called in a loop and drives
    /// the sequence through [`BStackGenOp`], all under one held write lock — but
    /// with three differences:
    ///
    /// - **Writes accumulate; they do not end the sequence.** Each
    ///   `Some(BStackGenOp::Write { offset, data })` records a pending in-place
    ///   write and `f` is called again. Every recorded write commits together
    ///   when the sequence ends (`None`), via the multi-write journal: after a
    ///   crash either all of them are applied or none is.
    /// - **Later writes override earlier overlapping ones.** Overlap is not
    ///   rejected (unlike [`set_batched`](Self::set_batched)); the later write
    ///   wins on the overlapping bytes. For `a<b<c<d`, writing `a..c` then `b..d`
    ///   commits `a..b` from the first write and `b..d` from the second.
    /// - **Reads see the batch-so-far content.** A `Some(BStackGenOp::Read {
    ///   offset, buf })` returns the payload as it *would* look with every pending
    ///   write applied — committed bytes overlaid with the edits recorded so far —
    ///   not the on-disk committed bytes.
    /// - **A `Read` can fail**, and does not end the call: its error is reported
    ///   through the next `f(feedback)` like any other op, and `buf` is left
    ///   holding whatever it held before. Check `feedback` before trusting it.
    ///
    /// `f` receives the [`io::Result`] of the **previous** op (the first call
    /// receives `Ok(())`): the outcome of a `Read`, or the validation result of a
    /// `Write`. An erroring op is simply not recorded — `f` can inspect the error
    /// and choose to continue, issue a different op, or end the sequence, rather
    /// than the whole batch being torn down. `Some(BStackGenOp::Len { out })`
    /// writes the current payload size into `out` (it never changes here) and
    /// continues. `None` ends the sequence and commits the accumulated writes.
    ///
    /// `Some(BStackGenOp::Abort { source })` ends it the other way: the accumulated
    /// writes are **discarded** — the only way to unwind a batch — and the call
    /// returns `Err(e)` for `Some(e)` or `Ok(())` for `None`.
    ///
    /// Only in-place operations are permitted: `Read`, `Write`, `Len`, and
    /// `Abort`. The
    /// size-changing ops (`Push`, `Pop`, `Discard`, `Atrunc`, `Splice`, `Sparse`)
    /// and `Swap` are rejected with [`io::ErrorKind::InvalidInput`] reported to `f`
    /// (they are not recorded and do not end the sequence) — the multi-write
    /// journal pins `clen` and `file_size` as the staging bounds, so no
    /// size-changing operation may be compounded with it.
    ///
    /// Every slice returned by `f` — both `Write` data and any sub-slice it is
    /// derived from — must outlive the call: pending `Write` data is borrowed
    /// (`&'a [u8]`) until the final commit and consulted by later `Read`s, so a
    /// buffer handed to a `Write` must not be reused or mutated until
    /// `inplace_gen` returns.
    ///
    /// Reads of the locked region `[0, locked_len())` are permitted (those bytes
    /// are immutable); `Write` ranges that touch it are rejected, matching
    /// [`set`](Self::set).
    ///
    /// # Feature flags
    ///
    /// Only available when both the `set` and `atomic` Cargo features are
    /// enabled.
    ///
    /// # Errors
    ///
    /// Per-op validation failures (overflow, out-of-range, locked-region or
    /// disallowed-op errors) are reported to `f` as the next call's argument, not
    /// returned; a generator that wants one to fail the whole call must respond
    /// with `Abort { source: Some(_) }`. Otherwise the call returns an error only if a `Read`'s I/O fails,
    /// or if staging, replaying, or disarming the final commit fails — propagating
    /// any I/O error from `read_exact`, `write_all`, `set_len`, or `durable_sync`.
    #[cfg(all(feature = "set", feature = "atomic"))]
    pub fn inplace_gen<'a, F>(&self, mut f: F) -> io::Result<()>
    where
        F: FnMut(io::Result<()>) -> Option<BStackGenOp<'a>>,
    {
        let mut guard = self.write_lock()?;
        let (file, _, replay) = &mut *guard;
        let data_size = file.seek(SeekFrom::End(0))?.saturating_sub(HEADER_SIZE);
        let locked = self.locked.load(Ordering::Acquire);
        fault_point!(self, "inplace_gen");
        // Sorted, pairwise-non-overlapping set of pending in-place edits, each
        // borrowing the caller's `Write` data for the lifetime of the call.
        let mut overlay: Vec<(u64, &'a [u8])> = Vec::new();
        let mut feedback: io::Result<()> = Ok(());
        loop {
            match f(feedback) {
                Some(BStackGenOp::Read { offset, buf }) => {
                    // Validate first so a bad range still beats an injected
                    // fault, then let the policy stand in for the read itself.
                    // Unlike `process_gen`, a failed `Read` here does not end
                    // the call: it is reported to the generator through its
                    // `feedback` argument, so an injected fault must take the
                    // same route as a genuine one.
                    feedback = match inplace_validate_read(offset, buf.len() as u64, data_size) {
                        Err(e) => Err(e),
                        Ok(()) => match fault_probe!(self, "inplace_gen:read") {
                            Some(e) => Err(e),
                            None => inplace_overlay_read(file, data_size, offset, buf, &overlay),
                        },
                    };
                }
                Some(BStackGenOp::Write { offset, data }) => {
                    feedback = inplace_validate_write(offset, data, data_size, locked);
                    if feedback.is_ok() && !data.is_empty() {
                        inplace_overlay_insert(&mut overlay, offset, data);
                    }
                }
                Some(BStackGenOp::Len { out }) => {
                    *out = data_size;
                    feedback = Ok(());
                }
                Some(BStackGenOp::Abort { source }) => {
                    // Drop the overlay without committing: the pending writes
                    // only ever existed in memory, so the file is untouched.
                    return source.map_or(Ok(()), Err);
                }
                Some(BStackGenOp::Swap { .. }) => {
                    feedback = Err(io_error!(
                        InvalidInput,
                        "inplace_gen: Swap is not permitted (Read/Write/Len only)"
                    ));
                }
                Some(BStackGenOp::Push { .. }) => {
                    feedback = Err(io_error!(
                        InvalidInput,
                        "inplace_gen: Push is not permitted (in-place writes only)"
                    ));
                }
                Some(BStackGenOp::Pop { .. }) => {
                    feedback = Err(io_error!(
                        InvalidInput,
                        "inplace_gen: Pop is not permitted (in-place writes only)"
                    ));
                }
                Some(BStackGenOp::Discard { .. }) => {
                    feedback = Err(io_error!(
                        InvalidInput,
                        "inplace_gen: Discard is not permitted (in-place writes only)"
                    ));
                }
                Some(BStackGenOp::Atrunc { .. }) => {
                    feedback = Err(io_error!(
                        InvalidInput,
                        "inplace_gen: Atrunc is not permitted (in-place writes only)"
                    ));
                }
                Some(BStackGenOp::Splice { .. }) => {
                    feedback = Err(io_error!(
                        InvalidInput,
                        "inplace_gen: Splice is not permitted (in-place writes only)"
                    ));
                }
                Some(BStackGenOp::Sparse { .. }) => {
                    feedback = Err(io_error!(
                        InvalidInput,
                        "inplace_gen: Sparse is not permitted (in-place writes only)"
                    ));
                }
                None => break,
            }
        }
        // Commit the accumulated edits. Zero → nothing to do; one → the ordinary
        // single-write path; many → the multi-write journal.
        match overlay.len() {
            0 => Ok(()),
            1 => {
                let (offset, data) = overlay[0];
                Self::mark_replay(replay, set_in_place(file, data_size, offset, data))
            }
            _ => Self::mark_replay(replay, journaled_multi_set(file, data_size, &overlay)),
        }
    }

    /// Cross-Region Dependent Swap — equal condition.
    ///
    /// Reads `a_expected.len()` bytes from logical offset `a_offset` and
    /// compares them to `a_expected`.  If they are **equal**, atomically swaps
    /// region B: reads `b_buf.len()` bytes from `b_offset`, writes the current
    /// contents of `b_buf` there, and returns the old region-B bytes as
    /// `Ok(Some(Vec<u8>))`.  If the comparison fails, returns `Ok(None)`
    /// without modifying the file.
    ///
    /// The read of region A, the comparison, the read of region B, and the
    /// write to region B all happen under the same write lock, so no other
    /// thread can observe an intermediate state.  The file size is never
    /// changed.
    ///
    /// An empty `a_expected` trivially compares equal (zero bytes match zero
    /// bytes).  An empty `b_buf` skips the B swap and returns
    /// `Ok(Some(Vec::new()))` when the condition passes.
    ///
    /// # Feature flags
    ///
    /// Only available when both the `set` and `atomic` Cargo features are
    /// enabled.
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::InvalidInput`] if either `a_offset + a_len` or
    /// `b_offset + b_len` overflows `u64`, exceeds the current payload size,
    /// or if region B overlaps the locked prefix.  Propagates any I/O error
    /// from `read_exact`, `write_all`, or `durable_sync`.
    #[cfg(all(feature = "set", feature = "atomic"))]
    pub fn eq_crds(
        &self,
        a_offset: u64,
        a_expected: impl AsRef<[u8]>,
        b_offset: u64,
        b_buf: impl AsRef<[u8]>,
    ) -> io::Result<Option<Vec<u8>>> {
        let a_expected = a_expected.as_ref();
        let b_buf = b_buf.as_ref();
        let a_len = a_expected.len() as u64;
        let b_len = b_buf.len() as u64;
        let a_end = checked_end(a_offset, a_len, "eq_crds: a_offset + a_len overflows u64")?;
        let b_end = checked_end(b_offset, b_len, "eq_crds: b_offset + b_len overflows u64")?;
        let mut guard = self.write_lock()?;
        let (file, _, replay) = &mut *guard;
        let locked = self.locked.load(Ordering::Acquire);
        if !b_buf.is_empty() && b_offset < locked {
            return Err(io_error!(
                InvalidInput,
                format!(
                    "eq_crds: B range [{b_offset}, {b_end}) overlaps locked region [0, {locked})"
                )
            ));
        }
        let data_size = file.seek(SeekFrom::End(0))?.saturating_sub(HEADER_SIZE);
        if !a_expected.is_empty() && a_end > data_size {
            return Err(io_error!(
                InvalidInput,
                format!(
                    "eq_crds: A range [{a_offset}, {a_end}) exceeds payload size ({data_size})"
                )
            ));
        }
        if !b_buf.is_empty() && b_end > data_size {
            return Err(io_error!(
                InvalidInput,
                format!(
                    "eq_crds: B range [{b_offset}, {b_end}) exceeds payload size ({data_size})"
                )
            ));
        }
        fault_point!(self, "eq_crds");
        let mut a_current = vec![0u8; a_expected.len()];
        if !a_expected.is_empty() {
            read_at(file, a_offset, &mut a_current)?;
        }
        if a_current != a_expected {
            return Ok(None);
        }
        if b_buf.is_empty() {
            return Ok(Some(Vec::new()));
        }
        let mut old_b = vec![0u8; b_buf.len()];
        read_at(file, b_offset, &mut old_b)?;
        Self::mark_replay(replay, set_in_place(file, data_size, b_offset, b_buf))?;
        Ok(Some(old_b))
    }

    /// Cross-Region Dependent Swap — not-equal condition.
    ///
    /// Like [`eq_crds`](Self::eq_crds) but performs the region-B swap only
    /// when the `a_expected.len()` bytes at `a_offset` are **not equal** to
    /// `a_expected`.  If the bytes are not equal, atomically swaps region B:
    /// reads `b_buf.len()` bytes from `b_offset`, writes the contents of
    /// `b_buf` there, and returns the old region-B bytes as
    /// `Ok(Some(Vec<u8>))`.  Returns `Ok(None)` if the bytes compare equal
    /// (swap suppressed).
    ///
    /// # Feature flags
    ///
    /// Only available when both the `set` and `atomic` Cargo features are
    /// enabled.
    ///
    /// # Errors
    ///
    /// Same conditions as [`eq_crds`](Self::eq_crds).
    #[cfg(all(feature = "set", feature = "atomic"))]
    pub fn ne_crds(
        &self,
        a_offset: u64,
        a_expected: impl AsRef<[u8]>,
        b_offset: u64,
        b_buf: impl AsRef<[u8]>,
    ) -> io::Result<Option<Vec<u8>>> {
        let a_expected = a_expected.as_ref();
        let b_buf = b_buf.as_ref();
        let a_len = a_expected.len() as u64;
        let b_len = b_buf.len() as u64;
        let a_end = checked_end(a_offset, a_len, "ne_crds: a_offset + a_len overflows u64")?;
        let b_end = checked_end(b_offset, b_len, "ne_crds: b_offset + b_len overflows u64")?;
        let mut guard = self.write_lock()?;
        let (file, _, replay) = &mut *guard;
        let locked = self.locked.load(Ordering::Acquire);
        if !b_buf.is_empty() && b_offset < locked {
            return Err(io_error!(
                InvalidInput,
                format!(
                    "ne_crds: B range [{b_offset}, {b_end}) overlaps locked region [0, {locked})"
                )
            ));
        }
        let data_size = file.seek(SeekFrom::End(0))?.saturating_sub(HEADER_SIZE);
        if !a_expected.is_empty() && a_end > data_size {
            return Err(io_error!(
                InvalidInput,
                format!(
                    "ne_crds: A range [{a_offset}, {a_end}) exceeds payload size ({data_size})"
                )
            ));
        }
        if !b_buf.is_empty() && b_end > data_size {
            return Err(io_error!(
                InvalidInput,
                format!(
                    "ne_crds: B range [{b_offset}, {b_end}) exceeds payload size ({data_size})"
                )
            ));
        }
        fault_point!(self, "ne_crds");
        let mut a_current = vec![0u8; a_expected.len()];
        if !a_expected.is_empty() {
            read_at(file, a_offset, &mut a_current)?;
        }
        if a_current == a_expected {
            return Ok(None);
        }
        if b_buf.is_empty() {
            return Ok(Some(Vec::new()));
        }
        let mut old_b = vec![0u8; b_buf.len()];
        read_at(file, b_offset, &mut old_b)?;
        Self::mark_replay(replay, set_in_place(file, data_size, b_offset, b_buf))?;
        Ok(Some(old_b))
    }

    /// Cross-Region Dependent Swap — masked-equal condition.
    ///
    /// Like [`eq_crds`](Self::eq_crds) but the comparison applies a bitwise
    /// AND mask before comparing: for each byte `i`, the condition is
    /// `(A[i] & mask[i]) == (a_expected[i] & mask[i])`.  `mask` and
    /// `a_expected` must have the same length, which determines how many
    /// bytes are read from region A.  If the masked condition holds,
    /// atomically swaps region B: reads `b_buf.len()` bytes from `b_offset`,
    /// writes the contents of `b_buf` there, and returns the old region-B
    /// bytes as `Ok(Some(Vec<u8>))`.  Returns `Ok(None)` if the masked
    /// condition does not hold.
    ///
    /// # Feature flags
    ///
    /// Only available when both the `set` and `atomic` Cargo features are
    /// enabled.
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::InvalidInput`] if `mask.len() != a_expected.len()`.
    /// Same additional conditions as [`eq_crds`](Self::eq_crds).
    #[cfg(all(feature = "set", feature = "atomic"))]
    pub fn masked_eq_crds(
        &self,
        a_offset: u64,
        mask: impl AsRef<[u8]>,
        a_expected: impl AsRef<[u8]>,
        b_offset: u64,
        b_buf: impl AsRef<[u8]>,
    ) -> io::Result<Option<Vec<u8>>> {
        let mask = mask.as_ref();
        let a_expected = a_expected.as_ref();
        let b_buf = b_buf.as_ref();
        if mask.len() != a_expected.len() {
            return Err(io_error!(
                InvalidInput,
                "masked_eq_crds: mask length ({}) != a_expected length ({})",
                mask.len(),
                a_expected.len()
            ));
        }
        let a_len = a_expected.len() as u64;
        let b_len = b_buf.len() as u64;
        let a_end = checked_end(
            a_offset,
            a_len,
            "masked_eq_crds: a_offset + a_len overflows u64",
        )?;
        let b_end = checked_end(
            b_offset,
            b_len,
            "masked_eq_crds: b_offset + b_len overflows u64",
        )?;
        let mut guard = self.write_lock()?;
        let (file, _, replay) = &mut *guard;
        let locked = self.locked.load(Ordering::Acquire);
        if !b_buf.is_empty() && b_offset < locked {
            return Err(io_error!(
                InvalidInput,
                format!(
                    "masked_eq_crds: B range [{b_offset}, {b_end}) overlaps locked region [0, {locked})"
                )
            ));
        }
        let data_size = file.seek(SeekFrom::End(0))?.saturating_sub(HEADER_SIZE);
        if !a_expected.is_empty() && a_end > data_size {
            return Err(io_error!(
                InvalidInput,
                format!(
                    "masked_eq_crds: A range [{a_offset}, {a_end}) exceeds payload size ({data_size})"
                )
            ));
        }
        if !b_buf.is_empty() && b_end > data_size {
            return Err(io_error!(
                InvalidInput,
                format!(
                    "masked_eq_crds: B range [{b_offset}, {b_end}) exceeds payload size ({data_size})"
                )
            ));
        }
        fault_point!(self, "masked_eq_crds");
        let mut a_current = vec![0u8; a_expected.len()];
        if !a_expected.is_empty() {
            read_at(file, a_offset, &mut a_current)?;
        }
        let masked_match = a_current
            .iter()
            .zip(mask.iter())
            .zip(a_expected.iter())
            .all(|((&a, &m), &e)| (a & m) == (e & m));
        if !masked_match {
            return Ok(None);
        }
        if b_buf.is_empty() {
            return Ok(Some(Vec::new()));
        }
        let mut old_b = vec![0u8; b_buf.len()];
        read_at(file, b_offset, &mut old_b)?;
        Self::mark_replay(replay, set_in_place(file, data_size, b_offset, b_buf))?;
        Ok(Some(old_b))
    }

    /// Cross-Region Dependent Swap — masked-not-equal condition.
    ///
    /// Like [`masked_eq_crds`](Self::masked_eq_crds) but performs the
    /// region-B swap only when **any** masked byte differs:
    /// `(A[i] & mask[i]) != (a_expected[i] & mask[i])` for at least one `i`.
    /// If any masked byte differs, atomically swaps region B: reads
    /// `b_buf.len()` bytes from `b_offset`, writes the contents of `b_buf`
    /// there, and returns the old region-B bytes as `Ok(Some(Vec<u8>))`.
    /// Returns `Ok(None)` if all masked bytes compare equal (swap suppressed).
    ///
    /// # Feature flags
    ///
    /// Only available when both the `set` and `atomic` Cargo features are
    /// enabled.
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::InvalidInput`] if `mask.len() != a_expected.len()`.
    /// Same additional conditions as [`eq_crds`](Self::eq_crds).
    #[cfg(all(feature = "set", feature = "atomic"))]
    pub fn masked_ne_crds(
        &self,
        a_offset: u64,
        mask: impl AsRef<[u8]>,
        a_expected: impl AsRef<[u8]>,
        b_offset: u64,
        b_buf: impl AsRef<[u8]>,
    ) -> io::Result<Option<Vec<u8>>> {
        let mask = mask.as_ref();
        let a_expected = a_expected.as_ref();
        let b_buf = b_buf.as_ref();
        if mask.len() != a_expected.len() {
            return Err(io_error!(
                InvalidInput,
                "masked_ne_crds: mask length ({}) != a_expected length ({})",
                mask.len(),
                a_expected.len()
            ));
        }
        let a_len = a_expected.len() as u64;
        let b_len = b_buf.len() as u64;
        let a_end = checked_end(
            a_offset,
            a_len,
            "masked_ne_crds: a_offset + a_len overflows u64",
        )?;
        let b_end = checked_end(
            b_offset,
            b_len,
            "masked_ne_crds: b_offset + b_len overflows u64",
        )?;
        let mut guard = self.write_lock()?;
        let (file, _, replay) = &mut *guard;
        let locked = self.locked.load(Ordering::Acquire);
        if !b_buf.is_empty() && b_offset < locked {
            return Err(io_error!(
                InvalidInput,
                format!(
                    "masked_ne_crds: B range [{b_offset}, {b_end}) overlaps locked region [0, {locked})"
                )
            ));
        }
        let data_size = file.seek(SeekFrom::End(0))?.saturating_sub(HEADER_SIZE);
        if !a_expected.is_empty() && a_end > data_size {
            return Err(io_error!(
                InvalidInput,
                format!(
                    "masked_ne_crds: A range [{a_offset}, {a_end}) exceeds payload size ({data_size})"
                )
            ));
        }
        if !b_buf.is_empty() && b_end > data_size {
            return Err(io_error!(
                InvalidInput,
                format!(
                    "masked_ne_crds: B range [{b_offset}, {b_end}) exceeds payload size ({data_size})"
                )
            ));
        }
        fault_point!(self, "masked_ne_crds");
        let mut a_current = vec![0u8; a_expected.len()];
        if !a_expected.is_empty() {
            read_at(file, a_offset, &mut a_current)?;
        }
        let masked_match = a_current
            .iter()
            .zip(mask.iter())
            .zip(a_expected.iter())
            .all(|((&a, &m), &e)| (a & m) == (e & m));
        if masked_match {
            return Ok(None);
        }
        if b_buf.is_empty() {
            return Ok(Some(Vec::new()));
        }
        let mut old_b = vec![0u8; b_buf.len()];
        read_at(file, b_offset, &mut old_b)?;
        Self::mark_replay(replay, set_in_place(file, data_size, b_offset, b_buf))?;
        Ok(Some(old_b))
    }
}

// ---------------------------------------------------------------------------

impl BStack {
    /// Return the current **logical** payload size in bytes (excludes the
    /// 32-byte header).
    ///
    /// Reads the in-memory `clen` cache under the read lock, so it can run
    /// concurrently with other `len` calls but blocks while any write-lock
    /// operation is in progress. No syscall is made. The returned value
    /// always reflects a clean operation boundary.
    ///
    /// # Errors
    ///
    /// Fails with [`InterruptedWrite`] while an interrupted write is pending
    /// replay: the cached length is exactly what that replay may correct.
    /// Otherwise never actually fails
    /// outside of an armed fault policy under the `fault-injection` feature;
    /// returns [`io::Result`] for source compatibility.
    #[inline]
    pub fn len(&self) -> io::Result<u64> {
        fault_point!(self, "len");
        Ok(self.read_lock()?.1)
    }

    /// Return `true` if the stack contains no payload bytes.
    ///
    /// # Errors
    ///
    /// Fails while an interrupted write is pending replay, as [`len`](Self::len)
    /// does.  Otherwise never actually fails outside of an armed fault policy
    /// under the `fault-injection` feature; returns [`io::Result`] for source
    /// compatibility.
    #[inline]
    pub fn is_empty(&self) -> io::Result<bool> {
        fault_point!(self, "is_empty");
        Ok(self.read_lock()?.1 == 0)
    }

    /// Returns the current locked length.  `0` means no bytes are locked.
    ///
    /// The locked region is `[0, locked_len())`.  All bytes within this range
    /// are permanently immutable: writes and shrink operations that would
    /// touch them return [`io::ErrorKind::InvalidInput`]. For
    /// [`get`](Self::get) and [`get_into`](Self::get_into), reads to ranges
    /// entirely within it skip the rwlock.
    #[inline]
    #[must_use]
    pub fn locked_len(&self) -> u64 {
        self.locked.load(Ordering::Acquire)
    }

    /// Extend the locked region to cover `[0, n)`.
    ///
    /// `n` must be ≥ the current locked length and ≤ the current payload
    /// length. After this call, [`get`](Self::get) and
    /// [`get_into`](Self::get_into) reads to `[0, n)` skip the rwlock
    /// (lock-free on non-cached Unix/Windows stacks; cache-backed under a
    /// `Mutex` on cached stacks), and all write and shrink operations that
    /// would touch `[0, n)` return [`io::ErrorKind::InvalidInput`].
    ///
    /// Acquires the exclusive write lock to ensure all in-flight writes to
    /// `[0, n)` have completed before the region is declared immutable.
    ///
    /// # Performance
    ///
    /// On stacks opened with [`open_cached`](Self::open_cached) this call
    /// reads only the newly added portion of the locked region, that is,
    /// `n - current_locked_len` bytes, from disk into the in-memory cache
    /// before returning. In the worst case this is `n` bytes, but only when
    /// locking from `0`. This makes `lock_up_to` significantly more expensive
    /// on cached stacks than on non-cached ones.
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::InvalidInput`] if `n` is less than the current
    /// locked length (partition can only grow) or if `n` exceeds the current
    /// payload length.
    ///
    /// Fails with [`InterruptedWrite`] while an earlier write is pending replay.
    pub fn lock_up_to(&self, n: u64) -> io::Result<()> {
        // Acquire the write lock to serialise against any in-flight writers.
        #[allow(unused_mut)]
        // `mut` is not needed on Unix and Windows, but other platforms may need it for the file handle.
        let mut guard = self.write_lock_read()?;
        let file = &mut guard.0;
        let data_size = file.metadata()?.len().saturating_sub(HEADER_SIZE);
        let current_locked = self.locked.load(Ordering::Relaxed);
        if n < current_locked {
            return Err(io_error!(
                InvalidInput,
                format!(
                    "lock_up_to: n ({n}) is less than the current locked length ({current_locked})"
                )
            ));
        }
        if n > data_size {
            return Err(io_error!(
                InvalidInput,
                format!("lock_up_to: n ({n}) exceeds payload size ({data_size})")
            ));
        }
        fault_point!(self, "lock_up_to");
        // Populate or extend the in-memory cache before publishing the new
        // boundary.  `locked` is only advanced after the cache is consistent,
        // so readers always see a coherent view.
        if self.cache_enabled && n > current_locked {
            // On 32-bit targets usize < u64, so very large regions cannot be
            // cached.  Validate before casting to avoid silent truncation or a
            // panic inside next_power_of_two.
            if n > usize::MAX as u64 {
                return Err(io_error!(
                    OutOfMemory,
                    "lock_up_to: locked region too large to cache on this platform"
                ));
            }
            let ol = current_locked as usize; // safe: <= n, which was just validated
            let nl = n as usize; // safe: checked above
            // isize::MAX is the maximum valid allocation size; values above it
            // would also cause next_power_of_two to overflow.
            if nl > isize::MAX as usize {
                return Err(io_error!(
                    OutOfMemory,
                    "lock_up_to: locked region too large to cache on this platform"
                ));
            }
            let mut cache = self.cache.lock().unwrap();

            if nl > cache.capacity() {
                // Reallocating: build a fresh Vec with power-of-2 capacity,
                // copy the existing valid bytes, read the new portion from disk.
                // On read failure new_cache is dropped; self.locked is unchanged
                // and the old cache remains valid for [0..ol].
                let new_cap = nl.next_power_of_two();
                let mut new_cache = Vec::with_capacity(new_cap);
                new_cache.extend_from_slice(&cache[..ol]);
                new_cache.resize(nl, 0u8);
                #[cfg(unix)]
                pread_exact_raw(self.fd, HEADER_SIZE + ol as u64, &mut new_cache[ol..nl])?;
                #[cfg(windows)]
                pread_exact_raw_handle(
                    self.handle,
                    HEADER_SIZE + ol as u64,
                    &mut new_cache[ol..nl],
                )?;
                #[cfg(not(any(unix, windows)))]
                {
                    file.seek(SeekFrom::Start(HEADER_SIZE + ol as u64))?;
                    file.read_exact(&mut new_cache[ol..nl])?;
                }
                *cache = new_cache;
            } else {
                // Non-reallocating: extend the Vec in-place and read the new
                // portion.  On read failure, truncate back to the old length.
                cache.resize(nl, 0u8);
                #[cfg(unix)]
                if let Err(e) =
                    pread_exact_raw(self.fd, HEADER_SIZE + ol as u64, &mut cache[ol..nl])
                {
                    cache.truncate(ol);
                    return Err(e);
                }
                #[cfg(windows)]
                if let Err(e) =
                    pread_exact_raw_handle(self.handle, HEADER_SIZE + ol as u64, &mut cache[ol..nl])
                {
                    cache.truncate(ol);
                    return Err(e);
                }
                #[cfg(not(any(unix, windows)))]
                if let Err(e) = file
                    .seek(SeekFrom::Start(HEADER_SIZE + ol as u64))
                    .and_then(|_| file.read_exact(&mut cache[ol..nl]))
                {
                    cache.truncate(ol);
                    return Err(e);
                }
            }
        }

        // Release store: all writes completed under the write lock above are
        // visible to any thread that subsequently loads `locked` with Acquire.
        self.locked.store(n, Ordering::Release);
        drop(guard);
        Ok(())
    }

    /// Open a `BStack` and immediately lock the first `n` bytes.
    ///
    /// Equivalent to [`BStack::open`] followed by [`lock_up_to`](Self::lock_up_to),
    /// but expressed as a single call for the common pattern where the locked
    /// region is known ahead of time (e.g. a fixed-size metadata block whose
    /// size is a compile-time or configuration constant).
    ///
    /// # Errors
    ///
    /// Propagates all errors from [`open`](Self::open).  Returns
    /// [`io::ErrorKind::InvalidInput`] if `n` exceeds the payload length of
    /// the opened file.
    #[inline]
    pub fn open_locked_up_to(path: impl AsRef<Path>, n: u64) -> io::Result<Self> {
        let stack = Self::open(path)?;
        stack.lock_up_to(n)?;
        Ok(stack)
    }

    /// Open or create a stack file at `path` with the in-memory locked-region
    /// cache enabled.
    ///
    /// Behaves identically to [`open`](Self::open) in all other respects.
    /// Once the cache is enabled, each subsequent [`lock_up_to`](Self::lock_up_to)
    /// call reads the newly locked bytes from disk into a heap buffer so that
    /// future reads whose range falls entirely within the locked region are
    /// served by copying from that buffer with no syscall.
    ///
    /// # Errors
    ///
    /// Propagates all errors from [`open`](Self::open).
    #[inline]
    pub fn open_cached(path: impl AsRef<Path>) -> io::Result<Self> {
        let mut stack = Self::open(path)?;
        stack.cache_enabled = true;
        Ok(stack)
    }

    /// Open a cached `BStack` and immediately lock the first `n` bytes.
    ///
    /// Equivalent to [`open_cached`](Self::open_cached) followed by
    /// [`lock_up_to`](Self::lock_up_to), but expressed as a single call.
    ///
    /// # Errors
    ///
    /// Propagates all errors from [`open_cached`](Self::open_cached) and
    /// [`lock_up_to`](Self::lock_up_to).
    /// Returns [`io::ErrorKind::InvalidInput`] if `n` exceeds the payload
    /// length of the opened file.
    #[inline]
    pub fn open_locked_up_to_cached(path: impl AsRef<Path>, n: u64) -> io::Result<Self> {
        let stack = Self::open_cached(path)?;
        stack.lock_up_to(n)?;
        Ok(stack)
    }
}

/// Deterministic I/O-fault injection controls.
///
/// These methods exist only in builds with `debug_assertions` on and the
/// `fault-injection` feature enabled (see the [`fault`] module); a normal release
/// build exposes none of them and carries no fault-injection machinery.
#[cfg(all(debug_assertions, feature = "fault-injection"))]
impl BStack {
    /// Install a [`FaultPolicy`] on this stack at construction time, consuming and
    /// returning `self` so it can be chained onto a constructor:
    ///
    /// ```ignore
    /// let stack = BStack::open(path)?.with_fault_policy(Arc::new(my_policy));
    /// ```
    ///
    /// Equivalent to [`open`](Self::open) followed by
    /// [`set_fault_policy`](Self::set_fault_policy)`(Some(policy))`; the operation
    /// sequence counter starts at 0.
    #[inline]
    #[must_use]
    pub fn with_fault_policy(self, policy: std::sync::Arc<dyn fault::FaultPolicy>) -> Self {
        self.fault.set(Some(policy));
        self
    }

    /// Arm, re-arm, or (with `None`) disarm the fault policy on an already-open
    /// stack. Setting a policy resets the operation sequence counter to 0, so a
    /// seeded schedule replays identically each time it is armed. Because this
    /// takes `&self`, a test holding a shared reference can arm a fault, drive the
    /// operation under test, then disarm before reading results back.
    #[inline]
    pub fn set_fault_policy(&self, policy: Option<std::sync::Arc<dyn fault::FaultPolicy>>) {
        self.fault.set(policy);
    }

    /// Return the currently armed [`FaultPolicy`], or `None` if the stack is
    /// unarmed.
    #[inline]
    #[must_use]
    pub fn fault_policy(&self) -> Option<std::sync::Arc<dyn fault::FaultPolicy>> {
        self.fault.get()
    }
}

// ---------------------------------------------------------------------------
// io::Write

/// Appends bytes to the stack.
///
/// Each call to [`write`](io::Write::write) is equivalent to [`push`](BStack::push):
/// all bytes are written atomically and durably synced before returning.
/// Calling `write_all` or chaining multiple `write` calls therefore issues
/// one `durable_sync` per call — callers that need to batch many small writes
/// without per-write syncs should accumulate data and call `push` directly.
///
/// [`flush`](io::Write::flush) is a no-op because every `write` is already
/// durable.
impl io::Write for BStack {
    #[inline]
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.push(buf)?;
        Ok(buf.len())
    }

    #[inline]
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Shared-reference counterpart of `impl Write for BStack`.
///
/// Because [`push`](BStack::push) takes `&self` (interior mutability via
/// `RwLock`), the `Write` implementation is also available on `&BStack`,
/// mirroring the standard library's `impl Write for &File`.
impl io::Write for &BStack {
    #[inline]
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.push(buf)?;
        Ok(buf.len())
    }

    #[inline]
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl fmt::Debug for BStack {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BStack")
            .field(
                "version",
                &format!("{}.{}.{}", MAGIC[4], MAGIC[5], MAGIC[6]),
            )
            .field("len", &self.len().ok())
            .finish_non_exhaustive()
    }
}

impl Eq for BStack {}

/// Two `BStack` instances are equal iff they are the **same instance** in memory.
///
/// Because [`BStack::open`] acquires an exclusive advisory lock, no two
/// `BStack` values within one process can refer to the same file at the same
/// time.  Pointer identity is therefore the only meaningful equality: a stack
/// is equal to itself and to nothing else.
impl PartialEq for BStack {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        std::ptr::eq(self, other)
    }
}

/// Hashes the raw fd (Unix) / handle (Windows), consistent with the
/// pointer-identity [`PartialEq`]: unique per live instance, and unlike the
/// address it is stable across moves. On other platforms the instance address
/// is hashed, which is not move-stable.
impl Hash for BStack {
    #[inline]
    fn hash<H: Hasher>(&self, state: &mut H) {
        #[cfg(unix)]
        self.fd.hash(state);
        #[cfg(windows)]
        self.handle.hash(state);
        #[cfg(not(any(unix, windows)))]
        (self as *const BStack).hash(state);
    }
}

/// A cursor-based reader over a [`BStack`] payload.
///
/// `BStackReader` implements [`io::Read`] and [`io::Seek`], allowing the
/// stack's payload to be consumed through any interface that expects a
/// readable, seekable byte stream.
///
/// # Construction
///
/// ```no_run
/// use bstack::BStack;
///
/// # fn main() -> std::io::Result<()> {
/// let stack = BStack::open("log.bin")?;
/// stack.push(b"hello world")?;
///
/// // Start reading from the beginning.
/// let mut reader = stack.reader();
///
/// // Or start from an arbitrary offset.
/// let mut mid = stack.reader_at(6);
/// # Ok(())
/// # }
/// ```
///
/// # Concurrency
///
/// `BStackReader` borrows the stack immutably, so multiple readers can coexist
/// and run concurrently with each other and with [`peek`](BStack::peek) /
/// [`get`](BStack::get) calls.  Concurrent [`push`](BStack::push) or
/// [`pop`](BStack::pop) operations are not blocked by an active reader, but
/// reading interleaved with writes may observe different snapshots of the
/// payload across calls — callers are responsible for synchronisation when
/// that matters.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct BStackReader<'a> {
    stack: &'a BStack,
    offset: u64,
}

impl<'a> fmt::Debug for BStackReader<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BStackReader")
            .field("position", &self.offset)
            .field("len", &self.stack.len().ok())
            .finish_non_exhaustive()
    }
}

impl BStack {
    /// Create a [`BStackReader`] positioned at the start of the payload.
    #[inline]
    #[must_use]
    pub fn reader(&self) -> BStackReader<'_> {
        BStackReader {
            stack: self,
            offset: 0,
        }
    }

    /// Create a [`BStackReader`] positioned at `offset` bytes into the payload.
    ///
    /// Seeking past the current end is allowed; [`read`](io::Read::read) will
    /// return `Ok(0)` until new data is pushed past that point.
    #[inline]
    #[must_use]
    pub fn reader_at(&self, offset: u64) -> BStackReader<'_> {
        BStackReader {
            stack: self,
            offset,
        }
    }
}

impl<'a> BStackReader<'a> {
    /// Return the current logical read offset within the payload.
    #[inline]
    #[must_use]
    pub fn position(&self) -> u64 {
        self.offset
    }
}

impl<'a> From<&'a BStack> for BStackReader<'a> {
    #[inline]
    fn from(stack: &'a BStack) -> Self {
        stack.reader()
    }
}

impl<'a> From<BStackReader<'a>> for &'a BStack {
    #[inline]
    fn from(val: BStackReader<'a>) -> Self {
        val.stack
    }
}

impl<'a> PartialOrd for BStackReader<'a> {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// Ordered by `BStack` instance address, then by cursor `offset`.
///
/// The address component groups all readers over the same stack together,
/// and within that group the natural read order (smaller offset first) applies.
/// This ordering is consistent with the pointer-identity [`PartialEq`].
impl<'a> Ord for BStackReader<'a> {
    #[inline]
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let self_ptr = self.stack as *const BStack as usize;
        let other_ptr = other.stack as *const BStack as usize;
        self_ptr
            .cmp(&other_ptr)
            .then(self.offset.cmp(&other.offset))
    }
}

impl<'a> io::Read for BStackReader<'a> {
    /// Read bytes from the current position into `buf`.
    ///
    /// Returns the number of bytes read, which may be less than `buf.len()` if
    /// the end of the payload is reached.  Returns `Ok(0)` at EOF.
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        let data_size = self.stack.len()?;
        if self.offset >= data_size {
            return Ok(0);
        }
        let available = (data_size - self.offset) as usize;
        let n = buf.len().min(available);
        self.stack.get_into(self.offset, &mut buf[..n])?;
        self.offset += n as u64;
        Ok(n)
    }
}

impl<'a> io::Seek for BStackReader<'a> {
    /// Move the read cursor.
    ///
    /// [`SeekFrom::Start`] and [`SeekFrom::Current`] with a non-negative delta
    /// may advance the cursor past the current end of the payload; subsequent
    /// [`read`](io::Read::read) calls will return `Ok(0)` until the payload
    /// grows past that point.  Seeking before the start of the payload returns
    /// [`io::ErrorKind::InvalidInput`].
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        let data_size = self.stack.len()? as i128;
        let new_offset = match pos {
            SeekFrom::Start(n) => n as i128,
            SeekFrom::End(n) => data_size + n as i128,
            SeekFrom::Current(n) => self.offset as i128 + n as i128,
        };
        if new_offset < 0 {
            return Err(io_error!(InvalidInput, "seek before beginning of payload"));
        }
        self.offset = new_offset as u64;
        Ok(self.offset)
    }
}
