//! Core I/O primitives for BStack's crash-atomic file format.

use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom, Write};

#[cfg(unix)]
use std::os::unix::fs::FileExt;
#[cfg(unix)]
use std::os::unix::io::AsRawFd;
#[cfg(unix)]
use std::os::unix::io::RawFd;

#[cfg(windows)]
use std::os::windows::fs::FileExt as WindowsFileExt;
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

/// Bytes occupied by the file header
/// (magic[8] + committed_len[8] + wip_ptr[8] + wip_aux[8]).
///
/// `wip_ptr`/`wip_aux` hold the write-in-progress journal that makes in-place
/// mutations crash-atomic (see *Crash recovery* in the crate docs); both are
/// zero in the steady state.
const HEADER_SIZE: u64 = 32;

/// Conservative power-fail atomic block size, in bytes.
///
/// Real storage writes whole blocks atomically: a single write confined to one
/// block either lands in full or not at all across a power loss. Devices differ
/// in their true block size (commonly 512 B or 4 KB), but 256 B is a lower bound
/// that holds on virtually all hardware — including eMMC and older NVMe
/// controllers that advertise larger blocks yet only guarantee 512 B or 256 B
/// power-fail atomicity. Because 256 divides those sizes and the file's first
/// byte is block-aligned, a write confined to one 256 B-aligned region is always
/// contained within a single hardware block. See *Derived atomicity beyond 8 B*
/// in `PLANNED.md`.
pub(crate) const ATOMIC_BLOCK: u64 = 256;

/// Upper bound on the streaming buffer for on-disk moves and repeat-fills, in
/// bytes. Each iteration copies at most this many bytes, so a relocation or fill
/// of any length uses O(1) memory. Sized to one typical filesystem page. Not
/// feature-gated: recovery's repeat-fill replay ([`write_repeated`]) uses it.
const MOVE_CHUNK: u64 = 4 * 1024;

// ------------------------------------------- OS Primitives -------------------------------------------

/// Read `len` bytes from absolute file position `offset` without modifying
/// the file-position cursor, so the caller only needs a shared (read) lock.
///
/// On Unix this uses `pread(2)` via `read_exact_at`.
/// On Windows this uses `ReadFile` with an `OVERLAPPED` offset (via
/// `seek_read`), which is also cursor-safe on synchronous handles.
#[cfg(unix)]
pub(crate) fn pread_exact(file: &File, offset: u64, len: usize) -> io::Result<Vec<u8>> {
    let mut buf = vec![0u8; len];
    file.read_exact_at(&mut buf, offset)?;
    Ok(buf)
}

/// Windows counterpart of `pread_exact` — see the shared doc comment above.
#[cfg(windows)]
pub(crate) fn pread_exact(file: &File, offset: u64, len: usize) -> io::Result<Vec<u8>> {
    let mut buf = vec![0u8; len];
    let mut filled = 0usize;
    while filled < len {
        let n = file.seek_read(&mut buf[filled..], offset + filled as u64)?;
        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "pread_exact: unexpected EOF",
            ));
        }
        filled += n;
    }
    Ok(buf)
}

/// Fill `buf` from absolute file position `offset` without modifying the
/// file-position cursor.  Unix uses `pread(2)` via `read_exact_at`;
/// Windows uses `ReadFile` with an `OVERLAPPED` offset via `seek_read`.
#[cfg(unix)]
pub(crate) fn pread_exact_into(file: &File, offset: u64, buf: &mut [u8]) -> io::Result<()> {
    file.read_exact_at(buf, offset)
}

/// Windows counterpart of `pread_exact_into`.
#[cfg(windows)]
pub(crate) fn pread_exact_into(file: &File, offset: u64, buf: &mut [u8]) -> io::Result<()> {
    let len = buf.len();
    let mut filled = 0usize;
    while filled < len {
        let n = file.seek_read(&mut buf[filled..], offset + filled as u64)?;
        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "pread_exact_into: unexpected EOF",
            ));
        }
        filled += n;
    }
    Ok(())
}

/// Lock-free positional read using a raw file descriptor (Unix).
///
/// Calls `pread(2)` directly, bypassing the `RwLock<File>`.  Safe only when
/// the target range is within the immutable locked region, ensuring no
/// concurrent writer can touch those bytes.
#[cfg(unix)]
pub(crate) fn pread_exact_raw(fd: RawFd, offset: u64, buf: &mut [u8]) -> io::Result<()> {
    let mut filled = 0usize;
    while filled < buf.len() {
        let n = unsafe {
            libc::pread(
                fd,
                buf[filled..].as_mut_ptr() as *mut libc::c_void,
                buf.len() - filled,
                (offset + filled as u64) as libc::off_t,
            )
        };
        if n < 0 {
            return Err(io::Error::last_os_error());
        }
        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "locked pread: unexpected EOF",
            ));
        }
        filled += n as usize;
    }
    Ok(())
}

/// Lock-free positional read using a raw Windows HANDLE.
///
/// Calls `ReadFile` with an `OVERLAPPED` offset, bypassing the `RwLock<File>`.
/// Safe under the same invariant as `pread_exact_raw`.
#[cfg(windows)]
pub(crate) fn pread_exact_raw_handle(handle: isize, offset: u64, buf: &mut [u8]) -> io::Result<()> {
    let handle = handle as HANDLE;
    let mut filled = 0usize;
    let len = buf.len();
    while filled < len {
        let current_offset = offset + filled as u64;
        let mut overlapped: OVERLAPPED = unsafe { std::mem::zeroed() };
        // SAFETY: the Anonymous/Anonymous path exists in the windows-sys OVERLAPPED layout.
        overlapped.Anonymous.Anonymous.Offset = current_offset as u32;
        overlapped.Anonymous.Anonymous.OffsetHigh = (current_offset >> 32) as u32;
        let mut bytes_read: u32 = 0;
        let ret = unsafe {
            ReadFile(
                handle,
                buf[filled..].as_mut_ptr(),
                (len - filled) as u32,
                &mut bytes_read,
                &mut overlapped,
            )
        };
        if ret == 0 {
            return Err(io::Error::last_os_error());
        }
        if bytes_read == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "locked ReadFile: unexpected EOF",
            ));
        }
        filled += bytes_read as usize;
    }
    Ok(())
}

/// Flush all in-flight writes to stable storage.
///
/// On macOS this uses `F_FULLFSYNC` to flush the drive's hardware write cache,
/// which `fdatasync` alone does not guarantee.  Falls back to `sync_data` if
/// `F_FULLFSYNC` returns an error (e.g. the device doesn't support it).
/// On all other platforms this delegates to `sync_data` (`fdatasync`).
pub(crate) fn durable_sync(file: &File) -> io::Result<()> {
    #[cfg(target_os = "macos")]
    {
        let ret = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_FULLFSYNC) };
        if ret != -1 {
            return Ok(());
        }
        // Device does not support F_FULLFSYNC; fall back to fdatasync.
    }
    file.sync_data()
}

/// Acquire an exclusive, non-blocking advisory flock on `file`.
///
/// Returns `Err(WouldBlock)` if another process already holds the lock.
#[cfg(unix)]
pub(crate) fn flock_exclusive(file: &File) -> io::Result<()> {
    let ret = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if ret == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

/// Acquire an exclusive, non-blocking `LockFileEx` lock on `file`.
///
/// Locks the entire file range (offset 0, length `u64::MAX`).
/// Returns `Err(WouldBlock)` if another process already holds the lock
/// (`ERROR_LOCK_VIOLATION` maps to `io::ErrorKind::WouldBlock` in Rust).
#[cfg(windows)]
pub(crate) fn lock_file_exclusive(file: &File) -> io::Result<()> {
    let handle = file.as_raw_handle() as windows_sys::Win32::Foundation::HANDLE;
    // OVERLAPPED is required by LockFileEx even for synchronous handles.
    // Offset fields (0, 0) anchor the lock at byte 0 of the file.
    let mut overlapped: OVERLAPPED = unsafe { std::mem::zeroed() };
    let ret = unsafe {
        LockFileEx(
            handle,
            LOCKFILE_EXCLUSIVE_LOCK | LOCKFILE_FAIL_IMMEDIATELY,
            0,        // reserved, must be zero
            u32::MAX, // nNumberOfBytesToLockLow  ─┐ lock entire
            u32::MAX, // nNumberOfBytesToLockHigh ─┘ file space
            &mut overlapped,
        )
    };
    if ret != 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

// ------------------------------------------- BStack Primitives -------------------------------------------

/// Return `true` if overwriting `[offset, offset + len)` of the payload is
/// guaranteed atomic at the storage level — i.e. the bytes are confined to a
/// single [`ATOMIC_BLOCK`]-aligned region of the file and therefore cannot tear
/// across a block boundary on power loss.
///
/// `offset` is a logical payload offset; the test is applied to the physical
/// file position `HEADER_SIZE + offset`, since the file's first byte is
/// block-aligned. The write is atomic exactly when its first and last bytes fall
/// in the same block. An empty write is trivially atomic; an offset/length that
/// overflows `u64` cannot be confined to one block and is reported non-atomic.
///
/// This is the gate for skipping the write-in-progress journal on a same-length
/// `set` whose slice fits within one block (the top rung of *Choosing a write
/// strategy* in `PLANNED.md`). It is conservative: a write it rejects may still
/// be atomic on hardware with a larger true block size, but a write it accepts
/// is atomic on all supported storage.
pub(crate) fn is_atomic_write(offset: u64, len: u64) -> bool {
    if len == 0 {
        return true;
    }
    let Some(start) = HEADER_SIZE.checked_add(offset) else {
        return false;
    };
    let Some(last) = start.checked_add(len - 1) else {
        return false;
    };
    start / ATOMIC_BLOCK == last / ATOMIC_BLOCK
}

/// Seek to logical `offset` (just past the header) and fill `buf` from the file.
///
/// Shared by every write-lock-held operation that reads a payload region
/// through the file cursor.
pub(crate) fn read_at(file: &mut File, offset: u64, buf: &mut [u8]) -> io::Result<()> {
    file.seek(SeekFrom::Start(HEADER_SIZE + offset))?;
    file.read_exact(buf)
}

/// Seek to logical `offset` (just past the header) and write `data` there.
///
/// Shared by every write-lock-held operation that mutates a payload region
/// through the file cursor.
pub(crate) fn write_at(file: &mut File, offset: u64, data: &[u8]) -> io::Result<()> {
    file.seek(SeekFrom::Start(HEADER_SIZE + offset))?;
    file.write_all(data)
}

/// Copy `n` bytes from logical `src` to logical `dst` within the file using a
/// buffer bounded by [`MOVE_CHUNK`] (O(1) memory).
///
/// The source and destination must not overlap. Every caller routes an
/// overlapping move through the disjoint tail region, so this always holds and no
/// memmove-direction handling is needed.
///
/// Splitting the copy into chunks introduces no concurrency hazard: a move is
/// only ever performed while its caller holds the `BStack` write lock, so no
/// other writer — and, on the rwlock-guarded paths, no reader — can observe or
/// touch the region between chunks. The chunking is purely an in-memory detail.
pub(crate) fn move_chunked(file: &mut File, src: u64, dst: u64, n: u64) -> io::Result<()> {
    let cap = n.min(MOVE_CHUNK) as usize;
    let mut buf = vec![0u8; cap];
    let mut done = 0u64;
    while done < n {
        let take = ((n - done) as usize).min(cap);
        read_at(file, src + done, &mut buf[..take])?;
        write_at(file, dst + done, &buf[..take])?;
        done += take as u64;
    }
    Ok(())
}

/// Overwrite the committed-length field at file offset 8 and update the
/// in-memory cache (`clen`) to match.
pub(crate) fn write_committed_len(file: &mut File, clen: &mut u64, len: u64) -> io::Result<()> {
    file.seek(SeekFrom::Start(8))?;
    file.write_all(&len.to_le_bytes())?;
    *clen = len;
    Ok(())
}

// --------------------------------------- Write-in-progress Journal --------------------------------------

/// The `wip_aux` header field: which kind of in-place journal is armed.
///
/// Each variant has an explicit on-disk `u64` value. `Set` is `0` (the steady
/// disarmed state also reads as `0`); every other mode takes a value near
/// `u64::MAX`, decrementing as modes are added, so the low-value range stays free
/// for any future packed encoding and unrecognized values are unmistakable. Any
/// per-operation data (a fill count, a length delta, …) lives in the staged tail,
/// not in `wip_aux`.
///
/// Convert to the on-disk value with `u64::from(aux)`; classify a value read back
/// from the header with `WipAux::try_from(v)` (an unknown value is an `Err`,
/// which recovery treats as a roll-back). Not feature-gated: recovery must
/// understand every mode regardless of which features this build enables.
#[repr(u64)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum WipAux {
    /// Same-length replay: the tail holds the exact bytes to write into
    /// `[wip_ptr, wip_ptr + tail_len)`. Armed by `set`, `swap`/`cas`, `copy`, and
    /// `cross_exchange` (whose commit is a `wip_ptr` flip between two `Set` arms).
    Set = 0,
    /// Repeat-fill: the tail holds `[k: u64 LE | s]`; recovery writes `k` copies
    /// of `s` into `[wip_ptr, wip_ptr + k*s.len())`.
    Repeat = u64::MAX - 3,
}

impl From<WipAux> for u64 {
    #[inline]
    fn from(aux: WipAux) -> Self {
        aux as u64
    }
}

impl TryFrom<u64> for WipAux {
    type Error = ();

    #[inline]
    fn try_from(value: u64) -> Result<Self, Self::Error> {
        match value {
            v if v == WipAux::Set as u64 => Ok(WipAux::Set),
            v if v == WipAux::Repeat as u64 => Ok(WipAux::Repeat),
            _ => Err(()),
        }
    }
}

/// Overwrite the write-in-progress journal fields — `wip_ptr` at offset 16,
/// `wip_aux` at offset 24 — in a single 16-byte write to the header.
///
/// Both fields lie within the first 32 bytes (one aligned block), so the write
/// is atomic at the storage level: recovery never observes `wip_ptr` and
/// `wip_aux` out of step. `wip_ptr == 0` is the disarmed (steady) state.
pub(crate) fn write_wip(file: &mut File, wip_ptr: u64, wip_aux: WipAux) -> io::Result<()> {
    let mut buf = [0u8; 16];
    buf[0..8].copy_from_slice(&wip_ptr.to_le_bytes());
    buf[8..16].copy_from_slice(&u64::from(wip_aux).to_le_bytes());
    file.seek(SeekFrom::Start(16))?;
    file.write_all(&buf)
}

/// Recover an armed in-place journal (`wip_ptr != 0`) found on open, restoring
/// the at-rest invariant `file_size == HEADER_SIZE + committed_len`.
///
/// [`WipAux::Set`] replays the staged tail `[HEADER_SIZE + committed_len,
/// raw_size)` — the exact new bytes — into `[wip_ptr, wip_ptr + n)`. [`WipAux::Repeat`]
/// reads `[k | s]` from that tail and writes `k` copies of `s` into `[wip_ptr,
/// wip_ptr + k*s.len())`. Both replays are idempotent (the tail is immutable and
/// disjoint from the target). Any other `wip_aux` is a mode this build does not
/// interpret and is rolled back to the committed length (the forward-compatibility
/// default). Either way the journal is disarmed and the staged tail dropped;
/// `committed_len` is unchanged.
///
/// Not feature-gated: a file armed by a `set`-enabled build must still recover
/// correctly when reopened by a build without the feature.
pub(crate) fn recover_wip(
    file: &mut File,
    committed_len: u64,
    wip_ptr: u64,
    wip_aux: u64,
    raw_size: u64,
) -> io::Result<()> {
    let tail_start = HEADER_SIZE + committed_len;
    let tail_len = raw_size.saturating_sub(tail_start);
    match WipAux::try_from(wip_aux) {
        Ok(WipAux::Set) => {
            // Replay the tail verbatim into the target, if the tail is present and
            // the target lies wholly within the committed payload.
            if tail_len > 0
                && wip_ptr >= HEADER_SIZE
                && wip_ptr.saturating_add(tail_len) <= tail_start
            {
                move_chunked(file, committed_len, wip_ptr - HEADER_SIZE, tail_len)?;
                durable_sync(file)?;
            }
        }
        Ok(WipAux::Repeat) => {
            // Tail is `[k: u64 LE | s]`; replay `k` copies of `s` into the target.
            if tail_len >= 8 {
                let mut kbuf = [0u8; 8];
                file.seek(SeekFrom::Start(tail_start))?;
                file.read_exact(&mut kbuf)?;
                let k = u64::from_le_bytes(kbuf);
                let s_len = tail_len - 8;
                let total = k.saturating_mul(s_len);
                if s_len > 0
                    && wip_ptr >= HEADER_SIZE
                    && wip_ptr.saturating_add(total) <= tail_start
                {
                    let mut s = vec![0u8; s_len as usize];
                    file.read_exact(&mut s)?;
                    write_repeated(file, wip_ptr, &s, k)?;
                    durable_sync(file)?;
                }
            }
        }
        // Unknown mode: roll back to the committed length (no replay).
        Err(()) => {}
    }
    // Disarm, then drop the staged tail.
    write_wip(file, 0, WipAux::Set)?;
    durable_sync(file)?;
    file.set_len(tail_start)?;
    durable_sync(file)
}

// ------------------------------------------ Set ------------------------------------------

/// Crash-atomically overwrite the payload slice `[offset, offset + data.len())`
/// with `data` (same length, so `clen` is unchanged) via the write-in-progress
/// journal.
///
/// Callers must have validated the range and already ruled out the atomic-write
/// fast path. `data_size` is the current payload length, so `HEADER_SIZE +
/// data_size` is both the committed end and the current file end.
///
/// The four-barrier protocol (stage → arm → commit → disarm) guarantees a crash
/// leaves either the old bytes or the new bytes, never a mix — see the
/// same-length `set` algorithm in `PLANNED.md`.
#[cfg(feature = "set")]
pub(crate) fn journaled_set(
    file: &mut File,
    data_size: u64,
    offset: u64,
    data: &[u8],
) -> io::Result<()> {
    // 1. Stage: append `data` as a tail backup beyond the committed end.
    write_at(file, data_size, data)?;
    durable_sync(file)?;
    // 2. Arm: point `wip_ptr` at the physical target.
    write_wip(file, HEADER_SIZE + offset, WipAux::Set)?;
    durable_sync(file)?;
    // 3. Commit in place. The new bytes are already in memory; the staged tail
    //    is the crash backup recovery would replay from.
    write_at(file, offset, data)?;
    durable_sync(file)?;
    // 4. Disarm.
    write_wip(file, 0, WipAux::Set)?;
    durable_sync(file)?;
    // 5. Drop the tail backup, restoring `file_size == HEADER_SIZE + data_size`.
    file.set_len(HEADER_SIZE + data_size)
}

/// Durably overwrite the same-length payload slice `[offset, offset+data.len())`
/// with `data`, picking the cheapest crash-safe strategy: a slice confined to a
/// single aligned block is written atomically at the storage level (one synced
/// write); anything larger goes through [`journaled_set`]. `data_size` is the
/// current payload length. See *Choosing a write strategy* in `PLANNED.md`.
///
/// Callers must have already validated the range and the locked region.
#[cfg(feature = "set")]
#[inline]
pub(crate) fn set_in_place(
    file: &mut File,
    data_size: u64,
    offset: u64,
    data: &[u8],
) -> io::Result<()> {
    if is_atomic_write(offset, data.len() as u64) {
        write_at(file, offset, data)?;
        durable_sync(file)
    } else {
        journaled_set(file, data_size, offset, data)
    }
}

// ------------------------------------------ Repeat ------------------------------------------

/// Crash-atomically fill `[offset, offset + k*s.len())` with `k` copies of `s`
/// through the repeat-fill journal.
///
/// Only the pattern and count are staged, so the tail is `8 + s.len()` bytes no
/// matter how large the filled region is — the win over journaling the whole
/// region. The fill itself streams through a bounded buffer ([`write_repeated`]),
/// so the operation is O(s.len()) memory.
#[cfg(feature = "set")]
pub(crate) fn journaled_repeat(
    file: &mut File,
    data_size: u64,
    offset: u64,
    s: &[u8],
    k: u64,
) -> io::Result<()> {
    // 1. Stage `[k | s]` beyond the committed end.
    let tail = HEADER_SIZE + data_size;
    file.seek(SeekFrom::Start(tail))?;
    file.write_all(&k.to_le_bytes())?;
    file.write_all(s)?;
    durable_sync(file)?;
    // 2. Arm the repeat-fill journal at the target.
    write_wip(file, HEADER_SIZE + offset, WipAux::Repeat)?;
    durable_sync(file)?;
    // 3. Fill in place.
    write_repeated(file, HEADER_SIZE + offset, s, k)?;
    durable_sync(file)?;
    // 4. Disarm.
    write_wip(file, 0, WipAux::Set)?;
    durable_sync(file)?;
    // 5. Drop the staged tail, restoring `file_size == HEADER_SIZE + data_size`.
    file.set_len(HEADER_SIZE + data_size)
}

/// Fill `[phys .. phys + k*s.len())` with `k` back-to-back copies of `s`, writing
/// through a buffer of whole copies of `s` bounded by [`MOVE_CHUNK`] (so O(1)
/// memory beyond `s` itself). `phys` is a physical file offset.
///
/// Because the buffer is an exact number of copies of `s` and the total is
/// `k*s.len()`, every chunk boundary lands on a copy boundary, so the pattern
/// stays aligned even on the final short write. Used both by the live repeat-fill
/// and by recovery. Not feature-gated (recovery needs it).
#[cfg(feature = "set")]
pub(crate) fn write_repeated(file: &mut File, phys: u64, s: &[u8], k: u64) -> io::Result<()> {
    let unit = s.len() as u64;
    if unit == 0 || k == 0 {
        return Ok(());
    }
    let total = k * unit;
    // Pack as many whole copies of `s` into one buffer as fit under MOVE_CHUNK
    // (at least one), capped at `k`.
    let copies = (MOVE_CHUNK / unit).max(1).min(k);
    let mut buf = Vec::with_capacity((copies * unit) as usize);
    for _ in 0..copies {
        buf.extend_from_slice(s);
    }
    let mut done = 0u64;
    while done < total {
        let take = ((total - done) as usize).min(buf.len());
        file.seek(SeekFrom::Start(phys + done))?;
        file.write_all(&buf[..take])?;
        done += take as u64;
    }
    Ok(())
}

/// Fill `[offset, offset + k*s.len())` with `k` copies of `s`, crash-atomically,
/// choosing the atomic-block fast path or the repeat-fill journal. Same length,
/// so `clen` is unchanged. `s` is non-empty and `k >= 1`, and the caller has
/// validated the region lies within the payload. `data_size` is the current
/// payload length.
#[cfg(feature = "set")]
#[inline]
pub(crate) fn repeat_fill(
    file: &mut File,
    data_size: u64,
    offset: u64,
    s: &[u8],
    k: u64,
) -> io::Result<()> {
    let total = k.saturating_mul(s.len() as u64);
    if is_atomic_write(offset, total) {
        // Confined to one aligned block: a single write of the pattern is atomic.
        write_repeated(file, HEADER_SIZE + offset, s, k)?;
        durable_sync(file)
    } else {
        journaled_repeat(file, data_size, offset, s, k)
    }
}

// ------------------------------------------ Exchange ------------------------------------------

/// Crash-atomically exchange the equal-length payload regions `[a, a+n)` and
/// `[b, b+n)`: on return A holds B's old bytes and B holds A's old bytes.
///
/// **O(1) memory** — the bytes are streamed on disk, never buffered whole. Only
/// A's original bytes are staged (in the tail); the exchange commits at a single
/// atomic `wip_ptr` flip from A to B. The journal reuses the same-length replay
/// format (`wip_aux == 0`): the tail is A's bytes and `wip_ptr` names where
/// recovery replays them — at A (a no-op that rolls the exchange back) before the
/// flip, at B (which rolls it forward) after. `data_size` is the current payload
/// length. `a`/`b` are logical offsets of non-overlapping, unlocked regions the
/// caller has already validated.
#[cfg(all(feature = "set", feature = "atomic"))]
pub(crate) fn journaled_exchange(
    file: &mut File,
    data_size: u64,
    a: u64,
    b: u64,
    n: u64,
) -> io::Result<()> {
    // 1. Stage A's bytes beyond the committed end as the replay backup.
    move_chunked(file, a, data_size, n)?;
    durable_sync(file)?;
    // 2. Arm "replay tail into A". While `wip_ptr == A` a crash rolls the whole
    //    exchange back: recovery writes A's original bytes back into A (a no-op
    //    right now, and a repair once step 3 has overwritten A), leaving B alone.
    write_wip(file, HEADER_SIZE + a, WipAux::Set)?;
    durable_sync(file)?;
    // 3. A <- B's bytes (streamed straight from B). Still armed at A → rolls back.
    move_chunked(file, b, a, n)?;
    durable_sync(file)?;
    // 4. Flip to "replay tail into B" — the atomic commit point. Before it a crash
    //    rolls back (A restored); after it a crash rolls forward (B filled with
    //    A's bytes while A already holds B's bytes).
    write_wip(file, HEADER_SIZE + b, WipAux::Set)?;
    durable_sync(file)?;
    // 5. B <- A's bytes (streamed from the tail). A crash now rolls forward.
    move_chunked(file, data_size, b, n)?;
    durable_sync(file)?;
    // 6. Disarm and drop the staged tail.
    write_wip(file, 0, WipAux::Set)?;
    durable_sync(file)?;
    file.set_len(HEADER_SIZE + data_size)
}

// ------------------------------------------ Move ------------------------------------------

/// Crash-atomically copy `n` bytes from logical `src` to logical `dst` (same
/// length) through the write-in-progress journal, **O(1) memory**.
///
/// `src` and `dst` may overlap: the bytes route through the tail backup, which is
/// disjoint from both, so the two streamed moves never self-overwrite. `data_size`
/// is the current payload length.
#[cfg(all(feature = "set", feature = "atomic"))]
pub(crate) fn journaled_move(
    file: &mut File,
    data_size: u64,
    src: u64,
    dst: u64,
    n: u64,
) -> io::Result<()> {
    // 1. Stage the source bytes in the tail.
    move_chunked(file, src, data_size, n)?;
    durable_sync(file)?;
    // 2. Arm the destination as the replay target.
    write_wip(file, HEADER_SIZE + dst, WipAux::Set)?;
    durable_sync(file)?;
    // 3. Commit: stream the staged bytes into the destination.
    move_chunked(file, data_size, dst, n)?;
    durable_sync(file)?;
    // 4. Disarm.
    write_wip(file, 0, WipAux::Set)?;
    durable_sync(file)?;
    // 5. Drop the tail.
    file.set_len(HEADER_SIZE + data_size)
}

/// Commit a payload growth to `new_len` and durably sync.
///
/// On failure the file is rolled back (best effort) to `file_end` bytes and the
/// cached/committed length is reset to `old_len` before the original error is
/// returned. The cache is reset up front so it reflects the rolled-back file
/// even if the best-effort header rewrite fails. Shared by `push`, `extend`,
/// `try_extend`, `try_extend_zeros`, and the `Push` arm of `process_gen`.
#[inline]
pub(crate) fn commit_grow(
    file: &mut File,
    clen: &mut u64,
    new_len: u64,
    old_len: u64,
    file_end: u64,
) -> io::Result<()> {
    if let Err(e) = write_committed_len(file, clen, new_len).and_then(|_| durable_sync(file)) {
        let _ = file.set_len(file_end);
        *clen = old_len;
        let _ = write_committed_len(file, clen, old_len);
        let _ = durable_sync(file);
        return Err(e);
    }
    Ok(())
}

/// Commit a payload shrink to `new_len`: truncate the file, update the cached
/// length, write the header, and durably sync.
///
/// The truncation is the commit point — recovery adopts the smaller file size —
/// so the cache is updated before the header write, which `?` could skip on
/// error. Shared by `pop`, `pop_into`, `discard`, `try_discard`, and the `Pop`
/// and `Discard` arms of `process_gen`.
#[inline]
pub(crate) fn commit_shrink(file: &mut File, clen: &mut u64, new_len: u64) -> io::Result<()> {
    file.set_len(HEADER_SIZE + new_len)?;
    *clen = new_len;
    write_committed_len(file, clen, new_len)?;
    durable_sync(file)
}

/// Commit a tail replacement: the payload from `new_tail_start` onward is
/// replaced by `buf`, changing the payload length to `new_tail_start +
/// buf.len()`. `n` is the number of bytes removed from the old tail; `file_end`
/// is the pre-operation raw file size, used for rollback on the net-extension
/// path.
///
/// The write ordering is chosen from the net size change to maximise crash
/// safety (see *Durability* in the crate docs). Shared by `atrunc`, `splice`,
/// `splice_into`, and `replace`.
#[cfg(feature = "atomic")]
pub(crate) fn commit_tail_replace(
    file: &mut File,
    clen: &mut u64,
    new_tail_start: u64,
    n: u64,
    buf: &[u8],
    file_end: u64,
) -> io::Result<()> {
    let buf_len = buf.len() as u64;
    let final_data_len = new_tail_start + buf_len;
    if buf_len > n {
        // Net extension: extend first so data is never lost, then write buf,
        // sync the data, then commit the new length.
        file.set_len(HEADER_SIZE + final_data_len)?;
        if let Err(e) = write_at(file, new_tail_start, buf) {
            let _ = file.set_len(file_end);
            return Err(e);
        }
        if let Err(e) = durable_sync(file) {
            let _ = file.set_len(file_end);
            return Err(e);
        }
        write_committed_len(file, clen, final_data_len)?;
        durable_sync(file)?;
    } else {
        // Net truncation or same size: write buf into the old tail first,
        // truncate, sync, then commit the new length.
        if !buf.is_empty() {
            write_at(file, new_tail_start, buf)?;
        }
        file.set_len(HEADER_SIZE + final_data_len)?;
        // The truncation is the commit point (recovery adopts the smaller file
        // size), so update the cache now — before the sync and header write,
        // which `?` could skip on error.
        *clen = final_data_len;
        durable_sync(file)?;
        write_committed_len(file, clen, final_data_len)?;
        durable_sync(file)?;
    }
    Ok(())
}
