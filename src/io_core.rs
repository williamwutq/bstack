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
///
/// Single source of truth for the header size; re-exported into `lib.rs` via
/// `use io_core::*`.
pub(crate) const HEADER_SIZE: u64 = 32;

/// Conservative power-fail atomic block size, in bytes.
///
/// Real storage writes whole blocks atomically: a single write confined to one
/// block either lands in full or not at all across a power loss. Devices differ
/// in their true block size (commonly 512 B or 4 KB), but 256 B is a lower bound
/// that holds on virtually all hardware — including eMMC and older NVMe
/// controllers that advertise larger blocks yet only guarantee 512 B or 256 B
/// power-fail atomicity. Because 256 divides those sizes and the file's first
/// byte is block-aligned, a write confined to one 256 B-aligned region is always
/// contained within a single hardware block. See *Derived atomicity* in
/// `algos/WIP.md`.
#[cfg(any(feature = "set", feature = "atomic"))]
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
///
/// **The crate's own test builds skip the flush entirely.**  The tests never
/// crash the process — crash consistency is exercised by injecting faults
/// logically and reopening the file in-process — so the physical sync changes
/// neither their observable behavior nor the on-disk bytes, yet on macOS
/// `F_FULLFSYNC` dominates their runtime (it takes the allocator fault fuzz from
/// minutes to seconds).  This applies only to `cfg(test)` debug builds of this
/// crate; release builds and any dependent crate always issue the real sync.
///
/// **Downstream `debug-no-sync` feature.**  A dependent crate can opt into the
/// same skip for its own fault-injection tests by enabling the `debug-no-sync`
/// feature; like above this only takes effect with `debug_assertions` on, so a
/// `--release` build always issues the real sync regardless. Durability is not
/// guaranteed while this is active — debug/testing use only.
pub(crate) fn durable_sync(file: &File) -> io::Result<()> {
    #[cfg(all(debug_assertions, any(test, feature = "debug-no-sync")))]
    {
        let _ = file;
        Ok(())
    }
    #[cfg(not(all(debug_assertions, any(test, feature = "debug-no-sync"))))]
    {
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
/// strategy* in `algos/WIP.md`). It is conservative: a write it rejects may still
/// be atomic on hardware with a larger true block size, but a write it accepts
/// is atomic on all supported storage.
#[cfg(any(feature = "set", feature = "atomic"))]
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
    /// Length-growing tail splice (`clen' > clen`): the tail holds the new bytes.
    /// `wip_ptr` is the splice point `a`; the staged bytes are replayed into
    /// `[a, clen')` and the new length is derived from the file size. See
    /// [`journaled_splice`].
    SpliceGrow = u64::MAX - 1,
    /// Length-shrinking tail splice (`clen' < clen`): the shrink counterpart of
    /// [`SpliceGrow`](WipAux::SpliceGrow).
    SpliceShrink = u64::MAX - 2,
    /// Repeat-fill: the tail holds `[k: u64 LE | s]`; recovery writes `k` copies
    /// of `s` into `[wip_ptr, wip_ptr + k*s.len())`.
    Repeat = u64::MAX - 3,
    /// Disjoint copy: the tail holds `[src: u64 LE | n: u64 LE]` and `wip_ptr` is
    /// the destination. Because the source region does not overlap the
    /// destination, the source is untouched during the copy, so recovery replays
    /// `move_chunked(src → dst)` directly from it — only the source coordinate is
    /// journaled, never the `n` payload bytes. See [`journaled_copy`]. (An
    /// overlapping copy cannot use this mode and routes through [`WipAux::Set`]
    /// via [`journaled_move`] instead.)
    Copy = u64::MAX - 4,
    /// Multi-write intent-complete sentinel: several non-overlapping in-place
    /// writes have all been staged and must commit as one atomic unit. Unlike
    /// every other mode this is armed with `wip_ptr == 0` (it never names a
    /// single target), so it can never be confused with a single-region journal,
    /// which always arms `wip_ptr != 0`. The staged tail is a back-to-back
    /// sequence of `[s: u64 LE | e: u64 LE | data]` blocks running from
    /// `HEADER_SIZE + clen` to `file_size`; recovery replays each into
    /// `[s, e)`. See [`journaled_multi_set`] and [`recover_multi_write`].
    MultiWrite = u64::MAX - 5,
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
            v if v == WipAux::SpliceGrow as u64 => Ok(WipAux::SpliceGrow),
            v if v == WipAux::SpliceShrink as u64 => Ok(WipAux::SpliceShrink),
            v if v == WipAux::Repeat as u64 => Ok(WipAux::Repeat),
            v if v == WipAux::Copy as u64 => Ok(WipAux::Copy),
            v if v == WipAux::MultiWrite as u64 => Ok(WipAux::MultiWrite),
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
#[cfg(any(feature = "set", feature = "atomic"))]
pub(crate) fn write_wip(file: &mut File, wip_ptr: u64, wip_aux: WipAux) -> io::Result<()> {
    let mut buf = [0u8; 16];
    buf[0..8].copy_from_slice(&wip_ptr.to_le_bytes());
    buf[8..16].copy_from_slice(&u64::from(wip_aux).to_le_bytes());
    file.seek(SeekFrom::Start(16))?;
    file.write_all(&buf)
}

/// Atomically commit a new committed length **and** disarm the journal in a
/// single 24-byte header write: `clen` at offset 8, `wip_ptr`/`wip_aux` at 16/24.
///
/// All three fields lie within the first 32 bytes (one aligned block), so the new
/// length and the disarm land together — recovery never sees `clen` updated while
/// `wip` is still armed (which would break the splice length derivation). Used to
/// commit a splice and, uniformly, to finalize every recovery. Does not touch the
/// in-memory cache; the caller updates it.
pub(crate) fn write_header_commit(
    file: &mut File,
    clen: u64,
    wip_ptr: u64,
    wip_aux: WipAux,
) -> io::Result<()> {
    let mut buf = [0u8; 24];
    buf[0..8].copy_from_slice(&clen.to_le_bytes());
    buf[8..16].copy_from_slice(&wip_ptr.to_le_bytes());
    buf[16..24].copy_from_slice(&u64::from(wip_aux).to_le_bytes());
    file.seek(SeekFrom::Start(8))?;
    file.write_all(&buf)
}

/// Recover an armed in-place journal (`wip_ptr != 0`) found on open, restoring
/// the at-rest invariant `file_size == HEADER_SIZE + clen`. Returns the committed
/// length after recovery (unchanged except by a splice).
///
/// - [`WipAux::Set`] replays the staged tail `[HEADER_SIZE + committed_len,
///   raw_size)` — the exact new bytes — into `[wip_ptr, wip_ptr + n)`.
/// - [`WipAux::Repeat`] reads `[k | s]` from that tail and writes `k` copies of
///   `s` into `[wip_ptr, wip_ptr + k*s.len())`.
/// - [`WipAux::Copy`] reads `[src | n]` from that tail and replays
///   `move_chunked(src → wip_ptr)` from the untouched (disjoint) source.
/// - [`WipAux::SpliceGrow`]/[`WipAux::SpliceShrink`] replay the staged new tail
///   into `[a, clen')`, deriving `clen'` from the file size and direction, and
///   commit the new length.
///
/// Every replay is idempotent (the staged bytes are immutable and disjoint from
/// their target), so a crash during recovery is safe to re-run. Any unrecognized
/// `wip_aux`, or an inconsistent splice header, rolls back to `committed_len`.
/// The finalize (commit length + disarm, then truncate) is uniform across modes.
///
/// Not feature-gated: a file armed by a feature-enabled build must still recover
/// correctly when reopened by a build without that feature.
pub(crate) fn recover_wip(
    file: &mut File,
    committed_len: u64,
    wip_ptr: u64,
    wip_aux: u64,
    raw_size: u64,
) -> io::Result<u64> {
    let tail_start = HEADER_SIZE + committed_len;
    let tail_len = raw_size.saturating_sub(tail_start);
    // Committed length after recovery; only a splice changes it.
    let mut final_clen = committed_len;
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
                    && let Ok(s_len_usize) = usize::try_from(s_len)
                {
                    let mut s = vec![0u8; s_len_usize];
                    file.read_exact(&mut s)?;
                    write_repeated(file, wip_ptr, &s, k)?;
                    durable_sync(file)?;
                }
            }
        }
        Ok(WipAux::Copy) => {
            // Tail is `[src: u64 LE | n: u64 LE]`; `wip_ptr` is the destination.
            // The source is disjoint from the destination (the writer only arms
            // this mode when they don't overlap), so the source still holds the
            // original bytes — replay the copy directly from it. Roll back on a
            // corrupt or inconsistent header (out of range, or overlapping).
            if tail_len >= 16 && wip_ptr >= HEADER_SIZE {
                let mut meta = [0u8; 16];
                file.seek(SeekFrom::Start(tail_start))?;
                file.read_exact(&mut meta)?;
                let src = u64::from_le_bytes(meta[0..8].try_into().unwrap());
                let n = u64::from_le_bytes(meta[8..16].try_into().unwrap());
                let dst = wip_ptr - HEADER_SIZE;
                let disjoint = dst >= src.saturating_add(n) || src >= dst.saturating_add(n);
                if n > 0
                    && src.saturating_add(n) <= committed_len
                    && dst.saturating_add(n) <= committed_len
                    && disjoint
                {
                    move_chunked(file, src, dst, n)?;
                    durable_sync(file)?;
                }
            }
        }
        Ok(dir @ (WipAux::SpliceGrow | WipAux::SpliceShrink)) => {
            // The staged new tail sits at `[s, s+m)` where `s = max(clen, clen')`;
            // replay it into `[a, clen')`. `clen'` is derived from the armed file
            // size (`payload_end = S + m`) and the direction. Roll back on any
            // inconsistency (a corrupt or truncated header).
            let grow = matches!(dir, WipAux::SpliceGrow);
            if wip_ptr >= HEADER_SIZE {
                let a = wip_ptr - HEADER_SIZE;
                let payload_end = raw_size - HEADER_SIZE;
                let clen_new = if grow {
                    // payload_end == 2*clen' - a
                    payload_end.checked_add(a).map(|x| x / 2)
                } else {
                    // payload_end == clen + (clen' - a)
                    payload_end
                        .checked_add(a)
                        .and_then(|x| x.checked_sub(committed_len))
                };
                if let Some(clen_new) = clen_new {
                    let s = committed_len.max(clen_new);
                    let m = clen_new.saturating_sub(a);
                    let dir_ok = if grow {
                        clen_new > committed_len
                    } else {
                        clen_new < committed_len
                    };
                    // `s + m == payload_end` also rejects the odd-sum grow case
                    // that integer division would otherwise round.
                    if a <= committed_len
                        && clen_new >= a
                        && dir_ok
                        && s.saturating_add(m) == payload_end
                    {
                        move_chunked(file, s, a, m)?;
                        durable_sync(file)?;
                        final_clen = clen_new;
                    }
                }
            }
        }
        // The multi-write sentinel is only ever armed with `wip_ptr == 0`, so
        // seeing it here (`recover_wip` runs only when `wip_ptr != 0`) is an
        // inconsistent header — roll back like any unknown mode.
        Ok(WipAux::MultiWrite) => {}
        // Unknown mode: roll back to the committed length (no replay).
        Err(()) => {}
    }
    // Atomically commit the (possibly new) length and disarm, then drop any bytes
    // beyond it — restoring `file_size == HEADER_SIZE + final_clen`.
    write_header_commit(file, final_clen, 0, WipAux::Set)?;
    durable_sync(file)?;
    file.set_len(HEADER_SIZE + final_clen)?;
    durable_sync(file)?;
    Ok(final_clen)
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
/// Set mode in `algos/WIP.md`.
#[cfg(any(feature = "set", feature = "atomic"))]
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
/// current payload length. See *Choosing a write strategy* in `algos/WIP.md`.
///
/// Callers must have already validated the range and the locked region.
#[cfg(any(feature = "set", feature = "atomic"))]
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
pub(crate) fn write_repeated(file: &mut File, phys: u64, s: &[u8], k: u64) -> io::Result<()> {
    let unit = s.len() as u64;
    if unit == 0 || k == 0 {
        return Ok(());
    }
    let total = k.checked_mul(unit).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "write_repeated: length overflow",
        )
    })?;
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

/// Crash-atomically copy `n` bytes from logical `src` to logical `dst` when the
/// two regions are **disjoint**, through the copy journal — O(1) memory *and*
/// O(1) staging (only the source coordinate is journaled, never the bytes).
///
/// Because `src` and `dst` do not overlap, the source is never modified during the
/// in-place copy, so recovery re-runs `move_chunked(src → dst)` idempotently from
/// the still-intact source — no tail backup of the payload is needed. Only
/// `[src | n]` (16 bytes) is staged so recovery knows what to replay. This is the
/// win over [`journaled_move`], which stages the full `n` bytes to route an
/// *overlapping* copy through a disjoint tail. `data_size` is the current payload
/// length. Callers guarantee `[src, src+n)` and `[dst, dst+n)` do not overlap and
/// `n > 0`.
#[cfg(all(feature = "set", feature = "atomic"))]
pub(crate) fn journaled_copy(
    file: &mut File,
    data_size: u64,
    src: u64,
    dst: u64,
    n: u64,
) -> io::Result<()> {
    // 1. Stage `[src | n]` past the payload so recovery knows the source.
    let mut meta = [0u8; 16];
    meta[0..8].copy_from_slice(&src.to_le_bytes());
    meta[8..16].copy_from_slice(&n.to_le_bytes());
    write_at(file, data_size, &meta)?;
    durable_sync(file)?;
    // 2. Arm the destination as the replay target.
    write_wip(file, HEADER_SIZE + dst, WipAux::Copy)?;
    durable_sync(file)?;
    // 3. Commit: stream source → destination in place (disjoint → idempotent).
    move_chunked(file, src, dst, n)?;
    durable_sync(file)?;
    // 4. Disarm.
    write_wip(file, 0, WipAux::Set)?;
    durable_sync(file)?;
    // 5. Drop the staged metadata, restoring `file_size == HEADER_SIZE + data_size`.
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

/// Commit a *sparse* payload growth to `new_len`, writing only `blocks` into the
/// freshly grown region and leaving the rest zero-filled by the filesystem.
///
/// `logical_offset` is the pre-op payload size (the tail the growth is anchored
/// at); `file_end == HEADER_SIZE + logical_offset` is the pre-op raw file size;
/// `new_len == logical_offset + length` is the post-op payload size (already
/// overflow-checked by the caller). Each `(rel, data)` block is written at logical
/// offset `logical_offset + rel`; callers guarantee every block fits within
/// `[logical_offset, new_len)` and that blocks do not overlap.
///
/// The efficiency win over a full `push` of `length` bytes: the extension is
/// realised with a single `set_len`, so the gaps between blocks cost no I/O (they
/// read back as zero from the sparse file), and only the header commit is synced.
/// No journal is needed — the entire grown region sits beyond the committed
/// length, so a crash before the header commit rolls back by truncation, exactly
/// like [`commit_grow`]. Shared by `extend_sparse`, `extend_sparse_batched`, and
/// their `try_` variants.
#[inline]
pub(crate) fn commit_sparse_extend(
    file: &mut File,
    clen: &mut u64,
    logical_offset: u64,
    file_end: u64,
    new_len: u64,
    blocks: &[(u64, &[u8])],
) -> io::Result<()> {
    file.set_len(HEADER_SIZE + new_len)?;
    for (rel, data) in blocks {
        if let Err(e) = write_at(file, logical_offset + rel, data) {
            let _ = file.set_len(file_end);
            return Err(e);
        }
    }
    commit_grow(file, clen, new_len, logical_offset, file_end)
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

/// Crash-atomically replace the last `n_old` payload bytes — the tail slice
/// `[*clen − n_old, *clen)` — with `dn` (`dn.len() != n_old`), via the splice
/// journal. Updates `clen` (the cache) to `clen' = *clen − n_old + dn.len()`.
///
/// This overwrites committed bytes in place, so — unlike a pure append or
/// truncation — it needs the journal to avoid a torn commit. The file grows during
/// the operation to hold `dn` staged past the payload; recovery derives `clen'`
/// from the file size and rolls a crash forward (see [`recover_wip`]). Memory is
/// O(1) beyond `dn`. Callers guarantee `0 < n_old <= *clen`, `dn` non-empty, and
/// `dn.len() != n_old`.
#[cfg(feature = "atomic")]
pub(crate) fn journaled_splice(
    file: &mut File,
    clen: &mut u64,
    n_old: u64,
    dn: &[u8],
) -> io::Result<()> {
    let old_clen = *clen;
    let a = old_clen - n_old; // splice point (tail slice start)
    let m = dn.len() as u64;
    let clen_new = a + m; // = old_clen - n_old + m
    let s = old_clen.max(clen_new); // staging base, past the live end and clen'

    // 1. Stage `dn` at [s, s+m), disjoint from the live payload and the target.
    file.set_len(HEADER_SIZE + s + m)?;
    write_at(file, s, dn)?;
    durable_sync(file)?;
    // 2. Arm with the direction; recovery derives `clen'` from the file size.
    let dir = if m > n_old {
        WipAux::SpliceGrow
    } else {
        WipAux::SpliceShrink
    };
    write_wip(file, HEADER_SIZE + a, dir)?;
    durable_sync(file)?;
    // 3. Replay `dn` into the target [a, clen'). Disjoint → idempotent.
    move_chunked(file, s, a, m)?;
    durable_sync(file)?;
    // 4. Commit the new length while disarming, in one atomic header write.
    write_header_commit(file, clen_new, 0, WipAux::Set)?;
    *clen = clen_new;
    durable_sync(file)?;
    // 5. Drop the staged bytes, restoring `file_size == HEADER_SIZE + clen'`.
    file.set_len(HEADER_SIZE + clen_new)
}

/// Commit a tail replacement: replace the last `n` payload bytes with `buf`,
/// changing the length to `*clen − n + buf.len()`. `new_tail_start` is the splice
/// point `a == *clen − n`; `file_end` is the pre-op raw file size (rollback anchor
/// for the append path).
///
/// Dispatches by shape to the cheapest crash-atomic path (see *Choosing a write
/// strategy* in `algos/WIP.md`):
/// - `buf` empty → pure truncation (drop the tail, commit `clen`);
/// - `n == 0` → pure append (bytes land beyond `clen`) — no journal, since no
///   committed bytes are overwritten;
/// - `buf.len() == n` → same-length tail overwrite via the `Set` journal
///   ([`set_in_place`]);
/// - otherwise → length-changing tail replace via the splice journal
///   ([`journaled_splice`]).
///
/// Shared by `atrunc`, `splice`, `splice_into`, and `replace`.
#[cfg(feature = "atomic")]
pub(crate) fn commit_tail_replace(
    file: &mut File,
    clen: &mut u64,
    new_tail_start: u64,
    n: u64,
    buf: &[u8],
    file_end: u64,
) -> io::Result<()> {
    let m = buf.len() as u64;
    let a = new_tail_start; // == *clen - n
    if m == 0 {
        // Pure truncation (or a no-op when n == 0): no committed bytes overwritten.
        commit_shrink(file, clen, a)
    } else if n == 0 {
        // Pure append: `buf` lands beyond the committed end, uncommitted until the
        // `clen` write, so a crash rolls back by truncation.
        let final_data_len = a + m;
        file.set_len(HEADER_SIZE + final_data_len)?;
        if let Err(e) = write_at(file, a, buf) {
            let _ = file.set_len(file_end);
            return Err(e);
        }
        if let Err(e) = durable_sync(file) {
            let _ = file.set_len(file_end);
            return Err(e);
        }
        write_committed_len(file, clen, final_data_len)?;
        durable_sync(file)
    } else if m == n {
        // Same-length overwrite of the tail: no length change, so the `Set`
        // journal (or a single-block atomic write) suffices.
        set_in_place(file, *clen, a, buf)
    } else {
        // Length-changing tail replace: journal it.
        journaled_splice(file, clen, n, buf)
    }
}

// --------------------------------------- Multi-write journal --------------------------------------

/// Crash-atomically commit `k` non-overlapping in-place writes `{(offset_i,
/// data_i)}` as a single unit through the multi-write journal, **O(1) memory
/// beyond the caller's own slices**.
///
/// `data_size` is the current payload length (`clen`); it is never changed —
/// every write is a same-length, in-place overwrite of committed bytes, so the
/// staging region is pinned between `HEADER_SIZE + data_size` (start) and the
/// grown file end. Callers guarantee every block lies within `[0, data_size)`,
/// no block overlaps the locked prefix, and the blocks are pairwise
/// non-overlapping (`set_batched` rejects overlap; `inplace_gen` resolves it).
/// Empty `data_i` blocks must be filtered out by the caller. `blocks` must be
/// non-empty (a lone write should take [`set_in_place`], and an empty batch is a
/// no-op) — this path is for `k >= 2`.
///
/// The four-barrier protocol (stage → arm → replay → disarm) guarantees a crash
/// leaves either every block's old bytes or every block's new bytes, never a
/// partial mix — see the Multi-write journal in `algos/WIP.md`. `wip_ptr` stays
/// `0` throughout; the intent-complete sentinel ([`WipAux::MultiWrite`]) is the
/// commit point.
#[cfg(all(feature = "set", feature = "atomic"))]
pub(crate) fn journaled_multi_set(
    file: &mut File,
    data_size: u64,
    blocks: &[(u64, &[u8])],
) -> io::Result<()> {
    // 1. Stage every block `[s | e | data]` back-to-back beyond the committed
    //    end. The blocks are contiguous, so one seek then sequential writes
    //    suffice; the file grows to hold them.
    file.seek(SeekFrom::Start(HEADER_SIZE + data_size))?;
    for (offset, data) in blocks {
        let end = offset + data.len() as u64;
        file.write_all(&offset.to_le_bytes())?;
        file.write_all(&end.to_le_bytes())?;
        file.write_all(data)?;
    }
    durable_sync(file)?;
    // 2. Arm the intent-complete sentinel. `wip_ptr` stays 0 (single header
    //    write), so this can never be confused with a single-region journal.
    write_wip(file, 0, WipAux::MultiWrite)?;
    durable_sync(file)?;
    // 3. Replay: write each block into its target in place. Order is arbitrary
    //    (the ranges are non-overlapping), and every block's staged copy is the
    //    crash backup recovery replays from.
    for (offset, data) in blocks {
        write_at(file, *offset, data)?;
    }
    durable_sync(file)?;
    // 4. Disarm.
    write_wip(file, 0, WipAux::Set)?;
    durable_sync(file)?;
    // 5. Drop the staged tail, restoring `file_size == HEADER_SIZE + data_size`.
    file.set_len(HEADER_SIZE + data_size)
}

/// Walk the staged multi-write tail `[tail_start, raw_size)` and, for each
/// well-formed `[s | e | data]` block, invoke `apply(s, data_src_logical, len)`.
///
/// Returns `Ok(true)` if the entire tail parsed into a clean sequence of blocks
/// that ends exactly at `raw_size` with every target range within
/// `[0, committed_len)`, and `Ok(false)` on any malformation (a truncated or
/// oversized block, a reversed range, an out-of-bounds target, or trailing
/// bytes). A legitimately-armed tail always parses; `false` means genuine
/// corruption, for which recovery rolls back.
///
/// `data_src_logical` is the logical offset of the block's payload bytes (their
/// physical position minus the header), suitable for [`move_chunked`]'s `src`.
fn walk_multi_blocks(
    file: &mut File,
    committed_len: u64,
    tail_start: u64,
    raw_size: u64,
    mut apply: impl FnMut(&mut File, u64, u64, u64) -> io::Result<()>,
) -> io::Result<bool> {
    let mut cursor = tail_start;
    while cursor < raw_size {
        // Header must be fully present.
        if raw_size - cursor < 16 {
            return Ok(false);
        }
        let mut hdr = [0u8; 16];
        file.seek(SeekFrom::Start(cursor))?;
        file.read_exact(&mut hdr)?;
        let s = u64::from_le_bytes(hdr[0..8].try_into().unwrap());
        let e = u64::from_le_bytes(hdr[8..16].try_into().unwrap());
        // Well-formedness: forward range, within the committed payload.
        if e < s || e > committed_len {
            return Ok(false);
        }
        let plen = e - s;
        let payload_phys = cursor + 16;
        // Payload must be fully present in the staged tail.
        if payload_phys.saturating_add(plen) > raw_size {
            return Ok(false);
        }
        apply(file, s, payload_phys - HEADER_SIZE, plen)?;
        cursor = payload_phys + plen;
    }
    // A clean walk lands exactly on `raw_size` (guaranteed by the fits-checks
    // above, which reject any trailing partial block).
    Ok(cursor == raw_size)
}

/// Recover a crashed multi-write journal found on open (`wip_ptr == 0`,
/// `wip_aux == MultiWrite`), restoring the at-rest invariant `file_size ==
/// HEADER_SIZE + committed_len`. Returns the committed length (unchanged — a
/// multi-write never changes the payload size).
///
/// The staged tail `[HEADER_SIZE + committed_len, raw_size)` holds a back-to-back
/// sequence of `[s | e | data]` blocks. Recovery validates the whole sequence
/// first (a two-pass walk), then — only if it is clean — replays every block into
/// `[s, e)`. Validating before applying makes the replay all-or-nothing: a
/// genuinely-armed tail always validates, and a corrupt one applies nothing and
/// rolls back. Each replay is idempotent (the staged bytes are immutable and
/// disjoint from every target, which all sit below `committed_len`), so a crash
/// during recovery is safe to re-run.
///
/// Not feature-gated: a file armed by a feature-enabled build must still recover
/// correctly when reopened by a build without that feature.
pub(crate) fn recover_multi_write(
    file: &mut File,
    committed_len: u64,
    raw_size: u64,
) -> io::Result<u64> {
    let actual_len = raw_size.saturating_sub(HEADER_SIZE);
    let committed_len = committed_len.min(actual_len);
    let tail_start = HEADER_SIZE + committed_len;
    if raw_size > tail_start {
        // Pass 1: validate the whole sequence without touching the payload.
        let valid = walk_multi_blocks(file, committed_len, tail_start, raw_size, |_, _, _, _| {
            Ok(())
        })?;
        // Pass 2: apply, but only if the sequence is clean end-to-end.
        if valid {
            walk_multi_blocks(
                file,
                committed_len,
                tail_start,
                raw_size,
                |f, dst, src, n| move_chunked(f, src, dst, n),
            )?;
            durable_sync(file)?;
        }
    }
    // Disarm and drop the staged tail (finalize). `clen` is unchanged.
    write_header_commit(file, committed_len, 0, WipAux::Set)?;
    durable_sync(file)?;
    file.set_len(tail_start)?;
    durable_sync(file)?;
    Ok(committed_len)
}

/// Insert an in-place write `[off, off + data.len())` into an `inplace_gen`
/// overlay, keeping it a sorted, pairwise-non-overlapping set of pending edits.
///
/// Overlap is resolved in favour of the newer write: any portion of an existing
/// edit covered by `[off, off + data.len())` is dropped, and its non-overlapping
/// prefix and/or suffix are retained as sub-slices of the same `&'a` data. All
/// containment cases fall out of the prefix/suffix split:
///
/// - **new encloses old** (`off <= s` and `e <= end`): neither prefix nor suffix
///   survives — the old edit is dropped entirely.
/// - **old encloses new** (`s < off` and `e > end`): both a prefix `[s, off)` and
///   a suffix `[end, e)` survive, and the new edit fills the gap between them —
///   one edit splits into three.
/// - **partial overlap on either side**: only the non-covered end survives.
///
/// So issuing `a..c` then `b..d` (with `a<b<c<d`) leaves `a..b` (first write),
/// `b..c` (second, overriding), and `c..d` (second) — the non-overlapping blocks
/// the multi-write journal commits. Callers pass only non-empty `data`.
#[cfg(all(feature = "set", feature = "atomic"))]
pub(crate) fn inplace_overlay_insert<'a>(
    overlay: &mut Vec<(u64, &'a [u8])>,
    off: u64,
    data: &'a [u8],
) {
    let end = off + data.len() as u64;
    // The overlay is sorted by start and non-overlapping, so its ends are sorted
    // too: the edits the new write touches form one contiguous run `[lo, hi)`.
    // Binary search both ends instead of scanning — `lo` is the first edit
    // reaching past `off` (`edit end > off`), `hi` the first edit starting at or
    // past `end`.
    let lo = overlay.partition_point(|&(s, d)| s + d.len() as u64 <= off);
    let hi = overlay.partition_point(|&(s, _)| s < end);
    // Only the run's first edit can start before `off`, and only its last can end
    // after `end` (interior edits are fully covered and dropped); each contributes
    // at most a surviving prefix / suffix around the new edit.
    let mut repl: Vec<(u64, &'a [u8])> = Vec::with_capacity(3);
    if lo < hi {
        let (s0, d0) = overlay[lo];
        if s0 < off {
            repl.push((s0, &d0[..(off - s0) as usize]));
        }
    }
    repl.push((off, data));
    if lo < hi {
        let (s_last, d_last) = overlay[hi - 1];
        let e_last = s_last + d_last.len() as u64;
        if e_last > end {
            repl.push((end, &d_last[(end - s_last) as usize..]));
        }
    }
    // Replace the touched run in place; the surrounding edits keep their order.
    overlay.splice(lo..hi, repl);
}

/// Validate an `inplace_gen` `Write` op against the fixed payload size and the
/// locked prefix, mirroring `set`'s checks. An empty `data` is a valid no-op
/// (nothing is staged).
#[cfg(all(feature = "set", feature = "atomic"))]
pub(crate) fn inplace_validate_write(
    offset: u64,
    data: &[u8],
    data_size: u64,
    locked: u64,
) -> io::Result<()> {
    if data.is_empty() {
        return Ok(());
    }
    let end = crate::checked_end(
        offset,
        data.len() as u64,
        "inplace_gen: write offset + data.len() overflows u64",
    )?;
    if offset < locked {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "inplace_gen: write range [{offset}, {end}) overlaps locked region [0, {locked})"
            ),
        ));
    }
    if end > data_size {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "inplace_gen: write range [{offset}, {end}) exceeds payload size ({data_size})"
            ),
        ));
    }
    Ok(())
}

/// Serve an `inplace_gen` `Read` op: validate the range, then fill `buf` with the
/// batch-so-far ("new") content — the pending edits overlaid on the committed
/// bytes. The disk read is skipped entirely when the pending edits already cover
/// the whole range, so a read fully served from the overlay does no I/O.
#[cfg(all(feature = "set", feature = "atomic"))]
pub(crate) fn inplace_overlay_read(
    file: &mut File,
    data_size: u64,
    offset: u64,
    buf: &mut [u8],
    overlay: &[(u64, &[u8])],
) -> io::Result<()> {
    let end = crate::checked_end(
        offset,
        buf.len() as u64,
        "inplace_gen: read offset + buf.len() overflows u64",
    )?;
    if end > data_size {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("inplace_gen: read range [{offset}, {end}) exceeds payload size ({data_size})"),
        ));
    }
    if buf.is_empty() {
        return Ok(());
    }
    // The overlay is sorted by start and non-overlapping, so its ends are sorted
    // too: the edits intersecting `[offset, end)` form one contiguous run. Binary
    // search for the first edit that could reach into the read (`edit end >
    // offset`) — O(log n) instead of scanning every pending edit.
    let start = overlay.partition_point(|&(s, d)| s + d.len() as u64 <= offset);
    // First pass: find the run's end index and whether the edits leave any gap
    // over `[offset, end)`. A fully-covered read needs no committed bytes at all,
    // so the disk read can be skipped.
    let mut run_end = start;
    let mut covered_to = offset;
    let mut has_gap = false;
    for &(s, d) in &overlay[start..] {
        if s >= end {
            break;
        }
        if s > covered_to {
            has_gap = true; // a stretch of committed bytes shows through here
        }
        covered_to = s + d.len() as u64;
        run_end += 1;
    }
    if covered_to < end {
        has_gap = true; // committed bytes show through past the last edit
    }
    // Only touch the disk when some committed bytes are actually visible.
    if has_gap {
        read_at(file, offset, buf)?;
    }
    // Second pass: overlay each edit in the run. When there was no gap these
    // copies fill `buf` completely, so the skipped read left nothing uninitialised.
    for &(s, d) in &overlay[start..run_end] {
        let e = s + d.len() as u64;
        let lo = s.max(offset);
        let hi = e.min(end);
        let (b_lo, b_hi) = ((lo - offset) as usize, (hi - offset) as usize);
        let (d_lo, d_hi) = ((lo - s) as usize, (hi - s) as usize);
        buf[b_lo..b_hi].copy_from_slice(&d[d_lo..d_hi]);
    }
    Ok(())
}
