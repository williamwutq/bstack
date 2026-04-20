# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.3] - 2026-04-20

### Added
- **`peek_into` method**: Fill a caller-supplied `&mut [u8]` from a logical offset, avoiding the `Vec` allocation of `peek`
- **`get_into` method**: Fill a caller-supplied `&mut [u8]` from a half-open logical range, avoiding the `Vec` allocation of `get`
- **`pop_into` method**: Pop bytes from the tail directly into a caller-supplied `&mut [u8]`, avoiding the `Vec` allocation of `pop`
- **`impl std::io::Write for BStack`**: Each `write` call forwards to `push` — atomically appended and durably synced; `flush` is a no-op
- **`impl std::io::Write for &BStack`**: Shared-reference counterpart, mirroring `impl Write for &File`; enables `BufWriter<&BStack>` for batched writes
- **`BStackReader` type**: Cursor-based reader over `&BStack` implementing `std::io::Read`, `std::io::Seek`, and `From<&BStack>`; multiple readers can coexist and run concurrently
- **`BStack::reader()`**: Construct a `BStackReader` positioned at the start of the payload
- **`BStack::reader_at(offset)`**: Construct a `BStackReader` at an arbitrary logical offset

### Changed
- Moved tests to `src/test.rs` for better organization and to avoid cluttering the main library file

## [0.1.2] - 2026-04-18

### Added
- **Windows support**: Full first-class Windows support with `LockFileEx` for exclusive file locking and `ReadFile` with `OVERLAPPED` for cursor-safe positional reads
- **Concurrent reads on Windows**: `peek` and `get` operations now use the read lock on Windows, enabling concurrent readers just like on Unix
- **Cross-platform durability**: `FlushFileBuffers` on Windows provides equivalent durability guarantees to `fdatasync` on Unix

### Changed
- Updated thread-safety documentation to reflect Windows support alongside Unix
- Updated multi-process safety documentation to cover both `flock` (Unix) and `LockFileEx` (Windows)
- Extended concurrent reads test to run on both Unix and Windows platforms

### Dependencies
- Added `windows-sys` crate for Windows platform support

## [0.1.1] - 2026-04-17

### Added
- **`get` method**: Read arbitrary half-open byte ranges `[start, end)` from logical offsets
- **Concurrent reads on Unix**: `peek` and `get` operations now use `pread(2)` and take only the read lock, allowing multiple concurrent readers
- **Enhanced durability on macOS**: `durable_sync` now uses `F_FULLFSYNC` to flush the drive's hardware write cache, providing stronger guarantees than plain `fdatasync`

### Changed
- Updated thread-safety model documentation to reflect read-lock usage for `peek`/`get` on Unix

## [0.1.0] - 2026-04-16

### Added
- Initial release of `bstack`: A persistent, fsync-durable binary stack backed by a single file
- Core operations: `push`, `pop`, `peek`, `len`
- Crash recovery with committed-length sentinel
- Multi-process safety via advisory `flock` on Unix
- File format with 16-byte header containing magic number and committed length
- Durability guarantees with `durable_sync` (fdatasync on Unix)
- Optional `set` feature for in-place payload mutation