//! Small deterministic hash helpers shared by stdlib containers.

/// 64-bit FNV-1a over `bytes`. Deterministic so map layout is stable on disk.
pub(super) fn fnv1a(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in bytes {
        h ^= b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}
