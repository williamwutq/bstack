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

/// Two independent 64-bit hashes of `bytes` for **double hashing**
/// (Kirsch–Mitzenmacher): the `i`-th derived hash is `h1 + i * h2`, which lets a
/// Bloom filter compute `k` indices from two base hashes with the same
/// distribution quality as `k` independent ones. `h2` is forced odd so, modulo
/// any table size, successive indices stride the whole array rather than cycling
/// a small subset.
pub(super) fn double_hash(bytes: &[u8]) -> (u64, u64) {
    let h1 = fnv1a(bytes);
    // Re-hash `h1`'s bytes for an independent second hash (cheap, deterministic).
    let h2 = fnv1a(&h1.to_le_bytes()) | 1;
    (h1, h2)
}
