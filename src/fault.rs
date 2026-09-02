//! Deterministic I/O-fault injection at the [`BStack`](crate::BStack) API level.
//!
//! The happy path of the public API — successful `push` / `pop` / `realloc` /
//! `dealloc` sequences — can never reach the failure branches that the 0.4.0
//! error-handling contract most needs tested: which surviving handle a failed
//! `realloc` / `dealloc` returns, whether it reads back valid bytes, and whether
//! best-effort rollback actually frees an orphaned region. This module lets a
//! test *arm a fault* at a chosen operation (or a seeded, reproducible schedule)
//! and drive those branches on demand.
//!
//! # Granularity
//!
//! Faults are injected at the **`BStack` public-API level**, not per underlying
//! syscall: each fault-instrumented method consults the installed [`FaultPolicy`]
//! once, *after* validating its arguments, and — if the policy elects to fail —
//! returns the policy-supplied [`io::Error`](std::io::Error) instead of performing its I/O.
//! Argument-validation errors (overflow, out-of-range offsets, locked-region
//! violations) are therefore always reported in preference to an injected fault:
//! a call that would fail validation never reaches its fault point.
//!
//! A pending deferred replay is the one step that precedes the fault point: it
//! runs when a write takes the lock, ahead of validation, so a faulted write may
//! still have repaired the file. An injected fault never sets the replay flag
//! itself — it returns before any mutating call.
//!
//! The batched methods (`get_batched`, `get_batched_into`, `set_batched`) consult
//! the policy **once**, at the point their batch I/O begins. Their up-front,
//! whole-request validation still precedes that consult, but per-item checks
//! performed while iterating the batch do not — a fault armed for such a method
//! fails the whole call before those per-item checks run.
//!
//! # Compile-time gating
//!
//! All instrumentation is behind `all(debug_assertions, feature =
//! "fault-injection")`. The [`fault-injection`](crate) feature is **not** enabled
//! by default, and even when it is, a build with `debug_assertions` off (a normal
//! `--release` build) contains none of the machinery: no extra struct field, no
//! per-call branch, no policy consultation. Runtime performance of release builds
//! is completely unaffected.
//!
//! # Determinism under `atomic`
//!
//! When compound/concurrent operations run against a single stack, they share one
//! fault schedule: the per-stack operation counter is a single [`AtomicU64`](std::sync::atomic::AtomicU64) and
//! the policy is consulted under a shared lock, so a seeded policy is reproducible
//! from its seed for a fixed interleaving. Reproducibility *across* arbitrary
//! thread interleavings is inherently limited.

// The `fault_point!` macro is defined unconditionally so that call sites in
// `lib.rs` compile in every configuration; when the feature is inactive it
// expands to nothing (the `#[cfg]`-gated block is stripped), leaving zero code.
//
// Everything else in this module — the policy trait, the per-stack state, and
// the `BStack` methods that arm/consult it — is gated on the same predicate.

/// Consult the installed [`FaultPolicy`], if any, and return its error from the
/// enclosing method when it elects to fail this operation.
///
/// Expands to nothing unless `all(debug_assertions, feature = "fault-injection")`
/// holds, so it is free to sprinkle at the I/O boundary of every public method.
///
/// Place it **after** all argument validation and **before** the first mutating
/// (or, for read-only methods, the primary) I/O, so validation errors take
/// precedence over injected faults. It must be used inside a method returning
/// `io::Result<_>`; on a fault it `return`s `Err(_)` from that method.
///
/// `$op` is the stable, human-readable operation name a policy matches on
/// (typically the method name as a string literal).
macro_rules! fault_point {
    ($stack:expr, $op:expr) => {
        #[cfg(all(debug_assertions, feature = "fault-injection"))]
        {
            if let Some(__fault_err) = $crate::fault::__consult(&$stack.fault, $op) {
                return Err(__fault_err);
            }
        }
    };
}

/// Consult the installed [`FaultPolicy`] and *yield* its decision as an
/// `Option<io::Error>`, instead of returning from the enclosing method.
///
/// The counterpart of [`fault_point!`] for call sites where an injected error has
/// somewhere better to go than the caller — chiefly
/// [`BStack::inplace_gen`](crate::BStack::inplace_gen), whose `Read` failures are
/// reported to the generator through its `feedback` argument rather than by
/// aborting the sequence. Use `fault_point!` wherever `return Err(_)` is the
/// faithful response to a real I/O failure at that point, and this where it is
/// not.
///
/// Placement follows the same rule: **after** the step's argument validation,
/// **before** its I/O.
///
/// Evaluates to `None` unless `all(debug_assertions, feature = "fault-injection")`
/// holds, so an inactive build folds the whole call site away.
///
/// Gated on `set` + `atomic`, the configuration of its only call site: unlike
/// [`fault_point!`], which every build has somewhere to use, this one would be an
/// unused macro without them.
#[cfg(all(feature = "set", feature = "atomic"))]
macro_rules! fault_probe {
    ($stack:expr, $op:expr) => {{
        #[cfg(all(debug_assertions, feature = "fault-injection"))]
        let __fault_probe = $crate::fault::__consult(&$stack.fault, $op);
        #[cfg(not(all(debug_assertions, feature = "fault-injection")))]
        let __fault_probe: ::core::option::Option<::std::io::Error> = ::core::option::Option::None;
        __fault_probe
    }};
}

// Bring the macros into scope for the rest of the crate. `pub(crate) use` on a
// `macro_rules!` item is the edition-2024 idiom for a crate-internal macro.
pub(crate) use fault_point;
#[cfg(all(feature = "set", feature = "atomic"))]
pub(crate) use fault_probe;

#[cfg(all(debug_assertions, feature = "fault-injection"))]
pub use active::*;

#[cfg(all(debug_assertions, feature = "fault-injection"))]
mod active {
    use std::io;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Arc, Mutex};

    /// A user-supplied decision procedure that chooses when a [`BStack`](crate::BStack) I/O
    /// operation should fail, and with what [`io::Error`].
    ///
    /// A policy is installed on a stack with
    /// [`BStack::with_fault_policy`](crate::BStack::with_fault_policy) (at
    /// construction) or [`BStack::set_fault_policy`](crate::BStack::set_fault_policy)
    /// (to arm, re-arm, or disarm mid-test). While armed, the stack calls
    /// [`next_fault`](FaultPolicy::next_fault) once for each fault-instrumented
    /// operation, *after* that operation has validated its arguments.
    ///
    /// Implementations own any scheduling state (a countdown, a predicate over
    /// the operation name, a seeded PRNG, …) and must provide their own interior
    /// mutability and synchronisation, since `next_fault` takes `&self` and, under
    /// the `atomic` feature, may be called concurrently. The stack supplies a
    /// monotonically increasing per-arming sequence number to make seeded
    /// schedules reproducible.
    ///
    /// # Examples
    ///
    /// A policy that fails the `n`th instrumented operation exactly once:
    ///
    /// ```ignore
    /// use std::io;
    /// use bstack::fault::FaultPolicy;
    ///
    /// struct FailNth { n: u64 }
    /// impl FaultPolicy for FailNth {
    ///     fn next_fault(&self, op: &'static str, seq: u64) -> Option<io::Error> {
    ///         (seq == self.n).then(|| {
    ///             io_error!(Other, format!("injected fault at {op}"))
    ///         })
    ///     }
    /// }
    /// ```
    pub trait FaultPolicy: Send + Sync {
        /// Decide whether the operation named `op` — the `seq`-th instrumented
        /// operation since this policy was armed (0-based) — should fail.
        ///
        /// Return `Some(err)` to make the operation return `err` in place of its
        /// I/O; return `None` to let it proceed normally. `op` is the stable
        /// operation name (usually the method name); `seq` counts every consulted
        /// operation while this policy is armed, giving a deterministic, seedable
        /// schedule.
        fn next_fault(&self, op: &'static str, seq: u64) -> Option<io::Error>;
    }

    /// Per-stack fault-injection state: the currently armed policy (if any) and a
    /// shared operation counter. One instance lives inside each [`BStack`](crate::BStack) under
    /// the fault-injection configuration.
    pub struct FaultState {
        policy: Mutex<Option<Arc<dyn FaultPolicy>>>,
        seq: AtomicU64,
    }

    impl FaultState {
        /// Create an unarmed state (no policy installed).
        ///
        /// Written out rather than derived: `Default` would be a public impl on a
        /// type whose every method is crate-internal.
        pub(crate) fn new() -> Self {
            Self {
                policy: Mutex::new(None),
                seq: AtomicU64::new(0),
            }
        }

        /// Install (or, with `None`, clear) the policy and reset the sequence
        /// counter to 0 so the next arming starts a fresh, reproducible schedule.
        pub(crate) fn set(&self, policy: Option<Arc<dyn FaultPolicy>>) {
            let mut guard = self.policy.lock().unwrap();
            *guard = policy;
            self.seq.store(0, Ordering::SeqCst);
        }

        /// Snapshot the currently armed policy, if any.
        pub(crate) fn get(&self) -> Option<Arc<dyn FaultPolicy>> {
            self.policy.lock().unwrap().clone()
        }
    }

    impl std::fmt::Debug for FaultState {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("FaultState")
                .field("armed", &self.policy.lock().unwrap().is_some())
                .field("seq", &self.seq.load(Ordering::Relaxed))
                .finish()
        }
    }

    /// Consult a stack's fault state for operation `op`. Returns the injected
    /// error when the armed policy elects to fail, advancing the shared sequence
    /// counter by one per consultation while a policy is armed.
    ///
    /// The policy `Arc` is cloned out before the callback runs so the internal
    /// lock is not held across user code (avoiding re-entrancy hazards).
    #[inline]
    pub(crate) fn __consult(state: &FaultState, op: &'static str) -> Option<io::Error> {
        let policy = state.get()?;
        let seq = state.seq.fetch_add(1, Ordering::SeqCst);
        policy.next_fault(op, seq)
    }
}

#[cfg(all(test, debug_assertions, feature = "fault-injection"))]
mod tests {
    use super::FaultPolicy;
    use crate::BStack;
    use std::io;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn mk_stack() -> (BStack, std::path::PathBuf) {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let id = COUNTER.fetch_add(1, Ordering::Relaxed);
        let pid = std::process::id();
        let path = std::env::temp_dir().join(format!("bstack_fault_{pid}_{id}.bin"));
        let stack = BStack::open(&path).unwrap();
        (stack, path)
    }

    struct Guard(std::path::PathBuf);
    impl Drop for Guard {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }

    /// Fail the `at`-th consultation of any operation whose name matches `op`,
    /// exactly once, with a chosen error kind. Counts every consultation it sees
    /// (regardless of name) toward `seq` via the shared counter the stack owns.
    struct FailOpAt {
        op: &'static str,
        at: u64,
        kind: io::ErrorKind,
    }
    impl FaultPolicy for FailOpAt {
        fn next_fault(&self, op: &'static str, seq: u64) -> Option<io::Error> {
            (op == self.op && seq == self.at)
                .then(|| io::Error::new(self.kind, format!("injected at {op}#{seq}")))
        }
    }

    /// Fail every consultation, recording each `(op, seq)` it is asked about so a
    /// test can assert on the exact schedule the stack drove.
    struct FailAll {
        seen: std::sync::Mutex<Vec<(&'static str, u64)>>,
    }
    impl FaultPolicy for FailAll {
        fn next_fault(&self, op: &'static str, seq: u64) -> Option<io::Error> {
            self.seen.lock().unwrap().push((op, seq));
            Some(io::Error::other("always"))
        }
    }

    #[test]
    fn injects_then_rolls_back_and_disarms() {
        let (stack, path) = mk_stack();
        let _g = Guard(path);
        stack.push(b"seed").unwrap();
        assert_eq!(stack.len().unwrap(), 4);

        // Arm a fault on the first `push`. `seq` starts at 0 on arming.
        stack.set_fault_policy(Some(Arc::new(FailOpAt {
            op: "push",
            at: 0,
            kind: io::ErrorKind::StorageFull,
        })));

        let err = stack.push(b"more").unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::StorageFull);
        // The fault fires before any mutation: the payload is untouched.
        assert_eq!(stack.len().unwrap(), 4);
        // Prior read call (`len`) also consulted, so it advanced the counter;
        // but the policy only fires on "push"#0, so `len` returned cleanly.

        // Disarm: the very same push now succeeds.
        stack.set_fault_policy(None);
        stack.push(b"more").unwrap();
        assert_eq!(stack.len().unwrap(), 8);
        // Drop `stack` before `_g` removes the backing file: on Windows,
        // deleting a file that's still open typically fails.
        drop(stack);
    }

    #[test]
    fn validation_beats_injected_fault() {
        let (stack, path) = mk_stack();
        let _g = Guard(path);
        stack.push(b"abc").unwrap();

        // Arm a policy that would fail every operation...
        let policy = Arc::new(FailAll {
            seen: std::sync::Mutex::new(Vec::new()),
        });
        stack.set_fault_policy(Some(policy.clone()));

        // ...yet a `pop` that over-runs the payload must surface the argument
        // -validation error, because the fault point sits *after* validation.
        let err = stack.pop(999).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        // The over-run pop returned before reaching its fault point, so the
        // policy was never consulted for "pop".
        assert!(
            !policy
                .seen
                .lock()
                .unwrap()
                .iter()
                .any(|(op, _)| *op == "pop")
        );

        // A valid pop does reach the fault point and fails there.
        let err = stack.pop(1).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::Other);
        assert!(
            policy
                .seen
                .lock()
                .unwrap()
                .iter()
                .any(|(op, _)| *op == "pop")
        );
        // Drop `stack` before `_g` removes the backing file: on Windows,
        // deleting a file that's still open typically fails.
        drop(stack);
    }

    #[test]
    fn sequence_counter_is_shared_and_monotonic() {
        let (stack, path) = mk_stack();
        let _g = Guard(path);

        let policy = Arc::new(FailAll {
            seen: std::sync::Mutex::new(Vec::new()),
        });
        stack.set_fault_policy(Some(policy.clone()));

        // Three distinct operations; each consults once, and the shared counter
        // hands out 0, 1, 2 in call order regardless of which op it is.
        let _ = stack.len();
        let _ = stack.push(b"x");
        let _ = stack.len();
        let seen = policy.seen.lock().unwrap();
        assert_eq!(&seen[..], &[("len", 0), ("push", 1), ("len", 2)]);
        drop(seen);
        // Drop `stack` before `_g` removes the backing file: on Windows,
        // deleting a file that's still open typically fails.
        drop(stack);
    }

    #[test]
    fn re_arming_resets_the_counter() {
        let (stack, path) = mk_stack();
        let _g = Guard(path);

        let policy = Arc::new(FailAll {
            seen: std::sync::Mutex::new(Vec::new()),
        });
        stack.set_fault_policy(Some(policy.clone()));
        let _ = stack.len();
        let _ = stack.len();
        // Re-arm with a fresh policy: the counter restarts at 0.
        let policy2 = Arc::new(FailAll {
            seen: std::sync::Mutex::new(Vec::new()),
        });
        stack.set_fault_policy(Some(policy2.clone()));
        let _ = stack.len();
        assert_eq!(&policy2.seen.lock().unwrap()[..], &[("len", 0)]);
        // Drop `stack` before `_g` removes the backing file: on Windows,
        // deleting a file that's still open typically fails.
        drop(stack);
    }
}
