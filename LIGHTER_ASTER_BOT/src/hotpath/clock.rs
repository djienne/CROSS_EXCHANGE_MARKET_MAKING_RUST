//! Process-local monotonic clock for freshness, execution ordering and permit deadlines.
//! Wall-clock corrections cannot reverse elapsed time. `OrderBook::local_recv_ts` and
//! journal dates remain UTC; monotonic stamps cannot be compared between processes.

use std::sync::OnceLock;
use std::time::Instant;

/// Nanoseconds since a fixed process-start epoch — monotonic and jump-immune.
/// Only differences between two `mono_now_ns()` values are meaningful (it is not a
/// wall-clock time). `.max(1)` keeps a genuine stamp distinct from the `last == 0`
/// "never stamped" sentinel that `age_ms`/`book_age_ms` rely on.
#[inline]
pub(crate) fn mono_now_ns() -> i64 {
    static EPOCH: OnceLock<Instant> = OnceLock::new();
    let ns = EPOCH.get_or_init(Instant::now).elapsed().as_nanos();
    i64::try_from(ns).unwrap_or(i64::MAX).max(1)
}
