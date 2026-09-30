//! Stream-staleness watchdog. A dedicated OS thread scans every [`super::book_cell::VenueBook`]'s
//! liveness stamp; when a stream goes silent (a half-open socket that never sent a
//! Close frame) it (a) asks that reader to drop and reconnect via a lock-free
//! [`ReconnectHandle`], and (b) closes a [`TradingGate`], which only drives the
//! OPEN/CLOSED log (the strategy gates each market on its own feed freshness).
//! Everything here is lock-free atomics + an edge-triggered `Notify`; no mutexes on
//! the read side.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Notify;
use tracing::{debug, info, warn};

use crate::types::MarketId;

use super::book_cell::VenueTag;
use super::clock::mono_now_ns;
use super::registry::VenueRegistry;

/// Lock-free GLOBAL "every book is fresh enough to trade" flag. The watchdog sets it and
/// logs OPEN↔CLOSED transitions as an observability *gauge*. It is NOT the livebot's quoting
/// enforcement input: the strategy gates each market off ITS OWN feed freshness
/// ([`crate::livebot`]'s `Strategy::market_feeds_fresh`), so one stale low-liquidity feed
/// can't halt quoting on every pair.
pub struct TradingGate(AtomicBool);

impl Default for TradingGate {
    fn default() -> Self {
        TradingGate(AtomicBool::new(true))
    }
}

impl TradingGate {
    pub fn new() -> Self {
        Self::default()
    }
    #[inline]
    pub fn block(&self) {
        self.0.store(false, Ordering::Release);
    }
    #[inline]
    pub fn allow(&self) {
        self.0.store(true, Ordering::Release);
    }
    #[inline]
    pub fn is_open(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }
}

/// A reconnect signal from the watchdog to one reader. `Notify::notify_one` retains one
/// permit between waits, so a request during a reconnect is consumed by the reader's next
/// [`ReconnectHandle::notify`]. Repeated requests coalesce. The watchdog retries after
/// 250 ms, and the reader also has an idle timeout.
#[derive(Clone, Default)]
pub struct ReconnectHandle {
    notify: Arc<Notify>,
}

impl ReconnectHandle {
    pub fn new() -> Self {
        Self::default()
    }
    /// Ask the reader to reconnect. `notify_one` stores a permit if the reader is not
    /// currently parked, closing the race where a request between two waits would
    /// otherwise be dropped.
    pub fn request(&self) {
        self.notify.notify_one();
    }
    /// The shared `Notify` to hand to the connector's `Tap` so the reader parks on
    /// the same signal this handle triggers.
    pub fn notify(&self) -> Arc<Notify> {
        self.notify.clone()
    }
}

/// One health scan. Sets the trading gate (open only when every cell is BOTH fresh
/// and not REST-divergent) and returns the keys to nudge for reconnect. Pure (no
/// sleeping / threads), so it is unit-testable.
///
/// Three independent ways a cell is unhealthy:
/// - **Connection-stale** (`age_ms > conn_stale_ms`): no frames of any kind arriving. A cell
///   that was alive and went silent is added to the reconnect list; one that has
///   **never** produced a frame (`last_msg_ns == 0`) is NOT — that is the connector's
///   own connect + in-reader idle-timeout job, and forcing a reconnect mid-handshake
///   would starve it. The watchdog only reconnects a feed that was alive then went quiet.
/// - **Book-stale** (`book_age_ms > book_stale_ms` while frames still arrive): the socket
///   is alive but the ORDER BOOK / BBO quote touch is stale (e.g. a feed streaming only
///   trades/pongs, or one that has not published market data yet). `book_stale_ms` is the
///   tighter trading threshold (`max_book_staleness_ms`, 10 s by default), NOT the 60 s
///   connection threshold.
///   A fast BBO update (Aster `bookTicker`) also counts as a fresh touch, so this check uses
///   `quote_age_ms` rather than full-depth `book_age_ms`.
/// - **Divergent** (`is_divergent()`): frames keep arriving but the book content
///   disagrees with a REST snapshot (set by [`super::book_check`]). This closes the
///   gate but is NOT reconnected here — the slow REST checker owns that reconnect, so
///   a persistently-divergent cell isn't hammered every 250 ms scan.
pub fn scan_once(
    reg: &VenueRegistry,
    gate: &TradingGate,
    conn_stale_ms: i64,
    book_stale_ms: i64,
    now_ns: i64,
) -> Vec<(MarketId, VenueTag)> {
    let mut reconnect = Vec::new();
    let mut all_healthy = true;
    for (key, cell) in reg.iter() {
        if cell.age_ms(now_ns) > conn_stale_ms {
            // No frames at all: connection is silent. Close the gate; reconnect only a
            // feed that was alive and went quiet (never-alive cells are the connector's).
            // A feed already known down is reconnecting on its own: a request stored now
            // would end its next session before the first frame, every time.
            all_healthy = false;
            if cell.last_msg_ns() != 0 && !cell.stream_down() {
                reconnect.push(key.clone());
            }
        } else if cell.quote_age_ms(now_ns) > book_stale_ms {
            // Frames ARE arriving (socket alive — no reconnect) but quote-touch data has
            // gone stale, or none was ever published. We can't trust a hedge priced off it,
            // so close the gate while leaving the healthy socket alone.
            all_healthy = false;
        }
        if cell.is_divergent() {
            all_healthy = false;
        }
    }
    if all_healthy {
        gate.allow();
    } else {
        gate.block();
    }
    reconnect
}

/// The watchdog thread body. Runs until `stop` is set. Scans every `scan_interval`,
/// nudges stale readers to reconnect, and logs gate open↔closed transitions (the
/// per-scan reconnect nudges are at `debug` to avoid spamming).
pub fn run_watchdog(
    reg: Arc<VenueRegistry>,
    gate: Arc<TradingGate>,
    reconnect: HashMap<(MarketId, VenueTag), ReconnectHandle>,
    conn_stale_ms: i64,
    book_stale_ms: i64,
    scan_interval: Duration,
    stop: Arc<AtomicBool>,
) {
    let mut was_open = gate.is_open();
    while !stop.load(Ordering::Acquire) {
        let now_ns = mono_now_ns();
        let to_reconnect = scan_once(&reg, &gate, conn_stale_ms, book_stale_ms, now_ns);
        for key in &to_reconnect {
            if let Some(h) = reconnect.get(key) {
                h.request();
            }
            debug!("watchdog: {} {} went silent -> reconnect requested", key.0 .0, key.1.as_str());
        }
        let open = gate.is_open();
        if open != was_open {
            if open {
                info!("watchdog: feeds fresh — trading gate OPEN");
            } else {
                warn!("watchdog: a feed is not fresh — global trading gate CLOSED (gauge only; quoting gates per-market)");
            }
            was_open = open;
        }
        std::thread::sleep(scan_interval);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use crate::book::OrderBook;
    use rust_decimal_macros::dec;

    // A minimal valid book to publish onto a cell (1 bid / 1 ask).
    fn bk() -> OrderBook {
        let now = Utc::now();
        OrderBook::from_levels(vec![(dec!(100), dec!(1))], vec![(dec!(101), dec!(1))], now, now)
    }

    #[test]
    fn gate_open_close() {
        let g = TradingGate::new();
        assert!(g.is_open());
        g.block();
        assert!(!g.is_open());
        g.allow();
        assert!(g.is_open());
    }

    #[tokio::test]
    async fn reconnect_handle_wakes_waiter() {
        let h = ReconnectHandle::new();
        let h2 = h.clone();
        let waiter = tokio::spawn(async move { h2.notify().notified().await });
        // Give the waiter a moment to park, then request.
        tokio::task::yield_now().await;
        h.request();
        // The waiter resolves (request delivered).
        tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .expect("waiter should wake")
            .unwrap();
    }

    #[tokio::test]
    async fn reconnect_request_is_buffered_when_no_waiter_parked() {
        // The race the notify_one fix closes: request() fires while the reader is NOT
        // parked (mid-frame/mid-reconnect). The stored permit makes the next
        // wait resolve immediately, rather than dropping the request (which
        // notify_waiters would do, blocking the wait until the timeout below).
        let h = ReconnectHandle::new();
        h.request(); // nobody is awaiting yet
        tokio::time::timeout(Duration::from_secs(1), h.notify().notified())
            .await
            .expect("a request with no parked reader must be buffered and delivered next");
    }

    #[test]
    fn scan_flags_stale_and_sets_gate() {
        let reg = VenueRegistry::new(&["BTC".into()]);
        let gate = TradingGate::new();
        let now_ns = mono_now_ns();

        // Nothing published yet => gate closed, but NO reconnect (never-alive cells
        // are the connector's own connect job, not the watchdog's).
        let reconnect = scan_once(&reg, &gate, 1_000, 1_000, now_ns);
        assert!(reconnect.is_empty());
        assert!(!gate.is_open());

        // Publish a book on both => both stamps fresh => gate opens, none to reconnect.
        reg.cell(&"BTC".into(), VenueTag::Aster).unwrap().publish(bk());
        reg.cell(&"BTC".into(), VenueTag::Hedge).unwrap().publish(bk());
        let now2 = mono_now_ns();
        let reconnect = scan_once(&reg, &gate, 1_000, 1_000, now2);
        assert!(reconnect.is_empty());
        assert!(gate.is_open());

        // Far-future scan => both were-alive cells went silent => reconnect both,
        // gate closes.
        let future = now2 + 10_000_000_000; // +10s
        let reconnect = scan_once(&reg, &gate, 1_000, 1_000, future);
        assert_eq!(reconnect.len(), 2);
        assert!(!gate.is_open());

        // Once a connector has seen its stream drop, it reconnects by itself: no request.
        reg.cell(&"BTC".into(), VenueTag::Aster).unwrap().mark_stream_down();
        assert_eq!(scan_once(&reg, &gate, 1_000, 1_000, future).len(), 1);
    }

    #[test]
    fn divergent_cell_closes_gate_without_reconnect() {
        let reg = VenueRegistry::new(&["BTC".into()]);
        let gate = TradingGate::new();
        reg.cell(&"BTC".into(), VenueTag::Aster).unwrap().publish(bk());
        reg.cell(&"BTC".into(), VenueTag::Hedge).unwrap().publish(bk());
        let now = mono_now_ns();

        // Both fresh and not divergent => gate open, nothing to reconnect.
        assert!(scan_once(&reg, &gate, 60_000, 60_000, now).is_empty());
        assert!(gate.is_open());

        // Flag one fresh cell divergent: the gate closes, but the staleness scan does
        // NOT reconnect it (the REST checker issues that reconnect itself).
        reg.cell(&"BTC".into(), VenueTag::Aster).unwrap().mark_divergent(true);
        let reconnect = scan_once(&reg, &gate, 60_000, 60_000, now);
        assert!(reconnect.is_empty());
        assert!(!gate.is_open());

        // Clearing it reopens the gate.
        reg.cell(&"BTC".into(), VenueTag::Aster).unwrap().mark_divergent(false);
        assert!(scan_once(&reg, &gate, 60_000, 60_000, now).is_empty());
        assert!(gate.is_open());
    }

    #[test]
    fn live_frames_but_no_book_keeps_gate_closed() {
        // The Fix-2 scenario: a feed whose socket is alive (frames keep arriving) but
        // that has not published a usable book. Connection liveness must NOT be mistaken
        // for trading-data freshness.
        let reg = VenueRegistry::new(&["BTC".into()]);
        let gate = TradingGate::new();
        reg.cell(&"BTC".into(), VenueTag::Aster).unwrap().touch();
        reg.cell(&"BTC".into(), VenueTag::Hedge).unwrap().touch();
        let now = mono_now_ns();
        // Live connection => NO reconnect, but no book => gate CLOSED.
        let reconnect = scan_once(&reg, &gate, 60_000, 60_000, now);
        assert!(reconnect.is_empty());
        assert!(!gate.is_open());
        // Once books publish on both venues, the gate opens.
        reg.cell(&"BTC".into(), VenueTag::Aster).unwrap().publish(bk());
        reg.cell(&"BTC".into(), VenueTag::Hedge).unwrap().publish(bk());
        let now2 = mono_now_ns();
        assert!(scan_once(&reg, &gate, 60_000, 60_000, now2).is_empty());
        assert!(gate.is_open());
    }

    #[test]
    fn book_stale_but_conn_fresh_closes_gate_without_reconnect() {
        // The Part-3 split: with distinct thresholds (conn 60s / book 5s), a book that
        // has gone stale (> book_stale) while the socket is still live (< conn_stale)
        // must CLOSE the gate (untrustworthy for quoting) WITHOUT forcing a reconnect —
        // the socket is healthy, only the data is old.
        let reg = VenueRegistry::new(&["BTC".into()]);
        let gate = TradingGate::new();
        reg.cell(&"BTC".into(), VenueTag::Aster).unwrap().publish(bk());
        reg.cell(&"BTC".into(), VenueTag::Hedge).unwrap().publish(bk());
        let now = mono_now_ns();

        // Fresh on both axes => gate open, nothing to reconnect.
        assert!(scan_once(&reg, &gate, 60_000, 5_000, now).is_empty());
        assert!(gate.is_open());

        // +6s: books are 6s old (> 5s book-stale) but the connection is fresh (< 60s
        // conn-stale) => gate CLOSES, but NO reconnect (the socket itself is fine).
        let book_stale = now + 6_000_000_000; // +6s
        let reconnect = scan_once(&reg, &gate, 60_000, 5_000, book_stale);
        assert!(reconnect.is_empty());
        assert!(!gate.is_open());

        // +70s: now even the connection is stale (> 60s) => reconnect both.
        let conn_stale = now + 70_000_000_000; // +70s
        let reconnect = scan_once(&reg, &gate, 60_000, 5_000, conn_stale);
        assert_eq!(reconnect.len(), 2);
        assert!(!gate.is_open());
    }
}
