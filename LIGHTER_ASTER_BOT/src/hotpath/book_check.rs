//! Periodic REST cross-check of the websocket-built books (the "are my order books
//! correct?" reconciler). A dedicated thread wakes every `interval` (default ~30 s,
//! deliberately slow / non-invasive) and, for each (market, venue) cell, loads the
//! latest WS book and fetches the matching REST snapshot, comparing top-of-book mids.
//!
//! A book that is internally **crossed**, or whose mid stays beyond `tolerance_bps`
//! of REST for `consecutive_breaches` scans in a row, is treated as STUCK/CORRUPT:
//! the cell is flagged [`VenueBook::mark_divergent`] (so the [`super::watchdog`]
//! closes the trading gate) and its reader is asked to drop and reconnect ("reset the
//! websocket"). A passing scan clears the flag. Single transient blips don't act —
//! only sustained disagreement does — so normal REST/WS timing skew never trips it.
//!
//! This is cold-path reconciliation: slow, off the quote hot loop, and the only
//! thing it mutates is the per-cell divergence flag + reconnect nudges.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use futures_util::stream::{self, StreamExt};
use rust_decimal::Decimal;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::book::OrderBook;
use crate::connectors::rest_book::{self, BookComparison};
use crate::types::MarketId;

use super::book_cell::{VenueBook, VenueTag};
use super::registry::VenueRegistry;
use super::watchdog::ReconnectHandle;

/// One thing to cross-check: a cell key plus the market it reads over REST.
#[derive(Debug, Clone)]
pub struct BookCheckTarget {
    pub market: MarketId,
    pub venue: VenueTag,
    pub spec: crate::markets::MarketSpec,
}

/// Tunables for the cross-check, resolved from `[book_check]` config.
#[derive(Debug, Clone)]
pub struct BookCheckParams {
    pub tolerance_bps: Decimal,
    pub consecutive_breaches: u32,
    pub depth_limit: u32,
    pub interval: Duration,
    /// Only compare a BBO assist to REST while it is fresh enough to be used by the
    /// strategy quote path. Older BBO is ignored; the watchdog owns staleness gating.
    pub max_quote_staleness_ms: i64,
    /// Bound REST fan-out per scan. This narrows the market-time window versus fully
    /// sequential scans without creating a REST stampede for large market lists.
    pub max_concurrent_requests: usize,
    /// Skip a REST snapshot whose exchange timestamp is already stale. A stale REST
    /// snapshot is a bad comparator and can otherwise create false divergence in volatility.
    pub max_rest_snapshot_age_ms: i64,
    pub live: crate::config::LiveCfg,
}

/// The cross-check thread body. Runs on its own OS thread (a current-thread tokio
/// runtime, because the REST fetch is async) until `shutdown` is cancelled. The first
/// scan is one `interval` after start, giving the feeds time to populate.
pub fn run_book_check(
    reg: Arc<VenueRegistry>,
    targets: Vec<BookCheckTarget>,
    reconnect: HashMap<(MarketId, VenueTag), ReconnectHandle>,
    params: BookCheckParams,
    shutdown: CancellationToken,
) {
    let rt = match tokio::runtime::Builder::new_current_thread().enable_all().build() {
        Ok(rt) => rt,
        Err(e) => {
            error!("book-check: failed to build runtime: {e}");
            return;
        }
    };
    rt.block_on(async move {
        let client = match rest_book::client() {
            Ok(c) => c,
            Err(e) => {
                error!("book-check: http client: {e:#}");
                return;
            }
        };
        info!(
            "book-check: REST cross-check every {}s (tolerance {}bps, {} consecutive breaches to act, max {} concurrent)",
            params.interval.as_secs(),
            params.tolerance_bps,
            params.consecutive_breaches,
            params.max_concurrent_requests.max(1)
        );
        // Consecutive-breach counter per cell (local; not shared).
        let mut breaches: HashMap<(MarketId, VenueTag), u32> = HashMap::new();
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => break,
                _ = tokio::time::sleep(params.interval) => {}
            }
            if shutdown.is_cancelled() {
                break;
            }
            scan_all(&reg, &targets, &client, &reconnect, params.clone(), &mut breaches).await;
        }
        debug!("book-check: stopped");
    });
}

struct FetchedBook {
    target: BookCheckTarget,
    key: (MarketId, VenueTag),
    cell: Arc<VenueBook>,
    ws_book: OrderBook,
    rest_book: OrderBook,
}

async fn fetch_one(
    target: BookCheckTarget,
    cell: Arc<VenueBook>,
    client: &reqwest::Client,
    params: BookCheckParams,
) -> Option<FetchedBook> {
    let key = (target.market.clone(), target.venue);
    let rest = match target.venue {
        VenueTag::Aster => {
            let symbol = target.spec.aster_symbol.to_uppercase();
            rest_book::fetch_aster_book_from_base(client, &params.live.aster.base_url, &symbol, params.depth_limit).await
        }
        VenueTag::Hedge => rest_book::fetch_hedge_book(client, &params.live, &target.spec, params.depth_limit).await,
    };
    let rest_book = match rest {
        Ok(b) => b,
        Err(e) => {
            // Our own REST hiccup — never penalize the WS feed for it.
            warn!("book-check: REST fetch failed for {} {}: {e:#}", target.market.0, target.venue.as_str());
            return None;
        }
    };

    let rest_age_ms = (Utc::now() - rest_book.exch_ts).num_milliseconds().max(0);
    if rest_age_ms > params.max_rest_snapshot_age_ms {
        warn!(
            "book-check: REST snapshot too old for {} {} ({}ms > {}ms); skipping comparator",
            target.market.0,
            target.venue.as_str(),
            rest_age_ms,
            params.max_rest_snapshot_age_ms
        );
        return None;
    }

    // Load the latest WS book AFTER the REST snapshot returns to compare against the freshest
    // locally-built book and keep the REST/WS comparison window tight.
    let ws_arc = cell.load();
    let Some(ws_book) = ws_arc.as_deref().cloned() else { return None };

    Some(FetchedBook { target, key, cell, ws_book, rest_book })
}

/// Cross-check every target once. Updates the per-cell breach counters and the
/// divergence flags, requesting a reconnect for any cell that crosses the breach
/// threshold. Logs a one-line summary at `info`, per-cell detail at `debug`.
async fn scan_all(
    reg: &VenueRegistry,
    targets: &[BookCheckTarget],
    client: &reqwest::Client,
    reconnect: &HashMap<(MarketId, VenueTag), ReconnectHandle>,
    params: BookCheckParams,
    breaches: &mut HashMap<(MarketId, VenueTag), u32>,
) {
    let mut checked = 0u32;
    let mut agreed = 0u32;
    let mut worst: Option<(Decimal, String)> = None;

    let jobs = targets.iter().filter_map(|t| reg.cell(&t.market, t.venue).map(|cell| (t.clone(), cell)));
    let max_concurrent = params.max_concurrent_requests.max(1);
    let mut fetched = stream::iter(jobs)
        .map(|(target, cell)| {
            let params = params.clone();
            async move { fetch_one(target, cell, client, params).await }
        })
        .buffer_unordered(max_concurrent);

    while let Some(item) = fetched.next().await {
        let Some(FetchedBook { target: t, key, cell, ws_book, rest_book }) = item else { continue };

        checked += 1;
        // Probe a fixed notional ($2000) of hedge depth, converted at the REST mid, so a
        // feed whose top-of-book is right but whose deeper levels are stale or malformed
        // is still caught. A thin book that cannot fill it skips the VWAP check.
        let vwap_size = rest_book
            .mid()
            .filter(|m| *m > Decimal::ZERO)
            .map(|m| Decimal::from(2000) / m)
            .unwrap_or(Decimal::ZERO);
        let cmp = BookComparison::compute(&ws_book, &rest_book, vwap_size);
        let now_ns = super::clock::mono_now_ns();
        let bbo_arc = cell.load_bbo();
        let bbo_cmp = if cell.bbo_age_ms(now_ns) <= params.max_quote_staleness_ms {
            bbo_arc
                .as_deref()
                // The BBO feed is independent from L2. Do not compare a locally-fresh but
                // exchange-older BBO against REST when a newer L2 snapshot is already installed;
                // that is normal cross-feed skew, not a corrupt/stuck book.
                .filter(|bbo| bbo.exch_ts >= ws_book.exch_ts)
                .map(|bbo| BookComparison::compute(bbo, &rest_book, Decimal::ZERO))
        } else {
            None
        };

        let bbo_divergent = bbo_cmp.as_ref().is_some_and(|c| {
            c.ws_crossed
                || (c.ws_mid.is_none() && c.rest_mid.is_some())
                || c.mid_diff_bps.is_some_and(|d| d > params.tolerance_bps)
        });

        if let Some(d) = cmp.mid_diff_bps {
            if worst.as_ref().is_none_or(|(w, _)| d > *w) {
                worst = Some((d, format!("{}/{}", t.market.0, t.venue.as_str())));
            }
        }

        // Divergent if: the WS book is crossed; or the WS book has no mid while REST
        // does (a broken/empty WS book); or the mids differ beyond tolerance.
        let divergent = cmp.ws_crossed
            || (cmp.ws_mid.is_none() && cmp.rest_mid.is_some())
            || cmp.mid_diff_bps.is_some_and(|d| d > params.tolerance_bps)
            || cmp.vwap_diff_bps.is_some_and(|d| d > params.tolerance_bps)
            || bbo_divergent;

        if divergent {
            let n = breaches.entry(key.clone()).or_insert(0);
            *n = step_breach_counter(*n, true, cell.is_divergent(), params.consecutive_breaches);
            warn!(
                "book-check: {} {} WS/BBO!=REST (ws_mid={:?} rest_mid={:?} mid_diff={:?}bps vwap_diff={:?}bps bbo_diff={:?}bps ws_crossed={} bbo_crossed={:?}) breach {}/{}",
                t.market.0,
                t.venue.as_str(),
                cmp.ws_mid,
                cmp.rest_mid,
                cmp.mid_diff_bps.map(|d| d.round_dp(2)),
                cmp.vwap_diff_bps.map(|d| d.round_dp(2)),
                bbo_cmp.as_ref().and_then(|c| c.mid_diff_bps).map(|d| d.round_dp(2)),
                cmp.ws_crossed,
                bbo_cmp.as_ref().map(|c| c.ws_crossed),
                *n,
                params.consecutive_breaches
            );
            if *n >= params.consecutive_breaches {
                let already_divergent = cell.is_divergent();
                cell.mark_divergent(true);
                if !already_divergent {
                    if let Some(h) = reconnect.get(&key) {
                        h.request();
                    }
                    error!(
                        "book-check: {} {} sustained divergence -> trading gate CLOSED + websocket reset requested",
                        t.market.0,
                        t.venue.as_str()
                    );
                }
            }
        } else {
            agreed += 1;
            let n = breaches.entry(key.clone()).or_insert(0);
            *n = step_breach_counter(*n, false, cell.is_divergent(), params.consecutive_breaches);
            if *n == 0 {
                if cell.is_divergent() {
                    info!(
                        "book-check: {} {} back in sustained agreement with REST -> divergence cleared, gate may reopen",
                        t.market.0,
                        t.venue.as_str()
                    );
                }
                cell.mark_divergent(false);
            } else if cell.is_divergent() {
                debug!(
                    "book-check: {} {} agreeing but waiting for sustained recovery ({}/{} remaining)",
                    t.market.0,
                    t.venue.as_str(),
                    *n,
                    params.consecutive_breaches
                );
            }
            debug!(
                "book-check: {} {} ok (diff={}bps)",
                t.market.0,
                t.venue.as_str(),
                cmp.mid_diff_bps.map(|d| d.round_dp(2)).unwrap_or_default()
            );
        }
    }

    let worst_s = worst
        .map(|(d, who)| format!("{}bps@{who}", d.round_dp(2)))
        .unwrap_or_else(|| "n/a".to_string());
    info!("book-check: {agreed}/{checked} books agree with REST (worst {worst_s})");
}

/// Fold one scan observation into a (market, venue) breach counter.
///
/// `limit` CONSECUTIVE divergent scans trip the gate (the counter saturates there); once
/// tripped, `limit` consecutive agreeing scans are required before it clears (gradual
/// decay, so a flapping book stays gated). A healthy (non-tripped) cell resets to zero on
/// any agreeing scan — isolated, non-consecutive blips must never accumulate toward a trip.
fn step_breach_counter(n: u32, divergent: bool, tripped: bool, limit: u32) -> u32 {
    if divergent {
        n.saturating_add(1).min(limit.max(1))
    } else if tripped {
        n.saturating_sub(1)
    } else {
        0
    }
}

#[cfg(test)]
mod tests {
    use super::step_breach_counter;

    const LIMIT: u32 = 3;

    #[test]
    fn isolated_blips_never_accumulate() {
        // Repeated bursts of LIMIT-1 breaches, each followed by one agreement on a
        // healthy cell, must never reach the trip threshold.
        let mut n = 0;
        for _ in 0..10 {
            n = step_breach_counter(n, true, false, LIMIT);
            n = step_breach_counter(n, true, false, LIMIT);
            assert!(n < LIMIT);
            n = step_breach_counter(n, false, false, LIMIT);
            assert_eq!(n, 0);
        }
    }

    #[test]
    fn consecutive_breaches_trip_and_saturate() {
        let mut n = 0;
        for _ in 0..LIMIT {
            n = step_breach_counter(n, true, false, LIMIT);
        }
        assert_eq!(n, LIMIT);
        // Further breaches hold at the limit so recovery always needs LIMIT agreements.
        n = step_breach_counter(n, true, true, LIMIT);
        assert_eq!(n, LIMIT);
    }

    #[test]
    fn tripped_cell_requires_sustained_agreement() {
        let mut n = LIMIT;
        n = step_breach_counter(n, false, true, LIMIT);
        assert_eq!(n, LIMIT - 1);
        // A relapse mid-recovery restores the full requirement.
        n = step_breach_counter(n, true, true, LIMIT);
        assert_eq!(n, LIMIT);
        for _ in 0..LIMIT {
            n = step_breach_counter(n, false, true, LIMIT);
        }
        assert_eq!(n, 0);
    }
}
