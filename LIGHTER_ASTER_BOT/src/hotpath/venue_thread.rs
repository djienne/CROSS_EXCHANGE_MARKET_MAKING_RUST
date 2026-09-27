//! Per-venue dedicated ingest thread. Each venue's WS reader gets its own OS thread
//! hosting a single-threaded tokio runtime, so Aster ingest latency/jitter is
//! isolated from Lighter's instead of sharing the default multi-thread pool.
//! The reader feeds the lock-free [`VenueBook`] (via the connector `Tap`).

use std::sync::Arc;
use std::thread::{self, JoinHandle};

use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use crate::connectors::{aster, lighter, BookTap, Tap};
use crate::livebot::scale::HotQtyScale;
use crate::types::MarketId;

use super::book_cell::{VenueBook, VenueTag};

/// Spawn one dedicated OS thread running the venue's reconnecting WS reader. The
/// thread exits (so `join()` returns) when `shutdown` is cancelled.
///
/// `ws_url` is the venue's websocket root (Aster) or `/stream` endpoint (Lighter), derived
/// from the configured REST origin. `core_hint` optionally pins the thread to a CPU core
/// (index taken modulo the available cores), see [`maybe_pin_core`].
#[allow(clippy::too_many_arguments)]
pub fn spawn_venue_thread(
    venue: VenueTag,
    ws_url: String,
    symbol: String,
    market: MarketId,
    cell: Arc<VenueBook>,
    reconnect: Arc<Notify>,
    shutdown: CancellationToken,
    core_hint: Option<usize>,
    scale: Option<crate::livebot::scale::MarketScale>,
) -> JoinHandle<()> {
    let name = format!("ingest-{}-{}", venue.as_str(), market.0);
    thread::Builder::new()
        .name(name)
        .spawn(move || {
            maybe_pin_core(core_hint);
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("current-thread ingest runtime");
            let book: Arc<dyn BookTap> = cell;
            let qty_scale = match venue {
                VenueTag::Aster => HotQtyScale::Aster,
                VenueTag::Hedge => HotQtyScale::Hedge,
            };
            let tap = Tap { book: Some(book), reconnect: Some(reconnect), scale, qty_scale };
            rt.block_on(async move {
                tokio::select! {
                    // The reader loops forever (reconnecting); it only returns if the
                    // task is dropped. The shutdown arm is what ends the thread cleanly.
                    _ = run_reader(venue, ws_url, symbol, market, tap) => {}
                    _ = shutdown.cancelled() => {}
                }
            });
        })
        .expect("spawn venue ingest thread")
}

async fn run_reader(
    venue: VenueTag,
    ws_url: String,
    symbol: String,
    market: MarketId,
    tap: Tap,
) {
    match venue {
        VenueTag::Aster => aster::run_with_tap(ws_url, symbol, tap).await,
        VenueTag::Hedge => {
            // Fail LOUDLY on a malformed "market_id:label" symbol: the old fallback of
            // market_id 0 silently subscribed a real (wrong) Lighter market's book, only
            // caught ~90s later by book-check divergence.
            let (market_id, label) = symbol
                .split_once(':')
                .and_then(|(id, label)| Some((id.parse::<u32>().ok()?, label.to_string())))
                .unwrap_or_else(|| {
                    panic!(
                        "malformed Lighter venue symbol {symbol:?} for market {market}: expected \"<market_id>:<label>\""
                    )
                });
            lighter::run_with_tap(ws_url, market_id, label, tap).await
        }
    }
}

/// Pin the current thread to a core (index modulo available cores) via `core_affinity`.
pub fn maybe_pin_core(hint: Option<usize>) {
    if let Some(idx) = hint {
        if let Some(ids) = core_affinity::get_core_ids() {
            if !ids.is_empty() {
                core_affinity::set_for_current(ids[idx % ids.len()]);
            }
        }
    }
}
