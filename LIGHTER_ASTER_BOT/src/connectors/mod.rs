//! Live market-data connectors (Aster, Lighter, Hyperliquid) and one-shot REST spec fetch.

use std::sync::Arc;
use std::time::Duration;

use futures_util::{Sink, SinkExt};
use tokio::sync::Notify;
use tokio_tungstenite::tungstenite::protocol::Message;

use crate::book::{OrderBook, PriceLevel};
use chrono::{DateTime, Utc};

pub mod aster;
pub mod hyperliquid;
pub mod lighter;
pub mod rest_book;
pub mod rest_specs;


/// A sink for the freshest book on the live hot path. Implemented by
/// `hotpath::VenueBook`; the connectors publish through this trait, never the concrete cell.
///
/// `publish` is called on each book snapshot; `touch` on every other inbound frame
/// (trades, pongs) so a quiet-but-alive stream still reads as fresh.
pub trait BookTap: Send + Sync {
    fn publish(&self, book: OrderBook);
    fn touch(&self);
    /// Publish both the raw `OrderBook` and the integer `HotBook`. Default
    /// implementation ignores the hot book.
    fn publish_hot(&self, book: OrderBook, _hot: crate::hot_types::HotBook) {
        self.publish(book);
    }
    /// Publish only the integer L2 projection before the raw Decimal book is ready.
    /// Default just stamps liveness; real hot cells use it for cancel-only prechecks.
    fn publish_hot_only(&self, _hot: crate::hot_types::HotBook, _exch_ts: DateTime<Utc>) {
        self.touch();
    }
    /// Publish a fast one-level best bid/ask assist. Default no-op for taps without a
    /// BBO assist.
    fn publish_bbo(&self, _book: OrderBook) {}
    /// Publish a fast BBO assist plus its integer projection.
    fn publish_bbo_hot(&self, book: OrderBook, _hot: crate::hot_types::HotBook) {
        self.publish_bbo(book);
    }
    /// Publish only the integer BBO projection before the raw Decimal BBO is ready.
    fn publish_bbo_hot_only(&self, _hot: crate::hot_types::HotBook, _exch_ts: DateTime<Utc>) {
        self.touch();
    }
    /// Publish a BBO assist but wake/reprice only when top prices changed. Venues
    /// whose BBO size affects quote safety should keep using `publish_bbo`.
    fn publish_bbo_price_wake(&self, book: OrderBook) {
        self.publish_bbo(book);
    }
    fn publish_bbo_price_wake_hot(&self, book: OrderBook, _hot: crate::hot_types::HotBook) {
        self.publish_bbo_price_wake(book);
    }
    /// Notify the cell that the venue stream is KNOWN down (disconnect/error/gap-resync),
    /// so the stored book must not be trusted until a full snapshot lands. Default no-op.
    fn mark_stream_down(&self) {}
}

/// The hot-path side outputs threaded into a connector reader. The ingest threads set
/// both the cell and the reconnect signal; connector unit tests may leave either `None`.
#[derive(Clone)]
pub struct Tap {
    /// Lock-free latest-book cell to publish into (the live strategy reads it).
    pub book: Option<Arc<dyn BookTap>>,
    /// Edge-triggered "drop and reconnect" signal from the stream watchdog.
    pub reconnect: Option<Arc<Notify>>,
    /// When set, `publish` builds a `HotBook` alongside the raw `OrderBook` and calls
    /// `BookTap::publish_hot` for lock-free integer reads on the strategy loop.
    pub scale: Option<crate::livebot::scale::MarketScale>,
    pub qty_scale: crate::livebot::scale::HotQtyScale,
}

impl Tap {
    /// No hot-path outputs (connector unit tests).
    #[cfg(test)]
    pub fn none() -> Self {
        Tap { book: None, reconnect: None, scale: None, qty_scale: crate::livebot::scale::HotQtyScale::Aster }
    }

    /// Build the integer hot book directly from exchange decimal strings. This avoids
    /// converting websocket levels to `rust_decimal::Decimal` just to convert them back
    /// into ticks/lots for the strategy precheck.
    #[inline]
    pub(crate) fn hot_book_from_raw<'a, I, J>(
        &self,
        bids: I,
        asks: J,
        exch_ts: DateTime<Utc>,
    ) -> Option<(crate::hot_types::HotBook, i64)>
    where
        I: IntoIterator<Item = (&'a str, &'a str)>,
        J: IntoIterator<Item = (&'a str, &'a str)>,
    {
        let scale = self.scale.as_ref()?;
        let t0 = crate::hotpath::clock::mono_now_ns();
        let recv_ns = crate::hotpath::clock::mono_now_ns();
        let mut hot = crate::livebot::scale::build_hot_book_from_strs_with_qty_scale(
            bids,
            asks,
            scale,
            self.qty_scale,
            recv_ns,
            exch_ts.timestamp_millis(),
        );
        hot.source_age_at_recv_ms = crate::hot_types::source_age_at_receive_ms(
            exch_ts.timestamp_millis(), Utc::now().timestamp_millis());
        let done_ns = crate::hotpath::clock::mono_now_ns();
        crate::metrics::BOOK_BUILD.record((done_ns - t0).max(0) as u64);
        Some((hot, recv_ns))
    }

    /// Build the integer hot book from already-parsed Decimal levels (best-first) — the
    /// numeric sibling of [`Tap::hot_book_from_raw`] for connectors that do not
    /// format levels as strings. Same stamps and metric.
    #[inline]
    pub(crate) fn hot_book_from_levels(
        &self,
        bids: &[PriceLevel],
        asks: &[PriceLevel],
        exch_ts: DateTime<Utc>,
    ) -> Option<(crate::hot_types::HotBook, i64)> {
        let scale = self.scale.as_ref()?;
        let t0 = crate::hotpath::clock::mono_now_ns();
        let recv_ns = crate::hotpath::clock::mono_now_ns();
        let mut hot = crate::livebot::scale::build_hot_book_from_dec_levels_with_qty_scale(
            bids,
            asks,
            scale,
            self.qty_scale,
            recv_ns,
            exch_ts.timestamp_millis(),
        );
        hot.source_age_at_recv_ms = crate::hot_types::source_age_at_receive_ms(
            exch_ts.timestamp_millis(), Utc::now().timestamp_millis());
        let done_ns = crate::hotpath::clock::mono_now_ns();
        crate::metrics::BOOK_BUILD.record((done_ns - t0).max(0) as u64);
        Some((hot, recv_ns))
    }

    /// Publish only a prebuilt integer L2 snapshot before the raw Decimal book is built.
    /// The hot cell marks this as cancel-only until the subsequent full publish clears it.
    #[inline]
    pub(crate) fn publish_hot_only(&self, hot: crate::hot_types::HotBook, exch_ts: DateTime<Utc>) {
        if let Some(cell) = &self.book {
            cell.publish_hot_only(hot, exch_ts);
        }
    }

    /// Publish a fresh book, optionally reusing a `HotBook` that was built directly from
    /// the websocket strings before the cold Decimal parse.
    #[inline]
    pub(crate) fn publish_prebuilt(
        &self,
        bids: &[PriceLevel],
        asks: &[PriceLevel],
        exch_ts: DateTime<Utc>,
        prebuilt_hot: Option<(crate::hot_types::HotBook, i64)>,
    ) {
        if let Some(cell) = &self.book {
            let t0 = crate::hotpath::clock::mono_now_ns();
            let book = OrderBook::from_levels(bids.iter().copied(), asks.iter().copied(), exch_ts, Utc::now());
            let recv_ns = prebuilt_hot
                .as_ref()
                .map(|(_, ns)| *ns)
                .unwrap_or_else(crate::hotpath::clock::mono_now_ns);
            if prebuilt_hot.is_none() {
                crate::metrics::BOOK_BUILD.record((recv_ns - t0).max(0) as u64);
            }
            if let Some(scale) = &self.scale {
                let hot = prebuilt_hot
                    .map(|(hot, _)| hot)
                    .unwrap_or_else(|| {
                        crate::livebot::scale::build_hot_book_with_qty_scale(
                            &book,
                            scale,
                            self.qty_scale,
                            recv_ns,
                        )
                    });
                cell.publish_hot(book, hot);
                return;
            }
            cell.publish(book);
        }
    }

    /// Publish only a prebuilt integer BBO before the raw Decimal BBO is built.
    #[inline]
    pub(crate) fn publish_bbo_hot_only(&self, hot: crate::hot_types::HotBook, exch_ts: DateTime<Utc>) {
        if let Some(cell) = &self.book {
            cell.publish_bbo_hot_only(hot, exch_ts);
        }
    }

    /// Publish a BBO assist, optionally reusing an already-built integer projection.
    #[inline]
    pub(crate) fn publish_bbo_prebuilt(
        &self,
        bid: PriceLevel,
        ask: PriceLevel,
        exch_ts: DateTime<Utc>,
        prebuilt_hot: Option<(crate::hot_types::HotBook, i64)>,
    ) {
        if let Some(cell) = &self.book {
            let t0 = crate::hotpath::clock::mono_now_ns();
            let book = OrderBook::from_levels([bid], [ask], exch_ts, Utc::now());
            let recv_ns = prebuilt_hot
                .as_ref()
                .map(|(_, ns)| *ns)
                .unwrap_or_else(crate::hotpath::clock::mono_now_ns);
            if prebuilt_hot.is_none() {
                crate::metrics::BOOK_BUILD.record((recv_ns - t0).max(0) as u64);
            }
            if let Some(scale) = &self.scale {
                let hot = prebuilt_hot
                    .map(|(hot, _)| hot)
                    .unwrap_or_else(|| {
                        crate::livebot::scale::build_hot_book_with_qty_scale(
                            &book,
                            scale,
                            self.qty_scale,
                            recv_ns,
                        )
                    });
                cell.publish_bbo_hot(book, hot);
                return;
            }
            cell.publish_bbo(book);
        }
    }

    /// Publish a BBO assist with a COALESCED wake: data + freshness always stored, the
    /// strategy woken only when the top prices changed (or a hot-only publish was
    /// pending). For a BBO that mirrors an L2 frame whose own publish already woke the
    /// strategy, the unconditional wake of `publish_bbo_prebuilt` is pure overhead.
    #[inline]
    pub(crate) fn publish_bbo_price_wake_prebuilt(
        &self,
        bid: PriceLevel,
        ask: PriceLevel,
        exch_ts: DateTime<Utc>,
        prebuilt_hot: Option<(crate::hot_types::HotBook, i64)>,
    ) {
        if let Some(cell) = &self.book {
            let book = OrderBook::from_levels([bid], [ask], exch_ts, Utc::now());
            if let Some(scale) = &self.scale {
                let recv_ns = prebuilt_hot
                    .as_ref()
                    .map(|(_, ns)| *ns)
                    .unwrap_or_else(crate::hotpath::clock::mono_now_ns);
                let hot = prebuilt_hot.map(|(hot, _)| hot).unwrap_or_else(|| {
                    crate::livebot::scale::build_hot_book_with_qty_scale(
                        &book,
                        scale,
                        self.qty_scale,
                        recv_ns,
                    )
                });
                cell.publish_bbo_price_wake_hot(book, hot);
                return;
            }
            cell.publish_bbo_price_wake(book);
        }
    }

    /// Stamp liveness on any inbound frame.
    #[inline]
    fn touch(&self) {
        if let Some(cell) = &self.book {
            cell.touch();
        }
    }

    /// Flag the attached book cell as stream-down (no-op without a cell).
    #[inline]
    pub(crate) fn mark_stream_down(&self) {
        if let Some(cell) = &self.book {
            cell.mark_stream_down();
        }
    }

    /// Await a watchdog reconnect request. When no signal is attached the returned
    /// future never resolves, so its `select!` arm never fires.
    async fn wait_reconnect(&self) {
        match &self.reconnect {
            Some(n) => n.notified().await,
            None => std::future::pending::<()>().await,
        }
    }
}

/// How long a keepalive/control write may block before we treat the socket's write
/// side as wedged. A healthy send completes in well under a millisecond; this only
/// fires on a genuinely stalled sink.
pub(crate) const WRITE_TIMEOUT: Duration = Duration::from_secs(5);

/// `connect_async` with a stall guard: a TCP, TLS or upgrade step that hangs must end the
/// attempt so the caller's reconnect loop runs, not hold the feed dark with no reader to wake.
pub(crate) async fn connect_guarded(
    url: &str,
) -> anyhow::Result<tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>> {
    match tokio::time::timeout(CONNECT_TIMEOUT, tokio_tungstenite::connect_async(url)).await {
        Ok(res) => Ok(res?.0),
        Err(_) => anyhow::bail!("ws connect stalled >{}s", CONNECT_TIMEOUT.as_secs()),
    }
}
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// Send a control/keepalive frame (Pong, subscribe, ping) with a stall guard.
/// Awaiting `write.send(...)` directly inside the reader's `select!` means a wedged
/// write side would starve the idle-timeout and reconnect arms; bounding the send
/// lets the reader bail and self-heal instead of hanging until the OS socket timeout.
pub(crate) async fn send_guarded<S>(write: &mut S, msg: Message) -> anyhow::Result<()>
where
    S: Sink<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin,
{
    match tokio::time::timeout(WRITE_TIMEOUT, write.send(msg)).await {
        Ok(res) => res.map_err(anyhow::Error::from),
        Err(_) => anyhow::bail!("ws write stalled >{}s, forcing reconnect", WRITE_TIMEOUT.as_secs()),
    }
}
