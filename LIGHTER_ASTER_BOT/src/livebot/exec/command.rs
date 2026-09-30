//! The execution command/event contract. The strategy
//! talks to the execution workers through **bounded command queues** and a shared **event
//! channel** — never by calling an `async` trait per book event. This keeps the hot path
//! single-owner and allocation-light: the strategy `try_send`s a small `Copy`-ish command
//! and moves on; the worker owns the venue client.
//!
//! Prices/quantities on the maker side are carried as scaled integers (`px_ticks`/
//! `qty_lots`); the worker converts to wire `Decimal` using its per-market
//! [`MarketScale`](crate::livebot::scale::MarketScale).

use rust_decimal::Decimal;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};

use crate::livebot::fills::{Admission, HedgeIntent, WireProof};
use crate::livebot::account::Venue;
use crate::livebot::ids::Cloid;
use crate::types::{MarketId, Side};

#[derive(Debug, Default)]
pub struct CommandBarrier {
    completed_ns: AtomicI64,
    wake: tokio::sync::Notify,
}
impl CommandBarrier {
    pub fn complete(&self, now_ns: i64) { self.completed_ns.store(now_ns, Ordering::Release); self.wake.notify_one(); }
    pub fn completed_ns(&self) -> i64 { self.completed_ns.load(Ordering::Acquire) }
}

#[derive(Debug, Default)]
pub struct DrainControl {
    pub quiesced: AtomicBool,
    pub maker_barrier: Arc<CommandBarrier>,
}

/// A queued quote is valid only for the exact published books and risk epoch
/// used to calculate it. The worker claims it after all rate-limit waiting.
#[derive(Clone)]
pub struct MakerPermit {
    pub admission: Arc<Admission>,
    books: Option<[(Arc<crate::hotpath::VenueBook>, u64); 2]>,
    epoch: Arc<AtomicU64>,
    expected_epoch: u64,
    hedge_readiness: Option<super::lighter::HedgeReadiness>,
}
impl std::fmt::Debug for MakerPermit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MakerPermit").field("admission", &self.admission).finish()
    }
}
impl MakerPermit {
    pub fn new(books: [(Arc<crate::hotpath::VenueBook>, u64); 2], epoch: Arc<AtomicU64>, deadline_ns: i64,
        hedge_readiness: Option<super::lighter::HedgeReadiness>) -> Self {
        let expected_epoch = epoch.load(Ordering::Acquire);
        Self { admission: Admission::new(deadline_ns), books: Some(books), epoch, expected_epoch, hedge_readiness }
    }
    pub fn try_claim(&self, now_ns: i64) -> bool {
        let valid = self.epoch.load(Ordering::Acquire) == self.expected_epoch
            && self.hedge_readiness.as_ref().is_none_or(|ready| ready.is_ready())
            && self.books.as_ref().is_none_or(|books| books.iter().all(|(cell, expected)|
                expected % 2 == 0 && cell.content_version() == *expected
                    && !cell.stream_down() && !cell.is_divergent() && !cell.has_hot_only_update()));
        if !valid { self.admission.cancel_queued(); return false; }
        self.admission.try_claim(now_ns)
    }
    pub fn is_cancelled(&self) -> bool { self.admission.is_cancelled() }
    pub fn cancel_queued(&self) -> bool { self.admission.cancel_queued() }
    /// A permit bound to no books, epoch or hedge readiness: only its own admission can lapse.
    /// For probes and tests.
    pub fn unguarded() -> Self {
        Self { admission: Admission::new(i64::MAX), books: None, epoch: Arc::new(AtomicU64::new(0)), expected_epoch: 0, hedge_readiness: None }
    }
}

/// Strategy → Aster execution worker. Maker placement / cancel / amend / safety cancels +
/// the dead-man heartbeat.
#[derive(Debug, Clone)]
pub enum ExecCommand {
    /// Rest a post-only (GTX) maker order.
    Place {
        permit: MakerPermit,
        market: MarketId,
        side: Side,
        price_ticks: i64,
        qty_lots: i64,
        client_id: String,
    },
    /// Cancel a specific resting order.
    Cancel {
        market: MarketId,
        client_id: String,
        venue_order_id: Option<String>,
    },
    /// Move a resting order to a new price and qty in place (Aster PUT `/fapi/v3/order`), one
    /// round trip where cancel-then-place took two. The order keeps its ids; a rejected amend
    /// leaves it resting unchanged.
    Amend {
        permit: MakerPermit,
        market: MarketId,
        side: Side,
        client_id: String,
        price_ticks: i64,
        qty_lots: i64,
    },
    /// Cancel every bot order across all markets (gate close / shutdown).
    CancelAllBot,
    /// Reduce-only taker (MARKET) order to FLATTEN an orphaned Aster position (recovery path):
    /// `side` closes the leg (SELL to close a long, BUY to close a short), `qty` base units.
    /// `client_id` is a session-prefixed id (OrderManager::next_flatten_client_id) so the
    /// resulting reduce-only fill passes the strategy's own-order attribution.
    FlattenAster { intent: HedgeIntent, client_id: String },
    /// Refresh the per-symbol dead-man countdown.
    RefreshDeadman { market: MarketId },
    /// FIFO barrier: all earlier maker commands finished before this timestamp.
    Barrier { completion: Arc<CommandBarrier> },
    /// Drain and stop the worker.
    Shutdown,
}

/// Commands eligible for the exec worker's priority lane (jump ahead of queued
/// places/amends). Safe by construction:
/// - `Cancel` with `venue_order_id: Some(_)`: the id is set ONLY when the strategy has
///   processed the `PlaceAck` (orders.rs is the sole setter), which proves no `Place` for
///   that client id can still be queued — so reordering cannot produce the
///   `-2011 → AlreadyGone → slot cleared → ghost order rests` sequence. Un-acked cancels
///   (`venue_order_id: None`) stay FIFO. An `Amend` it overtakes has lost its permit to that
///   cancel, so the worker sends a DELETE instead, which finds the order gone (`CancelAck`).
/// - `FlattenAster`: reduce-only MARKET with no slot interaction.
/// - `CancelAllBot` must stay FIFO: sweeping ahead of queued `Place`s
///   would let those places rest AFTER the sweep.
pub fn is_priority_cmd(cmd: &ExecCommand) -> bool {
    matches!(
        cmd,
        ExecCommand::Cancel {
            venue_order_id: Some(_),
            ..
        } | ExecCommand::FlattenAster { .. }
    )
}

/// Strategy → Lighter hedge worker.
#[derive(Debug, Clone)]
pub enum HedgeCommand {
    /// Send an aggressive IOC hedge for this intent at `aggressive_px`, which the sender has
    /// already priced with the normal or (for a retry) the wider emergency slippage.
    /// Residual corrections reuse this with a `ReduceDelta` intent.
    Hedge {
        intent: HedgeIntent,
        aggressive_px: Decimal,
    },
    /// Hard-refresh the Lighter nonce: under `run` the taker signs with the same API key, so
    /// XEMM re-reads the nonce after each taker turn. Queued, so later hedges use the new one.
    RefreshNonce,
    /// Drain and stop the worker.
    Shutdown,
}

/// Native per-trade evidence, formatted only by the cold journal writer.
#[derive(Debug, Clone, serde::Serialize)]
pub struct ExecutionTrade {
    pub attempt_id: String,
    pub logical_id: String,
    pub venue: Venue,
    pub market: String,
    pub side: Side,
    pub trade_id: String,
    pub identity_complete: bool,
    pub event_time_ms: Option<i64>,
    pub order_id: Option<String>,
    pub client_order_index: i64,
    pub qty: Decimal,
    pub px: Decimal,
    pub notional_usd: Decimal,
    pub maker: Option<bool>,
    pub fee_ticks: Option<Decimal>,
    pub fee_usd: Option<Decimal>,
}

/// Worker / venue → strategy. Order/hedge lifecycle notifications. Aster
/// maker fills arrive on the user-data stream, not here.
#[derive(Debug, Clone)]
pub enum ExecEvent {
    PlaceAck { client_id: String, venue_order_id: String },
    PlaceReject { client_id: String, reason: String },
    /// The venue refused an amend: the order rests unchanged at its old price and qty.
    AmendReject { client_id: String, reason: String },
    /// Placement outcome is ambiguous: the request may have reached the venue, but
    /// the worker did not receive a definitive response. The strategy must freeze
    /// and sweep/reconcile; it must NOT close the local slot as if this were a reject.
    PlaceUnknown { client_id: String, reason: String },
    CancelAck { client_id: String },
    /// Aster REST quota / overload signal (HTTP 429 or venue code -1003). The strategy freezes
    /// maker quoting and backs off Aster command dispatch briefly.
    AsterRateLimited { reason: String, backoff_ms: i64 },
    /// A cancel that FAILED at the venue (or returned a venue error body). The
    /// order may still be resting — the strategy must NOT close the slot; it freezes + reconciles.
    CancelReject { client_id: String, reason: String },
    /// The cancel found the order FILLED or EXPIRED: it no longer rests, but a fill may not
    /// have reached the strategy yet. The slot closes; the fill is hedged when it arrives.
    CancelFilledOrExpired { client_id: String },
    /// The reconciler's query found no such order (Aster -2013).
    MakerOrderMissing { client_id: String },
    MakerOrderProgress { market: MarketId, side: Side, client_id: String, order_id: String,
        cumulative_qty: Decimal, cumulative_quote_usd: Option<Decimal>, terminal: bool, event_time_ms: i64 },
    AttemptStarted { cloid: Cloid, proof: WireProof },
    AttemptNotSent { cloid: Cloid, reason: String },
    ExecutionProgress { cloid: Cloid, cumulative_qty: Decimal, cumulative_quote_usd: Option<Decimal>, cumulative_fee_usd: Option<Decimal>, terminal: bool, venue_order_id: Option<String>, event_time_ms: Option<i64> },
    HedgeReject { cloid: Cloid, reason: String },
    /// Hedge outcome is ambiguous: the request may have reached Lighter, but
    /// the worker did not receive a definitive response. The strategy must freeze
    /// and reconcile by deterministic cloid/position before any retry.
    HedgeUnknown { cloid: Cloid, reason: String },
    AsterFlattenAck { cloid: Cloid },
    AsterFlattenReject { cloid: Cloid, reason: String, terminal: bool },
}

/// Default bounded depth of each command queue. Deep enough to absorb a quoting burst, small
/// enough that a wedged worker is noticed (a `try_send` failure) rather than growing
/// unbounded.
pub const CMD_QUEUE_DEPTH: usize = 1024;
