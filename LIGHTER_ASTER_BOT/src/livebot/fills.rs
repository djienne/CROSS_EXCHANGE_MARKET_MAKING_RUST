//! Aster fill deduplication and hedge-attempt state machine.
//!
//! Repeated events dedup by `(order_id, trade_id)`, with cumulative quantity as fallback.
//! The strategy also tracks credited quantity and accumulates sub-minimum fills. Each hedge
//! attempt retains its identity; an unknown outcome needs terminal evidence before retry.

use std::collections::HashSet;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;

use rust_decimal::Decimal;

use crate::types::{MarketId, Side};

use super::ids::Cloid;
use super::account::Venue;

const QUEUED: u8 = 0;
const CLAIMED: u8 = 1;
const CANCELLED: u8 = 2;

/// One atomic admission decision shared by the strategy and execution worker.
/// A cancelled queued request can never subsequently reserve a nonce or write.
#[derive(Debug)]
pub struct Admission {
    state: AtomicU8,
    pub deadline_ns: i64,
}

impl Admission {
    pub fn new(deadline_ns: i64) -> Arc<Self> {
        Arc::new(Self { state: AtomicU8::new(QUEUED), deadline_ns })
    }

    pub fn try_claim(&self, now_ns: i64) -> bool {
        if now_ns >= self.deadline_ns {
            self.cancel_queued();
            return false;
        }
        self.state.compare_exchange(QUEUED, CLAIMED, Ordering::AcqRel, Ordering::Acquire).is_ok()
    }

    pub fn cancel_queued(&self) -> bool {
        self.state.compare_exchange(QUEUED, CANCELLED, Ordering::AcqRel, Ordering::Acquire).is_ok()
    }

    pub fn is_claimed(&self) -> bool {
        self.state.load(Ordering::Acquire) == CLAIMED
    }

    pub fn is_cancelled(&self) -> bool {
        self.state.load(Ordering::Acquire) == CANCELLED
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IntentPurpose {
    Hedge,
    ReduceDelta,
}

#[derive(Debug, Clone)]
pub struct WireProof {
    pub tx_hash: Option<String>,
    pub nonce: Option<i64>,
    pub client_order_index: Option<i64>,
    pub sent_ns: i64,
}

/// A parsed Aster maker fill (from `ORDER_TRADE_UPDATE` with `x = TRADE`). Field names
/// mirror the venue: `z` cumulative filled, `l` last filled qty, `L` last filled price.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AsterFill {
    pub market: MarketId,
    /// Aster maker side that filled (the hedge is the opposite side).
    pub aster_side: Side,
    /// Venue order id (`i`).
    pub order_id: String,
    /// Trade id (`t`) — may be empty if the venue omitted it.
    pub trade_id: String,
    /// Bot client order id (`c`), used to attribute the fill to a known quote.
    pub client_id: String,
    /// Last filled quantity (`l`) — the increment this event represents.
    pub last_fill_qty: Decimal,
    /// Last filled price (`L`).
    pub last_fill_px: Decimal,
    /// Cumulative filled quantity (`z`) on the order so far.
    pub cum_filled_qty: Decimal,
    /// Event time (`E`) in venue ms — used to order updates.
    pub event_time_ms: i64,
    /// Whether this fill was on a REDUCE-ONLY order (`o.R`). A reduce-only fill is one of our own
    /// flatten/recovery closes — it REDUCES delta, so it must update the predicted position but
    /// must NOT trigger a new hedge (which would loop: hedge → flatten → its fill → hedge → …).
    pub reduce_only: bool,
    pub commission: Option<Decimal>,
    pub commission_asset: Option<String>,
}

impl AsterFill {
    pub fn usd_fee(&self) -> Option<Decimal> {
        let fee = self.commission?;
        if fee == Decimal::ZERO { return Some(fee); }
        match self.commission_asset.as_deref()?.to_ascii_uppercase().as_str() {
            "USD" | "USDT" | "USDC" | "BUSD" | "FDUSD" | "DAI" | "USDF" => Some(fee),
            _ => None,
        }
    }
}

/// The dedup key for a fill. Prefers `(order_id, trade_id)`; when the trade id is absent or
/// unreliable, falls back to `(order_id, cumulative_filled_qty)` — two
/// different cumulative levels are two distinct fills.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum FillKey {
    Trade { order_id: String, trade_id: String },
    CumQty { order_id: String, cum_scaled: i64 },
}

impl FillKey {
    pub fn of(fill: &AsterFill) -> Self {
        // A SENTINEL trade id ("0"/"-1"/empty) is NOT a real, unique trade id — Aster can emit it
        // for every partial on an order. Treating it as reliable would collapse distinct partials
        // onto one key and silently DROP the later (unhedged) fills. Fall back to cumulative qty.
        let tid = fill.trade_id.trim();
        let reliable = !tid.is_empty() && tid != "0" && tid != "-1";
        if reliable {
            FillKey::Trade {
                order_id: fill.order_id.clone(),
                trade_id: fill.trade_id.clone(),
            }
        } else {
            // Scale cumulative qty to integer micro-units so the key is hashable/exact.
            FillKey::CumQty {
                order_id: fill.order_id.clone(),
                cum_scaled: cum_scaled(fill.cum_filled_qty),
            }
        }
    }
}

/// Tracks which fills have already triggered a hedge, so a repeated event never hedges
/// twice. A processed-but-not-yet-keyed fill can be re-observed safely.
#[derive(Debug, Default)]
pub struct FillDedup {
    hedged: HashSet<FillKey>,
}

impl FillDedup {
    pub fn new() -> Self {
        FillDedup::default()
    }

    /// Returns `true` the FIRST time this fill is seen (caller should create a hedge),
    /// `false` on every repeat (caller must NOT hedge again). Idempotent.
    pub fn observe(&mut self, fill: &AsterFill) -> bool {
        self.hedged.insert(FillKey::of(fill))
    }
}

/// Fill-to-hedge lifecycle. Forward path:
/// `Created → Submitted → Acked → Filled → Reconciled`. Any failure transition routes to a
/// terminal-ish state that freezes maker quoting until resolved.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HedgeState {
    Created,
    Submitted,
    Acked,
    Filled,
    Reconciled,
    // --- failure states ---
    Rejected,
    PartiallyFilled,
    Unknown,
    TimedOut,
}

impl HedgeState {
    /// A fully resolved hedge: no further action and does not block quoting.
    pub fn is_resolved(self) -> bool {
        matches!(self, HedgeState::Reconciled)
    }

    /// A state that must FREEZE maker quoting until an operator/reconciler resolves it
    /// (unknown / timed-out / rejected / partial — the orphan-leg danger zone).
    pub fn is_dangerous(self) -> bool {
        matches!(
            self,
            HedgeState::Rejected | HedgeState::PartiallyFilled | HedgeState::Unknown | HedgeState::TimedOut
        )
    }

    /// Still in flight (created/submitted/acked) — hedging is in progress, not yet orphaned.
    pub fn is_in_flight(self) -> bool {
        matches!(self, HedgeState::Created | HedgeState::Submitted | HedgeState::Acked)
    }
}

/// One obligation from an Aster fill; its cloid identifies the attempt on either hedge venue.
#[derive(Debug, Clone)]
pub struct HedgeIntent {
    pub cloid: Cloid,
    pub logical_id: Cloid,
    pub market: MarketId,
    pub venue: Venue,
    pub purpose: IntentPurpose,
    pub admission: Arc<Admission>,
    /// True only on execution-terminal evidence, never from an observation timeout.
    pub terminal: bool,
    pub terminal_ns: Option<i64>,
    pub wire: Option<WireProof>,
    pub client_id: Option<String>,
    pub filled_quote_usd: Option<Decimal>,
    pub fee_usd: Option<Decimal>,
    pub event_time_ms: Option<i64>,
    pub book_source: Option<&'static str>,
    pub book_age_ms: Option<i64>,
    /// Execution side: the hedge side (opposite the Aster fill), or the closing side
    /// of an Aster flatten.
    pub hedge_side: Side,
    pub qty: Decimal,
    /// Average Aster fill price the hedge is offsetting (for PnL attribution).
    pub aster_fill_px: Decimal,
    pub state: HedgeState,
    pub created_ns: i64,
    pub submitted_ns: Option<i64>,
    /// Venue-assigned order id once known, including an Aster flatten's id.
    pub hl_oid: Option<String>,
    /// Quantity actually hedged so far (for partial handling).
    pub filled_qty: Decimal,
    /// How many submit attempts have been made (normal → emergency → freeze).
    pub attempts: u32,
    /// True for reconciler-backstop (orphan recovery) hedges. Lets `recover_orphans`
    /// recognize its own outstanding intents so it never overwrites one or races a second
    /// order onto the wire for the same net.
    pub recovery: bool,
}

/// Scale a cumulative-fill quantity to integer micro-units: [`FillKey::of`]'s exact, hashable
/// fallback key. Derived purely from exchange data.
pub fn cum_scaled(cum_filled_qty: Decimal) -> i64 {
    use rust_decimal::prelude::ToPrimitive;
    (cum_filled_qty * Decimal::from(1_000_000)).round().to_i64().unwrap_or(i64::MAX)
}

impl HedgeIntent {
    /// A hedge intent for a given `qty` not tied 1:1 to a single fill — used both for an
    /// ACCUMULATED hedge (the net of several sub-min partials reached hedgeable size) and for a
    /// RECOVERY hedge (the reconciler backstop offsetting an orphaned net delta; set
    /// `recovery = true` on the returned intent). `cloid` is the caller's deterministic id;
    /// `qty` is the amount to hedge.
    pub fn with_qty(cloid: Cloid, market: MarketId, hedge_side: Side, qty: Decimal, ref_px: Decimal, now_ns: i64) -> Self {
        HedgeIntent {
            cloid,
            logical_id: cloid,
            market,
            venue: Venue::Hedge,
            purpose: IntentPurpose::Hedge,
            admission: Admission::new(i64::MAX),
            terminal: false,
            terminal_ns: None,
            wire: None,
            client_id: None,
            filled_quote_usd: Some(Decimal::ZERO),
            fee_usd: Some(Decimal::ZERO),
            event_time_ms: None,
            book_source: None,
            book_age_ms: None,
            hedge_side,
            qty,
            aster_fill_px: ref_px,
            state: HedgeState::Created,
            created_ns: now_ns,
            submitted_ns: None,
            hl_oid: None,
            filled_qty: Decimal::ZERO,
            attempts: 0,
            recovery: false,
        }
    }

    pub fn mark_submitted(&mut self, now_ns: i64) {
        self.state = HedgeState::Submitted;
        self.submitted_ns = Some(now_ns);
        self.attempts += 1;
    }

    pub fn mark_reconciled(&mut self) {
        self.terminal = true;
        self.state = HedgeState::Reconciled;
    }
    pub fn mark_rejected(&mut self) {
        self.terminal = true;
        self.state = HedgeState::Rejected;
    }
    pub fn mark_unknown(&mut self) {
        self.state = HedgeState::Unknown;
    }

    /// Mark timed-out if it has been in flight longer than `timeout_ns` without resolving.
    pub fn check_timeout(&mut self, now_ns: i64, timeout_ns: i64) {
        if !self.terminal && self.state.is_in_flight()
            && now_ns.saturating_sub(self.created_ns) > timeout_ns
        {
            self.state = HedgeState::TimedOut;
            // Claim and cancellation are mutually exclusive. A claimed timeout
            // remains uncertain until the worker supplies terminal evidence.
            self.terminal = self.admission.cancel_queued() || self.admission.is_cancelled();
            if self.terminal { self.terminal_ns = Some(now_ns); }
        }
    }

    pub fn arm_admission(&mut self, max_age_ms: i64) {
        self.admission = Admission::new(self.created_ns.saturating_add(max_age_ms.max(0).saturating_mul(1_000_000)));
    }

    pub fn unresolved(&self) -> bool {
        !self.terminal && !self.state.is_resolved()
    }

    /// Cumulative execution evidence is idempotent across WS and REST recovery.
    pub fn apply_progress(&mut self, qty: Decimal, quote: Option<Decimal>, fee: Option<Decimal>, terminal: bool, now_ns: i64) -> (Decimal, Option<Decimal>) {
        if qty < self.filled_qty || qty > self.qty || quote.is_some_and(|q| q < Decimal::ZERO) {
            self.mark_unknown();
            return (Decimal::ZERO, None);
        }
        let delta_qty = qty - self.filled_qty;
        let delta_quote = quote.zip(self.filled_quote_usd).map(|(q, old)| q - old);
        if delta_qty > Decimal::ZERO || quote.is_some() { self.filled_quote_usd = quote; }
        if delta_qty > Decimal::ZERO || fee.is_some() { self.fee_usd = fee; }
        self.filled_qty = qty;
        self.terminal |= terminal;
        if terminal && self.terminal_ns.is_none() { self.terminal_ns = Some(now_ns); }
        self.state = if qty == self.qty { HedgeState::Filled }
            else if self.terminal { HedgeState::PartiallyFilled }
            else { HedgeState::Acked };
        (delta_qty, delta_quote)
    }

    pub fn remaining_qty(&self) -> Decimal {
        (self.qty - self.filled_qty).max(Decimal::ZERO)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn fill(order: &str, trade: &str, last: Decimal, cum: Decimal) -> AsterFill {
        AsterFill {
            market: "BTC".into(),
            aster_side: Side::Buy,
            order_id: order.into(),
            trade_id: trade.into(),
            client_id: "Xabc-BTC-B-0".into(),
            last_fill_qty: last,
            last_fill_px: dec!(100),
            cum_filled_qty: cum,
            event_time_ms: 1,
            reduce_only: false,
            commission: None,
            commission_asset: None,
        }
    }

    #[test]
    fn dedup_hedges_once_per_trade() {
        let mut d = FillDedup::new();
        let f = fill("100", "T7", dec!(0.5), dec!(0.5));
        assert!(d.observe(&f)); // first sighting => hedge
        assert!(!d.observe(&f)); // repeat => do NOT hedge again
    }

    #[test]
    fn dedup_falls_back_to_cum_qty_without_trade_id() {
        let mut d = FillDedup::new();
        let a = fill("100", "", dec!(0.3), dec!(0.3));
        let b = fill("100", "", dec!(0.2), dec!(0.5)); // same order, new cumulative
        assert!(d.observe(&a));
        assert!(d.observe(&b)); // distinct cumulative => distinct fill
        assert!(!d.observe(&a)); // a repeated again => deduped
    }

    #[test]
    fn sentinel_trade_id_zero_is_not_a_reliable_key() {
        // A sentinel "0"/"-1" trade id must be treated as ABSENT, so two distinct partials with
        // trade_id "0" are keyed by cumulative qty (distinct) — not collapsed onto one Trade key
        // (which would DROP the second, unhedged).
        let mut d = FillDedup::new();
        let a = fill("100", "0", dec!(0.3), dec!(0.3));
        let b = fill("100", "0", dec!(0.2), dec!(0.5)); // same order + sentinel tid, NEW cumulative
        assert!(matches!(FillKey::of(&a), FillKey::CumQty { .. }), "sentinel tid must fall back to CumQty");
        assert!(d.observe(&a));
        assert!(d.observe(&b), "distinct cumulative with sentinel tid must NOT be deduped away");
    }

    #[test]
    fn hedge_lifecycle_forward_path() {
        let mut h = HedgeIntent::with_qty(Cloid::recovery(&"BTC".into(), 1), "BTC".into(), Side::Sell, dec!(0.5), dec!(100), 0);
        h.mark_submitted(10);
        assert_eq!(h.state, HedgeState::Submitted);
        assert_eq!(h.attempts, 1);
        h.mark_reconciled();
        assert!(h.state.is_resolved());
    }

    #[test]
    fn hedge_times_out_when_in_flight_too_long() {
        let mut h = HedgeIntent::with_qty(Cloid::recovery(&"BTC".into(), 1), "BTC".into(), Side::Sell, dec!(0.5), dec!(100), 0);
        h.mark_submitted(0);
        h.check_timeout(500, 1_000); // not yet
        assert!(h.state.is_in_flight());
        h.check_timeout(2_000, 1_000); // overdue
        assert_eq!(h.state, HedgeState::TimedOut);
        assert!(h.state.is_dangerous());
    }
    #[test]
    fn admission_claim_and_cancellation_are_mutually_exclusive() {
        for _ in 0..16 {
            let ticket=Admission::new(100); let start=Arc::new(std::sync::Barrier::new(3));
            let a=ticket.clone(); let a_start=start.clone();
            let claim=std::thread::spawn(move || {a_start.wait();a.try_claim(99)});
            let b=ticket.clone(); let b_start=start.clone();
            let cancel=std::thread::spawn(move || {b_start.wait();b.cancel_queued()});
            start.wait(); assert_ne!(claim.join().unwrap(),cancel.join().unwrap());
            assert!(!ticket.try_claim(99));
        }
        let expired=Admission::new(100); assert!(!expired.try_claim(100)); assert!(expired.is_cancelled());
    }

    #[test]
    fn cumulative_progress_is_idempotent_and_terminal_time_does_not_slide() {
        let mut h=HedgeIntent::with_qty(Cloid::recovery(&"BTC".into(),1),"BTC".into(),Side::Sell,Decimal::new(5,1),Decimal::from(100),0);
        assert_eq!(h.apply_progress(Decimal::new(2,1),Some(Decimal::from(20)),Some(Decimal::new(1,3)),false,10).0,Decimal::new(2,1));
        assert_eq!(h.apply_progress(Decimal::new(2,1),Some(Decimal::from(20)),Some(Decimal::new(1,3)),false,11).0,Decimal::ZERO);
        assert_eq!(h.apply_progress(Decimal::new(5,1),Some(Decimal::from(50)),Some(Decimal::new(25,4)),true,12).0,Decimal::new(3,1));
        assert_eq!(h.apply_progress(Decimal::new(5,1),Some(Decimal::from(50)),Some(Decimal::new(25,4)),true,100).0,Decimal::ZERO);
        assert_eq!(h.terminal_ns,Some(12)); assert_eq!(h.fee_usd,Some(Decimal::new(25,4)));
    }

}
