//! Per-market in-flight Aster maker order state. One
//! [`MakerSlot`] per (market, side): the bid and the ask are tracked independently, each
//! carrying its current order, lifecycle state, a per-side quote-epoch counter (feeds the
//! deterministic client id), a requote throttle, and a per-symbol replace-rate limiter.
//!
//! Single-owner: this lives inside the strategy thread, so it needs no locks.

use std::collections::VecDeque;

use crate::types::{MarketId, Side};

use super::ids::{aster_client_id, SessionId};
use super::precheck::HotCurrentOrder;

/// Lifecycle of a single resting maker order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderLifecycle {
    /// Place sent, not yet acked.
    PendingPlace,
    /// Resting on the book (acked).
    Open,
    /// Cancel sent, not yet confirmed (still potentially fillable).
    PendingCancel,
    /// Amend sent, not yet answered: the order rests at its old or its new values until then.
    PendingAmend,
    /// No live order in this slot.
    Idle,
}

impl OrderLifecycle {
    /// Whether this slot currently holds an order the venue might still fill.
    pub fn is_live(self) -> bool {
        matches!(
            self,
            OrderLifecycle::PendingPlace
                | OrderLifecycle::Open
                | OrderLifecycle::PendingCancel
                | OrderLifecycle::PendingAmend
        )
    }
}

/// Result of asking a slot for a targeted cancel command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CancelTarget {
    /// Send this client-id cancel now.
    Send {
        client_id: String,
        venue_order_id: Option<String>,
    },
    /// Do not send: a cancel for the same slot is already in flight or recently retried.
    Suppressed,
    /// No venue-live order is known for this slot.
    None,
}

/// Aster allows 10 000 amends per order; past this many the quote is cancelled, and the next
/// tick places a fresh order.
pub const MAX_AMENDS_PER_ORDER: u32 = 9_000;

/// One side's current order in one market.
#[derive(Debug, Clone)]
pub struct MakerSlot {
    queued_admission: Option<std::sync::Arc<super::fills::Admission>>,
    pub state: OrderLifecycle,
    pub client_id: Option<String>,
    pub venue_order_id: Option<String>,
    pub price_ticks: i64,
    /// Order size in lots, which only an amend changes. The live venue reports cumulative
    /// fills (`z`) per order, amends included, so track `filled_lots` separately; hot-path
    /// quote/cancel decisions use the remaining lots.
    pub qty_lots: i64,
    pub filled_lots: i64,
    /// While an amend is in flight, the price and qty it replaces: the order rests at either
    /// until the venue answers. `price_ticks` and `qty_lots` hold the amended values.
    amend_from: Option<(i64, i64)>,
    /// Amends of this order so far.
    pub amends: u32,
    /// Monotonic ns of the last targeted cancel successfully enqueued for this client id.
    /// Used to suppress duplicate cancel spam while a cancel is already pending.
    last_cancel_attempt_ns: i64,
    /// Monotonic ns of the last requote on this side (throttle).
    pub last_requote_ns: i64,
    /// Per-side quote-epoch counter; every new order increments it → unique client ids.
    quote_epoch: u64,
}

impl MakerSlot {
    fn new() -> Self {
        MakerSlot {
            queued_admission: None,
            state: OrderLifecycle::Idle,
            client_id: None,
            venue_order_id: None,
            price_ticks: 0,
            qty_lots: 0,
            filled_lots: 0,
            amend_from: None,
            amends: 0,
            last_cancel_attempt_ns: i64::MIN,
            last_requote_ns: i64::MIN,
            quote_epoch: 0,
        }
    }

    pub fn is_live(&self) -> bool {
        self.state.is_live()
    }

    #[inline]
    pub fn remaining_lots(&self) -> i64 {
        self.qty_lots.saturating_sub(self.filled_lots).max(0)
    }

    /// Whether enough time has passed since the last requote (non-urgent throttle).
    pub fn throttle_ok(&self, now_ns: i64, min_interval_ms: u64) -> bool {
        now_ns.saturating_sub(self.last_requote_ns) >= (min_interval_ms as i64) * 1_000_000
    }

    pub fn amending(&self) -> bool {
        self.amend_from.is_some()
    }

    /// The most this order can still fill: during an amend, from the larger of its two sizes.
    fn fillable_lots(&self) -> i64 {
        self.amend_from.map_or(self.qty_lots, |(_, old)| old.max(self.qty_lots))
    }
}

fn clear_slot(slot: &mut MakerSlot) {
    if let Some(ticket) = slot.queued_admission.take() { ticket.cancel_queued(); }
    slot.state = OrderLifecycle::Idle;
    slot.client_id = None;
    slot.venue_order_id = None;
    slot.price_ticks = 0;
    slot.qty_lots = 0;
    slot.filled_lots = 0;
    slot.amend_from = None;
    slot.amends = 0;
    slot.last_cancel_attempt_ns = i64::MIN;
}

/// The venue answered an amend: an ack keeps the new values, a refusal restores the old. A
/// fill that covered the order meanwhile closes the slot.
fn settle_amend(slot: &mut MakerSlot, accepted: bool) {
    let Some((price_ticks, qty_lots)) = slot.amend_from.take() else { return };
    if !accepted {
        (slot.price_ticks, slot.qty_lots) = (price_ticks, qty_lots);
    }
    if slot.state == OrderLifecycle::PendingAmend {
        slot.state = OrderLifecycle::Open;
    }
    if slot.filled_lots >= slot.qty_lots {
        clear_slot(slot);
    }
}

/// All maker slots for the bot, keyed by market then side, plus the per-symbol replace-rate
/// limiter. Owned by the strategy loop.
pub struct OrderManager {
    session: SessionId,
    slots: Vec<MarketSlots>,
    /// Monotonic counter for flatten (reduce-only close) client ids — session-prefixed so
    /// their fills pass `is_own_client_id` attribution (see `handle_maker_fill`).
    flatten_epoch: u64,
    recent_makers: VecDeque<super::account::MakerQuery>,
}

struct MarketSlots {
    market: MarketId,
    bid: MakerSlot,
    ask: MakerSlot,
    /// Monotonic ns of recent places/cancels/replaces, for the per-minute cap.
    replace_times_ns: VecDeque<i64>,
}

impl OrderManager {
    pub fn new(session: SessionId, markets: &[MarketId]) -> Self {
        let slots = markets
            .iter()
            .map(|m| MarketSlots {
                market: m.clone(),
                bid: MakerSlot::new(),
                ask: MakerSlot::new(),
                replace_times_ns: VecDeque::new(),
            })
            .collect();
        OrderManager { session, slots, flatten_epoch: 0, recent_makers: VecDeque::with_capacity(64) }
    }

    /// Fresh session-prefixed client id for a FLATTEN order (reduce-only close). These ids
    /// never match a maker slot, but they DO match this session's `is_own_client_id`
    /// prefix, so the resulting reduce-only fill updates predicted position instead of
    /// being dropped as a foreign fill.
    pub fn next_flatten_client_id(&mut self, market: &MarketId) -> String {
        let id = super::ids::aster_flatten_client_id(&self.session, market, self.flatten_epoch);
        self.flatten_epoch += 1;
        id
    }

    pub fn next_attempt_id(&mut self, market: &MarketId) -> super::ids::Cloid {
        let id = super::ids::Cloid::hedge(self.session.as_str(), &market.0, self.flatten_epoch as i64);
        self.flatten_epoch = self.flatten_epoch.checked_add(1).expect("execution sequence exhausted");
        id
    }

    pub fn recent_makers(&self) -> Vec<super::account::MakerQuery> { self.recent_makers.iter().cloned().collect() }
    pub fn expected_maker(&self, client_id: &str) -> Option<&super::account::MakerQuery> {
        self.recent_makers.iter().find(|q| q.client_id == client_id)
    }

    fn remember_maker(&mut self, market: &MarketId, side: Side, client_id: &str, qty_lots: i64) {
        if self.recent_makers.len() >= 64 { self.recent_makers.pop_front(); }
        self.recent_makers.push_back(super::account::MakerQuery { market: market.clone(), side, client_id: client_id.to_owned(), qty_lots });
    }

    fn market_mut(&mut self, market: &MarketId) -> Option<&mut MarketSlots> {
        self.slots.iter_mut().find(|s| &s.market == market)
    }
    fn market(&self, market: &MarketId) -> Option<&MarketSlots> {
        self.slots.iter().find(|s| &s.market == market)
    }

    pub fn slot(&self, market: &MarketId, side: Side) -> Option<&MakerSlot> {
        self.market(market).map(|m| match side {
            Side::Buy => &m.bid,
            Side::Sell => &m.ask,
        })
    }

    pub fn current_hot_order(&self, market: &MarketId, side: Side) -> Option<HotCurrentOrder> {
        self.slot(market, side).and_then(|s| {
            (s.is_live() && s.remaining_lots() > 0).then(|| HotCurrentOrder { px_ticks: s.price_ticks })
        })
    }

    pub fn bind_admission(&mut self, market: &MarketId, side: Side, ticket: std::sync::Arc<super::fills::Admission>) {
        if let Some(m) = self.market_mut(market) {
            let slot = match side { Side::Buy => &mut m.bid, Side::Sell => &mut m.ask };
            if let Some(old) = slot.queued_admission.replace(ticket) { old.cancel_queued(); }
        }
    }

    pub fn revoke_queued(&self, market: &MarketId, side: Side) {
        if let Some(ticket) = self.slot(market, side).and_then(|s| s.queued_admission.as_ref()) { ticket.cancel_queued(); }
    }

    pub fn potential_lots(&self, market: &MarketId, side: Side) -> i64 {
        self.slot(market, side).filter(|s| s.is_live()).map(|s|
            s.fillable_lots().saturating_sub(s.filled_lots).max(0)).unwrap_or(0)
    }

    /// Allocate the next client id for a new order on (market, side), bumping the epoch.
    pub fn next_client_id(&mut self, market: &MarketId, side: Side) -> Option<String> {
        let session = self.session.clone();
        let m = self.market_mut(market)?;
        let slot = match side {
            Side::Buy => &mut m.bid,
            Side::Sell => &mut m.ask,
        };
        let id = aster_client_id(&session, market, side, slot.quote_epoch);
        slot.quote_epoch += 1;
        Some(id)
    }

    /// Record that a place was sent for (market, side) with `client_id`.
    pub fn on_place_sent(&mut self, market: &MarketId, side: Side, client_id: String, price_ticks: i64, qty_lots: i64, now_ns: i64) {
        self.remember_maker(market, side, &client_id, qty_lots);
        self.record_replace(market, now_ns);
        if let Some(m) = self.market_mut(market) {
            let slot = match side {
                Side::Buy => &mut m.bid,
                Side::Sell => &mut m.ask,
            };
            slot.state = OrderLifecycle::PendingPlace;
            slot.client_id = Some(client_id);
            slot.venue_order_id = None;
            slot.price_ticks = price_ticks;
            slot.qty_lots = qty_lots;
            slot.filled_lots = 0;
            slot.amend_from = None;
            slot.amends = 0;
            slot.last_cancel_attempt_ns = i64::MIN;
            slot.last_requote_ns = now_ns;
        }
    }

    /// Record an amend of the Open order on (market, side) to new values. The order keeps its
    /// ids; the old values stay until the venue answers, since a fill may land at either.
    pub fn on_amend_sent(&mut self, market: &MarketId, side: Side, price_ticks: i64, qty_lots: i64, now_ns: i64) {
        self.record_replace(market, now_ns);
        let Some(m) = self.market_mut(market) else { return };
        let slot = match side {
            Side::Buy => &mut m.bid,
            Side::Sell => &mut m.ask,
        };
        if slot.state != OrderLifecycle::Open {
            return;
        }
        slot.amend_from = Some((slot.price_ticks, slot.qty_lots));
        (slot.price_ticks, slot.qty_lots) = (price_ticks, qty_lots);
        slot.state = OrderLifecycle::PendingAmend;
        slot.amends += 1;
        slot.last_requote_ns = now_ns;
        // The reconciler judges a filled order by its size: keep the larger one.
        let client_id = slot.client_id.clone();
        if let Some(query) = self.recent_makers.iter_mut().find(|q| Some(&q.client_id) == client_id.as_ref()) {
            query.qty_lots = query.qty_lots.max(qty_lots);
        }
    }

    /// The venue refused the amend: the order rests at its old values.
    pub fn on_amend_rejected(&mut self, market: &MarketId, side: Side) {
        if let Some(m) = self.market_mut(market) {
            settle_amend(match side { Side::Buy => &mut m.bid, Side::Sell => &mut m.ask }, false);
        }
    }

    /// Record a venue ack (order is now Open / known by `venue_order_id`).
    pub fn on_acked(&mut self, market: &MarketId, side: Side, venue_order_id: String) {
        if let Some(m) = self.market_mut(market) {
            let slot = match side {
                Side::Buy => &mut m.bid,
                Side::Sell => &mut m.ask,
            };
            slot.venue_order_id = Some(venue_order_id);
            if slot.state == OrderLifecycle::PendingPlace {
                slot.state = OrderLifecycle::Open;
            }
            settle_amend(slot, true);
        }
    }

    /// Return the targeted cancel to send for this slot, suppressing duplicates while a cancel is
    /// already pending. An order being amended is cancelled by its own id like any other.
    pub fn cancel_target(
        &mut self,
        market: &MarketId,
        side: Side,
        now_ns: i64,
        retry_backoff_ms: u64,
    ) -> CancelTarget {
        let retry_ns = (retry_backoff_ms as i64).saturating_mul(1_000_000);
        let Some(m) = self.market_mut(market) else { return CancelTarget::None };
        let slot = match side {
            Side::Buy => &mut m.bid,
            Side::Sell => &mut m.ask,
        };
        if !slot.is_live() {
            return CancelTarget::None;
        }
        // A place whose admission was cancelled never left: the worker's `PlaceReject` closes
        // the slot. A cancel would cost a request and hold up the next place behind it.
        if slot.state == OrderLifecycle::PendingPlace && slot.queued_admission.as_ref().is_some_and(|ticket| ticket.is_cancelled()) {
            return CancelTarget::Suppressed;
        }
        let Some(client_id) = slot.client_id.clone() else { return CancelTarget::None };
        if slot.state == OrderLifecycle::PendingCancel
            && now_ns.saturating_sub(slot.last_cancel_attempt_ns) < retry_ns
        {
            return CancelTarget::Suppressed;
        }
        CancelTarget::Send { client_id, venue_order_id: slot.venue_order_id.clone() }
    }

    /// Record that a cancel was sent for (market, side).
    pub fn on_cancel_sent(&mut self, market: &MarketId, side: Side, now_ns: i64) {
        self.record_replace(market, now_ns);
        if let Some(m) = self.market_mut(market) {
            let slot = match side {
                Side::Buy => &mut m.bid,
                Side::Sell => &mut m.ask,
            };
            if slot.is_live() {
                slot.last_cancel_attempt_ns = now_ns;
                slot.state = OrderLifecycle::PendingCancel;
            }
        }
    }

    /// Record maker-fill progress for the current client id. A partial fill keeps the slot
    /// live because the residual can still be canceled; a fully-filled order is no longer
    /// cancelable/resting, so clear the slot immediately. During an amend the order may hold
    /// either size, so the amend's answer settles it.
    ///
    /// `cum_filled_lots` is cumulative for the venue order, not the last-fill increment.
    /// We store it separately from the original `qty_lots`, so duplicate/out-of-order partial
    /// updates cannot double-subtract.
    pub fn on_maker_fill_progress(
        &mut self,
        market: &MarketId,
        side: Side,
        client_id: &str,
        cum_filled_lots: i64,
    ) {
        if cum_filled_lots <= 0 {
            return;
        }
        if let Some(m) = self.market_mut(market) {
            let slot = match side {
                Side::Buy => &mut m.bid,
                Side::Sell => &mut m.ask,
            };
            if slot.client_id.as_deref() == Some(client_id) {
                // Venue/user-stream updates carry cumulative filled quantity for the order. Accept
                // duplicate/out-of-order partials without moving backwards, and expose the residual
                // size to exact quote decisions.
                slot.filled_lots = slot.filled_lots.max(cum_filled_lots).min(slot.fillable_lots());
                if !slot.amending() && slot.filled_lots >= slot.qty_lots {
                    clear_slot(slot);
                }
            }
        }
    }

    /// Record that the slot is now empty (cancel confirmed, filled, or expired).
    pub fn on_closed(&mut self, market: &MarketId, side: Side) {
        if let Some(m) = self.market_mut(market) {
            let slot = match side {
                Side::Buy => &mut m.bid,
                Side::Sell => &mut m.ask,
            };
            clear_slot(slot);
        }
    }

    fn record_replace(&mut self, market: &MarketId, now_ns: i64) {
        if let Some(m) = self.market_mut(market) {
            m.replace_times_ns.push_back(now_ns);
            let cutoff = now_ns - 60_000_000_000; // 60s
            while m.replace_times_ns.front().is_some_and(|&t| t < cutoff) {
                m.replace_times_ns.pop_front();
            }
        }
    }

    /// Prune the rolling 60s window before checking admission, including while dispatch is
    /// blocked; pruning only on successful sends would leave an exhausted window latched shut.
    /// `max_per_min == 0` disables the cap here (the window is still pruned so it stays bounded);
    /// the strategy always passes the non-zero `effective_max_replaces_per_minute_per_symbol()`.
    pub fn replace_rate_ok(&mut self, market: &MarketId, max_per_min: u32, now_ns: i64) -> bool {
        let cutoff = now_ns - 60_000_000_000; // 60s window
        if let Some(m) = self.market_mut(market) {
            while m.replace_times_ns.front().is_some_and(|&t| t < cutoff) {
                m.replace_times_ns.pop_front();
            }
            max_per_min == 0 || (m.replace_times_ns.len() as u32) < max_per_min
        } else {
            true
        }
    }

    /// Every live order's client id.
    #[cfg(test)]
    pub fn known_client_ids(&self) -> std::collections::HashSet<String> {
        let mut out = std::collections::HashSet::new();
        for m in &self.slots {
            for slot in [&m.bid, &m.ask] {
                if let Some(id) = &slot.client_id {
                    out.insert(id.clone());
                }
            }
        }
        out
    }

    /// Whether a venue fill's client id belongs to THIS session's bot orders. Maker client ids are
    /// `X{session}-{MARKET}-{B|S}-{epoch}`, so the `X{session}-` prefix attributes a fill to us —
    /// it accepts a legitimate LATE fill (one that arrives after a cancel already closed the slot,
    /// so `known_client_ids()` would miss it) while rejecting foreign / manual / prior-run orders
    /// that must NEVER trigger a hedge.
    pub fn is_own_client_id(&self, client_id: &str) -> bool {
        !client_id.is_empty() && client_id.starts_with(&format!("X{}-", self.session.as_str()))
    }

    /// All live (market, side) slots — used to cancel-all on gate close / cooldown.
    pub fn live_slots(&self) -> Vec<(MarketId, Side)> {
        let mut out = Vec::new();
        for m in &self.slots {
            if m.bid.is_live() {
                out.push((m.market.clone(), Side::Buy));
            }
            if m.ask.is_live() {
                out.push((m.market.clone(), Side::Sell));
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mgr() -> OrderManager {
        OrderManager::new(SessionId::from_tag("sess01"), &["BTC".into(), "ETH".into()])
    }

    #[test]
    fn client_ids_increment_per_side() {
        let mut m = mgr();
        let a = m.next_client_id(&"BTC".into(), Side::Buy).unwrap();
        let b = m.next_client_id(&"BTC".into(), Side::Buy).unwrap();
        let c = m.next_client_id(&"BTC".into(), Side::Sell).unwrap();
        assert_ne!(a, b); // epoch bumped
        assert_ne!(a, c); // different side
        assert!(a.contains("-B-"));
        assert!(c.contains("-S-"));
    }

    #[test]
    fn is_own_client_id_matches_session_prefix_only() {
        let mut m = mgr(); // session "sess01"
        let own = m.next_client_id(&"BTC".into(), Side::Buy).unwrap();
        assert!(m.is_own_client_id(&own)); // our own order, even before/after a cancel closes the slot
        assert!(!m.is_own_client_id("")); // empty
        assert!(!m.is_own_client_id("Xother1-BTC-B-0")); // different session prefix
        assert!(!m.is_own_client_id("manual-order-123")); // foreign / manual
        assert!(!m.is_own_client_id("Xsess02-BTC-B-0")); // prior-run different session
    }

    #[test]
    fn place_ack_cancel_close_lifecycle() {
        let mut m = mgr();
        let id = m.next_client_id(&"BTC".into(), Side::Buy).unwrap();
        m.on_place_sent(&"BTC".into(), Side::Buy, id.clone(), 1000, 5, 0);
        assert_eq!(m.slot(&"BTC".into(), Side::Buy).unwrap().state, OrderLifecycle::PendingPlace);
        m.on_acked(&"BTC".into(), Side::Buy, "oid1".into());
        assert_eq!(m.slot(&"BTC".into(), Side::Buy).unwrap().state, OrderLifecycle::Open);
        assert!(m.known_client_ids().contains(&id));
        assert_eq!(m.live_slots(), vec![("BTC".into(), Side::Buy)]);
        m.on_cancel_sent(&"BTC".into(), Side::Buy, 1_000_000);
        assert_eq!(m.slot(&"BTC".into(), Side::Buy).unwrap().state, OrderLifecycle::PendingCancel);
        assert!(m.slot(&"BTC".into(), Side::Buy).unwrap().is_live()); // still fillable
        m.on_closed(&"BTC".into(), Side::Buy);
        assert_eq!(m.slot(&"BTC".into(), Side::Buy).unwrap().state, OrderLifecycle::Idle);
        assert!(m.known_client_ids().is_empty());
        assert!(m.live_slots().is_empty());
    }

    #[test]
    fn full_fill_closes_slot_but_partial_stays_live() {
        let mut m = mgr();
        let id = m.next_client_id(&"BTC".into(), Side::Buy).unwrap();
        m.on_place_sent(&"BTC".into(), Side::Buy, id.clone(), 1000, 10, 0);
        m.on_acked(&"BTC".into(), Side::Buy, "oid1".into());

        m.on_maker_fill_progress(&"BTC".into(), Side::Buy, &id, 4);
        assert_eq!(m.slot(&"BTC".into(), Side::Buy).unwrap().state, OrderLifecycle::Open);
        assert_eq!(m.slot(&"BTC".into(), Side::Buy).unwrap().filled_lots, 4);
        assert_eq!(m.slot(&"BTC".into(), Side::Buy).unwrap().remaining_lots(), 6);

        // A duplicate/out-of-order smaller cumulative update must not move filled_lots backwards.
        m.on_maker_fill_progress(&"BTC".into(), Side::Buy, &id, 3);
        assert_eq!(m.slot(&"BTC".into(), Side::Buy).unwrap().filled_lots, 4);

        m.on_maker_fill_progress(&"BTC".into(), Side::Buy, &id, 10);
        assert_eq!(m.slot(&"BTC".into(), Side::Buy).unwrap().state, OrderLifecycle::Idle);
        assert!(m.known_client_ids().is_empty());
    }

    #[test]
    fn an_amend_keeps_the_order_and_a_refusal_restores_its_values() {
        let (mut m, market): (_, MarketId) = (mgr(), "BTC".into());
        let id = m.next_client_id(&market, Side::Buy).unwrap();
        m.on_place_sent(&market, Side::Buy, id.clone(), 1000, 5, 0);
        m.on_acked(&market, Side::Buy, "oid1".into());
        m.on_amend_sent(&market, Side::Buy, 1001, 6, 10);
        let slot = m.slot(&market, Side::Buy).unwrap();
        assert_eq!((slot.state, slot.client_id.as_deref(), slot.price_ticks, slot.qty_lots, slot.amends),
            (OrderLifecycle::PendingAmend, Some(id.as_str()), 1001, 6, 1));
        m.on_amend_rejected(&market, Side::Buy);
        let slot = m.slot(&market, Side::Buy).unwrap();
        assert_eq!((slot.state, slot.price_ticks, slot.qty_lots), (OrderLifecycle::Open, 1000, 5));
        m.on_amend_sent(&market, Side::Buy, 999, 7, 20);
        m.on_acked(&market, Side::Buy, "oid1".into());
        let slot = m.slot(&market, Side::Buy).unwrap();
        assert_eq!((slot.state, slot.price_ticks, slot.qty_lots, slot.amends), (OrderLifecycle::Open, 999, 7, 2));
        // A cancel during an amend goes out by the order's own id.
        m.on_amend_sent(&market, Side::Buy, 998, 7, 30);
        assert!(matches!(m.cancel_target(&market, Side::Buy, 31, 1000), CancelTarget::Send { ref client_id, .. } if client_id == &id));
        m.on_cancel_sent(&market, Side::Buy, 31);
        m.on_acked(&market, Side::Buy, "oid1".into());
        assert_eq!(m.slot(&market, Side::Buy).unwrap().state, OrderLifecycle::PendingCancel);
    }

    #[test]
    fn a_fill_during_an_amend_counts_against_either_size_until_the_answer() {
        let (mut m, market): (_, MarketId) = (mgr(), "BTC".into());
        let id = m.next_client_id(&market, Side::Buy).unwrap();
        m.on_place_sent(&market, Side::Buy, id.clone(), 1000, 10, 0);
        m.on_acked(&market, Side::Buy, "oid1".into());
        // Shrink to 5 while 8 fill at the old size: nothing is settled before the answer.
        m.on_amend_sent(&market, Side::Buy, 1001, 5, 10);
        m.on_maker_fill_progress(&market, Side::Buy, &id, 8);
        let slot = m.slot(&market, Side::Buy).unwrap();
        assert_eq!((slot.state, slot.filled_lots), (OrderLifecycle::PendingAmend, 8));
        assert_eq!(m.potential_lots(&market, Side::Buy), 2);
        // Refused: the order still rests 2 of its old 10.
        m.on_amend_rejected(&market, Side::Buy);
        let slot = m.slot(&market, Side::Buy).unwrap();
        assert_eq!((slot.state, slot.remaining_lots()), (OrderLifecycle::Open, 2));
        // Grow to 12 while the old 10 fill: the amended order still rests 2.
        m.on_amend_sent(&market, Side::Buy, 1001, 12, 20);
        m.on_maker_fill_progress(&market, Side::Buy, &id, 10);
        m.on_acked(&market, Side::Buy, "oid1".into());
        assert_eq!(m.slot(&market, Side::Buy).unwrap().remaining_lots(), 2);
        // Refused after the old size filled: nothing rests.
        m.on_amend_sent(&market, Side::Buy, 1002, 14, 30);
        m.on_maker_fill_progress(&market, Side::Buy, &id, 12);
        m.on_amend_rejected(&market, Side::Buy);
        assert_eq!(m.slot(&market, Side::Buy).unwrap().state, OrderLifecycle::Idle);
    }

    #[test]
    fn throttle_respects_min_interval() {
        let mut m = mgr();
        let id = m.next_client_id(&"BTC".into(), Side::Buy).unwrap();
        m.on_place_sent(&"BTC".into(), Side::Buy, id, 1000, 5, 1_000_000_000);
        let slot = m.slot(&"BTC".into(), Side::Buy).unwrap();
        // 20ms interval: 10ms later not ok, 25ms later ok.
        assert!(!slot.throttle_ok(1_000_000_000 + 10_000_000, 20));
        assert!(slot.throttle_ok(1_000_000_000 + 25_000_000, 20));
    }

    #[test]
    fn replace_rate_limiter_caps_per_minute_and_drains_on_check() {
        let mut m = mgr();
        // 3 replaces within the window; cap of 3 => the 3rd is the last allowed.
        assert!(m.replace_rate_ok(&"BTC".into(), 3, 0));
        m.record_replace(&"BTC".into(), 0);
        m.record_replace(&"BTC".into(), 1_000_000);
        assert!(m.replace_rate_ok(&"BTC".into(), 3, 1_000_000));
        m.record_replace(&"BTC".into(), 2_000_000);
        assert!(!m.replace_rate_ok(&"BTC".into(), 3, 2_000_000)); // 3 in window, at cap
        // Far in the future the 60s window has drained — and the CHECK itself prunes, so placement
        // recovers WITHOUT any new send in between (the bug was: it never drained without a send).
        assert!(m.replace_rate_ok(&"BTC".into(), 3, 120_000_000_000));
    }

    #[test]
    fn a_place_that_never_left_gets_no_cancel_and_a_claimed_one_does() {
        let (mut m, market): (_, MarketId) = (mgr(), "BTC".into());
        for claimed in [false, true] {
            let id = m.next_client_id(&market, Side::Buy).unwrap();
            m.on_place_sent(&market, Side::Buy, id.clone(), 1000, 5, 0);
            let ticket = super::super::fills::Admission::new(i64::MAX);
            m.bind_admission(&market, Side::Buy, ticket.clone());
            if claimed {
                assert!(ticket.try_claim(0), "the worker took it");
            }
            m.revoke_queued(&market, Side::Buy);
            let target = m.cancel_target(&market, Side::Buy, 0, 1000);
            if claimed {
                assert!(matches!(target, CancelTarget::Send { ref client_id, .. } if client_id == &id), "{target:?}");
            } else {
                assert_eq!(target, CancelTarget::Suppressed);
            }
            m.on_closed(&market, Side::Buy);
        }
    }

    #[test]
    fn pending_cancel_suppresses_duplicate_until_backoff() {
        let mut m = mgr();
        let id = m.next_client_id(&"BTC".into(), Side::Buy).unwrap();
        m.on_place_sent(&"BTC".into(), Side::Buy, id.clone(), 1000, 5, 0);
        m.on_acked(&"BTC".into(), Side::Buy, "oid1".into());
        assert!(matches!(
            m.cancel_target(&"BTC".into(), Side::Buy, 1_000_000_000, 1000),
            CancelTarget::Send { ref client_id, .. } if client_id == &id
        ));
        m.on_cancel_sent(&"BTC".into(), Side::Buy, 1_000_000_000);
        assert_eq!(
            m.cancel_target(&"BTC".into(), Side::Buy, 1_100_000_000, 1000),
            CancelTarget::Suppressed
        );
        assert!(matches!(
            m.cancel_target(&"BTC".into(), Side::Buy, 2_100_000_000, 1000),
            CancelTarget::Send { ref client_id, .. } if client_id == &id
        ));
    }

}
