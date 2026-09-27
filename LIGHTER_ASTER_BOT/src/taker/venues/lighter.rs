use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, bail, Context, Result};
use arc_swap::ArcSwapOption;
use chrono::{DateTime, Utc};
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::{Decimal, RoundingStrategy};
use tokio::sync::{mpsc, Mutex as AsyncMutex, Notify};

use crate::taker::aster::creds::LighterCreds;
use crate::taker::book::{OrderBook, MAX_BOOK_LEVELS};
use crate::lighter::auth::generate_ws_auth_token;
use crate::lighter::messages::{
    AccountAllMsg, AccountAllPositionsMsg, BookUpdateContiguity, OrderBookMsgRef, PriceLevelRef,
    RemoteOrder, TradePayload, UserStatsMsg,
};
use crate::lighter::nonce::NonceManager;
use crate::lighter::rest::RestClient;
use crate::lighter::signer::{
    Signer, DEFAULT_IOC_EXPIRY, NIL_TRIGGER_PRICE, ORDER_TYPE_MARKET, TIF_IMMEDIATE_OR_CANCEL,
};
use crate::lighter::tx_ws::TxWebSocket;
use crate::lighter::ws::{subscribe_loop, subscribe_loop_authed, SubscribeOptions};
use crate::taker::markets::MarketSpec;
use crate::taker::types::{FeeEvidence, FeeProvenance, FillSummary, MarketId, Side, TxSendStatus};

const MAX_CLIENT_ORDER_INDEX: i64 = 281_474_976_710_655; // 2^48 - 1
static CLIENT_ORDER_COUNTER: AtomicI64 = AtomicI64::new(0);
/// Millisecond in which the 7-bit client-order counter last wrapped (see the wrap guard in
/// `random_client_order_index`).
static LAST_COUNTER_WRAP_MS: AtomicI64 = AtomicI64::new(-1);

#[derive(Debug, Clone)]
pub enum SubmitOutcome {
    Accepted {
        raw: String,
        client_order_index: i64,
        tx_hash: String,
        nonce: i64,
        fill: Option<FillSummary>,
    },
    Rejected {
        reason: String,
    },
    RejectedNonceStale {
        reason: String,
    },
    Unknown {
        reason: String,
        client_order_index: i64,
        tx_hash: String,
        nonce: i64,
    },
}

#[derive(Clone)]
struct Wire {
    market_index: i32,
    size_decimals: u32,
    price_decimals: u32,
}

#[derive(Default)]
struct FillTracker {
    pending: Mutex<HashMap<i64, mpsc::UnboundedSender<TradePayload>>>,
    registered: AtomicU64,
    trades_seen: AtomicU64,
    matched_trades: AtomicU64,
    unmatched_trades: AtomicU64,
    duplicate_trades: AtomicU64,
    timeouts: AtomicU64,
}

#[derive(Debug, Clone, Copy)]
pub struct FillTrackerStats {
    pub registered: u64,
    pub trades_seen: u64,
    pub matched_trades: u64,
    pub unmatched_trades: u64,
    pub duplicate_trades: u64,
    pub timeouts: u64,
}

#[derive(Debug, Clone, Copy)]
pub struct LighterAccountSnapshot {
    pub position_qty: Decimal,
    pub available_usdc: Decimal,
    /// Collateral-style account value: EXCLUDES open-position unrealized PnL.
    pub account_value_usdc: Option<Decimal>,
    /// Venue-reported unrealized PnL summed over all open positions; `None` when
    /// any nonzero position lacks the field (account unmarkable, not zero).
    pub unrealized_pnl_usdc: Option<Decimal>,
}

impl LighterAccountSnapshot {
    /// Marked equity: the collateral-style account value PLUS venue-reported uPnL (the
    /// 733ba55 marked-equity composition); `None` when either part is unavailable.
    pub fn equity_usdc(&self) -> Option<Decimal> {
        match (self.account_value_usdc, self.unrealized_pnl_usdc) {
            (Some(value), Some(upnl)) => Some(value + upnl),
            _ => None,
        }
    }
}

/// Market-free margin/equity read (no market wire needed) for recovery accounting.
#[derive(Debug, Clone, Copy)]
pub struct LighterMarginSnapshot {
    pub available_usdc: Decimal,
    /// Marked equity (account value + uPnL); `None` when either part is unavailable.
    pub equity_usdc: Option<Decimal>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LighterFillStatus {
    Filled,
    PartialFill,
    ExpiredNoFill,
    LiveOrUnknown,
}

impl LighterFillStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            LighterFillStatus::Filled => "filled",
            LighterFillStatus::PartialFill => "partial_fill",
            LighterFillStatus::ExpiredNoFill => "expired_no_fill",
            LighterFillStatus::LiveOrUnknown => "live_or_unknown",
        }
    }
}

#[derive(Debug, Clone)]
pub struct LighterFillConfirmation {
    pub fee_evidence: Vec<FeeEvidence>,
    pub fill: Option<FillSummary>,
    pub terminal_order: Option<RemoteOrder>,
    pub filled_qty: Decimal,
}

impl FillTracker {
    fn register(&self, client_order_index: i64) -> mpsc::UnboundedReceiver<TradePayload> {
        let (tx, rx) = mpsc::unbounded_channel();
        self.registered.fetch_add(1, Ordering::Relaxed);
        self.pending
            .lock()
            .expect("Lighter fill tracker poisoned")
            .insert(client_order_index, tx);
        rx
    }

    fn unregister(&self, client_order_index: i64) {
        self.pending
            .lock()
            .expect("Lighter fill tracker poisoned")
            .remove(&client_order_index);
    }

    fn on_trade(&self, trade: TradePayload) {
        self.trades_seen.fetch_add(1, Ordering::Relaxed);
        let ids = [trade.ask_client_id, trade.bid_client_id];
        let senders: Vec<_> = {
            let pending = self.pending.lock().expect("Lighter fill tracker poisoned");
            ids.into_iter()
                .flatten()
                .filter_map(|id| pending.get(&id).cloned())
                .collect()
        };
        if senders.is_empty() {
            self.unmatched_trades.fetch_add(1, Ordering::Relaxed);
        } else {
            self.matched_trades
                .fetch_add(senders.len() as u64, Ordering::Relaxed);
        }
        for tx in senders {
            let _ = tx.send(trade.clone());
        }
    }

    fn record_duplicate(&self) {
        self.duplicate_trades.fetch_add(1, Ordering::Relaxed);
    }

    fn record_timeout(&self) {
        self.timeouts.fetch_add(1, Ordering::Relaxed);
    }

    fn stats(&self) -> FillTrackerStats {
        FillTrackerStats {
            registered: self.registered.load(Ordering::Relaxed),
            trades_seen: self.trades_seen.load(Ordering::Relaxed),
            matched_trades: self.matched_trades.load(Ordering::Relaxed),
            unmatched_trades: self.unmatched_trades.load(Ordering::Relaxed),
            duplicate_trades: self.duplicate_trades.load(Ordering::Relaxed),
            timeouts: self.timeouts.load(Ordering::Relaxed),
        }
    }
}

struct FillProgress {
    fee_evidence: Vec<FeeEvidence>,
    seen: HashSet<u128>,
    qty: Decimal,
    notional: Decimal,
    fee: Decimal,
    fee_known: bool,
    matched_seen: u64,
}

impl Default for FillProgress {
    fn default() -> Self {
        Self { fee_evidence: Vec::new(), seen: HashSet::new(), qty: Decimal::ZERO, notional: Decimal::ZERO,
            fee: Decimal::ZERO, fee_known: true, matched_seen: 0 }
    }
}

pub struct PendingFill {
    fills: Arc<FillTracker>,
    account_feed: Arc<AccountFeedState>,
    market_id: u32,
    account_index: i64,
    client_order_index: i64,
    side: Side,
    expected_qty: Decimal,
    rx: mpsc::UnboundedReceiver<TradePayload>,
    progress: FillProgress,
}

impl PendingFill {
    pub async fn wait(self, timeout: Duration) -> Option<FillSummary> {
        self.wait_confirmed(timeout).await.fill
    }

    pub async fn wait_confirmed(mut self, timeout: Duration) -> LighterFillConfirmation {
        self.observe_confirmed(timeout).await
    }

    pub async fn observe_confirmed(&mut self, timeout: Duration) -> LighterFillConfirmation {
        let deadline = tokio::time::Instant::now() + timeout;
        let min_expected = self.expected_qty;
        let progress = &mut self.progress;
        loop {
            if progress.qty >= min_expected { break; }
            if self.account_feed.order_by_client(self.client_order_index)
                .filter(|order| remote_matches(order, self.account_index, self.market_id, self.client_order_index, self.side))
                .filter(RemoteOrder::is_terminal).and_then(|order| remote_order_filled_qty(&order))
                .is_some_and(|filled| progress.qty >= filled) { break; }
            let next_poll = (tokio::time::Instant::now() + Duration::from_millis(100)).min(deadline);
            match tokio::time::timeout_at(next_poll, self.rx.recv()).await {
                Ok(Some(trade)) => {
                    let Some(key) = fill_identity(&trade) else {
                        progress.fee_known = false;
                        continue;
                    };
                    if !progress.seen.insert(key) {
                        self.fills.record_duplicate();
                        continue;
                    }
                    let account = match self.side { Side::Sell => trade.ask_account_id, Side::Buy => trade.bid_account_id };
                    if !trade_matches_side(&trade, self.client_order_index, self.side)
                        || account.is_some_and(|account| account != self.account_index) {
                        continue;
                    }
                    progress.matched_seen += 1;
                    let q = trade
                        .size
                        .as_deref()
                        .and_then(|s| s.parse::<Decimal>().ok())
                        .unwrap_or(Decimal::ZERO);
                    let p = trade
                        .price
                        .as_deref()
                        .and_then(|s| s.parse::<Decimal>().ok())
                        .unwrap_or(Decimal::ZERO);
                    if q <= Decimal::ZERO || p <= Decimal::ZERO {
                        continue;
                    }
                    let quote = trade
                        .usd_amount
                        .as_deref()
                        .and_then(|s| s.parse::<Decimal>().ok())
                        .unwrap_or(Decimal::ZERO)
                        .abs();
                    let trade_notional = if quote > Decimal::ZERO { quote } else { q * p };
                    progress.qty += q;
                    progress.notional += trade_notional;
                    let evidence = trade_fee_evidence(&trade, self.side, self.client_order_index, trade_notional, "account_all_trades");
                    match evidence.fee_usd {
                        Some(amount) => progress.fee += amount,
                        None => progress.fee_known = false,
                    }
                    progress.fee_evidence.push(evidence);
                }
                Err(_) if tokio::time::Instant::now() < deadline => continue,
                _ => break,
            }
        }
        let terminal_order = self.account_feed.order_by_client(self.client_order_index)
            .filter(|order| remote_matches(order, self.account_index, self.market_id, self.client_order_index, self.side));
        let terminal_filled_qty = terminal_order
            .as_ref()
            .and_then(remote_order_filled_qty)
            .unwrap_or(Decimal::ZERO);
        let effective_filled_qty = progress.qty.max(terminal_filled_qty);
        let status = if effective_filled_qty >= min_expected {
            LighterFillStatus::Filled
        } else if effective_filled_qty > Decimal::ZERO {
            LighterFillStatus::PartialFill
        } else if terminal_order
            .as_ref()
            .is_some_and(RemoteOrder::is_terminal)
        {
            LighterFillStatus::ExpiredNoFill
        } else {
            LighterFillStatus::LiveOrUnknown
        };
        if progress.qty < min_expected {
            self.fills.record_timeout();
            tracing::warn!(
                "Lighter fill wait incomplete client_order_index={} market_id={} side={} expected_qty={} min_expected_qty={} filled_qty={} terminal_filled_qty={} status={} terminal_status={:?} matched_trades_seen={} tracker_stats={:?}",
                self.client_order_index,
                self.market_id,
                self.side,
                self.expected_qty,
                min_expected,
                progress.qty,
                terminal_filled_qty,
                status.as_str(),
                terminal_order.as_ref().and_then(|order| order.status.as_deref()),
                progress.matched_seen,
                self.fills.stats()
            );
        } else {
            tracing::debug!(
                "Lighter fill wait complete client_order_index={} market_id={} side={} expected_qty={} filled_qty={} status={} matched_trades_seen={} tracker_stats={:?}",
                self.client_order_index,
                self.market_id,
                self.side,
                self.expected_qty,
                progress.qty,
                status.as_str(),
                progress.matched_seen,
                self.fills.stats()
            );
        }
        LighterFillConfirmation {
            fee_evidence: progress.fee_evidence.clone(),
            fill: if status == LighterFillStatus::ExpiredNoFill { Some(FillSummary::zero()) }
                else { FillSummary::from_qty_notional(progress.qty, progress.notional, progress.fee).map(|fill|
                    fill.with_fee_provenance(if progress.fee_known { FeeProvenance::Venue } else { FeeProvenance::Unknown })) },
            terminal_order,
            filled_qty: effective_filled_qty,
        }
    }
}

impl Drop for PendingFill {
    fn drop(&mut self) {
        self.fills.unregister(self.client_order_index);
    }
}

/// How long a TERMINAL (filled/cancelled) order stays queryable via `order_by_client` after
/// the feed reports it. Terminal rows exist only to serve `PendingFill::wait_confirmed`'s
/// terminal-fill fallback, which resolves within seconds; without an expiry the maps grow by
/// one entry per Lighter order for the life of the process.
const TERMINAL_ORDER_TTL: std::time::Duration = std::time::Duration::from_secs(600);

#[derive(Default)]
struct AccountFeedState {
    account_index: Option<i64>,
    positions: Mutex<HashMap<u32, Decimal>>,
    available_balance: Mutex<Option<Decimal>>,
    open_orders: Mutex<HashMap<u32, usize>>,
    client_orders: Mutex<HashMap<i64, RemoteOrder>>,
    client_order_markets: Mutex<HashMap<i64, u32>>,
    /// When each terminal order was first reported, for the TTL purge.
    client_order_terminal_at: Mutex<HashMap<i64, std::time::Instant>>,
    account_all_ready: AtomicBool,
    user_stats_ready: AtomicBool,
    all_orders_ready: AtomicBool,
    /// Once-per-session canary: a NON-snapshot update is assumed to carry the complete live
    /// set for each mentioned market. If one ever shrinks a market's live count by more than
    /// half, that assumption may be wrong (per-order deltas?) — warn once, loudly.
    shrink_warned: AtomicBool,
}

impl AccountFeedState {
    fn set_position(&self, market_id: u32, qty: Decimal) {
        self.positions
            .lock()
            .expect("Lighter positions state poisoned")
            .insert(market_id, qty);
    }

    fn mark_account_all_ready(&self) {
        self.account_all_ready.store(true, Ordering::Release);
    }

    fn position(&self, market_id: u32) -> Option<Decimal> {
        if !self.account_all_ready.load(Ordering::Acquire) {
            return None;
        }
        self.positions
            .lock()
            .expect("Lighter positions state poisoned")
            .get(&market_id)
            .copied()
    }

    fn set_user_stats(&self, available_balance: Option<Decimal>) {
        if let Some(value) = available_balance {
            *self
                .available_balance
                .lock()
                .expect("Lighter available balance poisoned") = Some(value);
            self.user_stats_ready.store(true, Ordering::Release);
        }
    }

    fn available_balance(&self) -> Option<Decimal> {
        if !self.user_stats_ready.load(Ordering::Acquire) {
            return None;
        }
        *self
            .available_balance
            .lock()
            .expect("Lighter available balance poisoned")
    }

    fn set_open_orders_for_markets(&self, known_markets: &[u32], orders: &serde_json::Value, is_snapshot: bool) {
        let mut counts = HashMap::new();
        let mut seen_live_client_ids = HashSet::new();
        let mut parsed_orders = Vec::new();
        let order_obj = orders.as_object();
        let markets: Vec<u32> = if is_snapshot {
            known_markets.to_vec()
        } else {
            order_obj
                .map(|obj| {
                    obj.keys()
                        .filter_map(|key| key.parse::<u32>().ok())
                        .filter(|market_id| known_markets.contains(market_id))
                        .collect()
                })
                .unwrap_or_default()
        };
        for market_id in markets {
            let count = order_obj
                .and_then(|obj| obj.get(&market_id.to_string()))
                .and_then(|v| v.as_array())
                .map(|rows| {
                    rows.iter()
                        .filter_map(|row| serde_json::from_value::<RemoteOrder>(row.clone()).ok())
                        .filter_map(|mut order| {
                            if order.market_index.is_some_and(|id| id != market_id)
                                || order.owner_account_index.zip(self.account_index).is_some_and(|(reported, expected)| reported != expected) {
                                return None;
                            }
                            order.market_index = Some(market_id);
                            if order.owner_account_index.is_none() { order.owner_account_index = self.account_index; }
                            if let Some(client_order_index) = order.client_order_index {
                                if order.is_live() {
                                    seen_live_client_ids.insert(client_order_index);
                                }
                                parsed_orders.push((market_id, order.clone()));
                            }
                            Some(order)
                        })
                        .filter(RemoteOrder::is_live)
                        .count()
                })
                .unwrap_or(0);
            counts.insert(market_id, count);
        }
        let updated_markets: HashSet<u32> = if is_snapshot {
            known_markets.iter().copied().collect()
        } else {
            counts.keys().copied().collect()
        };
        let mut open_orders = self
            .open_orders
            .lock()
            .expect("Lighter open orders state poisoned");
        if is_snapshot {
            *open_orders = counts;
        } else {
            for (market_id, count) in counts {
                if let Some(prev) = open_orders.get(&market_id).copied() {
                    if prev >= 2
                        && count * 2 < prev
                        && !self.shrink_warned.swap(true, Ordering::Relaxed)
                    {
                        tracing::warn!(
                            "Lighter account_all_orders non-snapshot update shrank market {market_id} \
                             live orders {prev} -> {count}; if this recurs the feed may be sending \
                             per-order deltas rather than complete per-market sets"
                        );
                    }
                }
                open_orders.insert(market_id, count);
            }
        }
        let mut client_orders = self
            .client_orders
            .lock()
            .expect("Lighter client orders state poisoned");
        let mut client_order_markets = self
            .client_order_markets
            .lock()
            .expect("Lighter client order market state poisoned");
        let mut terminal_at = self
            .client_order_terminal_at
            .lock()
            .expect("Lighter terminal order state poisoned");
        let remove_ids: Vec<i64> = client_orders
            .iter()
            .filter_map(|(client_order_index, order)| {
                let market_id = client_order_markets.get(client_order_index)?;
                (updated_markets.contains(market_id)
                    && order.is_live()
                    && !seen_live_client_ids.contains(client_order_index))
                    .then_some(*client_order_index)
            })
            .collect();
        for client_order_index in remove_ids {
            client_orders.remove(&client_order_index);
            client_order_markets.remove(&client_order_index);
            terminal_at.remove(&client_order_index);
        }
        let now = std::time::Instant::now();
        for (market_id, order) in parsed_orders {
            if let Some(client_order_index) = order.client_order_index {
                if order.is_live() {
                    terminal_at.remove(&client_order_index);
                } else {
                    terminal_at.entry(client_order_index).or_insert(now);
                }
                client_order_markets.insert(client_order_index, market_id);
                client_orders.insert(client_order_index, order);
            }
        }
        // Purge terminal orders past their fallback-lookup window (see TERMINAL_ORDER_TTL).
        let expired: Vec<i64> = terminal_at
            .iter()
            .filter(|(_, at)| now.duration_since(**at) > TERMINAL_ORDER_TTL)
            .map(|(idx, _)| *idx)
            .collect();
        for client_order_index in expired {
            terminal_at.remove(&client_order_index);
            client_orders.remove(&client_order_index);
            client_order_markets.remove(&client_order_index);
        }
        if is_snapshot {
            self.all_orders_ready.store(true, Ordering::Release);
        }
    }

    fn open_orders_count(&self, market_id: u32) -> Option<usize> {
        if !self.all_orders_ready.load(Ordering::Acquire) {
            return None;
        }
        Some(
            *self
                .open_orders
                .lock()
                .expect("Lighter open orders state poisoned")
                .get(&market_id)
                .unwrap_or(&0),
            )
    }

    fn order_by_client(&self, client_order_index: i64) -> Option<RemoteOrder> {
        if !self.all_orders_ready.load(Ordering::Acquire) {
            return None;
        }
        self.client_orders
            .lock()
            .expect("Lighter client orders state poisoned")
            .get(&client_order_index)
            .cloned()
    }
}

#[derive(Default)]
struct BookFeedState {
    scan_notify: ArcSwapOption<Notify>,
    books: Mutex<HashMap<u32, LighterBook>>,
    reconnects: Mutex<HashMap<u32, Arc<Notify>>>,
}

impl BookFeedState {
    fn register_reconnect(&self, market_id: u32, reconnect: Arc<Notify>) {
        self.reconnects
            .lock()
            .expect("Lighter book reconnect state poisoned")
            .insert(market_id, reconnect);
    }

    fn apply(&self, market_id: u32, msg: &OrderBookMsgRef<'_>) -> bool {
        let mut books = self.books.lock().expect("Lighter book state poisoned");
        let book = books.entry(market_id).or_default();
        let applied = book.apply(msg);
        drop(books);
        if let Some(notify) = self.scan_notify.load_full() { notify.notify_one(); }
        applied
    }

    fn reset(&self, market_id: u32) {
        // Reset IN PLACE (never remove the entry): the published cell Arc is shared with
        // scan-path readers resolved at startup, and must stay identical across resyncs.
        if let Some(book) = self
            .books
            .lock()
            .expect("Lighter book state poisoned")
            .get_mut(&market_id)
        {
            book.reset_in_place();
        }
        if let Some(notify) = self.scan_notify.load_full() { notify.notify_one(); }
    }

    /// The lock-free published-book cell for a market (created on first use). Readers
    /// resolve this ONCE at startup and then load it without touching the books mutex —
    /// the scan hot path must not share a lock with the feed writer.
    fn cell(&self, market_id: u32) -> Arc<ArcSwapOption<OrderBook>> {
        self.books
            .lock()
            .expect("Lighter book state poisoned")
            .entry(market_id)
            .or_default()
            .cached
            .clone()
    }

    fn order_book(&self, market_id: u32) -> Option<OrderBook> {
        self.order_book_arc(market_id).map(|arc| (*arc).clone())
    }

    fn order_book_arc(&self, market_id: u32) -> Option<Arc<OrderBook>> {
        let books = self.books.lock().expect("Lighter book state poisoned");
        books.get(&market_id).and_then(LighterBook::load_cached)
    }

    fn request_reconnect(&self, market_id: u32) {
        self.reset(market_id);
        if let Some(reconnect) = self
            .reconnects
            .lock()
            .expect("Lighter book reconnect state poisoned")
            .get(&market_id)
            .cloned()
        {
            reconnect.notify_one();
        }
    }
}

#[derive(Default)]
struct LighterBook {
    bids: BTreeMap<Decimal, Decimal>,
    asks: BTreeMap<Decimal, Decimal>,
    initialized: bool,
    updated_at: Option<DateTime<Utc>>,
    source_at: Option<DateTime<Utc>>,
    engine_at: Option<DateTime<Utc>>,
    last_nonce: Option<i64>,
    last_offset: Option<u64>,
    /// Published snapshot cell. Behind an Arc so scan-path readers can hold the cell
    /// directly (resolved once at startup) and read it lock-free; it must survive
    /// `reset_in_place` across gap resyncs.
    cached: Arc<ArcSwapOption<OrderBook>>,
}

impl LighterBook {
    fn apply(&mut self, msg: &OrderBookMsgRef<'_>) -> bool {
        if !msg.is_snapshot() {
            if !self.initialized {
                // A delta before the subscribe snapshot must never seed the book.
                return false;
            }
            match msg.contiguity(self.last_nonce, self.last_offset) {
                BookUpdateContiguity::Apply => {}
                BookUpdateContiguity::SkipStale => return true, // duplicate/replay: keep book
                BookUpdateContiguity::Gap => return false,
            }
        }
        if msg.is_snapshot() || !self.initialized {
            self.bids.clear();
            self.asks.clear();
            self.initialized = true;
        }
        if !apply_levels(&mut self.bids, &msg.order_book.bids) || !apply_levels(&mut self.asks, &msg.order_book.asks) {
            return false; // the caller resets and resubscribes; nothing was published
        }
        self.updated_at = Some(Utc::now());
        self.source_at = msg.source_time_ms().and_then(DateTime::from_timestamp_millis);
        self.engine_at = msg.engine_time_ms().and_then(DateTime::from_timestamp_millis);
        self.last_nonce = msg.order_book.nonce.or(self.last_nonce);
        self.last_offset = msg.effective_offset().or(self.last_offset);
        self.refresh_cache();
        true
    }

    fn refresh_cache(&self) {
        if !self.initialized {
            self.cached.store(None);
            return;
        }
        let Some(ts) = self.updated_at else {
            self.cached.store(None);
            return;
        };
        let bids = self
            .bids
            .iter()
            .rev()
            .take(MAX_BOOK_LEVELS)
            .map(|(p, q)| (*p, *q));
        let asks = self
            .asks
            .iter()
            .take(MAX_BOOK_LEVELS)
            .map(|(p, q)| (*p, *q));
        let mut book = OrderBook::from_levels(bids, asks, self.source_at.unwrap_or(DateTime::<Utc>::UNIX_EPOCH), ts);
        book.engine_ts = self.engine_at;
        self.cached.store(Some(Arc::new(book)));
    }

    fn load_cached(&self) -> Option<Arc<OrderBook>> {
        self.cached.load_full()
    }

    /// Clear all state after a sequence gap, keeping the SAME published cell Arc (readers
    /// hold it directly); the next snapshot repopulates it.
    fn reset_in_place(&mut self) {
        self.bids.clear();
        self.asks.clear();
        self.initialized = false;
        self.updated_at = None;
        self.source_at = None;
        self.engine_at = None;
        self.last_nonce = None;
        self.last_offset = None;
        self.cached.store(None);
    }

    #[cfg(test)]
    fn to_order_book(&self) -> Option<OrderBook> {
        self.cached.load_full().map(|arc| (*arc).clone())
    }
}

pub struct LighterVenue {
    rest: RestClient,
    tx_ws: Arc<TxWebSocket>,
    signer: Arc<Signer>,
    nonce: Arc<NonceManager>,
    account_index: i64,
    api_key_index: i32,
    markets: std::collections::HashMap<MarketId, Wire>,
    fills: Arc<FillTracker>,
    account_feed: Arc<AccountFeedState>,
    book_feed: Arc<BookFeedState>,
    /// Per-market published-book cells resolved ONCE at construction so the scan hot path
    /// reads books with a plain ArcSwap load — no mutex shared with the feed writer.
    book_cells: std::collections::HashMap<u32, Arc<ArcSwapOption<OrderBook>>>,
    write_lock: AsyncMutex<()>,
    /// Monitoring-only venue (status subcommand): offline nonce stub, no tx-socket
    /// connect, no spawned streams. Submits hard-reject before touching the nonce.
    read_only: bool,
}

impl LighterVenue {
    pub fn set_scan_notify(&self, notify: Arc<Notify>) { self.book_feed.scan_notify.store(Some(notify)); }
    pub fn tx_ready(&self) -> bool { !self.read_only && self.tx_ws.is_ready() }
    pub fn account_index(&self) -> i64 { self.account_index }

    pub async fn new(
        base_url: &str,
        signers_dir: &Path,
        creds: LighterCreds,
        specs: &[MarketSpec],
    ) -> Result<Self> {
        let rest = RestClient::new(base_url, 0)?;
        let signer = Arc::new(Signer::load(
            signers_dir,
            base_url,
            &creds.api_private_key,
            creds.api_key_index,
            creds.account_index,
        )?);
        signer.check_client(creds.api_key_index)?;
        let nonce =
            Arc::new(NonceManager::init(&rest, creds.account_index, creds.api_key_index).await?);
        let ws_url = crate::lighter::ws::stream_url(base_url);
        let tx_ws = Arc::new(TxWebSocket::new(&ws_url));
        tx_ws
            .connect()
            .await
            .with_context(|| format!("preconnect Lighter tx websocket {ws_url}"))?;
        let fills = Arc::new(FillTracker::default());
        let account_feed = Arc::new(AccountFeedState { account_index: Some(creds.account_index), ..Default::default() });
        let book_feed = Arc::new(BookFeedState::default());
        let known_markets: Vec<u32> = specs.iter().map(|s| s.lighter_market_id).collect();
        spawn_order_book_stream(ws_url.clone(), specs, book_feed.clone());
        spawn_account_all_positions_stream(
            ws_url.clone(),
            creds.account_index,
            known_markets.clone(),
            account_feed.clone(),
        );
        spawn_account_all_stream(
            ws_url.clone(),
            signer.clone(),
            creds.api_key_index,
            creds.account_index,
            fills.clone(),
        );
        spawn_user_stats_stream(ws_url.clone(), creds.account_index, account_feed.clone());
        spawn_account_all_orders_stream(
            ws_url.clone(),
            signer.clone(),
            creds.api_key_index,
            creds.account_index,
            known_markets,
            account_feed.clone(),
        );

        let markets = specs
            .iter()
            .map(|s| {
                (
                    s.market_id.clone(),
                    Wire {
                        market_index: s.lighter_market_id as i32,
                        size_decimals: s.lighter_size_decimals,
                        price_decimals: s.lighter_price_decimals,
                    },
                )
            })
            .collect();
        let book_cells = specs
            .iter()
            .map(|s| (s.lighter_market_id, book_feed.cell(s.lighter_market_id)))
            .collect();
        Ok(LighterVenue {
            rest,
            tx_ws,
            signer,
            nonce,
            account_index: creds.account_index,
            api_key_index: creds.api_key_index,
            markets,
            fills,
            account_feed,
            book_feed,
            book_cells,
            write_lock: AsyncMutex::new(()),
            read_only: false,
        })
    }

    /// Read-only venue for the status/monitoring path: REST + signer (the
    /// accountActiveOrders auth token needs it) and the market wire context, but an
    /// OFFLINE nonce stub, an UNCONNECTED tx socket, and NO spawned streams — a status
    /// poll must never open an order-capable socket or pay wait_ready. Everything the
    /// report needs comes from REST; `submit_market_order_deferred_fill` hard-rejects.
    pub fn new_read_only(
        base_url: &str,
        signers_dir: &Path,
        creds: LighterCreds,
        specs: &[MarketSpec],
    ) -> Result<Self> {
        let rest = RestClient::new(base_url, 0)?;
        let signer = Arc::new(Signer::load(
            signers_dir,
            base_url,
            &creds.api_private_key,
            creds.api_key_index,
            creds.account_index,
        )?);
        let nonce = Arc::new(NonceManager::offline(
            creds.account_index,
            creds.api_key_index,
        ));
        let ws_url = crate::lighter::ws::stream_url(base_url);
        let tx_ws = Arc::new(TxWebSocket::new(&ws_url));
        let fills = Arc::new(FillTracker::default());
        let account_feed = Arc::new(AccountFeedState { account_index: Some(creds.account_index), ..Default::default() });
        let book_feed = Arc::new(BookFeedState::default());
        let markets = specs
            .iter()
            .map(|s| {
                (
                    s.market_id.clone(),
                    Wire {
                        market_index: s.lighter_market_id as i32,
                        size_decimals: s.lighter_size_decimals,
                        price_decimals: s.lighter_price_decimals,
                    },
                )
            })
            .collect();
        let book_cells = specs
            .iter()
            .map(|s| (s.lighter_market_id, book_feed.cell(s.lighter_market_id)))
            .collect();
        Ok(LighterVenue {
            rest,
            tx_ws,
            signer,
            nonce,
            account_index: creds.account_index,
            api_key_index: creds.api_key_index,
            markets,
            fills,
            account_feed,
            book_feed,
            book_cells,
            write_lock: AsyncMutex::new(()),
            read_only: true,
        })
    }

    fn wire(&self, market: &MarketId) -> Result<&Wire> {
        self.markets
            .get(market)
            .ok_or_else(|| anyhow!("no Lighter wire context for {market}"))
    }

    pub async fn wait_ready(&self, market: &MarketId, timeout: Duration) -> Result<()> {
        let wire = self.wire(market)?.clone();
        let market_id = wire.market_index as u32;
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let book_ready = self.book_feed.order_book(market_id).is_some();
            let position_ready = self.account_feed.position(market_id).is_some();
            let balance_ready = self.account_feed.available_balance().is_some();
            let orders_ready = self.account_feed.open_orders_count(market_id).is_some();
            if book_ready && position_ready && balance_ready && orders_ready {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                bail!(
                    "Lighter websocket state not ready for {market}: book={} position={} user_stats={} all_orders={}",
                    book_ready,
                    position_ready,
                    balance_ready,
                    orders_ready
                );
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    pub fn order_book(&self, market: &MarketId) -> Result<OrderBook> {
        self.order_book_arc(market).map(|arc| (*arc).clone())
    }

    /// Lock-free scan-path read WITHOUT the snapshot clone: the published Arc is
    /// returned directly, so the scan loop can also use pointer identity as its
    /// book-change detector (each applied update stores a fresh Arc; resets store None).
    pub fn order_book_arc(&self, market: &MarketId) -> Result<Arc<OrderBook>> {
        let wire = self.wire(market)?;
        // The cell was resolved at construction, so this is a plain ArcSwap load —
        // never contends with the feed writer's books mutex.
        if let Some(cell) = self.book_cells.get(&(wire.market_index as u32)) {
            return cell
                .load_full()
                .ok_or_else(|| anyhow!("Lighter order_book websocket not ready for {market}"));
        }
        self.book_feed
            .order_book_arc(wire.market_index as u32)
            .ok_or_else(|| anyhow!("Lighter order_book websocket not ready for {market}"))
    }

    pub fn request_order_book_reconnect(&self, market: &MarketId) -> Result<()> {
        let wire = self.wire(market)?;
        self.book_feed.request_reconnect(wire.market_index as u32);
        Ok(())
    }

    pub async fn submit_market_order(
        &self,
        market: &MarketId,
        side: Side,
        qty: Decimal,
        price_bound: Decimal,
        reduce_only: bool,
    ) -> SubmitOutcome {
        let (outcome, pending_fill) = self
            .submit_market_order_deferred_fill(market, side, qty, price_bound, reduce_only)
            .await;
        match (outcome, pending_fill) {
            (
                SubmitOutcome::Accepted {
                    raw,
                    client_order_index,
                    tx_hash,
                    nonce,
                    ..
                },
                Some(pending_fill),
            ) => {
                let fill = pending_fill.wait(Duration::from_secs(10)).await;
                SubmitOutcome::Accepted {
                    raw,
                    client_order_index,
                    tx_hash,
                    nonce,
                    fill,
                }
            }
            (outcome, _) => outcome,
        }
    }

    pub async fn submit_market_order_deferred_fill(
        &self,
        market: &MarketId,
        side: Side,
        qty: Decimal,
        price_bound: Decimal,
        reduce_only: bool,
    ) -> (SubmitOutcome, Option<PendingFill>) {
        // Hard gate BEFORE any nonce/sign work: a read-only (monitoring) venue carries
        // an offline nonce stub that must never sign a live order.
        if self.read_only {
            return (
                SubmitOutcome::Rejected {
                    reason: "read-only LighterVenue (status/monitoring): order submission disabled"
                        .to_string(),
                },
                None,
            );
        }
        let wire = match self.wire(market) {
            Ok(w) => w.clone(),
            Err(e) => {
                return (
                    SubmitOutcome::Rejected {
                        reason: e.to_string(),
                    },
                    None,
                );
            }
        };
        let client_order_index = random_client_order_index(market, side);
        let fill_rx = self.fills.register(client_order_index);
        let base_amount = match raw_amount(qty, wire.size_decimals) {
            Ok(v) => v,
            Err(e) => {
                self.fills.unregister(client_order_index);
                return (
                    SubmitOutcome::Rejected {
                        reason: e.to_string(),
                    },
                    None,
                );
            }
        };
        let price = match raw_price(price_bound, wire.price_decimals, side) {
            Ok(v) => v,
            Err(e) => {
                self.fills.unregister(client_order_index);
                return (
                    SubmitOutcome::Rejected {
                        reason: e.to_string(),
                    },
                    None,
                );
            }
        };
        let (result, tx_hash, reserved_nonce) = {
            // Keep nonce reservation, native signing and websocket write in the same critical
            // section. Lighter nonces are consumed in send order; allowing two callers to sign
            // concurrently can invert nonce order before they reach `TxWebSocket::send_batch`.
            let _write_guard = self.write_lock.lock().await;
            let nonce = self.nonce.next();
            let signed = match self.signer.sign_create_order(
                wire.market_index,
                client_order_index,
                base_amount,
                price,
                matches!(side, Side::Sell),
                ORDER_TYPE_MARKET,
                TIF_IMMEDIATE_OR_CANCEL,
                reduce_only,
                NIL_TRIGGER_PRICE,
                DEFAULT_IOC_EXPIRY,
                nonce,
                self.api_key_index,
            ) {
                Ok(tx) => tx,
                Err(e) => {
                    self.nonce.acknowledge_failure();
                    self.fills.unregister(client_order_index);
                    return (
                        SubmitOutcome::Rejected {
                            reason: e.to_string(),
                        },
                        None,
                    );
                }
            };
            let tx_hash = signed.tx_hash.clone();
            let result = self
                .tx_ws
                .send_batch(&[signed.tx_type], &[signed.tx_info])
                .await;
            match result.status {
                TxSendStatus::NotSent => self.nonce.rollback(1),
                TxSendStatus::Unknown | TxSendStatus::Ok => {}
                // Not every reject uses the nonce (a rate limit does not): re-read it, as XEMM does.
                TxSendStatus::Rejected => { let _ = self.nonce.hard_refresh(&self.rest).await; }
            }
            (result, tx_hash, nonce)
        };
        match result.status {
            TxSendStatus::Ok => {
                let pending_fill = PendingFill {
                    fills: self.fills.clone(),
                    account_feed: self.account_feed.clone(),
                    market_id: wire.market_index as u32,
                    account_index: self.account_index,
                    client_order_index,
                    side,
                    expected_qty: qty,
                    rx: fill_rx,
                    progress: FillProgress::default(),
                };
                (
                    SubmitOutcome::Accepted {
                        raw: serde_json::json!({
                            "code": result.code,
                            "message": result.message,
                            "client_order_index": client_order_index,
                            "quota_remaining": result.quota_remaining,
                        })
                        .to_string(),
                        client_order_index,
                        tx_hash,
                        nonce: reserved_nonce,
                        fill: None,
                    },
                    Some(pending_fill),
                )
            }
            TxSendStatus::Rejected => {
                self.fills.unregister(client_order_index);
                let reason = format!("Lighter reject code={} {}", result.code, result.message);
                if lighter_nonce_reject(result.code, &result.message) {
                    (SubmitOutcome::RejectedNonceStale { reason }, None)
                } else {
                    (SubmitOutcome::Rejected { reason }, None)
                }
            }
            TxSendStatus::NotSent => {
                self.fills.unregister(client_order_index);
                (
                    SubmitOutcome::Rejected {
                        reason: format!("Lighter tx not sent: {}", result.message),
                    },
                    None,
                )
            }
            TxSendStatus::Unknown => {
                let pending = PendingFill { fills: self.fills.clone(), account_feed: self.account_feed.clone(),
                    market_id: wire.market_index as u32, account_index: self.account_index,
                    client_order_index, side, expected_qty: qty, rx: fill_rx, progress: FillProgress::default() };
                (SubmitOutcome::Unknown { reason: format!("Lighter tx unknown: {}", result.message),
                    client_order_index, tx_hash, nonce: reserved_nonce }, Some(pending))
            }
        }
    }

    pub async fn position_qty(&self, market: &MarketId) -> Result<Decimal> {
        self.rest_position_qty(market).await
    }

    pub async fn rest_position_qty(&self, market: &MarketId) -> Result<Decimal> {
        Ok(self.account_snapshot(market).await?.position_qty)
    }

    pub async fn account_snapshot(&self, market: &MarketId) -> Result<LighterAccountSnapshot> {
        let wire = self.wire(market)?;
        let raw = self.rest.account_raw(self.account_index).await?;
        let account = account_root(&raw)?;
        Ok(LighterAccountSnapshot {
            position_qty: account_position_qty(account, wire.market_index as u32)?,
            available_usdc: account_available_usdc(account)?,
            account_value_usdc: account_value_usdc(account),
            unrealized_pnl_usdc: account_unrealized_pnl(account),
        })
    }

    pub fn ws_position_qty(&self, market: &MarketId) -> Result<Decimal> {
        let wire = self.wire(market)?;
        self.account_feed
            .position(wire.market_index as u32)
            .ok_or_else(|| {
                anyhow!("Lighter account_all_positions websocket position not ready for {market}")
            })
    }

    /// Fill-matching health counters for the periodic status log (rising `unmatched` or
    /// `timeouts` means the trades stream and our client-order registry are drifting).
    pub fn fill_tracker_stats(&self) -> FillTrackerStats {
        self.fills.stats()
    }

    pub async fn available_usdc(&self) -> Result<Decimal> {
        self.rest_available_usdc().await
    }

    pub async fn rest_available_usdc(&self) -> Result<Decimal> {
        let raw = self.rest.account_raw(self.account_index).await?;
        account_available_usdc(account_root(&raw)?)
    }

    /// Available margin plus marked equity from a single account payload — the same
    /// REST call as `rest_available_usdc`, so callers get equity for free.
    pub async fn rest_margin_snapshot(&self) -> Result<LighterMarginSnapshot> {
        let raw = self.rest.account_raw(self.account_index).await?;
        let account = account_root(&raw)?;
        let equity_usdc = match (account_value_usdc(account), account_unrealized_pnl(account)) {
            (Some(value), Some(upnl)) => Some(value + upnl),
            _ => None,
        };
        Ok(LighterMarginSnapshot {
            available_usdc: account_available_usdc(account)?,
            equity_usdc,
        })
    }

    pub fn ws_available_usdc(&self) -> Result<Decimal> {
        self.account_feed
            .available_balance()
            .ok_or_else(|| anyhow!("Lighter user_stats websocket available balance not ready"))
    }

    pub async fn open_orders_count(&self, market: &MarketId) -> Result<usize> {
        self.rest_open_orders_count(market).await
    }

    pub async fn rest_open_orders_count(&self, market: &MarketId) -> Result<usize> {
        let wire = self.wire(market)?;
        let auth = generate_ws_auth_token(&self.signer, self.api_key_index)?;
        let orders = self
            .rest
            .account_active_orders(self.account_index, wire.market_index as u32, &auth)
            .await?;
        Ok(orders.into_iter().filter(RemoteOrder::is_live).count())
    }

    pub fn ws_open_orders_count(&self, market: &MarketId) -> Result<usize> {
        let wire = self.wire(market)?;
        self.account_feed
            .open_orders_count(wire.market_index as u32)
            .ok_or_else(|| anyhow!("Lighter account_all_orders websocket not ready for {market}"))
    }

    /// Cold, identity-matched resolution. Exhausted/missing history is never no-fill evidence.
    pub async fn resolve_order_terminal(
        &self, market: &MarketId, client_order_index: i64, side: Side, timeout: Duration,
    ) -> Result<LighterFillConfirmation> {
        let market_index = self.wire(market)?.market_index as u32;
        let deadline = tokio::time::Instant::now() + timeout;
        let auth = generate_ws_auth_token(&self.signer, self.api_key_index)?;
        let mut last_error = None;
        loop {
            let pass = tokio::time::timeout_at(deadline, async {
                let cached = self.account_feed.order_by_client(client_order_index)
                    .filter(|order| remote_matches(order, self.account_index, market_index, client_order_index, side));
                let mut terminal = cached.filter(RemoteOrder::is_terminal);
                if terminal.is_none() {
                    let mut cursor: Option<String> = None;
                    let mut cursors = HashSet::new();
                    for _ in 0..crate::lighter::rest::HISTORY_PAGES {
                        let page = self.rest.account_inactive_orders(self.account_index, market_index,
                            &auth, cursor.as_deref()).await?;
                        let rows = page.get("orders").and_then(serde_json::Value::as_array)
                            .context("Lighter inactive-order history has no orders array")?;
                        for row in rows {
                            let Ok(order) = serde_json::from_value::<RemoteOrder>(row.clone()) else { continue };
                            if remote_matches(&order, self.account_index, market_index, client_order_index, side)
                                && order.is_terminal() { terminal = Some(order); break; }
                        }
                        if terminal.is_some() { break; }
                        cursor = next_history_cursor(&page);
                        let Some(next) = cursor.as_ref() else { break; };
                        anyhow::ensure!(cursors.insert(next.clone()), "repeated Lighter order-history cursor");
                    }
                }
                if let Some(order) = terminal {
                    let filled = remote_order_filled_qty(&order).context("terminal Lighter order lacks filled quantity")?;
                    anyhow::ensure!(filled >= Decimal::ZERO, "negative terminal filled quantity");
                    if filled == Decimal::ZERO {
                        return Ok(Some(LighterFillConfirmation { fee_evidence:Vec::new(), fill: Some(FillSummary::zero()),
                            terminal_order: Some(order), filled_qty: Decimal::ZERO }));
                    }
                    let order_index = order.order_index.context("terminal Lighter order lacks order index")?;
                    let mut cursor: Option<String> = None;
                    let mut cursors = HashSet::new();
                    let mut seen = HashSet::new();
                    let (mut qty, mut notional, mut fees) = (Decimal::ZERO, Decimal::ZERO, Decimal::ZERO);
                    let mut known_fee = true;
                    let mut fee_evidence = Vec::new();
                    loop {
                        let page = self.rest.trades_by_order(self.account_index, order_index, &auth, cursor.as_deref()).await?;
                        let trades = page.get("trades").and_then(serde_json::Value::as_array)
                            .context("Lighter trade history has no trades array")?;
                        for row in trades {
                            let trade: TradePayload = serde_json::from_value(row.clone())?;
                            let (account, own_id) = match side {
                                Side::Sell => (trade.ask_account_id, trade.ask_id),
                                Side::Buy => (trade.bid_account_id, trade.bid_id),
                            };
                            if account.is_some_and(|account| account != self.account_index)
                                || !(own_id == Some(order_index) || trade_matches_side(&trade, client_order_index, side)) { continue; }
                            let Some(key) = fill_identity(&trade) else { known_fee=false; continue; };
                            if !seen.insert(key) { continue; }
                            let q = trade.size.as_deref().and_then(|q| q.parse::<Decimal>().ok())
                                .filter(|q| *q > Decimal::ZERO).context("invalid Lighter historical fill size")?;
                            let price = trade.price.as_deref().and_then(|p| p.parse::<Decimal>().ok())
                                .filter(|p| *p > Decimal::ZERO).context("invalid Lighter historical fill price")?;
                            let amount = match trade.usd_amount.as_deref() {
                                Some(value) => value.parse::<Decimal>().context("invalid Lighter historical notional")?,
                                None => q * price,
                            };
                            anyhow::ensure!(amount > Decimal::ZERO, "nonpositive Lighter historical notional");
                            qty += q;
                            notional += amount;
                            let evidence = trade_fee_evidence(&trade,side,client_order_index,amount,"rest_trades");
                            match evidence.fee_usd {
                                Some(fee) => fees += fee,
                                None => known_fee = false,
                            }
                            fee_evidence.push(evidence);
                        }
                        cursor = next_history_cursor(&page);
                        let Some(next) = cursor.as_ref() else { break; };
                        anyhow::ensure!(cursors.insert(next.clone()), "repeated Lighter trade-history cursor");
                    }
                    let fill = if qty == filled {
                        FillSummary::from_qty_notional(qty, notional, fees).map(|fill| fill.with_fee_provenance(
                            if known_fee { FeeProvenance::Venue } else { FeeProvenance::Unknown }))
                    } else { None };
                    return Ok(Some(LighterFillConfirmation { fee_evidence, fill,
                        terminal_order: Some(order), filled_qty: filled }));
                }
                Ok::<_, anyhow::Error>(None)
            }).await;
            // A failed read is "not yet", never evidence: retry until the deadline, once a second
            // (XEMM polls every 2 s).
            match pass {
                Ok(Ok(Some(confirmation))) => return Ok(confirmation),
                Ok(Ok(None)) => {}
                Ok(Err(error)) => last_error = Some(error),
                Err(_) => return Err(last_error.unwrap_or_else(|| anyhow!("no terminal order in its history"))
                    .context("Lighter order remains unresolved at confirmation deadline")),
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }

    pub async fn refresh_nonce(&self) -> Result<()> {
        let _write_guard = self.write_lock.lock().await;
        self.nonce.hard_refresh(&self.rest).await
    }
}

fn remote_matches(order: &RemoteOrder, account: i64, market: u32, client: i64, side: Side) -> bool {
    order.client_order_index == Some(client) && order.market_index == Some(market)
        && order.owner_account_index == Some(account)
        && order.is_ask == Some(matches!(side, Side::Sell))
}

fn next_history_cursor(page: &serde_json::Value) -> Option<String> {
    page.get("next_cursor").and_then(serde_json::Value::as_str)
        .filter(|cursor| !cursor.is_empty()).map(str::to_string)
}

fn lighter_nonce_reject(code: i64, message: &str) -> bool {
    code == 21104 || message.to_ascii_lowercase().contains("invalid nonce")
}

/// Fail-closed envelope selection: an "accounts" key that is present but not a non-empty
/// array is a malformed payload (Err), never silently read as the legacy flat shape — the
/// legacy fallback applies only when the key is genuinely absent.
fn account_root(raw: &serde_json::Value) -> Result<&serde_json::Value> {
    match raw.get("accounts") {
        None => Ok(raw),
        Some(accounts) => accounts.as_array().and_then(|a| a.first()).ok_or_else(|| {
            anyhow!("Lighter account payload 'accounts' is not a non-empty array: {raw}")
        }),
    }
}

/// A missing positions array means a flat account (mirrors `account_unrealized_pnl`);
/// a matched row whose position magnitude cannot be parsed is an error — fabricating a
/// zero here would make reconciliation treat a live position as flat.
fn account_position_qty(account: &serde_json::Value, market_id: u32) -> Result<Decimal> {
    let Some(rows) = account.get("positions").and_then(|p| p.as_array()) else {
        return Ok(Decimal::ZERO);
    };
    for p in rows {
        if p.get("market_id").and_then(|m| m.as_u64()) == Some(market_id as u64) {
            let sign = p.get("sign").and_then(|x| x.as_i64()).unwrap_or(1);
            return signed_position_json_dec(p.get("position"), sign).ok_or_else(|| {
                anyhow!("Lighter position row for market {market_id} has no parseable position: {p}")
            });
        }
    }
    Ok(Decimal::ZERO)
}

fn account_available_usdc(account: &serde_json::Value) -> Result<Decimal> {
    value_dec(account.get("available_capital"))
        .or_else(|| value_dec(account.get("available_balance")))
        .or_else(|| value_dec(account.get("available")))
        .ok_or_else(|| anyhow!("Lighter account payload has no parseable available balance"))
}

fn account_unrealized_pnl(account: &serde_json::Value) -> Option<Decimal> {
    let Some(rows) = account.get("positions").and_then(|p| p.as_array()) else {
        return Some(Decimal::ZERO);
    };
    let mut total = Decimal::ZERO;
    for p in rows {
        let sign = p.get("sign").and_then(|x| x.as_i64()).unwrap_or(1);
        // Unparseable position magnitude: cannot even tell whether the row is flat, so
        // the account is unmarkable — same fail-closed rule as the uPnL read below.
        if signed_position_json_dec(p.get("position"), sign)? == Decimal::ZERO {
            continue;
        }
        // A nonzero position without a parseable mark makes the whole account
        // unmarkable: report None rather than silently under-counting equity.
        total += value_dec(p.get("unrealized_pnl"))?;
    }
    Some(total)
}

fn account_value_usdc(account: &serde_json::Value) -> Option<Decimal> {
    value_dec(account.get("portfolio_value"))
        .or_else(|| value_dec(account.get("account_value")))
        .or_else(|| value_dec(account.get("total_account_value")))
        .or_else(|| value_dec(account.get("total_collateral_value")))
        .or_else(|| value_dec(account.get("collateral")))
        .or_else(|| value_dec(account.get("total_collateral")))
        .or_else(|| value_dec(account.get("equity")))
        .or_else(|| value_dec(account.get("balance")))
}

fn apply_account_all_positions(
    account_feed: &AccountFeedState,
    known_markets: &[u32],
    msg: &AccountAllPositionsMsg,
) {
    let mut seen = HashSet::new();

    // Lighter's position updates are sparse: a market absent from an update means unchanged,
    // not flat. Only the explicitly tagged subscription snapshot is authoritative enough to
    // initialize missing configured markets to zero.
    for (market, position) in &msg.positions {
        if let Ok(market_id) = market.parse::<u32>() {
            seen.insert(market_id);
            account_feed.set_position(market_id, signed_position_payload_dec(position));
        }
    }

    if msg.is_snapshot() {
        for market_id in known_markets {
            if !seen.contains(market_id) {
                account_feed.set_position(*market_id, Decimal::ZERO);
            }
        }
        account_feed.mark_account_all_ready();
    }
}

fn spawn_account_all_positions_stream(
    ws_url: String,
    account_index: i64,
    known_markets: Vec<u32>,
    account_feed: Arc<AccountFeedState>,
) {
    let channel = format!("account_all_positions/{account_index}");
    let mut opts = SubscribeOptions::new(&ws_url, "lighter-account-all-positions", vec![channel]);
    opts.data_timeout = None;
    opts.frame_timeout = 90.0;
    tokio::spawn(async move {
        subscribe_loop(
            opts,
            None,
            move |frame| {
                let raw = frame.raw;
                if let Ok(msg) = serde_json::from_str::<AccountAllPositionsMsg>(raw) {
                    apply_account_all_positions(&account_feed, &known_markets, &msg);
                }
            },
            || {},
        )
        .await;
    });
}

fn spawn_account_all_stream(
    ws_url: String,
    signer: Arc<Signer>,
    api_key_index: i32,
    account_index: i64,
    fills: Arc<FillTracker>,
) {
    let channel = format!("account_all/{account_index}");
    let auth_channel = channel.clone();
    let mut opts = SubscribeOptions::new(&ws_url, "lighter-account-all", vec![channel]);
    opts.data_timeout = None;
    opts.frame_timeout = 90.0;
    tokio::spawn(async move {
        subscribe_loop_authed(
            opts,
            move || match generate_ws_auth_token(&signer, api_key_index) {
                Ok(token) => HashMap::from([(auth_channel.clone(), token)]),
                Err(e) => {
                    tracing::warn!("lighter-account-all auth token generation failed: {e:#}");
                    HashMap::new()
                }
            },
            move |frame| {
                let raw = frame.raw;
                if let Ok(msg) = serde_json::from_str::<AccountAllMsg>(raw) {
                    for trades in msg.trades.values() {
                        for trade in trades {
                            fills.on_trade(trade.clone());
                        }
                    }
                }
            },
        )
        .await;
    });
}

fn spawn_user_stats_stream(
    ws_url: String,
    account_index: i64,
    account_feed: Arc<AccountFeedState>,
) {
    let channel = format!("user_stats/{account_index}");
    let mut opts = SubscribeOptions::new(&ws_url, "lighter-user-stats", vec![channel]);
    opts.data_timeout = None;
    opts.frame_timeout = 90.0;
    tokio::spawn(async move {
        subscribe_loop(
            opts,
            None,
            move |frame| {
                let raw = frame.raw;
                if let Ok(msg) = serde_json::from_str::<UserStatsMsg>(raw) {
                    account_feed.set_user_stats(value_dec(msg.stats.available_balance.as_ref()));
                }
            },
            || {},
        )
        .await;
    });
}

fn spawn_account_all_orders_stream(
    ws_url: String,
    signer: Arc<Signer>,
    api_key_index: i32,
    account_index: i64,
    known_markets: Vec<u32>,
    account_feed: Arc<AccountFeedState>,
) {
    let channel = format!("account_all_orders/{account_index}");
    let auth_channel = channel.clone();
    let mut opts = SubscribeOptions::new(&ws_url, "lighter-account-all-orders", vec![channel]);
    opts.data_timeout = None;
    opts.frame_timeout = 90.0;
    tokio::spawn(async move {
        subscribe_loop_authed(
            opts,
            move || match generate_ws_auth_token(&signer, api_key_index) {
                Ok(token) => HashMap::from([(auth_channel.clone(), token)]),
                Err(e) => {
                    tracing::warn!(
                        "lighter-account-all-orders auth token generation failed: {e:#}"
                    );
                    HashMap::new()
                }
            },
            move |frame| {
                let raw = frame.raw;
                // Cold path: keep the owned-Value handler contract, just parse locally.
                let Ok(data) = serde_json::from_str::<serde_json::Value>(raw) else {
                    return;
                };
                let orders = data.get("orders").unwrap_or(&serde_json::Value::Null);
                let is_snapshot = data
                    .get("type")
                    .and_then(|v| v.as_str())
                    .is_some_and(|kind| kind.starts_with("subscribed/"));
                account_feed.set_open_orders_for_markets(&known_markets, orders, is_snapshot);
            },
        )
        .await;
    });
}

fn spawn_order_book_stream(ws_url: String, specs: &[MarketSpec], book_feed: Arc<BookFeedState>) {
    for spec in specs.iter().cloned() {
        let market_id = spec.lighter_market_id;
        let channel = format!("order_book/{market_id}");
        let mut opts =
            SubscribeOptions::new(&ws_url, &format!("lighter-order-book-{market_id}"), vec![channel]);
        // Every sequence-gap resync blanks the book and then waits out the reconnect
        // delay; with max_book_staleness_ms=2000 the 5s default base is a ≥5-6s full
        // scan/trading blackout per gap (the Aster feed's base is 0.25s). Healthy
        // sessions reset the delay to base, so this only shortens routine resyncs —
        // consecutive failures still escalate toward reconnect_max.
        opts.reconnect_base = 0.5;
        let books = book_feed.clone();
        let books_for_disconnect = book_feed.clone();
        let reconnect = Arc::new(Notify::new());
        book_feed.register_reconnect(market_id, reconnect.clone());
        let reconnect_on_gap = reconnect.clone();
        tokio::spawn(async move {
            subscribe_loop(
                opts,
                Some(reconnect),
                move |frame| {
                    let raw = frame.raw;
                    if let Ok(msg) = serde_json::from_str::<OrderBookMsgRef<'_>>(raw) {
                        if !books.apply(market_id, &msg) {
                            tracing::warn!(
                                "Lighter order_book sequence gap for market {}; reconnecting for fresh snapshot",
                                market_id
                            );
                            books.reset(market_id);
                            reconnect_on_gap.notify_one();
                        }
                    }
                },
                move || {
                    books_for_disconnect.reset(market_id);
                },
            )
            .await;
        });
    }
}

fn value_dec(v: Option<&serde_json::Value>) -> Option<Decimal> {
    match v? {
        serde_json::Value::String(s) => s.parse().ok(),
        serde_json::Value::Number(n) => n.to_string().parse().ok(),
        _ => None,
    }
}

fn remote_order_filled_qty(order: &RemoteOrder) -> Option<Decimal> {
    order
        .filled_base_amount
        .as_deref()
        .and_then(|s| s.parse::<Decimal>().ok())
        .filter(|qty| *qty >= Decimal::ZERO)
}

/// `false` when any level is unparseable: skipping it would leave a stale level in the book,
/// so the frame is a gap (reset + fresh snapshot), as in the XEMM hedge worker.
fn apply_levels(side: &mut BTreeMap<Decimal, Decimal>, levels: &[PriceLevelRef<'_>]) -> bool {
    for level in levels {
        let (Ok(px), Ok(qty)) = (level.price.parse::<Decimal>(), level.size.parse::<Decimal>()) else {
            return false;
        };
        if px <= Decimal::ZERO {
            continue;
        }
        if qty <= Decimal::ZERO {
            side.remove(&px);
        } else {
            side.insert(px, qty);
        }
    }
    true
}

fn signed_position_payload_dec(p: &crate::lighter::messages::PositionPayload) -> Decimal {
    let mag = p
        .position
        .as_deref()
        .and_then(|s| s.parse::<Decimal>().ok())
        .unwrap_or(Decimal::ZERO)
        .abs();
    if p.sign.is_some_and(|sign| sign < 0) {
        -mag
    } else {
        mag
    }
}

/// `None` when the position magnitude is absent or unparseable — callers decide whether
/// that means "malformed row" (error) or "account unmarkable" (propagate None); a silent
/// zero here masqueraded parse failures as flat positions.
fn signed_position_json_dec(position: Option<&serde_json::Value>, sign: i64) -> Option<Decimal> {
    let mag = value_dec(position)?.abs();
    Some(if sign < 0 { -mag } else { mag })
}

fn trade_matches_side(trade: &TradePayload, client_order_index: i64, side: Side) -> bool {
    match side {
        Side::Sell => trade.ask_client_id == Some(client_order_index),
        Side::Buy => trade.bid_client_id == Some(client_order_index),
    }
}

fn fill_identity(trade: &TradePayload) -> Option<u128> {
    // Price/quantity hashes collapse distinct identical fills and cannot establish identity.
    trade.trade_id.filter(|id| *id >= 0).map(|id| id as u128)
}

/// A fill's selected fee is a signed rate in millionths of USD notional. Lighter serializes
/// both fee fields `omitempty`, so omission means zero; explicit null/malformed stays unknown.
/// Every Lighter order this bot sends is IOC, so its own side is the taker: a missing
/// `is_maker_ask` selects the taker fee, and a fill flagged maker is contradictory evidence
/// whose fee stays unknown (the same rule as the XEMM hedge worker).
fn trade_fee_evidence(trade: &TradePayload, side: Side, client: i64, notional: Decimal, source: &str) -> FeeEvidence {
    let maker = trade.is_maker_ask.map(|ask| match side { Side::Sell=>ask, Side::Buy=>!ask }).unwrap_or(false);
    let selected = if maker { trade.maker_fee.as_ref() } else { trade.taker_fee.as_ref() };
    let rate = match selected { None => Some(Decimal::ZERO), Some(value) => value_dec(Some(value)) };
    FeeEvidence { trade_id:trade.trade_id, order_id:match side {Side::Sell=>trade.ask_id,Side::Buy=>trade.bid_id},
        client_order_index:client,maker:Some(maker),notional_usd:notional,fee_ticks:rate,
        fee_usd:if maker { None } else { rate.map(|rate|notional*rate/Decimal::from(1_000_000)) },
        event_time_ms:trade.event_time_ms(),source:source.to_string() }
}


fn raw_amount(qty: Decimal, decimals: u32) -> Result<i64> {
    if qty <= Decimal::ZERO {
        bail!("quantity must be positive");
    }
    let scale = Decimal::from(10u64.pow(decimals));
    let raw = (qty * scale)
        .round_dp_with_strategy(0, RoundingStrategy::ToZero)
        .to_i64()
        .ok_or_else(|| anyhow!("quantity raw amount overflow"))?;
    if raw <= 0 {
        bail!("quantity rounds to zero at {decimals} decimals");
    }
    Ok(raw)
}

fn raw_price(px: Decimal, decimals: u32, side: Side) -> Result<i32> {
    if px <= Decimal::ZERO {
        bail!("price must be positive");
    }
    let scale = Decimal::from(10u64.pow(decimals));
    let strat = match side {
        Side::Buy => RoundingStrategy::ToPositiveInfinity,
        Side::Sell => RoundingStrategy::ToNegativeInfinity,
    };
    let raw = (px * scale)
        .round_dp_with_strategy(0, strat)
        .to_i32()
        .ok_or_else(|| anyhow!("price raw amount overflow"))?;
    if raw <= 0 {
        bail!("price rounds to zero at {decimals} decimals");
    }
    Ok(raw)
}

/// Layout: 40 bits wall-clock ms | 7-bit counter | side bit. Collision-free ONLY within a
/// single process on one account (assumption: one submitting process per account at a time —
/// under `run` the taker submits only under XEMM's lease, while XEMM has nothing resting or in
/// flight; other live writers are refused by the market lock). Within a process, two ids collide
/// only if the 7-bit counter wraps inside one millisecond; the wrap guard below spins to
/// the next millisecond instead (128+ orders per ms never happens in practice — this is a
/// correctness backstop, not a hot path).
fn random_client_order_index(_market: &MarketId, side: Side) -> i64 {
    let now_ms = || {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0)
    };
    let mut millis = now_ms();
    let raw_counter = CLIENT_ORDER_COUNTER.fetch_add(1, Ordering::Relaxed);
    let counter = raw_counter & 0x7f;
    // Wrap guard: if this counter value was already used in the SAME millisecond (i.e. the
    // raw counter advanced 128+ since that millisecond started), wait out the millisecond.
    let last_wrap_ms = LAST_COUNTER_WRAP_MS.load(Ordering::Relaxed);
    if counter == 0 {
        if millis == last_wrap_ms {
            while now_ms() == millis {
                std::thread::yield_now();
            }
            millis = now_ms();
        }
        LAST_COUNTER_WRAP_MS.store(millis, Ordering::Relaxed);
    }
    let side_bit = if matches!(side, Side::Sell) { 1 } else { 0 };
    let idx = ((millis & 0x00ff_ffff_ffff) << 8) | (counter << 1) | side_bit;
    idx.min(MAX_CLIENT_ORDER_INDEX)
}

#[cfg(test)]
mod tests {
    use rust_decimal_macros::dec;
    use super::*;

    #[test]
    fn client_order_index_stays_in_lighter_range() {
        for _ in 0..128 {
            let idx = random_client_order_index(&MarketId("HYPE".to_string()), Side::Buy);
            assert!(idx >= 0);
            assert!(idx <= MAX_CLIENT_ORDER_INDEX);
        }
    }

    #[test]
    fn lighter_book_initial_update_and_delta_remove_level() {
        let initial: OrderBookMsgRef<'_> = serde_json::from_str(
            r#"{
                "type":"subscribed/order_book",
                "order_book":{
                    "nonce":100,
                    "bids":[{"price":"10.00","size":"2.50"}],
                    "asks":[{"price":"10.10","size":"1.25"}]
                }
            }"#,
        )
        .unwrap();
        let remove_bid: OrderBookMsgRef<'_> = serde_json::from_str(
            r#"{
                "type":"update/order_book",
                "order_book":{
                    "begin_nonce":100,
                    "nonce":101,
                    "bids":[{"price":"10.00","size":"0"}],
                    "asks":[{"price":"10.05","size":"3.00"}]
                }
            }"#,
        )
        .unwrap();
        let mut book = LighterBook::default();
        assert!(book.apply(&initial));
        let out = book.to_order_book().unwrap();
        assert_eq!(out.best_bid().unwrap().px, Decimal::new(1000, 2));
        assert_eq!(out.best_ask().unwrap().px, Decimal::new(1010, 2));
        assert!(book.apply(&remove_bid));
        let out = book.to_order_book().unwrap();
        assert!(out.best_bid().is_none());
        assert_eq!(out.best_ask().unwrap().px, Decimal::new(1005, 2));
    }

    #[test]
    fn lighter_book_treats_unparseable_level_as_gap() {
        let initial: OrderBookMsgRef<'_> = serde_json::from_str(
            r#"{"type":"subscribed/order_book","order_book":{"nonce":100,
                "bids":[{"price":"10.00","size":"2.50"}],"asks":[{"price":"10.10","size":"1.25"}]}}"#,
        )
        .unwrap();
        // Contiguous delta whose removal of the 10.00 bid cannot be parsed.
        let bad_size: OrderBookMsgRef<'_> = serde_json::from_str(
            r#"{"type":"update/order_book","order_book":{"begin_nonce":100,"nonce":101,
                "bids":[{"price":"10.00","size":""}],"asks":[]}}"#,
        )
        .unwrap();
        let mut book = LighterBook::default();
        assert!(book.apply(&initial));
        assert!(!book.apply(&bad_size), "a skipped level would leave the stale 10.00 bid live");
    }

    #[test]
    fn lighter_book_rejects_nonce_gap_delta() {
        let initial: OrderBookMsgRef<'_> = serde_json::from_str(
            r#"{
                "type":"subscribed/order_book",
                "order_book":{
                    "nonce":100,
                    "bids":[{"price":"10.00","size":"2.50"}],
                    "asks":[{"price":"10.10","size":"1.25"}]
                }
            }"#,
        )
        .unwrap();
        // begin_nonce ahead of our position => missed updates => resync.
        let gap: OrderBookMsgRef<'_> = serde_json::from_str(
            r#"{
                "type":"update/order_book",
                "order_book":{
                    "begin_nonce":101,
                    "nonce":102,
                    "bids":[{"price":"10.01","size":"2.00"}],
                    "asks":[]
                }
            }"#,
        )
        .unwrap();
        let mut book = LighterBook::default();
        assert!(book.apply(&initial));
        assert!(!book.apply(&gap));
        let out = book.to_order_book().unwrap();
        assert_eq!(out.best_bid().unwrap().px, Decimal::new(1000, 2));
    }

    #[test]
    fn lighter_book_applies_forward_overlap_and_skips_stale_replay() {
        let initial: OrderBookMsgRef<'_> = serde_json::from_str(
            r#"{
                "type":"subscribed/order_book",
                "order_book":{
                    "nonce":100,
                    "bids":[{"price":"10.00","size":"2.50"}],
                    "asks":[{"price":"10.10","size":"1.25"}]
                }
            }"#,
        )
        .unwrap();
        // Overlap extending forward (99 -> 101 over our 100): absolute level sizes make the
        // re-stated portion idempotent, so this must apply.
        let overlap: OrderBookMsgRef<'_> = serde_json::from_str(
            r#"{
                "type":"update/order_book",
                "order_book":{
                    "begin_nonce":99,
                    "nonce":101,
                    "bids":[{"price":"10.01","size":"2.00"}],
                    "asks":[]
                }
            }"#,
        )
        .unwrap();
        // Fully-stale replay (ends at-or-before our position): dropped, no resync.
        let stale: OrderBookMsgRef<'_> = serde_json::from_str(
            r#"{
                "type":"update/order_book",
                "order_book":{
                    "begin_nonce":98,
                    "nonce":99,
                    "bids":[{"price":"10.00","size":"0"}],
                    "asks":[]
                }
            }"#,
        )
        .unwrap();
        let mut book = LighterBook::default();
        assert!(book.apply(&initial));
        assert!(book.apply(&overlap));
        let out = book.to_order_book().unwrap();
        assert_eq!(out.best_bid().unwrap().px, Decimal::new(1001, 2));
        assert!(book.apply(&stale));
        let out = book.to_order_book().unwrap();
        assert_eq!(
            out.best_bid().unwrap().px,
            Decimal::new(1001, 2),
            "stale replay must not mutate the book"
        );
    }

    #[test]
    fn lighter_book_rejects_delta_before_snapshot() {
        let delta: OrderBookMsgRef<'_> = serde_json::from_str(
            r#"{
                "type":"update/order_book",
                "order_book":{
                    "nonce":100,
                    "bids":[{"price":"10.00","size":"2.50"}],
                    "asks":[{"price":"10.10","size":"1.25"}]
                }
            }"#,
        )
        .unwrap();
        let mut book = LighterBook::default();
        assert!(!book.apply(&delta), "delta must never seed an uninitialized book");
        assert!(book.to_order_book().is_none());
    }

    #[test]
    fn lighter_book_rejects_offset_gap_without_nonce() {
        let initial: OrderBookMsgRef<'_> = serde_json::from_str(
            r#"{
                "type":"subscribed/order_book",
                "offset":100,
                "order_book":{
                    "bids":[{"price":"10.00","size":"2.50"}],
                    "asks":[{"price":"10.10","size":"1.25"}]
                }
            }"#,
        )
        .unwrap();
        let gap: OrderBookMsgRef<'_> = serde_json::from_str(
            r#"{
                "type":"update/order_book",
                "offset":102,
                "order_book":{
                    "bids":[{"price":"10.01","size":"2.00"}],
                    "asks":[]
                }
            }"#,
        )
        .unwrap();
        let mut book = LighterBook::default();
        assert!(book.apply(&initial));
        assert!(!book.apply(&gap));
    }

    #[test]
    fn lighter_book_rejects_delta_without_sequence_metadata() {
        let initial: OrderBookMsgRef<'_> = serde_json::from_str(
            r#"{
                "type":"subscribed/order_book",
                "order_book":{
                    "bids":[{"price":"10.00","size":"2.50"}],
                    "asks":[{"price":"10.10","size":"1.25"}]
                }
            }"#,
        )
        .unwrap();
        let unsequenced: OrderBookMsgRef<'_> = serde_json::from_str(
            r#"{
                "type":"update/order_book",
                "order_book":{
                    "bids":[{"price":"10.01","size":"2.00"}],
                    "asks":[]
                }
            }"#,
        )
        .unwrap();
        let mut book = LighterBook::default();
        assert!(book.apply(&initial));
        assert!(!book.apply(&unsequenced));
    }

    #[test]
    fn lighter_book_publishes_multiple_depth_levels() {
        let initial: OrderBookMsgRef<'_> = serde_json::from_str(
            r#"{
                "type":"subscribed/order_book",
                "order_book":{
                    "nonce":100,
                    "bids":[{"price":"10.00","size":"2.50"},{"price":"9.95","size":"1.00"}],
                    "asks":[{"price":"10.10","size":"1.25"},{"price":"10.15","size":"2.00"}]
                }
            }"#,
        )
        .unwrap();
        let mut book = LighterBook::default();
        assert!(book.apply(&initial));
        let out = book.to_order_book().unwrap();
        assert_eq!(out.bids.len(), 2);
        assert_eq!(out.asks.len(), 2);
        assert_eq!(out.bids[0].px, Decimal::new(1000, 2));
        assert_eq!(out.bids[1].px, Decimal::new(995, 2));
        assert_eq!(out.asks[0].px, Decimal::new(1010, 2));
        assert_eq!(out.asks[1].px, Decimal::new(1015, 2));
    }

    #[test]
    fn signed_position_payload_uses_decimal_not_float() {
        let position = crate::lighter::messages::PositionPayload {
            position: Some("0.8499999999999999777955395072".to_string()),
            sign: Some(-1),
            avg_entry_price: None,
        };
        assert_eq!(
            signed_position_payload_dec(&position),
            "-0.8499999999999999777955395072"
                .parse::<Decimal>()
                .unwrap()
        );
    }

    #[test]
    fn account_snapshot_helpers_parse_one_account_payload() {
        let raw = serde_json::json!({
            "accounts": [{
                "available_capital": "123.45",
                "portfolio_value": "234.56",
                "positions": [
                    {"market_id": 23, "position": "9.99", "sign": 1, "unrealized_pnl": "-1.25"},
                    {"market_id": 24, "position": "0.42", "sign": -1, "unrealized_pnl": "7.804932"}
                ]
            }]
        });
        let account = account_root(&raw).unwrap();

        assert_eq!(
            account_position_qty(account, 24).unwrap(),
            "-0.42".parse::<Decimal>().unwrap()
        );
        assert_eq!(account_position_qty(account, 99).unwrap(), Decimal::ZERO);
        assert_eq!(
            account_available_usdc(account).unwrap(),
            "123.45".parse::<Decimal>().unwrap()
        );
        assert_eq!(
            account_value_usdc(account),
            Some("234.56".parse::<Decimal>().unwrap())
        );
        assert_eq!(
            account_unrealized_pnl(account),
            Some("6.554932".parse::<Decimal>().unwrap())
        );
    }

    #[test]
    fn account_unrealized_pnl_fails_closed_and_skips_flat_rows() {
        // Nonzero position without a parseable unrealized_pnl -> whole account
        // unmarkable (None), never a silent partial sum.
        let missing = serde_json::json!({
            "positions": [
                {"market_id": 24, "position": "0.99", "sign": 1}
            ]
        });
        assert_eq!(account_unrealized_pnl(&missing), None);

        // Flat rows are ignored even when their unrealized_pnl is absent or junk.
        let flat = serde_json::json!({
            "positions": [
                {"market_id": 23, "position": "0.00", "sign": 1},
                {"market_id": 24, "position": "1.17", "sign": -1, "unrealized_pnl": "0.5"}
            ]
        });
        assert_eq!(
            account_unrealized_pnl(&flat),
            Some("0.5".parse::<Decimal>().unwrap())
        );

        // No positions array at all: a flat account has zero unrealized PnL.
        assert_eq!(
            account_unrealized_pnl(&serde_json::json!({})),
            Some(Decimal::ZERO)
        );
    }

    #[test]
    fn account_root_malformed_envelope_is_error() {
        // "accounts" present but not a non-empty array: malformed payload, not the
        // legacy flat shape — reading it as legacy would fabricate a flat/zero account.
        assert!(account_root(&serde_json::json!({"accounts": []})).is_err());
        assert!(account_root(&serde_json::json!({"accounts": "oops"})).is_err());
        assert!(account_root(&serde_json::json!({"accounts": null})).is_err());
    }

    #[test]
    fn account_root_absent_accounts_key_is_legacy_shape() {
        let raw = serde_json::json!({"available_capital": "10.5"});
        let account = account_root(&raw).unwrap();
        assert_eq!(
            account_available_usdc(account).unwrap(),
            "10.5".parse::<Decimal>().unwrap()
        );
    }

    #[test]
    fn account_position_qty_missing_positions_array_means_flat() {
        assert_eq!(
            account_position_qty(&serde_json::json!({}), 24).unwrap(),
            Decimal::ZERO
        );
    }

    #[test]
    fn account_position_qty_unparseable_matched_position_is_error() {
        let account = serde_json::json!({
            "positions": [
                {"market_id": 24, "position": "garbage", "sign": 1}
            ]
        });
        assert!(account_position_qty(&account, 24).is_err());
        // Other markets are unaffected by the malformed row.
        assert_eq!(account_position_qty(&account, 23).unwrap(), Decimal::ZERO);
    }

    #[test]
    fn account_available_usdc_unparseable_is_error() {
        assert!(account_available_usdc(&serde_json::json!({})).is_err());
        assert!(
            account_available_usdc(&serde_json::json!({"available_capital": "junk"})).is_err()
        );
    }

    #[test]
    fn account_all_positions_partial_empty_update_does_not_zero_existing_position() {
        let state = AccountFeedState::default();
        let known_markets = [24u32];
        let initial: AccountAllPositionsMsg = serde_json::from_value(serde_json::json!({
            "type": "subscribed/account_all_positions",
            "positions": {
                "24": {"position": "1.42", "sign": 1}
            }
        }))
        .unwrap();
        apply_account_all_positions(&state, &known_markets, &initial);
        assert_eq!(state.position(24), Some("1.42".parse::<Decimal>().unwrap()));

        let empty_partial: AccountAllPositionsMsg = serde_json::from_value(serde_json::json!({
            "type": "update/account_all_positions",
            "positions": {}
        }))
        .unwrap();
        apply_account_all_positions(&state, &known_markets, &empty_partial);
        assert_eq!(state.position(24), Some("1.42".parse::<Decimal>().unwrap()));

        let missing_hype_partial: AccountAllPositionsMsg =
            serde_json::from_value(serde_json::json!({
                "type": "update/account_all_positions",
                "positions": {
                    "25": {"position": "0.5", "sign": -1}
                }
            }))
            .unwrap();
        apply_account_all_positions(&state, &[24u32, 25u32], &missing_hype_partial);
        assert_eq!(state.position(24), Some("1.42".parse::<Decimal>().unwrap()));
        assert_eq!(state.position(25), Some("-0.5".parse::<Decimal>().unwrap()));
    }

    #[test]
    fn account_all_positions_full_snapshot_missing_known_market_initializes_zero() {
        let state = AccountFeedState::default();
        let snapshot: AccountAllPositionsMsg = serde_json::from_value(serde_json::json!({
            "type": "subscribed/account_all_positions",
            "positions": {}
        }))
        .unwrap();
        apply_account_all_positions(&state, &[24u32], &snapshot);
        assert_eq!(state.position(24), Some(Decimal::ZERO));
    }

    #[test]
    fn account_feed_counts_live_open_orders_by_known_market() {
        let state = AccountFeedState::default();
        let orders = serde_json::json!({
            "24": [
                {"status": "open", "client_order_index": 1},
                {"status": "filled", "client_order_index": 2, "filled_base_amount": "0.20"},
                {"client_order_index": 3}
            ],
            "25": [
                {"status": "canceled", "client_order_index": 4}
            ],
            "27": [{"status":"new-unknown-status","client_order_index":5}]
        });
        state.set_open_orders_for_markets(&[24, 25, 26, 27], &orders, true);
        assert_eq!(state.open_orders_count(24), Some(2));
        assert_eq!(state.open_orders_count(25), Some(0));
        assert_eq!(state.open_orders_count(26), Some(0));
        assert_eq!(state.open_orders_count(27), Some(1));
        assert_eq!(
            state
                .order_by_client(2)
                .and_then(|order| order.filled_base_amount),
            Some("0.20".to_string())
        );
    }

    #[test]
    fn account_feed_sparse_order_update_keeps_unmentioned_markets() {
        let state = AccountFeedState::default();
        let snapshot = serde_json::json!({
            "24": [{"status": "open", "client_order_index": 24}],
            "25": [{"status": "open", "client_order_index": 25}]
        });
        state.set_open_orders_for_markets(&[24, 25], &snapshot, true);
        assert_eq!(state.open_orders_count(24), Some(1));
        assert_eq!(state.open_orders_count(25), Some(1));

        let sparse_update = serde_json::json!({
            "24": []
        });
        state.set_open_orders_for_markets(&[24, 25], &sparse_update, false);
        assert_eq!(state.open_orders_count(24), Some(0));
        assert_eq!(state.open_orders_count(25), Some(1));
        assert!(state.order_by_client(24).is_none());
        assert!(state.order_by_client(25).is_some());
    }

    #[test]
    fn account_feed_purges_terminal_orders_after_ttl() {
        let state = AccountFeedState::default();
        // Terminal order arrives: kept for the wait_confirmed fallback window.
        let with_terminal = serde_json::json!({
            "24": [{"status": "filled", "client_order_index": 7}]
        });
        state.set_open_orders_for_markets(&[24], &with_terminal, true);
        assert!(state.order_by_client(7).is_some(), "terminal order queryable within TTL");

        // Age the entry past the TTL, then any subsequent feed update purges it.
        {
            let mut terminal_at = state.client_order_terminal_at.lock().unwrap();
            let aged = std::time::Instant::now()
                .checked_sub(TERMINAL_ORDER_TTL + std::time::Duration::from_secs(1))
                .expect("clock supports backdating in tests");
            for at in terminal_at.values_mut() {
                *at = aged;
            }
        }
        let unrelated_update = serde_json::json!({ "24": [] });
        state.set_open_orders_for_markets(&[24], &unrelated_update, false);
        assert!(state.order_by_client(7).is_none(), "terminal order purged after TTL");
        assert!(state
            .client_orders
            .lock()
            .unwrap()
            .is_empty(), "no residual map growth");
    }

    #[tokio::test]
    async fn pending_fill_matches_side_and_reports_partial_timeout() {
        let fills = Arc::new(FillTracker::default());
        let client_order_index = 42;
        let rx = fills.register(client_order_index);
        let pending = PendingFill {
            fills: fills.clone(),
            account_feed: Arc::new(AccountFeedState::default()),
            market_id: 24,
            account_index: 1,
            client_order_index,
            side: Side::Buy,
            expected_qty: Decimal::new(200, 2),
            rx,
            progress: FillProgress::default(),
        };

        fills.on_trade(TradePayload {
            trade_id: Some(1),
            ask_client_id: Some(client_order_index),
            bid_client_id: None,
            price: Some("10".to_string()),
            size: Some("1".to_string()),
            usd_amount: Some("10".to_string()),
            ..Default::default()
        });
        fills.on_trade(TradePayload {
            trade_id: Some(2),
            ask_client_id: None,
            bid_client_id: Some(client_order_index),
            price: Some("10".to_string()),
            size: Some("1".to_string()),
            usd_amount: Some("10".to_string()),
            ..Default::default()
        });
        fills.on_trade(TradePayload {
            trade_id: Some(2),
            ask_client_id: None,
            bid_client_id: Some(client_order_index),
            price: Some("10".to_string()),
            size: Some("1".to_string()),
            usd_amount: Some("10".to_string()),
            ..Default::default()
        });

        let summary = pending.wait(Duration::from_millis(20)).await.unwrap();
        assert_eq!(summary.qty, Decimal::ONE);
        assert_eq!(summary.notional, Decimal::TEN);
        let stats = fills.stats();
        assert_eq!(stats.registered, 1);
        assert_eq!(stats.trades_seen, 3);
        assert_eq!(stats.matched_trades, 3);
        assert_eq!(stats.duplicate_trades, 1);
        assert_eq!(stats.timeouts, 1);
    }
    #[test]
    fn fees_use_each_fills_notional_and_own_liquidity_role() {
        let trade = TradePayload { is_maker_ask: Some(true),
            maker_fee: Some(serde_json::json!(-25)), taker_fee: Some(serde_json::json!(200)),
            ..Default::default() };
        let fee = |trade: &TradePayload, side, notional| trade_fee_evidence(trade, side, 11, notional, "fixture").fee_usd;
        assert_eq!(fee(&trade, Side::Buy, dec!(1000)), Some(dec!(0.2)));
        assert_eq!(fee(&trade, Side::Buy, dec!(10)), Some(dec!(0.002)));
        // Our IOC flagged as the maker contradicts its provenance: keep the fill, not the fee.
        let contradictory = trade_fee_evidence(&trade, Side::Sell, 11, dec!(1000), "fixture");
        assert_eq!((contradictory.maker, contradictory.fee_usd), (Some(true), None));
        let mut taker = trade.clone();
        // Lighter omits a zero fee (`omitempty`): omission is a confirmed zero, not unknown.
        taker.taker_fee = None;
        assert_eq!(fee(&taker, Side::Buy, dec!(1000)), Some(dec!(0)));
        taker.taker_fee = Some(serde_json::Value::Null);
        assert_eq!(fee(&taker, Side::Buy, dec!(1000)), None);
        taker.taker_fee = Some(serde_json::json!("bad"));
        assert_eq!(fee(&taker, Side::Buy, dec!(1000)), None);
        taker.taker_fee = Some(serde_json::json!(0));
        assert_eq!(fee(&taker, Side::Buy, dec!(1000)), Some(dec!(0)));
        // A missing role flag on our own IOC selects the taker fee.
        taker.is_maker_ask = None;
        taker.taker_fee = Some(serde_json::json!(40));
        assert_eq!(fee(&taker, Side::Sell, dec!(1000)), Some(dec!(0.04)));
    }

    #[tokio::test(start_paused = true)]
    async fn fill_progress_and_fee_evidence_survive_cold_resolution_waits() {
        let fills = Arc::new(FillTracker::default());
        let rx = fills.register(11);
        let mut pending = PendingFill {fills:fills.clone(),account_feed:Arc::new(AccountFeedState::default()),
            market_id:24,account_index:1,client_order_index:11,side:Side::Buy,expected_qty:dec!(0.2),rx,
            progress:FillProgress::default()};
        let trade = |id| TradePayload {trade_id:Some(id),bid_client_id:Some(11),bid_account_id:Some(1),
            size:Some("0.1".to_string()),price:Some("100".to_string()),usd_amount:Some("10".to_string()),
            is_maker_ask:Some(true),taker_fee:Some(serde_json::json!(200)),..Default::default()};
        fills.on_trade(trade(1));
        let first = pending.observe_confirmed(Duration::from_millis(1)).await;
        assert_eq!(first.filled_qty,dec!(0.1));
        // The waiter is still registered, and an old replay must not become a second fill.
        fills.on_trade(trade(1));
        fills.on_trade(trade(2));
        let second = pending.observe_confirmed(Duration::from_millis(1)).await;
        assert_eq!(second.filled_qty,dec!(0.2));
        assert_eq!(second.fill.unwrap().fee_usd,dec!(0.004));
        assert_eq!(second.fee_evidence.len(),2);
        assert_eq!(second.fee_evidence[0].fee_usd,Some(dec!(0.002)));
        assert_eq!(fills.stats().duplicate_trades,1);
    }

}
