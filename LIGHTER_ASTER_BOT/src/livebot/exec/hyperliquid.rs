//! Lighter hedge worker. The module name is kept as `hyperliquid` to minimize churn in
//! the existing strategy/reconciler code, but all live hedge I/O here goes to Lighter.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Utc};
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::{Decimal, RoundingStrategy};
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use super::command::{ExecEvent, HedgeCommand, ExecutionTrade};
use crate::livebot::fills::{HedgeIntent, WireProof, IntentPurpose};
use crate::livebot::account::Venue;
use crate::livebot::journal::Journal;
use super::creds::LighterCreds;
use crate::book::OrderBook;
use crate::connectors::rest_book;
use crate::lighter::auth::generate_ws_auth_token;
use crate::lighter::messages::{
    AccountAllMsg, AccountAllPositionsMsg, AccountOrdersMsg, BookUpdateContiguity, OrderBookMsg,
    PriceLevel,
    RemoteOrder, TradePayload,
    UserStatsMsg,
};
use crate::lighter::nonce::NonceManager;
use crate::lighter::rest::RestClient;
use crate::lighter::signer::{
    SignedTx, Signer, DEFAULT_IOC_EXPIRY, MARGIN_MODE_CROSS, NIL_TRIGGER_PRICE, ORDER_TYPE_LIMIT,
    ORDER_TYPE_MARKET, TIF_IMMEDIATE_OR_CANCEL,
};
use crate::lighter::tx_ws::TxWebSocket;
use crate::lighter::ws::{subscribe_loop, subscribe_loop_authed, SubscribeOptions};
use crate::livebot::ids::Cloid;
use crate::markets::MarketSpec;
use crate::types::{MarketId, Side, TxSendStatus};

/// Reason prefix for a hedge reject whose tx frame provably never reached the venue
/// (`TxSendStatus::NotSent`: connect failed/timed out before any write; nonce already
/// rolled back). Shared with the strategy's safe-retry gate — like the venue's
/// "could not immediately match", NotSent is guaranteed-nothing-landed, so an
/// immediate emergency resend cannot double-hedge.
pub(crate) const HEDGE_NOT_SENT_PREFIX: &str = "Lighter tx not sent:";

/// The ONLY two reject shapes safe to auto-retry (both guaranteed no order landed):
/// the venue-confirmed IOC no-fill, and a NotSent transport fast-fail. Everything
/// else (resting order, ambiguous transport error) must freeze + reconcile instead.
pub(crate) fn hedge_reject_is_definitive_no_fill(reason: &str) -> bool {
    reason.contains("could not immediately match") || reason.contains(HEDGE_NOT_SENT_PREFIX)
}

#[derive(Clone, Debug)]
struct LighterMarketWire {
    market_index: i32,
    symbol: String,
    size_decimals: u32,
    price_decimals: u32,
}

#[derive(Debug, Clone)]
pub struct LighterOrderPlan {
    pub market_index: i32,
    pub client_order_index: i64,
    pub base_amount: i64,
    pub price: i32,
    pub order_expiry: i64,
    pub is_ask: bool,
    pub order_type: i32,
    pub time_in_force: i32,
    pub reduce_only: bool,
}

fn order_plan(wire: &LighterMarketWire, side: Side, price: Decimal, qty: Decimal,
    client_order_index: i64, reduce_only: bool, order_type: i32) -> Result<LighterOrderPlan> {
    Ok(LighterOrderPlan { market_index: wire.market_index, client_order_index,
        base_amount: raw_amount(qty, wire.size_decimals)?, price: raw_price(price, wire.price_decimals, side)?,
        order_expiry: DEFAULT_IOC_EXPIRY, is_ask: side == Side::Sell, order_type,
        time_in_force: TIF_IMMEDIATE_OR_CANCEL, reduce_only })
}

const FILL_ROUTE_CAPACITY: usize = 256;
const RESOLUTION_BUDGET: Duration = Duration::from_secs(60);

#[derive(Debug, Clone)]
enum FillUpdate {
    Trade(TradePayload),
    Order(RemoteOrder),
}

struct FillSink {
    token: u64,
    tx: mpsc::Sender<FillUpdate>,
    overflow: Arc<AtomicBool>,
}

#[derive(Default)]
struct FillTracker {
    pending: Mutex<HashMap<i64, FillSink>>,
    next_token: std::sync::atomic::AtomicU64,
}

impl FillTracker {
    fn register(&self, client_order_index: i64) -> Result<(u64, Receiver<FillUpdate>, Arc<AtomicBool>)> {
        let (tx, rx) = mpsc::channel(FILL_ROUTE_CAPACITY);
        let overflow = Arc::new(AtomicBool::new(false));
        let mut pending = self.pending.lock().unwrap_or_else(|e| e.into_inner());
        if pending.contains_key(&client_order_index) {
            bail!("fill route identity already outstanding: {client_order_index}");
        }
        let token = self.next_token.fetch_add(1, Ordering::Relaxed) + 1;
        pending.insert(client_order_index, FillSink { token, tx, overflow: overflow.clone() });
        Ok((token, rx, overflow))
    }

    fn unregister(&self, client_order_index: i64, token: u64) {
        let mut pending = self.pending.lock().unwrap_or_else(|e| e.into_inner());
        if pending.get(&client_order_index).is_some_and(|s| s.token == token) {
            pending.remove(&client_order_index);
        }
    }

    fn deliver(&self, client_order_index: i64, update: FillUpdate) {
        let pending = self.pending.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(sink) = pending.get(&client_order_index) {
            if sink.tx.try_send(update).is_err() {
                sink.overflow.store(true, Ordering::Release);
            }
        }
    }

    fn on_trade(&self, trade: TradePayload) {
        for id in [trade.ask_client_id, trade.bid_client_id].into_iter().flatten() {
            self.deliver(id, FillUpdate::Trade(trade.clone()));
        }
    }

    fn on_order(&self, order: &RemoteOrder) {
        if let Some(id) = order.client_order_index {
            self.deliver(id, FillUpdate::Order(order.clone()));
        }
    }
}

fn lighter_trade_key(trade: &TradePayload) -> String {
    if let Some(id) = trade.trade_id { return format!("id:{id}"); }
    format!("fallback:{:?}", (trade.ask_id, trade.bid_id, trade.ask_client_id,
        trade.bid_client_id, &trade.price, &trade.size, trade.timestamp, trade.transaction_time))
}

/// A fill's selected fee is a signed rate in millionths of USD notional.
/// Omission means zero; explicit null/malformed data stays unknown.
fn own_trade_evidence(tr: &TradePayload, intent: &HedgeIntent, account: i64, order_id: Option<i64>) -> Option<ExecutionTrade> {
    let index = intent.cloid.to_lighter_client_order_index();
    let ask = tr.ask_client_id == Some(index) || order_id.is_some_and(|id| tr.ask_id == Some(id));
    let bid = tr.bid_client_id == Some(index) || order_id.is_some_and(|id| tr.bid_id == Some(id));
    if ask == bid { return None; }
    if (ask && tr.ask_account_id.is_some_and(|a| a != account))
        || (bid && tr.bid_account_id.is_some_and(|a| a != account)) { return None; }
    let side = if ask { Side::Sell } else { Side::Buy };
    if side != intent.hedge_side { return None; }
    let qty = tr.size.as_deref()?.parse::<Decimal>().ok()?;
    let px = tr.price.as_deref()?.parse::<Decimal>().ok()?;
    if qty <= Decimal::ZERO || px <= Decimal::ZERO { return None; }
    let notional = qty * px;
    let maker = tr.is_maker_ask.map(|m| m == ask).unwrap_or(false); // known IOC provenance
    let selected = if maker { tr.maker_fee.as_ref() } else { tr.taker_fee.as_ref() };
    let fee_ticks = match selected { None => Some(Decimal::ZERO), Some(v) => value_dec(Some(v)) };
    // A submitted IOC cannot be the maker: preserve the fill but flag contradictory fee evidence.
    let fee_usd = if maker { None } else { fee_ticks.map(|r| notional * r / Decimal::from(1_000_000)) };
    Some(ExecutionTrade {
        attempt_id: intent.cloid.to_hex(), logical_id: intent.logical_id.to_hex(), venue: Venue::Hyperliquid,
        market: intent.market.0.clone(), side, trade_id: lighter_trade_key(tr), identity_complete: tr.trade_id.is_some_and(|id| id > 0), event_time_ms: if tr.transaction_time.or(tr.timestamp).is_some_and(|t| t > 0) { tr.event_time_ms().filter(|t| *t > 0) } else { None },
        order_id: (if ask { tr.ask_id } else { tr.bid_id }).map(|x| x.to_string()),
        client_order_index: index, qty, px, notional_usd: notional,
        maker: Some(maker), fee_ticks, fee_usd,
    })
}

#[derive(Default)]
struct FillTotals {
    trades: HashMap<String, ExecutionTrade>,
}

impl FillTotals {
    fn observe(&mut self, tr: &TradePayload, intent: &HedgeIntent, account: i64, order_id: Option<i64>) -> Option<ExecutionTrade> {
        let evidence = own_trade_evidence(tr, intent, account, order_id)?;
        // Without a venue trade ID a repeat cannot be distinguished from a second
        // identical fill. Keep the raw evidence, but wait for terminal/history
        // quantity instead of treating this as uniquely counted execution.
        if !evidence.identity_complete { return Some(evidence); }
        if let Some(old) = self.trades.get(&evidence.trade_id) {
            if old.qty != evidence.qty || old.px != evidence.px { return None; }
            if old.fee_usd.is_some() || evidence.fee_usd.is_none() { return None; }
        }
        self.trades.insert(evidence.trade_id.clone(), evidence.clone());
        Some(evidence)
    }

    fn values(&self) -> (Decimal, Decimal, Option<Decimal>) {
        let mut qty = Decimal::ZERO;
        let mut quote = Decimal::ZERO;
        let mut fee = Some(Decimal::ZERO);
        for tr in self.trades.values() {
            qty += tr.qty;
            quote += tr.notional_usd;
            fee = fee.zip(tr.fee_usd).map(|(a, b)| a + b);
        }
        (qty, quote, fee)
    }
}

enum HedgeSendOutcome {
    Terminal { ev: ExecEvent, refresh_nonce_after_emit: bool, proof: Option<WireProof> },
    AwaitFills {
        token: u64, rx: Receiver<FillUpdate>, overflow: Arc<AtomicBool>,
        requested_qty: Decimal, proof: WireProof, ambiguous: bool,
    },
}

struct FillRouteGuard { fills: Arc<FillTracker>, client_order_index: i64, token: u64 }
impl Drop for FillRouteGuard {
    fn drop(&mut self) { self.fills.unregister(self.client_order_index, self.token); }
}

#[derive(Default)]
struct AccountFeedState {
    positions: Mutex<HashMap<u32, (Decimal, Decimal)>>,
    available_balance: Mutex<Option<Decimal>>,
    portfolio_value: Mutex<Option<Decimal>>,
    open_orders: Mutex<HashMap<u32, Vec<RemoteOrder>>>,
    positions_ready: AtomicBool,
    user_stats_ready: AtomicBool,
    open_orders_ready: AtomicBool,
    /// mono_now_ns of the last APPLIED message per feed. The reconciler uses these to
    /// decide whether the WS cache is fresh enough to stand in for a venue read — a
    /// "ready" flag alone says the feed produced data once, not that it is still alive.
    positions_updated_ns: std::sync::atomic::AtomicI64,
    stats_updated_ns: std::sync::atomic::AtomicI64,
    open_orders_updated_ns: std::sync::atomic::AtomicI64,
}

impl AccountFeedState {
    fn set_position(&self, market_id: u32, qty: Decimal, entry_px: Decimal) {
        self.positions
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(market_id, (qty, entry_px));
    }

    fn mark_positions_ready(&self) {
        self.positions_ready.store(true, Ordering::Release);
    }

    fn stamp_positions_updated(&self) {
        self.positions_updated_ns
            .store(crate::hotpath::clock::mono_now_ns(), Ordering::Release);
    }

    fn positions_updated_ns(&self) -> i64 {
        self.positions_updated_ns.load(Ordering::Acquire)
    }

    fn stats_updated_ns(&self) -> i64 {
        self.stats_updated_ns.load(Ordering::Acquire)
    }

    fn open_orders_updated_ns(&self) -> i64 {
        self.open_orders_updated_ns.load(Ordering::Acquire)
    }

    fn position(&self, market_id: u32) -> Option<(Decimal, Decimal)> {
        if !self.positions_ready.load(Ordering::Acquire) {
            return None;
        }
        self.positions
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&market_id)
            .copied()
    }

    fn all_positions(&self) -> Option<HashMap<u32, (Decimal, Decimal)>> {
        if !self.positions_ready.load(Ordering::Acquire) {
            return None;
        }
        Some(
            self.positions
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .clone(),
        )
    }

    fn set_stats(&self, available: Option<Decimal>, portfolio: Option<Decimal>) {
        *self
            .available_balance
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = available;
        *self
            .portfolio_value
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = portfolio;
        self.stats_updated_ns
            .store(crate::hotpath::clock::mono_now_ns(), Ordering::Release);
        self.user_stats_ready.store(true, Ordering::Release);
    }

    fn stats(&self) -> (Option<Decimal>, Option<Decimal>) {
        (
            *self
                .available_balance
                .lock()
                .unwrap_or_else(|e| e.into_inner()),
            *self
                .portfolio_value
                .lock()
                .unwrap_or_else(|e| e.into_inner()),
        )
    }

    fn stats_ready(&self) -> bool {
        self.user_stats_ready.load(Ordering::Acquire)
    }

    fn set_open_orders_for_markets(
        &self,
        known_markets: &[u32],
        orders: &HashMap<String, Vec<RemoteOrder>>,
    ) {
        let mut out = HashMap::new();
        for market_id in known_markets {
            let rows = orders
                .get(&market_id.to_string())
                .map(|rows| {
                    rows.iter()
                        .filter(|row| row.is_live())
                        .cloned()
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            out.insert(*market_id, rows);
        }
        *self.open_orders.lock().unwrap_or_else(|e| e.into_inner()) = out;
        self.open_orders_updated_ns
            .store(crate::hotpath::clock::mono_now_ns(), Ordering::Release);
        self.open_orders_ready.store(true, Ordering::Release);
    }

    fn open_orders(&self) -> Option<HashMap<u32, Vec<RemoteOrder>>> {
        if !self.open_orders_ready.load(Ordering::Acquire) {
            return None;
        }
        Some(
            self.open_orders
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .clone(),
        )
    }

    fn open_orders_ready(&self) -> bool {
        self.open_orders_ready.load(Ordering::Acquire)
    }
}

#[derive(Default)]
struct BookFeedState {
    books: Mutex<HashMap<u32, LighterBook>>,
}

impl BookFeedState {
    fn apply(&self, market_id: u32, msg: &OrderBookMsg) -> bool {
        let mut books = self.books.lock().unwrap_or_else(|e| e.into_inner());
        let book = books.entry(market_id).or_default();
        book.apply(msg)
    }

    fn reset(&self, market_id: u32) {
        self.books
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&market_id);
    }

    fn order_book(&self, market_id: u32) -> Option<OrderBook> {
        self.books
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&market_id)
            .and_then(LighterBook::to_order_book)
    }
}

#[derive(Default)]
struct LighterBook {
    bids: BTreeMap<Decimal, Decimal>,
    asks: BTreeMap<Decimal, Decimal>,
    initialized: bool,
    updated_at: Option<DateTime<Utc>>,
    source_ts: Option<DateTime<Utc>>,
    last_nonce: Option<i64>,
    last_offset: Option<u64>,
}

impl LighterBook {
    fn apply(&mut self, msg: &OrderBookMsg) -> bool {
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
        if !apply_levels(&mut self.bids, &msg.order_book.bids)
            || !apply_levels(&mut self.asks, &msg.order_book.asks)
        {
            // Malformed level: the book may now be partially applied — ride the gap
            // path (caller resets this book and reconnects for a fresh snapshot).
            return false;
        }
        self.updated_at = Some(Utc::now());
        self.source_ts = msg.source_time_ms().and_then(DateTime::from_timestamp_millis);
        self.last_nonce = msg.order_book.nonce.or(self.last_nonce);
        self.last_offset = msg.effective_offset().or(self.last_offset);
        true
    }

    fn to_order_book(&self) -> Option<OrderBook> {
        if !self.initialized {
            return None;
        }
        let ts = self.updated_at?;
        Some(OrderBook::from_levels(
            self.bids.iter().rev().map(|(p, q)| (*p, *q)),
            self.asks.iter().map(|(p, q)| (*p, *q)),
            self.source_ts.unwrap_or_default(),
            ts,
        ))
    }
}

#[derive(Clone)]
pub struct HedgeReadiness {
    socket: Arc<TxWebSocket>,
    nonce_uncertain: Arc<AtomicBool>,
}
impl HedgeReadiness {
    pub fn is_ready(&self) -> bool {
        self.socket.is_ready() && !self.nonce_uncertain.load(Ordering::Acquire)
    }
}

/// Lighter-backed hedge exchange. The public type name is intentionally kept as `HlExchange`
/// because many strategy/reconciler interfaces still use "HL" as shorthand for hedge leg.
#[derive(Clone)]
pub struct HlExchange {
    rest: RestClient,
    tx_ws: Arc<TxWebSocket>,
    signer: Arc<Signer>,
    nonce: Arc<NonceManager>,
    nonce_uncertain: Arc<AtomicBool>,
    account_index: i64,
    api_key_index: i32,
    base_url: String,
    markets: HashMap<MarketId, LighterMarketWire>,
    symbol_to_market: HashMap<String, MarketId>,
    fills: Arc<FillTracker>,
    account_feed: Arc<AccountFeedState>,
    book_feed: Arc<BookFeedState>,
    ws_url: String,
    fill_timeout: Duration,
    /// Max age of a WS account-feed cache entry before the reconciler's reads fall back to
    /// REST (see clearinghouse_state). At/below the reconcile cadence so two consecutive
    /// snapshots can never both be built from the same stale cache read.
    ws_account_max_age: Duration,
}

impl HlExchange {
    pub async fn new_lighter(
        base_url: String,
        signers_dir: &Path,
        creds: LighterCreds,
        specs: &[MarketSpec],
        fill_timeout_ms: i64,
        ws_account_max_age_ms: i64,
    ) -> Result<Self> {
        let mut ex = Self::new_read_only(base_url, signers_dir, &creds, specs, fill_timeout_ms, ws_account_max_age_ms)?;
        ex.signer
            .check_client(creds.api_key_index)
            .context("Lighter CheckClient")?;
        ex.nonce =
            Arc::new(NonceManager::init(&ex.rest, creds.account_index, creds.api_key_index).await?);
        ex.tx_ws
            .connect()
            .await
            .with_context(|| format!("preconnect Lighter tx websocket {}", ex.ws_url))?;
        Ok(ex)
    }

    /// Read-only client for the status poller: REST, the market wire context and the signer (the
    /// active-orders auth token needs it), but no key check, an offline nonce stub and an
    /// unconnected tx socket. A poll so spends no Lighter request on a nonce and holds no
    /// order-capable socket; nothing may be sent through it.
    pub fn new_read_only(
        base_url: String,
        signers_dir: &Path,
        creds: &LighterCreds,
        specs: &[MarketSpec],
        fill_timeout_ms: i64,
        ws_account_max_age_ms: i64,
    ) -> Result<Self> {
        let rest = RestClient::new(&base_url, 2)?;
        let signer = Arc::new(Signer::load(
            signers_dir,
            &base_url,
            &creds.api_private_key,
            creds.api_key_index,
            creds.account_index,
        )?);
        let nonce = Arc::new(NonceManager::offline(creds.account_index, creds.api_key_index));
        let ws_url = crate::lighter::ws::stream_url(&base_url);
        let tx_ws = Arc::new(TxWebSocket::new(&ws_url));

        let mut markets = HashMap::new();
        let mut symbol_to_market = HashMap::new();
        for s in specs {
            let wire = LighterMarketWire {
                market_index: s.lighter_market_id as i32,
                symbol: s.hl_coin.clone(),
                size_decimals: s.lighter_size_decimals,
                price_decimals: s.lighter_price_decimals,
            };
            symbol_to_market.insert(s.hl_coin.to_ascii_uppercase(), s.market_id.clone());
            markets.insert(s.market_id.clone(), wire);
        }
        Ok(HlExchange {
            rest,
            tx_ws,
            signer,
            nonce,
            nonce_uncertain: Arc::new(AtomicBool::new(false)),
            account_index: creds.account_index,
            api_key_index: creds.api_key_index,
            base_url,
            markets,
            symbol_to_market,
            fills: Arc::new(FillTracker::default()),
            account_feed: Arc::new(AccountFeedState::default()),
            book_feed: Arc::new(BookFeedState::default()),
            ws_url,
            fill_timeout: Duration::from_millis(fill_timeout_ms.max(250) as u64),
            ws_account_max_age: Duration::from_millis(ws_account_max_age_ms.max(250) as u64),
        })
    }

    pub fn tx_ready(&self) -> bool { self.tx_ws.is_ready() && !self.nonce_uncertain.load(Ordering::Acquire) }

    pub fn readiness(&self) -> HedgeReadiness {
        HedgeReadiness { socket: self.tx_ws.clone(), nonce_uncertain: self.nonce_uncertain.clone() }
    }

    fn wire(&self, market: &MarketId) -> Result<&LighterMarketWire> {
        self.markets
            .get(market)
            .ok_or_else(|| anyhow!("no Lighter wire context for market {market}"))
    }

    fn known_lighter_markets(&self) -> Vec<u32> {
        self.markets
            .values()
            .map(|w| w.market_index as u32)
            .collect()
    }

    pub async fn wait_ready(&self, market: &MarketId, timeout: Duration) -> Result<()> {
        let wire = self.wire(market)?;
        let market_id = wire.market_index as u32;
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let book_ready = self.book_feed.order_book(market_id).is_some();
            let position_ready = self.account_feed.position(market_id).is_some();
            let stats_ready = self.account_feed.stats_ready();
            let orders_ready = self.account_feed.open_orders_ready();
            if book_ready && position_ready && stats_ready && orders_ready {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                bail!(
                    "Lighter websocket state not ready for {market}: book={} position={} user_stats={} all_orders={}",
                    book_ready,
                    position_ready,
                    stats_ready,
                    orders_ready
                );
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    pub fn start_private_streams(
        &self,
        shutdown: CancellationToken,
    ) -> Vec<tokio::task::JoinHandle<()>> {
        vec![
            self.spawn_account_all(shutdown.clone()),
            self.spawn_account_all_positions(shutdown.clone()),
            self.spawn_account_all_orders(shutdown.clone()),
            self.spawn_user_stats(shutdown.clone()),
            self.spawn_order_books(shutdown),
        ]
    }

    fn spawn_account_all(&self, shutdown: CancellationToken) -> tokio::task::JoinHandle<()> {
        let signer = self.signer.clone();
        let api_key_index = self.api_key_index;
        let channel = format!("account_all/{}", self.account_index);
        let mut opts = SubscribeOptions::new(&self.ws_url, "lighter-account-all", vec![channel.clone()]);
        opts.data_timeout = None;
        opts.frame_timeout = 90.0;
        // The ~10-min auth-token TTL drops this socket every session; the 5s default
        // base would blind the private feed ~5-6s per expiry. Healthy sessions reset
        // the backoff to base, so 0.5s only shortens the routine re-auth gap —
        // consecutive failures still escalate toward reconnect_max.
        opts.reconnect_base = 0.5;
        let fills = self.fills.clone();
        tokio::spawn(async move {
            tokio::select! {
                _ = shutdown.cancelled() => {}
                _ = subscribe_loop_authed(
                    opts,
                    move || auth_map(&signer, api_key_index, &channel),
                    move |frame| {
                        if let Ok(msg) = serde_json::from_str::<AccountAllMsg>(frame.raw) {
                            for trades in msg.trades.values() {
                                for tr in trades {
                                    fills.on_trade(tr.clone());
                                }
                            }
                        }
                    },
                ) => {}
            }
        })
    }

    fn spawn_account_all_positions(
        &self,
        shutdown: CancellationToken,
    ) -> tokio::task::JoinHandle<()> {
        let channel = format!("account_all_positions/{}", self.account_index);
        let mut opts = SubscribeOptions::new(&self.ws_url, "lighter-account-all-positions", vec![channel]);
        opts.data_timeout = None;
        opts.frame_timeout = 90.0;
        // Unauthenticated, but on the same 0.5s reconnect base as the authed private feeds:
        // the 5s default would blind the position cache ~5s per drop. Healthy sessions reset
        // the backoff to base; consecutive failures still escalate toward reconnect_max.
        opts.reconnect_base = 0.5;
        let known_markets = self.known_lighter_markets();
        let state = self.account_feed.clone();
        tokio::spawn(async move {
            tokio::select! {
                _ = shutdown.cancelled() => {}
                _ = subscribe_loop(
                    opts,
                    None,
                    move |frame| {
                        if let Ok(msg) = serde_json::from_str::<AccountAllPositionsMsg>(frame.raw) {
                            apply_account_all_positions(&state, &known_markets, &msg);
                        }
                    },
                    || {},
                ) => {}
            }
        })
    }

    fn spawn_account_all_orders(&self, shutdown: CancellationToken) -> tokio::task::JoinHandle<()> {
        let signer = self.signer.clone();
        let api_key_index = self.api_key_index;
        let channel = format!("account_all_orders/{}", self.account_index);
        let auth_channel = channel.clone();
        let mut opts = SubscribeOptions::new(&self.ws_url, "lighter-account-all-orders", vec![channel]);
        opts.data_timeout = None;
        opts.frame_timeout = 90.0;
        // The ~10-min auth-token TTL drops this socket every session; the 5s default
        // base would blind the private feed ~5-6s per expiry. Healthy sessions reset
        // the backoff to base, so 0.5s only shortens the routine re-auth gap —
        // consecutive failures still escalate toward reconnect_max.
        opts.reconnect_base = 0.5;
        let known_markets = self.known_lighter_markets();
        let state = self.account_feed.clone();
        let fills = self.fills.clone();
        tokio::spawn(async move {
            tokio::select! {
                _ = shutdown.cancelled() => {}
                _ = subscribe_loop_authed(
                    opts,
                    move || auth_map(&signer, api_key_index, &auth_channel),
                    move |frame| {
                        if let Ok(msg) = serde_json::from_str::<AccountOrdersMsg>(frame.raw) {
                            for order in msg.orders.values().flatten() { fills.on_order(order); }
                            state.set_open_orders_for_markets(&known_markets, &msg.orders);
                        }
                    },
                ) => {}
            }
        })
    }

    fn spawn_user_stats(&self, shutdown: CancellationToken) -> tokio::task::JoinHandle<()> {
        let signer = self.signer.clone();
        let api_key_index = self.api_key_index;
        let channel = format!("user_stats/{}", self.account_index);
        let mut opts = SubscribeOptions::new(&self.ws_url, "lighter-user-stats", vec![channel.clone()]);
        opts.data_timeout = None;
        opts.frame_timeout = 90.0;
        // The ~10-min auth-token TTL drops this socket every session; the 5s default
        // base would blind the private feed ~5-6s per expiry. Healthy sessions reset
        // the backoff to base, so 0.5s only shortens the routine re-auth gap —
        // consecutive failures still escalate toward reconnect_max.
        opts.reconnect_base = 0.5;
        let state = self.account_feed.clone();
        tokio::spawn(async move {
            tokio::select! {
                _ = shutdown.cancelled() => {}
                _ = subscribe_loop_authed(
                    opts,
                    move || auth_map(&signer, api_key_index, &channel),
                    move |frame| {
                        if let Ok(msg) = serde_json::from_str::<UserStatsMsg>(frame.raw) {
                            state.set_stats(
                                value_dec(msg.stats.available_balance.as_ref()),
                                value_dec(msg.stats.portfolio_value.as_ref()),
                            );
                        }
                    },
                ) => {}
            }
        })
    }

    fn spawn_order_books(&self, shutdown: CancellationToken) -> tokio::task::JoinHandle<()> {
        let specs = self
            .markets
            .values()
            .map(|w| (w.market_index as u32, w.symbol.clone()))
            .collect::<Vec<_>>();
        let ws_url = self.ws_url.clone();
        let books = self.book_feed.clone();
        tokio::spawn(async move {
            let mut handles = Vec::new();
            for (market_id, symbol) in specs {
                handles.push(spawn_order_book_stream(
                    ws_url.clone(),
                    market_id,
                    symbol,
                    books.clone(),
                    shutdown.clone(),
                ));
            }
            tokio::select! {
                _ = shutdown.cancelled() => {}
                _ = futures_util::future::join_all(handles) => {}
            }
        })
    }

    pub fn build_ioc_limit_plan(&self, market: &MarketId, side: Side, px: Decimal, sz: Decimal,
        client_order_index: i64, reduce_only: bool) -> Result<LighterOrderPlan> {
        order_plan(self.wire(market)?, side, px, sz, client_order_index, reduce_only, ORDER_TYPE_LIMIT)
    }

    pub fn build_market_plan(&self, market: &MarketId, side: Side, px_bound: Decimal, sz: Decimal,
        client_order_index: i64, reduce_only: bool) -> Result<LighterOrderPlan> {
        order_plan(self.wire(market)?, side, px_bound, sz, client_order_index, reduce_only, ORDER_TYPE_MARKET)
    }

    pub fn sign_order_plan(&self, plan: &LighterOrderPlan, nonce: i64) -> Result<SignedTx> {
        self.signer.sign_create_order(
            plan.market_index,
            plan.client_order_index,
            plan.base_amount,
            plan.price,
            plan.is_ask,
            plan.order_type,
            plan.time_in_force,
            plan.reduce_only,
            NIL_TRIGGER_PRICE,
            plan.order_expiry,
            nonce,
            self.api_key_index,
        )
    }

    async fn send_signed(&self, tx: SignedTx) -> crate::types::TxSendResult {
        self.tx_ws.send_batch(&[tx.tx_type], &[tx.tx_info]).await
    }

    /// SEND PHASE of one hedge/flatten IOC: build → register fill route → reserve nonce →
    /// sign → wire send → classify. Runs entirely inside the worker loop so everything
    /// that touches the NonceManager or the tx websocket stays strictly serialized —
    /// including the nonce repair on failure (`acknowledge_failure` / `rollback` /
    /// `hard_refresh`), which must observe the outcome before the next command's nonce is
    /// reserved. Only a wire-accepted order returns `AwaitFills`; the caller runs the wait
    /// phase (which touches neither nonce nor socket) off the worker's critical path.
    async fn send_hedge_tx(&self, intent: &HedgeIntent, px: Decimal) -> HedgeSendOutcome {
        let cloid = intent.cloid;
        let client_order_index = cloid.to_lighter_client_order_index();
        let plan = match self.build_ioc_limit_plan(&intent.market, intent.hedge_side, px, intent.qty,
            client_order_index, intent.purpose == IntentPurpose::ReduceDelta) {
            Ok(plan) => plan,
            Err(e) => return HedgeSendOutcome::Terminal { ev: ExecEvent::AttemptNotSent {
                cloid, reason: e.to_string() }, refresh_nonce_after_emit: false, proof: None },
        };
        let requested_qty = match self.wire(&intent.market) {
            Ok(w) => Decimal::new(plan.base_amount, w.size_decimals),
            Err(e) => return HedgeSendOutcome::Terminal { ev: ExecEvent::AttemptNotSent {
                cloid, reason: e.to_string() }, refresh_nonce_after_emit: false, proof: None },
        };
        let (token, rx, overflow) = match self.fills.register(client_order_index) {
            Ok(route) => route,
            Err(e) => return HedgeSendOutcome::Terminal { ev: ExecEvent::AttemptNotSent {
                cloid, reason: e.to_string() }, refresh_nonce_after_emit: false, proof: None },
        };
        let nonce = self.nonce.next();
        let sign_start_ns = crate::hotpath::clock::mono_now_ns();
        let signed = match self.sign_order_plan(&plan, nonce) {
            Ok(tx) => tx,
            Err(e) => {
                self.nonce.acknowledge_failure();
                self.fills.unregister(client_order_index, token);
                return HedgeSendOutcome::Terminal { ev: ExecEvent::AttemptNotSent {
                    cloid, reason: e.to_string() }, refresh_nonce_after_emit: true, proof: None };
            }
        };
        let sent_ns = crate::hotpath::clock::mono_now_ns();
        let proof = WireProof { tx_hash: Some(signed.tx_hash.clone()), nonce: Some(nonce),
            client_order_index: Some(client_order_index), sent_ns };
        if sent_ns >= intent.admission.deadline_ns {
            self.nonce.rollback(1);
            self.fills.unregister(client_order_index, token);
            return HedgeSendOutcome::Terminal { ev: ExecEvent::AttemptNotSent {
                cloid, reason: "execution deadline elapsed before write".into() },
                refresh_nonce_after_emit: false, proof: Some(proof) };
        }
        let result = self.send_signed(signed).await;
        info!("hedge timing: coi={} queue_us={} sign_us={} response_us={}", client_order_index,
            sign_start_ns.saturating_sub(intent.created_ns) / 1_000,
            sent_ns.saturating_sub(sign_start_ns) / 1_000,
            crate::hotpath::clock::mono_now_ns().saturating_sub(sent_ns) / 1_000);
        match result.status {
            TxSendStatus::Ok => HedgeSendOutcome::AwaitFills { token, rx, overflow,
                requested_qty, proof, ambiguous: false },
            TxSendStatus::Rejected => {
                self.fills.unregister(client_order_index, token);
                HedgeSendOutcome::Terminal { ev: ExecEvent::HedgeReject { cloid,
                    reason: format!("Lighter reject code={} {}", result.code, result.message) },
                    refresh_nonce_after_emit: true, proof: Some(proof) }
            }
            TxSendStatus::NotSent => {
                self.nonce.rollback(1);
                self.fills.unregister(client_order_index, token);
                HedgeSendOutcome::Terminal { ev: ExecEvent::AttemptNotSent { cloid,
                    reason: format!("{HEDGE_NOT_SENT_PREFIX} {}", result.message) },
                    refresh_nonce_after_emit: false, proof: Some(proof) }
            }
            _ => {
                self.nonce_uncertain.store(true, Ordering::Release);
                // Keep route and native identity alive: response failure proves neither
                // acceptance nor rejection. Cold resolution owns the uncertainty.
                HedgeSendOutcome::AwaitFills { token, rx, overflow,
                    requested_qty, proof, ambiguous: true }
            }
        }
    }

    pub(crate) async fn place_raw(
        &self,
        market: &MarketId,
        side: Side,
        px: Decimal,
        sz: Decimal,
        tif: &str,
        reduce_only: bool,
        cloid_hex: Option<String>,
    ) -> Result<String> {
        let client_order_index = cloid_hex
            .as_deref()
            .and_then(client_index_from_hex)
            .unwrap_or_else(|| random_client_order_index(market, side));
        let plan = if tif.eq_ignore_ascii_case("market") {
            self.build_market_plan(market, side, px, sz, client_order_index, reduce_only)?
        } else {
            self.build_ioc_limit_plan(market, side, px, sz, client_order_index, reduce_only)?
        };
        let nonce = self.nonce.next();
        let signed = self.sign_order_plan(&plan, nonce)?;
        let result = self.send_signed(signed).await;
        serde_json::to_string(&serde_json::json!({
            "status": format!("{:?}", result.status),
            "code": result.code,
            "message": result.message,
            "client_order_index": client_order_index,
            "quota_remaining": result.quota_remaining,
        }))
        .context("serialize Lighter tx result")
    }

    /// Read the current Lighter market leverage from the account payload.
    ///
    /// Lighter exposes `initial_margin_fraction` as a percentage. A 1x market shows
    /// `100.00`, 2x shows `50.00`, etc., so leverage is `100 / fraction`.
    pub(crate) async fn get_leverage(&self, market: &MarketId) -> Result<Decimal> {
        let w = self.wire(market)?;
        let raw = self.rest.account_raw(self.account_index).await?;
        lighter_leverage_from_account(&raw, w.market_index as u32, &w.symbol)
    }

    pub async fn clearinghouse_state(&self) -> Result<HlClearinghouse> {
        // Read-start stamp BEFORE any venue read: if REST is used, the data's origin can
        // be no later than this instant — exactly the semantics the reconciler's straddle
        // guard needs (see AccountSnapshot::read_start_ns).
        let rest_read_start_ns = crate::hotpath::clock::mono_now_ns();
        let max_age_ns = self.ws_account_max_age.as_nanos() as i64;
        let fresh = |updated_ns: i64| {
            updated_ns > 0 && rest_read_start_ns.saturating_sub(updated_ns) <= max_age_ns
        };
        let (available, portfolio) = self.account_feed.stats();
        let positions_snapshot = self.account_feed.all_positions();
        // The WS cache may serve a piece only while that feed is FRESH. A cache that was
        // populated once and then went quiet (stalled stream, degraded endpoint) must not
        // masquerade as a venue read — that is the double-hedge window: a missed hedge
        // fill plus a stale rep_h=0 convinces recover_orphans to hedge again.
        let use_cached_positions =
            positions_snapshot.is_some() && fresh(self.account_feed.positions_updated_ns());
        let use_cached_stats = available.is_some()
            && portfolio.is_some()
            && fresh(self.account_feed.stats_updated_ns());
        let needs_rest = !use_cached_positions || !use_cached_stats;
        let raw = if needs_rest {
            Some(self.rest.account_raw(self.account_index).await?)
        } else {
            None
        };
        let account = raw.as_ref().map(|raw| {
            raw.get("accounts")
                .and_then(|a| a.as_array())
                .and_then(|a| a.first())
                .unwrap_or(raw)
        });
        let fallback_portfolio = account.and_then(|account| {
            value_dec(account.get("portfolio_value"))
                .or_else(|| value_dec(account.get("account_value")))
                .or_else(|| value_dec(account.get("collateral")))
        });
        let fallback_available = account.and_then(|account| {
            value_dec(account.get("available_balance"))
                .or_else(|| value_dec(account.get("available")))
        });
        // Prefer the source we decided to trust: fresh cache, else the REST fallback (a
        // stale cache value is last resort only if REST omitted the field entirely).
        let (account_value, withdrawable) = if use_cached_stats {
            (
                portfolio.ok_or_else(|| anyhow!("Lighter stats missing valid portfolio_value"))?,
                available.unwrap_or(Decimal::ZERO),
            )
        } else {
            (
                fallback_portfolio.ok_or_else(|| anyhow!("Lighter REST account missing valid portfolio_value"))?,
                fallback_available.unwrap_or(Decimal::ZERO),
            )
        };

        let mut positions = Vec::new();
        if let Some(ws_positions) = positions_snapshot.filter(|_| use_cached_positions) {
            for (market_id, (qty, entry)) in ws_positions {
                if qty == Decimal::ZERO {
                    continue;
                }
                let symbol = self
                    .markets
                    .values()
                    .find(|w| w.market_index == market_id as i32)
                    .map(|w| w.symbol.clone())
                    .unwrap_or_else(|| market_id.to_string());
                positions.push(HlAssetPosition {
                    position: HlPosition {
                        coin: symbol,
                        szi: qty.normalize().to_string(),
                        entry_px: Some(entry.normalize().to_string()),
                    },
                });
            }
        } else if let Some(rows) =
            account.and_then(|account| account.get("positions").and_then(|p| p.as_array()))
        {
            for p in rows {
                let Some(market_id) = p
                    .get("market_id")
                    .and_then(|m| m.as_u64())
                    .map(|v| v as u32)
                else {
                    continue;
                };
                let qty = signed_position_from_json(p);
                if qty == Decimal::ZERO {
                    continue;
                }
                let entry = value_dec(p.get("avg_entry_price"))
                    .or_else(|| value_dec(p.get("entry_price")))
                    .unwrap_or(Decimal::ZERO);
                let symbol = self
                    .markets
                    .values()
                    .find(|w| w.market_index == market_id as i32)
                    .map(|w| w.symbol.clone())
                    .unwrap_or_else(|| market_id.to_string());
                positions.push(HlAssetPosition {
                    position: HlPosition {
                        coin: symbol,
                        szi: qty.normalize().to_string(),
                        entry_px: Some(entry.normalize().to_string()),
                    },
                });
            }
        }
        // The earliest origin of any data actually used: a fresh-cache piece originated at
        // its feed stamp; a REST piece originated no later than the read-start stamp.
        let positions_origin_ns = if use_cached_positions {
            self.account_feed.positions_updated_ns()
        } else {
            rest_read_start_ns
        };
        let stats_origin_ns = if use_cached_stats {
            self.account_feed.stats_updated_ns()
        } else {
            rest_read_start_ns
        };
        Ok(HlClearinghouse {
            margin_summary: HlMarginSummary {
                account_value: account_value.normalize().to_string(),
            },
            asset_positions: positions,
            withdrawable: withdrawable.normalize().to_string(),
            data_origin_ns: positions_origin_ns.min(stats_origin_ns),
            margin_source_ns: if (use_cached_stats && available.is_some()) || (!use_cached_stats && fallback_available.is_some()) { stats_origin_ns } else { 0 },
        })
    }

    pub async fn open_orders_info(&self) -> Result<Vec<HlOpenOrder>> {
        let mut out = Vec::new();
        // Same freshness rule as clearinghouse_state: a quiet/stalled orders feed must
        // not stand in for a venue read (the sweep gate trusts this via read_start_ns).
        let now_ns = crate::hotpath::clock::mono_now_ns();
        let max_age_ns = self.ws_account_max_age.as_nanos() as i64;
        let orders_fresh = {
            let updated = self.account_feed.open_orders_updated_ns();
            updated > 0 && now_ns.saturating_sub(updated) <= max_age_ns
        };
        if let Some(cached) = self.account_feed.open_orders().filter(|_| orders_fresh) {
            for w in self.markets.values() {
                let rows = cached
                    .get(&(w.market_index as u32))
                    .cloned()
                    .unwrap_or_default();
                for o in rows.into_iter().filter(|o| o.is_live()) {
                    out.push(HlOpenOrder {
                        coin: w.symbol.clone(),
                        oid: o
                            .order_index
                            .or(o.client_order_index)
                            .unwrap_or_default()
                            .max(0) as u64,
                        side: if o.is_ask.unwrap_or(false) {
                            "A".into()
                        } else {
                            "B".into()
                        },
                        limit_px: o.price.unwrap_or_default(),
                        sz: o
                            .remaining_base_amount
                            .or(o.filled_base_amount)
                            .unwrap_or_default(),
                    });
                }
            }
            return Ok(out);
        }

        let auth = generate_ws_auth_token(&self.signer, self.api_key_index)?;
        for w in self.markets.values() {
            let rows = self
                .rest
                .account_active_orders(self.account_index, w.market_index as u32, &auth)
                .await
                .with_context(|| format!("accountActiveOrders {}", w.symbol))?;
            for o in rows.into_iter().filter(|o| o.is_live()) {
                out.push(HlOpenOrder {
                    coin: w.symbol.clone(),
                    oid: o
                        .order_index
                        .or(o.client_order_index)
                        .unwrap_or_default()
                        .max(0) as u64,
                    side: if o.is_ask.unwrap_or(false) {
                        "A".into()
                    } else {
                        "B".into()
                    },
                    limit_px: o.price.unwrap_or_default(),
                    sz: o
                        .remaining_base_amount
                        .or(o.filled_base_amount)
                        .unwrap_or_default(),
                });
            }
        }
        Ok(out)
    }

    /// Lighter mid for `market` from the live WS book cache, plus the age (ms) of the book
    /// data (wall-clock since the last applied order_book message). `None` when the cache has
    /// no initialized book (pre-warmup, or after a sequence-gap reset — the reset clears the
    /// book, so a dead stream reads as `None`, never as an old mid).
    pub fn cached_lighter_mid(&self, market: &MarketId) -> Option<(Decimal, i64)> {
        let w = self.wire(market).ok()?;
        let book = self.book_feed.order_book(w.market_index as u32)?;
        let mid = book.mid()?;
        Some((mid, book.age_ms(chrono::Utc::now())))
    }

    /// Lighter mid fetched directly over REST, bypassing the WS book cache. Used by the
    /// reconciler's uPnL marking when the cached book is stale — `mid()` would serve the
    /// stale cache first. Also the only path for the status poller (no WS streams).
    pub async fn rest_mid(&self, market: &MarketId) -> Result<Decimal> {
        let w = self.wire(market)?;
        let client = rest_book::client()?;
        let book = rest_book::fetch_lighter_book_from_base(
            &client,
            &self.base_url,
            w.market_index as u32,
            20,
        )
        .await?;
        book.mid()
            .ok_or_else(|| anyhow!("no Lighter mid for {}", market.0))
    }

    pub async fn mid(&self, coin: &str) -> Result<Decimal> {
        let market = self
            .symbol_to_market
            .get(&coin.to_ascii_uppercase())
            .ok_or_else(|| anyhow!("no Lighter market configured for {coin}"))?;
        let w = self.wire(market)?;
        if let Some(mid) = self
            .book_feed
            .order_book(w.market_index as u32)
            .and_then(|book| book.mid())
        {
            return Ok(mid);
        }
        let client = rest_book::client()?;
        let book = rest_book::fetch_lighter_book_from_base(
            &client,
            &self.base_url,
            w.market_index as u32,
            20,
        )
        .await?;
        book.mid()
            .ok_or_else(|| anyhow!("no Lighter mid for {coin}"))
    }
}

#[derive(Debug, Clone)]
pub struct HlClearinghouse {
    pub margin_summary: HlMarginSummary,
    pub asset_positions: Vec<HlAssetPosition>,
    pub withdrawable: String,
    /// mono_now_ns no later than when the OLDEST piece of this state was read from the
    /// venue (WS feed stamp for fresh-cache pieces, REST read-start otherwise). The
    /// reconciler mins this into `AccountSnapshot::read_start_ns` so the orphan backstop's
    /// straddle guard sees the true data origin, not the snapshot assembly time.
    pub data_origin_ns: i64,
    pub margin_source_ns: i64,
}
#[derive(Debug, Clone)]
pub struct HlMarginSummary {
    pub account_value: String,
}
#[derive(Debug, Clone)]
pub struct HlAssetPosition {
    pub position: HlPosition,
}
#[derive(Debug, Clone)]
pub struct HlPosition {
    pub coin: String,
    pub szi: String,
    pub entry_px: Option<String>,
}
#[derive(Debug, Clone)]
pub struct HlOpenOrder {
    pub coin: String,
    pub oid: u64,
    pub side: String,
    pub limit_px: String,
    pub sz: String,
}

pub async fn run_hl_worker(mut rx: Receiver<HedgeCommand>, tx: Sender<ExecEvent>, ex: HlExchange, journal: Journal) {
    info!("lighter hedge worker started");
    let mut waits = tokio::task::JoinSet::new();
    while let Some(cmd) = rx.recv().await {
        while waits.try_join_next().is_some() {}
        match cmd {
            HedgeCommand::Shutdown => {
                rx.close();
                while let Some(cmd) = rx.recv().await {
                    handle_hedge_cmd(&ex, &tx, &journal, &mut waits, cmd).await;
                }
                break;
            }
            HedgeCommand::RefreshNonce => refresh_nonce(&ex, &mut waits).await,
            cmd => handle_hedge_cmd(&ex, &tx, &journal, &mut waits, cmd).await,
        }
    }
    let deadline = tokio::time::Instant::now() + RESOLUTION_BUDGET + Duration::from_secs(1);
    while !waits.is_empty() {
        if tokio::time::timeout_at(deadline, waits.join_next()).await.is_err() {
            waits.abort_all();
            while waits.join_next().await.is_some() {}
            break;
        }
    }
    info!("lighter hedge worker stopped");
}

/// Hedges wait (`tx_ready`) until the nonce is re-read; a failed read is repaired in the
/// background.
async fn refresh_nonce(ex: &HlExchange, waits: &mut tokio::task::JoinSet<()>) {
    ex.nonce_uncertain.store(true, Ordering::Release);
    if ex.nonce.hard_refresh(&ex.rest).await.is_ok() { ex.nonce_uncertain.store(false, Ordering::Release); }
    else { let ex = ex.clone(); waits.spawn(async move { repair_nonce(&ex).await }); }
}

async fn handle_hedge_cmd(ex: &HlExchange, tx: &Sender<ExecEvent>, journal: &Journal,
    waits: &mut tokio::task::JoinSet<()>, cmd: HedgeCommand) {
    let HedgeCommand::Hedge { mut intent, aggressive_px, .. } = cmd else { return };
    let cloid = intent.cloid;
    if intent.admission.is_claimed() {
        warn!("duplicate claimed execution ticket ignored: {}", cloid.to_hex());
        return;
    }
    if !ex.tx_ready() || waits.len() >= 64 {
        intent.admission.cancel_queued();
        if !ex.tx_ws.is_ready() { ex.tx_ws.request_reconnect(); }
        if intent.admission.is_cancelled() {
            let _ = tx.send(ExecEvent::AttemptNotSent { cloid,
                reason: format!("{HEDGE_NOT_SENT_PREFIX} transport/nonce not ready or resolution capacity reached") }).await;
        }
        return;
    }
    if !intent.admission.try_claim(crate::hotpath::clock::mono_now_ns()) {
        if intent.admission.is_cancelled() {
            let _ = tx.send(ExecEvent::AttemptNotSent { cloid, reason: "expired or cancelled before send".into() }).await;
        }
        return;
    }
    match ex.send_hedge_tx(&intent, aggressive_px).await {
        HedgeSendOutcome::Terminal { ev, refresh_nonce_after_emit, proof } => {
            if let Some(proof) = proof { let _ = tx.send(ExecEvent::AttemptStarted { cloid, proof }).await; }
            let _ = tx.send(ev).await;
            if refresh_nonce_after_emit { refresh_nonce(ex, waits).await; }
        }
        HedgeSendOutcome::AwaitFills { token, rx, overflow, requested_qty, proof, ambiguous } => {
            intent.qty = requested_qty;
            intent.wire = Some(proof.clone());
            let _ = tx.send(ExecEvent::AttemptStarted { cloid, proof }).await;
            if ambiguous { let _ = tx.send(ExecEvent::HedgeUnknown { cloid, reason: "write/response outcome ambiguous; awaiting terminal evidence".into() }).await; }
            let (ex, tx, journal) = (ex.clone(), tx.clone(), journal.clone());
            waits.spawn(async move { resolve_attempt(ex, tx, journal, intent, token, rx, overflow, ambiguous).await; });
        }
    }
}

async fn publish_progress(tx: &Sender<ExecEvent>, intent: &HedgeIntent, totals: &FillTotals,
    terminal_qty: Option<Decimal>, order_id: Option<i64>) {
    let (observed_qty, quote, fee) = totals.values();
    let qty = terminal_qty.unwrap_or(observed_qty);
    let complete = qty == observed_qty;
    let _ = tx.send(ExecEvent::ExecutionProgress { cloid: intent.cloid,
        cumulative_qty: qty, cumulative_quote_usd: complete.then_some(quote),
        cumulative_fee_usd: if complete { fee } else { None }, terminal: terminal_qty.is_some(),
        venue_order_id: order_id.map(|x| x.to_string()),
        event_time_ms: if complete { totals.trades.values().filter_map(|t| t.event_time_ms).max() } else { None } }).await;
}

struct ResolutionContext {
    intent: HedgeIntent,
    account: i64,
    market: u32,
    fill_timeout: Duration,
    ambiguous: bool,
}

/// The send/nonce owner is separate from observation and cold history requests.
async fn resolve_attempt(ex: HlExchange, tx: Sender<ExecEvent>, journal: Journal, intent: HedgeIntent,
    token: u64, rx: Receiver<FillUpdate>, overflow: Arc<AtomicBool>, ambiguous: bool) {
    let index = intent.cloid.to_lighter_client_order_index();
    let _route = FillRouteGuard { fills: ex.fills.clone(), client_order_index: index, token };
    let context = ResolutionContext { market: ex.wire(&intent.market).map(|w| w.market_index as u32).unwrap_or(u32::MAX),
        account: ex.account_index, fill_timeout: ex.fill_timeout, intent: intent.clone(), ambiguous };
    let lookup_exchange = ex.clone();
    let resolution = resolve_updates(context, tx, journal, rx, overflow, move || {
        let ex = lookup_exchange.clone();
        let intent = intent.clone();
        async move { ex.terminal_order_and_trades(&intent).await }
    });
    tokio::pin!(resolution);
    // An ambiguous send leaves the nonce uncertain until it resolves, or until the active
    // budget is spent: by then the venue's next nonce counts it, landed or not.
    let resolved = tokio::select! {
        resolved = &mut resolution => Some(resolved),
        _ = tokio::time::sleep(RESOLUTION_BUDGET), if ambiguous => None,
    };
    if ambiguous && resolved != Some(false) { repair_nonce(&ex).await; }
    if resolved.is_none() { resolution.await; }
}

/// Re-reads the venue's nonce until it answers: one failed read must not leave the executor
/// dark for good (an uncertain nonce stops quoting as well as hedging).
async fn repair_nonce(ex: &HlExchange) {
    while ex.nonce.hard_refresh(&ex.rest).await.is_err() {
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
    ex.nonce_uncertain.store(false, Ordering::Release);
}

/// Production observation loop with a cold lookup seam for deterministic local tests.
async fn resolve_updates<F, Fut>(context: ResolutionContext, tx: Sender<ExecEvent>, journal: Journal,
    mut rx: Receiver<FillUpdate>, overflow: Arc<AtomicBool>, mut lookup: F) -> bool
where F: FnMut() -> Fut, Fut: std::future::Future<Output = Result<Option<(RemoteOrder, Vec<TradePayload>)>>> {
    let ResolutionContext { intent, account, market, fill_timeout, ambiguous } = context;
    let index = intent.cloid.to_lighter_client_order_index();
    let deadline = tokio::time::Instant::now() + RESOLUTION_BUDGET;
    let first_poll = tokio::time::Instant::now() + if ambiguous { Duration::ZERO } else { fill_timeout };
    let mut poll = tokio::time::interval_at(first_poll, Duration::from_secs(2));
    poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut totals = FillTotals::default();
    let mut terminal: Option<(Decimal, Option<i64>)> = None;
    let mut unknown_emitted = ambiguous;
    let mut passive = false;
    loop {
        if totals.values().0 == intent.qty { terminal = Some((intent.qty, terminal.and_then(|x| x.1))); }
        if let Some((qty, oid)) = terminal {
            if totals.values().0 == qty || passive {
                publish_progress(&tx, &intent, &totals, Some(qty), oid).await;
                return true;
            }
        }
        tokio::select! {
            _ = tokio::time::sleep_until(deadline), if !passive => {
                if let Some((qty, oid)) = terminal {
                    publish_progress(&tx, &intent, &totals, Some(qty), oid).await;
                    return true;
                }
                let _ = tx.send(ExecEvent::HedgeUnknown { cloid: intent.cloid,
                    reason: "60s active resolution exhausted; frozen reservation and passive late-event route retained".into() }).await;
                passive = true;
            }
            update = rx.recv() => match update {
                Some(FillUpdate::Trade(tr)) => {
                    if let Some(evidence) = totals.observe(&tr, &intent, account, terminal.and_then(|x| x.1)) {
                        journal.execution_trade(crate::hotpath::clock::mono_now_ns(), evidence);
                        publish_progress(&tx, &intent, &totals, None, terminal.and_then(|x| x.1)).await;
                    }
                }
                Some(FillUpdate::Order(order)) => {
                    if let Some(qty) = terminal_order_qty(&order, index, account, Some(market)) {
                        terminal = Some((qty, order.order_index));
                    }
                }
                None => return false,
            },
            _ = poll.tick(), if !passive => {
                if !unknown_emitted {
                    let _ = tx.send(ExecEvent::HedgeUnknown { cloid: intent.cloid,
                        reason: "fill observation timeout; cold terminal resolution pending".into() }).await;
                    unknown_emitted = true;
                }
                if overflow.swap(false, Ordering::AcqRel) { warn!("fill route overflow; reconciling {} from venue history", intent.cloid.to_hex()); }
                if let Ok(Ok(Some((order, trades)))) = tokio::time::timeout_at(deadline, lookup()).await {
                    if let Some(qty) = terminal_order_qty(&order, index, account, Some(market)) {
                        for tr in trades {
                            if let Some(evidence) = totals.observe(&tr, &intent, account, order.order_index) {
                                journal.execution_trade(crate::hotpath::clock::mono_now_ns(), evidence);
                            }
                        }
                        publish_progress(&tx, &intent, &totals, Some(qty), order.order_index).await;
                        return true;
                    }
                }
            }
        }
    }
}

fn terminal_order_qty(order: &RemoteOrder, index: i64, account: i64, market: Option<u32>) -> Option<Decimal> {
    if order.client_order_index != Some(index) || !order.is_terminal()
        || order.owner_account_index.is_some_and(|a| a != account)
        || order.market_index.is_some_and(|m| Some(m) != market) { return None; }
    order.filled_base_amount.as_deref()?.parse::<Decimal>().ok().filter(|q| *q >= Decimal::ZERO)
}

impl HlExchange {
    async fn terminal_order_and_trades(&self, intent: &HedgeIntent) -> Result<Option<(RemoteOrder, Vec<TradePayload>)>> {
        let market_id = self.wire(&intent.market)?.market_index as u32;
        let auth = generate_ws_auth_token(&self.signer, self.api_key_index)?;
        let index = intent.cloid.to_lighter_client_order_index();
        let mut cursor: Option<String> = None;
        for _ in 0..crate::lighter::rest::HISTORY_PAGES {
            let page = self.rest.account_inactive_orders(self.account_index, market_id, &auth, cursor.as_deref()).await?;
            let rows = page.get("orders").and_then(|x| x.as_array()).ok_or_else(|| anyhow!("inactive order response missing orders"))?;
            for row in rows {
                let Ok(order) = serde_json::from_value::<RemoteOrder>(row.clone()) else { continue };
                if terminal_order_qty(&order, index, self.account_index, Some(market_id)).is_none() { continue; }
                let mut trades = Vec::new();
                if let Some(oid) = order.order_index {
                    let mut trade_cursor: Option<String> = None;
                    for _ in 0..64 {
                        let Ok(page) = self.rest.trades_by_order(self.account_index, oid, &auth, trade_cursor.as_deref()).await else { break };
                        if let Some(rows) = page.get("trades").and_then(|x| x.as_array()) {
                            for row in rows { if let Ok(trade) = serde_json::from_value(row.clone()) { trades.push(trade); } }
                        }
                        let next = page.get("next_cursor").and_then(|v| v.as_str()).filter(|s| !s.is_empty()).map(str::to_owned);
                        if next.is_none() || next == trade_cursor { break; }
                        trade_cursor = next;
                    }
                }
                return Ok(Some((order, trades)));
            }
            let next = page.get("next_cursor").and_then(|v| v.as_str()).filter(|s| !s.is_empty()).map(str::to_owned);
            if next.is_none() || next == cursor { break; }
            cursor = next;
        }
        Ok(None)
    }
}


fn apply_account_all_positions(
    state: &AccountFeedState,
    known_markets: &[u32],
    msg: &AccountAllPositionsMsg,
) {
    let mut seen = HashSet::new();
    for (market, position) in &msg.positions {
        if let Ok(market_id) = market.parse::<u32>() {
            seen.insert(market_id);
            state.set_position(
                market_id,
                signed_position_payload_dec(position),
                position_entry_px_dec(position),
            );
        }
    }

    if msg.is_snapshot() {
        for market_id in known_markets {
            if !seen.contains(market_id) {
                state.set_position(*market_id, Decimal::ZERO, Decimal::ZERO);
            }
        }
        state.mark_positions_ready();
    }
    // Every applied message (snapshot or delta) proves the feed is alive; the reconciler
    // uses this stamp to decide whether the cache can stand in for a venue read.
    state.stamp_positions_updated();
}

fn spawn_order_book_stream(
    ws_url: String,
    market_id: u32,
    symbol: String,
    books: Arc<BookFeedState>,
    shutdown: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    let channel = format!("order_book/{market_id}");
    let mut opts = SubscribeOptions::new(
        &ws_url,
        &format!("lighter-order-book-{symbol}-{market_id}"),
        vec![channel],
    );
    // Sequence-gap resyncs deliberately drop the session for a fresh snapshot; the 5s
    // default base leaves the exec's book cache dark that whole time. 0.5s restores it
    // promptly; consecutive failures still back off toward reconnect_max.
    opts.reconnect_base = 0.5;
    let books_for_msg = books.clone();
    let books_for_disconnect = books.clone();
    let reconnect = Arc::new(Notify::new());
    let reconnect_on_gap = reconnect.clone();
    tokio::spawn(async move {
        tokio::select! {
            _ = shutdown.cancelled() => {}
            _ = subscribe_loop(
                opts,
                Some(reconnect),
                move |frame| {
                    if let Ok(msg) = serde_json::from_str::<OrderBookMsg>(frame.raw) {
                        if !books_for_msg.apply(market_id, &msg) {
                            tracing::warn!(
                                "Lighter order_book sequence gap for market {}; reconnecting for fresh snapshot",
                                market_id
                            );
                            books_for_msg.reset(market_id);
                            reconnect_on_gap.notify_one();
                        }
                    }
                },
                move || {
                    books_for_disconnect.reset(market_id);
                },
            ) => {}
        }
    })
}

fn auth_map(signer: &Signer, api_key_index: i32, channel: &str) -> HashMap<String, String> {
    // FFI sign on the (multi-thread) main runtime at reconnect time — never
    // latency-critical, so hand the blocking section to the scheduler explicitly
    // instead of stalling this worker thread's queue.
    tokio::task::block_in_place(|| generate_ws_auth_token(signer, api_key_index))
        .map(|token| HashMap::from([(channel.to_string(), token)]))
        .unwrap_or_default()
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

fn value_dec(v: Option<&serde_json::Value>) -> Option<Decimal> {
    match v? {
        serde_json::Value::String(s) => s.parse().ok(),
        serde_json::Value::Number(n) => n.to_string().parse().ok(),
        _ => None,
    }
}

/// `false` when any level is unparseable — the caller must treat the frame as a gap
/// (reset + reconnect) rather than silently dropping the level and desyncing the book.
fn apply_levels(side: &mut BTreeMap<Decimal, Decimal>, levels: &[PriceLevel]) -> bool {
    for level in levels {
        let Some(px) = level.price.parse::<Decimal>().ok() else {
            return false;
        };
        let Some(qty) = level.size.parse::<Decimal>().ok() else {
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

fn position_entry_px_dec(p: &crate::lighter::messages::PositionPayload) -> Decimal {
    p.avg_entry_price
        .as_deref()
        .and_then(|s| s.parse::<Decimal>().ok())
        .unwrap_or(Decimal::ZERO)
}

fn lighter_leverage_from_account(
    raw: &serde_json::Value,
    market_id: u32,
    symbol: &str,
) -> Result<Decimal> {
    let account = raw
        .get("accounts")
        .and_then(|a| a.as_array())
        .and_then(|a| a.first())
        .unwrap_or(raw);
    let rows = account
        .get("positions")
        .and_then(|p| p.as_array())
        .ok_or_else(|| anyhow!("Lighter account payload has no positions/leverage rows"))?;
    let row = rows
        .iter()
        .find(|p| p.get("market_id").and_then(|m| m.as_u64()) == Some(market_id as u64))
        .ok_or_else(|| anyhow!("Lighter account payload has no leverage row for {symbol}"))?;
    let margin_mode = row
        .get("margin_mode")
        .and_then(|m| m.as_i64())
        .unwrap_or(-1);
    if margin_mode != MARGIN_MODE_CROSS as i64 {
        bail!(
            "Lighter margin mode for {} is {} (expected cross margin mode {})",
            symbol,
            margin_mode,
            MARGIN_MODE_CROSS
        );
    }
    let fraction = value_dec(row.get("initial_margin_fraction")).ok_or_else(|| {
        anyhow!("Lighter leverage row for {symbol} has no initial_margin_fraction")
    })?;
    if fraction <= Decimal::ZERO {
        bail!("Lighter initial_margin_fraction for {symbol} is {fraction}");
    }
    Ok((Decimal::from(100u32) / fraction).normalize())
}

fn signed_position_from_json(v: &serde_json::Value) -> Decimal {
    let mag = value_dec(v.get("position"))
        .or_else(|| value_dec(v.get("size")))
        .unwrap_or(Decimal::ZERO)
        .abs();
    let sign = v.get("sign").and_then(|s| s.as_i64()).unwrap_or(1);
    if sign < 0 {
        -mag
    } else {
        mag
    }
}

fn client_index_from_hex(s: &str) -> Option<i64> {
    let hex = s.strip_prefix("0x").unwrap_or(s);
    if hex.len() != 32 {
        return None;
    }
    let bytes = hex::decode(hex).ok()?;
    let mut arr = [0u8; 16];
    arr.copy_from_slice(&bytes);
    Some(Cloid::from_bytes_for_lighter(arr).to_lighter_client_order_index())
}

fn random_client_order_index(market: &MarketId, side: Side) -> i64 {
    let cloid = Cloid::recovery(
        market,
        chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default() ^ side as i64,
    );
    cloid.to_lighter_client_order_index()
}

#[cfg(test)]
mod tests {
    #[test]
    fn hedge_retry_gate_accepts_only_guaranteed_no_fill_rejects() {
        use super::{hedge_reject_is_definitive_no_fill, HEDGE_NOT_SENT_PREFIX};
        // The two guaranteed-nothing-landed shapes are retryable...
        assert!(hedge_reject_is_definitive_no_fill(
            "Lighter reject code=21505 order could not immediately match against any resting orders"
        ));
        assert!(hedge_reject_is_definitive_no_fill(&format!(
            "{HEDGE_NOT_SENT_PREFIX} connect_timeout"
        )));
        // ...everything ambiguous or resting must stay frozen.
        assert!(!hedge_reject_is_definitive_no_fill(
            "Lighter reject code=0 order unexpectedly resting"
        ));
        assert!(!hedge_reject_is_definitive_no_fill("send_timeout"));
        assert!(!hedge_reject_is_definitive_no_fill(
            "Lighter tx outcome unknown: response_timeout"
        ));
    }

    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn raw_amount_floors_size() {
        assert_eq!(raw_amount(dec!(1.239), 2).unwrap(), 123);
        assert!(raw_amount(dec!(0.0001), 2).is_err());
    }

    #[test]
    fn raw_price_rounds_toward_marketability() {
        assert_eq!(raw_price(dec!(123.451), 2, Side::Buy).unwrap(), 12346);
        assert_eq!(raw_price(dec!(123.459), 2, Side::Sell).unwrap(), 12345);
    }

    #[test]
    fn native_plan_builder_enforces_wire_codes_and_directional_rounding() {
        let wire = LighterMarketWire { market_index: 24, symbol: "HYPE".into(), size_decimals: 3, price_decimals: 2 };
        let buy = order_plan(&wire, Side::Buy, dec!(123.451), dec!(0.5019), 91, true, ORDER_TYPE_LIMIT).unwrap();
        assert_eq!((buy.market_index, buy.client_order_index, buy.base_amount, buy.price), (24, 91, 501, 12346));
        assert_eq!((buy.order_type, buy.time_in_force, buy.order_expiry), (0, 0, 0));
        assert!(!buy.is_ask && buy.reduce_only);
        let sell = order_plan(&wire, Side::Sell, dec!(123.459), dec!(0.5), 92, true, ORDER_TYPE_MARKET).unwrap();
        assert_eq!((sell.order_type, sell.time_in_force, sell.order_expiry, sell.price), (1, 0, 0, 12345));
        assert!(sell.is_ask && sell.reduce_only);
    }

    #[test]
    fn signed_position_respects_lighter_sign() {
        let v = serde_json::json!({"position":"2.5","sign":-1});
        assert_eq!(signed_position_from_json(&v), dec!(-2.5));
    }

    fn test_intent() -> HedgeIntent {
        HedgeIntent::with_qty(Cloid::from_bytes_for_lighter([0;16]), "HYPE".into(), Side::Sell, dec!(0.5), dec!(100), 0)
    }
    fn trade(id: i64, qty: &str) -> TradePayload {
        TradePayload { trade_id: Some(id), ask_client_id: Some(1), ask_account_id: Some(9), ask_id: Some(100),
            price: Some("100".into()), size: Some(qty.into()), is_maker_ask: Some(false),
            taker_fee: Some(serde_json::json!(25)), ..TradePayload::default() }
    }
    #[test]
    fn fee_uses_own_role_notional_and_preserves_unknown() {
        let intent = test_intent();
        assert_eq!(own_trade_evidence(&trade(1,"0.5"), &intent, 9, None).unwrap().fee_usd, Some(dec!(0.00125)));
        let mut row = trade(1,"0.5");
        row.taker_fee = None;
        row.maker_fee = Some(serde_json::json!(99999));
        assert_eq!(own_trade_evidence(&row, &intent, 9, None).unwrap().fee_usd, Some(dec!(0)));
        row.taker_fee = Some(serde_json::Value::Null);
        assert_eq!(own_trade_evidence(&row, &intent, 9, None).unwrap().fee_usd, None);
        row.taker_fee = Some(serde_json::json!(-25));
        assert_eq!(own_trade_evidence(&row, &intent, 9, None).unwrap().fee_usd, Some(dec!(-0.00125)));
        row.is_maker_ask = Some(true);
        let contradictory = own_trade_evidence(&row, &intent, 9, None).unwrap();
        assert_eq!(contradictory.qty, dec!(0.5));
        assert_eq!(contradictory.fee_usd, None);
    }
    #[test]
    fn split_trade_fees_are_invariant_and_replays_do_not_double_count() {
        let intent = test_intent();
        let mut totals = FillTotals::default();
        for row in [trade(1,"0.2"), trade(2,"0.3"), trade(1,"0.2")] { totals.observe(&row, &intent, 9, None); }
        assert_eq!(totals.values(), (dec!(0.5), dec!(50), Some(dec!(0.00125))));
        let mut missing_id = trade(3,"0.5"); missing_id.trade_id = None;
        assert!(!totals.observe(&missing_id, &intent, 9, None).unwrap().identity_complete);
        assert_eq!(totals.values().0, dec!(0.5));
    }

    #[test]
    fn lighter_leverage_parses_initial_margin_fraction_percent() {
        let raw = serde_json::json!({
            "accounts": [{
                "positions": [
                    {"market_id": 1, "symbol": "BTC", "initial_margin_fraction": "50.00", "margin_mode": 0},
                    {"market_id": 24, "symbol": "HYPE", "initial_margin_fraction": "100.00", "margin_mode": 0}
                ]
            }]
        });
        assert_eq!(
            lighter_leverage_from_account(&raw, 24, "HYPE").unwrap(),
            dec!(1)
        );
        assert_eq!(
            lighter_leverage_from_account(&raw, 1, "BTC").unwrap(),
            dec!(2)
        );
    }

    #[test]
    fn lighter_leverage_fails_closed_on_missing_or_non_cross_market() {
        let missing = serde_json::json!({"accounts": [{"positions": []}]});
        assert!(lighter_leverage_from_account(&missing, 24, "HYPE").is_err());

        let isolated = serde_json::json!({
            "accounts": [{
                "positions": [
                    {"market_id": 24, "symbol": "HYPE", "initial_margin_fraction": "100.00", "margin_mode": 1}
                ]
            }]
        });
        assert!(lighter_leverage_from_account(&isolated, 24, "HYPE").is_err());
    }

    #[test]
    fn fill_routes_are_bounded_and_collision_cannot_evict_owner() {
        let tracker = FillTracker::default();
        let (token, mut rx, overflow) = tracker.register(1).unwrap();
        assert!(tracker.register(1).is_err());
        for _ in 0..=FILL_ROUTE_CAPACITY { tracker.on_trade(trade(1,"0.1")); }
        assert!(overflow.load(Ordering::Acquire));
        assert!(matches!(rx.try_recv().unwrap(), FillUpdate::Trade(_)));
        tracker.unregister(1, token+1);
        assert!(tracker.register(1).is_err());
        tracker.unregister(1, token);
        let (_next_token, mut next_rx, _) = tracker.register(1).unwrap();
        tracker.unregister(1, token);
        tracker.on_trade(trade(2,"0.1"));
        assert!(matches!(next_rx.try_recv().unwrap(), FillUpdate::Trade(_)));
    }

    fn resolution_context() -> ResolutionContext {
        ResolutionContext { intent: test_intent(), account: 9, market: 24, fill_timeout: Duration::from_millis(500), ambiguous: false }
    }
    fn terminal(qty: &str) -> RemoteOrder {
        RemoteOrder { client_order_index: Some(1), order_index: Some(100), owner_account_index: Some(9), market_index: Some(24),
            status: Some("canceled".into()), filled_base_amount: Some(qty.into()), ..RemoteOrder::default() }
    }

    #[tokio::test(start_paused = true)]
    async fn full_fill_resolves_without_waiting_for_timeout() {
        let (input, rx) = mpsc::channel(16); let (events, mut output) = mpsc::channel(16);
        input.send(FillUpdate::Trade(trade(1,"0.5"))).await.unwrap();
        let start = tokio::time::Instant::now();
        assert!(resolve_updates(resolution_context(), events, Journal::null(), rx, Arc::new(AtomicBool::new(false)),
            || async { Ok(None) }).await);
        assert_eq!(tokio::time::Instant::now(), start);
        let all: Vec<_> = std::iter::from_fn(|| output.try_recv().ok()).collect();
        assert!(all.iter().any(|ev| matches!(ev, ExecEvent::ExecutionProgress { cumulative_qty, terminal: true, .. } if *cumulative_qty==dec!(0.5))));
    }

    #[tokio::test(start_paused = true)]
    async fn partial_timeout_is_not_terminal_and_late_fill_remains_routed() {
        let (input, rx) = mpsc::channel(16); let (events, mut output) = mpsc::channel(32);
        input.send(FillUpdate::Trade(trade(1,"0.2"))).await.unwrap();
        let resolver = tokio::spawn(resolve_updates(resolution_context(), events, Journal::null(), rx,
            Arc::new(AtomicBool::new(false)), || async { Ok(None) }));
        assert!(matches!(output.recv().await.unwrap(), ExecEvent::ExecutionProgress { cumulative_qty, terminal: false, .. } if cumulative_qty==dec!(0.2)));
        tokio::time::advance(Duration::from_secs(61)).await;
        tokio::task::yield_now().await;
        assert!(!resolver.is_finished());
        let interim: Vec<_> = std::iter::from_fn(|| output.try_recv().ok()).collect();
        assert!(!interim.iter().any(|ev| matches!(ev, ExecEvent::ExecutionProgress { terminal: true, .. })));
        assert!(interim.iter().any(|ev| matches!(ev, ExecEvent::HedgeUnknown { reason, .. } if reason.contains("60s"))));
        input.send(FillUpdate::Trade(trade(2,"0.3"))).await.unwrap();
        assert!(resolver.await.unwrap());
        assert!(std::iter::from_fn(|| output.try_recv().ok()).any(|ev| matches!(ev, ExecEvent::ExecutionProgress { cumulative_qty, terminal: true, .. } if cumulative_qty==dec!(0.5))));
    }

    #[tokio::test(start_paused = true)]
    async fn cold_terminal_proof_preserves_quantity_when_trade_economics_missing() {
        let (_input, rx) = mpsc::channel(16); let (events, mut output) = mpsc::channel(16);
        assert!(resolve_updates(resolution_context(), events, Journal::null(), rx, Arc::new(AtomicBool::new(false)),
            || async { Ok(Some((terminal("0.2"), Vec::new()))) }).await);
        assert!(std::iter::from_fn(|| output.try_recv().ok()).any(|ev| matches!(ev,
            ExecEvent::ExecutionProgress { cumulative_qty, cumulative_quote_usd: None, cumulative_fee_usd: None, terminal: true, .. } if cumulative_qty==dec!(0.2))));
        let mut wrong_owner = terminal("0.2"); wrong_owner.owner_account_index = Some(8);
        assert!(terminal_order_qty(&wrong_owner,1,9,Some(24)).is_none());
    }

    #[test]
    fn account_all_positions_sparse_update_does_not_zero_missing_markets() {
        let state = AccountFeedState::default();
        let snapshot: AccountAllPositionsMsg = serde_json::from_value(serde_json::json!({
            "type": "subscribed/account_all_positions",
            "positions": {
                "24": {"position": "1.5", "sign": -1, "avg_entry_price": "2.25"}
            }
        }))
        .unwrap();
        apply_account_all_positions(&state, &[24, 25], &snapshot);
        assert_eq!(state.position(24), Some((dec!(-1.5), dec!(2.25))));
        assert_eq!(state.position(25), Some((Decimal::ZERO, Decimal::ZERO)));

        let sparse_update: AccountAllPositionsMsg = serde_json::from_value(serde_json::json!({
            "type": "update/account_all_positions",
            "positions": {}
        }))
        .unwrap();
        apply_account_all_positions(&state, &[24, 25], &sparse_update);
        assert_eq!(state.position(24), Some((dec!(-1.5), dec!(2.25))));
        assert_eq!(state.position(25), Some((Decimal::ZERO, Decimal::ZERO)));
    }

    #[test]
    fn account_feed_caches_live_open_orders_by_market() {
        let state = AccountFeedState::default();
        // Parsed exactly as the stream handler does: typed AccountOrdersMsg off raw text.
        let msg: AccountOrdersMsg = serde_json::from_str(
            r#"{
            "type": "update/account_all_orders",
            "orders": {
                "24": [
                    {"client_order_index": 1, "is_ask": true, "price": "101", "remaining_base_amount": "0.5", "status": "open"},
                    {"client_order_index": 2, "status": "filled"}
                ],
                "25": []
            }
        }"#,
        )
        .unwrap();
        state.set_open_orders_for_markets(&[24, 25, 26], &msg.orders);
        let cached = state.open_orders().unwrap();
        assert_eq!(cached.get(&24).unwrap().len(), 1);
        assert_eq!(cached.get(&25).unwrap().len(), 0);
        assert_eq!(cached.get(&26).unwrap().len(), 0);
    }

    #[test]
    fn book_feed_detects_nonce_gap_and_keeps_top_of_book() {
        let feed = BookFeedState::default();
        let first: OrderBookMsg = serde_json::from_value(serde_json::json!({
            "type": "subscribed/order_book",
            "order_book": {
                "nonce": 10,
                "bids": [{"price": "100", "size": "1"}],
                "asks": [{"price": "102", "size": "2"}]
            }
        }))
        .unwrap();
        assert!(feed.apply(24, &first));
        assert_eq!(feed.order_book(24).and_then(|b| b.mid()), Some(dec!(101)));

        // begin_nonce ahead of our position => missed updates => resync.
        let gap: OrderBookMsg = serde_json::from_value(serde_json::json!({
            "type": "update/order_book",
            "order_book": {
                "begin_nonce": 11,
                "nonce": 12,
                "bids": [{"price": "100", "size": "0"}],
                "asks": []
            }
        }))
        .unwrap();
        assert!(!feed.apply(24, &gap));
    }

    #[test]
    fn book_feed_resyncs_on_malformed_level_and_keeps_zero_size_deletes() {
        let feed = BookFeedState::default();
        let snapshot: OrderBookMsg = serde_json::from_value(serde_json::json!({
            "type": "subscribed/order_book",
            "order_book": {
                "nonce": 10,
                "bids": [{"price": "100", "size": "1"}, {"price": "99", "size": "2"}],
                "asks": [{"price": "102", "size": "2"}]
            }
        }))
        .unwrap();
        assert!(feed.apply(24, &snapshot));

        // Regression pin: an explicit "0" size deletes the level (no resync).
        let delete: OrderBookMsg = serde_json::from_value(serde_json::json!({
            "type": "update/order_book",
            "order_book": {
                "begin_nonce": 10,
                "nonce": 11,
                "bids": [{"price": "99", "size": "0"}],
                "asks": []
            }
        }))
        .unwrap();
        assert!(feed.apply(24, &delete));
        assert_eq!(
            feed.order_book(24).and_then(|b| b.best_bid().map(|l| l.px)),
            Some(dec!(100))
        );

        // A malformed size must ride the gap path — dropping it silently (old
        // behavior) desyncs the hedge book.
        let malformed: OrderBookMsg = serde_json::from_value(serde_json::json!({
            "type": "update/order_book",
            "order_book": {
                "begin_nonce": 11,
                "nonce": 12,
                "bids": [{"price": "100", "size": "nope"}],
                "asks": []
            }
        }))
        .unwrap();
        assert!(!feed.apply(24, &malformed));
    }

    #[test]
    fn book_feed_skips_stale_replay_and_rejects_pre_snapshot_delta() {
        let feed = BookFeedState::default();
        // A delta with no snapshot to apply to must never seed the book.
        let premature: OrderBookMsg = serde_json::from_value(serde_json::json!({
            "type": "update/order_book",
            "order_book": {
                "nonce": 10,
                "bids": [{"price": "100", "size": "1"}],
                "asks": [{"price": "102", "size": "2"}]
            }
        }))
        .unwrap();
        assert!(!feed.apply(24, &premature));
        assert!(feed.order_book(24).is_none());

        let snapshot: OrderBookMsg = serde_json::from_value(serde_json::json!({
            "type": "subscribed/order_book",
            "order_book": {
                "nonce": 10,
                "bids": [{"price": "100", "size": "1"}],
                "asks": [{"price": "102", "size": "2"}]
            }
        }))
        .unwrap();
        assert!(feed.apply(24, &snapshot));

        // A replay ending at-or-before our position is dropped without a resync.
        let stale: OrderBookMsg = serde_json::from_value(serde_json::json!({
            "type": "update/order_book",
            "order_book": {
                "begin_nonce": 8,
                "nonce": 9,
                "bids": [{"price": "100", "size": "0"}],
                "asks": []
            }
        }))
        .unwrap();
        assert!(feed.apply(24, &stale));
        assert_eq!(feed.order_book(24).and_then(|b| b.mid()), Some(dec!(101)));
    }

    #[test]
    fn book_feed_detects_offset_gap_without_nonce() {
        let feed = BookFeedState::default();
        let first: OrderBookMsg = serde_json::from_value(serde_json::json!({
            "type": "subscribed/order_book",
            "offset": 10,
            "order_book": {
                "bids": [{"price": "100", "size": "1"}],
                "asks": [{"price": "102", "size": "2"}]
            }
        }))
        .unwrap();
        assert!(feed.apply(24, &first));

        let gap: OrderBookMsg = serde_json::from_value(serde_json::json!({
            "type": "update/order_book",
            "offset": 12,
            "order_book": {
                "bids": [{"price": "100", "size": "0"}],
                "asks": []
            }
        }))
        .unwrap();
        assert!(!feed.apply(24, &gap));
    }

    #[test]
    fn book_feed_rejects_delta_without_sequence_metadata() {
        let feed = BookFeedState::default();
        let first: OrderBookMsg = serde_json::from_value(serde_json::json!({
            "type": "subscribed/order_book",
            "order_book": {
                "bids": [{"price": "100", "size": "1"}],
                "asks": [{"price": "102", "size": "2"}]
            }
        }))
        .unwrap();
        assert!(feed.apply(24, &first));

        let unsequenced: OrderBookMsg = serde_json::from_value(serde_json::json!({
            "type": "update/order_book",
            "order_book": {
                "bids": [{"price": "100", "size": "0"}],
                "asks": []
            }
        }))
        .unwrap();
        assert!(!feed.apply(24, &unsequenced));
    }
}
