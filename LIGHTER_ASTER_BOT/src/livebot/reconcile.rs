//! Account/position observations: signed Aster REST, fresh Lighter account cache with REST
//! fallback, or Hyperliquid REST. Publishes [`AccountSnapshot`] at startup and periodically.
//! The strategy confirms persistent residuals before recovery; this module submits no orders.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result};
use rust_decimal::Decimal;
use tokio::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::hotpath::clock::mono_now_ns;
use crate::markets::MarketSpec;
use crate::types::{MarketId, Side};

use super::account::{AccountSnapshot, AccountState, OpenOrderSnapshot, ScaledPosition, Venue};
use super::exec::aster::{AsterBalanceRow, AsterOpenOrder, AsterPositionRow, AsterRest};
use super::exec::hyperliquid::HyperliquidHedge;
use super::exec::lighter::{HedgeReadiness, HlClearinghouse, HlOpenOrder, LighterExchange};

/// The hedge venue's account reads.
#[derive(Clone)]
pub enum HedgeAccount {
    Lighter(LighterExchange),
    Hyperliquid(Arc<HyperliquidHedge>),
}

impl From<LighterExchange> for HedgeAccount {
    fn from(lighter: LighterExchange) -> Self {
        HedgeAccount::Lighter(lighter)
    }
}

impl HedgeAccount {
    async fn clearinghouse_state(&self) -> Result<HlClearinghouse> {
        match self {
            HedgeAccount::Lighter(l) => l.clearinghouse_state().await,
            HedgeAccount::Hyperliquid(h) => h.clearinghouse_state().await,
        }
    }

    async fn open_orders_info(&self) -> Result<Vec<HlOpenOrder>> {
        match self {
            HedgeAccount::Lighter(l) => l.open_orders_info().await,
            HedgeAccount::Hyperliquid(h) => h.open_orders_info().await,
        }
    }
}

/// USD-pegged collateral assets counted at face value in the wallet sum.
fn is_usd_stable_asset(asset: &str) -> bool {
    matches!(
        asset.to_ascii_uppercase().as_str(),
        "USD" | "USDT" | "USDC" | "BUSD" | "USDF" | "FDUSD" | "DAI"
    )
}

/// Net USD wallet balance across the Aster futures account's USD-pegged rows — SIGNED.
///
/// Cross-margin settles funding and PnL per asset: an account collateralized in USDC can
/// carry a NEGATIVE USDT row (real debt — observed live 2026-07-04: USDT at -5.67 while
/// USDC held +124.40). Skipping non-positive rows overstated the wallet by that debt and
/// blinded the equity metric to funding flowing through the negative row. Non-USD assets
/// (e.g. asBNB) are skipped with a warn: counting units as dollars is wrong in both
/// directions.
fn fold_aster_balance_rows(rows: &[AsterBalanceRow]) -> Decimal {
    let mut usd = Decimal::ZERO;
    for (idx, r) in rows.iter().enumerate() {
        let Some(b) =
            parse_untraded_row_decimal(&r.balance, &format!("aster.balance[{idx}].balance"))
        else {
            continue;
        };
        if b == Decimal::ZERO {
            continue;
        }
        if is_usd_stable_asset(&r.asset) {
            usd += b;
        } else {
            warn!(
                "reconcile: skipping non-USD Aster collateral row {} balance {} (not valued)",
                r.asset, b
            );
        }
    }
    usd
}

fn parse_decimal_field(raw: &str, field: &str) -> Result<Decimal> {
    raw.parse::<Decimal>()
        .with_context(|| format!("parse {field} decimal from {raw:?}"))
}

fn parse_optional_decimal_field(raw: Option<&str>, field: &str) -> Result<Decimal> {
    match raw {
        Some(value) => parse_defaulted_decimal_field(value, field),
        None => Ok(Decimal::ZERO),
    }
}

/// Like [`parse_decimal_field`] but an EMPTY string means "venue omitted the field" (these
/// rows deserialize with `#[serde(default)]`, so a missing JSON key arrives as `""`) and
/// parses as zero instead of failing the snapshot.
fn parse_defaulted_decimal_field(raw: &str, field: &str) -> Result<Decimal> {
    if raw.is_empty() {
        return Ok(Decimal::ZERO);
    }
    parse_decimal_field(raw, field)
}

/// Fold `positionRisk` rows into (Σ unrealized PnL, market -> (net qty, entry px)).
///
/// NET rows per market: in hedge (dual-side) mode Aster returns separate LONG and SHORT
/// rows for one symbol, each with its own signed positionAmt; summing gives the correct net
/// regardless of mode. (One-way mode is also asserted at startup — see is_one_way.)
/// Rows for TRADED markets are strict (a bad value fails the snapshot, fail-safe); rows for
/// untraded symbols can never fail the fold — see `parse_untraded_row_decimal`.
fn fold_aster_position_rows(
    pos: &[AsterPositionRow],
    sym_to_market: &HashMap<String, MarketId>,
) -> Result<(Decimal, HashMap<MarketId, (Decimal, Decimal)>)> {
    let mut unrealized_usd = Decimal::ZERO;
    let mut net: HashMap<MarketId, (Decimal, Decimal)> = HashMap::new();
    for (idx, p) in pos.iter().enumerate() {
        let market = sym_to_market.get(&p.symbol.to_uppercase());
        let field = format!("aster.positionRisk[{idx}].unRealizedProfit");
        if market.is_some() {
            unrealized_usd += parse_defaulted_decimal_field(&p.unrealized_profit, &field)?;
        } else {
            // Untraded symbol: contribute to equity when parseable, never poison the fold.
            if let Some(u) = parse_untraded_row_decimal(&p.unrealized_profit, &field) {
                unrealized_usd += u;
            }
            continue;
        }
        let market = market.expect("checked above");
        let qty = parse_decimal_field(&p.position_amt, &format!("aster.positionRisk[{idx}].positionAmt"))?;
        if qty == Decimal::ZERO {
            continue;
        }
        let e = net.entry(market.clone()).or_insert((Decimal::ZERO, Decimal::ZERO));
        e.0 += qty;
        // Keep the entry px of the larger-magnitude leg (informational only, so an omitted
        // field must not fail the snapshot).
        e.1 = parse_defaulted_decimal_field(&p.entry_price, &format!("aster.positionRisk[{idx}].entryPrice"))?;
    }
    Ok((unrealized_usd, net))
}

/// Additional hedge uPnL and mark validity. Missing marks or non-positive entries contribute
/// zero and invalidate the sample; Hyperliquid entry-price marks avoid double-counting its uPnL.
fn fold_hl_unrealized(
    positions: &[ScaledPosition],
    marks: &HashMap<MarketId, Decimal>,
) -> (Decimal, bool) {
    let mut upnl = Decimal::ZERO;
    let mut all_marked = true;
    for p in positions {
        if p.signed_qty == Decimal::ZERO {
            continue;
        }
        match (marks.get(&p.market), p.entry_px > Decimal::ZERO) {
            (Some(mark), true) => upnl += p.signed_qty * (*mark - p.entry_px),
            _ => all_marked = false,
        }
    }
    (upnl, all_marked)
}

/// Parse a decimal from an account row that the bot does NOT trade. A single junk field on
/// an unrelated row (a new listing, an odd venue format) must not be able to fail the whole
/// snapshot forever — that would age out the account state, which not only freezes quoting
/// (safe) but also disables `recover_orphans` and the circuit breaker (NOT safe). Skipped
/// rows are loudly warned so a format change is still visible. Rows for TRADED markets keep
/// the strict `parse_decimal_field` path: a bad value there means the snapshot itself
/// cannot be trusted.
fn parse_untraded_row_decimal(raw: &str, field: &str) -> Option<Decimal> {
    match raw.parse::<Decimal>() {
        Ok(d) => Some(d),
        Err(_) => {
            warn!("reconcile: skipping unparseable {field} ({raw:?}) on untraded/asset row");
            None
        }
    }
}

/// Reads both venues and publishes [`AccountSnapshot`]s.
pub struct Reconciler {
    aster: AsterRest,
    hl: HedgeAccount,
    /// Aster UPPER symbol → market id.
    aster_sym_to_market: HashMap<String, MarketId>,
    /// Hedge coin → market id.
    hl_coin_to_market: HashMap<String, MarketId>,
    /// Max age (ms) of a cached Lighter book mid used to mark the Lighter leg's uPnL —
    /// same freshness bound the strategy requires of a Lighter book before quoting.
    mark_max_age_ms: i64,
    /// Throttle for the "uPnL unmarked" warn (mono ns of the last emit).
    last_upnl_warn_ns: std::sync::atomic::AtomicI64,
    maker_query_cursor: std::sync::atomic::AtomicUsize,
    /// Read the resting Lighter orders (see `without_hedge_orders`).
    hedge_orders: bool,
}

impl Reconciler {
    pub fn new(aster: AsterRest, hl: impl Into<HedgeAccount>, specs: &[MarketSpec], mark_max_age_ms: i64) -> Self {
        let mut aster_sym_to_market = HashMap::new();
        let mut hl_coin_to_market = HashMap::new();
        for s in specs {
            aster_sym_to_market.insert(s.aster_symbol.to_uppercase(), s.market_id.clone());
            hl_coin_to_market.insert(s.hl_coin.clone(), s.market_id.clone());
        }
        Reconciler {
            aster,
            hl: hl.into(),
            aster_sym_to_market,
            hl_coin_to_market,
            mark_max_age_ms,
            last_upnl_warn_ns: std::sync::atomic::AtomicI64::new(0),
            maker_query_cursor: std::sync::atomic::AtomicUsize::new(0),
            hedge_orders: true,
        }
    }

    /// For the engine's 2 s loop: nothing it drives reads resting Lighter orders (XEMM only
    /// sends IOCs there), and the read cost half of Lighter's 60 requests/min.
    pub fn without_hedge_orders(mut self) -> Self {
        self.hedge_orders = false;
        self
    }

    /// Fresh Lighter mark for `market`: the exchange's WS book cache if within
    /// `mark_max_age_ms`, else ONE bounded REST attempt (covers the `status` command,
    /// which never starts the WS streams, and a live cache gap). `None` ⇒ the snapshot is
    /// published UNMARKED — never an `Err`: a missing mark must not age out the snapshot,
    /// which would silently disable orphan recovery AND the breaker (see module note on
    /// `parse_untraded_row_decimal`).
    async fn lighter_mark(&self, market: &MarketId) -> Option<Decimal> {
        let HedgeAccount::Lighter(lighter) = &self.hl else { return None };
        if let Some((mid, age_ms)) = lighter.cached_lighter_mid(market) {
            if (0..=self.mark_max_age_ms).contains(&age_ms) && mid > Decimal::ZERO {
                return Some(mid);
            }
        }
        // rest_mid bypasses the (stale) cache; 800ms keeps a worst-case cycle inside the
        // reconcile loop's `interval * 3` budget.
        match tokio::time::timeout(Duration::from_millis(800), lighter.rest_mid(market)).await {
            Ok(Ok(mid)) if mid > Decimal::ZERO => Some(mid),
            _ => None,
        }
    }

    /// Our symbols' open Aster orders, one symbol at a time: weight 1 each, where the
    /// account-wide read costs 40 (about 1200 of the IP's 2400 a minute at the 2 s cadence).
    async fn aster_open_orders(&self) -> Result<Vec<AsterOpenOrder>> {
        let mut out = Vec::new();
        for market in self.aster_sym_to_market.values() {
            out.extend(self.aster.open_orders(Some(market)).await?);
        }
        Ok(out)
    }

    /// Assemble a fresh snapshot from live reads on both venues.
    pub async fn snapshot(&self) -> Result<AccountSnapshot> {
        // Stamp the read-START before ANY venue read (the orphan backstop's straddle guard requires
        // a timestamp from BEFORE the reads, not the post-read `source_ts_ns`).
        let read_start_ns = mono_now_ns();
        // Both venues concurrently: sequential awaits marked the two uPnL legs at instants
        // up to the full read latency apart, so a fast move made the delta-neutral
        // cancellation transiently imperfect inside a single snapshot.
        // Aster: balance + positions + open orders (signed).
        // Lighter: account state + active orders (WS account cache when fresh, else REST).
        let (bal, pos, oo, ch, hloo, available) = tokio::join!(
            self.aster.balance(),
            self.aster.position_risk(),
            self.aster_open_orders(),
            self.hl.clearinghouse_state(),
            async { if self.hedge_orders { self.hl.open_orders_info().await } else { Ok(Vec::new()) } },
            self.aster.account_available_balance(),
        );
        let (bal, pos, oo, ch, hloo, aster_available_usd) = (bal?, pos?, oo?, ch?, hloo?, available?);

        // Wallet equity uses the signed stablecoin balance sum (negative rows are debt).
        // Free margin above comes from /fapi/v3/account's account-level availableBalance;
        // per-asset availableBalance projections must not be summed.
        let aster_wallet_usd = fold_aster_balance_rows(&bal);
        let hl_withdrawable_usd = parse_decimal_field(&ch.withdrawable, "lighter.withdrawable")?;

        // TOTAL (mark-to-market) equity per venue for the circuit breaker — NOT the free-margin
        // figures above, which drop by the locked margin when a hedge is open and would false-trip.
        // Aster: wallet + position uPnL. Lighter needs uPnL added below;
        // Hyperliquid's accountValue already includes it.
        let (aster_unrealized_usd, aster_net) =
            fold_aster_position_rows(&pos, &self.aster_sym_to_market)?;
        let aster_equity_usd = aster_wallet_usd + aster_unrealized_usd;
        let hl_equity_usd = parse_decimal_field(&ch.margin_summary.account_value, "lighter.marginSummary.accountValue")?;

        let aster_positions: Vec<ScaledPosition> = aster_net
            .into_iter()
            .filter(|(_, (q, _))| *q != Decimal::ZERO)
            .map(|(market, (signed_qty, entry_px))| ScaledPosition { venue: Venue::Aster, market, signed_qty, entry_px })
            .collect();

        let mut hl_positions = Vec::new();
        for (idx, ap) in ch.asset_positions.iter().enumerate() {
            // Market lookup first, for the same poisoning reason as the Aster rows above.
            let Some(market) = self.hl_coin_to_market.get(&ap.position.coin) else {
                continue;
            };
            let qty = parse_decimal_field(&ap.position.szi, &format!("lighter.assetPositions[{idx}].szi"))?;
            if qty == Decimal::ZERO {
                continue;
            }
            hl_positions.push(ScaledPosition {
                venue: Venue::Hedge,
                market: market.clone(),
                signed_qty: qty,
                entry_px: parse_optional_decimal_field(ap.position.entry_px.as_deref(), &format!("lighter.assetPositions[{idx}].entryPx"))?,
            });
        }

        // Add marked uPnL only when the hedge account value excludes it.
        let mut hl_marks: HashMap<MarketId, Decimal> = HashMap::new();
        // Hyperliquid's accountValue already moves with the uPnL: its legs count at their entry.
        let upnl_in_equity = matches!(self.hl, HedgeAccount::Hyperliquid(_));
        for p in &hl_positions {
            if upnl_in_equity {
                hl_marks.insert(p.market.clone(), p.entry_px);
                continue;
            }
            if p.signed_qty == Decimal::ZERO {
                continue;
            }
            if let Some(mark) = self.lighter_mark(&p.market).await {
                hl_marks.insert(p.market.clone(), mark);
            }
        }
        let (hl_unrealized_usd, hl_upnl_marked) = fold_hl_unrealized(&hl_positions, &hl_marks);
        if !hl_upnl_marked {
            use std::sync::atomic::Ordering;
            let now = mono_now_ns();
            let last = self.last_upnl_warn_ns.load(Ordering::Relaxed);
            if now.saturating_sub(last) > 30_000_000_000 {
                self.last_upnl_warn_ns.store(now, Ordering::Relaxed);
                warn!(
                    "reconcile: Lighter uPnL UNMARKED (missing mark or entry px) — \
                     circuit breaker paused on this sample"
                );
            }
        }

        let mut open_orders = Vec::new();
        for o in &oo {
            if let Some(market) = self.aster_sym_to_market.get(&o.symbol.to_uppercase()) {
                open_orders.push(OpenOrderSnapshot {
                    venue: Venue::Aster,
                    market: market.clone(),
                    side: if o.side.eq_ignore_ascii_case("SELL") { Side::Sell } else { Side::Buy },
                    price: parse_decimal_field(&o.price, "aster.openOrders.price")?,
                    qty: parse_decimal_field(&o.orig_qty, "aster.openOrders.origQty")?,
                    client_id: (!o.client_order_id.is_empty()).then(|| o.client_order_id.clone()),
                    venue_order_id: Some(o.order_id.to_string()),
                });
            }
        }
        for o in &hloo {
            if let Some(market) = self.hl_coin_to_market.get(&o.coin) {
                open_orders.push(OpenOrderSnapshot {
                    venue: Venue::Hedge,
                    market: market.clone(),
                    side: if o.side.eq_ignore_ascii_case("A") { Side::Sell } else { Side::Buy },
                    price: parse_decimal_field(&o.limit_px, "lighter.openOrders.limitPx")?,
                    qty: parse_decimal_field(&o.sz, "lighter.openOrders.sz")?,
                    client_id: None,
                    venue_order_id: Some(o.oid.to_string()),
                });
            }
        }

        // The Lighter side may have been served from the WS account cache, whose data ORIGINATED
        // before this function even started. Min the true data origin into read_start_ns so
        // the orphan backstop's straddle guard ("reads began strictly after the hot action")
        // judges the DATA's age, not the snapshot assembly time — otherwise a cached
        // rep_h=0 applied before a hedge fill could masquerade as a fresh venue read and
        // double-hedge. `source_ts_ns` stays assembly-time on purpose: it feeds the
        // strictly-increasing orphan_seen/heal_confirm gates, which must keep advancing.
        let read_start_ns = if ch.data_origin_ns > 0 {
            read_start_ns.min(ch.data_origin_ns)
        } else {
            read_start_ns
        };
        Ok(AccountSnapshot {
            aster_available_usd,
            aster_margin_source_ns: read_start_ns,
            hl_margin_source_ns: ch.margin_source_ns,
            hl_withdrawable_usd,
            aster_equity_usd,
            hl_equity_usd,
            hl_unrealized_usd,
            hl_upnl_marked,
            aster_positions,
            hl_positions,
            open_orders,
            generation: 0, // set by AccountState::publish
            source_ts_ns: mono_now_ns(),
            read_start_ns,
        })
    }

    /// Refuse to trade unless the Aster account is in ONE-WAY position mode (the bot sends
    /// `positionSide=BOTH` and nets positions assuming one-way; hedge mode would mis-route + mis-
    /// report — see the reconciler's per-market netting and aster.rs::place_params).
    pub async fn assert_one_way(&self) -> Result<()> {
        if !self.aster.is_one_way().await? {
            anyhow::bail!(
                "Aster account is in HEDGE (dual-side) position mode; this bot requires ONE-WAY \
                 mode. Switch it (asterdex.com or POST /fapi/v3/positionSide/dual dualSidePosition=false) \
                 before live trading."
            );
        }
        info!("aster position mode: ONE-WAY (verified)");
        Ok(())
    }

    /// Enforce the CLEAN-START invariant before quoting: cancel all resting orders on
    /// our symbols, then poll `openOrders` until no bot-prefixed (`X…`) order remains — so a fast
    /// startup can never begin quoting while stray orders from a PRIOR run still rest. Bounded poll
    /// (≤6 tries) so startup can't hang. With `require_clean_start`, a still-dirty book after the
    /// retries is a HARD error (refuse to quote into a dirty book). At startup the bot has placed
    /// nothing, so every `X…` order is by definition a prior-run stray (each run uses a fresh random
    /// session id).
    pub async fn ensure_clean_start(&self, startup_cancel_all: bool, require_clean_start: bool) -> Result<()> {
        if startup_cancel_all {
            for market in self.aster_sym_to_market.values() {
                if let Err(e) = self.aster.cancel_all_symbol(market).await {
                    warn!("startup cancel-all on {market} failed: {e:#}");
                }
            }
        }
        for attempt in 1..=6u32 {
            // A FAILED read must NOT be mistaken for an empty book — treating Err as "no orders"
            // would certify a possibly-dirty book clean on the first transient error (TLS reset on a
            // fresh pooled conn, a 429 after the cancel-all burst, a timeout) and skip both the
            // remaining retries and the `require_clean_start` bail. So on Err we warn, consume the
            // attempt, and retry — the early `return Ok(())` below is reachable ONLY after a
            // SUCCESSFUL read proves the stray set empty.
            let open = match self.aster_open_orders().await {
                Ok(o) => o,
                Err(e) => {
                    warn!("clean-start: openOrders read failed (attempt {attempt}/6): {e:#}");
                    tokio::time::sleep(Duration::from_millis(300)).await;
                    continue;
                }
            };
            let stray: Vec<String> = open
                .iter()
                .filter(|o| {
                    o.client_order_id.starts_with('X')
                        && self.aster_sym_to_market.contains_key(&o.symbol.to_uppercase())
                })
                .map(|o| o.client_order_id.clone())
                .collect();
            if stray.is_empty() {
                info!("clean start verified: no stray bot orders on our symbols");
                return Ok(());
            }
            warn!("clean-start: {} stray bot order(s) remain (attempt {attempt}/6): {stray:?}", stray.len());
            if startup_cancel_all {
                for market in self.aster_sym_to_market.values() {
                    let _ = self.aster.cancel_all_symbol(market).await; // re-cancel anything still resting
                }
            }
            tokio::time::sleep(Duration::from_millis(300)).await;
        }
        // Reached only if no SUCCESSFUL read ever proved the book empty — either stray bot orders
        // kept resting after cancel-all, or every openOrders read errored. Either way the book is
        // not VERIFIED clean.
        if require_clean_start {
            anyhow::bail!(
                "clean-start failed: could not verify an empty order book after cancel-all (stray bot \
                 orders still resting, or every openOrders read errored); refusing to quote into a \
                 possibly-dirty book (set [live] require_clean_start=false to override)"
            );
        }
        warn!("clean-start NOT verified but require_clean_start=false — proceeding (deadman backstop active)");
        Ok(())
    }

    pub fn hedge_readiness(&self) -> HedgeReadiness {
        match &self.hl {
            HedgeAccount::Lighter(l) => l.readiness(),
            HedgeAccount::Hyperliquid(_) => HedgeReadiness::default(),
        }
    }

    async fn resolve_aster_attempts(&self, account: &AccountState, events: &tokio::sync::mpsc::Sender<super::exec::command::ExecEvent>) {
        for intent in account.pending_exec().iter() {
            if intent.venue != Venue::Aster || !intent.unresolved()
                || !intent.admission.is_claimed()
                || mono_now_ns().saturating_sub(intent.created_ns) > 60_000_000_000 { continue; }
            let Some(client) = intent.client_id.as_deref() else { continue };
            let Ok(row) = self.aster.query_order(&intent.market, client).await else { continue };
            if row.get("clientOrderId").and_then(|v| v.as_str()) != Some(client) { continue; }
            let status = row.get("status").and_then(|v| v.as_str()).unwrap_or("");
            if !matches!(status, "FILLED" | "CANCELED" | "EXPIRED" | "REJECTED") { continue; }
            let Some(qty) = row.get("executedQty").and_then(|v| v.as_str()).and_then(|v| v.parse::<Decimal>().ok()) else { continue };
            if qty < Decimal::ZERO || qty > intent.qty { continue; }
            let quote = row.get("cumQuote").and_then(|v| v.as_str()).and_then(|v| v.parse::<Decimal>().ok());
            let oid = row.get("orderId").map(|v| v.as_i64().map(|x| x.to_string()).or_else(|| v.as_str().map(str::to_owned))).flatten();
            let _ = events.send(super::exec::command::ExecEvent::ExecutionProgress {
                cloid: intent.cloid, cumulative_qty: qty, cumulative_quote_usd: quote,
                cumulative_fee_usd: (qty == Decimal::ZERO).then_some(Decimal::ZERO), terminal: true, venue_order_id: oid, event_time_ms: row.get("updateTime").and_then(|v| v.as_i64()),
            }).await;
        }
    }

    async fn backfill_maker_orders(&self, account: &AccountState, events: &tokio::sync::mpsc::Sender<super::exec::command::ExecEvent>) {
        let queries = account.maker_queries();
        if queries.is_empty() { return; }
        for _ in 0..queries.len().min(8) {
            let index = self.maker_query_cursor.fetch_add(1, std::sync::atomic::Ordering::Relaxed) % queries.len();
            let query = &queries[index];
            let row = match self.aster.query_order(&query.market, &query.client_id).await {
                Ok(row) => row,
                Err(e) => {
                    if super::exec::aster::unknown_order(&e) {
                        let _ = events.send(super::exec::command::ExecEvent::MakerOrderMissing { client_id: query.client_id.clone() }).await;
                    }
                    continue;
                }
            };
            if row.get("clientOrderId").and_then(|v| v.as_str()) != Some(&query.client_id) { continue; }
            let Some(qty) = row.get("executedQty").and_then(|v| v.as_str()).and_then(|v| v.parse::<Decimal>().ok()) else { continue };
            let status = row.get("status").and_then(|v| v.as_str()).unwrap_or("");
            let terminal = matches!(status, "FILLED" | "CANCELED" | "EXPIRED" | "REJECTED");
            let quote = row.get("cumQuote").and_then(|v| v.as_str()).and_then(|v| v.parse::<Decimal>().ok());
            let oid = row.get("orderId").map(|v| v.as_i64().map(|x| x.to_string()).or_else(|| v.as_str().map(str::to_owned))).flatten();
            let Some(order_id) = oid else { continue };
            let _ = events.send(super::exec::command::ExecEvent::MakerOrderProgress {
                market: query.market.clone(), side: query.side, client_id: query.client_id.clone(), order_id,
                cumulative_qty: qty, cumulative_quote_usd: quote, terminal,
                event_time_ms: row.get("updateTime").and_then(|v| v.as_i64()).unwrap_or(0),
            }).await;
        }
    }

    /// Reconcile once and publish. Returns the published snapshot.
    pub async fn reconcile_and_publish(&self, account: &AccountState) -> Result<AccountSnapshot> {
        let snap = self.snapshot().await?;
        account.publish(snap.clone());
        Ok(snap)
    }

    /// Cold reconcile loop: publish a fresh snapshot every `interval`, until cancelled. A failed
    /// read keeps the prior snapshot (the strategy's `account_fresh` gate then closes quoting if
    /// it ages out — fail-safe). The snapshot must refresh well within
    /// `max_account_snapshot_age_ms`, so `interval` should be a fraction of it.
    pub async fn run(self, account: AccountState, shutdown: CancellationToken, interval: Duration, events: tokio::sync::mpsc::Sender<super::exec::command::ExecEvent>) {
        info!("account reconciler started (interval {:?})", interval);
        // A single reconcile must NEVER wedge the loop. It awaits venue REST reads; a
        // black-holed connection (no response AND no error) would otherwise hang the await forever —
        // the snapshot then ages out, which SILENTLY closes the maker gate (`account_fresh`) AND
        // disables the orphan-recovery backstop (which early-returns on a stale snapshot). Bounding
        // each cycle drops a hung read so the loop keeps retrying and a transient black-hole self-heals.
        let budget = (interval * 3).max(Duration::from_secs(5));
        let mut consecutive_stalls: u32 = 0;
        let mut tick = tokio::time::interval_at(Instant::now() + interval, interval);
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => break,
                _ = tick.tick() => {
                    let _ = tokio::time::timeout(budget, self.resolve_aster_attempts(&account, &events)).await;
                    let _ = tokio::time::timeout(budget, self.backfill_maker_orders(&account, &events)).await;
                    match tokio::time::timeout(budget, self.reconcile_and_publish(&account)).await {
                        Ok(Ok(_)) => {
                            if consecutive_stalls > 0 {
                                info!("reconcile recovered after {consecutive_stalls} stalled cycle(s)");
                            }
                            consecutive_stalls = 0;
                        }
                        Ok(Err(e)) => {
                            consecutive_stalls += 1;
                            warn!("reconcile failed (keeping prior snapshot, {consecutive_stalls} in a row): {e:#}");
                        }
                        Err(_) => {
                            consecutive_stalls += 1;
                            warn!("reconcile TIMED OUT after {budget:?} (venue read wedged?); keeping prior snapshot, {consecutive_stalls} in a row");
                        }
                    }
                    // Once snapshots stop advancing for several cycles the snapshot is going stale: the
                    // maker gate will close on ACCOUNT_SNAPSHOT_STALE and orphan recovery is paused.
                    // Make that LOUD so the operator sees it instead of discovering a dead bot hours later.
                    if consecutive_stalls == 3 {
                        error!(
                            "account reconciler STALLED {consecutive_stalls} cycles (~{:?}): snapshot going stale — \
                             maker quoting will freeze (ACCOUNT_SNAPSHOT_STALE) and orphan recovery is paused until reads recover",
                            interval * consecutive_stalls
                        );
                    }
                }
            }
        }
        info!("account reconciler stopped");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(symbol: &str, amt: &str, entry: &str, upnl: &str) -> AsterPositionRow {
        AsterPositionRow {
            symbol: symbol.to_string(),
            position_amt: amt.to_string(),
            entry_price: entry.to_string(),
            unrealized_profit: upnl.to_string(),
            position_side: String::new(),
            leverage: String::new(),
        }
    }

    fn traded_map() -> HashMap<String, MarketId> {
        HashMap::from([("HYPEUSDT".to_string(), MarketId("HYPE".to_string()))])
    }

    #[test]
    fn junk_untraded_row_never_fails_the_fold() {
        // A new listing with unparseable fields must not poison the snapshot.
        let rows = vec![
            row("NEWCOINUSDT", "not-a-number", "", "garbage"),
            row("HYPEUSDT", "1.5", "40.0", "0.25"),
        ];
        let (upnl, net) = fold_aster_position_rows(&rows, &traded_map()).expect("fold must succeed");
        assert_eq!(upnl, "0.25".parse().unwrap());
        let (qty, entry) = net[&MarketId("HYPE".to_string())];
        assert_eq!(qty, "1.5".parse().unwrap());
        assert_eq!(entry, "40.0".parse().unwrap());
    }

    #[test]
    fn junk_traded_row_fails_the_fold() {
        // A bad value on a TRADED market means the snapshot cannot be trusted.
        let rows = vec![row("HYPEUSDT", "not-a-number", "40.0", "0.25")];
        assert!(fold_aster_position_rows(&rows, &traded_map()).is_err());
    }

    #[test]
    fn omitted_default_fields_parse_as_zero() {
        // #[serde(default)] string fields arrive as "" when the venue omits them.
        let rows = vec![row("HYPEUSDT", "2", "", "")];
        let (upnl, net) = fold_aster_position_rows(&rows, &traded_map()).expect("fold must succeed");
        assert_eq!(upnl, Decimal::ZERO);
        let (qty, entry) = net[&MarketId("HYPE".to_string())];
        assert_eq!(qty, "2".parse().unwrap());
        assert_eq!(entry, Decimal::ZERO);
    }

    #[test]
    fn dual_side_rows_net_per_market() {
        let rows = vec![
            row("HYPEUSDT", "3", "40", "0.1"),
            row("HYPEUSDT", "-1", "41", "-0.05"),
        ];
        let (upnl, net) = fold_aster_position_rows(&rows, &traded_map()).expect("fold must succeed");
        assert_eq!(upnl, "0.05".parse().unwrap());
        let (qty, _) = net[&MarketId("HYPE".to_string())];
        assert_eq!(qty, "2".parse().unwrap());
    }

    fn bal_row(asset: &str, balance: &str) -> AsterBalanceRow {
        AsterBalanceRow {
            asset: asset.to_string(),
            balance: balance.to_string(),
            cross_wallet_balance: String::new(),
        }
    }

    #[test]
    fn balance_fold_nets_negative_stable_rows() {
        // The observed live composition: USDC collateral, NEGATIVE USDT (funding/settlement
        // debt), dust USDF. The old `> 0` filter overstated the wallet by the debt.
        let rows = vec![
            bal_row("USDC", "124.40000063"),
            bal_row("USDT", "-5.66890386"),
            bal_row("USDF", "0.02350381"),
        ];
        assert_eq!(fold_aster_balance_rows(&rows), "118.75460058".parse().unwrap());
    }

    #[test]
    fn balance_fold_skips_non_usd_assets_and_junk() {
        let rows = vec![
            bal_row("USDT", "100"),
            bal_row("ASBNB", "5.5"),      // units, not dollars — must not be counted
            bal_row("BNB", "-0.1"),       // nor negative non-USD rows
            bal_row("USDC", "not-a-number"), // junk row skip-with-warn
            bal_row("USDF", "0"),         // zero rows are ignored silently
        ];
        assert_eq!(fold_aster_balance_rows(&rows), "100".parse().unwrap());
    }

    fn hl_pos(market: &str, qty: &str, entry: &str) -> ScaledPosition {
        ScaledPosition {
            venue: Venue::Hedge,
            market: MarketId(market.to_string()),
            signed_qty: qty.parse().unwrap(),
            entry_px: entry.parse().unwrap(),
        }
    }

    #[test]
    fn fold_hl_unrealized_long_gain() {
        let positions = vec![hl_pos("HYPE", "1.17", "40")];
        let marks = HashMap::from([(MarketId("HYPE".to_string()), "47.27".parse().unwrap())]);
        let (upnl, marked) = fold_hl_unrealized(&positions, &marks);
        assert_eq!(upnl, "8.5059".parse().unwrap());
        assert!(marked);
    }

    #[test]
    fn fold_hl_unrealized_short_position_sign() {
        let positions = vec![hl_pos("HYPE", "-2", "40")];
        let marks = HashMap::from([(MarketId("HYPE".to_string()), "44".parse().unwrap())]);
        let (upnl, marked) = fold_hl_unrealized(&positions, &marks);
        assert_eq!(upnl, "-8".parse().unwrap());
        assert!(marked);
    }

    #[test]
    fn fold_hl_unrealized_missing_mark_unmarks_and_zeroes() {
        let positions = vec![hl_pos("HYPE", "1.17", "40")];
        let (upnl, marked) = fold_hl_unrealized(&positions, &HashMap::new());
        assert_eq!(upnl, Decimal::ZERO);
        assert!(!marked);
    }

    #[test]
    fn fold_hl_unrealized_zero_entry_unmarks() {
        // entry_px == 0 is the "venue omitted avg_entry_price" sentinel; computing with it
        // would count the full notional as uPnL.
        let positions = vec![hl_pos("HYPE", "1.17", "0")];
        let marks = HashMap::from([(MarketId("HYPE".to_string()), "44".parse().unwrap())]);
        let (upnl, marked) = fold_hl_unrealized(&positions, &marks);
        assert_eq!(upnl, Decimal::ZERO);
        assert!(!marked);
    }

    #[test]
    fn fold_hl_unrealized_flat_positions_marked_true() {
        let (upnl, marked) = fold_hl_unrealized(&[], &HashMap::new());
        assert_eq!(upnl, Decimal::ZERO);
        assert!(marked);
        // Zero-qty rows are ignored, not treated as unmarked.
        let positions = vec![hl_pos("HYPE", "0", "40")];
        let (upnl, marked) = fold_hl_unrealized(&positions, &HashMap::new());
        assert_eq!(upnl, Decimal::ZERO);
        assert!(marked);
    }
}
