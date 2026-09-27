use std::collections::HashMap;
use std::path::Path;

use anyhow::{bail, Context, Result};
use chrono::Utc;
use rust_decimal::Decimal;
use serde::Serialize;

use crate::book::OrderBook;
use crate::config::Config;
use crate::connectors::{rest_book, rest_specs};
use crate::markets::MarketSpec;
use crate::quote_engine::{compute_desired_quote, DesiredQuote, PositionContext};
use crate::types::{MarketId, RejectReason, Side};

use super::account::{AccountSnapshot, Venue};
use super::exec::aster::AsterRest;
use super::exec::creds::{venue_creds, AsterCreds};
use super::exec::lighter::LighterExchange;
use super::exec::sign::EvmAsterSigner;
use super::reconcile::Reconciler;
use super::scale::MarketScale;

#[derive(Debug, Serialize)]
pub struct StatusReport {
    pub timestamp: chrono::DateTime<Utc>,
    pub market: String,
    pub bot: &'static str,
    pub reduce_position_only: bool,
    pub mark_price: Option<Decimal>,
    pub desired_notional_usd: Decimal,
    pub max_abs_position_notional_usd: Decimal,
    pub max_position_mismatch_usd: Decimal,
    pub margin_buffer_usd: Decimal,
    pub positions: PositionStatus,
    pub accounts: AccountStatus,
    pub books: BookStatus,
    pub quotes: Vec<QuoteStatus>,
}

#[derive(Debug, Serialize)]
pub struct PositionStatus {
    pub aster_qty: Decimal,
    pub lighter_qty: Decimal,
    pub net_qty: Decimal,
    pub net_mismatch_notional_usd: Option<Decimal>,
    pub abs_position_notional_usd: Option<Decimal>,
    pub headroom_notional_usd: Option<Decimal>,
}

#[derive(Debug, Serialize)]
pub struct AccountStatus {
    pub aster_available_usd: Decimal,
    pub aster_equity_usd: Decimal,
    pub lighter_available_usd: Decimal,
    /// Lighter `portfolio_value` — collateral-style, EXCLUDES open-position uPnL.
    pub lighter_equity_usd: Decimal,
    /// Marked uPnL of the Lighter leg (see `AccountSnapshot::hl_unrealized_usd`).
    pub lighter_unrealized_usd: Decimal,
    pub total_available_usd: Decimal,
    /// Fully marked cross-venue equity (both legs' uPnL included).
    pub total_equity_usd: Decimal,
    pub aster_open_orders: usize,
    pub lighter_open_orders: usize,
}

#[derive(Debug, Serialize)]
pub struct BookStatus {
    pub aster_bid: Option<LevelStatus>,
    pub aster_ask: Option<LevelStatus>,
    pub lighter_bid: Option<LevelStatus>,
    pub lighter_ask: Option<LevelStatus>,
    pub aster_age_ms: i64,
    pub lighter_age_ms: i64,
    pub aster_crossed: bool,
    pub lighter_crossed: bool,
}

#[derive(Debug, Serialize)]
pub struct LevelStatus {
    pub px: Decimal,
    pub qty: Decimal,
}

#[derive(Debug, Serialize)]
pub struct QuoteStatus {
    pub aster_side: &'static str,
    pub lighter_side: &'static str,
    pub status: &'static str,
    pub reject_reason: Option<&'static str>,
    pub exposure_effect: &'static str,
    pub quote_qty: Decimal,
    pub quote_notional_usd: Option<Decimal>,
    pub quote_px: Option<Decimal>,
    pub expected_lighter_vwap: Option<Decimal>,
    pub expected_lighter_depth_target_qty: Option<Decimal>,
    pub expected_lighter_depth_filled_qty: Option<Decimal>,
    pub expected_lighter_worst_px: Option<Decimal>,
    pub expected_lighter_depth_levels_used: Option<usize>,
    pub aster_effective_touch_px: Option<Decimal>,
    pub aster_depth_filled_qty: Option<Decimal>,
    pub aster_depth_levels_used: Option<usize>,
    pub depth_liquidity_multiple: Option<Decimal>,
    pub instant_edge_bps: Option<Decimal>,
    pub required_bps: Option<Decimal>,
}

#[derive(Debug, Clone, Copy)]
struct PositionSnapshot {
    aster_qty: Decimal,
    lighter_qty: Decimal,
}

impl PositionSnapshot {
    fn of(snapshot: &AccountSnapshot, market: &MarketId) -> Self {
        Self { aster_qty: snapshot.reported_position(Venue::Aster, market), lighter_qty: snapshot.reported_position(Venue::Hedge, market) }
    }

    fn net_qty(self) -> Decimal {
        self.aster_qty + self.lighter_qty
    }
}

pub async fn run(cfg: &Config, target: Option<String>) -> Result<()> {
    let target = target.unwrap_or_else(|| "HYPE".into());
    let report = StatusPoller::new(cfg, &target).await?.report().await?;
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}

/// Read-only status for one market with clients built once, so the `run` controller can
/// poll it repeatedly. It never places or cancels an order.
pub struct StatusPoller {
    cfg: Config,
    spec: MarketSpec,
    reconciler: Reconciler,
    http: reqwest::Client,
}

impl StatusPoller {
    pub async fn new(cfg: &Config, target: &str) -> Result<Self> {
        let selected = cfg.select_markets(Some(target));
        if selected.is_empty() {
            bail!("no market '{target}' in config [[markets]]");
        }
        if selected.len() != 1 {
            bail!("status is single-market only; selected {} markets", selected.len());
        }
        let specs = rest_specs::build_market_specs(&selected, &cfg.live).await?;
        let spec = specs.first().context("no resolved market spec")?.clone();
        let (acreds, lcreds) = venue_creds(cfg.live.dry_run)?;
        let aster = build_aster(cfg, &specs, acreds)?;
        let lighter = LighterExchange::new_read_only(
            cfg.live.lighter.base_url.clone(),
            Path::new(&cfg.live.lighter.signers_dir),
            &lcreds,
            &specs,
            cfg.live.lighter.fill_timeout_ms,
            cfg.live.lighter.ws_account_max_age_ms,
        )?;
        let reconciler = Reconciler::new(aster, lighter, &specs, cfg.live.max_book_staleness_ms);
        Ok(Self { cfg: cfg.clone(), spec, reconciler, http: rest_book::client()? })
    }

    pub async fn report(&self) -> Result<StatusReport> {
        let snapshot = self.reconciler.snapshot().await?;
        let (aster_book, lighter_book) = tokio::join!(
            rest_book::fetch_aster_book_from_base(&self.http, &self.cfg.live.aster.base_url, &self.spec.aster_symbol, 20),
            rest_book::fetch_lighter_book_from_base(
                &self.http,
                &self.cfg.live.lighter.base_url,
                self.spec.lighter_market_id,
                20
            ),
        );
        Ok(build_report(&self.cfg, &self.spec, snapshot, &aster_book?, &lighter_book?))
    }

    /// The positions and accounts of `report` from a running engine's own snapshot, with no
    /// venue read. That snapshot has no resting Lighter orders (`without_hedge_orders`).
    pub fn from_snapshot(&self, snapshot: &AccountSnapshot) -> serde_json::Value {
        let market = &self.spec.market_id;
        let positions = position_status(&self.cfg, None, PositionSnapshot::of(snapshot, market));
        let mut report = serde_json::json!({"market": market.0, "positions": positions, "accounts": account_status(snapshot, market)});
        report["accounts"]["lighter_open_orders"] = serde_json::Value::Null;
        report
    }
}

pub(super) fn build_aster(cfg: &Config, specs: &[MarketSpec], creds: AsterCreds) -> Result<AsterRest> {
    let signer = std::sync::Arc::new(EvmAsterSigner::new(creds.user, creds.signer, creds.key)?);
    let mut scales: HashMap<MarketId, (MarketScale, String)> = HashMap::new();
    for s in specs {
        scales.insert(s.market_id.clone(), (MarketScale::from_spec(s), s.aster_symbol.clone()));
    }
    AsterRest::new(
        cfg.live.aster.base_url.clone(),
        signer,
        scales,
        cfg.live.aster.deadman_countdown_ms,
        cfg.live.aster.rate_limit_backoff_ms,
        cfg.live.aster.effective_max_rest_requests_per_minute(),
    )
}

fn build_report(
    cfg: &Config,
    spec: &MarketSpec,
    snapshot: AccountSnapshot,
    aster_book: &OrderBook,
    lighter_book: &OrderBook,
) -> StatusReport {
    let now = Utc::now();
    let pos = PositionSnapshot::of(&snapshot, &spec.market_id);
    let mark = aster_book.mid().or_else(|| lighter_book.mid());
    StatusReport {
        timestamp: now,
        market: spec.market_id.0.clone(),
        bot: "XEMM_LIGHTER_ASTER",
        reduce_position_only: cfg.live.quote.reduce_position_only,
        mark_price: mark,
        desired_notional_usd: cfg.quote.desired_notional,
        max_abs_position_notional_usd: cfg.capital.aster_cap_notional().min(cfg.capital.lighter_cap_notional()),
        max_position_mismatch_usd: cfg.live.max_position_mismatch_usd,
        margin_buffer_usd: cfg.live.margin_guard.aster_safety_buffer_usd,
        positions: position_status(cfg, mark, pos),
        accounts: account_status(&snapshot, &spec.market_id),
        books: book_status(now, aster_book, lighter_book),
        quotes: vec![
            quote_status(cfg, spec, aster_book, lighter_book, pos, Side::Buy),
            quote_status(cfg, spec, aster_book, lighter_book, pos, Side::Sell),
        ],
    }
}

fn position_status(cfg: &Config, mark: Option<Decimal>, pos: PositionSnapshot) -> PositionStatus {
    let net_qty = pos.net_qty();
    let net_mismatch_notional_usd = mark.map(|m| net_qty.abs() * m);
    let abs_position_notional_usd =
        mark.map(|m| pos.aster_qty.abs().max(pos.lighter_qty.abs()) * m);
    let cap = cfg.capital.aster_cap_notional().min(cfg.capital.lighter_cap_notional());
    let headroom_notional_usd = abs_position_notional_usd.map(|n| (cap - n).max(Decimal::ZERO));
    PositionStatus {
        aster_qty: pos.aster_qty,
        lighter_qty: pos.lighter_qty,
        net_qty,
        net_mismatch_notional_usd,
        abs_position_notional_usd,
        headroom_notional_usd,
    }
}

fn account_status(snapshot: &AccountSnapshot, market: &MarketId) -> AccountStatus {
    let aster_open_orders = snapshot
        .open_orders
        .iter()
        .filter(|o| o.venue == Venue::Aster && &o.market == market)
        .count();
    let lighter_open_orders = snapshot
        .open_orders
        .iter()
        .filter(|o| o.venue == Venue::Hedge && &o.market == market)
        .count();
    AccountStatus {
        aster_available_usd: snapshot.aster_available_usd,
        aster_equity_usd: snapshot.aster_equity_usd,
        lighter_available_usd: snapshot.hl_withdrawable_usd,
        lighter_equity_usd: snapshot.hl_equity_usd,
        lighter_unrealized_usd: snapshot.hl_unrealized_usd,
        total_available_usd: snapshot.aster_available_usd + snapshot.hl_withdrawable_usd,
        total_equity_usd: snapshot.total_equity_usd(),
        aster_open_orders,
        lighter_open_orders,
    }
}

fn book_status(now: chrono::DateTime<Utc>, aster: &OrderBook, lighter: &OrderBook) -> BookStatus {
    BookStatus {
        aster_bid: aster.best_bid().map(|l| LevelStatus { px: l.px, qty: l.qty }),
        aster_ask: aster.best_ask().map(|l| LevelStatus { px: l.px, qty: l.qty }),
        lighter_bid: lighter.best_bid().map(|l| LevelStatus { px: l.px, qty: l.qty }),
        lighter_ask: lighter.best_ask().map(|l| LevelStatus { px: l.px, qty: l.qty }),
        aster_age_ms: aster.age_ms(now),
        lighter_age_ms: lighter.age_ms(now),
        aster_crossed: aster.is_crossed(),
        lighter_crossed: lighter.is_crossed(),
    }
}

fn quote_status(
    cfg: &Config,
    spec: &MarketSpec,
    aster_book: &OrderBook,
    lighter_book: &OrderBook,
    pos: PositionSnapshot,
    side: Side,
) -> QuoteStatus {
    let pos_ctx = PositionContext {
        aster_pos_qty: pos.aster_qty,
        hl_pos_qty: pos.lighter_qty,
        aster_cap_notional: cfg.capital.aster_cap_notional(),
        hl_cap_notional: cfg.capital.lighter_cap_notional(),
        enforce: cfg.capital.enforce_position_cap,
        reduce_position_only: cfg.live.quote.reduce_position_only,
    };
    match compute_desired_quote(
        &cfg.edge,
        &cfg.quote,
        aster_book,
        lighter_book,
        side,
        spec.tick,
        spec.step,
        spec.aster_min_qty,
        spec.aster_min_notional,
        spec.hl_min_notional,
        cfg.live.max_book_staleness_ms,
        Utc::now(),
        &pos_ctx,
    ) {
        Ok(q) => quote_ok(pos, side, q),
        Err(reason) => quote_reject(pos, side, reason),
    }
}

fn quote_ok(pos: PositionSnapshot, side: Side, q: DesiredQuote) -> QuoteStatus {
    QuoteStatus {
        aster_side: side.as_str(),
        lighter_side: side.opposite().as_str(),
        status: "ok",
        reject_reason: None,
        exposure_effect: exposure_effect(pos, side, q.qty),
        quote_qty: q.qty,
        quote_notional_usd: Some(q.qty * q.ref_px),
        quote_px: Some(q.price),
        expected_lighter_vwap: Some(q.expected_hl_vwap),
        expected_lighter_depth_target_qty: Some(q.depth_target_qty),
        expected_lighter_depth_filled_qty: Some(q.expected_hl_depth_filled_qty),
        expected_lighter_worst_px: Some(q.expected_hl_worst_px),
        expected_lighter_depth_levels_used: Some(q.expected_hl_depth_levels_used),
        aster_effective_touch_px: Some(q.effective_aster_touch_px),
        aster_depth_filled_qty: Some(q.aster_depth_filled_qty),
        aster_depth_levels_used: Some(q.aster_depth_levels_used),
        depth_liquidity_multiple: Some(q.depth_liquidity_multiple),
        instant_edge_bps: Some(q.instant_edge_bps),
        required_bps: Some(q.required_bps),
    }
}

fn quote_reject(pos: PositionSnapshot, side: Side, reason: RejectReason) -> QuoteStatus {
    QuoteStatus {
        aster_side: side.as_str(),
        lighter_side: side.opposite().as_str(),
        status: "reject",
        reject_reason: Some(reason.as_str()),
        exposure_effect: exposure_effect(pos, side, Decimal::ONE),
        quote_qty: Decimal::ZERO,
        quote_notional_usd: None,
        quote_px: None,
        expected_lighter_vwap: None,
        expected_lighter_depth_target_qty: None,
        expected_lighter_depth_filled_qty: None,
        expected_lighter_worst_px: None,
        expected_lighter_depth_levels_used: None,
        aster_effective_touch_px: None,
        aster_depth_filled_qty: None,
        aster_depth_levels_used: None,
        depth_liquidity_multiple: None,
        instant_edge_bps: None,
        required_bps: None,
    }
}

fn exposure_effect(pos: PositionSnapshot, side: Side, qty: Decimal) -> &'static str {
    if qty <= Decimal::ZERO {
        return "unknown";
    }
    let a_sign = match side {
        Side::Buy => Decimal::ONE,
        Side::Sell => -Decimal::ONE,
    };
    let before = pos.aster_qty.abs().max(pos.lighter_qty.abs());
    let after_a = pos.aster_qty + a_sign * qty;
    let after_l = pos.lighter_qty - a_sign * qty;
    let after = after_a.abs().max(after_l.abs());
    if after < before {
        "reduce"
    } else if after > before {
        "increase"
    } else {
        "flat"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn exposure_effect_matches_reduce_only_inventory() {
        let pos = PositionSnapshot {
            aster_qty: dec!(-1.2),
            lighter_qty: dec!(1.2),
        };
        assert_eq!(exposure_effect(pos, Side::Buy, dec!(0.2)), "reduce");
        assert_eq!(exposure_effect(pos, Side::Sell, dec!(0.2)), "increase");
    }
}
