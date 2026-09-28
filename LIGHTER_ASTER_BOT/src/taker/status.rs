use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use chrono::Utc;
use rust_decimal::Decimal;
use serde::Serialize;

use crate::taker::arb::{Direction, PositionSnapshot};
use crate::taker::aster::creds::venue_creds;
use crate::taker::aster::rest::AsterRest;
use crate::taker::aster::sign::{AsterSigner, EvmAsterSigner};
use crate::taker::book::OrderBook;
use crate::taker::book_sanity::{self, BookSanitySnapshot};
use crate::taker::config::{Config, MarketCfg};
use crate::taker::connectors::{rest_book, rest_specs};
use crate::taker::markets::MarketSpec;
use crate::taker::types::Side;
use crate::taker::decimal::{ceil_to_step, common_qty_step, floor_to_step};
use crate::taker::venues::lighter::LighterVenue;

#[derive(Debug, Serialize)]
pub struct StatusReport {
    pub timestamp: chrono::DateTime<Utc>,
    pub market: String,
    pub mark_price: Option<Decimal>,
    pub required_gross_edge_bps: Decimal,
    pub desired_notional_usd: Decimal,
    pub max_abs_position_notional_usd: Decimal,
    pub margin_buffer_usd: Decimal,
    pub max_position_mismatch_usd: Decimal,
    pub positions: PositionStatus,
    pub accounts: AccountStatus,
    pub books: BookStatus,
    pub book_sanity: BookSanitySnapshot,
    pub opportunities: Vec<DirectionStatus>,
}

#[derive(Debug, Serialize)]
pub struct PositionStatus {
    pub aster_qty: Decimal,
    pub lighter_qty: Decimal,
    pub lighter_qty_source: &'static str,
    pub lighter_qty_ws: Option<Decimal>,
    pub lighter_rest_ws_divergence_qty: Option<Decimal>,
    pub net_qty: Decimal,
    pub net_mismatch_notional_usd: Option<Decimal>,
    pub abs_position_notional_usd: Option<Decimal>,
    pub headroom_notional_usd: Option<Decimal>,
}

#[derive(Debug, Serialize)]
pub struct AccountStatus {
    pub aster_available_usd: Decimal,
    pub aster_equity_usd: Option<Decimal>,
    pub lighter_available_usd: Decimal,
    pub lighter_equity_usd: Option<Decimal>,
    pub lighter_unrealized_usd: Option<Decimal>,
    pub lighter_available_usd_ws: Option<Decimal>,
    pub total_available_usd: Decimal,
    pub total_equity_usd: Option<Decimal>,
    pub aster_open_orders: usize,
    pub lighter_open_orders: usize,
    pub lighter_open_orders_ws: Option<usize>,
}

#[derive(Debug, Serialize)]
pub struct BookStatus {
    pub aster_bid: Option<LevelStatus>,
    pub aster_ask: Option<LevelStatus>,
    pub lighter_bid: Option<LevelStatus>,
    pub lighter_ask: Option<LevelStatus>,
    pub aster_age_ms: i64,
    pub lighter_age_ms: i64,
    pub aster_source_age_ms: Option<i64>,
    pub lighter_source_age_ms: Option<i64>,
    pub aster_engine_age_ms: Option<i64>,
    pub lighter_engine_age_ms: Option<i64>,
    pub aster_crossed: bool,
    pub lighter_crossed: bool,
}

#[derive(Debug, Serialize)]
pub struct LevelStatus {
    pub px: Decimal,
    pub qty: Decimal,
}

#[derive(Debug, Serialize)]
pub struct DirectionStatus {
    pub direction: &'static str,
    pub aster_side: &'static str,
    pub lighter_side: &'static str,
    pub gross_edge_bps: Option<Decimal>,
    pub expected_net_margin_bps: Option<Decimal>,
    pub executable_qty: Decimal,
    pub executable_notional_usd: Option<Decimal>,
    pub limiting_reason: &'static str,
    pub exposure_effect: &'static str,
    pub top_depth_qty: Decimal,
    pub depth_guard_enabled: bool,
    pub liquidity_multiple: Decimal,
    pub depth_supported_qty: Decimal,
    pub sell_depth_target_qty: Decimal,
    pub buy_depth_target_qty: Decimal,
    pub sell_depth_available_qty: Decimal,
    pub buy_depth_available_qty: Decimal,
    pub sell_depth_worst_px: Decimal,
    pub buy_depth_worst_px: Decimal,
    pub sell_depth_levels_used: usize,
    pub buy_depth_levels_used: usize,
    pub sell_best_px: Decimal,
    pub buy_best_px: Decimal,
    pub sell_best_qty: Decimal,
    pub buy_best_qty: Decimal,
    pub min_qty: Decimal,
    pub desired_qty: Decimal,
    pub headroom_qty: Decimal,
    pub margin_room_qty: Decimal,
}

#[derive(Debug, Clone, Copy)]
struct MarginSnapshot {
    aster_available_usd: Decimal,
    lighter_available_usd: Decimal,
}

pub async fn run(cfg: &Config, markets: Vec<MarketCfg>) -> Result<()> {
    let report = StatusPoller::new(cfg, markets).await?.report().await?;
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}

/// Read-only status for one market with clients built once, so the `run` controller can
/// poll it repeatedly. It can never place an order.
pub struct StatusPoller {
    cfg: Config,
    spec: MarketSpec,
    aster: AsterRest,
    lighter: LighterVenue,
}

impl StatusPoller {
    pub async fn new(cfg: &Config, markets: Vec<MarketCfg>) -> Result<Self> {
        anyhow::ensure!(
            markets.len() == 1,
            "status is single-market only; selected {} markets",
            markets.len()
        );
        let specs = rest_specs::build_market_specs(
            &markets,
            &cfg.venues.aster_base_url,
            &cfg.venues.lighter_base_url,
            None,
        )
        .await?;
        let spec = specs.first().context("no resolved market spec")?.clone();

        let (acreds, lcreds) = venue_creds(cfg.venues.dry_run)?;
        let signer: Arc<dyn AsterSigner> =
            Arc::new(EvmAsterSigner::new(acreds.user, acreds.signer, acreds.key)?);
        let aster = AsterRest::new(cfg.venues.aster_base_url.clone(), signer, &specs)?;
        // Read-only venue: no order-capable tx socket, no authed streams, no 20s
        // wait_ready — a monitoring poll answers from REST in a couple of seconds and can
        // never place an order. The report's *_ws fields are null by construction.
        let lighter = LighterVenue::new_read_only(
            &cfg.venues.lighter_base_url,
            Path::new(&cfg.venues.signers_dir),
            lcreds,
            &specs,
        )?;
        Ok(Self { cfg: cfg.clone(), spec, aster, lighter })
    }

    pub async fn report(&self) -> Result<StatusReport> {
        build_report(&self.cfg, &self.spec, &self.aster, &self.lighter).await
    }
}

async fn build_report(
    cfg: &Config,
    spec: &MarketSpec,
    aster: &AsterRest,
    lighter: &LighterVenue,
) -> Result<StatusReport> {
    let http = rest_book::client()?;
    let (
        aster_book,
        lighter_book,
        aster_pos,
        lighter_account,
        aster_balance,
        aster_open,
        lighter_open,
    ) = tokio::join!(
        rest_book::fetch_aster_book(&http, &cfg.venues.aster_base_url, &spec.aster_symbol, 20),
        rest_book::fetch_lighter_book(
            &http,
            &cfg.venues.lighter_base_url,
            spec.lighter_market_id,
            20
        ),
        aster.position_qty(&spec.market_id),
        lighter.account_snapshot(&spec.market_id),
        aster.balance_snapshot(),
        aster.open_orders(&spec.market_id),
        lighter.open_orders_count(&spec.market_id),
    );

    let aster_book = aster_book?;
    let lighter_book = lighter_book?;
    let lighter_account = lighter_account?;
    let pos = PositionSnapshot {
        aster_qty: aster_pos?,
        lighter_qty: lighter_account.position_qty,
    };
    let aster_balance = aster_balance?;
    let lighter_ws_pos = lighter.ws_position_qty(&spec.market_id).ok();
    let lighter_available_ws = lighter.ws_available_usdc().ok();
    let lighter_open_ws = lighter.ws_open_orders_count(&spec.market_id).ok();
    let margins = MarginSnapshot {
        aster_available_usd: aster_balance.available_usd,
        lighter_available_usd: lighter_account.available_usdc,
    };
    let lighter_equity = lighter_account.account_value_usdc;
    let lighter_upnl = lighter_account.unrealized_pnl_usdc;
    let now = Utc::now();
    let mark = aster_book.mid().or_else(|| lighter_book.mid());
    // Lighter's account value is collateral-style and EXCLUDES open-position uPnL,
    // so total equity must add the venue-reported unrealized PnL — otherwise a
    // delta-neutral book bleeds 1:1 with price in this metric (the 2026-07-04
    // false-trip class; XEMM fixed the same hole in 733ba55).
    let total_equity = match (aster_balance.equity_usd(), lighter_equity, lighter_upnl) {
        (Some(a), Some(l), Some(u)) => Some(a + l + u),
        _ => None,
    };

    Ok(StatusReport {
        timestamp: now,
        market: spec.market_id.0.clone(),
        mark_price: mark,
        required_gross_edge_bps: cfg.arb.required_gross_edge_bps(),
        desired_notional_usd: cfg.arb.desired_notional,
        max_abs_position_notional_usd: cfg.risk.max_abs_position_notional_usd,
        margin_buffer_usd: cfg.risk.margin_buffer_usd,
        max_position_mismatch_usd: cfg.risk.max_position_mismatch_usd,
        positions: position_status(cfg, mark, pos, lighter_ws_pos),
        accounts: AccountStatus {
            aster_available_usd: margins.aster_available_usd,
            aster_equity_usd: aster_balance.equity_usd(),
            lighter_available_usd: margins.lighter_available_usd,
            lighter_equity_usd: lighter_equity,
            lighter_unrealized_usd: lighter_upnl,
            lighter_available_usd_ws: lighter_available_ws,
            total_available_usd: margins.aster_available_usd + margins.lighter_available_usd,
            total_equity_usd: total_equity,
            aster_open_orders: aster_open?.len(),
            lighter_open_orders: lighter_open?,
            lighter_open_orders_ws: lighter_open_ws,
        },
        books: book_status(now, &aster_book, &lighter_book),
        book_sanity: book_sanity::load_snapshot(&cfg.pnl.persist_dir, &spec.market_id)
            .unwrap_or_else(|| BookSanitySnapshot::configured(cfg.arb.book_sanity.enabled)),
        opportunities: vec![
            direction_status(
                cfg,
                spec,
                &aster_book,
                &lighter_book,
                pos,
                margins,
                Direction::SellAsterBuyLighter,
            ),
            direction_status(
                cfg,
                spec,
                &aster_book,
                &lighter_book,
                pos,
                margins,
                Direction::SellLighterBuyAster,
            ),
        ],
    })
}

fn position_status(
    cfg: &Config,
    mark: Option<Decimal>,
    pos: PositionSnapshot,
    lighter_ws_qty: Option<Decimal>,
) -> PositionStatus {
    let net_qty = pos.net_qty();
    let net_mismatch_notional_usd = mark.map(|m| net_qty.abs() * m);
    let abs_position_notional_usd =
        mark.map(|m| pos.aster_qty.abs().max(pos.lighter_qty.abs()) * m);
    let headroom_notional_usd = abs_position_notional_usd
        .map(|n| (cfg.risk.max_abs_position_notional_usd - n).max(Decimal::ZERO));
    PositionStatus {
        aster_qty: pos.aster_qty,
        lighter_qty: pos.lighter_qty,
        lighter_qty_source: "rest",
        lighter_qty_ws: lighter_ws_qty,
        lighter_rest_ws_divergence_qty: lighter_ws_qty.map(|ws| (ws - pos.lighter_qty).abs()),
        net_qty,
        net_mismatch_notional_usd,
        abs_position_notional_usd,
        headroom_notional_usd,
    }
}

fn book_status(now: chrono::DateTime<Utc>, aster: &OrderBook, lighter: &OrderBook) -> BookStatus {
    BookStatus {
        aster_bid: aster.best_bid().map(|l| LevelStatus {
            px: l.px,
            qty: l.qty,
        }),
        aster_ask: aster.best_ask().map(|l| LevelStatus {
            px: l.px,
            qty: l.qty,
        }),
        lighter_bid: lighter.best_bid().map(|l| LevelStatus {
            px: l.px,
            qty: l.qty,
        }),
        lighter_ask: lighter.best_ask().map(|l| LevelStatus {
            px: l.px,
            qty: l.qty,
        }),
        aster_age_ms: aster.age_ms(now),
        lighter_age_ms: lighter.age_ms(now),
        aster_source_age_ms: aster.source_age_ms(now),
        lighter_source_age_ms: lighter.source_age_ms(now),
        aster_engine_age_ms: aster.engine_age_ms(now),
        lighter_engine_age_ms: lighter.engine_age_ms(now),
        aster_crossed: aster.is_crossed(),
        lighter_crossed: lighter.is_crossed(),
    }
}

fn direction_status(
    cfg: &Config,
    spec: &MarketSpec,
    aster: &OrderBook,
    lighter: &OrderBook,
    pos: PositionSnapshot,
    margins: MarginSnapshot,
    direction: Direction,
) -> DirectionStatus {
    let Some(a_bid) = aster.best_bid() else {
        return empty_direction(direction, "depth");
    };
    let Some(a_ask) = aster.best_ask() else {
        return empty_direction(direction, "depth");
    };
    let Some(l_bid) = lighter.best_bid() else {
        return empty_direction(direction, "depth");
    };
    let Some(l_ask) = lighter.best_ask() else {
        return empty_direction(direction, "depth");
    };
    let Some(ref_px) = aster.mid().or_else(|| lighter.mid()) else {
        return empty_direction(direction, "depth");
    };
    if ref_px <= Decimal::ZERO || aster.is_crossed() || lighter.is_crossed()
        || !aster.is_fresh(Utc::now(), cfg.arb.max_book_staleness_ms)
        || !lighter.is_fresh(Utc::now(), cfg.arb.max_book_staleness_ms) {
        return empty_direction(direction, "stale_book");
    }

    let (sell_book, buy_book) = match direction {
        Direction::SellAsterBuyLighter => (aster, lighter),
        Direction::SellLighterBuyAster => (lighter, aster),
    };
    let sell_top = sell_book
        .side_levels(Side::Sell)
        .first()
        .copied()
        .unwrap_or(match direction {
            Direction::SellAsterBuyLighter => a_bid,
            Direction::SellLighterBuyAster => l_bid,
        });
    let buy_top = buy_book
        .side_levels(Side::Buy)
        .first()
        .copied()
        .unwrap_or(match direction {
            Direction::SellAsterBuyLighter => l_ask,
            Direction::SellLighterBuyAster => a_ask,
        });
    let top_depth_qty = sell_top.qty.min(buy_top.qty);
    let desired_qty = cfg.arb.desired_notional / ref_px;
    let est_aster_px = if matches!(direction.aster_side(), Side::Sell) {
        sell_top.px
    } else {
        buy_top.px
    };
    let est_lighter_px = if matches!(direction.lighter_side(), Side::Sell) {
        sell_top.px
    } else {
        buy_top.px
    };
    let Some(est_min_qty) = min_trade_qty(spec, est_aster_px, est_lighter_px) else {
        return empty_direction(direction, "depth");
    };
    let a_sign = if matches!(direction.aster_side(), Side::Buy) {
        Decimal::ONE
    } else {
        -Decimal::ONE
    };
    let l_sign = -a_sign;
    let max_abs_qty = cfg.risk.max_abs_position_notional_usd / ref_px;
    let headroom_qty = max_qty_by_headroom(max_abs_qty, pos, a_sign, l_sign);
    let margin_room_qty = max_qty_by_available_margin(cfg.risk.margin_buffer_usd, ref_px, pos, margins, a_sign, l_sign);

    let depth_guard_enabled = cfg.arb.depth_guard.enabled;
    let liquidity_multiple = if depth_guard_enabled {
        cfg.arb.depth_guard.liquidity_multiple
    } else {
        Decimal::ONE
    };
    let max_levels = if depth_guard_enabled {
        cfg.arb.depth_guard.max_levels
    } else {
        1
    };
    let sell_depth_available_qty = sell_book.cumulative_qty(Side::Sell, max_levels);
    let buy_depth_available_qty = buy_book.cumulative_qty(Side::Buy, max_levels);
    let depth_supported_qty = sell_depth_available_qty.min(buy_depth_available_qty)
        / liquidity_multiple;
    let max_qty = depth_supported_qty.min(headroom_qty).min(margin_room_qty);
    let mut executable_qty = if max_qty > Decimal::ZERO {
        let q = floor_to_common_step(desired_qty.min(max_qty), spec.step, spec.lighter_qty_step);
        if q >= est_min_qty {
            q
        } else {
            Decimal::ZERO
        }
    } else {
        Decimal::ZERO
    };
    let mut min_qty = est_min_qty;
    let mut gross_edge_bps = None;
    let mut sell_depth_target_qty = Decimal::ZERO;
    let mut buy_depth_target_qty = Decimal::ZERO;
    let mut sell_depth_worst_px = Decimal::ZERO;
    let mut buy_depth_worst_px = Decimal::ZERO;
    let mut sell_depth_levels_used = 0usize;
    let mut buy_depth_levels_used = 0usize;
    if executable_qty > Decimal::ZERO {
        let depth_target = executable_qty * liquidity_multiple;
        match (
            sell_book.depth_vwap(Side::Sell, depth_target, max_levels),
            buy_book.depth_vwap(Side::Buy, depth_target, max_levels),
        ) {
            (Some(sell_quote), Some(buy_quote)) => {
                let sell_px = sell_quote.vwap_px;
                let buy_px = buy_quote.vwap_px;
                let aster_px = if matches!(direction.aster_side(), Side::Sell) {
                    sell_px
                } else {
                    buy_px
                };
                let lighter_px = if matches!(direction.lighter_side(), Side::Sell) {
                    sell_px
                } else {
                    buy_px
                };
                if let Some(vwap_min_qty) = min_trade_qty(spec, aster_px, lighter_px) {
                    min_qty = vwap_min_qty;
                    if executable_qty < min_qty {
                        executable_qty = Decimal::ZERO;
                    } else {
                        gross_edge_bps = Some((sell_px - buy_px) / ref_px * Decimal::from(10_000));
                    }
                } else {
                    executable_qty = Decimal::ZERO;
                }
                sell_depth_target_qty = sell_quote.target_qty;
                buy_depth_target_qty = buy_quote.target_qty;
                sell_depth_worst_px = sell_quote.worst_px;
                buy_depth_worst_px = buy_quote.worst_px;
                sell_depth_levels_used = sell_quote.levels_used;
                buy_depth_levels_used = buy_quote.levels_used;
            }
            _ => executable_qty = Decimal::ZERO,
        }
    }
    let limiting_reason = if depth_supported_qty <= Decimal::ZERO {
        "depth"
    } else if headroom_qty < min_qty {
        "headroom"
    } else if margin_room_qty < min_qty {
        "margin"
    } else if executable_qty <= Decimal::ZERO {
        "min_qty"
    } else if gross_edge_bps
        .is_some_and(|edge| edge < cfg.arb.required_gross_edge_bps())
    {
        "edge"
    } else {
        "ok"
    };
    let qty_for_effect = if executable_qty > Decimal::ZERO {
        executable_qty
    } else {
        ceil_to_common_step(min_qty, spec.step, spec.lighter_qty_step)
    };

    DirectionStatus {
        direction: direction.as_str(spec.first, spec.hedge),
        aster_side: direction.aster_side().as_str(),
        lighter_side: direction.lighter_side().as_str(),
        gross_edge_bps,
        expected_net_margin_bps: gross_edge_bps
            .map(|edge| edge - cfg.arb.required_gross_edge_bps()),
        executable_qty,
        executable_notional_usd: (executable_qty > Decimal::ZERO)
            .then_some(executable_qty * ref_px),
        limiting_reason,
        exposure_effect: exposure_effect(pos, a_sign, l_sign, qty_for_effect),
        top_depth_qty,
        depth_guard_enabled,
        liquidity_multiple,
        depth_supported_qty,
        sell_depth_target_qty,
        buy_depth_target_qty,
        sell_depth_available_qty,
        buy_depth_available_qty,
        sell_depth_worst_px,
        buy_depth_worst_px,
        sell_depth_levels_used,
        buy_depth_levels_used,
        sell_best_px: sell_top.px,
        buy_best_px: buy_top.px,
        sell_best_qty: sell_top.qty,
        buy_best_qty: buy_top.qty,
        min_qty,
        desired_qty,
        headroom_qty,
        margin_room_qty,
    }
}

fn empty_direction(direction: Direction, reason: &'static str) -> DirectionStatus {
    DirectionStatus {
        // `taker status` reads Lighter markets only.
        direction: direction.as_str(Default::default(), crate::config::HedgeVenue::Lighter),
        aster_side: direction.aster_side().as_str(),
        lighter_side: direction.lighter_side().as_str(),
        gross_edge_bps: None,
        expected_net_margin_bps: None,
        executable_qty: Decimal::ZERO,
        executable_notional_usd: None,
        limiting_reason: reason,
        exposure_effect: "unknown",
        top_depth_qty: Decimal::ZERO,
        depth_guard_enabled: false,
        liquidity_multiple: Decimal::ZERO,
        depth_supported_qty: Decimal::ZERO,
        sell_depth_target_qty: Decimal::ZERO,
        buy_depth_target_qty: Decimal::ZERO,
        sell_depth_available_qty: Decimal::ZERO,
        buy_depth_available_qty: Decimal::ZERO,
        sell_depth_worst_px: Decimal::ZERO,
        buy_depth_worst_px: Decimal::ZERO,
        sell_depth_levels_used: 0,
        buy_depth_levels_used: 0,
        sell_best_px: Decimal::ZERO,
        buy_best_px: Decimal::ZERO,
        sell_best_qty: Decimal::ZERO,
        buy_best_qty: Decimal::ZERO,
        min_qty: Decimal::ZERO,
        desired_qty: Decimal::ZERO,
        headroom_qty: Decimal::ZERO,
        margin_room_qty: Decimal::ZERO,
    }
}

fn exposure_effect(
    pos: PositionSnapshot,
    a_sign: Decimal,
    l_sign: Decimal,
    qty: Decimal,
) -> &'static str {
    if qty <= Decimal::ZERO {
        return "unknown";
    }
    let before = pos.aster_qty.abs().max(pos.lighter_qty.abs());
    let after_a = pos.aster_qty + a_sign * qty;
    let after_l = pos.lighter_qty + l_sign * qty;
    let after = after_a.abs().max(after_l.abs());
    if after < before {
        "reduce"
    } else if after > before {
        "increase"
    } else {
        "flat"
    }
}

fn max_qty_by_headroom(
    max_abs_qty: Decimal,
    pos: PositionSnapshot,
    a_sign: Decimal,
    l_sign: Decimal,
) -> Decimal {
    fn leg(max_abs_qty: Decimal, current: Decimal, sign: Decimal) -> Decimal {
        let same_direction = current == Decimal::ZERO
            || (current > Decimal::ZERO && sign > Decimal::ZERO)
            || (current < Decimal::ZERO && sign < Decimal::ZERO);
        if same_direction {
            (max_abs_qty - current.abs()).max(Decimal::ZERO)
        } else {
            current.abs() + max_abs_qty
        }
    }
    leg(max_abs_qty, pos.aster_qty, a_sign).min(leg(max_abs_qty, pos.lighter_qty, l_sign))
}

fn max_qty_by_available_margin(
    buffer: Decimal,
    ref_px: Decimal,
    pos: PositionSnapshot,
    margins: MarginSnapshot,
    a_sign: Decimal,
    l_sign: Decimal,
) -> Decimal {
    fn leg(
        ref_px: Decimal,
        current: Decimal,
        sign: Decimal,
        available: Decimal,
        buffer: Decimal,
    ) -> Decimal {
        let increases_abs = current == Decimal::ZERO
            || (current > Decimal::ZERO && sign > Decimal::ZERO)
            || (current < Decimal::ZERO && sign < Decimal::ZERO);
        let usable = available - buffer;
        let margin_qty = if usable <= Decimal::ZERO || ref_px <= Decimal::ZERO {
            Decimal::ZERO
        } else {
            usable / ref_px
        };
        // As in arb: reducing is margin-free only down to flat; beyond it the trade opens
        // the other side.
        if increases_abs { margin_qty } else { current.abs() + margin_qty }
    }
    leg(ref_px, pos.aster_qty, a_sign, margins.aster_available_usd, buffer)
        .min(leg(ref_px, pos.lighter_qty, l_sign, margins.lighter_available_usd, buffer))
}

fn min_trade_qty(
    spec: &MarketSpec,
    aster_px: Decimal,
    lighter_px: Decimal,
) -> Option<Decimal> {
    if aster_px <= Decimal::ZERO || lighter_px <= Decimal::ZERO {
        return None;
    }
    Some(
        spec.aster_min_qty
            .max(spec.aster_min_notional / aster_px)
            .max(spec.lighter_min_notional / lighter_px),
    )
}

fn floor_to_common_step(qty: Decimal, aster_step: Decimal, lighter_step: Decimal) -> Decimal {
    common_qty_step(aster_step,lighter_step).map(|step|floor_to_step(qty,step)).unwrap_or(Decimal::ZERO)
}

fn ceil_to_common_step(qty: Decimal, aster_step: Decimal, lighter_step: Decimal) -> Decimal {
    common_qty_step(aster_step,lighter_step).map(|step|ceil_to_step(qty,step)).unwrap_or(Decimal::ZERO)
}
#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn exposure_effect_identifies_reduce_and_increase() {
        let pos = PositionSnapshot {
            aster_qty: dec!(-1.2),
            lighter_qty: dec!(1.2),
        };
        assert_eq!(exposure_effect(pos, dec!(1), dec!(-1), dec!(0.2)), "reduce");
        assert_eq!(
            exposure_effect(pos, dec!(-1), dec!(1), dec!(0.2)),
            "increase"
        );
    }

    #[test]
    fn a_reducing_leg_is_margin_free_only_down_to_flat() {
        // Short 1 on Aster, long 1 on Lighter, 20 USD usable a venue at price 10: buying on
        // Aster reduces both legs by 1, then opens up to 2 more.
        let pos = PositionSnapshot { aster_qty: dec!(-1), lighter_qty: dec!(1) };
        let margins = MarginSnapshot { aster_available_usd: dec!(70), lighter_available_usd: dec!(70) };
        assert_eq!(max_qty_by_available_margin(dec!(50), dec!(10), pos, margins, dec!(1), dec!(-1)), dec!(3));
    }

    #[test]
    fn common_step_ceil_reaches_common_multiple() {
        assert_eq!(
            ceil_to_common_step(dec!(0.171), dec!(0.02), dec!(0.03)),
            dec!(0.18)
        );
    }
}
