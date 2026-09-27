//! Live primitive probes. `lighter_aster_bot probe <check>` exercises one venue action in
//! isolation with the REAL signers, printing action / round-trip latency / resulting state and
//! self-cleaning any order it opens. Signed reads and `lighter-order-dry-run` (local signing
//! only) send no order. `aster-place-cancel` rests REAL post-only orders 1.8% outside the touch
//! and cancels them; `lighter-market` trades REAL market orders and requires
//! `--i-understand-live` and a `--max-usd` cap it refuses to exceed.

use std::path::Path;
use std::time::Instant;

use anyhow::{anyhow, bail, Result};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use tracing::info;

use crate::config::{Config, MarketCfg};
use crate::connectors::rest_specs;
use crate::markets::MarketSpec;
use crate::types::{MarketId, Side};

use super::exec::aster::AsterRest;
use super::exec::command::ExecEvent;
use super::exec::creds::{AsterCreds, LighterCreds};
use super::exec::lighter::LighterExchange;

/// Build the live Aster client for the given specs.
fn build_aster(cfg: &Config, specs: &[MarketSpec]) -> Result<AsterRest> {
    super::status::build_aster(cfg, specs, AsterCreds::from_env()?)
}

/// Build the live Lighter client for the given specs.
async fn build_hl(cfg: &Config, specs: &[MarketSpec]) -> Result<LighterExchange> {
    let creds = LighterCreds::from_env()?;
    LighterExchange::new_lighter(
        cfg.live.lighter.base_url.clone(),
        Path::new(&cfg.live.lighter.signers_dir),
        creds,
        specs,
        cfg.live.lighter.fill_timeout_ms,
        cfg.live.lighter.ws_account_max_age_ms,
    )
    .await
}

/// Fetch the Aster best (bid, ask) for a symbol via the public bookTicker (unsigned).
async fn aster_book_ticker(cfg: &Config, symbol: &str) -> Result<(Decimal, Decimal)> {
    let url = format!("{}/fapi/v1/ticker/bookTicker?symbol={symbol}", cfg.live.aster.base_url);
    let v: serde_json::Value = reqwest::get(&url).await?.json().await?;
    let parse = |k: &str| v.get(k).and_then(|p| p.as_str()).and_then(|s| s.parse().ok());
    match (parse("bidPrice"), parse("askPrice")) {
        (Some(b), Some(a)) => Ok((b, a)),
        _ => Err(anyhow!("no bid/ask for {symbol}: {v}")),
    }
}

/// Resolve the single target market's specs.
async fn resolve(cfg: &Config, target: &str) -> Result<(Vec<MarketCfg>, Vec<MarketSpec>)> {
    let markets = cfg.select_markets(Some(target));
    if markets.is_empty() {
        bail!("no market '{target}' in config [[markets]] (try the market id, e.g. HYPE)");
    }
    let specs = rest_specs::build_market_specs_with_bases(
        &markets,
        cfg.live.partials.lighter_min_notional,
        &cfg.live.aster.base_url,
        &cfg.live.lighter.base_url,
    )
    .await?;
    Ok((markets, specs))
}

/// Entry point for `lighter_aster_bot probe <check>`.
pub async fn run(cfg: &Config, check: &str, target: Option<String>, i_understand_live: bool, max_usd: Decimal) -> Result<()> {
    let target = target.unwrap_or_else(|| "HYPE".into());
    match check {
        "aster-balance" => probe_aster_balance(cfg).await,
        "aster-positions" => probe_aster_positions(cfg).await,
        "aster-open-orders" => probe_aster_open_orders(cfg, &target).await,
        "aster-place-cancel" => probe_aster_place_cancel(cfg, &target).await,
        "leverage" | "live-leverage" => probe_leverage(cfg, &target).await,
        "lighter-balance" => probe_hl_balance(cfg, &target).await,
        "lighter-open-orders" => probe_lighter_open_orders(cfg, &target).await,
        "lighter-order-dry-run" => probe_lighter_order_dry_run(cfg, &target).await,
        "lighter-market" => probe_hl_market(cfg, &target, i_understand_live, max_usd).await,
        other => bail!(
            "unknown probe '{other}'. Available: aster-balance, aster-positions, aster-open-orders, \
             aster-place-cancel, leverage, lighter-balance, lighter-open-orders, lighter-order-dry-run, lighter-market"
        ),
    }
}

/// An Aster client for an account-wide read: any one configured market gives it wire context.
async fn account_wide_aster(cfg: &Config) -> Result<AsterRest> {
    let markets = cfg.select_markets(None);
    let specs = rest_specs::build_market_specs_with_bases(
        &markets[..1.min(markets.len())],
        cfg.live.partials.lighter_min_notional,
        &cfg.live.aster.base_url,
        &cfg.live.lighter.base_url,
    )
    .await?;
    build_aster(cfg, &specs)
}

async fn probe_aster_balance(cfg: &Config) -> Result<()> {
    let aster = account_wide_aster(cfg).await?;
    let t0 = Instant::now();
    let rows = aster.balance().await?;
    println!("aster balance ({}ms):", t0.elapsed().as_millis());
    for r in rows.iter().filter(|r| r.balance.parse::<f64>().unwrap_or(0.0) != 0.0) {
        println!("  {:<6} balance={} crossWallet={}", r.asset, r.balance, r.cross_wallet_balance);
    }
    Ok(())
}

/// Account-wide signed `positionRisk` read: prints every non-zero Aster position (signed
/// `positionAmt`, entry, unrealized PnL, leverage, side). The one-way (`positionSide=BOTH`)
/// `positionAmt` is directly comparable to Lighter's signed `szi`, so a hedged pair reads as
/// Aster `-x` against Lighter `+x`. No order risk — a pure signed read.
async fn probe_aster_positions(cfg: &Config) -> Result<()> {
    let aster = account_wide_aster(cfg).await?;
    let t0 = Instant::now();
    let rows = aster.position_risk().await?;
    println!("aster positions ({}ms):", t0.elapsed().as_millis());
    let mut any = false;
    for r in &rows {
        if r.position_amt.parse::<f64>().unwrap_or(0.0) == 0.0 {
            continue;
        }
        any = true;
        println!(
            "  {:<10} positionAmt={} entryPrice={} uPnL={} lev={} side={}",
            r.symbol, r.position_amt, r.entry_price, r.unrealized_profit, r.leverage, r.position_side
        );
    }
    if !any {
        println!("  (no open positions)");
    }
    Ok(())
}

async fn probe_aster_open_orders(cfg: &Config, target: &str) -> Result<()> {
    let (_m, specs) = resolve(cfg, target).await?;
    let aster = build_aster(cfg, &specs)?;
    let market = specs[0].market_id.clone();
    let orders = aster.open_orders(Some(&market)).await?;
    println!("aster open orders for {} ({}): {}", specs[0].aster_symbol, market, orders.len());
    for o in &orders {
        println!("  {} {} {} @ {} status={} cid={}", o.order_id, o.side, o.orig_qty, o.price, o.status, o.client_order_id);
    }
    Ok(())
}

async fn probe_aster_place_cancel(cfg: &Config, target: &str) -> Result<()> {
    let (_m, specs) = resolve(cfg, target).await?;
    let spec = &specs[0];
    let aster = build_aster(cfg, &specs)?;
    let market = spec.market_id.clone();
    let (bid, ask) = aster_book_ticker(cfg, &spec.aster_symbol).await?;
    // Test BOTH sides with REAL orders: a post-only BUY 1.8% BELOW bid and a post-only SELL 1.8%
    // ABOVE ask — both within the ±2% band, neither can cross, so neither can realistically fill.
    aster_place_cancel_side(&aster, spec, &market, Side::Buy, bid * dec!(0.982)).await?;
    aster_place_cancel_side(&aster, spec, &market, Side::Sell, ask * dec!(1.018)).await?;
    Ok(())
}

/// Place a post-only order on one side at `px` and cancel it; assert it rested then cleared.
async fn aster_place_cancel_side(aster: &AsterRest, spec: &MarketSpec, market: &MarketId, side: Side, px: Decimal) -> Result<()> {
    // Size to clear BOTH the min-notional AND the min-qty/lot-step (the latter binds for
    // high-priced coins like BNB, where min_notional/px underflows one step). Ceil to step.
    let by_notional = crate::decimal::ceil_to_step(spec.aster_min_notional * dec!(1.1) / px, spec.step);
    let qty = spec.aster_min_qty.max(by_notional).max(spec.step);
    let tag = match side { Side::Buy => "B", Side::Sell => "S" };
    let cid = format!("Xprb{tag}-{}-{}", short(&market.0), epoch_tag());
    info!("aster place-cancel {side:?}: post-only {side:?} {qty} {} @ ~{px}", spec.aster_symbol);
    let t0 = Instant::now();
    let ev = aster.place_decimal(market, side, px, qty, &cid, false).await;
    let place_ms = t0.elapsed().as_millis();
    let oid = match &ev {
        ExecEvent::PlaceAck { venue_order_id, .. } => {
            println!("PLACE  {side:?} ok ({place_ms}ms): orderId={venue_order_id} cid={cid}");
            venue_order_id.clone()
        }
        ExecEvent::PlaceReject { reason, .. } => bail!("PLACE {side:?} rejected: {reason}"),
        other => bail!("unexpected place event: {other:?}"),
    };
    let t1 = Instant::now();
    aster.cancel_order(market, &cid).await?;
    let cancel_ms = t1.elapsed().as_millis();
    let remaining = aster.open_orders(Some(market)).await?;
    let clean = !remaining.iter().any(|o| o.client_order_id == cid);
    println!("CANCEL {side:?} ok ({cancel_ms}ms): orderId={oid} -> clean={}", if clean { "YES" } else { "NO" });
    if !clean {
        bail!("{side:?} order {cid} still resting after cancel");
    }
    Ok(())
}

async fn probe_leverage(cfg: &Config, target: &str) -> Result<()> {
    let (_m, specs) = resolve(cfg, target).await?;
    let aster = build_aster(cfg, &specs)?;
    let hl = build_hl(cfg, &specs).await?;
    for spec in &specs {
        let aster_lev = aster.get_leverage(&spec.market_id).await?;
        let lighter_lev = hl.get_leverage(&spec.market_id).await?;
        println!(
            "{} leverage: Aster={}x Lighter={}x",
            spec.market_id, aster_lev, lighter_lev
        );
        if aster_lev != 1 {
            bail!("Aster leverage for {} is {aster_lev}x (expected 1x)", spec.market_id);
        }
        if lighter_lev != Decimal::ONE {
            bail!("Lighter leverage for {} is {lighter_lev}x (expected 1x)", spec.market_id);
        }
    }
    Ok(())
}

async fn probe_hl_balance(cfg: &Config, target: &str) -> Result<()> {
    let (_m, specs) = resolve(cfg, target).await?;
    let hl = build_hl(cfg, &specs).await?;
    let st = hl.clearinghouse_state().await?;
    println!("lighter account value: {} (available {})", st.margin_summary.account_value, st.withdrawable);
    for p in &st.asset_positions {
        if p.position.szi.parse::<f64>().unwrap_or(0.0) != 0.0 {
            println!("  {} szi={}", p.position.coin, p.position.szi);
        }
    }
    Ok(())
}

async fn probe_lighter_open_orders(cfg: &Config, target: &str) -> Result<()> {
    let (_m, specs) = resolve(cfg, target).await?;
    let hl = build_hl(cfg, &specs).await?;
    let rows = hl.open_orders_info().await?;
    println!("lighter open orders: {}", rows.len());
    for o in rows {
        println!("  {} oid={} side={} qty={} px={}", o.coin, o.oid, o.side, o.sz, o.limit_px);
    }
    Ok(())
}

async fn probe_lighter_order_dry_run(cfg: &Config, target: &str) -> Result<()> {
    let (_m, specs) = resolve(cfg, target).await?;
    let spec = &specs[0];
    let hl = build_hl(cfg, &specs).await?;
    let market = spec.market_id.clone();
    let mid = hl.mid(&spec.hl_coin).await?;
    let sz = round_up_size(spec.hl_min_notional * dec!(1.02) / mid, spec.lighter_size_decimals);
    let buy_ioc = hl.build_ioc_limit_plan(&market, Side::Buy, mid * dec!(1.005), sz, 42_000_001, false)?;
    let sell_ioc = hl.build_ioc_limit_plan(&market, Side::Sell, mid * dec!(0.995), sz, 42_000_002, false)?;
    let buy_market = hl.build_market_plan(&market, Side::Buy, market_bound_px(mid, Side::Buy), sz, 42_000_003, false)?;
    let sell_market = hl.build_market_plan(&market, Side::Sell, market_bound_px(mid, Side::Sell), sz, 42_000_004, false)?;
    for (name, plan) in [
        ("ioc-buy", buy_ioc),
        ("ioc-sell", sell_ioc),
        ("market-buy", buy_market),
        ("market-sell", sell_market),
    ] {
        let signed = hl.sign_order_plan(&plan, 123_456_789)?;
        println!(
            "{name}: market={} client={} base_amount={} price={} expiry={} order_type={} tif={} reduce_only={} tx_type={} tx_hash_len={} tx_info_len={}",
            plan.market_index,
            plan.client_order_index,
            plan.base_amount,
            plan.price,
            plan.order_expiry,
            plan.order_type,
            plan.time_in_force,
            plan.reduce_only,
            signed.tx_type,
            signed.tx_hash.len(),
            signed.tx_info.len()
        );
    }
    Ok(())
}

/// Money-risking: tiny native Lighter MARKET buy, position detection, then reduce-only
/// MARKET sell back to flat. Requires `--i-understand-live` and stays under `--max-usd`.
async fn probe_hl_market(cfg: &Config, target: &str, i_understand_live: bool, max_usd: Decimal) -> Result<()> {
    if !i_understand_live {
        bail!("lighter-market risks real funds: re-run with --i-understand-live --max-usd <N>");
    }
    if max_usd <= Decimal::ZERO || max_usd > dec!(20) {
        bail!("--max-usd must be in (0, 20] for the probe (got {max_usd})");
    }
    let (_m, specs) = resolve(cfg, target).await?;
    let spec = &specs[0];
    if max_usd < spec.hl_min_notional {
        bail!("--max-usd {max_usd} is below Lighter min notional {} — pick a larger cap", spec.hl_min_notional);
    }
    let hl = build_hl(cfg, &specs).await?;
    hl_market_buy_then_sell(&hl, spec, max_usd).await?;
    Ok(())
}

/// Native Lighter MARKET buy -> detected position size -> reduce-only MARKET sell to exactly flat.
async fn hl_market_buy_then_sell(hl: &LighterExchange, spec: &MarketSpec, max_usd: Decimal) -> Result<()> {
    let market = spec.market_id.clone();
    let mid = hl.mid(&spec.hl_coin).await?;
    // Size to clear the Lighter min notional with 2% to spare, rounded UP to the size decimals so
    // the order builder's floor keeps it, and stay under the cap.
    let sz = round_up_size(spec.hl_min_notional * dec!(1.02) / mid, spec.lighter_size_decimals);
    let est = sz * mid;
    if est > max_usd {
        bail!("smallest Lighter order ~${est:.2} exceeds --max-usd {max_usd}; raise the cap");
    }

    let start = lighter_position(hl, spec).await?;
    println!("START position: {} {}", start, spec.hl_coin);
    if start != Decimal::ZERO {
        bail!("refusing market-order test because starting {} position is {start}, not 0", spec.hl_coin);
    }

    info!("lighter-market: MARKET buy {sz} {} then reduce-only MARKET sell detected size", spec.hl_coin);
    let buy_bound = market_bound_px(mid, Side::Buy);
    let body = hl.place_raw(&market, Side::Buy, buy_bound, sz, "market", false, Some(probe_cloid())).await?;
    println!("BUY   MARKET qty={} est_notional~{}: {}", sz, est.round_dp(4), body);

    let after_buy = wait_for_position(hl, spec, |p| p > Decimal::ZERO, std::time::Duration::from_secs(8)).await?;
    println!("AFTER BUY position: {} {}", after_buy, spec.hl_coin);
    if after_buy <= Decimal::ZERO {
        bail!("MARKET buy did not create a positive position; detected {after_buy}");
    }

    let fbody = hl
        .place_raw(
            &market,
            Side::Sell,
            market_bound_px(hl.mid(&spec.hl_coin).await?, Side::Sell),
            after_buy,
            "market",
            true,
            Some(probe_cloid()),
        )
        .await?;
    println!("SELL  MARKET reduce_only qty={}: {}", after_buy, fbody);

    let mut final_pos = wait_for_position(hl, spec, |p| p == Decimal::ZERO, std::time::Duration::from_secs(8)).await?;
    if final_pos != Decimal::ZERO {
        println!("FINAL position after sell is {} {}; attempting reduce-only cleanup", final_pos, spec.hl_coin);
        final_pos = flatten_lighter_position(hl, spec, 3).await?;
    }
    println!("FINAL position: {} {}", final_pos, spec.hl_coin);
    if final_pos != Decimal::ZERO {
        bail!("final {} position is {final_pos}, not 0; manual check required", spec.hl_coin);
    }

    Ok(())
}

async fn lighter_position(hl: &LighterExchange, spec: &MarketSpec) -> Result<Decimal> {
    let st = hl.clearinghouse_state().await?;
    Ok(st
        .asset_positions
        .iter()
        .find(|p| p.position.coin == spec.hl_coin)
        .and_then(|p| p.position.szi.parse::<Decimal>().ok())
        .unwrap_or(Decimal::ZERO))
}

async fn wait_for_position<F>(
    hl: &LighterExchange,
    spec: &MarketSpec,
    ok: F,
    timeout: std::time::Duration,
) -> Result<Decimal>
where
    F: Fn(Decimal) -> bool,
{
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let pos = lighter_position(hl, spec).await?;
        if ok(pos) || tokio::time::Instant::now() >= deadline {
            return Ok(pos);
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
}

async fn flatten_lighter_position(hl: &LighterExchange, spec: &MarketSpec, attempts: usize) -> Result<Decimal> {
    let market = spec.market_id.clone();
    let mut pos = lighter_position(hl, spec).await?;
    for _ in 0..attempts {
        if pos == Decimal::ZERO {
            return Ok(pos);
        }
        let side = if pos > Decimal::ZERO { Side::Sell } else { Side::Buy };
        let px = market_bound_px(hl.mid(&spec.hl_coin).await?, side);
        let body = hl
            .place_raw(&market, side, px, pos.abs(), "market", true, Some(probe_cloid()))
            .await?;
        println!("CLEANUP {side:?} MARKET reduce_only qty={}: {body}", pos.abs());
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        pos = lighter_position(hl, spec).await?;
    }
    Ok(pos)
}

fn round_up_size(qty: Decimal, size_decimals: u32) -> Decimal {
    qty.round_dp_with_strategy(size_decimals, rust_decimal::RoundingStrategy::ToPositiveInfinity)
}

fn market_bound_px(mid: Decimal, side: Side) -> Decimal {
    match side {
        Side::Buy => mid * dec!(1.01),
        Side::Sell => mid * dec!(0.99),
    }
}

fn short(s: &str) -> String {
    s.chars().filter(|c| c.is_ascii_alphanumeric()).take(5).collect()
}

fn epoch_tag() -> String {
    let n = chrono::Utc::now().timestamp_millis() as u64 % 10_000_000;
    n.to_string()
}

/// A random 128-bit cloid hex for probe orders.
fn probe_cloid() -> String {
    format!("0x{}", uuid::Uuid::new_v4().simple())
}
