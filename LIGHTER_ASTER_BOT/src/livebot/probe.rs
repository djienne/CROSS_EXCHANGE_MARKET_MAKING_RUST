//! Live primitive probes. `lighter_aster_bot probe <check>` exercises XEMM's own venue calls
//! with the REAL signers, printing latency and state and attempting cleanup on failure.
//! [`run`] lists the checks; those that send REAL orders need
//! `--i-understand-live` and take the venue's leg lock. [`close`], the operator's exit, flattens
//! a coin on every venue through the same order paths.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use anyhow::{anyhow, bail, Context, Result};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use tokio_util::sync::CancellationToken;

use crate::config::{Config, HedgeVenue, MarketCfg};
use crate::connectors::{rest_book::fetch_hedge_book, rest_specs};
use crate::livebot::scale::MarketScale;
use crate::markets::MarketSpec;
use crate::types::Side;

use super::exec::aster::{order_progress, run_aster_worker, AsterRest};
use super::exec::command::{ExecCommand, ExecEvent, HedgeCommand, MakerPermit};
use super::exec::creds::{AsterCreds, HyperliquidCreds, LighterCreds};
use super::exec::hyperliquid::{run_hyperliquid_worker, HyperliquidHedge};
use super::exec::lighter::{hedge_reject_is_definitive_no_fill, run_lighter_worker, HlClearinghouse, LighterExchange};
use super::fills::{HedgeIntent, IntentPurpose};
use super::ids::Cloid;
use super::journal::Journal;
use super::userstream::StreamLiveness;

/// Build the live Aster client for the given specs.
fn build_aster(cfg: &Config, specs: &[MarketSpec]) -> Result<AsterRest> {
    super::status::build_aster(cfg, specs, AsterCreds::from_env()?)
}

/// Build the live Lighter client for the given specs.
async fn build_lighter(cfg: &Config, specs: &[MarketSpec]) -> Result<LighterExchange> {
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
    let specs = rest_specs::build_market_specs(&markets, &cfg.live).await?;
    Ok((markets, specs))
}

/// Entry point for `lighter_aster_bot probe <check>`.
pub async fn run(cfg: &Config, check: &str, target: Option<String>, i_understand_live: bool, max_usd: Decimal) -> Result<()> {
    let target = target.unwrap_or_else(|| "HYPE".into());
    match check {
        "aster-balance" => probe_aster_balance(cfg).await,
        "aster-positions" => probe_aster_positions(cfg).await,
        "aster-open-orders" => probe_aster_open_orders(cfg, &target).await,
        "aster-place-cancel" => probe_aster_place_cancel(cfg, &target, i_understand_live).await,
        "leverage" | "live-leverage" => probe_leverage(cfg, &target).await,
        "lighter-balance" => probe_lighter_balance(cfg, &target).await,
        "lighter-open-orders" => probe_lighter_open_orders(cfg, &target).await,
        "lighter-order-dry-run" => probe_lighter_order_dry_run(cfg, &target).await,
        "lighter-market" => probe_hedge(cfg, &target, CloseVenue::Lighter, i_understand_live, max_usd).await,
        "hl-hedge" => probe_hedge(cfg, &target, CloseVenue::Hyperliquid, i_understand_live, max_usd).await,
        "hl-balance" | "hl-place-cancel" | "hl-market" => crate::hyperliquid::probe::run(check, &target, i_understand_live, max_usd).await,
        other => bail!(
            "unknown probe '{other}'. Available: aster-balance, aster-positions, aster-open-orders, \
             aster-place-cancel, leverage, lighter-balance, lighter-open-orders, lighter-order-dry-run, lighter-market, hl-hedge, hl-balance, hl-place-cancel, hl-market"
        ),
    }
}

/// An Aster client for an account-wide read: any one configured market gives it wire context.
async fn account_wide_aster(cfg: &Config) -> Result<AsterRest> {
    let markets = cfg.select_markets(None);
    let specs = rest_specs::build_market_specs(&markets[..1.min(markets.len())], &cfg.live).await?;
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

/// XEMM's Aster wire calls, each timed, with the user stream up throughout: place; refreshes
/// through the bot's own worker (amends, one to the same values, one through the ask, one whose
/// permit lapsed, one of the cancelled order); a post-only that would cross; cancel-all on both
/// sides; the dead-man firing. Every order rests 1.8-2.2% from the touch except the crossing
/// ones. Resting orders can still fill if the market moves to them.
async fn probe_aster_place_cancel(cfg: &Config, target: &str, i_understand_live: bool) -> Result<()> {
    if !i_understand_live {
        bail!("aster-place-cancel amends an order across the book: re-run with --i-understand-live");
    }
    let (_m, specs) = resolve(cfg, target).await?;
    let spec = &specs[0];
    let _leg = crate::controller::lock_leg(CloseVenue::Aster, &spec.aster_symbol)?;
    let aster = build_aster(cfg, &specs)?;
    let market = spec.market_id.clone();
    if aster_position(&aster, spec).await? != Decimal::ZERO || !aster.open_orders(Some(&market)).await?.is_empty() {
        bail!("refusing: {} must start flat with no open orders", spec.aster_symbol);
    }
    let (http, time) = (reqwest::Client::new(), format!("{}/fapi/v1/time", cfg.live.aster.base_url));
    let ping = min_of_5(async || { http.get(&time).send().await?.bytes().await?; Ok(()) }).await?;
    println!("PING   min of 5 GET /fapi/v1/time: {ping}ms");
    let stop = CancellationToken::new();
    let liveness = Arc::new(StreamLiveness::default());
    let stream = spawn_fill_printer(build_aster(cfg, &specs)?, spec, liveness.clone(), stop.clone());
    let steps = aster_steps(cfg, &aster, spec).await;
    // Whatever failed above, nothing may stay resting or open.
    let closed = close_aster(&aster, spec).await;
    println!("USER STREAM last message {}ms ago", liveness.age_ms(crate::hotpath::clock::mono_now_ns()));
    stop.cancel();
    let _ = stream.await;
    anyhow::ensure!(aster.open_orders(Some(&market)).await?.is_empty(), "{} ends with open orders; manual check required", spec.aster_symbol);
    steps.and(closed)
}

async fn aster_steps(cfg: &Config, aster: &AsterRest, spec: &MarketSpec) -> Result<()> {
    let market = spec.market_id.clone();
    let (bid, ask) = aster_book_ticker(cfg, &spec.aster_symbol).await?;
    let scale = MarketScale::from_spec(spec);
    let price_of = |order: &serde_json::Value| order["price"].as_str().and_then(|p| p.parse::<Decimal>().ok());
    let (commands, rx) = tokio::sync::mpsc::channel(8);
    let (_priority, prio_rx) = tokio::sync::mpsc::channel(1);
    let (events_tx, mut events) = tokio::sync::mpsc::channel(16);
    let worker = tokio::spawn(run_aster_worker(rx, prio_rx, events_tx, build_aster(cfg, std::slice::from_ref(spec))?));
    let (cid, oid, lots) = place_ok(aster, spec, Side::Buy, bid * dec!(0.982)).await?;
    let mut amend = async |label: &str, price_ticks: i64, permit: MakerPermit| -> Result<ExecEvent> {
        let t = Instant::now();
        let cmd = ExecCommand::Amend { permit, market: market.clone(), side: Side::Buy, client_id: cid.clone(), price_ticks, qty_lots: lots };
        commands.send(cmd).await?;
        let ev = tokio::time::timeout(std::time::Duration::from_secs(10), events.recv()).await?.context("the Aster worker stopped")?;
        println!("AMEND  {label} ({}ms): {ev:?}", t.elapsed().as_millis());
        Ok(ev)
    };
    let mut at = 0;
    for step in 1..=3 {
        at = scale.price_floor_ticks(bid * (dec!(0.978) - Decimal::new(step, 3)));
        let ev = amend("far", at, MakerPermit::unguarded()).await?;
        if !matches!(&ev, ExecEvent::PlaceAck { venue_order_id, .. } if *venue_order_id == oid) {
            bail!("an amend must be acked on the order it moved");
        }
    }
    let order = aster.query_order(&market, &cid).await?;
    if order["status"] != "NEW" || price_of(&order) != Some(scale.ticks_to_price(at)) {
        bail!("the amended order does not rest at its new price: {order}");
    }
    // The same values again: whatever Aster answers, the order must stay resting and tracked.
    let ev = amend("to the same price and qty", at, MakerPermit::unguarded()).await?;
    if !matches!(ev, ExecEvent::PlaceAck { .. } | ExecEvent::AmendReject { .. })
        || aster.query_order(&market, &cid).await?["status"] != "NEW" {
        bail!("an amend to the same values must leave the order resting and tracked");
    }
    // 0.2% through a fresh ask, so a tick of drift cannot leave it resting.
    let through_ask = scale.price_ceil_ticks(aster_book_ticker(cfg, &spec.aster_symbol).await?.1 * dec!(1.002));
    let ev = amend("through the ask", through_ask, MakerPermit::unguarded()).await?;
    let order = aster.query_order(&market, &cid).await?;
    if !matches!(ev, ExecEvent::AmendReject { .. }) || order["status"] != "NEW" || price_of(&order) != Some(scale.ticks_to_price(at)) {
        bail!("a crossing amend must be refused and leave the order as it was: {order}");
    }
    // Rights lost before send: the worker pulls the quote instead.
    let lapsed = MakerPermit::unguarded();
    lapsed.cancel_queued();
    if !matches!(amend("with a lapsed permit", at, lapsed).await?, ExecEvent::CancelAck { .. }) {
        bail!("an amend whose permit lapsed must cancel the order");
    }
    // The order is gone: an amend of it must close the slot, never leave a ghost quote.
    if !matches!(amend("of the cancelled order", at, MakerPermit::unguarded()).await?, ExecEvent::CancelFilledOrExpired { .. }) {
        bail!("an amend of a gone order must close the slot");
    }
    commands.send(ExecCommand::Shutdown).await?;
    let _ = worker.await;

    // A post-only through the ask may be refused outright, or acknowledged and expire at matching.
    let (t, cross_id) = (Instant::now(), format!("XprbX-{}", epoch_tag()));
    match aster.place(&market, Side::Buy, through_ask, lots, &cross_id, false).await {
        ExecEvent::PlaceReject { reason, .. } => println!("POST-ONLY through the ask rejected ({}ms): {reason}", t.elapsed().as_millis()),
        ExecEvent::PlaceAck { .. } => {
            println!("POST-ONLY through the ask acknowledged ({}ms)", t.elapsed().as_millis());
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            let order = aster.query_order(&market, &cross_id).await?;
            println!("  then status={} executedQty={} ({}ms)", order["status"], order["executedQty"], t.elapsed().as_millis());
            if order["status"] != "EXPIRED" {
                bail!("a post-only through the ask did not expire: {order}");
            }
        }
        other => bail!("a post-only through the ask: {other:?}"),
    }

    place_ok(aster, spec, Side::Buy, bid * dec!(0.982)).await?;
    place_ok(aster, spec, Side::Sell, ask * dec!(1.018)).await?;
    let t = Instant::now();
    aster.cancel_all_symbol(&market).await?;
    let left = aster.open_orders(Some(&market)).await?.len();
    println!("CANCEL-ALL ok ({}ms): {left} left", t.elapsed().as_millis());
    if left != 0 {
        bail!("{left} order(s) still resting after cancel-all");
    }

    place_ok(aster, spec, Side::Buy, bid * dec!(0.982)).await?;
    let t = Instant::now();
    aster.refresh_deadman(&market).await?;
    println!("DEADMAN armed ({}ms), countdown {}ms", t.elapsed().as_millis(), cfg.live.aster.deadman_countdown_ms);
    while !aster.open_orders(Some(&market)).await?.is_empty() {
        if t.elapsed().as_millis() as i64 > cfg.live.aster.deadman_countdown_ms + 10_000 {
            bail!("the dead-man did not cancel the resting order");
        }
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }
    println!("DEADMAN fired: the order was gone {}ms after arming", t.elapsed().as_millis());
    Ok(())
}

/// Rest a post-only order at `px`, sized just over the minimum; returns (client id, orderId, lots).
async fn place_ok(aster: &AsterRest, spec: &MarketSpec, side: Side, px: Decimal) -> Result<(String, String, i64)> {
    // Size to clear BOTH the min-notional AND the min-qty/lot-step (the latter binds for
    // high-priced coins like BNB, where min_notional/px underflows one step). Ceil to step.
    let by_notional = crate::decimal::ceil_to_step(spec.aster_min_notional * dec!(1.1) / px, spec.step);
    let qty = spec.aster_min_qty.max(by_notional).max(spec.step);
    let tag = match side { Side::Buy => "B", Side::Sell => "S" };
    let cid = format!("Xprb{tag}-{}-{}", short(&spec.market_id.0), epoch_tag());
    let t0 = Instant::now();
    match aster.place_decimal(&spec.market_id, side, px, qty, &cid, false).await {
        ExecEvent::PlaceAck { venue_order_id, .. } => {
            println!("PLACE  {side:?} {qty} @ ~{px:.4} ok ({}ms): orderId={venue_order_id} cid={cid}", t0.elapsed().as_millis());
            Ok((cid, venue_order_id, MarketScale::from_spec(spec).qty_to_lots(qty)))
        }
        other => bail!("PLACE {side:?} failed: {other:?}"),
    }
}

async fn aster_position(aster: &AsterRest, spec: &MarketSpec) -> Result<Decimal> {
    Ok(aster.position_risk().await?.iter().find(|r| r.symbol == spec.aster_symbol)
        .and_then(|r| r.position_amt.parse().ok()).unwrap_or(Decimal::ZERO))
}

/// Minimum of five warm round trips of `call`: an application baseline including server processing.
async fn min_of_5(call: impl AsyncFn() -> Result<()>) -> Result<u128> {
    call().await?;
    let mut best = u128::MAX;
    for _ in 0..5 {
        let t0 = Instant::now();
        call().await?;
        best = best.min(t0.elapsed().as_millis());
    }
    Ok(best)
}

/// XEMM's user stream (`run_aster_user_stream`) for `spec`, printing each fill it parses with
/// the local time it arrived. The taker's Aster roundtrip uses it too.
pub(crate) fn spawn_fill_printer(aster: AsterRest, spec: &MarketSpec, liveness: Arc<StreamLiveness>,
    stop: CancellationToken) -> tokio::task::JoinHandle<()> {
    let (tx, mut rx) = tokio::sync::mpsc::channel(16);
    let symbols = HashMap::from([(spec.aster_symbol.clone(), spec.market_id.clone())]);
    let stream = tokio::spawn(super::userstream::run_aster_user_stream(aster, symbols, tx, liveness, stop));
    tokio::spawn(async move {
        while let Some(fill) = rx.recv().await {
            println!("USER STREAM fill at {}: {fill:?}", chrono::Utc::now().timestamp_millis());
        }
        let _ = stream.await;
    })
}

/// Two XEMM Aster clients (one for a user stream: `AsterRest` is not `Clone`) and the market spec
/// for `target`, for probes outside XEMM.
pub(crate) async fn xemm_aster(config: &Path, target: &str) -> Result<(AsterRest, AsterRest, MarketSpec)> {
    let cfg = Config::load(config)?;
    let (_m, specs) = resolve(&cfg, target).await?;
    Ok((build_aster(&cfg, &specs)?, build_aster(&cfg, &specs)?, specs[0].clone()))
}

async fn probe_leverage(cfg: &Config, target: &str) -> Result<()> {
    let (_m, specs) = resolve(cfg, target).await?;
    let aster = build_aster(cfg, &specs)?;
    let hl = build_lighter(cfg, &specs).await?;
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

async fn probe_lighter_balance(cfg: &Config, target: &str) -> Result<()> {
    let (_m, specs) = resolve(cfg, target).await?;
    let hl = build_lighter(cfg, &specs).await?;
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
    let hl = build_lighter(cfg, &specs).await?;
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
    let hl = build_lighter(cfg, &specs).await?;
    let market = spec.market_id.clone();
    let mid = hl.mid(&spec.hl_coin).await?;
    let sz = round_up_size(spec.hl_min_notional * dec!(1.02) / mid, spec.lighter_size_decimals);
    let buy_ioc = hl.build_ioc_limit_plan(&market, Side::Buy, through(mid, Side::Buy, dec!(50)), sz, 42_000_001, false)?;
    let sell_ioc = hl.build_ioc_limit_plan(&market, Side::Sell, through(mid, Side::Sell, dec!(50)), sz, 42_000_002, false)?;
    let buy_market = hl.build_market_plan(&market, Side::Buy, through(mid, Side::Buy, dec!(100)), sz, 42_000_003, false)?;
    let sell_market = hl.build_market_plan(&market, Side::Sell, through(mid, Side::Sell, dec!(100)), sz, 42_000_004, false)?;
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

/// Money-risking: XEMM's hedge path on Lighter, or on Hyperliquid for a market hedged there
/// (`--market HYPE-HL`), each step timed through the worker the strategy drives: a hedge buy at
/// the normal slippage, an IOC that cannot fill, and reduce-only closes back to flat. Requires
/// `--i-understand-live` and stays under `--max-usd`.
async fn probe_hedge(cfg: &Config, target: &str, venue: CloseVenue, i_understand_live: bool, max_usd: Decimal) -> Result<()> {
    if !i_understand_live {
        bail!("the {venue:?} hedge probe risks real funds: re-run with --i-understand-live --max-usd <N>");
    }
    if max_usd <= Decimal::ZERO || max_usd > dec!(20) {
        bail!("--max-usd must be in (0, 20] for the probe (got {max_usd})");
    }
    let (_m, specs) = resolve(cfg, target).await?;
    let spec = &specs[0];
    let _leg = crate::controller::lock_leg(venue, &spec.hl_coin)?;
    if venue == CloseVenue::Lighter {
        let hl = build_lighter(cfg, &specs).await?;
        let stop = CancellationToken::new();
        let _stop = stop.clone().drop_guard();
        let _streams = hl.start_private_streams(stop.clone());
        hl.wait_ready(&spec.market_id, std::time::Duration::from_secs(20)).await?;
        println!("PING   min of 5 REST nextNonce: {}ms", min_of_5(async || hl.server_next_nonce().await.map(drop)).await?);
        return hedge_steps(cfg, venue, spec, spec.lighter_size_decimals, max_usd, |rx, tx, journal| run_lighter_worker(rx, tx, hl.clone(), journal),
            &async || lighter_position(&hl, spec).await, &async || hl.mid(&spec.hl_coin).await, &async || Ok(hl.open_orders_info().await?.len())).await;
    }
    anyhow::ensure!(spec.hedge == HedgeVenue::Hyperliquid, "hl-hedge needs a market hedged on Hyperliquid (e.g. --market HYPE-HL), not {}", spec.market_id.0);
    let hl = HyperliquidHedge::new(&cfg.live.hyperliquid.base_url, HyperliquidCreds::from_env()?, &specs).await?;
    let http = reqwest::Client::new();
    let t0 = Instant::now();
    println!("LEVERAGE {}x ({}ms; XEMM's start requires 1)", hl.leverage(&spec.market_id).await?, t0.elapsed().as_millis());
    hedge_steps(cfg, venue, spec, spec.hl_sz_decimals as u32, max_usd, |rx, tx, journal| run_hyperliquid_worker(rx, tx, hl.clone(), journal),
        &async || Ok(position_in(&hl.clearinghouse_state().await?, &spec.hl_coin)),
        &async || fetch_hedge_book(&http, &cfg.live, spec, 1).await?.mid().context("Hyperliquid book without a mid"),
        &async || Ok(hl.open_orders_info().await?.len())).await
}

/// [`probe_hedge`]'s orders, from flat: sized above the venue minimum, with cleanup through
/// [`close_on_worker`] after the steps succeed or fail.
#[allow(clippy::too_many_arguments)]
async fn hedge_steps<W: std::future::Future<Output = ()> + Send + 'static>(cfg: &Config, venue: CloseVenue, spec: &MarketSpec,
    size_decimals: u32, max_usd: Decimal,
    worker: impl Fn(tokio::sync::mpsc::Receiver<HedgeCommand>, tokio::sync::mpsc::Sender<ExecEvent>, Journal) -> W,
    position: &impl AsyncFn() -> Result<Decimal>, mid: &impl AsyncFn() -> Result<Decimal>, open_orders: &impl AsyncFn() -> Result<usize>) -> Result<()> {
    let px = mid().await?;
    // Just over the venue minimum, rounded UP so the order builder's floor keeps it.
    let qty = round_up_size(spec.hl_min_notional * dec!(1.02) / px, size_decimals);
    let open = qty + spec.hl_qty_step * dec!(2);
    if open * px > max_usd {
        bail!("the probe's {venue:?} order ~${:.2} exceeds --max-usd {max_usd}; raise the cap", open * px);
    }
    if position().await? != Decimal::ZERO || open_orders().await? != 0 {
        bail!("refusing: {venue:?} {} must start flat with no open orders", spec.hl_coin);
    }
    let (cmd, commands) = tokio::sync::mpsc::channel(8);
    let (events_tx, mut events) = tokio::sync::mpsc::channel(64);
    let (journal, _journal_rx) = Journal::channel();
    let task = tokio::spawn(worker(commands, events_tx, journal));
    let slippage = &cfg.live.lighter;
    let opened = async {
        hedge(&cmd, &mut events, spec, Side::Buy, open, through(px, Side::Buy, slippage.normal_slippage_bps), false).await?;
        hedge(&cmd, &mut events, spec, Side::Buy, qty, px * dec!(0.99), false).await
    }.await;
    let _ = cmd.send(HedgeCommand::Shutdown).await;
    let _ = task.await;
    if let Err(error) = &opened {
        println!("FAILED {error:#}; flattening from the venue's position");
    }
    let closed = close_on_worker(venue, spec, slippage.emergency_slippage_bps, &worker, position, mid).await;
    anyhow::ensure!(open_orders().await? == 0, "{venue:?} {} ends with open orders; manual check required", spec.hl_coin);
    opened.and(closed)
}

/// A venue [`close`] flattens.
#[derive(Clone, Copy, Debug, PartialEq, Eq, clap::ValueEnum)]
pub enum CloseVenue {
    Aster,
    Lighter,
    Hyperliquid,
}

/// REAL orders, the operator's exit: flattens one coin at market, reduce-only, on Aster, Lighter
/// and Hyperliquid at once (or the `venues` given): Aster's open orders cancelled, then XEMM's
/// MARKET close; on the hedge venues, XEMM's hedge worker at the emergency slippage. Holds each
/// venue's leg lock, so it refuses while a live writer trades there. Ponytail: each venue's spec
/// read also needs Aster's and a hedge venue's metadata (`build_market_specs`), so one venue's
/// outage blocks another's close; split the spec read if that ever bites.
pub async fn close(cfg: &Config, target: &str, venues: &[CloseVenue], i_understand_live: bool) -> Result<()> {
    if !i_understand_live {
        bail!("close sends REAL orders: re-run with --i-understand-live");
    }
    let market = cfg.markets.iter().find(|m| m.id().0.eq_ignore_ascii_case(target) || m.hl_coin.eq_ignore_ascii_case(target))
        .with_context(|| format!("no market or coin '{target}' in config [[markets]]"))?;
    let on = |venue| venues.is_empty() || venues.contains(&venue);
    let _locks = [(CloseVenue::Aster, &market.aster_symbol), (CloseVenue::Lighter, &market.hl_coin), (CloseVenue::Hyperliquid, &market.hl_coin)]
        .into_iter().filter(|&(venue, _)| on(venue))
        .map(|(venue, symbol)| crate::controller::lock_leg(venue, symbol))
        .collect::<Result<Vec<_>>>()?;
    let hedged = |hedge_venue| MarketCfg { hedge_venue, ..market.clone() };
    let slippage_bps = cfg.live.lighter.emergency_slippage_bps;
    // Initialize all selected clients before sending orders; later reads or writes can still fail.
    let stop = CancellationToken::new();
    let _stop = stop.clone().drop_guard();
    let (aster, lighter, hyperliquid) = tokio::try_join!(
        async {
            if !on(CloseVenue::Aster) {
                return anyhow::Ok(None);
            }
            let specs = rest_specs::build_market_specs(std::slice::from_ref(market), &cfg.live).await.context("Aster")?;
            let aster = build_aster(cfg, &specs).context("Aster")?;
            Ok(Some((specs, aster)))
        },
        async {
            if !on(CloseVenue::Lighter) {
                return anyhow::Ok(None);
            }
            let specs = rest_specs::build_market_specs(&[hedged(HedgeVenue::Lighter)], &cfg.live).await.context("Lighter")?;
            let lighter = build_lighter(cfg, &specs).await.context("Lighter")?;
            let _streams = lighter.start_private_streams(stop.clone());
            lighter.wait_ready(&specs[0].market_id, std::time::Duration::from_secs(20)).await.context("Lighter")?;
            Ok(Some((specs, lighter)))
        },
        async {
            if !on(CloseVenue::Hyperliquid) {
                return anyhow::Ok(None);
            }
            let specs = rest_specs::build_market_specs(&[hedged(HedgeVenue::Hyperliquid)], &cfg.live).await.context("Hyperliquid")?;
            let hl = HyperliquidHedge::new(&cfg.live.hyperliquid.base_url, HyperliquidCreds::from_env()?, &specs).await.context("Hyperliquid")?;
            Ok(Some((specs, hl)))
        },
    ).context("nothing sent: pass --venue to close only the venues that are up")?;
    let http = reqwest::Client::new();
    let (aster, lighter, hyperliquid) = tokio::join!(
        async { let Some((specs, aster)) = &aster else { return Ok(()) }; close_aster(aster, &specs[0]).await },
        async {
            let Some((specs, lighter)) = &lighter else { return Ok(()) };
            close_on_worker(CloseVenue::Lighter, &specs[0], slippage_bps, |rx, tx, journal| run_lighter_worker(rx, tx, lighter.clone(), journal),
                &async || lighter_position(lighter, &specs[0]).await, &async || lighter.mid(&specs[0].hl_coin).await).await
        },
        async {
            let Some((specs, hl)) = &hyperliquid else { return Ok(()) };
            close_on_worker(CloseVenue::Hyperliquid, &specs[0], slippage_bps, |rx, tx, journal| run_hyperliquid_worker(rx, tx, hl.clone(), journal),
                &async || Ok(position_in(&hl.clearinghouse_state().await?, &specs[0].hl_coin)),
                &async || fetch_hedge_book(&http, &cfg.live, &specs[0], 1).await?.mid().context("Hyperliquid book without a mid")).await
        },
    );
    let mut failed = Vec::new();
    for (venue, result) in [(CloseVenue::Aster, aster), (CloseVenue::Lighter, lighter), (CloseVenue::Hyperliquid, hyperliquid)] {
        if let Err(error) = result {
            println!("FAILED {venue:?}: {error:#}");
            failed.push(venue);
        }
    }
    anyhow::ensure!(failed.is_empty(), "{failed:?} not closed: check by hand");
    Ok(())
}

/// Aster's part of [`close`]: its open orders cancelled, then XEMM's reduce-only MARKET until flat.
async fn close_aster(aster: &AsterRest, spec: &MarketSpec) -> Result<()> {
    aster.cancel_all_symbol(&spec.market_id).await.context("cancelling the symbol's orders")?;
    flatten(CloseVenue::Aster, &async || aster_position(aster, spec).await, &mut async |side: Side, qty: Decimal| {
        let t0 = Instant::now();
        let body = aster.flatten_result(&spec.market_id, side, qty, &format!("Xcls-{}", epoch_tag())).await?;
        let filled = order_progress(&body).map_or(Decimal::ZERO, |progress| progress.0);
        println!("  reduce-only MARKET {side:?} {qty}: filled {filled} ({}ms)", t0.elapsed().as_millis());
        Ok(filled)
    }).await
}

/// [`flatten`] through a hedge worker, as XEMM's reduce-only correction sends it: each IOC priced
/// `slippage_bps` through the venue's mid.
async fn close_on_worker<W: std::future::Future<Output = ()> + Send + 'static>(venue: CloseVenue, spec: &MarketSpec, slippage_bps: Decimal,
    worker: impl FnOnce(tokio::sync::mpsc::Receiver<HedgeCommand>, tokio::sync::mpsc::Sender<ExecEvent>, Journal) -> W,
    position: &impl AsyncFn() -> Result<Decimal>, mid: &impl AsyncFn() -> Result<Decimal>) -> Result<()> {
    let (cmd, commands) = tokio::sync::mpsc::channel(8);
    let (events_tx, mut events) = tokio::sync::mpsc::channel(64);
    let (journal, _journal_rx) = Journal::channel();
    let worker = tokio::spawn(worker(commands, events_tx, journal));
    let closed = flatten(venue, position, &mut async |side: Side, qty: Decimal| {
        hedge(&cmd, &mut events, spec, side, qty, through(mid().await?, side, slippage_bps), true).await
    }).await;
    let _ = cmd.send(HedgeCommand::Shutdown).await;
    let _ = worker.await;
    closed
}

/// Closes a position with up to three reduce-only orders, each sized from the fills so far (from a
/// fresh read after one that filled nothing: a fill can be reported late), then requires the
/// venue's position flat once settled.
async fn flatten(venue: CloseVenue, position: &impl AsyncFn() -> Result<Decimal>,
    send: &mut impl AsyncFnMut(Side, Decimal) -> Result<Decimal>) -> Result<()> {
    let mut left = position().await?;
    println!("{venue:?} position {left}");
    for _ in 0..3 {
        if left.is_zero() {
            break;
        }
        let side = if left > Decimal::ZERO { Side::Sell } else { Side::Buy };
        let filled = send(side, left.abs()).await.unwrap_or_else(|error| {
            println!("{venue:?} {side:?} {} failed: {error:#}", left.abs());
            Decimal::ZERO
        });
        left = if filled.is_zero() { position().await? } else if side == Side::Sell { left - filled } else { left + filled };
    }
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    let left = loop {
        let left = position().await?;
        if left.is_zero() || tokio::time::Instant::now() >= deadline {
            break left;
        }
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    };
    println!("{venue:?} position now {left}");
    anyhow::ensure!(left.is_zero(), "{venue:?} still holds {left}");
    Ok(())
}

/// One hedge through the worker, as the strategy sends it; waits for its terminal event, prints
/// each event's time since the command was queued, and returns the filled quantity.
async fn hedge(cmd: &tokio::sync::mpsc::Sender<HedgeCommand>, events: &mut tokio::sync::mpsc::Receiver<ExecEvent>,
    spec: &MarketSpec, side: Side, qty: Decimal, px: Decimal, reduce_only: bool) -> Result<Decimal> {
    let now = crate::hotpath::clock::mono_now_ns();
    let cloid = Cloid::hedge("probe", &spec.market_id.0, now);
    let mut intent = HedgeIntent::with_qty(cloid, spec.market_id.clone(), side, qty, px, now);
    if reduce_only {
        intent.purpose = IntentPurpose::ReduceDelta;
    }
    println!("HEDGE  {side:?} {qty} @ {px:.4} reduce_only={reduce_only}");
    let t0 = Instant::now();
    cmd.send(HedgeCommand::Hedge { intent, aggressive_px: px }).await?;
    loop {
        let event = tokio::time::timeout(std::time::Duration::from_secs(30), events.recv()).await
            .context("no terminal hedge event within 30 s")?.context("hedge worker stopped")?;
        let ms = t0.elapsed().as_millis();
        // A late answer to an earlier order (one that timed out) is not this order's.
        if let ExecEvent::AttemptStarted { cloid: of, .. } | ExecEvent::ExecutionProgress { cloid: of, .. }
            | ExecEvent::HedgeReject { cloid: of, .. } | ExecEvent::HedgeUnknown { cloid: of, .. } = &event {
            if *of != cloid {
                println!("  ignoring a late event of an earlier order ({ms}ms): {event:?}");
                continue;
            }
        }
        match event {
            ExecEvent::AttemptStarted { .. } => println!("  sent, the venue answered ({ms}ms)"),
            ExecEvent::ExecutionProgress { cumulative_qty, cumulative_quote_usd, cumulative_fee_usd, terminal, .. } => {
                println!("  filled {cumulative_qty} quote={cumulative_quote_usd:?} fee={cumulative_fee_usd:?} terminal={terminal} ({ms}ms)");
                if terminal {
                    return Ok(cumulative_qty);
                }
            }
            ExecEvent::HedgeReject { reason, .. } => {
                println!("  rejected ({ms}ms, retryable no-fill: {}): {reason}", hedge_reject_is_definitive_no_fill(&reason));
                return Ok(Decimal::ZERO);
            }
            // The worker resolves it (Hyperliquid: `orderStatus`, then the fills) and reports the end.
            ExecEvent::HedgeUnknown { reason, .. } => println!("  unknown ({ms}ms), resolving: {reason}"),
            other => bail!("unexpected hedge outcome after {ms}ms: {other:?}"),
        }
    }
}

/// Lighter's position from data that originated after this call: the positions feed can trail a
/// fill (the reconciler skips such reads by the same stamp). A cache older than
/// `ws_account_max_age_ms` (1.5 s) gives way to REST, which bounds the wait.
async fn lighter_position(hl: &LighterExchange, spec: &MarketSpec) -> Result<Decimal> {
    let asked = crate::hotpath::clock::mono_now_ns();
    loop {
        let st = hl.clearinghouse_state().await?;
        if st.data_origin_ns >= asked {
            return Ok(position_in(&st, &spec.hl_coin));
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}

/// A hedge account's signed position in `coin`.
fn position_in(st: &HlClearinghouse, coin: &str) -> Decimal {
    st.asset_positions.iter().find(|p| p.position.coin == coin).and_then(|p| p.position.szi.parse().ok()).unwrap_or(Decimal::ZERO)
}

fn round_up_size(qty: Decimal, size_decimals: u32) -> Decimal {
    qty.round_dp_with_strategy(size_decimals, rust_decimal::RoundingStrategy::ToPositiveInfinity)
}

/// `px` moved `bps` the way `side` pays: up for a buy, down for a sell.
fn through(px: Decimal, side: Side, bps: Decimal) -> Decimal {
    let slip = bps / dec!(10000);
    px * if side == Side::Buy { Decimal::ONE + slip } else { Decimal::ONE - slip }
}

fn short(s: &str) -> String {
    s.chars().filter(|c| c.is_ascii_alphanumeric()).take(5).collect()
}

fn epoch_tag() -> String {
    let n = chrono::Utc::now().timestamp_millis() as u64 % 10_000_000;
    n.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    /// A hedge worker that answers each order with the next of `fills`, (reported, executed) pairs:
    /// a late fill executes without being reported. Past the list, it fills the whole order.
    fn worker(position: Arc<Mutex<Decimal>>, orders: Arc<Mutex<Vec<(Side, Decimal, Decimal)>>>, fills: Vec<(Decimal, Decimal)>)
        -> impl FnOnce(tokio::sync::mpsc::Receiver<HedgeCommand>, tokio::sync::mpsc::Sender<ExecEvent>, Journal) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> {
        move |mut rx: tokio::sync::mpsc::Receiver<HedgeCommand>, tx: tokio::sync::mpsc::Sender<ExecEvent>, _journal: Journal| Box::pin(async move {
            let mut fills = fills.into_iter();
            while let Some(HedgeCommand::Hedge { intent, aggressive_px }) = rx.recv().await {
                assert_eq!(intent.purpose, IntentPurpose::ReduceDelta, "a close is reduce-only");
                orders.lock().unwrap().push((intent.hedge_side, intent.qty, aggressive_px));
                let (reported, executed) = fills.next().unwrap_or((intent.qty, intent.qty));
                *position.lock().unwrap() -= if intent.hedge_side == Side::Sell { executed } else { -executed };
                let _ = tx.send(ExecEvent::ExecutionProgress { cloid: intent.cloid, cumulative_qty: reported, cumulative_quote_usd: None,
                    cumulative_fee_usd: None, terminal: true, venue_order_id: None, event_time_ms: None }).await;
            }
        })
    }

    async fn close_from(start: Decimal, fills: Vec<(Decimal, Decimal)>) -> (Result<()>, Vec<(Side, Decimal, Decimal)>, Decimal) {
        let (position, orders) = (Arc::new(Mutex::new(start)), Arc::new(Mutex::new(Vec::new())));
        let closed = close_on_worker(CloseVenue::Hyperliquid, &crate::livebot::scale::tests::spec(), dec!(50),
            worker(position.clone(), orders.clone(), fills),
            &async || Ok(*position.lock().unwrap()), &async || Ok(dec!(100))).await;
        let (orders, left) = (orders.lock().unwrap().clone(), *position.lock().unwrap());
        (closed, orders, left)
    }

    #[tokio::test(start_paused = true)]
    async fn close_sizes_each_order_from_the_fills_and_ends_flat() {
        let (closed, orders, left) = close_from(dec!(0.14), vec![(dec!(0.1), dec!(0.1))]).await;
        closed.unwrap();
        assert_eq!(orders, [(Side::Sell, dec!(0.14), dec!(99.5)), (Side::Sell, dec!(0.04), dec!(99.5))]);
        assert_eq!(left, Decimal::ZERO);
    }

    #[tokio::test(start_paused = true)]
    async fn close_rereads_the_venue_after_an_order_that_filled_nothing() {
        // The first IOC reports no fill while the venue shows 0.04 bought back.
        let (closed, orders, left) = close_from(dec!(-0.14), vec![(Decimal::ZERO, dec!(0.04))]).await;
        closed.unwrap();
        assert_eq!(orders, [(Side::Buy, dec!(0.14), dec!(100.5)), (Side::Buy, dec!(0.1), dec!(100.5))]);
        assert_eq!(left, Decimal::ZERO);
    }

    #[tokio::test(start_paused = true)]
    async fn close_fails_loudly_when_the_venue_never_fills() {
        let (closed, orders, left) = close_from(dec!(0.14), vec![(Decimal::ZERO, Decimal::ZERO); 3]).await;
        assert!(closed.unwrap_err().to_string().contains("still holds 0.14"));
        assert_eq!(orders.len(), 3);
        assert_eq!(left, dec!(0.14));
    }

    #[tokio::test]
    async fn a_late_answer_to_an_earlier_order_is_not_this_orders_fill() {
        let (cmd, mut commands) = tokio::sync::mpsc::channel(8);
        let (tx, mut events) = tokio::sync::mpsc::channel(8);
        let progress = |cloid, qty| ExecEvent::ExecutionProgress { cloid, cumulative_qty: qty, cumulative_quote_usd: None,
            cumulative_fee_usd: None, terminal: true, venue_order_id: None, event_time_ms: None };
        // The earlier order timed out; its terminal answer lands first.
        tx.send(progress(Cloid::hedge("probe", "BTC", 1), dec!(1))).await.unwrap();
        tokio::spawn(async move {
            let Some(HedgeCommand::Hedge { intent, .. }) = commands.recv().await else { return };
            tx.send(progress(intent.cloid, dec!(0.1))).await.unwrap();
        });
        let spec = crate::livebot::scale::tests::spec();
        assert_eq!(hedge(&cmd, &mut events, &spec, Side::Sell, dec!(0.1), dec!(100), true).await.unwrap(), dec!(0.1));
    }

    #[test]
    fn close_takes_the_same_leg_locks_as_the_engines() {
        use crate::config::FirstVenue;
        use crate::controller::leg;
        assert_eq!([CloseVenue::Aster, CloseVenue::Lighter, CloseVenue::Hyperliquid].map(|venue| leg(venue, "hype")),
            [leg(FirstVenue::Aster, "hype"), leg(HedgeVenue::Lighter, "hype"), leg(HedgeVenue::Hyperliquid, "hype")]);
    }
}
