//! Live primitive probes. `lighter_aster_bot probe <check>` exercises XEMM's own venue calls
//! with the REAL signers, printing each call's latency and the resulting state, and cleans up
//! any order it opens. Signed reads and `lighter-order-dry-run` (local signing only) send no
//! order. `aster-place-cancel` and `lighter-market` send REAL orders and need
//! `--i-understand-live`; `lighter-market` also takes a `--max-usd` cap it refuses to exceed.

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

use super::exec::aster::{run_aster_worker, AsterRest};
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
        "lighter-market" => probe_lighter_market(cfg, &target, i_understand_live, max_usd).await,
        "hl-hedge" => probe_hl_hedge(cfg, &target, i_understand_live, max_usd).await,
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
/// ones; money is at risk only if Aster lets an amend cross.
async fn probe_aster_place_cancel(cfg: &Config, target: &str, i_understand_live: bool) -> Result<()> {
    if !i_understand_live {
        bail!("aster-place-cancel amends an order across the book: re-run with --i-understand-live");
    }
    let (_m, specs) = resolve(cfg, target).await?;
    let spec = &specs[0];
    let aster = build_aster(cfg, &specs)?;
    let market = spec.market_id.clone();
    if aster_position(&aster, spec).await? != Decimal::ZERO || !aster.open_orders(Some(&market)).await?.is_empty() {
        bail!("refusing: {} must start flat with no open orders", spec.aster_symbol);
    }
    println!("PING   min of 5 GET /fapi/v1/time: {}ms", min_rtt_ms(&format!("{}/fapi/v1/time", cfg.live.aster.base_url)).await?);
    let stop = CancellationToken::new();
    let liveness = Arc::new(StreamLiveness::default());
    let stream = spawn_fill_printer(build_aster(cfg, &specs)?, spec, liveness.clone(), stop.clone());
    let steps = aster_steps(cfg, &aster, spec).await;
    // Whatever failed above, nothing may stay resting or open: XEMM's reduce-only close.
    aster.cancel_all_symbol(&market).await?;
    let mut position = aster_position(&aster, spec).await?;
    if position != Decimal::ZERO {
        let side = if position > Decimal::ZERO { Side::Sell } else { Side::Buy };
        println!("FLATTEN {position}: {}", aster.flatten_result(&market, side, position.abs(), &format!("Xprb-flat-{}", epoch_tag())).await?);
        position = aster_position(&aster, spec).await?;
    }
    println!("USER STREAM last message {}ms ago", liveness.age_ms(crate::hotpath::clock::mono_now_ns()));
    stop.cancel();
    let _ = stream.await;
    if position != Decimal::ZERO || !aster.open_orders(Some(&market)).await?.is_empty() {
        bail!("{} ends with position {position} or open orders; manual check required", spec.aster_symbol);
    }
    steps
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

/// Minimum of five unsigned GETs on one warm connection: the network share of each latency.
async fn min_rtt_ms(url: &str) -> Result<u128> {
    let client = reqwest::Client::new();
    client.get(url).send().await?.bytes().await?;
    let mut best = u128::MAX;
    for _ in 0..5 {
        let t0 = Instant::now();
        client.get(url).send().await?.bytes().await?;
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

/// XEMM's Aster client and market spec for `target`, for probes outside XEMM.
pub(crate) async fn xemm_aster(config: &Path, target: &str) -> Result<(AsterRest, MarketSpec)> {
    let cfg = Config::load(config)?;
    let (_m, specs) = resolve(&cfg, target).await?;
    Ok((build_aster(&cfg, &specs)?, specs[0].clone()))
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

/// Money-risking: XEMM's own Lighter hedge path, each step timed. Drives `run_lighter_worker`
/// over the private streams as the strategy does: a hedge buy at the normal slippage, an IOC that
/// cannot fill (does its reject consume the nonce?), the venue's minimum for reduce-only orders
/// ([`minimum_rules`]), and a reduce-only correction back to flat. Requires `--i-understand-live`
/// and stays under `--max-usd`.
async fn probe_lighter_market(cfg: &Config, target: &str, i_understand_live: bool, max_usd: Decimal) -> Result<()> {
    if !i_understand_live {
        bail!("lighter-market risks real funds: re-run with --i-understand-live --max-usd <N>");
    }
    if max_usd <= Decimal::ZERO || max_usd > dec!(20) {
        bail!("--max-usd must be in (0, 20] for the probe (got {max_usd})");
    }
    let (_m, specs) = resolve(cfg, target).await?;
    let spec = &specs[0];
    let hl = build_lighter(cfg, &specs).await?;
    let stop = CancellationToken::new();
    let streams = hl.start_private_streams(stop.clone());
    hl.wait_ready(&spec.market_id, std::time::Duration::from_secs(20)).await?;
    let mid = hl.mid(&spec.hl_coin).await?;
    // Size to clear the Lighter min notional with 2% to spare, rounded UP to the size decimals so
    // the order builder's floor keeps it, and stay under the cap.
    let qty = round_up_size(spec.hl_min_notional * dec!(1.02) / mid, spec.lighter_size_decimals);
    let open = qty + spec.hl_qty_step * dec!(2);
    if open * mid > max_usd {
        bail!("the probe's Lighter order ~${:.2} exceeds --max-usd {max_usd}; raise the cap", open * mid);
    }
    if lighter_position(&hl, spec).await? != Decimal::ZERO || !hl.open_orders_info().await?.is_empty() {
        bail!("refusing: Lighter {} must start flat with no open orders", spec.hl_coin);
    }
    let mut ping = u128::MAX;
    for _ in 0..5 {
        let t0 = Instant::now();
        hl.server_next_nonce().await?;
        ping = ping.min(t0.elapsed().as_millis());
    }
    println!("PING   min of 5 REST nextNonce: {ping}ms");

    let (cmd, commands) = tokio::sync::mpsc::channel(8);
    let (events_tx, mut events) = tokio::sync::mpsc::channel(64);
    let (journal, _journal_rx) = Journal::channel();
    let worker = tokio::spawn(run_lighter_worker(commands, events_tx, hl.clone(), journal));
    let bps = |b: Decimal| b / dec!(10000);
    let lighter = &cfg.live.lighter;
    let opened = async {
        let bought = hedge(&cmd, &mut events, spec, Side::Buy, open, mid * (Decimal::ONE + bps(lighter.normal_slippage_bps)), false).await?;
        let before = hl.server_next_nonce().await?;
        let missed = hedge(&cmd, &mut events, spec, Side::Buy, qty, mid * dec!(0.99), false).await?;
        let after = hl.server_next_nonce().await?;
        println!("NONCE  venue nextNonce {before} -> {after} across the IOC that could not fill");
        minimum_rules(&cmd, &mut events, spec, bought + missed, qty, lighter.emergency_slippage_bps, &async || hl.mid(&spec.hl_coin).await).await
    }.await;
    let mut position = match &opened {
        Ok(position) => *position,
        Err(error) => {
            println!("FAILED {error:#}; flattening from the venue's position");
            lighter_position(&hl, spec).await?
        }
    };
    // Back to flat with XEMM's reduce-only correction: sized from the fills, then from the venue.
    for attempt in 0..3 {
        if attempt > 0 {
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            position = lighter_position(&hl, spec).await?;
        }
        if position == Decimal::ZERO {
            break;
        }
        let side = if position > Decimal::ZERO { Side::Sell } else { Side::Buy };
        let px = hl.mid(&spec.hl_coin).await? * match side {
            Side::Sell => Decimal::ONE - bps(lighter.emergency_slippage_bps),
            Side::Buy => Decimal::ONE + bps(lighter.emergency_slippage_bps),
        };
        match hedge(&cmd, &mut events, spec, side, position.abs(), px, true).await {
            Ok(sold) => position -= if side == Side::Sell { sold } else { -sold },
            Err(error) => println!("FAILED reduce-only close: {error:#}"),
        }
    }
    let _ = cmd.send(HedgeCommand::Shutdown).await;
    let _ = worker.await;
    stop.cancel();
    for stream in streams {
        let _ = stream.await;
    }
    let position = lighter_position(&hl, spec).await?;
    println!("FINAL position: {position} {}", spec.hl_coin);
    if position != Decimal::ZERO || !hl.open_orders_info().await?.is_empty() {
        bail!("{} ends with position {position} or open orders; manual check required", spec.hl_coin);
    }
    opened.map(|_| ())
}

/// Money-risking: XEMM's Hyperliquid hedge path, each step timed. Drives `run_hyperliquid_worker`
/// as the strategy does: the leverage gate's read, a hedge buy at the normal slippage, an IOC that
/// cannot fill, the venue's minimum for reduce-only orders ([`minimum_rules`]), and reduce-only
/// corrections back to flat. Needs a market hedged on Hyperliquid (`--market HYPE-HL`),
/// `--i-understand-live`, and stays under `--max-usd`.
async fn probe_hl_hedge(cfg: &Config, target: &str, i_understand_live: bool, max_usd: Decimal) -> Result<()> {
    if !i_understand_live {
        bail!("hl-hedge risks real funds: re-run with --i-understand-live --max-usd <N>");
    }
    if max_usd <= Decimal::ZERO || max_usd > dec!(20) {
        bail!("--max-usd must be in (0, 20] for the probe (got {max_usd})");
    }
    let (_m, specs) = resolve(cfg, target).await?;
    let spec = &specs[0];
    if spec.hedge != HedgeVenue::Hyperliquid {
        bail!("hl-hedge needs a market hedged on Hyperliquid (e.g. --market HYPE-HL), not {}", spec.market_id.0);
    }
    let hl = HyperliquidHedge::new(&cfg.live.hyperliquid.base_url, HyperliquidCreds::from_env()?, &specs).await?;
    let http = reqwest::Client::new();
    let mid = async || fetch_hedge_book(&http, &cfg.live, spec, 1).await?.mid().context("Hyperliquid book without a mid");
    let t0 = Instant::now();
    println!("LEVERAGE {}x ({}ms; XEMM's start requires 1)", hl.leverage(&spec.market_id).await?, t0.elapsed().as_millis());
    if position_in(&hl.clearinghouse_state().await?, &spec.hl_coin) != Decimal::ZERO || !hl.open_orders_info().await?.is_empty() {
        bail!("refusing: Hyperliquid {} must start flat with no open orders", spec.hl_coin);
    }
    let px = mid().await?;
    let decimals = spec.hl_sz_decimals as u32;
    let qty = round_up_size(spec.hl_min_notional * dec!(1.02) / px, decimals);
    let open = qty + spec.hl_qty_step * dec!(2);
    if open * px > max_usd {
        bail!("the probe's Hyperliquid order ~${:.2} exceeds --max-usd {max_usd}; raise the cap", open * px);
    }

    let (cmd, commands) = tokio::sync::mpsc::channel(8);
    let (events_tx, mut events) = tokio::sync::mpsc::channel(64);
    let (journal, _journal_rx) = Journal::channel();
    let worker = tokio::spawn(run_hyperliquid_worker(commands, events_tx, hl.clone(), journal));
    let bps = |b: Decimal| b / dec!(10000);
    let slippage = &cfg.live.lighter;
    let opened = async {
        let bought = hedge(&cmd, &mut events, spec, Side::Buy, open, px * (Decimal::ONE + bps(slippage.normal_slippage_bps)), false).await?;
        let missed = hedge(&cmd, &mut events, spec, Side::Buy, qty, px * dec!(0.99), false).await?;
        minimum_rules(&cmd, &mut events, spec, bought + missed, qty, slippage.emergency_slippage_bps, &mid).await
    }.await;
    let mut position = match &opened {
        Ok(position) => *position,
        Err(error) => {
            println!("FAILED {error:#}; flattening from the venue's position");
            position_in(&hl.clearinghouse_state().await?, &spec.hl_coin)
        }
    };
    // Back to flat with XEMM's reduce-only correction: sized from the fills, then from the venue.
    for attempt in 0..3 {
        if attempt > 0 {
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            position = position_in(&hl.clearinghouse_state().await?, &spec.hl_coin);
        }
        if position == Decimal::ZERO {
            break;
        }
        let side = if position > Decimal::ZERO { Side::Sell } else { Side::Buy };
        let px = mid().await? * match side {
            Side::Sell => Decimal::ONE - bps(slippage.emergency_slippage_bps),
            Side::Buy => Decimal::ONE + bps(slippage.emergency_slippage_bps),
        };
        match hedge(&cmd, &mut events, spec, side, position.abs(), px, true).await {
            Ok(sold) => position -= if side == Side::Sell { sold } else { -sold },
            Err(error) => println!("FAILED reduce-only close: {error:#}"),
        }
    }
    let _ = cmd.send(HedgeCommand::Shutdown).await;
    let _ = worker.await;
    let position = position_in(&hl.clearinghouse_state().await?, &spec.hl_coin);
    println!("FINAL position: {position} {}", spec.hl_coin);
    if position != Decimal::ZERO || !hl.open_orders_info().await?.is_empty() {
        bail!("{} ends with position {position} or open orders; manual check required", spec.hl_coin);
    }
    opened.map(|_| ())
}

/// Measures, from a long position of `min_qty` (the venue's minimum) plus two steps, whether the
/// hedge venue takes a reduce-only order under its minimum, as XEMM's correction of a small residual
/// sends one: a reduce-only step (a partial reduce), a plain sell down to one step, a reduce-only
/// close of that step (a full close) and, if that is refused, a plain buy back over the minimum.
/// Returns the position the fills leave, for the caller's close.
async fn minimum_rules(cmd: &tokio::sync::mpsc::Sender<HedgeCommand>, events: &mut tokio::sync::mpsc::Receiver<ExecEvent>,
    spec: &MarketSpec, mut position: Decimal, min_qty: Decimal, slippage_bps: Decimal,
    mid: &impl AsyncFn() -> Result<Decimal>) -> Result<Decimal> {
    let step = spec.hl_qty_step;
    if position < min_qty + step * dec!(2) {
        println!("RULES  skipped: position {position} is under the minimum plus two steps");
        return Ok(position);
    }
    let px = async |side: Side| Ok::<_, anyhow::Error>(mid().await? * match side {
        Side::Sell => Decimal::ONE - slippage_bps / dec!(10000),
        Side::Buy => Decimal::ONE + slippage_bps / dec!(10000),
    });
    let verdict = |filled: Decimal| if filled > Decimal::ZERO { "accepted" } else { "refused" };
    let sold = hedge(cmd, events, spec, Side::Sell, step, px(Side::Sell).await?, true).await?;
    println!("RULE   reduce-only partial close under the minimum: {}", verdict(sold));
    position -= sold;
    position -= hedge(cmd, events, spec, Side::Sell, position - step, px(Side::Sell).await?, false).await?;
    if position == step {
        let sold = hedge(cmd, events, spec, Side::Sell, step, px(Side::Sell).await?, true).await?;
        println!("RULE   reduce-only full close under the minimum: {}", verdict(sold));
        position -= sold;
        if position > Decimal::ZERO {
            position += hedge(cmd, events, spec, Side::Buy, min_qty, px(Side::Buy).await?, false).await?;
        }
    }
    Ok(position)
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
