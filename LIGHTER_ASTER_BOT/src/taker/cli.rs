use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand};
use rust_decimal::Decimal;

use crate::taker::aster::creds::{AsterCreds, LighterCreds};
use crate::taker::aster::rest::AsterRest;
use crate::taker::aster::sign::{AsterSigner, EvmAsterSigner};
use crate::taker::config::Config;
use crate::taker::connectors::rest_book;
use crate::taker::connectors::rest_specs;
use crate::decimal::bps_to_rate;
use crate::taker::decimal::{ceil_to_step, floor_to_step};
use crate::taker::types::Side;
use crate::taker::venues::lighter::LighterVenue;
use crate::taker::markets::MarketSpec;
use crate::taker::pnl::ActiveSession;
use std::time::Duration;

#[derive(Parser, Debug)]
#[command(
    name = "lighter_aster_bot taker",
    bin_name = "lighter_aster_bot taker",
    version,
    about = "Lighter/Aster taker-taker arbitrage engine"
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,

    /// Path to the TOML config file (bot.toml; the taker reads its [taker] table).
    #[arg(long, global = true, default_value = "bot.toml")]
    pub config: PathBuf,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Scan both books and trade taker-taker clips. Submits REAL orders unless `--observe-only`.
    Run {
        #[arg(long, default_value = "HYPE")]
        markets: Option<String>,
        #[arg(long)]
        secs: Option<u64>,
        #[arg(long)]
        max_trades: Option<u64>,
        #[arg(long)]
        min_size: bool,
        /// Scan and persist opportunity history without submitting orders.
        #[arg(long)]
        observe_only: bool,
        /// Restrict executable opportunities by exposure effect.
        #[arg(long, value_enum, default_value_t = crate::taker::arb::ExposureFilter::Any)]
        exposure_filter: crate::taker::arb::ExposureFilter,
        /// Cooldown after reduce-filtered trades.
        #[arg(long, default_value_t = 5_000)]
        reduce_cooldown_ms: u64,
    },
    /// Fetch and print resolved market specs (Aster exchangeInfo + Lighter orderBooks).
    FetchSpecs {
        #[arg(long)]
        markets: Option<String>,
    },
    /// Signed read-only check: positions, available USDC and open orders on both venues.
    Probe {
        #[arg(long, default_value = "HYPE")]
        market: Option<String>,
    },
    /// Live Aster roundtrip on the real order paths, each step timed.
    ///
    /// Requires a flat Aster starting position. An IOC under the bid must end unfilled; then the
    /// taker's entry IOC buys up to `--max-usd`, XEMM's user stream reports the fill, XEMM's
    /// reduce-only MARKET flatten sells it back, and cleanup verifies the position flat.
    AsterMarketRoundtrip {
        #[arg(long, default_value = "HYPE")]
        market: Option<String>,
        #[arg(long)]
        i_understand_live: bool,
        #[arg(long)]
        max_usd: Decimal,
    },
    /// Live Lighter MARKET-order roundtrip on the taker's order path, each step timed.
    ///
    /// Requires a flat Lighter starting position. A market order bounded under the bid must end
    /// unfilled; then it buys the minimum executable size capped by `--max-usd`, sells the
    /// resulting position reduce-only and verifies the final Lighter position is flat.
    LighterMarketRoundtrip {
        #[arg(long, default_value = "HYPE")]
        market: Option<String>,
        #[arg(long)]
        i_understand_live: bool,
        #[arg(long)]
        max_usd: Decimal,
    },
    /// Resolve an inactive session using matching terminal order evidence; never submits orders.
    ResolveSession {
        #[arg(long, default_value = "HYPE")]
        market: Option<String>,
    },
    /// Archive an active per-market loss circuit breaker so trading can be restarted manually.
    ResetCircuitBreaker {
        #[arg(long, default_value = "HYPE")]
        market: Option<String>,
        /// The dry run's breaker (runs/dry-run/) instead of live's.
        #[arg(long, default_value_t = false)]
        dry_run: bool,
    },
    /// Read-only account/book/opportunity status, as JSON: the taker report `run` polls every tick.
    Status {
        #[arg(long, default_value = "HYPE")]
        market: Option<String>,
    },
}

pub async fn dispatch(cli: Cli) -> Result<()> {
    let mut cfg = Config::load(&cli.config)?;
    match cli.command {
        Commands::Run {
            markets,
            secs,
            max_trades,
            min_size,
            observe_only,
            exposure_filter,
            reduce_cooldown_ms,
        } => {
            let selected = cfg.select_markets(markets.as_deref());
            if selected.is_empty() {
                anyhow::bail!("no markets selected");
            }
            let _lock = match (observe_only, selected.as_slice()) {
                (false, [market]) => {
                    let runs = std::path::Path::new(crate::controller::RUNS_DIR);
                    let legs = crate::controller::legs(market).iter().map(|leg| crate::controller::lock_market(runs, leg))
                        .collect::<anyhow::Result<Vec<_>>>()?;
                    Some((crate::controller::lock_market(runs, &market.id().0)?, legs))
                }
                _ => None,
            };
            let stop = crate::controller::stop_on_signals();
            crate::taker::arb::run(
                cfg,
                selected,
                crate::taker::arb::RunOptions {
                    secs,
                    max_trades,
                    min_size,
                    observe_only,
                    exposure_filter,
                    reduce_cooldown_ms,
                    ..Default::default()
                },
                stop,
            )
            .await
        }
        Commands::FetchSpecs { markets } => {
            let selected = cfg.select_markets(markets.as_deref());
            let specs = rest_specs::build_market_specs(
                &selected,
                &cfg.venues.aster_base_url,
                &cfg.venues.lighter_base_url,
                None,
            )
            .await?;
            println!(
                "{:<8} {:<12} {:<12} {:>8} {:>12} {:>12} {:>12} {:>12}",
                "id", "aster", "lighter", "mkt_id", "a_step", "l_step", "a_min_notl", "l_min_notl"
            );
            for s in specs {
                println!(
                    "{:<8} {:<12} {:<12} {:>8} {:>12} {:>12} {:>12} {:>12}",
                    s.market_id.0,
                    s.aster_symbol,
                    s.lighter_symbol,
                    s.lighter_market_id,
                    s.step,
                    s.lighter_qty_step,
                    s.aster_min_notional,
                    s.lighter_min_notional
                );
            }
            Ok(())
        }
        Commands::Probe { market } => {
            let spec = first_spec(&cfg, market.as_deref()).await?;
            let acreds = AsterCreds::from_env()?;
            let lcreds = LighterCreds::from_env()?;
            let signer: Arc<dyn AsterSigner> =
                Arc::new(EvmAsterSigner::new(acreds.user, acreds.signer, acreds.key)?);
            let aster = AsterRest::new(
                cfg.venues.aster_base_url.clone(),
                signer,
                std::slice::from_ref(&spec),
            )?;
            let lighter = LighterVenue::new(
                &cfg.venues.lighter_base_url,
                std::path::Path::new(&cfg.venues.signers_dir),
                lcreds,
                std::slice::from_ref(&spec),
            )
            .await?;
            lighter
                .wait_ready(&spec.market_id, std::time::Duration::from_secs(20))
                .await?;
            let (ap, lp, aa, la, ao, lo) = tokio::join!(
                aster.position_qty(&spec.market_id),
                lighter.position_qty(&spec.market_id),
                aster.available_usdc(),
                lighter.available_usdc(),
                aster.open_orders(&spec.market_id),
                lighter.open_orders_count(&spec.market_id),
            );
            println!("market={}", spec.market_id);
            println!("aster_position={}", ap?);
            println!("lighter_position={}", lp?);
            println!("aster_available_usd={}", aa?);
            println!("lighter_available_usd={}", la?);
            println!("aster_open_orders={}", ao?.len());
            println!("lighter_open_orders={}", lo?);
            Ok(())
        }
        Commands::AsterMarketRoundtrip {
            market,
            i_understand_live,
            max_usd,
        } => {
            if !i_understand_live {
                bail!("refusing live Aster market roundtrip without --i-understand-live");
            }
            if max_usd <= Decimal::ZERO {
                bail!("--max-usd must be positive");
            }
            let spec = first_spec(&cfg, market.as_deref()).await?;
            let acreds = AsterCreds::from_env()?;
            let signer: Arc<dyn AsterSigner> =
                Arc::new(EvmAsterSigner::new(acreds.user, acreds.signer, acreds.key)?);
            let aster_account_id = signer.user_address().to_string();
            let aster = AsterRest::new(
                cfg.venues.aster_base_url.clone(),
                signer,
                std::slice::from_ref(&spec),
            )?;

            let http = rest_book::client()?;
            let book = rest_book::fetch_aster_book(
                &http,
                &cfg.venues.aster_base_url,
                &spec.aster_symbol,
                20,
            )
            .await?;
            let ask = book.best_ask().context("Aster book has no ask")?;
            let bid = book.best_bid().context("Aster book has no bid")?;
            if book.is_crossed() {
                bail!("Aster book is crossed/locked; refusing roundtrip");
            }
            let initial_pos = aster.position_qty(&spec.market_id).await?;
            let open_orders = aster.open_orders(&spec.market_id).await?;
            if !open_orders.is_empty() {
                bail!(
                    "Aster has {} open order(s); refusing roundtrip",
                    open_orders.len()
                );
            }
            if initial_pos != Decimal::ZERO {
                bail!("Aster starting position is {initial_pos}, not flat; refusing roundtrip");
            }
            let qty = floor_to_step(max_usd / ask.px, spec.step);
            if qty <= Decimal::ZERO {
                bail!(
                    "--max-usd rounds to zero quantity at Aster step {}",
                    spec.step
                );
            }
            if qty < spec.aster_min_qty || qty * ask.px < spec.aster_min_notional {
                bail!(
                    "roundtrip size too small: qty={} notional={} min_qty={} min_notional={}",
                    qty,
                    qty * ask.px,
                    spec.aster_min_qty,
                    spec.aster_min_notional
                );
            }
            println!(
                "aster_market_roundtrip_start market={} qty={} ask={} bid={} max_usd={}",
                spec.market_id, qty, ask.px, bid.px, max_usd
            );
            let bal_before = aster.available_usdc().await?;
            println!("balance_before={bal_before}");
            // XEMM's user stream watches the fills, and XEMM's reduce-only MARKET flatten closes.
            let target = market.as_deref().unwrap_or("HYPE");
            let (xemm, xspec) = crate::livebot::probe::xemm_aster(&cli.config, target).await?;
            let stop = tokio_util::sync::CancellationToken::new();
            let stream = crate::livebot::probe::spawn_fill_printer(crate::livebot::probe::xemm_aster(&cli.config, target).await?.0,
                &xspec, Arc::default(), stop.clone());
            tokio::time::sleep(Duration::from_secs(2)).await; // the stream subscribes before the first fill

            let session = diagnostic_session(&cfg, &spec, serde_json::json!({"aster_account": aster_account_id}));
            session.arm().await?;
            let miss = timed_aster_ioc(&aster, &spec, qty, bid.px * rust_decimal_macros::dec!(0.99), "under_bid").await;
            let buy = timed_aster_ioc(&aster, &spec, qty, ask.px * (Decimal::ONE + bps_to_rate(cfg.arb.max_aster_slippage_bps)), "buy").await;
            let closed = std::cell::Cell::new(Decimal::ZERO);
            let operation = async {
                let miss = crate::taker::arb::resolve_aster_evidence(&spec, &aster, &miss, Duration::from_secs(5), true).await;
                anyhow::ensure!(miss.terminal && miss.qty == Some(Decimal::ZERO), "the IOC under the bid did not end unfilled");
                ensure_accepted("buy", &buy)?;
                let t = std::time::Instant::now();
                let fill = wait_aster_fill("buy", &aster, &spec.market_id, &buy, qty).await?;
                println!("buy_fill={fill:?} fee_known_after={}ms", t.elapsed().as_millis());
                let t = std::time::Instant::now();
                let position = wait_position_after_buy(&aster, &spec.market_id, qty, spec.step).await?;
                println!("position_after_buy={position} visible_after={}ms", t.elapsed().as_millis());
                let t = std::time::Instant::now();
                let body = xemm.flatten_result(&xspec.market_id, crate::types::Side::Sell, position,
                    &format!("Xprb-flat-{}", chrono::Utc::now().timestamp_millis())).await?;
                println!("xemm_flatten ({}ms): {body}", t.elapsed().as_millis());
                closed.set(crate::taker::aster::rest::immediate_fill_from_order_response(&body)?.qty);
                Ok(())
            };
            let result = run_diagnostic(&cfg,&spec,&session,operation,cleanup_aster_diagnostic(&cfg,&spec,&aster,&buy,qty,&closed)).await;
            stop.cancel();
            let _ = stream.await;
            result
        }
        Commands::LighterMarketRoundtrip {
            market,
            i_understand_live,
            max_usd,
        } => {
            if !i_understand_live {
                bail!("refusing live Lighter market roundtrip without --i-understand-live");
            }
            if max_usd <= Decimal::ZERO {
                bail!("--max-usd must be positive");
            }
            let spec = first_spec(&cfg, market.as_deref()).await?;
            let lcreds = LighterCreds::from_env()?;
            let lighter = LighterVenue::new(
                &cfg.venues.lighter_base_url,
                std::path::Path::new(&cfg.venues.signers_dir),
                lcreds,
                std::slice::from_ref(&spec),
            )
            .await?;

            lighter
                .wait_ready(&spec.market_id, std::time::Duration::from_secs(20))
                .await?;
            let book = lighter.order_book(&spec.market_id)?;
            let ask = book.best_ask().context("Lighter book has no ask")?;
            let bid = book.best_bid().context("Lighter book has no bid")?;
            if book.is_crossed() {
                bail!("Lighter book is crossed/locked; refusing roundtrip");
            }
            let initial_pos = lighter.position_qty(&spec.market_id).await?;
            let open_orders = lighter.open_orders_count(&spec.market_id).await?;
            if open_orders > 0 {
                bail!("Lighter has {open_orders} open order(s); refusing roundtrip");
            }
            if initial_pos != Decimal::ZERO {
                bail!("Lighter starting position is {initial_pos}, not flat; refusing roundtrip");
            }

            let min_qty = ceil_to_step(spec.lighter_min_notional / ask.px, spec.lighter_qty_step);
            let qty = min_qty.max(spec.lighter_qty_step);
            let notional = qty * ask.px;
            if qty <= Decimal::ZERO {
                bail!(
                    "Lighter minimum rounds to zero at qty step {}",
                    spec.lighter_qty_step
                );
            }
            if notional > max_usd {
                bail!(
                    "minimum Lighter roundtrip notional {} exceeds --max-usd {}; raise --max-usd",
                    notional,
                    max_usd
                );
            }

            // The entry's bound, as the taker prices it; cleanup closes at the emergency bound.
            let slippage = bps_to_rate(cfg.arb.max_lighter_slippage_bps);
            let buy_bound = ask.px * (Decimal::ONE + slippage);
            let sell_bound = bid.px * (Decimal::ONE - bps_to_rate(cfg.arb.emergency_slippage_bps));
            println!(
                "lighter_market_roundtrip_start market={} market_id={} qty={} bid={} ask={} buy_bound={} sell_bound={} max_usd={} min_notional={}",
                spec.market_id,
                spec.lighter_market_id,
                qty,
                bid.px,
                ask.px,
                buy_bound,
                sell_bound,
                max_usd,
                spec.lighter_min_notional
            );
            let bal_before = lighter.available_usdc().await?;
            println!("balance_before={bal_before}");

            let session = diagnostic_session(&cfg, &spec,
                serde_json::json!({"lighter_account_index": lighter.account_index()}));
            session.arm().await?;
            // First a market order bounded under the bid, which cannot fill: how the taker sees it.
            let t = std::time::Instant::now();
            let (miss, pending) = lighter.submit_market_order_deferred_fill(&spec.market_id, Side::Buy, qty,
                bid.px * rust_decimal_macros::dec!(0.99), false).await;
            println!("under_bid_market answered_after={}ms: {miss:?}", t.elapsed().as_millis());
            let miss = crate::taker::arb::resolve_lighter_evidence(&spec, &lighter, &miss, pending, Side::Buy, qty,
                Duration::from_secs(10)).await;
            println!("under_bid_market resolved_after={}ms terminal={} qty={:?}", t.elapsed().as_millis(), miss.terminal, miss.qty);
            let t = std::time::Instant::now();
            let buy = lighter.submit_market_order(&spec.market_id, Side::Buy, qty, buy_bound, false).await;
            println!("buy_result filled_after={}ms: {buy:?}", t.elapsed().as_millis());
            let operation = async {
                anyhow::ensure!(miss.terminal && miss.qty == Some(Decimal::ZERO), "the market order under the bid did not end unfilled");
                ensure_lighter_accepted("buy", &buy)?;
                let fill = ensure_lighter_fill("buy", &buy)?;
                println!("buy_fill={fill:?}");
                let position = wait_lighter_position_after_buy(&lighter, &spec.market_id, qty, spec.lighter_qty_step).await?;
                let balance = lighter.available_usdc().await?;
                println!("position_after_buy={position} balance_after_buy={balance}");
                Ok(())
            };
            run_diagnostic(&cfg,&spec,&session,operation,cleanup_lighter_diagnostic(&cfg,&spec,&lighter,&buy,qty)).await
        }
        Commands::ResolveSession { market } => {
            let selected = cfg.select_markets(market.as_deref());
            anyhow::ensure!(selected.len() == 1, "resolve-session requires one market");
            let spec = rest_specs::build_market_specs(&selected, &cfg.venues.aster_base_url, &cfg.venues.lighter_base_url, None)
                .await?.into_iter().next().context("market specification missing")?;
            let path = crate::taker::pnl::session_path(&cfg.pnl, &spec.market_id);
            let _ownership = crate::taker::pnl::lock_inactive_session(&path)?;
            let marker: serde_json::Value = serde_json::from_slice(&std::fs::read(&path)?)?;
            anyhow::ensure!(marker.get("market").and_then(serde_json::Value::as_str) == Some(spec.market_id.0.as_str()),
                "session market does not match the requested configuration");
            let evidence = marker.get("unresolved_execution").context(
                "session has no complete scoped order identities; flat positions alone cannot resolve a crash-before-receipt gap")?;
            anyhow::ensure!(evidence.get("orders_complete").and_then(serde_json::Value::as_bool) == Some(true),
                "session order identity coverage is incomplete; primary venue evidence is required");
            let orders = evidence.get("orders").and_then(serde_json::Value::as_array).context("session orders missing")?;
            anyhow::ensure!(!orders.is_empty(), "session has no scoped orders to verify");
            let acreds = AsterCreds::from_env()?;
            let lcreds = LighterCreds::from_env()?;
            if let Some(account) = marker.get("aster_account").and_then(serde_json::Value::as_str) {
                anyhow::ensure!(account.eq_ignore_ascii_case(&acreds.user), "Aster session account mismatch");
            }
            let signer: Arc<dyn AsterSigner> = Arc::new(EvmAsterSigner::new(acreds.user, acreds.signer, acreds.key)?);
            let aster = AsterRest::new(cfg.venues.aster_base_url.clone(), signer, std::slice::from_ref(&spec))?;
            let lighter = LighterVenue::new_read_only(&cfg.venues.lighter_base_url,
                std::path::Path::new(&cfg.venues.signers_dir), lcreds, std::slice::from_ref(&spec))?;
            if let Some(account) = marker.get("lighter_account_index").and_then(serde_json::Value::as_i64) {
                anyhow::ensure!(account == lighter.account_index(), "Lighter session account mismatch");
            }
            let baseline = evidence.get("pre_positions").context("session position baseline missing")?;
            let mut expected_aster = baseline.get("aster_qty").and_then(serde_json::Value::as_str)
                .context("Aster baseline quantity unavailable")?.parse::<Decimal>()?;
            let mut expected_lighter = baseline.get("lighter_qty").and_then(serde_json::Value::as_str)
                .context("Lighter baseline quantity unavailable")?.parse::<Decimal>()?;
            let mut verified = Vec::new();
            for order in orders {
                if order.get("submitted").and_then(serde_json::Value::as_bool) == Some(false) { continue; }
                let side = match order.get("side").and_then(serde_json::Value::as_str) {
                    Some("BUY") => Side::Buy, Some("SELL") => Side::Sell,
                    _ => bail!("session order side unavailable"),
                };
                match order.get("venue").and_then(serde_json::Value::as_str) {
                    Some("aster") => {
                        let client = order.get("client_order_id").and_then(serde_json::Value::as_str)
                            .context("Aster client identity missing")?;
                        let result = aster.query_order(&spec.market_id, client).await?;
                        anyhow::ensure!(result.get("clientOrderId").and_then(serde_json::Value::as_str) == Some(client), "Aster order identity mismatch");
                        anyhow::ensure!(crate::taker::aster::rest::order_response_is_terminal(&serde_json::to_string(&result)?)?, "Aster order is not terminal");
                        let fill = crate::taker::aster::rest::immediate_fill_from_order_response(&serde_json::to_string(&result)?)?;
                        expected_aster += if side == Side::Buy { fill.qty } else { -fill.qty };
                        verified.push(serde_json::json!({"venue":"aster", "order": result}));
                    }
                    Some("lighter") => {
                        let client = order.get("client_order_index").and_then(serde_json::Value::as_i64)
                            .context("Lighter client identity missing")?;
                        let result = lighter.resolve_order_terminal(&spec.market_id, client, side, Duration::from_secs(10)).await?;
                        anyhow::ensure!(result.terminal_order.as_ref().is_some_and(|order| order.is_terminal()), "Lighter terminal order unavailable");
                        expected_lighter += if side == Side::Buy { result.filled_qty } else { -result.filled_qty };
                        verified.push(serde_json::json!({"venue":"lighter", "client_order_index":client,
                            "terminal_order":format!("{:?}",result.terminal_order), "fill":result.fill}));
                    }
                    _ => bail!("unknown venue in session evidence"),
                }
            }
            let (a_pos, l_pos, a_open, l_open) = tokio::join!(aster.position_qty(&spec.market_id),
                lighter.rest_position_qty(&spec.market_id), aster.open_orders(&spec.market_id),
                lighter.rest_open_orders_count(&spec.market_id));
            let (a_pos, l_pos) = (a_pos?, l_pos?);
            anyhow::ensure!(a_open?.is_empty() && l_open? == 0, "session still has open orders");
            let http = rest_book::client()?;
            let book = rest_book::fetch_aster_book(&http, &cfg.venues.aster_base_url, &spec.aster_symbol, 20).await?;
            let mark = book.mid().context("resolution mark unavailable")?;
            anyhow::ensure!((a_pos + l_pos).abs() * mark <= cfg.risk.max_position_mismatch_usd,
                "session positions are not balanced; read-only resolver cannot close exposure");
            anyhow::ensure!((a_pos - expected_aster).abs() * mark <= cfg.risk.max_position_mismatch_usd
                && (l_pos - expected_lighter).abs() * mark <= cfg.risk.max_position_mismatch_usd,
                "session positions do not reflect the terminal fill evidence");
            let session_id = marker.get("session_id").and_then(serde_json::Value::as_str).context("session id missing")?.to_string();
            let artifact = crate::taker::pnl::retire_session_verified(path, session_id.clone(), serde_json::json!({
                "schema_version":2, "session_id":session_id, "resolved_at":chrono::Utc::now(),
                "terminal_evidence":verified, "aster_position":a_pos, "lighter_position":l_pos,
                "open_orders":0, "source_marker":marker,
            })).await?;
            println!("session_resolved artifact={}", artifact.display());
            Ok(())
        }
        Commands::ResetCircuitBreaker { market, dry_run } => {
            if dry_run {
                // Where `run --mode dry-run` keeps the taker's files.
                cfg.pnl.persist_dir = format!("{}/dry-run", crate::controller::RUNS_DIR);
            }
            let selected = cfg.select_markets(market.as_deref());
            let market_id = selected
                .into_iter()
                .next()
                .context("no selected market for reset-circuit-breaker")?
                .id();
            match crate::taker::pnl::reset_circuit_breaker(&cfg.pnl, &market_id)? {
                Some(path) => {
                    println!(
                        "circuit_breaker_reset market={} archive={}",
                        market_id,
                        path.display()
                    );
                }
                None => {
                    println!("circuit_breaker_reset market={} active=false", market_id);
                }
            }
            println!("pnl_since={}", cfg.pnl.since);
            println!("pnl_persist_dir={}", cfg.pnl.persist_dir);
            Ok(())
        }
        Commands::Status { market } => {
            let selected = cfg.select_markets(market.as_deref());
            if selected.is_empty() {
                anyhow::bail!("no markets selected");
            }
            crate::taker::status::run(&cfg, selected).await
        }
    }
}

/// The resolved spec of the first selected market.
async fn first_spec(cfg: &Config, market: Option<&str>) -> Result<MarketSpec> {
    let selected = cfg.select_markets(market);
    rest_specs::build_market_specs(&selected, &cfg.venues.aster_base_url, &cfg.venues.lighter_base_url, None)
        .await?
        .into_iter()
        .next()
        .context("no selected market spec")
}

fn diagnostic_session(cfg: &Config, spec: &MarketSpec, account: serde_json::Value) -> ActiveSession {
    let id = format!("diagnostic-{}-{}", std::process::id(), chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0));
    let mut metadata = serde_json::json!({
        "schema_version": 2, "session_id": id, "started_at": chrono::Utc::now(), "status": "active",
        "market": spec.market_id.to_string(),
    });
    if let Some(account) = account.as_object() {
        for (key,value) in account { metadata[key] = value.clone(); }
    }
    ActiveSession::new(crate::taker::pnl::session_path(&cfg.pnl, &spec.market_id), metadata)
}

struct DiagnosticCleanup {
    orders: Vec<serde_json::Value>,
    verified: bool,
    identity_complete: bool,
    error: Option<String>,
}

/// `closed`: what the diagnostic itself already sold back, read once it has finished.
async fn cleanup_aster_diagnostic(
    cfg: &Config, spec: &MarketSpec, aster: &AsterRest,
    buy: &crate::taker::aster::rest::SubmitOutcome, quantity: Decimal, closed: &std::cell::Cell<Decimal>,
) -> DiagnosticCleanup {
    let mut orders = vec![crate::taker::arb::aster_order_identity(buy, Side::Buy, quantity)];
    let mut in_flight = false;
    let result = tokio::time::timeout(Duration::from_secs(30), async {
        let buy_evidence = crate::taker::arb::resolve_aster_evidence(spec, aster, buy, Duration::from_secs(10), true).await;
        anyhow::ensure!(buy_evidence.terminal, "initial Aster buy is unresolved");
        let mut expected = buy_evidence.qty.context("Aster buy quantity unavailable")? - closed.get();
        let http = rest_book::client()?;
        let mut attempts = 0;
        loop {
            let position = match aster.position_qty(&spec.market_id).await {
                Ok(position) => position,
                Err(_) => { tokio::time::sleep(Duration::from_millis(250)).await; continue; }
            };
            // A pre-fill flat response is not a completed roundtrip.
            if position != expected {
                tokio::time::sleep(Duration::from_millis(250)).await;
                continue;
            }
            if position == Decimal::ZERO {
                anyhow::ensure!(aster.open_orders(&spec.market_id).await?.is_empty(), "Aster diagnostic has open orders");
                println!("position_final=0 cleanup_verified=true");
                return Ok::<_, anyhow::Error>(());
            }
            anyhow::ensure!(position > Decimal::ZERO && attempts < 3, "Aster diagnostic close failed after three attempts");
            let qty = floor_to_step(position, spec.step);
            anyhow::ensure!(qty > Decimal::ZERO, "Aster diagnostic residual below quantity step");
            let book = rest_book::fetch_aster_book(&http, &cfg.venues.aster_base_url, &spec.aster_symbol, 20).await?;
            let bid = book.best_bid().context("Aster cleanup bid missing")?.px;
            let bound = bid * (Decimal::ONE - bps_to_rate(cfg.arb.emergency_slippage_bps));
            in_flight = true;
            let close = aster.submit_ioc_order(&spec.market_id, Side::Sell, qty, bound, true).await;
            orders.push(crate::taker::arb::aster_order_identity(&close, Side::Sell, qty));
            in_flight = false;
            attempts += 1;
            let evidence = crate::taker::arb::resolve_aster_evidence(spec, aster, &close, Duration::from_secs(5), true).await;
            anyhow::ensure!(evidence.terminal, "Aster diagnostic close remains unresolved");
            expected -= evidence.qty.context("Aster close quantity unavailable")?;
            println!("cleanup_close={close:?} fill={:?}", evidence.fill);
        }
    }).await;
    let error = match result { Ok(Ok(())) => None, Ok(Err(error)) => Some(format!("{error:#}")),
        Err(_) => Some("Aster diagnostic cleanup exceeded thirty seconds".to_string()) };
    DiagnosticCleanup { orders, verified: error.is_none(), identity_complete: !in_flight, error }
}

async fn cleanup_lighter_diagnostic(
    cfg: &Config, spec: &MarketSpec, lighter: &LighterVenue,
    buy: &crate::taker::venues::lighter::SubmitOutcome, quantity: Decimal,
) -> DiagnosticCleanup {
    let mut orders = vec![crate::taker::arb::lighter_order_identity(buy, Side::Buy, quantity)];
    let mut in_flight = false;
    let result = tokio::time::timeout(Duration::from_secs(30), async {
        let buy_evidence = crate::taker::arb::resolve_lighter_evidence(spec, lighter, buy, None, Side::Buy,
            quantity, Duration::from_secs(10)).await;
        anyhow::ensure!(buy_evidence.terminal, "initial Lighter buy is unresolved");
        let mut expected = buy_evidence.qty.context("Lighter buy quantity unavailable")?;
        let http = rest_book::client()?;
        let mut attempts = 0;
        loop {
            let position = match lighter.position_qty(&spec.market_id).await {
                Ok(position) => position,
                Err(_) => { tokio::time::sleep(Duration::from_millis(250)).await; continue; }
            };
            if position != expected {
                tokio::time::sleep(Duration::from_millis(250)).await;
                continue;
            }
            if position == Decimal::ZERO {
                anyhow::ensure!(lighter.rest_open_orders_count(&spec.market_id).await? == 0, "Lighter diagnostic has open orders");
                println!("position_final=0 cleanup_verified=true");
                return Ok::<_, anyhow::Error>(());
            }
            anyhow::ensure!(position > Decimal::ZERO && attempts < 3, "Lighter diagnostic close failed after three attempts");
            let qty = floor_to_step(position, spec.lighter_qty_step);
            anyhow::ensure!(qty > Decimal::ZERO, "Lighter diagnostic residual below quantity step");
            let book = rest_book::fetch_lighter_book(&http, &cfg.venues.lighter_base_url, spec.lighter_market_id, 20).await?;
            let bid = book.best_bid().context("Lighter cleanup bid missing")?.px;
            let bound = bid * (Decimal::ONE - bps_to_rate(cfg.arb.emergency_slippage_bps));
            in_flight = true;
            let (close, pending) = lighter.submit_market_order_deferred_fill(&spec.market_id, Side::Sell, qty, bound, true).await;
            orders.push(crate::taker::arb::lighter_order_identity(&close, Side::Sell, qty));
            in_flight = false;
            attempts += 1;
            let evidence = crate::taker::arb::resolve_lighter_evidence(spec, lighter, &close, pending, Side::Sell,
                qty, Duration::from_secs(5)).await;
            anyhow::ensure!(evidence.terminal, "Lighter diagnostic close remains unresolved");
            expected -= evidence.qty.context("Lighter close quantity unavailable")?;
            println!("cleanup_close={close:?} fill={:?}", evidence.fill);
        }
    }).await;
    let error = match result { Ok(Ok(())) => None, Ok(Err(error)) => Some(format!("{error:#}")),
        Err(_) => Some("Lighter diagnostic cleanup exceeded thirty seconds".to_string()) };
    DiagnosticCleanup { orders, verified: error.is_none(), identity_complete: !in_flight, error }
}

async fn run_diagnostic<O,C>(cfg: &Config, spec: &MarketSpec, session: &ActiveSession,
    operation: O, cleanup: C) -> Result<()>
where O: std::future::Future<Output=Result<()>>, C: std::future::Future<Output=DiagnosticCleanup> {
    let operation = operation.await;
    let cleanup = cleanup.await;
    finish_diagnostic(cfg,spec,session,operation,cleanup).await
}

async fn finish_diagnostic(cfg: &Config, spec: &MarketSpec, session: &ActiveSession,
    operation: Result<()>, cleanup: DiagnosticCleanup) -> Result<()> {
    let row = serde_json::json!({ "schema_version": 2, "economic_status": "incomplete",
        "timestamp": chrono::Utc::now(), "session_id": session.id(), "market": spec.market_id.to_string(),
        "outcome": if cleanup.verified { "diagnostic_complete" } else { "diagnostic_unresolved" },
        "orders": cleanup.orders, "orders_complete": cleanup.identity_complete,
        "pre_positions":{"aster_qty":"0","lighter_qty":"0"},
        "diagnostic_error": operation.as_ref().err().map(|error| format!("{error:#}")), "cleanup_error": cleanup.error });
    if !cleanup.verified { session.record_unresolved(row.clone()).await?; }
    let path = PathBuf::from(&cfg.pnl.persist_dir).join(format!("diagnostics_{}.jsonl", spec.market_id));
    tokio::task::spawn_blocking(move || crate::taker::pnl::append_json_line(&path, &row, true)).await??;
    if cleanup.verified {
        session.resolve_execution().await?;
        session.clear_verified().await?;
        operation.context("diagnostic error; cleanup nevertheless verified the position flat")
    } else {
        bail!("diagnostic cleanup unresolved: {}; original diagnostic error: {}",
            cleanup.error.unwrap_or_else(|| "unknown".to_string()),
            operation.err().map(|error| format!("{error:#}")).unwrap_or_else(|| "none".to_string()))
    }
}

/// The taker's entry order (`submit_ioc_order`, a buy), with the wall-clock send time so its
/// RESULT can be compared with the fill's arrival on the user stream.
async fn timed_aster_ioc(aster: &AsterRest, spec: &MarketSpec, qty: Decimal, bound: Decimal, label: &str)
    -> crate::taker::aster::rest::SubmitOutcome {
    let sent_at = chrono::Utc::now().timestamp_millis();
    let t = std::time::Instant::now();
    let outcome = aster.submit_ioc_order(&spec.market_id, Side::Buy, qty, bound, false).await;
    println!("{label}_ioc bound={bound} sent_at={sent_at} result_after={}ms: {outcome:?}", t.elapsed().as_millis());
    outcome
}

fn ensure_accepted(label: &str, outcome: &crate::taker::aster::rest::SubmitOutcome) -> Result<()> {
    match outcome {
        crate::taker::aster::rest::SubmitOutcome::Accepted { .. } => Ok(()),
        other => bail!("{label} market order was not accepted: {other:?}"),
    }
}

async fn wait_aster_fill(
    label: &str,
    aster: &AsterRest,
    market: &crate::taker::types::MarketId,
    outcome: &crate::taker::aster::rest::SubmitOutcome,
    expected_qty: Decimal,
) -> Result<crate::taker::types::FillSummary> {
    let crate::taker::aster::rest::SubmitOutcome::Accepted {
        venue_order_id: Some(order_id),
        ..
    } = outcome
    else {
        bail!("{label} Aster market order was accepted without an orderId: {outcome:?}");
    };
    aster
        .wait_order_fill_summary(
            market,
            *order_id,
            expected_qty,
            std::time::Duration::from_secs(10),
        )
        .await
}

fn ensure_lighter_accepted(
    label: &str,
    outcome: &crate::taker::venues::lighter::SubmitOutcome,
) -> Result<()> {
    match outcome {
        crate::taker::venues::lighter::SubmitOutcome::Accepted { .. } => Ok(()),
        other => bail!("{label} Lighter market order was not accepted: {other:?}"),
    }
}

fn ensure_lighter_fill(
    label: &str,
    outcome: &crate::taker::venues::lighter::SubmitOutcome,
) -> Result<crate::taker::types::FillSummary> {
    match outcome {
        crate::taker::venues::lighter::SubmitOutcome::Accepted {
            fill: Some(fill), ..
        } => Ok(*fill),
        other => bail!("{label} Lighter market order accepted without fill detail: {other:?}"),
    }
}

async fn wait_position_after_buy(
    aster: &AsterRest,
    market: &crate::taker::types::MarketId,
    target: Decimal,
    tolerance: Decimal,
) -> Result<Decimal> {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
    let mut last = Decimal::ZERO;
    loop {
        let pos = aster.position_qty(market).await?;
        if pos > Decimal::ZERO {
            last = pos;
        }
        if pos >= target - tolerance {
            return Ok(pos);
        }
        if tokio::time::Instant::now() >= deadline {
            if last > Decimal::ZERO {
                return Ok(last);
            }
            bail!(
                "timed out waiting for Aster position >= {}; last={}",
                target,
                pos
            );
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
}


async fn wait_lighter_position_after_buy(
    lighter: &LighterVenue,
    market: &crate::taker::types::MarketId,
    target: Decimal,
    tolerance: Decimal,
) -> Result<Decimal> {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
    let mut last = Decimal::ZERO;
    loop {
        let pos = lighter.position_qty(market).await?;
        if pos > Decimal::ZERO {
            last = pos;
        }
        if pos >= target - tolerance {
            return Ok(pos);
        }
        if tokio::time::Instant::now() >= deadline {
            if last > Decimal::ZERO {
                return Ok(last);
            }
            bail!(
                "timed out waiting for Lighter position >= {}; last={}",
                target,
                pos
            );
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;
    use std::sync::atomic::{AtomicBool,AtomicUsize,Ordering};
    use tokio::io::{AsyncReadExt,AsyncWriteExt};

    #[tokio::test]
    async fn balance_failure_still_executes_and_verifies_reduce_only_cleanup() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}",listener.local_addr().unwrap());
        let closed = Arc::new(AtomicBool::new(false));
        let closes = Arc::new(AtomicUsize::new(0));
        let reduce_only = Arc::new(AtomicBool::new(true));
        let (server_closed,server_closes,server_reduce) = (closed.clone(),closes.clone(),reduce_only.clone());
        let server = tokio::spawn(async move {
            loop {
                let (mut stream,_) = listener.accept().await.unwrap();
                let mut bytes = Vec::new();
                let (header_end,length) = loop {
                    let mut buffer = [0u8;4096];
                    let count = stream.read(&mut buffer).await.unwrap();
                    if count == 0 { break (0,0); }
                    bytes.extend_from_slice(&buffer[..count]);
                    if let Some(end) = bytes.windows(4).position(|part|part==b"\r\n\r\n") {
                        let header = String::from_utf8_lossy(&bytes[..end]);
                        let length = header.lines().find_map(|line|line.to_ascii_lowercase().strip_prefix("content-length:")
                            .and_then(|value|value.trim().parse::<usize>().ok())).unwrap_or(0);
                        break (end+4,length);
                    }
                };
                if header_end == 0 { continue; }
                while bytes.len() < header_end+length {
                    let mut buffer = [0u8;4096];
                    let count = stream.read(&mut buffer).await.unwrap();
                    if count==0 { break; }
                    bytes.extend_from_slice(&buffer[..count]);
                }
                let request = String::from_utf8_lossy(&bytes);
                let first = request.lines().next().unwrap_or("");
                let body = String::from_utf8_lossy(&bytes[header_end..]);
                let mut status = "200 OK";
                let response = if first.starts_with("POST /fapi/v3/order") {
                    server_reduce.fetch_and(body.contains("reduceOnly=true") && body.contains("side=SELL"),Ordering::SeqCst);
                    server_closes.fetch_add(1,Ordering::SeqCst);
                    server_closed.store(true,Ordering::SeqCst);
                    let client = body.split('&').find_map(|part|part.strip_prefix("newClientOrderId=")).unwrap_or("missing");
                    serde_json::json!({"orderId":2,"clientOrderId":client,"status":"FILLED","executedQty":"1","cumQuote":"100","avgPrice":"100"})
                } else if first.contains("/positionRisk") {
                    serde_json::json!([{"symbol":"HYPEUSDT","positionAmt":if server_closed.load(Ordering::SeqCst){"0"}else{"1"}}])
                } else if first.contains("/userTrades") {
                    serde_json::json!([
                        {"id":1,"orderId":1,"price":"100","qty":"1","quoteQty":"100","commission":"0.04","commissionAsset":"USDT"},
                        {"id":2,"orderId":2,"price":"100","qty":"1","quoteQty":"100","commission":"0.04","commissionAsset":"USDT"}
                    ])
                } else if first.contains("/depth") {
                    serde_json::json!({"bids":[["100","10"]],"asks":[["101","10"]]})
                } else if first.contains("/openOrders") { serde_json::json!([]) }
                else { status="500 Internal Server Error"; serde_json::json!({"error":"injected balance failure"}) };
                let body = response.to_string();
                let response = format!("HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len());
                let _ = stream.write_all(response.as_bytes()).await;
            }
        });
        let mut cfg: Config = toml::from_str("[[markets]]\naster_symbol=\"HYPEUSDT\"\nlighter_symbol=\"HYPE\"").unwrap();
        cfg.venues.aster_base_url = url.clone();
        let dir = std::env::temp_dir().join(format!("taker_diagnostic_{}_{}",std::process::id(),chrono::Utc::now().timestamp_micros()));
        cfg.pnl.persist_dir = dir.to_string_lossy().into_owned();
        let spec = MarketSpec {market_id:"HYPE".into(),aster_symbol:"HYPEUSDT".to_string(),lighter_symbol:"HYPE".to_string(),
            lighter_market_id:24,lighter_price_decimals:4,lighter_size_decimals:2,lighter_price_tick:dec!(0.0001), hedge: Default::default(), first: Default::default(), first_market_index: 0,
            tick:dec!(0.01),step:dec!(0.01),aster_min_qty:dec!(0.01),aster_min_notional:dec!(10),
            lighter_qty_step:dec!(0.01),lighter_min_notional:dec!(10)};
        let aster = AsterRest::new(url,Arc::new(crate::taker::aster::sign::test_support::TestSigner::new()),std::slice::from_ref(&spec)).unwrap();
        let buy = crate::taker::aster::rest::SubmitOutcome::Accepted {venue_order_id:Some(1),client_order_id:"buy-fixture".to_string(),
            raw:serde_json::json!({"orderId":1,"clientOrderId":"buy-fixture","status":"FILLED","executedQty":"1","cumQuote":"100"}).to_string()};
        let session = diagnostic_session(&cfg,&spec,serde_json::json!({"fixture":true}));
        session.arm().await.unwrap();
        let result = run_diagnostic(&cfg,&spec,&session,
            async { aster.available_usdc().await?; Ok(()) },
            cleanup_aster_diagnostic(&cfg,&spec,&aster,&buy,dec!(1),&std::cell::Cell::new(Decimal::ZERO))).await;
        assert!(result.is_err());
        assert!(format!("{:#}",result.unwrap_err()).contains("cleanup nevertheless verified"));
        assert_eq!(closes.load(Ordering::SeqCst),1);
        assert!(reduce_only.load(Ordering::SeqCst));
        assert!(closed.load(Ordering::SeqCst));
        assert!(!crate::taker::pnl::session_path(&cfg.pnl,&spec.market_id).exists());
        server.abort();
        let _ = std::fs::remove_dir_all(dir);
    }
}
