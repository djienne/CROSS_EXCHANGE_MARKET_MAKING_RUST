//! The XEMM engine, wiring four planes: ingest threads + watchdog
//! (market-data hot path), the strategy loop (strategy/order hot path), the execution
//! workers behind command queues (execution hot path), and account/journal/book-check (cold
//! plane).
//!
//! ## Hard safety gate
//!
//! [`run`] refuses to start unless `[live] enabled = true`, and requires a single selected
//! market plus credentials and the Lighter signer: the real ones in `live`, the dry-run
//! identity in a dry run.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;

use anyhow::{bail, Result};
use chrono::Utc;
use tokio::sync::{mpsc, oneshot};
use tokio::sync::Notify;
use tokio::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::config::{Config, MarketCfg};
use crate::connectors::{rest_book, rest_specs};
use crate::hotpath::clock::mono_now_ns;
use crate::hotpath::{
    run_book_check, run_watchdog, spawn_venue_thread, BookCheckParams, BookCheckTarget, ReconnectHandle,
    TradingGate, VenueRegistry, VenueTag,
};
use crate::markets::MarketSpec;
use crate::types::MarketId;

use super::account::AccountState;
use super::scale;
use super::exec::command::{ExecCommand, ExecEvent, HedgeCommand, CMD_QUEUE_DEPTH};
use super::fills::AsterFill;
use super::ids::SessionId;
use super::journal::{run_journal_writer, Journal};
use super::pairs::{classify, is_eligible};
use super::strategy::{run_strategy, Strategy};

/// Connection-stale threshold for the watchdog: 60s.
const WATCHDOG_STALE_MS: i64 = 60_000;
const WATCHDOG_SCAN: std::time::Duration = std::time::Duration::from_millis(250);

async fn send_exec_safety(tx: &mpsc::Sender<ExecCommand>, cmd: ExecCommand, label: &'static str) {
    match tokio::time::timeout(Duration::from_secs(2), tx.send(cmd)).await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => warn!("safety send failed ({label}): receiver closed: {e}"),
        Err(_) => warn!("safety send timed out ({label})"),
    }
}

async fn send_hedge_safety(tx: &mpsc::Sender<HedgeCommand>, cmd: HedgeCommand, label: &'static str) {
    match tokio::time::timeout(Duration::from_secs(2), tx.send(cmd)).await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => warn!("hedge safety send failed ({label}): receiver closed: {e}"),
        Err(_) => warn!("hedge safety send timed out ({label})"),
    }
}

fn panic_payload_message(panic: &(dyn std::any::Any + Send)) -> String {
    panic
        .downcast_ref::<&str>()
        .map(|s| s.to_string())
        .or_else(|| panic.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "unknown panic".to_string())
}

/// The strategy thread's name: its panics are caught and shut the bot down in order, so the
/// abort-on-panic hook of `taker`/`run` lets them unwind.
pub const STRATEGY_THREAD: &str = "livebot-strategy";

/// Entry point for the XEMM engine of `run`.
///
/// The bot's hot planes (strategy/exec) run concurrently off the lock-free `VenueBook` cells.
/// `stem` (e.g. `runs/bot-HYPE`) names the journal, trip latch, active-session marker and
/// residual report (`<stem>*`).
///
/// Cancelling `stop` takes the same bounded drain as an internal safety halt.
pub async fn run(
    cfg: &Config,
    markets: Vec<MarketCfg>,
    stem: PathBuf,
    pause: Arc<AtomicBool>,
    rights: Option<super::strategy::Rights>,
    stop: CancellationToken,
) -> Result<()> {
    if markets.is_empty() {
        bail!("no markets selected for livebot");
    }
    // --- the hard safety gate ---
    if !cfg.live.enabled {
        bail!("livebot is disabled: set [maker.live] enabled = true in the config to run it");
    }
    if markets.len() != 1 {
        bail!(
            "refusing to run the XEMM engine with {} markets selected; it is single-market only",
            markets.len()
        );
    }
    let mode = if cfg.live.dry_run { "dry-run" } else { "live" };
    if !cfg.live.dry_run {
        warn!("livebot mode=LIVE: placing REAL orders on Aster + Lighter with REAL funds.");
    }
    info!("livebot starting: mode={mode}, {} market(s)", markets.len());

    // --- resolve specs + classify pair eligibility ---
    let specs = rest_specs::build_market_specs_with_bases(
        &markets,
        cfg.live.partials.lighter_min_notional,
        &cfg.live.aster.base_url,
        &cfg.live.hyperliquid.base_url,
    )
    .await?;
    let eligibility = classify_markets(&specs, cfg).await;
    let market_ids: Vec<MarketId> = specs.iter().map(|s| s.market_id.clone()).collect();
    let eligible_count = eligibility.values().filter(|&&e| e).count();
    let elig_basis = cfg.live.partials.policy.as_str();
    info!("pair eligibility: {eligible_count}/{} markets tradeable — {elig_basis}", specs.len());
    if eligible_count == 0 {
        warn!("no eligible pairs under the partial policy — the bot will quote nothing");
    }

    // --- market-data hot path: registry (with strategy wakeup + dirty bitset) + ingest threads + watchdog ---
    let wake = Arc::new(Notify::new());
    let dirty = Arc::new(crate::hotpath::dirty::DirtyMarkets::new(market_ids.len()));
    let registry = Arc::new(VenueRegistry::with_wake_and_dirty(&market_ids, wake.clone(), dirty.clone()));
    let gate = Arc::new(TradingGate::new());
    let shutdown = CancellationToken::new();
    let feeds_shutdown = CancellationToken::new();

    let mut reconnect_map: HashMap<(MarketId, VenueTag), ReconnectHandle> = HashMap::new();
    let mut venue_handles = Vec::new();
    let spec_by_id: HashMap<MarketId, &MarketSpec> = specs.iter().map(|s| (s.market_id.clone(), s)).collect();
    let aster_ws = crate::connectors::aster::ws_root(&cfg.live.aster.base_url);
    let lighter_ws = crate::lighter::ws::stream_url(&cfg.live.hyperliquid.base_url);
    let mut core_hint = 0usize;
    for m in &markets {
        let id = m.id();
        let scale = if cfg.live.quote.use_hot_integer_math {
            spec_by_id.get(&id).map(|s| scale::MarketScale::from_spec(s))
        } else {
            None
        };
        for (venue, ws_url, symbol) in [
            (VenueTag::Aster, &aster_ws, m.aster_symbol.to_lowercase()),
            (
                VenueTag::Hyperliquid,
                &lighter_ws,
                spec_by_id
                    .get(&id)
                    .map(|s| format!("{}:{}", s.lighter_market_id, s.hl_coin))
                    .expect("build_market_specs resolves every market or fails"),
            ),
        ] {
            let cell = registry.cell(&id, venue).expect("registry has every cell");
            let handle = ReconnectHandle::new();
            let notify = handle.notify();
            reconnect_map.insert((id.clone(), venue), handle);
            venue_handles.push(spawn_venue_thread(
                venue, ws_url.clone(), symbol, id.clone(), cell, notify, feeds_shutdown.clone(), Some(core_hint),
                scale.clone(),
            ));
            core_hint += 1;
        }
    }

    let book_check_reconnect = reconnect_map.clone();
    let watchdog_stop = Arc::new(AtomicBool::new(false));
    let watchdog_handle = {
        let (reg, g, stop) = (registry.clone(), gate.clone(), watchdog_stop.clone());
        let book_stale_ms = cfg.live.max_book_staleness_ms;
        thread::Builder::new()
            .name("livebot-watchdog".into())
            .spawn(move || run_watchdog(reg, g, reconnect_map, WATCHDOG_STALE_MS, book_stale_ms, WATCHDOG_SCAN, stop))
            .expect("spawn watchdog")
    };

    let book_check_handle = if cfg.book_check.enabled {
        let reg = registry.clone();
        let targets: Vec<BookCheckTarget> = markets
            .iter()
            .flat_map(|m| {
                let id = m.id();
                [
                    BookCheckTarget { market: id.clone(), venue: VenueTag::Aster, symbol: m.aster_symbol.to_uppercase() },
                    BookCheckTarget {
                        market: id.clone(),
                        venue: VenueTag::Hyperliquid,
                        symbol: spec_by_id
                            .get(&id)
                            .map(|s| s.lighter_market_id.to_string())
                            .unwrap_or_else(|| "0".into()),
                    },
                ]
            })
            .collect();
        let params = BookCheckParams {
            tolerance_bps: cfg.book_check.tolerance_bps,
            consecutive_breaches: cfg.book_check.consecutive_breaches,
            depth_limit: cfg.book_check.depth_limit,
            interval: std::time::Duration::from_secs(cfg.book_check.interval_secs.max(1)),
            max_quote_staleness_ms: cfg.live.max_book_staleness_ms,
            max_concurrent_requests: cfg.book_check.max_concurrent_requests,
            max_rest_snapshot_age_ms: cfg.book_check.max_rest_snapshot_age_ms,
            aster_base_url: cfg.live.aster.base_url.clone(),
            hl_base_url: cfg.live.hyperliquid.base_url.clone(),
        };
        let sd = feeds_shutdown.clone();
        Some(
            thread::Builder::new()
                .name("livebot-book-check".into())
                .spawn(move || run_book_check(reg, targets, book_check_reconnect, params, sd))
                .expect("spawn book-check"),
        )
    } else {
        None
    };

    // --- cold plane: account state + journal ---
    let account = AccountState::default();
    let (journal, jrx) = Journal::channel();
    // Stem `runs/bot-HYPE` → journal `runs/bot-HYPE-journal.jsonl`.
    let journal_path = crate::live_report::inferred_journal_path(&stem);
    if let Some(dir) = journal_path.parent() {
        std::fs::create_dir_all(dir).ok();
    }
    let journal_file = std::fs::OpenOptions::new().create(true).append(true).open(&journal_path)?;
    let (journal_done_tx, journal_done_rx) = oneshot::channel();
    let journal_thread = thread::Builder::new().name("livebot-journal".into()).spawn(move || {
        let result = tokio::runtime::Builder::new_current_thread().enable_all().build()
            .map_err(|e| e.to_string()).and_then(|rt| {
                rt.block_on(run_journal_writer(jrx, std::io::BufWriter::new(journal_file))).map_err(|e| e.to_string())
            });
        let _ = journal_done_tx.send(result);
    })?;

    // --- execution plane: bounded command queues ---
    let (exec_tx, exec_rx) = mpsc::channel::<ExecCommand>(CMD_QUEUE_DEPTH);
    // Priority lane for acked cancels + flattens (see exec::command::is_priority_cmd):
    // depth mirrors the strategy's EXEC_CANCEL_RESERVE. run() keeps `exec_prio_tx` alive
    // for its whole lifetime so the worker's priority arm never closes early.
    let (exec_prio_tx, exec_prio_rx) = mpsc::channel::<ExecCommand>(64);
    let (hedge_tx, hedge_rx) = mpsc::channel::<HedgeCommand>(CMD_QUEUE_DEPTH);
    let (events_tx, events_rx) = mpsc::channel::<ExecEvent>(CMD_QUEUE_DEPTH);
    let (maker_fill_tx, maker_fill_rx) = mpsc::channel::<AsterFill>(256);

    // --- circuit-breaker trip latch (persistent across restarts) ---
    // If a prior run tripped the cumulative-loss breaker it left a latch file named after this
    // run's stem. Refuse to start (bail) until an operator clears it (scripts/reset_breaker.py) —
    // checked BEFORE any live execution setup so a tripped bot can never resume trading. No-op for
    // a fresh stem.
    super::breaker::check_startup(&stem)?;
    super::breaker::check_active_session(&stem)?;

    // --- bootstrap + execution/cold planes ---
    // Real workers + the account reconciler (clean-start + cold backstop) + the Aster user (fill)
    // stream feeding the maker-fill channel. The initial reconcile gates clean-start.
    let mut aux_tasks: Vec<tokio::task::JoinHandle<()>> = Vec::new();
    let (worker_task, stream_liveness, shutdown_recon) = setup_live_planes(
        cfg, &specs, &account, exec_rx, exec_prio_rx, hedge_rx, events_tx, maker_fill_tx, feeds_shutdown.clone(), &mut aux_tasks, &journal,
    )
    .await?;

    // --- strategy ---
    let session = SessionId::random();
    super::breaker::start_active_session(&stem, session.as_str(), &market_ids)?;
    let drain_control = Arc::new(super::exec::command::DrainControl::default());
    // The global TradingGate stays wired to the watchdog (reconnect nudging + the OPEN/CLOSED
    // gauge log); the strategy gates per-market off each pair's own feed freshness, so one
    // stale feed no longer halts quoting on every pair.
    let mut strat = Strategy::new(
        cfg.clone(), &specs, &eligibility, registry.clone(), account.clone(),
        journal.clone(), session, exec_tx.clone(), hedge_tx.clone(),
    );
    strat.set_exec_prio_lane(exec_prio_tx.clone());
    strat.set_drain_control(drain_control.clone());
    strat.set_hedge_readiness(shutdown_recon.hedge_readiness());
    strat.mark_clean_start();
    // Seed predicted positions from the startup snapshot (published by the initial reconcile in
    // setup_live_planes). Without this, a non-neutral restart froze quoting forever with the
    // imbalance unhedged: predicted started empty, so the orphan cross-check treated every
    // snapshot as a transient venue read. Refuses stale/absent snapshots.
    strat.adopt_reported_positions(mono_now_ns());
    // Arm the cumulative-loss circuit breaker: it halts via this same shutdown token and persists a
    // trip latch at this run's per-stem path (the startup guard above reads the same path). Inert
    // unless live.circuit_breaker.enabled.
    strat.arm_circuit_breaker(super::breaker::trip_path(&stem), shutdown.clone());
    // In-memory trip backstop: guarantees an error return at shutdown even if the persistent
    // latch write fails (unwritable runs/ dir) — see the shutdown check at the end of run().
    let breaker_tripped_flag = Arc::new(AtomicBool::new(false));
    strat.set_trip_flag(breaker_tripped_flag.clone());
    strat.set_pause_flag(pause);
    strat.set_rights(rights);
    strat.set_dirty(dirty);
    strat.set_user_stream(stream_liveness); // freeze quoting if the Aster fill stream silently dies
    // --- strategy ---
    // Spawned on a DEDICATED OS thread with its own single-threaded tokio runtime, so the
    // strategy loop's latency-critical wake/reprice/fill→hedge path is isolated from the
    // reconciler, user stream, journal writer, and exec workers on the main runtime. Core-
    // pinned to the next available core after the ingest threads.
    let strat_core_hint = core_hint;
    let strat_shutdown = shutdown.clone();
    let (strat_done_tx, mut strat_done_rx) = oneshot::channel::<std::result::Result<(), String>>();
    let strat_handle = thread::Builder::new()
        .name(STRATEGY_THREAD.into())
        .spawn(move || {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
                crate::hotpath::maybe_pin_core(Some(strat_core_hint));
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("strategy runtime");
                rt.block_on(run_strategy(strat, wake.clone(), events_rx, maker_fill_rx, strat_shutdown))
            }))
            .map_err(|panic| panic_payload_message(panic.as_ref()))
            .and_then(|result| result.map_err(|e| e.to_string()));
            let _ = strat_done_tx.send(result);
        })
        .expect("spawn strategy thread");
    info!("livebot running. Journal/latches: {}.", stem.display());

    // --- main loop: wait for a stop request or an internal safety halt ---
    let mut strategy_done_seen = false;
    let mut strategy_error: Option<anyhow::Error> = None;
    tokio::select! {
        _ = shutdown.cancelled() => {
            warn!("internal shutdown requested: coordinating safety shutdown");
        }
        strat_result = &mut strat_done_rx => {
            strategy_done_seen = true;
            match strat_result {
                Ok(Ok(())) if shutdown.is_cancelled() => {}
                Ok(Ok(())) => {
                    warn!("strategy thread exited unexpectedly; coordinating safety shutdown");
                    strategy_error = Some(anyhow::anyhow!("strategy thread exited unexpectedly"));
                    shutdown.cancel();
                }
                Ok(Err(msg)) => {
                    warn!("strategy thread panicked: {msg}; coordinating safety shutdown");
                    strategy_error = Some(anyhow::anyhow!("strategy thread panicked: {msg}"));
                    shutdown.cancel();
                }
                Err(_) => {
                    warn!("strategy thread supervision channel closed; coordinating safety shutdown");
                    strategy_error = Some(anyhow::anyhow!("strategy thread supervision channel closed"));
                    shutdown.cancel();
                }
            }
        }
        _ = stop.cancelled() => {
            info!("stop requested: shutting down");
            shutdown.cancel();
        }
    }

    // Quiesce first; private streams/reconciliation remain alive through execution drain.
    shutdown.cancel();
    if !strategy_done_seen {
        let acknowledged = tokio::time::timeout(Duration::from_secs(5), async {
            while !drain_control.quiesced.load(Ordering::Acquire) { tokio::time::sleep(Duration::from_millis(5)).await; }
        }).await.is_ok();
        if !acknowledged && strategy_error.is_none() {
            strategy_error = Some(anyhow::anyhow!("strategy did not acknowledge quiescence"));
        }
    }
    if cfg.live.shutdown_cancel_all { send_exec_safety(&exec_tx, ExecCommand::CancelAllBot, "shutdown CancelAllBot").await; }
    send_exec_safety(&exec_tx, ExecCommand::Barrier { completion: drain_control.maker_barrier.clone() }, "maker command barrier").await;
    // Wait (bounded) for the strategy's shutdown fill-drain before stopping the workers:
    // a fill queued at ctrl-c raced the old immediate worker Shutdown and could be dropped
    // unhedged. The strategy's execution drain is bounded at 65s; a panic fires the supervision
    // channel immediately (the send is outside the unwind), so crash paths don't wait.
    if !strategy_done_seen {
        match tokio::time::timeout(Duration::from_secs(70), &mut strat_done_rx).await {
            Ok(res) => {
                strategy_done_seen = true;
                match res {
                    Ok(Ok(())) => {}
                    Ok(Err(msg)) => {
                        warn!("strategy thread panicked: {msg}");
                        if strategy_error.is_none() {
                            strategy_error = Some(anyhow::anyhow!("strategy thread panicked: {msg}"));
                        }
                    }
                    Err(_) => {
                        if strategy_error.is_none() {
                            strategy_error =
                                Some(anyhow::anyhow!("strategy thread supervision channel closed"));
                        }
                    }
                }
            }
            Err(_) => {
                warn!("strategy did not finish its bounded execution drain");
                if strategy_error.is_none() { strategy_error = Some(anyhow::anyhow!("execution drain deadline exceeded")); }
            }
        }
    }
    send_exec_safety(&exec_tx, ExecCommand::Shutdown, "exec Shutdown").await;
    send_hedge_safety(&hedge_tx, HedgeCommand::Shutdown, "hedge Shutdown").await;
    let workers_ok = matches!(tokio::time::timeout(Duration::from_secs(65), worker_task).await, Ok(Ok(Ok(()))));
    if !workers_ok && strategy_error.is_none() { strategy_error = Some(anyhow::anyhow!("execution worker drain incomplete")); }
    let final_verified = workers_ok && shutdown_verify(&shutdown_recon, &journal, &stem, &market_ids).await;
    feeds_shutdown.cancel();
    watchdog_stop.store(true, Ordering::Release);
    for h in venue_handles {
        let _ = h.join();
    }
    let _ = watchdog_handle.join();
    if let Some(h) = book_check_handle {
        let _ = h.join();
    }
    if let Err(panic) = strat_handle.join() {
        let msg = panic_payload_message(panic.as_ref());
        warn!("strategy thread panicked outside supervisor: {msg}");
        if strategy_error.is_none() {
            strategy_error = Some(anyhow::anyhow!("strategy thread panicked: {msg}"));
        }
    }
    if !strategy_done_seen {
        match strat_done_rx.try_recv() {
            Ok(Ok(())) => {}
            Ok(Err(msg)) => {
                warn!("strategy thread panicked: {msg}");
                if strategy_error.is_none() {
                    strategy_error = Some(anyhow::anyhow!("strategy thread panicked: {msg}"));
                }
            }
            Err(oneshot::error::TryRecvError::Empty) => {}
            Err(oneshot::error::TryRecvError::Closed) => {
                if strategy_error.is_none() {
                    strategy_error = Some(anyhow::anyhow!("strategy thread supervision channel closed"));
                }
            }
        }
    }
    for h in aux_tasks {
        let _ = h.await;
    }
    let journal_healthy = journal.healthy();
    let trip_backup = journal.clone();
    drop(journal);
    // The backup must not keep the record sender open during the writer drain.
    let retry_trip = trip_backup.detached_control();
    drop(trip_backup);
    let journal_ok = match tokio::time::timeout(Duration::from_secs(5), journal_done_rx).await {
        Ok(Ok(Ok(()))) => { let _ = journal_thread.join(); true }
        _ => { drop(journal_thread); false }
    };
    if breaker_tripped_flag.load(Ordering::Acquire) && !journal_ok {
        let (done_tx, done_rx) = oneshot::channel();
        thread::spawn(move || { let _ = done_tx.send(retry_trip.persist_trip_cold()); });
        let _ = tokio::time::timeout(Duration::from_secs(5), done_rx).await;
    }
    if final_verified && workers_ok && strategy_error.is_none() && journal_ok && journal_healthy {
        super::breaker::finish_active_session(&stem)?;
    }
    if !journal_ok || !journal_healthy { bail!("journal did not drain cleanly; active-session marker retained"); }
    if !final_verified { bail!("final positions/orders could not be verified neutral; active-session marker retained"); }

    if let Some(e) = strategy_error {
        return Err(e);
    }
    // A circuit-breaker trip rides the graceful-shutdown path above; without this guard it
    // returns Ok — indistinguishable from a clean stop — and the supervisor restarts the bot
    // straight into the startup latch (observed 2026-07-04). The startup guard barred any
    // pre-existing latch, so "latch exists at shutdown" ⇔ "the breaker fired THIS run".
    super::breaker::check_shutdown(&stem)?;
    if breaker_tripped_flag.load(Ordering::Acquire) { bail!("circuit breaker tripped during this run"); }

    info!("livebot stopped.");
    Ok(())
}

/// Classify every market's pair eligibility against a REST-fetched Lighter reference mid.
///
/// The strict Class-A-only filter is a REAL-MONEY orphan-leg safety: it bars pairs where a
/// sub-minimum partial Aster fill could be un-hedgeable on Lighter. The configured policy
/// applies (with the `accumulate_sub_min` fallback noted below).
async fn classify_markets(specs: &[MarketSpec], cfg: &Config) -> HashMap<MarketId, bool> {
    // Sub-min handling IS implemented now (a sub-min Aster partial ACCUMULATES into pending
    // inventory and hedges on Lighter once the net clears the minimum; a genuinely stuck residual is
    // flattened reduce-only on Aster, and the reconciler backstop neutralizes anything else), so a
    // Class-B pair's sub-min partial can no longer orphan. `accumulate_sub_min` therefore safely
    // admits Class A and B; `strict` still restricts to Class A.
    let effective_policy = cfg.live.partials.policy;
    let client = rest_book::client().ok();
    let mut out = HashMap::new();
    for s in specs {
        let ref_px = match &client {
            Some(c) => rest_book::fetch_lighter_book_from_base(
                c,
                &cfg.live.hyperliquid.base_url,
                s.lighter_market_id,
                cfg.book_check.depth_limit,
            )
                .await
                .ok()
                .and_then(|b| b.mid()),
            None => None,
        };
        let eligible = match ref_px {
            Some(px) => {
                let c = classify(s, px, cfg.quote.desired_notional);
                let ok = is_eligible(c.class, effective_policy);
                info!("  {} class {} (aster_min_fill {}, hl_min_hedge {}) -> {}", s.market_id, c.class.as_str(), c.aster_min_fill_qty, c.hl_min_hedge_qty, if ok { "eligible" } else { "EXCLUDED" });
                ok
            }
            None => {
                warn!("  {} reference price unavailable -> EXCLUDED (conservative)", s.market_id);
                false
            }
        };
        out.insert(s.market_id.clone(), eligible);
    }
    out
}

/// Build + spawn ALL live planes: the venue workers, the account reconciler
/// (initial reconcile for clean-start + a cold backstop loop), and the Aster user (fill) stream.
/// Signing uses `aster.env`/`lighter.env` in `live` (roles derived from the keys, not the env
/// field names; see [`super::exec::creds`]) and the dry-run identity in a dry run.
/// Returns the worker task, the user-stream liveness stamp and the shutdown reconciler. Clean
/// start is then established (quoting is still gated per-market on feed freshness and position
/// reconciliation).
#[allow(clippy::too_many_arguments)]
async fn setup_live_planes(
    cfg: &Config,
    specs: &[MarketSpec],
    account: &AccountState,
    exec_rx: mpsc::Receiver<ExecCommand>,
    exec_prio_rx: mpsc::Receiver<ExecCommand>,
    hedge_rx: mpsc::Receiver<HedgeCommand>,
    events_tx: mpsc::Sender<ExecEvent>,
    maker_fill_tx: mpsc::Sender<AsterFill>,
    shutdown: CancellationToken,
    aux: &mut Vec<tokio::task::JoinHandle<()>>,
    journal: &Journal,
) -> Result<(
    tokio::task::JoinHandle<Result<()>>,
    Arc<super::userstream::StreamLiveness>,
    super::reconcile::Reconciler,
)> {
    use std::path::Path;

    use super::exec::aster::{run_aster_worker, AsterRest};
    use super::exec::creds::venue_creds;
    use super::exec::hyperliquid::{run_hl_worker, HlExchange};
    use super::exec::sign::{AsterSigner, EvmAsterSigner};
    use super::reconcile::Reconciler;
    use super::scale::MarketScale;
    use super::userstream::{run_aster_user_stream, StreamLiveness};

    // Load + role-resolve credentials; build the signers once (shared across clients).
    let (acreds, hcreds) = venue_creds(cfg.live.dry_run)?;
    let aster_signer: Arc<dyn AsterSigner> = Arc::new(EvmAsterSigner::new(acreds.user, acreds.signer, acreds.key)?);

    // Per-market wire data shared by all Aster client instances.
    let mut scales: HashMap<MarketId, (MarketScale, String)> = HashMap::new();
    for s in specs {
        scales.insert(s.market_id.clone(), (MarketScale::from_spec(s), s.aster_symbol.clone()));
    }
    for s in specs {
        if s.lighter_market_id == 0 {
            anyhow::bail!(
                "Lighter market id not resolved for market {} (symbol {}); refusing to start live",
                s.market_id.0,
                s.hl_coin
            );
        }
    }
    let new_aster = || {
        AsterRest::new(
            cfg.live.aster.base_url.clone(),
            aster_signer.clone(),
            scales.clone(),
            cfg.live.aster.deadman_countdown_ms,
            cfg.live.aster.rate_limit_backoff_ms,
            cfg.live.aster.effective_max_rest_requests_per_minute(),
        )
    };
    let signers_dir = Path::new(&cfg.live.hyperliquid.signers_dir);
    let hedge = HlExchange::new_lighter(
        cfg.live.hyperliquid.base_url.clone(),
        signers_dir,
        hcreds,
        specs,
        cfg.live.hyperliquid.fill_timeout_ms,
        cfg.live.hyperliquid.ws_account_max_age_ms,
    )
    .await?;

    // Separate client instances per plane (each is a cheap reqwest client + shared signer Arc):
    // writes (worker), reads (reconciler), listenKey+WS (user stream).
    let worker_aster = new_aster()?;
    let worker_hl = hedge.clone();
    let recon = Reconciler::new(new_aster()?, hedge.clone(), specs, cfg.live.max_book_staleness_ms);
    // A second reconciler instance reserved for SHUTDOWN verification (cheap: a reqwest client +
    // the shared signer Arc). The main one is consumed by its cold loop task and dies with the
    // shutdown token; this one performs the post-drain cancel-confirmation + residual sweep.
    let shutdown_recon = Reconciler::new(new_aster()?, hedge.clone(), specs, cfg.live.max_book_staleness_ms);
    let stream_aster = new_aster()?;
    let mut sym_to_market: HashMap<String, MarketId> = HashMap::new();
    for s in specs {
        sym_to_market.insert(s.aster_symbol.to_uppercase(), s.market_id.clone());
    }

    // Pre-warm the worker connections (establish TLS now, off the hot path) so the FIRST real
    // order / hedge doesn't pay a handshake — latency matters most on the very first fill.
    aux.extend(worker_hl.start_private_streams(shutdown.clone()));
    let _ = worker_aster.balance().await;
    for s in specs {
        worker_hl
            .wait_ready(&s.market_id, Duration::from_secs(15))
            .await
            .map_err(|e| anyhow::anyhow!("Lighter websocket warmup failed for {}: {e:#}", s.market_id.0))?;
    }
    let _ = worker_hl.clearinghouse_state().await;

    // LEVERAGE GATE: ensure REAL venue leverage == 1 on BOTH venues for every traded market, else
    // BAIL. This is the actual exchange leverage (NOT the config [capital] soft cap, which only sizes
    // orders) — a leftover 5x/20x amplifies exposure beyond the deposited capital. Aster has no
    // EVM-signed set-leverage endpoint, so we VERIFY it (operator sets it once on the Aster UI).
    // Lighter is also verified read-only from the account payload's per-market
    // initial_margin_fraction. Done before any trading.
    for s in specs {
        let aster_lev = worker_aster
            .get_leverage(&s.market_id)
            .await
            .map_err(|e| anyhow::anyhow!("Aster leverage read failed for {}: {e:#}", s.market_id.0))?;
        if aster_lev != 1 {
            anyhow::bail!(
                "Aster leverage for {} is {aster_lev}x (expected 1x) — set {} to 1x cross on the Aster UI and restart",
                s.market_id.0, s.aster_symbol
            );
        }
        let lighter_lev = worker_hl
            .get_leverage(&s.market_id)
            .await
            .map_err(|e| anyhow::anyhow!("Lighter leverage read failed for {}: {e:#}", s.market_id.0))?;
        if lighter_lev != rust_decimal::Decimal::ONE {
            anyhow::bail!(
                "Lighter leverage for {} is {lighter_lev}x (expected 1x) — set {} to 1x cross on the Lighter UI and restart",
                s.market_id.0, s.hl_coin
            );
        }
        info!("leverage gate: {} = 1x (Aster verified, Lighter verified)", s.market_id.0);
    }

    // Spawn the venue workers (writes) as SEPARATE tasks: a join! in one task would
    // serialize them, letting an Aster ECDSA sign (or response parse) head-of-line
    // block a hedge dequeue exactly when a fill just landed. The returned handle is a
    // supervisor that only awaits the two real tasks at shutdown.
    let etx = events_tx.clone();
    let aster_worker_task = tokio::spawn(run_aster_worker(exec_rx, exec_prio_rx, etx, worker_aster));
    let hl_worker_task = tokio::spawn(run_hl_worker(hedge_rx, events_tx.clone(), worker_hl, journal.clone()));
    let worker_task = tokio::spawn(async move {
        let (aster, hedge) = tokio::join!(aster_worker_task, hl_worker_task);
        aster.map_err(|e| anyhow::anyhow!("Aster worker failed: {e}"))?;
        hedge.map_err(|e| anyhow::anyhow!("hedge worker failed: {e}"))?;
        Ok(())
    });

    // Refuse to trade live in hedge mode (the bot assumes one-way).
    recon.assert_one_way().await?;

    // Enforce the CLEAN-START invariant BEFORE the initial reconcile and before any quoting: cancel
    // stray orders on our symbols and poll until the book is clean (or bail if require_clean_start),
    // so a fast startup can never quote while prior-run orders still rest.
    recon
        .ensure_clean_start(cfg.live.startup_cancel_all, cfg.live.require_clean_start)
        .await?;

    // Initial reconcile — publishes the first snapshot; failure aborts clean-start.
    let snap = recon
        .reconcile_and_publish(account)
        .await
        .map_err(|e| anyhow::anyhow!("initial account reconcile failed (cannot clean-start live): {e:#}"))?;
    info!(
        "initial reconcile: aster_avail=${} hl_withdrawable=${} aster_pos={} hl_pos={} open_orders={}",
        snap.aster_available_usd, snap.hl_withdrawable_usd, snap.aster_positions.len(), snap.hl_positions.len(), snap.open_orders.len()
    );

    // Cold reconcile loop: refresh well inside `max_account_snapshot_age_ms`.
    let interval = Duration::from_millis((cfg.live.max_account_snapshot_age_ms / 2).clamp(500, 2000) as u64);
    aux.push(tokio::spawn(recon.run(account.clone(), shutdown.clone(), interval, events_tx)));

    // Aster user (fill) stream → maker-fill channel. Keep a clone of the liveness stamp to hand
    // to the strategy so it can freeze quoting if the stream silently dies.
    let liveness = Arc::new(StreamLiveness::default());
    aux.push(tokio::spawn(run_aster_user_stream(stream_aster, sym_to_market, maker_fill_tx, liveness.clone(), shutdown.clone())));

    Ok((worker_task, liveness, shutdown_recon))
}

/// Post-drain shutdown verification. Re-cancels + polls `openOrders` for
/// bot-prefixed strays (a failure there is only warned), then takes one final snapshot to
/// report/persist any residual positions — the XEMM Aster client reads no userTrades, so a
/// late fill is detected as a position. Every step is timeout-bounded so shutdown never hangs.
/// Returns `true` only when the final snapshot shows no bot order and every market net-flat (a
/// delta-neutral pair left open is normal). A failed snapshot or report write, a stray order or
/// a NET imbalance returns `false`: `run` returns an error and keeps the active-session marker.
async fn shutdown_verify(
    recon: &super::reconcile::Reconciler,
    journal: &Journal,
    stem: &std::path::Path,
    markets: &[MarketId],
) -> bool {
    // 1. Re-cancel + poll for bot-prefixed strays. require_clean_start=false keeps it non-fatal
    //    (a still-dirty book is loudly warned inside, and the deadman countdown remains armed).
    match tokio::time::timeout(Duration::from_secs(20), recon.ensure_clean_start(true, false)).await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => warn!("shutdown verification incomplete: cancel/verify step failed: {e:#}"),
        Err(_) => warn!("shutdown verification incomplete: cancel/verify step timed out after 20s"),
    }
    // 2. One final snapshot: residual positions + open-order check.
    let snap = match tokio::time::timeout(Duration::from_secs(10), recon.snapshot()).await {
        Ok(Ok(s)) => s,
        Ok(Err(e)) => {
            tracing::error!("shutdown verification incomplete: final snapshot read failed: {e:#}");
            return false;
        }
        Err(_) => {
            tracing::error!("shutdown verification incomplete: final snapshot read timed out after 10s");
            return false;
        }
    };
    let orders_verified_empty = !snap.open_orders.iter().any(|o| o.is_bot_order());
    if !orders_verified_empty {
        tracing::error!("CRITICAL: bot orders still resting after shutdown cancel (deadman countdown will reap them)");
    }
    let residuals = super::breaker::residual_positions(&snap, markets);
    let now_ns = mono_now_ns();
    for line in &residuals {
        if line.net_qty == rust_decimal::Decimal::ZERO {
            tracing::info!(
                "shutdown residual on {}: delta-neutral pair left open (aster={} lighter={}) — \
                 documented-normal: graceful shutdown cancels orders but leaves positions",
                line.market, line.aster_qty, line.hl_qty
            );
        } else {
            tracing::error!(
                "CRITICAL shutdown residual on {}: NET imbalance (aster={} lighter={} net={}) — \
                 unhedged exposure; automatic restart remains blocked",
                line.market, line.aster_qty, line.hl_qty, line.net_qty
            );
        }
        journal.record(
            now_ns,
            "shutdown_residual",
            Some(line.market.clone()),
            serde_json::json!({
                "aster_qty": line.aster_qty.to_string(),
                "hl_qty": line.hl_qty.to_string(),
                "net_qty": line.net_qty.to_string(),
                "orders_verified_empty": orders_verified_empty,
            }),
        );
    }
    // 3. Persist the residual report next to the trip latch (atomic, overwritten per shutdown).
    let rec = super::breaker::ResidualRecord {
        ts_utc: Utc::now().to_rfc3339(),
        orders_verified_empty,
        residuals,
    };
    let path = super::breaker::residual_path(stem);
    if let Err(e) = super::breaker::write_residual(&path, &rec) {
        warn!("failed to write shutdown residual report to {}: {e:#}", path.display());
        return false;
    }
    orders_verified_empty && rec.residuals.iter().all(|r| r.net_qty == rust_decimal::Decimal::ZERO)
}
