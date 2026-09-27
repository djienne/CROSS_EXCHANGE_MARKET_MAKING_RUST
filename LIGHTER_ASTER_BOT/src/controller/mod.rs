//! The `run` command: one process running both engines for one market. XEMM quotes; when an
//! arbitrage passes the taker's entry gate, XEMM pulls its quotes and hands the execution
//! rights to the taker, which trades and hands them back. It replaces the retired
//! orchestrator.py and its child processes.
//!
//! * [`risk`]: the cross-engine loss stops (equity drawdown, realized trade PnL).
//! * `supervisor`: the loop — engine tasks, loss stops, network pause, halts.
//! * `engines`: the real engine tasks and status pollers behind the supervisor.

pub(crate) mod engines;
pub mod risk;
pub(crate) mod supervisor;

use std::fs::File;
use std::io::{Read, Seek, Write};
use std::path::{Path, PathBuf};

use anyhow::{bail, ensure, Context, Result};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::Deserialize;
use serde_json::{Map, Value};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::config::LiveMode;

/// Runtime state (journals, latches, controller files) lives here, relative to the working
/// directory: the crate dir, bind-mounted as /app/runs in Docker.
pub const RUNS_DIR: &str = "runs";

/// `[controller]` of bot.toml.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct ControllerCfg {
    /// Status poll and loss-stop cadence.
    pub poll_sec: u64,
    /// Cross-engine loss stop, inclusive (equity drawdown or realized trade PnL).
    pub max_loss_usdc: Decimal,
    /// A persisted equity baseline not refreshed for this long is discarded; `0` keeps it.
    pub baseline_max_gap_hours: u64,
}

impl Default for ControllerCfg {
    fn default() -> Self {
        Self {
            poll_sec: 15,
            max_loss_usdc: Decimal::from(15),
            baseline_max_gap_hours: 48,
        }
    }
}

impl ControllerCfg {
    pub fn validate(&self) -> Result<()> {
        // The loss stops run once per poll.
        ensure!((1..=60).contains(&self.poll_sec), "controller.poll_sec must be within 1..=60");
        ensure!(self.max_loss_usdc > Decimal::ZERO, "controller.max_loss_usdc must be > 0");
        Ok(())
    }
}

/// bot.toml: `[controller]`, plus each engine's full config under `[taker]` and `[maker]`
/// (the standalone `taker ...` and XEMM commands read their own table of the same file), and
/// the simulated venues of `--mode dry-run` under `[dry_run]`.
pub struct BotConfig {
    pub controller: ControllerCfg,
    pub taker: crate::taker::config::Config,
    pub maker: crate::config::Config,
    pub dry_run: Option<crate::dryrun::DryRunCfg>,
}

impl BotConfig {
    pub fn load(path: &Path) -> Result<Self> {
        let text = std::fs::read_to_string(path).with_context(|| format!("reading config {}", path.display()))?;
        let mut value: toml::Value = toml::from_str(&text).with_context(|| format!("parsing config {}", path.display()))?;
        let table = value.as_table_mut().context("config is not a TOML table")?;
        let dry_run = table.remove("dry_run");
        let mut take = |name: &str| table.remove(name).with_context(|| format!("{} has no [{name}] table", path.display()));
        let (controller, taker, maker) = (take("controller")?, take("taker")?, take("maker")?);
        if let Some(extra) = table.keys().next() {
            bail!("unknown top-level table [{extra}] in {}", path.display());
        }
        let controller: ControllerCfg = crate::config::strict_from_toml(controller).context("[controller]")?;
        controller.validate()?;
        let dry_run: Option<crate::dryrun::DryRunCfg> =
            dry_run.map(crate::config::strict_from_toml).transpose().context("[dry_run]")?;
        if let Some(dry_run) = &dry_run {
            dry_run.validate()?;
        }
        Ok(Self {
            controller,
            taker: crate::taker::config::Config::from_table(taker).context("[taker]")?,
            maker: crate::config::Config::from_table(maker).context("[maker]")?,
            dry_run,
        })
    }

    /// Both engines' entry for `market`, which must name the same instruments on the same
    /// venues.
    pub fn select(&self, market: &str) -> Result<(Vec<crate::taker::config::MarketCfg>, Vec<crate::config::MarketCfg>)> {
        let taker = self.taker.select_markets(Some(market));
        let maker = self.maker.select_markets(Some(market));
        let (Some(t), Some(m), 1, 1) = (taker.first(), maker.first(), taker.len(), maker.len()) else {
            bail!("market {market} must appear exactly once in both [[taker.markets]] and [[maker.markets]]");
        };
        ensure!(t.id().0 == market && m.id().0 == market, "market ids must be spelled {market} in both engine configs");
        ensure!(t.aster_symbol.eq_ignore_ascii_case(&m.aster_symbol) && t.lighter_symbol.eq_ignore_ascii_case(&m.hl_coin)
            && t.hedge_venue == m.hedge_venue, "taker and maker configs name different instruments for {market}");
        ensure!(self.maker.live.enabled, "[maker.live] enabled must be true under `run`");
        ensure!(self.taker.pnl.enabled && self.maker.live.circuit_breaker.enabled,
            "`run` keeps both engines' own loss stops: [taker.pnl] and [maker.live.circuit_breaker] need enabled = true");
        Ok((taker, maker))
    }
}

/// `run`: both engines for `market`, sharing the execution rights in memory. Returns `Err` on a
/// safe halt or an unresolved engine stop, `Ok` after a clean signal-driven stop; a halted
/// dry run instead stays parked until stopped.
pub async fn run(config: &Path, market: &str, mode: LiveMode, ack_breaker: bool, reset_baseline: bool, stop: CancellationToken) -> Result<()> {
    let cfg = BotConfig::load(config)?;
    run_with(cfg, Path::new(RUNS_DIR), market, mode, ack_breaker, reset_baseline, stop).await
}

/// [`run`] with the config loaded and the runs directory given.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn run_with(
    mut cfg: BotConfig,
    runs_root: &Path,
    market: &str,
    mode: LiveMode,
    ack_breaker: bool,
    reset_baseline: bool,
    stop: CancellationToken,
) -> Result<()> {
    let market = market.to_ascii_uppercase();
    let (taker_markets, maker_markets) = cfg.select(&market)?;
    let live = mode.is_real();
    // Live and dry-run never share a file.
    let runs_dir = if live { runs_root.to_path_buf() } else { runs_root.join("dry-run") };
    let _lock = lock_market(&runs_dir, &market)?;
    // Markets on one Aster symbol share its one-way position there: one live writer per symbol.
    let _aster_lock = if live { Some(lock_market(&runs_dir, &format!("ASTER-{}", maker_markets[0].aster_symbol))?) } else { None };
    let sim = if live {
        refuse_insecure_env_files()?;
        refuse_legacy_stack(&market, &[runs_root, Path::new("../runs")])?;
        None
    } else {
        let dry_run = cfg.dry_run.clone().context("--mode dry-run needs a [dry_run] table in the config")?;
        Some(crate::dryrun::start(&dry_run, &mut cfg, &maker_markets[0], &runs_dir).await?)
    };
    let parked = stop.clone();
    let result = async move {
        let files = supervisor::Files::new(&runs_dir, &market);
        let mut events = EventLog::new(files.events.clone());
        let taker_session = crate::taker::pnl::session_path(&cfg.taker.pnl, &taker_markets[0].id());
        let markers = [taker_session, crate::livebot::breaker::active_path(&files.xemm_stem)];
        if !live {
            archive_unclean_sessions(markers, &mut events)?;
        } else if let Some(marker) = markers.iter().find(|marker| marker.exists()) {
            // Found now with the fix named, rather than as an engine exit that halts the bot.
            bail!("{} is an unresolved engine session: resolve it first (RUNBOOK.md, Halts and recovery)", marker.display());
        }
        risk::check_breaker(&files.breaker, ack_breaker, reset_baseline, &mut events)?;
        if reset_baseline && files.baseline.exists() {
            std::fs::remove_file(&files.baseline)?;
            events.emit("baseline_reset", serde_json::json!({"path": files.baseline.display().to_string()}));
        }
        let taker_ledger = crate::taker::pnl::ledger_path(&cfg.taker.pnl, &taker_markets[0].id());
        let engines = engines::LiveEngines::new(&cfg, &market, taker_markets, maker_markets, files.xemm_stem.clone()).await?;
        supervisor::Supervisor::new(cfg.controller, market, mode, files, taker_ledger, engines, events, stop).run().await
    }
    .await;
    if let Some(sim) = sim {
        if let Err(error) = &result {
            // Once the venues are up, a dry run parks on any halt, a refused start included:
            // exiting would let a restart policy resume it unreviewed (or loop), and a
            // deliberate restart is the review.
            warn!("dry run halted, parked until stopped: {error:#}");
            parked.cancelled().await;
        }
        if let Err(error) = sim.save().await {
            warn!("dry run: saving the simulated venues: {error:#}");
        }
    }
    result
}

/// The engines' unclean-session markers (`markers`) a killed dry run, or an unresolved engine
/// stop, left behind. Live keeps them until an operator resolves the session against the venues'
/// records; the simulated venues' own saved state is the only record here, and the engines
/// reconcile to it at start (a halt was already recorded, and restarting is its review).
/// Archives each as `<name>.unclean.<stamp>`.
fn archive_unclean_sessions(markers: [PathBuf; 2], events: &mut EventLog) -> Result<()> {
    for marker in markers.into_iter().filter(|path| path.exists()) {
        let archived = PathBuf::from(format!("{}.unclean.{}", marker.display(), Utc::now().format("%Y%m%dT%H%M%SZ")));
        std::fs::rename(&marker, &archived).with_context(|| format!("archiving {}", marker.display()))?;
        warn!("dry run: the last run stopped uncleanly; archived {} as {}", marker.display(), archived.display());
        events.emit("dry_run_unclean_session_archived", serde_json::json!({"path": marker.display().to_string(), "archived": archived.display().to_string()}));
    }
    Ok(())
}

/// Live refuses credential files readable by group or other (mode must be 600).
fn refuse_insecure_env_files() -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut insecure = Vec::new();
        for path in crate::livebot::exec::creds::env_files() {
            if let Ok(meta) = std::fs::metadata(&path) {
                let mode = meta.permissions().mode() & 0o777;
                if mode & 0o077 != 0 {
                    insecure.push(format!("{} (mode {mode:03o})", path.display()));
                }
            }
        }
        ensure!(insecure.is_empty(), "credential env file(s) readable by group/other; chmod 600 them: {}", insecure.join(", "));
    }
    Ok(())
}

/// The two-process stack must be stopped, and its latches reviewed, before `run`: a running
/// orchestrator.py (it holds this flock for life), its breaker, and XEMM's trip latch and
/// unclean-session marker under the old `orchestrator-xemm-<M>` stem, which the new file names
/// would silently skip. Looked for in `runs/` and in the stack root's `runs/`, where the
/// orchestrator kept them. Children orphaned by a killed orchestrator hold no lock, so this
/// cannot see them. Transitional: delete once no host runs the old layout.
fn refuse_legacy_stack(market: &str, dirs: &[&Path]) -> Result<()> {
    for dir in dirs {
        let lock = dir.join(format!("orchestrator_{market}.lock"));
        if let Ok(file) = File::open(&lock) {
            if let Err(std::fs::TryLockError::WouldBlock) = file.try_lock() {
                bail!("orchestrator.py is still running ({} is locked); stop it and every bot it started", lock.display());
            }
        }
        for name in [
            format!("orchestrator_breaker_{market}.json"),
            format!("orchestrator-xemm-{market}.trip.json"),
            format!("orchestrator-xemm-{market}.active.json"),
        ] {
            let path = dir.join(name);
            if path.exists() {
                bail!("legacy latch {} (orchestrator era): review it, then archive or delete it before `run`", path.display());
            }
        }
    }
    Ok(())
}

/// Exclusive per-market lock held by every writer (`run`, `taker run`) in its runs directory:
/// two writers on one account and market break client-order-index uniqueness, nonce sequencing
/// and position accounting. The OS drops the lock with the process, so it never goes stale.
/// Ponytail: keyed by the runs directory under the working directory, so a writer started from
/// another directory is not excluded.
pub fn lock_market(runs_dir: &Path, market: &str) -> Result<File> {
    std::fs::create_dir_all(runs_dir)?;
    let path = runs_dir.join(format!("bot-{}.lock", market.to_ascii_uppercase()));
    let mut file = std::fs::OpenOptions::new().create(true).truncate(false).read(true).write(true).open(&path)
        .with_context(|| format!("opening {}", path.display()))?;
    match file.try_lock() {
        Ok(()) => {}
        Err(std::fs::TryLockError::WouldBlock) => {
            let mut holder = String::new();
            let _ = file.read_to_string(&mut holder);
            bail!("another live writer holds {} (pid {})", path.display(), holder.trim());
        }
        Err(std::fs::TryLockError::Error(error)) => return Err(error).with_context(|| format!("locking {}", path.display())),
    }
    file.set_len(0)?;
    file.rewind()?;
    writeln!(file, "{}", std::process::id())?;
    Ok(file)
}

/// A token cancelled by SIGINT, SIGTERM or SIGHUP (Ctrl-C only off Unix): every live entry
/// point takes the same graceful drain whether stopped from a terminal, by Docker or by a
/// closed session.
pub fn stop_on_signals() -> CancellationToken {
    let stop = CancellationToken::new();
    let token = stop.clone();
    tokio::spawn(async move {
        let name = stop_signal().await;
        info!("{name}: shutting down");
        token.cancel();
    });
    stop
}

async fn ctrl_c() {
    if tokio::signal::ctrl_c().await.is_err() {
        std::future::pending::<()>().await; // no handler: never "received"
    }
}

#[cfg(unix)]
async fn stop_signal() -> &'static str {
    use tokio::signal::unix::{signal, SignalKind};
    let (Ok(mut term), Ok(mut hup)) = (signal(SignalKind::terminate()), signal(SignalKind::hangup())) else {
        warn!("SIGTERM/SIGHUP handlers unavailable; only Ctrl-C drains gracefully");
        ctrl_c().await;
        return "SIGINT";
    };
    tokio::select! {
        _ = ctrl_c() => "SIGINT",
        _ = term.recv() => "SIGTERM",
        _ = hup.recv() => "SIGHUP",
    }
}

#[cfg(not(unix))]
async fn stop_signal() -> &'static str {
    ctrl_c().await;
    "ctrl-c"
}

/// Append-only JSONL record of controller events (`bot-<M>.events.jsonl`), mirrored to the
/// log. It never takes the bot down by itself: a failed write is reported, and only a file
/// unwritable 10 times in a row (e.g. a full disk) asks the supervisor to stop.
pub struct EventLog {
    path: PathBuf,
    failures: u32,
}

impl EventLog {
    pub fn new(path: PathBuf) -> Self {
        Self { path, failures: 0 }
    }

    pub fn emit(&mut self, kind: &str, details: Value) {
        info!("event {kind} {details}");
        let mut row = match details {
            Value::Object(map) => map,
            Value::Null => Map::new(),
            other => Map::from_iter([("details".to_owned(), other)]),
        };
        row.insert("timestamp".into(), Value::String(iso(Utc::now())));
        row.insert("kind".into(), Value::String(kind.to_owned()));
        match crate::taker::pnl::append_json_line(&self.path, &row, false) {
            Ok(()) => self.failures = 0,
            Err(error) => {
                self.failures += 1;
                warn!("event log write failed ({} in a row): {error:#}", self.failures);
            }
        }
    }

    pub fn unwritable(&self) -> bool {
        self.failures >= 10
    }
}

pub(crate) fn iso(at: DateTime<Utc>) -> String {
    at.to_rfc3339_opts(chrono::SecondsFormat::Micros, true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shipped_bot_toml_loads_strictly_and_rejects_stray_keys() {
        let shipped = include_str!("../../bot.toml");
        let dir = crate::dryrun::tests::temp_dir("bot-config");
        let path = dir.join("bot.toml");
        std::fs::write(&path, shipped).unwrap();
        let cfg = BotConfig::load(&path).unwrap();
        let (taker, maker) = cfg.select("HYPE").unwrap();
        assert_eq!((taker[0].lighter_market_index, maker[0].aster_symbol.as_str()), (Some(24), "HYPEUSDT"));
        assert!(cfg.select("BNB").is_err(), "BNB has no taker entry");
        let (taker, maker) = cfg.select("HYPE-HL").unwrap();
        assert!(taker[0].hedge_venue == maker[0].hedge_venue && maker[0].hedge_venue == crate::config::HedgeVenue::Hyperliquid);
        for (edited, expected) in [
            // XEMM's old `[live] mode` key: the mode is a command-line choice only.
            (shipped.replace("[maker.live]", "[maker.live]\nmode = \"live\""), "live.mode"),
            (shipped.replacen("poll_sec", "poll_secs", 1), "poll_secs"),
            (format!("{shipped}\n[venues]\nsigners_dir = \"signers\"\n"), "top-level table [venues]"),
        ] {
            std::fs::write(&path, edited).unwrap();
            let error = format!("{:#}", BotConfig::load(&path).err().expect("stray key accepted"));
            assert!(error.contains(expected), "{error}");
        }
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// `run --mode dry-run` end to end: the real controller and engines against the simulated
    /// venues, fed by a scripted market standing in for mainnet. XEMM quotes; an arbitrage
    /// takes the rights from it (cancel, grant, hedged taker trade, hand-back); XEMM quotes
    /// again. Docker runs it with `--network none`, so nothing can reach a real venue.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_dry_run_yields_to_a_scripted_arbitrage_then_quotes_again_and_drains_on_stop() {
        use rust_decimal_macros::dec;
        use std::time::Duration;
        let _ = tracing_subscriber::fmt().with_test_writer().try_init();
        let market = std::sync::Arc::new(crate::dryrun::tests::World::start().await);
        let dir = crate::dryrun::tests::temp_dir("dry-run-e2e");
        // The shipped 15 s poll and request budget: Lighter answers 429 past 60 requests/min.
        let mut cfg = crate::dryrun::tests::shipped_config(&market, &dir);
        // The taker's warm-up and history gates would need minutes of market data.
        let arb = &mut cfg.taker.arb;
        (arb.startup_warmup_ms, arb.entry_gate.enabled, arb.book_sanity.enabled) = (0, false, false);
        let fresh = market.keep_fresh();
        let stop = CancellationToken::new();
        let bot = tokio::spawn({
            let (stop, runs) = (stop.clone(), dir.clone());
            async move { run_with(cfg, &runs, "HYPE", LiveMode::DryRun, false, false, stop).await }
        });
        // XEMM's journal: the order updates and its hand-over rows, in order.
        let journal = crate::live_report::inferred_journal_path(&dir.join("dry-run").join("bot-HYPE"));
        let kinds = || -> Vec<String> {
            let text = std::fs::read_to_string(&journal).unwrap_or_default();
            text.lines().filter_map(|line| serde_json::from_str::<Value>(line).ok()).filter_map(|row| match row["kind"].as_str() {
                Some("order_update") => row["detail"]["state"].as_str().map(str::to_owned),
                Some(kind @ ("yield" | "rights_returned" | "resumed")) => Some(kind.to_owned()),
                _ => None,
            }).collect()
        };
        async fn wait_for(what: &str, kinds: &dyn Fn() -> Vec<String>, done: impl Fn(&[String]) -> bool) {
            for _ in 0..600 {
                if done(&kinds()) {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            panic!("{what} within 60 s: {:?}", kinds());
        }
        wait_for("an XEMM quote", &kinds, |k| k.iter().any(|k| k == "accepted")).await;
        // Aster asks 98 while Lighter bids 99: 100 bps across the venues.
        market.set_aster(dec!(97), dec!(98));
        let ledger = dir.join("dry-run").join("trades_HYPE.jsonl");
        let row = tokio::time::timeout(Duration::from_secs(60), async {
            loop {
                if let Some(line) = std::fs::read_to_string(&ledger).ok().and_then(|text| text.lines().next().map(str::to_string)) {
                    break serde_json::from_str::<crate::taker::pnl::TradeLedgerRow>(&line).unwrap();
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .expect("no hedged trade within 60 s");
        let confirmed = crate::taker::pnl::EconomicStatus::Confirmed;
        assert_eq!((row.economic_status, row.direction.as_str()), (confirmed, "SELL_LIGHTER_BUY_ASTER"), "{row:?}");
        assert_eq!((row.aster_fill.vwap, row.lighter_fill.vwap), (dec!(98), dec!(99)), "each leg took the top: {row:?}");
        assert_eq!(row.aster_fill.qty, row.lighter_fill.qty, "{row:?}");
        assert!(row.aster_fill.fee_usd > Decimal::ZERO && row.lighter_fill.fee_usd.is_zero(), "Aster charges 4 bps: {row:?}");
        assert!(row.final_net_position.is_zero(), "{row:?}");
        // XEMM stayed out from its yield to its resume, then quotes again.
        market.set_aster(dec!(99), dec!(101));
        let position = |k: &[String], kind: &str| k.iter().position(|x| x == kind);
        wait_for("a quote after the resume", &kinds, |k| position(k, "resumed").is_some_and(|at| k[at..].iter().any(|x| x == "accepted"))).await;
        let k = kinds();
        let (yielded, returned, resumed) = (position(&k, "yield").unwrap(), position(&k, "rights_returned").unwrap(), position(&k, "resumed").unwrap());
        assert!(yielded < returned && returned < resumed && !k[yielded..resumed].iter().any(|x| x == "accepted"), "{k:?}");
        // After its first start-up read, the controller reads XEMM's own snapshot, which has
        // no resting Lighter orders, instead of the venues.
        let state = dir.join("dry-run").join("bot-HYPE.state.json");
        let from_snapshot = || std::fs::read_to_string(&state).ok().and_then(|text| serde_json::from_str::<Value>(&text).ok())
            .is_some_and(|state| state["accounts"]["xemm"]["lighter_open_orders"].is_null() && state["accounts"]["xemm"]["total_equity_usd"].is_string());
        wait_for("a status from XEMM's snapshot", &|| vec![from_snapshot().to_string()], |k| k[0] == "true").await;
        stop.cancel();
        tokio::time::timeout(Duration::from_secs(60), bot).await.expect("the drain hung").unwrap().expect("a clean stop");
        let diag = std::fs::read_to_string(dir.join("dry-run").join("sim-HYPE.diag.jsonl")).unwrap();
        let lighter: Vec<Value> = diag.lines().map(|line| serde_json::from_str::<Value>(line).unwrap()["lighter"].clone()).collect();
        assert!(lighter.iter().all(|w| w["rejects"].get("RateLimited").is_none()), "Lighter answered 429: {lighter:?}");
        // The final save keeps the hedged pair for the next start.
        let state = std::fs::read_to_string(dir.join("dry-run").join("sim-HYPE.state.json")).unwrap();
        let state: serde_json::Value = serde_json::from_str(&state).unwrap();
        let qty = |venue: usize, market: &str| state[venue]["account"]["positions"][market]["qty"].as_str().and_then(|q| q.parse::<Decimal>().ok());
        // The taker's pair, plus any XEMM bid the drop to 97/98 filled and XEMM hedged.
        let (aster, lighter) = (qty(0, "HYPEUSDT").unwrap(), qty(1, "24").unwrap());
        assert!(aster + lighter == Decimal::ZERO && aster >= dec!(0.13), "{state}");
        let mut written: Vec<_> = std::fs::read_dir(&dir).unwrap().map(|e| e.unwrap().file_name().into_string().unwrap()).collect();
        written.sort();
        assert_eq!(written, ["bot.toml", "dry-run"], "a dry run writes under runs/dry-run only");

        // A restart that must not resume (a drawdown halt acknowledged without a baseline
        // reset) parks, so a restart policy cannot loop on it. A kill had also left the
        // taker's unclean-session marker, which the dry run archives.
        let latch = r#"{"reason":"pnl_breaker","details":{"breaker_reason":"equity_drawdown"}}"#;
        std::fs::write(dir.join("dry-run").join("bot-HYPE.breaker.json"), latch).unwrap();
        let marker = dir.join("dry-run").join("active_session_HYPE.json");
        std::fs::write(&marker, "{}").unwrap();
        let (cfg, stop) = (crate::dryrun::tests::shipped_config(&market, &dir), CancellationToken::new());
        let again = tokio::spawn({
            let (stop, runs) = (stop.clone(), dir.clone());
            async move { run_with(cfg, &runs, "HYPE", LiveMode::DryRun, true, false, stop).await }
        });
        tokio::time::sleep(Duration::from_secs(3)).await;
        assert!(!again.is_finished(), "a refused dry-run start parks");
        stop.cancel();
        let error = again.await.unwrap().expect_err("still a halt");
        assert!(format!("{error:#}").contains("--reset-breaker-baseline"), "{error:#}");
        let archived = std::fs::read_dir(dir.join("dry-run")).unwrap().flatten()
            .any(|entry| entry.file_name().to_string_lossy().starts_with("active_session_HYPE.json.unclean."));
        assert!(!marker.exists() && archived, "the unclean-session marker is archived");
        fresh.abort();
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// The same arbitrage with both engines hedging on Hyperliquid: the taker's second leg takes
    /// the Hyperliquid bid and pays the fee the venue's own fills report.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_dry_run_arbitrage_hedges_on_hyperliquid() {
        use rust_decimal_macros::dec;
        use std::time::Duration;
        let _ = tracing_subscriber::fmt().with_test_writer().try_init();
        let market = std::sync::Arc::new(crate::dryrun::tests::World::hedged_on(crate::dryrun::matching::Venue::Hyperliquid).await);
        let dir = crate::dryrun::tests::temp_dir("dry-run-e2e-hl");
        let mut cfg = crate::dryrun::tests::shipped_config(&market, &dir);
        let arb = &mut cfg.taker.arb;
        (arb.startup_warmup_ms, arb.entry_gate.enabled) = (0, false);
        let fresh = market.keep_fresh();
        let stop = CancellationToken::new();
        let bot = tokio::spawn({
            let (stop, runs) = (stop.clone(), dir.clone());
            async move { run_with(cfg, &runs, "HYPE-HL", LiveMode::DryRun, false, false, stop).await }
        });
        // Aster asks 98 while Hyperliquid bids 99.
        market.set_aster(dec!(97), dec!(98));
        let ledger = dir.join("dry-run").join("trades_HYPE-HL.jsonl");
        let row = tokio::time::timeout(Duration::from_secs(60), async {
            loop {
                if let Some(line) = std::fs::read_to_string(&ledger).ok().and_then(|text| text.lines().next().map(str::to_string)) {
                    break serde_json::from_str::<crate::taker::pnl::TradeLedgerRow>(&line).unwrap();
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .expect("no hedged trade within 60 s");
        let confirmed = crate::taker::pnl::EconomicStatus::Confirmed;
        assert_eq!((row.economic_status, row.direction.as_str()), (confirmed, "SELL_LIGHTER_BUY_ASTER"), "{row:?}");
        let hedge = row.lighter_fill;
        assert_eq!((row.aster_fill.vwap, hedge.vwap, row.aster_fill.qty), (dec!(98), dec!(99), hedge.qty), "{row:?}");
        let fee = hedge.notional * dec!(0.00045);
        assert!(hedge.fee_provenance == crate::taker::types::FeeProvenance::Venue && (hedge.fee_usd - fee).abs() < dec!(0.000001), "{row:?}");
        // Both legs took their tops: the net it expected is the one it made, fees included.
        assert!(row.final_net_position.is_zero() && (row.expected_net_usd - row.actual_net_usd).abs() < dec!(0.001), "{row:?}");
        stop.cancel();
        tokio::time::timeout(Duration::from_secs(60), bot).await.expect("the drain hung").unwrap().expect("a clean stop");
        fresh.abort();
        let state = std::fs::read_to_string(dir.join("dry-run").join("sim-HYPE-HL.state.json")).unwrap();
        let state: serde_json::Value = serde_json::from_str(&state).unwrap();
        let qty = |venue: usize, market: &str| state[venue]["account"]["positions"][market]["qty"].as_str().and_then(|q| q.parse::<Decimal>().ok());
        let (aster, hyperliquid) = (qty(0, "HYPEUSDT").unwrap(), qty(1, "HYPE").unwrap());
        assert!(aster + hyperliquid == Decimal::ZERO && aster >= dec!(0.13), "{state}");
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn a_running_orchestrator_or_its_latches_block_run() {
        let dir = crate::dryrun::tests::temp_dir("bot-legacy");
        let check = || refuse_legacy_stack("HYPE", &[dir.as_path()]);
        // A stopped orchestrator leaves its lock file behind, unlocked.
        let lock = File::create(dir.join("orchestrator_HYPE.lock")).unwrap();
        check().unwrap();
        lock.try_lock().unwrap();
        assert!(format!("{:#}", check().unwrap_err()).contains("orchestrator.py is still running"));
        drop(lock);
        std::fs::write(dir.join("orchestrator-xemm-HYPE.trip.json"), "{}").unwrap();
        assert!(format!("{:#}", check().unwrap_err()).contains("legacy latch"));
        std::fs::remove_dir_all(dir).unwrap();
    }
}
