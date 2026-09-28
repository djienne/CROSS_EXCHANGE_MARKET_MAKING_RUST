//! The supervisor loop: runs both engine tasks (or a lone taker) for the whole session and
//! halts fail-closed.
//! * at start: both venues verified clear, then XEMM and the taker spawned once;
//! * every 250 ms: an engine that exited halts the bot;
//! * every `poll_sec` (`tick`): read the accounts' status, update the loss stops and the
//!   network pause.
//!
//! Execution rights move between the engines without the supervisor, through the two watch
//! channels of [`EngineIo`]: XEMM quotes until the taker asks for the rights (an arbitrage
//! passed its entry gate), then cancels, settles and grants them; the taker trades and hands
//! them back.
//!
//! Stopping is each engine's own bounded graceful drain, XEMM first so the taker's final check
//! sees no resting quote, awaited in full: there is no kill ladder. An engine that fails or
//! outlives `ENGINE_STOP_TIMEOUT` makes the stop an error.

use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, bail, Result};
use chrono::{DateTime, Utc};
use serde_json::{json, Value};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use super::risk::{py_decimal, EquityTracker, RealizedTrades};
use super::{iso, ControllerCfg, EventLog};
use crate::config::LiveMode;
use crate::taker::arb::ExecutionLease;
use crate::taker::pnl::write_json_atomic;

const FAST_POLL: Duration = Duration::from_millis(250);
const STATUS_TIMEOUT: Duration = Duration::from_secs(25);
/// Consecutive ticks without a status before the network pause.
const MAX_STATUS_FAILURES: u32 = 3;
/// Consecutive good status ticks (4 x poll_sec, ~60 s) before a network pause lifts: a
/// flapping network must not resume two-leg trades that a drop can leave half-filled.
const STABLE_TICKS: u32 = 4;
/// At startup, long enough for the Aster deadman countdown (`deadman_countdown_ms`, 10 s in
/// bot.toml) to cancel orders left by a crashed process.
const STARTUP_ORDERS_CLEAR_TIMEOUT: Duration = Duration::from_secs(15);
/// Above XEMM's worst-case bounded drain (~190 s: 5 quiesce + 4x2 sends + 70 + 65 + 30 verify
/// + 5 journal + 5 trip retry). Compose's stop_grace_period (460 s) covers a status tick in
/// progress (STATUS_TIMEOUT) plus this for XEMM and again for the taker.
const ENGINE_STOP_TIMEOUT: Duration = Duration::from_secs(200);

/// Bot labels kept from the two-process era: persisted state and reports use them.
pub const TAKER_BOT: &str = "LIGHTER_ASTER_TAKER_ARB";
pub const XEMM_BOT: &str = "XEMM_LIGHTER_ASTER";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Bot {
    Taker,
    Xemm,
}

impl Bot {
    pub fn label(self) -> &'static str {
        match self {
            Bot::Taker => TAKER_BOT,
            Bot::Xemm => XEMM_BOT,
        }
    }
}

/// What the engines share: the execution-rights handshake and the network pause.
#[derive(Clone)]
pub struct EngineIo {
    /// Taker → XEMM: `Some(id)` asks for the execution rights, `None` hands them back.
    pub want: watch::Sender<Option<u64>>,
    /// XEMM → taker: the rights, granted once no quote rests and nothing is in flight.
    pub lease: watch::Sender<Option<ExecutionLease>>,
    /// Network pause: while set, no engine opens new exposure (taker entries, XEMM quotes);
    /// in-flight executions, hedges, recovery and shutdown carry on.
    pub paused: Arc<AtomicBool>,
}

/// What the supervisor drives: the real engines in production, fakes in tests.
pub trait Engines {
    /// The engines to run, in stop order.
    fn bots(&self) -> &'static [Bot] {
        &[Bot::Xemm, Bot::Taker]
    }
    fn spawn(&mut self, bot: Bot, io: &EngineIo, stop: CancellationToken) -> JoinHandle<Result<()>>;
    /// The status of the accounts both engines trade, as JSON (`livebot::status`).
    fn status(&self) -> impl Future<Output = Result<Value>>;
}

/// Controller files: `bot-<M>.*`. The XEMM stem `bot-<M>` names XEMM's own journal, trip latch
/// and unclean-session marker (`bot-<M>-journal.jsonl`, `bot-<M>.trip.json`, `bot-<M>.active.json`).
pub struct Files {
    pub events: PathBuf,
    pub state: PathBuf,
    pub breaker: PathBuf,
    pub baseline: PathBuf,
    pub equity: PathBuf,
    pub xemm_stem: PathBuf,
}

impl Files {
    pub fn new(dir: &Path, market: &str) -> Self {
        let stem = format!("bot-{market}");
        let path = |suffix: &str| dir.join(format!("{stem}{suffix}"));
        Self {
            events: path(".events.jsonl"),
            state: path(".state.json"),
            breaker: path(".breaker.json"),
            baseline: path(".baseline.json"),
            equity: path(".equity.jsonl"),
            xemm_stem: path(""),
        }
    }
}

struct Task {
    bot: Bot,
    stop: CancellationToken,
    handle: JoinHandle<Result<()>>,
}

impl Task {
    /// Cancels the engine and awaits its own graceful drain.
    async fn stop(mut self) -> Result<()> {
        self.stop.cancel();
        match tokio::time::timeout(ENGINE_STOP_TIMEOUT, &mut self.handle).await {
            Ok(joined) => flatten(joined),
            Err(_) => {
                // Dropping the handle would leave it running (a parked dry run would trade on).
                self.handle.abort();
                bail!("{} did not stop within {}s", self.bot.label(), ENGINE_STOP_TIMEOUT.as_secs())
            }
        }
    }
}

fn flatten(joined: std::result::Result<Result<()>, tokio::task::JoinError>) -> Result<()> {
    joined.unwrap_or_else(|error| Err(anyhow!("engine task failed: {error}")))
}

fn error_text(result: &Result<()>) -> Value {
    result.as_ref().err().map_or(Value::Null, |error| json!(format!("{error:#}")))
}

pub struct Supervisor<E: Engines> {
    cfg: ControllerCfg,
    market: String,
    run_mode: LiveMode,
    files: Files,
    engines: E,
    events: EventLog,
    /// Both engines, XEMM first (the stop order).
    tasks: Vec<Task>,
    io: EngineIo,
    status_failures: u32,
    /// Set while no engine may open new exposure, because a status became unreadable.
    paused_since: Option<DateTime<Utc>>,
    stable_ticks: u32,
    equity: EquityTracker,
    trades: RealizedTrades,
    equity_failures: u32,
    halted: Option<&'static str>,
    shutdown: bool,
    stop: CancellationToken,
}

impl<E: Engines> Supervisor<E> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        cfg: ControllerCfg,
        market: String,
        run_mode: LiveMode,
        files: Files,
        taker_ledger: PathBuf,
        engines: E,
        mut events: EventLog,
        stop: CancellationToken,
    ) -> Self {
        let now = Utc::now();
        let equity = EquityTracker::load(
            &market,
            files.baseline.clone(),
            files.equity.clone(),
            cfg.max_loss_usdc,
            cfg.baseline_max_gap_hours,
            now,
            &mut events,
        );
        let trades = RealizedTrades::new(&market, now, taker_ledger, crate::live_report::inferred_journal_path(&files.xemm_stem));
        Self {
            cfg,
            market,
            run_mode,
            files,
            engines,
            events,
            tasks: Vec::new(),
            io: EngineIo { want: watch::channel(None).0, lease: watch::channel(None).0, paused: Arc::new(AtomicBool::new(false)) },
            status_failures: 0,
            paused_since: None,
            stable_ticks: 0,
            equity,
            trades,
            equity_failures: 0,
            halted: None,
            shutdown: false,
            stop,
        }
    }

    pub async fn run(mut self) -> Result<()> {
        self.events.emit("bot_started", json!({"market": self.market, "run_mode": self.run_mode.as_str()}));
        self.trades.prime();
        // In-process engines die with the process: a crash can leave XEMM's quotes resting, so
        // no engine starts until both venues are verified clear. A lone taker sends only IOCs,
        // and its own clean-start check refuses any open order.
        let bots = self.engines.bots();
        if bots.contains(&Bot::Xemm) {
            if let Err(status) = self.verify_orders_clear(STARTUP_ORDERS_CLEAR_TIMEOUT).await {
                self.safe_halt("startup_orders_not_clear", json!({"xemm_status": status})).await;
            }
        }
        if !self.stopping() {
            for &bot in bots {
                let stop = CancellationToken::new();
                let handle = self.engines.spawn(bot, &self.io, stop.clone());
                self.events.emit("bot_started_engine", json!({"bot": bot.label()}));
                self.tasks.push(Task { bot, stop, handle });
            }
        }
        let mut next_tick = Instant::now();
        while !self.stopping() {
            self.check_exits().await;
            if !self.stopping() && Instant::now() >= next_tick {
                self.tick().await;
                next_tick = Instant::now() + Duration::from_secs(self.cfg.poll_sec);
            }
            if self.events.unwritable() {
                self.shutdown = true;
            }
            tokio::select! {
                _ = tokio::time::sleep(FAST_POLL) => {}
                _ = self.stop.cancelled() => {}
            }
        }
        self.events.emit("bot_stopping", json!({"rights": self.rights()}));
        let stopped = self.stop_engines("shutdown").await;
        if let Some(reason) = self.halted {
            bail!("safe halt: {reason} (see {})", self.files.breaker.display());
        }
        if self.events.unwritable() {
            bail!("event log {} is unwritable", self.files.events.display());
        }
        stopped.map_err(|error| error.context("engine stop unresolved"))
    }

    fn stopping(&self) -> bool {
        self.halted.is_some() || self.shutdown || self.stop.is_cancelled()
    }

    /// Who holds the execution rights now.
    fn rights(&self) -> &'static str {
        if self.io.lease.borrow().is_some() || !self.engines.bots().contains(&Bot::Xemm) { "taker" } else { "xemm" }
    }

    async fn tick(&mut self) {
        let Some(status) = self.read_status().await else {
            // Unreadable status is almost always the network: halting cannot drain or cancel
            // without it either, so pause new exposure and wait for it to come back.
            self.status_failures += 1;
            self.stable_ticks = 0;
            self.events.emit("status_poll_failed", json!({"consecutive_failures": self.status_failures}));
            if self.status_failures >= MAX_STATUS_FAILURES && self.paused_since.is_none() {
                self.paused_since = Some(Utc::now());
                self.io.paused.store(true, Ordering::Release);
                self.events.emit("network_pause", json!({"consecutive_failures": self.status_failures}));
            }
            return;
        };
        self.status_failures = 0;
        self.trades.poll(&mut self.events);
        let sample = self.record_equity(&status);
        if let Some(reason) = self.equity.breach().or_else(|| self.trades.breach(self.cfg.max_loss_usdc)) {
            return self.safe_halt("pnl_breaker", json!({"breaker_reason": reason, "pnl_sample": sample})).await;
        }
        if let Some(since) = self.paused_since {
            self.stable_ticks += 1;
            if self.stable_ticks >= STABLE_TICKS {
                self.paused_since = None;
                self.io.paused.store(false, Ordering::Release);
                self.events.emit("network_resume", json!({"paused_secs": (Utc::now() - since).num_seconds()}));
            }
        }
        self.write_state(Some(&status), Value::Null);
    }

    async fn read_status(&mut self) -> Option<Value> {
        let failure = match tokio::time::timeout(STATUS_TIMEOUT, self.engines.status()).await {
            Ok(Ok(status)) if status.get("market").and_then(Value::as_str) == Some(self.market.as_str()) => return Some(status),
            Ok(Ok(status)) => json!({"error": "status for another market", "market": status.get("market")}),
            Ok(Err(error)) => json!({"error": format!("{error:#}")}),
            Err(_) => json!({"error": "timeout"}),
        };
        self.events.emit("status_read_failed", json!({"failure": failure}));
        None
    }

    /// Polls the status until both venues show no open order for the market, at least
    /// once. Fail-closed: anything unreadable counts as not clear.
    async fn verify_orders_clear(&mut self, deadline: Duration) -> std::result::Result<(), Value> {
        let until = Instant::now() + deadline;
        let mut last = Value::Null;
        loop {
            if let Some(status) = self.read_status().await {
                let count = |venue: &str| status.pointer(&format!("/accounts/{venue}_open_orders")).and_then(Value::as_u64);
                if count("aster") == Some(0) && count("lighter") == Some(0) {
                    return Ok(());
                }
                last = status;
            }
            if Instant::now() >= until {
                return Err(last);
            }
            tokio::time::sleep(FAST_POLL).await;
        }
    }

    /// The drawdown stop samples the taker's marked equity when its status has one, else
    /// XEMM's, so one definition holds unless the taker status is unavailable.
    fn record_equity(&mut self, status: &Value) -> Value {
        let Some(equity) = py_decimal(status.pointer("/accounts/total_equity_usd")) else {
            self.equity_failures += 1;
            if self.equity_failures == 3 {
                self.events.emit("equity_feed_starving", json!({"consecutive_failures": self.equity_failures}));
            }
            return Value::Null;
        };
        self.equity_failures = 0;
        let accounts = status.get("accounts").cloned().unwrap_or(Value::Null);
        let source = self.engines.bots()[0].label();
        self.equity.record(equity, source, &accounts, Utc::now(), &mut self.events)
    }

    /// Stops XEMM, then the taker; the first failure is returned once both were tried.
    async fn stop_engines(&mut self, reason: &str) -> Result<()> {
        let mut result = Ok(());
        for task in std::mem::take(&mut self.tasks) {
            let bot = task.bot;
            let stopped = task.stop().await;
            self.events.emit("bot_stopped", json!({"bot": bot.label(), "reason": reason, "error": error_text(&stopped)}));
            result = result.and(stopped);
        }
        result
    }

    /// Both engines run for the whole session: one that exits, cleanly or not, halts the bot.
    async fn check_exits(&mut self) {
        let Some(index) = self.tasks.iter().position(|task| task.handle.is_finished()) else { return };
        let task = self.tasks.remove(index);
        let details = json!({"bot": task.bot.label(), "error": error_text(&flatten(task.handle.await))});
        self.events.emit("bot_exited", details.clone());
        self.safe_halt("engine_exited", details).await;
    }

    /// Stops both engines and latches the breaker file that refuses the next start until
    /// `--ack-breaker`.
    async fn safe_halt(&mut self, reason: &'static str, details: Value) {
        if self.halted.is_some() {
            return;
        }
        self.halted = Some(reason);
        self.events.emit("safe_halt", json!({"reason": reason, "details": details}));
        if let Err(error) = self.stop_engines("safe_halt").await {
            self.events.emit("stop_failed", json!({"error": format!("{error:#}")}));
        }
        let breaker = json!({
            "active": true, "triggered_at": iso(Utc::now()), "market": self.market, "reason": reason,
            "details": details, "pnl": self.equity.summary(), "trades": self.trades.summary(),
        });
        if let Err(error) = write_json_atomic(&self.files.breaker, &breaker, true) {
            self.events.emit("breaker_file_write_failed", json!({"path": self.files.breaker.display().to_string(), "error": format!("{error:#}")}));
        }
        self.write_state(None, json!({"reason": reason, "details": details}));
    }

    /// `bot-<M>.state.json`: read by combined_pnl.py (and trade_history.py through it) and
    /// bot_stats.py (`rights`, `accounts.xemm.total_equity_usd`, `pnl`) and by operators.
    /// `halt` is set once a safe halt latched.
    fn write_state(&mut self, status: Option<&Value>, halt: Value) {
        let field = |key: &str| status.and_then(|s| s.get(key)).cloned().unwrap_or(Value::Null);
        // Whose status it is: XEMM's, or a lone taker's.
        let source = if self.engines.bots().contains(&Bot::Xemm) { "xemm" } else { "taker" };
        let state = json!({
            "timestamp": iso(Utc::now()), "market": self.market, "run_mode": self.run_mode.as_str(),
            "rights": self.rights(), "halt": halt,
            "pnl": self.equity.summary(),
            "trades": self.trades.summary(),
            "positions": {source: field("positions")},
            "accounts": {source: field("accounts")},
        });
        if let Err(error) = write_json_atomic(&self.files.state, &state, false) {
            self.events.emit("state_write_failed", json!({"error": format!("{error:#}")}));
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;

    #[derive(Clone, Default)]
    struct Shared {
        status: Arc<Mutex<Option<Value>>>,
        /// Engine stops, in order.
        stopped: Arc<Mutex<Vec<Bot>>>,
    }

    /// Engines that run until stopped; with `taker_fails` the taker instead fails at once.
    struct Fake {
        shared: Shared,
        taker_fails: bool,
    }

    impl Engines for Fake {
        fn spawn(&mut self, bot: Bot, _io: &EngineIo, stop: CancellationToken) -> JoinHandle<Result<()>> {
            let (shared, fails) = (self.shared.clone(), self.taker_fails && bot == Bot::Taker);
            tokio::spawn(async move {
                if fails {
                    return Err(anyhow!("taker failed"));
                }
                stop.cancelled().await;
                shared.stopped.lock().unwrap().push(bot);
                Ok(())
            })
        }

        async fn status(&self) -> Result<Value> {
            self.shared.status.lock().unwrap().clone().ok_or_else(|| anyhow!("status unavailable"))
        }
    }

    fn supervisor(taker_fails: bool) -> (Supervisor<Fake>, Shared, PathBuf) {
        let dir = crate::dryrun::tests::temp_dir("bot-supervisor");
        let shared = Shared::default();
        let files = Files::new(&dir, "HYPE");
        let events = EventLog::new(files.events.clone());
        let fake = Fake { shared: shared.clone(), taker_fails };
        let sup = Supervisor::new(ControllerCfg::default(), "HYPE".into(), LiveMode::Live, files, dir.join("trades_HYPE.jsonl"), fake, events, CancellationToken::new());
        (sup, shared, dir)
    }

    fn set_status(shared: &Shared) {
        let status = json!({"market": "HYPE", "accounts": {"aster_open_orders": 0, "lighter_open_orders": 0, "total_equity_usd": "200"}});
        *shared.status.lock().unwrap() = Some(status);
    }

    #[tokio::test(start_paused = true)]
    async fn an_engine_exit_halts_and_stops_xemm_before_the_taker() {
        let (sup, shared, dir) = supervisor(true);
        set_status(&shared);
        let error = sup.run().await.expect_err("an engine exit is a halt");
        assert!(format!("{error:#}").contains("engine_exited"), "{error:#}");
        assert_eq!(*shared.stopped.lock().unwrap(), [Bot::Xemm]);
        assert!(dir.join("bot-HYPE.breaker.json").exists());

        // A clean stop drains XEMM first, then the taker.
        let (sup, shared, clean) = supervisor(false);
        set_status(&shared);
        let stop = sup.stop.clone();
        let running = tokio::spawn(sup.run());
        tokio::time::sleep(Duration::from_secs(1)).await;
        stop.cancel();
        running.await.unwrap().unwrap();
        assert_eq!(*shared.stopped.lock().unwrap(), [Bot::Xemm, Bot::Taker]);
        for dir in [dir, clean] {
            std::fs::remove_dir_all(dir).unwrap();
        }
    }

    #[tokio::test(start_paused = true)]
    async fn unreadable_status_pauses_and_a_stable_network_resumes() {
        let (mut sup, shared, dir) = supervisor(false);
        let paused = sup.io.paused.clone();
        for _ in 0..3 {
            sup.tick().await;
        }
        assert!(sup.halted.is_none() && paused.load(Ordering::Acquire), "an outage pauses, never halts");
        set_status(&shared);
        for _ in 0..STABLE_TICKS - 1 {
            sup.tick().await;
        }
        shared.status.lock().unwrap().take();
        sup.tick().await; // a drop inside the window restarts it
        set_status(&shared);
        for _ in 0..STABLE_TICKS - 1 {
            sup.tick().await;
            assert!(paused.load(Ordering::Acquire), "still inside the stability window");
        }
        sup.tick().await;
        assert!(!paused.load(Ordering::Acquire) && sup.halted.is_none());
        std::fs::remove_dir_all(dir).unwrap();
    }
}
