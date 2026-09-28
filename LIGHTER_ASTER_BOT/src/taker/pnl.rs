use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use fs2::FileExt;
use tokio::sync::{mpsc, oneshot};

use anyhow::{Context, Result};
use chrono::{DateTime, SecondsFormat, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::taker::config::PnlCfg;
use crate::taker::types::{FeeEvidence, FillSummary, MarketId};

/// Armed once on the cold activation path, so crashes before an order receipt still
/// leave a durable barrier. Only verified, drained shutdown removes this session's marker.
#[derive(Clone)]
pub struct ActiveSession {
    inner: Arc<ActiveSessionInner>,
}

struct ActiveSessionInner {
    path: PathBuf,
    metadata: serde_json::Value,
    baseline_equity: arc_swap::ArcSwapOption<Decimal>,
    armed: AtomicBool,
    unresolved: AtomicBool,
    mutation: std::sync::Mutex<()>,
    ownership: std::sync::Mutex<Option<File>>,
}

impl ActiveSession {
    pub fn new(path: PathBuf, metadata: serde_json::Value) -> Self {
        Self { inner: Arc::new(ActiveSessionInner { path, metadata,
            baseline_equity:arc_swap::ArcSwapOption::empty(),
            armed: AtomicBool::new(false), unresolved: AtomicBool::new(false), mutation: std::sync::Mutex::new(()),
            ownership: std::sync::Mutex::new(None) }) }
    }

    pub fn id(&self) -> &str { self.inner.metadata["session_id"].as_str().unwrap_or("unknown") }
    pub fn armed(&self) -> bool { self.inner.armed.load(Ordering::Acquire) }
    pub fn unresolved(&self) -> bool { self.inner.unresolved.load(Ordering::Acquire) }
    pub fn baseline_equity(&self) -> Option<Decimal> { self.inner.baseline_equity.load_full().map(|value|*value) }

    pub async fn arm(&self) -> Result<()> { self.arm_inner(None).await }
    pub async fn arm_with_equity(&self, equity: Decimal) -> Result<()> { self.arm_inner(Some(equity)).await }

    async fn arm_inner(&self, baseline: Option<Decimal>) -> Result<()> {
        if self.inner.armed.load(Ordering::Acquire) { return Ok(()); }
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || -> Result<()> {
            let _guard = inner.mutation.lock().expect("session mutation poisoned");
            if inner.armed.load(Ordering::Acquire) { return Ok(()); }
            if let Some(parent) = inner.path.parent() { fs::create_dir_all(parent)?; }
            let ownership = lock_inactive_session(&inner.path)?;
            let mut file = OpenOptions::new().write(true).create_new(true).open(&inner.path)
                .with_context(|| format!("unclean or active session at {}; execution blocked until session-scoped order evidence is resolved", inner.path.display()))?;
            let mut metadata = inner.metadata.clone();
            if let Some(equity) = baseline { metadata["marked_equity_baseline_usd"] = serde_json::json!(equity); }
            serde_json::to_writer(&mut file, &metadata)?;
            file.write_all(b"\n")?;
            file.sync_all()?;
            if let Some(equity) = baseline { inner.baseline_equity.store(Some(Arc::new(equity))); }
            *inner.ownership.lock().expect("session ownership poisoned") = Some(ownership);
            inner.armed.store(true, Ordering::Release);
            Ok(())
        }).await?
    }

    pub async fn record_unresolved(&self, evidence: serde_json::Value) -> Result<()> {
        self.inner.unresolved.store(true, Ordering::Release);
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || -> Result<()> {
            let _guard = inner.mutation.lock().expect("session mutation poisoned");
            let mut evidence = evidence;
            if let Ok(bytes) = fs::read(&inner.path) {
                if let Ok(previous) = serde_json::from_slice::<serde_json::Value>(&bytes) {
                    if let Some(old) = previous.get("unresolved_execution") {
                        {
                            if let (Some(earlier), Some(later)) = (old.get("orders").and_then(serde_json::Value::as_array),
                                evidence.get("orders").and_then(serde_json::Value::as_array)) {
                                let mut orders = earlier.clone();
                                for order in later { if !orders.contains(order) { orders.push(order.clone()); } }
                                evidence["orders"] = serde_json::json!(orders);
                                evidence["orders_complete"] = serde_json::json!(
                                    old.get("orders_complete").and_then(serde_json::Value::as_bool) == Some(true)
                                    && evidence.get("orders_complete").and_then(serde_json::Value::as_bool) == Some(true));
                                if let Some(baseline) = old.get("pre_positions") { evidence["pre_positions"] = baseline.clone(); }
                            }
                        }
                    }
                }
            }
            let mut body = active_session_metadata(&inner);
            body["status"] = serde_json::json!("unresolved");
            body["unresolved_execution"] = evidence;
            write_json_atomic(&inner.path, &body, true)
        }).await?
    }

    pub async fn resolve_execution(&self) -> Result<()> {
        if !self.inner.unresolved.load(Ordering::Acquire) { return Ok(()); }
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || -> Result<()> {
            let _guard = inner.mutation.lock().expect("session mutation poisoned");
            write_json_atomic(&inner.path, &active_session_metadata(&inner), true)?;
            inner.unresolved.store(false, Ordering::Release);
            Ok(())
        }).await?
    }

    pub async fn clear_verified(&self) -> Result<()> {
        if !self.inner.armed.load(Ordering::Acquire) { return Ok(()); }
        anyhow::ensure!(!self.inner.unresolved.load(Ordering::Acquire), "unresolved session must remain armed");
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || -> Result<()> {
            let _guard = inner.mutation.lock().expect("session mutation poisoned");
            let current: serde_json::Value = serde_json::from_slice(&fs::read(&inner.path)?)?;
            anyhow::ensure!(current["session_id"] == inner.metadata["session_id"], "session marker ownership changed");
            fs::remove_file(&inner.path)?;
            inner.armed.store(false, Ordering::Release);
            inner.ownership.lock().expect("session ownership poisoned").take();
            Ok(())
        }).await?
    }
}

fn active_session_metadata(inner: &ActiveSessionInner) -> serde_json::Value {
    let mut metadata = inner.metadata.clone();
    if let Some(equity) = inner.baseline_equity.load_full() {
        metadata["marked_equity_baseline_usd"] = serde_json::json!(*equity);
    }
    metadata
}

/// Hold this lock throughout read-only session resolution so a running owner cannot
/// lose its barrier. The separate lock inode survives atomic marker replacement.
pub fn lock_inactive_session(path: &Path) -> Result<File> {
    if let Some(parent) = path.parent() { fs::create_dir_all(parent)?; }
    let lock = OpenOptions::new().create(true).read(true).write(true).open(path.with_extension("lock"))?;
    lock.try_lock_exclusive().context("session still has a live owner; stop it before cold resolution")?;
    Ok(lock)
}

pub async fn retire_session_verified(path: PathBuf, session_id: String, evidence: serde_json::Value) -> Result<PathBuf> {
    tokio::task::spawn_blocking(move || -> Result<PathBuf> {
        let current: serde_json::Value = serde_json::from_slice(&fs::read(&path)?)?;
        anyhow::ensure!(current["session_id"].as_str() == Some(session_id.as_str()), "session marker changed during resolution");
        let artifact = path.with_file_name(format!("session_resolution_{}_{}.json",
            Utc::now().timestamp_micros(), std::process::id()));
        write_json_atomic(&artifact, &evidence, true)?;
        fs::remove_file(&path)?;
        Ok(artifact)
    }).await?
}

static ATOMIC_WRITE_SEQUENCE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

pub fn write_json_atomic<T: Serialize>(path: &Path, value: &T, durable: bool) -> Result<()> {
    if let Some(parent) = path.parent() { fs::create_dir_all(parent)?; }
    let serial = ATOMIC_WRITE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let name = path.file_name().and_then(|name| name.to_str()).unwrap_or("state.json");
    let tmp = path.with_file_name(format!(".{name}.{}.{}.tmp", std::process::id(), serial));
    let result = (|| -> Result<()> {
        let mut file = OpenOptions::new().write(true).create_new(true).open(&tmp)?;
        serde_json::to_writer(&mut file, value)?;
        file.write_all(b"\n")?;
        if durable { file.sync_all()?; }
        drop(file);
        fs::rename(&tmp, path)?;
        Ok(())
    })();
    if result.is_err() { let _ = fs::remove_file(&tmp); }
    result
}

const JOURNAL_QUEUE_CAPACITY: usize = 1024;

/// File locking covers a whole serialized row, including its newline, across bot processes.
/// Serialization, locking and disk I/O are called only by cold workers/startup tools.
pub struct LockedJournal {
    file: File,
    lock: File,
}

impl LockedJournal {
    pub fn open(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() { fs::create_dir_all(parent)?; }
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        let lock = OpenOptions::new().create(true).read(true).write(true)
            .open(path.with_extension("jsonl.lock"))?;
        Ok(Self {file,lock})
    }

    pub fn append<T: Serialize>(&mut self, row: &T, durable: bool) -> Result<()> {
        let mut bytes = serde_json::to_vec(row).context("serialize journal row")?;
        bytes.push(b'\n');
        self.lock.lock_exclusive().context("lock journal for append")?;
        let write = self.file.write_all(&bytes).and_then(|()| if durable { self.file.sync_data() } else { Ok(()) });
        let unlock = FileExt::unlock(&self.lock);
        write.context("append journal row")?;
        unlock.context("unlock journal")?;
        Ok(())
    }
}

pub fn append_json_line<T: Serialize>(path: &Path, row: &T, durable: bool) -> Result<()> {
    LockedJournal::open(path)?.append(row,durable)
}

enum JournalCommand<T> {
    Append(T, Option<oneshot::Sender<std::result::Result<(), String>>>),
    Stop(oneshot::Sender<()>),
}

pub struct ColdJournal<T> {
    tx: mpsc::Sender<JournalCommand<T>>,
    healthy: Arc<AtomicBool>,
}

impl<T: Serialize + Send + 'static> ColdJournal<T> {
    pub fn new(path: PathBuf, durable: bool) -> Result<Self> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        // Check access before publishing a healthy writer; this is startup/cold work.
        let mut file = LockedJournal::open(&path)?;
        let (tx, mut rx) = mpsc::channel(JOURNAL_QUEUE_CAPACITY);
        let healthy = Arc::new(AtomicBool::new(true));
        let worker_health = healthy.clone();
        std::thread::Builder::new().name("taker-journal".to_string()).spawn(move || {
            while let Some(command) = rx.blocking_recv() {
                match command {
                    JournalCommand::Append(row, ack) => {
                        let result = if worker_health.load(Ordering::Acquire) {
                            file.append(&row, durable)
                                .map_err(|error| format!("{}: {error:#}", path.display()))
                        } else { Err("journal failed earlier; row was not written".to_string()) };
                        if let Err(error) = &result {
                            worker_health.store(false, Ordering::Release);
                            tracing::error!("journal append failed: {error}");
                        }
                        if let Some(ack) = ack { let _ = ack.send(result); }
                    }
                    JournalCommand::Stop(ack) => { let _ = ack.send(()); break; }
                }
            }
        })?;
        Ok(Self { tx, healthy })
    }

    pub fn healthy(&self) -> bool {
        self.healthy.load(Ordering::Acquire) && !self.tx.is_closed()
    }

    pub fn try_append(&self, row: T) -> Result<()> {
        if !self.healthy() { anyhow::bail!("journal worker unhealthy"); }
        if let Err(error) = self.tx.try_send(JournalCommand::Append(row, None)) {
            self.healthy.store(false, Ordering::Release);
            anyhow::bail!("journal queue unavailable; new entries blocked: {error}");
        }
        Ok(())
    }

    pub async fn append_confirmed(&self, row: T) -> Result<()> {
        let (ack, done) = oneshot::channel();
        self.tx.send(JournalCommand::Append(row, Some(ack))).await
            .map_err(|_| anyhow::anyhow!("journal worker stopped"))?;
        done.await.context("journal acknowledgement missing")?
            .map_err(anyhow::Error::msg)
    }

    pub async fn shutdown(&self) -> Result<()> {
        let (ack, done) = oneshot::channel();
        tokio::time::timeout(Duration::from_secs(5), async {
            self.tx.send(JournalCommand::Stop(ack)).await
                .map_err(|_| anyhow::anyhow!("journal worker stopped before drain"))?;
            done.await.context("journal drain acknowledgement missing")
        }).await.context("journal drain exceeded five seconds")??;
        if !self.healthy.load(Ordering::Acquire) { anyhow::bail!("journal had write failures"); }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EconomicStatus {
    Confirmed,
    Estimated,
    Incomplete,
    #[default]
    LegacyUnverified,
}

fn legacy_schema_version() -> u32 { 1 }

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradeLedgerRow {
    #[serde(default = "legacy_schema_version")]
    pub schema_version: u32,
    #[serde(default)]
    pub economic_status: EconomicStatus,
    #[serde(default)]
    pub execution_id: Option<String>,
    #[serde(default)]
    pub source_event_id: Option<String>,
    pub timestamp: DateTime<Utc>,
    pub market: String,
    pub direction: String,
    pub qty: Decimal,
    pub expected_net_usd: Decimal,
    pub actual_gross_usd: Decimal,
    pub actual_fees_usd: Decimal,
    pub actual_net_usd: Decimal,
    pub actual_net_bps: Decimal,
    pub fill_qty_mismatch: Decimal,
    pub aster_fill: FillSummary,
    /// The first leg's venue, whose fill `aster_fill` is; rows from before it were Aster's.
    #[serde(default)]
    pub first_venue: crate::config::FirstVenue,
    /// The second leg's venue; rows from before it hedged on Lighter.
    #[serde(default)]
    pub hedge_venue: crate::config::HedgeVenue,
    #[serde(alias = "lighter_fill")]
    pub hedge_fill: FillSummary,
    #[serde(default, alias = "lighter_fee_evidence")]
    pub hedge_fee_evidence: Vec<FeeEvidence>,
    pub aster_order_id: i64,
    #[serde(alias = "lighter_client_order_index")]
    pub hedge_client_order_index: i64,
    pub final_aster_position: Decimal,
    #[serde(alias = "final_lighter_position")]
    pub final_hedge_position: Decimal,
    pub final_net_position: Decimal,
    pub available_before_usd: Decimal,
    pub available_after_usd: Decimal,
    pub aster_available_before_usd: Decimal,
    pub aster_available_after_usd: Decimal,
    #[serde(alias = "lighter_available_before_usd")]
    pub hedge_available_before_usd: Decimal,
    #[serde(alias = "lighter_available_after_usd")]
    pub hedge_available_after_usd: Decimal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CircuitBreakerState {
    pub active: bool,
    pub triggered_at: DateTime<Utc>,
    pub market: String,
    pub pnl_since: DateTime<Utc>,
    pub cumulative_pnl_usdc: Decimal,
    pub max_loss_usdc: Decimal,
    pub last_trade_timestamp: DateTime<Utc>,
    pub last_trade_actual_net_usd: Decimal,
    pub last_aster_order_id: i64,
    #[serde(alias = "last_lighter_client_order_index")]
    pub last_hedge_client_order_index: i64,
    pub final_aster_position: Decimal,
    #[serde(alias = "final_lighter_position")]
    pub final_hedge_position: Decimal,
    pub final_net_position: Decimal,
}

#[derive(Debug, Clone)]
pub struct PnlSnapshot {
    pub since: DateTime<Utc>,
    pub loaded_trades: usize,
    pub cumulative_pnl_usdc: Decimal,
    pub max_loss_usdc: Decimal,
    pub ledger_path: PathBuf,
    pub breaker_path: PathBuf,
}

#[derive(Debug, Clone)]
pub struct PnlUpdate {
    pub cumulative_pnl_usdc: Decimal,
    pub trade_count: usize,
    pub breaker: Option<CircuitBreakerState>,
}

pub struct PnlTracker {
    market: MarketId,
    since: DateTime<Utc>,
    max_loss_usdc: Decimal,
    ledger_path: PathBuf,
    breaker_path: PathBuf,
    cumulative_pnl_usdc: Decimal,
    loaded_trades: usize,
    last_trade: Option<TradeLedgerRow>,
    writer: ColdJournal<TradeLedgerRow>,
}

fn loss_guard_value(row: &TradeLedgerRow) -> Decimal {
    let known = row.schema_version >= 2 && row.economic_status == EconomicStatus::Confirmed
        && row.aster_fill.fee_provenance == crate::taker::types::FeeProvenance::Venue
        && row.hedge_fill.fee_provenance == crate::taker::types::FeeProvenance::Venue;
    if known { row.actual_net_usd } else { row.actual_net_usd.min(Decimal::ZERO) }
}

impl PnlTracker {
    pub fn new(cfg: &PnlCfg, market: &MarketId, bot_start: DateTime<Utc>) -> Result<Self> {
        let since = parse_since(&cfg.since, bot_start)?;
        let dir = PathBuf::from(&cfg.persist_dir);
        fs::create_dir_all(&dir)
            .with_context(|| format!("create pnl persist dir {}", dir.display()))?;
        let component = market_component(market);
        let ledger_path = ledger_path(cfg, market);
        let breaker_path = dir.join(format!("circuit_breaker_{component}.json"));
        let (loaded_trades, cumulative_pnl_usdc, last_trade) =
            load_cumulative_pnl(&ledger_path, since)?;
        let writer = ColdJournal::new(ledger_path.clone(), true)?;
        Ok(Self {
            market: market.clone(),
            since,
            max_loss_usdc: cfg.max_loss_usdc,
            ledger_path,
            breaker_path,
            cumulative_pnl_usdc,
            loaded_trades,
            last_trade,
            writer,
        })
    }

    pub fn snapshot(&self) -> PnlSnapshot {
        PnlSnapshot {
            since: self.since,
            loaded_trades: self.loaded_trades,
            cumulative_pnl_usdc: self.cumulative_pnl_usdc,
            max_loss_usdc: self.max_loss_usdc,
            ledger_path: self.ledger_path.clone(),
            breaker_path: self.breaker_path.clone(),
        }
    }

    pub fn active_breaker(&self) -> Result<Option<CircuitBreakerState>> {
        read_active_breaker(&self.breaker_path)
    }

    pub fn trip_from_loaded_pnl_if_needed(&self) -> Result<Option<CircuitBreakerState>> {
        if !self.should_trip() {
            return Ok(None);
        }
        let Some(last_trade) = self.last_trade.as_ref() else {
            return Ok(None);
        };
        let state = self.breaker_from_row(last_trade);
        write_breaker(&self.breaker_path, &state)?;
        Ok(Some(state))
    }

    pub async fn record_trade(&mut self, row: TradeLedgerRow) -> Result<PnlUpdate> {
        self.writer.append_confirmed(row.clone()).await?;
        if row.timestamp >= self.since {
            self.loaded_trades += 1;
            self.cumulative_pnl_usdc += loss_guard_value(&row);
            self.last_trade = Some(row.clone());
        }
        let breaker = if self.should_trip() {
            let state = self.breaker_from_row(&row);
            let path = self.breaker_path.clone();
            let saved = state.clone();
            tokio::task::spawn_blocking(move || write_breaker(&path, &saved)).await??;
            Some(state)
        } else {
            None
        };
        Ok(PnlUpdate {
            cumulative_pnl_usdc: self.cumulative_pnl_usdc,
            trade_count: self.loaded_trades,
            breaker,
        })
    }

    pub async fn shutdown(&self) -> Result<()> { self.writer.shutdown().await }

    fn should_trip(&self) -> bool {
        self.cumulative_pnl_usdc <= -self.max_loss_usdc
    }

    fn breaker_from_row(&self, row: &TradeLedgerRow) -> CircuitBreakerState {
        CircuitBreakerState {
            active: true,
            triggered_at: Utc::now(),
            market: self.market.0.clone(),
            pnl_since: self.since,
            cumulative_pnl_usdc: self.cumulative_pnl_usdc,
            max_loss_usdc: self.max_loss_usdc,
            last_trade_timestamp: row.timestamp,
            last_trade_actual_net_usd: row.actual_net_usd,
            last_aster_order_id: row.aster_order_id,
            last_hedge_client_order_index: row.hedge_client_order_index,
            final_aster_position: row.final_aster_position,
            final_hedge_position: row.final_hedge_position,
            final_net_position: row.final_net_position,
        }
    }
}

pub fn reset_circuit_breaker(cfg: &PnlCfg, market: &MarketId) -> Result<Option<PathBuf>> {
    let dir = PathBuf::from(&cfg.persist_dir);
    let component = market_component(market);
    let breaker_path = dir.join(format!("circuit_breaker_{component}.json"));
    if !breaker_path.exists() {
        return Ok(None);
    }
    fs::create_dir_all(&dir)
        .with_context(|| format!("create pnl persist dir {}", dir.display()))?;
    let stamp = Utc::now().format("%Y%m%dT%H%M%SZ").to_string();
    let archive_path = dir.join(format!("circuit_breaker_{component}.{stamp}.json"));
    fs::rename(&breaker_path, &archive_path).with_context(|| {
        format!(
            "archive breaker {} to {}",
            breaker_path.display(),
            archive_path.display()
        )
    })?;
    Ok(Some(archive_path))
}

pub fn parse_since(raw: &str, bot_start: DateTime<Utc>) -> Result<DateTime<Utc>> {
    let raw = raw.trim();
    if raw.eq_ignore_ascii_case("startup") || raw.eq_ignore_ascii_case("now") {
        return Ok(bot_start);
    }
    DateTime::parse_from_rfc3339(raw)
        .map(|dt| dt.with_timezone(&Utc))
        .with_context(|| format!("parse pnl.since={raw:?} as RFC3339 timestamp"))
}

fn load_cumulative_pnl(
    path: &Path,
    since: DateTime<Utc>,
) -> Result<(usize, Decimal, Option<TradeLedgerRow>)> {
    if !path.exists() {
        return Ok((0, Decimal::ZERO, None));
    }
    let file = File::open(path).with_context(|| format!("open pnl ledger {}", path.display()))?;
    let mut count = 0usize;
    let mut total = Decimal::ZERO;
    let mut last_trade = None;
    for (idx, line) in BufReader::new(file).lines().enumerate() {
        let line = line.with_context(|| format!("read pnl ledger line {}", idx + 1))?;
        if line.trim().is_empty() {
            continue;
        }
        let row: TradeLedgerRow = serde_json::from_str(&line)
            .with_context(|| format!("parse pnl ledger {} line {}", path.display(), idx + 1))?;
        if row.timestamp >= since {
            count += 1;
            total += loss_guard_value(&row);
            last_trade = Some(row);
        }
    }
    Ok((count, total, last_trade))
}

#[cfg(test)]
fn append_trade(path: &Path, row: &TradeLedgerRow) -> Result<()> {
    append_json_line(path, row, true)
}

fn read_active_breaker(path: &Path) -> Result<Option<CircuitBreakerState>> {
    if !path.exists() {
        return Ok(None);
    }
    let text =
        fs::read_to_string(path).with_context(|| format!("read breaker {}", path.display()))?;
    let state: CircuitBreakerState =
        serde_json::from_str(&text).with_context(|| format!("parse breaker {}", path.display()))?;
    if state.active {
        Ok(Some(state))
    } else {
        Ok(None)
    }
}

fn write_breaker(path: &Path, state: &CircuitBreakerState) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("create breaker dir {}", parent.display()))?;
    }
    let tmp = path.with_extension("json.tmp");
    let text = serde_json::to_string_pretty(state).context("serialize circuit breaker")?;
    fs::write(&tmp, text).with_context(|| format!("write breaker temp {}", tmp.display()))?;
    fs::rename(&tmp, path)
        .with_context(|| format!("move breaker temp {} to {}", tmp.display(), path.display()))?;
    Ok(())
}

/// The taker's trade ledger (`trades_<M>.jsonl`), also tailed by the `run` loss stop.
pub fn ledger_path(cfg: &PnlCfg, market: &MarketId) -> PathBuf {
    PathBuf::from(&cfg.persist_dir).join(format!("trades_{}.jsonl", market_component(market)))
}

pub fn session_path(cfg: &PnlCfg, market: &MarketId) -> PathBuf {
    PathBuf::from(&cfg.persist_dir).join(format!("active_session_{}.json", market_component(market)))
}

/// `market` as a file-name component: anything but `[A-Za-z0-9_-]` becomes `_`.
pub(super) fn market_component(market: &MarketId) -> String {
    market
        .0
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' || c == '-' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

pub fn format_ts(ts: DateTime<Utc>) -> String {
    ts.to_rfc3339_opts(SecondsFormat::Secs, true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn tmp_dir(name: &str) -> PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "lighter_aster_taker_arb_{name}_{}_{}",
            std::process::id(),
            nanos
        ))
    }

    fn cfg(dir: &Path) -> PnlCfg {
        PnlCfg {
            enabled: true,
            persist_dir: dir.display().to_string(),
            since: "2026-06-23T23:00:00Z".to_string(),
            max_loss_usdc: dec!(5),
        }
    }

    fn row(ts: &str, net: Decimal) -> TradeLedgerRow {
        TradeLedgerRow {
            schema_version: 2,
            economic_status: EconomicStatus::Confirmed,
            execution_id: None,
            source_event_id: None,
            timestamp: DateTime::parse_from_rfc3339(ts)
                .unwrap()
                .with_timezone(&Utc),
            market: "HYPE".to_string(),
            direction: "SELL_ASTER_BUY_LIGHTER".to_string(),
            qty: dec!(0.17),
            expected_net_usd: net,
            actual_gross_usd: net,
            actual_fees_usd: Decimal::ZERO,
            actual_net_usd: net,
            actual_net_bps: Decimal::ZERO,
            fill_qty_mismatch: Decimal::ZERO,
            aster_fill: FillSummary::from_qty_notional(dec!(0.17), dec!(10), Decimal::ZERO)
                .unwrap(),
            first_venue: Default::default(),
            hedge_venue: Default::default(),
            hedge_fill: FillSummary::from_qty_notional(dec!(0.17), dec!(10), Decimal::ZERO)
                .unwrap(),
            hedge_fee_evidence: Vec::new(),
            aster_order_id: 1,
            hedge_client_order_index: 2,
            final_aster_position: dec!(-0.17),
            final_hedge_position: dec!(0.17),
            final_net_position: Decimal::ZERO,
            available_before_usd: dec!(100),
            available_after_usd: dec!(100),
            aster_available_before_usd: dec!(50),
            aster_available_after_usd: dec!(50),
            hedge_available_before_usd: dec!(50),
            hedge_available_after_usd: dec!(50),
        }
    }

    #[test]
    fn a_row_written_before_the_hedge_names_still_reads() {
        let written = serde_json::to_value(row("2026-06-23T23:00:01Z", dec!(5))).unwrap();
        let old: serde_json::Map<_, _> = written.as_object().unwrap().iter()
            .filter(|(key, _)| *key != "hedge_venue").map(|(key, value)| (key.replace("hedge", "lighter"), value.clone())).collect();
        assert!(old.contains_key("lighter_fill") && old.contains_key("final_lighter_position"), "{old:?}");
        let read: TradeLedgerRow = serde_json::from_value(old.into()).unwrap();
        assert_eq!((read.hedge_venue, read.hedge_client_order_index, read.final_hedge_position), (crate::config::HedgeVenue::Lighter, 2, dec!(0.17)));
    }

    #[test]
    fn startup_since_resolves_to_bot_start() {
        let start = DateTime::parse_from_rfc3339("2026-06-24T01:02:03Z")
            .unwrap()
            .with_timezone(&Utc);
        assert_eq!(parse_since("startup", start).unwrap(), start);
        assert_eq!(parse_since("now", start).unwrap(), start);
    }

    #[test]
    fn rfc3339_since_parses() {
        let start = Utc::now();
        let parsed = parse_since("2026-06-23T23:00:00Z", start).unwrap();
        assert_eq!(format_ts(parsed), "2026-06-23T23:00:00Z");
    }

    #[test]
    fn ledger_ignores_rows_before_since() {
        let dir = tmp_dir("ledger");
        let market = MarketId::from("HYPE");
        let tracker = PnlTracker::new(&cfg(&dir), &market, Utc::now()).unwrap();
        append_trade(
            &tracker.ledger_path,
            &row("2026-06-23T22:59:59Z", dec!(-100)),
        )
        .unwrap();
        append_trade(
            &tracker.ledger_path,
            &row("2026-06-23T23:00:00Z", dec!(1.25)),
        )
        .unwrap();
        let tracker = PnlTracker::new(&cfg(&dir), &market, Utc::now()).unwrap();
        assert_eq!(tracker.snapshot().loaded_trades, 1);
        assert_eq!(tracker.snapshot().cumulative_pnl_usdc, dec!(1.25));
        let _ = fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn breaker_trips_at_exact_max_loss() {
        let dir = tmp_dir("trip");
        let market = MarketId::from("HYPE");
        let mut tracker = PnlTracker::new(&cfg(&dir), &market, Utc::now()).unwrap();
        let update = tracker
            .record_trade(row("2026-06-23T23:00:01Z", dec!(-5.00)))
            .await.unwrap();
        assert!(update.breaker.is_some());
        assert!(tracker.active_breaker().unwrap().is_some());
        let _ = fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn breaker_does_not_trip_before_max_loss() {
        let dir = tmp_dir("notrip");
        let market = MarketId::from("HYPE");
        let mut tracker = PnlTracker::new(&cfg(&dir), &market, Utc::now()).unwrap();
        let update = tracker
            .record_trade(row("2026-06-23T23:00:01Z", dec!(-4.99)))
            .await.unwrap();
        assert!(update.breaker.is_none());
        assert!(tracker.active_breaker().unwrap().is_none());
        let _ = fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn reset_archives_and_clears_breaker() {
        let dir = tmp_dir("reset");
        let market = MarketId::from("HYPE");
        let config = cfg(&dir);
        let mut tracker = PnlTracker::new(&config, &market, Utc::now()).unwrap();
        tracker
            .record_trade(row("2026-06-23T23:00:01Z", dec!(-5.00)))
            .await.unwrap();
        let archive = reset_circuit_breaker(&config, &market).unwrap().unwrap();
        assert!(archive.exists());
        assert!(tracker.active_breaker().unwrap().is_none());
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn startup_recreates_breaker_from_loaded_loss_window() {
        let dir = tmp_dir("startup_trip");
        let market = MarketId::from("HYPE");
        let config = cfg(&dir);
        let tracker = PnlTracker::new(&config, &market, Utc::now()).unwrap();
        append_trade(
            &tracker.ledger_path,
            &row("2026-06-23T23:00:01Z", dec!(-5.01)),
        )
        .unwrap();
        let tracker = PnlTracker::new(&config, &market, Utc::now()).unwrap();
        let breaker = tracker.trip_from_loaded_pnl_if_needed().unwrap().unwrap();
        assert_eq!(breaker.cumulative_pnl_usdc, dec!(-5.01));
        assert!(tracker.active_breaker().unwrap().is_some());
        let _ = fs::remove_dir_all(dir);
    }
    #[tokio::test]
    async fn session_lock_and_unclean_marker_both_prevent_rearming() {
        let dir = tmp_dir("session");
        let path = dir.join("active_session_HYPE.json");
        let first = ActiveSession::new(path.clone(),serde_json::json!({"session_id":"one"}));
        first.arm_with_equity(Decimal::from(200)).await.unwrap();
        first.arm_with_equity(Decimal::from(999)).await.unwrap();
        assert_eq!(first.baseline_equity(),Some(Decimal::from(200)));
        let stored: serde_json::Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        assert_eq!(stored["marked_equity_baseline_usd"],"200");
        let second = ActiveSession::new(path.clone(),serde_json::json!({"session_id":"two"}));
        assert!(second.arm().await.is_err());
        drop(first); // A process death releases the lock but leaves the durable marker.
        assert!(second.arm().await.is_err());
        let _lock = lock_inactive_session(&path).unwrap();
        let artifact = retire_session_verified(path.clone(),"one".to_string(),serde_json::json!({"terminal_evidence":"fixture"})).await.unwrap();
        assert!(artifact.exists());
        drop(_lock);
        second.arm().await.unwrap();
        second.clear_verified().await.unwrap();
        let _ = fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn independent_journal_workers_preserve_complete_shared_file_rows() {
        let dir = tmp_dir("shared_rows");
        let path = dir.join("shared.jsonl");
        let first = ColdJournal::new(path.clone(),false).unwrap();
        let second = ColdJournal::new(path.clone(),false).unwrap();
        async fn write(writer: &ColdJournal<serde_json::Value>, prefix: &str) {
            for index in 0..32 {
                writer.append_confirmed(serde_json::json!({"id":format!("{prefix}-{index}"),"payload":"x".repeat(8192)})).await.unwrap();
            }
        }
        tokio::join!(write(&first,"a"),write(&second,"b"));
        first.shutdown().await.unwrap();
        second.shutdown().await.unwrap();
        let mut ids = std::collections::HashSet::new();
        for line in fs::read_to_string(&path).unwrap().lines() {
            let row: serde_json::Value = serde_json::from_str(line).unwrap();
            assert_eq!(row["payload"].as_str().unwrap().len(),8192);
            assert!(ids.insert(row["id"].as_str().unwrap().to_string()));
        }
        assert_eq!(ids.len(),64);
        let _ = fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn unknown_fees_and_legacy_gains_cannot_hide_conservative_losses() {
        let dir = tmp_dir("uncertain_guard");
        let mut tracker = PnlTracker::new(&cfg(&dir),&MarketId::from("HYPE"),Utc::now()).unwrap();
        let mut legacy = row("2026-06-23T23:00:01Z",dec!(5));
        legacy.schema_version = 1;
        legacy.economic_status = EconomicStatus::LegacyUnverified;
        tracker.record_trade(legacy).await.unwrap();
        let mut unknown = row("2026-06-23T23:00:02Z",dec!(5));
        unknown.hedge_fill.fee_provenance = crate::taker::types::FeeProvenance::Unknown;
        tracker.record_trade(unknown).await.unwrap();
        let mut estimated = row("2026-06-23T23:00:03Z",dec!(-2));
        estimated.economic_status = EconomicStatus::Estimated;
        tracker.record_trade(estimated).await.unwrap();
        assert_eq!(tracker.snapshot().cumulative_pnl_usdc,dec!(-2));
        tracker.shutdown().await.unwrap();
        let _ = fs::remove_dir_all(dir);
    }

}
