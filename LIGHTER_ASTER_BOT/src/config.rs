//! TOML configuration. Decimal-bearing fields are quoted strings (the
//! `rust_decimal` `serde-str` feature parses them). The pure-config structs from
//! `edge`/`quote_engine` are reused directly; this module adds the capital,
//! book-check, live and market sections.

use anyhow::{bail, Context, Result};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::path::Path;

use crate::edge::EdgeConfig;
use crate::quote_engine::QuoteEngineConfig;
use crate::types::MarketId;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub edge: EdgeConfig,
    pub quote: QuoteEngineConfig,
    #[serde(default)]
    pub capital: CapitalCfg,
    /// Periodic REST cross-check of the live websocket books. Optional; defaults on.
    #[serde(default)]
    pub book_check: BookCheckCfg,
    /// XEMM live-trading settings. Entirely optional and `enabled = false` by default;
    /// the XEMM engine refuses to start until it is enabled.
    #[serde(default)]
    pub live: LiveCfg,
    pub markets: Vec<MarketCfg>,
}

fn default_true() -> bool {
    true
}

/// Per-exchange capital backing each leg. Both legs are perpetual futures, so at
/// `leverage` the maximum position notional a leg may carry is `capital * leverage`.
/// The cap is enforced per pair: each pair gets its own capital.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CapitalCfg {
    pub aster_capital_usd: Decimal,
    pub lighter_capital_usd: Decimal,
    pub leverage: Decimal,
    /// When true, clamp a quote to the remaining position headroom (or reject it
    /// if the headroom is below the minimum order size).
    pub enforce_position_cap: bool,
}

impl Default for CapitalCfg {
    fn default() -> Self {
        CapitalCfg {
            aster_capital_usd: Decimal::from(1000),
            lighter_capital_usd: Decimal::from(1000),
            leverage: Decimal::ONE,
            enforce_position_cap: true,
        }
    }
}

impl CapitalCfg {
    /// Max position notional allowed on the Aster maker leg.
    pub fn aster_cap_notional(&self) -> Decimal {
        self.aster_capital_usd * self.leverage
    }
    /// Max position notional allowed on the Lighter hedge leg.
    pub fn lighter_cap_notional(&self) -> Decimal {
        self.lighter_capital_usd * self.leverage
    }
}

/// Tunables for the XEMM engine's REST-vs-websocket order-book cross-check
/// (`hotpath::book_check`). Slow, off-hot-path reconciliation. All fields default so a
/// config without a `[book_check]` section still parses with the check enabled.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BookCheckCfg {
    /// Run the periodic REST cross-check. Default true.
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Seconds between cross-check scans (kept generous to stay non-invasive). Default 30.
    #[serde(default = "default_book_check_interval_secs")]
    pub interval_secs: u64,
    /// Mid divergence (bps) above which a single scan counts as a breach. Default 50.
    #[serde(default = "default_book_check_tolerance_bps")]
    pub tolerance_bps: Decimal,
    /// Consecutive breaches before acting (gate closed + websocket reset). Default 3,
    /// so a one-off REST/WS timing skew never trips it — only sustained divergence.
    #[serde(default = "default_book_check_breaches")]
    pub consecutive_breaches: u32,
    /// REST depth levels requested from Aster (Lighter always returns up to 20). Default 20.
    #[serde(default = "default_book_check_depth_limit")]
    pub depth_limit: u32,
    /// Max concurrent REST requests in one scan. Bounded concurrency narrows the scan
    /// time window without letting a large market list stampede the venues. Default 8.
    #[serde(default = "default_book_check_max_concurrent_requests")]
    pub max_concurrent_requests: usize,
    /// Skip a REST snapshot whose exchange timestamp is older than this many milliseconds.
    /// This avoids false divergence when REST returns a stale/cache-lagged snapshot. Default 3000.
    #[serde(default = "default_book_check_max_rest_snapshot_age_ms")]
    pub max_rest_snapshot_age_ms: i64,
}

fn default_book_check_interval_secs() -> u64 {
    30
}
fn default_book_check_tolerance_bps() -> Decimal {
    Decimal::from(50)
}
fn default_book_check_breaches() -> u32 {
    3
}
fn default_book_check_depth_limit() -> u32 {
    20
}
fn default_book_check_max_concurrent_requests() -> usize {
    8
}
fn default_book_check_max_rest_snapshot_age_ms() -> i64 {
    3_000
}

impl Default for BookCheckCfg {
    fn default() -> Self {
        BookCheckCfg {
            enabled: true,
            interval_secs: default_book_check_interval_secs(),
            tolerance_bps: default_book_check_tolerance_bps(),
            consecutive_breaches: default_book_check_breaches(),
            depth_limit: default_book_check_depth_limit(),
            max_concurrent_requests: default_book_check_max_concurrent_requests(),
            max_rest_snapshot_age_ms: default_book_check_max_rest_snapshot_age_ms(),
        }
    }
}

/// Execution mode of `run`, from the command line only (`--mode`); the config has no mode key.
/// Both modes run the same engines on one market.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum LiveMode {
    /// Real signed orders on Aster + Lighter. Real funds. Hard-gated.
    Live,
    /// The simulated venues of `dryrun`, fed by live market data, and a dry-run identity no
    /// venue knows: nothing can reach a real account.
    DryRun,
}

impl LiveMode {
    pub fn as_str(self) -> &'static str {
        match self {
            LiveMode::Live => "live",
            LiveMode::DryRun => "dry-run",
        }
    }
    /// True only for the mode that sends real orders.
    pub fn is_real(self) -> bool {
        matches!(self, LiveMode::Live)
    }
}

/// Sub-min partial-fill policy for live trading.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum PartialPolicy {
    /// Only trade markets where the smallest possible Aster fill is itself Lighter-hedgeable;
    /// reject every other pair. The safe first-live default.
    #[default]
    StrictEveryFillMustBeHedgeable,
    /// Accumulate sub-min fills into pending inventory and hedge once it clears the Lighter
    /// minimum. More permissive; only after strict mode is proven.
    AccumulateSubMin,
}

impl PartialPolicy {
    pub fn as_str(self) -> &'static str {
        match self {
            PartialPolicy::StrictEveryFillMustBeHedgeable => "strict_every_fill_must_be_hedgeable",
            PartialPolicy::AccumulateSubMin => "accumulate_sub_min",
        }
    }
}

/// The XEMM engine's `[live]` settings (`[maker.live]` in bot.toml). Every field defaults so
/// the section is fully optional; the whole struct is inert until `enabled = true`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiveCfg {
    /// Master switch. While false the XEMM engine (`livebot::run`) refuses to start. Default false.
    #[serde(default)]
    pub enabled: bool,
    /// Cooldown after ANY execution event during which no new maker quote may be placed
    /// (risk-reducing cancels/hedges stay allowed). Default 60_000.
    #[serde(default = "default_cooldown_ms")]
    pub post_trade_cooldown_ms: i64,
    /// `global` (one cooldown across all markets) or `per_market`. Default `global` (safer
    /// while cross-venue snapshots may lag).
    #[serde(default = "default_cooldown_scope")]
    pub cooldown_scope: String,
    /// Cancel all known Aster orders at startup before quoting. Default true.
    #[serde(default = "default_true")]
    pub startup_cancel_all: bool,
    /// Cancel all Aster maker orders on shutdown. Default true.
    #[serde(default = "default_true")]
    pub shutdown_cancel_all: bool,
    /// Require a fully reconciled, orphan-free start before quoting.
    #[serde(default = "default_true")]
    pub require_clean_start: bool,
    /// Max unhedged Aster notional tolerated before maker quoting freezes. Default "5".
    #[serde(default = "default_max_unhedged_notional")]
    pub max_unhedged_notional_usd: Decimal,
    /// Max age (ms) an unhedged Aster leg may persist before freeze. Default 1000.
    #[serde(default = "default_max_unhedged_age_ms")]
    pub max_unhedged_age_ms: i64,
    /// Predicted-vs-reported position divergence (USD) that freezes maker quoting. "2".
    #[serde(default = "default_max_position_mismatch")]
    pub max_position_mismatch_usd: Decimal,
    /// Max account-snapshot staleness (ms) before the snapshot is distrusted. 3000.
    #[serde(default = "default_max_account_snapshot_age_ms")]
    pub max_account_snapshot_age_ms: i64,
    /// Max user-stream silence (ms) before the gate closes + orders cancel. 5000.
    #[serde(default = "default_max_user_stream_staleness_ms")]
    pub max_user_stream_staleness_ms: i64,
    /// Max venue book age (ms) before a feed counts as stale: the per-market quote gate,
    /// hedge-book selection, the feed watchdog and reconciler marks. Default 10000.
    #[serde(default = "default_max_book_staleness_ms")]
    pub max_book_staleness_ms: i64,
    /// Cancel all Aster maker orders whenever the trading gate closes. Default true.
    #[serde(default = "default_true")]
    pub cancel_all_on_gate_close: bool,
    /// Cancel all Aster maker orders when a user stream goes stale. Default true.
    #[serde(default = "default_true")]
    pub cancel_all_on_user_stream_stale: bool,
    #[serde(default)]
    pub quote: LiveQuoteCfg,
    #[serde(default)]
    pub partials: LivePartialsCfg,
    #[serde(default)]
    pub aster: LiveAsterCfg,
    #[serde(default)]
    pub lighter: LiveLighterCfg,
    #[serde(default)]
    pub hyperliquid: LiveHyperliquidCfg,
    #[serde(default)]
    pub circuit_breaker: LiveCircuitBreakerCfg,
    /// Proactive per-venue margin guard: cap each venue's position notional at its real free
    /// collateral (minus that venue's safety buffer) so the position-increasing side stops quoting
    /// BEFORE the exchange rejects (Aster -2019). Default enabled.
    #[serde(default)]
    pub margin_guard: LiveMarginGuardCfg,
    /// Set only by `run --mode dry-run` after pointing the venue URLs at the simulated venues:
    /// the engine then signs with the dry-run identity. No file can set it.
    #[serde(skip)]
    pub dry_run: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiveQuoteCfg {
    /// Use scaled-integer tick/lot math on the quote hot path. Default true.
    #[serde(default = "default_true")]
    pub use_hot_integer_math: bool,
    /// Only place maker quotes whose paired Aster fill + Lighter hedge reduces absolute
    /// cross-venue inventory. Default true for live inventory unwind mode.
    #[serde(default = "default_true")]
    pub reduce_position_only: bool,
    /// Cancel/replace a now-unprofitable quote immediately, ignoring the requote
    /// throttle. Default true.
    #[serde(default = "default_true")]
    pub replace_immediately_if_unprofitable: bool,
    /// Minimum spacing (ms) between requotes of the same side. Default 20.
    #[serde(default = "default_min_requote_interval_ms")]
    pub min_requote_interval_ms: u64,
    /// Replace-rate ceiling per symbol per minute (venue rate-limit guard). Default 100.
    #[serde(default = "default_max_replaces_per_min")]
    pub max_replaces_per_minute_per_symbol: u32,
}

impl Default for LiveQuoteCfg {
    fn default() -> Self {
        LiveQuoteCfg {
            use_hot_integer_math: true,
            reduce_position_only: true,
            replace_immediately_if_unprofitable: true,
            min_requote_interval_ms: default_min_requote_interval_ms(),
            max_replaces_per_minute_per_symbol: default_max_replaces_per_min(),
        }
    }
}

impl LiveQuoteCfg {
    /// Effective per-symbol replace cap. `0` resolves to the conservative default instead
    /// of disabling the limiter.
    pub fn effective_max_replaces_per_minute_per_symbol(&self) -> u32 {
        if self.max_replaces_per_minute_per_symbol == 0 {
            default_max_replaces_per_min()
        } else {
            self.max_replaces_per_minute_per_symbol
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LivePartialsCfg {
    #[serde(default)]
    pub policy: PartialPolicy,
    /// Accumulation cap (USD). Only meaningful under `accumulate_sub_min`. Default "0".
    #[serde(default)]
    pub max_pending_notional_usd: Decimal,
    /// Accumulation age cap (ms). Default 0 (strict).
    #[serde(default)]
    pub max_pending_age_ms: i64,
    /// Lighter's minimum order notional (USD): the smallest hedge the pair can send. Default "10".
    #[serde(default = "default_lighter_min_notional")]
    pub lighter_min_notional: Decimal,
}

impl Default for LivePartialsCfg {
    fn default() -> Self {
        LivePartialsCfg {
            policy: PartialPolicy::default(),
            max_pending_notional_usd: Decimal::ZERO,
            max_pending_age_ms: 0,
            lighter_min_notional: default_lighter_min_notional(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiveAsterCfg {
    #[serde(default = "default_aster_base_url")]
    pub base_url: String,
    /// Aster countdown dead-man cancel-all backstop. Default true.
    #[serde(default = "default_true")]
    pub deadman_enabled: bool,
    /// Countdown window (ms) handed to Aster's countdownCancelAll. Default 5000.
    #[serde(default = "default_deadman_countdown_ms")]
    pub deadman_countdown_ms: i64,
    /// Control-plane refresh cadence (ms) of the dead-man countdown. Default 1000.
    #[serde(default = "default_deadman_refresh_ms")]
    pub deadman_refresh_ms: i64,
    /// Live Aster REST write budget per minute. Counts real REST request units, not logical
    /// strategy decisions: place=1, targeted cancel=1, cancel+place replace=2, each
    /// CancelAllBot/deadman request=1. `0` resolves to the safe default, not unlimited.
    #[serde(default = "default_aster_max_rest_requests_per_minute")]
    pub max_rest_requests_per_minute: u32,
    /// Portion of the above budget reserved for risk-reducing Aster work. Optional place/replace
    /// churn may only consume `max - reserve`, leaving room for cancels, CancelAllBot, and deadman.
    #[serde(default = "default_aster_optional_rest_reserve_per_minute")]
    pub optional_rest_reserve_per_minute: u32,
    /// Minimum delay before the same slot may enqueue another targeted cancel while still pending.
    /// This suppresses cancel spam on every book wake when local state remains live.
    #[serde(default = "default_aster_cancel_retry_backoff_ms")]
    pub cancel_retry_backoff_ms: u64,
    /// Retry cadence for CancelAllBot while a safety sweep is pending.
    #[serde(default = "default_aster_safety_sweep_retry_ms")]
    pub safety_sweep_retry_ms: u64,
    /// Backoff after a real Aster 429/-1003. The worker pauses before sending more Aster REST
    /// requests and emits an event that freezes maker quoting.
    #[serde(default = "default_aster_rate_limit_backoff_ms")]
    pub rate_limit_backoff_ms: i64,
}

impl Default for LiveAsterCfg {
    fn default() -> Self {
        LiveAsterCfg {
            base_url: default_aster_base_url(),
            deadman_enabled: true,
            deadman_countdown_ms: default_deadman_countdown_ms(),
            deadman_refresh_ms: default_deadman_refresh_ms(),
            max_rest_requests_per_minute: default_aster_max_rest_requests_per_minute(),
            optional_rest_reserve_per_minute: default_aster_optional_rest_reserve_per_minute(),
            cancel_retry_backoff_ms: default_aster_cancel_retry_backoff_ms(),
            safety_sweep_retry_ms: default_aster_safety_sweep_retry_ms(),
            rate_limit_backoff_ms: default_aster_rate_limit_backoff_ms(),
        }
    }
}

impl LiveAsterCfg {
    /// Effective write cap. `0` intentionally maps to the safe default; live Aster REST has a hard
    /// external quota and should never be unlimited by accident.
    pub fn effective_max_rest_requests_per_minute(&self) -> u32 {
        if self.max_rest_requests_per_minute == 0 {
            default_aster_max_rest_requests_per_minute()
        } else {
            self.max_rest_requests_per_minute
        }
    }

    /// Reserve held back from optional quote work. Clamp so at least one request unit remains
    /// available for optional work when the max itself is positive.
    pub fn effective_optional_rest_reserve_per_minute(&self) -> u32 {
        let max = self.effective_max_rest_requests_per_minute();
        self.optional_rest_reserve_per_minute.min(max.saturating_sub(1))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiveLighterCfg {
    #[serde(default = "default_hl_base_url")]
    pub base_url: String,
    #[serde(default = "default_lighter_signers_dir")]
    pub signers_dir: String,
    /// Normal IOC hedge slippage cap (bps). Default "5".
    #[serde(default = "default_normal_slippage_bps")]
    pub normal_slippage_bps: Decimal,
    /// Emergency (second-attempt) IOC hedge slippage cap (bps). Default "20".
    #[serde(default = "default_emergency_slippage_bps")]
    pub emergency_slippage_bps: Decimal,
    /// Max wait for an accepted Lighter transaction to surface as an account trade.
    #[serde(default = "default_lighter_fill_timeout_ms")]
    pub fill_timeout_ms: i64,
    /// Max age (ms) of a WS account-feed cache entry before the reconciler's reads fall
    /// back to REST. Keep at/below the reconcile cadence (~max_account_snapshot_age_ms/2,
    /// clamped 500..2000) so two consecutive snapshots can never share one stale cache read.
    #[serde(default = "default_ws_account_max_age_ms")]
    pub ws_account_max_age_ms: i64,
}

/// The hedge on Hyperliquid reuses `[live.lighter]`'s slippage caps.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiveHyperliquidCfg {
    #[serde(default = "default_hyperliquid_base_url")]
    pub base_url: String,
}

impl Default for LiveHyperliquidCfg {
    fn default() -> Self {
        LiveHyperliquidCfg { base_url: default_hyperliquid_base_url() }
    }
}

pub(crate) fn default_hyperliquid_base_url() -> String {
    crate::hyperliquid::client::MAINNET.to_string()
}

impl Default for LiveLighterCfg {
    fn default() -> Self {
        LiveLighterCfg {
            base_url: default_hl_base_url(),
            signers_dir: default_lighter_signers_dir(),
            normal_slippage_bps: default_normal_slippage_bps(),
            emergency_slippage_bps: default_emergency_slippage_bps(),
            fill_timeout_ms: default_lighter_fill_timeout_ms(),
            ws_account_max_age_ms: default_ws_account_max_age_ms(),
        }
    }
}

/// Cumulative-loss circuit breaker. When enabled, the strategy tracks total
/// cross-venue marked equity (Aster wallet + unrealized, Lighter portfolio value + marked uPnL)
/// against a baseline = the median of the first 5 fresh samples. A drawdown beyond
/// `max_cumulative_loss_usdc` on 3 consecutive fresh samples cancels orders, leaves the
/// (delta-neutral) position open, writes a persistent trip-latch file, and halts. The bot then
/// refuses to restart until the latch is cleared (see `scripts/reset_breaker.py`). Inert unless
/// `enabled`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiveCircuitBreakerCfg {
    /// Arm the breaker. Default false (off unless a live config turns it on).
    #[serde(default)]
    pub enabled: bool,
    /// Total account equity drawdown (USDC) from the startup baseline that trips the halt.
    /// Default "0", which validate() rejects while `enabled`.
    #[serde(default)]
    pub max_cumulative_loss_usdc: Decimal,
}

impl Default for LiveCircuitBreakerCfg {
    fn default() -> Self {
        LiveCircuitBreakerCfg { enabled: false, max_cumulative_loss_usdc: Decimal::ZERO }
    }
}

impl Default for LiveCfg {
    fn default() -> Self {
        LiveCfg {
            enabled: false,
            post_trade_cooldown_ms: default_cooldown_ms(),
            cooldown_scope: default_cooldown_scope(),
            startup_cancel_all: true,
            shutdown_cancel_all: true,
            require_clean_start: true,
            max_unhedged_notional_usd: default_max_unhedged_notional(),
            max_unhedged_age_ms: default_max_unhedged_age_ms(),
            max_position_mismatch_usd: default_max_position_mismatch(),
            max_account_snapshot_age_ms: default_max_account_snapshot_age_ms(),
            max_user_stream_staleness_ms: default_max_user_stream_staleness_ms(),
            max_book_staleness_ms: default_max_book_staleness_ms(),
            cancel_all_on_gate_close: true,
            cancel_all_on_user_stream_stale: true,
            quote: LiveQuoteCfg::default(),
            partials: LivePartialsCfg::default(),
            aster: LiveAsterCfg::default(),
            lighter: LiveLighterCfg::default(),
            hyperliquid: LiveHyperliquidCfg::default(),
            circuit_breaker: LiveCircuitBreakerCfg::default(),
            margin_guard: LiveMarginGuardCfg::default(),
            dry_run: false,
        }
    }
}

impl LiveCfg {
    /// Whether the cooldown is global (vs per-market). Defaults to global on any
    /// unrecognized value, the safer choice.
    pub fn cooldown_is_global(&self) -> bool {
        !self.cooldown_scope.eq_ignore_ascii_case("per_market")
    }

    /// Sanity-check the live section. Only enforced when `enabled` — a disabled (default)
    /// section is inert and never blocks loading the config.
    pub fn validate(&self) -> Result<()> {
        if !self.enabled {
            return Ok(());
        }
        if self.post_trade_cooldown_ms < 0 {
            bail!("live.post_trade_cooldown_ms must be non-negative");
        }
        if self.max_unhedged_age_ms < 0
            || self.max_account_snapshot_age_ms <= 0
            || self.max_user_stream_staleness_ms <= 0
        {
            bail!("live staleness/age timeouts must be positive except max_unhedged_age_ms may be zero");
        }
        if self.max_unhedged_notional_usd < Decimal::ZERO
            || self.max_position_mismatch_usd < Decimal::ZERO
        {
            bail!("live risk notionals must be non-negative");
        }
        if self.lighter.normal_slippage_bps < Decimal::ZERO
            || self.lighter.emergency_slippage_bps < Decimal::ZERO
        {
            bail!("live.lighter slippage bps must be non-negative");
        }
        if self.lighter.emergency_slippage_bps < self.lighter.normal_slippage_bps {
            bail!("live.lighter.emergency_slippage_bps should be >= normal_slippage_bps");
        }
        if self.lighter.fill_timeout_ms <= 0 {
            bail!("live.lighter.fill_timeout_ms must be positive");
        }
        if self.aster.deadman_enabled && self.aster.deadman_refresh_ms >= self.aster.deadman_countdown_ms {
            bail!("live.aster.deadman_refresh_ms must be < deadman_countdown_ms (refresh before it fires)");
        }
        if self.aster.effective_max_rest_requests_per_minute() == 0 {
            bail!("live.aster.max_rest_requests_per_minute must resolve to > 0");
        }
        if self.aster.cancel_retry_backoff_ms == 0 {
            bail!("live.aster.cancel_retry_backoff_ms must be > 0");
        }
        if self.aster.safety_sweep_retry_ms == 0 {
            bail!("live.aster.safety_sweep_retry_ms must be > 0");
        }
        if self.aster.rate_limit_backoff_ms <= 0 {
            bail!("live.aster.rate_limit_backoff_ms must be > 0");
        }
        if self.circuit_breaker.enabled && self.circuit_breaker.max_cumulative_loss_usdc <= Decimal::ZERO {
            bail!("live.circuit_breaker.max_cumulative_loss_usdc must be > 0 when the breaker is enabled");
        }
        if self.margin_guard.aster_safety_buffer_usd < Decimal::ZERO {
            bail!("live.margin_guard.aster_safety_buffer_usd must be non-negative");
        }
        if self.margin_guard.lighter_safety_buffer_usd < Decimal::ZERO {
            bail!("live.margin_guard.lighter_safety_buffer_usd must be non-negative");
        }
        Ok(())
    }
}

/// Per-venue reserve held back from fresh free margin before admitting exposure.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiveMarginGuardCfg {
    /// Master switch for the proactive guard. Default true.
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// USD held back from real Aster collateral before the dynamic position-notional cap. Covers an
    /// in-flight/stale quote plus mark movement over the reconcile/forced-tick cadence. Default "25".
    #[serde(default = "default_margin_safety_buffer")]
    pub aster_safety_buffer_usd: Decimal,
    #[serde(default = "default_margin_safety_buffer")]
    pub lighter_safety_buffer_usd: Decimal,
}

impl Default for LiveMarginGuardCfg {
    fn default() -> Self {
        LiveMarginGuardCfg {
            enabled: true,
            aster_safety_buffer_usd: default_margin_safety_buffer(),
            lighter_safety_buffer_usd: default_margin_safety_buffer(),
        }
    }
}

fn default_margin_safety_buffer() -> Decimal {
    Decimal::from(25)
}

fn default_cooldown_ms() -> i64 {
    60_000
}
fn default_cooldown_scope() -> String {
    "global".to_string()
}
fn default_max_unhedged_notional() -> Decimal {
    Decimal::from(5)
}
fn default_max_unhedged_age_ms() -> i64 {
    1_000
}
fn default_max_position_mismatch() -> Decimal {
    Decimal::from(2)
}
fn default_max_account_snapshot_age_ms() -> i64 {
    3_000
}
fn default_max_user_stream_staleness_ms() -> i64 {
    5_000
}
fn default_max_book_staleness_ms() -> i64 {
    10_000
}
fn default_lighter_min_notional() -> Decimal {
    Decimal::from(10)
}
fn default_min_requote_interval_ms() -> u64 {
    20
}
fn default_max_replaces_per_min() -> u32 {
    100
}
pub(crate) fn default_aster_base_url() -> String {
    "https://fapi.asterdex.com".to_string()
}
pub(crate) fn default_hl_base_url() -> String {
    "https://mainnet.zklighter.elliot.ai".to_string()
}

/// File-loaded operational configs of both engines perform venue I/O: pin the supported origins.
pub(crate) fn require_mainnet_origins(aster_base_url: &str, lighter_base_url: &str, hyperliquid_base_url: &str) -> Result<()> {
    if aster_base_url.trim_end_matches('/') != default_aster_base_url()
        || lighter_base_url.trim_end_matches('/') != default_hl_base_url()
        || hyperliquid_base_url.trim_end_matches('/') != default_hyperliquid_base_url()
    {
        bail!("operational venue URLs must use the supported Aster, Lighter and Hyperliquid mainnet origins");
    }
    Ok(())
}
fn default_lighter_signers_dir() -> String {
    "signers".to_string()
}
fn default_lighter_fill_timeout_ms() -> i64 {
    2_000
}

fn default_ws_account_max_age_ms() -> i64 {
    1_500
}
fn default_normal_slippage_bps() -> Decimal {
    Decimal::from(5)
}
fn default_emergency_slippage_bps() -> Decimal {
    Decimal::from(20)
}
fn default_deadman_countdown_ms() -> i64 {
    5_000
}
fn default_deadman_refresh_ms() -> i64 {
    1_000
}
fn default_aster_max_rest_requests_per_minute() -> u32 {
    // Aster reported 2400/min during the bad run. Stay well below that so account reads,
    // listen-key maintenance, deadman refreshes, and manual intervention still have headroom.
    1_200
}
fn default_aster_optional_rest_reserve_per_minute() -> u32 {
    120
}
fn default_aster_cancel_retry_backoff_ms() -> u64 {
    1_000
}
fn default_aster_safety_sweep_retry_ms() -> u64 {
    2_000
}
fn default_aster_rate_limit_backoff_ms() -> i64 {
    5_000
}

/// The venue a market hedges on.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum HedgeVenue {
    #[default]
    Lighter,
    Hyperliquid,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketCfg {
    pub aster_symbol: String,
    /// The hedge venue's coin, on Hyperliquid as on Lighter.
    #[serde(rename = "lighter_symbol")]
    pub hl_coin: String,
    #[serde(default)]
    pub hedge_venue: HedgeVenue,
    /// Optional logical id; defaults to `lighter_symbol`.
    #[serde(default)]
    pub market_id: Option<String>,
}

impl MarketCfg {
    pub fn id(&self) -> MarketId {
        MarketId(self.market_id.clone().unwrap_or_else(|| self.hl_coin.clone()))
    }
}

/// Reads a TOML config file and returns its `section` table: `bot.toml` holds each engine's
/// config under its own table (`[maker]`, `[taker]`); a single-engine file is used whole.
pub fn read_table(path: &Path, section: &str) -> Result<toml::Value> {
    let text = std::fs::read_to_string(path).with_context(|| format!("reading config {}", path.display()))?;
    let mut value: toml::Value =
        toml::from_str(&text).with_context(|| format!("parsing config {}", path.display()))?;
    Ok(value.as_table_mut().and_then(|table| table.remove(section)).unwrap_or(value))
}

/// Deserializes a config table, rejecting keys the target does not define: a typo or a key in
/// the wrong table must fail startup, not silently leave a default in force.
pub fn strict_from_toml<T: serde::de::DeserializeOwned>(value: toml::Value) -> Result<T> {
    let mut unknown = Vec::new();
    let parsed = serde_ignored::deserialize(value, |path| unknown.push(path.to_string()))?;
    if !unknown.is_empty() {
        bail!("unknown config keys: {}", unknown.join(", "));
    }
    Ok(parsed)
}

impl Config {
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        Self::from_table(read_table(path, "maker")?).with_context(|| format!("config {}", path.display()))
    }

    /// Checks a file-loaded XEMM table (`[maker]` of `bot.toml`).
    pub fn from_table(value: toml::Value) -> Result<Self> {
        for retired in [
            "live.partials.max_pending_count",
            "live.quote.price_change_ticks_to_requote",
            "live.lighter.expires_after_ms",
            "live.lighter.expires_after_ms",
        ] {
            if retired.split('.').try_fold(&value, |v, key| v.get(key)).is_some() {
                bail!("retired no-op setting {retired}; remove it (requote ticks belong in [quote])");
            }
        }
        let cfg: Config = strict_from_toml(value)?;
        cfg.validate()?;
        require_mainnet_origins(&cfg.live.aster.base_url, &cfg.live.lighter.base_url, &cfg.live.hyperliquid.base_url)?;
        Ok(cfg)
    }

    pub fn validate(&self) -> Result<()> {
        if self.markets.is_empty() {
            bail!("config has no [[markets]]");
        }
        let mut ids = HashSet::new();
        let mut aster_symbols = HashSet::new();
        let mut hl_coins = HashSet::new();
        for m in &self.markets {
            let id = m.id().0.to_ascii_uppercase();
            if !ids.insert(id.clone()) {
                bail!("duplicate market_id {id:?} in [[markets]]");
            }
            // One symbol may be hedged on each venue; live, the Aster symbol's lock keeps them apart.
            let aster = m.aster_symbol.to_ascii_uppercase();
            if !aster_symbols.insert((aster.clone(), m.hedge_venue)) {
                bail!("duplicate aster_symbol {aster:?} for one hedge venue in [[markets]]");
            }
            let hl = m.hl_coin.to_ascii_uppercase();
            if !hl_coins.insert((hl.clone(), m.hedge_venue)) {
                bail!("duplicate lighter_symbol {hl:?} for one hedge venue in [[markets]]");
            }
        }
        if self.book_check.enabled {
            if self.book_check.max_concurrent_requests == 0 {
                bail!("book_check.max_concurrent_requests must be >= 1");
            }
            if self.book_check.max_rest_snapshot_age_ms <= 0 {
                bail!("book_check.max_rest_snapshot_age_ms must be positive");
            }
            // Each check reads Lighter's book over REST: at 1 s that is a Standard account's
            // whole 60 calls a minute.
            if self.book_check.interval_secs < 10 {
                bail!("book_check.interval_secs must be >= 10");
            }
        }
        let e = &self.edge;
        for (name, bps) in [("min_net_profit_bps", e.min_net_profit_bps), ("slippage_buffer_bps", e.slippage_buffer_bps),
            ("latency_buffer_bps", e.latency_buffer_bps), ("basis_buffer_bps", e.basis_buffer_bps),
            ("funding_buffer_bps", e.funding_buffer_bps)] {
            if bps < Decimal::ZERO {
                bail!("edge.{name} must be non-negative: a negative one admits quotes below the fees");
            }
        }
        if self.live.max_book_staleness_ms < 0 {
            bail!("live.max_book_staleness_ms must be non-negative");
        }
        if self.capital.aster_capital_usd <= Decimal::ZERO
            || self.capital.lighter_capital_usd <= Decimal::ZERO
            || self.capital.leverage <= Decimal::ZERO
        {
            bail!("capital amounts and leverage must be positive");
        }
        if self.quote.min_aster_touch_hysteresis_bps < Decimal::ZERO {
            bail!("quote.min_aster_touch_hysteresis_bps must be non-negative");
        }
        if self.quote.max_aster_touch_hysteresis_ms < 0 {
            bail!("quote.max_aster_touch_hysteresis_ms must be non-negative");
        }
        if self.quote.depth_liquidity_multiple < Decimal::ONE {
            bail!("quote.depth_liquidity_multiple must be >= 1");
        }
        // Live margin-guard prerequisites: the dynamic cap reuses the position-cap path and assumes
        // venue leverage == 1 (the startup leverage gate verifies 1x on both venues). Require a
        // matching config so the cap can't be sized for >1x while the venue is 1x, and require the
        // cap to actually be enforced (else the guard is a silent no-op).
        if self.live.enabled && self.live.margin_guard.enabled {
            if self.capital.leverage != Decimal::ONE {
                bail!("live.margin_guard requires capital.leverage = 1 (venue leverage is gated to 1x)");
            }
            if !self.capital.enforce_position_cap {
                bail!("live.margin_guard requires capital.enforce_position_cap = true (else the cap is a no-op)");
            }
        }
        self.live.validate()?;
        Ok(())
    }

    /// Filter the configured markets by an optional comma-separated id list.
    pub fn select_markets(&self, filter: Option<&str>) -> Vec<MarketCfg> {
        match filter {
            None => self.markets.clone(),
            Some(list) => {
                let wanted: Vec<String> = list
                    .split(',')
                    .map(|s| s.trim().to_ascii_uppercase())
                    .filter(|s| !s.is_empty())
                    .collect();
                self.markets
                    .iter()
                    .filter(|m| wanted.contains(&m.id().0.to_ascii_uppercase()))
                    .cloned()
                    .collect()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    const SAMPLE: &str = r#"
[edge]
min_net_profit_bps = "3.0"
slippage_buffer_bps = "1.5"
latency_buffer_bps = "2.0"
basis_buffer_bps = "1.0"
funding_buffer_bps = "0.0"
aster_maker_fee_bps = "0.0"
taker_fee_bps = "0.0"

[quote]
desired_notional = "100"
max_quote_distance_bps = "5.0"
min_lighter_bbo_depth_multiple = "10.0"
max_hedge_slippage_bps = "5.0"
price_change_ticks_to_requote = 1
clamp_to_min_lot = true

[capital]
aster_capital_usd = "1000"
lighter_capital_usd = "1000"
leverage = "1"
enforce_position_cap = true

[[markets]]
aster_symbol = "BTCUSDT"
lighter_symbol = "BTC"

[[markets]]
aster_symbol = "DOGEUSDT"
lighter_symbol = "DOGE"
"#;

    #[test]
    fn parse_sample() {
        let cfg: Config = toml::from_str(SAMPLE).unwrap();
        cfg.validate().unwrap();
        assert_eq!(cfg.edge.min_net_profit_bps, dec!(3.0));
        assert_eq!(cfg.quote.desired_notional, dec!(100));
        assert_eq!(cfg.quote.min_aster_touch_distance_bps, dec!(0));
        assert_eq!(cfg.quote.min_aster_touch_hysteresis_bps, dec!(2));
        assert_eq!(cfg.quote.max_aster_touch_hysteresis_ms, 300_000);
        assert_eq!(cfg.quote.depth_liquidity_multiple, dec!(10.0));
        assert!(cfg.quote.clamp_to_min_lot);
        // No [book_check] section in SAMPLE => the cross-check defaults on.
        assert!(cfg.book_check.enabled);
        assert_eq!(cfg.book_check.interval_secs, 30);
        assert_eq!(cfg.book_check.tolerance_bps, dec!(50));
        assert_eq!(cfg.book_check.consecutive_breaches, 3);
        assert_eq!(cfg.book_check.max_concurrent_requests, 8);
        assert_eq!(cfg.book_check.max_rest_snapshot_age_ms, 3_000);
        assert_eq!(cfg.capital.aster_capital_usd, dec!(1000));
        assert_eq!(cfg.capital.aster_cap_notional(), dec!(1000));
        assert_eq!(cfg.markets.len(), 2);
        assert_eq!(cfg.markets[0].id().0, "BTC");
    }

    #[test]
    fn clamp_to_min_lot_defaults_on_when_absent() {
        let without = SAMPLE.replace("clamp_to_min_lot = true\n", "");
        let cfg: Config = toml::from_str(&without).unwrap();
        assert!(cfg.quote.clamp_to_min_lot);
    }

    #[test]
    fn select_markets_filter() {
        let cfg: Config = toml::from_str(SAMPLE).unwrap();
        let only = cfg.select_markets(Some("doge"));
        assert_eq!(only.len(), 1);
        assert_eq!(only[0].hl_coin, "DOGE");
    }

    #[test]
    fn live_section_defaults_when_absent_and_is_inert() {
        // SAMPLE has no [live] section: it must default to a disabled, inert live config.
        let cfg: Config = toml::from_str(SAMPLE).unwrap();
        cfg.validate().unwrap();
        assert!(!cfg.live.enabled);
        assert_eq!(cfg.live.post_trade_cooldown_ms, 60_000);
        assert_eq!(cfg.live.max_book_staleness_ms, 10_000);
        assert!(cfg.live.cooldown_is_global());
        assert_eq!(cfg.live.partials.policy, PartialPolicy::StrictEveryFillMustBeHedgeable);
        assert_eq!(cfg.live.partials.lighter_min_notional, dec!(10));
        assert_eq!(cfg.live.aster.base_url, "https://fapi.asterdex.com");
        assert_eq!(cfg.live.lighter.normal_slippage_bps, dec!(5));
        assert!(cfg.live.aster.deadman_refresh_ms < cfg.live.aster.deadman_countdown_ms);
        assert!(cfg.live.quote.reduce_position_only);
        assert_eq!(cfg.live.quote.effective_max_replaces_per_minute_per_symbol(), 100);
        assert_eq!(cfg.live.aster.effective_max_rest_requests_per_minute(), 1_200);
        assert_eq!(cfg.live.aster.effective_optional_rest_reserve_per_minute(), 120);
        assert_eq!(cfg.live.aster.cancel_retry_backoff_ms, 1_000);
        assert_eq!(cfg.live.aster.safety_sweep_retry_ms, 2_000);
        assert_eq!(cfg.live.aster.rate_limit_backoff_ms, 5_000);
    }

    #[test]
    fn live_section_parses_and_validates() {
        let with_live = format!(
            "{SAMPLE}\n[live]\nenabled = true\npost_trade_cooldown_ms = 30000\ncooldown_scope = \"per_market\"\nmax_unhedged_notional_usd = \"7\"\nmax_book_staleness_ms = 750\n\n[live.partials]\npolicy = \"accumulate_sub_min\"\nmax_pending_notional_usd = \"5\"\nmax_pending_age_ms = 1000\nlighter_min_notional = \"12\"\n\n[live.lighter]\nnormal_slippage_bps = \"4\"\nemergency_slippage_bps = \"25\"\n"
        );
        let cfg: Config = toml::from_str(&with_live).unwrap();
        cfg.validate().unwrap();
        assert!(cfg.live.enabled);
        assert_eq!(cfg.live.post_trade_cooldown_ms, 30_000);
        assert_eq!(cfg.live.max_book_staleness_ms, 750);
        assert!(!cfg.live.cooldown_is_global());
        assert_eq!(cfg.live.partials.policy, PartialPolicy::AccumulateSubMin);
        assert_eq!(cfg.live.partials.lighter_min_notional, dec!(12));
        assert_eq!(cfg.live.lighter.emergency_slippage_bps, dec!(25));
    }

    #[test]
    fn live_quote_reduce_position_only_can_be_disabled() {
        let with_live = format!(
            "{SAMPLE}\n[live]\nenabled = true\n\n[live.quote]\nreduce_position_only = false\n"
        );
        let cfg: Config = toml::from_str(&with_live).unwrap();
        cfg.validate().unwrap();
        assert!(!cfg.live.quote.reduce_position_only);
    }

    #[test]
    fn live_zero_caps_resolve_to_safe_defaults_not_unlimited() {
        let with_live = format!(
            "{SAMPLE}\n[live]\nenabled = true\n\n[live.quote]\nmax_replaces_per_minute_per_symbol = 0\n\n[live.aster]\nmax_rest_requests_per_minute = 0\noptional_rest_reserve_per_minute = 999999\nrate_limit_backoff_ms = 2500\n"
        );
        let cfg: Config = toml::from_str(&with_live).unwrap();
        cfg.validate().unwrap();
        assert_eq!(cfg.live.quote.effective_max_replaces_per_minute_per_symbol(), 100);
        assert_eq!(cfg.live.aster.effective_max_rest_requests_per_minute(), 1_200);
        assert_eq!(cfg.live.aster.effective_optional_rest_reserve_per_minute(), 1_199);
        assert_eq!(cfg.live.aster.rate_limit_backoff_ms, 2_500);
    }

    #[test]
    fn live_validate_rejects_inverted_slippage_when_enabled() {
        let bad = format!(
            "{SAMPLE}\n[live]\nenabled = true\n\n[live.lighter]\nnormal_slippage_bps = \"20\"\nemergency_slippage_bps = \"5\"\n"
        );
        let cfg: Config = toml::from_str(&bad).unwrap();
        assert!(cfg.validate().is_err());
        // ...but the same inversion is ignored while disabled (section is inert).
        let bad_disabled = bad.replace("enabled = true", "enabled = false");
        let cfg: Config = toml::from_str(&bad_disabled).unwrap();
        cfg.validate().unwrap();
    }

    #[test]
    fn margin_guard_defaults_enabled_with_25_buffer() {
        // No [live.margin_guard] block => defaults: enabled, $25 buffer. (SAMPLE has leverage=1 +
        // enforce_position_cap=true, so the guard's prerequisites are satisfied.)
        let with_live = format!("{SAMPLE}\n[live]\nenabled = true\n");
        let cfg: Config = toml::from_str(&with_live).unwrap();
        cfg.validate().unwrap();
        assert!(cfg.live.margin_guard.enabled);
        assert_eq!(cfg.live.margin_guard.aster_safety_buffer_usd, dec!(25));
        assert_eq!(cfg.live.margin_guard.lighter_safety_buffer_usd, dec!(25));
        // Explicit block parses and overrides the buffer.
        let explicit = format!(
            "{SAMPLE}\n[live]\nenabled = true\n\n[live.margin_guard]\nenabled = true\naster_safety_buffer_usd = \"26\"\n"
        );
        let cfg: Config = toml::from_str(&explicit).unwrap();
        cfg.validate().unwrap();
        assert_eq!(cfg.live.margin_guard.aster_safety_buffer_usd, dec!(26));
    }

    #[test]
    fn file_loading_rejects_retired_controls_and_mixed_environments() {
        let dir = crate::dryrun::tests::temp_dir("xemm-config");
        let path = dir.join("config.toml");
        for extra in [
            "[live.partials]\nmax_pending_count=2",
            "[live.quote]\nprice_change_ticks_to_requote=2",
            "[live.lighter]\nexpires_after_ms=1000",
        ] {
            std::fs::write(&path, format!("{SAMPLE}\n{extra}\n")).unwrap();
            assert!(format!("{:#}", Config::load(&path).unwrap_err()).contains("retired no-op"));
        }
        // The research-simulator sections are gone: the strict loader rejects them.
        for section in [
            "[simulation]\nmax_book_staleness_ms=750",
            "[partials]\nlighter_min_notional=\"10\"",
            "[queue_model]\nmodels=[\"optimistic\"]",
            "[runtime]\ndb_path=\"runs/eval.sqlite\"",
        ] {
            std::fs::write(&path, format!("{SAMPLE}\n{section}\n")).unwrap();
            assert!(format!("{:#}", Config::load(&path).unwrap_err()).contains("unknown config keys"));
        }
        std::fs::write(&path, format!("{SAMPLE}\n[live.aster]\nbase_url=\"https://example.test\"\n")).unwrap();
        assert!(format!("{:#}", Config::load(&path).unwrap_err()).contains("mainnet origins"));
        std::fs::write(&path, include_str!("../bot.toml")).unwrap();
        let cfg = Config::load(&path).unwrap();
        assert_eq!(cfg.live.margin_guard.lighter_safety_buffer_usd, dec!(26)); // bot.toml value, default is 25
        std::fs::remove_file(path).unwrap();
        std::fs::remove_dir(dir).unwrap();
    }

    #[test]
    fn margin_guard_requires_leverage_one_and_enforced_cap() {
        let base = format!("{SAMPLE}\n[live]\nenabled = true\n");
        // leverage != 1 with the guard enabled => reject (real venue leverage is gated to 1x).
        let lev2 = base.replace("leverage = \"1\"", "leverage = \"2\"");
        assert!(toml::from_str::<Config>(&lev2).unwrap().validate().is_err());
        // enforce_position_cap = false with the guard enabled => reject (the cap would be a no-op).
        let noenforce = base.replace("enforce_position_cap = true", "enforce_position_cap = false");
        assert!(toml::from_str::<Config>(&noenforce).unwrap().validate().is_err());
        // ...but with the guard DISABLED, leverage != 1 is allowed again (the guard is inert).
        let lev2_off = format!(
            "{SAMPLE}\n[live]\nenabled = true\n\n[live.margin_guard]\nenabled = false\n"
        )
        .replace("leverage = \"1\"", "leverage = \"2\"");
        toml::from_str::<Config>(&lev2_off).unwrap().validate().unwrap();
    }

    #[test]
    fn margin_guard_negative_buffer_rejected() {
        let neg = format!(
            "{SAMPLE}\n[live]\nenabled = true\n\n[live.margin_guard]\naster_safety_buffer_usd = \"-1\"\n"
        );
        assert!(toml::from_str::<Config>(&neg).unwrap().validate().is_err());
    }

    #[test]
    fn negative_touch_hysteresis_rejected() {
        let bad = SAMPLE.replace(
            "price_change_ticks_to_requote = 1\n",
            "price_change_ticks_to_requote = 1\nmin_aster_touch_hysteresis_bps = \"-1\"\n",
        );
        assert!(toml::from_str::<Config>(&bad).unwrap().validate().is_err());
    }

    #[test]
    fn negative_touch_hysteresis_timeout_rejected() {
        let bad = SAMPLE.replace(
            "price_change_ticks_to_requote = 1\n",
            "price_change_ticks_to_requote = 1\nmax_aster_touch_hysteresis_ms = -1\n",
        );
        assert!(toml::from_str::<Config>(&bad).unwrap().validate().is_err());
    }
}
