use std::collections::HashSet;
use std::path::Path;

use anyhow::{bail, Context, Result};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::taker::book::MAX_BOOK_LEVELS;
use crate::taker::types::MarketId;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub arb: ArbCfg,
    #[serde(default)]
    pub pnl: PnlCfg,
    #[serde(default)]
    pub live: LiveCfg,
    #[serde(default)]
    pub venues: VenueCfg,
    #[serde(default)]
    pub risk: RiskCfg,
    #[serde(default)]
    pub markets: Vec<MarketCfg>,
}

impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        Self::from_table(crate::config::read_table(path, "taker")?)
            .with_context(|| format!("config {}", path.display()))
    }

    /// Checks a file-loaded taker table (`[taker]` of `bot.toml`).
    pub fn from_table(value: toml::Value) -> Result<Self> {
        let cfg: Config = crate::config::strict_from_toml(value)?;
        cfg.validate()?;
        let venues = &cfg.venues;
        crate::config::require_mainnet_origins(&venues.aster_base_url, &venues.lighter_base_url, &venues.hyperliquid_base_url)?;
        Ok(cfg)
    }

    pub fn validate(&self) -> Result<()> {
        if self.markets.is_empty() {
            bail!("config has no [[markets]]");
        }
        if self.arb.desired_notional <= Decimal::ZERO {
            bail!("arb.desired_notional must be positive");
        }
        if self.arb.margin_bps < Decimal::ZERO {
            bail!("arb.margin_bps must be non-negative");
        }
        if self.arb.aster_taker_fee_bps < Decimal::ZERO
            || self.arb.lighter_taker_fee_bps < Decimal::ZERO
            || self.arb.hyperliquid_taker_fee_bps.is_some_and(|fee| fee < Decimal::ZERO)
        {
            bail!("taker fees must be non-negative");
        }
        if self.arb.max_aster_slippage_bps < Decimal::ZERO
            || self.arb.max_lighter_slippage_bps < Decimal::ZERO
            || self.arb.emergency_slippage_bps < Decimal::ZERO
            || self.arb.hedge_retry_slippage_bps < Decimal::ZERO
        {
            bail!("slippage bps must be non-negative");
        }
        if self.arb.hedge_retry_timeout_ms == 0 {
            bail!("arb.hedge_retry_timeout_ms must be > 0");
        }
        if self.arb.max_hedge_retry_attempts != 1 {
            bail!("arb.max_hedge_retry_attempts must be 1; unresolved orders must not be resubmitted");
        }
        self.arb.depth_guard.validate()?;
        self.arb.book_sanity.validate()?;
        if self.arb.poll_interval_ms == 0 {
            bail!("arb.poll_interval_ms must be > 0");
        }
        if self.arb.max_book_staleness_ms <= 0 {
            bail!("arb.max_book_staleness_ms must be > 0");
        }
        if self.arb.max_recovered_failures_per_hour == 0 {
            bail!("arb.max_recovered_failures_per_hour must be > 0");
        }
        if self.arb.max_recovered_loss_usdc_per_hour <= Decimal::ZERO {
            bail!("arb.max_recovered_loss_usdc_per_hour must be positive");
        }
        self.arb.entry_gate.validate()?;
        if self.pnl.enabled && self.pnl.max_loss_usdc <= Decimal::ZERO {
            bail!("pnl.max_loss_usdc must be positive when pnl is enabled");
        }
        if self.live.max_account_snapshot_age_ms <= 0 {
            bail!("live.max_account_snapshot_age_ms must be > 0");
        }
        if self.risk.max_abs_position_notional_usd <= Decimal::ZERO {
            bail!("risk.max_abs_position_notional_usd must be positive");
        }
        if self.risk.max_position_mismatch_usd < Decimal::ZERO
            || self.risk.margin_buffer_usd < Decimal::ZERO
        {
            bail!("risk mismatch and margin buffer values must be non-negative");
        }
        if self.risk.min_reconcile_interval_ms == 0 {
            bail!("risk.min_reconcile_interval_ms must be > 0");
        }
        if self.risk.mismatch_flatten_after_checks == 0 {
            bail!("risk.mismatch_flatten_after_checks must be >= 1");
        }
        // Recovery fires when the market has moved against the naked leg; an emergency
        // bound tighter than the normal entry bounds cannot cross a moved book and would
        // leave the leg open through repeated recovery attempts.
        let normal_max = self
            .arb
            .max_aster_slippage_bps
            .max(self.arb.max_lighter_slippage_bps);
        if self.arb.emergency_slippage_bps < normal_max {
            bail!(
                "arb.emergency_slippage_bps ({}) must be >= the normal slippage bounds ({}): recovery must be able to cross a moved book",
                self.arb.emergency_slippage_bps,
                normal_max
            );
        }
        let mut ids = HashSet::new();
        for m in &self.markets {
            if !ids.insert(m.id().0) {
                bail!("duplicate market_id in [[markets]]");
            }
            let aster_first = m.first_venue == crate::config::FirstVenue::Aster;
            if aster_first == m.aster_symbol.is_empty() || !(aster_first || m.hedge_venue == crate::config::HedgeVenue::Hyperliquid) {
                bail!("{}: an Aster first leg needs aster_symbol; a Lighter one has none and trades against hedge_venue = \"hyperliquid\"", m.id());
            }
        }
        Ok(())
    }

    pub fn select_markets(&self, filter: Option<&str>) -> Vec<MarketCfg> {
        let Some(filter) = filter else {
            return self.markets.clone();
        };
        let wanted: HashSet<String> = filter
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

fn default_max_aster_slippage_bps() -> Decimal {
    Decimal::from(3)
}

fn default_max_recovered_failures_per_hour() -> u64 {
    3
}

fn default_max_recovered_loss_usdc_per_hour() -> Decimal {
    Decimal::new(25, 2)
}

fn default_hedge_retry_slippage_bps() -> Decimal {
    Decimal::from(30)
}

fn default_hedge_retry_timeout_ms() -> u64 {
    5_000
}

fn default_max_hedge_retry_attempts() -> u64 {
    1
}

fn default_depth_guard_enabled() -> bool {
    true
}

fn default_depth_guard_liquidity_multiple() -> Decimal {
    Decimal::from(10)
}

fn default_depth_guard_max_levels() -> usize {
    MAX_BOOK_LEVELS
}

fn default_book_sanity_enabled() -> bool {
    false
}

fn default_book_sanity_interval_ms() -> u64 {
    10_000
}

fn default_book_sanity_max_top_bps() -> Decimal {
    Decimal::from(8)
}

fn default_book_sanity_max_vwap_bps() -> Decimal {
    Decimal::from(8)
}

fn default_book_sanity_required_failures() -> u64 {
    2
}

fn default_book_sanity_required_successes() -> u64 {
    2
}

fn default_book_sanity_block_cooldown_ms() -> u64 {
    15_000
}

fn default_book_sanity_rest_depth_levels() -> usize {
    MAX_BOOK_LEVELS
}

fn default_book_sanity_liquidity_multiple() -> Decimal {
    Decimal::from(10)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArbCfg {
    pub desired_notional: Decimal,
    pub margin_bps: Decimal,
    pub aster_taker_fee_bps: Decimal,
    pub lighter_taker_fee_bps: Decimal,
    /// The second leg's fee when a market hedges on Hyperliquid; the slippage caps stay Lighter's.
    #[serde(default)]
    pub hyperliquid_taker_fee_bps: Option<Decimal>,
    #[serde(default = "default_max_aster_slippage_bps")]
    pub max_aster_slippage_bps: Decimal,
    pub max_lighter_slippage_bps: Decimal,
    pub emergency_slippage_bps: Decimal,
    #[serde(default = "default_hedge_retry_slippage_bps")]
    pub hedge_retry_slippage_bps: Decimal,
    #[serde(default = "default_hedge_retry_timeout_ms")]
    pub hedge_retry_timeout_ms: u64,
    #[serde(default = "default_max_hedge_retry_attempts")]
    pub max_hedge_retry_attempts: u64,
    #[serde(default)]
    pub depth_guard: DepthGuardCfg,
    #[serde(default)]
    pub book_sanity: BookSanityCfg,
    pub cooldown_ms: u64,
    pub startup_warmup_ms: u64,
    pub poll_interval_ms: u64,
    pub max_book_staleness_ms: i64,
    #[serde(default = "default_max_recovered_failures_per_hour")]
    pub max_recovered_failures_per_hour: u64,
    #[serde(default = "default_max_recovered_loss_usdc_per_hour")]
    pub max_recovered_loss_usdc_per_hour: Decimal,
    #[serde(default)]
    pub entry_gate: EntryGateCfg,
}

impl Default for ArbCfg {
    fn default() -> Self {
        ArbCfg {
            desired_notional: Decimal::from(13),
            margin_bps: Decimal::from(2),
            aster_taker_fee_bps: Decimal::from(4),
            lighter_taker_fee_bps: Decimal::ZERO,
            hyperliquid_taker_fee_bps: None,
            max_aster_slippage_bps: Decimal::from(3),
            max_lighter_slippage_bps: Decimal::from(3),
            emergency_slippage_bps: Decimal::from(25),
            hedge_retry_slippage_bps: default_hedge_retry_slippage_bps(),
            hedge_retry_timeout_ms: default_hedge_retry_timeout_ms(),
            max_hedge_retry_attempts: default_max_hedge_retry_attempts(),
            depth_guard: DepthGuardCfg::default(),
            book_sanity: BookSanityCfg::default(),
            cooldown_ms: 60_000,
            startup_warmup_ms: 15_000,
            poll_interval_ms: 250,
            max_book_staleness_ms: 2000,
            max_recovered_failures_per_hour: default_max_recovered_failures_per_hour(),
            max_recovered_loss_usdc_per_hour: default_max_recovered_loss_usdc_per_hour(),
            entry_gate: EntryGateCfg::default(),
        }
    }
}

impl ArbCfg {
    pub fn required_gross_edge_bps(&self) -> Decimal {
        self.aster_taker_fee_bps + self.lighter_taker_fee_bps + self.margin_bps
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct BookSanityCfg {
    pub enabled: bool,
    pub interval_ms: u64,
    pub max_top_bps: Decimal,
    pub max_vwap_bps: Decimal,
    pub required_failures: u64,
    pub required_successes: u64,
    pub block_cooldown_ms: u64,
    pub rest_depth_levels: usize,
    pub liquidity_multiple: Decimal,
}

impl Default for BookSanityCfg {
    fn default() -> Self {
        Self {
            enabled: default_book_sanity_enabled(),
            interval_ms: default_book_sanity_interval_ms(),
            max_top_bps: default_book_sanity_max_top_bps(),
            max_vwap_bps: default_book_sanity_max_vwap_bps(),
            required_failures: default_book_sanity_required_failures(),
            required_successes: default_book_sanity_required_successes(),
            block_cooldown_ms: default_book_sanity_block_cooldown_ms(),
            rest_depth_levels: default_book_sanity_rest_depth_levels(),
            liquidity_multiple: default_book_sanity_liquidity_multiple(),
        }
    }
}

impl BookSanityCfg {
    fn validate(&self) -> Result<()> {
        if self.interval_ms == 0 {
            bail!("arb.book_sanity.interval_ms must be > 0");
        }
        if self.max_top_bps < Decimal::ZERO || self.max_vwap_bps < Decimal::ZERO {
            bail!("arb.book_sanity divergence thresholds must be non-negative");
        }
        if self.required_failures == 0 {
            bail!("arb.book_sanity.required_failures must be > 0");
        }
        if self.required_successes == 0 {
            bail!("arb.book_sanity.required_successes must be > 0");
        }
        if self.block_cooldown_ms == 0 {
            bail!("arb.book_sanity.block_cooldown_ms must be > 0");
        }
        if self.rest_depth_levels == 0 || self.rest_depth_levels > MAX_BOOK_LEVELS {
            bail!("arb.book_sanity.rest_depth_levels must be in [1, {MAX_BOOK_LEVELS}]");
        }
        if self.liquidity_multiple <= Decimal::ZERO {
            bail!("arb.book_sanity.liquidity_multiple must be positive");
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct DepthGuardCfg {
    pub enabled: bool,
    pub liquidity_multiple: Decimal,
    pub max_levels: usize,
}

impl Default for DepthGuardCfg {
    fn default() -> Self {
        Self {
            enabled: default_depth_guard_enabled(),
            liquidity_multiple: default_depth_guard_liquidity_multiple(),
            max_levels: default_depth_guard_max_levels(),
        }
    }
}

impl DepthGuardCfg {
    fn validate(&self) -> Result<()> {
        if self.liquidity_multiple <= Decimal::ZERO {
            bail!("arb.depth_guard.liquidity_multiple must be positive");
        }
        if self.max_levels == 0 || self.max_levels > MAX_BOOK_LEVELS {
            bail!("arb.depth_guard.max_levels must be in [1, {MAX_BOOK_LEVELS}]");
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EntryGateMode {
    Off,
    Shadow,
    Enforce,
}

impl EntryGateMode {
    pub fn as_str(self) -> &'static str {
        match self {
            EntryGateMode::Off => "off",
            EntryGateMode::Shadow => "shadow",
            EntryGateMode::Enforce => "enforce",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct EntryGateCfg {
    pub enabled: bool,
    pub mode: EntryGateMode,
    pub history_window_hours: u64,
    pub sample_interval_ms: u64,
    pub min_history_samples: usize,
    pub entry_percentile: Decimal,
    pub min_extra_bps: Decimal,
}

impl Default for EntryGateCfg {
    fn default() -> Self {
        EntryGateCfg {
            enabled: true,
            mode: EntryGateMode::Shadow,
            history_window_hours: 72,
            sample_interval_ms: 1000,
            min_history_samples: 500,
            entry_percentile: Decimal::from(90),
            min_extra_bps: Decimal::new(5, 1),
        }
    }
}

impl EntryGateCfg {
    pub fn active(&self) -> bool {
        self.enabled && self.mode != EntryGateMode::Off
    }

    fn validate(&self) -> Result<()> {
        if self.history_window_hours == 0 {
            bail!("arb.entry_gate.history_window_hours must be > 0");
        }
        if self.sample_interval_ms == 0 {
            bail!("arb.entry_gate.sample_interval_ms must be > 0");
        }
        if self.min_history_samples == 0 {
            bail!("arb.entry_gate.min_history_samples must be > 0");
        }
        if self.entry_percentile <= Decimal::ZERO || self.entry_percentile > Decimal::from(100) {
            bail!("arb.entry_gate.entry_percentile must be in (0, 100]");
        }
        if self.min_extra_bps < Decimal::ZERO {
            bail!("arb.entry_gate.min_extra_bps must be non-negative");
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PnlCfg {
    pub enabled: bool,
    pub persist_dir: String,
    pub since: String,
    pub max_loss_usdc: Decimal,
}

impl Default for PnlCfg {
    fn default() -> Self {
        PnlCfg {
            enabled: true,
            persist_dir: "runs".to_string(),
            since: "2026-06-23T23:00:00Z".to_string(),
            max_loss_usdc: Decimal::from(5),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiveCfg {
    pub enabled: bool,
    pub mode: String,
    pub max_account_snapshot_age_ms: i64,
}

impl Default for LiveCfg {
    fn default() -> Self {
        LiveCfg {
            enabled: false,
            mode: "live".to_string(),
            max_account_snapshot_age_ms: 30000,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VenueCfg {
    pub aster_base_url: String,
    pub lighter_base_url: String,
    #[serde(default = "crate::config::default_hyperliquid_base_url")]
    pub hyperliquid_base_url: String,
    pub signers_dir: String,
    /// Set only by `run --mode dry-run` after pointing the URLs at the simulated venues: the
    /// taker then signs with the dry-run identity. No file can set it.
    #[serde(skip)]
    pub dry_run: bool,
}

impl Default for VenueCfg {
    fn default() -> Self {
        VenueCfg {
            aster_base_url: crate::config::default_aster_base_url(),
            lighter_base_url: crate::config::default_hl_base_url(),
            hyperliquid_base_url: crate::config::default_hyperliquid_base_url(),
            signers_dir: "signers".to_string(),
            dry_run: false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RiskCfg {
    pub max_abs_position_notional_usd: Decimal,
    pub max_position_mismatch_usd: Decimal,
    pub margin_buffer_usd: Decimal,
    pub min_reconcile_interval_ms: u64,
    /// When the main-loop position-mismatch guard sees an unhedged residual persist for
    /// `mismatch_flatten_after_checks` consecutive checks, actively flatten it reduce-only
    /// at the emergency slippage bound instead of pausing forever on a naked position.
    #[serde(default = "default_auto_flatten_on_mismatch")]
    pub auto_flatten_on_mismatch: bool,
    /// Consecutive fresh account snapshots showing the mismatch before the auto-flatten fires
    /// (a mismatch requests an immediate re-read instead of waiting for the ~15 s refresh).
    /// Filters transient reconcile lag.
    #[serde(default = "default_mismatch_flatten_after_checks")]
    pub mismatch_flatten_after_checks: u32,
}

fn default_auto_flatten_on_mismatch() -> bool {
    true
}

fn default_mismatch_flatten_after_checks() -> u32 {
    4
}

impl Default for RiskCfg {
    fn default() -> Self {
        RiskCfg {
            max_abs_position_notional_usd: Decimal::from(200),
            max_position_mismatch_usd: Decimal::from(3),
            margin_buffer_usd: Decimal::from(25),
            min_reconcile_interval_ms: 500,
            auto_flatten_on_mismatch: default_auto_flatten_on_mismatch(),
            mismatch_flatten_after_checks: default_mismatch_flatten_after_checks(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketCfg {
    #[serde(default)]
    pub first_venue: crate::config::FirstVenue,
    /// Empty unless the first leg is Aster.
    #[serde(default)]
    pub aster_symbol: String,
    /// The coin on each leg that is not Aster: Lighter's or Hyperliquid's.
    pub lighter_symbol: String,
    #[serde(default)]
    pub hedge_venue: crate::config::HedgeVenue,
    pub market_id: Option<String>,
    pub lighter_market_index: Option<u32>,
    pub lighter_price_decimals: Option<u32>,
    pub lighter_size_decimals: Option<u32>,
    pub lighter_min_notional: Option<Decimal>,
}

impl MarketCfg {
    pub fn id(&self) -> MarketId {
        MarketId(
            self.market_id
                .clone()
                .unwrap_or_else(|| self.lighter_symbol.clone()),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn valid_config() -> Config {
        Config {
            arb: ArbCfg::default(),
            pnl: PnlCfg::default(),
            live: LiveCfg::default(),
            venues: VenueCfg::default(),
            risk: RiskCfg::default(),
            markets: vec![MarketCfg {
                first_venue: Default::default(),
                aster_symbol: "HYPEUSDT".to_string(),
                lighter_symbol: "HYPE".to_string(),
                hedge_venue: Default::default(),
                market_id: Some("HYPE".to_string()),
                lighter_market_index: Some(24),
                lighter_price_decimals: Some(4),
                lighter_size_decimals: Some(2),
                lighter_min_notional: Some(dec!(10)),
            }],
        }
    }

    #[test]
    fn emergency_slippage_must_cover_normal_bounds() {
        let mut cfg = valid_config();
        cfg.arb.max_aster_slippage_bps = dec!(30);
        cfg.arb.max_lighter_slippage_bps = dec!(30);
        cfg.arb.emergency_slippage_bps = dec!(25);
        assert!(
            cfg.validate().is_err(),
            "emergency bound tighter than normal bounds cannot cross a moved book"
        );
        cfg.arb.emergency_slippage_bps = dec!(75);
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn mismatch_flatten_after_checks_must_be_positive() {
        let mut cfg = valid_config();
        cfg.risk.mismatch_flatten_after_checks = 0;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn minimal_toml_requires_explicit_live_enablement_and_keeps_warmup_gate() {
        let cfg: Config = toml::from_str("[[markets]]\naster_symbol=\"HYPEUSDT\"\nlighter_symbol=\"HYPE\"").unwrap();
        cfg.validate().unwrap();
        assert!(!cfg.live.enabled);
        assert!(cfg.arb.entry_gate.enabled);
        assert_eq!(cfg.arb.entry_gate.mode,EntryGateMode::Shadow);
    }

    #[test]
    fn the_file_loader_pins_the_mainnet_origins() {
        let table = |venues: &str| {
            let raw = format!("[[markets]]\naster_symbol=\"HYPEUSDT\"\nlighter_symbol=\"HYPE\"\n{venues}");
            Config::from_table(toml::from_str(&raw).unwrap())
        };
        table("").unwrap();
        let venues = "[venues]\nsigners_dir=\"signers\"\naster_base_url=\"https://fapi.asterdex.com/\"\n";
        table(&format!("{venues}lighter_base_url=\"https://mainnet.zklighter.elliot.ai\"")).unwrap();
        for lighter in ["http://127.0.0.1:18082", "https://testnet.zklighter.elliot.ai"] {
            let err = table(&format!("{venues}lighter_base_url=\"{lighter}\"")).unwrap_err();
            assert!(format!("{err:#}").contains("mainnet origins"), "{lighter}: {err:#}");
        }
        // Only `run --mode dry-run` may switch the identity, never a file.
        let err = table(&format!("{venues}lighter_base_url=\"https://mainnet.zklighter.elliot.ai\"\ndry_run=true"))
            .unwrap_err();
        assert!(format!("{err:#}").contains("unknown config keys: venues.dry_run"), "{err:#}");
    }

    #[test]
    fn entry_gate_rejects_invalid_values() {
        let mut cfg = EntryGateCfg::default();
        cfg.history_window_hours = 0;
        assert!(cfg.validate().is_err());

        let mut cfg = EntryGateCfg::default();
        cfg.sample_interval_ms = 0;
        assert!(cfg.validate().is_err());

        let mut cfg = EntryGateCfg::default();
        cfg.min_history_samples = 0;
        assert!(cfg.validate().is_err());

        let mut cfg = EntryGateCfg::default();
        cfg.entry_percentile = Decimal::ZERO;
        assert!(cfg.validate().is_err());

        let mut cfg = EntryGateCfg::default();
        cfg.entry_percentile = dec!(100.1);
        assert!(cfg.validate().is_err());

        let mut cfg = EntryGateCfg::default();
        cfg.min_extra_bps = dec!(-0.1);
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn entry_gate_toml_allows_partial_override() {
        let cfg: EntryGateCfg = toml::from_str("mode = \"enforce\"").unwrap();
        assert_eq!(cfg.mode, EntryGateMode::Enforce);
        assert_eq!(cfg.history_window_hours, 72);
        assert_eq!(cfg.entry_percentile, dec!(90));
        assert_eq!(cfg.min_extra_bps, dec!(0.5));
    }

    #[test]
    fn arb_rejects_invalid_hedge_retry_values() {
        let mut cfg = valid_config();
        cfg.arb.hedge_retry_slippage_bps = dec!(-0.1);
        assert!(cfg.validate().is_err());

        let mut cfg = valid_config();
        cfg.arb.hedge_retry_timeout_ms = 0;
        assert!(cfg.validate().is_err());

        let mut cfg = valid_config();
        cfg.arb.max_hedge_retry_attempts = 0;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn arb_rejects_invalid_depth_guard_values() {
        let mut cfg = valid_config();
        cfg.arb.depth_guard.liquidity_multiple = Decimal::ZERO;
        assert!(cfg.validate().is_err());

        let mut cfg = valid_config();
        cfg.arb.depth_guard.max_levels = 0;
        assert!(cfg.validate().is_err());

        let mut cfg = valid_config();
        cfg.arb.depth_guard.max_levels = MAX_BOOK_LEVELS + 1;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn arb_rejects_invalid_book_sanity_values() {
        let mut cfg = valid_config();
        cfg.arb.book_sanity.interval_ms = 0;
        assert!(cfg.validate().is_err());

        let mut cfg = valid_config();
        cfg.arb.book_sanity.max_top_bps = dec!(-0.1);
        assert!(cfg.validate().is_err());

        let mut cfg = valid_config();
        cfg.arb.book_sanity.required_failures = 0;
        assert!(cfg.validate().is_err());

        let mut cfg = valid_config();
        cfg.arb.book_sanity.required_successes = 0;
        assert!(cfg.validate().is_err());

        let mut cfg = valid_config();
        cfg.arb.book_sanity.block_cooldown_ms = 0;
        assert!(cfg.validate().is_err());

        let mut cfg = valid_config();
        cfg.arb.book_sanity.rest_depth_levels = MAX_BOOK_LEVELS + 1;
        assert!(cfg.validate().is_err());

        let mut cfg = valid_config();
        cfg.arb.book_sanity.liquidity_multiple = Decimal::ZERO;
        assert!(cfg.validate().is_err());
    }
}
