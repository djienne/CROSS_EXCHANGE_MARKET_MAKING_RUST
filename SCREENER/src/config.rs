//! `screener.toml` (the values and their sources are documented there).

use std::path::Path;

use anyhow::{ensure, Context, Result};
use serde::{Deserialize, Serialize};

use crate::universe::{Market, Pair, Venue};

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub collect: Collect,
    pub report: Report,
}

/// Which pairs are followed; written at the head of every data file. What is recorded of them
/// follows from `[report]` (`collect::Recording`).
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Collect {
    pub min_volume_usd: f64,
    pub max_pairs: usize,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Report {
    pub clip_usd: f64,
    pub depth_multiple: f64,
    pub lighter_tier: String,
    pub aster_taker_ms: f64,
    pub lighter_rtt_ms: f64,
    pub aster_fill_notice_ms: f64,
    pub lighter_fill_notice_ms: f64,
    pub aster_print_delay_ms: f64,
    pub aster: AsterFees,
    pub lighter: LighterTiers,
    pub hyperliquid: Hyperliquid,
    pub taker: Taker,
    pub xemm: Xemm,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Hyperliquid {
    pub maker_bps: f64,
    pub taker_bps: f64,
    pub taker_ms: f64,
    pub fill_notice_ms: f64,
    pub quote_age_ms: f64,
}

#[derive(Debug, Clone, Copy, Serialize)]
pub struct Costs {
    pub maker_bps: f64,
    pub taker_bps: f64,
    pub taker_ms: f64,
    pub notice_ms: f64,
    pub quote_age_ms: f64,
    /// How much later a print arrives than the book change it made.
    pub print_delay_ms: f64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AsterFees {
    pub maker_bps: f64,
    pub taker_bps: f64,
    pub group_b_taker_bps: f64,
    pub rwa_taker_bps: f64,
    pub rwa_subtypes: Vec<String>,
    pub group_b: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LighterTiers {
    pub standard: LighterTier,
    pub premium: LighterTier,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LighterTier {
    pub maker_bps: f64,
    pub taker_bps: f64,
    pub taker_delay_ms: f64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Taker {
    pub margin_bps: f64,
    pub gate_percentile: f64,
    pub gate_window_hours: f64,
    pub gate_min_samples: usize,
    pub gate_extra_bps: f64,
    pub sample_interval_ms: i64,
    pub cooldown_ms: i64,
    pub max_position_usd: f64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Xemm {
    pub required_bps: f64,
    pub min_touch_distance_bps: f64,
    pub max_quote_distance_bps: f64,
    pub quote_age_ms: i64,
    pub cooldown_ms: i64,
    pub sweep_bps: Vec<f64>,
}

impl Config {
    pub fn load(path: &Path) -> Result<Config> {
        let text = std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
        let cfg: Config = toml::from_str(&text).with_context(|| format!("parsing {}", path.display()))?;
        cfg.report.validate()?;
        ensure!(cfg.collect.max_pairs > 0 && cfg.collect.min_volume_usd.is_finite() && cfg.collect.min_volume_usd >= 0.0, "invalid universe limits");
        Ok(cfg)
    }
}

impl Report {
    pub fn validate(&self) -> Result<()> {
        self.lighter()?;
        let h = &self.hyperliquid;
        let values = [self.clip_usd, self.depth_multiple, self.aster_taker_ms, self.lighter_rtt_ms,
            self.aster_fill_notice_ms, self.lighter_fill_notice_ms, self.aster_print_delay_ms, self.aster.maker_bps, self.aster.taker_bps,
            self.aster.group_b_taker_bps, self.aster.rwa_taker_bps, self.lighter.standard.maker_bps,
            self.lighter.standard.taker_bps, self.lighter.standard.taker_delay_ms, self.lighter.premium.maker_bps,
            self.lighter.premium.taker_bps, self.lighter.premium.taker_delay_ms, h.maker_bps, h.taker_bps,
            h.taker_ms, h.fill_notice_ms, h.quote_age_ms, self.taker.margin_bps, self.xemm.required_bps];
        ensure!(values.iter().chain(&self.xemm.sweep_bps).all(|x| x.is_finite() && *x >= 0.0), "fees, latencies and thresholds must be finite and nonnegative (rebates require a different recording floor)");
        ensure!(self.clip_usd > 0.0 && self.depth_multiple > 0.0 && self.xemm.quote_age_ms >= 0, "invalid clip, depth or quote age");
        Ok(())
    }

    pub fn costs(&self, market: &Market, lighter: &LighterTier) -> Costs {
        let (maker_bps, taker_bps, taker_ms, notice_ms, quote_age_ms, print_delay_ms) = match market.venue {
            Venue::Aster => (self.aster.maker_bps, self.aster_taker_bps(&market.symbol, &market.subtypes), self.aster_taker_ms, self.aster_fill_notice_ms, self.xemm.quote_age_ms as f64, self.aster_print_delay_ms),
            Venue::Lighter => (lighter.maker_bps, lighter.taker_bps, self.lighter_rtt_ms + lighter.taker_delay_ms, self.lighter_fill_notice_ms, self.xemm.quote_age_ms as f64, 0.0),
            Venue::Hyperliquid => { let h = &self.hyperliquid; (h.maker_bps, h.taker_bps, h.taker_ms, h.fill_notice_ms, h.quote_age_ms, 0.0) },
        };
        Costs { maker_bps, taker_bps, taker_ms, notice_ms, quote_age_ms, print_delay_ms }
    }

    pub fn lighter(&self) -> Result<&LighterTier> {
        match self.lighter_tier.as_str() {
            "standard" => Ok(&self.lighter.standard),
            "premium" => Ok(&self.lighter.premium),
            other => anyhow::bail!("lighter_tier must be standard or premium, not {other}"),
        }
    }

    /// What a side needs at the top for the bot to trade it.
    pub fn depth_usd(&self) -> f64 {
        self.depth_multiple * self.clip_usd
    }

    /// The lowest XEMM required edge scored: the bot's or its sweep's.
    pub fn xemm_min_bps(&self) -> f64 {
        self.xemm.sweep_bps.iter().copied().fold(self.xemm.required_bps, f64::min)
    }

    /// The bot's taker threshold on `pair` at `lighter`'s fees: both taker fees plus the margin.
    pub fn taker_required_bps(&self, pair: &Pair, lighter: &LighterTier) -> f64 {
        self.costs(&pair.left, lighter).taker_bps + self.costs(&pair.right, lighter).taker_bps + self.taker.margin_bps
    }

    /// Aster's taker fee for `symbol`, whose exchangeInfo `underlyingSubType` is `subtypes`.
    pub fn aster_taker_bps(&self, symbol: &str, subtypes: &[String]) -> f64 {
        let fees = &self.aster;
        if fees.group_b.iter().any(|s| s == symbol) {
            fees.group_b_taker_bps
        } else if subtypes.iter().any(|s| fees.rwa_subtypes.contains(s)) {
            fees.rwa_taker_bps
        } else {
            fees.taker_bps
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_shipped_config_loads_and_group_b_beats_rwa() {
        let cfg = Config::load(Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/screener.toml"))).unwrap();
        let r = &cfg.report;
        let stock = vec!["STOCK".to_string(), "Semiconductor".to_string()];
        assert_eq!(r.aster_taker_bps("SKHYNIXUSDT", &stock), 10.0);
        assert_eq!(r.aster_taker_bps("NVDAUSDT", &stock), 1.25);
        assert_eq!(r.aster_taker_bps("HYPEUSDT", &[]), 4.0);
        assert_eq!(r.lighter().unwrap().taker_delay_ms, 300.0);
    }
}
