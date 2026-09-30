//! Live risk gating: the post-trade cooldown, predicted-vs-reported position
//! reconciliation, and the orphan-leg invariant gate that decides whether new
//! maker quoting is allowed.
//!
//! This gate blocks new maker quotes, not cancels or recovery. Recovery still needs fresh
//! data, executor readiness, request budget and resolved prior attempts.

use std::collections::HashMap;

use rust_decimal::Decimal;

use crate::types::MarketId;

/// Cooldown scope: `Global` (the config default) suppresses every market after an
/// execution event; `PerMarket` suppresses only that market.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CooldownScope {
    Global,
    PerMarket,
}

/// Tracks the post-trade cooldown deadlines in monotonic-clock nanos. Owned by the
/// strategy loop.
#[derive(Debug, Clone)]
pub struct CooldownState {
    scope: CooldownScope,
    global_until_ns: i64,
    per_market: HashMap<MarketId, i64>,
}

impl CooldownState {
    pub fn new(scope: CooldownScope) -> Self {
        CooldownState {
            scope,
            global_until_ns: 0,
            per_market: HashMap::new(),
        }
    }

    /// Start (or extend) a cooldown of `dur_ns` from `now_ns` after an execution event on
    /// `market`. Under `Global` scope every market is suppressed regardless of `market`.
    /// Never shortens an in-flight cooldown (takes the max).
    pub fn trigger(&mut self, now_ns: i64, dur_ns: i64, market: &MarketId) {
        let until = now_ns.saturating_add(dur_ns);
        match self.scope {
            CooldownScope::Global => {
                self.global_until_ns = self.global_until_ns.max(until);
            }
            CooldownScope::PerMarket => {
                let e = self.per_market.entry(market.clone()).or_insert(0);
                *e = (*e).max(until);
            }
        }
    }

    /// Whether `market` is in cooldown at `now_ns`.
    pub fn active(&self, now_ns: i64, market: &MarketId) -> bool {
        match self.scope {
            CooldownScope::Global => now_ns < self.global_until_ns,
            CooldownScope::PerMarket => {
                self.per_market.get(market).is_some_and(|&until| now_ns < until)
            }
        }
    }


}

/// Why new maker quoting is currently frozen.
/// `None` of these means quoting may proceed (subject to cooldown + the feed gate).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FreezeReason {
    /// Startup reconciliation / clean-start not finished.
    NotReconciled,
    /// This market's own feeds aren't fresh enough to quote (its Aster/Lighter book is stale,
    /// REST-divergent, or dead). Set per-market by `Strategy::market_feeds_fresh`, not the
    /// global watchdog gate, so one stale pair never freezes the others.
    FeedGateClosed,
    /// The account snapshot is older than `max_account_snapshot_age_ms`.
    AccountSnapshotStale,
    /// The Aster user data stream has gone silent.
    AsterUserStreamStale,
    /// Predicted and exchange-reported positions disagree beyond tolerance.
    PositionMismatch,
    /// An open hedge is in an unknown / timed-out state.
    OrphanHedge,
    /// Unhedged notional or age exceeds the configured limit.
    UnhedgedOverLimit,
}

impl FreezeReason {
    pub fn as_str(self) -> &'static str {
        match self {
            FreezeReason::NotReconciled => "NOT_RECONCILED",
            FreezeReason::FeedGateClosed => "FEED_GATE_CLOSED",
            FreezeReason::AccountSnapshotStale => "ACCOUNT_SNAPSHOT_STALE",
            FreezeReason::AsterUserStreamStale => "ASTER_USER_STREAM_STALE",
            FreezeReason::PositionMismatch => "POSITION_MISMATCH",
            FreezeReason::OrphanHedge => "ORPHAN_HEDGE",
            FreezeReason::UnhedgedOverLimit => "UNHEDGED_OVER_LIMIT",
        }
    }
}

/// The conditions the maker gate must ALL satisfy to allow new quoting (reopen conditions
/// and orphan-leg invariants). Pure inputs → pure decision, so it is exhaustively
/// testable and the strategy just feeds it live values.
#[derive(Debug, Clone, Copy)]
pub struct MakerGateInputs {
    pub clean_start_done: bool,
    /// This market's own Aster+Lighter feeds are fresh & non-divergent (per-market, not the global
    /// watchdog gate). See `Strategy::market_feeds_fresh`.
    pub feed_gate_open: bool,
    pub account_fresh: bool,
    pub aster_stream_fresh: bool,
    pub positions_reconciled: bool,
    pub no_orphan_hedge: bool,
    pub unhedged_within_limits: bool,
}

impl MakerGateInputs {
    /// All-clear inputs (everything healthy) — a convenient test/base value.
    #[cfg(test)]
    pub fn all_clear() -> Self {
        MakerGateInputs {
            clean_start_done: true,
            feed_gate_open: true,
            account_fresh: true,
            aster_stream_fresh: true,
            positions_reconciled: true,
            no_orphan_hedge: true,
            unhedged_within_limits: true,
        }
    }
}

/// Evaluate the maker gate. `Ok(())` ⇒ new maker quoting allowed (still subject to the
/// cooldown, which is checked separately). `Err(reason)` ⇒ frozen, with the first failing
/// reason for logging. Order is worst-first so the most serious cause surfaces.
pub fn evaluate_maker_gate(i: &MakerGateInputs) -> Result<(), FreezeReason> {
    if !i.clean_start_done {
        return Err(FreezeReason::NotReconciled);
    }
    if !i.no_orphan_hedge {
        return Err(FreezeReason::OrphanHedge);
    }
    if !i.positions_reconciled {
        return Err(FreezeReason::PositionMismatch);
    }
    if !i.unhedged_within_limits {
        return Err(FreezeReason::UnhedgedOverLimit);
    }
    if !i.feed_gate_open {
        return Err(FreezeReason::FeedGateClosed);
    }
    if !i.aster_stream_fresh {
        return Err(FreezeReason::AsterUserStreamStale);
    }
    if !i.account_fresh {
        return Err(FreezeReason::AccountSnapshotStale);
    }
    Ok(())
}

/// Whether predicted and reported positions on one leg disagree by more than
/// `tolerance_usd` at `mark_px`. A mismatch freezes maker quoting.
pub fn position_mismatch(
    predicted_qty: Decimal,
    reported_qty: Decimal,
    mark_px: Decimal,
    tolerance_usd: Decimal,
) -> bool {
    let diff_notional = (predicted_qty - reported_qty).abs() * mark_px.abs();
    diff_notional > tolerance_usd
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn global_cooldown_suppresses_every_market() {
        let mut c = CooldownState::new(CooldownScope::Global);
        c.trigger(1_000, 60_000_000_000, &"BTC".into()); // 60s in ns
        assert!(c.active(1_000, &"BTC".into()));
        assert!(c.active(1_000, &"ETH".into())); // global: ETH suppressed too
        assert!(!c.active(60_001_000_000, &"ETH".into())); // expired
    }

    #[test]
    fn per_market_cooldown_is_isolated() {
        let mut c = CooldownState::new(CooldownScope::PerMarket);
        c.trigger(1_000, 60_000_000_000, &"BTC".into());
        assert!(c.active(1_000, &"BTC".into()));
        assert!(!c.active(1_000, &"ETH".into())); // ETH not triggered
        assert!(c.active(60_000_000_999, &"BTC".into()));
        assert!(!c.active(60_000_001_000, &"BTC".into()));
    }

    #[test]
    fn cooldown_never_shortens() {
        let mut c = CooldownState::new(CooldownScope::Global);
        c.trigger(0, 60_000_000_000, &"BTC".into());
        // a later, shorter trigger must not pull the deadline in
        c.trigger(1_000, 10_000_000_000, &"BTC".into());
        assert!(c.active(59_999_999_999, &"BTC".into()));
        assert!(!c.active(60_000_000_000, &"BTC".into()));
    }

    #[test]
    fn gate_passes_when_all_clear() {
        assert!(evaluate_maker_gate(&MakerGateInputs::all_clear()).is_ok());
    }

    #[test]
    fn gate_reports_worst_reason_first() {
        // orphan hedge outranks a stale account snapshot
        let mut i = MakerGateInputs::all_clear();
        i.no_orphan_hedge = false;
        i.account_fresh = false;
        assert_eq!(evaluate_maker_gate(&i), Err(FreezeReason::OrphanHedge));
        // clean-start outranks everything
        let mut j = MakerGateInputs::all_clear();
        j.clean_start_done = false;
        j.no_orphan_hedge = false;
        assert_eq!(evaluate_maker_gate(&j), Err(FreezeReason::NotReconciled));
        // a single feed-gate closure freezes
        let mut k = MakerGateInputs::all_clear();
        k.feed_gate_open = false;
        assert_eq!(evaluate_maker_gate(&k), Err(FreezeReason::FeedGateClosed));
    }

    #[test]
    fn position_mismatch_respects_tolerance() {
        // 0.01 BTC diff at 100 = $1 notional; tolerance $2 => not a mismatch.
        assert!(!position_mismatch(dec!(0.51), dec!(0.50), dec!(100), dec!(2)));
        // 0.05 diff at 100 = $5 > $2 => mismatch.
        assert!(position_mismatch(dec!(0.55), dec!(0.50), dec!(100), dec!(2)));
        // sign-agnostic on the mark.
        assert!(position_mismatch(dec!(-0.55), dec!(-0.50), dec!(100), dec!(2)));
    }
}
