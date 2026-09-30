//! The XEMM engine: Aster maker quotes, Lighter or Hyperliquid hedges.
//!
//! It runs only with `[maker.live] enabled` and one market; a dry run points it at the simulated
//! venues.
//!
//! Book publication and strategy ownership stay in memory; bounded queues connect
//! venue workers, cold account reconciliation and persistent journal writing.
//! Aster uses EIP-712 requests, Lighter its native signer/socket, Hyperliquid signed HTTP actions.
//! See `RUNBOOK.md` for uncertainty, shutdown and restart handling.

pub mod account;
pub mod breaker;
pub mod exec;
pub mod fills;
pub mod ids;
pub mod journal;
pub mod orders;
pub mod pairs;
pub mod precheck;
pub mod probe;
pub mod reconcile;
pub mod risk;
pub mod run;
pub mod scale;
pub mod status;
pub mod strategy;
pub mod userstream;

pub use run::{run, STRATEGY_THREAD};
