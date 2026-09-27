//! Aster/Lighter bot. `run` (`controller`) trades one market with the
//! taker–taker engine (`taker`) and the XEMM engine (`livebot`: quotes on Aster, hedges
//! fills on Lighter), which hands the execution rights to the taker for each arbitrage.

pub mod book;
pub mod cli;
pub mod config;
/// The `run` controller: both engines in one process, execution rights handed over in memory.
pub mod controller;
pub mod connectors;
pub mod decimal;
/// `run --mode dry-run`: simulated Aster and Lighter venues, seen from AWS Tokyo.
pub mod dryrun;
pub mod edge;
pub mod hot_types;
/// Lock-free real-time substrate (latest-book cells, venue ingest threads, stream watchdog,
/// REST book cross-check) used by the XEMM engine (`livebot`).
pub mod hotpath;
pub mod inventory;
pub mod lighter;
/// The XEMM engine (`run`'s maker) for one market, gated behind `[live] enabled`; a dry run
/// points it at the simulated venues.
pub mod livebot;
pub mod markets;
pub mod metrics;
pub mod position;
pub mod quote_engine;
pub mod live_report;
/// Taker–taker arbitrage engine (`run`'s taker; `lighter_aster_bot taker ...` on its own).
pub mod taker;
pub mod types;
pub mod vwap;

/// Crate-wide result alias.
pub type Result<T> = anyhow::Result<T>;
