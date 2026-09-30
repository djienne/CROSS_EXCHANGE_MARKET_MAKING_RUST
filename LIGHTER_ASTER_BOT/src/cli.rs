//! Command-line interface: `run` (the bot: one market) and the XEMM
//! subcommands `live-report`, `probe`, `status`, `fetch-specs`, plus `close` (the operator's exit)
//! and `record` (the market-data tape for backtests). The taker engine has its own CLI under `taker`.

use anyhow::Result;
use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(
    name = "lighter_aster_bot",
    version,
    about = "Aster/Lighter/Hyperliquid bot: XEMM with taker arbitrage on Aster routes, taker alone on Lighter/Hyperliquid; plus probes, status and reports",
    after_help = "Taker engine on its own: `lighter_aster_bot taker --help`."
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,

    /// Path to the TOML config file (bot.toml; XEMM commands read its `[maker]` table).
    #[arg(long, global = true, default_value = "bot.toml")]
    pub config: PathBuf,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Run the bot for one market: XEMM quotes and hands the execution rights to the taker for
    /// each arbitrage that passes its entry gate. Sends REAL orders in `--mode live`.
    Run {
        /// Market id in [[taker.markets]]; Aster routes also need [[maker.markets]] (e.g. HYPE).
        #[arg(long)]
        market: String,
        /// Required. live: real orders with the real credentials, files in runs/. dry-run: the
        /// same bot against simulated venues fed by live market data (`[dry_run]` in the config),
        /// no credentials, files in runs/dry-run/.
        #[arg(long)]
        mode: String,
        /// Archive a latched bot breaker (`runs/bot-<M>.breaker.json`) and start.
        #[arg(long, default_value_t = false)]
        ack_breaker: bool,
        /// Discard the persisted equity baseline: the drawdown stop re-arms on the first sample.
        #[arg(long, default_value_t = false)]
        reset_breaker_baseline: bool,
    },

    /// Summarize an XEMM journal into logical trades and versioned execution economics.
    LiveReport {
        /// XEMM journal path. The default is the live `run` bot's journal for HYPE.
        #[arg(long, default_value = "runs/bot-HYPE-journal.jsonl")]
        journal: PathBuf,
        /// Restrict to one market id, e.g. HYPE.
        #[arg(long)]
        market: Option<String>,
        /// Include logical trades at/after this epoch-milliseconds timestamp.
        /// Pair cumulative and individual evidence before filtering by economic time.
        #[arg(long)]
        since_ms: Option<i64>,
        /// Print one row per logical trade, including incomplete economics.
        #[arg(long, default_value_t = false)]
        details: bool,
        /// Print a machine-readable JSON summary.
        #[arg(long, default_value_t = false)]
        json: bool,
    },

    /// Probe a live venue primitive with its real signer and venue credential file: signed reads,
    /// local signing (lighter-order-dry-run), or REAL orders behind `--i-understand-live` and the
    /// venue's leg lock (RUNBOOK.md, "Probes").
    Probe {
        /// Which check; an unknown one lists them all.
        check: String,
        /// Config market id; hl-balance/hl-place-cancel/hl-market take a coin (HYPE), hl-hedge a route (HYPE-HL).
        #[arg(long)]
        market: Option<String>,
        /// Required confirmation for the checks that send real orders.
        #[arg(long, default_value_t = false)]
        i_understand_live: bool,
        /// USD cap for lighter-market and hl-hedge, in (0, 20]; for hl-place-cancel and hl-market, in [10.5, 20].
        #[arg(long, default_value = "0")]
        max_usd: rust_decimal::Decimal,
    },

    /// REAL orders, the operator's exit: close one coin's positions at market, reduce-only, on
    /// Aster, Lighter and Hyperliquid at once, then check each venue flat. Refuses while a live
    /// `run` or `taker run` trades one of those venues.
    Close {
        /// Market id or coin from the configured maker/taker markets (HYPE-HL means HYPE too).
        #[arg(long)]
        market: String,
        /// Venues to close, comma-separated; all three when omitted.
        #[arg(long, value_enum, value_delimiter = ',')]
        venue: Vec<crate::livebot::probe::CloseVenue>,
        /// Required confirmation.
        #[arg(long, default_value_t = false)]
        i_understand_live: bool,
    },

    /// Read-only XEMM route account/book/quote status as JSON (Aster routes only).
    Status {
        /// Target market id from config (e.g. HYPE). Defaults to HYPE.
        #[arg(long)]
        market: Option<String>,
    },

    /// Fetch configured XEMM specs from Aster and the Lighter/Hyperliquid hedge venue.
    FetchSpecs {
        #[arg(long)]
        markets: Option<String>,
    },

    /// Record one market's raw public Aster and Lighter feeds, the dry run's input, to
    /// `<out>/<MARKET>/*.tape.zst` for backtests (format in src/dryrun/tape.rs).
    /// Public data only: no credentials, no orders.
    Record {
        /// Market id from config (e.g. HYPE).
        #[arg(long)]
        market: String,
        #[arg(long, default_value = "data")]
        out: PathBuf,
    },
}

/// Parse a `--mode` string into a [`crate::config::LiveMode`].
fn parse_live_mode(s: &str) -> Result<crate::config::LiveMode> {
    use crate::config::LiveMode;
    match s.trim().to_ascii_lowercase().as_str() {
        "live" => Ok(LiveMode::Live),
        "dry-run" => Ok(LiveMode::DryRun),
        other => anyhow::bail!("unknown --mode {other:?} (expected live or dry-run)"),
    }
}

/// Dispatch a parsed CLI to the appropriate module entry point.
pub async fn dispatch(cli: Cli) -> Result<()> {
    match cli.command {
        Commands::Run { market, mode, ack_breaker, reset_breaker_baseline } => {
            let mode = parse_live_mode(&mode)?;
            crate::taker::abort_on_panic();
            // Listen from the start: a signal during setup still takes the graceful drain.
            let stop = crate::controller::stop_on_signals();
            crate::controller::run(&cli.config, &market, mode, ack_breaker, reset_breaker_baseline, stop).await?;
        }
        Commands::LiveReport { journal: journal_path, market, since_ms, details, json } => {
            let summary = crate::live_report::summarize_path(&journal_path, market.as_deref(), since_ms)?;
            if json {
                crate::live_report::print_summary_json(&journal_path, &summary)?;
            } else {
                crate::live_report::print_summary(&journal_path, &summary, details);
            }
        }
        Commands::Probe { check, market, i_understand_live, max_usd } => {
            let cfg = crate::config::Config::load(&cli.config)?;
            crate::livebot::probe::run(&cfg, &check, market, i_understand_live, max_usd).await?;
        }
        Commands::Close { market, venue, i_understand_live } => {
            let cfg = crate::config::Config::load(&cli.config)?;
            crate::livebot::probe::close(&cfg, &market, &venue, i_understand_live).await?;
        }
        Commands::Status { market } => {
            let cfg = crate::config::Config::load(&cli.config)?;
            crate::livebot::status::run(&cfg, market).await?;
        }
        Commands::FetchSpecs { markets } => {
            let cfg = crate::config::Config::load(&cli.config)?;
            let selected = cfg.select_markets(markets.as_deref());
            let specs = crate::connectors::rest_specs::build_market_specs(&selected, &cfg.live).await?;
            println!(
                "{:<6} {:<10} {:<10} {:>8} {:>12} {:>12} {:>12} {:>12} {:>7} {:>14}",
                "id", "aster", "lighter", "mkt_id", "tick", "step", "minQty", "minNotl", "szDec", "qtyStep"
            );
            for s in &specs {
                println!(
                    "{:<6} {:<10} {:<10} {:>8} {:>12} {:>12} {:>12} {:>12} {:>7} {:>14}",
                    s.market_id.0,
                    s.aster_symbol,
                    s.hl_coin,
                    s.lighter_market_id,
                    s.tick,
                    s.step,
                    s.aster_min_qty,
                    s.aster_min_notional,
                    s.hl_sz_decimals,
                    s.hl_qty_step
                );
            }
        }
        Commands::Record { market, out } => {
            crate::dryrun::tape::record(&cli.config, &market, out, crate::controller::stop_on_signals()).await?;
        }
    }
    Ok(())
}
