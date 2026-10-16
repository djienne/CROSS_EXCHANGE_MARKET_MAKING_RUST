//! Binary entry point: initialize tracing, parse the CLI, and dispatch. `taker ...` runs the
//! taker-taker arbitrage engine with its own CLI; everything else is the bot/XEMM CLI.

use std::ffi::OsString;

use anyhow::Result;
use clap::Parser;
use lighter_aster_bot::cli::{dispatch, Cli};

#[tokio::main]
async fn main() -> Result<()> {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    // Non-blocking writer: a log line from a hot thread (strategy/ingest/worker) must
    // never be a synchronous write(2) that can stall on a slow disk. The guard flushes
    // the buffer on drop, so shutdown logs survive. ANSI only on a real terminal (the
    // stdout is often redirected to a file). A replay also copies it into its directory.
    let (writer, _guard) = tracing_appender::non_blocking(lighter_aster_bot::log_copy::Tee);
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .with_ansi(std::io::IsTerminal::is_terminal(&std::io::stdout()))
        .with_writer(writer)
        .init();

    let mut args: Vec<OsString> = std::env::args_os().collect();
    if args.get(1).is_some_and(|arg| arg == "taker") {
        args.remove(0);
        return lighter_aster_bot::taker::run(args).await;
    }
    dispatch(Cli::parse()).await
}
