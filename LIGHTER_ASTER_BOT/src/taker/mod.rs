//! Taker–taker arbitrage engine (formerly the standalone `lighter_aster_taker_arb` crate):
//! `run`'s taker, or `lighter_aster_bot taker ...` on its own with its own CLI; both read the
//! `[taker]` table of the config. Venue code that was
//! identical on the XEMM side is shared from `crate::lighter` and `crate::livebot::exec`.

pub(crate) mod arb;
mod aster;
mod book;
mod book_sanity;
mod cli;
pub(crate) mod config;
mod connectors;
mod decimal;
mod entry_gate;
mod markets;
pub(crate) mod pnl;
pub(crate) mod status;
pub(crate) mod types;
mod venues;

use std::ffi::OsString;

use anyhow::Result;
use clap::Parser;

/// The standalone taker was built with `panic = "abort"`: a panicking task takes the whole
/// process down instead of dying silently while the scanner keeps trading. The merged binary
/// must unwind (XEMM catches strategy-thread panics), so `taker` and `run` restore abort
/// semantics with a hook that runs before any unwinding — except on XEMM's strategy thread,
/// whose `catch_unwind` turns the panic into an orderly, order-cancelling shutdown.
pub fn abort_on_panic() {
    let report = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        report(info);
        if std::thread::current().name() != Some(crate::livebot::STRATEGY_THREAD) {
            std::process::abort();
        }
    }));
}

/// `lighter_aster_bot taker <args>`: `args` starts with the program name for the taker CLI.
pub async fn run(args: Vec<OsString>) -> Result<()> {
    abort_on_panic();
    cli::dispatch(cli::Cli::parse_from(args)).await
}

#[cfg(test)]
mod tests {
    /// Child half of `panicking_taker_task_aborts_the_process`.
    #[test]
    #[ignore = "subprocess helper; run by panicking_taker_task_aborts_the_process"]
    fn abort_on_panic_child() {
        if std::env::var_os("TAKER_ABORT_TEST_CHILD").is_none() {
            return;
        }
        super::abort_on_panic();
        let runtime = tokio::runtime::Runtime::new().unwrap();
        // Without the hook, tokio turns this panic into a JoinError and the process exits 0.
        let _ = runtime.block_on(tokio::spawn(async { panic!("taker task panic") }));
        std::process::exit(0);
    }

    #[test]
    fn panicking_taker_task_aborts_the_process() {
        let out = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "taker::tests::abort_on_panic_child", "--ignored", "--nocapture"])
            .env("TAKER_ABORT_TEST_CHILD", "1")
            .output()
            .unwrap();
        assert!(!out.status.success(), "a panicking taker task must abort the process");
        #[cfg(unix)]
        {
            use std::os::unix::process::ExitStatusExt;
            assert_eq!(out.status.signal(), Some(libc::SIGABRT));
        }
    }
}
