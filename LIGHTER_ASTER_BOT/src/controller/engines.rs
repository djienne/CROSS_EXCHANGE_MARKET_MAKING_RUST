//! The real engines behind the supervisor: the taker (`taker::arb::run`) and XEMM
//! (`livebot::run`) as tasks of this process, and the status of the accounts they trade. A
//! market with no Aster leg runs the taker alone, with the execution rights to itself.

use std::path::PathBuf;

use anyhow::{bail, Context, Result};
use serde_json::Value;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use super::supervisor::{Bot, EngineIo, Engines};
use super::BotConfig;
use crate::livebot::strategy::Rights;
use crate::taker::arb::RunOptions;

pub struct LiveEngines {
    taker_cfg: crate::taker::config::Config,
    taker_markets: Vec<crate::taker::config::MarketCfg>,
    maker_cfg: crate::config::Config,
    maker_markets: Vec<crate::config::MarketCfg>,
    xemm_stem: PathBuf,
    /// Venue reads, for the startup check before XEMM publishes its first snapshot; none when
    /// the taker runs alone.
    xemm_status: Option<crate::livebot::status::StatusPoller>,
    /// The lone taker's status, from its own account snapshots, and when it was taken.
    taker_status: watch::Sender<Option<(tokio::time::Instant, Value)>>,
    /// XEMM's account snapshot, refreshed by its reconciler every ~2 s.
    xemm_account: crate::livebot::account::AccountState,
    /// An older snapshot is a failed status read: XEMM's own reads keep failing.
    max_status_age_ms: i64,
}

impl LiveEngines {
    pub async fn new(
        cfg: &BotConfig,
        market: &str,
        taker_markets: Vec<crate::taker::config::MarketCfg>,
        maker_markets: Vec<crate::config::MarketCfg>,
        xemm_stem: PathBuf,
    ) -> Result<Self> {
        let poll_ms = cfg.controller.poll_sec.saturating_mul(1000) as i64;
        let xemm_status = match maker_markets.is_empty() {
            true => None,
            false => Some(crate::livebot::status::StatusPoller::new(&cfg.maker, market).await?),
        };
        Ok(Self {
            // The taker refreshes its snapshot within its own age limit, and the status is read
            // once a poll.
            max_status_age_ms: if xemm_status.is_some() { poll_ms } else { cfg.taker.live.max_account_snapshot_age_ms + poll_ms },
            xemm_status,
            taker_status: watch::channel(None).0,
            xemm_account: Default::default(),
            taker_cfg: cfg.taker.clone(),
            taker_markets,
            maker_cfg: cfg.maker.clone(),
            maker_markets,
            xemm_stem,
        })
    }
}

impl Engines for LiveEngines {
    fn bots(&self) -> &'static [Bot] {
        if self.xemm_status.is_some() { &[Bot::Xemm, Bot::Taker] } else { &[Bot::Taker] }
    }

    fn spawn(&mut self, bot: Bot, io: &EngineIo, stop: CancellationToken) -> JoinHandle<Result<()>> {
        let pause = io.paused.clone();
        match bot {
            Bot::Xemm => {
                let (cfg, markets, stem) = (self.maker_cfg.clone(), self.maker_markets.clone(), self.xemm_stem.clone());
                let rights = Rights { want: io.want.subscribe(), lease: io.lease.clone() };
                let account = self.xemm_account.clone();
                tokio::spawn(async move { crate::livebot::run(&cfg, markets, stem, pause, Some(rights), account, stop).await })
            }
            Bot::Taker => {
                let with_xemm = self.xemm_status.is_some();
                let options = RunOptions {
                    lease: with_xemm.then(|| io.lease.subscribe()),
                    want: with_xemm.then(|| io.want.clone()),
                    pause: Some(pause),
                    status: (!with_xemm).then(|| self.taker_status.clone()),
                    ..RunOptions::default()
                };
                tokio::spawn(crate::taker::arb::run(self.taker_cfg.clone(), self.taker_markets.clone(), options, stop))
            }
        }
    }

    async fn status(&self) -> Result<Value> {
        let Some(xemm_status) = &self.xemm_status else {
            // Its own snapshot, like XEMM's below: no Lighter request of its own.
            let status = self.taker_status.borrow().clone();
            let (at, status) = status.context("the taker has published no account snapshot yet")?;
            let age = at.elapsed().as_millis() as i64;
            return if age <= self.max_status_age_ms { Ok(status) } else { bail!("the taker's account snapshot is {age} ms old") };
        };
        // Once XEMM publishes, its own snapshot: every venue read here again would come out
        // of Lighter's 60 requests/min, which XEMM's 2 s loop already half fills.
        match self.xemm_account.age_ms(crate::hotpath::clock::mono_now_ns()) {
            i64::MAX => Ok(serde_json::to_value(xemm_status.report().await?)?),
            age if age <= self.max_status_age_ms => Ok(xemm_status.from_snapshot(&self.xemm_account.load())),
            age => bail!("XEMM's account snapshot is {age} ms old"),
        }
    }
}
