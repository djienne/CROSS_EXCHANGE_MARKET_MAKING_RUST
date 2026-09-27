//! The real engines behind the supervisor: the taker (`taker::arb::run`) and XEMM
//! (`livebot::run`) as tasks of this process, and the status of the accounts they trade.

use std::path::PathBuf;

use anyhow::{bail, Result};
use serde_json::Value;
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
    /// Venue reads, for the startup check before XEMM publishes its first snapshot.
    xemm_status: crate::livebot::status::StatusPoller,
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
        Ok(Self {
            xemm_status: crate::livebot::status::StatusPoller::new(&cfg.maker, market).await?,
            xemm_account: Default::default(),
            max_status_age_ms: cfg.controller.poll_sec.saturating_mul(1000) as i64,
            taker_cfg: cfg.taker.clone(),
            taker_markets,
            maker_cfg: cfg.maker.clone(),
            maker_markets,
            xemm_stem,
        })
    }
}

impl Engines for LiveEngines {
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
                let options = RunOptions {
                    lease: Some(io.lease.subscribe()),
                    want: Some(io.want.clone()),
                    pause: Some(pause),
                    ..RunOptions::default()
                };
                tokio::spawn(crate::taker::arb::run(self.taker_cfg.clone(), self.taker_markets.clone(), options, stop))
            }
        }
    }

    async fn status(&self) -> Result<Value> {
        // Once XEMM publishes, its own snapshot: every venue read here again would come out
        // of Lighter's 60 requests/min, which XEMM's 2 s loop already half fills.
        match self.xemm_account.age_ms(crate::hotpath::clock::mono_now_ns()) {
            i64::MAX => Ok(serde_json::to_value(self.xemm_status.report().await?)?),
            age if age <= self.max_status_age_ms => Ok(self.xemm_status.from_snapshot(&self.xemm_account.load())),
            age => bail!("XEMM's account snapshot is {age} ms old"),
        }
    }
}
