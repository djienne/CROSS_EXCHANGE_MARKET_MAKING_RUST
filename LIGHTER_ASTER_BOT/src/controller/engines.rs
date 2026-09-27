//! The real engines behind the supervisor: the taker (`taker::arb::run`) and XEMM
//! (`livebot::run`) as tasks of this process, and their status reports from long-lived
//! pollers that keep one REST client per venue.

use std::path::PathBuf;

use anyhow::Result;
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
    taker_status: crate::taker::status::StatusPoller,
    xemm_status: crate::livebot::status::StatusPoller,
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
            taker_status: crate::taker::status::StatusPoller::new(&cfg.taker, taker_markets.clone()).await?,
            xemm_status: crate::livebot::status::StatusPoller::new(&cfg.maker, market).await?,
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
                tokio::spawn(async move { crate::livebot::run(&cfg, markets, stem, pause, Some(rights), stop).await })
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

    async fn status(&self, bot: Bot) -> Result<Value> {
        Ok(match bot {
            Bot::Taker => serde_json::to_value(self.taker_status.report().await?)?,
            Bot::Xemm => serde_json::to_value(self.xemm_status.report().await?)?,
        })
    }
}
