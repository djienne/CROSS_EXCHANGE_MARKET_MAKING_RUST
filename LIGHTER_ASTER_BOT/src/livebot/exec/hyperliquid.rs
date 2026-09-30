//! The hedge on Hyperliquid: one IOC per `Hedge` command, carrying the intent's cloid
//! (reduce-only for a correction), and the account reads the reconciler needs. An IOC's
//! `/exchange` reply is terminal (`filled` or an error). This worker polls `userFillsByTime`
//! for fees/trade ids and `orderStatus` for lost replies rather than subscribing to private fills.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use rust_decimal::Decimal;
use serde_json::{json, Value};
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::{info, warn};

use super::command::{ExecEvent, ExecutionTrade, HedgeCommand};
use super::creds::HyperliquidCreds;
use super::lighter::{HlAssetPosition, HlClearinghouse, HlMarginSummary, HlOpenOrder, HlPosition};
use crate::hotpath::clock::mono_now_ns;
use crate::hyperliquid::client::{asset_in, dec, Asset, Client, Placed, Tif, FILLS_LOOKBACK_MS, ORDER_TTL_MS};
use crate::livebot::account::Venue;
use crate::livebot::fills::{HedgeIntent, IntentPurpose, WireProof};
use crate::livebot::journal::Journal;
use crate::markets::MarketSpec;
use crate::types::{MarketId, Side};

/// How long a sent hedge is chased before it is left to the reconciler (as on Lighter).
const RESOLUTION_BUDGET: Duration = Duration::from_secs(60);

pub struct HyperliquidHedge {
    client: Client,
    assets: HashMap<MarketId, Asset>,
}

impl HyperliquidHedge {
    pub async fn new(base_url: &str, creds: HyperliquidCreds, specs: &[MarketSpec]) -> Result<Arc<Self>> {
        let client = Client::new(base_url, creds)?;
        let meta = client.info(json!({"type": "meta"})).await?;
        let assets = specs.iter().map(|s| Ok((s.market_id.clone(), asset_in(&meta, &s.hl_coin)?))).collect::<Result<_>>()?;
        Ok(Arc::new(Self { client, assets }))
    }

    /// The account in the reconciler's shape; `accountValue` already carries the positions' uPnL.
    pub async fn clearinghouse_state(&self) -> Result<HlClearinghouse> {
        let read_start_ns = mono_now_ns();
        let state = self.client.user_info("clearinghouseState").await?;
        let text = |v: &Value| v.as_str().map(str::to_owned).with_context(|| format!("clearinghouseState field {v}"));
        let positions = state["assetPositions"].as_array().context("clearinghouseState without assetPositions")?;
        let asset_positions = positions
            .iter()
            .map(|p| {
                let p = &p["position"];
                let position = HlPosition { coin: text(&p["coin"])?, szi: text(&p["szi"])?, entry_px: p["entryPx"].as_str().map(str::to_owned) };
                Ok(HlAssetPosition { position })
            })
            .collect::<Result<_>>()?;
        Ok(HlClearinghouse {
            margin_summary: HlMarginSummary { account_value: text(&state["marginSummary"]["accountValue"])? },
            asset_positions,
            withdrawable: text(&state["withdrawable"])?,
            data_origin_ns: read_start_ns,
            margin_source_ns: read_start_ns,
        })
    }

    pub async fn open_orders_info(&self) -> Result<Vec<HlOpenOrder>> {
        let text = |v: &Value| v.as_str().map(str::to_owned).with_context(|| format!("openOrders field {v}"));
        self.client
            .open_orders()
            .await?
            .iter()
            .map(|o| {
                let oid = o["oid"].as_u64().context("openOrders without an oid")?;
                Ok(HlOpenOrder { coin: text(&o["coin"])?, oid, side: text(&o["side"])?, limit_px: text(&o["limitPx"])?, sz: text(&o["sz"])? })
            })
            .collect()
    }

    /// The leverage set on `market`'s coin.
    pub async fn leverage(&self, market: &MarketId) -> Result<Decimal> {
        let coin = &self.assets.get(market).with_context(|| format!("no Hyperliquid asset for {}", market.0))?.coin;
        let data = self.client.info(json!({"type": "activeAssetData", "user": self.client.account(), "coin": coin})).await?;
        data["leverage"]["value"].as_u64().map(Decimal::from).with_context(|| format!("activeAssetData without a leverage: {data}"))
    }
}

pub async fn run_hyperliquid_worker(mut rx: Receiver<HedgeCommand>, tx: Sender<ExecEvent>, hl: Arc<HyperliquidHedge>, journal: Journal) {
    info!("hyperliquid hedge worker started");
    let mut settling = tokio::task::JoinSet::new();
    while let Some(cmd) = rx.recv().await {
        while settling.try_join_next().is_some() {}
        match cmd {
            HedgeCommand::Hedge { intent, aggressive_px } => hedge(&hl, &tx, &journal, &mut settling, intent, aggressive_px).await,
            // Hyperliquid nonces are this process's own clock (`client::next_nonce`).
            HedgeCommand::RefreshNonce => {}
            // What is queued still goes out, as on Lighter.
            HedgeCommand::Shutdown => rx.close(),
        }
    }
    let deadline = tokio::time::Instant::now() + RESOLUTION_BUDGET + Duration::from_secs(1);
    while !settling.is_empty() {
        if tokio::time::timeout_at(deadline, settling.join_next()).await.is_err() {
            settling.abort_all();
            break;
        }
    }
    info!("hyperliquid hedge worker stopped");
}

async fn hedge(hl: &Arc<HyperliquidHedge>, tx: &Sender<ExecEvent>, journal: &Journal, settling: &mut tokio::task::JoinSet<()>, intent: HedgeIntent, px: Decimal) {
    let cloid = intent.cloid;
    if intent.admission.is_claimed() {
        warn!("duplicate claimed execution ticket ignored: {}", cloid.to_hex());
        return;
    }
    if !intent.admission.try_claim(mono_now_ns()) {
        if intent.admission.is_cancelled() {
            let _ = tx.send(ExecEvent::AttemptNotSent { cloid, reason: "expired or cancelled before send".into() }).await;
        }
        return;
    }
    let Some(asset) = hl.assets.get(&intent.market) else {
        let _ = tx.send(ExecEvent::AttemptNotSent { cloid, reason: format!("no Hyperliquid asset for {}", intent.market.0) }).await;
        return;
    };
    let since_ms = chrono::Utc::now().timestamp_millis() - FILLS_LOOKBACK_MS;
    let sent_ns = mono_now_ns();
    let _ = tx.send(ExecEvent::AttemptStarted { cloid, proof: WireProof { tx_hash: None, nonce: None, client_order_index: None, sent_ns } }).await;
    let buy = intent.hedge_side == Side::Buy;
    let placed = hl.client.place(asset, buy, px, intent.qty, Tif::Ioc, intent.purpose == IntentPurpose::ReduceDelta, &cloid.to_hex()).await;
    info!("hedge timing: cloid={} queue_us={} response_us={}", cloid.to_hex(),
        sent_ns.saturating_sub(intent.created_ns) / 1_000, mono_now_ns().saturating_sub(sent_ns) / 1_000);
    let reply = match placed {
        Placed::Filled { oid, size, avg_px } => {
            // The position moves now; its fee and trade ids follow from the fills.
            let _ = tx.send(ExecEvent::ExecutionProgress { cloid, cumulative_qty: size, cumulative_quote_usd: Some(size * avg_px),
                cumulative_fee_usd: None, terminal: false, venue_order_id: Some(oid.to_string()), event_time_ms: None }).await;
            Some((size, avg_px, oid))
        }
        // "could not immediately match" is the retryable no-fill (`hedge_reject_is_definitive_no_fill`).
        Placed::Rejected(error) => {
            let _ = tx.send(ExecEvent::HedgeReject { cloid, reason: format!("Hyperliquid refused: {error}") }).await;
            return;
        }
        Placed::Resting { oid } => {
            let _ = tx.send(ExecEvent::HedgeUnknown { cloid, reason: format!("the IOC rests as {oid}") }).await;
            None
        }
        Placed::Unknown(error) => {
            let _ = tx.send(ExecEvent::HedgeUnknown { cloid, reason: format!("reply lost ({error}); resolving by cloid") }).await;
            None
        }
    };
    settling.spawn(settle(hl.clone(), tx.clone(), journal.clone(), intent, since_ms, reply));
}

/// Waits for a sent IOC's end and for its fills to be listed, journals them, and reports the
/// terminal total with its fees. `reply` is the fill the venue answered, `None` if the answer
/// was lost: then `orderStatus` gives the end and the size its fills must reach.
async fn settle(hl: Arc<HyperliquidHedge>, tx: Sender<ExecEvent>, journal: Journal, intent: HedgeIntent, since_ms: i64, reply: Option<(Decimal, Decimal, u64)>) {
    let cloid = intent.cloid.to_hex();
    let started = Instant::now();
    // After its `expiresAfter` an order the venue never saw never will be.
    let unknown_is_final = started + Duration::from_millis(ORDER_TTL_MS + 1_000);
    let mut poll = tokio::time::interval(Duration::from_millis(500));
    while started.elapsed() < RESOLUTION_BUDGET {
        poll.tick().await;
        let filled = reply.map(|(size, ..)| size);
        let Some(fills) = hl.client.ioc_fills(&cloid, since_ms, filled, Instant::now() >= unknown_is_final).await else { continue };
        let trades: Vec<ExecutionTrade> = fills.iter().filter_map(|f| trade(f, &intent)).collect();
        let qty: Decimal = trades.iter().map(|t| t.qty).sum();
        let quote: Decimal = trades.iter().map(|t| t.notional_usd).sum();
        let fee: Option<Decimal> = trades.iter().map(|t| t.fee_usd).sum();
        let (oid, time) = (trades.iter().find_map(|t| t.order_id.clone()), trades.iter().filter_map(|t| t.event_time_ms).max());
        for trade in trades {
            journal.execution_trade(mono_now_ns(), trade);
        }
        let _ = tx.send(ExecEvent::ExecutionProgress { cloid: intent.cloid, cumulative_qty: qty, cumulative_quote_usd: Some(quote),
            cumulative_fee_usd: fee, terminal: true, venue_order_id: oid, event_time_ms: time }).await;
        return;
    }
    let _ = match reply {
        // The venue's answer stands; only its fee stays unknown.
        Some((size, avg_px, oid)) => tx.send(ExecEvent::ExecutionProgress { cloid: intent.cloid, cumulative_qty: size,
            cumulative_quote_usd: Some(size * avg_px), cumulative_fee_usd: None, terminal: true, venue_order_id: Some(oid.to_string()), event_time_ms: None }).await,
        None => tx.send(ExecEvent::HedgeUnknown { cloid: intent.cloid, reason: "60 s of resolution exhausted; left to the reconciler".into() }).await,
    };
}

/// One of the account's fills as the journal's trade evidence; the fee is in USDC.
fn trade(fill: &Value, intent: &HedgeIntent) -> Option<ExecutionTrade> {
    let (qty, px) = (dec(&fill["sz"]).ok()?, dec(&fill["px"]).ok()?);
    Some(ExecutionTrade {
        attempt_id: intent.cloid.to_hex(),
        logical_id: intent.logical_id.to_hex(),
        venue: Venue::Hedge,
        market: intent.market.0.clone(),
        side: if fill["side"] == "B" { Side::Buy } else { Side::Sell },
        trade_id: fill["tid"].to_string(),
        identity_complete: fill["tid"].is_u64(),
        event_time_ms: fill["time"].as_i64(),
        order_id: fill["oid"].as_u64().map(|oid| oid.to_string()),
        client_order_index: 0,
        qty,
        px,
        notional_usd: qty * px,
        maker: fill["crossed"].as_bool().map(|crossed| !crossed),
        fee_ticks: None,
        fee_usd: dec(&fill["fee"]).ok(),
    })
}
