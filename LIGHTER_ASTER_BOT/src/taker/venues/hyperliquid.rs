//! The arbitrage's Hyperliquid leg, answering in the Lighter leg's types so `arb.rs` drives both
//! alike. An IOC's `/exchange` reply is terminal; its end and fees are read back by cloid. No
//! account state is pushed here, so positions, margins and open orders are REST reads and the
//! websocket-vs-REST position checks are skipped.

use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use anyhow::{bail, Context, Result};
use arc_swap::ArcSwapOption;
use chrono::Utc;
use rust_decimal::Decimal;
use serde_json::{json, Value};
use tokio::sync::Notify;

use super::lighter::{LighterAccountSnapshot, LighterFillConfirmation, LighterMarginSnapshot, SubmitOutcome};
use crate::connectors::{BookTap, Tap};
use crate::hyperliquid::client::{asset_in, dec, position_of, Asset, Client, Placed, Tif, FILLS_LOOKBACK_MS, ORDER_TTL_MS};
use crate::lighter::messages::RemoteOrder;
use crate::livebot::exec::creds::HyperliquidCreds;
use crate::taker::book::OrderBook;
use crate::taker::markets::MarketSpec;
use crate::taker::types::{FeeEvidence, FeeProvenance, FillSummary, MarketId, Side};

pub struct HyperliquidVenue {
    client: Client,
    asset: Asset,
    market: MarketId,
    book: Arc<Book>,
}

impl HyperliquidVenue {
    /// The taker trades one market: `spec`'s coin, whose book feed starts here.
    pub async fn new(base_url: &str, creds: HyperliquidCreds, spec: &MarketSpec) -> Result<Self> {
        let client = Client::new(base_url, creds)?;
        let asset = asset_in(&client.info(json!({"type": "meta"})).await?, &spec.lighter_symbol)?;
        let book = Arc::new(Book::default());
        let tap = Tap { book: Some(book.clone()), reconnect: Some(book.reconnect.clone()), scale: None, qty_scale: crate::livebot::scale::HotQtyScale::Hedge };
        let ws = crate::connectors::hyperliquid::ws_url(base_url);
        tokio::spawn(crate::connectors::hyperliquid::run_with_tap(ws, asset.coin.clone(), tap));
        Ok(Self { client, asset, market: spec.market_id.clone(), book })
    }

    fn check(&self, market: &MarketId) -> Result<()> {
        if *market != self.market {
            bail!("the Hyperliquid leg trades {}, not {market}", self.market);
        }
        Ok(())
    }

    pub fn order_book_arc(&self, market: &MarketId) -> Result<Arc<OrderBook>> {
        self.check(market)?;
        self.book.latest.load_full().with_context(|| format!("no Hyperliquid book for {market} yet"))
    }

    pub fn set_scan_notify(&self, wake: Arc<Notify>) {
        let _ = self.book.wake.set(wake);
    }

    pub fn request_order_book_reconnect(&self) {
        self.book.reconnect.notify_one();
    }

    /// Only the book: the account is read over REST when needed.
    pub async fn wait_ready(&self, market: &MarketId, timeout: Duration) -> Result<()> {
        tokio::time::timeout(timeout, async {
            while self.order_book_arc(market).is_err() {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .with_context(|| format!("no Hyperliquid book for {market} within {timeout:?}"))
    }

    /// An IOC at `price_bound`; no fill stream, so no `PendingFill`: `resolve_order_terminal`
    /// reads the end back. The client index carries the send time (ms × 1000 + a counter),
    /// which is where that read starts.
    pub async fn submit_market_order_deferred_fill(
        &self, market: &MarketId, side: Side, qty: Decimal, price_bound: Decimal, reduce_only: bool,
    ) -> (SubmitOutcome, Option<super::lighter::PendingFill>) {
        if let Err(error) = self.check(market) {
            return (SubmitOutcome::Rejected { reason: format!("{error:#}") }, None);
        }
        static COUNTER: std::sync::atomic::AtomicI64 = std::sync::atomic::AtomicI64::new(0);
        let index = Utc::now().timestamp_millis() * 1000 + COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed) % 1000;
        let placed = self.client.place(&self.asset, side == Side::Buy, price_bound, qty, Tif::Ioc, reduce_only, &cloid(index)).await;
        let outcome = match placed {
            Placed::Filled { oid, size, avg_px } => SubmitOutcome::Accepted {
                raw: format!("filled {size} at {avg_px}"), client_order_index: index, tx_hash: oid.to_string(), nonce: 0,
                fill: FillSummary::from_qty_notional(size, size * avg_px, Decimal::ZERO).map(|f| f.with_fee_provenance(FeeProvenance::Unknown)),
            },
            Placed::Rejected(reason) => SubmitOutcome::Rejected { reason },
            Placed::Resting { oid } => SubmitOutcome::Unknown { reason: format!("an IOC rests as {oid}"), client_order_index: index, tx_hash: String::new(), nonce: 0 },
            Placed::Unknown(reason) => SubmitOutcome::Unknown { reason, client_order_index: index, tx_hash: String::new(), nonce: 0 },
        };
        (outcome, None)
    }

    /// The order's end and its fills with their fees, by cloid.
    pub async fn resolve_order_terminal(&self, market: &MarketId, client_order_index: i64, timeout: Duration) -> Result<LighterFillConfirmation> {
        self.check(market)?;
        let (cloid, sent_ms) = (cloid(client_order_index), client_order_index / 1000);
        let fills = tokio::time::timeout(timeout, async {
            loop {
                let unknown_is_final = Utc::now().timestamp_millis() > sent_ms + ORDER_TTL_MS as i64 + 1_000;
                if let Some(fills) = self.client.ioc_fills(&cloid, sent_ms - FILLS_LOOKBACK_MS, None, unknown_is_final).await {
                    break fills;
                }
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        })
        .await
        .with_context(|| format!("Hyperliquid order {cloid} unresolved within {timeout:?}"))?;
        Ok(confirmation(client_order_index, &fills))
    }

    async fn state(&self) -> Result<Value> {
        self.client.user_info("clearinghouseState").await
    }

    pub async fn rest_position_qty(&self, market: &MarketId) -> Result<Decimal> {
        self.check(market)?;
        position_of(&self.state().await?, &self.asset.coin)
    }

    pub async fn account_snapshot(&self, market: &MarketId) -> Result<LighterAccountSnapshot> {
        self.check(market)?;
        account_of(&self.state().await?, &self.asset.coin)
    }

    pub async fn rest_margin_snapshot(&self) -> Result<LighterMarginSnapshot> {
        let state = self.state().await?;
        Ok(LighterMarginSnapshot { available_usdc: dec(&state["withdrawable"])?, equity_usdc: Some(dec(&state["marginSummary"]["accountValue"])?) })
    }

    pub async fn rest_open_orders_count(&self, market: &MarketId) -> Result<usize> {
        self.check(market)?;
        Ok(self.client.open_orders().await?.iter().filter(|o| o["coin"] == self.asset.coin.as_str()).count())
    }
}

/// `accountValue` already holds the positions' uPnL: it is split back out, so equity is counted
/// once as on Lighter (collateral + uPnL).
fn account_of(state: &Value, coin: &str) -> Result<LighterAccountSnapshot> {
    let upnl = state["assetPositions"].as_array().context("clearinghouseState without assetPositions")?
        .iter().map(|p| dec(&p["position"]["unrealizedPnl"])).sum::<Result<Decimal>>()?;
    Ok(LighterAccountSnapshot {
        position_qty: position_of(state, coin)?,
        available_usdc: dec(&state["withdrawable"])?,
        account_value_usdc: Some(dec(&state["marginSummary"]["accountValue"])? - upnl),
        unrealized_pnl_usdc: Some(upnl),
    })
}

/// The cloid of client index `index`: 16 bytes, the index in the low ones.
fn cloid(index: i64) -> String {
    format!("0x{index:032x}")
}

/// The order's fills as the Lighter leg's evidence; the order is terminal by then.
fn confirmation(client_order_index: i64, fills: &[Value]) -> LighterFillConfirmation {
    let fee_evidence: Vec<FeeEvidence> = fills.iter().filter_map(|f| {
        let notional_usd = dec(&f["sz"]).ok()? * dec(&f["px"]).ok()?;
        Some(FeeEvidence {
            trade_id: f["tid"].as_i64(), order_id: f["oid"].as_i64(), client_order_index,
            maker: f["crossed"].as_bool().map(|crossed| !crossed), notional_usd, fee_ticks: None,
            fee_usd: dec(&f["fee"]).ok(), event_time_ms: f["time"].as_i64(), source: "hyperliquid_user_fills".into(),
        })
    }).collect();
    let qty: Decimal = fills.iter().filter_map(|f| dec(&f["sz"]).ok()).sum();
    let notional: Decimal = fee_evidence.iter().map(|e| e.notional_usd).sum();
    let fee: Option<Decimal> = fee_evidence.iter().map(|e| e.fee_usd).sum();
    let fill = match fee {
        _ if qty.is_zero() => Some(FillSummary::zero()),
        Some(fee) => FillSummary::from_qty_notional(qty, notional, fee),
        None => FillSummary::from_qty_notional(qty, notional, Decimal::ZERO).map(|f| f.with_fee_provenance(FeeProvenance::Unknown)),
    };
    let order = RemoteOrder {
        client_order_index: Some(client_order_index),
        order_index: fee_evidence.first().and_then(|e| e.order_id),
        status: Some(if qty.is_zero() { "canceled" } else { "filled" }.into()),
        ..Default::default()
    };
    LighterFillConfirmation { fee_evidence, fill, terminal_order: Some(order), filled_qty: qty }
}

/// Fast five-level `l2Book` snapshots under a newer `bbo` top. HYPE snapshots were ~0.54 s
/// apart on 2026-09-28; deeper levels retain their snapshot age. The IOC bound caps fill price.
#[derive(Default)]
struct Book {
    /// The last snapshot and the newer top, if any.
    parts: Mutex<(Option<OrderBook>, Option<OrderBook>)>,
    latest: ArcSwapOption<OrderBook>,
    wake: OnceLock<Arc<Notify>>,
    reconnect: Arc<Notify>,
}

impl Book {
    fn apply(&self, depth: Option<OrderBook>, top: Option<OrderBook>) {
        let mut parts = self.parts.lock().unwrap();
        let (last_depth, last_top) = &mut *parts;
        if let Some(depth) = depth {
            *last_top = last_top.take().filter(|top| top.exch_ts > depth.exch_ts);
            *last_depth = Some(depth);
        }
        if let Some(top) = top {
            if last_depth.as_ref().is_some_and(|depth| top.exch_ts <= depth.exch_ts) {
                return;
            }
            *last_top = Some(top);
        }
        let Some(depth) = last_depth else { return };
        let book = match last_top {
            Some(top) => crate::taker::aster::ws::overlay(depth, top),
            None => depth.clone(),
        };
        self.latest.store(Some(Arc::new(book)));
        if let Some(wake) = self.wake.get() {
            wake.notify_one();
        }
    }
}

pub(crate) fn taker_book(book: crate::book::OrderBook) -> OrderBook {
    let rows = |levels: &[crate::book::Level]| levels.iter().map(|l| (l.px, l.qty)).collect::<Vec<_>>();
    OrderBook::from_levels(rows(&book.bids), rows(&book.asks), book.exch_ts, book.local_recv_ts)
}

impl BookTap for Book {
    fn publish(&self, book: crate::book::OrderBook) {
        self.apply(Some(taker_book(book)), None);
    }

    fn publish_bbo(&self, book: crate::book::OrderBook) {
        self.apply(None, Some(taker_book(book)));
    }

    fn touch(&self) {}

    fn mark_stream_down(&self) {
        *self.parts.lock().unwrap() = (None, None);
        self.latest.store(None);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::DateTime;
    use rust_decimal_macros::dec;

    fn book(bid: Decimal, ask: Decimal, ms: i64, depth: bool) -> crate::book::OrderBook {
        let at = DateTime::from_timestamp_millis(ms).unwrap();
        let (bids, asks) = if depth { (vec![(bid, dec!(1)), (bid - dec!(1), dec!(9))], vec![(ask, dec!(1)), (ask + dec!(1), dec!(9))]) } else { (vec![(bid, dec!(2))], vec![(ask, dec!(2))]) };
        crate::book::OrderBook::from_levels(bids, asks, at, at)
    }

    /// The top is laid over the snapshot only while newer than it; a stream down clears both.
    #[test]
    fn the_newer_bbo_tops_the_last_snapshot() {
        let cell = Book::default();
        cell.publish_bbo(book(dec!(40), dec!(41), 5, false));
        assert!(cell.latest.load().is_none(), "no book before a snapshot");
        cell.publish(book(dec!(40), dec!(41), 10, true));
        cell.publish_bbo(book(dec!(39), dec!(42), 10, false));
        assert_eq!(cell.latest.load_full().unwrap().best_bid().unwrap().px, dec!(40), "a top no newer than the snapshot is ignored");
        cell.publish_bbo(book(dec!(39.5), dec!(40.5), 11, false));
        let top = cell.latest.load_full().unwrap();
        let bids: Vec<_> = top.bids.iter().map(|l| (l.px, l.qty)).collect();
        assert_eq!(bids, [(dec!(39.5), dec!(2)), (dec!(39), dec!(9))], "the newer top, then the snapshot's levels behind it");
        assert_eq!(top.exch_ts.timestamp_millis(), 11);
        cell.publish(book(dec!(40), dec!(41), 12, true));
        assert_eq!(cell.latest.load_full().unwrap().best_bid().unwrap().px, dec!(40), "a newer snapshot drops the older top");
        cell.mark_stream_down();
        assert!(cell.latest.load().is_none());
    }

    #[test]
    fn the_account_counts_upnl_once() {
        let position = |coin: &str, szi: &str, upnl: &str| json!({"position": {"coin": coin, "szi": szi, "unrealizedPnl": upnl}});
        let state = json!({"marginSummary": {"accountValue": "100"}, "withdrawable": "80",
            "assetPositions": [position("HYPE", "-0.5", "3"), position("BTC", "0.001", "-1")]});
        let a = account_of(&state, "HYPE").unwrap();
        assert_eq!((a.position_qty, a.available_usdc, a.unrealized_pnl_usdc, a.equity_usdc()), (dec!(-0.5), dec!(80), Some(dec!(2)), Some(dec!(100))));
    }

    #[test]
    fn a_confirmation_carries_the_venue_fees() {
        let fills = [json!({"sz": "0.3", "px": "40", "fee": "0.0054", "tid": 7, "oid": 9, "crossed": true, "time": 1}),
                     json!({"sz": "0.2", "px": "41", "fee": "0.0037", "tid": 8, "oid": 9, "crossed": true, "time": 2})];
        let c = confirmation(1_000_000, &fills);
        let fill = c.fill.unwrap();
        assert_eq!((c.filled_qty, fill.notional, fill.fee_usd, fill.fee_provenance), (dec!(0.5), dec!(20.2), dec!(0.0091), FeeProvenance::Venue));
        assert!(c.terminal_order.as_ref().is_some_and(|o| o.is_terminal() && o.order_index == Some(9)));
        assert_eq!(c.fee_evidence.iter().map(|e| e.trade_id).collect::<Vec<_>>(), [Some(7), Some(8)]);
        let unpriced = confirmation(1, &[json!({"sz": "0.5", "px": "40", "tid": 7, "oid": 9})]).fill.unwrap();
        assert_eq!(unpriced.fee_provenance, FeeProvenance::Unknown, "a fill without its fee is not a venue fee of 0");
        assert!(confirmation(1, &[]).fill.is_some_and(|f| f.qty.is_zero()), "no fill is a terminal zero");
    }
}
