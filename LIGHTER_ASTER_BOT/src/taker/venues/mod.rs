//! The arbitrage's legs. The second is Lighter or Hyperliquid, both answering in the Lighter
//! leg's types; the first is Aster, or Lighter standing in for it against Hyperliquid.

pub mod hyperliquid;
pub mod lighter;

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Result};
use rust_decimal::Decimal;
use tokio::sync::Notify;

use crate::taker::aster::rest::AsterRest;
use crate::taker::aster::ws::AsterBookFeed;
use crate::taker::book::OrderBook;
use crate::taker::types::{MarketId, Side};
use hyperliquid::HyperliquidVenue;
use lighter::{LighterAccountSnapshot, LighterFillConfirmation, LighterMarginSnapshot, LighterVenue, PendingFill, SubmitOutcome};

pub enum OtherLeg {
    Lighter(LighterVenue),
    Hyperliquid(HyperliquidVenue),
}

/// `$body` on whichever venue `$leg` holds.
macro_rules! either {
    ($leg:expr, $venue:ident => $body:expr) => {
        match $leg {
            OtherLeg::Lighter($venue) => $body,
            OtherLeg::Hyperliquid($venue) => $body,
        }
    };
}

impl OtherLeg {
    /// Its name in order identities and logs.
    pub fn name(&self) -> &'static str {
        match self {
            Self::Lighter(_) => "lighter",
            Self::Hyperliquid(_) => "hyperliquid",
        }
    }

    pub fn order_book_arc(&self, market: &MarketId) -> Result<Arc<OrderBook>> {
        either!(self, v => v.order_book_arc(market))
    }

    pub fn order_book(&self, market: &MarketId) -> Result<OrderBook> {
        self.order_book_arc(market).map(|book| (*book).clone())
    }

    pub fn set_scan_notify(&self, wake: Arc<Notify>) {
        either!(self, v => v.set_scan_notify(wake))
    }

    pub async fn wait_ready(&self, market: &MarketId, timeout: Duration) -> Result<()> {
        either!(self, v => v.wait_ready(market, timeout).await)
    }

    pub fn request_order_book_reconnect(&self, market: &MarketId) -> Result<()> {
        match self {
            Self::Lighter(l) => l.request_order_book_reconnect(market),
            Self::Hyperliquid(h) => Ok(h.request_order_book_reconnect()),
        }
    }

    pub async fn submit_market_order_deferred_fill(
        &self, market: &MarketId, side: Side, qty: Decimal, price_bound: Decimal, reduce_only: bool,
    ) -> (SubmitOutcome, Option<PendingFill>) {
        either!(self, v => v.submit_market_order_deferred_fill(market, side, qty, price_bound, reduce_only).await)
    }

    pub async fn rest_position_qty(&self, market: &MarketId) -> Result<Decimal> {
        either!(self, v => v.rest_position_qty(market).await)
    }

    pub async fn account_snapshot(&self, market: &MarketId) -> Result<LighterAccountSnapshot> {
        either!(self, v => v.account_snapshot(market).await)
    }

    pub async fn rest_margin_snapshot(&self) -> Result<LighterMarginSnapshot> {
        either!(self, v => v.rest_margin_snapshot().await)
    }

    pub async fn rest_open_orders_count(&self, market: &MarketId) -> Result<usize> {
        either!(self, v => v.rest_open_orders_count(market).await)
    }

    /// Lighter's websocket count; Hyperliquid's REST one.
    pub async fn open_orders_count(&self, market: &MarketId) -> Result<usize> {
        match self {
            Self::Lighter(l) => l.open_orders_count(market).await,
            Self::Hyperliquid(h) => h.rest_open_orders_count(market).await,
        }
    }

    /// Lighter's websocket position. Hyperliquid pushes none here, so the checks that hold it
    /// against the REST one skip.
    pub fn ws_position_qty(&self, market: &MarketId) -> Result<Decimal> {
        match self {
            Self::Lighter(l) => l.ws_position_qty(market),
            Self::Hyperliquid(_) => bail!("Hyperliquid pushes no position to the taker"),
        }
    }

    /// Hyperliquid's nonces are this process's clock: nothing to refresh or wait for.
    pub async fn refresh_nonce(&self) -> Result<()> {
        match self {
            Self::Lighter(l) => l.refresh_nonce().await,
            Self::Hyperliquid(_) => Ok(()),
        }
    }

    pub fn tx_ready(&self) -> bool {
        match self {
            Self::Lighter(l) => l.tx_ready(),
            Self::Hyperliquid(_) => true,
        }
    }
}

/// An order's end by its client index: what the evidence resolver reads, from the arbitrage's
/// leg or from the Lighter-only diagnostics' venue.
pub trait TerminalOrders: Sync {
    fn resolve_order_terminal(&self, market: &MarketId, client_order_index: i64, side: Side, timeout: Duration)
        -> impl Future<Output = Result<LighterFillConfirmation>> + Send;
}

impl TerminalOrders for LighterVenue {
    fn resolve_order_terminal(&self, market: &MarketId, client_order_index: i64, side: Side, timeout: Duration)
        -> impl Future<Output = Result<LighterFillConfirmation>> + Send {
        LighterVenue::resolve_order_terminal(self, market, client_order_index, side, timeout)
    }
}

impl TerminalOrders for OtherLeg {
    async fn resolve_order_terminal(&self, market: &MarketId, client_order_index: i64, side: Side, timeout: Duration) -> Result<LighterFillConfirmation> {
        match self {
            Self::Lighter(l) => l.resolve_order_terminal(market, client_order_index, side, timeout).await,
            Self::Hyperliquid(h) => h.resolve_order_terminal(market, client_order_index, timeout).await,
        }
    }
}

/// The arbitrage's first leg. Lighter standing in for Aster answers as the second leg does
/// (in the first leg's own market), and Aster in the same types.
pub enum FirstLeg {
    Aster { rest: Arc<AsterRest>, books: AsterBookFeed },
    Other(OtherLeg),
}

impl FirstLeg {
    pub fn order_book_arc(&self, market: &MarketId) -> Result<Arc<OrderBook>> {
        match self {
            Self::Aster { books, .. } => books.order_book_arc(),
            Self::Other(leg) => leg.order_book_arc(market),
        }
    }

    pub fn set_scan_notify(&self, wake: Arc<Notify>) {
        match self {
            Self::Aster { books, .. } => books.set_scan_notify(wake),
            Self::Other(leg) => leg.set_scan_notify(wake),
        }
    }

    pub async fn wait_ready(&self, market: &MarketId, timeout: Duration) -> Result<()> {
        match self {
            Self::Aster { books, .. } => books.wait_ready(timeout).await,
            Self::Other(leg) => leg.wait_ready(market, timeout).await,
        }
    }

    pub async fn position_qty(&self, market: &MarketId) -> Result<Decimal> {
        match self {
            Self::Aster { rest, .. } => rest.position_qty(market).await,
            Self::Other(leg) => leg.rest_position_qty(market).await,
        }
    }

    pub async fn open_orders_count(&self, market: &MarketId) -> Result<usize> {
        match self {
            Self::Aster { rest, .. } => rest.open_orders(market).await.map(|orders| orders.len()),
            Self::Other(leg) => leg.rest_open_orders_count(market).await,
        }
    }

    pub async fn margin_snapshot(&self) -> Result<LighterMarginSnapshot> {
        match self {
            Self::Aster { rest, .. } => rest.balance_snapshot().await
                .map(|b| LighterMarginSnapshot { available_usdc: b.available_usd, equity_usdc: b.equity_usd() }),
            Self::Other(leg) => leg.rest_margin_snapshot().await,
        }
    }

    pub async fn account_snapshot(&self, market: &MarketId) -> Result<LighterAccountSnapshot> {
        match self {
            Self::Aster { rest, .. } => {
                let (position_qty, b) = tokio::try_join!(rest.position_qty(market), rest.balance_snapshot())?;
                Ok(LighterAccountSnapshot { position_qty, available_usdc: b.available_usd,
                    account_value_usdc: b.cross_wallet_balance_usd, unrealized_pnl_usdc: b.cross_unrealized_pnl_usd })
            }
            Self::Other(leg) => leg.account_snapshot(market).await,
        }
    }

    /// Lighter's websocket position; Aster pushes none to the taker.
    pub fn ws_position_qty(&self, market: &MarketId) -> Option<Decimal> {
        match self {
            Self::Aster { .. } => None,
            Self::Other(leg) => leg.ws_position_qty(market).ok(),
        }
    }

    pub async fn refresh_nonce(&self) -> Result<()> {
        match self {
            Self::Aster { .. } => Ok(()),
            Self::Other(leg) => leg.refresh_nonce().await,
        }
    }

    pub fn tx_ready(&self) -> bool {
        match self {
            Self::Aster { .. } => true,
            Self::Other(leg) => leg.tx_ready(),
        }
    }
}
