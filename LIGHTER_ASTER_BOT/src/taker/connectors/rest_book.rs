//! The taker's REST books: the shared fetchers (`crate::connectors::rest_book`) on the taker's own
//! client, as its book type.

use std::time::Duration;

use anyhow::{Context, Result};

use crate::connectors::rest_book as shared;
use crate::taker::book::OrderBook;
use crate::taker::venues::hyperliquid::taker_book;

pub fn client() -> Result<reqwest::Client> {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .tcp_nodelay(true)
        .pool_idle_timeout(Some(Duration::from_secs(120)))
        .pool_max_idle_per_host(4)
        .tcp_keepalive(Some(Duration::from_secs(30)))
        .build()
        .context("building REST book client")
}

pub async fn fetch_aster_book(client: &reqwest::Client, base_url: &str, symbol_upper: &str, limit: u32) -> Result<OrderBook> {
    shared::fetch_aster_book_from_base(client, base_url, symbol_upper, limit).await.map(taker_book)
}

pub async fn fetch_lighter_book(client: &reqwest::Client, base_url: &str, market_id: u32, limit: u32) -> Result<OrderBook> {
    shared::fetch_lighter_book_from_base(client, base_url, market_id, limit).await.map(taker_book)
}
