//! `probe hl-*` against the real account of `hyperliquid.env`. `hl-balance` only reads.
//! `hl-place-cancel` rests one post-only buy 10% under the bid and cancels it; `hl-market`
//! buys ~$10.5 by IOC and sells it back reduce-only. Both trade only from flat with no open
//! order, under `--i-understand-live` and a `--max-usd` cap in [10.5, 20].

use std::time::Instant;

use anyhow::{bail, ensure, Result};
use rust_decimal::{Decimal, RoundingStrategy};
use rust_decimal_macros::dec;
use serde_json::{json, Value};

use super::client::{dec, position_of, Asset, Client, Placed, Tif, MAINNET, ORDER_TTL_MS};
use crate::livebot::exec::creds::HyperliquidCreds;

/// Above the venue's $10 minimum, with room for the price to move.
const MIN_ORDER_USD: Decimal = dec!(10.5);

pub async fn run(check: &str, coin: &str, i_understand_live: bool, max_usd: Decimal) -> Result<()> {
    let client = Client::new(MAINNET, HyperliquidCreds::from_env()?)?;
    let asset = client.asset(coin).await?;
    if check == "hl-balance" {
        return balance(&client, &asset).await;
    }
    ensure!(i_understand_live, "{check} trades real funds: re-run with --i-understand-live --max-usd <N>");
    ensure!((MIN_ORDER_USD..=dec!(20)).contains(&max_usd), "--max-usd must be within [10.5, 20], got {max_usd}");
    let (position, open) = (client.position(coin).await?, client.open_orders().await?.len());
    ensure!(position.is_zero() && open == 0, "{check} starts only flat with no open order: {coin} {position}, {open} open");
    match check {
        "hl-place-cancel" => place_cancel(&client, &asset, max_usd).await?,
        "hl-market" => market(&client, &asset, max_usd).await?,
        other => bail!("unknown Hyperliquid probe {other}"),
    }
    let (position, open) = (client.position(coin).await?, client.open_orders().await?.len());
    ensure!(position.is_zero() && open == 0, "not clean after {check}: {coin} {position}, {open} open; check the account");
    println!("clean: flat, no open order");
    Ok(())
}

async fn balance(client: &Client, asset: &Asset) -> Result<()> {
    let at = Instant::now();
    let state = client.user_info("clearinghouseState").await?;
    println!("info rtt {} ms", at.elapsed().as_millis());
    println!("account {} signed by {}", client.account(), client.signer());
    println!("value {} withdrawable {} | {} {} (asset {}, {} size decimals)", state["marginSummary"]["accountValue"],
        state["withdrawable"], asset.coin, position_of(&state, &asset.coin)?, asset.index, asset.sz_decimals);
    println!("open orders {}", Value::from(client.open_orders().await?));
    let active = client.info(json!({"type": "activeAssetData", "user": client.account(), "coin": asset.coin})).await?;
    println!("leverage {}", active["leverage"]);
    let fees = client.user_info("userFees").await?;
    println!("fees maker {} taker {}", fees["userAddRate"], fees["userCrossRate"]);
    let limit = client.user_info("userRateLimit").await?;
    println!("actions used {} of {} (volume {})", limit["nRequestsUsed"], limit["nRequestsCap"], limit["cumVlm"]);
    Ok(())
}

fn cloid() -> String {
    format!("0x{}", uuid::Uuid::new_v4().simple())
}

/// The smallest size worth `MIN_ORDER_USD` at `px`, within the cap.
fn size_at(px: Decimal, asset: &Asset, max_usd: Decimal) -> Result<Decimal> {
    let size = (MIN_ORDER_USD / px).round_dp_with_strategy(asset.sz_decimals, RoundingStrategy::AwayFromZero);
    ensure!(size * px <= max_usd, "{size} {} at {px} exceeds --max-usd {max_usd}", asset.coin);
    Ok(size)
}

/// Checks run in order; the order is cancelled whatever happens.
async fn place_cancel(client: &Client, asset: &Asset, max_usd: Decimal) -> Result<()> {
    let (bid, _) = client.top(&asset.coin).await?;
    let (px, id) = (bid * dec!(0.9), cloid());
    let size = size_at(px, asset, max_usd)?;
    let at = Instant::now();
    let placed = client.place(asset, true, px, size, Tif::Alo, false, &id).await;
    println!("post-only buy {size} at {px}: {placed:?} in {} ms", at.elapsed().as_millis());
    let checks = async {
        ensure!(matches!(placed, Placed::Resting { .. }), "the order does not rest");
        ensure!(client.open_orders().await?.iter().any(|o| o["cloid"] == id.as_str()), "the order is not in openOrders");
        ensure!(client.order_status(&id).await? == "open", "orderStatus is not open");
        let at = Instant::now();
        client.cancel(asset, &id).await?;
        println!("cancelled in {} ms", at.elapsed().as_millis());
        ensure!(client.order_status(&id).await? == "canceled", "orderStatus is not canceled");
        ensure!(client.order_status(&cloid()).await? == "unknownOid", "an unused cloid is not unknownOid");
        Ok::<_, anyhow::Error>(())
    }.await;
    if checks.is_err() && !matches!(placed, Placed::Rejected(_)) {
        let _ = client.cancel(asset, &id).await;
    }
    checks
}

/// The size an IOC filled; a lost reply is resolved by cloid once the order can no longer
/// land, and the size read back from the fills.
async fn filled(client: &Client, placed: Placed, id: &str, since_ms: i64) -> Result<Decimal> {
    match placed {
        Placed::Filled { size, .. } => return Ok(size),
        Placed::Rejected(_) => return Ok(Decimal::ZERO),
        Placed::Resting { oid } => bail!("IOC {id} rests as {oid}"),
        Placed::Unknown(_) => {}
    }
    let deadline = Instant::now() + std::time::Duration::from_millis(ORDER_TTL_MS + 2_000);
    while client.order_status(id).await? == "unknownOid" && Instant::now() < deadline {
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
    let fills = client.fills_since(since_ms).await?;
    fills.iter().filter(|f| f["cloid"] == id).map(|f| dec(&f["sz"])).sum()
}

/// Buy, sell back reduce-only, and on any failure still close what is open.
async fn market(client: &Client, asset: &Asset, max_usd: Decimal) -> Result<()> {
    let since_ms = chrono::Utc::now().timestamp_millis();
    let (_, ask) = client.top(&asset.coin).await?;
    let size = size_at(ask * dec!(1.005), asset, max_usd)?;
    let ids = [cloid(), cloid()];
    let trade = async {
        let at = Instant::now();
        let placed = client.place(asset, true, ask * dec!(1.005), size, Tif::Ioc, false, &ids[0]).await;
        println!("IOC buy {size} up to {}: {placed:?} in {} ms", ask * dec!(1.005), at.elapsed().as_millis());
        let bought = filled(client, placed, &ids[0], since_ms).await?;
        ensure!(bought > Decimal::ZERO, "the IOC buy did not fill");
        let held = client.position(&asset.coin).await?;
        ensure!(held == bought, "position {held} after buying {bought}");
        let (bid, _) = client.top(&asset.coin).await?;
        let at = Instant::now();
        let placed = client.place(asset, false, bid * dec!(0.995), held, Tif::Ioc, true, &ids[1]).await;
        println!("reduce-only IOC sell {held} down to {}: {placed:?} in {} ms", bid * dec!(0.995), at.elapsed().as_millis());
        filled(client, placed, &ids[1], since_ms).await
    }.await;
    for _ in 0..3 {
        let left = client.position(&asset.coin).await?;
        if left.is_zero() {
            break;
        }
        let (bid, ask) = client.top(&asset.coin).await?;
        let (buy, px) = if left < Decimal::ZERO { (true, ask * dec!(1.01)) } else { (false, bid * dec!(0.99)) };
        println!("closing {left}: {:?}", client.place(asset, buy, px, left.abs(), Tif::Ioc, true, &cloid()).await);
    }
    trade?;
    let fills = client.fills_since(since_ms).await?;
    let (mut cash, mut fees) = (Decimal::ZERO, Decimal::ZERO);
    for fill in fills.iter().filter(|f| ids.iter().any(|id| f["cloid"] == id.as_str())) {
        let (px, sz, fee) = (dec(&fill["px"])?, dec(&fill["sz"])?, dec(&fill["fee"])?);
        cash += if fill["side"] == "B" { -px * sz } else { px * sz };
        fees += fee;
        println!("fill {} {sz} at {px}, fee {fee} {}, crossed {}", fill["side"], fill["feeToken"], fill["crossed"]);
    }
    println!("round trip: spread {cash} USDC, fees {fees} USDC, net {}", cash - fees);
    Ok(())
}
