//! Hyperliquid REST: one keep-alive client for `/info` reads and signed `/exchange` actions.
//! An action whose reply is lost (timeout, 5xx, unreadable body) has an unknown outcome:
//! resolve it by cloid with [`Client::order_status`], never by sending it again.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use k256::ecdsa::SigningKey;
use rust_decimal::{Decimal, RoundingStrategy};
use serde::Serialize;
use serde_json::{json, Value};

use super::sign::{sign_action, Signature};
use crate::livebot::exec::creds::HyperliquidCreds;

pub const MAINNET: &str = "https://api.hyperliquid.xyz";
/// An order's `expiresAfter` past its nonce: a later arrival is refused (and costs five times
/// the usual rate limit), so a lost reply resolves within this plus the resolver's margin.
pub const ORDER_TTL_MS: u64 = 5_000;

/// A perp as `meta` lists it; `index` is its asset id in actions.
#[derive(Debug, Clone)]
pub struct Asset {
    pub index: u32,
    pub coin: String,
    pub sz_decimals: u32,
}

#[derive(Debug, Clone, Copy)]
pub enum Tif {
    /// Post-only.
    Alo,
    Ioc,
}

/// What `/exchange` said about one order.
#[derive(Debug, Clone, PartialEq)]
pub enum Placed {
    Resting { oid: u64 },
    Filled { oid: u64, size: Decimal, avg_px: Decimal },
    /// Refused: nothing rests or filled (an IOC that found no match included).
    Rejected(String),
    /// The reply was lost or unreadable: resolve by cloid, never resend.
    Unknown(String),
}

/// The action wire structs: field order is the MessagePack order the signature covers.
#[derive(Serialize)]
struct Limit {
    tif: &'static str,
}

#[derive(Serialize)]
struct OrderType {
    limit: Limit,
}

#[derive(Serialize)]
struct OrderWire<'a> {
    a: u32,
    b: bool,
    p: String,
    s: String,
    r: bool,
    t: OrderType,
    c: &'a str,
}

#[derive(Serialize)]
struct OrderAction<'a> {
    #[serde(rename = "type")]
    kind: &'static str,
    orders: [OrderWire<'a>; 1],
    grouping: &'static str,
}

#[derive(Serialize)]
struct CancelWire<'a> {
    asset: u32,
    cloid: &'a str,
}

#[derive(Serialize)]
struct CancelAction<'a> {
    #[serde(rename = "type")]
    kind: &'static str,
    cancels: [CancelWire<'a>; 1],
}

#[derive(Serialize)]
struct Envelope<'a, A> {
    action: &'a A,
    nonce: u64,
    signature: Signature,
    #[serde(rename = "vaultAddress", skip_serializing_if = "Option::is_none")]
    vault_address: Option<&'a str>,
    #[serde(rename = "expiresAfter", skip_serializing_if = "Option::is_none")]
    expires_after: Option<u64>,
}

/// `/exchange`'s answer to one action.
enum Reply {
    Ok(Value),
    Refused(String),
    Unknown(String),
}

/// Nonces belong to the agent key and the venue keeps its highest 100: one strictly
/// increasing millisecond clock for the whole process.
fn next_nonce() -> u64 {
    static LAST: AtomicU64 = AtomicU64::new(0);
    let now = chrono::Utc::now().timestamp_millis() as u64;
    let prev = LAST.fetch_update(Ordering::AcqRel, Ordering::Acquire, |prev| Some(now.max(prev + 1))).expect("always Some");
    now.max(prev + 1)
}

/// A price as Hyperliquid accepts it: at most 5 significant figures (an integer always
/// passes) and `6 - sz_decimals` decimals, without trailing zeros. `up` rounds away from zero:
/// a buy IOC rounds up and a sell down, so each keeps its reach.
pub fn fmt_px(px: Decimal, sz_decimals: u32, up: bool) -> String {
    let rounding = if up { RoundingStrategy::AwayFromZero } else { RoundingStrategy::ToZero };
    let px = if px >= Decimal::from(10_000) { px.round_dp_with_strategy(0, rounding) } else { px.round_sf_with_strategy(5, rounding).unwrap_or(px) };
    px.round_dp_with_strategy(6u32.saturating_sub(sz_decimals), rounding).normalize().to_string()
}

/// A size at the asset's decimals, rounded down.
pub fn fmt_sz(size: Decimal, sz_decimals: u32) -> String {
    size.round_dp_with_strategy(sz_decimals, RoundingStrategy::ToZero).normalize().to_string()
}

pub fn dec(v: &Value) -> Result<Decimal> {
    v.as_str().context("not a decimal string")?.parse().with_context(|| format!("not a decimal: {v}"))
}

/// The one order status of an `order` reply; statuses are positional, so another count is
/// unknown.
fn placed(response: &Value) -> Placed {
    let statuses = response.pointer("/data/statuses").and_then(Value::as_array);
    let Some([status]) = statuses.map(Vec::as_slice) else { return Placed::Unknown(format!("unexpected order reply {response}")) };
    if let Some(oid) = status.pointer("/resting/oid").and_then(Value::as_u64) {
        return Placed::Resting { oid };
    }
    if let Some(filled) = status.get("filled") {
        return match (filled["oid"].as_u64(), dec(&filled["totalSz"]), dec(&filled["avgPx"])) {
            (Some(oid), Ok(size), Ok(avg_px)) => Placed::Filled { oid, size, avg_px },
            _ => Placed::Unknown(format!("unreadable fill {filled}")),
        };
    }
    match status["error"].as_str() {
        Some(error) => Placed::Rejected(error.to_string()),
        None => Placed::Unknown(format!("unexpected order status {status}")),
    }
}

/// `szi` (signed, + long) of `coin` in a `clearinghouseState`; 0 without a position.
pub fn position_of(state: &Value, coin: &str) -> Result<Decimal> {
    let positions = state["assetPositions"].as_array().context("clearinghouseState without assetPositions")?;
    positions.iter().find(|p| p["position"]["coin"] == coin).map_or(Ok(Decimal::ZERO), |p| dec(&p["position"]["szi"]))
}

/// POST `/info` at `base`.
pub async fn info(http: &reqwest::Client, base: &str, body: Value) -> Result<Value> {
    let reply = http.post(format!("{}/info", base.trim_end_matches('/'))).json(&body).send().await
        .and_then(reqwest::Response::error_for_status)
        .with_context(|| format!("Hyperliquid info {}", body["type"]))?;
    Ok(reply.json().await?)
}

/// `coin`'s asset id (its place in the universe) and size decimals, from a `meta` reply.
pub fn asset_in(meta: &Value, coin: &str) -> Result<Asset> {
    let universe = meta["universe"].as_array().context("meta without a universe")?;
    let (index, entry) = universe.iter().enumerate().find(|(_, a)| a["name"] == coin)
        .with_context(|| format!("{coin} is not a Hyperliquid perp"))?;
    let sz_decimals = entry["szDecimals"].as_u64().context("meta without szDecimals")? as u32;
    Ok(Asset { index: index as u32, coin: coin.to_string(), sz_decimals })
}

pub struct Client {
    http: reqwest::Client,
    base: String,
    creds: HyperliquidCreds,
    key: SigningKey,
}

impl Client {
    pub fn new(base: &str, creds: HyperliquidCreds) -> Result<Self> {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .tcp_nodelay(true)
            .pool_idle_timeout(Duration::from_secs(120))
            .build()?;
        let key = SigningKey::from_slice(&creds.key).map_err(|_| anyhow!("hyperliquid env: invalid private_key"))?;
        Ok(Self { http, base: base.trim_end_matches('/').to_string(), creds, key })
    }

    /// The traded account: the `/info` user.
    pub fn account(&self) -> &str {
        &self.creds.account
    }

    pub fn signer(&self) -> &str {
        &self.creds.signer
    }

    pub async fn info(&self, body: Value) -> Result<Value> {
        info(&self.http, &self.base, body).await
    }

    /// `/info` for the traded account.
    pub async fn user_info(&self, kind: &str) -> Result<Value> {
        self.info(json!({"type": kind, "user": self.account()})).await
    }

    pub async fn asset(&self, coin: &str) -> Result<Asset> {
        asset_in(&self.info(json!({"type": "meta"})).await?, coin)
    }

    /// The best bid and ask.
    pub async fn top(&self, coin: &str) -> Result<(Decimal, Decimal)> {
        let book = self.info(json!({"type": "l2Book", "coin": coin})).await?;
        Ok((dec(&book["levels"][0][0]["px"]).context("l2Book bid")?, dec(&book["levels"][1][0]["px"]).context("l2Book ask")?))
    }

    pub async fn position(&self, coin: &str) -> Result<Decimal> {
        position_of(&self.user_info("clearinghouseState").await?, coin)
    }

    pub async fn open_orders(&self) -> Result<Vec<Value>> {
        self.user_info("openOrders").await?.as_array().cloned().context("openOrders is not a list")
    }

    /// `open`, `filled`, `canceled`, `rejected`, ..., or `unknownOid` (never accepted, or not
    /// yet: only past its `expiresAfter` does that prove it never will be).
    pub async fn order_status(&self, cloid: &str) -> Result<String> {
        let reply = self.info(json!({"type": "orderStatus", "user": self.account(), "oid": cloid})).await?;
        reply.pointer("/order/status").or(reply.get("status")).and_then(Value::as_str).map(str::to_owned)
            .with_context(|| format!("orderStatus without a status: {reply}"))
    }

    /// The account's fills since `start_ms`, oldest first.
    pub async fn fills_since(&self, start_ms: i64) -> Result<Vec<Value>> {
        let fills = self.info(json!({"type": "userFillsByTime", "user": self.account(), "startTime": start_ms})).await?;
        fills.as_array().cloned().context("userFillsByTime is not a list")
    }

    /// One limit order; the price is rounded as [`fmt_px`] with `up = buy`, the size down.
    #[allow(clippy::too_many_arguments)]
    pub async fn place(&self, asset: &Asset, buy: bool, px: Decimal, size: Decimal, tif: Tif, reduce_only: bool, cloid: &str) -> Placed {
        let tif = match tif { Tif::Alo => "Alo", Tif::Ioc => "Ioc" };
        let order = OrderWire {
            a: asset.index, b: buy, p: fmt_px(px, asset.sz_decimals, buy), s: fmt_sz(size, asset.sz_decimals),
            r: reduce_only, t: OrderType { limit: Limit { tif } }, c: cloid,
        };
        match self.exchange(&OrderAction { kind: "order", orders: [order], grouping: "na" }, true).await {
            Reply::Ok(response) => placed(&response),
            Reply::Refused(error) => Placed::Rejected(error),
            Reply::Unknown(error) => Placed::Unknown(error),
        }
    }

    pub async fn cancel(&self, asset: &Asset, cloid: &str) -> Result<()> {
        let action = CancelAction { kind: "cancelByCloid", cancels: [CancelWire { asset: asset.index, cloid }] };
        match self.exchange(&action, false).await {
            Reply::Ok(response) if response.pointer("/data/statuses/0") == Some(&json!("success")) => Ok(()),
            Reply::Ok(response) => bail!("cancel {cloid} refused: {response}"),
            Reply::Refused(error) => bail!("cancel {cloid} refused: {error}"),
            Reply::Unknown(error) => bail!("cancel {cloid} outcome unknown: {error}"),
        }
    }

    /// Signs and posts `action`; an order also carries `expiresAfter`.
    async fn exchange<A: Serialize>(&self, action: &A, expires: bool) -> Reply {
        let nonce = next_nonce();
        let expires_after = expires.then_some(nonce + ORDER_TTL_MS);
        let signature = match sign_action(&self.key, action, nonce, self.creds.vault.as_ref(), expires_after, self.base == MAINNET) {
            Ok(signature) => signature,
            Err(error) => return Reply::Refused(format!("{error:#}")),
        };
        let vault_address = self.creds.vault.is_some().then_some(self.account());
        let envelope = Envelope { action, nonce, signature, vault_address, expires_after };
        let response = match self.http.post(format!("{}/exchange", self.base)).json(&envelope).send().await {
            Ok(response) => response,
            Err(error) => return Reply::Unknown(format!("{error}")),
        };
        let status = response.status();
        if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
            return Reply::Refused("HTTP 429: rate limited".into());
        }
        let body: Value = match response.json().await {
            Ok(body) => body,
            Err(error) => return Reply::Unknown(format!("HTTP {status}: {error}")),
        };
        match (status.is_success(), status.is_server_error(), body["status"].as_str()) {
            (true, _, Some("ok")) => Reply::Ok(body["response"].clone()),
            (true, _, Some("err")) | (false, false, _) => Reply::Refused(format!("HTTP {status}: {}", body["response"])),
            _ => Reply::Unknown(format!("HTTP {status}: {body}")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn the_order_action_packs_and_hashes_as_the_python_sdk() {
        let order = OrderWire {
            a: 5, b: true, p: fmt_px(dec!(123.45), 1, true), s: fmt_sz(dec!(0.5), 1), r: false,
            t: OrderType { limit: Limit { tif: "Ioc" } }, c: "0x000102030405060708090a0b0c0d0e0f",
        };
        let packed = rmp_serde::to_vec_named(&OrderAction { kind: "order", orders: [order], grouping: "na" }).unwrap();
        assert_eq!(hex::encode(&packed), "83a474797065a56f72646572a66f72646572739187a16105a162c3a170a63132332e3435a173a3302e35a172c2a17481a56c696d697481a3746966a3496f63a163d92230783030303130323033303430353036303730383039306130623063306430653066a867726f7570696e67a26e61");
        let hash = |vault: Option<&[u8; 20]>, expires| hex::encode(super::super::sign::action_hash(&packed, 1_234_567, vault, expires));
        assert_eq!(hash(None, None), "dc76e4ee96d5ed33eae5fc25d0c3efd2ae108782b741627fd794be7b006eeba5");
        assert_eq!(hash(Some(&[0x11; 20]), None), "c9faf441009cc3eca3112b510bd4bde8b1643edddcdd869cabed0d86d5468f88");
        assert_eq!(hash(Some(&[0x11; 20]), Some(9_999_999)), "e82fb2a10383cb80986147f11cb09de79436a1dd8c976f3ab29bf64678b8836f");
    }

    #[test]
    fn prices_keep_five_significant_figures_and_the_decimals_the_size_leaves() {
        for (px, sz_decimals, up, wire) in [
            (dec!(123.456), 2, true, "123.46"),
            (dec!(123.456), 2, false, "123.45"),
            (dec!(45.12345), 2, true, "45.124"),
            (dec!(0.0123456), 0, false, "0.012345"),
            (dec!(0.0123456), 2, false, "0.0123"),
            (dec!(123456.7), 5, false, "123456"),
            (dec!(88.0), 2, true, "88"),
            (dec!(1.000005), 0, true, "1.0001"),
        ] {
            assert_eq!(fmt_px(px, sz_decimals, up), wire, "{px} at {sz_decimals}, up {up}");
        }
        assert_eq!((fmt_sz(dec!(0.129), 2), fmt_sz(dec!(88.0), 0)), ("0.12".into(), "88".into()));
    }

    #[test]
    fn order_replies_map_to_outcomes() {
        let reply = |statuses: Value| json!({"type": "order", "data": {"statuses": statuses}});
        assert_eq!(placed(&reply(json!([{"resting": {"oid": 77}}]))), Placed::Resting { oid: 77 });
        assert_eq!(placed(&reply(json!([{"filled": {"totalSz": "0.24", "avgPx": "44.51", "oid": 9}}]))),
            Placed::Filled { oid: 9, size: dec!(0.24), avg_px: dec!(44.51) });
        let no_match = "Order could not immediately match against any resting orders.";
        assert_eq!(placed(&reply(json!([{"error": no_match}]))), Placed::Rejected(no_match.into()));
        assert!(matches!(placed(&reply(json!([]))), Placed::Unknown(_)));
        assert!(matches!(placed(&reply(json!([{"resting": {"oid": 1}}, {"resting": {"oid": 2}}]))), Placed::Unknown(_)));
        assert!(matches!(placed(&reply(json!([{"waiting": {}}]))), Placed::Unknown(_)));
        let state = json!({"assetPositions": [{"position": {"coin": "HYPE", "szi": "-0.24"}}]});
        assert_eq!((position_of(&state, "HYPE").unwrap(), position_of(&state, "BTC").unwrap()), (dec!(-0.24), Decimal::ZERO));
        let (a, b) = (next_nonce(), next_nonce());
        assert!(b > a);
    }
}
