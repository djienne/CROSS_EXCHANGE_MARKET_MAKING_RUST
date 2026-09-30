//! Execution economics from observed fills: same-venue realized closes plus the
//! spread on opposite remaining positions. This is not portfolio mark-to-market.
//! Raw trades cover cumulative progress before residual coverage is added; missing
//! fees/prices remain unknown. Logical obligations are paired before time filtering.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::io::{BufRead, BufReader, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use serde::Serialize;
use serde_json::Value;

use crate::types::Side;

#[derive(Debug, Clone, Serialize)]
pub struct TradeSummary {
    pub schema_version: u32,
    pub economic_status: &'static str,
    pub cloid: String,
    pub logical_id: String,
    pub attempt_ids: Vec<String>,
    pub market: String,
    pub timestamp_ms: Option<i64>,
    pub first_mono_ns: i64,
    pub last_mono_ns: i64,
    pub hedge_side: Option<Side>,
    /// Observed maker volume; correction volume is reported separately in aster_qty.
    pub qty: Decimal,
    pub aster_qty: Decimal,
    pub lighter_qty: Decimal,
    pub hedged_qty: Decimal,
    pub matched_qty: Decimal,
    pub residual_qty: Decimal,
    pub aster_residual_qty: Decimal,
    pub lighter_residual_qty: Decimal,
    pub aster_quote: Option<Decimal>,
    pub lighter_quote: Option<Decimal>,
    /// Volume-weighted execution prices, including corrections.
    pub aster_px: Option<Decimal>,
    pub lighter_px: Option<Decimal>,
    pub gross_pnl: Option<Decimal>,
    pub venue_realized_pnl_usdc: Option<Decimal>,
    pub execution_spread_usdc: Option<Decimal>,
    pub aster_fee: Option<Decimal>,
    pub lighter_fee: Option<Decimal>,
    pub fees: Option<Decimal>,
    pub known_fees: Decimal,
    pub net_pnl: Option<Decimal>,
    pub qty_mismatch: bool,
    pub terminal: bool,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct LiveReportSummary {
    pub schema_version: u32,
    pub economic_status: &'static str,
    pub trades: Vec<TradeSummary>,
    pub unmatched_fills: usize,
    pub unmatched_hedges: usize,
    pub qty_mismatches: usize,
    pub malformed_rows: usize,
    pub gross_pnl: Option<Decimal>,
    pub venue_realized_pnl_usdc: Option<Decimal>,
    pub execution_spread_usdc: Option<Decimal>,
    pub aster_fees: Option<Decimal>,
    pub lighter_fees: Option<Decimal>,
    pub net_pnl: Option<Decimal>,
    pub known_fees: Decimal,
    pub total_qty: Decimal,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Venue { Aster, Lighter }
impl Venue {
    fn parse(value: &str) -> Option<Self> {
        match value.to_ascii_lowercase().as_str() {
            "aster" => Some(Self::Aster),
            "lighter" | "hyperliquid" | "hl" => Some(Self::Lighter),
            _ => None,
        }
    }
    fn index(self) -> usize { if self == Self::Aster { 0 } else { 1 } }
    fn name(self) -> &'static str { if self == Self::Aster { "aster" } else { "lighter" } }
}

#[derive(Clone)]
struct Fill {
    venue: Venue,
    side: Side,
    qty: Decimal,
    quote: Option<Decimal>,
    fee: Option<Decimal>,
    timestamp_ms: Option<i64>,
    ordinal: usize,
    attempt: String,
    client_id: Option<String>,
    order_id: Option<String>,
    maker_origin: bool,
}

struct Progress {
    venue: Venue,
    side: Option<Side>,
    qty: Decimal,
    quote: Option<Decimal>,
    fee: Option<Decimal>,
    timestamp_ms: Option<i64>,
    ordinal: usize,
    client_id: Option<String>,
    order_id: Option<String>,
    maker_origin: bool,
    terminal: bool,
}

#[derive(Default)]
struct Group {
    fills: BTreeMap<String, Fill>,
    progress: BTreeMap<String, Progress>,
    unidentified: BTreeMap<String, Option<Decimal>>,
    legacy: bool,
    first_mono_ns: i64,
    last_mono_ns: i64,
}

#[derive(Default)]
struct Position {
    qty: Decimal,
    average: Decimal,
    realized: Decimal,
    volume: Decimal,
    quote: Decimal,
    fee: Decimal,
    price_unknown: bool,
    fee_unknown: bool,
}

fn amount(value: Option<&Value>) -> Option<Decimal> {
    match value? {
        Value::String(value) => value.parse().ok(),
        Value::Number(value) => value.to_string().parse().ok(),
        _ => None,
    }
}

fn identifier(value: &Value, key: &str) -> Option<String> {
    match value.get(key)? {
        Value::String(value) if !value.is_empty() => Some(value.clone()),
        Value::Number(value) => Some(value.to_string()),
        _ => None,
    }
}

fn side(value: Option<&Value>) -> Option<Side> {
    match value?.as_str()?.to_ascii_lowercase().as_str() {
        "buy" => Some(Side::Buy),
        "sell" => Some(Side::Sell),
        _ => None,
    }
}

fn timestamp_ms(row: &Value) -> Option<i64> {
    for object in row.get("detail").into_iter().chain(std::iter::once(row)) {
        for key in ["event_time_ms", "timestamp_ms", "ts_ms"] {
            if let Some(value) = amount(object.get(key)).and_then(|value| value.to_i64()).filter(|value| *value > 0) {
                if chrono::DateTime::from_timestamp_millis(value).is_some() { return Some(value); }
            }
        }
        for key in ["timestamp", "ts", "time", "created_at"] {
            let Some(value) = object.get(key).and_then(Value::as_str) else { continue; };
            if let Ok(value) = chrono::DateTime::parse_from_rfc3339(value) { return Some(value.timestamp_millis()); }
            for format in ["%Y-%m-%dT%H:%M:%S%.f", "%Y-%m-%d %H:%M:%S%.f"] {
                if let Ok(value) = chrono::NaiveDateTime::parse_from_str(value, format) {
                    return Some(value.and_utc().timestamp_millis());
                }
            }
        }
    }
    None
}

fn fee(detail: &Value, trusted: bool) -> Option<Decimal> {
    let notional = amount(detail.get("notional_usd").or_else(|| detail.get("usd_amount")));
    let ticks = amount(detail.get("fee_ticks"));
    if detail.get("fee_ticks").is_some_and(|value| !value.is_null()) {
        // An explicit null fee_usd next to a rate is the bot marking contradictory evidence.
        if !detail.get("maker").is_some_and(Value::is_boolean)
            || detail.get("fee_complete").and_then(Value::as_bool) == Some(false)
            || detail.get("fee_usd").is_some_and(Value::is_null) { return None; }
        return notional?.abs().checked_mul(ticks?)?.checked_div(Decimal::from(1_000_000));
    }
    if trusted && detail.get("fee_complete").and_then(Value::as_bool).unwrap_or(true) {
        amount(detail.get("fee_usd"))
    } else { None }
}

fn checked(value: Option<Decimal>) -> Result<Decimal> {
    value.context("execution amount exceeds the decimal range")
}

fn parse_fill(row: &Value, detail: &Value, kind: &str, logical: &str, ordinal: usize) -> Result<(String, Fill)> {
    let trusted = row.get("schema_version").and_then(Value::as_u64).unwrap_or(1) >= 2
        && row.get("economic_status").and_then(Value::as_str) == Some("confirmed");
    let (venue, fill_side, price, identity, attempt) = match kind {
        "maker_fill" => (
            Venue::Aster, side(detail.get("maker_side")), amount(detail.get("px")),
            format!("aster:{}:{}", identifier(detail, "order_id").unwrap_or_default(), identifier(detail, "trade_id").unwrap_or_default()),
            identifier(detail, "client_id").or_else(|| identifier(detail, "order_id")).unwrap_or_default(),
        ),
        "execution_trade" => {
            let venue = detail.get("venue").and_then(Value::as_str).and_then(Venue::parse).context("invalid execution venue")?;
            (venue, side(detail.get("side")), amount(detail.get("px")),
                format!("{}:{}:{}", venue.name(), identifier(detail, "order_id").unwrap_or_default(), identifier(detail, "trade_id").unwrap_or_default()),
                identifier(detail, "attempt_id").unwrap_or_default())
        }
        "fill" => (
            Venue::Aster, side(detail.get("side")).map(Side::opposite), amount(detail.get("avg_aster_px")),
            format!("legacy-maker:{logical}"), logical.to_string(),
        ),
        _ => (
            Venue::Lighter, side(detail.get("side")), amount(detail.get("px")),
            format!("legacy-hedge:{logical}:{}", identifier(detail, "trade_id")
                .or_else(|| identifier(row, "mono_ns")).unwrap_or_else(|| ordinal.to_string())),
            logical.to_string(),
        ),
    };
    let qty = amount(detail.get("qty")).filter(|value| *value >= Decimal::ZERO).context("invalid fill quantity")?;
    let quote = amount(detail.get("notional_usd")).or_else(|| price.filter(|p| *p > Decimal::ZERO).and_then(|p| qty.checked_mul(p)));
    if quote.is_some_and(|value| value < Decimal::ZERO || (qty > Decimal::ZERO && value.is_zero())) {
        anyhow::bail!("invalid fill quote amount");
    }
    Ok((identity, Fill {
        venue, side: fill_side.context("invalid fill side")?, qty, quote, fee: fee(detail, trusted),
        timestamp_ms: timestamp_ms(row), ordinal, attempt,
        client_id: identifier(detail, "client_id"), order_id: identifier(detail, "order_id"),
        maker_origin: kind == "fill" || (kind == "maker_fill"
            && detail.get("reduce_only").and_then(Value::as_bool) != Some(true)),
    }))
}

fn apply(position: &mut Position, fill: &Fill) -> Result<()> {
    position.volume = checked(position.volume.checked_add(fill.qty))?;
    position.quote = checked(position.quote.checked_add(fill.quote.unwrap_or(Decimal::ZERO)))?;
    match fill.fee {
        Some(fee) => position.fee = checked(position.fee.checked_add(fee))?,
        None => position.fee_unknown = true,
    }
    let delta = if fill.side == Side::Buy { fill.qty } else { -fill.qty };
    let old = position.qty;
    position.qty = checked(old.checked_add(delta))?;
    if fill.qty.is_zero() { return Ok(()); }
    let Some(quote) = fill.quote else { position.price_unknown = true; return Ok(()); };
    let price = checked(quote.checked_div(fill.qty))?;
    if old.is_zero() || old.is_sign_positive() == delta.is_sign_positive() {
        let previous = checked(old.abs().checked_mul(position.average))?;
        position.average = checked(checked(previous.checked_add(quote))?.checked_div(checked(old.abs().checked_add(fill.qty))?))?;
    } else {
        let closed = old.abs().min(fill.qty);
        let change = checked(price.checked_sub(position.average))?;
        let pnl = checked(closed.checked_mul(change))?;
        position.realized = checked(position.realized.checked_add(if old > Decimal::ZERO { pnl } else { -pnl }))?;
        if position.qty.is_zero() {
            position.average = Decimal::ZERO;
        } else if position.qty.is_sign_positive() != old.is_sign_positive() {
            position.average = price;
        }
    }
    Ok(())
}

fn coverage(group: &Group, malformed: &mut usize) -> Result<Vec<Fill>> {
    let mut fills: Vec<_> = group.fills.values().cloned().collect();
    for (attempt, progress) in &group.progress {
        let covered: Vec<_> = fills.iter().filter(|fill| fill.venue == progress.venue && (
            fill.attempt == *attempt
            || (progress.client_id.is_some() && fill.client_id == progress.client_id)
            || (progress.order_id.is_some() && fill.order_id == progress.order_id)
        )).collect();
        let raw_qty = covered.iter().try_fold(Decimal::ZERO, |total, fill| checked(total.checked_add(fill.qty)))?;
        if raw_qty >= progress.qty { continue; }
        let sum = |field: fn(&Fill) -> Option<Decimal>| -> Option<Decimal> {
            covered.iter().try_fold(Decimal::ZERO, |total, fill| total.checked_add(field(fill)?))
        };
        let quote = progress.quote.and_then(|total| total.checked_sub(sum(|fill| fill.quote)?));
        let fee = progress.fee.and_then(|total| total.checked_sub(sum(|fill| fill.fee)?));
        let qty = checked(progress.qty.checked_sub(raw_qty))?;
        let Some(fill_side) = progress.side else { *malformed += 1; continue; };
        if quote.is_some_and(|quote| quote <= Decimal::ZERO) { *malformed += 1; continue; }
        fills.push(Fill {
            venue: progress.venue, side: fill_side, qty, quote, fee,
            timestamp_ms: progress.timestamp_ms, ordinal: progress.ordinal, attempt: attempt.clone(),
            client_id: progress.client_id.clone(), order_id: progress.order_id.clone(),
            maker_origin: progress.maker_origin,
        });
    }
    fills.sort_by_key(|fill| (fill.timestamp_ms.unwrap_or(i64::MIN), fill.ordinal));
    Ok(fills)
}

fn calculate(market: String, logical: String, group: &Group, fills: &[Fill]) -> Result<TradeSummary> {
    let mut positions = [Position::default(), Position::default()];
    for fill in fills { apply(&mut positions[fill.venue.index()], fill)?; }
    let [aster, lighter] = positions;
    let opposite = !aster.qty.is_zero() && !lighter.qty.is_zero()
        && aster.qty.is_sign_positive() != lighter.qty.is_sign_positive();
    let matched = if opposite { aster.qty.abs().min(lighter.qty.abs()) } else { Decimal::ZERO };
    let realized = checked(aster.realized.checked_add(lighter.realized))?;
    let spread = if opposite {
        let price_difference = checked(lighter.average.checked_sub(aster.average))?;
        let matched_pnl = checked(matched.checked_mul(price_difference))?;
        if aster.qty > Decimal::ZERO { matched_pnl } else { -matched_pnl }
    } else { Decimal::ZERO };
    let prices_known = !aster.price_unknown && !lighter.price_unknown;
    let gross = prices_known.then_some(checked(realized.checked_add(spread))?);
    let known_fees = checked(aster.fee.checked_add(lighter.fee))?;
    let fees = (!aster.fee_unknown && !lighter.fee_unknown).then_some(known_fees);
    let mut net = gross.zip(fees).and_then(|(gross, fees)| gross.checked_sub(fees));
    let unidentified = group.unidentified.iter().any(|(attempt, lower)| {
        !group.progress.get(attempt).is_some_and(|progress|
            progress.terminal && lower.is_some_and(|lower| progress.qty >= lower))
    });
    if unidentified { net = None; }
    let status = if unidentified { "incomplete" } else if net.is_some() { "confirmed" } else if group.legacy { "legacy_unverified" } else { "incomplete" };
    let maker_qty = fills.iter().filter(|fill| fill.maker_origin)
        .try_fold(Decimal::ZERO, |total, fill| checked(total.checked_add(fill.qty)))?;
    let residual = checked(aster.qty.checked_add(lighter.qty))?;
    let attempts: BTreeSet<_> = fills.iter().map(|fill| fill.attempt.clone())
        .chain(group.progress.keys().cloned()).filter(|id| !id.is_empty()).collect();
    Ok(TradeSummary {
        schema_version: 2, economic_status: status, cloid: logical.clone(), logical_id: logical,
        attempt_ids: attempts.into_iter().collect(), market,
        timestamp_ms: fills.iter().filter_map(|fill| fill.timestamp_ms).max(),
        first_mono_ns: group.first_mono_ns, last_mono_ns: group.last_mono_ns,
        hedge_side: fills.iter().find(|fill| fill.venue == Venue::Aster).map(|fill| fill.side.opposite()),
        qty: maker_qty, aster_qty: aster.volume, lighter_qty: lighter.volume, hedged_qty: lighter.volume,
        matched_qty: matched, residual_qty: residual, aster_residual_qty: aster.qty, lighter_residual_qty: lighter.qty,
        aster_quote: (!aster.price_unknown).then_some(aster.quote),
        lighter_quote: (!lighter.price_unknown).then_some(lighter.quote),
        aster_px: (!aster.price_unknown && aster.volume > Decimal::ZERO).then(|| aster.quote / aster.volume),
        lighter_px: (!lighter.price_unknown && lighter.volume > Decimal::ZERO).then(|| lighter.quote / lighter.volume),
        gross_pnl: gross, venue_realized_pnl_usdc: prices_known.then_some(realized),
        execution_spread_usdc: prices_known.then_some(spread), aster_fee: (!aster.fee_unknown).then_some(aster.fee),
        lighter_fee: (!lighter.fee_unknown).then_some(lighter.fee), fees, known_fees, net_pnl: net,
        qty_mismatch: !residual.is_zero(),
        terminal: !group.progress.is_empty() && group.progress.values().all(|progress| progress.terminal),
    })
}

/// The XEMM journal of a run's file stem: `runs/bot-HYPE` → `runs/bot-HYPE-journal.jsonl`.
pub fn inferred_journal_path(stem: &Path) -> PathBuf {
    let name = stem.file_name().and_then(|s| s.to_str()).unwrap_or("livebot");
    let directory = stem.parent().filter(|p| !p.as_os_str().is_empty()).unwrap_or_else(|| Path::new("runs"));
    directory.join(format!("{name}-journal.jsonl"))
}

pub fn summarize_path(path: &Path, market: Option<&str>, since_ms: Option<i64>) -> Result<LiveReportSummary> {
    summarize(BufReader::new(File::open(path).with_context(|| format!("opening {}", path.display()))?), market, since_ms)
}

fn summarize<R: BufRead>(reader: R, market: Option<&str>, since_ms: Option<i64>) -> Result<LiveReportSummary> {
    let mut groups = Groups::new(market);
    for line in reader.lines() {
        let line = line.with_context(|| format!("reading journal line {}", groups.lines + 1))?;
        groups.feed(&line);
    }
    groups.finish(since_ms)
}

/// A journal summarized as it grows: each call reads only the complete lines appended since the
/// last. A shrunken file was rotated or truncated, and is read again from the start.
pub struct JournalTail {
    groups: Groups,
    offset: u64,
}

impl JournalTail {
    pub fn new(market: Option<&str>) -> Self {
        Self { groups: Groups::new(market), offset: 0 }
    }

    pub fn summarize(&mut self, path: &Path, since_ms: Option<i64>) -> Result<LiveReportSummary> {
        let mut file = File::open(path).with_context(|| format!("opening {}", path.display()))?;
        if file.metadata()?.len() < self.offset {
            *self = Self::new(self.groups.market.as_deref());
        }
        file.seek(SeekFrom::Start(self.offset))?;
        let mut reader = BufReader::new(file);
        let mut line = String::new();
        loop {
            line.clear();
            let read = reader.read_line(&mut line)
                .with_context(|| format!("reading journal line {}", self.groups.lines + 1))?;
            if read == 0 || !line.ends_with('\n') {
                break; // a partial trailing line waits for the writer to finish it
            }
            self.offset += read as u64;
            self.groups.feed(&line);
        }
        self.groups.finish(since_ms)
    }
}

/// Journal rows folded into logical execution groups, in file order.
struct Groups {
    market: Option<String>,
    groups: BTreeMap<(String, String), Group>,
    malformed: usize,
    lines: usize,
}

impl Groups {
    fn new(market: Option<&str>) -> Self {
        Self { market: market.map(str::to_owned), groups: BTreeMap::new(), malformed: 0, lines: 0 }
    }

    fn feed(&mut self, line: &str) {
        let index = self.lines;
        self.lines += 1;
        if line.trim().is_empty() { return; }
        let row: Value = match serde_json::from_str(line) { Ok(row) => row, Err(_) => { self.malformed += 1; return; } };
        let Some(market) = row.get("market").and_then(Value::as_str) else { return; };
        if self.market.as_deref().is_some_and(|wanted| wanted != market) { return; }
        let kind = row.get("kind").and_then(Value::as_str).unwrap_or_default();
        if !matches!(kind, "maker_fill" | "execution_trade" | "execution_progress" | "maker_order_progress" | "fill" | "hedge_fill") { return; }
        let Some(detail) = row.get("detail").filter(|detail| detail.is_object()) else { self.malformed += 1; return; };
        let Some(logical) = identifier(detail, "logical_id").or_else(|| identifier(detail, "cloid")) else { self.malformed += 1; return; };
        let group = self.groups.entry((market.to_string(), logical.clone())).or_default();
        group.legacy |= row.get("schema_version").and_then(Value::as_u64).unwrap_or(1) < 2;
        if let Some(mono) = row.get("mono_ns").and_then(Value::as_i64).filter(|value| *value > 0) {
            if group.first_mono_ns == 0 { group.first_mono_ns = mono; }
            group.first_mono_ns = group.first_mono_ns.min(mono);
            group.last_mono_ns = group.last_mono_ns.max(mono);
        }
        if matches!(kind, "execution_progress" | "maker_order_progress") {
            let attempt = identifier(detail, "attempt_id").or_else(|| identifier(detail, "client_id"));
            let qty = amount(detail.get("cumulative_qty")).filter(|qty| *qty >= Decimal::ZERO);
            let venue = detail.get("venue").and_then(Value::as_str).unwrap_or("aster");
            let (Some(attempt), Some(qty), Some(venue)) = (attempt, qty, Venue::parse(venue)) else { self.malformed += 1; return; };
            if group.progress.get(&attempt).is_none_or(|old| qty >= old.qty) {
                let (economic_time, economic_ordinal) = group.progress.get(&attempt)
                    .filter(|old| old.qty == qty)
                    .map(|old| (old.timestamp_ms, old.ordinal))
                    .unwrap_or((timestamp_ms(&row), index));
                group.progress.insert(attempt, Progress {
                    venue, side: side(detail.get("side")), qty, quote: amount(detail.get("cumulative_quote_usd")),
                    fee: detail.get("fee_complete").and_then(Value::as_bool).unwrap_or(false)
                        .then(|| amount(detail.get("cumulative_fee_usd"))).flatten(),
                    timestamp_ms: economic_time, ordinal: economic_ordinal,
                    client_id: identifier(detail, "client_id"), order_id: identifier(detail, "venue_order_id"),
                    maker_origin: kind == "maker_order_progress",
                    terminal: detail.get("terminal").and_then(Value::as_bool).unwrap_or(false),
                });
            }
        } else {
            if kind == "execution_trade" && detail.get("identity_complete").and_then(Value::as_bool) == Some(false) {
                let attempt = identifier(detail, "attempt_id").unwrap_or_default();
                let lower = amount(detail.get("qty")).filter(|qty| *qty >= Decimal::ZERO);
                group.unidentified.entry(attempt).and_modify(|old| {
                    *old = old.zip(lower).map(|(old, next)| old.max(next));
                }).or_insert(lower);
                return;
            }
            match parse_fill(&row, detail, kind, &logical, index) {
                Ok((identity, mut fill)) => {
                    if let Some(old) = group.fills.get(&identity) { fill.ordinal = old.ordinal; }
                    group.fills.insert(identity, fill);
                }
                Err(_) => self.malformed += 1,
            }
        }
    }

    /// Prices every group; the fed rows stay, so later lines can extend them.
    fn finish(&self, since_ms: Option<i64>) -> Result<LiveReportSummary> {
        price(&self.groups, self.malformed, since_ms)
    }
}

fn price(groups: &BTreeMap<(String, String), Group>, mut malformed: usize, since_ms: Option<i64>) -> Result<LiveReportSummary> {
    let mut output = LiveReportSummary {
        schema_version: 2, economic_status: "confirmed", gross_pnl: Some(Decimal::ZERO),
        venue_realized_pnl_usdc: Some(Decimal::ZERO), execution_spread_usdc: Some(Decimal::ZERO),
        aster_fees: Some(Decimal::ZERO), lighter_fees: Some(Decimal::ZERO), net_pnl: Some(Decimal::ZERO),
        ..LiveReportSummary::default()
    };
    let sum = |a: Option<Decimal>, b: Option<Decimal>| a.zip(b).and_then(|(a,b)| a.checked_add(b));
    for ((market, logical), group) in groups {
        let fills = coverage(group, &mut malformed)?;
        if !fills.iter().any(|fill| fill.qty > Decimal::ZERO) { continue; }
        let trade = calculate(market.clone(), logical.clone(), group, &fills)?;
        if since_ms.is_some_and(|since| trade.timestamp_ms.is_none_or(|time| time < since)) { continue; }
        output.unmatched_fills += usize::from(trade.qty > Decimal::ZERO && trade.lighter_qty.is_zero() && !trade.residual_qty.is_zero());
        output.unmatched_hedges += usize::from(trade.qty.is_zero() && trade.lighter_qty > Decimal::ZERO);
        output.qty_mismatches += usize::from(trade.qty_mismatch);
        output.gross_pnl = sum(output.gross_pnl, trade.gross_pnl);
        output.venue_realized_pnl_usdc = sum(output.venue_realized_pnl_usdc, trade.venue_realized_pnl_usdc);
        output.execution_spread_usdc = sum(output.execution_spread_usdc, trade.execution_spread_usdc);
        output.aster_fees = sum(output.aster_fees, trade.aster_fee);
        output.lighter_fees = sum(output.lighter_fees, trade.lighter_fee);
        output.net_pnl = sum(output.net_pnl, trade.net_pnl);
        output.known_fees = checked(output.known_fees.checked_add(trade.known_fees))?;
        output.total_qty = checked(output.total_qty.checked_add(trade.qty))?;
        output.trades.push(trade);
    }
    output.trades.sort_by_key(|trade| (trade.timestamp_ms, trade.last_mono_ns));
    output.malformed_rows = malformed;
    if malformed > 0 {
        output.net_pnl = None;
        output.economic_status = "incomplete";
    } else if output.trades.iter().any(|trade| trade.economic_status == "incomplete") {
        output.economic_status = "incomplete";
    } else if output.trades.iter().any(|trade| trade.economic_status == "legacy_unverified") {
        output.economic_status = "legacy_unverified";
    }
    Ok(output)
}

fn display(value: Option<Decimal>) -> String {
    value.map(|value| value.round_dp(6).normalize().to_string()).unwrap_or_else(|| "unknown".into())
}

pub fn print_summary(path: &Path, summary: &LiveReportSummary, details: bool) {
    println!("live-report: {}", path.display());
    println!("Economics: same-venue closes + matched entry spreads - observed fees.");
    println!("execution groups: {} ({})", summary.trades.len(), summary.economic_status);
    println!("unmatched makers/hedges: {}/{}; residual groups: {}; malformed rows: {}",
        summary.unmatched_fills, summary.unmatched_hedges, summary.qty_mismatches, summary.malformed_rows);
    println!("maker qty: {}", summary.total_qty.normalize());
    println!("venue realized: {}; matched entry spread: {} USD",
        display(summary.venue_realized_pnl_usdc), display(summary.execution_spread_usdc));
    println!("gross pnl: {}; Aster fees: {}; Lighter fees: {}; net pnl: {} USD",
        display(summary.gross_pnl), display(summary.aster_fees), display(summary.lighter_fees), display(summary.net_pnl));
    if details {
        for trade in &summary.trades {
            println!("{} {} matched={} residual={} gross={} net={} {}",
                trade.market, trade.logical_id, trade.matched_qty.normalize(), trade.residual_qty.normalize(),
                display(trade.gross_pnl), display(trade.net_pnl), trade.economic_status);
        }
    }
}

pub fn print_summary_json(path: &Path, summary: &LiveReportSummary) -> Result<()> {
    println!("{}", serde_json::to_string_pretty(&serde_json::json!({
        "journal_path": path.to_string_lossy(), "summary": summary
    }))?);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn rows_text(rows: &[Value]) -> String {
        rows.iter().map(|row| serde_json::to_string(row).unwrap()).collect::<Vec<_>>().join("\n")
    }

    #[test]
    fn malformed_selected_fee_rate_cannot_fall_back_to_a_derived_amount() {
        let native = serde_json::json!({"notional_usd":"1000","fee_ticks":"280","maker":false,"fee_usd":"0.00028"});
        assert_eq!(fee(&native, false), Some("0.28".parse().unwrap()));
        for replacement in [serde_json::json!({"fee_ticks":"NaN"}), serde_json::json!({"maker":null}),
            serde_json::json!({"notional_usd":null}), serde_json::json!({"fee_complete":false}),
            serde_json::json!({"fee_usd":null})] {
            let mut invalid = native.clone();
            invalid.as_object_mut().unwrap().extend(replacement.as_object().unwrap().clone());
            assert_eq!(fee(&invalid, true), None, "{invalid}");
        }
        let optional = serde_json::json!({"fee_ticks":null,"fee_usd":"0.5"});
        assert_eq!(fee(&optional, true), Some("0.5".parse().unwrap()));
    }

    #[test]
    fn shared_hand_calculated_execution_fixtures() {
        let fixtures: Vec<Value> = serde_json::from_str(include_str!("../../tests/fixtures/execution_economics.json")).unwrap();
        for fixture in fixtures {
            let rows = fixture["rows"].as_array().unwrap();
            let report = summarize(Cursor::new(rows_text(rows)), Some("HYPE"), None).unwrap();
            assert_eq!(report.trades.len(), 1, "{}", fixture["name"]);
            let trade = &report.trades[0];
            let expected = &fixture["expected"];
            assert_eq!(trade.qty, amount(expected.get("qty")).unwrap(), "{}", fixture["name"]);
            assert_eq!(trade.gross_pnl, amount(expected.get("gross_pnl_usdc")), "{}", fixture["name"]);
            for (key, actual) in [("execution_spread_usdc", trade.execution_spread_usdc),
                ("venue_realized_pnl_usdc", trade.venue_realized_pnl_usdc)] {
                if expected.get(key).is_some() { assert_eq!(actual, amount(expected.get(key)), "{} {key}", fixture["name"]); }
            }
            assert_eq!(trade.fees, amount(expected.get("fees_usdc")), "{}", fixture["name"]);
            assert_eq!(trade.net_pnl, amount(expected.get("net_pnl_usdc")), "{}", fixture["name"]);
            assert_eq!(trade.matched_qty, amount(expected.get("matched_qty")).unwrap(), "{}", fixture["name"]);
            assert_eq!(trade.residual_qty, amount(expected.get("residual_qty")).unwrap(), "{}", fixture["name"]);
            assert_eq!(trade.economic_status, expected["economic_status"].as_str().unwrap(), "{}", fixture["name"]);
        }
    }


    #[test]
    fn unchanged_cumulative_refinement_does_not_move_the_economic_window() {
        let fixtures: Vec<Value> = serde_json::from_str(include_str!("../../tests/fixtures/execution_economics.json")).unwrap();
        let case = fixtures.iter().find(|case| case["name"] == "cumulative_only").unwrap();
        let mut rows = case["rows"].as_array().unwrap().clone();
        let mut later = rows.last().unwrap().clone();
        later["ts_ms"] = serde_json::json!(1767312010000i64);
        rows.push(later);
        let text = rows_text(&rows);
        let all = summarize(Cursor::new(&text), None, None).unwrap();
        assert_eq!(all.trades[0].timestamp_ms, Some(1767312000003));
        let recent = summarize(Cursor::new(&text), None, Some(1767312005000)).unwrap();
        assert!(recent.trades.is_empty());
    }

    #[test]
    fn windowing_happens_after_pairing_and_unknown_fees_remain_null() {
        let fixtures: Vec<Value> = serde_json::from_str(include_str!("../../tests/fixtures/execution_economics.json")).unwrap();
        let partial = &fixtures[0];
        let rows = partial["rows"].as_array().unwrap();
        let report = summarize(Cursor::new(rows_text(rows)), None, Some(1767312000001)).unwrap();
        assert_eq!(report.trades.len(), 1);
        assert_eq!(report.trades[0].qty, "0.20".parse::<Decimal>().unwrap());
        assert_eq!(report.net_pnl, Some("0.11".parse::<Decimal>().unwrap()));
        let legacy = fixtures.iter().find(|fixture| fixture["name"] == "legacy_unverified").unwrap();
        let report = summarize(Cursor::new(rows_text(legacy["rows"].as_array().unwrap())), None, None).unwrap();
        assert!(serde_json::to_value(&report).unwrap()["net_pnl"].is_null());
        let malformed = rows_text(rows) + "\nnot json";
        let report = summarize(Cursor::new(malformed), None, None).unwrap();
        assert_eq!(report.malformed_rows, 1);
        assert!(report.net_pnl.is_none());
    }

    #[test]
    fn a_journal_read_in_chunks_summarizes_as_the_whole_file() {
        let fixtures: Vec<Value> = serde_json::from_str(include_str!("../../tests/fixtures/execution_economics.json")).unwrap();
        let mut rows = Vec::new();
        for (i, fixture) in fixtures.iter().enumerate() {
            for row in fixture["rows"].as_array().unwrap() {
                let mut row = row.clone();
                let key = if row["detail"].get("logical_id").is_some() { "logical_id" } else { "cloid" };
                row["detail"][key] = serde_json::json!(format!("t{i}"));
                rows.push(row);
            }
        }
        let text = rows_text(&rows) + "\nnot json\n";
        let whole = |text: &str| serde_json::to_value(summarize(Cursor::new(text), Some("HYPE"), None).unwrap()).unwrap();
        let path = std::env::temp_dir().join(format!("journal_tail_test_{}.jsonl", std::process::id()));
        let mut tail = JournalTail::new(Some("HYPE"));
        let cut = text.len() / 2;
        assert!(!text[..cut].ends_with('\n'), "the first chunk ends mid-line");
        std::fs::write(&path, &text[..cut]).unwrap();
        tail.summarize(&path, None).unwrap();
        std::fs::write(&path, &text).unwrap();
        assert_eq!(serde_json::to_value(tail.summarize(&path, None).unwrap()).unwrap(), whole(&text));
        // A shorter file was rotated: it is read again from the start.
        let rotated = rows_text(&rows[..3]) + "\n";
        std::fs::write(&path, &rotated).unwrap();
        assert_eq!(serde_json::to_value(tail.summarize(&path, None).unwrap()).unwrap(), whole(&rotated));
        let _ = std::fs::remove_file(&path);
    }
}
