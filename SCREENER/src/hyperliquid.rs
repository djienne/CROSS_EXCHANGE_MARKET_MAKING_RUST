//! Public native-perp frames. No SDK, account access or order endpoints.
use std::collections::{HashMap, HashSet};

use anyhow::{ensure, Result};
use serde::Deserialize;

use crate::collect::{Bbo, Event, Leg};

#[derive(Deserialize)]
struct Level { px: String, sz: String }

#[derive(Deserialize)]
#[serde(tag = "channel", content = "data")]
enum Frame {
    #[serde(rename = "bbo")]
    Bbo { coin: String, time: i64, bbo: [Option<Level>; 2] },
    #[serde(rename = "l2Book")]
    Book { coin: String, time: i64, levels: [Vec<Level>; 2] },
    #[serde(rename = "trades")]
    Trades(Vec<Print>),
    #[serde(rename = "subscriptionResponse", alias = "hubStatus")]
    Subscribed(serde::de::IgnoredAny),
    #[serde(other)]
    Other,
}

#[derive(Deserialize)]
struct Print { coin: String, time: i64, tid: u64, px: String, sz: String, side: String }

#[derive(Default)]
struct MarketState {
    first_book: i64,
    last_book: i64,
    received: i64,
    /// One minute is enough for within-session retransmits; reconnect history is filtered by
    /// the first book's exchange time. No growing lifetime trade-ID set.
    seen: HashSet<(i64, u64)>,
    newest_trade: i64,
}

#[derive(Default)]
pub struct Feed {
    markets: HashMap<usize, MarketState>,
    pub historical: u64,
    pub duplicates: u64,
}

impl Feed {
    pub fn reset(&mut self) { self.markets.clear(); }

    pub fn events(&mut self, text: &str, index: &HashMap<String, usize>, now: i64) -> Result<Vec<Event>> {
        let mut events = Vec::new();
        let book = match serde_json::from_str::<Frame>(text)? {
            Frame::Bbo { coin, time, bbo } => Some((coin, time, bbo)),
            Frame::Book { coin, time, levels: [bids, asks] } => Some((coin, time, [bids.into_iter().next(), asks.into_iter().next()])),
            Frame::Trades(trades) => {
                for t in trades {
                    let Some(&pair) = index.get(&t.coin) else { continue };
                    let Some(state) = self.markets.get_mut(&pair) else { self.historical += 1; continue };
                    // Initial recent-trade batches have no isSnapshot flag and can span frames.
                    // A fresh book supplies a venue-clock boundary, avoiding host clock skew.
                    if state.first_book == 0 || t.time <= state.first_book || t.time < state.newest_trade - 60_000 {
                        self.historical += 1; continue;
                    }
                    let price: f64 = t.px.parse()?;
                    let size: f64 = t.sz.parse()?;
                    ensure!(price.is_finite() && price > 0.0 && size.is_finite() && size > 0.0, "invalid Hyperliquid trade");
                    let buy = match t.side.as_str() { "B" => true, "A" => false, _ => anyhow::bail!("invalid Hyperliquid aggressor") };
                    if !state.seen.insert((t.time, t.tid)) { self.duplicates += 1; continue; }
                    if t.time > state.newest_trade {
                        state.newest_trade = t.time;
                        state.seen.retain(|(time, _)| *time >= t.time - 60_000);
                    }
                    events.push(Event::Trade { pair, venue: Leg::Left, price, size, buy, stamp: t.time });
                }
                None
            }
            Frame::Subscribed(_) | Frame::Other => None,
        };
        if let Some((coin, time, levels)) = book {
            let Some(&pair) = index.get(&coin) else { return Ok(events) };
            ensure!(time > 0, "missing Hyperliquid book time");
            let state = self.markets.entry(pair).or_default();
            if time < state.last_book { return Ok(events); }
            let bbo = match levels {
                [Some(b), Some(a)] => {
                    let bbo = Bbo { bid: b.px.parse()?, bid_size: b.sz.parse()?, ask: a.px.parse()?, ask_size: a.sz.parse()? };
                    if bbo.known() && bbo.bid_size.is_finite() && bbo.bid_size > 0.0 && bbo.ask_size.is_finite() && bbo.ask_size > 0.0 { bbo } else { Bbo::default() }
                }
                _ => Bbo::default(),
            };
            if !bbo.known() { state.first_book = 0; }
            else if state.first_book == 0 { state.first_book = time; }
            state.last_book = time;
            state.received = now;
            events.push(Event::Book { pair, venue: Leg::Left, bbo, stamp: time });
        }
        Ok(events)
    }

    /// Pongs prove socket health, not book health. Periodic L2 snapshots refresh quiet BBOs.
    pub fn expire(&mut self, now: i64) -> Vec<Event> {
        self.markets.iter_mut().filter_map(|(&pair, s)| {
            if s.received != 0 && now - s.received > 30_000 {
                s.received = 0;
                s.first_book = 0;
                Some(Event::Book { pair, venue: Leg::Left, bbo: Bbo::default(), stamp: 0 })
            } else { None }
        }).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshots_replays_duplicates_and_quiet_books() {
        let index = HashMap::from([("BTC".to_string(), 0)]);
        let mut f = Feed::default();
        assert!(f.events(r#"{"channel":"subscriptionResponse","data":{"method":"subscribe","subscription":{"type":"bbo","coin":"BTC"}}}"#, &index, 1000).unwrap().is_empty());
        let print = |time, tid| format!(r#"{{"channel":"trades","data":[{{"coin":"BTC","time":{time},"tid":{tid},"px":"100","sz":"2","side":"A"}}]}}"#);
        let book = |time| format!(r#"{{"channel":"bbo","data":{{"coin":"BTC","time":{time},"bbo":[{{"px":"99","sz":"2"}},{{"px":"101","sz":"3"}}]}}}}"#);
        assert!(f.events(&print(90, 1), &index, 1000).unwrap().is_empty());
        assert_eq!(f.events(&book(100), &index, 1000).unwrap().len(), 1);
        assert!(f.events(&print(99, 2), &index, 1001).unwrap().is_empty());
        assert!(f.events(&print(100, 3), &index, 1002).unwrap().is_empty());
        assert!(matches!(f.events(&print(101, 4), &index, 1003).unwrap()[0], Event::Trade { buy: false, .. }));
        assert!(f.events(&print(101, 4), &index, 1004).unwrap().is_empty());
        assert_eq!(f.events(&print(101, 5), &index, 1005).unwrap().len(), 1);
        assert!(f.events(&book(99), &index, 1006).unwrap().is_empty());
        let null = r#"{"channel":"bbo","data":{"coin":"BTC","time":102,"bbo":[null,null]}}"#;
        assert!(matches!(f.events(null, &index, 1007).unwrap()[0], Event::Book { bbo, .. } if !bbo.known()));
        let snapshot = r#"{"channel":"l2Book","data":{"coin":"BTC","time":103,"levels":[[{"px":"99","sz":"2"}],[{"px":"101","sz":"3"}]]}}"#;
        assert_eq!(f.events(snapshot, &index, 20_000).unwrap().len(), 1);
        assert!(f.expire(40_000).is_empty());
        f.events(r#"{"channel":"pong"}"#, &index, 50_000).unwrap();
        assert!(f.events(r#"{"channel":"hubStatus","data":{"registered":["BTC"]}}"#, &index, 50_000).unwrap().is_empty());
        assert_eq!(f.expire(50_001).len(), 1);
        f.reset();
        assert!(f.events(&print(101, 5), &index, 50_002).unwrap().is_empty());
        assert!(f.events(&book(200).replace("99", "NaN"), &index, 50_003).unwrap().iter().all(|e| matches!(e, Event::Book { bbo, .. } if !bbo.known())));
    }
}
