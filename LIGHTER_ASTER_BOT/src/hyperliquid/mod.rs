//! Hyperliquid perps: action signing, the REST client and the `hl-*` probes. The bot sends
//! only IOCs: the account's action budget is for life (10k + ~1 per USDC traded), and below
//! $1M of volume the venue refuses a dead-man switch (`scheduleCancel`).

pub mod client;
pub mod probe;
pub mod sign;
