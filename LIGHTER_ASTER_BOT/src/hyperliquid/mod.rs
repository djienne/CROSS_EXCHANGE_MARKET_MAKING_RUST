//! Hyperliquid perps: action signing, the REST client and the `hl-*` probes.
//! The engines submit bounded IOCs; post-only placements are confined to probes.

pub mod client;
pub mod probe;
pub mod sign;
