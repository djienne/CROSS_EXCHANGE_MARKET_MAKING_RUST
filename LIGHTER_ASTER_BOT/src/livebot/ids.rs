//! Deterministic client IDs: every order carries an id we can recompute and query by, so
//! an attempt whose outcome is unknown can ask the venue "did this fill?".
//!
//! - **Aster maker client id**: `X{session}-{market}-{B|S}-{epoch}` — unique per quote,
//!   kept inside Aster's `newClientOrderId` charset/length budget (Binance-style
//!   `^[A-Za-z0-9_:/.\-]{1,36}$`). Maker ids need not survive a restart (startup cancels
//!   all Aster orders), only be unique within a session.
//! - **Lighter hedge cloid** (a Hyperliquid-era name): a 128-bit id per hedge attempt, from
//!   `(session, market, attempt epoch)`, whose `client_order_index` finds the order in
//!   Lighter's history. It does not survive a restart, and needs not: live refuses to start
//!   after an unclean session until it is reviewed, and within a session the fill ledger
//!   keeps a fill from being hedged twice.
//!
//! Hashing is a tiny inline FNV-1a (no new dependency, and stable across toolchains —
//! `std`'s `DefaultHasher` is explicitly NOT stable, so it must not be used here).

use crate::types::{MarketId, Side};

/// 64-bit FNV-1a over bytes with a caller-chosen offset basis (varying the basis gives an
/// independent hash for packing >64 bits). Stable forever by construction.
fn fnv1a64(bytes: &[u8], mut hash: u64) -> u64 {
    const PRIME: u64 = 0x0000_0100_0000_01b3;
    for &b in bytes {
        hash ^= b as u64;
        hash = hash.wrapping_mul(PRIME);
    }
    hash
}

const FNV_BASIS_A: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_BASIS_B: u64 = 0x1099_5163_2d4b_c7e1; // a distinct basis for the high 64 bits
pub const LIGHTER_MAX_CLIENT_ORDER_INDEX: i64 = 281_474_976_710_655; // 2^48 - 1

/// A short per-process session tag for maker order ids. Derived from a UUID so it is
/// unique per run; truncated to keep order ids short.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionId(String);

impl SessionId {
    /// A fresh 6-char base36 session tag from a v4 UUID's low bits.
    pub fn random() -> Self {
        let u = uuid::Uuid::new_v4();
        let n = u128::from_le_bytes(*u.as_bytes()) as u64;
        SessionId(base36(n, 6))
    }
    /// Construct from an explicit tag (tests). Sanitized to the
    /// allowed charset and clamped to 6 chars.
    #[cfg(test)]
    pub fn from_tag(tag: &str) -> Self {
        let s: String = tag.chars().filter(|c| c.is_ascii_alphanumeric()).take(6).collect();
        SessionId(if s.is_empty() { "0".into() } else { s.to_ascii_lowercase() })
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// A compact market code for an order id: uppercase alphanumerics, ≤ 7 chars. Market ids
/// are already short symbols ("BTC", "TRUMP"), so this is just a defensive clamp.
fn market_code(market: &MarketId) -> String {
    market
        .0
        .chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .take(7)
        .collect::<String>()
        .to_ascii_uppercase()
}

/// Session-prefixed client id for a FLATTEN (reduce-only close) order. Carries the same
/// `X{session}-` prefix as maker ids so `OrderManager::is_own_client_id` attributes the
/// resulting reduce-only fills to this session — without it the venue assigns a foreign-
/// looking id and the strategy drops its own flatten fills ("non-bot Aster fill").
pub fn aster_flatten_client_id(session: &SessionId, market: &MarketId, epoch: u64) -> String {
    let s = format!(
        "X{}-{}-F-{}",
        session.as_str(),
        market_code(market),
        base36(epoch, 0),
    );
    if s.len() > 36 {
        return s[..36].to_string();
    }
    s
}

/// Aster maker `newClientOrderId`. `quote_epoch` is a per-(market,side) monotonic counter
/// the order-state layer increments on each new quote, guaranteeing uniqueness. Form:
/// `X{session}-{MARKET}-{B|S}-{epoch36}` — always within the 36-char / charset budget.
pub fn aster_client_id(session: &SessionId, market: &MarketId, side: Side, quote_epoch: u64) -> String {
    let s = format!(
        "X{}-{}-{}-{}",
        session.as_str(),
        market_code(market),
        match side {
            Side::Buy => "B",
            Side::Sell => "S",
        },
        base36(quote_epoch, 0),
    );
    // Defensive: never exceed Aster's 36-char client-id cap even with pathological inputs.
    if s.len() > 36 {
        s[..36].to_string()
    } else {
        s
    }
}

/// A 128-bit hedge id (`cloid`): a deterministic hash of the `XEMM-HEDGE-...` fields. Lighter
/// orders carry it as a 48-bit `client_order_index` ([`Cloid::to_lighter_client_order_index`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Cloid([u8; 16]);

impl Cloid {
    /// Deterministic hedge-attempt cloid: SAME inputs ⇒ SAME cloid. The caller passes the
    /// session, the market and a per-session attempt epoch, so each attempt is unique.
    pub fn hedge(session: &str, market: &str, epoch: i64) -> Self {
        let key = format!("XEMM-HEDGE-{session}-{market}-{epoch}");
        let lo = fnv1a64(key.as_bytes(), FNV_BASIS_A);
        let hi = fnv1a64(key.as_bytes(), FNV_BASIS_B);
        let mut b = [0u8; 16];
        b[..8].copy_from_slice(&hi.to_be_bytes());
        b[8..].copy_from_slice(&lo.to_be_bytes());
        Cloid(b)
    }

    /// Deterministic RECOVERY cloid from a market + scaled net delta. Same inputs ⇒ same
    /// cloid.
    ///
    /// IMPORTANT: the venue provides NO dedupe — Lighter keys orders on the derived
    /// `client_order_index` and happily accepts a reused one, which would cross-attribute
    /// fills in the FillTracker. Distinct from [`Cloid::hedge`] (the `RECOVER` tag changes
    /// the hash).
    pub fn recovery(market: &MarketId, net_scaled: i64) -> Self {
        let key = format!("XEMM-RECOVER-{}-{net_scaled}", market.0);
        Self::from_key(&key)
    }

    fn from_key(key: &str) -> Self {
        let lo = fnv1a64(key.as_bytes(), FNV_BASIS_A);
        let hi = fnv1a64(key.as_bytes(), FNV_BASIS_B);
        let mut b = [0u8; 16];
        b[..8].copy_from_slice(&hi.to_be_bytes());
        b[8..].copy_from_slice(&lo.to_be_bytes());
        Cloid(b)
    }

    /// Hex form (journal/log identity): `0x` followed by 32 lowercase hex digits. Single allocation
    /// (`hex::encode`) — this is on the fill→hedge hot path, so avoid the per-byte `format!` loop.
    pub fn to_hex(self) -> String {
        format!("0x{}", hex::encode(self.0))
    }

    #[cfg(test)]
    pub(crate) fn from_bytes_for_lighter(bytes: [u8; 16]) -> Self {
        Cloid(bytes)
    }

    /// Lighter wire form: a positive integer `client_order_index` in `[1, 2^48 - 1]`.
    /// The mapping is deterministic from the 128-bit id; zero
    /// is remapped to one because several exchange/client paths treat `0` as unset.
    pub fn to_lighter_client_order_index(self) -> i64 {
        let mut lo = [0u8; 8];
        lo.copy_from_slice(&self.0[8..]);
        let raw = u64::from_be_bytes(lo) % (LIGHTER_MAX_CLIENT_ORDER_INDEX as u64);
        let idx = raw as i64;
        if idx <= 0 { 1 } else { idx }
    }
}

/// Lowercase base36 of `n`, left-padded with '0' to at least `width` chars.
fn base36(mut n: u64, width: usize) -> String {
    const DIGITS: &[u8; 36] = b"0123456789abcdefghijklmnopqrstuvwxyz";
    if n == 0 {
        return "0".repeat(width.max(1));
    }
    let mut buf = Vec::new();
    while n > 0 {
        buf.push(DIGITS[(n % 36) as usize]);
        n /= 36;
    }
    while buf.len() < width {
        buf.push(b'0');
    }
    buf.reverse();
    String::from_utf8(buf).expect("base36 digits are ascii")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn aster_id_is_unique_and_in_charset() {
        let s = SessionId::from_tag("abc123");
        let a = aster_client_id(&s, &"BTC".into(), Side::Buy, 0);
        let b = aster_client_id(&s, &"BTC".into(), Side::Buy, 1);
        let c = aster_client_id(&s, &"BTC".into(), Side::Sell, 0);
        assert_ne!(a, b); // different epoch
        assert_ne!(a, c); // different side
        for id in [&a, &b, &c] {
            assert!(id.len() <= 36, "id too long: {id} ({})", id.len());
            assert!(
                id.chars().all(|ch| ch.is_ascii_alphanumeric() || "-_:/.".contains(ch)),
                "bad charset: {id}"
            );
        }
        assert!(a.starts_with("Xabc123-BTC-B-"));
    }

    #[test]
    fn aster_id_respects_36_char_cap_under_pathological_input() {
        let s = SessionId::from_tag("zzzzzz");
        let long_market = MarketId("VERYLONGMARKETNAME".into());
        let id = aster_client_id(&s, &long_market, Side::Sell, u64::MAX);
        assert!(id.len() <= 36);
    }

    #[test]
    fn hedge_cloid_is_deterministic_and_distinct() {
        let c1 = Cloid::hedge("AST-100", "T-7", 500_000);
        let c2 = Cloid::hedge("AST-100", "T-7", 500_000);
        let c3 = Cloid::hedge("AST-100", "T-7", 600_000); // different attempt epoch
        let c4 = Cloid::hedge("AST-101", "T-7", 500_000); // different session
        assert_eq!(c1, c2, "same (session, market, epoch) must yield the same cloid");
        assert_ne!(c1, c3);
        assert_ne!(c1, c4);
    }

    #[test]
    fn hedge_cloid_hex_format() {
        let c = Cloid::hedge("AST-100", "T-7", 500_000);
        let h = c.to_hex();
        assert!(h.starts_with("0x"));
        assert_eq!(h.len(), 34); // 0x + 32 hex
        assert!(h[2..].chars().all(|ch| ch.is_ascii_hexdigit()));
        // hex round-trips the bytes
        let bytes = c.0;
        assert_eq!(&h[2..4], &format!("{:02x}", bytes[0]));
    }

    #[test]
    fn lighter_client_order_index_is_stable_positive_and_in_range() {
        let c1 = Cloid::hedge("AST-100", "T-7", 500_000);
        let c2 = Cloid::hedge("AST-100", "T-7", 500_000);
        let idx = c1.to_lighter_client_order_index();
        assert_eq!(idx, c2.to_lighter_client_order_index());
        assert!(idx > 0);
        assert!(idx <= LIGHTER_MAX_CLIENT_ORDER_INDEX);
    }

    #[test]
    fn base36_pads_and_encodes() {
        assert_eq!(base36(0, 6), "000000");
        assert_eq!(base36(35, 0), "z");
        assert_eq!(base36(36, 0), "10");
    }
}
