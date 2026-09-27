//! Hyperliquid L1 action signing: the MessagePack action hash, and the EIP-712 phantom
//! `Agent` over it signed by the agent (API wallet) key. The vectors below come from the
//! Python SDK; the venue accepting a signed order (`probe hl-place-cancel`) is the
//! independent check.

use anyhow::{Context, Result};
use k256::ecdsa::SigningKey;
use serde::Serialize;

use crate::livebot::exec::crypto::keccak256;

/// `keccak256(msgpack(action) ‖ nonce ‖ vault marker ‖ expiry marker)`, integers 8 bytes big
/// endian. The vault marker is `0x01 ‖ address` when acting for a subaccount or vault, else
/// `0x00`; the expiry marker `0x00 ‖ expiresAfter` is present only when set.
pub fn action_hash(msgpack: &[u8], nonce: u64, vault: Option<&[u8; 20]>, expires_after: Option<u64>) -> [u8; 32] {
    let mut data = Vec::with_capacity(msgpack.len() + 38);
    data.extend_from_slice(msgpack);
    data.extend_from_slice(&nonce.to_be_bytes());
    match vault {
        Some(address) => {
            data.push(1);
            data.extend_from_slice(address);
        }
        None => data.push(0),
    }
    if let Some(expires_after) = expires_after {
        data.push(0);
        data.extend_from_slice(&expires_after.to_be_bytes());
    }
    keccak256(&data)
}

/// The EIP-712 digest of `Agent{source, connectionId}` in the `Exchange` domain (version 1,
/// chain id 1337, zero verifying contract); the source is "a" on mainnet, "b" on testnet.
fn agent_digest(connection_id: &[u8; 32], mainnet: bool) -> [u8; 32] {
    let mut domain = [0u8; 160];
    domain[..32].copy_from_slice(&keccak256(
        b"EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)",
    ));
    domain[32..64].copy_from_slice(&keccak256(b"Exchange"));
    domain[64..96].copy_from_slice(&keccak256(b"1"));
    domain[120..128].copy_from_slice(&1337u64.to_be_bytes());
    let mut agent = [0u8; 96];
    agent[..32].copy_from_slice(&keccak256(b"Agent(string source,bytes32 connectionId)"));
    agent[32..64].copy_from_slice(&keccak256(if mainnet { b"a" } else { b"b" }));
    agent[64..].copy_from_slice(connection_id);
    let mut digest = [0u8; 66];
    digest[..2].copy_from_slice(&[0x19, 0x01]);
    digest[2..34].copy_from_slice(&keccak256(&domain));
    digest[34..].copy_from_slice(&keccak256(&agent));
    keccak256(&digest)
}

/// The `/exchange` request's `signature`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Signature {
    pub r: String,
    pub s: String,
    pub v: u8,
}

fn sign_connection_id(key: &SigningKey, connection_id: &[u8; 32], mainnet: bool) -> Result<Signature> {
    let (signature, recovery_id) = key
        .sign_prehash_recoverable(&agent_digest(connection_id, mainnet))
        .context("signing a Hyperliquid action")?;
    Ok(Signature {
        r: format!("0x{}", hex::encode(signature.r().to_bytes())),
        s: format!("0x{}", hex::encode(signature.s().to_bytes())),
        v: 27 + recovery_id.to_byte(),
    })
}

/// Signs `action` as the agent `key`; `vault` is the subaccount or vault it acts for.
pub fn sign_action<A: Serialize>(
    key: &SigningKey,
    action: &A,
    nonce: u64,
    vault: Option<&[u8; 20]>,
    expires_after: Option<u64>,
    mainnet: bool,
) -> Result<Signature> {
    let msgpack = rmp_serde::to_vec_named(action).context("MessagePack of a Hyperliquid action")?;
    sign_connection_id(key, &action_hash(&msgpack, nonce, vault, expires_after), mainnet)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn digest_and_signature_match_the_python_sdk() {
        let connection_id: [u8; 32] = std::array::from_fn(|i| i as u8);
        assert_eq!(hex::encode(agent_digest(&connection_id, true)),
            "b9e7c81cff512fa0969928e37d7c2475af657f1b314b7458c8dd7a023044cac0");
        assert_eq!(hex::encode(agent_digest(&connection_id, false)),
            "4384ea9179d358ab65dd0375834d2e206330bd138f8e05c763d97f6e3f54bac1");
        let mut raw_key = [0u8; 32];
        raw_key[31] = 1;
        let signature = sign_connection_id(&SigningKey::from_slice(&raw_key).unwrap(), &connection_id, true).unwrap();
        assert_eq!((signature.r.as_str(), signature.s.as_str(), signature.v), (
            "0x6fac96b099be7b8cdf3bc5ccec1b7966b2543f97941734f89f106b3f18e28d3b",
            "0x1ab95dff17bf2a60a06d9471d504e174cabc3496da75c50c04fe21ddb30f13ef", 28));
    }
}
