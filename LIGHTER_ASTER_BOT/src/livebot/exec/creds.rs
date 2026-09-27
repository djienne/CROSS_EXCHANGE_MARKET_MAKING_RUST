//! Credential loading. Live trading reads the `aster.env` / `lighter.env` dotenv files of
//! [`env_files`], derives the venue ROLE for each address from the **key**, not from the
//! (user-editable, sometimes mislabeled) field names, and validates the mapping before a single
//! signed call. A dry run signs with a fixed identity instead ([`venue_creds`]).
//!
//! These files contain real private keys in plaintext — they MUST be gitignored and never
//! logged. This module logs only public addresses, never key material.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, bail, ensure, Context, Result};
use tracing::info;

use super::crypto::{address_from_priv, address_hex, keccak256, parse_address, parse_priv_key};

/// The live credential files: `ASTER_ENV_PATH` and `LIGHTER_ENV_PATH`, by default `aster.env`
/// and `lighter.env` in the working directory.
pub fn env_files() -> [PathBuf; 2] {
    [("ASTER_ENV_PATH", "aster.env"), ("LIGHTER_ENV_PATH", "lighter.env")]
        .map(|(var, default)| std::env::var_os(var).map_or_else(|| PathBuf::from(default), PathBuf::from))
}

/// What both engines and their status pollers sign with: the live env files, or the dry-run
/// identity once `run --mode dry-run` has pointed the config at the simulated venues.
pub fn venue_creds(dry_run: bool) -> Result<(AsterCreds, LighterCreds)> {
    if dry_run {
        return Ok((AsterCreds::dry_run(), LighterCreds::dry_run()));
    }
    Ok((AsterCreds::from_env()?, LighterCreds::from_env()?))
}

// The dry-run identity is registered on no venue, so it is public by design: nothing signed with
// it can move money, yet every request still passes through the live signers unchanged.

/// A dry-run secp256k1 key, derived so the repo holds no key literal.
fn dry_run_key(role: &str) -> [u8; 32] {
    keccak256(format!("lighter-aster-bot dry run: {role}").as_bytes())
}

fn dry_run_owner() -> String {
    address_hex(&address_from_priv(&dry_run_key("owner")).expect("the fixed dry-run key is valid"))
}

/// A pair from the signer library's `GenerateAPIKey`, pinned because that generator is not
/// deterministic in its seed and the dry-run venue must answer `apikeys` with this public key.
const DRY_RUN_LIGHTER_PRIVATE_KEY: &str =
    "0xffaaab6ffc4d3379f43ac385ba2be5dc84ffbe279f65e2dcc7f7338206b2c146fcb8981de2e66c12";
const DRY_RUN_LIGHTER_PUBLIC_KEY: &str =
    "84b37246a1e5efb5cdbbe31e31bc2d1b1e7d030ed228265bb3ad451e21ca8deec1c76e446b03b8c4";

/// Parse a tiny `key=value` dotenv file (the live env files are a handful of lines).
fn parse_env_file(path: &Path) -> Result<HashMap<String, String>> {
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("could not read credentials file {}", path.display()))?;
    let mut out = HashMap::new();
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') || !line.contains('=') {
            continue;
        }
        let (k, v) = line.split_once('=').unwrap();
        out.insert(k.trim().to_string(), v.trim().to_string());
    }
    Ok(out)
}

fn required(m: &HashMap<String, String>, key: &str) -> Result<String> {
    m.get(key)
        .filter(|v| !v.trim().is_empty())
        .cloned()
        .ok_or_else(|| anyhow!("credentials env missing {key}"))
}

/// Resolved Aster credentials: the main-account `user`, the API-wallet `signer` (= the key's
/// address), and the signing key. The string forms preserve the source case (Aster looks up the
/// agent pair case-insensitively).
pub struct AsterCreds {
    pub user: String,
    pub signer: String,
    pub key: [u8; 32],
}

impl AsterCreds {
    /// The live credentials, from the Aster file of [`env_files`].
    pub fn from_env() -> Result<Self> {
        let [aster, _] = env_files();
        Self::load(&aster)
    }

    /// The dry-run identity: an API wallet trading for the dry-run owner.
    pub fn dry_run() -> Self {
        let key = dry_run_key("aster api wallet");
        let signer = address_hex(&address_from_priv(&key).expect("the fixed dry-run key is valid"));
        AsterCreds { user: dry_run_owner(), signer, key }
    }

    /// Load + role-resolve from a dotenv file. `signer` = the address derived from `private_key`;
    /// `user` = the address field (`wallet_address`/`subaccount_address`) that ISN'T the signer.
    pub fn load(path: &Path) -> Result<Self> {
        let m = parse_env_file(path)?;
        let key = parse_priv_key(m.get("private_key").context("aster env missing private_key")?)?;
        let derived = address_from_priv(&key)?;
        let derived_lc = address_hex(&derived); // lowercase 0x form

        let mut signer: Option<String> = None;
        let mut user: Option<String> = None;
        for field in ["wallet_address", "subaccount_address"] {
            if let Some(v) = m.get(field) {
                let vb = parse_address(v)
                    .with_context(|| format!("aster env {field} is not a valid address"))?;
                if address_hex(&vb) == derived_lc {
                    signer = Some(v.clone()); // preserve source case for the request field
                } else {
                    user = Some(v.clone());
                }
            }
        }
        // Cross-check: the env file must explicitly list the API-wallet (signer) address and it
        // must match the private key. This catches a swapped/rotated key against a stale env
        // file before anything is signed with the wrong identity. Deliberately strict — the
        // signer address is NOT synthesized from the key alone.
        let Some(signer) = signer else {
            bail!(
                "aster env cross-check failed: neither wallet_address nor subaccount_address \
                 matches the private key's address {derived_lc}. Add the API-wallet address \
                 (e.g. `subaccount_address={derived_lc}`) alongside the main-account address, \
                 or fix private_key if it was rotated."
            );
        };
        // The user (main account) is mandatory and cannot be the signer.
        let user = user.ok_or_else(|| {
            anyhow!(
                "could not determine the Aster main-account (user) address from the env file: \
                 no wallet_address/subaccount_address differs from the key's address {derived_lc}"
            )
        })?;
        info!("aster credentials: user={user} signer={signer} (roles derived from the key)");
        Ok(AsterCreds { user, signer, key })
    }
}

/// Resolved Lighter API-key credentials. These are not EVM keys, so validation is limited to
/// required-field presence and numeric account/API-key ids; the native signer performs the
/// authoritative key check through `CreateClient` / `CheckClient`.
#[derive(Clone)]
pub struct LighterCreds {
    pub api_private_key: String,
    pub api_public_key: String,
    pub api_key_index: i32,
    pub account_index: i64,
}

impl std::fmt::Debug for LighterCreds {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LighterCreds")
            .field("api_private_key", &"<redacted>")
            .field("api_public_key", &self.api_public_key)
            .field("api_key_index", &self.api_key_index)
            .field("account_index", &self.account_index)
            .finish()
    }
}

impl LighterCreds {
    /// The live credentials, from the Lighter file of [`env_files`].
    pub fn from_env() -> Result<Self> {
        let [_, lighter] = env_files();
        Self::load(&lighter)
    }

    /// The dry-run identity: the pinned API key in slot 2 of a made-up account.
    pub fn dry_run() -> Self {
        LighterCreds {
            api_private_key: DRY_RUN_LIGHTER_PRIVATE_KEY.to_string(),
            api_public_key: DRY_RUN_LIGHTER_PUBLIC_KEY.to_string(),
            api_key_index: 2,
            account_index: 1_000_000_000,
        }
    }

    pub fn load(path: &Path) -> Result<Self> {
        let m = parse_env_file(path)?;
        let api_private_key = required(&m, "API_KEY_PRIVATE_KEY")?;
        let api_public_key = required(&m, "API_KEY_PUBLIC_KEY")?;
        let api_key_index = required(&m, "API_KEY_INDEX")?
            .parse::<i32>()
            .context("API_KEY_INDEX must be an integer")?;
        let account_index = required(&m, "ACCOUNT_INDEX")?
            .parse::<i64>()
            .context("ACCOUNT_INDEX must be an integer")?;
        info!(
            "lighter credentials: account_index={} api_key_index={} public_key_len={}",
            account_index,
            api_key_index,
            api_public_key.len()
        );
        Ok(LighterCreds {
            api_private_key,
            api_public_key,
            api_key_index,
            account_index,
        })
    }
}

/// Hyperliquid credentials from `HYPERLIQUID_ENV_PATH` (default `hyperliquid.env`), keys
/// `wallet_address` (the traded subaccount or vault: the `/info` user), `private_key` (the agent
/// key that signs; its address owns the nonces) and `is_vault` (sign for `wallet_address` as
/// `vaultAddress`). rust_live's copies also carry `exchange=hyperliquid`, which is ignored.
pub struct HyperliquidCreds {
    pub account: String,
    pub signer: String,
    pub vault: Option<[u8; 20]>,
    pub key: [u8; 32],
}

impl std::fmt::Debug for HyperliquidCreds {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "HyperliquidCreds {{ account: {}, signer: {}, vault: {}, key: <redacted> }}", self.account, self.signer, self.vault.is_some())
    }
}

impl HyperliquidCreds {
    pub fn from_env() -> Result<Self> {
        Self::load(&std::env::var_os("HYPERLIQUID_ENV_PATH").map_or_else(|| PathBuf::from("hyperliquid.env"), PathBuf::from))
    }

    /// The dry-run identity: an agent key trading the dry-run owner's account.
    pub fn dry_run() -> Self {
        let key = dry_run_key("hyperliquid agent");
        let signer = address_hex(&address_from_priv(&key).expect("the fixed dry-run key is valid"));
        HyperliquidCreds { account: dry_run_owner(), signer, vault: None, key }
    }

    pub fn load(path: &Path) -> Result<Self> {
        let m = parse_env_file(path)?;
        if let Some(extra) = m.keys().find(|k| !["exchange", "wallet_address", "private_key", "is_vault"].contains(&k.as_str())) {
            bail!("hyperliquid env: unknown key {extra}");
        }
        let key = parse_priv_key(&required(&m, "private_key")?)?;
        let account = parse_address(&required(&m, "wallet_address")?).context("hyperliquid env wallet_address")?;
        let vault = match required(&m, "is_vault")?.as_str() {
            "true" => true,
            "false" => false,
            other => bail!("hyperliquid env: is_vault must be true or false, got {other}"),
        };
        let signer = address_from_priv(&key)?;
        ensure!(!vault || signer != account, "hyperliquid env: private_key must be an agent key, not the vault's own");
        let (account_hex, signer_hex) = (address_hex(&account), address_hex(&signer));
        info!("hyperliquid credentials: account={account_hex} signer={signer_hex} vault={vault}");
        Ok(Self { account: account_hex, signer: signer_hex, vault: vault.then_some(account), key })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn write_tmp(name: &str, body: &str) -> std::path::PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!("xemm-creds-{name}-{}.env", std::process::id()));
        let mut f = std::fs::File::create(&p).unwrap();
        f.write_all(body.as_bytes()).unwrap();
        p
    }

    // Test key 0x…01 → address 0x7E5F4552091A69125d5DfCb7b8C2659029395Bdf.
    const KEY1: &str = "0x0000000000000000000000000000000000000000000000000000000000000001";
    const ADDR1: &str = "0x7e5f4552091a69125d5dfcb7b8c2659029395bdf";
    const USER1: &str = "0x1111111111111111111111111111111111111111";

    #[test]
    fn the_dry_run_identity_is_fixed_valid_and_needs_no_files() {
        let (aster, lighter) = venue_creds(true).unwrap();
        let (again, _) = venue_creds(true).unwrap();
        assert_eq!((aster.key, &aster.signer, &aster.user), (again.key, &again.signer, &again.user));
        assert_ne!(aster.user, aster.signer);
        // The live signer accepts it: the signer address is the key's.
        super::super::sign::EvmAsterSigner::new(aster.user.clone(), aster.signer, aster.key).unwrap();
        assert_eq!((lighter.account_index, lighter.api_key_index), (1_000_000_000, 2));
    }

    #[test]
    fn hyperliquid_signs_as_the_agent_for_the_vault_and_hides_its_key() {
        let p = write_tmp("hl", &format!("wallet_address={USER1}\nprivate_key={KEY1}\nis_vault=true\n"));
        let c = HyperliquidCreds::load(&p).unwrap();
        assert_eq!((c.account.as_str(), c.signer.as_str(), c.vault), (USER1, ADDR1, Some([0x11; 20])));
        assert!(!format!("{c:?}").contains(&KEY1[40..]), "{c:?}");
        for (body, error) in [
            (format!("exchange=hyperliquid\nwallet_address={ADDR1}\nprivate_key={KEY1}\nis_vault=true\n"), "agent key"),
            (format!("exchange=hyperliquid\nwallet_address={USER1}\nprivate_key={KEY1}\nis_vault=yes\n"), "is_vault"),
            (format!("exchange=hyperliquid\nwallet_address={USER1}\nprivate_key={KEY1}\nis_vault=true\nvault=1\n"), "unknown key"),
        ] {
            let p = write_tmp("hl-bad", &body);
            assert!(format!("{:#}", HyperliquidCreds::load(&p).unwrap_err()).contains(error));
            std::fs::remove_file(p).ok();
        }
        std::fs::remove_file(p).ok();
    }

    #[test]
    fn aster_roles_derived_from_key_not_field_names() {
        // Sample role layout: signer in subaccount_address, user in wallet_address.
        let body = format!(
            "wallet_name=xemm\nwallet_address={USER1}\nprivate_key={KEY1}\nsubaccount_address={ADDR1}\n"
        );
        let p = write_tmp("aster", &body);
        let c = AsterCreds::load(&p).unwrap();
        assert_eq!(c.user.to_lowercase(), USER1);
        assert_eq!(c.signer.to_lowercase(), ADDR1);
        std::fs::remove_file(p).ok();
    }

    #[test]
    fn aster_rejects_when_no_user_distinct_from_signer() {
        let body = format!("private_key={KEY1}\nwallet_address={ADDR1}\nsubaccount_address={ADDR1}\n");
        let p = write_tmp("aster-bad", &body);
        assert!(AsterCreds::load(&p).is_err());
        std::fs::remove_file(p).ok();
    }

    #[test]
    fn aster_rejects_when_no_address_matches_key() {
        let body = format!("private_key={KEY1}\nwallet_address={USER1}\nsubaccount_address=0x2222222222222222222222222222222222222222\n");
        let p = write_tmp("aster-mismatch", &body);
        assert!(AsterCreds::load(&p).is_err());
        std::fs::remove_file(p).ok();
    }

    #[test]
    fn lighter_debug_redacts_private_key() {
        let creds = LighterCreds {
            api_private_key: "secret".to_string(),
            api_public_key: "pub".to_string(),
            api_key_index: 1,
            account_index: 2,
        };
        let text = format!("{creds:?}");
        assert!(text.contains("<redacted>"));
        assert!(!text.contains("secret"));
    }

    #[test]
    fn lighter_loads_required_fields() {
        let body = "\
API_KEY_PRIVATE_KEY=priv
API_KEY_PUBLIC_KEY=pub
API_KEY_INDEX=2
ACCOUNT_INDEX=42
";
        let p = write_tmp("lighter", body);
        let c = LighterCreds::load(&p).unwrap();
        assert_eq!(c.api_private_key, "priv");
        assert_eq!(c.api_public_key, "pub");
        assert_eq!(c.api_key_index, 2);
        assert_eq!(c.account_index, 42);
        std::fs::remove_file(p).ok();
    }
}
