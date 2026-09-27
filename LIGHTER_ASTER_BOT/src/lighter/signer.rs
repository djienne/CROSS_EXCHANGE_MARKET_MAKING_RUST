//! FFI bindings to the official Lighter native signer shared library
//! (`lighter-signer-<os>-<arch>.so/.dylib/.dll`), the exact same binary the Python
//! SDK loads via `ctypes`. We do NOT reimplement Lighter's signing scheme — we call
//! into the vetted Go/cgo library so signatures are byte-identical to the Python SDK's.
//!
//! ABI validated against lighter-python `signer_client.py`:
//!   * structs `SignedTxResponse`, `StrOrErr`
//!   * `CreateClient` returns an *error-string pointer* (NULL on success); client state
//!     is held inside the library keyed by (api_key_index, account_index).
//!   * every returned `char*` is malloc'd by the library and must be freed with libc
//!     `free` after copying (mirrors Python `decode_and_free`).
//!   * `chain_id` 300 on testnet, else mainnet's 304: the dry-run venue on loopback replays
//!     mainnet, so its transactions are signed exactly as live ones.

use anyhow::{bail, Context, Result};
use libloading::{Library, Symbol};
use std::ffi::{c_char, c_int, c_longlong, c_void, CStr, CString};
use std::path::Path;

// ---- Order / TIF constants (from SignerClient) ----
pub const ORDER_TYPE_LIMIT: i32 = 0;
pub const ORDER_TYPE_MARKET: i32 = 1;
pub const TIF_IMMEDIATE_OR_CANCEL: i32 = 0;
#[cfg(test)]
pub const TIF_POST_ONLY: i32 = 2;
pub const NIL_TRIGGER_PRICE: i32 = 0;
#[cfg(test)]
pub const DEFAULT_28_DAY_ORDER_EXPIRY: i64 = -1;
pub const DEFAULT_IOC_EXPIRY: i64 = 0;
pub const MARGIN_MODE_CROSS: i32 = 0;

// ---- repr(C) structs mirroring ctypes.Structure layouts ----
#[repr(C)]
struct SignedTxResponse {
    tx_type: u8,
    tx_info: *mut c_char,
    tx_hash: *mut c_char,
    message_to_sign: *mut c_char,
    err: *mut c_char,
}

#[repr(C)]
struct StrOrErr {
    s: *mut c_char,
    err: *mut c_char,
}

// ABI guards: on 64-bit, SignedTxResponse = u8 + 4*ptr = 40 bytes
// (u8 padded to 8 for pointer alignment), StrOrErr = 2*ptr = 16 bytes. If these ever
// fail the layout no longer matches the ctypes.Structure the Python SDK relies on.
const _: () = assert!(std::mem::size_of::<SignedTxResponse>() == 40);
const _: () = assert!(std::mem::align_of::<SignedTxResponse>() == 8);
const _: () = assert!(std::mem::size_of::<StrOrErr>() == 16);

// ---- extern fn signatures (argtypes verified in signer_client.py) ----
type CreateClientFn =
    unsafe extern "C" fn(*const c_char, *const c_char, c_int, c_int, c_longlong) -> *mut c_char;
type CheckClientFn = unsafe extern "C" fn(c_int, c_longlong) -> *mut c_char;
type SignCreateOrderFn = unsafe extern "C" fn(
    c_int,      // market_index
    c_longlong, // client_order_index
    c_longlong, // base_amount
    c_int,      // price
    c_int,      // is_ask
    c_int,      // order_type
    c_int,      // time_in_force
    c_int,      // reduce_only
    c_int,      // trigger_price
    c_longlong, // order_expiry
    c_longlong, // nonce
    c_int,      // api_key_index
    c_longlong, // account_index
) -> SignedTxResponse;
type CreateAuthTokenFn = unsafe extern "C" fn(c_longlong, c_int, c_longlong) -> StrOrErr;

/// A signed transaction ready to send: (tx_type, tx_info json, tx_hash).
#[derive(Debug, Clone)]
pub struct SignedTx {
    pub tx_type: u8,
    pub tx_info: String,
    pub tx_hash: String,
}

/// Safe wrapper around the native signer. Holds raw fn pointers into a leaked
/// (process-lifetime) `Library`, so it is `Send + Sync` (fn pointers are both).
pub struct Signer {
    account_index: i64,
    create_client: CreateClientFn,
    check_client: CheckClientFn,
    sign_create_order: SignCreateOrderFn,
    create_auth_token: CreateAuthTokenFn,
}

/// Free a library-allocated C string after copying it into an owned `String`.
/// Mirrors Python `decode_and_free`.
unsafe fn take_cstring(ptr: *mut c_char) -> Option<String> {
    if ptr.is_null() {
        return None;
    }
    let s = CStr::from_ptr(ptr).to_string_lossy().into_owned();
    libc::free(ptr as *mut c_void);
    Some(s)
}

#[inline]
fn nonempty(o: Option<String>) -> Option<String> {
    o.filter(|s| !s.is_empty())
}

impl Signer {
    /// Load the signer library for the current platform and register the API key.
    /// `signers_dir` is the directory containing the `lighter-signer-*` binaries.
    /// `api_private_key` is the API key private key hex (with or without 0x).
    pub fn load(
        signers_dir: &Path,
        url: &str,
        api_private_key: &str,
        api_key_index: i32,
        account_index: i64,
    ) -> Result<Self> {
        let file = signer_filename();
        let path = signers_dir.join(file);
        // SAFETY: loading a trusted, first-party shared library shipped with the project.
        let lib: &'static Library = Box::leak(Box::new(unsafe {
            Library::new(&path).with_context(|| format!("loading signer lib {}", path.display()))?
        }));

        macro_rules! sym {
            ($name:literal, $ty:ty) => {{
                let s: Symbol<$ty> = unsafe {
                    lib.get($name)
                        .with_context(|| format!("missing symbol {}", String::from_utf8_lossy($name)))?
                };
                *s // copy out the fn pointer (points into the 'static library)
            }};
        }

        let signer = Signer {
            account_index,
            create_client: sym!(b"CreateClient\0", CreateClientFn),
            check_client: sym!(b"CheckClient\0", CheckClientFn),
            sign_create_order: sym!(b"SignCreateOrder\0", SignCreateOrderFn),
            create_auth_token: sym!(b"CreateAuthToken\0", CreateAuthTokenFn),
        };

        let chain_id = chain_id_for_url(url);
        let key = api_private_key.strip_prefix("0x").unwrap_or(api_private_key);
        let c_url = CString::new(url)?;
        let c_key = CString::new(key)?;
        // SAFETY: valid C strings, fn pointer from the loaded library.
        let err = unsafe {
            take_cstring((signer.create_client)(
                c_url.as_ptr(),
                c_key.as_ptr(),
                chain_id,
                api_key_index,
                account_index,
            ))
        };
        if let Some(e) = nonempty(err) {
            bail!("CreateClient failed: {e}");
        }
        Ok(signer)
    }

    /// Verify the API key matches the one registered on Lighter (network call inside lib).
    /// On a multi-thread runtime it runs off the async workers: under `run` both engines check
    /// at once, and the dry run's simulated Lighter answers from the same runtime.
    pub fn check_client(&self, api_key_index: i32) -> Result<()> {
        let call = || unsafe { take_cstring((self.check_client)(api_key_index, self.account_index)) };
        let err = match tokio::runtime::Handle::try_current() {
            Ok(rt) if rt.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread => tokio::task::block_in_place(call),
            _ => call(),
        };
        match nonempty(err) {
            Some(e) => bail!("CheckClient failed: {e}"),
            None => Ok(()),
        }
    }

    fn decode(&self, r: SignedTxResponse) -> Result<SignedTx> {
        // Free all four library-allocated pointers (matches __decode_tx_info order).
        let err = unsafe { take_cstring(r.err) };
        let tx_info = unsafe { take_cstring(r.tx_info) };
        let tx_hash = unsafe { take_cstring(r.tx_hash) };
        let _ = unsafe { take_cstring(r.message_to_sign) };
        if let Some(e) = nonempty(err) {
            bail!("sign error: {e}");
        }
        Ok(SignedTx {
            tx_type: r.tx_type,
            tx_info: tx_info.unwrap_or_default(),
            tx_hash: tx_hash.unwrap_or_default(),
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn sign_create_order(
        &self,
        market_index: i32,
        client_order_index: i64,
        base_amount: i64,
        price: i32,
        is_ask: bool,
        order_type: i32,
        time_in_force: i32,
        reduce_only: bool,
        trigger_price: i32,
        order_expiry: i64,
        nonce: i64,
        api_key_index: i32,
    ) -> Result<SignedTx> {
        let r = unsafe {
            (self.sign_create_order)(
                market_index,
                client_order_index,
                base_amount,
                price,
                is_ask as c_int,
                order_type,
                time_in_force,
                reduce_only as c_int,
                trigger_price,
                order_expiry,
                nonce,
                api_key_index,
                self.account_index,
            )
        };
        self.decode(r)
    }

    /// Create a short-lived WS auth token. `deadline_unix` is an absolute unix-seconds
    /// expiry (Python passes `timestamp + ttl`).
    pub fn create_auth_token(&self, deadline_unix: i64, api_key_index: i32) -> Result<String> {
        let r = unsafe { (self.create_auth_token)(deadline_unix, api_key_index, self.account_index) };
        let s = unsafe { take_cstring(r.s) };
        let err = unsafe { take_cstring(r.err) };
        if let Some(e) = nonempty(err) {
            bail!("CreateAuthToken failed: {e}");
        }
        s.context("CreateAuthToken returned no token")
    }
}

// fn pointers + i64 are Send/Sync; the underlying lib is leaked ('static).
unsafe impl Send for Signer {}
unsafe impl Sync for Signer {}

pub fn chain_id_for_url(url: &str) -> i32 {
    if url.contains("testnet") {
        300
    } else {
        304
    }
}

/// The native library keeps one client per API key for the whole process, so tests that load
/// it take turns.
#[cfg(test)]
pub(crate) static NATIVE: std::sync::Mutex<()> = std::sync::Mutex::new(());

pub(crate) fn signer_filename() -> &'static str {
    match (std::env::consts::OS, std::env::consts::ARCH) {
        ("linux", "x86_64") => "lighter-signer-linux-amd64.so",
        ("linux", "aarch64") => "lighter-signer-linux-arm64.so",
        ("macos", "aarch64") => "lighter-signer-darwin-arm64.dylib",
        ("windows", "x86_64") => "lighter-signer-windows-amd64.dll",
        (os, arch) => panic!("unsupported platform for lighter signer: {os}/{arch}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::livebot::exec::creds::LighterCreds;
    use std::io::{BufRead, BufReader, Write};

    /// Runs where the platform's library ships (the Docker test image carries the linux build).
    #[test]
    fn the_dry_run_key_passes_the_venue_check_and_signs_readable_orders() {
        let dir = Path::new("signers");
        if !dir.join(signer_filename()).exists() {
            eprintln!("skipped: no signer library for this platform");
            return;
        }
        let _native = NATIVE.lock().unwrap_or_else(|e| e.into_inner());
        let creds = LighterCreds::dry_run();
        let (account, key) = (creds.account_index, creds.api_key_index);
        // A venue holding the dry-run public key. CheckClient compares it with the key the
        // library derives from the private one, so it passes only for a matching pair.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let public = creds.api_public_key.clone();
        let venue = std::thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            let mut reader = BufReader::new(stream);
            let mut request = String::new();
            reader.read_line(&mut request).unwrap();
            let mut line = String::new();
            while reader.read_line(&mut line).unwrap() > 2 {
                line.clear();
            }
            let body = format!(
                r#"{{"code":200,"api_keys":[{{"account_index":{account},"api_key_index":{key},"nonce":1,"public_key":"{public}"}}]}}"#
            );
            write!(
                reader.get_mut(),
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            )
            .unwrap();
            request
        });
        let signer = Signer::load(dir, &url, &creds.api_private_key, key, account).unwrap();
        signer.check_client(key).unwrap();
        let request = venue.join().unwrap();
        assert!(request.starts_with("GET /api/v1/apikeys?"), "{request}");

        // The fields the dry-run venue reads from a signed order.
        let tx = signer
            .sign_create_order(24, 11, 150, 250_000, true, ORDER_TYPE_LIMIT, TIF_POST_ONLY, false,
                NIL_TRIGGER_PRICE, DEFAULT_28_DAY_ORDER_EXPIRY, 5, key)
            .unwrap();
        let info: serde_json::Value = serde_json::from_str(&tx.tx_info).unwrap();
        for (field, value) in [
            ("AccountIndex", account), ("ApiKeyIndex", key.into()), ("MarketIndex", 24), ("ClientOrderIndex", 11),
            ("BaseAmount", 150), ("Price", 250_000), ("IsAsk", 1), ("Type", 0), ("TimeInForce", 2),
            ("ReduceOnly", 0), ("Nonce", 5),
        ] {
            assert_eq!(info[field], value, "{field} in {info}");
        }
    }
}
