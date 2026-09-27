//! Execution plane. Concrete venue workers behind bounded command
//! queues — NOT an `async` trait invoked per book event. The strategy `try_send`s small
//! commands; a worker owns the venue client and publishes lifecycle events back.
//!
//! - [`command`] — the `ExecCommand` / `HedgeCommand` / `ExecEvent` contract + queue depth.
//! - [`sign`] — signer traits + monotonic nonces + the real Aster signer.
//! - [`creds`] — `aster.env`/`lighter.env` loading + key-derived role resolution.
//! - [`crypto`] — golden-tested Aster EIP-712 signing primitives.
//! - [`aster`] / [`lighter`] — the GATED live workers (real funds; signer-gated).

pub mod aster;
pub mod command;
pub mod creds;
pub mod crypto;
pub mod lighter;
pub mod sign;
