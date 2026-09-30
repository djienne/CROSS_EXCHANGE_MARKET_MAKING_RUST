//! Lognormal delays fitted to configured median/p99 values: measurements or estimates
//! as labeled in `[dry_run]`. Public and private streams have separate draws.
//! Draws come from a seeded splitmix64 stream, so a run's randomness is repeatable.
//!
//! Time itself needs no machinery here: the simulated world is the real one delayed by a
//! constant shift, applied once where exchange-stamped frames enter the simulator
//! (`Exchange::ingest`). Everything else runs on this host's wall clock in microseconds.

use serde::{Deserialize, Serialize};

/// This host's wall clock, µs since the Unix epoch: the venues stamp their frames on the same
/// scale.
pub fn wall_us() -> i64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_or(0, |d| d.as_micros() as i64)
}

/// splitmix64: tiny, fast and well mixed; plenty for latency draws.
#[derive(Debug, Clone)]
pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Self {
        Self(seed)
    }

    pub fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform on (0, 1); never 0, so `ln` is safe.
    pub fn unit(&mut self) -> f64 {
        ((self.next_u64() >> 11) as f64 + 0.5) / (1u64 << 53) as f64
    }

    /// Standard normal (Box–Muller; the second value of each pair is dropped).
    pub fn normal(&mut self) -> f64 {
        let (u1, u2) = (self.unit(), self.unit());
        (-2.0 * u1.ln()).sqrt() * (std::f64::consts::TAU * u2).cos()
    }
}

/// z-score of the standard normal's 99th percentile.
const Z99: f64 = 2.326_347_874_040_841;

/// A delay distribution given as `[p50_ms, p99_ms]`. `[0, 0]` means no delay (limit cases).
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "[f64; 2]", into = "[f64; 2]")]
pub struct Latency {
    p50_ms: f64,
    p99_ms: f64,
}

impl TryFrom<[f64; 2]> for Latency {
    type Error = String;
    fn try_from([p50_ms, p99_ms]: [f64; 2]) -> Result<Self, String> {
        let zero = p50_ms == 0.0 && p99_ms == 0.0;
        if !(zero || (p50_ms > 0.0 && p99_ms >= p50_ms && p99_ms.is_finite())) {
            return Err(format!("latency [p50, p99] must satisfy 0 < p50 <= p99 (or be [0, 0]); got [{p50_ms}, {p99_ms}]"));
        }
        Ok(Self { p50_ms, p99_ms })
    }
}

impl From<Latency> for [f64; 2] {
    fn from(l: Latency) -> Self {
        [l.p50_ms, l.p99_ms]
    }
}

impl Latency {
    pub const ZERO: Latency = Latency { p50_ms: 0.0, p99_ms: 0.0 };

    pub fn sample_us(&self, rng: &mut Rng) -> i64 {
        if self.p50_ms == 0.0 {
            return 0;
        }
        let sigma = (self.p99_ms / self.p50_ms).ln() / Z99;
        (self.p50_ms * (sigma * rng.normal()).exp() * 1_000.0).round() as i64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn percentile(sorted: &[i64], p: f64) -> f64 {
        sorted[((sorted.len() - 1) as f64 * p).round() as usize] as f64 / 1_000.0
    }

    #[test]
    fn samples_reproduce_the_configured_median_and_tail() {
        let lat = Latency::try_from([101.0, 164.0]).unwrap();
        let mut rng = Rng::new(7);
        let mut draws: Vec<i64> = (0..200_000).map(|_| lat.sample_us(&mut rng)).collect();
        draws.sort_unstable();
        let (p50, p99) = (percentile(&draws, 0.5), percentile(&draws, 0.99));
        assert!((p50 / 101.0 - 1.0).abs() < 0.01, "p50 {p50}");
        assert!((p99 / 164.0 - 1.0).abs() < 0.02, "p99 {p99}");
        assert!(draws[0] > 0);
    }

    #[test]
    fn seeded_streams_repeat_and_zero_means_no_delay() {
        let lat = Latency::try_from([8.0, 36.0]).unwrap();
        let (mut a, mut b) = (Rng::new(3), Rng::new(3));
        for _ in 0..100 {
            assert_eq!(lat.sample_us(&mut a), lat.sample_us(&mut b));
        }
        assert_eq!(Latency::ZERO.sample_us(&mut a), 0);
        assert!(Latency::try_from([10.0, 5.0]).is_err());
        assert!(Latency::try_from([0.0, 5.0]).is_err());
    }
}
