# Live measurements and calibration

Dated results of the real-order probes and bounded live trials, kept out of RUNBOOK.md so it
stays operational. Each holds for the date, host and code it names; re-measure after a venue or
code change.

## Model changes and known gaps

- 2026-09-30 (`3472012`): `[dry_run] hyperliquid_rtt_ms` went from `[200, 500]` to
  `[200, 900]` for both Hyperliquid routes. Regular dry runs adopted it at 10:11:52 UTC;
  reports across that restart mix models, so pass `--since` after it.
- The simulator gives Lighter separate 60/min read and transaction buckets; Standard shares one,
  and idle reads already use ~46/min, so the model can miss 429s caused by combined traffic.
- A live-vs-twin comparison should run the twin with the live host's measured latencies (from
  Windows: Aster ~350 ms per order call, Hyperliquid ~950 ms per action), not the Tokyo profile;
  otherwise fill-rate differences mix location with simulator fidelity. With a handful of fills
  per cell, differences in counts are not yet significant.

## Probes, 2026-09-28

From this Windows host, ~250 ms application ping to Aster/Lighter. Subtracting that baseline
estimates additional order-processing time; it does not isolate matching latency.

- Aster takes ~100 ms per order call: post-only ~360 ms, cancel ~340 ms, amend ~350 ms,
  IOC result ~350 ms, so a refresh by cancel+place would take ~700 ms; the bot refreshes
  a quote by one amend. The user stream has a
  fill 5-11 ms after the REST result; userTrades has its fee ~1 RTT later. The 10 s
  dead-man fired at 11.1 s.
- Aster rejects an amend to a crossing price (-2036) and the order rests unchanged. A
  post-only through the book is acknowledged NEW, then EXPIRED with nothing filled.
- Lighter's order WS answers in one ping, and the fill is terminal ~300 ms later. An IOC
  that cannot fill is accepted, consumes its nonce, and ends terminal with 0 filled
  (`canceled-too-much-slippage`).
- Taker fees: Aster 4.0 bps, Lighter 0, as in `bot.toml`.
- Hyperliquid (`probe hl-hedge`): the IOC result ~0.9-1.2 s, the fee ~0.3-3.7 s later
  (`userFillsByTime`), taker fee 4.5 bps. An IOC that cannot fill is refused with "could not
  immediately match", which the bot retries as a no-fill.
- A reduce-only order under the venue minimum: Aster fills one ($0.88 under its $5), and so
  does Lighter (0.01 HYPE under 0.07 and $10), partial or closing. Hyperliquid refuses a
  partial one ("Order must have minimum value of $10") and fills one that closes the whole
  position, so XEMM's correction and the taker's recovery share the rule that rounds up to
  the minimum or to the whole position. Any excess is corrected on the other leg after
  confirming the positions.

## `close`, 2026-09-29

Closing Aster −0.14 and Hyperliquid +0.14 HYPE: Aster filled in 339 ms, Hyperliquid in 1.3 s
(fee known at 1.3 s), all three venues flat within 5 s of the start.

## Live tests and twin calibration, 2026-09-30

Bounded trials used commit `7f2ee12`, $13 clips (0.15 HYPE here), $40 caps, $3 loss stops,
maker minimum net edge 0 and minimum touch distance 1 bp. Each real route ran alone with an
isolated paper twin on the identical saved profile. That profile retains the old Hyperliquid
latency `[200, 500]`; the calibration below applies to subsequent normal dry runs.
Profiles, logs, shutdown residuals and JSON reports are local in `runs/stage3-20260930/`.
The HYPE interval is 08:37:52–09:16:05 UTC; HYPE-HL is 09:18:47–10:03:45 UTC.

| Measurement | HYPE live | HYPE twin | HYPE-HL live | HYPE-HL twin |
| --- | ---: | ---: | ---: | ---: |
| Window, minutes | 38.22 | 38.22 | 44.97 | 44.97 |
| Native maker fills (buy/sell) | 6 (2/4) | 4 (1/3) | 2 (1/1) | 1 (0/1) |
| Maker fills/hour | 9.42 | 6.28 | 2.67 | 1.33 |
| First hedge fill observed, p50 ms (logical trades) | 562 (5) | 323 (4) | 943 (1) | 654 (1) |
| Amend round trip, p50 ms (samples) | 345 (565) | 105 (555) | 351 (828) | 103 (1317) |
| Maker/hedge fees, bps | 0/0 | 0/0 | 0/4.5 | 0/4.5 |
| Gross bps, mean per logical trade | -1.764 | -2.609 | 0.809 | -2.310 |
| Execution net, USD | -0.011445 | -0.013470 | -0.009575 | -0.00884658 |

The first-hedge-fill row was recomputed with `bot_stats.py` at `1c59df5`: per logical trade,
from its first maker fill on the local monotonic clock, including retries and accumulation.

All four containers stopped with exit 0, empty orders and zero net residual. Real accounts
finished flat: HYPE's remaining 0.30 pair was closed by `close`, while HYPE-HL's buy/sell
cycle closed itself. HYPE's whole-cycle wallet delta was -$0.02276013, including the operator
close and funding; operator closes do not enter the engine journal. HYPE-HL's flat equity
fell from $228.80656775 to $228.79699275, exactly the journal's -$0.009575. The combined PnL
and trade-history reports agree on the journal economics. Both streams stayed connected
through their 25-minute keepalive cadence. One HYPE stale-account sweep cleared in 2.54 s;
the pre-fill-snapshot mismatch and missing-amend false uncertainty did not recur.

Aster's ~250 ms network floor leaves ~95–101 ms per amend, consistent with the Tokyo model.
Lighter's 562 ms observed hedge includes a ~252 ms reply and its documented 300 ms Standard
delay ([account types](https://apidocs.lighter.xyz/docs/account-types)); its twin's 323 ms is
consistent with that delay and the configured short RTT. Hyperliquid's original per-attempt
first-fill timings were 942/973 ms with a 290 ms info round trip. Its API forwards actions to a node and waits for
commitment ([API servers](https://hyperliquid.gitbook.io/hyperliquid-docs/hypercore/api-servers)),
so subtracting an info ping also leaves forwarding/consensus time. The normal simulation's
Hyperliquid tail is widened to `[200, 900]`, matching the published co-located median/p99
([HyperCore](https://hyperliquid.gitbook.io/hyperliquid-docs/hypercore/overview)); these two
Windows samples do not estimate a Tokyo median or p99. Fill counts and unequal resting
uptime are insufficient to fit `hidden_queue_multiplier`, which remains 0.5. Gross/net
capture includes adverse selection and hedge price movement; the journal lacks a separate
submission-time hedge VWAP, so it cannot identify hedge slippage independently.

The 15-minute standalone HYPE and HYPE-HL taker trials had no qualifying opportunity.
HYPE-LH executed one matched 0.15 entry: expected/realized gross 4.666/2.802 bps, edge kept
0.601, fees 4.499 bps and entry net -$0.002171. Its reduce-filter trial found no reverse
opportunity; the operator close left both accounts flat, with whole-cycle cost $0.007532.
Neither maker trial observed a taker rights handover; its entry gate remained enforced.
These are operational checks with limited market coverage, not evidence of profitability.

## Real-order regression, 2026-09-30 15:53–16:02 UTC

At `1c59df5`, Aster placement/amend/reject/cancel/dead-man checks and real buy/reduce-only
roundtrips passed; so did Lighter's hedge-worker and native taker paths, and Hyperliquid's
post-only/cancel, IOC and hedge-worker paths. Capped probes used $13, or $14 for the Hyperliquid
worker after a $13 cap correctly refused its rounded size. Final reads showed all accounts
flat with no open orders. Net execution cash change was -$0.032819 at stablecoin parity.
The shared `close` command was also checked from flat; no simultaneous three-venue close
of open positions or live taker handover was exercised. Detailed logs are local in
`runs/live-order-check-20260930155334Z/`; the screener and dry runs kept collecting.
