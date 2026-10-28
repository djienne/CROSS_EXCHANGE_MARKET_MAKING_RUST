# Live measurements and calibration

Dated results of the real-order probes and bounded live trials, kept out of RUNBOOK.md so it
stays operational. Each holds for the date, host and code it names; re-measure after a venue or
code change.

## Model changes and known gaps

- 2026-09-30 (`3472012`): `[dry_run] hyperliquid_rtt_ms` went from `[200, 500]` to
  `[200, 900]` for both Hyperliquid routes. Regular dry runs adopted it at 10:11:52 UTC;
  reports across that restart mix models, so pass `--since` after it.
- 2026-10-19 05:56 UTC (`870ceec`): the Tokyo dry runs adopted two simulator fixes, so reports
  across that restart mix models. `e1dc383`: a taker or hedge IOC fills against the worse book
  state per quantity; ~15% of simulated Hyperliquid hedges had failed as no-match (2026-10-12).
  `175d7e5`: a resubscribe snapshot no longer runs before its break, which had left the Lighter
  book unknown until the next snapshot.
- 2026-10-27 (`525820a`): `[dry_run] hyperliquid_rtt_ms` went from `[200, 900]` to
  `[575, 700]`, the live bot's Tokyo IOCs (CASHCAT-LH on the VPS, below). The VPS dry runs
  adopted it at 03:18:39 UTC; reports across that restart mix models.
- 2026-10-27 (`ad3be0c`): CASHCAT-LH's orders wait for a gap held 500 ms (`confirm_ms`), live
  from 22:29:43 UTC and in the twin from 22:30:55; reports across those restarts mix policies.
  From `58ca5b0` (live 23:16:45, the twin ~23:18) the 500 ms count in the hedge venue's own book
  stamps: the local hold had still lost both Lighter buys after it, on Hyperliquid books
  464-642 ms old.
- A live-vs-twin comparison should run the twin with the live host's measured latencies: the
  Tokyo profile for the VPS live bot (its Hyperliquid latency fitted to it, `525820a`), but for a
  live bot on Windows its own (Aster ~350 ms per order call, Hyperliquid ~950 ms per action);
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

What the trials say about the model:

- **Aster:** its ~250 ms network floor leaves ~95–101 ms per amend, consistent with the Tokyo
  model.
- **Lighter:** the 562 ms observed hedge includes a ~252 ms reply and its documented 300 ms
  Standard delay ([account types](https://apidocs.lighter.xyz/docs/account-types)); its twin's
  323 ms is consistent with that delay and the configured short RTT.
- **Hyperliquid:** the original per-attempt first-fill timings were 942/973 ms with a 290 ms info
  round trip. Its API forwards actions to a node and waits for commitment
  ([API servers](https://hyperliquid.gitbook.io/hyperliquid-docs/hypercore/api-servers)), so
  subtracting an info ping also leaves forwarding/consensus time. The normal simulation's
  Hyperliquid tail is widened to `[200, 900]`, matching the published co-located median/p99
  ([HyperCore](https://hyperliquid.gitbook.io/hyperliquid-docs/hypercore/overview)); these two
  Windows samples do not estimate a Tokyo median or p99.
- **Not fitted:** fill counts and unequal resting uptime are insufficient to fit
  `hidden_queue_multiplier`, which remains 0.5. Gross/net capture includes adverse selection and
  hedge price movement; the journal lacks a separate submission-time hedge VWAP, so it cannot
  identify hedge slippage independently.

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

## CASHCAT-LH first live trade, 2026-10-26 19:33 UTC

At `5343506`, from Windows, after CASHCAT was set to 1x cross on both venues (Hyperliquid had been
isolated 2x), `taker run --max-trades 1` with the entry gate off opened one $20 clip: sold 217 on
Lighter at 0.09206, bought 217 on Hyperliquid at 0.091894. Expected/realized gross 17.42/18.03
bps, edge kept 1.035, fees 4.49 bps (Lighter 0), net +$0.027; the Hyperliquid leg filled in 572
ms. `close` then flattened both legs through its taker-only path (Hyperliquid 1.1 s, Lighter
0.56 s); the whole cycle cost $0.020. One trade checks the mechanics, not profitability.

## CASHCAT-LH production path, 2026-10-26 20:45–22:45 UTC

`run --market CASHCAT-LH --mode live` from Windows, gate seeded with the VPS twin's 192 samples
(threshold 11.5-11.8 bps), $20 clips. The leverage gate passed on the real accounts. All four
trades sold Lighter's premium (none reversed in 2 h) until the margin buffer stopped the fifth: 888
a side. Expected/realized gross 13.9/8.0 bps (edge kept 0.58), mostly lost on the Hyperliquid leg
(4-8 bps slippage on a ~300 ms old book, fills ~590 ms); execution net +$0.028. SIGINT stopped it
in 1 s. The plan's closing `close` bought Lighter back at a wider premium: the whole cycle, by
account value, cost $0.165 (fees $0.072, funding ~0).

- **Lighter marks below its book:** at the close the CASHCAT mark was 0.09085, the book mid
  0.09109 (+26 bps) and Hyperliquid's mid 14.6 bps lower. Venue equity values the short leg at
  the mark, so it read +$0.145 just before a close that realized -$0.165. On this route, equity
  is not liquidation value: judge it by closing prices.
- **Funding:** Lighter's `fundings` `direction` names the side that receives (the 21:00 row
  matched the $0.012 our short received). Over 7 days the short-Lighter, long-Hyperliquid carry
  averaged ~0 bps/h, with Lighter's hourly rate swinging by up to ±3-9 bps.
- A forced close mid-cycle pays the premium back; the screener's profit comes from round trips
  over days. Two hours of one-way trades test the mechanics, not the economics.

## CASHCAT-LH on the Tokyo VPS, from 2026-10-26 22:53 UTC

The `xemm-live` unit, $20 clips, no forced closes. To 2026-10-27 03:11 UTC: 21 trades, net
+$0.11, and the paired position held at the margin cap (862 a side).

- **Hyperliquid is the slow leg:**
  - Its IOCs filled (block `time`) 439-606 ms after their send: p50 518 ms over 20 orders.
  - Lighter's filled 309-325 ms after the decision.
  - A kept-alive `/info` call takes 15-25 ms, so the rest of the 518 ms is Hyperliquid's own.
  - The twin had filled the same orders at p50 238 ms; hence `525820a` (Model changes, above).
    From 03:19 to 08:50 the twin then filled its hedges at p50 533 ms (live 527 ms). Its trades
    against Lighter's premium also lost: -$0.125 over 3, against live's -$0.084 over 3.
- **The two directions differ** (`bot_stats.py` `by_direction`):
  - Trades selling Lighter's usual premium kept 77% of their edge: 12 trades, +$0.133.
  - Those buying Lighter under Hyperliquid kept 13%: 8 trades, -$0.042. Their Hyperliquid leg
    filled 9-26 bps worse than decided.
  - Those gaps open when Lighter moves first, and they close before the hedge lands.
  - The trades still free the margin the next premium sale needs.
  - To the 11:31 halt (43 trades, priced by their fills): premium sales +$0.250 over 23,
    Lighter buys -$0.500 over 20. The live opportunity samples (<= 1/s) show which: Lighter buys
    on gaps first seen at the decision lost 15.7 bps on average (11, 2 won), those on gaps in
    view up to 1.7 s earlier 3.4 bps (6). Hence `confirm_ms = 500` (`ad3be0c`), about the
    Hyperliquid IOC's own latency; each trade report logs its gap's `held_ms`.
  - The screener report with `confirm_ms` (Tokyo recording, 2026-10-26 to 27, $13 clips): at any
    hold of 250 ms or more no Lighter buy survives (53 gaps cleared the gate, none held), while
    Lighter sales keep 13-15 bps net instead of 6.5. The route then runs one way: sales fill the
    inventory cap and nothing frees it, so the cap, not the hold, sets the day's profit. The
    report prices the Lighter buys near break-even where live lost 16 bps.
- **Breaker halts:** each successful hedge retry counts toward the 3-an-hour limit, so a fast
  market halted live at 11:31 (4 retries, all rebalanced within 2.7 s) and the twin at 11:28.
  A halted dry run parks until restarted: the twin then sat out 11 h.
- **Four taker bugs:**
  - `6b3cae6`: the missing-hedge retry reused the missed IOC's decision-price bound, so it
    repeated the miss. It is now priced off fresh REST mids.
  - `3d0f28e`: a Hyperliquid IOC that replied "filled 214" read back as unfilled, because the
    info API still said `unknownOid` 6 s later. Recovery then ran on a balanced position. The
    reply's size now stands.
  - `117ff28`, at 09:00: a retry filled in 1.1 s, but its fill listed ~6.5 s late. Waiting for
    it used up the retry's 5 s deadline, and recovery again ran on balanced legs. A filled retry
    now gets its own window to show in the positions.
  - `5be2efc`, at 10:48: both Hyperliquid sells missed a 50 bps drop, and recovery sold the 199
    for a real loss of $0.078. The loss estimate from the equity delta carried the marks of the
    860-a-side inventory, said $0.33, and tripped the $0.25/h breaker. A recovery is now priced
    by its fills.

