# Pair screener

Which Aster-Lighter, Aster-Hyperliquid or Lighter-Hyperliquid pairs suit the bot's two
strategies? The screener answers from public data. It needs no credentials, sends no orders, and
runs apart from the bot, with its own crate, image, container and data.

<!-- ft-facts: container=aster-lighter-screener image=aster_lighter_screener:latest -->

- **`collect`** (the container) follows best bid/offer and trades, subscribing once per venue
  instrument and sharing updates between pairs. It records only the moments the report could
  trade on, plus 5-minute summaries per pair:
  - **taker:** an edge reaching the bot's entry gate (the 90th percentile of its samples over 72 h),
    whose samples the summaries count;
  - **XEMM:** a trade that could have filled a quote at the lowest edge scored (5 bps), with the
    second before it (what the quote was priced from);
  - each moment keeps two seconds after it (where latency-delayed orders fill).
- **`report`** applies a simplified model of the bot's rules and ranks the pairs.
  - **Taker-taker:** the entry gate, cooldown, inventory cap and each leg's latency.
  - **XEMM:** the quote price, a fill only when trades print through the quote (those of one ms
    add up), the hedge latency and the distance gate (skipped behind a maker top thinner than the
    bot's depth). After a fill the market pauses 3 s; with inventory, only the side that reduces it
    is quoted (`reduce_position_only`, which the bot now ships off). A fill under the hedge venue's $10 minimum is held, then
    corrected by a taker order, from flat on the maker venue, and the market pauses 6 s more.
  - Both close their leftover inventory at the last window's mean basis, without fees or spread: a
    standing basis is paid back, not earned.

  Fees, latencies and thresholds apply at report time (`screener.toml` `[report]`). What is recorded
  follows from `[report]` at its loosest, so the same data answers Standard vs Premium, latency
  within its recorded pre-roll/tail, or a higher margin, gate percentile or XEMM edge. It refuses
  settings that would trade on moments the data did not record (a lower percentile, another depth
  or gate window). A test checks that the recording keeps every trade the report would make on the
  full stream.

Each venue combination is matched independently: names (including verified `kPEPE`/`1000PEPE`
scales), prices within 2%, and at least $200k 24 h volume on both venues. The 100-pair limit is
**per combination**. Hyperliquid covers native perpetuals only; spot and builder/HIP-3 markets
are excluded. Pairs refresh at UTC midnight. A venue unavailable then (or at boot) leaves the
combination of the other two running, is retried every 5 minutes, and rejoins in a new run as soon
as it answers. A silent connection is dropped after 30 s and its books are unknown until it
returns. Reconnect backoff is capped at 5 s, with connection/subscription time added: an outage records a gap, except its first 30 s, which keep the
last prices (a 60 s cut, 2026-09-26: 33 s down, every pair back within seconds). After a reboot
or a crash, Docker restarts the container (`unless-stopped`). The writer flushes every 30 s;
an unclean stop can lose queued or unflushed data.

Hyperliquid uses public `bbo`, `l2Book` and `trades`. BBO gives the faster touch; L2 confirms an
unchanged book. `l2Book` is subscribed `fast` (5 levels, every ~0.54 s instead of 20 every ~5.4 s;
from 2026-09-28 on, earlier days carry the slow one). Historical trades received on subscription are excluded using the first book's
exchange timestamp; subsequent duplicates use `(coin, time, tid)`. A disconnected or 30-second
stale Hyperliquid book invalidates only its own pairs. No credentials or order calls are involved.

## Run

```bash
docker compose up -d --build                        # collect (start_all.bat starts it too)
docker compose logs -f                              # traffic and memory every 5 min
docker compose run --rm screener universe           # today's pairs, with their Aster taker fee
docker compose run --rm report                      # the ranking, from all the data
docker compose run --rm report --since 2026-09-27 --lighter premium --json
docker compose run --rm report --latency 2          # sensitivity: every latency doubled
```

`report` is its own service (profile `report`, never started by `up`): it holds every recorded
state of the days it scores in memory, ~75 bytes a row: ~1 GB a day at the three-venue rate below
(0.4 GB for Aster-Lighter alone). Parsing/replay buffers add to peak memory, so the history
that fits under the 8 GB cap varies with market activity.

## Reading the report

- **Pair / direction.** `A-L`, `A-H`, `L-H` identify the venues; A=Aster, L=Lighter, H=Hyperliquid.
  `0` is the left venue, `1` the right. `xf0` means maker on 0, hedge on 1; `xf1` reverses them.
- **Units / coverage.** $/day at one $13 clip, divided by the time both books were known.
  `days` and `down%` show coverage; `unres` counts fixed-strategy fills with an unknown delayed
  book, whose PnL cannot be estimated. JSON includes each leg's fees/delays and daily results.
- **`tk`.** The existing gated taker, requiring 50 opportunity samples before leaving warmup.
  `kept` is realized/expected gross edge after latency.
- **Fixed ranking.** `xf0` / `xf1` use the 18.5 bps required edge and 18–50 bps distance gate.
  `best fixed` selects between these and taker. `+days` counts positive daily results.
- **Exploratory sweeps.** `sweep0` / `sweep1` choose the best required edge (`req`) on these same
  data without the distance gate. These fitted results are displayed separately from the ranking.
- **Comparison.** Use the same complete UTC dates (`--since` / `--until`), after warmup, with at
  least seven concurrent days as an initial route-review window; this does not establish rank
  stability or profitability. Hyperliquid trading is already supported. Compare
  `--latency 0.5`, `1`, and `2`, and Standard/Premium Lighter. Routes are alternatives, not additive
  portfolio profits. Day-to-day rank correlation needs at least two days.

Hyperliquid defaults to **1.5 bps maker / 4.5 bps taker**, the undiscounted native-perp base tier.
The **500 ms execution / 250 ms fill notice / 500 ms quote age** are provisional assumptions,
not order-latency measurements. All are configurable in `[report.hyperliquid]`. Lowering fees
below a recording's taker floor can expose unrecorded opportunities: the report rejects that
rather than silently overstate coverage.

Current references checked 2026-09-26: [full documentation index](https://hyperliquid.gitbook.io/hyperliquid-docs/llms.txt),
[public feeds](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/subscriptions),
[fees](https://hyperliquid.gitbook.io/hyperliquid-docs/trading/fees),
[latency semantics](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/optimizing-latency).
Local collector/connector behavior was checked against these and public responses; no local SDK
or trading connector is imported.

## Known limits

- **Top of book only.** Top of book stands in for the bot's VWAP over 10× the clip; moments with
  less than that at the top are skipped. The bot's `record` command can collect full depth for
  Aster/Lighter candidates; it does not yet record Hyperliquid routes.
- **The gate.** The gate's samples are counted in 0.25 bps bins, from the pair's required edge at
  the cheaper Lighter tier, and it reads whole 5-minute windows: it opens within a bin of the bot's,
  a window later. At the Premium tier its samples are the Standard ones above Premium's edge. The
  bot samples only outside its cooldown and when it could size a trade, so it misses the peaks it
  trades on: on busy pairs its gate sits lower (2026-09-26: TAO 7.25 vs 9.05 bps, WLD 11.4 vs 14.7)
  and the report trades less often than it would.
- **Arrival times.** Replay uses arrival times on this host (Europe), not synchronized exchange
  event times. Venue/feed delays need not cancel; neither ping RTT nor an old local model
  establishes order execution latency. Measured 2026-09-27, venue timestamp to arrival (median /
  p99): Aster book 145 / 239 ms, Lighter trades 214 / 568 ms, Hyperliquid bbo 361 / 682 ms. So a
  Hyperliquid BBO arrived ~0.2 s later than Aster's book in that host's sample; exchange clock
  differences also enter this comparison. Two connections can
  receive the same Hyperliquid frame up to seconds apart. Small Hyperliquid taker edges may be
  partly this lag. The fills at the latency charge for it only in part. Aster also sends each trade
  ~150 ms after the book change it made (Lighter and Hyperliquid send them together), so the
  report moves Aster prints back by `aster_print_delay_ms` before pricing and hedging XEMM fills.
  That is a median: the delay varies by print (aggTrade E − T is 186 ms at the median, 25–50 ms
  for ~13%), so some prints are moved back too far.
- **XEMM scope.** The bot makes only on Aster. Reverse-maker and Lighter/Hyperliquid maker
  rankings are exploratory models.
- **XEMM fills.** XEMM ignores queue position (a fill needs a trade *through* the quote) and our
  own market impact. A held fill's correction is priced when the hedge would have filled, not 6 s
  later (the recording ends 2 s after a print), and an opposite fill that nets it meanwhile is not
  modeled. The distance gate's 1 bps re-arm hysteresis is not modeled either.
- **Stablecoin parity.** USD-equivalent results assume USDT/USDC parity and omit conversion costs.
- **No funding.** Funding is not scored: Lighter's funding sources disagree on units. Check it by
  hand for a candidate pair.

## Files

`data/<YYYY-MM-DD>T<HHMMSS>Z.screen.zst` holds one file per run (a run ends at each UTC midnight,
or when a venue missing at discovery answers).
Each file is zstd-compressed tab-separated lines, with the formats in `src/collect.rs`.
Version 2 uses venue-qualified keys and two-second tails. Existing Aster-Lighter files remain
readable without migration; their one-second tails still limit their own replay settings.
A new run reads the last 72 h of summaries back for its gate.

With Hyperliquid, files take ~60 MB a day for ~155 pairs (18:03–20:07 UTC on 2026-09-26; twice
that in the first hours, while the new pairs' gates warmed up at their floors). Aster-Lighter
alone took ~23 MB for 64 pairs. A burst on one pair can double a 5-minute window. Nothing deletes them:
remove old days by hand, keeping the last 3 for the gate.

The image build runs `cargo test --release --locked` before building the release binary. Checks
cover all three venue combinations, compressed-vs-full replay, units, public frame handling,
restart history, and legacy file compatibility.
