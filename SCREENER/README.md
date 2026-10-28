# Pair screener

Which Aster-Lighter, Aster-Hyperliquid or Lighter-Hyperliquid pairs suit the bot's two
strategies? The screener answers from public data. It needs no credentials, sends no orders, and
runs apart from the bot, with its own crate, image, container and data.

<!-- ft-facts: container=aster-lighter-screener image=aster_lighter_screener:latest -->

- **`collect`** (`xemm-screener` on the Tokyo VPS) follows best bid/offer and trades, subscribing
  once per venue instrument and sharing updates between pairs. It records only the moments the
  report could trade on, plus 5-minute summaries per pair:
  - **taker:** an edge reaching the bot's entry gate (the 90th percentile of its samples over 72 h),
    whose samples the summaries count;
  - **XEMM:** a trade that could have filled a quote at the lowest edge scored (5 bps), with the
    second before it (what the quote was priced from);
  - each moment keeps two seconds after it (where latency-delayed orders fill).
- **`report`** applies a simplified model of the bot's rules and ranks the pairs.
  - **Taker-taker:** the entry gate (also at least the required edge plus 0.5 bps, the bot's
    `min_extra_bps`), cooldown, inventory cap and each leg's latency. `confirm_ms` (`[report.taker]`,
    0 by default) holds a gap as the bot's per-market setting does, on the hedge venue's stamps
    (arrival replay of v3+ data, so not with `--scenario`); the recording shows a gap first at the
    start of its moment, so the hold may end later than the bot's.
  - **XEMM:** the quote price, a fill only when trades print through the quote (those of one ms
    add up), the hedge latency and the distance gate (skipped behind a maker top thinner than the
    bot's depth). Both sides quote a clip, as the bot (`reduce_position_only = false`); the side that
    would grow the inventory past $161 (the bot's cap under its margin guard at the dry run's $200
    accounts; a live account's free margin is not modelled) is full. After a fill the market pauses
    3 s. A fill under the hedge venue's $10 minimum is held, then corrected by a taker order (on the
    maker venue if it grew the inventory), and the market pauses 6 s more.
  - Both close their leftover inventory at the last window's mean basis, without fees or spread: a
    standing basis is paid back, not earned.

  Fees, latencies and thresholds apply at report time (`screener.toml` `[report]`). Collection
  follows `[report]` at its loosest, so one recording answers Standard vs Premium, latency within
  its recorded pre-roll/tail, or a higher margin, gate percentile or XEMM edge. The report refuses
  settings that would trade on moments the data did not record (a lower percentile, another depth
  or gate window). A test checks that the recording keeps every trade the report would make on the
  full stream.

- **Pairs.** Each venue combination is matched independently: names (including verified
  `kPEPE`/`1000PEPE` scales), prices within 2%, and at least $200k 24 h volume on both venues. The
  100-pair limit is **per combination**. Hyperliquid covers native perpetuals only; spot and
  builder/HIP-3 markets are excluded. Pairs refresh at UTC midnight.
- **Outages.** A venue unavailable then (or at boot) leaves the combination of the other two
  running, is retried every 5 minutes, and rejoins in a new run as soon as it answers. A silent
  connection is dropped after 30 s and its books are unknown until it returns. Reconnect backoff
  is capped at 5 s, with connection/subscription time added: an outage records a gap, except its
  first 30 s, which keep the last prices (a 60 s cut, 2026-09-26: 33 s down, every pair back
  within seconds).
- **Restarts.** After a reboot or a crash, systemd restarts it on the VPS (Docker's
  `unless-stopped` here). The writer flushes every 30 s; an unclean stop can lose queued or
  unflushed data.

Hyperliquid uses public `bbo`, `l2Book` and `trades`. BBO gives the faster touch; L2 confirms an
unchanged book. `l2Book` is subscribed `fast` (5 levels, every ~0.54 s instead of 20 every
~5.4 s; from 2026-09-28 on, earlier days carry the slow one). Historical trades received on
subscription are excluded using the first book's exchange timestamp; subsequent duplicates use
`(coin, time, tid)`. A disconnected or 30-second
stale Hyperliquid book invalidates only its own pairs. No credentials or order calls are involved.

## Run

```bash
docker compose up -d --build screener               # collect on this PC: never while the Tokyo VPS does
docker compose logs -f                              # traffic and memory every 5 min
docker compose run --rm screener universe           # today's pairs, with their Aster taker fee
docker compose run --rm report                      # the ranking of data/ (this PC's, to 2026-10-10)
docker compose run --rm report --since 2026-09-27 --lighter premium --json
docker compose run --rm report --latency 2          # sensitivity: every latency doubled
docker compose run --rm report --data data-tokyo --since 2026-10-27 --scenario tokyo   # venue clocks
```

`report` is its own service (profile `report`, never started by `up`): it holds every recorded
state of the days it scores in memory, ~75 bytes a row: 1.4 GB for one Tokyo day (2026-10-17),
and 2026-10-12 to 10-18 (~236 M lines) went over 16 GB. Parsing/replay buffers add to peak memory,
so the history that fits under the 8 GB cap varies with market activity. `--pair-groups N` reads
the files N times, scoring 1/N of the pairs each time: the same report in N times the time, with
less memory (10-17 at N=3: 0.67 GB instead of 1.36). `screener_summary.py` picks N itself.

## Reading the report

- **Pair / direction.** `A-L`, `A-H`, `L-H` identify the venues; A=Aster, L=Lighter, H=Hyperliquid.
  `0` is the left venue, `1` the right. `xf0` means maker on 0, hedge on 1; `xf1` reverses them.
- **Units / coverage.** $/day at one $13 clip, divided by the time both books were known.
  `days` and `down%` show coverage; `unres` counts fixed-strategy fills with an unknown delayed
  book or one not recorded, whose PnL cannot be estimated. JSON includes each leg's fees/delays,
  daily results and the taker's split by direction (with the gaps `confirm_ms` skipped).
- **`tk`.** The existing gated taker, requiring 50 opportunity samples before leaving warmup.
  `kept` is realized/expected gross edge after latency.
- **Fixed ranking.** `xf0` / `xf1` use the 18.5 bps required edge and 18–50 bps distance gate.
  `best fixed` selects between these and taker. `+days` counts positive daily results.
- **Exploratory sweeps.** `sweep0` / `sweep1` choose the best required edge (`req`) on these same
  data without the distance gate. These fitted results are displayed separately from the ranking.
- **Comparison.** Use the same complete UTC dates (`--since` / `--until`), after warmup, with at
  least seven concurrent days as an initial route-review window; this does not establish rank
  stability or profitability. Compare
  `--latency 0.5`, `1`, and `2`, and Standard/Premium Lighter. Routes are alternatives, not additive
  portfolio profits. Day-to-day rank correlation needs at least two days.

Hyperliquid defaults to **1.5 bps maker / 4.5 bps taker**, the undiscounted native-perp base tier.
The **500 ms execution** is close to the live bot's Tokyo IOCs (filled p50 518 ms after their send,
2026-10-27: `LIGHTER_ASTER_BOT/CALIBRATION.md`); the **250 ms fill notice / 500 ms quote age** are
provisional assumptions, not measurements. All are configurable in `[report.hyperliquid]`.
Lowering fees below a recording's taker floor can expose unrecorded opportunities: the report
rejects that rather than silently overstate coverage.

Current references checked 2026-09-26: [full documentation index](https://hyperliquid.gitbook.io/hyperliquid-docs/llms.txt),
[public feeds](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/subscriptions),
[fees](https://hyperliquid.gitbook.io/hyperliquid-docs/trading/fees),
[latency semantics](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/optimizing-latency).
Local collector/connector behavior was checked against these and public responses; no local SDK
or trading connector is imported.

## Known limits

- **Top of book only.** Top of book stands in for the bot's VWAP over 10× the clip; moments with
  less than that at the top are skipped. The bot's `record` command collects full depth (Aster,
  Lighter and Hyperliquid) for a market a bot trades; it runs on the Tokyo VPS for HYPE only.
- **The gate.** On busy pairs the report trades less often than the bot would: the bot samples
  only outside its cooldown and when it could size a trade, so it misses the peaks it trades on
  and its gate sits lower (2026-09-26: TAO 7.25 vs 9.05 bps, WLD 11.4 vs 14.7). The report counts
  its samples in 0.25 bps bins from the pair's required edge at the cheaper Lighter tier and reads
  whole 5-minute windows, so its gate opens within a bin of the bot's, a window later. At the
  Premium tier its samples are the Standard ones above Premium's edge.
- **Arrival times.** By default the replay uses arrival times on the recording host (the Tokyo
  VPS for `data-tokyo/`, this PC in Europe for `data/`), not the venues' clocks.
  - Aster sends each trade ~150 ms after the book change it made (Lighter and Hyperliquid send
    them together), so arrival replay moves Aster prints back by `aster_print_delay_ms`, a median
    (aggTrade E − T is 186 ms at the median, 25–50 ms for ~13%).
  - Feed delay can make or hide edges, small Hyperliquid taker ones especially; two connections
    can receive the same Hyperliquid frame up to seconds apart. Venue/feed delays need not cancel,
    and neither ping RTT nor an old local model establishes order execution latency. This PC's,
    venue timestamp to arrival (median / p99, 2026-09-27): Aster book 145 / 239 ms, Lighter
    trades 214 / 568 ms, Hyperliquid bbo 361 / 682 ms; its path to Tokyo also stalls for tens of
    seconds now and then (2026-10-10).
- **Venue clocks (v4 data, 2026-10-10 on).** Rank on the Tokyo recording, not on a scenario of
  this PC's (the check below).
  - `--scenario <name>` replays on the venues' engine stamps: the bot sees each venue a constant
    `[report.scenario.<name>]` delay after its stamp, its orders and hedges meet the venues' own
    books, and a print sits at its stamp. `--scenario host` rebuilds this host's own delays from
    the stamps: the arrival result, a check. Every report prints this host's feed delays per venue
    (books, prints). Delays are constants: jitter is second order. Exchange clocks may differ by a
    few ms.
  - A scenario leaves out, and counts, the time around a stall here: a book change over 0.7 s
    late, plus the 1 s pre-roll either side (on 2026-10-10, at least one in two thirds of the
    5-minute pair windows). `days` and the $/day rates leave that time out too. It refuses delays
    more than 0.3 s apart; a quote older than the pre-roll before its print arrived meets an
    unrecorded book, an `unres` fill (the Hyperliquid quote age at `--latency 2`). The gate's
    samples stay those counted at arrival here.
  - Checked 2026-10-10 12:18–21:28Z against the Tokyo recording of the same hours: this PC's data
    scored as recorded came closer to Tokyo than with `--scenario tokyo` (best $/day rank
    correlation 0.67 vs 0.46; XEMM fills 0.96 vs 0.80). The scenario lost most Hyperliquid-route
    PnL (stalls left out 20% of their time; 468 `unres` fills), while Aster-Lighter agreed either
    way.
- **Run starts.** A new run (daily at UTC midnight, or a restart) does not see the states before
  it: XEMM fills in its first second priced from them are not recorded.
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
or when a venue missing at discovery answers): zstd-compressed tab-separated lines, with the
formats in `src/collect.rs`. A new run reads the last 72 h of summaries back for its gate. Older
versions stay readable without migration:

- **v4** (2026-10-10, current) adds each summary's stall spans; scenarios need it.
- **v3** (2026-10-10 11:41Z) adds each leg's age against the venue's engine stamp to `B` and `T`
  lines, and `R` marks where an unrecorded change makes a leg unknown, for replay on venue time.
  It also selects the moments that any mix of the two legs' states within the pre-roll could
  trade, as feed delays other than this host's could produce.
- **v2** uses venue-qualified keys and two-second tails. Earlier Aster-Lighter files' one-second
  tails still limit their own replay settings.

The collector runs permanently on the Tokyo VPS, whose feeds do not stall (VPS.md), and
`python vps.py pull` copies its closed files to `data-tokyo/` daily. `python screener_summary.py`
scores `data-tokyo/` by default (`--local` for `data/`); the report reads either with `--data`.

The Tokyo recording takes 0.09–0.37 GB a day (median 0.24, 2026-10-12 to 10-24); a burst on one
pair can double a 5-minute window. On the VPS, `pull` deletes a file once copied and 7 days old;
nothing deletes the copies here or this PC's own `data/`: remove old days by hand, keeping the
last 3 for the gate.

The image build runs `cargo test --release --locked` before building the release binary. Checks
cover all three venue combinations, compressed-vs-full replay, units, public frame handling,
restart history, and legacy file compatibility.
