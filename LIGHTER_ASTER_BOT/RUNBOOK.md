# Operating the bot

`lighter_aster_bot run` trades one market in one process. On Aster routes, XEMM quotes;
when an arbitrage passes the taker's entry gate, XEMM pulls its quotes and hands the execution
rights to the taker, which trades and hands them back. `--mode live` trades real money; `--mode dry-run` runs the same bot
against simulated venues fed by live market data ([Dry run](#dry-run)). Commands run from
this directory (`LIGHTER_ASTER_BOT/`) and read `bot.toml`. `run --mode live`, `taker run`
without `--observe-only`, `close`, `probe aster-place-cancel`, `probe hl-hedge`, `probe hl-place-cancel`
and the `*-market`/`*-roundtrip` probes submit real orders.

## How `run` shares execution rights

One regime at a time, like Hummingbot's XEMM with the taker's arbitrage in between:

- **XEMM (normal)** quotes both Aster sides within its inventory cap and hedges on Lighter or
  Hyperliquid (`[maker.live.quote] reduce_position_only = true` would keep only the side that
  reduces the inventory).
- **An arbitrage** that passes the taker's entry gate asks XEMM for the rights. XEMM cancels
  both quotes and grants a 3 s lease once nothing rests or is in flight: no live or uncertain
  quote, and every fill (one caught by the cancel included) hedged.
- **The taker** re-reads its Lighter nonce and an account snapshot with no open order on either
  venue, re-checks the opportunity on current books, trades or skips, and hands the rights back.
  A request not granted within 5 s is withdrawn; a skip or a withdrawal is followed by
  `[taker.arb] cooldown_ms`.
- **XEMM resumes** after re-reading the Lighter nonce and adopting the positions the taker left,
  once two account reads in a row, both started after the hand-back, agree.

On Aster routes, both engines run for the whole session; one that exits halts the bot. XEMM's
journal records each hand-over (`yield`, `yield_withdrawn`, `rights_returned`, `resumed`), and
`bot_stats.py` summarizes them.

The `[controller]` table of `bot.toml` holds the status poll and the cross-engine loss stop. The
controller reads the venues only at start-up; after that it reads the accounts from XEMM's own
snapshot, which XEMM refreshes every 6 s with the shipped settings and every 2 s while a fill,
hedge, sweep or hand-over settles. A taker-only route has no XEMM: the controller reads the
taker's snapshot, refreshed every 15 s (half `[taker.live] max_account_snapshot_age_ms`).

Lighter's Standard account shares one 60/min cap between REST reads and transactions
([rate limits](https://apidocs.lighter.xyz/docs/rate-limits)). The HYPE route's idle reads take
about 26/min of it (XEMM accounts 10, taker accounts 8, book checks 8), and a start about 15 more
(each account read once); executions and other clients use the rest. While it waits for XEMM's
lease, the taker keeps its 15 s pace: its scan skips any snapshot older than 30 s.

## Build, secrets, configuration

```bash
cargo build --release --locked        # Rust 1.92; the Lighter signers exist for Linux and macOS only
```

- `aster.env` and `lighter.env` sit in the working directory (or at `ASTER_ENV_PATH` /
  `LIGHTER_ENV_PATH`), mode `600`, with the keys of `aster.env.example` / `lighter.env.example`;
  `run --mode live` refuses them if group/other can read them. `API_SIGNER` in `aster.env` must be
  the address of `API_PRIVATE_KEY`.
- Every process that signs for the same Aster API wallet must see the same `ASTER_NONCE_DIR`
  (default: the OS temp dir's `lighter-aster-nonces`), writable by the bot user. The nonces
  are clock-based and strictly increasing, so losing the directory at a reboot is safe; never
  delete it while a signer runs.
- `bot.toml` has `[controller]`, `[taker]`, `[maker]` and `[dry_run]`. Unknown, misplaced or
  retired keys fail the load, and so do venue URLs other than the mainnet origins
  (`[maker.live]`, `[taker.venues]`). The mode is a command-line choice only: in either mode,
  `[taker.live] enabled = true, mode = "live"` and `[maker.live] enabled = true` arm the
  engines.
- A market's `hedge_venue` names the second leg, the same in its `[[taker.markets]]` and
  `[[maker.markets]]` entries: `"lighter"` by default (`HYPE`), `"hyperliquid"` for `HYPE-HL`.
  The engines send Hyperliquid IOCs only; resting orders exist only in probes. Live, a
  Hyperliquid route reads `hyperliquid.env` ([Probes](#probes)), and the leverage gate wants 1x
  on both legs: set the Hyperliquid market to 1x cross first.
- A `[[taker.markets]]` entry with `first_venue = "lighter"` has no `aster_symbol` and no
  `[[maker.markets]]` entry, and must hedge on Hyperliquid: `HYPE-LH` takes both sides on Lighter
  HYPE and Hyperliquid HYPE. `run` starts the taker alone, with the taker's own account snapshot
  as the controller's status, once both legs read 1x. Each leg pays its own venue's taker fee.
- A `[[taker.markets]]` entry names its coin only: Lighter's listing gives the market index,
  decimals and minimums, so an order is at least Lighter's minimum base amount and $10.50 on
  either venue. Its `desired_notional` sizes that market's orders in place of `[taker.arb]`'s,
  and its `confirm_ms` (default 0) sends them only once a gap has stayed in view that long, in
  the same direction. Each trade report logs the gap's `held_ms`.
- The taker refuses to start on a delisted Hyperliquid coin, on a coin whose legs' mids are over
  5% apart (two tokens under one name), and on one whose common size step is worth more than
  `max_position_mismatch_usd`: a partial fill could leave a residual that recovery cannot close
  (ZEC-LH: $12).

## Run and stop

Native, in tmux:

```bash
tmux new -s lighter_aster_bot
./target/release/lighter_aster_bot run --market HYPE --mode live 2>&1 | tee -ai runs/bot-HYPE.log
```

Docker, here or on a Linux host (see [Deploy](#deploy)):

```bash
docker compose --profile live run -d --name bot-hype bot run --market HYPE --mode live
docker logs -f bot-hype
```

The bot logs to stdout only; `runs/` holds its journals, ledgers, latches and state. The
stopped container keeps its log for review; `docker rm bot-hype` before the next start.

Stop with Ctrl-C, SIGINT, SIGTERM or SIGHUP (`tmux send-keys -t lighter_aster_bot C-c`,
`docker kill --signal=SIGINT bot-hype`):

- **Drain.** XEMM drains first, then the taker. XEMM quiesces admission, cancels makers, drains
  fills and execution outcomes, corrects net residuals, reconciles and flushes persistence. The
  controller allows 200 s per engine, after any status poll in progress (up to 25 s). Never stop
  it with a shorter kill: `docker stop` needs `-t 460`, and compose already sets
  `stop_grace_period: 460s`.
- **After.** Paired positions stay open and delta-neutral ([`close`](#close-a-position) exits
  them). Exit 0 means a clean stop; nonzero means a halt, an unresolved engine stop or an
  unwritable event log. A panic outside XEMM's strategy thread aborts the process at once, with
  no drain, like a kill.

Only one live writer per market runs at a time: `run --mode live` and `taker run` (unless
`--observe-only`) take the exclusive lock `runs/bot-<MARKET>.lock` and name the holder's pid
on contention. Live, they also lock each leg, `runs/bot-<VENUE>-<SYMBOL>.lock`
(`bot-LIGHTER-HYPE.lock`), so markets sharing a leg (HYPE and HYPE-LH) never both trade it. A
dry run locks `runs/dry-run/bot-<MARKET>.lock`, so it can run beside live.

**After a fill (XEMM).** After a known fill, `POSITION_SNAPSHOT_PENDING` holds new quotes until
account reads begin after that execution; an older read cannot establish a position mismatch or
request a sweep. Hedging, cancels and the exposure limits still apply. A missing-amend reply after a full
private fill does not reopen uncertainty; partial fills or a larger amended size still need
terminal order evidence.

### Close a position

Stop the bot, then close the coin at market on every venue:

```bash
docker compose --profile live run --rm -T bot close --market HYPE --i-understand-live
```

`close` sends REAL reduce-only orders:

- **Scope.** `--market` takes a market id or its coin: `HYPE` and `HYPE-HL` both close HYPE, on
  Aster, Lighter and Hyperliquid at once (`--venue aster,lighter` for some); a taker-only market
  (`CASHCAT-LH`) closes on Lighter and Hyperliquid. It takes each
  venue's leg lock, so it refuses while a live `run` or `taker run` trades there.
- **Orders.** It initializes every selected client before sending anything, so a setup failure
  sends nothing. It then cancels the Aster symbol's open orders and closes at market: an Aster
  MARKET order, and IOCs through XEMM's hedge worker at `emergency_slippage_bps` on Lighter and
  Hyperliquid.
- **Check.** Each venue gets up to three orders, sized from the fills (or from a fresh read after
  an order that filled nothing), and must then read flat, or the command names it and exits
  nonzero. Later read or order failures can still leave a leg open: check the reported results
  and positions.

## Dry run

`run --mode dry-run` is the whole bot, its engines, the controller and every client and
signer unchanged, against the market's two venues simulated in process on loopback: Aster and
Lighter, Aster and Hyperliquid for `HYPE-HL`, Lighter and Hyperliquid for `HYPE-LH` and
`CASHCAT-LH`. The
simulator follows the live public market data and answers in each venue's own protocol. It
needs no credentials: the bot signs with a fixed dry-run identity whose keys exist on no
venue, so a request that escaped to mainnet could not trade. Its files live in
`runs/dry-run/`, which live never touches.

The venue model targets AWS Tokyo timings, with estimates where measurements are unavailable
(`[dry_run]` in `bot.toml` cites each value's source):

- **Time shift.** The simulated world is the live one `shift_ms` (90000) late, timestamps
  included, to absorb this host's feed delay before serving books (sized to its multi-second
  stalls, not its median; see `bot.toml`). A frame that reaches this host later than that is
  applied on arrival and counted in `late_frames`. Orders,
  cancels and deadmen wait until the feed has caught up with their time, so a cancel cannot
  beat prints that were late; one that waits over 2 s is answered as unavailable.
- **Latency.** Each request draws a lognormal round trip from the configured `[p50, p99]`
  and takes effect at `effect_fraction` (0.9) of it. Lighter taker orders wait a further
  `lighter_taker_delay_ms` (300, the Standard account's delay).
- **Takers** fill against the worse of the two book states around their effect time, and
  liquidity the bot took stays gone until the feed shows that level smaller.
- **Makers** wait behind the visible size at their price, plus `hidden_queue_multiplier` times
  it in hidden orders. Prints at their price work through the visible queue, then the hidden
  one, before filling them; the book shrinking only shortens the visible queue. A print through
  their price fills them outright in the model; a crossing book fills up to its crossing size.
- **Venue rules**: live filters, measured reduce-only exceptions, Aster deadman and listen-key
  expiry, Lighter sequential nonces and Hyperliquid five significant figures. Rate limits are
  approximations: separate 60/min Lighter read and transaction buckets (Standard shares one
  and has per-type limits, so the model can miss 429s) and 1200 Hyperliquid weight/min.
- **Accounts**: one cross account per venue from the `[dry_run]` balances. Fees come from the
  bot's own fee keys, so the dry run cannot catch a wrong one. Funding follows the public
  rates at Aster and Lighter settlement times; Hyperliquid funding is not simulated.
  A maintenance-margin breach is reported, not liquidated.

The four dry runs run only on the Tokyo VPS (`../VPS.md`); `python vps.py pull` copies their
files to `runs/tokyo/dry-run/` daily, and the reports read that copy. To run one here by name (the
services sit behind the `dryrun` profile, so `start_all.bat` starts none), from this directory:

```bash
docker compose up -d --build dryrun dryrun-hl dryrun-lh   # HYPE, HYPE-HL, HYPE-LH (taker only)
docker compose logs -f dryrun
```

Natively (Linux or macOS): `./target/release/lighter_aster_bot run --market HYPE --mode dry-run`.

**Restarts.** It stops like live (SIGINT, a drain, positions stay open). The simulator saves
the venues every second and on a clean stop; an unclean stop loses the changes since the last
save. The next start takes up their accounts, positions and resting orders in a market that has
moved: the first book fills any resting order it crosses, an expired Aster deadman cancels, and
funding that fell due is charged. The start also archives the engines' unclean-session markers
(left by a kill or an unresolved engine stop) as `<name>.unclean.<stamp>`, and the engines
reconcile to the simulated venues' state, the only record here; live keeps those markers until
an operator has resolved the session against the venues' records. Docker treats `docker kill` as
a deliberate stop and does not restart the container; `docker compose up -d dryrun` does.

**Starting afresh.** Stop it and move `runs/dry-run/` away. Moving only `sim-<M>.state.json`
would leave the drawdown baseline measuring the reset accounts. A fresh directory also restarts
the taker's entry-gate history: as after a fresh live start, the taker trades only once it has
recorded `min_history_samples` (50) opportunities above its required edge
(`[taker.arb.entry_gate]`).

**Halts.** A halted dry run parks: the process, the simulated venues and the diagnostics keep
running until it is stopped, so the restart policy cannot loop on a halt. Every start passes
`--ack-breaker`, so any restart resumes it: `docker compose restart dryrun` after a review, but
also a host reboot, or `start_all.bat` recreating it on a new image. The halt stays in
`bot-<M>.events.jsonl` and the archived breaker, so check them after an unplanned restart. An
equity-drawdown halt parks again, asking for `--reset-breaker-baseline`:

```bash
docker compose stop dryrun
docker compose run --rm dryrun run --market HYPE --mode dry-run --ack-breaker --reset-breaker-baseline
# Ctrl-C once it has started, then resume in the background:
docker compose up -d dryrun
```

The engines' own loss latches ([Halts and recovery](#halts-and-recovery)) have dry-run
resets: `python scripts/reset_breaker.py --runs-dir runs/dry-run --coin HYPE` for XEMM, and
`docker compose run --rm dryrun taker reset-circuit-breaker --market HYPE --dry-run` for the
taker. Without `--dry-run` that command resets live's breaker.

**Diagnostics.** Every minute the simulator logs a one-line gist and appends a row to
`runs/dry-run/sim-<M>.diag.jsonl`, per venue (quantiles are `{n, p50, p90, p99, max}`):

| Field | Read it as |
|---|---|
| `late_frames` of `frames` | Frames later than the shift. Keep them under 1 %, or raise `shift_ms`. |
| `lag_ms` (`book`, `top`, `trade`) | Arrival minus exchange time, clock skew included. The p99 must stay under `shift_ms` − 250 (the lookahead that finds the later book state). |
| `stale_frames`, `gaps` | Out-of-order book frames, and upstream breaks. A gap closes the bot's streams, as the venue would, and orders are rejected `Unavailable` until the next snapshot. |
| `held` | Orders, cancels and deadmen that waited for a late feed. |
| `lateness_ms` (whole row) | Delayed event processing. CPU contention or host suspension can contribute; stalls past the shift also make late frames. |
| `rtt_ms`, `private_ms` | The latencies drawn. |
| `requests`, `orders`, `rejects` | Rejects by reason. `RateLimited` means the bot exceeded a modeled limit; compare it with the venue's actual rules. |
| `maker_fills`, `taker_fills`, `queue_ahead`, `maker_wait_ms` | Fills, the queue ahead of each order that came to rest, and each maker fill's wait since placement. |
| `prints`, `prints_inside_spread`, `prints_over_visible` | Trades the visible book cannot explain. Hidden orders, between-frame changes and feed delay can all contribute; these counts cannot identify `hidden_queue_multiplier`. |
| `account` | Balance, unrealized, equity, realized, fees, funding, positions, maintenance breach. |

Simulator warnings start with `dry-run`. `no route`, `no websocket` or `not simulated` means
the bot used something the simulator does not serve, so the dry run no longer matches live:
treat it as a bug. The reports take `--dry-run` (`python3 ../combined_pnl.py --dry-run`;
`python3 ../trade_history.py --dry-run` keeps its own database in `runs/dry-run/`).
`python3 ../bot_stats.py --dry-run` summarizes these diagnostics together with the trades'
edge kept and slippage per leg.

For an isolated live/paper comparison, `bot_stats.py --runs DIR` reads another runs directory;
its docstring defines each metric. Simulator diagnostics use nearest-rank percentiles.

What the dry run cannot tell:

- whether the maker fee keys are right (the taker keys matched the live probes);
- the bot's market impact beyond the liquidity it takes;
- how Aster's ~100 ms splits around matching (assumed pessimistically);
- anything about liquidation;
- whether Lighter signatures are valid (the simulator does not verify them).

Live timings and the calibration trials are in [CALIBRATION.md](CALIBRATION.md).

## Going live

Live runs in the `bot` container, on this Windows host or a Linux host ([Deploy](#deploy)). Live
refuses credential files that group or other can read: the `bot` container's entrypoint checks
the host's env files, then copies them at 0600 into a memory-only tmpfs of the bot user and
points `*_ENV_PATH` there. Docker Desktop shows every Windows bind mount as mode 777, so on this
host put `XEMM_ENV_MODE_CHECK=off` in the `.env` beside `docker-compose.yml`.

Before the first start:

1. The dry run has run for days with no unexplained reject, halt or `no route` warning, and
   its reports agree with the simulated equity net of funding and open-position marks.
2. The fee keys in `bot.toml` match both accounts' actual tiers.
3. On a VPS, ship the sources, the secrets and the live image with `scripts/deploy_vps.sh`
   ([Deploy](#deploy)); on the Tokyo VPS, `python vps.py live creds` then `live start`
   ([VPS.md](../VPS.md)); here, `docker compose --profile live build bot`.
4. On the host, the route's read-only probes pass ([Probes](#probes)); both legs are at 1x
   cross, and Aster routes use one-way position mode.
5. Neither venue has open orders, and positions are flat or paired.
6. `runs/` holds no latch from an earlier run (`bot-<M>.breaker.json`, `*.trip.json`,
   `circuit_breaker_<M>.json`): each engine checks its own when it starts. `run` itself refuses to start on an engine's unclean-session
   marker (`*.active.json`, `active_session_<M>.json`).
7. Start it as in [Run and stop](#run-and-stop). The controller reuses its persisted drawdown
   baseline; XEMM's re-arms ([Halts and recovery](#halts-and-recovery)).

## Market data tape

`record --market HYPE` records HYPE's public feeds for [backtests](#backtest) into `data/HYPE/`.
It runs on the Tokyo VPS as `xemm-recorder` ([VPS.md](../VPS.md)), since a stall on this PC's
route to Tokyo is a hole in the tape, and `python vps.py pull` copies its files to
`data-tokyo/HYPE/` here daily. This PC's own tapes, up to 2026-10-10, stay in `data/HYPE/`
(compose service `recorder`, behind the `recorder` profile).

- **Aster:** `depth20@100ms` (20 levels), `bookTicker` and `aggTrade`.
- **Lighter:** `order_book` (a snapshot, then deltas: the whole book), `trade` and `market_stats`.
- **Hyperliquid** (from 2026-10-10): `l2Book` fast (5 levels, ~0.54 s), `bbo`, `trades` and
  `activeAssetCtx` on one connection; `l2Book` slow (20 levels, ~5.4 s) on another, since its
  frames do not say which mode they are.
- **REST:** Aster `premiumIndex` every minute; `exchangeInfo` and `orderBooks` hourly.

Every line carries its arrival time on the recording host (the VPS, NTP-synced) beside the venues'
own stamps, so the tape holds that host's feed latency too. The recorder is its own process, so
the dry run never waits on it; `python vps.py sync` rebuilds and restarts it (a new file).

**Files.** `data-tokyo/HYPE/<YYYY-MM-DD>T<HHMMSS>Z.tape.zst` (`data/HYPE/` before 2026-10-10),
one per UTC day and per recorder start, named after the first line's arrival: tab-separated
`<arrival µs>\t<kind>\t<payload>`, with the kinds listed in `src/dryrun/tape.rs`. Read a day with
`zstd -dc data-tokyo/HYPE/<day>T*.tape.zst | head`.
- The writer flushes every 10 s; an unclean stop can lose unflushed data.
- `A-`, `L-`, `H-` and `HS-` lines mark a lost connection. Reconnect attempts back off up to 5 s;
  connection and subscription time add to the recovery delay.
- Aster has no order-book history to download again, and the fleet backup skips `.zst` files
  (MAKE_BACKUP.py), so copy the tapes elsewhere if they must survive a disk loss.

## Backtest

`replay` is the dry run fed by the tape instead of the live market: the same bot, simulator and
`[dry_run]` latency model. Its world is shifted by the days since `--from` plus a 2-minute lead
(the run's `shift_ms` in `replay.json`, in place of the dry run's 90 s): each line goes in
2 minutes ahead of its venue time (the tape's worst feed stalls were 75 s), so this host's stalls
on the day do not reach the bot. It reads `data-tokyo/<M>/` once a Tokyo file starts by
`--from`, else `data/<M>/` (`--tape` overrides; `replay.json` lists the files used). The `replay`
service has no network at all and no credentials.

```bash
docker compose run -d --rm --name replay-0928 replay replay --market HYPE --from 2026-09-28T00:00:00Z --until 2026-09-29T00:00:00Z
docker logs -f replay-0928
python ../dryrun_summary.py --runs LIGHTER_ASTER_BOT/runs/replay/HYPE-20260928T000000Z-20260929T000000Z-k1-standard-s0
```

Always `--rm`: the container goes when the replay ends, and its log is copied into the run
directory as `replay.log` (all but a failure to open the tape at startup, ~2 s: rerun that one in
the foreground).

- **Pace.** It runs at the market's own pace: a day takes a day, after ~2 minutes of warm-up,
  at ~60-75 MB and a few % of a core (2026-10-20). Speed comes from running windows side by side,
  on free ports: faster than real time does not work (libfaketime at x10 changed the bot's
  behavior: Aster rejects 4/min vs 0.2, Lighter rate-limited).
- **Start and end.** Its start reads the tape up to `--from` and replays the Lighter book from its
  last snapshot (2026-09-28 06:00: 9.7 hours of updates, a minute). It ends by itself: the bot
  drains once its clock passes `--until` while the tape keeps feeding, or once the tape ends, two
  minutes of market ahead of the bot.
- **Host load.** It runs on wall time, so a busy host delays the simulated venues as it delays the
  bot: the summary's `scheduler` line should read a few ms (2026-10-10: 0.2-1.6 s p99 while heavy
  jobs shared the CPU, 2 ms after).
- **Variants.**
  - `--latency k` multiplies every network and stream delay of `[dry_run]` (round trips, feeds,
    user/account streams; `0`: none), not Lighter's 300 ms taker delay, a venue rule.
  - `--lighter premium` is Lighter's Premium account: 140 ms taker delay and 2.8 bps taker fee,
    which the bot decides with (taker required edge 4 + 2.8 + 2 = 8.8 bps, XEMM's hedge fee) and
    the simulator charges. Its 0.4 bps maker fee is never charged (the bot rests no order on
    Lighter), and its higher request limit is not modelled (Standard's cost 2 requests in 2 h on
    2026-09-28).
  - `--seed` overrides `[dry_run] seed`; `--config` takes a scratch copy of bot.toml (e.g. another
    `hidden_queue_multiplier`) and adds its name to the run's directory.
- **Files.** Each run gets a new
  `runs/replay/<M>-<from>-<until>-k<k>-<standard|premium>-s<seed>[-<config>]/` (an existing one is
  refused), so no simulated account, latch or deadman carries over.
  - `replay.json` holds what its numbers depend on: window, shift, tape files, k, account type,
    seed, the effective `[dry_run]`, the fees, the gate seed and the commit (`unknown` unless the
    image was built with `GIT_COMMIT=$(git describe --always --dirty) docker compose build`).
  - The bot's files are in its `dry-run/`. `bot_stats.py`/`combined_pnl.py --runs <run>/dry-run`
    default to its window, and `combined_pnl`'s `by_day` gives net per UTC day of the market (its
    times less the run's `shift_ms`).
- **Gate.** The entry gate starts from the samples, at or above the run's required edge, that its
  bot would have logged in the 72 h before `--from` (Premium's are fewer); below 50 the taker
  stays in warm-up. It reads them, read-only, from the dry run that ran beside the tape's
  recorder: `runs/tokyo/dry-run/opportunities_<M>.jsonl` (the VPS's) for a `data-tokyo` tape,
  else this PC's `runs/dry-run/`; `--gate <file>` overrides. The live sampler was frozen
  2026-10-02 to 10-09, so replay that week as one window, not per day.
- **Limits.**
  - HYPE Aster-Lighter only: the tape's Hyperliquid lines (2026-10-10 on) are not replayed yet.
  - A recorder restart without `A-`/`L-` lines freezes the feeds (the replay warns about silences
    over 30 s), and a Lighter sequence break (a dropped line) closes the book until the next
    snapshot, which the tape has only after a reconnect.
  - This PC's tapes (`data/`) have holes where its connection was down, and those replay as books
    rejected `Unavailable`: 2026-10-01 has 78 minutes of Lighter, 2026-09-28 11 of both venues
    (14:39-14:49), the same in every run of the day.
  - The simulator's `lag_ms` and the first window's `late_frames` measure the shift and the
    catch-up, not feed health.
  - Latencies are drawn with the seed but the bot runs on this host's scheduler, so two runs of a
    window differ: compare them as distributions.

## Runtime files (`runs/`)

| File | What |
|---|---|
| `bot-<M>.events.jsonl` | Controller events: engine starts and stops, loss samples, network pauses, halts |
| `bot-<M>.state.json` | Who holds the rights, loss stops, positions, accounts; read by `combined_pnl.py` / `trade_history.py` |
| `bot-<M>.breaker.json` | Controller halt latch |
| `bot-<M>.baseline.json`, `bot-<M>.equity.jsonl` | Equity-drawdown baseline and samples |
| `bot-<M>-journal.jsonl` | XEMM execution journal (its `"lighter"` venue is the hedge leg, Hyperliquid on `HYPE-HL`) |
| `bot-<M>.trip.json`, `bot-<M>.active.json` | XEMM loss latch and unclean-session marker |
| `bot-<M>.residual.json` | Legs XEMM left open at its last stop (a report, not a latch) |
| `trades_<M>.jsonl`, `executions_<M>.jsonl`, `opportunities_<M>.jsonl` | Taker ledger, execution log and entry-gate history (their `aster_*` fields are the first leg, named by `first_venue`) |
| `active_session_<M>.json`, `circuit_breaker_<M>.json` | Taker unclean-session marker and loss breaker |

The taker's observe-only history still feeds `opportunities_<M>.jsonl`, as live history
collection does. An archived latch keeps its name plus a timestamp (`.acked.`, `.cleared.`,
`.resolved.`, `.unclean.`) and no longer blocks. A dry run writes the same files in `runs/dry-run/`, plus
the simulated venues' `sim-<M>.state.json` and `sim-<M>.diag.jsonl`.

## Halts and recovery

**Controller halt.** The bot stops both engines (XEMM first, then the taker), writes
`bot-<M>.breaker.json` with the reason, and exits nonzero. The reasons are:

- the cross-engine loss stop (`pnl_breaker`), on either of:
  - marked equity at or below the baseline minus `max_loss_usdc` (15); the baseline is kept
    across restarts unless no sample has refreshed it for `baseline_max_gap_hours` (48);
  - realized trade PnL of both engines since this start at or below −15.

  Unverified gains never count; unverified losses do. The engines' own stops (10) normally trip
  first.
- resting orders at startup (`startup_orders_not_clear`);
- an engine that exits, with an error or not (`engine_exited`).

An engine that fails its drain or does not stop within 200 s makes the exit nonzero
(`bot_stopped` carries the error).

**Network outage.** Three unreadable statuses in a row (45 s) are treated as a lost network, not
a halt. A status is unreadable while the account snapshot it reads is too old: XEMM's older than
`poll_sec`, or on a taker-only route the taker's older than `max_account_snapshot_age_ms` plus
`poll_sec`. The bot then emits `network_pause`, and no engine opens new exposure (no taker entry,
XEMM quotes pulled). In-flight executions, hedges, corrections and the Aster deadman carry on,
and the taker asks for no rights. The loss stops still run on every readable status. After 4
readable statuses in a row (about 60 s, restarting at any failure) it emits `network_resume` and
trades on; no restart is needed. An order in flight when the network drops can still end the
engine with an unresolved execution, which halts as `engine_exited`.

Review `bot-<M>.events.jsonl` and the breaker, then restart with `--ack-breaker`, which
archives it as `.acked.<stamp>`. An equity-drawdown breaker also needs
`--reset-breaker-baseline`, which re-arms the drawdown stop on the next sample. The
engines' own latches below are separate and each blocks its engine on its own.

**XEMM.**

- The unclean-session marker `bot-<M>.active.json` remains after an unclean or unresolved
  session, and no tool resolves it. Once the venues' own records resolve every attempted order in
  the journal, and orders and positions are reconciled (an empty orders snapshot alone is not
  enough), archive it by hand: `mv bot-<M>.active.json bot-<M>.active.json.resolved.<stamp>`.
- The loss latch `bot-<M>.trip.json` trips when marked equity falls `max_cumulative_loss_usdc`
  (10) below the median of the first 5 fresh samples, on 3 consecutive samples; that baseline
  re-arms at every XEMM start. After review, clear the latch (not an unresolved session) with
  `python scripts/reset_breaker.py --coin <M> --archive`.

**Taker.** The session marker `active_session_<M>.json` is armed before execution rights are
granted, and only a verified, drained shutdown retires it. After an unclean exit, when the
marker holds complete scoped order identities:

```bash
./target/release/lighter_aster_bot taker resolve-session --market HYPE
./target/release/lighter_aster_bot taker reset-circuit-breaker --market HYPE
```

- `resolve-session` needs an inactive owner, matching terminal orders, positions consistent
  with those fills, and no open orders; it saves `session_resolution_*.json`. It refuses a marker
  from a crash before any receipt, without enough identities, or whose Lighter order has left the
  newest 200 rows of the account's order history: resolve that one from the venues' own records,
  then archive it by hand as XEMM's.
- The loss breaker `circuit_breaker_<M>.json` trips when the ledger's PnL since
  `[taker.pnl] since` reaches −`max_loss_usdc` (10), on the same terms. `reset-circuit-breaker`
  archives it only: it neither resolves a session nor rewrites the ledger, so the breaker returns
  at the next start while that PnL is still at or below the limit.

## Engines

**Taker.** It prices configured depth in both directions on the route's two venues.
A clip trades only when the depth-weighted edge clears both taker fees plus the
margin, both books hold `liquidity_multiple` times the clip within `max_levels` and are
fresher than `max_book_staleness_ms`, and the edge passes the entry gate: the greater of the
90th percentile of recent samples and the required edge plus `min_extra_bps`, blocking during
history warmup. Its Aster book is the `depth20@100ms` snapshot with the newer `bookTicker` top
laid over it (by update id): depth alone lags the top by up to 100 ms. Aster orders are bounded
IOC limits; Lighter uses its native market/IOC path and Hyperliquid bounded IOCs.

An unknown submission outcome keeps its order and client ids: a missing order row or a flat
position does not prove no fill. A known missing hedge gets one retry within
`hedge_retry_timeout_ms`, bounded `hedge_retry_slippage_bps` off the venue's current REST mid
(the missed order's decision-price bound would miss again); one that ends unfilled hands over at
once, and a filled one has another `hedge_retry_timeout_ms` to show in the positions. Recovery then closes only the same-sign net residual, and an unresolved retry or close
stops further submissions. The ledger's `actual_net_usd` is
matched spread capture minus fees, and a recovery row prices the failed execution's and its
closes' fills the same way. It falls back to a conservative equity-delta estimate when a fill is
unknown, and that estimate also moves with the held inventory's marks.

**XEMM.** Stale books block new exposure. A timeout, a balanced position snapshot or an empty
open-order list never proves that an order did not execute: an unresolved attempt stays
reserved, and after 60 s new exposure freezes while late evidence is still accepted. A net
residual is corrected reduce-only, at most twice per incident. Hyperliquid partial closes
must meet its minimum, so sizing rounds up to the buffered minimum or the whole held position;
any excess is corrected on the other leg after a fresh position confirmation. Aster and Lighter
accept sub-minimum reduce-only closes in the measured cases. The margin guard reserves margin
for resting makers and hedge obligations above the per-venue buffers ($26 shipped); reductions
stay possible.

## Deploy

A live VPS does not compile (unlike the Tokyo dry-run VPS, which builds natively: `../VPS.md`).
Build the image locally and ship it:

```bash
export VPS_HOST='ubuntu@<host>' KEY="$HOME/.ssh/<deploy-key>.pem"
scripts/deploy_vps.sh source     # bot.toml, compose, sources, signers -> ~/LIGHTER_ASTER_BOT
scripts/deploy_vps.sh secrets    # aster.env + lighter.env, chmod 600 (once)
scripts/deploy_vps.sh image      # docker build here, docker save | ssh docker load
```

The secrets step copies Aster/Lighter only. For a Hyperliquid route, separately place
`hyperliquid.env` in the remote working directory with mode `600`.

On the VPS, match the container user to the host user, and create the output and nonce dirs:

```bash
export XEMM_UID="$(id -u)" XEMM_GID="$(id -g)" ASTER_NONCE_DIR=/tmp/lighter-aster-nonces
mkdir -p runs "$ASTER_NONCE_DIR" && chmod 700 "$ASTER_NONCE_DIR"
docker compose --profile live run --rm bot probe aster-balance        # signed reads, no orders
docker compose --profile live run --rm bot taker probe --market HYPE
```

Compose mounts this directory read-only (config, signers, env files), `runs/` read-write and
the nonce dir at `/nonce`. It never restarts the live bot: a halt stays halted until reviewed.

## Probes

- **Read-only:** `probe aster-balance | aster-positions | aster-open-orders | leverage |
  lighter-balance | lighter-open-orders | hl-balance` and `fetch-specs`; the JSON `status` (XEMM
  routes) and `taker probe` / `taker status` (Aster/Lighter only: use `probe hl-balance --market
  HYPE` for Hyperliquid account reads). `taker run --markets HYPE --observe-only` scans and
  records entry-gate history without orders, and `probe lighter-order-dry-run` signs IOC and
  native market plans without submitting them.
- **Real orders:** run these only with explicit approval. Each needs `--i-understand-live`, a flat
  start and no open orders, takes its venue's leg lock (so it refuses beside a live `run`), prints
  each step's latency, and ends by checking flat with no orders.
  - `probe aster-place-cancel`: XEMM's Aster calls on post-only orders ~2 % from the book,
    with XEMM's user stream running: place; amends through XEMM's own Aster worker (to far
    prices, to the same values, through the ask, with a lapsed permit, and of the cancelled
    order, which must close the slot); a post-only through the ask; cancel-all; the 10 s
    dead-man. A position left over is closed reduce-only.
  - `probe lighter-market --max-usd 13` / `probe hl-hedge --market HYPE-HL --max-usd 14`:
    XEMM's hedge worker on Lighter / Hyperliquid sends a hedge, an IOC that cannot fill
    (printing whether its reject reads as the retryable no-fill), and reduce-only closes.
    The rounded minimum plus two size steps must fit the cap, or no order is sent.
  - `taker aster-market-roundtrip --max-usd 7` / `taker lighter-market-roundtrip --max-usd
    12`: the taker's entry order, after one bounded under the bid that cannot fill. The Aster
    one closes with XEMM's reduce-only MARKET. They clean
    up reduce-only (at most three closes in 30 s) and stay blocked without terminal-order and
    flat-position evidence.
- Hyperliquid reads `HYPERLIQUID_ENV_PATH` (default `hyperliquid.env`, keys as in
  `hyperliquid.env.example`: `wallet_address` = the traded subaccount, `private_key` = its agent
  key, `is_vault`). `docker compose --profile live run --rm bot probe hl-balance --market HYPE`
  reads the account, fees and action budget. `probe hl-place-cancel` (one post-only buy 10 %
  under the bid, cancelled) and `probe hl-market` (a ~$10.5 IOC buy sold back reduce-only) trade
  real funds: both need `--i-understand-live --max-usd <10.5..20>`, a flat start and no open
  order. Actions spend the address's allowance (initial 10k + ~1 per USDC traded).
  The engines do not implement a Hyperliquid dead-man refresh.
