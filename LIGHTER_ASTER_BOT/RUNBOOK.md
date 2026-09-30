# Operating the bot

`lighter_aster_bot run` trades one market with both engines in one process. XEMM quotes;
when an arbitrage passes the taker's entry gate, XEMM pulls its quotes and hands the execution
rights to the taker, which trades and hands them back. `--mode live` trades real money; `--mode dry-run` runs the same bot
against simulated venues fed by live market data ([Dry run](#dry-run)). Commands run from
this directory (`LIGHTER_ASTER_BOT/`) and read `bot.toml`. `run --mode live`, `taker run`
without `--observe-only`, `probe aster-place-cancel` and the `*-market`/`*-roundtrip` probes
submit real orders.

## How `run` shares execution rights

One regime at a time, like Hummingbot's XEMM with the taker's arbitrage in between:

- **XEMM (normal)** quotes both Aster sides and hedges each fill on Lighter at once
  (`[maker.live.quote] reduce_position_only = true` keeps only the side whose hedge reduces
  inventory).
- **An arbitrage** that passes the taker's entry gate asks XEMM for the rights. XEMM cancels
  both quotes and grants a 3 s lease once nothing rests or is in flight: no live or uncertain
  quote, and every fill (one caught by the cancel included) hedged.
- **The taker** re-reads its Lighter nonce and an account snapshot that shows no open order on
  either venue, re-checks the opportunity on current books, trades or skips, and hands the
  rights back. A request not granted within 5 s is withdrawn; a skip or a withdrawal is followed
  by `[taker.arb] cooldown_ms`.
- **XEMM resumes** after re-reading the Lighter nonce and adopting the positions the taker left:
  only account reads started after the hand-back count, and two in a row must agree.

Both engines run for the whole session; one that exits halts the bot. XEMM's journal records
each hand-over (`yield`, `yield_withdrawn`, `rights_returned`, `resumed`), and `bot_stats.py`
summarizes them. The `[controller]` table of `bot.toml` holds the status poll and the
cross-engine loss stop. The controller reads the accounts from XEMM's own snapshot, which XEMM
refreshes every 2 s; it reads the venues only at start-up. Lighter Standard allows 60 REST
requests a minute per IP, and the whole process sends about 46 when idle: XEMM's account read
30, the taker's account refresh 8, and the two book checks 8.

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
- A market's `hedge_venue` (`"lighter"` by default, the same in its `[[taker.markets]]` and
  `[[maker.markets]]` entries) names the second leg: `HYPE-HL` trades Aster against
  Hyperliquid. The bot sends Hyperliquid IOCs only, since the venue offers this account no
  dead-man to cancel a resting order. Live, it reads `hyperliquid.env` ([Probes](#probes)), and
  the leverage gate wants 1x on both legs: set the Hyperliquid market to 1x cross first.
- A `[[taker.markets]]` entry with `first_venue = "lighter"` has no `aster_symbol` and no
  `[[maker.markets]]` entry, and must hedge on Hyperliquid: `HYPE-LH` takes both sides on
  Lighter HYPE and Hyperliquid HYPE, and `run` starts the taker alone, with the taker's own
  account snapshot as the controller's status. Each leg pays its own venue's taker fee.

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
`docker kill --signal=SIGINT bot-hype`). XEMM drains first, then the taker.
XEMM quiesces admission, cancels makers, drains fills and execution outcomes, corrects net
residuals, reconciles and flushes persistence; this can take up to ~190 s per engine, after
any status poll in progress (up to 25 s). Never stop it with a shorter kill: `docker stop`
needs `-t 460`, and compose already sets `stop_grace_period: 460s`. Paired positions stay
open and delta-neutral ([`close`](#close-a-position) exits them). Exit 0 means a clean stop; nonzero means a halt, an unresolved engine
stop or an unwritable event log. A panic outside XEMM's strategy thread aborts the process at
once, with no drain, like a kill.

Only one live writer per market runs at a time: `run --mode live` and `taker run` (unless
`--observe-only`) take the exclusive lock `runs/bot-<MARKET>.lock` and name the holder's pid
on contention. Live, they also lock each leg, `runs/bot-<VENUE>-<SYMBOL>.lock`
(`bot-LIGHTER-HYPE.lock`), so markets sharing a leg (HYPE and HYPE-LH) never both trade it. A
dry run locks `runs/dry-run/bot-<MARKET>.lock`, so it can run beside live.

After a known fill, `POSITION_SNAPSHOT_PENDING` holds new quotes until account reads begin
after that execution; an older read cannot establish a position mismatch or request a sweep.
Hedging, cancels and the exposure limits still apply. A missing-amend reply after a full
private fill does not reopen uncertainty; partial fills or a larger amended size still need
terminal order evidence.

### Close a position

Stop the bot, then close the coin at market on every venue:

```bash
docker compose --profile live run --rm -T bot close --market HYPE --i-understand-live
```

`close` sends REAL orders. It cancels the Aster symbol's open orders, then closes on Aster,
Lighter and Hyperliquid at once (`--venue aster,lighter` for some) with reduce-only orders at
market: an Aster MARKET order, and IOCs through XEMM's hedge worker at
`emergency_slippage_bps` on Lighter and Hyperliquid. Each venue gets up to three orders, sized
from the fills (from a fresh read after one that filled nothing), and must then read flat, or
the command names it and exits nonzero. `--market` takes a market id or its coin: HYPE and
HYPE-HL both close HYPE. It takes each venue's leg lock, so it refuses while a live `run` or
`taker run` trades there ("another live writer holds runs/bot-LIGHTER-HYPE.lock"). Measured
2026-09-29, closing Aster −0.14 and Hyperliquid +0.14 HYPE: Aster filled in 339 ms,
Hyperliquid in 1.3 s (fee known at 1.3 s), all three venues flat within 5 s of the start.

## Dry run

`run --mode dry-run` is the whole bot, its engines, the controller and every client and
signer unchanged, against the market's two venues simulated in process on loopback: Aster and
Lighter, Aster and Hyperliquid for `HYPE-HL`, Lighter and Hyperliquid for `HYPE-LH`. The
simulator follows the live public market data and answers in each venue's own protocol. It
needs no credentials: the bot signs with a fixed dry-run identity whose keys exist on no
venue, so a request that escaped to mainnet could not trade. Its files live in
`runs/dry-run/`, which live never touches.

The venues respond as seen from AWS Tokyo, and pessimistically where the data cannot decide
(`[dry_run]` in `bot.toml` cites each value's source):

- **Time shift.** The simulated world is the live one `shift_ms` (1000) late, timestamps
  included, so the bot sees books as fresh as a Tokyo host would, by its own clocks. A frame
  that reaches this host later than that is applied on arrival and counted as late. Orders,
  cancels and deadmen wait until the feed has caught up with their time, so a cancel cannot
  beat prints that were late; one that waits over 2 s is answered as unavailable.
- **Latency.** Each request draws a lognormal round trip from the benchmarked `[p50, p99]`
  and takes effect at `effect_fraction` (0.9) of it. Lighter taker orders wait a further
  `lighter_taker_delay_ms` (300, the Standard account's delay).
- **Takers** fill against the worse of the two book states around their effect time, and
  liquidity the bot took stays gone until the feed shows that level smaller.
- **Makers** wait behind the visible size at their price, plus `hidden_queue_multiplier` times
  it in hidden orders. Prints at their price work through the visible queue, then the hidden
  one, before filling them; the book shrinking only shortens the visible queue. A print through
  their price, or a book that crosses them, fills them outright.
- **Venue rules**: the live filters, reduce-only, the Aster deadman and listen-key expiry,
  Lighter's sequential nonces, Hyperliquid's five significant figures, and the venues' rate
  limits (Lighter Standard: 60 REST requests and 60 transactions a minute; Hyperliquid: 1200
  weight a minute).
- **Accounts**: one cross account per venue from the `[dry_run]` balances. Fees come from the
  bot's own fee keys, so the dry run cannot catch a wrong one. Funding follows the public
  rates at each venue's funding times. A maintenance-margin breach is reported, not
  liquidated.

Docker (from this directory; the fleet's `start_all.bat` also starts it):

```bash
docker compose up -d --build dryrun dryrun-hl dryrun-lh   # HYPE, HYPE-HL, HYPE-LH (taker only)
docker compose logs -f dryrun
```

Natively (Linux or macOS): `./target/release/lighter_aster_bot run --market HYPE --mode dry-run`.

It stops like live (SIGINT, a drain, positions stay open) and saves the simulated venues;
the next start takes up their accounts, positions and resting orders. The market moved
meanwhile, so the first book fills any resting order it crosses, an expired Aster deadman
cancels, and funding that fell due is charged. To start afresh, stop it and move
`runs/dry-run/` away. Moving only `sim-<M>.state.json` would leave the drawdown baseline
measuring the reset accounts. A fresh directory also restarts the taker's entry-gate
history: as after a fresh live start, the taker trades only once it has recorded
`min_history_samples` (50) opportunities above its required edge (`[taker.arb.entry_gate]`).

An unclean stop (a host reboot, `docker kill`) loses at most the venues' last second. The
next start archives the engines' unclean-session markers, whether a kill or an unresolved
engine stop left them, as `<name>.unclean.<stamp>`. Live keeps them until an operator has
resolved the session against the venues' records; here the simulated venues' own state is
the only record, and the engines reconcile to it at start. Docker treats `docker kill` as a
deliberate stop and does not restart the container; `docker compose up -d dryrun` does.

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
| `lateness_ms` (whole row) | How late the simulator ran its events. Hundreds of ms mean this host starved it of CPU (a Docker build on the same host does); stalls past the shift also make late frames. |
| `rtt_ms`, `private_ms` | The latencies drawn. |
| `requests`, `orders`, `rejects` | Rejects by reason. Each needs an explanation in the bot's log; `RateLimited` means the bot outran a venue limit, which is a finding about the bot. |
| `maker_fills`, `taker_fills`, `queue_ahead`, `maker_wait_ms` | Fills, the queue ahead of each order that came to rest, and each maker fill's wait since placement. |
| `prints`, `prints_inside_spread`, `prints_over_visible` | Trades the visible book cannot explain: hidden orders, or orders placed and taken between two book updates. Their share bounds from above the hidden liquidity `hidden_queue_multiplier` assumes. |
| `account` | Balance, unrealized, equity, realized, fees, funding, positions, maintenance breach. |

Simulator warnings start with `dry-run`. `no route`, `no websocket` or `not simulated` means
the bot used something the simulator does not serve, so the dry run no longer matches live:
treat it as a bug. The reports take `--dry-run` (`python3 ../combined_pnl.py --dry-run`;
`python3 ../trade_history.py --dry-run` keeps its own database in `runs/dry-run/`).
`python3 ../bot_stats.py --dry-run` summarizes these diagnostics together with the trades'
edge kept and slippage per leg.

What the dry run cannot tell: whether the maker fee keys are right (the taker keys matched
the live probes); the bot's market impact beyond the liquidity it takes; how Aster's ~100 ms
splits around matching (assumed pessimistically); anything about liquidation. Lighter
signatures are not verified. The venues' live timings are under [Probes](#probes).

**Going live.** Live runs in the `bot` container, on this Windows host or a Linux host
([Deploy](#deploy)). Live refuses credential files that group or other can read. Docker Desktop
shows Windows bind mounts as mode 777, so the container's entrypoint copies the env files at
0600 into a memory-only tmpfs of the bot user and points `*_ENV_PATH` there; the check still
applies to what the bot reads.

1. The dry run has run for days with no unexplained reject, halt or `no route` warning, and
   its reports agree with the simulated equity net of funding and open-position marks.
2. The fee keys in `bot.toml` match both accounts' actual tiers.
3. On a VPS, ship the sources, the secrets and the live image with `scripts/deploy_vps.sh`
   ([Deploy](#deploy)); here, `docker compose --profile live build bot`.
4. On the host, the read-only probes pass: `docker compose --profile live run --rm bot probe aster-balance`,
   then `probe lighter-balance`, `probe lighter-open-orders`, `probe leverage` and `taker probe
   --market HYPE`. Both venues must be at 1x cross and Aster in one-way position mode, which XEMM
   checks at start.
5. Neither venue has open orders, and positions are flat or paired.
6. `runs/` holds no latch from an earlier run (`bot-<M>.breaker.json`, `*.trip.json`,
   `circuit_breaker_<M>.json`): each engine checks its own when it starts. `run` itself refuses to start on an engine's unclean-session
   marker (`*.active.json`, `active_session_<M>.json`).
7. Start it as in [Run and stop](#run-and-stop). Its drawdown baseline starts at the first
   sample.

## Market data tape

The `recorder` service (container `lighter-aster-recorder`, `record --market HYPE`) records
HYPE's raw public feeds, the dry run's input, for backtests:
- **Aster:** `depth20@100ms` (20 levels), `bookTicker` and `aggTrade`.
- **Lighter:** `order_book` (a snapshot, then deltas: the whole book), `trade` and `market_stats`.
- **REST:** Aster `premiumIndex` every minute; `exchangeInfo` and `orderBooks` hourly.

Every line carries its arrival time on this host, so a replay can reproduce the feed latency
the dry run saw. The recorder is its own process, so the bot never waits on it. Rebuilding the
dry run does not interrupt it; `docker compose up -d --build recorder` restarts it.

**Files.** `data/HYPE/<YYYY-MM-DD>T<HHMMSS>Z.tape.zst`, one per UTC day and per recorder start,
named after the first line's arrival: tab-separated `<arrival µs>\t<kind>\t<payload>`, with the
kinds listed in `src/dryrun/tape.rs`. Read a day with `zstd -dc data/HYPE/<day>T*.tape.zst | head`.
- A kill loses at most the last 10 s.
- `A-` and `L-` lines mark a lost connection. The feeds reconnect within 5 s of the network
  returning, and a start without network (a reboot) waits for it.
- Aster has no order-book history to download again, and the fleet backup skips files over
  100 MB (MAKE_BACKUP.py), so copy the tapes elsewhere if they must survive a disk loss.

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

**Controller halt.** The bot stops both engines (writer first), writes
`bot-<M>.breaker.json` with the reason, and exits nonzero. The reasons are:

- the cross-engine loss stop (`pnl_breaker`): marked equity at or below the baseline minus
  `max_loss_usdc` (15), a baseline kept across restarts unless no sample has refreshed it for
  `baseline_max_gap_hours` (48); or realized trade PnL of both engines
  since this start at or below −15. Unverified gains never count; unverified losses do. The
  engines' own stops (10) normally trip first.
- resting orders at startup (`startup_orders_not_clear`);
- an engine that exits, with an error or not (`engine_exited`).

An engine that fails its drain or does not stop within 200 s makes the exit nonzero
(`bot_stopped` carries the error).

**Network outage.** Three unreadable statuses in a row (45 s) are treated as a lost network
(a status is unreadable while XEMM's account snapshot is older than `poll_sec`), not a halt:
the bot emits `network_pause`, and no engine opens new exposure (no
taker entry, XEMM quotes pulled). In-flight executions, hedges, corrections and the Aster
deadman carry on, and the taker asks for no rights. The loss
stops still run on every readable status. After 4 readable statuses in a row (about 60 s,
restarting at any failure) it emits `network_resume` and trades on; no restart is needed. An
order in flight when the network drops can still end the engine with an unresolved
execution, which halts as `engine_exited`.

Review `bot-<M>.events.jsonl` and the breaker, then restart with `--ack-breaker`, which
archives it as `.acked.<stamp>`. An equity-drawdown breaker also needs
`--reset-breaker-baseline`, which re-arms the drawdown stop on the next sample. The
engines' own latches below are separate and each blocks its engine on its own.

**XEMM.** `bot-<M>.active.json` remains after an unclean or unresolved session, and no tool
resolves it. Once the venues' own records resolve every attempted order in the journal, and
orders and positions are reconciled (an empty orders snapshot alone is not enough), archive it
by hand: `mv bot-<M>.active.json bot-<M>.active.json.resolved.<stamp>`. The loss latch
`bot-<M>.trip.json` trips when marked equity falls `max_cumulative_loss_usdc` (10) below the
median of the first 5 fresh samples, on 3 consecutive samples; that baseline re-arms at every
XEMM start. After review, clear the latch (not an unresolved session) with:
`python scripts/reset_breaker.py --coin <M> --archive`.

**Taker.** The session marker `active_session_<M>.json` is armed before execution rights are
granted, and only a verified, drained shutdown retires it. After an unclean exit, when the
marker holds complete scoped order identities:

```bash
./target/release/lighter_aster_bot taker resolve-session --market HYPE
./target/release/lighter_aster_bot taker reset-circuit-breaker --market HYPE
```

`resolve-session` needs an inactive owner, matching terminal orders, positions consistent
with those fills, and no open orders; it saves `session_resolution_*.json`. It refuses a
marker from a crash before any receipt, without enough identities, or whose Lighter order has
left the newest 200 rows of the account's order history: resolve that one from the venues'
own records, then archive it by hand as above. The loss breaker
`circuit_breaker_<M>.json` trips when the ledger's PnL since `[taker.pnl] since` reaches
−`max_loss_usdc` (10), on the same terms. `reset-circuit-breaker` archives it only: it neither
resolves a session nor rewrites the ledger, so the breaker returns at the next start while
that PnL is still at or below the limit.

## Engines

**Taker.** It prices configured depth in both directions (Aster sell/Lighter buy and the
reverse). A clip trades only when the depth-weighted edge clears both taker fees plus the
margin, both books hold `liquidity_multiple` times the clip within `max_levels` and are
fresher than `max_book_staleness_ms`, and the edge passes the entry gate: the greater of the
90th percentile of recent samples and the required edge plus `min_extra_bps`, blocking during
history warmup. Its Aster book is the `depth20@100ms` snapshot with the newer `bookTicker` top
laid over it (by update id): depth alone lags the top by up to 100 ms. Aster orders are bounded
IOC limits; Lighter uses its native market/IOC path.

An unknown submission outcome keeps its order and client ids: a missing order row or a flat
position does not prove no fill. A known missing hedge gets one retry within
`hedge_retry_timeout_ms`; recovery then closes only the same-sign net residual, and an
unresolved retry or close stops further submissions. The ledger's `actual_net_usd` is
matched spread capture minus fees, and recovery rows are conservative equity-delta estimates.

**XEMM.** Stale books block new exposure. A timeout, a balanced position snapshot or an empty
open-order list never proves that an order did not execute: an unresolved attempt stays
reserved, and after 60 s new exposure freezes while late evidence is still accepted. A net
residual is corrected with the smallest rounded-down quantity that removes it, at most twice
per incident. The margin guard reserves margin for resting makers and hedge obligations above
the per-venue buffers ($26 shipped); reductions stay possible.

## Deploy

The VPS does not compile. Build the image locally and ship it:

```bash
export VPS_HOST='ubuntu@<host>' KEY="$HOME/.ssh/<deploy-key>.pem"
scripts/deploy_vps.sh source     # bot.toml, compose, sources, signers -> ~/LIGHTER_ASTER_BOT
scripts/deploy_vps.sh secrets    # aster.env + lighter.env, chmod 600 (once)
scripts/deploy_vps.sh image      # docker build here, docker save | ssh docker load
```

On the VPS, match the container user to the host user, and create the output and nonce dirs:

```bash
export XEMM_UID="$(id -u)" XEMM_GID="$(id -g)" ASTER_NONCE_DIR=/tmp/lighter-aster-nonces
mkdir -p runs "$ASTER_NONCE_DIR" && chmod 700 "$ASTER_NONCE_DIR"
docker compose run --rm bot probe aster-balance        # signed reads, no orders
docker compose run --rm bot taker probe --market HYPE
```

Compose mounts this directory read-only (config, signers, env files), `runs/` read-write and
the nonce dir at `/nonce`. It never restarts the live bot: a halt stays halted until reviewed.

## Probes

- Read-only: `probe aster-balance | aster-positions | aster-open-orders | leverage |
  lighter-balance | lighter-open-orders`, `taker probe`, `status` and `taker status` (JSON),
  `fetch-specs`. `taker run --markets HYPE --observe-only` scans and records entry-gate
  history without orders.
- `probe lighter-order-dry-run` signs IOC and native market plans without submitting them.
- These submit real orders; run them only with explicit approval, and never beside a live
  `run`. Each needs `--i-understand-live`, a flat start and no open orders, prints every
  step's latency beside a ping, and ends by checking flat with no orders.
  - `probe aster-place-cancel`: XEMM's Aster calls on post-only orders ~2 % from the book,
    with XEMM's user stream running: place; amends through XEMM's own Aster worker (to far
    prices, to the same values, through the ask, with a lapsed permit, and of the cancelled
    order, which must close the slot); a post-only through the ask; cancel-all; the 10 s
    dead-man. A position left over is closed reduce-only.
  - `probe lighter-market --max-usd 12` / `probe hl-hedge --market HYPE-HL --max-usd 12`:
    XEMM's hedge worker on Lighter / Hyperliquid sends a hedge, an IOC that cannot fill
    (printing whether its reject reads as the retryable no-fill), whether the venue takes a
    reduce-only order under its minimum (a partial and a full close; `RULE` lines), and a
    reduce-only close.
  - `taker aster-market-roundtrip --max-usd 7` / `taker lighter-market-roundtrip --max-usd
    12`: the taker's entry order, after one bounded under the bid that cannot fill. The Aster
    one closes with XEMM's reduce-only MARKET, one step (under the $5 minimum) first. They clean
    up reduce-only (at most three closes in 30 s) and stay blocked without terminal-order and
    flat-position evidence.
- Measured 2026-09-28 from Windows, ~250 ms ping to both venues; venue time = RTT - ping:
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
- Hyperliquid reads `HYPERLIQUID_ENV_PATH` (default `hyperliquid.env`, keys as in
  `hyperliquid.env.example`: `wallet_address` = the traded subaccount, `private_key` = its agent
  key, `is_vault`). `docker compose --profile live run --rm bot probe hl-balance --market HYPE`
  reads the account, fees and action budget. `probe hl-place-cancel` (one post-only buy 10 %
  under the bid, cancelled) and `probe hl-market` (a ~$10.5 IOC buy sold back reduce-only) trade
  real funds: both need `--i-understand-live --max-usd <10.5..20>`, a flat start and no open
  order. Every action spends the account's lifetime budget (10k + ~1 per USDC traded), and
  there is no dead-man below $1M of volume.

## Orchestrator leftovers

The stack used to run `orchestrator.py` with the engines as child processes. `run --mode live`
looks in `runs/` and the stack root's `runs/`, and refuses to start while the orchestrator
still runs (its lock `orchestrator_<M>.lock` is held) or while its latches exist:
`orchestrator_breaker_<M>.json`, `orchestrator-xemm-<M>.trip.json` and
`orchestrator-xemm-<M>.active.json`, the last an unresolved session. Review and archive them
like the latches above. Engines left running by a killed orchestrator hold no lock, so `run`
cannot see them: stop any before the first `run`. The reports still read its journals and state.
