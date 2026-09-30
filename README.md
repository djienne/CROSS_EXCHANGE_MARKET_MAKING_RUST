# Aster/Lighter Cross-Exchange Market Making and Arbitrage

One Rust binary, `lighter_aster_bot` in `LIGHTER_ASTER_BOT/`, trades one market across Aster
and Lighter (or Hyperliquid) with two engines: a taker–taker arbitrage engine and an XEMM
engine that quotes on Aster and hedges on the other venue. Its `run` command holds both in one process and enforces a
cross-engine loss stop: XEMM quotes, and when an arbitrage passes the taker's entry gate it
pulls its quotes and hands the execution rights to the taker until the trade is done. A market
with no Aster leg (`HYPE-LH`: Lighter against Hyperliquid) runs the taker alone. `run --mode dry-run`
runs the same bot against simulated venues fed by live market data, with no credentials and no
real orders.

This is a live trading codebase. `run --mode live`, `taker run` without `--observe-only`,
`probe aster-place-cancel` and the `*-market`/`*-roundtrip` probes place real orders and can
lose money through spread, fees, slippage and execution failures. Neither the tests nor the
dry run prove live venue acceptance, latency or profitability.

The [runbook](LIGHTER_ASTER_BOT/RUNBOOK.md) is the operating manual: how the rights are shared,
configuration and secrets, the dry run, going live, runtime files, halts and recovery, deploy
and probes.

## Layout

```text
.
├── dryrun_summary.py       one screen per dry-run market, condensed from the two reports below
├── screener_summary.py     the screener's best routes so far (runs SCREENER's report in Docker)
├── combined_pnl.py         execution economics across both engines' records
├── trade_history.py        the same records as a SQLite trade history, with repair
├── bot_stats.py            why: edge kept per leg, entry gate, controller, simulator health
├── economics.py            the parser the reports share
├── tests/                  Python tests and shared fixtures (tests/fixtures/)
├── SCREENER/               which pairs would suit the bot: its own crate and container, public data only (SCREENER/README.md)
└── LIGHTER_ASTER_BOT/
    ├── bot.toml            config: [controller], [taker], [maker], [dry_run]
    ├── docker-compose.yml  services: dryrun, dryrun-hl, dryrun-lh, recorder (market-data tape), and bot (live, behind the `live` profile)
    ├── scripts/            deploy_vps.sh, reset_breaker.py
    ├── signers/            Lighter signer libraries (binaries, not secrets)
    ├── src/                controller/ (run), taker/, livebot/ (XEMM), dryrun/ (simulated venues)
    └── RUNBOOK.md
```

Git ignores `runs/` (journals, ledgers, latches, state), `data/` (market-data recordings),
`target/` and `*.env` credential files, including `hyperliquid.env`.

## Build

```bash
(cd LIGHTER_ASTER_BOT && cargo build --release --locked)   # Rust 1.92
```

The binary is `LIGHTER_ASTER_BOT/target/release/lighter_aster_bot`. `signers/` holds the Lighter
signer for Linux (amd64, arm64) and macOS (arm64) only, so on Windows the bot runs in Docker.
The reports need Python 3.

## Dry run

```bash
cd LIGHTER_ASTER_BOT
docker compose up -d --build dryrun dryrun-hl dryrun-lh   # HYPE, HYPE-HL, HYPE-LH
docker compose logs -f dryrun
```

It runs in the background, the fleet's `start_all.bat` starts it too, and its files are in
`LIGHTER_ASTER_BOT/runs/dry-run/`. The model, halts, diagnostics and the going-live checklist
are in the [runbook](LIGHTER_ASTER_BOT/RUNBOOK.md#dry-run). Beside it, the `recorder` service
records the same public feeds to `LIGHTER_ASTER_BOT/data/HYPE/` for backtests
([Market data tape](LIGHTER_ASTER_BOT/RUNBOOK.md#market-data-tape)).

## Reports

Run from the repository root. The reports read the bot's records in `LIGHTER_ASTER_BOT/runs/`;
`combined_pnl.py` and `trade_history.py` also read the retired orchestrator's in the root
`runs/` where they exist. With `--dry-run` they read only `LIGHTER_ASTER_BOT/runs/dry-run/`
and, without `--since`, count from the dry run's first start.

```bash
python3 dryrun_summary.py                       # every dry-run market on one screen
python3 screener_summary.py --top 10            # the screener's ranking; --since/--until/--lighter/--latency pass through
python3 combined_pnl.py --market HYPE --since 2026-06-23T16:00:00Z   # add --json for JSON
python3 trade_history.py --market HYPE          # database: runs/trade_history.sqlite
python3 combined_pnl.py --market HYPE --dry-run
python3 trade_history.py --market HYPE --dry-run  # its own database, in the dry run's directory
python3 bot_stats.py --market HYPE --dry-run      # execution quality and health; add --json
```

`bot_stats.py` explains the totals. For taker trades it reports expected vs realized edge, how much
each leg filled worse than the decision price, book ages and fill delays. It also covers the entry
gate's decisions, XEMM edge and hedge delay, XEMM's hand-overs to the taker, and controller halts and network pauses. For
a dry run it adds the simulator's late frames (with host freezes separated), latencies against the
`[dry_run]` model, request rates and rejects.

What the numbers mean:

- Execution economics are venue-realized closes plus the spread on matched opposite positions,
  less actual fees. They exclude funding and the marked value of unpaired exposure: account
  equity and residual positions show those, and equity changes also include transfers.
- Fees come from each fill's own evidence. Lighter fees are per fill, notional × own-role fee
  ticks / 1,000,000, so rebates keep their sign. Lighter omits zero fees, so an omitted fee is
  zero; an explicit null or malformed fee, or an IOC fill flagged as maker, stays unknown.
- Unknown economics are `null` in JSON and SQL. The reports show the known subtotal and an
  incomplete count, and suppress complete totals and projections. Legacy aggregates without
  enough evidence stay incomplete, and corrections revise their original logical trade without
  adding trades or volume.

The reports' `economics.py` and the bot's own `live-report` command, which lists one XEMM
journal's logical trades (`lighter_aster_bot live-report --journal <journal> --details`), are
both checked against `tests/fixtures/execution_economics.json` for agreement on its cases.

Historical repair builds a separate candidate database and a before/after JSON comparison:

```bash
python3 trade_history.py --market HYPE --rebuild \
  --xemm-journal <journal>.jsonl --raw-fills <own-account-fills>.jsonl
# after inspecting the comparison, apply that unchanged candidate (the original is kept):
python3 trade_history.py --market HYPE --replace-rebuilt
```

`--xemm-journal` and `--raw-fills` (own-account fills with order and trade identities,
quantities, notional and fee evidence) are optional and repeatable. Rebuilding is idempotent,
never edits journals, never multiplies aggregate legacy fees by aggregate notional, and refuses
the replacement if either database changed since the review.

## Protocol references

- Aster signing: the [EIP-712 contract](https://asterdex.github.io/aster-api-website/asterCode/authentication/).
- Lighter fee units, and "an omitted fee means zero": the
  [trade circuit](https://github.com/elliottech/lighter-prover/blob/main/circuit/src/apply_trade.rs),
  the [fee constants](https://github.com/elliottech/lighter-prover/blob/main/circuit/src/types/constants.rs)
  and the [WebSocket reference](https://apidocs.lighter.xyz/docs/websocket-reference).
