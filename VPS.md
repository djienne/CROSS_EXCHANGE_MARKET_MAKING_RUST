# VPS (Tokyo dry run)

| | |
|---|---|
| Connect | `ssh -i ~/.ssh/id_rsa root@<VPS_IP>`; the address is in the local, git-ignored `vps.env` (`VPS_HOST=root@<ip>`), which `vps.py` reads |
| Key | `~/.ssh/id_rsa` on the Windows box (never commit it) |
| Host | Tokyo, Ubuntu 22.04, 1 vCPU, ~1 GB RAM + 4 GB swap, 25 GB disk |
| Toolchain | rustup, Rust 1.92 (`. ~/.cargo/env`), build-essential; no Docker |
| Code | `/root/xemm/LIGHTER_ASTER_BOT` (git-tracked files only; no `*.env`, no credentials) |
| Service | `xemm-dryrun` (systemd): `run --market HYPE --mode dry-run --ack-breaker`, 512 MB cap, SIGINT stop |
| Output | `/root/xemm/LIGHTER_ASTER_BOT/runs/dry-run/` |

Only the dry run runs here: no recorder tape and no other bulk data collection, except the
3-day screener run below. `runs/dry-run`
grows ~37 MB/day for HYPE (2026-10-10: journal 26, book sanity 7.5, diagnostics 2, equity 1.5).
Disk stays bounded, all set up by `sync`:

- The daily timer `xemm-dryrun-archive` (03:30 UTC) acts once those four logs pass 1 GB (about
  monthly). It stops the dry run, moves them to `runs/archive/<UTC time>/`, restarts the dry run
  and gzips them (~10x smaller, ~4 MB/day: years of the 25 GB disk). Nothing is deleted. The bot
  appends to these logs and never reads them back, so they move only while it is stopped.
- journald is capped at 200 MB, and the cargo build dir at 3 GB (a clean rebuild past that).
- The apt cache is cleared.

`/usr/local/sbin/xemm-dryrun-archive 1` archives now (the argument is the threshold in MB).

## Sync after code changes

From the repository root on Windows:

```
python vps.py sync      # upload tracked files (uncommitted edits too), build, restart the dry run
python vps.py status    # service, memory, disk, runs/ size
python vps.py logs 100  # journal tail
python vps.py latency   # TCP and HTTPS round trips to Aster, Lighter, Hyperliquid
```

The release build (thin LTO, one job) takes a while on one CPU and leans on swap; the running dry
run keeps going until the restart, but its timing is noisier while cargo runs.

## Screener ground truth (3 days from 2026-10-10)

`python vps.py screener` uploads the tracked `SCREENER/` files, builds them and runs the pair
screener's collector as `xemm-screener`: 400 MB cap, niced below the dry run, not enabled at boot.
A transient timer, `xemm-screener-stop`, stops it 3 days after the last `screener` call. It writes
~200 MB/day to `/root/xemm/SCREENER/data`; `python vps.py screener-pull` copies the files to
`SCREENER/data-tokyo/` (git-ignored). They give `[report.scenario.tokyo]` its delays (the report
prints them) and the comparison of `report --scenario host` on them with `--scenario tokyo` on the
local data. Delete the VPS data dir once pulled.

## Latency from the VPS (2026-10-10)

| Venue host | TCP connect p50 | HTTPS request p50 (keep-alive) |
|---|---|---|
| fapi.asterdex.com | 2.1 ms | 7.3 ms |
| fstream.asterdex.com | 2.3 ms | - |
| mainnet.zklighter.elliot.ai | 2.2 ms | 3.6 ms |
| api.hyperliquid.xyz | 2.8 ms | 14.1 ms |

All three venues are a few ms away, consistent with the Tokyo scenario `[dry_run]` in
`LIGHTER_ASTER_BOT/bot.toml` models (its order RTTs include venue processing, not just network).

## By hand on the VPS

```
systemctl status xemm-dryrun
journalctl -u xemm-dryrun -f
systemctl stop xemm-dryrun        # SIGINT, graceful
```
