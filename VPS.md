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

Only the dry run runs here: no recorder tape and no other bulk data collection. `runs/dry-run`
grows ~40 MB/day for HYPE (mostly the journal); journald is capped at 200 MB.

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
