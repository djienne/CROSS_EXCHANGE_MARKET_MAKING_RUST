# VPS (Tokyo dry run and data collection)

| | |
|---|---|
| Connect | `ssh -i ~/.ssh/id_rsa root@<VPS_IP>`; the address is in the local, git-ignored `vps.env` (`VPS_HOST=root@<ip>`), which `vps.py` reads |
| Key | `~/.ssh/id_rsa` on the Windows box (never commit it) |
| Host | Tokyo, Ubuntu 22.04 (kernel 5.15.0-198), 1 vCPU, ~1 GB RAM + 2.5 GB swap (`/swapfile2g` + a 0.5 GB partition), 25 GB disk; no snapd |
| Toolchain | rustup, Rust 1.92 (`. ~/.cargo/env`), build-essential; no Docker |
| Code | `/root/xemm/LIGHTER_ASTER_BOT` (git-tracked files only; no `*.env`, no credentials) |
| Services | `xemm-dryrun`, `xemm-dryrun-hl`, `xemm-dryrun-lh` (systemd): `run --market HYPE` / `HYPE-HL` / `HYPE-LH` `--mode dry-run --ack-breaker` (Aster-Lighter, Aster-Hyperliquid, Lighter-Hyperliquid), 512 MB cap each, SIGINT stop; their simulated venues take free ports (`[dry_run]` ports 0) |
| Output | `/root/xemm/LIGHTER_ASTER_BOT/runs/dry-run/`, copied daily to `LIGHTER_ASTER_BOT/runs/tokyo/` (the reports' default) |
| Collectors | `xemm-recorder` (`record --market HYPE`, 256 MB cap) → `LIGHTER_ASTER_BOT/data/HYPE/`; `xemm-screener` (`screener collect`, 400 MB cap) → `SCREENER/data/`; both enabled at boot, niced below the dry run |

Since 2026-10-10 the VPS collects and the Windows PC stores: this PC's route to Tokyo stalls for
tens of seconds, which leaves holes in what it records, while the VPS's feeds are clean. Disk use
is ~0.45 GB/day: the tape ~0.19 (Aster + Lighter 0.15, Hyperliquid ~0.04; estimates until a full
day is measured), the screener 0.1-0.2, and `runs/dry-run` ~37 MB/day for HYPE (2026-10-10:
journal 26, book sanity 7.5, diagnostics 2, equity 1.5), plus the two Hyperliquid routes (unmeasured). That is ~25 days of slack on the free
disk if no pull happens. Disk stays bounded, all set up by `sync` and `pull`:

- `python vps.py pull` (the Windows scheduled task "XEMM Tokyo pull" runs it daily at 06:00 local
  time, i.e. 04:00 UTC in summer and 05:00 in winter: after the 00:00 UTC rotations and outside the
  06-09 UTC stall window; output appended to `LIGHTER_ASTER_BOT/data-tokyo/pull.log`) copies every closed file (all but the
  newest of each directory) to `LIGHTER_ASTER_BOT/data-tokyo/` and `SCREENER/data-tokyo/` at
  ~2 MB/s, file by file, and accepts a copy only when its SHA-256 equals the VPS's
  (`pulled.json` beside them lists the verified files). It deletes a VPS file only once verified
  here and started more than 7 days ago (the screener's gate reads 72 h back). `--dry-run` lists
  what it would copy and delete.
- `pull` then copies the VPS's `runs/dry-run/` (and each `runs/archive/` batch not here yet) whole,
  gzipped, to `LIGHTER_ASTER_BOT/runs/tokyo/` and writes the time to `pulled_at.txt`, where the
  reports end their window. Only the VPS runs the dry runs and collectors (user, 2026-10-10): this
  PC's stopped, so nothing is recorded twice.
- The daily 03:30 UTC script stops both collectors below 4 GB free, so the dry run keeps its disk;
  the next `pull` starts them again once there is room.

- The daily timer `xemm-dryrun-archive` (03:30 UTC) acts once those four logs pass 1 GB (about
  monthly). It stops the dry runs, moves them to `runs/archive/<UTC time>/`, restarts the dry runs
  and gzips them (~10x smaller, ~4 MB/day: years of the 25 GB disk). Nothing is deleted. The bot
  appends to these logs and never reads them back, so they move only while it is stopped.
- journald is capped at 200 MB, and the cargo build dir at 3 GB (a clean rebuild past that).
- The apt cache is cleared.

`/usr/local/sbin/xemm-dryrun-archive 1` archives now (the argument is the threshold in MB).

## Sync after code changes

From the repository root on Windows:

```
python vps.py sync      # upload tracked files (uncommitted edits too), build, restart the dry run and recorder
python vps.py status    # services, memory, disk, runs/ and data sizes
python vps.py pull      # bring the collectors' closed files home (also daily, scheduled)
python vps.py logs 100  # journal tail
python vps.py latency   # TCP and HTTPS round trips to Aster, Lighter, Hyperliquid
```

The release build (thin LTO, one job) takes a while on one CPU and leans on swap; the running dry
run keeps going until the restart, but its timing is noisier while cargo runs.

## Screener collector (permanent since 2026-10-10)

`python vps.py screener` uploads the tracked `SCREENER/` files, builds them and (re)starts the pair
screener's collector as `xemm-screener`, with the full pair list: 400 MB cap, niced below the dry
run, enabled at boot. It began as a 3-day ground truth (from 2026-10-10 12:18Z) and stays. `pull`
copies its files to `SCREENER/data-tokyo/` (git-ignored), which `python screener_summary.py`
scores by default (`--local` for this PC's own recording). The first days give
`[report.scenario.tokyo]` its delays (the report prints them) and the comparison of
`report --scenario host` on them with `--scenario tokyo` on the local data.

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
systemctl status xemm-dryrun      # -hl, -lh: the Hyperliquid routes
journalctl -u xemm-dryrun -f
systemctl stop xemm-dryrun        # SIGINT, graceful
```
