"""Run the dry runs, the market-data collectors and the live bot on the Tokyo VPS; bring their data home.

  python vps.py sync      # upload tracked files (working tree), build, (re)start dry runs + recorder
  python vps.py latency   # TCP-connect and HTTPS round trips to the venues, from the VPS
  python vps.py status    # services, memory, disk, runs/ and data sizes
  python vps.py logs [N] [MARKET]  # last N journal lines of a dry run (default 50, HYPE)
  python vps.py screener  # upload, build and (re)start the pair screener's collector
  python vps.py pull [--dry-run]  # copy closed data files home, verify, prune the VPS's old ones;
                                  # copy the VPS's runs/ (live and dry runs) to LIGHTER_ASTER_BOT/runs/tokyo/
  python vps.py live creds|start|stop|logs|status  # the live bot, its own unit apart from the dry runs

Only git-tracked files go up (uncommitted edits included, untracked files and *.env never); the
live bot's Lighter and Hyperliquid env files go up only by `live creds`, at mode 600. Ponytail:
files deleted locally stay on the VPS; if that ever matters, wipe ~/xemm/LIGHTER_ASTER_BOT/src
before extracting.

Since 2026-10-10 the VPS collects and this PC stores (this PC's route to Tokyo stalls for tens of
seconds): the dry runs, the tape recorder (`record --market HYPE`: Aster, Lighter and
Hyperliquid, ~0.19 GB/day) and the screener's collector (full pair list, ~0.24 GB/day) run
permanently there and nowhere else, and a Windows scheduled task runs `pull` daily (VPS.md). Disk stays bounded: journald 200 MB, the build dir
<= 3 GB, a daily timer gzips the dry run's logs past 1 GB and stops the collectors below 4 GB
free, and `pull` deletes a VPS file only once copied, verified and KEEP_DAYS old.
"""
import hashlib
import io
import json
import posixpath
import shlex
import subprocess
import sys
import tarfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent
# The address stays out of git: vps.env (ignored by *.env) holds `VPS_HOST=root@<ip>`.
HOST = dict(l.split("=", 1) for l in (ROOT / "vps.env").read_text().split()
            if "=" in l)["VPS_HOST"]
KEY = Path.home() / ".ssh" / "id_rsa"
REMOTE = "~/xemm/LIGHTER_ASTER_BOT"
SERVICE = "xemm-dryrun"
# Each dry run's unit and market: Aster-Lighter, Aster-Hyperliquid, Lighter-Hyperliquid.
DRY_RUNS = {SERVICE: "HYPE", f"{SERVICE}-hl": "HYPE-HL", f"{SERVICE}-lh": "HYPE-LH", f"{SERVICE}-cashcat-lh": "CASHCAT-LH"}
UNITS = " ".join(DRY_RUNS)
SCREENER = "xemm-screener"
RECORDER = "xemm-recorder"
# Below this much free disk the collectors stop, so the dry run keeps its disk.
FLOOR_MB = 4096
# A VPS file is deleted only once pulled, verified and older than this (the screener's gate reads 72 h).
KEEP_DAYS = 7
# Remote data dir, local copy, file pattern; names start with their UTC start (YYYY-MM-DDTHHMMSSZ).
PULLS = [("/root/xemm/LIGHTER_ASTER_BOT/data", ROOT / "LIGHTER_ASTER_BOT" / "data-tokyo", "*/*.tape.zst"),
         ("/root/xemm/SCREENER/data", ROOT / "SCREENER" / "data-tokyo", "*.screen.zst")]
# The VPS's runs/ comes here: the live bot's files at its top, the dry runs' in dry-run/ (the
# reports' default).
RUNS_COPY = ROOT / "LIGHTER_ASTER_BOT" / "runs" / "tokyo"

UNIT = """[Unit]
Description=XEMM {market} dry run (simulated venues, no credentials)
After=network-online.target
Wants=network-online.target

[Service]
WorkingDirectory=/root/xemm/LIGHTER_ASTER_BOT
ExecStart=/root/xemm/LIGHTER_ASTER_BOT/target/release/lighter_aster_bot run --market {market} --mode dry-run --ack-breaker
Restart=always
RestartSec=10
KillSignal=SIGINT
TimeoutStopSec=460
MemoryMax=512M

[Install]
WantedBy=multi-user.target
"""

SCREENER_UNIT = """[Unit]
Description=Pair screener collector (public data, full pair list)
After=network-online.target
Wants=network-online.target

[Service]
WorkingDirectory=/root/xemm/SCREENER
ExecStart=/root/xemm/SCREENER/target/release/screener collect
Restart=always
RestartSec=10
KillSignal=SIGINT
MemoryMax=400M
# Below the dry run, whose timing matters more.
Nice=10
CPUWeight=20

[Install]
WantedBy=multi-user.target
"""

# The live bot: sync never starts or restarts it, and a halt or a reboot leaves it stopped until
# `live start`, after review. Above the dry runs and collectors: its timing is real money.
LIVE, LIVE_MARKET = "xemm-live", "CASHCAT-LH"
LIVE_UNIT = """[Unit]
Description=XEMM {market} LIVE (real orders)
After=network-online.target
Wants=network-online.target

[Service]
WorkingDirectory=/root/xemm/LIGHTER_ASTER_BOT
ExecStart=/root/xemm/LIGHTER_ASTER_BOT/target/release/lighter_aster_bot run --market {market} --mode live{ack}
Restart=no
KillSignal=SIGINT
TimeoutStopSec=460
MemoryMax=512M
Nice=-5
"""

RECORDER_UNIT = """[Unit]
Description=HYPE market-data tape (public Aster, Lighter and Hyperliquid feeds)
After=network-online.target
Wants=network-online.target

[Service]
WorkingDirectory=/root/xemm/LIGHTER_ASTER_BOT
ExecStart=/root/xemm/LIGHTER_ASTER_BOT/target/release/lighter_aster_bot record --market HYPE
Restart=always
RestartSec=10
KillSignal=SIGINT
MemoryMax=256M
# Below the dry run, above the screener: its arrival times are the tape's record of feed latency.
Nice=5
CPUWeight=50

[Install]
WantedBy=multi-user.target
"""

# The dry runs' append-only logs (~75 MB/day for the three HYPE ones, mostly HYPE's journal). The bot appends to them
# without reopening and reads none back at startup (the risk monitor starts at the session start),
# so they move only while it is stopped: copy-and-truncate could drop a line written in between,
# such as a fill. Runs daily; acts only past 1 GB of logs (or the MB given as its argument), about
# every two weeks (e.g. 2026-10-24), at the cost of one restart (drain, then the shift's warm-up).
# The archive grows ~6 MB/day (gzip 13x), which the 25 GB disk holds for years; nothing is deleted.
ARCHIVE = f"""#!/bin/sh
set -eu
# Ponytail: checked daily against ~0.5 GB/day written, so a 4 GB floor leaves days of slack;
# `vps.py pull` starts the collectors again once there is room.
free=$(df -m --output=avail / | tail -1 | tr -d ' ')
[ "$free" -ge {FLOOR_MB} ] || {{ systemctl stop {RECORDER} {SCREENER}; echo "stopped the collectors: $free MB free"; }}
cd /root/xemm/LIGHTER_ASTER_BOT/runs/dry-run
LOGS="bot-*-journal.jsonl book_sanity_*.jsonl bot-*.equity.jsonl sim-*.diag.jsonl"
used=$(du -cm $LOGS 2>/dev/null | tail -1 | cut -f1)
[ "$used" -ge "${{1:-1024}}" ] || exit 0
dest=../archive/$(date -u +%Y%m%dT%H%M%SZ)
mkdir -p "$dest"
systemctl stop {UNITS}
for f in $LOGS; do if [ -f "$f" ]; then mv "$f" "$dest/"; fi; done
systemctl start {UNITS}
nice gzip -9 "$dest"/*
echo "archived $used MB of logs to $dest"
"""

ARCHIVE_SERVICE = f"""[Unit]
Description=Archive the XEMM dry run's logs

[Service]
Type=oneshot
ExecStart=/usr/local/sbin/{SERVICE}-archive
"""

ARCHIVE_TIMER = """[Unit]
Description=Archive the XEMM dry run's logs past 1 GB

[Timer]
OnCalendar=*-*-* 03:30:00 UTC
Persistent=true

[Install]
WantedBy=timers.target
"""

LATENCY = r'''
import socket, ssl, statistics, time, http.client
hosts = [("fapi.asterdex.com", "/fapi/v1/ping"), ("fstream.asterdex.com", None),
         ("mainnet.zklighter.elliot.ai", "/"), ("api.hyperliquid.xyz", "/info")]
def q(xs):
    xs = sorted(xs); return "p50 %6.1f  min %6.1f  max %6.1f" % (statistics.median(xs), xs[0], xs[-1])
for host, path in hosts:
    tcp = []
    for _ in range(10):
        t = time.perf_counter(); socket.create_connection((host, 443), 5).close()
        tcp.append((time.perf_counter() - t) * 1e3)
    line = "%-28s tcp %s" % (host, q(tcp))
    if path:
        c = http.client.HTTPSConnection(host, timeout=5, context=ssl.create_default_context())
        rtt = []
        for i in range(21):
            t = time.perf_counter()
            if path == "/info":
                c.request("POST", path, '{"type":"exchangeStatus"}', {"Content-Type": "application/json"})
            else:
                c.request("GET", path)
            c.getresponse().read()
            if i: rtt.append((time.perf_counter() - t) * 1e3)  # first one pays the TLS handshake
        line += "\n%-28s http %s" % ("", q(rtt))
    print(line)
'''


# A dead route fails in ~1 min instead of hanging (a pull once hung 3 h); stalls of tens of seconds
# are ridden out.
OPTS = ["-i", str(KEY), "-o", "IdentitiesOnly=yes", "-o", "BatchMode=yes", "-o", "ConnectTimeout=30",
        "-o", "ServerAliveInterval=15", "-o", "ServerAliveCountMax=4"]


def ssh(cmd, stdin=None, capture=False):
    return subprocess.run(["ssh", *OPTS, HOST, cmd], input=stdin, check=True, capture_output=capture, text=capture)


def tracked_tar(folder="LIGHTER_ASTER_BOT"):
    files = subprocess.run(["git", "ls-files", "-z", folder], cwd=ROOT, check=True,
                           capture_output=True).stdout.decode().split("\0")
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w") as tar:
        for f in filter(None, files):
            p = ROOT / f
            if p.is_file() and not p.name.endswith(".env"):
                tar.add(p, arcname=f)
    return buf.getvalue()


def sync():
    ssh("mkdir -p ~/xemm && tar -x -C ~/xemm", stdin=tracked_tar())
    # Ponytail: cargo never drops artifacts that a toolchain or dependency update orphans; past
    # 3 GB start clean (a full build takes ~30 min on the 1 CPU). The apt cache is not needed.
    ssh(f"apt-get clean; cd {REMOTE} && [ $(du -sm target 2>/dev/null | cut -f1) -lt 3072 ]"
        " || (. ~/.cargo/env && cargo clean --release)")
    # One job: the VPS has 1 CPU and ~1 GB RAM. Only the build needs swap: the thin-LTO link
    # peaked at 1.27 of its 2.5 GB (2026-10-10); the running services use < 100 MB together.
    # The binary logs the commit it was built from (`-dirty`: uncommitted edits went up too).
    commit = subprocess.run(["git", "describe", "--always", "--dirty"], cwd=ROOT, check=True, capture_output=True, text=True).stdout.strip()
    # Niced: a live bot may be trading on the one CPU.
    ssh(f". ~/.cargo/env && cd {REMOTE} && GIT_COMMIT={shlex.quote(commit)} CARGO_BUILD_JOBS=1 nice cargo build --release")
    # The bot logs to stdout -> journald; cap it so the small disk only grows by runs/dry-run.
    ssh("mkdir -p /etc/systemd/journald.conf.d && printf '[Journal]\\nSystemMaxUse=200M\\n'"
        " > /etc/systemd/journald.conf.d/size.conf && systemctl restart systemd-journald")
    for path, text in ((f"/usr/local/sbin/{SERVICE}-archive", ARCHIVE),
                       (f"/etc/systemd/system/{SERVICE}-archive.service", ARCHIVE_SERVICE),
                       (f"/etc/systemd/system/{SERVICE}-archive.timer", ARCHIVE_TIMER)):
        ssh(f"cat > {path} <<'EOF'\n{text}EOF")
    ssh(f"chmod 755 /usr/local/sbin/{SERVICE}-archive && systemctl daemon-reload"
        f" && systemctl enable -q --now {SERVICE}-archive.timer")
    ssh(f"{unit_files()}cat > /etc/systemd/system/{RECORDER}.service <<'EOF'\n{RECORDER_UNIT}EOF\n"
        f"systemctl daemon-reload && systemctl enable -q {UNITS} {RECORDER} && systemctl restart {UNITS} {RECORDER}"
        f" && sleep 5 && systemctl --no-pager status {UNITS} {RECORDER} | grep -E '^. |Active:'")


def unit_files():
    """Shell lines that write each dry run's unit file."""
    return "".join(f"cat > /etc/systemd/system/{u}.service <<'EOF'\n{UNIT.format(market=m)}EOF\n" for u, m in DRY_RUNS.items())


def live(action, ack=False):
    """`creds` uploads lighter.env and hyperliquid.env at mode 600 (never printed); `start` (re)writes
    the unit and starts it unless it runs (`start --ack-breaker`: this once, after a breaker halt
    was reviewed); `stop` drains it (SIGINT, up to 460 s)."""
    if action == "creds":
        buf = io.BytesIO()
        with tarfile.open(fileobj=buf, mode="w") as tar:
            for name in ("lighter.env", "hyperliquid.env"):
                path = ROOT / "LIGHTER_ASTER_BOT" / name
                info = tar.gettarinfo(path, arcname=name)
                info.mode = 0o600
                with open(path, "rb") as f:
                    tar.addfile(info, f)
        ssh(f"umask 077 && tar -x --no-same-owner -C {REMOTE} && chmod 600 {REMOTE}/*.env && ls -l {REMOTE}/*.env", stdin=buf.getvalue())
    elif action == "start":
        unit = LIVE_UNIT.format(market=LIVE_MARKET, ack=" --ack-breaker" if ack else "")
        ssh(f"cat > /etc/systemd/system/{LIVE}.service <<'EOF'\n{unit}EOF\n"
            f"systemctl daemon-reload && if systemctl is-active -q {LIVE}; then echo already running; else systemctl start {LIVE}; fi"
            f" && sleep 5 && systemctl --no-pager status {LIVE} | head -5")
    elif action == "stop":
        ssh(f"systemctl stop {LIVE}; systemctl --no-pager status {LIVE} | head -3")
    elif action == "logs":
        ssh(f"journalctl -u {LIVE} --no-pager -n 100")
    else:
        ssh(f"systemctl --no-pager status {LIVE} | head -5; true")


def screener():
    ssh("mkdir -p ~/xemm && tar -x -C ~/xemm", stdin=tracked_tar("SCREENER"))
    ssh(". ~/.cargo/env && cd ~/xemm/SCREENER && CARGO_BUILD_JOBS=1 nice cargo build --release")
    ssh(f"cat > /etc/systemd/system/{SCREENER}.service <<'EOF'\n{SCREENER_UNIT}EOF\n"
        f"systemctl daemon-reload && systemctl enable -q {SCREENER} && systemctl restart {SCREENER}"
        f" && sleep 5 && systemctl --no-pager status {SCREENER} | head -5")


def sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def started(rel):
    """A data file's UTC start, from its name; None if its name has none."""
    try:
        return datetime.strptime(posixpath.basename(rel)[:18], "%Y-%m-%dT%H%M%SZ").replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def pull(dry_run):
    """Copies each closed data file (every one but the newest of its directory: the collectors only
    append to their newest) that is not here yet, checks its SHA-256 against the VPS's, and deletes
    the VPS's copy once verified and KEEP_DAYS old. File by file, so a stall costs one file's retry;
    ~2 MB/s, to spare the route this PC's live feeds share."""
    now = datetime.now(timezone.utc)
    print(f"{now:%Y-%m-%d %H:%M:%SZ} pull{' --dry-run' if dry_run else ''}", flush=True)
    for src, dest, pattern in PULLS:
        listing = ssh(f"cd {src} 2>/dev/null && stat -c '%s %n' {pattern} 2>/dev/null; true", capture=True).stdout
        sizes = {rel: int(size) for size, rel in (line.split(" ", 1) for line in listing.splitlines())}
        newest = {posixpath.dirname(rel): rel for rel in sorted(sizes)}
        closed = sorted(rel for rel in sizes if rel != newest[posixpath.dirname(rel)] and started(rel))
        manifest_path = dest / "pulled.json"
        manifest = json.loads(manifest_path.read_text()) if manifest_path.exists() else {}
        todo = [rel for rel in closed if manifest.get(rel, {}).get("size") != sizes[rel]]
        remote = {}
        if todo and not dry_run:
            out = ssh(f"cd {src} && nice sha256sum " + " ".join(shlex.quote(r) for r in todo), capture=True).stdout
            remote = {rel: digest for digest, rel in (line.split("  ", 1) for line in out.splitlines())}
        copied = failed = 0
        for rel in todo:
            if dry_run:
                print(f"  would copy {src}/{rel} ({sizes[rel] / 1e6:.1f} MB)")
                continue
            local = dest / rel
            local.parent.mkdir(parents=True, exist_ok=True)
            part = local.with_name(local.name + ".part")
            # A relative target: scp would read a drive letter (C:) as a host name.
            got = subprocess.run(["scp", "-q", "-l", "16000", *OPTS, f"{HOST}:{src}/{rel}", part.name], cwd=part.parent)
            if got.returncode != 0 or sha256(part) != remote[rel]:
                part.unlink(missing_ok=True)
                failed += 1
                print(f"  FAILED {rel}: copy or checksum; the next pull retries it")
                continue
            part.replace(local)
            copied += 1
            manifest[rel] = {"size": sizes[rel], "sha256": remote[rel]}
            manifest_path.write_text(json.dumps(manifest, indent=1, sort_keys=True))
        old = [rel for rel in closed if manifest.get(rel, {}).get("size") == sizes[rel]
               and (dest / rel).is_file() and (dest / rel).stat().st_size == sizes[rel]
               and started(rel) < now - timedelta(days=KEEP_DAYS)]
        for rel in old:
            print(f"  {'would delete' if dry_run else 'delete'} {src}/{rel}")
        if old and not dry_run:
            ssh(f"cd {src} && rm -- " + " ".join(shlex.quote(r) for r in old))
        print(f"  {src}: {len(sizes)} files, {len(closed)} closed, {len(todo)} to copy"
              f"{'' if dry_run else f' ({copied} copied, {failed} failed)'}, {len(old)} past {KEEP_DAYS} days", flush=True)
    try:
        pull_runs(dry_run)
    except (OSError, tarfile.TarError) as e:
        print(f"  FAILED runs/ copy ({e}); the next pull retries it", flush=True)
    if not dry_run:
        ssh(f"free=$(df -m --output=avail / | tail -1 | tr -d ' '); echo \"  VPS: $free MB free\";"
            f" [ \"$free\" -lt {FLOOR_MB} ] || systemctl start {RECORDER} {SCREENER}")


def pull_runs(dry_run):
    """Copies the VPS's runs/ (the live bot's files at its top, runs/dry-run/, and each runs/archive/
    batch not here yet) over RUNS_COPY, then writes the copy's time to pulled_at.txt: the reports
    end their window there. Ponytail: the logs come whole each day, gzipped (13x); they reach ~1 GB before
    the archive moves them (~2 weeks), so up to ~80 MB, ~1 min at 2 MB/s.
    Past that, rsync --append would send only the new bytes."""
    have = [p.name for p in (RUNS_COPY / "archive").glob("*")]
    if dry_run:
        print(f"  would copy {REMOTE}/runs (archive/ but {len(have)} batches here) to {RUNS_COPY}")
        return
    at = datetime.now(timezone.utc)
    skip = " ".join(shlex.quote(f"--exclude=archive/{name}") for name in have)
    # tar exits 1 when a log grew while read: the copy holds it up to then.
    ssh(f"rm -f /tmp/runs.tgz; cd {REMOTE}/runs || exit 2; nice tar -czf /tmp/runs.tgz --exclude='*.lock' {skip} dry-run archive"
        " $(find . -maxdepth 1 -type f ! -name '*.lock'); [ $? -le 1 ]")
    RUNS_COPY.mkdir(parents=True, exist_ok=True)
    part = RUNS_COPY / "runs.tgz.part"
    subprocess.run(["scp", "-q", "-l", "16000", *OPTS, f"{HOST}:/tmp/runs.tgz", part.name], cwd=RUNS_COPY, check=True)
    with tarfile.open(part) as tar:
        files = [m for m in tar.getmembers() if (m.isfile() or m.isdir()) and not m.name.startswith("/") and ".." not in m.name.split("/")]
        tar.extractall(RUNS_COPY, members=files)
    part.unlink()
    (RUNS_COPY / "pulled_at.txt").write_text(f"{at:%Y-%m-%dT%H:%M:%SZ}\n")
    print(f"  {REMOTE}/runs: {sum(m.isfile() for m in files)} files copied to {RUNS_COPY}", flush=True)


def main():
    cmd = sys.argv[1] if len(sys.argv) > 1 else ""
    if cmd == "sync":
        sync()
    elif cmd == "latency":
        ssh("python3 -", stdin=LATENCY.encode())
    elif cmd == "status":
        ssh(f"systemctl --no-pager status {UNITS} {LIVE} | grep -E '^. |Active:|Memory:'; free -h; df -h /; du -sh {REMOTE}/runs/*; "
            f"systemctl list-timers {SERVICE}-archive.timer --no-pager | head -2; "
            f"echo recorder: $(systemctl is-active {RECORDER}) screener: $(systemctl is-active {SCREENER}); "
            f"du -sh {REMOTE}/data/* ~/xemm/SCREENER/data 2>/dev/null; true")
    elif cmd == "screener":
        screener()
    elif cmd == "live":
        live(sys.argv[2] if len(sys.argv) > 2 else "status", "--ack-breaker" in sys.argv[3:])
    elif cmd == "pull":
        # It starts over after a dropped connection: every step is idempotent.
        for attempt in range(3):
            if attempt:
                time.sleep(300)
            try:
                return pull("--dry-run" in sys.argv[2:])
            except subprocess.CalledProcessError as e:
                print(f"  attempt {attempt + 1} failed ({e.returncode}: {e.cmd[-1][:60]})", flush=True)
        sys.exit(1)
    elif cmd == "logs":
        n = sys.argv[2] if len(sys.argv) > 2 else "50"
        market = sys.argv[3].upper() if len(sys.argv) > 3 else "HYPE"
        unit = next(u for u, m in DRY_RUNS.items() if m == market)
        ssh(f"journalctl -u {unit} --no-pager -n {int(n)}")
    else:
        sys.exit(__doc__)


if __name__ == "__main__":
    main()
