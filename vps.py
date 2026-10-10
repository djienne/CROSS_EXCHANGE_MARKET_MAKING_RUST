"""Sync the bot to the VPS, build it there and run the HYPE dry run as a systemd service.

  python vps.py sync      # upload tracked files (working tree), build, (re)start the dry run
  python vps.py latency   # TCP-connect and HTTPS round trips to the venues, from the VPS
  python vps.py status    # service state, memory, disk, runs/ and archive size
  python vps.py logs [N]  # last N journal lines (default 50)

Only git-tracked files of LIGHTER_ASTER_BOT go up (uncommitted edits included, untracked files
and *.env never), so no credentials or runs/ leave this machine. The VPS runs the dry run only:
no recorder, no tape. Disk stays bounded: journald 200 MB, the build dir <= 3 GB, and a daily
timer gzips the dry run's growing logs into runs/archive (~10x smaller) past 1 GB. Ponytail:
files deleted locally stay on the VPS; if that ever matters, wipe ~/xemm/LIGHTER_ASTER_BOT/src
before extracting.
"""
import io
import subprocess
import sys
import tarfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent
# The address stays out of git: vps.env (ignored by *.env) holds `VPS_HOST=root@<ip>`.
HOST = dict(l.split("=", 1) for l in (ROOT / "vps.env").read_text().split()
            if "=" in l)["VPS_HOST"]
KEY = Path.home() / ".ssh" / "id_rsa"
REMOTE = "~/xemm/LIGHTER_ASTER_BOT"
SERVICE = "xemm-dryrun"

UNIT = f"""[Unit]
Description=XEMM HYPE dry run (simulated venues, no credentials)
After=network-online.target
Wants=network-online.target

[Service]
WorkingDirectory=/root/xemm/LIGHTER_ASTER_BOT
ExecStart=/root/xemm/LIGHTER_ASTER_BOT/target/release/lighter_aster_bot run --market HYPE --mode dry-run --ack-breaker
Restart=always
RestartSec=10
KillSignal=SIGINT
TimeoutStopSec=460
MemoryMax=512M

[Install]
WantedBy=multi-user.target
"""

# The dry run's append-only logs (~37 MB/day for HYPE, mostly the journal). The bot appends to them
# without reopening and reads none back at startup (the risk monitor starts at the session start),
# so they move only while it is stopped: copy-and-truncate could drop a line written in between,
# such as a fill. Runs daily; acts only past 1 GB of logs (or the MB given as its argument), about
# monthly, at the cost of one restart (drain, then the shift's warm-up). The archive grows
# ~4 MB/day, which the 25 GB disk holds for years; nothing is deleted.
ARCHIVE = f"""#!/bin/sh
set -eu
cd /root/xemm/LIGHTER_ASTER_BOT/runs/dry-run
LOGS="bot-*-journal.jsonl book_sanity_*.jsonl bot-*.equity.jsonl sim-*.diag.jsonl"
used=$(du -cm $LOGS 2>/dev/null | tail -1 | cut -f1)
[ "$used" -ge "${{1:-1024}}" ] || exit 0
dest=../archive/$(date -u +%Y%m%dT%H%M%SZ)
mkdir -p "$dest"
systemctl stop {SERVICE}
for f in $LOGS; do if [ -f "$f" ]; then mv "$f" "$dest/"; fi; done
systemctl start {SERVICE}
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


def ssh(cmd, stdin=None):
    args = ["ssh", "-i", str(KEY), "-o", "IdentitiesOnly=yes", "-o", "BatchMode=yes", HOST, cmd]
    return subprocess.run(args, input=stdin, check=True)


def tracked_tar():
    files = subprocess.run(["git", "ls-files", "-z", "LIGHTER_ASTER_BOT"], cwd=ROOT, check=True,
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
    # One job: the VPS has 1 CPU and ~1 GB RAM (plus 4 GB swap for the thin-LTO link).
    ssh(f". ~/.cargo/env && cd {REMOTE} && CARGO_BUILD_JOBS=1 cargo build --release")
    # The bot logs to stdout -> journald; cap it so the small disk only grows by runs/dry-run.
    ssh("mkdir -p /etc/systemd/journald.conf.d && printf '[Journal]\\nSystemMaxUse=200M\\n'"
        " > /etc/systemd/journald.conf.d/size.conf && systemctl restart systemd-journald")
    for path, text in ((f"/usr/local/sbin/{SERVICE}-archive", ARCHIVE),
                       (f"/etc/systemd/system/{SERVICE}-archive.service", ARCHIVE_SERVICE),
                       (f"/etc/systemd/system/{SERVICE}-archive.timer", ARCHIVE_TIMER)):
        ssh(f"cat > {path} <<'EOF'\n{text}EOF")
    ssh(f"chmod 755 /usr/local/sbin/{SERVICE}-archive && systemctl daemon-reload"
        f" && systemctl enable -q --now {SERVICE}-archive.timer")
    ssh(f"cat > /etc/systemd/system/{SERVICE}.service <<'EOF'\n{UNIT}EOF\n"
        f"systemctl daemon-reload && systemctl enable -q {SERVICE} && systemctl restart {SERVICE}"
        f" && sleep 5 && systemctl --no-pager status {SERVICE} | head -5")


def main():
    cmd = sys.argv[1] if len(sys.argv) > 1 else ""
    if cmd == "sync":
        sync()
    elif cmd == "latency":
        ssh("python3 -", stdin=LATENCY.encode())
    elif cmd == "status":
        ssh(f"systemctl --no-pager status {SERVICE} | head -12; free -h; df -h /; du -sh {REMOTE}/runs/*; "
            f"systemctl list-timers {SERVICE}-archive.timer --no-pager | head -2; true")
    elif cmd == "logs":
        n = sys.argv[2] if len(sys.argv) > 2 else "50"
        ssh(f"journalctl -u {SERVICE} --no-pager -n {int(n)}")
    else:
        sys.exit(__doc__)


if __name__ == "__main__":
    main()
