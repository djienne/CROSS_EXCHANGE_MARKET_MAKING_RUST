"""Sync the bot to the VPS, build it there and run the HYPE dry run as a systemd service.

  python vps.py sync      # upload tracked files (working tree), build, (re)start the dry run
  python vps.py latency   # TCP-connect and HTTPS round trips to the venues, from the VPS
  python vps.py status    # service state, memory, disk
  python vps.py logs [N]  # last N journal lines (default 50)

Only git-tracked files of LIGHTER_ASTER_BOT go up (uncommitted edits included, untracked files
and *.env never), so no credentials or runs/ leave this machine. The VPS runs the dry run only:
no recorder, no tape. Ponytail: files deleted locally stay on the VPS; if that ever matters, wipe
~/xemm/LIGHTER_ASTER_BOT/src before extracting.
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
    # One job: the VPS has 1 CPU and ~1 GB RAM (plus 4 GB swap for the thin-LTO link).
    ssh(f". ~/.cargo/env && cd {REMOTE} && CARGO_BUILD_JOBS=1 cargo build --release")
    # The bot logs to stdout -> journald; cap it so the small disk only grows by runs/dry-run.
    ssh("mkdir -p /etc/systemd/journald.conf.d && printf '[Journal]\\nSystemMaxUse=200M\\n'"
        " > /etc/systemd/journald.conf.d/size.conf && systemctl restart systemd-journald")
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
        ssh(f"systemctl --no-pager status {SERVICE} | head -12; free -h; df -h /; du -sh {REMOTE}/runs; true")
    elif cmd == "logs":
        n = sys.argv[2] if len(sys.argv) > 2 else "50"
        ssh(f"journalctl -u {SERVICE} --no-pager -n {int(n)}")
    else:
        sys.exit(__doc__)


if __name__ == "__main__":
    main()
