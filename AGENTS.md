# Repository Instructions

- Do not run `cargo fmt` anywhere in this stack unless the user explicitly overrides this instruction. Keep edits narrowly formatted by hand to avoid broad formatting churn.
- Always use release-mode cargo (`cargo build --release`, `cargo test --release`) unless the user says otherwise, so `target/debug` artifacts do not accumulate.
- Delete any consumed patch or diff files after applying them, so stale artifacts do not confuse later work.
- After implementing bot behavior/config changes from a plan, commit the validated changes, then rebuild and restart the dry run (`docker compose up -d --build dryrun` in `LIGHTER_ASTER_BOT/`). Restart a live bot (`run --mode live`, see `LIGHTER_ASTER_BOT/RUNBOOK.md`) only when one is running, and not if the user says not to.
- `run --mode live`, `taker run` without `--observe-only`, `probe aster-place-cancel`, and the `*-market`/`*-roundtrip` probes submit real orders. Routine validation uses unit/local-transport tests and the dry run (`run --mode dry-run`, RUNBOOK "Dry run"), which needs no credentials and submits nothing real.
- `aster.env`, `lighter.env` and `runs/` stay local and ignored by git; check `git status --short` before each commit.
- For results, run the existing reports from the repository root before writing any analysis code: `python dryrun_summary.py` (one screen per dry-run market), `python screener_summary.py` (the screener's ranking; it runs the report in Docker), and for detail `combined_pnl.py`, `bot_stats.py` and `trade_history.py` (`--dry-run --market <M>`). If a question needs something they lack, extend the fitting script instead of writing a one-off.
