# deploy/ — run kdp as a service (capture 24/7 → process → Google Drive)

Turnkey artifacts for hosting data collection on a small US-East VPS. Full
step-by-step in [`docs/runbooks/runbook-server.md`](../docs/runbooks/runbook-server.md).

**Decision:** Hetzner **CPX21** (3 vCPU / 4 GB / 80 GB NVMe, ~$15/mo) in
**Ashburn, VA** (next to Kalshi's `us-east-1`), Ubuntu 24.04. Capture runs always
on local NVMe; the compressed output (+ a raw backup) is archived to **Google
Drive** via `rclone`; local disk is reclaimed only after a verified upload.

## Files

| File | Role |
|---|---|
| `install.sh` | one-shot, idempotent: packages + kdp user + dirs + binaries + units + timers; prints any missing secret |
| `kdp.env.example` | config template → `/etc/kdp/kdp.env` (chmod 600) |
| `kdp-capture@.service` | capture, one instance per session (`systemctl start kdp-capture@<name>`); `Restart=always` |
| `kdp-archive.sh` | `[session]` = archive one now (event-driven) · no arg = sweep all settled. process → upload Parquet + raw to Drive (verified) → opt-in prune |
| `kdp-archive@.service` | per-session immediate archive (triggered by the watcher on a stop) |
| `kdp-archive.{service,timer}` | nightly sweep (catch-all) |
| `kdp-settlewatch.sh` | poll Kalshi status → on settlement: grace → graceful stop → fire the per-session archive |
| `kdp-rawsync.sh` | rolling, additive `rclone copy` of the IN-FLIGHT raw JSONL to `$KDP_RCLONE_REMOTE/<session>/raw-inflight/` — mid-event safety net so losing the box can't lose un-archived data; self-stops after capture ends |
| `kdp-rawsync@.{service,timer}` | per-session rolling-backup cadence (every ~3 min); start alongside capture: `systemctl start kdp-rawsync@<session>.timer` |
| `kdp-health.sh` | watchdog: low disk / failed units / stale capture → webhook alert |
| `kdp-health.{service,timer}` | run the watchdog every 5 min |

## Quick start (on the server, as root)

```bash
git clone https://github.com/PCeltide/snapevent.git && cd snapevent
cargo build --release --workspace        # needs rustup; see runbook §2
sudo bash deploy/install.sh              # packages, kdp user, dirs, binaries, units, timers
# then place the 3 secrets (RSA key, KALSHI_API_KEY_ID, rclone Drive) — install.sh
# lists exactly which are missing — and edit /etc/kdp/kdp.env (KDP_TICKERS, remote).
systemctl start kdp-capture@2026-06-12-AAABBB     # ... and `stop` it after the event
```

Full ordered bring-up + the **sensitive-files manifest** are in the
[runbook](../docs/runbooks/runbook-server.md).

## Safety (built in)

`kdp-archive.sh` deletes raw **only** when: the session's capture is stopped +
settled (no writes for `KDP_SETTLE_MINUTES`), `kdp-process` succeeded, every
`manifest.complete == true`, the Parquet upload **and** the raw `tar.gz` backup
are both checksum-verified on Drive, and `KDP_PRUNE=1` is explicitly set. It
starts in **report-only** mode (`KDP_PRUNE=0`) — flip it on once you've watched a
few cycles. After pruning, both the Parquet and the original raw still live on
Drive; local is just a hot cache.

> Shell scripts here target `bash` on Linux. Run `shellcheck deploy/*.sh` on the
> server before first use.
