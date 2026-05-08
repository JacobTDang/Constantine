# Autonomous operation guide

The bot is built to run unattended. Set this up once, leave the box on,
and the bot survives reboots, network blips, and process crashes.

## What runs autonomously

After setup, four loops live independently:

```
1. polymarket-bot                    Rust binary — main runtime
   ├── data streams (Binance + chainlink + Polymarket CLOB)
   ├── runner (200ms tick) — evaluates signals, dispatches trades
   ├── watchdog (every 5s) — auto-trips data_stale on stream loss
   ├── allowance watchdog (every 5min) — trips kill switch on USDC drain
   ├── heartbeat (every 30s) — emits "HB" log line
   ├── settlement monitor — reconciles closed markets to position P&L
   ├── window watcher — pre-signs orders on new BTC windows
   ├── execution runner — routes oracle/intramarket/prop signals
   ├── event arb scanner (every 5min) — populates event arb cache
   ├── order TTL loop (every 60s) — cancels stale resting limit orders
   └── periodic reconcile (every 5min) — syncs PositionStore w/ Polymarket

2. nba_projections.py                Python sidecar — Strategy 1 only
   └── refreshes data/nba_projections.json every 5min

3. (optional) daily_review.py        Python cron — once per day
   └── reads yesterday's logs, writes a markdown summary

4. (optional) dashboard              Rust binary — separate terminal
   └── live view of positions, P&L, kill switch state
```

The bot is hard-coded to:
- Auto-reset `data_stale` when streams recover (G11)
- Auto-recover from CLOB rate limits via 3-retry exponential backoff
- Cap total open exposure (`max_open_exposure_dollars`)
- Trip the kill switch on daily/weekly loss limits
- Reload projections cache on file change

## Pre-launch checklist

1. **Fill in `.env`** with your Polymarket credentials. See `READINESS.md`
   for derivation steps.

2. **Run `python scripts/preflight.py`** — must show all 5 checks green.

3. **Set `DRY_RUN=true` and `EXECUTION_ENABLED=false`** at first. Run
   for a full day in observe-only mode. Verify:
   - `data/db/signals.jsonl` is growing
   - Heartbeat log lines appear every 30s
   - `cargo run --release --bin dashboard` shows reasonable state

4. **Flip to `EXECUTION_ENABLED=true`** with `DRY_RUN=true`. Run another
   day. The bot will go through the full submit path but ClobClient
   short-circuits before HTTP, so no real orders. Inspect:
   - `data/db/positions.jsonl` shows expected order_ids (`local-...`)
   - Per-strategy signal counts match what you expect

5. **Testnet drill** (Sprint 9) — set chain_id to Amoy testnet, fund
   with test USDC, set `DRY_RUN=false`, place a $1 trade. Verify
   reconciliation picks up the result.

6. **Live ramp** — `MAX_BET_DOLLARS=5` for 24h, then `15`, then `30`.

## Windows setup (Task Scheduler)

```cmd
REM Build the release binary
cargo build --release --bin polymarket-bot
cargo build --release --bin dashboard

REM Test the supervisor manually first
scripts\supervisor\run_bot.bat
```

Once verified manually, register both supervisors with Task Scheduler:

1. Open Task Scheduler
2. Action → Create Task
3. General tab:
   - Name: "Constantine Bot"
   - Run whether user is logged on or not (yes)
   - Run with highest privileges (yes)
4. Triggers tab → New → "At startup"
5. Actions tab → New → Program: `C:\Users\YOU\Constantine\scripts\supervisor\run_bot.bat`
6. Repeat for `run_sidecar.bat` as a separate task.

The supervisor `.bat` files include exponential backoff (2s → 60s) on
crash. Logs go to standard output — capture them via Task Scheduler's
own logging or wrap with `>> bot.log 2>&1` in the .bat.

## Linux setup (systemd)

```bash
# Build the release binaries
cargo build --release --bin polymarket-bot
cargo build --release --bin dashboard

# Install the systemd units
# Edit YOUR_USERNAME_HERE in both files first!
sudo cp scripts/supervisor/polymarket-bot.service /etc/systemd/system/
sudo cp scripts/supervisor/polymarket-sidecar.service /etc/systemd/system/
sudo systemctl daemon-reload

# Enable + start
sudo systemctl enable polymarket-bot polymarket-sidecar
sudo systemctl start  polymarket-bot polymarket-sidecar

# Watch logs
journalctl -u polymarket-bot -f -u polymarket-sidecar
```

Both units are configured with `Restart=on-failure` + 5s backoff +
StartLimit guards (5 restarts in 5min, then give up — prevents
crash-loops from masking real bugs).

## Daily cron — LLM analysis (optional)

`scripts/daily_review.py` reads the last 24h of your trading data and
generates a markdown report via the Claude API. It runs once per day,
not in the trading loop. **Cost: ~$0.10-0.50/day** at typical trade
volume.

Setup:

```bash
pip install anthropic
echo "ANTHROPIC_API_KEY=sk-ant-..." >> .env
```

Schedule (Linux cron):

```
0 8 * * * cd /home/YOU/Constantine && .venv/bin/python scripts/daily_review.py
```

Schedule (Windows Task Scheduler): same as the supervisor scripts but
pointing at `python scripts\daily_review.py`, triggered daily at 8am.

The report goes to `data/reports/YYYY-MM-DD.md`. Review it manually;
suggested config tweaks (e.g. "narrow oracle threshold from 4¢ to 5¢")
are NOT auto-applied — you must edit `.env` if you agree.

## Operational hygiene

```bash
# Disk usage check (JSONL files grow forever)
du -sh data/db/

# Rotate logs once a month (compress + archive)
gzip data/db/positions.jsonl
mv data/db/positions.jsonl.gz data/db/archive/positions-$(date +%Y%m).jsonl.gz

# Manual kill switches (if something looks wrong)
# Send SIGUSR1 to bot — TODO, not yet implemented (it's a GAPS.md item)
# For now: kill the bot process; it'll auto-restart and pick up state from disk.

# Per-strategy disable (without rebuilding) — TODO, GAPS.md item
# For now: edit RunnerConfig defaults in main.rs and rebuild.
```

## When to manually intervene

The bot will run fine without you for weeks. You SHOULD intervene when:

- **Daily loss limit tripped** (`kill_switch_active` in heartbeat). The
  global kill switch is sticky — only manual reset clears. Investigate
  why losses spiked before resetting.
- **Allowance watchdog tripped** — USDC allowance dropped below floor.
  Top up via Polymarket UI, then `risk.reset_kill_switch()`.
- **Disk filling up** — `du -sh data/db/`. Rotate / archive logs.
- **Per-strategy edge collapses** — review weekly. If Strategy 1 is
  consistently negative, kill it (`risk.kill_strategy(Strategy::PlayerProps)`).

## Reasonable expectations for compute / cost

```
Bot CPU:                      single-digit %, 20 cores idle most of the time
Bot RAM:                      < 200 MB resident
Bot disk:                     < 1 GB/month JSONL growth at light volume
Network:                      ~50 KB/sec sustained (WebSocket streams)
Trading fees (Polymarket):    2% × notional × ~5-50 trades/day
RPC quota (Alchemy free tier): well under 100 req/min limit
NBA API:                      free; rate-limited but the sidecar caches
                              per-player so we don't hammer it
LLM cost (optional):          $3-15/month for daily reviews
```

All of this fits on a $5-10/month VPS or a dedicated home box.
