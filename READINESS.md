# Pre-Live Readiness Checklist

This document is the gate between "observe-mode" and "real money on Polygon."
Every step must pass before flipping `EXECUTION_ENABLED=true`.

## Where the bot is right now

- 489 tests passing through Sprint 8
- Architecture: data streams → features → multi-market signals → executor →
  CLOB submission → position ledger → settlement reconciliation
- Watchdog + heartbeat + dashboard binary all built
- Wire format verified against Polymarket's official `rs-clob-client`
- Default `OrderType::Fak`, `EXECUTION_ENABLED=false`, `DRY_RUN=true`

## What you (the human) must do before going live

### 1. Derive Polymarket L2 API credentials

Polymarket has no UI for this — must be done programmatically.

```bash
pip install py-clob-client python-dotenv
python scripts/derive_polymarket_creds.py
# ... copy the three lines into .env
```

Output goes to STDOUT in `.env` format:
```
POLYMARKET_API_KEY=...
POLYMARKET_API_SECRET=...
POLYMARKET_API_PASSPHRASE=...
```

### 2. Decide your signature type

| Wallet type | `POLYMARKET_SIGNATURE_TYPE` | `POLYMARKET_FUNDER_ADDRESS` |
|---|---|---|
| Raw EOA (just a private key, no Polymarket UI proxy) | `EOA` (default) | unset |
| Magic-link smart wallet (the Polymarket UI flow) | `POLY_PROXY` | the proxy address shown in Polymarket UI |
| Gnosis Safe | `POLY_GNOSIS_SAFE` | the safe address |

**For Magic-link wallets:** open Polymarket in a browser, connect, and copy
the address shown in the wallet header. That's your funder address. The
private key in your `.env` is the EOA that controls the proxy — the bot
signs with that key but the orders are sent on behalf of the proxy.

### 3. Set USDC allowance on the CTF Exchange

This is a **one-time on-chain transaction** that lets the exchange spend
USDC from your wallet. Without it, every order will fail.

Contract address (Polygon mainnet):
- USDC: `0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174`
- CTF Exchange: `0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E`

The simplest way: use the Polymarket UI to do a tiny manual trade. The
first trade triggers the approval prompt automatically. After that the
bot can trade up to that allowance.

Alternatively, send `USDC.approve(CTF_EXCHANGE, MAX_UINT256)` directly via
a wallet UI. **Do not script this from the bot** — keeping the approval
out of automation is a safety property.

### 4. Run pre-flight check

```bash
pip install py-clob-client web3 eth-account python-dotenv requests
python scripts/preflight.py
```

Must show **all 5 checks passed** before proceeding.

### 5. Run observe-mode for at least 24 hours

Before flipping anything live, let the bot run with `EXECUTION_ENABLED=false`
(default) for a full day. Verify:
- `data/db/signals.jsonl` is growing — signals are firing
- `data/db/positions.jsonl` is empty (no execution yet — correct)
- `data/db/settlements.jsonl` has entries as windows close
- Dashboard renders cleanly: `cargo run --release --bin dashboard`
- Heartbeat lines in the log (grep for `"HB"`)
- Watchdog hasn't tripped (no `DATA STALE` lines)

Then run `python scripts/observe_report.py` and check:
- Signals fired at a reasonable rate (5-30/hr is normal)
- Hypothetical P&L is consistent with the backtest
- No "fired despite thin book" warnings

### 6. Testnet drill (Amoy)

Polymarket has a testnet at chain_id `80002` (Amoy). Get free test USDC
from the Polygon Amoy faucet, then:

1. Set `chain-id=80002` in `derive_polymarket_creds.py`
2. Update RPC URL to Amoy
3. Run a $1 test trade end-to-end
4. Verify the position appears in `data/db/positions.jsonl`
5. Verify settlement reconciles when the window closes

This is the **only** way to verify the EIP-712 wire format byte-for-byte.
Even though we matched it against `rs-clob-client`, a real Polygon
contract call is the ground truth.

### 7. Live ramp ($5 → $15 → $30)

After the testnet drill passes:

1. Set `MAX_BET_DOLLARS=5` in `.env`
2. Set `EXECUTION_ENABLED=true`
3. Run for 24 hours. Monitor the dashboard. Check that:
   - At least 1 trade lands and settles correctly
   - Realised P&L matches the observe-mode hypothetical within ±10%
4. If healthy, bump `MAX_BET_DOLLARS=15` for another day
5. If healthy, bump `MAX_BET_DOLLARS=30` for the steady-state ramp

## Kill switches

The bot has multiple ways to halt itself:

- **Manual:** `pkill polymarket-bot`
- **Daily loss limit** (`DAILY_LOSS_LIMIT_DOLLARS`, default $75) — auto-trips
- **Weekly loss limit** (`WEEKLY_LOSS_LIMIT_DOLLARS`, default $150) — auto-trips
- **Data-loss watchdog** — trips on stale spot/chainlink/book streams
- **Max open exposure** (`MAX_OPEN_EXPOSURE_DOLLARS`, default $300) — refuses
  new trades when exposure is at the cap (NOT a kill switch — a gate)

Once the kill switch trips, you must restart the bot manually after
investigating the cause.

## Known limitations (post-Sprint 8)

These do NOT block going live but are worth understanding:

- **No fill-confirmation websocket** — we record positions as `Submitted` on
  HTTP 200, but don't subscribe to Polymarket's user-channel websocket to
  hear about fills. Settlement reconciliation works via market resolution,
  not fill events. Sprint 9.5 / 10 territory.
- **Single-leg intramarket arb** — the executor's `Skipped` path for
  intramarket signals is not yet wired into a multi-leg coordinator.
  Multi-leg execution lands in a future sprint.
- **Slippage feedback loop** — we don't yet track actual fill prices vs
  expected, so we can't auto-tune the slippage assumption in
  `compute_paper_pnl`.
- **No automatic USDC approval** — by design. You approve once via UI.

## Useful commands

```bash
# Bot in observe mode (default)
cargo run --release --bin polymarket-bot

# Dashboard alongside (separate terminal)
cargo run --release --bin dashboard

# Pre-flight check
python scripts/preflight.py

# Observe-mode report
python scripts/observe_report.py

# Backtest (ML training + alpha sims)
python scripts/backtest.py --klines data/btc_5min_klines.parquet
```
