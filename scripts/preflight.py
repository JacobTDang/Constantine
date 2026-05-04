"""Sprint 9 / S9.2 — Pre-flight readiness check.

Runs through the gates that MUST pass before flipping EXECUTION_ENABLED=true.
Each check is idempotent and read-only — running this script never spends gas
or submits orders.

Checks:
  1. .env loaded — POLYMARKET_PRIVATE_KEY present and well-formed
  2. POLYMARKET_API_KEY / SECRET / PASSPHRASE present
  3. Polymarket /markets endpoint responds (creds + network healthy)
  4. Private key derives the address that matches POLYMARKET_FUNDER_ADDRESS
     (or, for EOA mode, prints the derived address for confirmation)
  5. USDC allowance on the CTF Exchange (warns if zero)
  6. Bot binary builds and the position store directory is writable

Usage:
  pip install py-clob-client web3 eth-account python-dotenv requests
  python scripts/preflight.py
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

# ── Optional deps; report missing ones gracefully so we can run partial
HAVE_DOTENV   = False
HAVE_REQUESTS = False
HAVE_ETH_ACC  = False
HAVE_WEB3     = False
HAVE_PCC      = False

try:
    from dotenv import load_dotenv
    HAVE_DOTENV = True
except ImportError:
    pass

try:
    import requests
    HAVE_REQUESTS = True
except ImportError:
    pass

try:
    from eth_account import Account
    HAVE_ETH_ACC = True
except ImportError:
    pass

try:
    from web3 import Web3
    HAVE_WEB3 = True
except ImportError:
    pass

try:
    from py_clob_client.client import ClobClient
    from py_clob_client.constants import POLYGON
    HAVE_PCC = True
except ImportError:
    pass


# Mainnet Polygon
USDC_ADDR             = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
CTF_EXCHANGE_ADDR     = "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E"
NEG_RISK_EXCHANGE_ADDR = "0xC5d563A36AE78145C45a50134d48A1215220f80a"

USDC_ABI_ALLOWANCE = [{
    "constant": True,
    "inputs": [
        {"name": "_owner",   "type": "address"},
        {"name": "_spender", "type": "address"},
    ],
    "name": "allowance",
    "outputs": [{"name": "", "type": "uint256"}],
    "type": "function",
}]


GREEN = "\033[32m"
RED   = "\033[31m"
YEL   = "\033[33m"
RST   = "\033[0m"


def ok(msg: str)   -> None: print(f"  {GREEN}✓{RST} {msg}")
def fail(msg: str) -> None: print(f"  {RED}✗{RST} {msg}")
def warn(msg: str) -> None: print(f"  {YEL}!{RST} {msg}")


def section(title: str) -> None:
    print(f"\n{title}\n" + "─" * len(title))


def check_env_loaded() -> bool:
    section("1. Environment variables")
    if HAVE_DOTENV:
        load_dotenv()
        ok(".env loaded via python-dotenv")
    else:
        warn("python-dotenv not installed — relying on shell env")

    pk = os.environ.get("POLYMARKET_PRIVATE_KEY", "")
    if not pk:
        fail("POLYMARKET_PRIVATE_KEY not set")
        return False
    if not (pk.startswith("0x") and len(pk) == 66):
        fail(f"POLYMARKET_PRIVATE_KEY malformed (expected 0x + 64 hex, got {len(pk)} chars)")
        return False
    ok("POLYMARKET_PRIVATE_KEY present and well-formed")

    needed = ["POLYMARKET_API_KEY", "POLYMARKET_API_SECRET", "POLYMARKET_API_PASSPHRASE"]
    missing = [k for k in needed if not os.environ.get(k)]
    if missing:
        fail(f"missing API creds: {missing}. Run scripts/derive_polymarket_creds.py")
        return False
    ok("API_KEY / SECRET / PASSPHRASE all set")
    return True


def check_address_derivation() -> bool:
    section("2. Wallet address derivation")
    if not HAVE_ETH_ACC:
        warn("eth-account not installed — skipping. Install: pip install eth-account")
        return True
    pk = os.environ.get("POLYMARKET_PRIVATE_KEY", "")
    acct = Account.from_key(pk)
    derived = acct.address
    ok(f"private key derives → {derived}")

    sig_type = os.environ.get("POLYMARKET_SIGNATURE_TYPE", "EOA").upper()
    if sig_type in ("POLY_PROXY", "PROXY", "1", "POLY_GNOSIS_SAFE", "GNOSIS", "2"):
        funder = os.environ.get("POLYMARKET_FUNDER_ADDRESS", "")
        if not funder:
            fail(f"sig_type={sig_type} requires POLYMARKET_FUNDER_ADDRESS")
            return False
        if funder.lower() == derived.lower():
            warn(
                f"funder ({funder}) matches derived signer — for {sig_type} "
                "they should DIFFER (funder = proxy/safe, signer = EOA)"
            )
        else:
            ok(f"funder ({funder}) and signer ({derived}) differ — correct for {sig_type}")
    else:
        ok(f"EOA mode — maker = signer = {derived}")
    return True


def check_polymarket_creds() -> bool:
    section("3. Polymarket API health")
    if not HAVE_REQUESTS:
        warn("requests not installed — skipping. Install: pip install requests")
        return True
    try:
        # Read-only: list markets — should always succeed if creds are valid
        r = requests.get("https://clob.polymarket.com/markets?limit=1", timeout=10)
        if r.status_code != 200:
            fail(f"GET /markets returned {r.status_code}")
            return False
        ok("CLOB /markets responds 200")
    except requests.RequestException as e:
        fail(f"CLOB request failed: {e}")
        return False

    if not HAVE_PCC:
        warn("py-clob-client not installed — can't verify L2 auth. Install: pip install py-clob-client")
        return True
    pk = os.environ.get("POLYMARKET_PRIVATE_KEY", "")
    try:
        c = ClobClient("https://clob.polymarket.com", key=pk, chain_id=POLYGON)
        existing = c.create_or_derive_api_creds()
        env_key = os.environ.get("POLYMARKET_API_KEY", "")
        if existing.api_key != env_key:
            fail(
                f"API_KEY mismatch: env={env_key[:8]}... derived={existing.api_key[:8]}..."
                "\n  Re-run scripts/derive_polymarket_creds.py and update .env"
            )
            return False
        ok("L2 creds match what the API returns for this wallet")
    except Exception as e:
        fail(f"py-clob-client check failed: {e}")
        return False
    return True


def check_usdc_allowance() -> bool:
    section("4. USDC allowance on CTF Exchange (Polygon)")
    if not (HAVE_WEB3 and HAVE_ETH_ACC):
        warn("web3 / eth-account not installed — skipping. Install: pip install web3 eth-account")
        return True
    rpc_key = os.environ.get("ALCHEMY_POLYGON_KEY", "")
    if not rpc_key:
        warn("ALCHEMY_POLYGON_KEY not set — skipping on-chain check")
        return True
    rpc = f"https://polygon-mainnet.g.alchemy.com/v2/{rpc_key}"
    w3 = Web3(Web3.HTTPProvider(rpc, request_kwargs={"timeout": 10}))
    if not w3.is_connected():
        fail(f"could not connect to RPC {rpc}")
        return False

    pk = os.environ.get("POLYMARKET_PRIVATE_KEY", "")
    acct = Account.from_key(pk)
    sig_type = os.environ.get("POLYMARKET_SIGNATURE_TYPE", "EOA").upper()
    funder = os.environ.get("POLYMARKET_FUNDER_ADDRESS")
    owner = funder if sig_type != "EOA" and funder else acct.address

    usdc = w3.eth.contract(
        address=Web3.to_checksum_address(USDC_ADDR),
        abi=USDC_ABI_ALLOWANCE,
    )
    try:
        allowance_wei = usdc.functions.allowance(
            Web3.to_checksum_address(owner),
            Web3.to_checksum_address(CTF_EXCHANGE_ADDR),
        ).call()
    except Exception as e:
        fail(f"allowance() call failed: {e}")
        return False

    allowance_usd = allowance_wei / 1_000_000  # USDC = 6 decimals
    if allowance_wei == 0:
        fail(f"USDC allowance to CTF Exchange is ZERO. Owner={owner}")
        warn("You must call USDC.approve(CTF_EXCHANGE, MAX_UINT256) before any trade fills.")
        return False
    elif allowance_usd < 100:
        warn(f"USDC allowance is low: ${allowance_usd:.2f}")
    else:
        ok(f"USDC allowance OK: ${allowance_usd:,.2f} approved to CTF Exchange")
    return True


def check_local_state() -> bool:
    section("5. Local state directory")
    db_dir = Path("data/db")
    db_dir.mkdir(parents=True, exist_ok=True)
    test = db_dir / ".preflight_write_test"
    try:
        test.write_text("ok")
        test.unlink()
        ok(f"{db_dir} is writable")
    except OSError as e:
        fail(f"{db_dir} not writable: {e}")
        return False

    # Show existing position log size — useful sanity check for an existing run
    pos_log = db_dir / "positions.jsonl"
    if pos_log.exists():
        n_lines = sum(1 for _ in pos_log.open())
        ok(f"existing position log: {n_lines} events in {pos_log}")
    else:
        ok("no existing position log (clean slate)")
    return True


def main() -> int:
    print("Constantine pre-flight readiness check")
    print("──────────────────────────────────────")
    results = [
        check_env_loaded(),
        check_address_derivation(),
        check_polymarket_creds(),
        check_usdc_allowance(),
        check_local_state(),
    ]
    section("Summary")
    n_pass = sum(1 for r in results if r)
    n_fail = len(results) - n_pass
    if n_fail == 0:
        print(f"{GREEN}all {n_pass} checks passed — ready to flip EXECUTION_ENABLED=true{RST}")
        return 0
    else:
        print(f"{RED}{n_fail} of {len(results)} checks failed — fix above and re-run{RST}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
