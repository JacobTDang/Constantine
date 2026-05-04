"""Sprint 9 / S9.1 — Derive Polymarket L2 API credentials from a private key.

Polymarket's CLOB has no UI for creating L2 trading credentials. They must
be derived programmatically using py-clob-client. This script:

  1. Takes a private key from --key or POLYMARKET_PRIVATE_KEY env var
  2. Calls clob_client.create_or_derive_api_creds()
  3. Prints the (api_key, api_secret, api_passphrase) triple in .env format

Run ONCE per wallet. The credentials are stable for the lifetime of the
wallet — you don't need to re-derive on every bot restart.

Usage:
  pip install py-clob-client python-dotenv
  python scripts/derive_polymarket_creds.py
  # ...prints lines like:
  # POLYMARKET_API_KEY=xxxx
  # POLYMARKET_API_SECRET=xxxx
  # POLYMARKET_API_PASSPHRASE=xxxx
  # ...append these to .env

Safety:
  - The script never logs your private key
  - The output goes to STDOUT — pipe carefully if scripting
  - Treat the api_secret like a private key: it can sign trades on your
    behalf if combined with your address
"""
from __future__ import annotations

import argparse
import os
import sys
from typing import Optional

try:
    from py_clob_client.client import ClobClient
    from py_clob_client.constants import POLYGON
except ImportError as e:
    print(
        "ERROR: py-clob-client not installed. Run:\n"
        "  pip install py-clob-client",
        file=sys.stderr,
    )
    sys.exit(1)


CLOB_HOST = "https://clob.polymarket.com"


def derive_creds(private_key: str, chain_id: int = POLYGON) -> dict:
    """Returns {'api_key', 'api_secret', 'api_passphrase'}.

    create_or_derive_api_creds:
      - If creds already exist for this address, returns them
      - Else creates new ones (still tied deterministically to the key)
    """
    client = ClobClient(CLOB_HOST, key=private_key, chain_id=chain_id)
    creds = client.create_or_derive_api_creds()
    return {
        "api_key":        creds.api_key,
        "api_secret":     creds.api_secret,
        "api_passphrase": creds.api_passphrase,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument(
        "--key",
        type=str,
        default=None,
        help="Private key (0x...). Defaults to POLYMARKET_PRIVATE_KEY env var.",
    )
    parser.add_argument(
        "--chain-id",
        type=int,
        default=POLYGON,
        help=f"Polygon mainnet ({POLYGON}) or Amoy testnet (80002)",
    )
    parser.add_argument(
        "--format",
        choices=["env", "json"],
        default="env",
        help="env: lines for .env, json: machine-readable dict",
    )
    args = parser.parse_args()

    pk: Optional[str] = args.key or os.environ.get("POLYMARKET_PRIVATE_KEY")
    if not pk:
        print(
            "ERROR: no private key. Set --key or POLYMARKET_PRIVATE_KEY env var.",
            file=sys.stderr,
        )
        return 2
    if not pk.startswith("0x") or len(pk) != 66:
        print(
            f"ERROR: private key looks malformed (expected 0x-prefixed 64-hex, got {len(pk)} chars)",
            file=sys.stderr,
        )
        return 2

    print(f"# Deriving credentials on chain_id={args.chain_id}...", file=sys.stderr)
    try:
        creds = derive_creds(pk, args.chain_id)
    except Exception as e:
        print(f"ERROR: derivation failed: {e}", file=sys.stderr)
        return 1

    if args.format == "json":
        import json
        print(json.dumps(creds, indent=2))
    else:
        print(f"POLYMARKET_API_KEY={creds['api_key']}")
        print(f"POLYMARKET_API_SECRET={creds['api_secret']}")
        print(f"POLYMARKET_API_PASSPHRASE={creds['api_passphrase']}")
        print(
            "\n# Append the lines above to your .env file.",
            file=sys.stderr,
        )

    return 0


if __name__ == "__main__":
    sys.exit(main())
