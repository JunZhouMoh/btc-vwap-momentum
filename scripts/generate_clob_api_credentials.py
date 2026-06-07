#!/usr/bin/env python3
"""Generate Polymarket CLOB API credentials.

Uses PRIVATE_KEY (+ optional FUNDER_ADDRESS / SIGNATURE_TYPE) to generate
(or derive) a valid API key bundle via py_clob_client.

Default behavior updates POLY_API_KEY / POLY_API_SECRET / POLY_API_PASSPHRASE
in .env. Use --print-only to only print masked details.
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any, Dict

from dotenv import dotenv_values
from py_clob_client.client import ClobClient


def _clean(value: str | None) -> str:
    if value is None:
        return ""
    return str(value).strip().strip('"').strip("'")


def _coerce_int(value: str | None, default: int) -> int:
    text = _clean(value)
    if text.startswith("$"):
        text = text[1:]
    return int(text or str(default))


def _pick(obj: Any, *names: str) -> str:
    for name in names:
        if isinstance(obj, dict) and name in obj and obj[name]:
            return str(obj[name])
        if hasattr(obj, name):
            value = getattr(obj, name)
            if value:
                return str(value)
    return ""


def _load_env(env_path: Path) -> Dict[str, str]:
    values = dotenv_values(env_path)
    return {k: "" if v is None else str(v) for k, v in values.items()}


def _replace_env_keys(env_path: Path, updates: Dict[str, str]) -> None:
    lines = env_path.read_text(encoding="utf-8").splitlines()
    seen = {k: False for k in updates}
    out = []

    for line in lines:
        stripped = line.strip()
        replaced = False
        for key, value in updates.items():
            if stripped.startswith(key + "="):
                out.append(f"{key}={value}")
                seen[key] = True
                replaced = True
                break
        if not replaced:
            out.append(line)

    for key, value in updates.items():
        if not seen[key]:
            out.append(f"{key}={value}")

    env_path.write_text("\n".join(out) + "\n", encoding="utf-8")


def _masked(value: str) -> str:
    if len(value) <= 8:
        return "*" * len(value)
    return f"{value[:4]}...{value[-4:]}"


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate Polymarket CLOB API credentials")
    parser.add_argument(
        "--env",
        default=".env",
        help="Path to .env file (default: ./.env)",
    )
    parser.add_argument(
        "--print-only",
        action="store_true",
        help="Do not modify .env, only print masked credential info",
    )
    args = parser.parse_args()

    env_path = Path(args.env).resolve()
    if not env_path.exists():
        print(f"ERROR: .env file not found: {env_path}")
        return 1

    env = _load_env(env_path)

    private_key = _clean(env.get("PRIVATE_KEY", ""))
    clob_host = _clean(env.get("CLOB_HOST", "https://clob.polymarket.com")) or "https://clob.polymarket.com"
    chain_id = _coerce_int(env.get("CHAIN_ID", "137"), 137)
    signature_type = _coerce_int(env.get("SIGNATURE_TYPE", "0"), 0)
    funder_address = _clean(env.get("FUNDER_ADDRESS", "")) or None

    if not private_key:
        print("ERROR: PRIVATE_KEY is missing in .env")
        return 1
    if not private_key.startswith("0x"):
        print("ERROR: PRIVATE_KEY must start with 0x")
        return 1

    client = ClobClient(
        host=clob_host,
        key=private_key,
        chain_id=chain_id,
        signature_type=signature_type,
        funder=funder_address,
    )

    if hasattr(client, "create_or_derive_api_creds"):
        creds = client.create_or_derive_api_creds()
    elif hasattr(client, "create_api_key"):
        creds = client.create_api_key()
    else:
        print("ERROR: Installed py_clob_client does not support API credential generation")
        return 1

    api_key = _pick(creds, "api_key", "apiKey", "key")
    api_secret = _pick(creds, "api_secret", "secret", "apiSecret")
    api_passphrase = _pick(creds, "api_passphrase", "passphrase", "apiPassphrase")

    if not api_key or not api_secret or not api_passphrase:
        print("ERROR: Generated credential bundle is incomplete")
        return 1

    print("Generated CLOB credential bundle:")
    print(f"  POLY_API_KEY:        {_masked(api_key)} (len={len(api_key)})")
    print(f"  POLY_API_SECRET:     {_masked(api_secret)} (len={len(api_secret)})")
    print(f"  POLY_API_PASSPHRASE: {_masked(api_passphrase)} (len={len(api_passphrase)})")

    if args.print_only:
        print("Skipped .env update (--print-only)")
        return 0

    _replace_env_keys(
        env_path,
        {
            "POLY_API_KEY": api_key,
            "POLY_API_SECRET": api_secret,
            "POLY_API_PASSPHRASE": api_passphrase,
        },
    )
    print(f"Updated .env at: {env_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
