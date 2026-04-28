from __future__ import annotations

import os
import json
from pathlib import Path
from dotenv import load_dotenv

BASE_DIR = Path(__file__).parent
load_dotenv(BASE_DIR / ".env", override=True)
DATA_DIR = BASE_DIR / "data"
CHARTS_DIR = BASE_DIR / "charts"

DATA_DIR.mkdir(exist_ok=True)
CHARTS_DIR.mkdir(exist_ok=True)

DUCKDB_PATH = DATA_DIR / "cache.duckdb"

LINKEDIN_CLIENT_ID = os.environ.get("LINKEDIN_CLIENT_ID", "")
LINKEDIN_CLIENT_SECRET = os.environ.get("LINKEDIN_CLIENT_SECRET", "")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")

# Parse account IDs from env
_raw_ids = os.environ.get("LINKEDIN_ACCOUNT_IDS", "")
ACCOUNT_IDS: list[str] = [a.strip() for a in _raw_ids.split(",") if a.strip()]

# Human-readable client names mapped to account IDs
ACCOUNT_NAMES: dict[str, str] = {
    "511678682": "Guardian",
    "515647769": "Eureka",
    "508147050": "PTL",
    "519977572": "Eigen",
}

# OAuth redirect URI for local auth flow
OAUTH_REDIRECT_URI = "http://localhost:8765/callback"
OAUTH_SCOPES = ["r_ads", "r_ads_reporting", "rw_ads"]

# LinkedIn API version (YYYYMM format)
API_VERSION = "202601"


def get_token(account_id: str) -> dict | None:
    """Load stored OAuth token for an account."""
    env_key = f"LINKEDIN_TOKEN_{account_id}"
    raw = os.environ.get(env_key)
    if raw:
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            return None
    return None


def save_token(account_id: str, token_data: dict) -> None:
    """Persist token to .env file."""
    env_path = BASE_DIR / ".env"
    env_key = f"LINKEDIN_TOKEN_{account_id}"
    token_json = json.dumps(token_data)

    if env_path.exists():
        lines = env_path.read_text().splitlines()
        found = False
        new_lines = []
        for line in lines:
            if line.startswith(f"{env_key}="):
                new_lines.append(f'{env_key}={token_json}')
                found = True
            else:
                new_lines.append(line)
        if not found:
            new_lines.append(f'{env_key}={token_json}')
        env_path.write_text("\n".join(new_lines) + "\n")
    else:
        env_path.write_text(f'{env_key}={token_json}\n')

    # Update in-memory env so subsequent get_token() calls work in same session
    os.environ[env_key] = token_json
