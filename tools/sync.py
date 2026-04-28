from __future__ import annotations

"""
Sync LinkedIn Ads API data → local DuckDB cache.

Smart sync logic:
  - First run (no data): pulls full 365 days of history
  - Subsequent syncs: refreshes only last 2 days (today + yesterday)
"""

import uuid
from datetime import datetime, timedelta

import config
from tools import cache, linkedin_client


def _has_data(account_id: str) -> bool:
    """Check if we have any analytics data for this account."""
    rows = cache.query(
        f"SELECT COUNT(*) AS n FROM ad_analytics WHERE account_id = '{account_id}'"
    )
    return rows[0]["n"] > 0 if rows else False


def sync_account(
    account_id: str,
    weeks_back: int | None = None,
    days_back: int | None = None,
    verbose: bool = True,
) -> dict:
    """
    Sync one account. Smart mode:
      - If no existing data → pull 365 days (full history)
      - If data exists → pull last 2 days only (refresh)
      - Override with weeks_back or days_back if explicitly provided
    """
    cache.init_db()
    sync_id = str(uuid.uuid4())
    started_at = datetime.now()
    total_rows = 0
    errors = []

    def _log(msg: str):
        if verbose:
            print(f"  [{account_id}] {msg}")

    # Determine how far back to pull
    if days_back is not None:
        effective_days = days_back
        mode = "full" if days_back >= 90 else "refresh"
    elif weeks_back is not None:
        effective_days = weeks_back * 7
        mode = "historical"
    elif _has_data(account_id):
        effective_days = 2
        mode = "refresh"
    else:
        effective_days = 365
        mode = "full_history"

    _log(f"Sync mode: {mode} ({effective_days} days back)")

    try:
        # --- Campaign groups ---
        _log("Fetching campaign groups...")
        groups = linkedin_client.get_campaign_groups(account_id)
        n = cache.upsert_campaign_groups(account_id, groups)
        total_rows += n
        _log(f"  {n} campaign groups cached.")

        # --- Campaigns ---
        _log("Fetching campaigns...")
        campaigns = linkedin_client.get_campaigns(account_id)
        n = cache.upsert_campaigns(account_id, campaigns)
        total_rows += n
        _log(f"  {n} campaigns cached.")

        # --- Daily analytics by campaign group ---
        _log(f"Fetching daily analytics by campaign group ({effective_days} days)...")
        rows = linkedin_client.get_analytics(
            account_id=account_id,
            pivot="CAMPAIGN_GROUP",
            granularity="DAILY",
            days_back=effective_days,
        )
        n = cache.upsert_analytics(account_id, rows, "DAILY")
        total_rows += n
        _log(f"  {n} daily campaign-group rows cached.")

        # --- Daily analytics by campaign ---
        _log(f"Fetching daily analytics by campaign ({effective_days} days)...")
        rows = linkedin_client.get_analytics(
            account_id=account_id,
            pivot="CAMPAIGN",
            granularity="DAILY",
            days_back=effective_days,
        )
        n = cache.upsert_analytics(account_id, rows, "DAILY")
        total_rows += n
        _log(f"  {n} daily campaign rows cached.")

        completed_at = datetime.now()
        cache.log_sync(sync_id, account_id, mode, started_at, completed_at, total_rows, None)
        _log(f"Sync complete. {total_rows} total rows written.")

        return {
            "account_id": account_id,
            "status": "ok",
            "mode": mode,
            "rows_written": total_rows,
            "duration_seconds": round((completed_at - started_at).total_seconds(), 1),
        }

    except Exception as e:
        error_msg = str(e)
        cache.log_sync(sync_id, account_id, mode, started_at, None, total_rows, error_msg)
        _log(f"ERROR: {error_msg}")
        return {
            "account_id": account_id,
            "status": "error",
            "error": error_msg,
            "rows_written": total_rows,
        }


def sync_demographics(
    account_id: str,
    campaign_id: str,
    pivot_type: str = "MEMBER_JOB_TITLE",
    weeks_back: int = 4,
    verbose: bool = True,
) -> dict:
    """Sync demographic data for a specific campaign."""
    cache.init_db()

    def _log(msg: str):
        if verbose:
            print(f"  [{account_id}] {msg}")

    try:
        _log(f"Fetching {pivot_type} demographics for campaign {campaign_id} ({weeks_back} weeks)...")
        rows = linkedin_client.get_demographics(
            account_id=account_id,
            pivot_type=pivot_type,
            weeks_back=weeks_back,
            campaign_ids=[campaign_id],
        )
        n = cache.upsert_demographics(account_id, campaign_id, pivot_type, rows)
        _log(f"  {n} demographic rows cached.")
        return {"status": "ok", "rows_written": n}

    except Exception as e:
        _log(f"ERROR: {e}")
        return {"status": "error", "error": str(e)}


def sync_all_accounts(
    weeks_back: int | None = None,
    days_back: int | None = None,
    verbose: bool = True,
) -> list[dict]:
    """Sync all configured accounts using smart sync logic."""
    results = []
    for account_id in config.ACCOUNT_IDS:
        if verbose:
            print(f"\nSyncing account {account_id}...")
        result = sync_account(
            account_id,
            weeks_back=weeks_back,
            days_back=days_back,
            verbose=verbose,
        )
        results.append(result)
    return results
