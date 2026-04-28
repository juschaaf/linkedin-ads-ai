from __future__ import annotations

"""
DuckDB cache layer.

Schema:
  accounts            — ad account metadata
  campaign_groups     — campaign group metadata
  campaigns           — campaign metadata (includes bid, status, targeting)
  ad_analytics        — time-series metrics (spend, impressions, clicks) per pivot
  demographics        — demographic breakdown rows

All monetary values stored in USD (float). LinkedIn returns micro-currency; conversion
happens during sync.
"""

import json
from contextlib import contextmanager
from datetime import datetime

import duckdb

import config


@contextmanager
def _conn():
    con = duckdb.connect(str(config.DUCKDB_PATH))
    try:
        yield con
        con.commit()
    finally:
        con.close()


# ---------------------------------------------------------------------------
# Schema init
# ---------------------------------------------------------------------------

_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS accounts (
    account_id      VARCHAR PRIMARY KEY,
    name            VARCHAR,
    currency        VARCHAR,
    synced_at       TIMESTAMP
);

CREATE TABLE IF NOT EXISTS campaign_groups (
    group_id        VARCHAR PRIMARY KEY,
    account_id      VARCHAR,
    name            VARCHAR,
    status          VARCHAR,
    total_budget_usd DOUBLE,
    raw_json        VARCHAR
);

CREATE TABLE IF NOT EXISTS campaigns (
    campaign_id     VARCHAR PRIMARY KEY,
    account_id      VARCHAR,
    group_id        VARCHAR,
    name            VARCHAR,
    status          VARCHAR,
    objective       VARCHAR,
    bid_usd         DOUBLE,
    daily_budget_usd DOUBLE,
    total_budget_usd DOUBLE,
    raw_json        VARCHAR
);

CREATE TABLE IF NOT EXISTS ad_analytics (
    id              VARCHAR PRIMARY KEY,  -- composite key: account_id|pivot_by|pivot_value|date_start
    account_id      VARCHAR,
    pivot_by        VARCHAR,   -- e.g. CAMPAIGN, CAMPAIGN_GROUP
    pivot_value     VARCHAR,   -- URN of the pivot entity
    date_start      DATE,
    date_end        DATE,
    granularity     VARCHAR,   -- DAILY, WEEKLY, MONTHLY, ALL
    spend_usd       DOUBLE,
    impressions     BIGINT,
    clicks          BIGINT,
    conversions     BIGINT
);

CREATE TABLE IF NOT EXISTS demographics (
    id              VARCHAR PRIMARY KEY,  -- account_id|campaign_id|pivot_type|pivot_value
    account_id      VARCHAR,
    campaign_id     VARCHAR,
    pivot_type      VARCHAR,  -- MEMBER_JOB_TITLE, MEMBER_SENIORITY, etc.
    pivot_value     VARCHAR,  -- entity URN
    pivot_label     VARCHAR,  -- human-readable name if available
    date_start      DATE,
    date_end        DATE,
    spend_usd       DOUBLE,
    impressions     BIGINT,
    clicks          BIGINT
);

CREATE TABLE IF NOT EXISTS sync_log (
    sync_id         VARCHAR PRIMARY KEY,
    account_id      VARCHAR,
    sync_type       VARCHAR,
    started_at      TIMESTAMP,
    completed_at    TIMESTAMP,
    rows_written    INTEGER,
    error           VARCHAR
);

"""


def init_db() -> None:
    """Create tables if they don't exist."""
    with _conn() as con:
        con.execute(_SCHEMA_SQL)


# ---------------------------------------------------------------------------
# Write helpers
# ---------------------------------------------------------------------------

def upsert_campaign_groups(account_id: str, groups: list[dict]) -> int:
    rows = []
    for g in groups:
        raw_id = g.get("id", "")
        gid = str(raw_id).replace("urn:li:sponsoredCampaignGroup:", "")
        budget = g.get("totalBudget", {}).get("amount")
        rows.append((
            gid,
            account_id,
            g.get("name", ""),
            g.get("status", ""),
            float(budget) if budget else None,
            json.dumps(g),
        ))
    with _conn() as con:
        con.executemany("""
            INSERT OR REPLACE INTO campaign_groups
              (group_id, account_id, name, status, total_budget_usd, raw_json)
            VALUES (?, ?, ?, ?, ?, ?)
        """, rows)
    return len(rows)


def upsert_campaigns(account_id: str, campaigns: list[dict]) -> int:
    rows = []
    for c in campaigns:
        cid = str(c.get("id", "")).replace("urn:li:sponsoredCampaign:", "")
        gid = str(c.get("campaignGroup") or "").replace("urn:li:sponsoredCampaignGroup:", "")
        bid_raw = c.get("unitCost", {}).get("amount")
        daily_raw = c.get("dailyBudget", {}).get("amount")
        total_raw = c.get("totalBudget", {}).get("amount")
        rows.append((
            cid,
            account_id,
            gid,
            c.get("name", ""),
            c.get("status", ""),
            c.get("objectiveType", ""),
            float(bid_raw) if bid_raw else None,
            float(daily_raw) if daily_raw else None,
            float(total_raw) if total_raw else None,
            json.dumps(c),
        ))
    with _conn() as con:
        con.executemany("""
            INSERT OR REPLACE INTO campaigns
              (campaign_id, account_id, group_id, name, status, objective,
               bid_usd, daily_budget_usd, total_budget_usd, raw_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, rows)
    return len(rows)


def upsert_analytics(account_id: str, rows_in: list[dict], granularity: str) -> int:
    rows = []
    for r in rows_in:
        dr = r.get("dateRange", {})
        start = dr.get("start", {})
        end = dr.get("end", {})
        date_start = _dict_to_date(start)
        date_end = _dict_to_date(end)
        pivot = r.get("pivot", "")
        pivot_val = r.get("pivotValue", "")
        row_id = f"{account_id}|{pivot}|{pivot_val}|{date_start}|{granularity}"
        spend_raw = r.get("costInLocalCurrency", 0)
        rows.append((
            row_id,
            account_id,
            pivot,
            pivot_val,
            date_start,
            date_end,
            granularity,
            float(spend_raw) if spend_raw else 0.0,
            int(r.get("impressions", 0) or 0),
            int(r.get("clicks", 0) or 0),
            int(r.get("externalWebsiteConversions", 0) or 0),
        ))
    with _conn() as con:
        con.executemany("""
            INSERT OR REPLACE INTO ad_analytics
              (id, account_id, pivot_by, pivot_value, date_start, date_end,
               granularity, spend_usd, impressions, clicks, conversions)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, rows)
    return len(rows)


def upsert_demographics(account_id: str, campaign_id: str, pivot_type: str, rows_in: list[dict]) -> int:
    rows = []
    for r in rows_in:
        dr = r.get("dateRange", {})
        date_start = _dict_to_date(dr.get("start", {}))
        date_end = _dict_to_date(dr.get("end", {}))
        pivot_val = r.get("pivotValue", "")
        row_id = f"{account_id}|{campaign_id}|{pivot_type}|{pivot_val}"
        spend_raw = r.get("costInLocalCurrency", 0)
        rows.append((
            row_id,
            account_id,
            campaign_id,
            pivot_type,
            pivot_val,
            "",  # label resolved separately if needed
            date_start,
            date_end,
            float(spend_raw) if spend_raw else 0.0,
            int(r.get("impressions", 0) or 0),
            int(r.get("clicks", 0) or 0),
        ))
    with _conn() as con:
        con.executemany("""
            INSERT OR REPLACE INTO demographics
              (id, account_id, campaign_id, pivot_type, pivot_value, pivot_label,
               date_start, date_end, spend_usd, impressions, clicks)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, rows)
    return len(rows)


# ---------------------------------------------------------------------------
# Read helpers (used by agent tools)
# ---------------------------------------------------------------------------

def query(sql: str) -> list[dict]:
    """Run arbitrary SQL and return results as list of dicts."""
    with _conn() as con:
        result = con.execute(sql).fetchdf()
    return result.to_dict(orient="records")


def last_sync_age_hours(account_id: str) -> float | None:
    """Return hours since last successful sync for an account, or None."""
    with _conn() as con:
        rows = con.execute("""
            SELECT completed_at FROM sync_log
            WHERE account_id = ? AND error IS NULL
            ORDER BY completed_at DESC LIMIT 1
        """, [account_id]).fetchall()
    if not rows:
        return None
    last = rows[0][0]
    if isinstance(last, str):
        last = datetime.fromisoformat(last)
    return (datetime.now() - last.replace(tzinfo=None)).total_seconds() / 3600


def log_sync(sync_id: str, account_id: str, sync_type: str, started_at: datetime,
             completed_at: datetime | None, rows_written: int, error: str | None) -> None:
    with _conn() as con:
        con.execute("""
            INSERT OR REPLACE INTO sync_log
              (sync_id, account_id, sync_type, started_at, completed_at, rows_written, error)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, [sync_id, account_id, sync_type, started_at, completed_at, rows_written, error])


# ---------------------------------------------------------------------------
# Util
# ---------------------------------------------------------------------------

def _dict_to_date(d: dict) -> str | None:
    """Convert LinkedIn date dict {year, month, day} to ISO date string."""
    if not d or not d.get("year"):
        return None
    return f"{d['year']:04d}-{d.get('month', 1):02d}-{d.get('day', 1):02d}"
