from __future__ import annotations

"""
Local web server for the LinkedIn Ads AI chat interface.
Run with: python3 server.py
Then open http://localhost:5000 in your browser.
"""

import json
import time
import threading
import webbrowser
from datetime import datetime, date, timedelta, timezone
from pathlib import Path

from flask import Flask, request, jsonify, send_from_directory

import config
from tools import cache, sync

app = Flask(__name__, static_folder="ui")

# Track ongoing sync
_sync_status: dict = {"running": False, "last": None}


# ---------------------------------------------------------------------------
# JSON encoder that handles dates
# ---------------------------------------------------------------------------

class _Encoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, (datetime, date)):
            return o.isoformat()
        return super().default(o)


def _jsonify(data):
    return app.response_class(
        json.dumps(data, cls=_Encoder),
        mimetype="application/json",
    )


# ---------------------------------------------------------------------------
# Routes — static
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    return send_from_directory("ui", "index.html")


# ---------------------------------------------------------------------------
# Routes — accounts & campaigns
# ---------------------------------------------------------------------------

@app.route("/accounts")
def accounts():
    accs = [
        {"account_id": aid, "client_name": config.ACCOUNT_NAMES.get(aid, aid)}
        for aid in config.ACCOUNT_IDS
    ]
    return jsonify(accs)


@app.route("/campaigns")
def campaigns():
    """
    Return campaigns sorted by total impressions within the given date range.
    Query params: account_id (optional), date_from (optional), date_to (optional)
    """
    account_id = request.args.get("account_id", "")
    date_from  = request.args.get("date_from", "")
    date_to    = request.args.get("date_to", "")
    try:
        cache.init_db()

        # Build optional WHERE fragments for the analytics join and campaigns filter
        analytics_date_clauses = ""
        if date_from:
            analytics_date_clauses += f" AND a.date_start >= '{date_from}'"
        if date_to:
            analytics_date_clauses += f" AND a.date_start <= '{date_to}'"

        campaign_where = ""
        if account_id:
            campaign_where = f"WHERE c.account_id = '{account_id}'"

        sql = f"""
            SELECT
                c.campaign_id,
                c.name,
                c.status,
                c.group_id,
                c.account_id,
                COALESCE(SUM(a.impressions), 0) AS total_impressions
            FROM campaigns c
            LEFT JOIN ad_analytics a
                ON a.pivot_value = 'urn:li:sponsoredCampaign:' || c.campaign_id
                AND a.pivot_by = 'CAMPAIGN'
                {analytics_date_clauses}
            {campaign_where}
            GROUP BY c.campaign_id, c.name, c.status, c.group_id, c.account_id
            ORDER BY total_impressions DESC
        """
        rows = cache.query(sql)
        return _jsonify(rows)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ---------------------------------------------------------------------------
# Routes — explorer data
# ---------------------------------------------------------------------------

@app.route("/explorer")
def explorer():
    """
    Return time-series metrics for the explorer panel.
    Query params: account_id, campaign_ids (comma-separated), date_from, date_to, metric
    """
    account_id      = request.args.get("account_id", "")
    campaign_ids_raw = request.args.get("campaign_ids", "")
    date_from       = request.args.get("date_from", "")
    date_to         = request.args.get("date_to", "")
    metric          = request.args.get("metric", "spend_usd")

    # Parse comma-separated campaign IDs
    campaign_ids = [c.strip() for c in campaign_ids_raw.split(",") if c.strip()]

    try:
        cache.init_db()

        # Build WHERE clauses
        wheres = []
        if account_id:
            wheres.append(f"a.account_id = '{account_id}'")
        if campaign_ids:
            id_list = ", ".join(f"'urn:li:sponsoredCampaign:{cid}'" for cid in campaign_ids)
            wheres.append(f"a.pivot_value IN ({id_list})")
            wheres.append("a.pivot_by = 'CAMPAIGN'")
        else:
            wheres.append("a.pivot_by = 'CAMPAIGN_GROUP'")
        if date_from:
            wheres.append(f"a.date_start >= '{date_from}'")
        if date_to:
            wheres.append(f"a.date_start <= '{date_to}'")

        where_sql = "WHERE " + " AND ".join(wheres) if wheres else ""

        # Daily breakdown by campaign group (or specific campaigns if filtered)
        if campaign_ids:
            label_join = "LEFT JOIN campaigns c ON a.pivot_value = 'urn:li:sponsoredCampaign:' || c.campaign_id"
            label_col = "COALESCE(c.name, a.pivot_value) AS label"
        else:
            label_join = "LEFT JOIN campaign_groups cg ON a.pivot_value = 'urn:li:sponsoredCampaignGroup:' || cg.group_id"
            label_col = "COALESCE(cg.name, a.pivot_value) AS label"

        sql = f"""
            SELECT
                a.date_start,
                {label_col},
                SUM(a.spend_usd)    AS spend_usd,
                SUM(a.impressions)  AS impressions,
                SUM(a.clicks)       AS clicks,
                SUM(a.conversions)  AS conversions,
                CASE WHEN SUM(a.impressions) > 0
                     THEN ROUND(SUM(a.clicks) * 100.0 / SUM(a.impressions), 2)
                     ELSE 0 END AS ctr
            FROM ad_analytics a
            {label_join}
            {where_sql}
            GROUP BY a.date_start, label
            ORDER BY a.date_start ASC
        """
        rows = cache.query(sql)

        # Summary totals
        summary_sql = f"""
            SELECT
                SUM(a.spend_usd)    AS total_spend,
                SUM(a.impressions)  AS total_impressions,
                SUM(a.clicks)       AS total_clicks,
                SUM(a.conversions)  AS total_conversions,
                CASE WHEN SUM(a.impressions) > 0
                     THEN ROUND(SUM(a.clicks) * 100.0 / SUM(a.impressions), 2)
                     ELSE 0 END AS avg_ctr
            FROM ad_analytics a
            {where_sql}
        """
        summary = cache.query(summary_sql)

        # Top performers (by selected metric)
        agg_sql = f"""
            SELECT
                {label_col},
                SUM(a.spend_usd)    AS spend_usd,
                SUM(a.impressions)  AS impressions,
                SUM(a.clicks)       AS clicks,
                SUM(a.conversions)  AS conversions,
                CASE WHEN SUM(a.impressions) > 0
                     THEN ROUND(SUM(a.clicks) * 100.0 / SUM(a.impressions), 2)
                     ELSE 0 END AS ctr
            FROM ad_analytics a
            {label_join}
            {where_sql}
            GROUP BY label
            ORDER BY {metric} DESC
            LIMIT 20
        """
        top = cache.query(agg_sql)

        return _jsonify({
            "timeseries": rows,
            "summary": summary[0] if summary else {},
            "top": top,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ---------------------------------------------------------------------------
# Routes — sync
# ---------------------------------------------------------------------------

@app.route("/sync-status")
def sync_status():
    return jsonify(_sync_status)


def _run_sync(days_back: int | None = None):
    """Background sync worker. Pass days_back to force a specific lookback window."""
    global _sync_status
    _sync_status = {"running": True, "last": None, "progress": []}
    results = []
    for aid in config.ACCOUNT_IDS:
        name = config.ACCOUNT_NAMES.get(aid, aid)
        _sync_status["progress"].append(f"Syncing {name}...")
        result = sync.sync_account(aid, days_back=days_back, verbose=False)
        results.append(result)
        status = "✓" if result["status"] == "ok" else "✗"
        _sync_status["progress"].append(
            f"{status} {name}: {result.get('rows_written', 0)} rows "
            f"({result.get('mode', '')}, {result.get('duration_seconds', 0)}s)"
        )
    _sync_status = {
        "running": False,
        "last": datetime.now().isoformat(),
        "results": results,
        "progress": _sync_status.get("progress", []),
    }


@app.route("/sync", methods=["POST"])
def do_sync():
    """Trigger a background refresh sync (last 2 days)."""
    if _sync_status["running"]:
        return jsonify({"error": "Sync already running"}), 409
    threading.Thread(target=_run_sync, kwargs={"days_back": 2}, daemon=True).start()
    return jsonify({"status": "started"})


@app.route("/sync-full", methods=["POST"])
def do_sync_full():
    """Trigger a full 365-day re-sync, overwriting existing data."""
    if _sync_status["running"]:
        return jsonify({"error": "Sync already running"}), 409
    threading.Thread(target=_run_sync, kwargs={"days_back": 365}, daemon=True).start()
    return jsonify({"status": "started"})


def _daily_sync_scheduler() -> None:
    """Sleep until midnight UTC, then run a 2-day refresh. Repeats forever."""
    while True:
        now = datetime.now(timezone.utc)
        next_midnight = (now + timedelta(days=1)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        time.sleep((next_midnight - now).total_seconds())
        if not _sync_status["running"]:
            _run_sync(days_back=2)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("\nLinkedIn Ads AI — Web UI")
    print("Opening http://127.0.0.1:5000 in your browser...\n")
    threading.Thread(target=_daily_sync_scheduler, daemon=True).start()
    threading.Timer(1.0, lambda: webbrowser.open("http://127.0.0.1:5000")).start()
    app.run(host="127.0.0.1", port=5000, debug=False, threaded=True)
