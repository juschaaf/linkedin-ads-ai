from __future__ import annotations

"""
LinkedIn Ads API client (versioned REST API, v202601+).

All endpoints use account-scoped URLs:
  /rest/adAccounts/{accountId}/adCampaignGroups
  /rest/adAccounts/{accountId}/adCampaigns
  /rest/adAnalytics

Partial updates use POST with {"patch": {"$set": {...}}} body.
Pagination uses cursor-based pageToken / pageSize.
"""

import time
import urllib.parse
import http.server
import threading
import webbrowser
from datetime import datetime, timedelta, timezone

import requests

import config


# ---------------------------------------------------------------------------
# Token management
# ---------------------------------------------------------------------------

def _token_is_expired(token_data: dict) -> bool:
    expires_at = token_data.get("expires_at", 0)
    return time.time() >= expires_at - 60  # 60s buffer


def _refresh_access_token(account_id: str, token_data: dict) -> dict:
    refresh_token = token_data.get("refresh_token")
    if not refresh_token:
        raise ValueError(f"No refresh token for account {account_id}. Re-run --auth.")

    resp = requests.post(
        "https://www.linkedin.com/oauth/v2/accessToken",
        data={
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "client_id": config.LINKEDIN_CLIENT_ID,
            "client_secret": config.LINKEDIN_CLIENT_SECRET,
        },
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        timeout=30,
    )
    resp.raise_for_status()
    new_token = resp.json()
    new_token["expires_at"] = time.time() + new_token.get("expires_in", 3600)
    if "refresh_token" not in new_token:
        new_token["refresh_token"] = refresh_token
    config.save_token(account_id, new_token)
    return new_token


def get_access_token(account_id: str) -> str:
    token_data = config.get_token(account_id)
    if not token_data:
        raise ValueError(
            f"No token for account {account_id}. Run: python3 main.py --auth"
        )
    if _token_is_expired(token_data):
        token_data = _refresh_access_token(account_id, token_data)
    return token_data["access_token"]


# ---------------------------------------------------------------------------
# OAuth flow (3-legged)
# ---------------------------------------------------------------------------

def run_oauth_flow(account_id: str) -> None:
    import secrets

    state = secrets.token_urlsafe(16)
    auth_url = (
        "https://www.linkedin.com/oauth/v2/authorization?"
        + urllib.parse.urlencode({
            "response_type": "code",
            "client_id": config.LINKEDIN_CLIENT_ID,
            "redirect_uri": config.OAUTH_REDIRECT_URI,
            "scope": " ".join(config.OAUTH_SCOPES),
            "state": state,
        })
    )

    auth_code: list[str] = []
    received_state: list[str] = []

    class _CallbackHandler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            parsed = urllib.parse.urlparse(self.path)
            params = urllib.parse.parse_qs(parsed.query)
            auth_code.append(params.get("code", [""])[0])
            received_state.append(params.get("state", [""])[0])
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"<h2>Auth complete. You can close this tab.</h2>")

        def log_message(self, *args):
            pass

    server = http.server.HTTPServer(("localhost", 8765), _CallbackHandler)
    thread = threading.Thread(target=server.handle_request)
    thread.start()

    print(f"\nOpening browser for account {account_id}...")
    print(f"If it doesn't open, visit:\n{auth_url}\n")
    webbrowser.open(auth_url)
    thread.join(timeout=120)
    server.server_close()

    if not auth_code[0]:
        raise RuntimeError("No auth code received.")
    if received_state[0] != state:
        raise RuntimeError("State mismatch — retry auth.")

    resp = requests.post(
        "https://www.linkedin.com/oauth/v2/accessToken",
        data={
            "grant_type": "authorization_code",
            "code": auth_code[0],
            "redirect_uri": config.OAUTH_REDIRECT_URI,
            "client_id": config.LINKEDIN_CLIENT_ID,
            "client_secret": config.LINKEDIN_CLIENT_SECRET,
        },
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        timeout=30,
    )
    resp.raise_for_status()
    token_data = resp.json()
    token_data["expires_at"] = time.time() + token_data.get("expires_in", 3600)
    config.save_token(account_id, token_data)
    print(f"Token saved for account {account_id}.")


# ---------------------------------------------------------------------------
# Base request helpers
# ---------------------------------------------------------------------------

_BASE = "https://api.linkedin.com/rest"


def _headers(account_id: str) -> dict:
    return {
        "Authorization": f"Bearer {get_access_token(account_id)}",
        "Linkedin-Version": config.API_VERSION,
        "X-Restli-Protocol-Version": "2.0.0",
        "Content-Type": "application/json",
    }


def _get(account_id: str, path: str, params: dict | None = None) -> dict:
    # Build URL manually to avoid requests double-encoding commas in `fields` and brackets in `accounts[0]`.
    # LinkedIn requires these characters unencoded.
    if params:
        qs = urllib.parse.urlencode(params, safe=":,[]@")
        url = f"{_BASE}{path}?{qs}"
    else:
        url = f"{_BASE}{path}"
    resp = requests.get(url, headers=_headers(account_id), timeout=30)
    resp.raise_for_status()
    return resp.json()


def _post(account_id: str, path: str, body: dict, extra_headers: dict | None = None) -> dict:
    h = _headers(account_id)
    if extra_headers:
        h.update(extra_headers)
    resp = requests.post(
        f"{_BASE}{path}",
        headers=h,
        json=body,
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json() if resp.content else {}


# ---------------------------------------------------------------------------
# Pagination helper
# ---------------------------------------------------------------------------

def _get_all_pages(account_id: str, path: str, params: dict, page_size: int = 500) -> list[dict]:
    """Fetch all pages of a cursor-paginated endpoint."""
    all_elements = []
    params = {**params, "pageSize": page_size}
    page_token = None

    while True:
        if page_token:
            params["pageToken"] = page_token
        data = _get(account_id, path, params)
        elements = data.get("elements", [])
        all_elements.extend(elements)

        next_token = data.get("metadata", {}).get("nextPageToken")
        if not next_token or not elements:
            break
        page_token = next_token

    return all_elements


# ---------------------------------------------------------------------------
# Campaign structure
# ---------------------------------------------------------------------------

def get_campaign_groups(account_id: str) -> list[dict]:
    """Return all campaign groups for an account."""
    return _get_all_pages(
        account_id,
        f"/adAccounts/{account_id}/adCampaignGroups",
        {"q": "search"},
    )


def get_campaigns(account_id: str, campaign_group_id: str | None = None) -> list[dict]:
    """Return all campaigns for an account."""
    return _get_all_pages(
        account_id,
        f"/adAccounts/{account_id}/adCampaigns",
        {"q": "search"},
    )


def get_campaign(account_id: str, campaign_id: str) -> dict:
    """Return a single campaign by ID."""
    return _get(account_id, f"/adAccounts/{account_id}/adCampaigns/{campaign_id}")


# ---------------------------------------------------------------------------
# Analytics
# ---------------------------------------------------------------------------

def get_analytics(
    account_id: str,
    pivot: str,
    granularity: str,
    weeks_back: int | None = None,
    days_back: int | None = None,
    campaign_ids: list[str] | None = None,
    campaign_group_ids: list[str] | None = None,
) -> list[dict]:
    """
    Fetch ad analytics from /adAnalytics.

    pivot: CAMPAIGN, CAMPAIGN_GROUP, ACCOUNT, CREATIVE
    granularity: DAILY, WEEKLY, MONTHLY, ALL

    The versioned API uses Rest.li object syntax for dateRange and List() for accounts.
    Parameters are passed as a pre-built query string to avoid requests double-encoding.
    """
    end = datetime.now(timezone.utc)
    if days_back is not None:
        start = end - timedelta(days=days_back)
    else:
        start = end - timedelta(weeks=weeks_back if weeks_back is not None else 8)

    # Rest.li object format for dateRange
    date_range = (
        f"(start:(year:{start.year},month:{start.month},day:{start.day}),"
        f"end:(year:{end.year},month:{end.month},day:{end.day}))"
    )

    # Build facet filter. Colons in URNs must be percent-encoded (%3A) but
    # List() parentheses and commas must remain literal.
    def _urn(prefix: str, entity_id: str) -> str:
        return f"urn%3Ali%3A{prefix}%3A{entity_id}"

    if campaign_ids:
        urns = ",".join(_urn("sponsoredCampaign", cid) for cid in campaign_ids)
        facet = f"campaigns=List({urns})"
    elif campaign_group_ids:
        urns = ",".join(_urn("sponsoredCampaignGroup", gid) for gid in campaign_group_ids)
        facet = f"campaignGroups=List({urns})"
    else:
        facet = f"accounts=List({_urn('sponsoredAccount', account_id)})"

    fields = "dateRange,pivotValues,costInLocalCurrency,impressions,clicks,externalWebsiteConversions"

    base_qs = (
        f"q=analytics"
        f"&pivot={pivot}"
        f"&timeGranularity={granularity}"
        f"&dateRange={date_range}"
        f"&{facet}"
        f"&fields={fields}"
    )

    # adAnalytics uses start/count pagination (not pageToken/pageSize)
    all_rows: list[dict] = []
    start_idx = 0
    page_size = 100

    while True:
        qs = base_qs + f"&start={start_idx}&count={page_size}"
        resp = requests.get(
            f"{_BASE}/adAnalytics?{qs}",
            headers=_headers(account_id),
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        elements = data.get("elements", [])

        # Normalize: flatten pivotValues list into a single pivotValue string
        for el in elements:
            pv = el.get("pivotValues", [])
            el["pivotValue"] = pv[0] if pv else ""
            el["pivot"] = pivot

        all_rows.extend(elements)

        paging = data.get("paging", {})
        total = paging.get("total", 0)
        start_idx += len(elements)
        if start_idx >= total or not elements:
            break

    return all_rows


def get_demographics(
    account_id: str,
    pivot_type: str,
    weeks_back: int = 4,
    campaign_ids: list[str] | None = None,
) -> list[dict]:
    """
    Fetch demographic breakdown for campaigns.

    pivot_type: MEMBER_JOB_TITLE | MEMBER_SENIORITY | MEMBER_INDUSTRY |
                MEMBER_COMPANY | MEMBER_COMPANY_SIZE | MEMBER_GEOGRAPHY | MEMBER_FUNCTION
    """
    return get_analytics(
        account_id=account_id,
        pivot=pivot_type,
        granularity="ALL",
        weeks_back=weeks_back,
        campaign_ids=campaign_ids,
    )


# ---------------------------------------------------------------------------
# Write operations
# ---------------------------------------------------------------------------

def update_campaign_bid(account_id: str, campaign_id: str, bid_amount_usd: float) -> dict:
    """
    Update the bid on a campaign.
    LinkedIn stores amounts as strings in dollars (NOT micro-currency) in the versioned API.
    """
    _post(account_id, f"/adAccounts/{account_id}/adCampaigns/{campaign_id}", {
        "patch": {
            "$set": {
                "unitCost": {
                    "amount": f"{bid_amount_usd:.2f}",
                    "currencyCode": "USD",
                }
            }
        }
    })
    return {"campaign_id": campaign_id, "new_bid_usd": bid_amount_usd, "status": "updated"}


def add_targeting_exclusions(
    account_id: str,
    campaign_id: str,
    facet_urn: str,
    exclusion_urns: list[str],
) -> dict:
    """Add targeting exclusions to a campaign."""
    campaign = get_campaign(account_id, campaign_id)
    targeting = campaign.get("targetingCriteria", {})

    excluded = targeting.get("exclude", {}).get("or", [])
    for urn in exclusion_urns:
        entry = {"urn": facet_urn, "values": [urn]}
        if entry not in excluded:
            excluded.append(entry)

    _post(account_id, f"/adAccounts/{account_id}/adCampaigns/{campaign_id}", {
        "patch": {
            "$set": {
                "targetingCriteria": {
                    **targeting,
                    "exclude": {"or": excluded},
                }
            }
        }
    })
    return {
        "campaign_id": campaign_id,
        "added_exclusions": exclusion_urns,
        "status": "updated",
    }


def search_targeting_entities(account_id: str, facet_urn: str, query: str) -> list[dict]:
    """Search for targeting entity URNs (e.g. job titles) by name."""
    data = _get(account_id, "/adTargetingEntities", {
        "q": "typeahead",
        "facetUrn": facet_urn,
        "query": query,
        "count": 20,
    })
    return data.get("elements", [])
