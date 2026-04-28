from __future__ import annotations

"""
Write operations with confirmation gate.

All mutating actions follow this pattern:
  1. Claude proposes the change (via tool response)
  2. User confirms in the REPL
  3. This module executes it

The confirmation happens in agent.py — these functions assume confirmation
has already been granted and just execute.
"""

from tools import linkedin_client, cache


def execute_bid_update(account_id: str, campaign_id: str, new_bid_usd: float) -> dict:
    """
    Execute a bid update on a campaign.
    Refreshes the campaign in cache after update.
    """
    result = linkedin_client.update_campaign_bid(account_id, campaign_id, new_bid_usd)

    # Refresh this campaign in cache
    try:
        campaign = linkedin_client.get_campaign(account_id, campaign_id)
        cache.upsert_campaigns(account_id, [campaign])
    except Exception:
        pass  # cache refresh is best-effort

    return result


def execute_targeting_exclusion(
    account_id: str,
    campaign_id: str,
    facet_urn: str,
    exclusion_urns: list[str],
) -> dict:
    """
    Execute adding targeting exclusions to a campaign.
    Refreshes the campaign in cache after update.
    """
    result = linkedin_client.add_targeting_exclusions(
        account_id, campaign_id, facet_urn, exclusion_urns
    )

    # Refresh this campaign in cache
    try:
        campaign = linkedin_client.get_campaign(account_id, campaign_id)
        cache.upsert_campaigns(account_id, [campaign])
    except Exception:
        pass

    return result


def search_job_titles(account_id: str, query: str) -> list[dict]:
    """Search for job title URNs by name (for building exclusion lists)."""
    return linkedin_client.search_targeting_entities(
        account_id=account_id,
        facet_urn="urn:li:adTargetingFacet:titles",
        query=query,
    )
