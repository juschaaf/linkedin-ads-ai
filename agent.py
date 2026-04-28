from __future__ import annotations

"""
Claude agent loop.

Defines tools Claude can call, dispatches them, and runs the multi-turn REPL.
"""

import json
from datetime import datetime

import anthropic

import config
from tools import cache, sync, visualize, write_ops, linkedin_client

client = anthropic.Anthropic(api_key=config.ANTHROPIC_API_KEY)
MODEL = "claude-opus-4-6"

# ---------------------------------------------------------------------------
# Pending write operation state (for confirmation gate)
# ---------------------------------------------------------------------------

_pending_write: dict | None = None


# ---------------------------------------------------------------------------
# Tool definitions
# ---------------------------------------------------------------------------

TOOLS = [
    {
        "name": "sync_data",
        "description": (
            "Pull fresh data from LinkedIn Ads API into the local cache. "
            "Call this when the user asks about data that might be stale, "
            "or when cache is empty. Syncs campaign groups, campaigns, and "
            "weekly/daily analytics."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "account_ids": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": (
                        "Account IDs to sync. Omit or pass empty list to sync all configured accounts."
                    ),
                },
                "weeks_back": {
                    "type": "integer",
                    "description": "How many weeks of history to pull. Default: 8.",
                    "default": 8,
                },
            },
            "required": [],
        },
    },
    {
        "name": "sync_demographics",
        "description": (
            "Pull demographic breakdown data for a specific campaign into the local cache. "
            "Use when the user asks about job title, seniority, industry, company size, or "
            "other demographic distribution for a campaign."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "account_id": {
                    "type": "string",
                    "description": "The ad account ID that owns the campaign.",
                },
                "campaign_id": {
                    "type": "string",
                    "description": "The campaign ID to get demographics for.",
                },
                "pivot_type": {
                    "type": "string",
                    "enum": [
                        "MEMBER_JOB_TITLE",
                        "MEMBER_SENIORITY",
                        "MEMBER_INDUSTRY",
                        "MEMBER_COMPANY",
                        "MEMBER_COMPANY_SIZE",
                        "MEMBER_GEOGRAPHY",
                        "MEMBER_FUNCTION",
                    ],
                    "description": "The demographic dimension to fetch. Default: MEMBER_JOB_TITLE.",
                    "default": "MEMBER_JOB_TITLE",
                },
                "weeks_back": {
                    "type": "integer",
                    "description": "Weeks of history. Default: 4.",
                    "default": 4,
                },
            },
            "required": ["account_id", "campaign_id"],
        },
    },
    {
        "name": "query_cache",
        "description": (
            "Run a SQL query against the local DuckDB cache and return results as JSON. "
            "Use this for all analytics questions. "
            "Available tables: accounts, campaign_groups, campaigns, ad_analytics, demographics. "
            "\n\nKey columns:\n"
            "  ad_analytics: account_id, pivot_by, pivot_value (URN), date_start, date_end, "
            "granularity (DAILY/MONTHLY/ALL), spend_usd, impressions, clicks, conversions\n"
            "  NOTE: data is stored at DAILY granularity. To get weekly totals use:\n"
            "    DATE_TRUNC('week', date_start) AS week_start, SUM(spend_usd)\n"
            "  campaigns: campaign_id, account_id, group_id, name, status, bid_usd, "
            "daily_budget_usd, total_budget_usd\n"
            "  campaign_groups: group_id, account_id, name, status, total_budget_usd\n"
            "  demographics: account_id, campaign_id, pivot_type, pivot_value, pivot_label, "
            "date_start, date_end, spend_usd, impressions, clicks\n"
            "\nTo join analytics to campaign names: "
            "WHERE pivot_by='CAMPAIGN' AND pivot_value = 'urn:li:sponsoredCampaign:' || campaigns.campaign_id\n"
            "For campaign groups: "
            "WHERE pivot_by='CAMPAIGN_GROUP' AND pivot_value = 'urn:li:sponsoredCampaignGroup:' || campaign_groups.group_id"
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "sql": {
                    "type": "string",
                    "description": "The SQL query to run. DuckDB dialect.",
                },
            },
            "required": ["sql"],
        },
    },
    {
        "name": "plot_chart",
        "description": (
            "Generate and open an interactive Plotly chart in the browser. "
            "Returns the path to the saved HTML file. "
            "chart_type options: bar, stacked_bar, grouped_bar, line, pie, table. "
            "Pass the data as a list of row dicts from query_cache results."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "chart_type": {
                    "type": "string",
                    "enum": ["bar", "stacked_bar", "grouped_bar", "line", "pie", "table", "horizontal_bar"],
                    "description": "Type of chart to render.",
                },
                "title": {
                    "type": "string",
                    "description": "Chart title shown to the user.",
                },
                "data": {
                    "type": "array",
                    "items": {"type": "object"},
                    "description": "Array of row objects (from query_cache results).",
                },
                "x_col": {
                    "type": "string",
                    "description": "Column name for X axis (or row labels for pie/table).",
                },
                "y_col": {
                    "type": "string",
                    "description": "Column name for Y axis (or values for pie).",
                },
                "color_col": {
                    "type": "string",
                    "description": "Optional: column to use for color grouping (for stacked/grouped bars, lines).",
                },
            },
            "required": ["chart_type", "title", "data", "x_col", "y_col"],
        },
    },
    {
        "name": "propose_bid_update",
        "description": (
            "Propose a bid change for a campaign. This does NOT execute immediately — "
            "it presents the change to the user for confirmation. "
            "After calling this, tell the user what will happen and ask them to type 'confirm' to proceed."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "account_id": {"type": "string", "description": "Ad account ID."},
                "campaign_id": {"type": "string", "description": "Campaign ID to update."},
                "campaign_name": {"type": "string", "description": "Human-readable name for display."},
                "current_bid_usd": {"type": "number", "description": "Current bid in USD."},
                "new_bid_usd": {"type": "number", "description": "Proposed new bid in USD."},
                "reason": {"type": "string", "description": "Explanation of why this change is proposed."},
            },
            "required": ["account_id", "campaign_id", "new_bid_usd", "reason"],
        },
    },
    {
        "name": "propose_targeting_exclusion",
        "description": (
            "Propose adding targeting exclusions to a campaign. Does NOT execute immediately — "
            "presents the change for user confirmation. After calling this, ask the user to type 'confirm'."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "account_id": {"type": "string", "description": "Ad account ID."},
                "campaign_id": {"type": "string", "description": "Campaign ID to update."},
                "campaign_name": {"type": "string", "description": "Human-readable name for display."},
                "exclusion_type": {
                    "type": "string",
                    "description": "What type of targeting is being excluded (e.g. 'job titles').",
                },
                "exclusion_labels": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Human-readable list of what will be excluded.",
                },
                "facet_urn": {
                    "type": "string",
                    "description": "LinkedIn facet URN, e.g. 'urn:li:adTargetingFacet:titles'.",
                },
                "exclusion_urns": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "LinkedIn entity URNs to exclude.",
                },
            },
            "required": [
                "account_id", "campaign_id", "exclusion_type",
                "exclusion_labels", "facet_urn", "exclusion_urns"
            ],
        },
    },
    {
        "name": "search_job_titles",
        "description": (
            "Search for LinkedIn job title URNs by name. Use this to find the correct "
            "URNs before calling propose_targeting_exclusion."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "account_id": {"type": "string", "description": "Any valid ad account ID."},
                "query": {"type": "string", "description": "Job title search term."},
            },
            "required": ["account_id", "query"],
        },
    },
    {
        "name": "list_accounts",
        "description": "List all configured ad accounts with their IDs and names.",
        "input_schema": {
            "type": "object",
            "properties": {},
            "required": [],
        },
    },
]


# ---------------------------------------------------------------------------
# Tool dispatch
# ---------------------------------------------------------------------------

def dispatch_tool(tool_name: str, tool_input: dict) -> str:
    """Execute a tool call and return result as JSON string."""
    global _pending_write

    try:
        if tool_name == "list_accounts":
            accounts = [
                {"account_id": aid, "client_name": config.ACCOUNT_NAMES.get(aid, "Unknown")}
                for aid in config.ACCOUNT_IDS
            ]
            return json.dumps({"accounts": accounts})

        elif tool_name == "sync_data":
            account_ids = tool_input.get("account_ids") or config.ACCOUNT_IDS
            weeks_back = tool_input.get("weeks_back", 8)
            results = []
            for aid in account_ids:
                print(f"\n  Syncing account {aid}...")
                r = sync.sync_account(aid, weeks_back=weeks_back, verbose=True)
                results.append(r)
            return json.dumps(results)

        elif tool_name == "sync_demographics":
            r = sync.sync_demographics(
                account_id=tool_input["account_id"],
                campaign_id=tool_input["campaign_id"],
                pivot_type=tool_input.get("pivot_type", "MEMBER_JOB_TITLE"),
                weeks_back=tool_input.get("weeks_back", 4),
                verbose=True,
            )
            return json.dumps(r)

        elif tool_name == "query_cache":
            cache.init_db()
            rows = cache.query(tool_input["sql"])
            return json.dumps(rows, default=str)

        elif tool_name == "plot_chart":
            chart_type = tool_input["chart_type"]
            title = tool_input["title"]
            data = tool_input["data"]
            x_col = tool_input["x_col"]
            y_col = tool_input["y_col"]
            color_col = tool_input.get("color_col")

            if chart_type in ("stacked_bar", "grouped_bar"):
                barmode = "stack" if chart_type == "stacked_bar" else "group"
                path = visualize.stacked_bar_chart(data, x_col, y_col, color_col or x_col, title, barmode)
            elif chart_type == "horizontal_bar":
                path = visualize.bar_chart(data, x_col, y_col, title, color_col, horizontal=True)
            elif chart_type == "bar":
                path = visualize.bar_chart(data, x_col, y_col, title, color_col)
            elif chart_type == "line":
                path = visualize.line_chart(data, x_col, y_col, title, color_col)
            elif chart_type == "pie":
                path = visualize.pie_chart(data, x_col, y_col, title)
            elif chart_type == "table":
                path = visualize.table_chart(data, title)
            else:
                return json.dumps({"error": f"Unknown chart type: {chart_type}"})

            return json.dumps({"status": "opened", "path": path})

        elif tool_name == "propose_bid_update":
            _pending_write = {
                "type": "bid_update",
                "account_id": tool_input["account_id"],
                "campaign_id": tool_input["campaign_id"],
                "new_bid_usd": tool_input["new_bid_usd"],
            }
            return json.dumps({
                "status": "pending_confirmation",
                "proposed": {
                    "campaign": tool_input.get("campaign_name", tool_input["campaign_id"]),
                    "current_bid": tool_input.get("current_bid_usd"),
                    "new_bid": tool_input["new_bid_usd"],
                    "reason": tool_input["reason"],
                },
                "instruction": "Awaiting user confirmation. Tell the user to type 'confirm' to execute or 'cancel' to abort.",
            })

        elif tool_name == "propose_targeting_exclusion":
            _pending_write = {
                "type": "targeting_exclusion",
                "account_id": tool_input["account_id"],
                "campaign_id": tool_input["campaign_id"],
                "facet_urn": tool_input["facet_urn"],
                "exclusion_urns": tool_input["exclusion_urns"],
            }
            return json.dumps({
                "status": "pending_confirmation",
                "proposed": {
                    "campaign": tool_input.get("campaign_name", tool_input["campaign_id"]),
                    "exclusion_type": tool_input["exclusion_type"],
                    "will_exclude": tool_input["exclusion_labels"],
                },
                "instruction": "Awaiting user confirmation. Tell the user to type 'confirm' to execute or 'cancel' to abort.",
            })

        elif tool_name == "search_job_titles":
            results = write_ops.search_job_titles(
                account_id=tool_input["account_id"],
                query=tool_input["query"],
            )
            return json.dumps(results)

        else:
            return json.dumps({"error": f"Unknown tool: {tool_name}"})

    except Exception as e:
        return json.dumps({"error": str(e), "tool": tool_name})


# ---------------------------------------------------------------------------
# Confirmation execution
# ---------------------------------------------------------------------------

def execute_pending_write() -> str:
    global _pending_write
    if not _pending_write:
        return "No pending write operation."

    op = _pending_write
    _pending_write = None

    try:
        if op["type"] == "bid_update":
            result = write_ops.execute_bid_update(
                op["account_id"], op["campaign_id"], op["new_bid_usd"]
            )
            return f"Done. Bid updated to ${op['new_bid_usd']:.2f} for campaign {op['campaign_id']}."

        elif op["type"] == "targeting_exclusion":
            result = write_ops.execute_targeting_exclusion(
                op["account_id"], op["campaign_id"], op["facet_urn"], op["exclusion_urns"]
            )
            return f"Done. {len(op['exclusion_urns'])} exclusion(s) added to campaign {op['campaign_id']}."

        else:
            return f"Unknown pending write type: {op['type']}"

    except Exception as e:
        return f"Error executing write: {e}"


# ---------------------------------------------------------------------------
# Agent REPL
# ---------------------------------------------------------------------------

_account_list = ", ".join(
    f"{name} ({aid})"
    for aid, name in config.ACCOUNT_NAMES.items()
) or ", ".join(config.ACCOUNT_IDS)

SYSTEM_PROMPT = f"""You are a LinkedIn Ads analytics and management assistant.

You have access to data for these ad accounts:
{_account_list}

When the user mentions a client by name (e.g. "Guardian", "Eureka", "PTL"), use the corresponding account ID automatically — no need to ask.

Your capabilities:
- Answer questions about ad performance: spend, impressions, clicks, conversions
- Visualize data: weekly spend trends, campaign group breakdowns, demographic reports
- Propose and execute changes: bid updates, targeting exclusions (with user confirmation)
- Sync fresh data from the LinkedIn Ads API on demand

Guidelines:
- Always query the local cache first (query_cache). Only call sync_data if the user says data is stale or the cache is empty.
- When building SQL, join ad_analytics to campaigns/campaign_groups for readable names.
- For demographic questions, first sync_demographics if the data isn't in cache.
- For write operations (bids, exclusions), always use the propose_ tools first — never execute directly.
  After proposing, clearly tell the user what will change and ask them to type 'confirm'.
- Format currency as USD with 2 decimal places. Format dates as readable strings.
- If unsure which account a campaign belongs to, query the campaigns table to find it.

Today's date: {datetime.now().strftime('%B %d, %Y')}.
"""


def run_repl():
    """Interactive REPL loop."""
    global _pending_write

    print("\nLinkedIn Ads AI — type your question, or 'quit' to exit.")
    print("Tip: start with 'sync my data' if this is your first run.\n")

    messages: list[dict] = []

    while True:
        try:
            user_input = input("You: ").strip()
        except (KeyboardInterrupt, EOFError):
            print("\nBye.")
            break

        if not user_input:
            continue
        if user_input.lower() in ("quit", "exit", "q"):
            print("Bye.")
            break

        # Handle confirmation/cancellation of pending writes
        if _pending_write:
            if user_input.lower() in ("confirm", "yes", "y"):
                result = execute_pending_write()
                print(f"\nAssistant: {result}\n")
                messages.append({"role": "user", "content": user_input})
                messages.append({"role": "assistant", "content": result})
                continue
            elif user_input.lower() in ("cancel", "no", "n", "abort"):
                _pending_write = None
                msg = "Cancelled. No changes were made."
                print(f"\nAssistant: {msg}\n")
                messages.append({"role": "user", "content": user_input})
                messages.append({"role": "assistant", "content": msg})
                continue

        messages.append({"role": "user", "content": user_input})

        # Agentic loop — Claude may call multiple tools
        while True:
            response = client.messages.create(
                model=MODEL,
                max_tokens=4096,
                system=SYSTEM_PROMPT,
                tools=TOOLS,
                messages=messages,
            )

            # Collect any text content to print
            text_parts = []
            tool_calls = []
            for block in response.content:
                if block.type == "text":
                    text_parts.append(block.text)
                elif block.type == "tool_use":
                    tool_calls.append(block)

            if text_parts:
                print(f"\nAssistant: {''.join(text_parts)}")

            if response.stop_reason == "end_turn" or not tool_calls:
                # Final response — append to messages and break inner loop
                messages.append({"role": "assistant", "content": response.content})
                print()
                break

            # Execute tools
            messages.append({"role": "assistant", "content": response.content})
            tool_results = []
            for tc in tool_calls:
                print(f"  [tool: {tc.name}]")
                result = dispatch_tool(tc.name, tc.input)
                tool_results.append({
                    "type": "tool_result",
                    "tool_use_id": tc.id,
                    "content": result,
                })
            messages.append({"role": "user", "content": tool_results})
