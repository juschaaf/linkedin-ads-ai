#!/usr/bin/env python3
"""
LinkedIn Ads — entry point.

Usage:
  python main.py              # Start web server (default)
  python main.py --auth       # Run OAuth flow for all configured accounts
  python main.py --auth 123   # Run OAuth flow for a specific account ID
  python main.py --sync       # Sync latest data and exit
  python main.py --sync 8     # Sync last 8 weeks and exit
"""

import sys

import config
from tools import cache


def cmd_auth(account_ids: list[str]):
    from tools.linkedin_client import run_oauth_flow
    if not config.LINKEDIN_CLIENT_ID or not config.LINKEDIN_CLIENT_SECRET:
        print("ERROR: LINKEDIN_CLIENT_ID and LINKEDIN_CLIENT_SECRET must be set in .env")
        sys.exit(1)
    for aid in account_ids:
        print(f"\nStarting OAuth for account {aid}...")
        run_oauth_flow(aid)
    print("\nAll accounts authenticated.")


def cmd_sync(weeks_back: int):
    from tools.sync import sync_all_accounts
    if not config.ACCOUNT_IDS:
        print("ERROR: LINKEDIN_ACCOUNT_IDS not set in .env")
        sys.exit(1)
    print(f"Syncing {len(config.ACCOUNT_IDS)} account(s), last {weeks_back} weeks...")
    results = sync_all_accounts(weeks_back=weeks_back, verbose=True)
    for r in results:
        status = r.get("status")
        if status == "ok":
            print(f"  ✓ {r['account_id']}: {r['rows_written']} rows in {r['duration_seconds']:.1f}s")
        else:
            print(f"  ✗ {r['account_id']}: {r.get('error')}")


def cmd_server():
    import server
    cache.init_db()
    server.app.run(host="0.0.0.0", port=5000, debug=False, threaded=True)


def main():
    args = sys.argv[1:]

    if not args:
        cmd_server()
        return

    if args[0] == "--auth":
        account_ids = args[1:] if len(args) > 1 else config.ACCOUNT_IDS
        if not account_ids:
            print("ERROR: Provide account IDs as arguments or set LINKEDIN_ACCOUNT_IDS in .env")
            sys.exit(1)
        cmd_auth(account_ids)

    elif args[0] == "--sync":
        weeks_back = int(args[1]) if len(args) > 1 else 8
        cmd_sync(weeks_back)

    else:
        print(__doc__)
        sys.exit(1)


if __name__ == "__main__":
    main()
