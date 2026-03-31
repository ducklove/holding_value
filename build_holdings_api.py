#!/usr/bin/env python3
"""
Build a static API payload that lists the holding companies from config.json.
"""

import json
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

BASE_DIR = Path(__file__).parent
CONFIG_PATH = BASE_DIR / "config.json"
OUTPUT_PATH = BASE_DIR / "api" / "holdings.json"
SEOUL_TZ = ZoneInfo("Asia/Seoul")


def ticker_code(ticker):
    return ticker.split(".")[0]


def build_subsidiary_item(entry):
    ticker = entry["ticker"]
    return {
        "name": entry["name"],
        "ticker": ticker,
        "code": ticker_code(ticker),
        "sharesHeld": entry["sharesHeld"],
    }


def build_holding_item(entry):
    holding_ticker = entry["holdingTicker"]
    subsidiaries = [build_subsidiary_item(sub) for sub in entry["subsidiaries"]]
    return {
        "id": entry["id"],
        "name": entry["name"],
        "holdingName": entry["holdingName"],
        "holdingTicker": holding_ticker,
        "holdingCode": ticker_code(holding_ticker),
        "holdingTotalShares": entry["holdingTotalShares"],
        "holdingTreasuryShares": entry["holdingTreasuryShares"],
        "subsidiaryCount": len(subsidiaries),
        "subsidiaries": subsidiaries,
    }


def main():
    config = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    now_local = datetime.now(SEOUL_TZ)
    items = [build_holding_item(entry) for entry in config]

    payload = {
        "generatedAt": now_local.isoformat(timespec="seconds"),
        "lastUpdated": now_local.strftime("%Y-%m-%d %H:%M:%S"),
        "count": len(items),
        "items": items,
    }

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )

    print(f"Generated {OUTPUT_PATH} ({len(items)} holdings)")


if __name__ == "__main__":
    main()
