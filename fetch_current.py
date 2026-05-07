#!/usr/bin/env python3
"""
Generate the current.js snapshot for the holding value dashboard.
"""

import json
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import yfinance as yf
import pandas as pd

from price_api import download_close_frame as download_internal_close_frame

BASE_DIR = Path(__file__).parent
CONFIG_PATH = BASE_DIR / "config.json"
OUTPUT_PATH = BASE_DIR / "current.js"
SEOUL_TZ = ZoneInfo("Asia/Seoul")

with open(CONFIG_PATH, encoding="utf-8") as f:
    PAIRS = json.load(f)


def is_korean(ticker):
    return ticker.endswith(".KS") or ticker.endswith(".KQ")


def parse_existing_current():
    if not OUTPUT_PATH.exists():
        return None

    text = OUTPUT_PATH.read_text(encoding="utf-8")
    json_str = re.sub(r"^const CURRENT_DATA\s*=\s*", "", text)
    json_str = re.sub(r";\s*$", "", json_str)
    try:
        return json.loads(json_str)
    except json.JSONDecodeError:
        return None


def build_session_info(now_local):
    weekday = now_local.weekday()
    minutes = now_local.hour * 60 + now_local.minute
    is_weekday = weekday < 5

    if is_weekday and 9 * 60 <= minutes <= 16 * 60:
        return {
            "name": "kr_day",
            "date": now_local.strftime("%Y-%m-%d"),
            "label": "KR day session",
        }

    if (is_weekday and minutes >= 21 * 60) or (minutes < 6 * 60 + 30 and (now_local - timedelta(days=1)).weekday() < 5):
        session_date = now_local if minutes >= 21 * 60 else now_local - timedelta(days=1)
        return {
            "name": "us_night",
            "date": session_date.strftime("%Y-%m-%d"),
            "label": "US night session",
        }

    return {
        "name": "offhours",
        "date": now_local.strftime("%Y-%m-%d"),
        "label": "Off hours",
    }


def same_session(previous_snapshot, current_session):
    if not previous_snapshot:
        return False

    previous_session = previous_snapshot.get("session") or {}
    return (
        previous_session.get("name") == current_session["name"]
        and previous_session.get("date") == current_session["date"]
    )


def deep_copy_json(value):
    return json.loads(json.dumps(value))


def build_previous_pair_map(previous_snapshot, current_session):
    if not same_session(previous_snapshot, current_session):
        return {}

    return {
        pair["id"]: deep_copy_json(pair)
        for pair in previous_snapshot.get("pairs", [])
        if pair.get("id") and pair.get("id") != "_average"
    }


def is_cached_entry_compatible(pair, previous_entry):
    if previous_entry is None:
        return False

    expected_names = [sub["name"] for sub in pair["subsidiaries"]]
    cached_subs = previous_entry.get("subsidiaries")

    if len(expected_names) == 1:
        return not cached_subs

    if not isinstance(cached_subs, list):
        return False

    cached_names = [sub.get("name") for sub in cached_subs]
    return cached_names == expected_names


def calculate_pct_change(current_price, previous_price):
    if current_price is None or previous_price in (None, 0):
        return None
    return round((current_price - previous_price) / previous_price * 100, 2)


def get_holding_adjusted_shares(pair):
    return pair.get(
        "holdingAdjustedShares",
        pair["holdingTotalShares"] - pair["holdingTreasuryShares"],
    )


def normalize_close_frame(data, tickers):
    if data.empty or "Close" not in data:
        return pd.DataFrame()

    close = data["Close"]
    if getattr(close, "ndim", 1) == 1:
        close = close.to_frame(name=tickers[0])
    return close


def merge_close_frames(base, frames):
    if not frames:
        return base

    extra = pd.concat(frames, axis=1)
    extra = extra.loc[:, ~extra.columns.duplicated()]
    if base.empty:
        return extra

    replace_columns = [
        column for column in extra.columns
        if column in base.columns
        and base[column].dropna().empty
        and not extra[column].dropna().empty
    ]
    if replace_columns:
        base = base.drop(columns=replace_columns)

    merged = pd.concat([base, extra], axis=1)
    return merged.loc[:, ~merged.columns.duplicated()]


def download_close_prices(tickers, now_local):
    since = (now_local - timedelta(days=14)).strftime("%Y-%m-%d")
    until = now_local.strftime("%Y-%m-%d")
    internal_close, internal_loaded = download_internal_close_frame(tickers, since, until)
    close = merge_close_frames(pd.DataFrame(), [internal_close] if not internal_close.empty else [])
    sources = {ticker: "internal_price_api" for ticker in internal_loaded}
    if internal_loaded:
        print(f"Loaded {len(internal_loaded)} tickers from internal price API.")

    yfinance_targets = [
        ticker for ticker in tickers
        if ticker not in close.columns or close[ticker].dropna().empty
    ]
    if yfinance_targets:
        data = yf.download(yfinance_targets, period="5d", auto_adjust=True, progress=False)
        yfinance_close = normalize_close_frame(data, yfinance_targets)
        close = merge_close_frames(close, [yfinance_close] if not yfinance_close.empty else [])
        for ticker in yfinance_targets:
            if ticker in close.columns and not close[ticker].dropna().empty:
                sources[ticker] = "yfinance"

    return close, sources


def build_price_maps(close, tickers):
    prices = {}
    previous_prices = {}

    for ticker in tickers:
        if ticker not in close.columns:
            continue
        series = close[ticker].dropna()
        if len(series) > 0:
            prices[ticker] = float(series.iloc[-1])
        if len(series) > 1:
            previous_prices[ticker] = float(series.iloc[-2])

    return prices, previous_prices


def build_pair_entry(pair, prices, previous_prices, fx_rate, previous_fx_rate, price_sources):
    holding_ticker = pair["holdingTicker"]
    if holding_ticker not in prices:
        return None

    holding_price = prices[holding_ticker]
    previous_holding_price = previous_prices.get(holding_ticker)
    adjusted_shares = get_holding_adjusted_shares(pair)

    holding_value = 0.0
    used_sources = {price_sources.get(holding_ticker, "unknown")}
    sub_details = []

    for sub in pair["subsidiaries"]:
        sub_ticker = sub["ticker"]
        if sub_ticker not in prices:
            return None

        used_sources.add(price_sources.get(sub_ticker, "unknown"))
        sub_price = prices[sub_ticker]
        previous_sub_price = previous_prices.get(sub_ticker)
        if not is_korean(sub_ticker):
            if fx_rate is None:
                return None
            sub_price *= fx_rate
            if previous_sub_price is None or previous_fx_rate is None:
                previous_sub_price = None
            else:
                previous_sub_price *= previous_fx_rate

        sub_value = sub["sharesHeld"] * sub_price
        holding_value += sub_value
        sub_details.append(
            {
                "name": sub["name"],
                "price": round(sub_price, 0),
                "change": calculate_pct_change(sub_price, previous_sub_price),
                "value": round(sub_value / 1e8, 1),
                "rawValue": sub_value,
            }
        )

    market_cap = adjusted_shares * holding_price
    ratio = holding_value / market_cap * 100

    for detail in sub_details:
        detail["ratio"] = round(detail["rawValue"] / market_cap * 100, 2)
        del detail["rawValue"]

    entry = {
        "id": pair["id"],
        "holdingPrice": round(holding_price, 0),
        "holdingChange": calculate_pct_change(holding_price, previous_holding_price),
        "holdingValue": round(holding_value / 1e8, 1),
        "marketCap": round(market_cap / 1e8, 1),
        "ratio": round(ratio, 2),
        "quoteSource": (
            "internal_price_api"
            if used_sources == {"internal_price_api"}
            else "yfinance" if used_sources == {"yfinance"} else "mixed"
        ),
    }

    if len(sub_details) == 1:
        entry["subsidiaryPrice"] = sub_details[0]["price"]
        entry["subsidiaryChange"] = sub_details[0]["change"]
    else:
        entry["subsidiaries"] = sub_details

    return entry


def build_average_entry(pairs_result):
    live_pairs = [pair for pair in pairs_result if pair.get("id") != "_average"]
    if not live_pairs:
        return None

    avg_ratio = sum(pair["ratio"] for pair in live_pairs) / len(live_pairs)
    return {
        "id": "_average",
        "ratio": round(avg_ratio, 2),
        "quoteSource": "derived",
    }


def main():
    previous_snapshot = parse_existing_current()
    now_local = datetime.now(SEOUL_TZ)
    session_info = build_session_info(now_local)
    previous_pairs = build_previous_pair_map(previous_snapshot, session_info)

    all_tickers = []
    needs_fx = False
    for pair in PAIRS:
        all_tickers.append(pair["holdingTicker"])
        for sub in pair["subsidiaries"]:
            all_tickers.append(sub["ticker"])
            if not is_korean(sub["ticker"]):
                needs_fx = True

    all_tickers = list(dict.fromkeys(all_tickers))
    if needs_fx:
        all_tickers.append("USDKRW=X")

    print(f"Fetching current prices for {len(all_tickers)} tickers...")

    close, price_sources = download_close_prices(all_tickers, now_local)

    if close.empty:
        print("ERROR: No data downloaded.")
        return

    prices, previous_prices = build_price_maps(close, all_tickers)
    fx_rate = prices.get("USDKRW=X")
    previous_fx_rate = previous_prices.get("USDKRW=X")
    pairs_result = []
    preserved_pair_ids = []
    missing_pair_ids = []

    for pair in PAIRS:
        entry = build_pair_entry(pair, prices, previous_prices, fx_rate, previous_fx_rate, price_sources)
        if entry is None:
            previous_entry = previous_pairs.get(pair["id"])
            if is_cached_entry_compatible(pair, previous_entry):
                previous_entry["quoteSource"] = "cached_same_session"
                pairs_result.append(previous_entry)
                preserved_pair_ids.append(pair["id"])
                print(f"  {pair['id']}: reused previous same-session snapshot")
            else:
                missing_pair_ids.append(pair["id"])
                print(f"  {pair['id']}: missing live quotes")
            continue

        pairs_result.append(entry)
        print(f"  {pair['id']}: ratio {entry['ratio']:.2f}%")

    average_entry = build_average_entry(pairs_result)
    if average_entry is not None:
        pairs_result.append(average_entry)

    current_data = {
        "lastUpdated": now_local.strftime("%Y-%m-%d %H:%M:%S"),
        "generatedAt": now_local.isoformat(timespec="seconds"),
        "snapshotTimestamp": int(now_local.astimezone(timezone.utc).timestamp() * 1000),
        "session": session_info,
        "isPartial": bool(preserved_pair_ids or missing_pair_ids),
        "preservedPairIds": preserved_pair_ids,
        "missingPairIds": missing_pair_ids,
        "summary": {
            "pairCount": len([pair for pair in pairs_result if pair.get("id") != "_average"]),
            "preservedCount": len(preserved_pair_ids),
            "missingCount": len(missing_pair_ids),
            "averageRatio": average_entry["ratio"] if average_entry else None,
        },
        "pairs": pairs_result,
    }

    js_content = "const CURRENT_DATA = " + json.dumps(current_data, ensure_ascii=False, indent=2) + ";\n"
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        f.write(js_content)

    print(f"\nGenerated {OUTPUT_PATH} ({len(pairs_result)} pairs)")


if __name__ == "__main__":
    main()
