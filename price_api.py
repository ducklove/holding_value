#!/usr/bin/env python3
"""
Internal close-price API helpers.
"""

import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlencode
from urllib.request import urlopen

import pandas as pd

PRICE_API_URL = os.environ.get(
    "HOLDING_VALUE_PRICE_API_URL",
    "http://192.168.68.84/api/prices/close",
)
PRICE_API_TIMEOUT = float(os.environ.get("HOLDING_VALUE_PRICE_API_TIMEOUT", "30"))
PRICE_API_HEALTH_TIMEOUT = float(os.environ.get("HOLDING_VALUE_PRICE_API_HEALTH_TIMEOUT", "8"))
PRICE_API_WORKERS = int(os.environ.get("HOLDING_VALUE_PRICE_API_WORKERS", "1"))
_PRICE_API_AVAILABLE = None


def is_korean_ticker(ticker):
    return ticker.endswith(".KS") or ticker.endswith(".KQ")


def ticker_code(ticker):
    return ticker.split(".", 1)[0]


def is_price_api_enabled():
    return os.environ.get("HOLDING_VALUE_PRICE_API", "1").lower() not in {
        "0",
        "false",
        "no",
    }


def is_price_api_available():
    global _PRICE_API_AVAILABLE

    if _PRICE_API_AVAILABLE is not None:
        return _PRICE_API_AVAILABLE
    if not is_price_api_enabled():
        _PRICE_API_AVAILABLE = False
        return _PRICE_API_AVAILABLE

    query = urlencode({
        "ticker": "005930",
        "since": "2026-04-28",
        "until": "2026-04-30",
    })
    try:
        with urlopen(f"{PRICE_API_URL}?{query}", timeout=PRICE_API_HEALTH_TIMEOUT) as response:
            payload = json.loads(response.read().decode("utf-8"))
        _PRICE_API_AVAILABLE = bool(payload.get("prices"))
    except Exception as exc:
        print(f"Internal price API unavailable; falling back to yfinance ({exc})")
        _PRICE_API_AVAILABLE = False

    return _PRICE_API_AVAILABLE


def fetch_close_series(ticker, since, until):
    if not is_price_api_available() or not is_korean_ticker(ticker):
        return None

    code = ticker_code(ticker)
    if not code.isalnum():
        return None

    query = urlencode({"ticker": code, "since": since, "until": until})
    with urlopen(f"{PRICE_API_URL}?{query}", timeout=PRICE_API_TIMEOUT) as response:
        payload = json.loads(response.read().decode("utf-8"))

    rows = payload.get("prices") or []
    if not rows:
        return None

    series = pd.Series(
        (float(row["close"]) for row in rows),
        index=pd.to_datetime([row["date"] for row in rows]),
        name=ticker,
        dtype="float64",
    )
    return series[~series.index.duplicated(keep="last")].sort_index()


def download_close_frame(tickers, since, until):
    targets = [ticker for ticker in tickers if is_korean_ticker(ticker)]
    if not targets or not since or not until or not is_price_api_available():
        return pd.DataFrame(), []

    frames = []
    loaded = []
    workers = min(PRICE_API_WORKERS, len(targets))
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(fetch_close_series, ticker, since, until): ticker
            for ticker in targets
        }
        for future in as_completed(futures):
            ticker = futures[future]
            try:
                series = future.result()
            except Exception as exc:
                print(f"  {ticker}: internal price API failed ({exc})")
                continue
            if series is None or series.dropna().empty:
                continue
            frames.append(series.to_frame())
            loaded.append(ticker)

    if not frames:
        return pd.DataFrame(), loaded

    close = pd.concat(frames, axis=1)
    return close.loc[:, ~close.columns.duplicated()], loaded
