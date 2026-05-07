#!/usr/bin/env python3
"""
Internal close-price API helpers used as the primary Korean price source.
"""

import json
import os
from datetime import datetime, timedelta
from urllib.parse import urlencode
from urllib.request import urlopen

import pandas as pd

PRICE_API_URL = os.environ.get(
    "HOLDING_VALUE_PRICE_API_URL",
    "http://192.168.68.84:8400/api/prices/close",
)
PRICE_API_BASE_URL = PRICE_API_URL.rsplit("/api/", 1)[0]
PRICE_API_TIMEOUT = float(os.environ.get("HOLDING_VALUE_PRICE_API_TIMEOUT", "30"))
PRICE_API_HEALTH_TIMEOUT = float(os.environ.get("HOLDING_VALUE_PRICE_API_HEALTH_TIMEOUT", "8"))
PRICE_API_MAX_DAYS = int(os.environ.get("HOLDING_VALUE_PRICE_API_MAX_DAYS", "3700"))
PRICE_API_MAX_TICKERS = int(os.environ.get("HOLDING_VALUE_PRICE_API_MAX_TICKERS", "500"))
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

    try:
        with urlopen(f"{PRICE_API_BASE_URL}/api/health", timeout=PRICE_API_HEALTH_TIMEOUT) as response:
            payload = json.loads(response.read().decode("utf-8"))
        _PRICE_API_AVAILABLE = payload.get("status") == "ok"
    except Exception as exc:
        print(f"Internal price API unavailable; falling back to yfinance ({exc})")
        _PRICE_API_AVAILABLE = False

    return _PRICE_API_AVAILABLE


def date_chunks(since, until):
    start = datetime.strptime(since, "%Y-%m-%d").date()
    end = datetime.strptime(until, "%Y-%m-%d").date()
    cursor = start
    while cursor <= end:
        chunk_end = min(cursor + timedelta(days=PRICE_API_MAX_DAYS - 1), end)
        yield cursor.strftime("%Y-%m-%d"), chunk_end.strftime("%Y-%m-%d")
        cursor = chunk_end + timedelta(days=1)


def batched(values, size):
    for start in range(0, len(values), size):
        yield values[start:start + size]


def series_from_rows(ticker, rows):
    if not rows:
        return None

    series = pd.Series(
        (float(row["close"]) for row in rows),
        index=pd.to_datetime([row["date"] for row in rows]),
        name=ticker,
        dtype="float64",
    )
    return series[~series.index.duplicated(keep="last")].sort_index()


def fetch_close_batch(code_to_tickers, since, until):
    query = urlencode({
        "tickers": ",".join(code_to_tickers.keys()),
        "since": since,
        "until": until,
    })
    with urlopen(f"{PRICE_API_URL}?{query}", timeout=PRICE_API_TIMEOUT) as response:
        payload = json.loads(response.read().decode("utf-8"))

    payload_prices = payload.get("prices") or {}
    if isinstance(payload_prices, list):
        payload_prices = {payload.get("ticker"): payload_prices}

    frames = []
    loaded = []
    for code, rows in payload_prices.items():
        for ticker in code_to_tickers.get(code, []):
            series = series_from_rows(ticker, rows)
            if series is None or series.dropna().empty:
                continue
            frames.append(series.to_frame())
            loaded.append(ticker)

    return frames, loaded


def fetch_fx_series(since, until):
    query = urlencode({
        "series_id": "USD_KRW",
        "since": since,
        "until": until,
    })
    with urlopen(f"{PRICE_API_BASE_URL}/api/macro/fx?{query}", timeout=PRICE_API_TIMEOUT) as response:
        payload = json.loads(response.read().decode("utf-8"))

    rows = payload.get("fx") or []
    if not rows:
        return None

    series = pd.Series(
        (float(row["value"]) for row in rows),
        index=pd.to_datetime([row["date"] for row in rows]),
        name="USDKRW=X",
        dtype="float64",
    )
    return series[~series.index.duplicated(keep="last")].sort_index()


def download_close_frame(tickers, since, until):
    if not since or not until or not is_price_api_available():
        return pd.DataFrame(), []

    frames = []
    loaded = []

    code_to_tickers = {}
    for ticker in tickers:
        if not is_korean_ticker(ticker):
            continue
        code = ticker_code(ticker)
        if code.isalnum():
            code_to_tickers.setdefault(code, []).append(ticker)

    for chunk_since, chunk_until in date_chunks(since, until):
        for code_batch in batched(list(code_to_tickers), PRICE_API_MAX_TICKERS):
            batch_map = {code: code_to_tickers[code] for code in code_batch}
            try:
                batch_frames, batch_loaded = fetch_close_batch(batch_map, chunk_since, chunk_until)
            except Exception as exc:
                print(f"  internal price API batch failed ({chunk_since}~{chunk_until}: {exc})")
                continue
            frames.extend(batch_frames)
            loaded.extend(batch_loaded)

        if "USDKRW=X" in tickers:
            try:
                fx_series = fetch_fx_series(chunk_since, chunk_until)
            except Exception as exc:
                print(f"  internal FX API failed ({chunk_since}~{chunk_until}: {exc})")
            else:
                if fx_series is not None and not fx_series.dropna().empty:
                    frames.append(fx_series.to_frame())
                    loaded.append("USDKRW=X")

    if not frames:
        return pd.DataFrame(), loaded

    close = pd.concat(frames, axis=1).sort_index()
    close = close.T.groupby(level=0).last().T
    return close, sorted(set(loaded))
