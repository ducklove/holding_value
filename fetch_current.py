#!/usr/bin/env python3
"""
Generate the current.js snapshot for the holding value dashboard.
"""

import json
import os
import re
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

import yfinance as yf
import pandas as pd

BASE_DIR = Path(__file__).parent
CONFIG_PATH = BASE_DIR / "config.json"
OUTPUT_PATH = BASE_DIR / "current.js"
OUTPUT_JSON_PATH = BASE_DIR / "current.json"
SEOUL_TZ = ZoneInfo("Asia/Seoul")
KIS_BASE_URL = os.environ.get("KIS_BASE_URL", "https://openapi.koreainvestment.com:9443")
KIS_APP_KEY = (
    os.environ.get("KIS_APP_KEY")
    or os.environ.get("KIS_APPKEY")
    or os.environ.get("KOREAINVESTMENT_APP_KEY")
)
KIS_APP_SECRET = (
    os.environ.get("KIS_APP_SECRET")
    or os.environ.get("KIS_APPSECRET")
    or os.environ.get("KOREAINVESTMENT_APP_SECRET")
)
KIS_TOKEN_PATH = Path(os.environ.get("KIS_TOKEN_PATH", BASE_DIR / ".kis_token.json"))
KIS_TIMEOUT = float(os.environ.get("KIS_TIMEOUT", "10"))
KIS_REQUEST_INTERVAL = float(os.environ.get("KIS_REQUEST_INTERVAL", "0.05"))
KIS_DOMESTIC_QUOTE_TR_ID = os.environ.get("KIS_DOMESTIC_QUOTE_TR_ID", "FHKST01010100")
KIS_PROXY_BASE_URL = os.environ.get("KIS_PROXY_BASE_URL", "http://cantabile.tplinkdns.com:3288")
KIS_PROXY_TIMEOUT = float(os.environ.get("KIS_PROXY_TIMEOUT", "10"))
KIS_PROXY_REQUEST_INTERVAL = float(os.environ.get("KIS_PROXY_REQUEST_INTERVAL", "0.7"))

with open(CONFIG_PATH, encoding="utf-8") as f:
    PAIRS = json.load(f)


def is_korean(ticker):
    return ticker.endswith(".KS") or ticker.endswith(".KQ")


def ticker_code(ticker):
    return ticker.split(".", 1)[0]


def batched(values, size):
    for start in range(0, len(values), size):
        yield values[start:start + size]


def parse_number(value):
    if value in (None, ""):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(str(value).replace(",", ""))
    except ValueError:
        return None


def request_json(url, method="GET", headers=None, payload=None, params=None, timeout=KIS_TIMEOUT):
    if params:
        url = f"{url}?{urlencode(params)}"
    data = None
    request_headers = dict(headers or {})
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        request_headers.setdefault("content-type", "application/json; charset=utf-8")

    request = Request(url, data=data, headers=request_headers, method=method)
    with urlopen(request, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8"))


def parse_kis_expiry(value):
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d %H:%M:%S").replace(tzinfo=SEOUL_TZ).timestamp()
    except ValueError:
        return None


def read_cached_kis_token():
    try:
        payload = json.loads(KIS_TOKEN_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None

    expires_at = payload.get("expires_at", 0)
    if payload.get("access_token") and expires_at > time.time() + 300:
        return payload["access_token"]
    return None


def write_cached_kis_token(access_token, expires_at):
    try:
        KIS_TOKEN_PATH.write_text(
            json.dumps({"access_token": access_token, "expires_at": expires_at}, indent=2),
            encoding="utf-8",
        )
    except OSError as exc:
        print(f"  KIS token cache write failed: {exc}")


def get_kis_access_token():
    if not KIS_APP_KEY or not KIS_APP_SECRET:
        print("KIS credentials are not set; falling back to KIS proxy/yfinance for Korean quotes.")
        return None

    cached = read_cached_kis_token()
    if cached:
        return cached

    payload = {
        "grant_type": "client_credentials",
        "appkey": KIS_APP_KEY,
        "appsecret": KIS_APP_SECRET,
    }
    token_response = request_json(f"{KIS_BASE_URL}/oauth2/tokenP", method="POST", payload=payload)
    access_token = token_response.get("access_token")
    if not access_token:
        raise RuntimeError(f"KIS token response missing access_token: {token_response}")

    expires_at = parse_kis_expiry(token_response.get("access_token_token_expired"))
    if expires_at is None:
        expires_at = time.time() + int(token_response.get("expires_in", 86400))
    write_cached_kis_token(access_token, expires_at)
    return access_token


def signed_kis_value(value, sign_code):
    number = parse_number(value)
    if number is None:
        return None

    sign_code = str(sign_code or "")
    if sign_code in {"4", "5"}:
        return -abs(number)
    if sign_code in {"1", "2"}:
        return abs(number)
    if sign_code == "3":
        return 0.0
    return number


def fetch_kis_domestic_quote(ticker, access_token):
    code = ticker_code(ticker)
    headers = {
        "authorization": f"Bearer {access_token}",
        "appkey": KIS_APP_KEY,
        "appsecret": KIS_APP_SECRET,
        "tr_id": KIS_DOMESTIC_QUOTE_TR_ID,
        "custtype": "P",
    }
    params = {
        "fid_cond_mrkt_div_code": "J",
        "fid_input_iscd": code,
    }
    payload = request_json(
        f"{KIS_BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-price",
        headers=headers,
        params=params,
    )
    if payload.get("rt_cd") != "0":
        message = payload.get("msg1") or payload.get("msg_cd") or payload
        raise RuntimeError(f"KIS quote failed for {ticker}: {message}")

    output = payload.get("output") or {}
    current_price = parse_number(output.get("stck_prpr"))
    if current_price is None:
        raise RuntimeError(f"KIS quote missing current price for {ticker}")

    change_amount = signed_kis_value(output.get("prdy_vrss"), output.get("prdy_vrss_sign"))
    previous_price = parse_number(output.get("stck_sdpr")) or parse_number(output.get("prdy_clpr"))
    if previous_price is None and change_amount is not None:
        previous_price = current_price - change_amount

    return current_price, previous_price


def frame_from_price_maps(prices, previous_prices, now_local):
    if not prices:
        return pd.DataFrame()

    previous_date = pd.Timestamp((now_local - timedelta(days=1)).date())
    current_date = pd.Timestamp(now_local.date())
    return pd.DataFrame(
        {
            ticker: [previous_prices.get(ticker), price]
            for ticker, price in prices.items()
        },
        index=[previous_date, current_date],
    )


def fetch_kis_domestic_frame(tickers, now_local):
    domestic_tickers = [ticker for ticker in tickers if is_korean(ticker)]
    if not domestic_tickers:
        return pd.DataFrame(), []

    try:
        access_token = get_kis_access_token()
    except Exception as exc:
        print(f"KIS token request failed; falling back to yfinance ({exc})")
        return pd.DataFrame(), []

    if not access_token:
        return pd.DataFrame(), []

    prices = {}
    previous_prices = {}
    loaded = []
    for ticker in domestic_tickers:
        try:
            price, previous_price = fetch_kis_domestic_quote(ticker, access_token)
        except Exception as exc:
            print(f"  KIS quote failed for {ticker}: {exc}")
            continue

        prices[ticker] = price
        if previous_price is not None:
            previous_prices[ticker] = previous_price
        loaded.append(ticker)
        if KIS_REQUEST_INTERVAL > 0:
            time.sleep(KIS_REQUEST_INTERVAL)

    return frame_from_price_maps(prices, previous_prices, now_local), loaded


def fetch_kis_proxy_domestic_quote(ticker):
    code = ticker_code(ticker)
    payload = request_json(
        f"{KIS_PROXY_BASE_URL}/v1/stocks/{code}/quote",
        timeout=KIS_PROXY_TIMEOUT,
    )
    summary = payload.get("summary") or {}
    raw = payload.get("raw") or {}

    current_price = parse_number(summary.get("current_price")) or parse_number(raw.get("stck_prpr"))
    if current_price is None:
        raise RuntimeError(f"KIS proxy quote missing current price for {ticker}")

    previous_price = parse_number(raw.get("stck_sdpr"))
    if previous_price is None:
        change_amount = parse_number(summary.get("change"))
        if change_amount is None:
            change_amount = signed_kis_value(raw.get("prdy_vrss"), raw.get("prdy_vrss_sign"))
        if change_amount is not None:
            previous_price = current_price - change_amount

    return current_price, previous_price


def fetch_kis_proxy_domestic_frame(tickers, now_local):
    if not KIS_PROXY_BASE_URL:
        return pd.DataFrame(), []

    domestic_tickers = [ticker for ticker in tickers if is_korean(ticker)]
    if not domestic_tickers:
        return pd.DataFrame(), []

    prices = {}
    previous_prices = {}
    loaded = []
    for ticker in domestic_tickers:
        try:
            price, previous_price = fetch_kis_proxy_domestic_quote(ticker)
        except Exception as exc:
            print(f"  KIS proxy quote failed for {ticker}: {exc}")
            continue

        prices[ticker] = price
        if previous_price is not None:
            previous_prices[ticker] = previous_price
        loaded.append(ticker)
        if KIS_PROXY_REQUEST_INTERVAL > 0:
            time.sleep(KIS_PROXY_REQUEST_INTERVAL)

    return frame_from_price_maps(prices, previous_prices, now_local), loaded


def fetch_kis_proxy_index(index_id):
    if not KIS_PROXY_BASE_URL:
        return None

    try:
        payload = request_json(
            f"{KIS_PROXY_BASE_URL}/v1/indexes/{index_id}/quote",
            timeout=KIS_PROXY_TIMEOUT,
        )
    except Exception as exc:
        print(f"  KIS proxy index failed for {index_id}: {exc}")
        return None

    summary = payload.get("summary") or {}
    raw = payload.get("raw") or {}
    price = parse_number(summary.get("current_price")) or parse_number(raw.get("bstp_nmix_prpr"))
    if price is None:
        return None

    return {
        "id": index_id,
        "name": index_id,
        "price": price,
        "change": parse_number(summary.get("change")) or parse_number(raw.get("bstp_nmix_prdy_vrss")),
        "changePct": parse_number(summary.get("change_rate")) or parse_number(raw.get("bstp_nmix_prdy_ctrt")),
        "source": "KIS proxy",
        "priceDecimals": 2,
    }


def fetch_market_summary():
    kospi = fetch_kis_proxy_index("KOSPI")
    kosdaq = fetch_kis_proxy_index("KOSDAQ")
    if kospi and kosdaq:
        kospi["extras"] = [kosdaq]
    return kospi


def parse_existing_current():
    try:
        if OUTPUT_JSON_PATH.exists():
            return json.loads(OUTPUT_JSON_PATH.read_text(encoding="utf-8"))

        if not OUTPUT_PATH.exists():
            return None

        text = OUTPUT_PATH.read_text(encoding="utf-8")
        json_str = re.sub(r"^const CURRENT_DATA\s*=\s*", "", text)
        json_str = re.sub(r";\s*$", "", json_str)
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
    kis_close, kis_loaded = fetch_kis_domestic_frame(tickers, now_local)
    close = merge_close_frames(pd.DataFrame(), [kis_close] if not kis_close.empty else [])
    sources = {ticker: "kis_openapi" for ticker in kis_loaded}
    if kis_loaded:
        print(f"Loaded {len(kis_loaded)} Korean tickers from KIS Open API.")

    proxy_targets = [
        ticker for ticker in tickers
        if is_korean(ticker)
        and (ticker not in close.columns or close[ticker].dropna().empty)
    ]
    proxy_close, proxy_loaded = fetch_kis_proxy_domestic_frame(proxy_targets, now_local)
    close = merge_close_frames(close, [proxy_close] if not proxy_close.empty else [])
    for ticker in proxy_loaded:
        sources[ticker] = "kis_proxy"
    if proxy_loaded:
        print(f"Loaded {len(proxy_loaded)} Korean tickers from KIS proxy.")

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
    previous_holding_value = 0.0
    has_previous_ratio = previous_holding_price not in (None, 0)
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
        if previous_sub_price is None:
            has_previous_ratio = False
        else:
            previous_holding_value += sub["sharesHeld"] * previous_sub_price
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
    previous_ratio = None
    if has_previous_ratio:
        previous_market_cap = adjusted_shares * previous_holding_price
        if previous_market_cap:
            previous_ratio = previous_holding_value / previous_market_cap * 100

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
        "ratioChange": round(ratio - previous_ratio, 2) if previous_ratio is not None else None,
        "quoteSource": (
            "kis_openapi"
            if used_sources == {"kis_openapi"}
            else "kis_proxy" if used_sources == {"kis_proxy"} else "yfinance" if used_sources == {"yfinance"} else "mixed"
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
    ratio_changes = [
        pair["ratioChange"]
        for pair in live_pairs
        if isinstance(pair.get("ratioChange"), (int, float))
    ]
    return {
        "id": "_average",
        "ratio": round(avg_ratio, 2),
        "ratioChange": round(sum(ratio_changes) / len(ratio_changes), 2) if ratio_changes else None,
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
    market = fetch_market_summary()

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
        "market": market,
        "pairs": pairs_result,
    }

    json_content = json.dumps(current_data, ensure_ascii=False, indent=2)
    js_content = "const CURRENT_DATA = " + json_content + ";\n"
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        f.write(js_content)
    with open(OUTPUT_JSON_PATH, "w", encoding="utf-8") as f:
        f.write(json_content + "\n")

    print(f"\nGenerated {OUTPUT_PATH} and {OUTPUT_JSON_PATH} ({len(pairs_result)} pairs)")


if __name__ == "__main__":
    main()
