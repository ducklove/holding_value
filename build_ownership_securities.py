#!/usr/bin/env python3
"""Persist share-class-specific listed shares and ordinary market caps for the graph.

KIS ``lstn_stcn`` is the listed share count for the requested security code.  It is
never reduced by treasury shares.  Market cap is that count times the same quote's
price, in KRW, rather than the dashboard's adjusted market cap or rounded HTS cap.

The public proxy requires no repository secret.  Its quote has no trading-time
field, so *ObservedAt records retrieval time while *AsOf remains null.  A failed
refresh retains the old observation and source; config is an explicit last resort.
"""

import argparse
import json
import math
import os
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


BASE_DIR = Path(__file__).resolve().parent
DEFAULT_PROXY = "http://ducklove.duckdns.org:3288"
DOMESTIC_TICKER = re.compile(r"^[0-9A-Z]{6}\.(KS|KQ)$")
PROVIDER_SOURCE = "kis_proxy"
FOREIGN_SOURCE = "yahoo_quote"
SUPPORTED_FOREIGN = {"AAPL", "BRK-A"}


def positive_number(value, integer=False):
    """Reject absent, zero, boolean and non-finite values without coercing to zero."""
    if isinstance(value, bool) or value is None:
        return None
    try:
        number = float(str(value).replace(",", "").strip())
    except (TypeError, ValueError):
        return None
    if not math.isfinite(number) or number <= 0 or (integer and not number.is_integer()):
        return None
    return int(number) if number.is_integer() else number


def build_targets(config):
    """Index exact tickers; sharesHeld and adjusted shares are never denominators."""
    targets = {}

    def add(ticker, name, currency=None, shares=None):
        ticker = str(ticker or "").strip().upper()
        if not ticker:
            return
        is_domestic = bool(DOMESTIC_TICKER.fullmatch(ticker))
        currency = "KRW" if is_domestic else currency or ("USD" if ticker in {"AAPL", "BRK-A"} else None)
        item = targets.setdefault(ticker, {
            "ticker": ticker, "name": name or ticker, "currency": currency,
            "configTotalShares": None,
        })
        if name:
            item["name"] = name
        shares = positive_number(shares, integer=True)
        if shares is not None:
            if item["configTotalShares"] not in (None, shares):
                item["configSharesConflict"] = True
            item["configTotalShares"] = shares

    for pair in config:
        add(pair.get("holdingTicker"), pair.get("holdingName"), pair.get("holdingCurrency"), pair.get("holdingTotalShares"))
        for sub in pair.get("subsidiaries") or []:
            add(sub.get("ticker"), sub.get("name"), sub.get("currency"), sub.get("totalShares"))
    for target in targets.values():
        if target.pop("configSharesConflict", False):
            target["configTotalShares"] = None
    return dict(sorted(targets.items()))


def parse_proxy_security(ticker, payload, observed_at):
    """Validate the returned share class before accepting any numeric metadata."""
    if not DOMESTIC_TICKER.fullmatch(ticker):
        raise ValueError("Only exact domestic security codes are supported")
    if not isinstance(payload, dict):
        raise ValueError("Quote payload is not an object")
    summary = payload.get("summary") or {}
    raw = payload.get("raw") or {}
    expected = ticker.split(".")[0]
    codes = [payload.get("symbol"), summary.get("symbol"), raw.get("stck_shrn_iscd")]
    codes = [str(code).strip().upper() for code in codes if code is not None]
    if not codes or any(code != expected for code in codes):
        raise ValueError("Quote security code does not match requested share class")
    if payload.get("rt_cd") not in (None, "0", 0):
        raise ValueError("KIS returned an unsuccessful quote")

    shares = positive_number(raw.get("lstn_stcn"), integer=True)
    if shares is None:
        shares = positive_number(summary.get("listed_shares"), integer=True)
    if shares is None:
        raise ValueError("Quote has no positive listed share count")
    price = positive_number(raw.get("stck_prpr"))
    if price is None:
        price = positive_number(summary.get("current_price"))
    return {
        "ticker": ticker,
        "currency": "KRW",
        "totalShares": shares,
        "sharesSource": PROVIDER_SOURCE,
        "sharesSourceField": "lstn_stcn",
        "sharesAsOf": None,
        "sharesObservedAt": observed_at,
        "price": price,
        "priceSource": PROVIDER_SOURCE if price is not None else None,
        "priceSourceField": "stck_prpr" if price is not None else None,
        "priceAsOf": None,
        "priceObservedAt": observed_at if price is not None else None,
        "marketCap": shares * price if price is not None else None,
        "marketCapSource": "listedShares*price" if price is not None else None,
        "marketCapBasis": "listedShares",
        "shareCountBasis": "listedShares",
        "sharesIncludeTreasury": True,
        "marketCapAsOf": None,
        "marketCapObservedAt": observed_at if price is not None else None,
        "observedAt": observed_at,
        "timestampBasis": "retrieval",
        "status": "current" if price is not None else "partial",
    }


def parse_foreign_security(ticker, payload, observed_at):
    """Only accept sharesOutstanding for the exact verified US share class.

    Yahoo's marketCap and impliedSharesOutstanding may aggregate multiple share
    classes (notably BRK-A/BRK-B). They must never enter this security-level graph.
    """
    if ticker not in SUPPORTED_FOREIGN or not isinstance(payload, dict):
        raise ValueError("Share-class-specific foreign source not configured")
    if str(payload.get("symbol") or "").upper() != ticker:
        raise ValueError("Foreign quote security code does not match requested share class")
    if payload.get("currency") != "USD" or payload.get("quoteType") != "EQUITY":
        raise ValueError("Foreign quote is not the expected USD equity")
    shares = positive_number(payload.get("sharesOutstanding"), integer=True)
    if shares is None:
        raise ValueError("Foreign quote has no exact sharesOutstanding")
    price = positive_number(payload.get("currentPrice"))
    if price is None:
        price = positive_number(payload.get("regularMarketPrice"))
    traded_at = None
    market_time = positive_number(payload.get("regularMarketTime"))
    if price is not None and market_time is not None:
        try:
            traded_at = datetime.fromtimestamp(market_time, tz=timezone.utc).isoformat(timespec="seconds")
        except (ValueError, OverflowError, OSError):
            pass
    return {
        "ticker": ticker, "currency": "USD",
        "totalShares": shares, "sharesSource": FOREIGN_SOURCE,
        "sharesSourceField": "sharesOutstanding", "sharesAsOf": None,
        "sharesObservedAt": observed_at,
        "shareCountBasis": "sharesOutstanding",
        "sharesIncludeTreasury": False,
        "price": price, "priceSource": FOREIGN_SOURCE if price is not None else None,
        "priceSourceField": "currentPrice" if price is not None else None,
        "priceAsOf": traded_at, "priceObservedAt": observed_at if price is not None else None,
        "marketCap": shares * price if price is not None else None,
        "marketCapSource": "sharesOutstanding*price" if price is not None else None,
        "marketCapBasis": "sharesOutstanding",
        "marketCapAsOf": traded_at,
        "marketCapObservedAt": observed_at if price is not None else None,
        "observedAt": observed_at,
        "timestampBasis": "shares: retrieval; price: exchange trading time when available",
        "status": "current" if price is not None else "partial",
    }


def fetch_foreign_quote(ticker):
    # yfinance is already a pinned pipeline dependency; domestic metadata remains
    # stdlib-only, so importing this module in offline tests does not load it.
    import yfinance as yf

    return yf.Ticker(ticker).get_info()


def fetch_proxy_quote(ticker, base_url=DEFAULT_PROXY, timeout=8):
    code = ticker.split(".")[0]
    request = Request(
        f"{base_url.rstrip('/')}/v1/stocks/{code}/quote",
        headers={"User-Agent": "holding-value-ownership/1.0", "Accept": "application/json"},
    )
    with urlopen(request, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8"))


def parse_timestamp(value):
    try:
        stamp = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return stamp if stamp.tzinfo is not None else None
    except (ValueError, TypeError):
        return None


def is_fresh(record, now, max_age_hours):
    observed = parse_timestamp(record.get("sharesObservedAt"))
    if observed is None or record.get("sharesSource") not in {PROVIDER_SOURCE, FOREIGN_SOURCE}:
        return False
    if positive_number(record.get("totalShares"), integer=True) is None:
        return False
    age = (now - observed).total_seconds()
    return 0 <= age < max_age_hours * 3600


def fallback_record(target, previous, error):
    ticker = target["ticker"]
    previous = previous if isinstance(previous, dict) else {}
    # A previous provider observation is more informative than an undated config.
    if previous.get("ticker") == ticker and previous.get("currency") == target["currency"] and positive_number(previous.get("totalShares"), integer=True) is not None:
        record = dict(previous)
    else:
        shares = target["configTotalShares"]
        record = {
            "ticker": ticker, "currency": target["currency"],
            "totalShares": shares, "sharesSource": "config" if shares is not None else None,
            "sharesAsOf": None, "sharesObservedAt": None,
            "price": None, "priceSource": None, "priceAsOf": None, "priceObservedAt": None,
            "marketCap": None, "marketCapSource": None, "marketCapBasis": "listedShares",
            "shareCountBasis": "listedShares" if DOMESTIC_TICKER.fullmatch(ticker) else None,
            "sharesIncludeTreasury": True if DOMESTIC_TICKER.fullmatch(ticker) else None,
            "marketCapAsOf": None, "marketCapObservedAt": None,
            "observedAt": None, "timestampBasis": None,
        }
    record["name"] = target["name"]
    record["status"] = "stale" if record.get("sharesSource") in {PROVIDER_SOURCE, FOREIGN_SOURCE} else "unavailable"
    record["fetchError"] = error
    return record


def collect_securities(config, previous=None, fetcher=None, now=None, max_age_hours=24,
                       request_interval=0.7, sleeper=time.sleep, max_consecutive_failures=3,
                       reporter=print, foreign_fetcher=None, only_foreign=False):
    """Refresh expired records; circuit-break outages without losing prior data."""
    fixed_clock = now is not None
    now = now or datetime.now(timezone.utc)
    observed_at = now.isoformat(timespec="seconds")
    previous = previous if isinstance(previous, dict) else {}
    prior_records = previous.get("securities") or {}
    fetcher = fetcher or fetch_proxy_quote
    foreign_fetcher = foreign_fetcher or fetch_foreign_quote
    targets = build_targets(config)
    records = {}
    failed = []
    unsupported = []
    consecutive_failures = 0
    requested = False
    for ticker, target in targets.items():
        old = prior_records.get(ticker) or {}
        is_domestic = bool(DOMESTIC_TICKER.fullmatch(ticker))
        if only_foreign and is_domestic:
            records[ticker] = dict(old, name=target["name"]) if old else fallback_record(target, {}, "Not refreshed by --only-foreign")
            if records[ticker].get("status") in {"stale", "unavailable"}:
                failed.append(ticker)
            continue
        if not is_domestic and ticker not in SUPPORTED_FOREIGN:
            record = fallback_record(target, {}, "Share-class-specific source not configured")
            record["status"] = "unsupported"
            records[ticker] = record
            unsupported.append(ticker)
            continue
        if is_fresh(old, now, max_age_hours) and old.get("ticker") == ticker and old.get("currency") == target["currency"]:
            records[ticker] = dict(old, name=target["name"])
            continue
        if is_domestic and consecutive_failures >= max_consecutive_failures:
            records[ticker] = fallback_record(target, old, "Upstream unavailable; refresh deferred")
            failed.append(ticker)
            continue
        if requested and request_interval > 0:
            sleeper(request_interval)
        requested = True
        try:
            payload = fetcher(ticker) if is_domestic else foreign_fetcher(ticker)
            record_time = observed_at if fixed_clock else datetime.now(timezone.utc).isoformat(timespec="seconds")
            record = (parse_proxy_security if is_domestic else parse_foreign_security)(ticker, payload, record_time)
        except Exception as exc:
            # Invalid/delisted securities must not stop unrelated metadata refreshes.
            # Only a transport outage or provider-wide HTTP failure opens the circuit.
            provider_down = (
                (isinstance(exc, HTTPError) and (exc.code >= 500 or exc.code == 429))
                or (not isinstance(exc, HTTPError) and isinstance(exc, (URLError, TimeoutError, OSError)))
            )
            if is_domestic:
                consecutive_failures = consecutive_failures + 1 if provider_down else 0
            records[ticker] = fallback_record(target, old, f"{type(exc).__name__}: {exc}")
            failed.append(ticker)
            reporter(f"  {ticker}: refresh failed ({type(exc).__name__})")
        else:
            if is_domestic:
                consecutive_failures = 0
            record["name"] = target["name"]
            records[ticker] = record
            reporter(f"  {ticker}: {record['totalShares']:,} shares ({record['sharesSource']})")
    for record in records.values():
        # Add semantic metadata when reusing observations generated by v1's first
        # domestic-only collection; retain every original value and timestamp.
        if record.get("sharesSource") == PROVIDER_SOURCE:
            record.setdefault("shareCountBasis", "listedShares")
            record.setdefault("sharesIncludeTreasury", True)
    return {
        "schemaVersion": 1,
        "generatedAt": observed_at if fixed_clock else datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "shareCountBasis": "exact ticker: domestic listed shares including treasury; foreign outstanding shares excluding treasury",
        "marketCapBasis": "exact security shares*native-currency price; domestic counts not treasury-adjusted",
        "timestampBasis": "ObservedAt is retrieval time; AsOf is source trading/report time, null when unavailable",
        "securities": records,
        "failedTickers": failed,
        "unsupportedTickers": unsupported,
        "isPartial": bool(failed or unsupported),
    }


def write_if_changed(path, payload):
    """Skip ten-minute commits when all observations are still inside their TTL."""
    try:
        old = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        old = None
    comparable = {key: value for key, value in payload.items() if key != "generatedAt"}
    previous = {key: value for key, value in (old or {}).items() if key != "generatedAt"}
    if comparable == previous:
        return False
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    temporary.replace(path)
    return True


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=BASE_DIR / "config.json")
    parser.add_argument("--output", type=Path, default=BASE_DIR / "data" / "ownership-securities.json")
    parser.add_argument("--max-age-hours", type=float, default=24)
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--only-foreign", action="store_true", help="Refresh configured US securities, preserving domestic records")
    parser.add_argument("--request-interval", type=float, default=0.7)
    args = parser.parse_args()
    config = json.loads(args.config.read_text(encoding="utf-8"))
    try:
        previous = json.loads(args.output.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        previous = {}
    proxy = os.environ.get("KIS_PROXY_BASE_URL", DEFAULT_PROXY)
    timeout = float(os.environ.get("KIS_PROXY_TIMEOUT", "8"))
    payload = collect_securities(
        config, previous, fetcher=lambda ticker: fetch_proxy_quote(ticker, proxy, timeout),
        max_age_hours=0 if args.force else args.max_age_hours,
        request_interval=max(0, args.request_interval),
        only_foreign=args.only_foreign,
    )
    changed = write_if_changed(args.output, payload)
    records = payload["securities"]
    sourced = sum(record.get("sharesSource") in {PROVIDER_SOURCE, FOREIGN_SOURCE} for record in records.values())
    print(f"Ownership securities: {sourced}/{len(records)} provider observations; "
          f"{len(payload['failedTickers'])} refresh failures; "
          f"{len(payload['unsupportedTickers'])} unsupported; "
          f"{'updated' if changed else 'unchanged'} {args.output}")


if __name__ == "__main__":
    main()
