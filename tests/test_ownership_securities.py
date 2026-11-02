"""Ownership metadata keeps exact share classes, source dates and unadjusted caps."""

import json
from datetime import datetime, timedelta, timezone

import pytest

import build_ownership_securities as securities


NOW = datetime(2026, 11, 2, 3, tzinfo=timezone.utc)
STAMP = NOW.isoformat(timespec="seconds")


def config(ticker="005930.KS", shares=1000, subsidiaries=None):
    return [{"holdingTicker": ticker, "holdingName": "모회사", "holdingTotalShares": shares,
             "holdingTreasuryShares": 200, "holdingAdjustedShares": 700,
             "subsidiaries": subsidiaries or []}]


def quote(code="005930", shares="1,000", price="50,000"):
    return {"symbol": code, "summary": {"symbol": code, "listed_shares": shares, "current_price": price},
            "raw": {"stck_shrn_iscd": code, "lstn_stcn": shares, "stck_prpr": price, "hts_avls": "99999"}}


def collect(entries=None, previous=None, fetcher=None, **kwargs):
    def no_foreign_fixture(ticker):
        raise ValueError("No foreign fixture")

    return securities.collect_securities(
        entries or config(), previous, fetcher=fetcher or (lambda ticker: quote(ticker.split(".")[0])),
        now=NOW, request_interval=0, reporter=lambda text: None,
        foreign_fetcher=kwargs.pop("foreign_fetcher", no_foreign_fixture), **kwargs,
    )


def test_market_cap_uses_exact_listed_shares_not_adjusted_or_rounded_hts_cap():
    record = collect()["securities"]["005930.KS"]
    assert record["totalShares"] == 1000
    assert record["marketCap"] == 1000 * 50000
    assert record["currency"] == "KRW"
    assert record["marketCapBasis"] == "listedShares"
    assert record["sharesSource"] == "kis_proxy"


def test_observation_time_is_not_claimed_to_be_exchange_trading_time():
    record = securities.parse_proxy_security("005930.KS", quote(), STAMP)
    assert record["sharesAsOf"] is None
    assert record["priceAsOf"] is None
    assert record["marketCapAsOf"] is None
    assert record["sharesObservedAt"] == STAMP
    assert record["priceObservedAt"] == STAMP
    assert record["timestampBasis"] == "retrieval"


@pytest.mark.parametrize("field", ["symbol", "summary", "raw"])
def test_rejects_different_share_class_even_when_other_fields_match(field):
    payload = quote()
    if field == "symbol":
        payload[field] = "005935"
    elif field == "summary":
        payload[field]["symbol"] = "005935"
    else:
        payload[field]["stck_shrn_iscd"] = "005935"
    with pytest.raises(ValueError, match="share class"):
        securities.parse_proxy_security("005930.KS", payload, STAMP)


def test_missing_code_is_not_accepted_as_exact_share_class():
    with pytest.raises(ValueError, match="share class"):
        securities.parse_proxy_security("005930.KS", {"summary": {"listed_shares": 100}}, STAMP)


@pytest.mark.parametrize("shares", [None, "-", "0", -1, True, "NaN", "Infinity", "12.5"])
def test_invalid_share_count_is_missing_not_zero(shares):
    with pytest.raises(ValueError, match="share count"):
        securities.parse_proxy_security("005930.KS", quote(shares=shares), STAMP)


def test_missing_price_keeps_share_count_without_inventing_market_cap():
    record = securities.parse_proxy_security("005930.KS", quote(price="-"), STAMP)
    assert record["totalShares"] == 1000
    assert record["price"] is None
    assert record["marketCap"] is None
    assert record["priceObservedAt"] is None


def test_common_and_preferred_are_independent_and_no_shares_held_fallback():
    entries = config(subsidiaries=[{"ticker": "005935.KS", "name": "우선주", "sharesHeld": 25}])
    targets = securities.build_targets(entries)
    assert targets["005930.KS"]["configTotalShares"] == 1000
    assert targets["005935.KS"]["configTotalShares"] is None
    result = collect(entries, fetcher=lambda ticker: quote(ticker[:6], shares=200 if ticker[:6] == "005935" else 1000))
    assert result["securities"]["005935.KS"]["totalShares"] == 200
    assert result["securities"]["005930.KS"]["totalShares"] == 1000


def test_conflicting_undated_config_counts_are_not_used_as_fallback():
    entries = config() + config(shares=2000)
    assert securities.build_targets(entries)["005930.KS"]["configTotalShares"] is None


def test_upstream_failure_preserves_provider_value_source_and_observation_date():
    previous = collect()
    previous["securities"]["005930.KS"]["sharesObservedAt"] = (NOW - timedelta(days=2)).isoformat()
    old = dict(previous["securities"]["005930.KS"])

    def fail(ticker):
        raise TimeoutError("unavailable")

    current = collect(previous=previous, fetcher=fail)["securities"]["005930.KS"]
    assert current["totalShares"] == old["totalShares"]
    assert current["sharesSource"] == old["sharesSource"]
    assert current["sharesObservedAt"] == old["sharesObservedAt"]
    assert current["priceObservedAt"] == old["priceObservedAt"]
    assert current["status"] == "stale"


def test_without_previous_data_fallback_is_labelled_undated_config():
    def fail(ticker):
        raise TimeoutError("unavailable")

    current = collect(fetcher=fail)["securities"]["005930.KS"]
    assert current["totalShares"] == 1000
    assert current["sharesSource"] == "config"
    assert current["sharesAsOf"] is None
    assert current["sharesObservedAt"] is None
    assert current["price"] is None
    assert current["marketCap"] is None


def test_fresh_provider_metadata_is_cached_without_network_requests():
    previous = collect()

    def should_not_fetch(ticker):
        pytest.fail("Fresh counts should not cause another provider request")

    assert collect(previous=previous, fetcher=should_not_fetch)["securities"] == previous["securities"]


def test_future_or_timezone_missing_observation_does_not_skip_refresh():
    assert not securities.is_fresh({"sharesSource": "kis_proxy", "totalShares": 5,
                                    "sharesObservedAt": (NOW + timedelta(hours=1)).isoformat()}, NOW, 24)
    assert not securities.is_fresh({"sharesSource": "kis_proxy", "totalShares": 5,
                                    "sharesObservedAt": "2026-11-02T03:00:00"}, NOW, 24)


def test_failed_foreign_class_counts_are_missing_and_currency_never_implicitly_krw():
    entries = config(subsidiaries=[{"ticker": "BRK-A", "name": "버크셔", "sharesHeld": 286},
                                  {"ticker": "AAPL", "name": "애플", "sharesHeld": 319982}])
    result = collect(entries)
    for ticker in ("BRK-A", "AAPL"):
        record = result["securities"][ticker]
        assert record["currency"] == "USD"
        assert record["totalShares"] is None
        assert record["marketCap"] is None
        assert record["status"] == "unavailable"
    assert result["failedTickers"] == ["AAPL", "BRK-A"]


def test_provider_outage_opens_circuit_after_three_attempts():
    entries = config(subsidiaries=[{"ticker": f"{code:06d}.KS", "name": str(code)} for code in range(1, 9)])
    called = []

    def fail(ticker):
        called.append(ticker)
        raise TimeoutError("offline")

    result = collect(entries, fetcher=fail)
    assert len(called) == 3
    assert len(result["failedTickers"]) == 9
    assert len(result["securities"]) == 9


def test_invalid_security_responses_do_not_open_provider_outage_circuit():
    entries = config(subsidiaries=[{"ticker": f"{code:06d}.KS", "name": str(code)} for code in range(1, 6)])
    called = []

    def invalid(ticker):
        called.append(ticker)
        return quote(ticker[:6], shares="-")

    result = collect(entries, fetcher=invalid)
    assert len(called) == 6
    assert len(result["failedTickers"]) == 6


def test_unchanged_cached_data_does_not_update_file_generated_date(tmp_path):
    path = tmp_path / "ownership.json"
    result = collect()
    assert securities.write_if_changed(path, result)
    changed_stamp = dict(result, generatedAt=(NOW + timedelta(minutes=10)).isoformat())
    assert not securities.write_if_changed(path, changed_stamp)
    assert json.loads(path.read_text())["generatedAt"] == STAMP


def test_new_security_is_fetched_even_when_existing_count_is_cached():
    previous = collect()
    entries = config(subsidiaries=[{"ticker": "000660.KS", "name": "SK하이닉스"}])
    called = []

    def fetch(ticker):
        called.append(ticker)
        return quote(ticker[:6], shares=730492365)

    current = collect(entries, previous=previous, fetcher=fetch)
    assert called == ["000660.KS"]
    assert len(current["securities"]) == 2


def foreign_quote(symbol="BRK-A"):
    return {"symbol": symbol, "currency": "USD", "quoteType": "EQUITY",
            "sharesOutstanding": 488450, "impliedSharesOutstanding": 1427140,
            "marketCap": 1163900000000, "currentPrice": 815600,
            "regularMarketTime": 1793390459}


def test_foreign_market_cap_ignores_implied_equivalents_and_company_market_cap():
    record = securities.parse_foreign_security("BRK-A", foreign_quote(), STAMP)
    assert record["totalShares"] == 488450
    assert record["marketCap"] == 398379820000
    assert record["currency"] == "USD"
    assert record["shareCountBasis"] == "sharesOutstanding"
    assert record["sharesIncludeTreasury"] is False
    assert record["sharesSourceField"] == "sharesOutstanding"
    assert record["sharesSource"] == "yahoo_quote"
    assert record["sharesAsOf"] is None
    assert record["sharesObservedAt"] == STAMP
    assert record["priceAsOf"] == "2026-10-30T20:00:59+00:00"


def test_implied_shares_are_never_used_when_exact_class_count_is_missing():
    payload = foreign_quote()
    payload.pop("sharesOutstanding")
    with pytest.raises(ValueError, match="exact sharesOutstanding"):
        securities.parse_foreign_security("BRK-A", payload, STAMP)


@pytest.mark.parametrize("changed", [{"symbol": "BRK-B"}, {"currency": "KRW"}, {"quoteType": "ETF"}])
def test_foreign_parser_rejects_wrong_symbol_currency_or_security_type(changed):
    with pytest.raises(ValueError):
        securities.parse_foreign_security("BRK-A", dict(foreign_quote(), **changed), STAMP)


def test_foreign_refresh_preserves_domestic_record_without_requesting_it():
    previous = collect()
    entries = config(subsidiaries=[{"ticker": "BRK-A", "name": "버크셔", "sharesHeld": 286}])

    def never_domestic(ticker):
        pytest.fail("--only-foreign must preserve domestic data without fetching")

    current = collect(entries, previous=previous, fetcher=never_domestic,
                      foreign_fetcher=lambda ticker: foreign_quote(ticker), only_foreign=True)
    assert current["securities"]["005930.KS"] == previous["securities"]["005930.KS"]
    assert current["securities"]["BRK-A"]["totalShares"] == 488450
    assert current["failedTickers"] == []
    assert not current["isPartial"]


def test_unconfigured_foreign_symbol_stays_unsupported_without_guessing_currency():
    entries = config(subsidiaries=[{"ticker": "UNKNOWN", "name": "미상"}])
    record = collect(entries)["securities"]["UNKNOWN"]
    assert record["status"] == "unsupported"
    assert record["currency"] is None
    assert record["totalShares"] is None
