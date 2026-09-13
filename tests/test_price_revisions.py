import pandas as pd
import pytest

from price_revisions import has_price_revision, require_complete_refresh
from fetch_data import find_price_revision_pairs


def test_split_revision_uses_same_date_even_across_trading_halt():
    history = [{"date": "2026-08-13", "commonPrice": 1000}]
    series = pd.Series([2000, 1874], index=pd.to_datetime(["2026-08-13", "2026-09-10"]))
    assert has_price_revision(history, {"commonPrice": series})
    # 같은 날 값이 같으면 이후의 실제 주가 변동만으로 수정주가라 추정하지 않는다.
    series.iloc[0] = 1000
    assert not has_price_revision(history, {"commonPrice": series})


def test_rounding_and_missing_quotes_are_not_revisions():
    history = [{"date": "2026-08-13", "commonPrice": 1000}]
    series = pd.Series([1000.49], index=pd.to_datetime(["2026-08-13"]))
    assert not has_price_revision(history, {"commonPrice": series})
    series.iloc[0] = float("nan")
    assert not has_price_revision(history, {"commonPrice": series})


def test_partial_refresh_cannot_preserve_unadjusted_older_prices():
    old = [{"date": "2026-08-12"}, {"date": "2026-08-13"}]
    with pytest.raises(ValueError, match="재수집 누락"):
        require_complete_refresh(old[1:], old, "daekyo")
    require_complete_refresh(old, old, "daekyo")


def test_subsidiary_split_rebuilds_every_parent_pair():
    pairs = [
        {"id": pid, "holdingTicker": ht, "subsidiaries": [
            {"ticker": "032860.KQ", "name": "더라미"},
            {"ticker": "057540.KQ", "name": "옴니시스템"},
        ]}
        for pid, ht in [("one", "038460.KQ"), ("two", "111111.KQ")]
    ]
    old = {p["id"]: {"history": [{"date": "2026-08-13", "holdingPrice": 2000,
            "subsidiaries": [{"name": "더라미", "price": 690},
                             {"name": "옴니시스템", "price": 1000}]}]} for p in pairs}
    close = pd.DataFrame({"032860.KQ": [3450], "038460.KQ": [2000],
                          "111111.KQ": [2000], "057540.KQ": [1000]},
                         index=pd.to_datetime(["2026-08-13"]))
    assert find_price_revision_pairs(pairs, old, close) == {"one", "two"}
