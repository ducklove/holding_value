"""fetch_current.py의 평균·세션 판별·캐시 호환성 회귀 테스트."""

from datetime import datetime

import fetch_current


SEOUL = fetch_current.SEOUL_TZ


def kst(year, month, day, hour, minute=0):
    return datetime(year, month, day, hour, minute, tzinfo=SEOUL)


# --- build_average_entry ---

def test_average_entry_uses_median():
    pairs = [
        {"id": "a", "ratio": 10.0, "ratioChange": 1.0},
        {"id": "b", "ratio": 20.0, "ratioChange": -1.0},
        {"id": "c", "ratio": 4000.0, "ratioChange": 3.0},
    ]
    entry = fetch_current.build_average_entry(pairs)
    assert entry["ratio"] == 20.0          # 이상치가 지배하지 못함
    assert entry["mean"] == round((10 + 20 + 4000) / 3, 2)
    assert entry["count"] == 3
    assert entry["ratioChange"] == 1.0     # median(1, -1, 3)
    assert entry["quoteSource"] == "derived"


def test_average_entry_ignores_existing_average_and_handles_empty():
    assert fetch_current.build_average_entry([]) is None
    pairs = [{"id": "_average", "ratio": 99.0}]
    assert fetch_current.build_average_entry(pairs) is None


def test_average_entry_without_changes():
    pairs = [{"id": "a", "ratio": 10.0, "ratioChange": None}]
    entry = fetch_current.build_average_entry(pairs)
    assert entry["ratioChange"] is None


# --- build_session_info ---

def test_session_kr_day():
    info = fetch_current.build_session_info(kst(2026, 6, 10, 10))  # 수요일 10:00
    assert info["name"] == "kr_day"
    assert info["date"] == "2026-06-10"


def test_session_us_night_evening_and_next_morning():
    evening = fetch_current.build_session_info(kst(2026, 6, 10, 22))  # 수요일 22:00
    assert evening["name"] == "us_night"
    assert evening["date"] == "2026-06-10"

    morning = fetch_current.build_session_info(kst(2026, 6, 11, 2))  # 목요일 02:00
    assert morning["name"] == "us_night"
    assert morning["date"] == "2026-06-10"  # 전일 세션의 연속


def test_session_offhours():
    assert fetch_current.build_session_info(kst(2026, 6, 10, 17))["name"] == "offhours"
    assert fetch_current.build_session_info(kst(2026, 6, 13, 12))["name"] == "offhours"  # 토요일


# --- is_cached_entry_compatible ---

def test_cached_entry_compatibility_single_sub():
    pair = {"subsidiaries": [{"name": "자회사A"}]}
    assert fetch_current.is_cached_entry_compatible(pair, {"id": "x"}) is True
    assert fetch_current.is_cached_entry_compatible(pair, {"subsidiaries": [{"name": "자회사A"}]}) is False
    assert fetch_current.is_cached_entry_compatible(pair, None) is False


def test_cached_entry_compatibility_multi_sub_names_must_match():
    pair = {"subsidiaries": [{"name": "A"}, {"name": "B"}]}
    matching = {"subsidiaries": [{"name": "A"}, {"name": "B"}]}
    reordered = {"subsidiaries": [{"name": "B"}, {"name": "A"}]}
    assert fetch_current.is_cached_entry_compatible(pair, matching) is True
    assert fetch_current.is_cached_entry_compatible(pair, reordered) is False


# --- signed_kis_value ---

def test_signed_kis_value_sign_codes():
    assert fetch_current.signed_kis_value("120", "5") == -120.0  # 하락
    assert fetch_current.signed_kis_value("120", "2") == 120.0   # 상승
    assert fetch_current.signed_kis_value("120", "3") == 0.0     # 보합
    assert fetch_current.signed_kis_value(None, "2") is None
