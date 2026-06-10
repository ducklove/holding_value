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


# --- build_alert_messages ---

ALERT_CONFIG = {"demo": {"id": "demo", "name": "데모", "alertBelow": 80, "alertAbove": 300}}


def alert_messages(ratio, previous_ratio):
    pairs = [{"id": "demo", "ratio": ratio}]
    previous = {"demo": {"id": "demo", "ratio": previous_ratio}} if previous_ratio is not None else {}
    return fetch_current.build_alert_messages(pairs, previous, ALERT_CONFIG)


def test_alert_fires_on_crossing_below():
    messages = alert_messages(79.0, 85.0)
    assert len(messages) == 1 and "하한" in messages[0]


def test_alert_does_not_refire_while_staying_below():
    assert alert_messages(78.0, 79.0) == []


def test_alert_refires_after_recovery_and_recross():
    assert alert_messages(79.5, 81.0) != []   # 회복 후 재교차


def test_alert_fires_on_crossing_above_and_without_previous():
    assert any("상한" in m for m in alert_messages(301.0, 299.0))
    assert alert_messages(79.0, None) != []   # 직전 스냅샷 없으면 발송


def test_alert_ignores_pairs_without_thresholds():
    pairs = [{"id": "other", "ratio": 10.0}]
    config = {"other": {"id": "other", "name": "기타"}}
    assert fetch_current.build_alert_messages(pairs, {}, config) == []


# --- signed_kis_value ---

def test_signed_kis_value_sign_codes():
    assert fetch_current.signed_kis_value("120", "5") == -120.0  # 하락
    assert fetch_current.signed_kis_value("120", "2") == 120.0   # 상승
    assert fetch_current.signed_kis_value("120", "3") == 0.0     # 보합
    assert fetch_current.signed_kis_value(None, "2") is None
