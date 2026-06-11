"""장중 스냅샷의 순수 로직 (fetch_current.py에서 이동, stdlib 전용).

세션 판별·캐시 호환성·전체 지표 엔트리·알림 메시지·KIS 값 파싱을 담당한다.
fetch_current.py가 이 모듈의 이름을 재수출하므로 기존 fetch_current.X 참조는 그대로 동작한다.
"""

import json
import math
import statistics
from datetime import timedelta
from zoneinfo import ZoneInfo

from pipeline.core import calculate_pct_change as calculate_pct_change  # 재수출

SEOUL_TZ = ZoneInfo("Asia/Seoul")


def parse_number(value):
    if value in (None, ""):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(str(value).replace(",", ""))
    except ValueError:
        return None


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


def build_average_entry(pairs_result):
    """전체 지표 엔트리. 대표값은 중앙값(ratio), 평균은 mean으로 병기.

    일별 배치(fetch_data.build_average_history)와 같은 정의를 유지해야 한다.
    """
    live_pairs = [pair for pair in pairs_result if pair.get("id") != "_average"]
    if not live_pairs:
        return None

    ratios = [pair["ratio"] for pair in live_pairs]
    ratio_changes = [
        pair["ratioChange"]
        for pair in live_pairs
        if isinstance(pair.get("ratioChange"), (int, float))
    ]
    return {
        "id": "_average",
        "ratio": round(statistics.median(ratios), 2),
        "mean": round(math.fsum(ratios) / len(ratios), 2),
        "count": len(live_pairs),
        "ratioChange": round(statistics.median(ratio_changes), 2) if ratio_changes else None,
        "quoteSource": "derived",
    }


def build_alert_messages(pairs_result, previous_pairs_by_id, pair_config_map):
    """비율이 알림 임계(alertBelow/alertAbove)를 '교차'한 종목의 메시지 목록.

    직전 스냅샷에서 이미 같은 쪽에 있었으면 재발송하지 않는다
    (10분 주기 중복 방지 — 상태 파일 없이 직전 스냅샷 비교로 해결).
    """
    messages = []
    for entry in pairs_result:
        pair_id = entry.get("id")
        config = pair_config_map.get(pair_id)
        if not config:
            continue
        ratio = entry.get("ratio")
        if not isinstance(ratio, (int, float)):
            continue
        previous_ratio = (previous_pairs_by_id.get(pair_id) or {}).get("ratio")
        below = config.get("alertBelow")
        above = config.get("alertAbove")
        name = config.get("name") or pair_id
        if isinstance(below, (int, float)) and ratio < below:
            if not (isinstance(previous_ratio, (int, float)) and previous_ratio < below):
                messages.append(f"🔻 {name}: {ratio:.2f}% < 하한 {below:g}%")
        if isinstance(above, (int, float)) and ratio > above:
            if not (isinstance(previous_ratio, (int, float)) and previous_ratio > above):
                messages.append(f"🔺 {name}: {ratio:.2f}% > 상한 {above:g}%")
    return messages
