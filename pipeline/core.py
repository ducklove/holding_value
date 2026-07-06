"""일별 히스토리 파이프라인의 순수 로직 (fetch_data.py에서 이동).

병합·다운샘플·추세 주석·전체 지표·품질 가드·컬럼형 직렬화를 담당한다.
fetch_data.py가 이 모듈의 이름을 재수출하므로 기존 fetch_data.X 참조는 그대로 동작한다.
의존성은 표준 라이브러리 + fin-commons(stdlib only)뿐이다.
"""

import math
import statistics
from collections import defaultdict, deque
from datetime import datetime, timedelta

from fin_commons.jsonio import atomic_write_text

DAILY_RETENTION_DAYS = 730
SMA_WINDOW = 250
EMA_ALPHA = 0.1
# 전체 지표(중앙값) 산출에 필요한 최소 구성 종목 수 — 미만 날짜는 산출 제외
MIN_AVERAGE_COUNT = 20
# 이 값을 넘는 비율은 validFrom 미설정/데이터 오류 후보로 경고
MAX_SANE_RATIO = 1500.0
STALE_PAIR_WARN_DAYS = 14
# 증분 수집 시 가장 뒤처진 종목 기준 하한 (무한 후행 방지)
INCREMENTAL_MAX_LOOKBACK_DAYS = 90
# 백분위 칩 계산에 필요한 최소 표본 수
PCTILE_MIN_POINTS = 30
# 컬럼형 히스토리에 직렬화하는 행 필드 (date·subsidiaries 제외)
HISTORY_COLUMNS = [
    "holdingPrice",
    "subsidiaryPrice",
    "holdingValue",
    "marketCap",
    "ratio",
    "sma250",
    "ema01",
    "mean",
    "count",
]


def find_multi_sub_backfill_ids(existing, pair_config_map):
    """다중 자회사별 히스토리 분해가 없는 종목만 골라 재수집 대상으로 반환한다."""
    rebuild_ids = set()
    for pair in existing.get("pairs", []):
        if pair.get("isAverage"):
            continue
        pair_id = pair.get("id")
        config = pair_config_map.get(pair_id)
        if not config or len(config.get("subsidiaries", [])) <= 1:
            continue
        history = pair.get("history", [])
        if any("subsidiaries" not in entry for entry in history):
            rebuild_ids.add(pair_id)
    return rebuild_ids


def find_new_pair_ids(existing, pair_config_map):
    """기존 데이터에 히스토리가 없는 신규 종목 id 집합을 반환한다.

    과거에는 신규 종목이 감지되면 전 종목을 풀 재빌드해 누적 이력이 통째로
    재작성되는 문제(C1)가 있었다. 신규 종목만 전체 기간을 수집하고 기존
    종목은 증분 병합을 유지하기 위해 대상을 집합으로 분리한다.
    """
    existing_ids = {
        pair.get("id")
        for pair in existing.get("pairs", [])
        if pair.get("id") and pair.get("history") and not pair.get("isAverage")
    }
    return {pair_id for pair_id in pair_config_map if pair_id not in existing_ids}


def parse_date_key(date_str):
    return datetime.strptime(date_str, "%Y-%m-%d").date()


def calculate_pct_change(current_price, previous_price):
    if current_price is None or previous_price in (None, 0):
        return None
    return round((current_price - previous_price) / previous_price * 100, 2)


def get_holding_adjusted_shares(pair):
    return pair.get(
        "holdingAdjustedShares",
        pair["holdingTotalShares"] - pair["holdingTreasuryShares"],
    )


def annotate_history_with_trends(history, start_idx=0):
    if not history:
        return history

    start_idx = max(0, min(start_idx, len(history) - 1))

    if start_idx > 0 and history[start_idx - 1].get("ema01") is not None:
        ema = float(history[start_idx - 1]["ema01"])
    else:
        ema = None
        start_idx = 0

    window_start = max(0, start_idx - (SMA_WINDOW - 1))
    window = deque(
        (history[idx]["ratio"] for idx in range(window_start, start_idx)),
        maxlen=SMA_WINDOW,
    )
    rolling_sum = sum(window)

    for idx in range(start_idx, len(history)):
        ratio = history[idx]["ratio"]
        if len(window) == SMA_WINDOW:
            rolling_sum -= window[0]
        window.append(ratio)
        rolling_sum += ratio

        history[idx]["sma250"] = round(rolling_sum / SMA_WINDOW, 2) if len(window) == SMA_WINDOW else None
        ema = ratio if ema is None else (EMA_ALPHA * ratio) + ((1 - EMA_ALPHA) * ema)
        history[idx]["ema01"] = round(ema, 2)

    return history


def downsample_history(history):
    if len(history) < 2:
        return history

    latest_date = parse_date_key(history[-1]["date"])
    cutoff_date = latest_date - timedelta(days=DAILY_RETENTION_DAYS)

    older = []
    recent = []
    for entry in history:
        if parse_date_key(entry["date"]) < cutoff_date:
            older.append(entry)
        else:
            recent.append(entry)

    if not older:
        return history

    weekly = []
    current_week = None
    for entry in older:
        date = parse_date_key(entry["date"])
        iso = date.isocalendar()
        week_key = (iso.year, iso.week)
        if weekly and week_key == current_week:
            weekly[-1] = entry
        else:
            weekly.append(entry)
            current_week = week_key

    return weekly + recent


def trailing_percentile(history, days, min_points=PCTILE_MIN_POINTS):
    """마지막 비율이 최근 days일 분포에서 차지하는 백분위(0~100, 정수).

    3년(1095일) 창은 730일 이전 주간 다운샘플 구간을 포함하므로 근사치다.
    표본이 min_points 미만이면 None.
    """
    if not history:
        return None
    latest = history[-1]
    cutoff = (parse_date_key(latest["date"]) - timedelta(days=days)).strftime("%Y-%m-%d")
    window = [entry["ratio"] for entry in history if entry["date"] >= cutoff]
    if len(window) < min_points:
        return None
    rank = sum(1 for ratio in window if ratio <= latest["ratio"])
    return round(rank / len(window) * 100)


def annotate_current_percentiles(current, history):
    """current에 pctile1y/pctile3y를 추가한다 (계산 불가 시 키 생략)."""
    pctile_1y = trailing_percentile(history, 365)
    pctile_3y = trailing_percentile(history, 1095)
    if pctile_1y is not None:
        current["pctile1y"] = pctile_1y
    if pctile_3y is not None:
        current["pctile3y"] = pctile_3y
    return current


def merge_pair_history(new_history, old_history, valid_from=""):
    """새 구간을 기존 히스토리에 병합한다. validFrom 이전 구간은 양쪽 모두 제거.

    반환: (merged_history, trend_recompute_idx)
    """
    if valid_from:
        new_history = [e for e in new_history if e["date"] >= valid_from]
    if not old_history:
        return new_history, 0

    new_dates = {e["date"] for e in new_history}
    merged = [
        e for e in old_history
        if e["date"] not in new_dates and (not valid_from or e["date"] >= valid_from)
    ]
    merged.extend(new_history)
    merged.sort(key=lambda e: e["date"])

    trend_recompute_idx = 0
    if new_dates:
        first_changed_date = min(new_dates)
        changed_idx = next(
            (idx for idx, entry in enumerate(merged) if entry["date"] >= first_changed_date),
            0,
        )
        trend_recompute_idx = max(0, changed_idx - (SMA_WINDOW - 1))
    return merged, trend_recompute_idx


def build_average_history(pairs_result, min_count=MIN_AVERAGE_COUNT):
    """전체 지표 히스토리를 만든다. 대표값은 중앙값(ratio), 평균은 mean으로 병기.

    구성 종목이 min_count 미만인 날짜는 제외한다 — 초기 구간의 소표본·
    구성 드리프트가 장기 곡선을 왜곡하는 문제(H2)의 방어선.
    """
    daily_ratios = defaultdict(list)
    for pair_data in pairs_result:
        for h in pair_data["history"]:
            daily_ratios[h["date"]].append(h["ratio"])

    avg_history = []
    for date in sorted(daily_ratios.keys()):
        ratios = daily_ratios[date]
        if len(ratios) < min_count:
            continue
        avg_history.append({
            "date": date,
            "holdingPrice": 0,
            "subsidiaryPrice": 0,
            "holdingValue": 0,
            "marketCap": 0,
            "ratio": round(statistics.median(ratios), 2),
            "mean": round(math.fsum(ratios) / len(ratios), 2),  # fsum: 합산 순서 무관 결정성
            "count": len(ratios),
        })
    return avg_history


def build_average_pair(pairs_result, min_count=MIN_AVERAGE_COUNT):
    """전체 지표 pair(_average)를 구성한다. 히스토리가 없으면 None."""
    avg_history = build_average_history(pairs_result, min_count)
    annotate_history_with_trends(avg_history)
    avg_history = downsample_history(avg_history)
    if not avg_history:
        return None

    latest_avg = avg_history[-1]
    prev_avg = avg_history[-2] if len(avg_history) >= 2 else latest_avg
    avg_change = round(latest_avg["ratio"] - prev_avg["ratio"], 2)
    current = annotate_current_percentiles(
        {
            "holdingPrice": 0,
            "subsidiaryPrice": 0,
            "holdingValue": 0,
            "marketCap": 0,
            "ratio": latest_avg["ratio"],
            "mean": latest_avg.get("mean"),
            "count": latest_avg.get("count"),
            "ratioChange": avg_change,
        },
        avg_history,
    )
    return {
        "id": "_average",
        "name": "전체 중앙값",
        "holdingName": "",
        "subsidiaryName": "",
        "isAverage": True,
        "current": current,
        "history": avg_history,
    }


def check_history_regressions(previous_pairs, pairs_result, rebuild_ids, valid_from_map):
    """병합 결과가 기존 기록을 후퇴시키지 않는지 검사한다.

    반환: (errors, warnings). errors가 있으면 산출물을 쓰지 않고 실패해야 한다.
    재수집 대상(rebuild_ids)과 validFrom 의도적 절단은 경고로 완화한다.
    """
    errors = []
    warnings = []

    latest_dates = [
        p["history"][-1]["date"]
        for p in pairs_result
        if p.get("history") and not p.get("isAverage")
    ]
    global_latest = max(latest_dates) if latest_dates else ""

    for pair_data in pairs_result:
        if pair_data.get("isAverage"):
            continue
        pair_id = pair_data["id"]
        history = pair_data.get("history") or []
        if not history:
            continue

        if global_latest:
            gap_days = (parse_date_key(global_latest) - parse_date_key(history[-1]["date"])).days
            if gap_days > STALE_PAIR_WARN_DAYS:
                warnings.append(
                    f"{pair_id}: 마지막 데이터가 전체 최신일보다 {gap_days}일 뒤처짐 ({history[-1]['date']})"
                )

        max_ratio = max(e["ratio"] for e in history)
        if max_ratio > MAX_SANE_RATIO:
            warnings.append(
                f"{pair_id}: 최대 비율 {max_ratio:.0f}% > {MAX_SANE_RATIO:.0f}% — validFrom 미설정 또는 데이터 오류 후보"
            )

        previous = previous_pairs.get(pair_id)
        old_history = (previous or {}).get("history") or []
        if not old_history:
            continue

        valid_from = valid_from_map.get(pair_id) or ""
        truncated_by_valid_from = bool(valid_from and valid_from > old_history[0]["date"])
        relaxed = pair_id in rebuild_ids or truncated_by_valid_from

        if history[0]["date"] > old_history[0]["date"] and not truncated_by_valid_from:
            message = (
                f"{pair_id}: 히스토리 시작일 후퇴 {old_history[0]['date']} -> {history[0]['date']}"
            )
            (warnings if relaxed else errors).append(message)

        if len(history) < len(old_history) * 0.9:
            message = f"{pair_id}: 히스토리 포인트 급감 {len(old_history)} -> {len(history)}"
            (warnings if relaxed else errors).append(message)

    return errors, warnings


def write_atomic(path, content):
    """원자적 텍스트 쓰기 — fin-commons로 위임 (실패 시 tmp 정리, 부모 디렉터리 생성 포함)."""
    atomic_write_text(path, content)


def incremental_start(last_dates, max_lookback_days=INCREMENTAL_MAX_LOOKBACK_DAYS):
    """증분 수집 시작일. 가장 뒤처진 종목의 마지막 날짜(-5일 중첩)를 기준으로,
    영구 정지 종목이 창을 무한히 끌어내리지 않도록 최신 종목 기준 하한을 둔다.

    (기존에는 가장 앞선 종목 기준이어서 일시적으로 수집이 끊긴 종목의 공백이
    영구화되는 잠재 결함이 있었다 — 리뷰 이슈 M5)
    """
    newest = datetime.strptime(max(last_dates), "%Y-%m-%d")
    oldest = datetime.strptime(min(last_dates), "%Y-%m-%d")
    start = oldest - timedelta(days=5)
    floor = newest - timedelta(days=max_lookback_days)
    return max(start, floor)


def history_to_columnar(pair_id, history):
    """행 배열 히스토리를 컬럼형으로 변환한다 (data/history/{id}.json 포맷).

    값이 한 번도 등장하지 않는 컬럼은 생략한다. 다중 자회사는 subs[name] 아래
    price/value/ratio 배열로 펼친다 (행에 없는 날짜는 null).
    """
    cols = {"id": pair_id, "dates": [entry["date"] for entry in history]}
    for key in HISTORY_COLUMNS:
        if any(key in entry for entry in history):
            cols[key] = [entry.get(key) for entry in history]

    sub_names = []
    for entry in history:
        for sub in entry.get("subsidiaries") or []:
            if sub["name"] not in sub_names:
                sub_names.append(sub["name"])
    if sub_names:
        subs = {}
        for name in sub_names:
            price, value, ratio = [], [], []
            for entry in history:
                row = next(
                    (s for s in entry.get("subsidiaries") or [] if s["name"] == name),
                    None,
                )
                price.append(row.get("price") if row else None)
                value.append(row.get("value") if row else None)
                ratio.append(row.get("ratio") if row else None)
            subs[name] = {"price": price, "value": value, "ratio": ratio}
        cols["subs"] = subs
    return cols
