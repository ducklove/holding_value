"""fetch_data.py의 병합·평균·품질 가드 회귀 테스트."""

import fetch_data


def entry(date, ratio, **extra):
    base = {
        "date": date,
        "holdingPrice": 1000,
        "subsidiaryPrice": 500,
        "holdingValue": 10.0,
        "marketCap": 20.0,
        "ratio": ratio,
    }
    base.update(extra)
    return base


def pair(pair_id, history):
    return {"id": pair_id, "name": pair_id, "history": history}


# --- merge_pair_history ---

def test_merge_keeps_old_and_replaces_overlap():
    old = [entry("2026-01-02", 50.0), entry("2026-01-03", 51.0)]
    new = [entry("2026-01-03", 99.0), entry("2026-01-06", 52.0)]
    merged, _ = fetch_data.merge_pair_history(new, old)
    assert [e["date"] for e in merged] == ["2026-01-02", "2026-01-03", "2026-01-06"]
    assert merged[1]["ratio"] == 99.0  # 중첩 날짜는 새 값이 우선


def test_merge_without_old_returns_new():
    new = [entry("2026-01-02", 50.0)]
    merged, idx = fetch_data.merge_pair_history(new, None)
    assert merged == new
    assert idx == 0


def test_merge_valid_from_drops_both_sides():
    old = [entry("2019-12-30", 40.0), entry("2026-01-02", 50.0)]
    new = [entry("2019-12-31", 41.0), entry("2026-01-03", 51.0)]
    merged, _ = fetch_data.merge_pair_history(new, old, valid_from="2020-01-01")
    assert [e["date"] for e in merged] == ["2026-01-02", "2026-01-03"]


def test_merge_trend_recompute_idx_floors_at_zero():
    old = [entry(f"2026-01-{d:02d}", 50.0) for d in range(2, 6)]
    new = [entry("2026-01-05", 55.0)]
    merged, idx = fetch_data.merge_pair_history(new, old)
    assert idx == 0  # 변경 지점 - (SMA_WINDOW - 1) 이 음수면 0


# --- build_average_history / build_average_pair ---

def test_average_uses_median_and_count_filter():
    pairs_result = [
        pair("a", [entry("2026-01-02", 10.0)]),
        pair("b", [entry("2026-01-02", 20.0)]),
        pair("c", [entry("2026-01-02", 4000.0)]),  # 이상치가 중앙값을 지배하지 못함
        pair("d", [entry("2026-01-03", 30.0)]),     # 단독 날짜 → min_count 미달
    ]
    history = fetch_data.build_average_history(pairs_result, min_count=3)
    assert len(history) == 1
    assert history[0]["date"] == "2026-01-02"
    assert history[0]["ratio"] == 20.0  # median(10, 20, 4000)
    assert history[0]["mean"] == round((10 + 20 + 4000) / 3, 2)
    assert history[0]["count"] == 3


def test_average_pair_metadata():
    pairs_result = [
        pair("a", [entry("2026-01-02", 10.0), entry("2026-01-03", 12.0)]),
        pair("b", [entry("2026-01-02", 20.0), entry("2026-01-03", 24.0)]),
    ]
    avg_pair = fetch_data.build_average_pair(pairs_result, min_count=2)
    assert avg_pair["id"] == "_average"
    assert avg_pair["isAverage"] is True
    assert avg_pair["current"]["ratio"] == 18.0       # median(12, 24)
    assert avg_pair["current"]["count"] == 2
    assert avg_pair["current"]["ratioChange"] == 3.0  # 18.0 - 15.0


def test_average_pair_empty_when_below_min_count():
    pairs_result = [pair("a", [entry("2026-01-02", 10.0)])]
    assert fetch_data.build_average_pair(pairs_result, min_count=2) is None


# --- check_history_regressions ---

def long_history(start_day, count, ratio=50.0):
    return [entry(f"2026-01-{d:02d}", ratio) for d in range(start_day, start_day + count)]


def test_guard_flags_start_regression_as_error():
    previous = {"a": pair("a", [entry("2025-12-01", 50.0), entry("2026-01-02", 50.0)])}
    result = [pair("a", [entry("2026-01-02", 50.0)])]
    errors, _ = fetch_data.check_history_regressions(previous, result, set(), {})
    assert any("시작일 후퇴" in e for e in errors)


def test_guard_relaxes_rebuild_pairs_to_warning():
    previous = {"a": pair("a", [entry("2025-12-01", 50.0), entry("2026-01-02", 50.0)])}
    result = [pair("a", [entry("2026-01-02", 50.0)])]
    errors, warnings = fetch_data.check_history_regressions(previous, result, {"a"}, {})
    assert not errors
    assert any("시작일 후퇴" in w for w in warnings)


def test_guard_allows_valid_from_truncation():
    previous = {"a": pair("a", [entry("2025-12-01", 50.0), entry("2026-01-02", 50.0)])}
    result = [pair("a", [entry("2026-01-02", 50.0)])]
    errors, _ = fetch_data.check_history_regressions(
        previous, result, set(), {"a": "2026-01-01"}
    )
    assert not any("시작일 후퇴" in e for e in errors)


def test_guard_flags_point_shrink_as_error():
    previous = {"a": pair("a", long_history(2, 20))}
    result = [pair("a", long_history(2, 10))]
    errors, _ = fetch_data.check_history_regressions(previous, result, set(), {})
    assert any("포인트 급감" in e for e in errors)


def test_guard_warns_on_insane_ratio_and_stale_pair():
    fresh = pair("fresh", [entry("2026-01-02", 50.0), entry("2026-03-02", 50.0)])
    insane = pair("insane", [entry("2026-03-02", fetch_data.MAX_SANE_RATIO + 1)])
    stale = pair("stale", [entry("2026-01-02", 50.0)])
    errors, warnings = fetch_data.check_history_regressions({}, [fresh, insane, stale], set(), {})
    assert not errors
    assert any("insane" in w and "validFrom" in w for w in warnings)
    assert any("stale" in w and "뒤처짐" in w for w in warnings)


# --- incremental_start (M5) ---

def test_incremental_start_uses_most_lagging_pair():
    start = fetch_data.incremental_start(["2026-06-09", "2026-05-25", "2026-06-09"])
    assert start.strftime("%Y-%m-%d") == "2026-05-20"  # 가장 뒤처진 날짜 - 5일


def test_incremental_start_caps_lookback_for_dead_pairs():
    start = fetch_data.incremental_start(["2026-06-09", "2025-01-01"])
    assert start.strftime("%Y-%m-%d") == "2026-03-11"  # 최신 - 90일 하한


def test_incremental_start_all_fresh():
    start = fetch_data.incremental_start(["2026-06-09", "2026-06-09"])
    assert start.strftime("%Y-%m-%d") == "2026-06-04"


# --- history_to_columnar 왕복 (JS rowsFromColumnar의 역변환과 동치) ---

def rows_from_columnar(columnar):
    """index.html rowsFromColumnar와 동일한 복원 규칙 (테스트용 역변환)."""
    dates = columnar.get("dates") or []
    sub_names = list((columnar.get("subs") or {}).keys())
    rows = []
    for i in range(len(dates)):
        row = {"date": dates[i]}
        for key in fetch_data.HISTORY_COLUMNS:
            if key in columnar:
                row[key] = columnar[key][i]
        if sub_names:
            subs = []
            for name in sub_names:
                series = columnar["subs"][name]
                if series["price"][i] is None and series["value"][i] is None and series["ratio"][i] is None:
                    continue
                subs.append({
                    "name": name,
                    "price": series["price"][i],
                    "value": series["value"][i],
                    "ratio": series["ratio"][i],
                })
            if subs:
                row["subsidiaries"] = subs
        rows.append(row)
    return rows


def strip_nones(rows):
    """null과 미존재 키는 렌더 코드에서 동치이므로 비교 전 제거한다."""
    return [{k: v for k, v in row.items() if v is not None} for row in rows]


def test_columnar_roundtrip_single_subsidiary():
    history = [
        dict(entry("2026-01-02", 50.0), sma250=None, ema01=49.5),
        dict(entry("2026-01-03", 51.0), sma250=50.5, ema01=49.7),
    ]
    columnar = fetch_data.history_to_columnar("demo", history)
    assert columnar["id"] == "demo"
    assert "mean" not in columnar  # 등장하지 않는 컬럼은 생략
    assert strip_nones(rows_from_columnar(columnar)) == strip_nones(history)


def test_columnar_roundtrip_multi_subsidiary_with_gaps():
    history = [
        {
            "date": "2026-01-02", "holdingPrice": 1000, "subsidiaryPrice": 0,
            "holdingValue": 10.0, "marketCap": 20.0, "ratio": 50.0,
            "subsidiaries": [
                {"name": "A", "price": 100, "value": 5.0, "ratio": 25.0},
                {"name": "B", "price": 200, "value": 5.0, "ratio": 25.0},
            ],
        },
        {
            "date": "2026-01-03", "holdingPrice": 1010, "subsidiaryPrice": 0,
            "holdingValue": 11.0, "marketCap": 20.0, "ratio": 55.0,
            "subsidiaries": [
                {"name": "A", "price": 101, "value": 11.0, "ratio": 55.0},
            ],
        },
    ]
    columnar = fetch_data.history_to_columnar("multi", history)
    assert list(columnar["subs"].keys()) == ["A", "B"]
    assert columnar["subs"]["B"]["price"] == [200, None]
    assert strip_nones(rows_from_columnar(columnar)) == strip_nones(history)


def test_columnar_roundtrip_average_with_mean_count():
    history = [
        {"date": "2026-01-02", "holdingPrice": 0, "subsidiaryPrice": 0, "holdingValue": 0,
         "marketCap": 0, "ratio": 50.0, "mean": 60.0, "count": 30, "sma250": None, "ema01": 50.0},
    ]
    columnar = fetch_data.history_to_columnar("_average", history)
    assert columnar["mean"] == [60.0]
    assert columnar["count"] == [30]
    assert strip_nones(rows_from_columnar(columnar)) == strip_nones(history)


# --- downsample_history ---

def test_downsample_collapses_old_region_to_weekly():
    history = (
        # 2020년 1월: 보존 기간(730일) 밖 — 주 단위 마지막 값만 유지
        [entry("2020-01-06", 1.0), entry("2020-01-07", 2.0), entry("2020-01-08", 3.0),
         entry("2020-01-13", 4.0), entry("2020-01-14", 5.0)]
        # 최근 구간 — 그대로 유지
        + [entry("2026-01-05", 10.0), entry("2026-01-06", 11.0)]
    )
    result = fetch_data.downsample_history(history)
    dates = [e["date"] for e in result]
    assert dates == ["2020-01-08", "2020-01-14", "2026-01-05", "2026-01-06"]
