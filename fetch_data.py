#!/usr/bin/env python3
"""
보유지분가치/시가총액 비율 데이터 수집 스크립트
Yahoo Finance에서 지주사/자회사 가격 데이터를 가져와 data.js를 생성한다.
다중 자회사 및 해외 종목(BRK-A 등) 환율 변환을 지원한다.
"""

import argparse
import json
import re
import sys
import time
from datetime import datetime
from pathlib import Path

import yfinance as yf
import pandas as pd

from fin_commons.timeutil import KST

from pipeline.core import (
    DAILY_RETENTION_DAYS,
    EMA_ALPHA,
    HISTORY_COLUMNS,
    INCREMENTAL_MAX_LOOKBACK_DAYS,
    MAX_SANE_RATIO,
    MIN_AVERAGE_COUNT,
    PCTILE_MIN_POINTS,
    SMA_WINDOW,
    STALE_PAIR_WARN_DAYS,
    annotate_current_percentiles,
    annotate_history_with_trends,
    build_average_history,
    build_average_pair,
    calculate_pct_change,
    check_history_regressions,
    downsample_history,
    find_multi_sub_backfill_ids,
    find_new_pair_ids,
    get_holding_adjusted_shares,
    history_to_columnar,
    incremental_start,
    merge_pair_history,
    parse_date_key,
    trailing_percentile,
    write_atomic,
)
from price_api import download_close_frame as download_internal_close_frame

# pipeline.core 재수출 — 기존 fetch_data.X 참조(테스트 포함)가 그대로 동작한다.
__all__ = [
    "DAILY_RETENTION_DAYS",
    "EMA_ALPHA",
    "HISTORY_COLUMNS",
    "INCREMENTAL_MAX_LOOKBACK_DAYS",
    "MAX_SANE_RATIO",
    "MIN_AVERAGE_COUNT",
    "PCTILE_MIN_POINTS",
    "SMA_WINDOW",
    "STALE_PAIR_WARN_DAYS",
    "annotate_current_percentiles",
    "annotate_history_with_trends",
    "build_average_history",
    "build_average_pair",
    "calculate_pct_change",
    "check_history_regressions",
    "downsample_history",
    "find_multi_sub_backfill_ids",
    "find_new_pair_ids",
    "get_holding_adjusted_shares",
    "history_to_columnar",
    "incremental_start",
    "merge_pair_history",
    "parse_date_key",
    "trailing_percentile",
    "write_atomic",
]

CONFIG_PATH = Path(__file__).parent / "config.json"
with open(CONFIG_PATH, encoding="utf-8") as f:
    PAIRS = json.load(f)


OUTPUT_PATH = Path(__file__).parent / "data.js"
DATA_DIR = Path(__file__).parent / "data"
HISTORY_DIR = DATA_DIR / "history"
SEOUL_TZ = KST  # KST는 DST 없음 — 고정 오프셋으로 충분 (fin-commons)


def parse_existing_data():
    """기존 data.js를 파싱하여 데이터를 반환한다."""
    if not OUTPUT_PATH.exists():
        return None
    text = OUTPUT_PATH.read_text(encoding="utf-8")
    json_str = re.sub(r'^const STOCK_DATA\s*=\s*', '', text)
    json_str = re.sub(r';\s*$', '', json_str)
    try:
        return json.loads(json_str)
    except json.JSONDecodeError:
        return None


def is_korean(ticker):
    return ticker.endswith(".KS") or ticker.endswith(".KQ")


def write_split_outputs(stock_data):
    """분할 산출물 생성: data/summary.json(메타+현재가) + data/history/{id}.json.

    프런트는 summary로 첫 화면을 그리고 선택 종목 히스토리만 지연 로드한다.
    data.js는 과도기 폴백으로 병행 생성된다.
    """
    HISTORY_DIR.mkdir(parents=True, exist_ok=True)
    summary_pairs = []
    valid_stems = set()
    for pair_data in stock_data["pairs"]:
        pair_id = pair_data.get("id") or ""
        if not re.fullmatch(r"[A-Za-z0-9_-]+", pair_id):
            print(f"WARNING: 분할 산출물에서 제외 — 허용되지 않는 pair id: {pair_id!r}")
            continue
        summary_pairs.append({k: v for k, v in pair_data.items() if k != "history"})
        valid_stems.add(pair_id)
        columnar = history_to_columnar(pair_id, pair_data.get("history") or [])
        write_atomic(
            HISTORY_DIR / f"{pair_id}.json",
            json.dumps(columnar, ensure_ascii=False, separators=(",", ":")) + "\n",
        )

    summary = {"lastUpdated": stock_data["lastUpdated"], "pairs": summary_pairs}
    write_atomic(
        DATA_DIR / "summary.json",
        json.dumps(summary, ensure_ascii=False, separators=(",", ":")) + "\n",
    )

    # 삭제된 종목의 히스토리 파일 정리
    for stale in HISTORY_DIR.glob("*.json"):
        if stale.stem not in valid_stems:
            stale.unlink()


def normalize_close_frame(data, tickers):
    if data.empty or "Close" not in data:
        return pd.DataFrame()

    close = data["Close"]
    if getattr(close, "ndim", 1) == 1:
        close = close.to_frame(name=tickers[0])
    return close


def alternate_korean_ticker(ticker):
    if ticker.endswith(".KS"):
        return ticker[:-3] + ".KQ"
    if ticker.endswith(".KQ"):
        return ticker[:-3] + ".KS"
    return None


def merge_close_frames(base, frames):
    if not frames:
        return base

    extra = pd.concat(frames, axis=1, sort=True)
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

    merged = pd.concat([base, extra], axis=1, sort=True)
    return merged.loc[:, ~merged.columns.duplicated()]


def download_close_prices(tickers, download_kwargs, chunk_size=20):
    kwargs = dict(download_kwargs)
    kwargs["progress"] = False

    since = kwargs.get("start")
    until = kwargs.get("end")
    internal_close, internal_loaded = download_internal_close_frame(tickers, since, until)
    close = merge_close_frames(pd.DataFrame(), [internal_close] if not internal_close.empty else [])
    if internal_loaded:
        print(f"Loaded {len(internal_loaded)} tickers from internal price API.")

    yfinance_targets = [
        ticker for ticker in tickers
        if ticker not in close.columns or close[ticker].dropna().empty
    ]
    frames = []
    for start in range(0, len(yfinance_targets), chunk_size):
        chunk = yfinance_targets[start:start + chunk_size]
        data = yf.download(chunk, **kwargs)
        chunk_close = normalize_close_frame(data, chunk)
        if not chunk_close.empty:
            frames.append(chunk_close)

    close = merge_close_frames(close, frames)

    missing = [
        ticker for ticker in tickers
        if ticker not in close.columns or close[ticker].dropna().empty
    ]
    if missing:
        print(f"Retrying {len(missing)} tickers individually...")

    for attempt in range(6):
        retry_targets = [
            ticker for ticker in tickers
            if ticker not in close.columns or close[ticker].dropna().empty
        ]
        if not retry_targets:
            break
        if attempt > 0:
            print(f"Retry pass {attempt + 1}: {len(retry_targets)} tickers...")
            time.sleep(2)

        retry_frames = []
        for ticker in retry_targets:
            time.sleep(0.2)
            data = yf.download(ticker, threads=False, **kwargs)
            retry_close = normalize_close_frame(data, [ticker])
            if not retry_close.empty and ticker in retry_close.columns and not retry_close[ticker].dropna().empty:
                retry_frames.append(retry_close[[ticker]])

        if retry_frames:
            close = merge_close_frames(close, retry_frames)

    fallback_targets = [
        ticker for ticker in tickers
        if ticker not in close.columns or close[ticker].dropna().empty
    ]
    if fallback_targets:
        print(f"Trying alternate Korean suffix for {len(fallback_targets)} tickers...")

    fallback_frames = []
    for ticker in fallback_targets:
        alternate = alternate_korean_ticker(ticker)
        if not alternate:
            continue

        for attempt in range(3):
            time.sleep(0.2 if attempt == 0 else 1)
            data = yf.download(alternate, threads=False, **kwargs)
            fallback_close = normalize_close_frame(data, [alternate])
            if (
                not fallback_close.empty
                and alternate in fallback_close.columns
                and not fallback_close[alternate].dropna().empty
            ):
                print(f"  {ticker}: using {alternate} as Yahoo fallback")
                fallback_frames.append(
                    fallback_close[[alternate]].rename(columns={alternate: ticker})
                )
                break

    if fallback_frames:
        close = merge_close_frames(close, fallback_frames)

    unresolved = [
        ticker for ticker in tickers
        if ticker not in close.columns or close[ticker].dropna().empty
    ]
    if unresolved:
        print("WARNING: Unresolved tickers after retry: " + ", ".join(unresolved))

    return close


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--full', action='store_true', help='전체 최대 기간 데이터를 다시 다운로드')
    args = parser.parse_args()

    # baseline은 --full 여부와 무관하게 품질 가드의 비교 기준으로 사용한다.
    baseline = parse_existing_data()
    existing = None if args.full else baseline
    pair_config_map = {pair["id"]: pair for pair in PAIRS}

    rebuild_ids = set()
    if args.full:
        rebuild_ids = set(pair_config_map)
    elif existing:
        new_ids = find_new_pair_ids(existing, pair_config_map)
        backfill_ids = find_multi_sub_backfill_ids(existing, pair_config_map)
        rebuild_ids = new_ids | backfill_ids
        if rebuild_ids:
            print(
                f"부분 재수집 대상 {len(rebuild_ids)}개: {', '.join(sorted(rebuild_ids))}"
                " (다른 종목의 누적 히스토리는 유지)"
            )

    previous_pairs = {}
    if baseline:
        previous_pairs = {
            p["id"]: p
            for p in baseline.get("pairs", [])
            if p.get("id") and not p.get("isAverage")
        }

    existing_history = {}
    if existing:
        for p in existing.get('pairs', []):
            if (
                p.get('id')
                and p.get('history')
                and not p.get('isAverage')
                and p['id'] not in rebuild_ids
            ):
                existing_history[p['id']] = p['history']

    # 모든 티커 수집 (중복 제거)
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

    now_local = datetime.now(SEOUL_TZ)
    end_date = now_local
    download_kwargs = {
        "auto_adjust": True,
        "progress": True,
    }

    incr_cutoff = None
    if existing_history:
        last_dates = [h[-1]['date'] for h in existing_history.values() if h]
        start_date = incremental_start(last_dates)
        incr_cutoff = pd.Timestamp(start_date.date())
        print(f"증분 모드: {start_date.strftime('%Y-%m-%d')}부터 다운로드")
        download_kwargs["start"] = start_date.strftime("%Y-%m-%d")
        download_kwargs["end"] = end_date.strftime("%Y-%m-%d")
        print(f"Period: {start_date.strftime('%Y-%m-%d')} ~ {end_date.strftime('%Y-%m-%d')}")
    else:
        print("전체 모드: Yahoo Finance에서 가능한 최대 기간을 다운로드합니다.")
        download_kwargs["start"] = "2000-01-01"
        download_kwargs["end"] = end_date.strftime("%Y-%m-%d")

    print(f"Downloading data for {len(all_tickers)} tickers...")

    close = download_close_prices(all_tickers, download_kwargs)

    # 재수집 대상 종목만 전체 기간을 별도로 받아 합류시킨다 (다른 종목은 증분 유지).
    if rebuild_ids and existing_history:
        rebuild_tickers = []
        rebuild_needs_fx = False
        for pair in PAIRS:
            if pair["id"] not in rebuild_ids:
                continue
            rebuild_tickers.append(pair["holdingTicker"])
            for sub in pair["subsidiaries"]:
                rebuild_tickers.append(sub["ticker"])
                if not is_korean(sub["ticker"]):
                    rebuild_needs_fx = True
        rebuild_tickers = list(dict.fromkeys(rebuild_tickers))
        if rebuild_needs_fx:
            rebuild_tickers.append("USDKRW=X")

        if rebuild_tickers:
            full_kwargs = dict(download_kwargs)
            full_kwargs["start"] = "2000-01-01"
            print(f"재수집 대상 {len(rebuild_tickers)}개 티커 전체 기간 다운로드...")
            full_close = download_close_prices(rebuild_tickers, full_kwargs)
            if not full_close.empty:
                overlap = [c for c in full_close.columns if c in close.columns]
                if overlap:
                    close = close.drop(columns=overlap)
                close = merge_close_frames(close, [full_close])

    if close.empty:
        if existing:
            print("새 데이터 없음. 기존 data.js를 유지합니다.")
            return
        print("ERROR: 데이터를 다운로드하지 못했습니다.")
        return

    fx_rate = None
    if needs_fx and "USDKRW=X" in close.columns:
        fx_rate = close["USDKRW=X"].dropna()

    pairs_result = []

    for pair in PAIRS:
        ht = pair["holdingTicker"]
        subs = pair["subsidiaries"]

        if ht not in close.columns:
            print(f"  WARNING: Holding ticker {ht} not found, skipping {pair['name']}")
            continue

        # 재수집 대상이 아닌 기존 종목은 증분 구간만 사용한다.
        # (재수집 종목과 티커를 공유하는 경우 전체 기간 프레임이 들어와도
        #  기존 누적 히스토리를 덮어쓰지 않게 하는 안전장치)
        is_rebuild = pair["id"] in rebuild_ids or pair["id"] not in existing_history

        def window_series(series):
            if incr_cutoff is None or is_rebuild:
                return series
            return series[series.index >= incr_cutoff]

        holding_close = window_series(close[ht].dropna())
        common_dates = holding_close.index

        # 각 자회사의 가격 시리즈 수집 및 공통 날짜 계산
        sub_series = {}
        skip = False
        for sub in subs:
            st = sub["ticker"]
            if st not in close.columns:
                print(f"  WARNING: Subsidiary ticker {st} not found, skipping {pair['name']}")
                skip = True
                break
            s = window_series(close[st].dropna())
            sub_series[st] = s
            common_dates = common_dates.intersection(s.index)

        if skip:
            continue

        # validFrom(현 지분 구조 성립일) 이전 구간은 생성하지 않는다 — 현재
        # 지분·주식수의 소급 적용으로 생기는 허구 비율(C2) 방지.
        valid_from = (pair.get("validFrom") or "").strip()
        if valid_from:
            common_dates = common_dates[common_dates >= pd.Timestamp(valid_from)]

        # 해외 종목이 있으면 환율 데이터와도 교차
        has_foreign = any(not is_korean(sub["ticker"]) for sub in subs)
        if has_foreign and fx_rate is not None:
            common_dates = common_dates.intersection(fx_rate.index)

        if len(common_dates) == 0:
            print(f"  WARNING: No overlapping dates for {pair['name']}, skipping.")
            continue

        h = holding_close.loc[common_dates]
        adjusted_shares = get_holding_adjusted_shares(pair)

        # 보유지분가치 합산 (모든 자회사)
        holding_value_series = pd.Series(0.0, index=common_dates)
        sub_value_series = {}
        sub_ratio_series = {}
        for sub in subs:
            st = sub["ticker"]
            s = sub_series[st].loc[common_dates]
            if not is_korean(st) and fx_rate is not None:
                s = s * fx_rate.loc[common_dates]
            value_series = sub["sharesHeld"] * s
            sub_value_series[st] = value_series
            holding_value_series = holding_value_series + value_series

        market_cap_series = adjusted_shares * h
        ratio_series = holding_value_series / market_cap_series * 100
        for sub in subs:
            st = sub["ticker"]
            sub_ratio_series[st] = sub_value_series[st] / market_cap_series * 100

        # 자회사 이름 구성
        if len(subs) == 1:
            subsidiary_name = subs[0]["name"]
        else:
            subsidiary_name = "+".join(sub["name"] for sub in subs)

        # 히스토리 구성
        history = []
        for date in common_dates:
            holding_price = round(float(h.loc[date]), 0)
            hv = round(float(holding_value_series.loc[date]) / 1e8, 1)
            mc = round(float(market_cap_series.loc[date]) / 1e8, 1)
            r = round(float(ratio_series.loc[date]), 2)

            entry = {
                "date": date.strftime("%Y-%m-%d"),
                "holdingPrice": holding_price,
                "subsidiaryPrice": 0,
                "holdingValue": hv,
                "marketCap": mc,
                "ratio": r,
            }

            if len(subs) == 1:
                sp = float(sub_series[subs[0]["ticker"]].loc[date])
                if not is_korean(subs[0]["ticker"]) and fx_rate is not None:
                    sp *= float(fx_rate.loc[date])
                entry["subsidiaryPrice"] = round(sp, 0)
            else:
                entry["subsidiaries"] = []
                for sub in subs:
                    st = sub["ticker"]
                    sp = float(sub_series[st].loc[date])
                    if not is_korean(st) and fx_rate is not None:
                        sp *= float(fx_rate.loc[date])
                    sv = float(sub_value_series[st].loc[date]) / 1e8
                    sr = float(sub_ratio_series[st].loc[date])
                    entry["subsidiaries"].append({
                        "name": sub["name"],
                        "price": round(sp, 0),
                        "value": round(sv, 1),
                        "ratio": round(sr, 2),
                    })

            history.append(entry)

        # 기존 히스토리와 병합 (validFrom 이전 구간은 기존 데이터에서도 제거)
        history, trend_recompute_idx = merge_pair_history(
            history, existing_history.get(pair["id"]), valid_from
        )

        if not history:
            continue

        annotate_history_with_trends(history, trend_recompute_idx)
        history = downsample_history(history)

        latest = history[-1]
        prev = history[-2] if len(history) >= 2 else latest
        ratio_change = round(latest["ratio"] - prev["ratio"], 2)
        holding_change = calculate_pct_change(latest["holdingPrice"], prev["holdingPrice"])

        current = {
            "holdingPrice": latest["holdingPrice"],
            "holdingChange": holding_change,
            "subsidiaryPrice": latest.get("subsidiaryPrice", 0),
            "holdingValue": latest["holdingValue"],
            "marketCap": latest["marketCap"],
            "ratio": latest["ratio"],
            "ratioChange": ratio_change,
        }

        if len(subs) == 1:
            current["subsidiaryChange"] = calculate_pct_change(
                latest.get("subsidiaryPrice"),
                prev.get("subsidiaryPrice"),
            )

        # 다중 자회사 상세 정보
        if len(subs) > 1:
            prev_sub_map = {sub["name"]: sub for sub in prev.get("subsidiaries", [])}
            current_subs = []
            for sub in latest.get("subsidiaries", []):
                previous_sub = prev_sub_map.get(sub["name"], {})
                current_subs.append({
                    "name": sub["name"],
                    "price": sub["price"],
                    "change": calculate_pct_change(sub["price"], previous_sub.get("price")),
                    "value": sub["value"],
                    "ratio": sub["ratio"],
                })
            current["subsidiaries"] = current_subs

        annotate_current_percentiles(current, history)

        pair_data = {
            "id": pair["id"],
            "name": pair["name"],
            "holdingName": pair["holdingName"],
            "holdingTicker": pair["holdingTicker"],
            "subsidiaryName": subsidiary_name,
            "current": current,
            "history": history,
        }
        pairs_result.append(pair_data)

        print(
            f"  {pair['name']}: {len(history)} days, "
            f"current ratio {latest['ratio']:.2f}% "
            f"({'↑' if ratio_change > 0 else '↓'}{abs(ratio_change):.2f}%p)"
        )

    # 새 데이터가 없는 기존 종목 유지
    if existing:
        processed_ids = {p['id'] for p in pairs_result}
        for p in existing.get('pairs', []):
            if p.get('id') and p['id'] not in processed_ids and not p.get('isAverage'):
                pairs_result.append(p)
                print(f"  {p['name']}: 기존 데이터 유지 ({len(p.get('history', []))} days)")

    # 전체 지표(중앙값, 최소 구성 종목 수 필터) 계산
    avg_pair = build_average_pair(pairs_result)
    if avg_pair:
        pairs_result.append(avg_pair)
        avg_current = avg_pair["current"]
        print(
            f"  전체 중앙값: {len(avg_pair['history'])} days, "
            f"current ratio {avg_current['ratio']:.2f}% "
            f"({'↑' if avg_current['ratioChange'] > 0 else '↓'}{abs(avg_current['ratioChange']):.2f}%p, "
            f"구성 {avg_current.get('count')}종목)"
        )
    pairs_result.sort(key=lambda p: p["current"]["ratio"], reverse=True)

    # 데이터 품질 가드 — 기존 기록을 후퇴시키는 결과면 쓰지 않고 실패한다.
    valid_from_map = {p["id"]: (p.get("validFrom") or "").strip() for p in PAIRS}
    errors, warnings = check_history_regressions(
        previous_pairs, pairs_result, rebuild_ids, valid_from_map
    )
    for warning in warnings:
        print(f"WARNING: {warning}")
    if errors:
        for error in errors:
            print(f"ERROR: {error}", file=sys.stderr)
        print("데이터 품질 가드 실패 — data.js를 변경하지 않고 종료합니다.", file=sys.stderr)
        sys.exit(1)

    stock_data = {
        "lastUpdated": now_local.strftime("%Y-%m-%d %H:%M:%S"),
        "pairs": pairs_result,
    }

    js_content = (
        "const STOCK_DATA = "
        + json.dumps(stock_data, ensure_ascii=False, separators=(",", ":"))
        + ";\n"
    )

    write_atomic(OUTPUT_PATH, js_content)
    write_split_outputs(stock_data)

    print(f"\nGenerated {OUTPUT_PATH} ({len(pairs_result)} pairs, {len(js_content)} bytes)")
    print(f"Generated {DATA_DIR}/summary.json + {HISTORY_DIR}/*.json ({len(pairs_result)} files)")


if __name__ == "__main__":
    main()
