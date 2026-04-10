#!/usr/bin/env python3
"""
보유지분가치/시가총액 비율 데이터 수집 스크립트
Yahoo Finance에서 지주사/자회사 가격 데이터를 가져와 data.js를 생성한다.
다중 자회사 및 해외 종목(BRK-A 등) 환율 변환을 지원한다.
"""

import argparse
import json
import re
from collections import defaultdict, deque
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import yfinance as yf
import pandas as pd

CONFIG_PATH = Path(__file__).parent / "config.json"
with open(CONFIG_PATH, encoding="utf-8") as f:
    PAIRS = json.load(f)


OUTPUT_PATH = Path(__file__).parent / "data.js"
SEOUL_TZ = ZoneInfo("Asia/Seoul")
DAILY_RETENTION_DAYS = 730
SMA_WINDOW = 250
EMA_ALPHA = 0.1


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


def needs_multi_sub_backfill(existing, pair_config_map):
    """기존 data.js에 다중 자회사별 히스토리 분해가 없으면 전체 재생성한다."""
    for pair in existing.get("pairs", []):
        if pair.get("isAverage"):
            continue
        pair_id = pair.get("id")
        config = pair_config_map.get(pair_id)
        if not config or len(config.get("subsidiaries", [])) <= 1:
            continue
        history = pair.get("history", [])
        if any("subsidiaries" not in entry for entry in history):
            return True
    return False


def has_new_pair_ids(existing, pair_config_map):
    """Trigger a full rebuild when config.json contains brand-new pairs."""
    existing_ids = {
        pair.get("id")
        for pair in existing.get("pairs", [])
        if pair.get("id") and not pair.get("isAverage")
    }
    current_ids = set(pair_config_map.keys())
    return any(pair_id not in existing_ids for pair_id in current_ids)


def parse_date_key(date_str):
    return datetime.strptime(date_str, "%Y-%m-%d").date()


def calculate_pct_change(current_price, previous_price):
    if current_price is None or previous_price in (None, 0):
        return None
    return round((current_price - previous_price) / previous_price * 100, 2)


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


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--full', action='store_true', help='전체 최대 기간 데이터를 다시 다운로드')
    args = parser.parse_args()

    existing = None if args.full else parse_existing_data()
    pair_config_map = {pair["id"]: pair for pair in PAIRS}
    if existing and needs_multi_sub_backfill(existing, pair_config_map):
        print("다중 자회사 히스토리 확장을 위해 전체 데이터를 다시 생성합니다.")
        existing = None
    if existing and has_new_pair_ids(existing, pair_config_map):
        print("New pair ids detected in config.json. Switching to a full rebuild.")
        existing = None

    existing_history = {}
    if existing:
        for p in existing.get('pairs', []):
            if p.get('id') and p.get('history') and not p.get('isAverage'):
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

    if existing_history:
        last_dates = [h[-1]['date'] for h in existing_history.values() if h]
        latest = max(last_dates)
        start_date = datetime.strptime(latest, '%Y-%m-%d') - timedelta(days=5)
        print(f"증분 모드: {start_date.strftime('%Y-%m-%d')}부터 다운로드")
        download_kwargs["start"] = start_date.strftime("%Y-%m-%d")
        download_kwargs["end"] = end_date.strftime("%Y-%m-%d")
        print(f"Period: {start_date.strftime('%Y-%m-%d')} ~ {end_date.strftime('%Y-%m-%d')}")
    else:
        print("전체 모드: Yahoo Finance에서 가능한 최대 기간을 다운로드합니다.")
        download_kwargs["period"] = "max"

    print(f"Downloading data for {len(all_tickers)} tickers...")

    data = yf.download(
        all_tickers,
        **download_kwargs,
    )

    if data.empty:
        if existing:
            print("새 데이터 없음. 기존 data.js를 유지합니다.")
            return
        print("ERROR: 데이터를 다운로드하지 못했습니다.")
        return

    close = data["Close"]

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

        holding_close = close[ht].dropna()
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
            s = close[st].dropna()
            sub_series[st] = s
            common_dates = common_dates.intersection(s.index)

        if skip:
            continue

        # 해외 종목이 있으면 환율 데이터와도 교차
        has_foreign = any(not is_korean(sub["ticker"]) for sub in subs)
        if has_foreign and fx_rate is not None:
            common_dates = common_dates.intersection(fx_rate.index)

        if len(common_dates) == 0:
            print(f"  WARNING: No overlapping dates for {pair['name']}, skipping.")
            continue

        h = holding_close.loc[common_dates]
        total_shares = pair["holdingTotalShares"]
        treasury_shares = pair["holdingTreasuryShares"]
        adjusted_shares = total_shares - treasury_shares

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

        trend_recompute_idx = 0

        # 기존 히스토리와 병합
        if pair["id"] in existing_history:
            old_hist = existing_history[pair["id"]]
            new_dates = {e["date"] for e in history}
            merged = [e for e in old_hist if e["date"] not in new_dates]
            merged.extend(history)
            merged.sort(key=lambda e: e["date"])
            history = merged
            if new_dates:
                first_changed_date = min(new_dates)
                changed_idx = next(
                    (idx for idx, entry in enumerate(history) if entry["date"] >= first_changed_date),
                    0,
                )
                trend_recompute_idx = max(0, changed_idx - (SMA_WINDOW - 1))

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

    # 일별 전체 평균 비율 계산
    daily_ratios = defaultdict(list)
    for pair_data in pairs_result:
        for h in pair_data["history"]:
            daily_ratios[h["date"]].append(h["ratio"])

    avg_history = []
    for date in sorted(daily_ratios.keys()):
        ratios = daily_ratios[date]
        avg_history.append({
            "date": date,
            "holdingPrice": 0,
            "subsidiaryPrice": 0,
            "holdingValue": 0,
            "marketCap": 0,
            "ratio": round(sum(ratios) / len(ratios), 2),
        })

    annotate_history_with_trends(avg_history)
    avg_history = downsample_history(avg_history)

    if avg_history:
        latest_avg = avg_history[-1]
        prev_avg = avg_history[-2] if len(avg_history) >= 2 else latest_avg
        avg_change = round(latest_avg["ratio"] - prev_avg["ratio"], 2)
        avg_pair = {
            "id": "_average",
            "name": "전체 평균",
            "holdingName": "",
            "subsidiaryName": "",
            "isAverage": True,
            "current": {
                "holdingPrice": 0,
                "subsidiaryPrice": 0,
                "holdingValue": 0,
                "marketCap": 0,
                "ratio": latest_avg["ratio"],
                "ratioChange": avg_change,
            },
            "history": avg_history,
        }
        print(
            f"  전체 평균: {len(avg_history)} days, "
            f"current ratio {latest_avg['ratio']:.2f}% "
            f"({'↑' if avg_change > 0 else '↓'}{abs(avg_change):.2f}%p)"
        )

    if avg_history:
        pairs_result.append(avg_pair)
    pairs_result.sort(key=lambda p: p["current"]["ratio"], reverse=True)

    stock_data = {
        "lastUpdated": now_local.strftime("%Y-%m-%d %H:%M:%S"),
        "pairs": pairs_result,
    }

    js_content = "const STOCK_DATA = " + json.dumps(stock_data, ensure_ascii=False, indent=2) + ";\n"

    output_path = OUTPUT_PATH
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(js_content)

    print(f"\nGenerated {output_path} ({len(pairs_result)} pairs, {len(js_content)} bytes)")


if __name__ == "__main__":
    main()
