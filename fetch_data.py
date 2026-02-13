#!/usr/bin/env python3
"""
보유지분가치/시가총액 비율 데이터 수집 스크립트
Yahoo Finance에서 지주사/자회사 가격 데이터를 가져와 data.js를 생성한다.
다중 자회사 및 해외 종목(BRK-A 등) 환율 변환을 지원한다.
"""

import json
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

import yfinance as yf
import pandas as pd

CONFIG_PATH = Path(__file__).parent / "config.json"
with open(CONFIG_PATH, encoding="utf-8") as f:
    PAIRS = json.load(f)


def is_korean(ticker):
    return ticker.endswith(".KS") or ticker.endswith(".KQ")


def main():
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

    end_date = datetime.now()
    start_date = end_date - timedelta(days=3 * 365 + 30)

    print(f"Downloading data for {len(all_tickers)} tickers...")
    print(f"Period: {start_date.strftime('%Y-%m-%d')} ~ {end_date.strftime('%Y-%m-%d')}")

    data = yf.download(
        all_tickers,
        start=start_date.strftime("%Y-%m-%d"),
        end=end_date.strftime("%Y-%m-%d"),
        auto_adjust=True,
        progress=True,
    )

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
        for sub in subs:
            st = sub["ticker"]
            s = sub_series[st].loc[common_dates]
            if not is_korean(st) and fx_rate is not None:
                s = s * fx_rate.loc[common_dates]
            holding_value_series = holding_value_series + sub["sharesHeld"] * s

        market_cap_series = adjusted_shares * h
        ratio_series = holding_value_series / market_cap_series * 100

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

            history.append(entry)

        latest = history[-1]
        prev = history[-2] if len(history) >= 2 else latest
        ratio_change = round(latest["ratio"] - prev["ratio"], 2)

        current = {
            "holdingPrice": latest["holdingPrice"],
            "subsidiaryPrice": latest.get("subsidiaryPrice", 0),
            "holdingValue": latest["holdingValue"],
            "marketCap": latest["marketCap"],
            "ratio": latest["ratio"],
            "ratioChange": ratio_change,
        }

        # 다중 자회사 상세 정보
        if len(subs) > 1:
            current_subs = []
            last_date = common_dates[-1]
            for sub in subs:
                st = sub["ticker"]
                sp = float(sub_series[st].loc[last_date])
                if not is_korean(st) and fx_rate is not None:
                    sp *= float(fx_rate.loc[last_date])
                sv = round(sub["sharesHeld"] * sp / 1e8, 1)
                current_subs.append({
                    "name": sub["name"],
                    "price": round(sp, 0),
                    "value": sv,
                })
            current["subsidiaries"] = current_subs

        pair_data = {
            "id": pair["id"],
            "name": pair["name"],
            "holdingName": pair["holdingName"],
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
        "lastUpdated": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "pairs": pairs_result,
    }

    js_content = "const STOCK_DATA = " + json.dumps(stock_data, ensure_ascii=False, indent=2) + ";\n"

    output_path = Path(__file__).parent / "data.js"
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(js_content)

    print(f"\nGenerated {output_path} ({len(pairs_result)} pairs, {len(js_content)} bytes)")


if __name__ == "__main__":
    main()
