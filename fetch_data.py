#!/usr/bin/env python3
"""
보유지분가치/시가총액 비율 데이터 수집 스크립트
Yahoo Finance에서 지주사/자회사 가격 데이터를 가져와 data.js를 생성한다.
"""

import json
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

import yfinance as yf
import pandas as pd

# 종목 설정을 config.json에서 로드
CONFIG_PATH = Path(__file__).parent / "config.json"
with open(CONFIG_PATH, encoding="utf-8") as f:
    PAIRS = json.load(f)


def main():
    # 모든 티커 수집 (중복 제거)
    all_tickers = list(
        dict.fromkeys(
            ticker
            for pair in PAIRS
            for ticker in [pair["holdingTicker"], pair["subsidiaryTicker"]]
        )
    )

    end_date = datetime.now()
    start_date = end_date - timedelta(days=3 * 365 + 30)  # 3년 + 여유분

    print(f"Downloading data for {len(all_tickers)} tickers...")
    print(f"Period: {start_date.strftime('%Y-%m-%d')} ~ {end_date.strftime('%Y-%m-%d')}")

    # 일괄 다운로드
    data = yf.download(
        all_tickers,
        start=start_date.strftime("%Y-%m-%d"),
        end=end_date.strftime("%Y-%m-%d"),
        auto_adjust=True,
        progress=True,
    )

    close = data["Close"]

    # 각 페어별로 비율 계산
    pairs_result = []

    for pair in PAIRS:
        ht = pair["holdingTicker"]
        st = pair["subsidiaryTicker"]

        holding_close = close[ht].dropna()
        subsidiary_close = close[st].dropna()

        # 두 시리즈의 공통 날짜만 사용
        common_dates = holding_close.index.intersection(subsidiary_close.index)
        if len(common_dates) == 0:
            print(f"  WARNING: No overlapping dates for {pair['name']}, skipping.")
            continue

        h = holding_close.loc[common_dates]
        s = subsidiary_close.loc[common_dates]

        total_shares = pair["holdingTotalShares"]
        treasury_shares = pair["holdingTreasuryShares"]
        shares_held = pair["sharesHeld"]
        adjusted_shares = total_shares - treasury_shares

        # 비율: 보유지분가치 / 조정시가총액 × 100
        # = (sharesHeld × 자회사가) / ((totalShares - treasuryShares) × 지주사가) × 100
        ratio = (shares_held * s) / (adjusted_shares * h) * 100

        # 히스토리 구성
        history = []
        for date in common_dates:
            holding_price = round(float(h.loc[date]), 0)
            subsidiary_price = round(float(s.loc[date]), 0)
            holding_value = shares_held * subsidiary_price  # 보유지분가치 (원)
            market_cap = adjusted_shares * holding_price  # 조정시가총액 (원)
            history.append(
                {
                    "date": date.strftime("%Y-%m-%d"),
                    "holdingPrice": holding_price,
                    "subsidiaryPrice": subsidiary_price,
                    "holdingValue": round(holding_value / 1e8, 1),  # 억원
                    "marketCap": round(market_cap / 1e8, 1),  # 억원
                    "ratio": round(float(ratio.loc[date]), 2),
                }
            )

        # 현재 (마지막 거래일) 정보
        latest = history[-1]
        prev = history[-2] if len(history) >= 2 else latest
        ratio_change = round(latest["ratio"] - prev["ratio"], 2)

        pair_data = {
            "id": pair["id"],
            "name": pair["name"],
            "holdingName": pair["holdingName"],
            "subsidiaryName": pair["subsidiaryName"],
            "current": {
                "holdingPrice": latest["holdingPrice"],
                "subsidiaryPrice": latest["subsidiaryPrice"],
                "holdingValue": latest["holdingValue"],
                "marketCap": latest["marketCap"],
                "ratio": latest["ratio"],
                "ratioChange": ratio_change,
            },
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

    # 전체 평균도 포함하여 비율 높은 순 정렬
    if avg_history:
        pairs_result.append(avg_pair)
    pairs_result.sort(key=lambda p: p["current"]["ratio"], reverse=True)

    # data.js 출력
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
