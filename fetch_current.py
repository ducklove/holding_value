#!/usr/bin/env python3
"""
현재 주가만 가져와서 current.js를 생성한다.
10분 간격으로 실행하여 실시간 가격을 제공한다.
"""

import json
from datetime import datetime
from pathlib import Path

import yfinance as yf

CONFIG_PATH = Path(__file__).parent / "config.json"
with open(CONFIG_PATH, encoding="utf-8") as f:
    PAIRS = json.load(f)


def is_korean(ticker):
    return ticker.endswith(".KS") or ticker.endswith(".KQ")


def main():
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

    print(f"Fetching current prices for {len(all_tickers)} tickers...")

    data = yf.download(all_tickers, period="5d", auto_adjust=True, progress=False)

    if data.empty:
        print("ERROR: No data downloaded.")
        return

    close = data["Close"]

    # 최신 가격 추출
    prices = {}
    for ticker in all_tickers:
        if ticker in close.columns:
            series = close[ticker].dropna()
            if len(series) > 0:
                prices[ticker] = float(series.iloc[-1])

    fx_rate = prices.get("USDKRW=X")

    pairs_result = []
    for pair in PAIRS:
        ht = pair["holdingTicker"]
        if ht not in prices:
            continue

        holding_price = prices[ht]
        total_shares = pair["holdingTotalShares"]
        treasury_shares = pair["holdingTreasuryShares"]
        adjusted_shares = total_shares - treasury_shares

        holding_value = 0
        subs = pair["subsidiaries"]
        sub_details = []
        skip = False
        for sub in subs:
            st = sub["ticker"]
            if st not in prices:
                skip = True
                break
            sp = prices[st]
            if not is_korean(st) and fx_rate:
                sp *= fx_rate
            sv = sub["sharesHeld"] * sp
            holding_value += sv
            sub_details.append({
                "name": sub["name"],
                "price": round(sp, 0),
                "value": round(sv / 1e8, 1),
            })

        if skip:
            continue

        market_cap = adjusted_shares * holding_price
        ratio = holding_value / market_cap * 100

        entry = {
            "id": pair["id"],
            "holdingPrice": round(holding_price, 0),
            "holdingValue": round(holding_value / 1e8, 1),
            "marketCap": round(market_cap / 1e8, 1),
            "ratio": round(ratio, 2),
        }

        if len(subs) == 1:
            entry["subsidiaryPrice"] = sub_details[0]["price"]
        else:
            entry["subsidiaries"] = sub_details

        pairs_result.append(entry)
        print(f"  {pair['name']}: ratio {ratio:.2f}%")

    # 전체 평균
    if pairs_result:
        avg_ratio = sum(p["ratio"] for p in pairs_result) / len(pairs_result)
        pairs_result.append({
            "id": "_average",
            "ratio": round(avg_ratio, 2),
        })

    current_data = {
        "lastUpdated": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "pairs": pairs_result,
    }

    js_content = "const CURRENT_DATA = " + json.dumps(current_data, ensure_ascii=False, indent=2) + ";\n"

    output_path = Path(__file__).parent / "current.js"
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(js_content)

    print(f"\nGenerated {output_path} ({len(pairs_result)} pairs)")


if __name__ == "__main__":
    main()
