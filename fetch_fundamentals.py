#!/usr/bin/env python3
"""잔존자본(별도 총자본 − 리스트 자회사 지분 장부가액) 수집 스크립트.

OpenDART에서 지주사별로
  1) 별도재무제표 자본총계 (fnlttSinglAcntAll, fs_div=OFS) — 분기보고서 포함 최신치
  2) 타법인 출자현황 기말 장부가액 (otrCprInvstmntSttus) — 사업/반기보고서 기준
을 받아 `data/fundamentals.json`을 만든다. 주가와 무관한 분기성 데이터라 정기보고서
제출 시즌에만 갱신하면 된다 (실시간 시가총액은 프런트에서 결합한다).

    DART_API_KEY=... python fetch_fundamentals.py
    python fetch_fundamentals.py --only lg_corp sk_inc   # 일부만 갱신 (기존 값 유지)
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime
from io import BytesIO
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from xml.etree import ElementTree
from zipfile import ZipFile

from fin_commons.jsonio import atomic_write_text
from fin_commons.timeutil import KST

from pipeline.fundamentals import (  # noqa: F401 — 재수출 (기존 참조 호환)
    DART_MIN_YEAR,
    FS_REPORT_CODES,
    INVESTMENT_REPORT_CODES,
    QTY_WARN_TOLERANCE,
    SCALE_ANOMALY_FACTOR,
    apply_overrides,
    build_pair_fundamentals,
    has_real_amounts,
    looks_like_scale_anomaly,
    match_investment_rows,
    normalize_corp_name,
    report_candidates,
    residual_ratio,
    to_amount,
    usable_investment_rows,
)

BASE_URL = "https://opendart.fss.or.kr/api"
ROOT = Path(__file__).resolve().parent
CONFIG_PATH = ROOT / "config.json"
OVERRIDES_PATH = ROOT / "fundamentals_overrides.json"
OUTPUT_PATH = ROOT / "data" / "fundamentals.json"
CORP_CODE_CACHE = ROOT / "data" / ".corp_codes.json"
EQUITY_ACCOUNT_ID = "ifrs-full_Equity"
INVESTMENT_ACCOUNT_ID = "ifrs-full_InvestmentsInSubsidiariesJointVenturesAndAssociates"
REQUEST_INTERVAL = 0.1
REQUEST_TIMEOUT = 20


class DartError(RuntimeError):
    pass


def request_bytes(endpoint, params, api_key, retries=2):
    url = f"{BASE_URL}/{endpoint}?{urlencode({'crtfc_key': api_key, **params})}"
    last_error = None
    for attempt in range(retries + 1):
        if attempt:
            time.sleep(min(2.0, 0.5 * attempt))
        try:
            request = Request(url, headers={"User-Agent": "hodling-value/0.1"})
            with urlopen(request, timeout=REQUEST_TIMEOUT) as response:
                time.sleep(REQUEST_INTERVAL)
                return response.read()
        except (HTTPError, URLError, TimeoutError) as exc:
            last_error = exc
    raise DartError(f"{endpoint} 요청 실패: {last_error}")


def request_json(endpoint, params, api_key):
    return json.loads(request_bytes(f"{endpoint}.json", params, api_key).decode("utf-8"))


def load_corp_codes(api_key, refresh=False):
    """종목코드 → {corp_code, corp_name} 매핑 (상장사만). 하루 단위로 캐시한다."""
    if CORP_CODE_CACHE.exists() and not refresh:
        cached = json.loads(CORP_CODE_CACHE.read_text(encoding="utf-8"))
        if cached.get("date") == datetime.now(KST).strftime("%Y-%m-%d"):
            return cached["codes"]

    payload = request_bytes("corpCode.xml", {}, api_key)
    with ZipFile(BytesIO(payload)) as archive:
        name = next(n for n in archive.namelist() if n.lower().endswith(".xml"))
        xml_payload = archive.read(name)

    codes = {}
    for item in ElementTree.fromstring(xml_payload).findall("list"):
        def text(tag):
            found = item.find(tag)
            return (found.text or "").strip() if found is not None else ""

        stock_code = text("stock_code")
        if len(stock_code) != 6:
            continue
        codes[stock_code] = {"corpCode": text("corp_code"), "corpName": text("corp_name")}

    CORP_CODE_CACHE.parent.mkdir(parents=True, exist_ok=True)
    CORP_CODE_CACHE.write_text(
        json.dumps({"date": datetime.now(KST).strftime("%Y-%m-%d"), "codes": codes}, ensure_ascii=False),
        encoding="utf-8",
    )
    return codes


def fetch_balance_sheet(corp_code, api_key, year):
    """가장 최근 정기보고서의 별도 재무상태표 행과 보고서 라벨을 돌려준다."""
    for bsns_year, reprt_code, label in report_candidates(year, FS_REPORT_CODES):
        try:
            payload = request_json(
                "fnlttSinglAcntAll",
                {
                    "corp_code": corp_code,
                    "bsns_year": bsns_year,
                    "reprt_code": reprt_code,
                    "fs_div": "OFS",
                },
                api_key,
            )
        except DartError as exc:
            print(f"    ! {label} 재무제표 요청 실패: {exc}", file=sys.stderr)
            continue
        if payload.get("status") != "000":
            continue
        rows = [row for row in (payload.get("list") or []) if row.get("sj_div") == "BS"]
        if rows:
            return rows, label
    return [], None


def pick_account(rows, account_id, names):
    hit = next((row for row in rows if row.get("account_id") == account_id), None)
    if hit is None:
        wanted = {normalize_corp_name(name) for name in names}
        hit = next((row for row in rows if normalize_corp_name(row.get("account_nm", "")) in wanted), None)
    if hit is None:
        return None, None
    return to_amount(hit.get("thstrm_amount")), hit


def fetch_equity_and_investments_account(corp_code, api_key, year):
    rows, label = fetch_balance_sheet(corp_code, api_key, year)
    if not rows:
        return None, None
    equity_amount, equity_row = pick_account(rows, EQUITY_ACCOUNT_ID, ["자본총계"])
    if equity_amount is None:
        return None, None
    investments, _ = pick_account(
        rows,
        INVESTMENT_ACCOUNT_ID,
        ["종속기업관계기업및공동기업투자", "종속기업관계기업및공동기업투자주식", "종속및관계기업투자", "관계기업투자"],
    )
    equity = {
        "equity": equity_amount,
        "report": label,
        "termName": equity_row.get("thstrm_nm"),
        "rceptNo": equity_row.get("rcept_no"),
    }
    return equity, investments


def fetch_investments(corp_code, api_key, year):
    """사업/반기보고서 중 가장 최근의 타법인 출자현황. 기재 단위가 튀면 직전 보고서로 폴백."""
    previous = None
    for bsns_year, reprt_code, label in report_candidates(year, INVESTMENT_REPORT_CODES):
        try:
            payload = request_json(
                "otrCprInvstmntSttus",
                {"corp_code": corp_code, "bsns_year": bsns_year, "reprt_code": reprt_code},
                api_key,
            )
        except DartError as exc:
            print(f"    ! {label} 출자현황 요청 실패: {exc}", file=sys.stderr)
            continue
        if payload.get("status") != "000":
            continue
        rows = usable_investment_rows(payload.get("list"))
        if not rows or not has_real_amounts(rows):
            continue
        current = {"report": label, "stlmDt": rows[0].get("stlm_dt"), "rows": rows}
        if previous is None:
            previous = current
            continue
        if looks_like_scale_anomaly(previous["rows"], rows):
            print(
                f"    ! {previous['report']} 장부가액 합계가 {label} 대비 100배 이상 어긋납니다"
                f" — 기재 단위 오류로 보고 {label}를 사용합니다",
                file=sys.stderr,
            )
            return current
        return previous
    return previous


def collect(pairs, api_key, year, only=None, overrides=None):
    codes = load_corp_codes(api_key)
    results = {}
    for raw_pair in pairs:
        pair = apply_overrides(raw_pair, overrides)
        pair_id = pair["id"]
        if only and pair_id not in only:
            continue
        ticker6 = str(pair["holdingTicker"]).split(".")[0]
        info = codes.get(ticker6)
        print(f"### {pair_id} {pair['holdingName']} ({ticker6})")
        if not info:
            print("    ! DART 고유번호를 찾지 못했습니다", file=sys.stderr)
            results[pair_id] = {"error": "corp_code_not_found", "warnings": ["DART 고유번호 없음"]}
            continue

        equity, bs_investments = fetch_equity_and_investments_account(info["corpCode"], api_key, year)
        investments = fetch_investments(info["corpCode"], api_key, year)
        dart_names = {}
        for subsidiary in pair.get("subsidiaries", []):
            sub_ticker = str(subsidiary.get("ticker", "")).split(".")[0]
            sub_info = codes.get(sub_ticker)
            if sub_info:
                dart_names[sub_ticker] = sub_info["corpName"]

        record = build_pair_fundamentals(pair, equity, investments, bs_investments, dart_names)
        record["corpCode"] = info["corpCode"]
        record["holdingName"] = pair.get("holdingName")
        results[pair_id] = record

        if record["equity"] is None:
            print("    ! 별도 자본총계를 찾지 못했습니다", file=sys.stderr)
        else:
            print(
                f"    자본총계 {record['equity'] / 1e8:,.0f}억 ({record['equityReport']}) / "
                f"장부가액 {(record['bookValue'] or 0) / 1e8:,.0f}억 ({record['bookValueReport']}) / "
                f"잔존자본 {(record['residualEquity'] or 0) / 1e8:,.0f}억"
            )
        for warning in record["warnings"]:
            print(f"    ⚠ {warning}", file=sys.stderr)
    return results


def main():
    parser = argparse.ArgumentParser(description="별도 총자본·자회사 장부가액 수집 (OpenDART)")
    parser.add_argument("--only", nargs="*", help="갱신할 pair id (미지정 시 전체)")
    parser.add_argument("--year", type=int, default=None, help="탐색 시작 사업연도 (기본: 올해)")
    parser.add_argument("--output", type=Path, default=OUTPUT_PATH)
    args = parser.parse_args()

    api_key = os.environ.get("DART_API_KEY") or os.environ.get("OPENDART_API_KEY")
    if not api_key:
        raise SystemExit("DART_API_KEY(또는 OPENDART_API_KEY) 환경변수가 필요합니다")

    pairs = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    overrides = json.loads(OVERRIDES_PATH.read_text(encoding="utf-8")) if OVERRIDES_PATH.exists() else {}
    year = args.year or datetime.now(KST).year
    results = collect(pairs, api_key, year, set(args.only) if args.only else None, overrides)

    if args.only and args.output.exists():
        existing = json.loads(args.output.read_text(encoding="utf-8"))
        merged = existing.get("pairs", {})
        merged.update(results)
        results = merged

    payload = {
        "generatedAt": datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S"),
        "note": "잔존자본 = 별도재무제표 자본총계 − 리스트 자회사 지분 장부가액(타법인 출자현황 기말 장부가액). 금액 단위는 원.",
        "pairs": results,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    atomic_write_text(args.output, json.dumps(payload, ensure_ascii=False, indent=2) + "\n")
    print(f"\n{len(results)}개 종목 → {args.output}")


if __name__ == "__main__":
    main()
