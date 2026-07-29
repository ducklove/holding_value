"""잔존자본(별도 총자본 − 리스트 자회사 지분 장부가액) 산출의 순수 로직.

fetch_fundamentals.py가 이 모듈을 재수출한다. 표준 라이브러리만 사용한다.

용어
- **별도 총자본**: 지주사 별도(개별)재무제표의 자본총계. OpenDART `fnlttSinglAcntAll`
  (fs_div=OFS)의 `ifrs-full_Equity`.
- **지분 장부가액**: 정기보고서 "타법인 출자현황"(OpenDART `otrCprInvstmntSttus`)의
  기말 장부가액. config.json의 자회사 목록과 매칭한 합계만 센다.
- **잔존자본**: 별도 총자본 − 지분 장부가액 합계. 자회사 지분을 제외한 모회사 자체 순자산.
"""

import re

# 타법인 출자현황은 분기보고서에 "-"만 제출되는 사례가 많아 사업/반기보고서만 쓴다.
INVESTMENT_REPORT_CODES = (("11011", "사업"), ("11012", "반기"))
# 자본총계는 분기보고서에도 정상 기재되므로 최신치를 위해 전 보고서를 본다.
FS_REPORT_CODES = (("11011", "사업"), ("11014", "3분기"), ("11012", "반기"), ("11013", "1분기"))
# OpenDART 정기보고서 재무정보 제공 시작 연도
DART_MIN_YEAR = 2015
# 보유수량이 이 비율 이상 어긋나면 경고 (config sharesHeld ↔ 출자현황 기말수량)
QTY_WARN_TOLERANCE = 0.02
# 보고서 간 장부가액 합계가 이 배수 이상 튀면 기재 단위 오류로 보고 이전 보고서로 폴백
SCALE_ANOMALY_FACTOR = 100.0

_SUFFIX_RE = re.compile(
    r"(주식회사|㈜|\(주\)|\(유\)|유한회사|유한책임회사|co\.?\s*,?\s*ltd\.?|corp(oration)?\.?|inc\.?|holdings?|gmbh|s\.?a\.?)",
    re.IGNORECASE,
)
_STOCK_KIND_RE = re.compile(r"\((보통주|우선주|구주|신주|전환우선주|상환전환우선주|주\d+)\)")
_NOTE_RE = re.compile(r"\((주\d+|참고\d*|\*+)\)")
_TOTAL_NAMES = {"합계", "계", "소계", "총계"}


def normalize_corp_name(name):
    """법인명을 매칭용으로 정규화한다 (법인격·주식종류·주석·구두점 제거)."""
    text = _STOCK_KIND_RE.sub("", name or "")
    text = _NOTE_RE.sub("", text)
    text = _SUFFIX_RE.sub("", text)
    return re.sub(r"[\s.,\-()&·'\"]", "", text).lower()


def to_amount(raw):
    """'1,234' / '(1,234)' / '-' 형태의 DART 금액 문자열을 float으로 변환한다."""
    if raw is None:
        return None
    if isinstance(raw, (int, float)):
        return float(raw)
    text = str(raw).replace(",", "").strip()
    if text in {"", "-", "N/A", "해당사항없음"}:
        return None
    negative = text.startswith("(") and text.endswith(")")
    if negative:
        text = text[1:-1]
    try:
        value = float(text)
    except ValueError:
        return None
    return -value if negative else value


def report_candidates(year, codes, min_year=DART_MIN_YEAR):
    """(사업연도, 보고서코드, 라벨)을 최근순으로 생성한다."""
    candidates = []
    for bsns_year in range(year, min_year - 1, -1):
        for code, label in codes:
            candidates.append((str(bsns_year), code, f"{bsns_year} {label}보고서"))
    return candidates


def is_total_row(row):
    return normalize_corp_name(row.get("inv_prm", "")) in {normalize_corp_name(n) for n in _TOTAL_NAMES}


def usable_investment_rows(rows):
    """합계 행과 전부 공란인 행을 걸러낸 출자현황 행 목록."""
    return [
        row
        for row in rows or []
        if normalize_corp_name(row.get("inv_prm", "")) and not is_total_row(row)
    ]


def has_real_amounts(rows):
    """장부가액이 하나라도 기재된 보고서인지 (분기보고서 '-' 제출 판별)."""
    return any(to_amount(row.get("trmend_blce_acntbk_amount")) is not None for row in rows)


def total_book_value(rows):
    return sum(to_amount(row.get("trmend_blce_acntbk_amount")) or 0.0 for row in rows)


def looks_like_scale_anomaly(current_rows, previous_rows, factor=SCALE_ANOMALY_FACTOR):
    """같은 회사의 두 보고서 장부가액 합계가 100배 이상 어긋나면 단위 오기로 본다.

    타법인 출자현황은 기재 단위(원/천원/백만원)가 보고서마다 흔들리는 사례가 있어,
    더 최근 보고서가 튀면 직전 보고서로 폴백하기 위한 판별이다.
    """
    current = total_book_value(current_rows)
    previous = total_book_value(previous_rows)
    if current <= 0 or previous <= 0:
        return False
    ratio = current / previous
    return ratio >= factor or ratio <= 1 / factor


def match_investment_rows(subsidiary, rows, dart_name=None):
    """config 자회사 1건에 대응하는 출자현황 행들을 찾는다.

    보통주/우선주가 별도 행으로 기재되는 사례가 있어 매칭된 행을 모두 합산한다.
    매칭 근거는 how로 반환한다: name(법인명 일치) → qty(기말수량 일치) → partial(부분 일치).
    """
    rows = usable_investment_rows(rows)
    aliases = subsidiary.get("dartInvestmentName")
    if isinstance(aliases, str):
        aliases = [aliases]
    targets = {normalize_corp_name(name) for name in (aliases or [])}
    if not targets:
        targets = {normalize_corp_name(subsidiary.get("name", "")), normalize_corp_name(dart_name or "")}
    targets.discard("")

    shares = subsidiary.get("sharesHeld") or 0

    hits = [row for row in rows if normalize_corp_name(row.get("inv_prm", "")) in targets]
    how = "name"
    if not hits and shares:
        hits = [
            row
            for row in rows
            if (qty := to_amount(row.get("trmend_blce_qy"))) is not None
            and abs(qty - shares) <= max(1.0, shares * 0.001)
        ]
        how = "qty"
    if not hits:
        hits = [
            row
            for row in rows
            if (name := normalize_corp_name(row.get("inv_prm", "")))
            and any(len(target) >= 2 and (target in name or name in target) for target in targets)
        ]
        how = "partial"
    if not hits:
        return {"how": "none", "bookValue": None, "qty": None, "matchedNames": []}

    return {
        "how": how,
        "bookValue": total_book_value(hits),
        "qty": sum(to_amount(row.get("trmend_blce_qy")) or 0.0 for row in hits),
        "matchedNames": [row.get("inv_prm") for row in hits],
    }


def qty_warning(subsidiary, matched_qty, tolerance=QTY_WARN_TOLERANCE):
    """config sharesHeld와 출자현황 기말수량의 괴리를 경고 문구로 돌려준다.

    지분을 중간 지주회사로 넘긴 경우처럼 애초에 다른 법인의 수량과 비교하게 되는
    자리에서는 오버라이드에 skipQtyCheck를 세워 비교를 건너뛴다.
    """
    if subsidiary.get("skipQtyCheck"):
        return None
    shares = subsidiary.get("sharesHeld") or 0
    if not shares or not matched_qty:
        return None
    ratio = matched_qty / shares
    if abs(ratio - 1) <= tolerance:
        return None
    return (
        f"{subsidiary.get('name')}: config 보유수량 {shares:,.0f}주 ↔ "
        f"출자현황 기말수량 {matched_qty:,.0f}주 (비 {ratio:.3f})"
    )


def apply_overrides(pair, overrides):
    """fundamentals_overrides.json의 자회사별 예외를 pair에 병합한 사본을 돌려준다.

    config.json은 admin.html이 폼 값으로 통째로 다시 쓰므로(자회사는 name/ticker/sharesHeld만
    살아남는다) 매칭 예외는 config가 아니라 별도 파일에 둔다.
    """
    rules = (overrides or {}).get("pairs", {}).get(pair.get("id"), {})
    if not rules:
        return pair
    merged = dict(pair)
    merged["subsidiaries"] = [
        {**subsidiary, **rules.get(subsidiary.get("name"), {})}
        for subsidiary in pair.get("subsidiaries", [])
    ]
    return merged


def build_pair_fundamentals(pair, equity, investments, bs_investments=None, dart_names=None):
    """지주사 1곳의 잔존자본 레코드를 만든다.

    pair: config.json 항목
    equity: {"equity": 원, "report": ..., "termName": ..., "rceptNo": ...} 또는 None
    investments: {"report": ..., "stlmDt": ..., "rows": [...]} 또는 None
    bs_investments: 별도 재무상태표의 종속·관계기업투자 계정 금액(원) 또는 None
    dart_names: {ticker6: DART 정식 법인명}
    """
    dart_names = dart_names or {}
    subs = []
    warnings = []
    book_total = 0.0
    matched_any = False

    for subsidiary in pair.get("subsidiaries", []):
        ticker6 = str(subsidiary.get("ticker", "")).split(".")[0]
        source = subsidiary.get("bookValueFrom")
        if source == "bsInvestments":
            book = bs_investments
            record = {
                "how": "bsInvestments",
                "bookValue": book,
                "qty": None,
                "matchedNames": ["별도 재무상태표 종속·관계기업투자"],
            }
            if book is None:
                warnings.append(f"{subsidiary.get('name')}: 별도 BS 종속·관계기업투자 계정을 찾지 못했습니다")
        else:
            record = match_investment_rows(
                subsidiary, (investments or {}).get("rows"), dart_names.get(ticker6)
            )

        if record["bookValue"] is not None:
            book_total += record["bookValue"]
            matched_any = True
        else:
            warnings.append(f"{subsidiary.get('name')}: 타법인 출자현황에서 지분을 찾지 못했습니다")

        warning = qty_warning(subsidiary, record.get("qty"))
        if warning:
            warnings.append(warning)

        entry = {
            "name": subsidiary.get("name"),
            "ticker": subsidiary.get("ticker"),
            "sharesHeld": subsidiary.get("sharesHeld"),
            "bookValue": record["bookValue"],
            "matchHow": record["how"],
            "matchedNames": record["matchedNames"],
            "qty": record.get("qty"),
        }
        if subsidiary.get("note"):
            entry["note"] = subsidiary["note"]
        subs.append(entry)

    equity_value = (equity or {}).get("equity")
    residual = None
    if equity_value is not None and matched_any:
        residual = equity_value - book_total

    return {
        "equity": equity_value,
        "equityReport": (equity or {}).get("report"),
        "equityTerm": (equity or {}).get("termName"),
        "equityRceptNo": (equity or {}).get("rceptNo"),
        "bookValue": book_total if matched_any else None,
        "bookValueReport": (investments or {}).get("report"),
        "bookValueDate": (investments or {}).get("stlmDt"),
        "residualEquity": residual,
        "subsidiaries": subs,
        "warnings": warnings,
    }


def residual_ratio(residual_equity_won, market_cap_eok):
    """잔존자본 ÷ 조정시가총액 (%). market_cap은 억원 단위."""
    if residual_equity_won is None or not market_cap_eok:
        return None
    return (residual_equity_won / 1e8) / market_cap_eok * 100
