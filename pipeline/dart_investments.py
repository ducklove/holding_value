"""Read the original DART investment table to apply the units declared in the filing."""

import re
from html.parser import HTMLParser
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from pipeline.fundamentals import to_amount

DART_URL = "https://dart.fss.or.kr"
_VIEWER_FIELDS = ("rcpNo", "dcmNo", "eleId", "offset", "length", "dtd")
_FIELDS = {
    4: ("frst_acqs_amount", "money"),
    5: ("bsis_blce_qy", "shares"),
    6: ("bsis_blce_qota_rt", "percent"),
    7: ("bsis_blce_acntbk_amount", "money"),
    8: ("incrs_dcrs_acqs_dsps_qy", "shares"),
    9: ("incrs_dcrs_acqs_dsps_amount", "money"),
    10: ("incrs_dcrs_evl_lstmn", "money"),
    11: ("trmend_blce_qy", "shares"),
    12: ("trmend_blce_qota_rt", "percent"),
    13: ("trmend_blce_acntbk_amount", "money"),
    14: ("recent_bsns_year_fnnr_sttus_tot_assets", "money"),
    15: ("recent_bsns_year_fnnr_sttus_thstrm_ntpf", "money"),
}


class _TableRows(HTMLParser):
    def __init__(self):
        super().__init__()
        self.rows = []
        self.row = []
        self.cell = None

    def handle_starttag(self, tag, attrs):
        if tag == "tr":
            self.row = []
        elif tag in ("td", "th"):
            self.cell = []

    def handle_data(self, data):
        if self.cell is not None:
            self.cell.append(data)

    def handle_endtag(self, tag):
        if tag in ("td", "th") and self.cell is not None:
            self.row.append(" ".join("".join(self.cell).split()))
            self.cell = None
        elif tag == "tr" and self.row:
            self.rows.append(self.row)


def investment_viewer_params(index, receipt):
    for node in re.split(r"\['text'\]\s*=", index)[1:]:
        title = re.match(r'\s*"[^"]*타법인\s*출자\s*현황\s*\(상세\)', node)
        if not title:
            continue
        params = dict(re.findall(r"\['(\w+)'\]\s*=\s*\"([^\"]+)\"", node.split("var node", 1)[0]))
        if params.get("rcpNo") == receipt and all(key in params for key in _VIEWER_FIELDS):
            return {key: params[key] for key in _VIEWER_FIELDS}
    return None


def parse_investment_table(document, date, receipt):
    """Return rows in won and shares. None signals an unsupported table, [] an omitted table."""
    parser = _TableRows()
    parser.feed(document)
    if not parser.rows or not any("기말잔액" in "".join(row).replace(" ", "") for row in parser.rows):
        return [] if "기재하지" in document or "해당사항 없음" in document else None
    header = " ".join(cell for row in parser.rows[:8] for cell in row)
    units = re.search(r"단위\s*:\s*(백만원|천원|원)\s*,\s*(천주|주)\s*,", header)
    period = re.search(r"기준일\s*:\s*(\d{4})\s*[년.\-/]\s*(\d{1,2})\s*[월.\-/]\s*(\d{1,2})", header)
    if units is None or period is None or "-".join(part.zfill(2) for part in period.groups()) != date:
        return None
    money, shares = units.groups()
    scales = {"money": {"원": 1, "천원": 1000, "백만원": 1000000}[money],
              "shares": 1000 if shares == "천주" else 1, "percent": 1}
    results = []
    for cells in parser.rows:
        if len(cells) != 16 or not any(status in cells[1] for status in ("상장", "비상장")):
            continue
        row = {"inv_prm": cells[0], "frst_acqs_de": cells[2], "stlm_dt": date, "rcept_no": receipt}
        for column, (name, unit) in _FIELDS.items():
            number = to_amount(cells[column])
            row[name] = number * scales[unit] if number is not None else None
        results.append(row)
    return results or None


def _get(path, params):
    url = f"{DART_URL}{path}?{urlencode(params)}"
    request = Request(url, headers={"User-Agent": "holding-value/0.1", "Referer": DART_URL + "/"})
    with urlopen(request, timeout=12) as response:
        return response.read().decode("utf-8")


def read_original_investments(receipt, date):
    if not receipt or not re.fullmatch(r"\d{14}", receipt):
        return None
    index = _get("/dsaf001/main.do", {"rcpNo": receipt})
    params = investment_viewer_params(index, receipt)
    if params is None:
        return None
    return parse_investment_table(_get("/report/viewer.do", params), date, receipt)
