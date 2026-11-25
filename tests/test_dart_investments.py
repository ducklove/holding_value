from fetch_fundamentals import preserve_verified_period, select_pair_fundamentals
from pipeline.dart_investments import investment_viewer_params, parse_investment_table


def test_original_filing_supplies_units_date_and_receipt():
    receipt = "20261116000731"
    index = f'''
        node2['text'] = "3. 타법인출자 현황(상세)";
        node2['rcpNo'] = "{receipt}";
        node2['dcmNo'] = "11659701";
        node2['eleId'] = "138";
        node2['offset'] = "100";
        node2['length'] = "200";
        node2['dtd'] = "dart4.xsd";
    '''
    assert investment_viewer_params(index, receipt)["eleId"] == "138"
    assert investment_viewer_params(index, "20261116001150") is None
    cells = ["삼성물산㈜", "상장", "2012.01.27", "경영참여", "20", "17,010", "10.01", "32", "-", "-",
             "(10)", "17,010", "10.49", "5,808,750", "50", "60"]
    detail = "".join(f"<td>{cell}</td>" for cell in cells)
    html = f'''<table><tr><td>(기준일 :</td><td>2026년 09월 30일</td><td>(단위 : 백만원, 천주, %)</td></tr>
        <tr><td>기말잔액</td></tr><tr>{detail}</tr></table>'''
    [row] = parse_investment_table(html, "2026-09-30", receipt)
    assert row["trmend_blce_qy"] == 17010000
    assert row["trmend_blce_acntbk_amount"] == 5808750000000
    assert row["incrs_dcrs_evl_lstmn"] == -10000000
    assert row["rcept_no"] == receipt
    assert parse_investment_table(html, "2026-06-30", receipt) is None
    assert parse_investment_table("<p>분기보고서에는 기재하지 않습니다.</p>", "2026-09-30", receipt) == []


def test_selects_latest_complete_aligned_report_and_ignores_stale_override():
    pair = {"subsidiaries": [{"name": "풍산", "ticker": "103140.KS", "sharesHeld": 1}]}
    quarter = {"equity": 200, "rceptNo": "20261116002026", "date": "2026-09-30"}
    half = {"equity": 150, "rceptNo": "20260814001234", "date": "2026-06-30"}
    half_book = {"stlmDt": half["date"], "rows": [{"inv_prm": "풍산", "trmend_blce_acntbk_amount": 100}]}
    reports = [(quarter, {"stlmDt": quarter["date"], "rows": []}), (half, half_book)]
    filings = {"20260515001111": {"103140.KS": {"bookValue": 1, "source": "별도 주석"}}}
    result = select_pair_fundamentals(pair, reports, filings=filings)
    assert result["equityDate"] == half["date"]
    assert result["latestEquityDate"] == quarter["date"]
    assert result["residualEquity"] == 50


def test_quote_basket_changes_prevent_reuse_of_a_verified_period():
    earlier = {"equityDate": "2026-06-30", "bookValueDate": "2026-06-30", "residualEquity": 100,
               "subsidiaries": [{"ticker": "103140.KS", "sharesHeld": 2}]}
    later = {"equityDate": "2026-09-30", "bookValueDate": "2026-09-30", "residualEquity": 200,
             "subsidiaries": [{"ticker": "103140.KS", "sharesHeld": 2}]}
    assert preserve_verified_period(later, earlier)["residualEquity"] == 200
    different = {**earlier, "subsidiaries": [{"ticker": "103140.KS", "sharesHeld": 3}]}
    assert preserve_verified_period(later, different) is different
