"""pipeline/fundamentals.py 순수 로직 테스트 (네트워크 없음)."""

import pytest

from pipeline.fundamentals import (
    apply_overrides,
    build_pair_fundamentals,
    has_real_amounts,
    looks_like_scale_anomaly,
    match_investment_rows,
    normalize_corp_name,
    qty_warning,
    report_candidates,
    residual_ratio,
    summarize_investment_details,
    to_amount,
    total_book_value,
    usable_investment_rows,
)


def inv_row(name, qty=None, book=None, stlm="2025-12-31", **extra):
    row = {
        "inv_prm": name,
        "trmend_blce_qy": qty,
        "trmend_blce_acntbk_amount": book,
        "stlm_dt": stlm,
    }
    row.update(extra)
    return row


class TestNormalize:
    @pytest.mark.parametrize(
        "raw,expected",
        [
            ("고려아연㈜", "고려아연"),
            ("(주)엘지화학", "엘지화학"),
            ("주식회사 LG에너지솔루션", "lg에너지솔루션"),
            ("SNT다이내믹스(주)", "snt다이내믹스"),
            ("(주)아모레퍼시픽(보통주)", "아모레퍼시픽"),
            ("두산로보틱스㈜(주1)(주2)", "두산로보틱스"),
            ("Berkshire Hathaway Inc.", "berkshirehathaway"),
            ("  한성크린텍(주) ", "한성크린텍"),
        ],
    )
    def test_normalize_strips_legal_forms(self, raw, expected):
        assert normalize_corp_name(raw) == expected

    def test_normalize_handles_blank(self):
        assert normalize_corp_name("") == ""
        assert normalize_corp_name(None) == ""


class TestToAmount:
    def test_parses_dart_number_formats(self):
        assert to_amount("670,052,000,000") == 670052000000.0
        assert to_amount("13") == 13.0
        assert to_amount("(1,234)") == -1234.0
        assert to_amount(1234) == 1234.0

    def test_placeholders_become_none(self):
        for raw in (None, "", "-", "N/A", "해당사항없음", "알수없음"):
            assert to_amount(raw) is None


class TestReportCandidates:
    def test_orders_recent_first(self):
        candidates = report_candidates(2026, (("11011", "사업"), ("11012", "반기")), min_year=2025)
        assert candidates[0] == ("2026", "11011", "2026 사업보고서")
        assert candidates[1] == ("2026", "11012", "2026 반기보고서")
        assert candidates[-1] == ("2025", "11012", "2025 반기보고서")


class TestRowFiltering:
    def test_drops_total_and_blank_rows(self):
        rows = [inv_row("고려아연㈜", "10", "13"), inv_row("합계", "10", "13"), inv_row("-", "-", "-")]
        assert [row["inv_prm"] for row in usable_investment_rows(rows)] == ["고려아연㈜"]

    def test_has_real_amounts_detects_placeholder_report(self):
        # 분기보고서는 타법인 출자현황을 "-"로만 제출하는 사례가 많다
        assert has_real_amounts([inv_row("-", "-", "-")]) is False
        assert has_real_amounts([inv_row("(주)하림", "1", "100")]) is True


class TestScaleAnomaly:
    def test_detects_unit_shift_between_reports(self):
        # 같은 지분을 원 단위(884,000,000)와 백만원 단위(884)로 번갈아 적는 사례
        won = [inv_row("건설공제조합", "571", "884,000,000")]
        million = [inv_row("건설공제조합", "571", "884")]
        assert looks_like_scale_anomaly(million, won) is True
        assert looks_like_scale_anomaly(won, won) is False

    def test_ignores_when_either_side_is_empty(self):
        assert looks_like_scale_anomaly([], [inv_row("A", "1", "100")]) is False


class TestMatching:
    def test_matches_by_normalized_name(self):
        rows = [inv_row("(주)하림", "60,928,422", "143,014,000,000")]
        result = match_investment_rows({"name": "하림", "sharesHeld": 60928422}, rows)
        assert result["how"] == "name"
        assert result["bookValue"] == 143014000000.0

    def test_sums_common_and_preferred_share_rows(self):
        rows = [
            inv_row("(주)아모레퍼시픽(보통주)", "22,250,869", "1,262,637,000,000"),
            inv_row("(주)아모레퍼시픽(우선주)", "1,511,030", "30,484,000,000"),
        ]
        result = match_investment_rows({"name": "아모레퍼시픽", "sharesHeld": 23761899}, rows)
        assert result["bookValue"] == 1293121000000.0
        assert result["qty"] == 23761899.0
        assert len(result["matchedNames"]) == 2

    def test_falls_back_to_exact_share_count(self):
        rows = [inv_row("에스케이하이닉스㈜", "146,049,275", "3,374,726,000,000")]
        result = match_investment_rows({"name": "SK하이닉스", "sharesHeld": 146049275}, rows)
        assert result["how"] == "qty"
        assert result["bookValue"] == 3374726000000.0

    def test_uses_dart_official_name_when_config_name_differs(self):
        rows = [inv_row("엘지전자(주)", "57,433,029", "3,004,623,000,000")]
        result = match_investment_rows({"name": "LG전자", "sharesHeld": 1}, rows, dart_name="엘지전자")
        assert result["how"] == "name"

    def test_override_name_wins_over_config_name(self):
        # 영풍은 고려아연 지분을 100% 자회사 (유)와이피씨로 넘겨 별도 F/S에는 YPC로 잡힌다
        rows = [
            inv_row("고려아연㈜", "10", "13,000,000"),
            inv_row("(유)와이피씨", "877,075", "670,052,000,000"),
        ]
        subsidiary = {"name": "고려아연", "sharesHeld": 5262450, "dartInvestmentName": ["(유)와이피씨"]}
        result = match_investment_rows(subsidiary, rows)
        assert result["bookValue"] == 670052000000.0
        assert result["matchedNames"] == ["(유)와이피씨"]

    def test_exclude_drops_other_share_class_of_same_investee(self):
        # INVENI는 대신증권 보통주(상장)와 상환전환우선주(비상장)를 함께 들고 있다.
        # 주식 종류 표기는 정규화에서 지워져 두 행이 같이 잡히므로 RCPS를 이름으로 뺀다.
        rows = [
            inv_row("대신증권(보통주)", "2,070,000", "55,890,000,000"),
            inv_row("대신증권(상환전환우선주)", "123,456", "10,000,000,000"),
        ]
        subsidiary = {
            "name": "대신증권",
            "sharesHeld": 2070000,
            "dartInvestmentExclude": ["대신증권(상환전환우선주)"],
        }
        result = match_investment_rows(subsidiary, rows)
        assert result["bookValue"] == 55890000000.0
        assert result["qty"] == 2070000.0
        assert result["matchedNames"] == ["대신증권(보통주)"]
        assert qty_warning(subsidiary, result["qty"]) is None

    def test_without_exclude_both_share_classes_are_summed(self):
        rows = [
            inv_row("대신증권(보통주)", "2,070,000", "55,890,000,000"),
            inv_row("대신증권(상환전환우선주)", "123,456", "10,000,000,000"),
        ]
        result = match_investment_rows({"name": "대신증권", "sharesHeld": 2070000}, rows)
        assert result["bookValue"] == 65890000000.0

    def test_no_match_returns_none_book_value(self):
        rows = [inv_row("건설공제조합", "571", "884,000,000")]
        result = match_investment_rows({"name": "세방전지", "sharesHeld": 5569225}, rows)
        assert result["how"] == "none"
        assert result["bookValue"] is None

    def test_empty_name_row_does_not_match_everything(self):
        # 정규화 결과가 빈 문자열인 행이 부분 일치로 아무 종목에나 붙던 회귀 방지
        rows = [inv_row("-", "-", "-"), inv_row("...", "1", "100")]
        result = match_investment_rows({"name": "하림", "sharesHeld": 1000}, rows)
        assert result["how"] == "none"


class TestInvestmentDetail:
    def test_extracts_stake_change_and_investee_performance(self):
        rows = [
            inv_row(
                "㈜풍산",
                "10,650,000",
                "273,644,000,000",
                trmend_blce_qota_rt="35.11",
                bsis_blce_qy="10,000,000",
                bsis_blce_qota_rt="32.97",
                incrs_dcrs_acqs_dsps_qy="650,000",
                incrs_dcrs_acqs_dsps_amount="21,000,000,000",
                recent_bsns_year_fnnr_sttus_tot_assets="3,000,000,000,000",
                recent_bsns_year_fnnr_sttus_thstrm_ntpf="250,000,000,000",
                frst_acqs_de="2008-07-01",
            )
        ]
        detail = summarize_investment_details(rows)
        assert detail["stakePct"] == 35.11
        assert detail["beginQty"] == 10000000.0
        assert detail["acqQty"] == 650000.0
        assert detail["acqAmount"] == 21000000000.0
        assert detail["recentNetIncome"] == 250000000000.0
        assert detail["recentTotalAssets"] == 3000000000000.0
        assert detail["firstAcquiredAt"] == "2008-07-01"

    def test_sums_share_classes_but_keeps_performance_once(self):
        # 보통주/우선주 행이 나뉘어도 피출자법인 실적은 같은 값이 중복 기재된다
        rows = [
            inv_row(
                "(주)아모레퍼시픽(보통주)",
                "22,250,869",
                "1,262,637,000,000",
                trmend_blce_qota_rt="37.79",
                incrs_dcrs_acqs_dsps_qy="1,000",
                recent_bsns_year_fnnr_sttus_thstrm_ntpf="120,000,000,000",
            ),
            inv_row(
                "(주)아모레퍼시픽(우선주)",
                "1,511,030",
                "30,484,000,000",
                trmend_blce_qota_rt="14.66",
                incrs_dcrs_acqs_dsps_qy="-",
                recent_bsns_year_fnnr_sttus_thstrm_ntpf="120,000,000,000",
            ),
        ]
        detail = summarize_investment_details(rows)
        assert detail["stakePct"] == pytest.approx(52.45)
        assert detail["acqQty"] == 1000.0
        assert detail["recentNetIncome"] == 120000000000.0

    def test_all_placeholder_fields_stay_none(self):
        # "-"만 기재된 필드는 0이 아니라 None (미기재와 순변동 0을 구분)
        detail = summarize_investment_details(
            [inv_row("(주)하림", "1", "100", incrs_dcrs_acqs_dsps_qy="-", trmend_blce_qota_rt="-")]
        )
        assert detail["stakePct"] is None
        assert detail["acqQty"] is None
        assert detail["firstAcquiredAt"] is None

    def test_empty_hits_return_none(self):
        assert summarize_investment_details([]) is None

    def test_match_result_carries_detail_into_pair_record(self):
        pair = {
            "id": "poongsan_holdings",
            "subsidiaries": [{"name": "풍산", "ticker": "103140.KS", "sharesHeld": 10650000}],
        }
        investments = {
            "report": "2025 사업보고서",
            "stlmDt": "2025-12-31",
            "rows": [
                inv_row(
                    "㈜풍산",
                    "10,650,000",
                    "273,644,000,000",
                    trmend_blce_qota_rt="35.11",
                    recent_bsns_year_fnnr_sttus_thstrm_ntpf="250,000,000,000",
                )
            ],
        }
        record = build_pair_fundamentals(pair, {"equity": 433595463414.0}, investments)
        detail = record["subsidiaries"][0]["detail"]
        assert detail["stakePct"] == 35.11
        assert detail["recentNetIncome"] == 250000000000.0

    def test_unmatched_stake_has_no_detail(self):
        pair = {"id": "x", "subsidiaries": [{"name": "세방전지", "sharesHeld": 5312352}]}
        record = build_pair_fundamentals(pair, {"equity": 1.0}, {"rows": []})
        assert "detail" not in record["subsidiaries"][0]


class TestQtyWarning:
    def test_warns_on_material_gap(self):
        warning = qty_warning({"name": "SNT모티브", "sharesHeld": 5124000}, 11975536)
        assert "SNT모티브" in warning
        assert "2.337" in warning

    def test_silent_within_tolerance(self):
        assert qty_warning({"name": "SK하이닉스", "sharesHeld": 146049275}, 146100000) is None

    def test_skips_when_override_declares_different_entity(self):
        subsidiary = {"name": "고려아연", "sharesHeld": 5262450, "skipQtyCheck": True}
        assert qty_warning(subsidiary, 877075) is None


class TestOverrides:
    def test_merges_subsidiary_rules_without_touching_config(self):
        pair = {"id": "youngpoong_koreazinc", "subsidiaries": [{"name": "고려아연", "sharesHeld": 1}]}
        overrides = {"pairs": {"youngpoong_koreazinc": {"고려아연": {"dartInvestmentName": ["(유)와이피씨"]}}}}
        merged = apply_overrides(pair, overrides)
        assert merged["subsidiaries"][0]["dartInvestmentName"] == ["(유)와이피씨"]
        assert merged["subsidiaries"][0]["sharesHeld"] == 1
        assert "dartInvestmentName" not in pair["subsidiaries"][0]  # 원본 불변

    def test_returns_pair_unchanged_when_no_rule(self):
        pair = {"id": "lg_corp", "subsidiaries": []}
        assert apply_overrides(pair, {"pairs": {}}) is pair
        assert apply_overrides(pair, None) is pair


class TestBuildPairFundamentals:
    date = "2026-09-30"

    def test_residual_is_equity_minus_matched_book_values(self):
        pair = {"subsidiaries": [{"name": "풍산", "ticker": "103140.KS", "sharesHeld": 10650000}]}
        equity = {"equity": 433595463414.0, "report": "2026 3분기보고서", "date": self.date}
        investments = {
            "report": "2026 3분기보고서", "stlmDt": self.date,
            "rows": [inv_row("㈜풍산", "10,650,000", "273,644,000,000", stlm=self.date)],
        }
        record = build_pair_fundamentals(pair, equity, investments)
        assert record["bookValue"] == 273644000000.0
        assert record["residualEquity"] == pytest.approx(159951463414.0)
        assert record["equityReport"] == "2026 3분기보고서"
        assert record["bookValueDate"] == self.date
        assert record["warnings"] == []

    def test_negative_residual_is_kept(self):
        pair = {"subsidiaries": [{"name": "엘앤에프", "sharesHeld": 5188000}]}
        equity = {"equity": 48625016640.0, "date": self.date}
        investments = {"stlmDt": self.date, "rows": [inv_row("엘앤에프", "5,188,000", "80,176,000,000")]}
        assert build_pair_fundamentals(pair, equity, investments)["residualEquity"] < 0

    def test_specific_filing_note_replaces_the_missing_investee_book(self):
        pair = {"subsidiaries": [{"name": "세방전지", "ticker": "004490.KS", "sharesHeld": 5312352}]}
        equity = {"equity": 668934270381.0, "date": self.date}
        investments = {"stlmDt": self.date, "rows": [inv_row("건설공제조합", "571", "884,000,000")]}
        books = {"004490.KS": {"bookValue": 132024549000, "qty": 5312352, "source": "별도 주석"}}
        record = build_pair_fundamentals(pair, equity, investments, filing_books=books)
        assert record["subsidiaries"][0]["matchHow"] == "financialNotes"
        assert record["subsidiaries"][0]["bookValueSource"] == "별도 주석"
        assert record["residualEquity"] == 536909721381.0

    def test_period_mismatch_blocks_residual(self):
        pair = {"subsidiaries": [{"name": "풍산", "sharesHeld": 1}]}
        investments = {"stlmDt": "2026-06-30", "rows": [inv_row("풍산", "1", "100")]}
        record = build_pair_fundamentals(pair, {"equity": 1000, "date": self.date}, investments)
        assert record["bookValue"] == 100
        assert record["residualEquity"] is None
        assert any("기준일이 다릅니다" in warning for warning in record["warnings"])

    @pytest.mark.parametrize("second_book", [None, "-"])
    def test_incomplete_basket_blocks_residual(self, second_book):
        pair = {"subsidiaries": [{"name": "풍산", "sharesHeld": 1}, {"name": "풍산특수금속", "sharesHeld": 2}]}
        rows = [inv_row("풍산", "1", "100"), inv_row("풍산특수금속", "2", second_book)]
        record = build_pair_fundamentals(pair, {"equity": 1000.0,"date": self.date}, {"stlmDt": self.date,"rows": rows})
        assert record["residualEquity"] is None
        assert record["bookValue"] is None
        assert any("찾지 못했습니다" in warning for warning in record["warnings"])

    def test_zero_book_value_is_a_complete_stake(self):
        pair = {"subsidiaries": [{"name": "풍산", "sharesHeld": 1}]}
        investments = {"stlmDt": self.date, "rows": [inv_row("풍산", "1", "0")]}
        record = build_pair_fundamentals(pair, {"equity": 1000, "date": self.date}, investments)
        assert record["residualEquity"] == 1000

    def test_missing_equity_blocks_residual(self):
        pair = {"subsidiaries": [{"name": "풍산", "sharesHeld": 1}]}
        record = build_pair_fundamentals(pair, None, {"rows": [inv_row("㈜풍산", "1", "100")]})
        assert record["residualEquity"] is None


class TestResidualRatio:
    def test_matches_frontend_formula(self):
        # 잔존자본 6,263억 ÷ 조정시총 5,952억 → 105.2% (js/calc.js residualCapitalRatio와 동일)
        assert residual_ratio(626332971489.0, 5952) == pytest.approx(105.2, abs=0.05)

    def test_guards_missing_inputs(self):
        assert residual_ratio(None, 100) is None
        assert residual_ratio(1e12, 0) is None


def test_total_book_value_skips_placeholders():
    rows = [inv_row("A", "1", "100"), inv_row("B", "1", "-"), inv_row("C", "1", "50")]
    assert total_book_value(rows) == 150.0
