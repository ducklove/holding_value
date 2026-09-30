"""허브용 summary.json(envelope v1) 조립·검증·no-op 쓰기 테스트 (오프라인).

계약: value-invest docs/ecosystem/data-contract.md §6.1,
config/schemas/summary/holding_value.schema.json.
"""

import json
import math
from pathlib import Path

import pytest

from pipeline import hub_summary
from pipeline import vc_publish as vp

ROOT = Path(__file__).resolve().parent.parent

# 허브 스키마(holding_value.schema.json)의 required 키 — 스키마가 바뀌면 함께 고친다.
DATA_REQUIRED = {"lastUpdated", "isPartial", "pairCount", "averageRatio", "pairs"}
PAIR_REQUIRED = {"id", "name", "code", "ratio", "ratioChange", "holdingValue", "marketCap"}

CONFIG = [
    {"id": "b_pair", "name": "나지주→나자회사", "holdingName": "나지주", "holdingTicker": "000020.KQ",
     "subsidiaries": [{"name": "나자회사", "ticker": "000021.KQ", "sharesHeld": 1}]},
    {"id": "a_pair", "name": "가지주→가자회사", "holdingName": "가지주", "holdingTicker": "000010.KS",
     "subsidiaries": [{"name": "가자회사", "ticker": "000011.KS", "sharesHeld": 1}]},
    {"id": "no_quote", "name": "다지주→다자회사", "holdingName": "다지주", "holdingTicker": "0009K0.KS",
     "subsidiaries": [{"name": "다자회사", "ticker": "000031.KS", "sharesHeld": 1}]},
]
CURRENT = {
    "lastUpdated": "2026-09-26 09:21:15",
    "generatedAt": "2026-09-26T09:21:15+09:00",
    "isPartial": True,
    "missingPairIds": ["no_quote"],
    "summary": {"pairCount": 2, "preservedCount": 0, "missingCount": 1, "averageRatio": 150.5},
    "pairs": [
        {"id": "a_pair", "holdingPrice": 1000.0, "ratio": 200.0, "ratioChange": -1.5,
         "holdingValue": 20.0, "marketCap": 10.0, "quoteSource": "kis_proxy"},
        {"id": "b_pair", "holdingPrice": 500.0, "ratio": 101.0, "ratioChange": None,
         "holdingValue": 10.1, "marketCap": 10.0, "quoteSource": "kis_proxy"},
        {"id": "_average", "ratio": 150.5, "mean": 150.5, "count": 2, "ratioChange": -1.5, "quoteSource": "derived"},
    ],
}


def write_inputs(tmp_path, current=CURRENT, config=CONFIG):
    (tmp_path / "current.json").write_text(json.dumps(current, ensure_ascii=False), encoding="utf-8")
    (tmp_path / "config.json").write_text(json.dumps(config, ensure_ascii=False), encoding="utf-8")


def assert_schema_shape(data):
    assert DATA_REQUIRED <= set(data)
    assert isinstance(data["isPartial"], bool)
    assert data["pairCount"] is None or (isinstance(data["pairCount"], int) and data["pairCount"] >= 0)
    for pair in data["pairs"]:
        assert PAIR_REQUIRED <= set(pair), pair
        assert hub_summary.KR_CODE_RE.match(pair["code"]), pair["code"]
        assert pair["id"] and pair["name"]
        for key in ("ratio", "ratioChange", "holdingValue", "marketCap"):
            assert pair[key] is None or (isinstance(pair[key], (int, float)) and math.isfinite(pair[key]))


# --- builder ---

def test_builder_keeps_config_order_and_excludes_average():
    data = hub_summary.build_holding_summary(CURRENT, CONFIG)
    assert [p["id"] for p in data["pairs"]] == ["b_pair", "a_pair", "no_quote"]  # config.json 순서
    assert "_average" not in {p["id"] for p in data["pairs"]}
    assert data["pairs"][1] == {
        "id": "a_pair", "name": "가지주→가자회사", "holdingName": "가지주", "code": "000010",
        "ratio": 200.0, "ratioChange": -1.5, "holdingValue": 20.0, "marketCap": 10.0,
    }
    assert data["lastUpdated"] == "2026-09-26 09:21:15"
    assert data["isPartial"] is True
    assert data["pairCount"] == 2
    assert data["averageRatio"] == 150.5
    assert_schema_shape(data)


def test_builder_uses_null_for_unknown_numbers():
    data = hub_summary.build_holding_summary(CURRENT, CONFIG)
    missing = data["pairs"][2]
    assert missing["code"] == "0009K0"  # 영숫자 신형 코드도 허용
    assert [missing[k] for k in ("ratio", "ratioChange", "holdingValue", "marketCap")] == [None] * 4
    assert data["pairs"][0]["ratioChange"] is None  # 전일 비교 불가 → 0이 아니라 null

    weird = dict(CURRENT, pairs=[{"id": "a_pair", "ratio": float("nan"), "ratioChange": True,
                                  "holdingValue": "12", "marketCap": float("inf")}], summary={})
    row = hub_summary.build_holding_summary(weird, CONFIG[1:2])["pairs"][0]
    assert [row[k] for k in ("ratio", "ratioChange", "holdingValue", "marketCap")] == [None] * 4


def test_builder_average_fallback_and_invalid_codes():
    current = dict(CURRENT, summary={})
    config = CONFIG + [{"id": "us", "name": "해외", "holdingTicker": "BRK-A"}, {"name": "id 없음", "holdingTicker": "000040.KS"}]
    data = hub_summary.build_holding_summary(current, config)
    assert data["averageRatio"] == 150.5  # summary 없으면 _average 행
    assert data["pairCount"] is None
    assert [p["id"] for p in data["pairs"]] == ["b_pair", "a_pair", "no_quote"]  # 스키마 밖 코드·id 없음 제외


# --- envelope ---

def test_envelope_validates_and_as_of_is_snapshot_time():
    env = hub_summary.build_envelope(CURRENT, CONFIG, generated_at="2026-09-30T09:00:00+09:00")
    vp.validate_envelope(env)
    assert env["tool"] == "holding_value" and env["kind"] == "summary" and env["schemaVersion"] == 1
    assert env["asOf"] == "2026-09-26T09:21:15+09:00"  # 실행 시각이 아니라 데이터 시각
    assert env["contentHash"] == vp.content_hash(env["data"])
    assert all("192.168." not in json.dumps(s) for s in env["sources"])  # LAN 주소 비공개

    legacy = {k: v for k, v in CURRENT.items() if k != "generatedAt"}
    assert hub_summary.snapshot_as_of(legacy) == "2026-09-26T09:21:15+09:00"
    with pytest.raises(vp.EnvelopeError):
        hub_summary.snapshot_as_of({})


def test_tampered_envelope_is_rejected():
    env = hub_summary.build_envelope(CURRENT, CONFIG)
    env["data"]["pairs"][0]["ratio"] = 999.0
    with pytest.raises(vp.EnvelopeError):
        vp.validate_envelope(env)


# --- 발행 (write_if_changed / write_version) ---

def test_publish_writes_then_skips_unchanged(tmp_path):
    write_inputs(tmp_path)
    first = hub_summary.publish(tmp_path, generated_at="2026-09-30T09:00:00+09:00")
    assert first["summaryChanged"] and first["versionChanged"]
    summary_bytes = (tmp_path / "summary.json").read_bytes()
    version_bytes = (tmp_path / "version.json").read_bytes()
    assert summary_bytes.endswith(b"\n") and summary_bytes.count(b"\n") == 1  # compact 한 줄
    published = json.loads(summary_bytes)
    vp.validate_envelope(published)
    version = json.loads(version_bytes)
    assert version["tool"] == "holding_value"
    assert version["files"] == {"summary.json": published["contentHash"]}

    # 같은 데이터로 재실행(다른 실행 시각) → 파일을 건드리지 않는다 = git diff 없음
    second = hub_summary.publish(tmp_path, generated_at="2026-09-30T09:10:00+09:00")
    assert second["summaryChanged"] is False and second["versionChanged"] is False
    assert (tmp_path / "summary.json").read_bytes() == summary_bytes
    assert (tmp_path / "version.json").read_bytes() == version_bytes

    # 시세가 바뀌면 다시 쓴다
    moved = json.loads(json.dumps(CURRENT))
    moved["pairs"][0]["ratio"] = 201.0
    write_inputs(tmp_path, current=moved)
    third = hub_summary.publish(tmp_path, generated_at="2026-09-30T09:20:00+09:00")
    assert third["summaryChanged"] and third["versionChanged"]
    assert json.loads((tmp_path / "summary.json").read_text(encoding="utf-8"))["data"]["pairs"][1]["ratio"] == 201.0


def test_publish_summary_cli_reports_github_output(tmp_path, monkeypatch):
    import publish_summary

    write_inputs(tmp_path)
    out = tmp_path / "gh_output"
    monkeypatch.setattr(publish_summary, "BASE_DIR", tmp_path)
    monkeypatch.setenv("GITHUB_OUTPUT", str(out))
    publish_summary.main()
    publish_summary.main()
    assert out.read_text(encoding="utf-8").splitlines() == ["summary_changed=true", "summary_changed=false"]


# --- 커밋된 산출물 ---

def test_committed_summary_is_a_valid_envelope():
    """저장소 루트(= Pages 루트)의 summary.json / version.json이 계약을 지킨다."""
    env = json.loads((ROOT / "summary.json").read_text(encoding="utf-8"))
    vp.validate_envelope(env)
    assert env["tool"] == "holding_value"
    assert_schema_shape(env["data"])
    assert len(env["data"]["pairs"]) > 0
    assert len((ROOT / "summary.json").read_bytes()) < 64 * 1024  # 크기 예산 (계약 §9)
    version = json.loads((ROOT / "version.json").read_text(encoding="utf-8"))
    assert version["files"]["summary.json"] == env["contentHash"]
