"""허브(Value Compass)용 발행 요약 summary.json / version.json 조립 (stdlib 전용).

계약: value-invest ``docs/ecosystem/data-contract.md`` §6.1 +
``config/schemas/summary/holding_value.schema.json``. 허브는 지금 current.json 전체와
config.json을 raw.githubusercontent에서 받아 TOP 카드·종목 매칭을 만든다. 이 모듈은 그 두
파일에서 허브가 쓰는 필드만 추린 ~9 KB 요약을 공통 envelope(v1)로 감싸 Pages 루트에 쓴다.
기존 current.json·config.json은 그대로 발행한다(추가만 — 허브 폴백·외부 소비자 유지).

- ``pairs``는 **config.json 순서의 전체 목록**이다. current.json의 의사 행(``_average``)은
  빼고, 시세가 없는 쌍도 수치를 ``null``로 둔 채 남긴다(모르는 숫자는 0이 아니라 null).
- envelope ``asOf``는 current.json ``generatedAt``(스냅샷 시각, KST) — 실행 시각이 아니다.
- 쓰기는 벤더링 헬퍼 ``pipeline/vc_publish.py``의 ``write_if_changed``/``write_version`` —
  데이터가 같으면 파일을 건드리지 않는다(no-op → git diff 없음).
- 백분위(``ratioPct1y``/``ratioPct3y``)는 ``data/summary.json``의 ``percentileHistory``와 현재 비율로
  **다시 계산한다**. 카드 칩과 같은 식이다(현재 비율을 스냅샷 날짜 표본으로 넣고, 이하 표본 비율, 최소 30개).
  그 파일의 ``current.pctile*``은 일봉 작업이 멈추면 같이 멈추므로 쓰지 않는다. 분포가 어디서 끝나는지는
  ``ratioPctEnd``로 함께 발행해 허브가 기준일을 드러내게 한다.
"""

from __future__ import annotations

import json
import math
import re
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional

from pipeline import vc_publish as vp

TOOL_ID = "holding_value"
AVERAGE_PAIR_ID = "_average"
KR_CODE_RE = re.compile(r"^[0-9A-Z]{6}$")
DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
PERCENTILE_MIN_POINTS = 30  # js/dashboard-core.js computePairPercentiles 와 같은 최소 표본
PERCENTILE_WINDOWS = {"ratioPct1y": 365, "ratioPct3y": 1095}
# 공개 URL만 둔다(LAN 주소·토큰 금지 — 계약 §10). 해시 비교 대상이 아니다.
SOURCES = [
    {"id": "kis", "name": "한국투자증권 KIS Open API·시세 프록시"},
    {"id": "yahoo-finance", "name": "Yahoo Finance (폴백)", "url": "https://finance.yahoo.com/"},
]


def to_number(value: Any) -> Optional[float]:
    """숫자면 그대로(유한값만), 아니면 None. bool·문자열·NaN·Infinity는 None."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def holding_code(ticker: Any) -> str:
    """'000670.KS' → '000670' (대문자, 접미사 제거)."""
    return str(ticker or "").split(".", 1)[0].strip().upper()


def _history_rows(history: Any) -> List[tuple]:
    """percentileHistory(열 배열) → 날짜순 (date, ratio). 숫자가 아닌 표본과 잘못된 날짜는 뺀다."""
    if not isinstance(history, Mapping):
        return []
    dates, ratios = history.get("dates"), history.get("ratio")
    if not isinstance(dates, list) or not isinstance(ratios, list):
        return []
    rows = [
        (day, number)
        for day, number in ((day, to_number(value)) for day, value in zip(dates, ratios))
        if number is not None and isinstance(day, str) and DATE_RE.match(day)
    ]
    return sorted(rows, key=lambda row: row[0])


def percentile_fields(history: Any, ratio: Any, snapshot_date: Optional[str]) -> Dict[str, Any]:
    """현재 비율의 1년/3년 백분위와 분포 끝 날짜 (js/dashboard-core.js 카드 칩과 같은 식).

    카드처럼 현재 비율을 스냅샷 날짜 표본으로 넣는다(같은 날이면 교체, 새 날이면 추가).
    ``ratioPctEnd``는 넣기 전 분포의 마지막 날짜다 — 일봉이 멈추면 스냅샷보다 오래된 날짜로 남는다.
    """
    rows = _history_rows(history)
    fields: Dict[str, Any] = {key: None for key in PERCENTILE_WINDOWS}
    fields["ratioPctEnd"] = rows[-1][0] if rows else None
    current = to_number(ratio)
    if current is None or not rows:
        return fields
    if snapshot_date and DATE_RE.match(snapshot_date) and snapshot_date >= rows[-1][0]:
        rows = [row for row in rows if row[0] != snapshot_date] + [(snapshot_date, current)]
    latest = date.fromisoformat(rows[-1][0])
    for key, days in PERCENTILE_WINDOWS.items():
        cutoff = (latest - timedelta(days=days)).isoformat()
        window = [value for day, value in rows if day >= cutoff]
        if len(window) >= PERCENTILE_MIN_POINTS:
            rank = sum(1 for value in window if value <= current) / len(window) * 100
            fields[key] = math.floor(rank + 0.5)  # JS Math.round 와 같은 반올림
    return fields


def build_holding_summary(
    current: Mapping[str, Any],
    config: Iterable[Mapping[str, Any]],
    dashboard: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    """current.json + config.json → summary ``data`` (§6.1).

    ``dashboard``(data/summary.json)가 있으면 쌍마다 백분위 필드를 더한다. 없으면 필드를 싣지 않는다.
    """
    history_by_id: Dict[str, Any] = {}
    if isinstance(dashboard, Mapping):
        for entry in dashboard.get("pairs") or []:
            if isinstance(entry, Mapping) and entry.get("id"):
                history_by_id[entry["id"]] = entry.get("percentileHistory")
    snapshot_date = str(current.get("generatedAt") or current.get("lastUpdated") or "")[:10]
    live_by_id: Dict[str, Mapping[str, Any]] = {}
    average_entry: Optional[Mapping[str, Any]] = None
    for entry in current.get("pairs") or []:
        if not isinstance(entry, Mapping) or not entry.get("id"):
            continue
        if entry["id"] == AVERAGE_PAIR_ID:
            average_entry = entry
            continue
        live_by_id.setdefault(entry["id"], entry)

    pairs: List[Dict[str, Any]] = []
    for item in config:
        pair_id = item.get("id")
        code = holding_code(item.get("holdingTicker"))
        if not pair_id or not KR_CODE_RE.match(code):
            continue  # 허브 분석·매칭이 받을 수 없는 코드 — 스키마(krCode) 위반이라 싣지 않는다
        live = live_by_id.get(pair_id) or {}
        row = {
            "id": pair_id,
            "name": item.get("name") or pair_id,
            "holdingName": item.get("holdingName"),
            "code": code,
            "ratio": to_number(live.get("ratio")),
            "ratioChange": to_number(live.get("ratioChange")),
            "holdingValue": to_number(live.get("holdingValue")),
            "marketCap": to_number(live.get("marketCap")),
        }
        if isinstance(dashboard, Mapping):
            row.update(percentile_fields(history_by_id.get(pair_id), live.get("ratio"), snapshot_date))
        pairs.append(row)

    summary = current.get("summary") if isinstance(current.get("summary"), Mapping) else {}
    pair_count = summary.get("pairCount")
    average_ratio = to_number(summary.get("averageRatio"))
    if average_ratio is None and average_entry is not None:
        average_ratio = to_number(average_entry.get("ratio"))
    return {
        "lastUpdated": current.get("lastUpdated") if isinstance(current.get("lastUpdated"), str) else None,
        "isPartial": bool(current.get("isPartial")),
        "pairCount": pair_count if isinstance(pair_count, int) and not isinstance(pair_count, bool) and pair_count >= 0 else None,
        "averageRatio": average_ratio,
        "pairs": pairs,
    }


def snapshot_as_of(current: Mapping[str, Any]) -> str:
    """데이터 기준 시각: generatedAt(+09:00) → lastUpdated('YYYY-MM-DD HH:MM:SS', KST)."""
    generated = current.get("generatedAt")
    if isinstance(generated, str) and generated:
        return generated
    last = current.get("lastUpdated")
    if isinstance(last, str) and re.match(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$", last):
        return last.replace(" ", "T") + "+09:00"
    raise vp.EnvelopeError("current.json에 generatedAt/lastUpdated가 없어 asOf를 정할 수 없다")


def build_envelope(
    current: Mapping[str, Any],
    config: Iterable[Mapping[str, Any]],
    *,
    generated_at: Any = None,
    dashboard: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    return vp.build_envelope(
        TOOL_ID,
        build_holding_summary(current, config, dashboard),
        as_of=snapshot_as_of(current),
        sources=SOURCES,
        generated_at=generated_at,
    )


def _read_dashboard(base_dir: Path) -> Optional[Mapping[str, Any]]:
    """data/summary.json. 없거나 깨졌으면 None — 백분위만 빠지고 나머지 발행은 계속된다."""
    try:
        with open(base_dir / "data" / "summary.json", encoding="utf-8") as fh:
            dashboard = json.load(fh)
    except (OSError, ValueError):
        return None
    return dashboard if isinstance(dashboard, Mapping) else None


def publish(base_dir: Path, *, generated_at: Any = None) -> Dict[str, Any]:
    """base_dir의 current.json·config.json → summary.json·version.json (바뀐 경우에만 쓰기)."""
    base_dir = Path(base_dir)
    with open(base_dir / "current.json", encoding="utf-8") as fh:
        current = json.load(fh)
    with open(base_dir / "config.json", encoding="utf-8") as fh:
        config = json.load(fh)
    envelope = build_envelope(current, config, generated_at=generated_at, dashboard=_read_dashboard(base_dir))
    summary_changed = vp.write_if_changed(base_dir / "summary.json", envelope)
    version_changed = vp.write_version(
        base_dir / "version.json", {"summary.json": envelope}, generated_at=envelope["generatedAt"],
    )
    return {
        "envelope": envelope,
        "summaryChanged": summary_changed,
        "versionChanged": version_changed,
    }
