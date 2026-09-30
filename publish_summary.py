#!/usr/bin/env python3
"""허브용 summary.json / version.json 발행 (update-current.yml: fetch_current.py 다음 단계).

네트워크를 쓰지 않는다 — 커밋된 current.json·config.json만 읽는다. 데이터가 같으면
파일을 다시 쓰지 않으므로(write_if_changed) git diff가 생기지 않는다.
GitHub Actions에서는 ``summary_changed=true|false``를 $GITHUB_OUTPUT에 남긴다.
로직은 pipeline/hub_summary.py, 계약은 value-invest docs/ecosystem/data-contract.md §6.1.
"""

import os
from pathlib import Path

from pipeline.hub_summary import publish

BASE_DIR = Path(__file__).parent


def main():
    result = publish(BASE_DIR)
    envelope = result["envelope"]
    state = "갱신" if result["summaryChanged"] else "변경 없음"
    print(
        f"summary.json {state} — pairs {len(envelope['data']['pairs'])}, "
        f"asOf {envelope['asOf']}, {envelope['contentHash']}"
    )
    output = os.environ.get("GITHUB_OUTPUT")
    if output:
        with open(output, "a", encoding="utf-8") as fh:
            fh.write(f"summary_changed={'true' if result['summaryChanged'] else 'false'}\n")


if __name__ == "__main__":
    main()
