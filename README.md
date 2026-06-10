# 지주사 보유지분가치 대시보드 (holding_value)

지주회사의 **상장 자회사 보유지분가치 ÷ 자사 조정시가총액** 비율을 추적하는 정적 대시보드.
GitHub Actions가 데이터를 수집해 저장소에 커밋하고, GitHub Pages가 그대로 서빙한다
(서버리스, 빌드 도구 없음). 자매 프로젝트: [common_preferred_spread](https://github.com/ducklove/common_preferred_spread)
— 우선주 괴리율 대시보드로 같은 아키텍처를 공유한다.

## 아키텍처

```
Yahoo Finance ─┐
KIS Open API ──┤   fetch_data.py    (일 1회, KST 05:00)  →  data.js     (전체 히스토리)
KIS 프록시 ────┼─→ fetch_current.py (주중 10분 간격)      →  current.js / current.json
내부 가격 API ─┘   build_holdings_api.py (config push 시) →  api/holdings.json
                          │ git commit/push (저장소 = DB)
                          ▼
            GitHub Pages → index.html (대시보드) / admin.html (종목 관리)
```

| 파일 | 역할 |
|---|---|
| `config.json` | 종목 정의 (admin.html에서 관리). pair별 선택 필드 `validFrom` 지원 |
| `fetch_data.py` | 일별 히스토리 파이프라인 (증분 병합, 주간 다운샘플, 품질 가드) |
| `fetch_current.py` | 장중 스냅샷 (세션 인지 보존, KIS → KIS 프록시 → yfinance 폴백) |
| `price_api.py` | 내부 가격 API 어댑터 (LAN 전용 — CI에서는 자동 스킵) |
| `build_holdings_api.py` | `api/holdings.json` 생성 (config.json push 시 워크플로우가 실행) |
| `data/summary.json` | 생성 산출물 — 메타+현재가 (~25KB). 프런트 첫 화면의 데이터 소스 |
| `data/history/{id}.json` | 생성 산출물 — 종목별 컬럼형 히스토리. 선택 종목만 지연 로드 |
| `data.js` / `current.js(.json)` | 생성 산출물 — 직접 편집 금지. data.js는 분할 로드 실패 시 폴백(과도기) |
| `docs/refactoring_review_202606.html` | 구조·품질 리뷰 보고서 및 로드맵 |

## 데이터 규칙

- **비율** = Σ(자회사 `sharesHeld` × 자회사 종가[× 환율]) ÷ (조정주식수 × 지주사 종가) × 100.
  조정주식수 = `holdingAdjustedShares`(있으면) 또는 발행주식수 − 자사주.
- **validFrom** (pair 선택 필드, `YYYY-MM-DD`): 현 지분 구조 성립일. 이 날짜 이전 구간은
  히스토리를 생성하지 않는다. 분할·지주전환 종목에서 현재 지분을 과거에 소급해
  허구 비율이 생기는 것을 막는다. *(분할 신설·지주 전환 종목은 반드시 설정할 것.
  파이프라인이 최대 비율 1,500% 초과 종목을 validFrom 후보로 경고한다.)*
  예: `seah_holdings`는 기아특수강(001430)이 2003-11 세아그룹에 인수되어
  `validFrom: 2004-01-01` — 설정 전에는 2002년에 4,006% 같은 허구 비율이 기록됐었다.
- **전체 중앙값** (`_average`): 그날 데이터가 있는 종목 비율의 **중앙값**.
  구성 종목이 20개 미만인 날짜는 산출하지 않는다(소표본 왜곡 방지).
  `mean`(단순평균)·`count`(구성 종목 수)가 함께 기록된다.
- **보존 정책**: 최근 730일은 일별, 그 이전은 ISO 주 단위(주 마지막 거래일)로 다운샘플.
- **신규 종목 추가 시**: 해당 종목만 전체 기간을 수집하고 기존 종목의 누적 히스토리는
  유지한다. (과거의 "신규 1개 = 전 종목 풀 재빌드" 동작은 제거됨)
- **증분 수집 시작일**: 가장 뒤처진 종목의 마지막 날짜 −5일 기준(거래정지 복귀 시
  공백 자동 회복), 단 최신 종목 기준 90일 하한(영구 정지 종목의 무한 후행 방지).
- **분할 산출물 포맷**: `data/history/{id}.json`은 컬럼형
  (`{dates:[], holdingPrice:[], ratio:[], …, subs:{이름:{price,value,ratio}}}`).
  프런트의 `rowsFromColumnar`가 행 배열로 복원하며, 행 포맷과 무손실 동치임을
  pytest와 브라우저 스모크 테스트로 고정해 두었다. data.js 폴백은 분할 로드
  실패 시(배포 전환기·롤백)에만 쓰인다 — 안정화 후 제거 예정.

## 데이터 품질 가드

`fetch_data.py`는 산출물을 쓰기 전에 직전 `data.js`와 비교해 다음을 검사한다:

- **실패(커밋 차단)**: 종목 히스토리 시작일 후퇴, 포인트 수 10% 초과 감소
  (재수집 대상·validFrom 의도적 절단은 경고로 완화)
- **경고(로그만)**: 최대 비율 1,500% 초과(validFrom 후보), 마지막 데이터가 전체
  최신일보다 14일 이상 뒤처진 종목(거래정지 등)

가드 실패 시 워크플로우가 빨갛게 실패하고 data.js는 변경되지 않는다.
직전 데이터는 매 실행 전 **Actions 아티팩트(`data-backup-<run_id>`, 90일 보존)**로
백업되므로, 잘못된 커밋이 발생해도 아티팩트를 내려받아 복원할 수 있다.

## 운영

- **종목 추가/편집**: `admin.html` (GitHub Pages) → fine-grained PAT(Contents RW) 필요.
  저장하면 config.json 커밋 → holdings 워크플로우가 api/holdings.json 재생성 →
  data 워크플로우가 신규 종목만 수집.
- **전체 재생성**: admin의 "전체 재생성" 버튼 또는 `update-data.yml` 수동 실행에서
  `full_rebuild: true`. 전 종목이 현재 시점 Yahoo 조정가로 재계산되므로 과거 비율이
  소급 변동한다 — 꼭 필요할 때만.
- **스케줄**: data 일 1회(20:00 UTC = KST 05:00), current 주중 10분 간격, CI(pytest)는
  Python 변경 시.
- **동시성**: 세 워크플로우가 `data-commit` concurrency 그룹을 공유하고, push는
  `pull --rebase` + 4회 재시도.

### 환경 변수 / Secrets

| 이름 | 용도 | 기본값 |
|---|---|---|
| `KIS_APP_KEY` / `KIS_APP_SECRET` (Secrets) | KIS Open API 직접 호출 (1차 시세 소스) | 미설정 시 프록시로 폴백 |
| `KIS_PROXY_BASE_URL` (Variable) | KIS 프록시 주소 | `http://cantabile.tplinkdns.com:3288` |
| `HOLDING_VALUE_PRICE_API_URL` | 내부 가격 API (LAN) | `http://192.168.68.84:8400/...` |
| `HOLDING_VALUE_PRICE_API` | `0`이면 내부 API 비활성 | `1` |

> **권고**: 현재 장중 시세의 사실상 전부가 개인 프록시 한 대에 의존한다.
> 저장소 Secrets에 KIS 키를 설정해 직접 호출을 1차로 승격할 것.

## 로컬 실행

```bash
pip install -r requirements.txt
python fetch_data.py            # 증분 갱신 (data.js)
python fetch_data.py --full     # 전체 재생성 (주의: 과거 값 소급 변동)
python fetch_current.py         # 장중 스냅샷 (current.js/current.json)
python -m pytest -q             # 테스트
python -m http.server 8000      # 대시보드: http://localhost:8000
```

## 알려진 한계 / 로드맵

상세는 `docs/refactoring_review_202606.html` 참고. 요약:

- 다중 자회사 페어는 막내 자회사 상장일부터 히스토리가 시작된다(교집합 설계).
- 차트 x축이 인덱스 간격이라 730일 이전 주간 구간이 시간적으로 압축되어 보인다
  (Phase 3 시간축 전환 예정).
- admin.html의 PAT는 localStorage에 저장된다. 공용 브라우저에서 사용 금지,
  사용 후 "토큰 삭제" 권장.
- 세아 외 분할·지주전환 종목(코스맥스비티아이, 한국콜마홀딩스, 오리온홀딩스 등)의
  validFrom은 사업보고서 확인 후 채워야 한다.
