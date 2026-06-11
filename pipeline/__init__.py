"""보유지분가치 파이프라인의 순수 로직 패키지.

stdlib만 사용하는 계산·병합·검증 로직을 모아둔다 (pandas/yfinance 금지).
fetch_data.py는 pipeline.core를, fetch_current.py는 pipeline.snapshot을
명시적으로 재수출하므로 기존 fetch_data.X / fetch_current.X 참조는 그대로 동작한다.
"""
