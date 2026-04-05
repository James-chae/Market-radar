# Trade Value Fix

핵심 수정
- `aq`를 거래대금으로 쓰지 않음.
- `aa / at / am / ta` 필드를 우선 사용하고, `100백만원 = 1억원` 가정으로 `/100` 적용.
- 금액 필드가 없으면 마지막 수단으로 `aq * nv / 1e8` 추정치를 사용하고 `≈` 표시.

적용 순서
1. 기존 잘 동작하는 `index.html`은 유지.
2. 거래대금 문제가 있으면 `index_tradevalue_fixed.html`의 관련 함수만 반영.
3. `scripts/generate_latest_krx_from_naver.py`를 교체.
4. `manual_update_latest.bat` 실행 후 `data/latest_krx.json` 업로드.
