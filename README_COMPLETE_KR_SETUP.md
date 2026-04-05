# KR 완성본 운용 방식

이 패키지는 KRX OTP 대신 **Naver Finance 기반**으로 안정성을 우선한 버전입니다.

## 핵심 구조
- `index.html`은 건드리지 않습니다.
- `data/latest_krx.json`만 현재 페이지가 읽습니다.
- 유니버스는 `KOSPI 시총 상위 200 + KOSDAQ 시총 상위 200`으로 자동 생성합니다.
- 주도주는 이 400종목을 기준으로 계산합니다.

## 파일
- `scripts/build_universe_from_naver.py`
  - 네이버 시가총액 페이지에서 KOSPI 200 + KOSDAQ 200 종목을 만듭니다.
- `scripts/generate_latest_krx_from_naver.py`
  - 네이버 실시간 API로 등락률/거래대금을 받아 `data/latest_krx.json`을 생성합니다.
- `scripts/manual_build_universe.bat`
  - PC에서 유니버스 재생성
- `scripts/manual_update_latest.bat`
  - PC에서 최신 데이터 생성
- `.github/workflows/update-kr-universe-and-latest.yml`
  - GitHub Actions 자동 갱신

## PC에서 수동 실행
1. `scripts/manual_build_universe.bat`
2. `scripts/manual_update_latest.bat`
3. 생성된 `data/latest_krx.json`을 GitHub에 업로드

## GitHub Actions
1. Settings → Actions → General
2. Workflow permissions → Read and write permissions
3. Actions 탭 → `Update KR universe and latest data`
4. `Run workflow`

## 확인 포인트
- `data/universe_kr_top400.json`의 `counts.total`이 400 전후인지
- `data/latest_krx.json`의 `status`가 `ok`인지
- `counts.universe`가 300 이상인지
- `leaders`가 비어 있지 않은지

## 주의
- 이 버전은 **KRX 공식 장마감 거래대금**이 아니라, **Naver realtime 기준 거래대금**입니다.
- 대신 현재 환경에서 KRX OTP 실패 문제를 피하고 더 안정적으로 동작합니다.
