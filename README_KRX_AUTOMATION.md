# KRX 거래대금 자동화 패키지

이 패키지는 현재 `index.html`을 건드리지 않고 `data/latest_krx.json`만 자동 갱신하기 위한 묶음입니다.

## 구성
- `scripts/generate_krx_json.py`
  - KRX OTP → CSV 다운로드 → `data/latest_krx.json` 생성
  - 실패하면 기존 JSON 유지
- `.github/workflows/update-krx-data.yml`
  - GitHub Actions 수동 실행 / 평일 장마감 후 스케줄 실행
- `scripts/manual_update_krx.bat`
  - Windows에서 더블클릭 실행용
- `data/krx300_universe.json`
  - KRX300 유니버스
- `data/latest_krx.json`
  - 초기 샘플 파일

## 권장 운영 방식
1. **가장 안정적**
   - PC에서 `manual_update_krx.bat` 실행
   - 생성된 `data/latest_krx.json`만 GitHub에 업로드
2. **보조 자동화**
   - GitHub Actions도 켜두기
   - 다만 GitHub 서버에서 KRX 응답이 막히면 기존 JSON을 유지하도록 설계됨

## Windows 로컬 실행
```bash
pip install -r requirements.txt
python scripts/generate_krx_json.py
```

또는 `scripts/manual_update_krx.bat` 더블클릭

## GitHub Actions 설정
- `Settings > Actions > General > Workflow permissions`
- `Read and write permissions` 로 변경

## 결과 확인
- `data/latest_krx.json`
- 성공 시:
  - `"status": "ok"`
  - `"fallback_used": false`
- 실패 시:
  - 기존 파일 유지
  - `"status": "fallback"`
  - `"fallback_used": true`
