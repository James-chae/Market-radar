# Complete KR replacement (Top1000 + exact turnover)

1. Keep this package in a NEW folder.
2. Run `scripts/manual_build_universe.bat`.
3. Run `scripts/manual_update_latest.bat`.
4. Check `data/latest_krx.json`.
5. Upload all files to GitHub.

This version uses:
- universe: KOSPI500 + KOSDAQ500 by market cap
- intraday/post-close turnover: parsed from Naver item/main page (displayed 거래대금, 백만원 단위 -> 억원)
- no KRX OTP
