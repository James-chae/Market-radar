전체 교체용 패키지

1) index.html 포함 전부 덮어쓰기
2) PC에서 scripts/manual_build_universe.bat 실행
3) 다음으로 scripts/manual_update_latest.bat 실행
4) 생성된 data/latest_krx.json을 GitHub에 업로드
5) 대시보드 새로고침 (?v=top400final)

주의:
- 예전 manual_update_krx.bat / generate_krx_json.py 는 사용하지 마세요.
- 이 패키지는 Naver realtime + TOP400 유니버스 기준입니다.
- 거래대금은 data/latest_krx.json 값이 우선 반영됩니다.
