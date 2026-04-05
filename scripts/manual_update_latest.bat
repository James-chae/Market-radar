@echo off
cd /d "%~dp0\.."
python scripts\generate_latest_krx_from_naver.py
pause
