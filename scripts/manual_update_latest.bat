@echo off
cd /d "%~dp0.."
python -m pip install -r requirements.txt
python scripts/generate_latest_krx_from_naver.py
pause
