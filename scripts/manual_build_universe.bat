@echo off
cd /d "%~dp0.."
python -m pip install -r requirements.txt
python scripts/build_universe_from_naver.py
pause
