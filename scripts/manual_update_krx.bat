@echo off
cd /d "%~dp0\.."
python -m pip install -r requirements.txt
python scripts\generate_krx_json.py
pause
