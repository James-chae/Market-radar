#!/usr/bin/env python3
from __future__ import annotations

import json
import math
import re
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup

SEOUL = timezone(timedelta(hours=9))
ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
DEBUG_DIR = ROOT / "debug"

UNIVERSE_PATH = DATA_DIR / "universe_kr_top1000.json"
OUTPUT_PATH = DATA_DIR / "latest_krx.json"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
    "Referer": "https://finance.naver.com/",
}


def now_seoul() -> datetime:
    return datetime.now(SEOUL)


def ensure_dirs() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    DEBUG_DIR.mkdir(parents=True, exist_ok=True)


def load_universe() -> List[dict]:
    with UNIVERSE_PATH.open("r", encoding="utf-8") as f:
        data = json.load(f)
    return data["items"] if isinstance(data, dict) else data


def save_payload(payload: dict) -> None:
    ensure_dirs()
    OUTPUT_PATH.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def build_payload(merged_rows: List[dict]) -> dict:
    amount_ranked = sorted(
        merged_rows,
        key=lambda x: (-float(x["trade_value_eok"]), -float(x["pct"]))
    )
    rise_ranked = sorted(
        merged_rows,
        key=lambda x: (-float(x["pct"]), -float(x["trade_value_eok"]))
    )

    # ✅ 수정: 30 → 50
    top_c = {item["symbol"] for item in amount_ranked[:50]}
    top_e = {item["symbol"] for item in rise_ranked[:50]}

    leaders = [item for item in amount_ranked if item["symbol"] in top_c and item["symbol"] in top_e]

    return {
        "generated_at": now_seoul().isoformat(),
        "counts": {
            "universe": len(merged_rows),
            "top_current_50": min(50, len(amount_ranked)),  # ✅ 수정
            "top_rise_50": min(50, len(rise_ranked)),
            "leaders": len(leaders),
        },
        "leaders": leaders,
    }


def main() -> None:
    universe = load_universe()
    payload = build_payload(universe)
    save_payload(payload)


if __name__ == "__main__":
    main()
