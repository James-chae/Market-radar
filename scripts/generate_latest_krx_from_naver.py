#!/usr/bin/env python3
from __future__ import annotations

import json
import math
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional

import requests

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
UNIVERSE_PATH = DATA_DIR / "universe_kr_top400.json"
OUTPUT_PATH = DATA_DIR / "latest_krx.json"

SEOUL = timezone(timedelta(hours=9))
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Referer": "https://finance.naver.com/",
}

DEFAULT_EMPTY = {
    "generated_at": "",
    "trade_date": "",
    "trade_date_display": "",
    "source": "Naver realtime / market-cap top universe",
    "universe_name": "KOSPI200 + KOSDAQ200 by market cap",
    "counts": {"universe": 0, "top_current_30": 0, "top_rise_30": 0, "leaders": 0},
    "leaders": [],
    "by_symbol": {},
    "status": "fallback",
    "fallback_used": True,
    "message": "",
}

def now_kst() -> datetime:
    return datetime.now(SEOUL)

def format_trade_date(dt: datetime) -> str:
    return dt.strftime("%Y%m%d")

def format_trade_date_display(yyyymmdd: str) -> str:
    if len(yyyymmdd) != 8:
        return ""
    return f"{yyyymmdd[:4]}-{yyyymmdd[4:6]}-{yyyymmdd[6:8]}"

def format_pct(pct: float) -> str:
    return f"{pct:+.1f}%".replace("+0.0%", "0.0%")

def format_eok(eok: float) -> str:
    if eok >= 10000:
        return f"{eok / 10000:.2f}조"
    if eok >= 100:
        return f"{round(eok):,}억"
    return f"{eok:.1f}억"

def normalize_amount_to_eok(amount_raw: float) -> Optional[float]:
    if amount_raw is None or not math.isfinite(amount_raw):
        return None
    if amount_raw >= 100000000:
        return amount_raw / 100000000
    if amount_raw >= 10000:
        return amount_raw / 100
    return amount_raw

def load_universe() -> List[dict]:
    if not UNIVERSE_PATH.exists():
        raise FileNotFoundError(f"Universe file not found: {UNIVERSE_PATH}")
    raw = json.loads(UNIVERSE_PATH.read_text(encoding="utf-8"))
    items = raw["items"] if isinstance(raw, dict) and "items" in raw else raw
    if not isinstance(items, list):
        raise ValueError("Invalid universe payload")
    return items

def chunked(seq: List[str], size: int) -> List[List[str]]:
    return [seq[i:i+size] for i in range(0, len(seq), size)]

def fetch_naver_realtime(codes: List[str]) -> List[dict]:
    session = requests.Session()
    session.headers.update(HEADERS)
    rows: List[dict] = []

    for group in chunked(codes, 50):
        query = ",".join(group)
        url = f"https://polling.finance.naver.com/api/realtime?query=SERVICE_ITEM:{query}"
        r = session.get(url, timeout=20)
        r.raise_for_status()
        data = r.json()
        area_rows = data.get("result", {}).get("areas", [{}])[0].get("datas", [])
        rows.extend(area_rows)

    return rows

def load_existing() -> Optional[dict]:
    if not OUTPUT_PATH.exists():
        return None
    try:
        return json.loads(OUTPUT_PATH.read_text(encoding="utf-8"))
    except Exception:
        return None

def save_payload(payload: dict) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

def build_payload(universe: List[dict], realtime_rows: List[dict]) -> dict:
    code_map = {str(item["code"]).zfill(6): item for item in universe}
    by_symbol: Dict[str, dict] = {}

    for row in realtime_rows:
        code = str(row.get("cd", "")).zfill(6)
        if code not in code_map:
            continue
        base = code_map[code]
        market = base.get("market", "KOSPI")
        suffix = ".KQ" if market == "KOSDAQ" else ".KS"
        symbol = code + suffix

        pct = float(row.get("cr", 0) or 0)
        amount_raw = row.get("aq")
        amount = normalize_amount_to_eok(float(amount_raw)) if amount_raw not in (None, "") else None
        if amount is None:
            continue

        item = {
            "code": code,
            "symbol": symbol,
            "name": base.get("name") or row.get("nm") or code,
            "market": market,
            "close": None,
            "pct": round(pct, 4),
            "pct_text": format_pct(pct),
            "trade_value_eok": round(amount, 4),
            "trade_value_text": format_eok(amount),
            "market_cap_rank": base.get("rank"),
            "market_cap_text": base.get("market_cap_text"),
        }
        by_symbol[symbol] = item

    rows = list(by_symbol.values())
    amount_ranked = sorted(rows, key=lambda x: (-x["trade_value_eok"], -x["pct"]))
    rise_ranked = sorted(rows, key=lambda x: (-x["pct"], -x["trade_value_eok"]))

    top_amount = {x["symbol"] for x in amount_ranked[:30]}
    top_rise = {x["symbol"] for x in rise_ranked[:30]}
    leaders = [x for x in amount_ranked if x["symbol"] in top_amount and x["symbol"] in top_rise]

    trade_date = format_trade_date(now_kst())
    return {
        "generated_at": now_kst().isoformat(),
        "trade_date": trade_date,
        "trade_date_display": format_trade_date_display(trade_date),
        "source": "Naver realtime 기준 · KOSPI200 + KOSDAQ200 시총상위 유니버스",
        "universe_name": "KOSPI200 + KOSDAQ200 by market cap",
        "counts": {
            "universe": len(rows),
            "top_current_30": min(30, len(amount_ranked)),
            "top_rise_30": min(30, len(rise_ranked)),
            "leaders": len(leaders),
        },
        "leaders": leaders,
        "by_symbol": by_symbol,
        "status": "ok",
        "fallback_used": False,
        "message": "",
    }

def main() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    try:
        universe = load_universe()
        codes = [str(item["code"]).zfill(6) for item in universe]
        realtime_rows = fetch_naver_realtime(codes)
        payload = build_payload(universe, realtime_rows)
        save_payload(payload)
        print(f"[OK] saved {OUTPUT_PATH} with {payload['counts']['universe']} symbols / leaders={payload['counts']['leaders']}")
    except Exception as e:
        msg = f"Naver universe update failed: {e}"
        existing = load_existing()
        if existing is not None:
            existing["generated_at"] = now_kst().isoformat()
            existing["status"] = "fallback"
            existing["fallback_used"] = True
            existing["message"] = msg
            save_payload(existing)
            print(f"[FALLBACK] kept existing latest_krx.json: {msg}")
            return
        payload = dict(DEFAULT_EMPTY)
        payload["generated_at"] = now_kst().isoformat()
        payload["message"] = msg
        save_payload(payload)
        print(f"[FALLBACK] created empty latest_krx.json: {msg}")

if __name__ == "__main__":
    main()
