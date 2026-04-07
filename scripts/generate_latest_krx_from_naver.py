#!/usr/bin/env python3
from __future__ import annotations

import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List

SEOUL = timezone(timedelta(hours=9))
ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
UNIVERSE_PATH = DATA_DIR / "universe_kr_top1000.json"
OUTPUT_PATH = DATA_DIR / "latest_krx.json"


def now_seoul() -> datetime:
    return datetime.now(SEOUL)


def load_json(path: Path):
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def has_required_market_fields(rows: List[dict]) -> bool:
    if not rows:
        return False
    sample_hits = 0
    for row in rows[:50]:
        if isinstance(row, dict) and ("pct" in row) and ("trade_value_eok" in row):
            sample_hits += 1
    return sample_hits >= 5


def main() -> None:
    if not UNIVERSE_PATH.exists():
        raise FileNotFoundError(f"universe file not found: {UNIVERSE_PATH}")

    universe_raw = load_json(UNIVERSE_PATH)
    rows = universe_raw.get("items", []) if isinstance(universe_raw, dict) else universe_raw

    # 안전장치:
    # 현재 업로드된 universe 파일은 시총 유니버스 목록일 뿐,
    # pct / trade_value_eok 같은 장마감 계산 필드가 없습니다.
    # 이런 상태에서 latest_krx.json을 새로 쓰면 기존 정상 데이터가 깨지므로
    # 여기서는 덮어쓰기를 막고 명확하게 실패시킵니다.
    if not has_required_market_fields(rows):
        message = (
            "latest_krx.json 생성 중단: universe_kr_top1000.json에는 "
            "pct / trade_value_eok 필드가 없어 장마감 주도주 데이터를 계산할 수 없습니다. "
            "기존 latest_krx.json 보존."
        )
        print(message)
        if OUTPUT_PATH.exists():
            existing = load_json(OUTPUT_PATH)
            existing["status"] = existing.get("status", "ok")
            existing["message"] = message
            existing["guarded_at"] = now_seoul().isoformat()
            OUTPUT_PATH.write_text(json.dumps(existing, ensure_ascii=False, indent=2), encoding="utf-8")
            print(f"[SAFE] preserved existing -> {OUTPUT_PATH}")
            return
        raise RuntimeError(message)

    # 아래 분기는 현재 구조상 실행되기 어렵지만,
    # 향후 rows에 실제 pct/trade_value_eok가 채워질 경우에만 동작하도록 유지.
    amount_ranked = sorted(rows, key=lambda x: (-float(x["trade_value_eok"]), -float(x["pct"])))
    rise_ranked = sorted(rows, key=lambda x: (-float(x["pct"]), -float(x["trade_value_eok"])))

    top_c = {item["symbol"] for item in amount_ranked[:50]}
    top_e = {item["symbol"] for item in rise_ranked[:50]}
    leaders = [item for item in amount_ranked if item["symbol"] in top_c and item["symbol"] in top_e]

    payload = {
        "generated_at": now_seoul().isoformat(),
        "counts": {
            "universe": len(rows),
            "top_current_50": min(50, len(amount_ranked)),
            "top_rise_50": min(50, len(rise_ranked)),
            "leaders": len(leaders),
        },
        "leaders": leaders,
        "by_symbol": {row["symbol"]: row for row in rows if isinstance(row, dict) and row.get("symbol")},
        "status": "ok",
        "message": "",
    }
    OUTPUT_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[OK] saved -> {OUTPUT_PATH}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"[ERROR] {e}")
        sys.exit(1)
