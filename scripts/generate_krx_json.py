#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from io import StringIO
from pathlib import Path
from typing import Dict, List, Optional

import requests


SEOUL = timezone(timedelta(hours=9))
ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
UNIVERSE_PATH = DATA_DIR / "krx300_universe.json"
OUTPUT_PATH = DATA_DIR / "latest_krx.json"

GEN_OTP_URL = "https://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd"
DOWNLOAD_URL = "https://data.krx.co.kr/comm/fileDn/download_csv/download.cmd"
REFERER = "https://data.krx.co.kr/contents/MDC/MDI/mdiLoader"

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Referer": REFERER,
    "Origin": "https://data.krx.co.kr",
}

OTP_FORM = {
    "name": "fileDown",
    "filetype": "csv",
    "url": "dbms/MDC/STAT/standard/MDCSTAT01501",
    "locale": "ko_KR",
    "mktId": "ALL",
    "share": "1",
    "money": "1",
    "csvxls_isNo": "false",
}


def load_universe() -> List[dict]:
    with UNIVERSE_PATH.open("r", encoding="utf-8") as f:
        return json.load(f)


def yyyy_mm_dd(date_str: str) -> str:
    return f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}" if len(date_str) == 8 else date_str


def to_float(value: object) -> Optional[float]:
    if value is None:
        return None
    text = str(value).strip().replace(",", "")
    if not text or text in {"-", "—"}:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def pick(row: dict, *keys: str) -> str:
    for key in keys:
        if key in row and str(row[key]).strip():
            return str(row[key]).strip()
    return ""


def market_suffix(row: dict) -> str:
    market = pick(row, "MKT_NM", "시장구분", "시장")
    if "KOSDAQ" in market.upper() or "코스닥" in market:
        return ".KQ"
    return ".KS"


def format_pct(pct: float) -> str:
    return f"{pct:+.1f}%".replace("+0.0%", "0.0%")


def format_eok(eok: float) -> str:
    if eok >= 10000:
        return f"{eok / 10000:.2f}조"
    if eok >= 100:
        return f"{round(eok):,}억"
    return f"{eok:.1f}억"


def get_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(HEADERS)
    return s


def request_otp(session: requests.Session, trade_date: str) -> str:
    form = {**OTP_FORM, "trdDd": trade_date}
    r = session.post(GEN_OTP_URL, data=form, timeout=20)
    r.raise_for_status()
    otp = r.text.strip()
    if not otp:
        raise RuntimeError("OTP 응답이 비어 있습니다.")
    return otp


def download_csv(session: requests.Session, otp: str) -> str:
    r = session.post(DOWNLOAD_URL, data={"code": otp}, timeout=30)
    r.raise_for_status()
    text = r.content.decode("cp949", errors="replace")
    if "종목코드" not in text and "ISU_SRT_CD" not in text:
        raise RuntimeError("CSV 헤더를 찾지 못했습니다.")
    return text


def parse_rows(csv_text: str) -> List[dict]:
    reader = csv.DictReader(StringIO(csv_text))
    return [dict(row) for row in reader]


def pick_trade_date(session: requests.Session, max_back_days: int = 10) -> tuple[str, List[dict]]:
    now = datetime.now(SEOUL)
    for offset in range(max_back_days + 1):
        day = now - timedelta(days=offset)
        if day.weekday() >= 5:
            continue
        trade_date = day.strftime("%Y%m%d")
        try:
            otp = request_otp(session, trade_date)
            rows = parse_rows(download_csv(session, otp))
            if rows:
                return trade_date, rows
        except Exception:
            continue
    raise RuntimeError("최근 거래일 KRX CSV를 찾지 못했습니다.")


def build_payload(trade_date: str, rows: List[dict], universe: List[dict]) -> dict:
    universe_map = {str(item["code"]).zfill(6): item["name"] for item in universe}
    filtered = []
    by_symbol: Dict[str, dict] = {}

    for row in rows:
        code = pick(row, "ISU_SRT_CD", "종목코드", "단축코드", "단축코드 ")
        code = code.zfill(6)
        if code not in universe_map:
            continue

        name = pick(row, "ISU_ABBRV", "종목명", "한글 종목약명") or universe_map[code]
        pct = to_float(pick(row, "FLUC_RT", "등락률"))
        trade_value = to_float(pick(row, "ACC_TRDVAL", "거래대금"))
        if pct is None or trade_value is None:
            continue

        eok = trade_value / 100000000
        symbol = code + market_suffix(row)
        item = {
            "code": code,
            "symbol": symbol,
            "name": name,
            "market": pick(row, "MKT_NM", "시장구분", "시장") or ("KOSDAQ" if symbol.endswith(".KQ") else "KOSPI"),
            "close": to_float(pick(row, "TDD_CLSPRC", "종가")),
            "pct": round(pct, 4),
            "pct_text": format_pct(pct),
            "trade_value_krw": int(round(trade_value)),
            "trade_value_eok": round(eok, 4),
            "trade_value_text": format_eok(eok),
        }
        filtered.append(item)
        by_symbol[symbol] = item

    amount_ranked = sorted(filtered, key=lambda x: (-x["trade_value_eok"], -x["pct"]))
    rise_ranked = sorted(filtered, key=lambda x: (-x["pct"], -x["trade_value_eok"]))

    top_current = {item["symbol"] for item in amount_ranked[:30]}
    top_rise = {item["symbol"] for item in rise_ranked[:30]}
    leaders = [
        item for item in amount_ranked
        if item["symbol"] in top_current and item["symbol"] in top_rise
    ]

    payload = {
        "generated_at": datetime.now(SEOUL).isoformat(),
        "trade_date": trade_date,
        "trade_date_display": yyyy_mm_dd(trade_date),
        "source": "KRX OTP 장마감 기준",
        "universe_name": "KRX300",
        "counts": {
            "universe": len(filtered),
            "top_current_30": min(30, len(amount_ranked)),
            "top_rise_30": min(30, len(rise_ranked)),
            "leaders": len(leaders),
        },
        "leaders": leaders,
        "by_symbol": by_symbol,
    }
    return payload


def main() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    session = get_session()
    universe = load_universe()
    trade_date, rows = pick_trade_date(session)
    payload = build_payload(trade_date, rows, universe)
    OUTPUT_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"saved {OUTPUT_PATH} ({payload['trade_date_display']}, {payload['counts']['universe']} symbols)")


if __name__ == "__main__":
    main()
