#!/usr/bin/env python3
from __future__ import annotations

import json
import re
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional

import requests
from bs4 import BeautifulSoup


SEOUL = timezone(timedelta(hours=9))
ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
DEBUG_DIR = ROOT / "debug"

OUTPUT_PATH = DATA_DIR / "universe_kr_top1000.json"

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


def read_text_with_fallback(raw: bytes) -> str:
    for enc in ("euc-kr", "cp949", "utf-8"):
        try:
            return raw.decode(enc)
        except Exception:
            pass
    return raw.decode("utf-8", errors="replace")


def save_debug_html(name: str, html_text: str) -> None:
    ensure_dirs()
    path = DEBUG_DIR / name
    path.write_text(html_text, encoding="utf-8")
    print(f"[DEBUG] saved html -> {path}")


def extract_code_from_href(href: str) -> Optional[str]:
    if not href:
        return None
    m = re.search(r"code=(\d{6})", href)
    return m.group(1) if m else None


def save_payload(payload: dict) -> None:
    ensure_dirs()
    OUTPUT_PATH.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(f"[OK] saved -> {OUTPUT_PATH}")


def build_error_payload(message: str) -> dict:
    return {
        "generated_at": now_seoul().isoformat(),
        "source": "Naver Finance market cap ranking",
        "counts": {
            "KOSPI": 0,
            "KOSDAQ": 0,
            "total": 0,
        },
        "items": [],
        "status": "error",
        "message": message,
    }


def build_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(HEADERS)
    return s


def fetch_html(
    session: requests.Session,
    url: str,
    *,
    params: Optional[dict] = None,
    debug_name: str = "",
) -> str:
    r = session.get(url, params=params, timeout=20)
    r.raise_for_status()
    text = read_text_with_fallback(r.content)
    if debug_name:
        save_debug_html(debug_name, text)
    return text


def parse_market_cap_table(html_text: str, market_name: str) -> List[dict]:
    """
    NAVER 시총 상위 페이지에서 종목코드/종목명 파싱.
    URL 예:
      https://finance.naver.com/sise/sise_market_sum.naver?page=1
      https://finance.naver.com/sise/sise_market_sum.naver?sosok=1&page=1
    """
    soup = BeautifulSoup(html_text, "lxml")
    table = soup.select_one("table.type_2")
    if not table:
        return []

    tbody = table.find("tbody")
    if not tbody:
        return []

    rows: List[dict] = []

    for tr in tbody.find_all("tr"):
        a = tr.select_one("a[href*='item/main.naver?code=']")
        if not a:
            continue

        code = extract_code_from_href(a.get("href", ""))
        name = a.get_text(strip=True)

        if not code or not name:
            continue

        rows.append({
            "code": code,
            "name": name,
            "market": market_name,
            "suffix": ".KQ" if market_name.upper() == "KOSDAQ" else ".KS",
        })

    return rows


def fetch_market_cap_pages(
    session: requests.Session,
    sosok: int,
    market_name: str,
    max_pages: int = 25,
) -> List[dict]:
    """
    KOSPI / KOSDAQ 시총 상위 페이지 순회.
    """
    base_url = "https://finance.naver.com/sise/sise_market_sum.naver"
    all_rows: List[dict] = []

    for page in range(1, max_pages + 1):
        params = {"page": page}
        if sosok:
            params["sosok"] = sosok

        html = fetch_html(
            session,
            base_url,
            params=params,
            debug_name=f"debug_universe_{market_name.lower()}_p{page}.html",
        )

        rows = parse_market_cap_table(html, market_name)
        print(f"[PARSE] {market_name} page {page}: {len(rows)} rows")

        if not rows:
            break

        before = len(all_rows)
        seen = {r["code"] for r in all_rows}
        for row in rows:
            if row["code"] not in seen:
                all_rows.append(row)

        if len(all_rows) == before:
            break

        time.sleep(0.2)

    return all_rows


def dedupe_rows(rows: List[dict]) -> List[dict]:
    out: List[dict] = []
    seen = set()
    for row in rows:
        code = row["code"]
        if code in seen:
            continue
        seen.add(code)
        out.append(row)
    return out


def build_payload(kospi_rows: List[dict], kosdaq_rows: List[dict]) -> dict:
    kospi_top = dedupe_rows(kospi_rows)[:500]
    kosdaq_top = dedupe_rows(kosdaq_rows)[:500]
    items = kospi_top + kosdaq_top

    return {
        "generated_at": now_seoul().isoformat(),
        "source": "Naver Finance market cap ranking",
        "counts": {
            "KOSPI": len(kospi_top),
            "KOSDAQ": len(kosdaq_top),
            "total": len(items),
        },
        "items": items,
        "status": "ok",
        "message": "",
    }


def main() -> None:
    ensure_dirs()
    session = build_session()

    try:
        kospi_rows = fetch_market_cap_pages(session, sosok=0, market_name="KOSPI")
        kosdaq_rows = fetch_market_cap_pages(session, sosok=1, market_name="KOSDAQ")

        print(f"[TOTAL] KOSPI parsed rows : {len(kospi_rows)}")
        print(f"[TOTAL] KOSDAQ parsed rows: {len(kosdaq_rows)}")

        payload = build_payload(kospi_rows, kosdaq_rows)
        save_payload(payload)

        total = payload["counts"]["total"]
        print(f"[DONE] total universe items: {total}")

        if total == 0:
            err = build_error_payload(
                "Naver 시총 상위 표 파싱 결과가 0건입니다. debug/*.html 파일을 확인하세요."
            )
            save_payload(err)
            sys.exit(2)

        # 400 미만이어도 일단 저장은 하되, 경고 출력
        if total < 800:
            print(f"[WARN] universe items too small: {total}")

    except Exception as e:
        err = build_error_payload(f"실행 실패: {e}")
        save_payload(err)
        raise


if __name__ == "__main__":
    main()