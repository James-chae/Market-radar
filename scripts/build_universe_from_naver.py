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

# 유니버스 필터 기준: 시가총액 3000억원 이상
MIN_MARKET_CAP_EOK = 1000  # 억원

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


def parse_market_cap_text_to_eok(text: str) -> Optional[float]:
    """
    Naver 시총 텍스트를 억원 단위로 변환.
    Naver sise_market_sum 기본 페이지: 숫자는 백만원(=0.01억) 단위로 표시.
    예: '3,456,789' → 34567.89억, '3조4568억' → 34568억
    """
    if not text:
        return None
    s = str(text).strip().replace(",", "").replace(" ", "").replace("\xa0", "")
    if not s or s in {"-", "--", "N/A"}:
        return None
    if "조" in s:
        m = re.search(r"([0-9]+(?:\.[0-9]+)?)조", s)
        if m:
            val = float(m.group(1)) * 10000
            m2 = re.search(r"조([0-9]+(?:\.[0-9]+)?)억", s)
            if m2:
                val += float(m2.group(1))
            return val
    if "억" in s:
        m = re.search(r"([0-9]+(?:\.[0-9]+)?)억", s)
        return float(m.group(1)) if m else None
    # 순수 숫자 → 백만원 단위
    try:
        return float(s) / 100
    except ValueError:
        return None


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
        "min_market_cap_eok": MIN_MARKET_CAP_EOK,
        "counts": {"KOSPI": 0, "KOSDAQ": 0, "total": 0},
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
    NAVER 시총 상위 페이지에서 종목코드 / 종목명 / 시가총액 파싱.

    Naver sise_market_sum 기본 컬럼 순서:
      0:순위 1:종목명 2:현재가 3:전일비 4:등락률 5:액면가
      6:시가총액 7:상장주식수 8:외국인비율 9:거래량 10:PER 11:ROE
    헤더에서 '시가총액' 텍스트로 컬럼 인덱스를 동적으로 찾음.
    """
    soup = BeautifulSoup(html_text, "lxml")
    table = soup.select_one("table.type_2")
    if not table:
        return []

    tbody = table.find("tbody")
    if not tbody:
        return []

    # 헤더에서 시가총액 컬럼 인덱스 탐색
    market_cap_col_idx: Optional[int] = None
    thead = table.find("thead")
    if thead:
        for i, th in enumerate(thead.find_all("th")):
            if "시가총액" in th.get_text(" ", strip=True).replace(" ", ""):
                market_cap_col_idx = i
                break

    rows: List[dict] = []
    for tr in tbody.find_all("tr"):
        a = tr.select_one("a[href*='item/main.naver?code=']")
        if not a:
            continue
        code = extract_code_from_href(a.get("href", ""))
        name = a.get_text(strip=True)
        if not code or not name:
            continue

        tds = tr.find_all("td")
        market_cap_eok: Optional[float] = None
        if market_cap_col_idx is not None and market_cap_col_idx < len(tds):
            market_cap_eok = parse_market_cap_text_to_eok(
                tds[market_cap_col_idx].get_text(strip=True)
            )

        rows.append({
            "code": code,
            "name": name,
            "market": market_name,
            "suffix": ".KQ" if market_name.upper() == "KOSDAQ" else ".KS",
            "market_cap_eok": market_cap_eok,
        })

    return rows


def fetch_market_cap_pages(
    session: requests.Session,
    sosok: int,
    market_name: str,
    max_pages: int = 40,
) -> List[dict]:
    """
    KOSPI / KOSDAQ 시총 상위 페이지 순회.
    시총 3000억 미만 종목만 남는 페이지에서 조기 종료.
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
        print(f"[PARSE] {market_name} page {page}: {len(rows)} rows parsed")

        if not rows:
            break

        before = len(all_rows)
        seen = {r["code"] for r in all_rows}
        added = 0
        for row in rows:
            if row["code"] in seen:
                continue
            cap = row.get("market_cap_eok")
            if cap is not None and cap < MIN_MARKET_CAP_EOK:
                # 시총순 정렬이므로 이 행 이후는 모두 기준 미달 → 페이지 순회 종료
                print(
                    f"[STOP] {market_name} page {page}: "
                    f"'{row['name']}' 시총 {cap:.0f}억 < {MIN_MARKET_CAP_EOK}억, 순회 종료"
                )
                all_rows.extend(
                    r for r in rows
                    if r["code"] not in seen
                    and (r.get("market_cap_eok") is None or r["market_cap_eok"] >= MIN_MARKET_CAP_EOK)
                )
                return all_rows
            all_rows.append(row)
            added += 1

        print(f"  → 누적 {len(all_rows)}종목 (이번 페이지 추가 {added})")

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
    kospi_top = dedupe_rows(kospi_rows)
    kosdaq_top = dedupe_rows(kosdaq_rows)
    items = kospi_top + kosdaq_top

    return {
        "generated_at": now_seoul().isoformat(),
        "source": "Naver Finance market cap ranking",
        "min_market_cap_eok": MIN_MARKET_CAP_EOK,
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

        print(f"[TOTAL] KOSPI  (시총 {MIN_MARKET_CAP_EOK}억+): {len(kospi_rows)}")
        print(f"[TOTAL] KOSDAQ (시총 {MIN_MARKET_CAP_EOK}억+): {len(kosdaq_rows)}")

        payload = build_payload(kospi_rows, kosdaq_rows)
        save_payload(payload)

        total = payload["counts"]["total"]
        print(f"[DONE] total universe items: {total}")

        if total == 0:
            err = build_error_payload(
                "파싱 결과가 0건입니다. debug/*.html 파일을 확인하세요."
            )
            save_payload(err)
            sys.exit(2)

        if total < 100:
            print(f"[WARN] universe items too small: {total}")

    except Exception as e:
        err = build_error_payload(f"실행 실패: {e}")
        save_payload(err)
        raise


if __name__ == "__main__":
    main()
