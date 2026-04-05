#!/usr/bin/env python3
from __future__ import annotations

import json
import re
import time
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import List, Optional

import requests
from bs4 import BeautifulSoup

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
OUTPUT_PATH = DATA_DIR / "universe_kr_top400.json"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Referer": "https://finance.naver.com/",
}

MARKETS = [
    {"name": "KOSPI", "sosok": 0, "suffix": ".KS", "target": 200},
    {"name": "KOSDAQ", "sosok": 1, "suffix": ".KQ", "target": 200},
]

@dataclass
class UniverseRow:
    code: str
    symbol: str
    name: str
    market: str
    market_cap_text: str
    market_cap_eok: Optional[float]
    rank: int


def decode_response(resp: requests.Response) -> str:
    for enc in (resp.encoding, resp.apparent_encoding, 'euc-kr', 'cp949', 'utf-8'):
        if not enc:
            continue
        try:
            return resp.content.decode(enc, errors='replace')
        except Exception:
            pass
    return resp.text


def parse_market_cap_to_eok(text: str) -> Optional[float]:
    if not text:
        return None
    cleaned = re.sub(r"\s+", "", text).replace(",", "")
    if cleaned in {"", "-", "N/A"}:
        return None
    total = 0.0
    m = re.search(r"([\d.]+)조", cleaned)
    if m:
        total += float(m.group(1)) * 10000
    m = re.search(r"([\d.]+)억", cleaned)
    if m:
        total += float(m.group(1))
    return total or None


def get_page(session: requests.Session, sosok: int, page: int) -> str:
    url = f"https://finance.naver.com/sise/sise_market_sum.naver?sosok={sosok}&page={page}"
    r = session.get(url, headers=HEADERS, timeout=20)
    r.raise_for_status()
    return decode_response(r)


def extract_last_page(html: str) -> int:
    soup = BeautifulSoup(html, 'lxml')
    pgrr = soup.select_one('td.pgRR a')
    if pgrr and pgrr.get('href'):
        m = re.search(r"page=(\d+)", pgrr['href'])
        if m:
            return int(m.group(1))
    return 1


def parse_rows(html: str, market_name: str, suffix: str, start_rank: int) -> List[UniverseRow]:
    soup = BeautifulSoup(html, 'lxml')
    table = soup.select_one('table.type_2')
    if not table:
        return []
    out: List[UniverseRow] = []
    seen = set()
    for tr in table.select('tr'):
        a = tr.select_one('a.tltle')
        if not a:
            continue
        href = a.get('href', '')
        m = re.search(r"code=(\d{6})", href)
        if not m:
            continue
        code = m.group(1)
        if code in seen:
            continue
        seen.add(code)
        tds = tr.select('td')
        if len(tds) < 7:
            continue
        market_cap_text = tds[6].get_text(' ', strip=True)
        out.append(UniverseRow(
            code=code,
            symbol=code + suffix,
            name=a.get_text(' ', strip=True),
            market=market_name,
            market_cap_text=market_cap_text,
            market_cap_eok=parse_market_cap_to_eok(market_cap_text),
            rank=start_rank + len(out) + 1,
        ))
    return out


def build_universe() -> List[UniverseRow]:
    session = requests.Session()
    session.headers.update(HEADERS)
    all_rows: List[UniverseRow] = []
    for market in MARKETS:
        first_html = get_page(session, market['sosok'], 1)
        last_page = extract_last_page(first_html)
        collected: List[UniverseRow] = []
        collected.extend(parse_rows(first_html, market['name'], market['suffix'], 0))
        page = 2
        while len(collected) < market['target'] and page <= last_page:
            html = get_page(session, market['sosok'], page)
            rows = parse_rows(html, market['name'], market['suffix'], len(collected))
            if not rows:
                break
            collected.extend(rows)
            page += 1
            time.sleep(0.15)
        collected = collected[:market['target']]
        all_rows.extend(collected)
        print(f"[{market['name']}] collected {len(collected)}")
    return all_rows


def main() -> None:
    rows = build_universe()
    payload = {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "universe_name": "KOSPI200 + KOSDAQ200 by market cap",
        "counts": {"total": len(rows), "kospi": 200, "kosdaq": 200},
        "items": [asdict(r) for r in rows],
    }
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
    print(f"saved {OUTPUT_PATH} ({len(rows)} items)")


if __name__ == '__main__':
    main()
