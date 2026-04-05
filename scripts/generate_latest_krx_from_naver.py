#!/usr/bin/env python3
from __future__ import annotations

import json
import math
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup

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
    "source": "Naver item main exact value",
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


def to_float(value) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value) if math.isfinite(value) else None
    text = str(value).strip().replace(',', '')
    if not text:
        return None
    try:
        num = float(text)
    except ValueError:
        return None
    return num if math.isfinite(num) else None


def load_universe() -> List[dict]:
    raw = json.loads(UNIVERSE_PATH.read_text(encoding="utf-8"))
    return raw["items"] if isinstance(raw, dict) and "items" in raw else raw


def chunked(seq: List[str], size: int) -> List[List[str]]:
    return [seq[i:i+size] for i in range(0, len(seq), size)]


def fetch_naver_realtime(codes: List[str]) -> Dict[str, dict]:
    session = requests.Session()
    session.headers.update(HEADERS)
    out: Dict[str, dict] = {}
    for group in chunked(codes, 80):
        query = ",".join(group)
        url = f"https://polling.finance.naver.com/api/realtime?query=SERVICE_ITEM:{query}"
        r = session.get(url, timeout=20)
        r.raise_for_status()
        data = r.json()
        area_rows = data.get("result", {}).get("areas", [{}])[0].get("datas", [])
        for row in area_rows:
            cd = str(row.get("cd", "")).zfill(6)
            out[cd] = row
    return out


def extract_digits(text: str) -> Optional[float]:
    if not text:
        return None
    nums = re.findall(r"[\d,.]+", text)
    if not nums:
        return None
    try:
        return float(nums[-1].replace(',', ''))
    except Exception:
        return None



def parse_trade_value_text_to_eok(text: str) -> Optional[float]:
    if not text:
        return None
    cleaned = re.sub(r"\s+", "", text).replace(",", "")
    m = re.search(r"([\d.]+)조", cleaned)
    if m:
        return float(m.group(1)) * 10000
    m = re.search(r"([\d.]+)억", cleaned)
    if m:
        return float(m.group(1))
    m = re.search(r"([\d.]+)만", cleaned)
    if m:
        return float(m.group(1)) / 10000
    m = re.search(r"([\d.]+)", cleaned)
    if m and ("백만" in cleaned or "백만원" in cleaned):
        return float(m.group(1)) / 100.0
    return None


def parse_item_main_exact(code: str) -> Tuple[Optional[float], Optional[float]]:
    """Return (close_price, trade_value_eok) from Naver item main page.

    거래대금은 페이지에 보이는 단위를 그대로 읽어 억원으로 환산한다.
    """
    url = f"https://finance.naver.com/item/main.naver?code={code}"
    r = requests.get(url, headers=HEADERS, timeout=20)
    r.raise_for_status()
    html = r.text
    soup = BeautifulSoup(html, 'lxml')

    price = None
    no_today = soup.select_one('p.no_today')
    if no_today:
        blind = no_today.select_one('span.blind')
        price = extract_digits(blind.get_text(' ', strip=True) if blind else no_today.get_text(' ', strip=True))

    trade_value_eok = None

    for tr in soup.select('table.no_info tr'):
        cells = [td.get_text(" ", strip=True) for td in tr.select('th,td')]
        for i, cell in enumerate(cells):
            if '거래대금' in cell:
                candidates = [cell]
                if i + 1 < len(cells):
                    candidates.append(cells[i + 1])
                if i + 2 < len(cells):
                    candidates.append(cells[i + 2])
                for cand in candidates:
                    val = parse_trade_value_text_to_eok(cell + ' ' + cand)
                    if val is not None:
                        trade_value_eok = val
                        break
            if trade_value_eok is not None:
                break
        if trade_value_eok is not None:
            break

    if trade_value_eok is None:
        whole_text = soup.get_text('\n', strip=True)
        patterns = [
            r'거래대금[^\d]{0,20}([\d,.]+\s*조)',
            r'거래대금[^\d]{0,20}([\d,.]+\s*억)',
            r'거래대금[^\d]{0,20}([\d,.]+\s*만)',
            r'거래대금\(백만\)[^\d]{0,20}([\d,.]+)',
            r'거래대금\(백만원\)[^\d]{0,20}([\d,.]+)',
        ]
        for pat in patterns:
            m = re.search(pat, whole_text)
            if m:
                trade_value_eok = parse_trade_value_text_to_eok(m.group(0))
                if trade_value_eok is not None:
                    break

    return price, trade_value_eok

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


def build_payload(universe: List[dict], realtime_map: Dict[str, dict], exact_map: Dict[str, Tuple[Optional[float], Optional[float]]]) -> dict:
    by_symbol: Dict[str, dict] = {}
    for base in universe:
        code = str(base['code']).zfill(6)
        symbol = base['symbol']
        rt = realtime_map.get(code, {})
        exact_price, exact_amount = exact_map.get(code, (None, None))
        pct = to_float(rt.get('cr'))
        if pct is None:
            cv = to_float(rt.get('cv'))
            nv = to_float(rt.get('nv')) or exact_price
            if cv is not None and nv not in (None, 0):
                prev = nv - cv
                pct = (cv / prev) * 100 if prev else 0.0
            else:
                pct = 0.0
        close = exact_price if exact_price is not None else to_float(rt.get('nv'))
        amount_eok = exact_amount
        if amount_eok is None or amount_eok <= 0:
            continue
        by_symbol[symbol] = {
            'code': code,
            'symbol': symbol,
            'name': base.get('name') or rt.get('nm') or code,
            'market': base.get('market', 'KOSPI'),
            'close': close,
            'pct': round(float(pct), 4),
            'pct_text': format_pct(float(pct)),
            'trade_value_eok': round(float(amount_eok), 4),
            'trade_value_text': format_eok(float(amount_eok)),
            'trade_value_source': 'naver item/main 거래대금(백만원→억원)',
            'trade_value_approximate': False,
            'market_cap_rank': base.get('rank'),
            'market_cap_text': base.get('market_cap_text'),
        }

    rows = list(by_symbol.values())
    amount_ranked = sorted(rows, key=lambda x: (-x['trade_value_eok'], -x['pct']))
    rise_ranked = sorted(rows, key=lambda x: (-x['pct'], -x['trade_value_eok']))
    top_amount = {x['symbol'] for x in amount_ranked[:30]}
    top_rise = {x['symbol'] for x in rise_ranked[:30]}
    leaders = [x for x in amount_ranked if x['symbol'] in top_amount and x['symbol'] in top_rise]

    return {
        'generated_at': now_kst().isoformat(),
        'trade_date': format_trade_date(now_kst()),
        'trade_date_display': format_trade_date_display(format_trade_date(now_kst())),
        'source': 'Naver item/main exact value',
        'universe_name': 'KOSPI200 + KOSDAQ200 by market cap',
        'counts': {
            'universe': len(rows),
            'top_current_30': min(30, len(amount_ranked)),
            'top_rise_30': min(30, len(rise_ranked)),
            'leaders': len(leaders),
        },
        'leaders': leaders,
        'by_symbol': by_symbol,
        'status': 'ok',
        'fallback_used': False,
        'message': '',
    }


def main() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    try:
        universe = load_universe()
        codes = [str(x['code']).zfill(6) for x in universe]
        print(f'[INFO] universe loaded: {len(codes)}')
        realtime_map = fetch_naver_realtime(codes)
        print(f'[INFO] realtime rows: {len(realtime_map)}')
        exact_map: Dict[str, Tuple[Optional[float], Optional[float]]] = {}
        with ThreadPoolExecutor(max_workers=12) as ex:
            futs = {ex.submit(parse_item_main_exact, code): code for code in codes}
            for idx, fut in enumerate(as_completed(futs), 1):
                code = futs[fut]
                try:
                    exact_map[code] = fut.result()
                except Exception as e:
                    exact_map[code] = (None, None)
                    print(f'[WARN] {code} exact fetch failed: {e}')
                if idx % 50 == 0:
                    print(f'[INFO] parsed {idx}/{len(codes)} item pages')
        payload = build_payload(universe, realtime_map, exact_map)
        save_payload(payload)
        print(f"[OK] saved {OUTPUT_PATH} ({payload['counts']['universe']} symbols, leaders={payload['counts']['leaders']})")
    except Exception as e:
        msg = f'Naver latest generation failed: {e}'
        print(f'[WARN] {msg}')
        existing = load_existing()
        if existing is not None:
            existing['generated_at'] = now_kst().isoformat()
            existing['status'] = 'fallback'
            existing['fallback_used'] = True
            existing['message'] = msg
            save_payload(existing)
            print('[FALLBACK] existing latest_krx.json kept')
            return
        payload = dict(DEFAULT_EMPTY)
        payload['generated_at'] = now_kst().isoformat()
        payload['message'] = msg
        save_payload(payload)
        print('[FALLBACK] empty latest_krx.json created')


if __name__ == '__main__':
    main()
