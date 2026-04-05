#!/usr/bin/env python3
from __future__ import annotations

import json
import math
import re
import time
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
    "Origin": "https://finance.naver.com",
}
MARKETS = [
    {"name": "KOSPI", "sosok": 0, "suffix": ".KS"},
    {"name": "KOSDAQ", "sosok": 1, "suffix": ".KQ"},
]
DEFAULT_EMPTY = {
    "generated_at": "",
    "trade_date": "",
    "trade_date_display": "",
    "source": "Naver KRX+NXT table 기준",
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

def decode_response(resp: requests.Response) -> str:
    for enc in (resp.encoding, resp.apparent_encoding, 'euc-kr', 'cp949', 'utf-8'):
        if not enc:
            continue
        try:
            return resp.content.decode(enc, errors='replace')
        except Exception:
            pass
    return resp.text

def to_float(value) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value) if math.isfinite(value) else None
    text = str(value).replace(',', '').strip()
    if not text:
        return None
    try:
        num = float(text)
    except ValueError:
        return None
    return num if math.isfinite(num) else None

def format_pct(pct: float) -> str:
    return f"{pct:+.1f}%".replace('+0.0%', '0.0%')

def format_eok(eok: float) -> str:
    if eok >= 10000:
        return f"{eok / 10000:.2f}조"
    if eok >= 100:
        return f"{round(eok):,}억"
    return f"{eok:.1f}억"

def load_universe() -> List[dict]:
    raw = json.loads(UNIVERSE_PATH.read_text(encoding='utf-8'))
    return raw['items'] if isinstance(raw, dict) and 'items' in raw else raw

def load_existing() -> Optional[dict]:
    if not OUTPUT_PATH.exists():
        return None
    try:
        return json.loads(OUTPUT_PATH.read_text(encoding='utf-8'))
    except Exception:
        return None

def save_payload(payload: dict) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')

def parse_money_to_eok(text: str, header_text: str = '') -> Optional[float]:
    if not text:
        return None
    cleaned = re.sub(r"\s+", "", str(text)).replace(',', '')
    if cleaned in {'', '-', 'N/A'}:
        return None
    total = 0.0
    matched = False
    m = re.search(r'([\d.]+)조', cleaned)
    if m:
        total += float(m.group(1)) * 10000
        matched = True
    m = re.search(r'([\d.]+)억', cleaned)
    if m:
        total += float(m.group(1))
        matched = True
    if not matched:
        m = re.search(r'([\d.]+)만', cleaned)
        if m:
            total += float(m.group(1)) / 10000
            matched = True
    if matched:
        return total
    num = to_float(cleaned)
    if num is None:
        return None
    unit_text = header_text.replace(' ', '')
    if '백만' in unit_text:
        return num / 100.0
    if '만원' in unit_text or '(만)' in unit_text:
        return num / 10000.0
    if '억원' in unit_text or '(억)' in unit_text:
        return num
    return num

def extract_last_page(soup: BeautifulSoup) -> int:
    pgrr = soup.select_one('td.pgRR a')
    if pgrr and pgrr.get('href'):
        m = re.search(r'page=(\d+)', pgrr['href'])
        if m:
            return int(m.group(1))
    return 1

def discover_field_map(session: requests.Session, sosok: int) -> Dict[str, str]:
    url = f'https://finance.naver.com/sise/sise_market_sum.naver?sosok={sosok}&page=1'
    r = session.get(url, headers=HEADERS, timeout=20)
    r.raise_for_status()
    soup = BeautifulSoup(decode_response(r), 'lxml')
    mapping = {}
    for inp in soup.select('input[name="fieldIds"]'):
        value = inp.get('value', '').strip()
        label = ''
        parent_label = inp.find_parent('label')
        if parent_label:
            label = parent_label.get_text(' ', strip=True)
        if not label:
            nxt = inp.find_next('label')
            if nxt:
                label = nxt.get_text(' ', strip=True)
        if not label and inp.parent:
            label = inp.parent.get_text(' ', strip=True)
        if value and label:
            mapping[label] = value
    return mapping

def post_market_sum_with_fields(session: requests.Session, sosok: int, page: int, fields: Dict[str, str]) -> str:
    return_url = f'https://finance.naver.com/sise/sise_market_sum.naver?sosok={sosok}&page={page}'
    desired_values = []
    for wanted in ('거래대금', '거래대금(백만)', '등락률', '현재가', '시가총액'):
        for label, value in fields.items():
            if wanted in label and value not in desired_values:
                desired_values.append(value)
                break
    data = [('menu', 'market_sum'), ('returnUrl', return_url)]
    for value in desired_values:
        data.append(('fieldIds', value))
    r = session.post('https://finance.naver.com/sise/field_submit.naver', data=data, headers=HEADERS, timeout=20)
    r.raise_for_status()
    return decode_response(r)

def parse_table_rows(html: str, market_name: str, suffix: str, source_name: str) -> Tuple[List[dict], int]:
    soup = BeautifulSoup(html, 'lxml')
    table = soup.select_one('table.type_2')
    if not table:
        return [], 1
    last_page = extract_last_page(soup)
    headers = [th.get_text(' ', strip=True) for th in table.select('tr th') if th.get_text(' ', strip=True)]
    rows = []
    seen = set()
    for tr in table.select('tr'):
        a = tr.select_one('a.tltle')
        if not a:
            continue
        m = re.search(r'code=(\d{6})', a.get('href', ''))
        if not m:
            continue
        code = m.group(1)
        if code in seen:
            continue
        seen.add(code)
        cells = [td.get_text(' ', strip=True) for td in tr.select('td')]
        if not cells:
            continue
        row_map = {headers[i] if i < len(headers) else f'col{i}': cells[i] for i in range(len(cells))}
        pct = None
        trade_eok = None
        close = None
        for k, v in row_map.items():
            if '등락률' in k and pct is None:
                pct = to_float(v)
            if '거래대금' in k and trade_eok is None:
                trade_eok = parse_money_to_eok(v, k)
            if '현재가' in k and close is None:
                close = to_float(v)
        rows.append({
            'code': code,
            'symbol': code + suffix,
            'name': a.get_text(' ', strip=True),
            'market': market_name,
            'pct': pct,
            'close': close,
            'trade_value_eok': trade_eok,
            'source': source_name,
        })
    return rows, last_page

def scrape_market_sum(session: requests.Session, sosok: int, market_name: str, suffix: str) -> List[dict]:
    fields = discover_field_map(session, sosok)
    html = post_market_sum_with_fields(session, sosok, 1, fields) if fields else decode_response(session.get(f'https://finance.naver.com/sise/sise_market_sum.naver?sosok={sosok}&page=1', headers=HEADERS, timeout=20))
    rows, last_page = parse_table_rows(html, market_name, suffix, 'NAVER_KRX')
    out = rows[:]
    for page in range(2, last_page + 1):
        html = post_market_sum_with_fields(session, sosok, page, fields) if fields else decode_response(session.get(f'https://finance.naver.com/sise/sise_market_sum.naver?sosok={sosok}&page={page}', headers=HEADERS, timeout=20))
        page_rows, _ = parse_table_rows(html, market_name, suffix, 'NAVER_KRX')
        if not page_rows:
            break
        out.extend(page_rows)
        time.sleep(0.12)
    return out

def scrape_nxt(session: requests.Session, sosok: int, market_name: str, suffix: str) -> List[dict]:
    url = f'https://finance.naver.com/sise/nxt_sise_quant.naver?sosok={sosok}&page=1'
    r = session.get(url, headers=HEADERS, timeout=20)
    r.raise_for_status()
    html = decode_response(r)
    rows, last_page = parse_table_rows(html, market_name, suffix, 'NAVER_NXT')
    out = rows[:]
    for page in range(2, last_page + 1):
        r = session.get(f'https://finance.naver.com/sise/nxt_sise_quant.naver?sosok={sosok}&page={page}', headers=HEADERS, timeout=20)
        r.raise_for_status()
        page_html = decode_response(r)
        page_rows, _ = parse_table_rows(page_html, market_name, suffix, 'NAVER_NXT')
        if not page_rows:
            break
        out.extend(page_rows)
        time.sleep(0.12)
    return out

def merge_rows(universe: List[dict], krx_rows: List[dict], nxt_rows: List[dict]) -> dict:
    universe_map = {str(item['code']).zfill(6): item for item in universe}
    by_code: Dict[str, dict] = {}
    def ensure_row(code: str, base_item: dict) -> dict:
        if code not in by_code:
            by_code[code] = {
                'code': code,
                'symbol': base_item['symbol'],
                'name': base_item['name'],
                'market': base_item['market'],
                'close': None,
                'pct': None,
                'trade_value_eok': 0.0,
            }
        return by_code[code]
    for row in krx_rows:
        code = row['code']
        if code not in universe_map:
            continue
        dst = ensure_row(code, universe_map[code])
        if row.get('close') is not None:
            dst['close'] = row['close']
        if row.get('pct') is not None:
            dst['pct'] = row['pct']
        if row.get('trade_value_eok') is not None:
            dst['trade_value_eok'] += float(row['trade_value_eok'])
    for row in nxt_rows:
        code = row['code']
        if code not in universe_map:
            continue
        dst = ensure_row(code, universe_map[code])
        if dst['pct'] is None and row.get('pct') is not None:
            dst['pct'] = row['pct']
        if dst['close'] is None and row.get('close') is not None:
            dst['close'] = row['close']
        if row.get('trade_value_eok') is not None:
            dst['trade_value_eok'] += float(row['trade_value_eok'])
    rows_out = []
    by_symbol = {}
    for code, item in by_code.items():
        if item['pct'] is None or item['trade_value_eok'] <= 0:
            continue
        item['pct'] = float(item['pct'])
        item['pct_text'] = format_pct(item['pct'])
        item['trade_value_text'] = format_eok(item['trade_value_eok'])
        rows_out.append(item)
        by_symbol[item['symbol']] = item
    amount_ranked = sorted(rows_out, key=lambda x: (-x['trade_value_eok'], -x['pct']))
    rise_ranked = sorted(rows_out, key=lambda x: (-x['pct'], -x['trade_value_eok']))
    top_c = {r['symbol'] for r in amount_ranked[:30]}
    top_e = {r['symbol'] for r in rise_ranked[:30]}
    leaders = [r for r in amount_ranked if r['symbol'] in top_c and r['symbol'] in top_e]
    trade_date = format_trade_date(now_kst())
    return {
        'generated_at': now_kst().isoformat(),
        'trade_date': trade_date,
        'trade_date_display': format_trade_date_display(trade_date),
        'source': 'Naver KRX+NXT table 기준',
        'universe_name': 'KOSPI200 + KOSDAQ200 by market cap',
        'counts': {'universe': len(rows_out), 'top_current_30': min(30, len(amount_ranked)), 'top_rise_30': min(30, len(rise_ranked)), 'leaders': len(leaders)},
        'leaders': leaders,
        'by_symbol': by_symbol,
        'status': 'ok',
        'fallback_used': False,
        'message': '',
    }

def main() -> None:
    try:
        universe = load_universe()
    except Exception as e:
        payload = load_existing() or DEFAULT_EMPTY.copy()
        payload['generated_at'] = now_kst().isoformat()
        payload['message'] = f'universe load failed: {e}'
        save_payload(payload)
        print(f'[FALLBACK] universe load failed: {e}')
        return
    try:
        session = requests.Session()
        session.headers.update(HEADERS)
        krx_rows, nxt_rows = [], []
        for m in MARKETS:
            k = scrape_market_sum(session, m['sosok'], m['name'], m['suffix'])
            n = scrape_nxt(session, m['sosok'], m['name'], m['suffix'])
            print(f"[{m['name']}] KRX rows={len(k)} NXT rows={len(n)}")
            krx_rows.extend(k)
            nxt_rows.extend(n)
        payload = merge_rows(universe, krx_rows, nxt_rows)
        save_payload(payload)
        print(f"[OK] saved {OUTPUT_PATH} ({payload['counts']['universe']} rows / leaders {payload['counts']['leaders']})")
    except Exception as e:
        payload = load_existing() or DEFAULT_EMPTY.copy()
        payload['generated_at'] = now_kst().isoformat()
        payload['status'] = 'fallback'
        payload['fallback_used'] = True
        payload['message'] = f'naver table update failed: {e}'
        save_payload(payload)
        print(f'[FALLBACK] {e}')

if __name__ == '__main__':
    main()
