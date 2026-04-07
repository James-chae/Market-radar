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


def save_debug_json(name: str, obj: object) -> None:
    ensure_dirs()
    path = DEBUG_DIR / name
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[DEBUG] saved json -> {path}")


def to_number(text: str) -> Optional[float]:
    if text is None:
        return None
    s = str(text).strip().replace(",", "").replace("%", "")
    s = s.replace("＋", "+").replace("－", "-")
    # 한국 테이블에서 쓰이는 화살표 기호 제거 (부호는 get_td_sign이 담당)
    s = s.replace("▼", "").replace("▲", "").replace("▽", "").replace("△", "").strip()
    if not s or s in {"-", "--", "N/A"}:
        return None
    try:
        return float(s)
    except Exception:
        return None


def yyyy_mm_dd(date_str: str) -> str:
    if len(date_str) == 8:
        return f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
    return date_str


def format_pct(pct: float) -> str:
    return f"{pct:+.1f}%".replace("+0.0%", "0.0%")


def format_eok(eok: float) -> str:
    if not math.isfinite(eok):
        return "--"
    if eok >= 10000:
        return f"{eok / 10000:.2f}조"
    if eok >= 100:
        return f"{round(eok):,}억"
    return f"{eok:.1f}억"


# 유니버스 필터 기준: 시가총액 3000억원 이상
MIN_MARKET_CAP_EOK = 3000  # 억원


def load_universe() -> List[dict]:
    with UNIVERSE_PATH.open("r", encoding="utf-8") as f:
        data = json.load(f)

    if isinstance(data, dict) and "items" in data:
        items = data["items"]
    elif isinstance(data, list):
        items = data
    else:
        raise ValueError("universe_kr_top1000.json 형식이 올바르지 않습니다.")

    if not isinstance(items, list):
        raise ValueError("universe items 형식이 리스트가 아닙니다.")

    # 시총 필터: market_cap_eok 정보가 있으면 3000억 이상만 사용
    before = len(items)
    filtered = [
        item for item in items
        if item.get("market_cap_eok") is None  # 시총 정보 없으면 포함 (안전 fallback)
        or float(item["market_cap_eok"]) >= MIN_MARKET_CAP_EOK
    ]
    if before != len(filtered):
        print(f"[UNIVERSE] 시총 {MIN_MARKET_CAP_EOK}억 미만 제외: {before} → {len(filtered)}종목")

    return filtered


def load_existing_payload() -> Optional[dict]:
    if not OUTPUT_PATH.exists():
        return None
    try:
        return json.loads(OUTPUT_PATH.read_text(encoding="utf-8"))
    except Exception as e:
        print(f"[WARN] 기존 latest_krx.json 읽기 실패: {e}")
        return None


def save_payload(payload: dict) -> None:
    ensure_dirs()
    OUTPUT_PATH.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(f"[OK] saved -> {OUTPUT_PATH}")


def build_error_payload(message: str) -> dict:
    trade_date = now_seoul().strftime("%Y%m%d")
    return {
        "generated_at": now_seoul().isoformat(),
        "trade_date": trade_date,
        "trade_date_display": yyyy_mm_dd(trade_date),
        "source": "Naver KRX+NXT table 기준",
        "universe_name": f"시총 {MIN_MARKET_CAP_EOK}억원 이상 유니버스 (KOSPI+KOSDAQ)",
        "counts": {
            "universe": 0,
            "top_current_30": 0,
            "top_rise_30": 0,
            "leaders": 0,
        },
        "leaders": [],
        "by_symbol": {},
        "status": "error",
        "fallback_used": False,
        "message": message,
    }


def build_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(HEADERS)
    return s


def fetch_html(session: requests.Session, method: str, url: str, *, data=None, params=None, debug_name: str = "") -> str:
    r = session.request(method, url, data=data, params=params, timeout=20)
    r.raise_for_status()
    text = read_text_with_fallback(r.content)
    if debug_name:
        save_debug_html(debug_name, text)
    return text


def fetch_krx_market_sum_with_amount(session: requests.Session, sosok: int, page: int) -> str:
    return_url = f"http://finance.naver.com/sise/sise_market_sum.naver?page={page}"
    if sosok:
        return_url += f"&sosok={sosok}"

    data = [
        ("menu", "market_sum"),
        ("returnUrl", return_url),
        ("fieldIds", "quant"),
        ("fieldIds", "amount"),
        ("fieldIds", "market_sum"),
        ("fieldIds", "listed_stock_cnt"),
        ("fieldIds", "frgn_rate"),
        ("fieldIds", "per"),
        ("fieldIds", "roe"),
    ]
    url = "https://finance.naver.com/sise/field_submit.naver"
    return fetch_html(
        session,
        "POST",
        url,
        data=data,
        debug_name=f"debug_krx_post_sosok{sosok}_p{page}.html",
    )


def fetch_nxt_market_sum(session: requests.Session, sosok: int, page: int) -> str:
    url = "https://finance.naver.com/sise/nxt_sise_market_sum.naver"
    params = {"page": page}
    if sosok:
        params["sosok"] = sosok
    return fetch_html(
        session,
        "GET",
        url,
        params=params,
        debug_name=f"debug_nxt_market_sum_sosok{sosok}_p{page}.html",
    )


def extract_code_from_href(href: str) -> Optional[str]:
    if not href:
        return None
    m = re.search(r"code=(\d{6})", href)
    return m.group(1) if m else None


def find_header_indexes(table) -> Tuple[Dict[str, int], List[str]]:
    header_map: Dict[str, int] = {}
    thead = table.find("thead")
    headers: List[str] = []

    if not thead:
        return header_map, headers

    for idx, th in enumerate(thead.find_all("th")):
        txt = th.get_text(" ", strip=True).replace("\n", " ").strip()
        compact = txt.replace(" ", "")
        headers.append(txt)

        if "종목명" in compact:
            header_map["name"] = idx
        elif "현재가" in compact:
            header_map["price"] = idx
        elif "등락률" in compact:
            header_map["pct"] = idx
        elif "거래대금" in compact:
            header_map["amount"] = idx
        elif "거래량" in compact:
            header_map["volume"] = idx

    return header_map, headers


def parse_amount_text_to_eok(text: str, *, numeric_is_million_krw: bool) -> Optional[float]:
    if text is None:
        return None

    s = str(text).strip()
    s = s.replace(",", "").replace(" ", "").replace("\xa0", "")
    if not s or s in {"-", "--"}:
        return None

    try:
        if "조" in s:
            m = re.search(r"([+-]?\d+(?:\.\d+)?)조", s)
            if m:
                return float(m.group(1)) * 10000
        if "억" in s:
            m = re.search(r"([+-]?\d+(?:\.\d+)?)억", s)
            if m:
                return float(m.group(1))
        if "백만" in s:
            m = re.search(r"([+-]?\d+(?:\.\d+)?)백만", s)
            if m:
                return float(m.group(1)) / 100
        if "만" in s:
            m = re.search(r"([+-]?\d+(?:\.\d+)?)만", s)
            if m:
                return float(m.group(1)) / 10000

        if re.fullmatch(r"[+-]?\d+(?:\.\d+)?", s):
            n = float(s)
            return n / 100 if numeric_is_million_krw else n
    except Exception:
        return None

    return None


def get_td_sign(td) -> int:
    """Naver KRX/NXT 테이블에서 td class로 등락 방향 판별.
    rate_down / fall / down → -1
    rate_up   / rise / up   → +1
    보합(rate_dn0) 또는 미확인 → 0 (부호 없음, get_text 값 그대로)
    """
    classes = " ".join(td.get("class") or []).lower()
    # 하락 계열 클래스
    if any(k in classes for k in ("rate_down", "fall", " down", "minus")):
        return -1
    # 상승 계열 클래스
    if any(k in classes for k in ("rate_up", "rise", " up", "plus")):
        return 1
    # span 자식 요소로 2차 판별 (ico down / ico up)
    for span in td.find_all("span"):
        sc = " ".join(span.get("class") or []).lower()
        if "down" in sc or "fall" in sc or "minus" in sc:
            return -1
        if "up" in sc or "rise" in sc or "plus" in sc:
            return 1
    return 0  # 보합 또는 판별 불가


def extract_signed_pct(td) -> Optional[float]:
    """td 요소에서 부호를 포함한 등락률(%) 추출."""
    txt = td.get_text(strip=True)
    if "%" not in txt:
        return None
    num = to_number(txt)
    if num is None:
        return None
    sign = get_td_sign(td)
    if sign == -1:
        return -abs(num)
    if sign == 1:
        return abs(num)
    # sign == 0: get_text에 이미 +/- 포함된 경우 그대로 사용
    return num


def infer_pct_from_tds(tds) -> Optional[float]:
    for td in tds:
        val = extract_signed_pct(td)
        if val is not None:
            return val
    return None


def infer_amount_from_tds(tds, *, numeric_is_million_krw: bool) -> Optional[float]:
    # 단위 텍스트(조/억/만/백만)가 포함된 셀만 시도 — 안전한 경우만 추론
    for td in reversed(tds):
        txt = td.get_text(strip=True)
        if any(unit in txt for unit in ("조", "억", "만", "백만")):
            parsed = parse_amount_text_to_eok(txt, numeric_is_million_krw=numeric_is_million_krw)
            if parsed is not None:
                return parsed

    # ※ 순수 숫자 max() 폴백 제거:
    #   네이버 KRX 테이블에서 header 매핑 실패 시 max(nums)를 거래대금으로
    #   채택하면 시가총액·상장주식수 등 훨씬 큰 값이 잘못 선택됨.
    #   header_idx["amount"] 매핑이 실패했을 때는 None을 반환해 해당 행을 드롭.
    return None


def parse_market_table(
    html_text: str,
    market_label: str,
    source_name: str,
    *,
    numeric_is_million_krw: bool,
) -> Tuple[List[dict], dict]:
    soup = BeautifulSoup(html_text, "lxml")
    tables = soup.select("table.type_2")

    debug_info = {
        "source": source_name,
        "tables_found": len(tables),
        "header_idx": {},
        "headers": [],
        "sample_rows": [],
        "drop_counts": {
            "no_anchor": 0,
            "no_code": 0,
            "no_pct": 0,
            "no_amount": 0,
        },
    }

    if not tables:
        return [], debug_info

    table = tables[0]
    header_idx, headers = find_header_indexes(table)
    debug_info["header_idx"] = header_idx
    debug_info["headers"] = headers

    tbody = table.find("tbody")
    if not tbody:
        return [], debug_info

    rows: List[dict] = []
    sample_limit = 8

    for tr in tbody.find_all("tr"):
        tds = tr.find_all("td")
        if not tds:
            continue

        a = tr.select_one("a[href*='item/main.naver?code=']")
        if not a:
            debug_info["drop_counts"]["no_anchor"] += 1
            continue

        code = extract_code_from_href(a.get("href", ""))
        name = a.get_text(strip=True)
        if not code or not name:
            debug_info["drop_counts"]["no_code"] += 1
            continue

        pct = None
        amount_eok = None

        if "pct" in header_idx and header_idx["pct"] < len(tds):
            pct = extract_signed_pct(tds[header_idx["pct"]])
        if pct is None:
            pct = infer_pct_from_tds(tds)

        if "amount" in header_idx and header_idx["amount"] < len(tds):
            amount_eok = parse_amount_text_to_eok(
                tds[header_idx["amount"]].get_text(strip=True),
                numeric_is_million_krw=numeric_is_million_krw,
            )
        if amount_eok is None:
            amount_eok = infer_amount_from_tds(tds, numeric_is_million_krw=numeric_is_million_krw)

        if pct is None:
            debug_info["drop_counts"]["no_pct"] += 1
            if len(debug_info["sample_rows"]) < sample_limit:
                debug_info["sample_rows"].append({
                    "code": code,
                    "name": name,
                    "reason": "no_pct",
                    "td_texts": [td.get_text(" ", strip=True) for td in tds[:12]],
                })
            continue

        if amount_eok is None:
            debug_info["drop_counts"]["no_amount"] += 1
            if len(debug_info["sample_rows"]) < sample_limit:
                debug_info["sample_rows"].append({
                    "code": code,
                    "name": name,
                    "reason": "no_amount",
                    "td_texts": [td.get_text(" ", strip=True) for td in tds[:12]],
                })
            continue

        row = {
            "code": code,
            "name": name,
            "pct": float(pct),
            "trade_value_eok": float(amount_eok),
            "market": market_label,
        }
        rows.append(row)

        if len(debug_info["sample_rows"]) < sample_limit:
            debug_info["sample_rows"].append({
                "code": code,
                "name": name,
                "pct": pct,
                "trade_value_eok": amount_eok,
                "td_texts": [td.get_text(" ", strip=True) for td in tds[:12]],
            })

    return rows, debug_info


def fetch_krx_market_pages(session: requests.Session, sosok: int, market_name: str, max_pages: int = 25) -> List[dict]:
    all_rows: List[dict] = []
    for page in range(1, max_pages + 1):
        html = fetch_krx_market_sum_with_amount(session, sosok=sosok, page=page)
        rows, debug_info = parse_market_table(
            html,
            market_label=market_name,
            source_name=f"KRX_{market_name}_p{page}",
            numeric_is_million_krw=True,
        )
        save_debug_json(f"debug_parse_krx_{market_name.lower()}_p{page}.json", debug_info)
        print(f"[PARSE] KRX {market_name} page {page}: {len(rows)} rows")
        if not rows:
            break
        all_rows.extend(rows)
        if len(all_rows) >= 700:
            break
        time.sleep(0.15)
    return all_rows


def fetch_nxt_pages(session: requests.Session, sosok: int, market_name: str, max_pages: int = 25) -> List[dict]:
    all_rows: List[dict] = []
    for page in range(1, max_pages + 1):
        html = fetch_nxt_market_sum(session, sosok=sosok, page=page)
        rows, debug_info = parse_market_table(
            html,
            market_label=market_name,
            source_name=f"NXT_{market_name}_p{page}",
            numeric_is_million_krw=True,
        )
        save_debug_json(f"debug_parse_nxt_{market_name.lower()}_p{page}.json", debug_info)
        print(f"[PARSE] NXT {market_name} page {page}: {len(rows)} rows")
        if not rows:
            break
        all_rows.extend(rows)
        if len(all_rows) >= 700:
            break
        time.sleep(0.15)
    return all_rows


def build_universe_maps(universe: List[dict]) -> Tuple[Dict[str, dict], Dict[str, str]]:
    by_code: Dict[str, dict] = {}
    suffix_map: Dict[str, str] = {}
    for item in universe:
        code = str(item.get("code", "")).zfill(6)
        if not code:
            continue
        by_code[code] = item
        suffix = item.get("suffix")
        if isinstance(suffix, str) and suffix in {".KS", ".KQ"}:
            suffix_map[code] = suffix
        else:
            market = str(item.get("market", "")).upper()
            suffix_map[code] = ".KQ" if "KOSDAQ" in market else ".KS"
    return by_code, suffix_map


def merge_rows(
    universe: List[dict],
    krx_kospi: List[dict],
    krx_kosdaq: List[dict],
    nxt_kospi: List[dict],
    nxt_kosdaq: List[dict],
) -> List[dict]:
    universe_map, suffix_map = build_universe_maps(universe)
    agg: Dict[str, dict] = {}

    def apply(rows: List[dict], source_tag: str) -> None:
        is_krx = source_tag.startswith("KRX_")

        for row in rows:
            code = str(row["code"]).zfill(6)
            if code not in universe_map:
                continue

            base = universe_map[code]
            suffix = suffix_map.get(code, ".KS")
            symbol = code + suffix

            if code not in agg:
                agg[code] = {
                    "code": code,
                    "symbol": symbol,
                    "name": base.get("name") or row["name"],
                    "market": base.get("market") or row["market"],
                    "pct": row["pct"],
                    "pct_source": source_tag,   # 어느 소스의 pct를 쓰고 있는지 추적
                    "trade_value_eok": 0.0,
                    "source_parts": [],
                }

            # ── 등락률(pct) 정책: KRX 우선 ─────────────────────────────
            # KRX 데이터: 항상 채택 (KRX끼리 여러 페이지면 마지막 KRX로 갱신)
            # NXT 데이터: KRX 데이터가 아직 없는 경우에만 폴백으로 사용
            if is_krx:
                agg[code]["pct"] = row["pct"]
                agg[code]["pct_source"] = source_tag
            else:
                # NXT이고 아직 KRX 데이터를 받지 못한 경우에만 업데이트
                current_src = agg[code].get("pct_source", "")
                if not current_src.startswith("KRX_"):
                    agg[code]["pct"] = row["pct"]
                    agg[code]["pct_source"] = source_tag

            # ── 거래대금: KRX + NXT 합산 (기존과 동일) ──────────────────
            agg[code]["trade_value_eok"] += row["trade_value_eok"]
            agg[code]["source_parts"].append(source_tag)

    # 반드시 KRX를 먼저 처리해야 "KRX 우선" 정책이 의미있음
    apply(krx_kospi, "KRX_KOSPI")
    apply(krx_kosdaq, "KRX_KOSDAQ")
    apply(nxt_kospi, "NXT_KOSPI")
    apply(nxt_kosdaq, "NXT_KOSDAQ")

    merged: List[dict] = []
    for code, item in agg.items():
        pct = float(item["pct"])
        eok = float(item["trade_value_eok"])
        merged.append({
            "code": code,
            "symbol": item["symbol"],
            "name": item["name"],
            "market": item["market"],
            "close": None,
            "pct": round(pct, 4),
            "pct_text": format_pct(pct),
            "trade_value_eok": round(eok, 4),
            "trade_value_text": format_eok(eok),
            "source_parts": item["source_parts"],
            # pct_source는 내부 추적용이므로 출력에서 제외
        })

    return merged


def build_payload(merged_rows: List[dict]) -> dict:
    amount_ranked = sorted(
        merged_rows,
        key=lambda x: (-float(x["trade_value_eok"]), -float(x["pct"]))
    )
    rise_ranked = sorted(
        merged_rows,
        key=lambda x: (-float(x["pct"]), -float(x["trade_value_eok"]))
    )

    top_c = {item["symbol"] for item in amount_ranked[:30]}
    top_e = {item["symbol"] for item in rise_ranked[:30]}
    leaders = [item for item in amount_ranked if item["symbol"] in top_c and item["symbol"] in top_e]
    by_symbol = {item["symbol"]: item for item in merged_rows}

    trade_date = now_seoul().strftime("%Y%m%d")

    return {
        "generated_at": now_seoul().isoformat(),
        "trade_date": trade_date,
        "trade_date_display": yyyy_mm_dd(trade_date),
        "source": "Naver KRX+NXT table 기준",
        "universe_name": f"시총 {MIN_MARKET_CAP_EOK}억원 이상 유니버스 (KOSPI+KOSDAQ)",
        "counts": {
            "universe": len(merged_rows),
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
    ensure_dirs()

    try:
        universe = load_universe()
    except Exception as e:
        payload = build_error_payload(f"유니버스 로드 실패: {e}")
        save_payload(payload)
        sys.exit(1)

    session = build_session()

    try:
        krx_kospi = fetch_krx_market_pages(session, sosok=0, market_name="KOSPI")
        nxt_kospi = fetch_nxt_pages(session, sosok=0, market_name="KOSPI")
        print(f"[KOSPI] KRX rows={len(krx_kospi)} NXT rows={len(nxt_kospi)}")

        krx_kosdaq = fetch_krx_market_pages(session, sosok=1, market_name="KOSDAQ")
        nxt_kosdaq = fetch_nxt_pages(session, sosok=1, market_name="KOSDAQ")
        print(f"[KOSDAQ] KRX rows={len(krx_kosdaq)} NXT rows={len(nxt_kosdaq)}")

        merged = merge_rows(universe, krx_kospi, krx_kosdaq, nxt_kospi, nxt_kosdaq)
        print(f"[TOTAL] merged universe rows={len(merged)}")

        if len(merged) == 0:
            payload = build_error_payload(
                "Naver KRX/NXT 표 파싱 결과가 0건입니다. debug/*.html 및 debug/*.json 파일을 확인하세요."
            )
            save_payload(payload)
            sys.exit(2)

        payload = build_payload(merged)
        save_payload(payload)
        print(f"[DONE] leaders={payload['counts']['leaders']} universe={payload['counts']['universe']}")

    except Exception as e:
        existing = load_existing_payload()
        msg = str(e)
        print(f"[FALLBACK] {msg}")

        if existing is not None:
            existing["generated_at"] = now_seoul().isoformat()
            existing["status"] = "fallback"
            existing["fallback_used"] = True
            existing["message"] = msg
            save_payload(existing)
            sys.exit(3)

        payload = build_error_payload(msg)
        save_payload(payload)
        sys.exit(4)


if __name__ == "__main__":
    main()
