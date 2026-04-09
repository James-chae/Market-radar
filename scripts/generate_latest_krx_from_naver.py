#!/usr/bin/env python3
"""
generate_latest_krx_from_naver.py
KRX + NXT 당일 시세를 수집해서 data/latest_krx.json 생성
"""
from __future__ import annotations

import json
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List

import requests
from bs4 import BeautifulSoup

SEOUL = timezone(timedelta(hours=9))
ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
UNIVERSE_PATH = DATA_DIR / "universe_kr_top1000.json"
OUTPUT_PATH = DATA_DIR / "latest_krx.json"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ko-KR,ko;q=0.9",
    "Referer": "https://finance.naver.com/",
}


def now_seoul() -> datetime:
    return datetime.now(SEOUL)


def load_universe() -> List[dict]:
    with UNIVERSE_PATH.open("r", encoding="utf-8") as f:
        data = json.load(f)
    return data["items"] if isinstance(data, dict) else data


# ── 1. Naver polling API (실시간 시세) ──────────────────────────
def fetch_naver_realtime(symbols: List[str]) -> Dict[str, dict]:
    """Naver polling API로 시세 수집 (100개씩 배치)"""
    result = {}
    batch_size = 100

    for i in range(0, len(symbols), batch_size):
        batch = symbols[i:i + batch_size]
        query = "|".join(f"SERVICE_ITEM:{s}" for s in batch)
        url = f"https://polling.finance.naver.com/api/realtime?query={query}"
        try:
            r = requests.get(url, headers=HEADERS, timeout=10)
            r.raise_for_status()
            areas = r.json().get("result", {}).get("areas", [])
            datas = areas[0].get("datas", []) if areas else []
            for item in datas:
                sym = item.get("cd", "")
                if not sym:
                    continue
                rf = str(item.get("rf", "3"))
                cr = float(item.get("cr", 0) or 0)
                pct = cr if rf in ("1", "2") else (-cr if rf in ("4", "5") else 0.0)
                cp = float(item.get("cp", 0) or 0)
                aq = float(item.get("aq", 0) or 0)
                tv_eok = round(cp * aq / 1e8, 2) if cp > 0 else 0.0
                result[sym] = {
                    "naver_pct": round(pct, 2),
                    "naver_close": cp or None,
                    "naver_tv_eok": tv_eok,
                    "naver_name": item.get("nm", ""),
                }
        except Exception as e:
            print(f"  [Naver] 배치 {i//batch_size+1} 실패: {e}")
        time.sleep(0.3)

    return result


# ── 2. KRX 공식 데이터 ──────────────────────────────────────────
def fetch_krx_official(trade_date: str) -> Dict[str, dict]:
    """KRX 정보데이터시스템 당일 전종목 시세"""
    result = {}
    url = "http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"

    for mkt_id, mkt_name, suffix in [
        ("STK", "KOSPI", ".KS"),
        ("KSQ", "KOSDAQ", ".KQ"),
    ]:
        body = {
            "bld": "dbms/MDC/STAT/standard/MDCSTAT01501",
            "locale": "ko_KR",
            "mktId": mkt_id,
            "trdDd": trade_date,
            "share": "1",
            "money": "1",
            "csvxls_isNo": "false",
        }
        hdrs = {
            **HEADERS,
            "Content-Type": "application/x-www-form-urlencoded",
            "Referer": "http://data.krx.co.kr/",
        }
        try:
            r = requests.post(url, data=body, headers=hdrs, timeout=20)
            r.raise_for_status()
            items = r.json().get("OutBlock_1", [])
            for item in items:
                code = str(item.get("ISU_SRT_CD", "")).strip()
                if not code or len(code) != 6:
                    continue
                try:
                    pct = float(str(item.get("FLUC_RT", 0)).replace(",", "") or 0)
                    tv_eok = float(str(item.get("ACC_TRDVAL", 0)).replace(",", "") or 0) / 1e8
                    cp = float(str(item.get("TDD_CLSPRC", 0)).replace(",", "") or 0)
                    name = str(item.get("ISU_ABBRV", "")).strip()
                    symbol = f"{code}{suffix}"
                    result[symbol] = {
                        "krx_pct": round(pct, 2),
                        "krx_close": cp or None,
                        "krx_tv_eok": round(tv_eok, 2),
                        "krx_name": name,
                        "market": mkt_name,
                    }
                except Exception:
                    continue
            print(f"  [KRX] {mkt_name}: {sum(1 for v in result.values() if v.get('market')==mkt_name)}개")
        except Exception as e:
            print(f"  [KRX] {mkt_name} 실패: {e}")

    return result


# ── 3. NXT 데이터 (Naver NXT 테이블) ───────────────────────────
def fetch_nxt(market: str = "KOSPI") -> Dict[str, dict]:
    """Naver NXT 거래량 테이블"""
    result = {}
    sosok = "0" if market == "KOSPI" else "1"
    url = f"https://finance.naver.com/sise/sise_quant.naver?sosok={sosok}"
    try:
        r = requests.get(url, headers=HEADERS, timeout=10)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "lxml")
        for row in soup.select("table.type_2 tr"):
            cells = row.select("td")
            if len(cells) < 10:
                continue
            a = cells[1].find("a")
            if not a:
                continue
            href = a.get("href", "")
            import re
            m = re.search(r"code=(\d{6})", href)
            if not m:
                continue
            code = m.group(1)
            suffix = ".KS" if market == "KOSPI" else ".KQ"
            symbol = f"{code}{suffix}"
            try:
                tv_text = cells[9].get_text(strip=True).replace(",", "")
                tv_eok = float(tv_text) / 1e8 if tv_text.isdigit() else 0.0
                result[symbol] = {"nxt_tv_eok": round(tv_eok, 2)}
            except Exception:
                continue
    except Exception as e:
        print(f"  [NXT] {market} 실패: {e}")
    return result


# ── 4. 병합 ────────────────────────────────────────────────────
def merge(universe: List[dict], krx: dict, naver: dict, nxt_kp: dict, nxt_kq: dict) -> Dict[str, dict]:
    by_symbol: Dict[str, dict] = {}

    for item in universe:
        code = str(item.get("code", "")).strip()
        mkt = item.get("market", "KOSPI")
        suffix = ".KS" if mkt == "KOSPI" else ".KQ"
        symbol = f"{code}{suffix}"
        name = item.get("name", code)

        k = krx.get(symbol, {})
        n = naver.get(symbol, {})
        nxt = (nxt_kp if mkt == "KOSPI" else nxt_kq).get(symbol, {})

        # 거래대금: KRX 우선, 없으면 NXT, 없으면 Naver
        tv_eok = k.get("krx_tv_eok") or nxt.get("nxt_tv_eok") or n.get("naver_tv_eok") or 0.0
        # 등락률: KRX 우선, 없으면 Naver
        pct = k.get("krx_pct") if k.get("krx_pct") is not None else n.get("naver_pct", 0.0)
        # 종가
        close = k.get("krx_close") or n.get("naver_close")
        # 이름
        final_name = k.get("krx_name") or n.get("naver_name") or name

        # source_parts
        sources = []
        if k.get("krx_tv_eok"):
            sources.append(f"KRX_{mkt}")
        if nxt.get("nxt_tv_eok"):
            sources.append(f"NXT_{mkt}")
        if n.get("naver_tv_eok") and not sources:
            sources.append("NAVER")

        pct_f = float(pct or 0)
        tv_f = float(tv_eok or 0)

        by_symbol[symbol] = {
            "code": code,
            "symbol": symbol,
            "name": final_name,
            "market": mkt,
            "close": close,
            "pct": round(pct_f, 2),
            "pct_text": f"{'+' if pct_f > 0 else ''}{pct_f:.1f}%",
            "trade_value_eok": round(tv_f, 2),
            "trade_value_text": (
                f"{tv_f/10000:.2f}조" if tv_f >= 10000
                else f"{tv_f:,.0f}억"
            ),
            "source_parts": sources,
        }

    return by_symbol


# ── main ────────────────────────────────────────────────────────
def main() -> None:
    t0 = now_seoul()
    trade_date = t0.strftime("%Y%m%d")
    print(f"[generate_latest_krx] {trade_date} {t0.strftime('%H:%M:%S')} KST")

    # 1. 유니버스 로드
    print("[1] 유니버스 로드...")
    universe = load_universe()
    print(f"    {len(universe)}개 종목")

    symbols_kp = [f"{i['code']}.KS" for i in universe if i.get("market") == "KOSPI"]
    symbols_kq = [f"{i['code']}.KQ" for i in universe if i.get("market") == "KOSDAQ"]
    all_symbols = symbols_kp + symbols_kq

    # 2. KRX 공식 데이터
    print("[2] KRX 공식 시세 수집...")
    krx = fetch_krx_official(trade_date)
    print(f"    KRX 수집: {len(krx)}개")

    # 3. Naver 실시간
    print("[3] Naver 실시간 시세 수집...")
    naver = fetch_naver_realtime(all_symbols)
    print(f"    Naver 수집: {len(naver)}개")

    # 4. NXT
    print("[4] NXT 거래대금 수집...")
    nxt_kp = fetch_nxt("KOSPI")
    nxt_kq = fetch_nxt("KOSDAQ")
    print(f"    NXT KOSPI:{len(nxt_kp)} KOSDAQ:{len(nxt_kq)}")

    # 5. 병합
    print("[5] 데이터 병합...")
    by_symbol = merge(universe, krx, naver, nxt_kp, nxt_kq)
    print(f"    병합 결과: {len(by_symbol)}개")

    # fallback: KRX 실패 시 표시
    fallback = len(krx) < 100

    # 6. 저장
    payload = {
        "generated_at": now_seoul().isoformat(),
        "trade_date": trade_date,
        "trade_date_display": t0.strftime("%Y-%m-%d"),
        "source": "Naver KRX+NXT table 기준",
        "universe_name": "KOSPI+KOSDAQ top universe",
        "status": "ok",
        "fallback_used": fallback,
        "message": "KRX 데이터 없음, Naver 폴백 사용" if fallback else "",
        "counts": {
            "universe": len(by_symbol),
            "top_current_30": 30,
            "top_rise_50": 50,
            "leaders": 0,
        },
        "leaders": [],
        "by_symbol": by_symbol,
    }

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(
        json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8",
    )

    elapsed = (now_seoul() - t0).total_seconds()
    print(f"\n✅ 저장 완료: {OUTPUT_PATH}")
    print(f"   trade_date: {trade_date}")
    print(f"   by_symbol: {len(by_symbol)}개")
    print(f"   소요시간: {elapsed:.1f}초")


if __name__ == "__main__":
    main()
