#!/usr/bin/env python3
"""
generate_latest_krx_from_naver.py
KRX + Naver 당일 시세 수집 → data/latest_krx.json 생성
유니버스: data/kr_sector_meta.json (Excel 기반 KOSPI+KOSDAQ 전종목, ETF 제외)
"""
from __future__ import annotations
import json, time
from datetime import datetime, timedelta, timezone
from pathlib import Path
import requests
from bs4 import BeautifulSoup

SEOUL = timezone(timedelta(hours=9))
ROOT  = Path(__file__).resolve().parents[1]
DATA_DIR  = ROOT / "data"
META_PATH = DATA_DIR / "kr_sector_meta.json"   # Excel 기반 전종목 메타
OUTPUT_PATH = DATA_DIR / "latest_krx.json"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept-Language": "ko-KR,ko;q=0.9",
    "Referer": "https://finance.naver.com/",
}

def now_kst():
    return datetime.now(SEOUL)

# ── 유니버스 로드 (Excel 기반 kr_sector_meta.json) ──────────────
def load_universe():
    """ETF 제외 실제 종목만 반환: {code: {name, market, sector}}"""
    universe = {}
    if META_PATH.exists():
        with META_PATH.open("r", encoding="utf-8") as f:
            meta = json.load(f)
        for code, d in meta.items():
            if d.get("etf"):        continue   # ETF/ETN 제외
            if not code.isdigit():  continue   # 숫자 6자리만
            if len(code) != 6:      continue
            universe[code] = {
                "name":   d.get("name", code),
                "market": d.get("market", "KOSPI"),
                "sector": d.get("sector1", ""),
            }
        print(f"  유니버스 로드: {len(universe)}개 (kr_sector_meta.json)")
    else:
        # fallback: universe_kr_top1000.json
        fallback = DATA_DIR / "universe_kr_top1000.json"
        if fallback.exists():
            with fallback.open("r", encoding="utf-8") as f:
                data = json.load(f)
            items = data["items"] if isinstance(data, dict) else data
            for item in items:
                code = str(item.get("code","")).strip()
                if not code or len(code) != 6: continue
                universe[code] = {
                    "name":   item.get("name", code),
                    "market": item.get("market", "KOSPI"),
                    "sector": "",
                }
            print(f"  유니버스 로드(fallback): {len(universe)}개 (universe_kr_top1000.json)")
    return universe

# ── KRX 공식 시세 수집 ──────────────────────────────────────────
def fetch_krx_official(trade_date: str) -> dict:
    """KRX 정보데이터시스템 당일 전종목 시세"""
    result = {}
    url = "http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"
    for mkt_id, mkt_name, suffix in [("STK","KOSPI",".KS"),("KSQ","KOSDAQ",".KQ")]:
        body = {
            "bld": "dbms/MDC/STAT/standard/MDCSTAT01501",
            "locale": "ko_KR", "mktId": mkt_id, "trdDd": trade_date,
            "share": "1", "money": "1", "csvxls_isNo": "false",
        }
        hdrs = {**HEADERS, "Content-Type":"application/x-www-form-urlencoded",
                "Referer":"http://data.krx.co.kr/"}
        try:
            r = requests.post(url, data=body, headers=hdrs, timeout=20)
            r.raise_for_status()
            items = r.json().get("OutBlock_1", [])
            cnt = 0
            for item in items:
                code = str(item.get("ISU_SRT_CD","")).strip()
                if not code or len(code) != 6: continue
                try:
                    pct    = float(str(item.get("FLUC_RT",0)).replace(",","") or 0)
                    tv_eok = float(str(item.get("ACC_TRDVAL",0)).replace(",","") or 0) / 1e8
                    cp     = float(str(item.get("TDD_CLSPRC",0)).replace(",","") or 0)
                    name   = str(item.get("ISU_ABBRV","")).strip()
                    symbol = f"{code}{suffix}"
                    result[symbol] = {
                        "krx_pct": round(pct,2), "krx_close": cp or None,
                        "krx_tv_eok": round(tv_eok,2), "krx_name": name, "market": mkt_name,
                    }
                    cnt += 1
                except: continue
            print(f"  KRX {mkt_name}: {cnt}개")
        except Exception as e:
            print(f"  KRX {mkt_name} 실패: {e}")
    return result

# ── Naver 실시간 시세 ───────────────────────────────────────────
def fetch_naver_realtime(codes_markets: dict) -> dict:
    """Naver polling API 100개씩 배치
    - 요청: SERVICE_ITEM:005930,000660 (코드만, suffix 없이)
    - 응답: item.cd = '005930' (코드만)
    - 거래대금: item.aa (누적거래대금 원) ÷ 1e8 → 억
    """
    result = {}
    codes = list(codes_markets.keys())
    for i in range(0, len(codes), 100):
        batch = codes[i:i+100]
        # ★ 코드만 쉼표로 연결 (suffix .KS/.KQ 붙이면 datas:0 실패)
        query = "SERVICE_ITEM:" + ",".join(batch)
        url = f"https://polling.finance.naver.com/api/realtime?query={query}"
        try:
            r = requests.get(url, headers=HEADERS, timeout=10)
            r.raise_for_status()
            areas = r.json().get("result",{}).get("areas",[])
            datas = areas[0].get("datas",[]) if areas else []
            for item in datas:
                code = str(item.get("cd","")).strip()  # 코드만 반환 ('005930')
                if not code or len(code) != 6: continue
                # suffix는 유니버스에서 결정
                mkt = codes_markets.get(code, "KOSPI")
                suffix = ".KS" if mkt == "KOSPI" else ".KQ"
                symbol = f"{code}{suffix}"
                rf = str(item.get("rf","3"))
                cr = float(item.get("cr",0) or 0)
                pct = cr if rf in ("1","2") else (-cr if rf in ("4","5") else 0.0)
                nv  = float(item.get("nv",0) or 0)   # 현재가
                # ★ aa = 누적거래대금(원) → 억 변환 (tv, cp 필드는 Naver API 미제공)
                aa  = float(item.get("aa",0) or 0)
                tv_eok = round(aa / 1e8, 2) if aa > 0 else 0.0
                result[symbol] = {
                    "naver_pct": round(pct,2), "naver_close": nv or None,
                    "naver_tv_eok": tv_eok, "naver_name": item.get("nm",""),
                }
        except Exception as e:
            print(f"  Naver 배치 {i//100+1} 실패: {e}")
        time.sleep(0.2)
    return result

# ── 병합 → by_symbol ────────────────────────────────────────────
def build_by_symbol(universe: dict, krx: dict, naver: dict) -> dict:
    by_symbol = {}
    for code, udata in universe.items():
        mkt    = udata["market"]
        suffix = ".KS" if mkt == "KOSPI" else ".KQ"
        symbol = f"{code}{suffix}"
        k = krx.get(symbol, {})
        n = naver.get(symbol, {})
        tv_eok = k.get("krx_tv_eok") or n.get("naver_tv_eok") or 0.0
        pct    = k.get("krx_pct") if k.get("krx_pct") is not None else n.get("naver_pct", 0.0)
        close  = k.get("krx_close") or n.get("naver_close")
        name   = k.get("krx_name") or n.get("naver_name") or udata["name"]
        sources = []
        if k.get("krx_tv_eok"): sources.append(f"KRX_{mkt}")
        if n.get("naver_tv_eok") and not sources: sources.append("NAVER")
        pct_f = float(pct or 0)
        tv_f  = float(tv_eok or 0)
        by_symbol[symbol] = {
            "code": code, "symbol": symbol, "name": name, "market": mkt,
            "close": close,
            "pct": round(pct_f,2),
            "pct_text": f"{'+' if pct_f>0 else ''}{pct_f:.1f}%",
            "trade_value_eok": round(tv_f,2),
            "trade_value_text": f"{tv_f/10000:.2f}조" if tv_f>=10000 else f"{tv_f:,.0f}억",
            "source_parts": sources,
        }
    return by_symbol

# ── main ─────────────────────────────────────────────────────────
def main():
    t0 = now_kst()
    trade_date = t0.strftime("%Y%m%d")
    print(f"[generate_latest_krx] {trade_date} {t0.strftime('%H:%M:%S')} KST")

    print("[1] 유니버스 로드...")
    universe = load_universe()
    print(f"    총 {len(universe)}개 종목")

    print("[2] KRX 공식 시세 수집...")
    krx = fetch_krx_official(trade_date)
    print(f"    KRX 합계: {len(krx)}개")

    print("[3] Naver 실시간 시세 수집...")
    codes_markets = {c: d["market"] for c,d in universe.items()}
    naver = fetch_naver_realtime(codes_markets)
    print(f"    Naver 합계: {len(naver)}개")

    print("[4] 병합...")
    by_symbol = build_by_symbol(universe, krx, naver)
    print(f"    by_symbol: {len(by_symbol)}개")

    fallback = len(krx) < 100
    payload = {
        "generated_at":      now_kst().isoformat(),
        "trade_date":        trade_date,
        "trade_date_display": t0.strftime("%Y-%m-%d"),
        "source":            "KRX 공식 시세 + Naver 실시간",
        "universe_name":     "Excel 기반 KOSPI+KOSDAQ 전종목 (ETF 제외)",
        "status":            "ok",
        "fallback_used":     fallback,
        "message":           "KRX 미수집, Naver 폴백" if fallback else "",
        "counts": {
            "universe": len(by_symbol),
            "top_current_30": 30,
            "top_rise_50": 50,
        },
        "leaders":    [],
        "by_symbol":  by_symbol,
    }

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(
        json.dumps(payload, ensure_ascii=False, separators=(",",":")),
        encoding="utf-8"
    )
    elapsed = (now_kst()-t0).total_seconds()
    print(f"\n✅ 저장: {OUTPUT_PATH}")
    print(f"   trade_date: {trade_date} | 종목: {len(by_symbol)}개 | {elapsed:.1f}초")

if __name__ == "__main__":
    main()
