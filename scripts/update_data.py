import json, os, re, time
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup

KST = timezone(timedelta(hours=9))
OUT_LATEST = "data/latest.json"
OUT_HISTORY = "data/history.json"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0 Safari/537.36",
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
}

YAHOO_PROXIES = [
    lambda sym, rng="5d", interval="1d": f"https://corsproxy.io/?url={requests.utils.quote(f'https://query1.finance.yahoo.com/v8/finance/chart/{sym}?interval={interval}&range={rng}', safe='')}",
    lambda sym, rng="5d", interval="1d": f"https://api.allorigins.win/get?url={requests.utils.quote(f'https://query1.finance.yahoo.com/v8/finance/chart/{sym}?interval={interval}&range={rng}', safe='')}",
    lambda sym, rng="5d", interval="1d": f"https://api.codetabs.com/v1/proxy?quest={requests.utils.quote(f'https://query2.finance.yahoo.com/v8/finance/chart/{sym}?interval={interval}&range={rng}', safe='')}",
]

ASSETS = {
    "sp500": ("S&P 500", "%5EGSPC", "", "Yahoo Finance (^GSPC)"),
    "nasdaq100": ("Nasdaq 100", "%5ENDX", "", "Yahoo Finance (^NDX)"),
    "dow": ("Dow Jones", "%5EDJI", "", "Yahoo Finance (^DJI)"),
    "russell2000": ("Russell 2000", "%5ERUT", "", "Yahoo Finance (^RUT)"),
    "vix": ("VIX", "%5EVIX", "", "Yahoo Finance (^VIX)"),
    "gold": ("Gold", "GC%3DF", "USD", "Yahoo Finance (GC=F)"),
    "wti": ("WTI", "CL%3DF", "USD", "Yahoo Finance (CL=F)"),
    "dxy": ("DXY", "DX-Y.NYB", "", "Yahoo Finance (DX-Y.NYB)"),
    "usdkrw": ("USD/KRW", "KRW%3DX", "KRW", "Yahoo Finance (KRW=X)"),
    "kospi": ("KOSPI", "%5EKS11", "KRW", "Yahoo Finance (^KS11)"),
    "kosdaq": ("KOSDAQ", "%5EKQ11", "KRW", "Yahoo Finance (^KQ11)"),
    "samsung": ("Samsung Electronics", "005930.KS", "KRW", "Yahoo Finance (005930.KS)"),
}

FRED_SERIES = {
    "us10y": ("US 10Y", "DGS10", "%"),
    "us2y": ("US 2Y", "DGS2", "%"),
    "hy_spread": ("HY Spread", "BAMLH0A0HYM2", "%"),
}

session = requests.Session()
session.headers.update(HEADERS)

def now_iso():
    return datetime.now(KST).isoformat()

def ensure_dirs():
    os.makedirs("data", exist_ok=True)

def parse_proxy_payload(payload: Any) -> Dict[str, Any]:
    if isinstance(payload, dict) and "contents" in payload:
        return json.loads(payload["contents"])
    return payload

def fetch_json(url: str, timeout: int = 20) -> Any:
    r = session.get(url, timeout=timeout)
    r.raise_for_status()
    return r.json()

def fetch_text(url: str, timeout: int = 20) -> str:
    r = session.get(url, timeout=timeout)
    r.raise_for_status()
    return r.text

def yahoo_chart(symbol: str, rng: str = "5d", interval: str = "1d") -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    errors = []
    for idx, proxy in enumerate(YAHOO_PROXIES, start=1):
        try:
            data = parse_proxy_payload(fetch_json(proxy(symbol, rng=rng, interval=interval), timeout=25))
            result = data["chart"]["result"][0]
            meta = result.get("meta", {})
            timestamps = result.get("timestamp") or []
            quotes = result.get("indicators", {}).get("quote", [{}])[0]
            closes = quotes.get("close") or []
            rows = [(ts, c) for ts, c in zip(timestamps, closes) if c is not None]
            if not rows:
                raise ValueError("유효 종가 없음")
            price = float(rows[-1][1])
            prev = float(rows[-2][1]) if len(rows) >= 2 else float(meta.get("previousClose") or price)
            history = [{"date": datetime.fromtimestamp(ts, timezone.utc).astimezone(KST).date().isoformat(), "close": float(c)} for ts, c in rows]
            return {
                "price": price,
                "prev": prev,
                "change": price - prev,
                "change_pct": ((price - prev) / prev * 100) if prev else None,
                "as_of": now_iso(),
                "history": history,
            }, None
        except Exception as e:
            errors.append(f"proxy{idx}: {e}")
            time.sleep(0.4)
    return None, " | ".join(errors)

def binance_btc() -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    try:
        d = fetch_json("https://api.binance.com/api/v3/ticker/24hr?symbol=BTCUSDT", timeout=20)
        price = float(d["lastPrice"])
        prev = float(d["prevClosePrice"])
        change = float(d["priceChange"])
        pct = float(d["priceChangePercent"])
        kl = fetch_json("https://api.binance.com/api/v3/klines?symbol=BTCUSDT&interval=1d&limit=120", timeout=20)
        history = [{"date": datetime.fromtimestamp(int(x[0]) / 1000, timezone.utc).astimezone(KST).date().isoformat(), "close": float(x[4])} for x in kl]
        return {"price": price, "prev": prev, "change": change, "change_pct": pct, "as_of": now_iso(), "history": history}, None
    except Exception as e:
        return None, str(e)

def alt_fear_greed() -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    try:
        d = fetch_json("https://api.alternative.me/fng/", timeout=20)
        item = d["data"][0]
        return {
            "score": int(item["value"]),
            "label": item["value_classification"],
            "as_of": now_iso(),
            "source": "alternative.me",
            "status": "ok",
            "error": None,
        }, None
    except Exception as e:
        return None, str(e)

def fred_csv(series: str) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    try:
        text = fetch_text(f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={series}", timeout=20)
        rows = []
        for line in text.splitlines()[1:]:
            parts = line.split(",")
            if len(parts) < 2:
                continue
            try:
                val = float(parts[1])
            except Exception:
                continue
            rows.append((parts[0], val))
        if not rows:
            raise ValueError("관측값 없음")
        latest_date, latest_val = rows[-1]
        prev_val = rows[-2][1] if len(rows) >= 2 else latest_val
        history = [{"date": d, "close": v} for d, v in rows[-120:]]
        return {
            "price": latest_val,
            "prev": prev_val,
            "change": latest_val - prev_val,
            "change_pct": ((latest_val - prev_val) / prev_val * 100) if prev_val else None,
            "as_of": latest_date,
            "history": history,
        }, None
    except Exception as e:
        return None, str(e)

def naver_index(code: str, label: str) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    # code: KOSPI / KOSDAQ
    try:
        url = f"https://finance.naver.com/sise/sise_index_day.naver?code={code}&page=1"
        html = fetch_text(url, timeout=20)
        nums = re.findall(r'<td class="number_1">([\d,\.]+)</td>', html)
        # expected order per row: close, change, open, high, low, volume...
        if len(nums) < 7:
            raise ValueError("네이버 지수 파싱 실패")
        today = float(nums[0].replace(",", ""))
        prev_close = float(nums[6].replace(",", "")) if len(nums) >= 7 else None
        change = today - prev_close if prev_close is not None else None
        pct = (change / prev_close * 100) if prev_close else None

        # collect first page rows: 6 rows * close only from first number cell every 4-ish? use date+close robust parse
        rows = re.findall(r'<td class="date">(\d{4}\.\d{2}\.\d{2})</td>\s*<td class="number_1">([\d,\.]+)</td>', html)
        history = [{"date": d.replace(".", "-"), "close": float(v.replace(",", ""))} for d, v in rows]
        return {"price": today, "prev": prev_close, "change": change, "change_pct": pct, "as_of": now_iso(), "history": history}, None
    except Exception as e:
        return None, str(e)

def naver_stock(code: str) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    try:
        url = f"https://finance.naver.com/item/sise_day.naver?code={code}&page=1"
        html = fetch_text(url, timeout=20)
        rows = re.findall(r'<span class="tah p10 gray03">(\d{4}\.\d{2}\.\d{2})</span>.*?<span class="tah p11">([\d,]+)</span>', html, re.S)
        if len(rows) < 2:
            raise ValueError("네이버 종목 파싱 실패")
        history = [{"date": d.replace(".", "-"), "close": float(v.replace(",", ""))} for d, v in rows]
        price = history[0]["close"]
        prev = history[1]["close"] if len(history) >= 2 else price
        change = price - prev
        pct = (change / prev * 100) if prev else None
        return {"price": price, "prev": prev, "change": change, "change_pct": pct, "as_of": now_iso(), "history": history}, None
    except Exception as e:
        return None, str(e)

def asset_obj(key: str, label: str, unit: str, source: str, data: Optional[Dict[str, Any]], error: Optional[str]) -> Dict[str, Any]:
    ok = data is not None and error is None
    return {
        "key": key,
        "label": label,
        "value": round(data["price"], 6) if ok and data.get("price") is not None else None,
        "prev": round(data["prev"], 6) if ok and data.get("prev") is not None else None,
        "change": round(data["change"], 6) if ok and data.get("change") is not None else None,
        "change_pct": round(data["change_pct"], 6) if ok and data.get("change_pct") is not None else None,
        "unit": unit,
        "source": source,
        "as_of": data.get("as_of") if ok else now_iso(),
        "error": None if ok else error,
        "status": "ok" if ok else "error",
    }

def main():
    ensure_dirs()
    logs: List[str] = []
    assets: Dict[str, Dict[str, Any]] = {}
    history: Dict[str, Any] = {"generated_at": now_iso(), "timezone": "Asia/Seoul", "series": {}}

    for key, (label, sym, unit, source) in ASSETS.items():
        if key == "samsung":
            data, err = naver_stock("005930")
            if err:
                ydata, yerr = yahoo_chart(sym)
                data, err = (ydata, None) if ydata else (None, f"naver:{err} | yahoo:{yerr}")
        elif key == "kospi":
            data, err = yahoo_chart(sym)
            if err:
                ndata, nerr = naver_index("KOSPI", label)
                data, err = (ndata, None) if ndata else (None, f"yahoo:{err} | naver:{nerr}")
        elif key == "kosdaq":
            data, err = yahoo_chart(sym)
            if err:
                ndata, nerr = naver_index("KOSDAQ", label)
                data, err = (ndata, None) if ndata else (None, f"yahoo:{err} | naver:{nerr}")
        else:
            data, err = yahoo_chart(sym)
        assets[key] = asset_obj(key, label, unit, source, data, err)
        if err:
            logs.append(f"{label}: {err}")
        if data and data.get("history"):
            history["series"][key] = data["history"]

    btc, err = binance_btc()
    assets["bitcoin"] = asset_obj("bitcoin", "Bitcoin", "USD", "Binance", btc, err)
    if err:
        logs.append(f"Bitcoin: {err}")
    elif btc.get("history"):
        history["series"]["bitcoin"] = btc["history"]

    for key, (label, series, unit) in FRED_SERIES.items():
        data, err = fred_csv(series)
        assets[key] = asset_obj(key, label, unit, f"FRED {series}", data, err)
        if err:
            logs.append(f"{label}: {err}")
        elif data.get("history"):
            history["series"][key] = data["history"]

    fg, fg_err = alt_fear_greed()
    if fg_err:
        logs.append(f"Fear & Greed: {fg_err}")
        fear_greed = {
            "score": None, "label": "데이터 없음", "as_of": None,
            "source": "alternative.me", "status": "error", "error": fg_err
        }
    else:
        fear_greed = fg

    us10 = assets.get("us10y", {}).get("value")
    us2 = assets.get("us2y", {}).get("value")
    if us10 is not None and us2 is not None:
        val = us10 - us2
        assets["yield_spread"] = {
            "key": "yield_spread",
            "label": "US 10Y-2Y",
            "value": round(val, 6),
            "prev": None,
            "change": None,
            "change_pct": None,
            "unit": "%p",
            "source": "computed from FRED",
            "as_of": now_iso(),
            "error": None,
            "status": "ok",
        }
    else:
        assets["yield_spread"] = {
            "key": "yield_spread", "label": "US 10Y-2Y", "value": None, "prev": None, "change": None, "change_pct": None,
            "unit": "%p", "source": "computed from FRED", "as_of": now_iso(), "error": "금리 데이터 없음", "status": "error"
        }

    latest = {
        "generated_at": now_iso(),
        "timezone": "Asia/Seoul",
        "mode": "snapshot",
        "repo_note": "Integrated from the user's working HTML source chain: Yahoo proxy fallbacks + Binance + alternative.me + FRED CSV + Naver fallback.",
        "logs": logs,
        "fear_greed": fear_greed,
        "assets": assets,
    }

    with open(OUT_LATEST, "w", encoding="utf-8") as f:
        json.dump(latest, f, ensure_ascii=False, indent=2)
    with open(OUT_HISTORY, "w", encoding="utf-8") as f:
        json.dump(history, f, ensure_ascii=False, indent=2)
    print(f"Wrote {OUT_LATEST}")
    print(f"Wrote {OUT_HISTORY}")

if __name__ == "__main__":
    main()
