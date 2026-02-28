"""
Stock MA Tracker — Web App Backend
FastAPI + WebSockets. Open http://localhost:8000 in any browser.
Run with: python stock_tracker_web.py
"""

import asyncio, datetime, json, os, re, time, threading
import warnings
warnings.filterwarnings("ignore")

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
import uvicorn
import yfinance as yf
import pandas as pd
import requests

# ── CONFIG ──────────────────────────────────────────────────────────
WATCHLIST_FILE        = os.path.join(os.path.dirname(os.path.abspath(__file__)), "watchlist.json")
DEFAULT_WATCHLIST     = ["AAPL","MSFT","GOOGL","AMZN","NVDA","META","TSLA","JPM","V","SPY"]
ALERT_THRESHOLD_PCT   = 2.0
AUTO_REFRESH_MINUTES  = 10
ATH_DCA_THRESHOLD_PCT = 15.0
NTFY_ENABLED          = True
NTFY_TOPIC            = "chris-ma-tracker-9274"
PORT                  = int(os.environ.get("PORT", 8000))

# ── WATCHLIST ────────────────────────────────────────────────────────

def load_watchlist():
    if os.path.exists(WATCHLIST_FILE):
        try:
            with open(WATCHLIST_FILE) as f:
                data = json.load(f)
                if isinstance(data, list) and data:
                    return [t.upper().strip() for t in data]
        except Exception:
            pass
    return list(DEFAULT_WATCHLIST)

def save_watchlist(tickers):
    try:
        with open(WATCHLIST_FILE, "w") as f:
            json.dump(tickers, f)
    except Exception as e:
        print(f"Could not save watchlist: {e}")

# ── DATA FETCHING ────────────────────────────────────────────────────

_ath_cache = {}

def _download_with_retry(ticker, max_retries=4, delay=3):
    last_err = None
    for attempt in range(max_retries):
        try:
            if attempt > 0:
                try:
                    yf.utils.get_crumb_and_cookies.cache_clear()
                except Exception:
                    pass
                session = requests.Session()
                session.headers.update({"User-Agent": f"Mozilla/5.0 (attempt {attempt})"})
                df = yf.download(ticker, period="max", interval="1d", progress=False, session=session)
            else:
                df = yf.download(ticker, period="max", interval="1d", progress=False)
            if df is not None and not df.empty:
                return df
            time.sleep(delay)
        except Exception as e:
            last_err = e
            time.sleep(delay * (attempt + 1))
    raise RuntimeError(f"Failed after {max_retries} attempts: {last_err}")


def fetch_ticker(t):
    global _ath_cache
    df = _download_with_retry(t)
    if df.empty:
        return None
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    price = float(df["Close"].iloc[-1])
    ma200 = float(df["Close"].rolling(200).mean().iloc[-1]) if len(df) >= 200 else None
    ma400 = float(df["Close"].rolling(400).mean().iloc[-1]) if len(df) >= 400 else None
    hist_ath = float(df["High"].max()) if "High" in df.columns else float(df["Close"].max())
    try:
        info_high = float(yf.Ticker(t).info.get("fiftyTwoWeekHigh", 0) or 0)
    except Exception:
        info_high = 0
    cached = _ath_cache.get(t, 0)
    ath = max(hist_ath, info_high, cached)
    _ath_cache[t] = ath
    pct_from_ath = round((price - ath) / ath * 100, 2) if ath > 0 else None
    pct200 = round((price - ma200) / ma200 * 100, 2) if ma200 else None
    pct400 = round((price - ma400) / ma400 * 100, 2) if ma400 else None
    return {
        "ticker": t,
        "price": round(price, 2),
        "ma200": round(ma200, 2) if ma200 else None,
        "ma400": round(ma400, 2) if ma400 else None,
        "pct200": pct200,
        "pct400": pct400,
        "dir200": ("above" if pct200 >= 0 else "below") if pct200 is not None else None,
        "dir400": ("above" if pct400 >= 0 else "below") if pct400 is not None else None,
        "ath": round(ath, 2),
        "pct_from_ath": pct_from_ath,
    }

def get_action(d):
    below_400 = d["pct400"] is not None and d["dir400"] == "below"
    near_400  = d["pct400"] is not None and d["dir400"] == "above" and d["pct400"] <= 10.0
    below_200 = d["pct200"] is not None and d["dir200"] == "below"
    far_ath   = d["pct_from_ath"] is not None and d["pct_from_ath"] <= -ATH_DCA_THRESHOLD_PCT
    if below_400 or near_400: return "Buy A Lot"
    elif below_200:            return "Buy Some"
    elif far_ath:              return "DCA"
    else:                      return "Hold"

def send_ntfy(title, body):
    if not NTFY_ENABLED: return
    try:
        import urllib.request
        body_ascii = body.encode("ascii", "ignore").decode("ascii")
        title_ascii = title.encode("ascii", "ignore").decode("ascii")
        req = urllib.request.Request(
            f"https://ntfy.sh/{NTFY_TOPIC}",
            data=body_ascii.encode("utf-8"),
            headers={"Title": title_ascii}, method="POST")
        urllib.request.urlopen(req, timeout=10)
    except Exception as e:
        print(f"ntfy error: {e}")

# ── FASTAPI APP ──────────────────────────────────────────────────────

app = FastAPI()

# State
_stock_data   = {}
_watchlist    = load_watchlist()
_clients      = set()
_is_fetching  = False
_last_updated = None

async def broadcast(msg: dict):
    dead = set()
    for ws in _clients:
        try:
            await ws.send_json(msg)
        except Exception:
            dead.add(ws)
    _clients.difference_update(dead)

def fetch_all_sync(tickers, loop):
    global _stock_data, _is_fetching, _last_updated
    _is_fetching = True
    results = {}
    total = len(tickers)

    for i, t in enumerate(tickers):
        asyncio.run_coroutine_threadsafe(
            broadcast({"type": "status", "msg": f"Fetching {t} ({i+1}/{total})..."}), loop)
        try:
            d = fetch_ticker(t)
            if d:
                d["action"] = get_action(d)
                results[t] = d
                asyncio.run_coroutine_threadsafe(
                    broadcast({"type": "ticker_update", "data": d}), loop)
        except Exception as e:
            print(f"Error fetching {t}: {e}")

    _stock_data = results
    _last_updated = datetime.datetime.now().strftime("%I:%M:%S %p")
    _is_fetching = False

    # Check alerts
    alerts = []
    for t, d in results.items():
        action = d["action"]
        if action == "Buy A Lot":
            alerts.append(f"Buy A Lot of {t} - ${d['price']:.2f} near 400-day MA")
        elif action == "Buy Some":
            alerts.append(f"Buy Some of {t} - ${d['price']:.2f} near 200-day MA")
        elif action == "DCA":
            alerts.append(f"DCA into {t} - ${d['price']:.2f} is {abs(d['pct_from_ath']):.1f}% below ATH")

    asyncio.run_coroutine_threadsafe(
        broadcast({"type": "refresh_complete", "last_updated": _last_updated,
                   "count": len(results), "total": total}), loop)

    if alerts:
        msg = "\n".join(alerts)
        send_ntfy("MA Alert", msg)


def start_fetch(tickers=None):
    if tickers is None:
        tickers = list(_watchlist)
    loop = asyncio.get_event_loop()
    thread = threading.Thread(target=fetch_all_sync, args=(tickers, loop), daemon=True)
    thread.start()


# ── HTTP ROUTES ──────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def serve_ui():
    html_path = os.path.join(os.path.dirname(__file__), "stock_tracker_web.html")
    if os.path.exists(html_path):
        return HTMLResponse(open(html_path).read())
    return HTMLResponse("<h1>stock_tracker_web.html not found</h1>")

@app.get("/api/data")
async def get_data():
    return {
        "data": list(_stock_data.values()),
        "watchlist": _watchlist,
        "last_updated": _last_updated,
        "is_fetching": _is_fetching
    }

@app.get("/api/watchlist")
async def get_watchlist():
    return {"watchlist": _watchlist}

@app.post("/api/watchlist")
async def update_watchlist(body: dict):
    global _watchlist
    tickers = body.get("tickers", [])
    tickers = [t.upper().strip() for t in tickers if re.match(r'^[A-Z.\-]{1,10}$', t.upper().strip())]
    if not tickers:
        return {"error": "No valid tickers provided"}
    _watchlist = tickers
    save_watchlist(tickers)
    start_fetch(tickers)
    return {"watchlist": _watchlist, "status": "saved"}

@app.post("/api/refresh")
async def trigger_refresh():
    if not _is_fetching:
        start_fetch()
    return {"status": "started"}

@app.get("/api/financials/{ticker}")
async def get_financials(ticker: str):
    ticker = ticker.upper()
    try:
        stock = yf.Ticker(ticker)
        info  = stock.info or {}
        # Earnings
        earnings = []
        try:
            ed = stock.earnings_dates
            if ed is not None and not ed.empty:
                for idx, row in ed.head(4).iterrows():
                    date_str = idx.strftime("%Y-%m-%d") if hasattr(idx, "strftime") else str(idx)[:10]
                    eps_est = row.get("EPS Estimate", row.get("Consensus EPS"))
                    eps_act = row.get("Reported EPS", row.get("Actual EPS"))
                    surprise = row.get("Surprise(%)")
                    earnings.append({
                        "date": date_str,
                        "eps_est": float(eps_est) if eps_est is not None and not pd.isna(eps_est) else None,
                        "eps_actual": float(eps_act) if eps_act is not None and not pd.isna(eps_act) else None,
                        "surprise_pct": float(surprise) if surprise is not None and not pd.isna(surprise) else None,
                    })
        except Exception:
            pass

        # Income stmt fallback
        if not earnings:
            try:
                inc = stock.quarterly_income_stmt
                if inc is not None and not inc.empty:
                    for col in list(inc.columns)[:4]:
                        q = col.strftime("%Y-%m-%d") if hasattr(col, "strftime") else str(col)[:10]
                        rev = net = eps = None
                        for rn in ["Total Revenue","Revenue"]:
                            if rn in inc.index:
                                v = inc.loc[rn, col]
                                if not pd.isna(v): rev = float(v); break
                        for nn in ["Net Income","Net Income Common Stockholders"]:
                            if nn in inc.index:
                                v = inc.loc[nn, col]
                                if not pd.isna(v): net = float(v); break
                        for en in ["Diluted EPS","Basic EPS"]:
                            if en in inc.index:
                                v = inc.loc[en, col]
                                if not pd.isna(v): eps = float(v); break
                        earnings.append({"date": q, "revenue": rev, "net_income": net, "eps": eps})
            except Exception:
                pass

        # News
        news = []
        try:
            raw_news = stock.news or []
            for item in raw_news[:8]:
                if not isinstance(item, dict): continue
                title = item.get("title","") or (item.get("content",{}).get("title","") if isinstance(item.get("content"),dict) else "")
                link  = item.get("link","") or item.get("url","")
                if not link and isinstance(item.get("content"),dict):
                    link = item["content"].get("canonicalUrl",{}).get("url","")
                pub   = item.get("publisher","")
                pt    = item.get("providerPublishTime")
                time_str = ""
                if pt:
                    try: time_str = datetime.datetime.fromtimestamp(pt).strftime("%m/%d %I:%M%p")
                    except: pass
                if title: news.append({"title":title,"link":link,"publisher":pub,"time":time_str})
        except Exception:
            pass

        ratios = {
            "pe_ttm":       info.get("trailingPE"),
            "pe_fwd":       info.get("forwardPE"),
            "pb":           info.get("priceToBook"),
            "debt_equity":  info.get("debtToEquity"),
            "profit_margin":info.get("profitMargins"),
            "gross_margin": info.get("grossMargins"),
            "op_margin":    info.get("operatingMargins"),
            "roe":          info.get("returnOnEquity"),
            "roa":          info.get("returnOnAssets"),
            "eps_ttm":      info.get("trailingEps"),
            "div_yield":    info.get("dividendYield"),
            "beta":         info.get("beta"),
        }

        return {
            "ticker": ticker,
            "name": info.get("longName") or info.get("shortName",""),
            "sector": info.get("sector","N/A"),
            "industry": info.get("industry","N/A"),
            "market_cap": info.get("marketCap"),
            "price": info.get("currentPrice") or info.get("regularMarketPrice"),
            "week52_high": info.get("fiftyTwoWeekHigh"),
            "week52_low":  info.get("fiftyTwoWeekLow"),
            "avg_volume":  info.get("averageVolume"),
            "description": (info.get("longBusinessSummary","") or "")[:500],
            "ratios": {k: v for k, v in ratios.items() if v is not None},
            "earnings": earnings,
            "news": news,
        }
    except Exception as e:
        return {"error": str(e)}


# ── WEBSOCKET ────────────────────────────────────────────────────────

@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    _clients.add(ws)
    try:
        # Send current data immediately on connect
        await ws.send_json({
            "type": "init",
            "data": list(_stock_data.values()),
            "watchlist": _watchlist,
            "last_updated": _last_updated,
            "is_fetching": _is_fetching
        })
        while True:
            msg = await ws.receive_json()
            if msg.get("action") == "refresh":
                if not _is_fetching:
                    start_fetch()
            elif msg.get("action") == "snapshot":
                now = datetime.datetime.now().strftime("%m/%d %I:%M%p")
                lines = [f"MA Snapshot {now}", ""]
                for d in sorted(_stock_data.values(), key=lambda x: x["ticker"]):
                    action = d["action"]
                    if action != "Hold":
                        lines.append(f"  {action}: {d['ticker']} ${d['price']:.2f}")
                if not any(d["action"] != "Hold" for d in _stock_data.values()):
                    lines.append("  All stocks at Hold")
                send_ntfy("MA Snapshot", "\n".join(lines))
                await ws.send_json({"type": "status", "msg": "Snapshot sent!"})
            elif msg.get("action") == "alerts":
                alerts = []
                for d in _stock_data.values():
                    action = d["action"]
                    if action == "Buy A Lot":
                        alerts.append(f"Buy A Lot of {d['ticker']} - ${d['price']:.2f}")
                    elif action == "Buy Some":
                        alerts.append(f"Buy Some of {d['ticker']} - ${d['price']:.2f}")
                    elif action == "DCA":
                        alerts.append(f"DCA into {d['ticker']} - {abs(d['pct_from_ath']):.1f}% below ATH")
                msg_body = "\n".join(alerts) if alerts else "All clear - no stocks near MA levels"
                send_ntfy("MA Alert", msg_body)
                await ws.send_json({"type": "status", "msg": f"Alerts sent! ({len(alerts)} triggered)"})
    except WebSocketDisconnect:
        _clients.discard(ws)
    except Exception:
        _clients.discard(ws)


# ── STARTUP ──────────────────────────────────────────────────────────

@app.on_event("startup")
async def on_startup():
    # Kick off first fetch after a short delay so WS clients can connect
    loop = asyncio.get_event_loop()
    loop.call_later(1.5, start_fetch)

    # Schedule auto-refresh
    async def auto_refresh():
        while True:
            await asyncio.sleep(AUTO_REFRESH_MINUTES * 60)
            if not _is_fetching:
                start_fetch()
    asyncio.create_task(auto_refresh())


if __name__ == "__main__":
    print("\n" + "="*55)
    print("  STOCK MA TRACKER — Web App")
    print("="*55)
    print(f"  Open in browser:  http://localhost:{PORT}")
    print(f"  Phone/other PC:   http://YOUR_PC_IP:{PORT}")
    print(f"  Stop server:      Ctrl+C")
    print("="*55 + "\n")
    uvicorn.run(app, host="0.0.0.0", port=PORT, log_level="info")
