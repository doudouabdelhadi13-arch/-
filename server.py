#!/usr/bin/env python3
"""
========================================================================
 TradeMaster Pro — Backend / Market Data Engine
========================================================================
Zero-dependency (stdlib + requests) market data server that powers the
TradeMaster Pro trading platform.

Providers (with automatic fallback):
  CRYPTO   : OKX  ->  Coinbase Exchange  ->  Kraken  ->  CoinGecko
  FX/STOCK : Yahoo Finance chart API
  INDEX    : Yahoo Finance
  COMMODITY: Yahoo Finance futures
  SENTIMENT: alternative.me (Crypto Fear & Greed Index)
  NEWS     : Yahoo Finance / Investing.com / CoinDesk / CNBC RSS
  EDUCATION: YouTube (search + metadata scraping, no API key needed)

Everything is cached in memory (TTL) and mirrored to ./data/cache so
restarts stay fast and public rate-limits are respected.
========================================================================
"""
import json
import os
import re
import sys
import threading
import time
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

try:
    import requests
    HAVE_REQUESTS = True
except Exception:
    HAVE_REQUESTS = False

BASE = os.path.dirname(os.path.abspath(__file__))
PUBLIC = os.path.join(BASE, "public")
DATA = os.path.join(BASE, "data")
CACHE_DIR = os.path.join(DATA, "cache")
for d in (DATA, CACHE_DIR, PUBLIC):
    os.makedirs(d, exist_ok=True)

PORT = int(os.environ.get("PORT", "8000"))
HOST = os.environ.get("HOST", "0.0.0.0")
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")

# --------------------------------------------------------------------- #
#  HTTP helper                                                          #
# --------------------------------------------------------------------- #
_sess = None
if HAVE_REQUESTS:
    _sess = requests.Session()
    _sess.headers.update({"User-Agent": UA, "Accept-Language": "en-US,en;q=0.9,ar;q=0.8"})


def http_get_bytes(url, timeout=12, headers=None):
    """Fetch raw bytes (images/fonts). Used by the thumbnail proxy."""
    hdrs = {"User-Agent": UA, "Accept": "*/*"}
    if headers:
        hdrs.update(headers)
    try:
        if _sess is not None:
            r = _sess.get(url, timeout=timeout, headers=hdrs)
            r.raise_for_status()
            return r.content, r.headers.get("Content-Type", "image/jpeg")
        req = urllib.request.Request(url, headers=hdrs)
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.read(), resp.headers.get("Content-Type", "image/jpeg")
    except Exception:
        return None, None


def http_get(url, timeout=20, headers=None, as_json=True):
    hdrs = {"User-Agent": UA, "Accept-Language": "en-US,en;q=0.9,ar;q=0.8"}
    if headers:
        hdrs.update(headers)
    if _sess is not None:
        r = _sess.get(url, timeout=timeout, headers=hdrs)
        r.raise_for_status()
        return r.json() if as_json else r.text
    req = urllib.request.Request(url, headers=hdrs)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        raw = resp.read().decode("utf-8", "ignore")
    return json.loads(raw) if as_json else raw


# --------------------------------------------------------------------- #
#  Cache                                                                #
# --------------------------------------------------------------------- #
class Cache:
    def __init__(self):
        self.mem = {}
        self.lock = threading.Lock()

    @staticmethod
    def _key(*a):
        return "|".join(str(x) for x in a)

    def get(self, *a):
        k = self._key(*a)
        with self.lock:
            hit = self.mem.get(k)
        if not hit:
            return None
        val, exp = hit
        if time.time() > exp:
            return None
        return val

    def set(self, val, ttl, *a):
        k = self._key(*a)
        with self.lock:
            self.mem[k] = (val, time.time() + ttl)
        return val

    def get_or_set(self, ttl, producer, *a):
        v = self.get(*a)
        if v is not None:
            return v
        v = producer()
        if v is not None:
            self.set(v, ttl, *a)
        return v


cache = Cache()


def ttl_for(interval):
    """Fresh enough for live feel, gentle on public APIs."""
    if interval in ("1m", "3m", "5m"):
        return 20
    if interval in ("15m", "30m"):
        return 40
    if interval in ("1h", "2h", "4h"):
        return 90
    return 300


# ===================================================================== #
#  SYMBOL UNIVERSE                                                      #
# ===================================================================== #
CRYPTO_MAJOR = [
    ("BTC-USDT", "BTC/USDT", "Bitcoin"), ("ETH-USDT", "ETH/USDT", "Ethereum"),
    ("SOL-USDT", "SOL/USDT", "Solana"), ("XRP-USDT", "XRP/USDT", "XRP"),
    ("BNB-USDT", "BNB/USDT", "BNB"), ("DOGE-USDT", "DOGE/USDT", "Dogecoin"),
    ("ADA-USDT", "ADA/USDT", "Cardano"), ("AVAX-USDT", "AVAX/USDT", "Avalanche"),
    ("LINK-USDT", "LINK/USDT", "Chainlink"), ("DOT-USDT", "DOT/USDT", "Polkadot"),
    ("LTC-USDT", "LTC/USDT", "Litecoin"), ("TRX-USDT", "TRX/USDT", "TRON"),
    ("TON-USDT", "TON/USDT", "Toncoin"), ("SUI-USDT", "SUI/USDT", "Sui"),
    ("NEAR-USDT", "NEAR/USDT", "NEAR"), ("APT-USDT", "APT/USDT", "Aptos"),
    ("ARB-USDT", "ARB/USDT", "Arbitrum"), ("OP-USDT", "OP/USDT", "Optimism"),
    ("UNI-USDT", "UNI/USDT", "Uniswap"), ("ATOM-USDT", "ATOM/USDT", "Cosmos"),
    ("FIL-USDT", "FIL/USDT", "Filecoin"), ("ICP-USDT", "ICP/USDT", "Internet Computer"),
    ("INJ-USDT", "INJ/USDT", "Injective"), ("SEI-USDT", "SEI/USDT", "Sei"),
    ("TIA-USDT", "TIA/USDT", "Celestia"), ("PEPE-USDT", "PEPE/USDT", "Pepe"),
    ("SHIB-USDT", "SHIB/USDT", "Shiba Inu"), ("WIF-USDT", "WIF/USDT", "dogwifhat"),
    ("RENDER-USDT", "RNDR/USDT", "Render"), ("FET-USDT", "FET/USDT", "Fetch.ai"),
    ("AAVE-USDT", "AAVE/USDT", "Aave"), ("MKR-USDT", "MKR/USDT", "Maker"),
    ("ETC-USDT", "ETC/USDT", "Ethereum Classic"), ("XLM-USDT", "XLM/USDT", "Stellar"),
    ("ALGO-USDT", "ALGO/USDT", "Algorand"), ("SAND-USDT", "SAND/USDT", "The Sandbox"),
    ("MANA-USDT", "MANA/USDT", "Decentraland"), ("GRT-USDT", "GRT/USDT", "The Graph"),
    ("HBAR-USDT", "HBAR/USDT", "Hedera"), ("VET-USDT", "VET/USDT", "VeChain"),
    ("ONDO-USDT", "ONDO/USDT", "Ondo"), ("JUP-USDT", "JUP/USDT", "Jupiter"),
    ("ENA-USDT", "ENA/USDT", "Ethena"), ("PENDLE-USDT", "PENDLE/USDT", "Pendle"),
]

FOREX = [
    ("EURUSD=X", "EUR/USD", "يورو / دولار", 5), ("GBPUSD=X", "GBP/USD", "إسترليني / دولار", 5),
    ("USDJPY=X", "USD/JPY", "دولار / ين", 3), ("USDCHF=X", "USD/CHF", "دولار / فرنك", 5),
    ("AUDUSD=X", "AUD/USD", "أسترالي / دولار", 5), ("USDCAD=X", "USD/CAD", "دولار / كندي", 5),
    ("NZDUSD=X", "NZD/USD", "نيوزلندي / دولار", 5), ("EURGBP=X", "EUR/GBP", "يورو / إسترليني", 5),
    ("EURJPY=X", "EUR/JPY", "يورو / ين", 3), ("GBPJPY=X", "GBP/JPY", "إسترليني / ين", 3),
    ("AUDJPY=X", "AUD/JPY", "أسترالي / ين", 3), ("EURCHF=X", "EUR/CHF", "يورو / فرنك", 5),
    ("CADJPY=X", "CAD/JPY", "كندي / ين", 3), ("CHFJPY=X", "CHF/JPY", "فرنك / ين", 3),
    ("USDTRY=X", "USD/TRY", "دولار / ليرة تركية", 5), ("USDZAR=X", "USD/ZAR", "دولار / راند", 5),
    ("USDSAR=X", "USD/SAR", "دولار / ريال سعودي", 5), ("USDEGP=X", "USD/EGP", "دولار / جنيه مصري", 4),
    ("USDAED=X", "USD/AED", "دولار / درهم إماراتي", 4), ("USDQAR=X", "USD/QAR", "دولار / ريال قطري", 4),
    ("USDKWD=X", "USD/KWD", "دولار / دينار كويتي", 4), ("USDINR=X", "USD/INR", "دولار / روبية هندية", 4),
    ("DXY=X", "DXY", "مؤشر الدولار", 3),
]

COMMODITIES = [
    ("GC=F", "XAU/USD", "الذهب", 2), ("SI=F", "XAG/USD", "الفضة", 3),
    ("CL=F", "WTI", "خام غرب تكساس", 2), ("BZ=F", "BRENT", "خام برنت", 2),
    ("NG=F", "NATGAS", "الغاز الطبيعي", 3), ("HG=F", "COPPER", "النحاس", 4),
    ("ZW=F", "WHEAT", "القمح", 2), ("ZC=F", "CORN", "الذرة", 2),
    ("KC=F", "COFFEE", "القهوة", 2), ("ZS=F", "SOYBEAN", "فول الصويا", 2),
]

INDICES = [
    ("^GSPC", "S&P 500", "إس آند بي 500", 2), ("^IXIC", "NASDAQ", "ناسداك 100", 2),
    ("^DJI", "DOW JONES", "داو جونز", 2), ("^RUT", "RUSSELL 2000", "راسل 2000", 2),
    ("^VIX", "VIX", "مؤشر الخوف", 2), ("^FTSE", "FTSE 100", "فوتسي البريطاني", 2),
    ("^GDAXI", "DAX 40", "داكس الألماني", 2), ("^FCHI", "CAC 40", "كاك الفرنسي", 2),
    ("^N225", "NIKKEI 225", "نيكي الياباني", 2), ("^TASI", "TASI", "تاسي السعودي", 2),
    ("^TA125.TA", "TA-125", "تل أبيب 125", 2), ("^BSESN", "SENSEX", "سينسكس الهندي", 2),
]

STOCKS = [
    ("AAPL", "Apple", 2), ("MSFT", "Microsoft", 2), ("NVDA", "NVIDIA", 2),
    ("TSLA", "Tesla", 2), ("AMZN", "Amazon", 2), ("GOOGL", "Alphabet", 2),
    ("META", "Meta", 2), ("AMD", "AMD", 2), ("NFLX", "Netflix", 2),
    ("JPM", "JPMorgan", 2), ("V", "Visa", 2), ("WMT", "Walmart", 2),
    ("DIS", "Disney", 2), ("BA", "Boeing", 2), ("INTC", "Intel", 2),
    ("COIN", "Coinbase", 2), ("MSTR", "MicroStrategy", 2), ("PLTR", "Palantir", 2),
    ("SPY", "SPDR S&P500 ETF", 2), ("QQQ", "Invesco QQQ ETF", 2),
    ("GLD", "SPDR Gold ETF", 2), ("USO", "United States Oil ETF", 2),
    ("XOM", "Exxon Mobil", 2), ("PFE", "Pfizer", 2), ("NKE", "Nike", 2),
    ("UBER", "Uber", 2), ("SQ", "Block", 2), ("SHOP", "Shopify", 2),
]

MARKETS = {
    "crypto":    {"label_ar": "العملات الرقمية", "label_en": "Crypto",
                  "items": [{"id": a, "symbol": b, "name": c, "dp": 6} for a, b, c in CRYPTO_MAJOR]},
    "forex":     {"label_ar": "الفوركس", "label_en": "Forex",
                  "items": [{"id": a, "symbol": b, "name": c, "dp": d} for a, b, c, d in FOREX]},
    "commodities": {"label_ar": "السلع والمعادن", "label_en": "Commodities",
                    "items": [{"id": a, "symbol": b, "name": c, "dp": d} for a, b, c, d in COMMODITIES]},
    "indices":   {"label_ar": "المؤشرات", "label_en": "Indices",
                  "items": [{"id": a, "symbol": b, "name": c, "dp": d} for a, b, c, d in INDICES]},
    "stocks":    {"label_ar": "الأسهم", "label_en": "Stocks",
                  "items": [{"id": a, "symbol": b, "dp": d} for a, b, d in STOCKS]},
}
for m in MARKETS.values():
    for it in m["items"]:
        it.setdefault("name", it["symbol"])

LOOKUP = {}
for mk, mv in MARKETS.items():
    for it in mv["items"]:
        LOOKUP[(mk, it["id"])] = it
        LOOKUP[(mk, it["symbol"])] = it


def resolve(market, symbol):
    it = LOOKUP.get((market, symbol))
    if it:
        return it
    # fuzzy: allow "BTCUSDT" for crypto id "BTC-USDT"
    if market == "crypto":
        norm = symbol.replace("-", "").replace("/", "").upper()
        for (mk, k), v in LOOKUP.items():
            if mk != "crypto":
                continue
            if k.replace("-", "").replace("/", "").upper() == norm:
                return v
    # any market fallback
    for (mk, k), v in LOOKUP.items():
        if k.upper() == symbol.upper():
            return v
    return {"id": symbol, "symbol": symbol, "name": symbol, "dp": 4}


# ===================================================================== #
#  INTERVAL MAPPING                                                     #
# ===================================================================== #
OKX_BAR = {"1m": "1m", "3m": "3m", "5m": "5m", "15m": "15m", "30m": "30m",
           "1h": "1H", "2h": "2H", "4h": "4H", "6h": "6H", "12h": "12H",
           "1d": "1D", "3d": "3D", "1w": "1W", "1M": "1M"}
CB_GRAN = {"1m": 60, "5m": 300, "15m": 900, "30m": 1800, "1h": 3600,
           "2h": 7200, "4h": 14400, "6h": 21600, "12h": 43200, "1d": 86400}
KRAKEN_INT = {"1m": 1, "5m": 5, "15m": 15, "30m": 30, "1h": 60, "4h": 240,
              "1d": 1440, "1w": 10080}
# interval -> (yahoo base interval, merge factor)
YH_MAP = {"1m": ("1m", 1), "3m": ("3m", 1), "5m": ("5m", 1), "15m": ("15m", 1),
          "30m": ("30m", 1), "1h": ("60m", 1), "2h": ("60m", 2), "4h": ("60m", 4),
          "6h": ("60m", 6), "12h": ("60m", 12), "1d": ("1d", 1), "3d": ("1d", 3),
          "1w": ("1wk", 1), "1M": ("1mo", 1)}
YH_INT = {k: v[0] for k, v in YH_MAP.items()}
INTERVAL_SECONDS = {"1m": 60, "3m": 180, "5m": 300, "15m": 900, "30m": 1800,
                    "1h": 3600, "2h": 7200, "4h": 14400, "6h": 21600,
                    "12h": 43200, "1d": 86400, "3d": 259200,
                    "1w": 604800, "1M": 2592000}
ALL_INTERVALS = list(INTERVAL_SECONDS.keys())


def normalise_candles(rows):
    """rows: list of [ts(ms), o,h,l,c,v] already numeric & ascending."""
    out, last = [], -1
    for r in rows:
        try:
            ts, o, h, l, c, v = int(r[0]), float(r[1]), float(r[2]), float(r[3]), float(r[4]), float(r[5])
        except Exception:
            continue
        if ts <= last or o <= 0:
            continue
        last = ts
        out.append({"t": ts, "o": o, "h": h, "l": l, "c": c, "v": round(v, 6)})
    return out


# ===================================================================== #
#  CRYPTO PROVIDERS                                                     #
# ===================================================================== #
def okx_candles(inst, interval, limit):
    bar = OKX_BAR.get(interval)
    if not bar:
        raise ValueError("interval")
    out, after = [], ""
    per = min(300, max(limit, 1))
    for _ in range(min(12, (limit + per - 1) // per)):
        url = f"https://www.okx.com/api/v5/market/history-candles?instId={inst}&bar={bar}&limit={per}"
        if after:
            url += f"&after={after}"
        j = http_get(url, timeout=18)
        data = j.get("data") or []
        if not data:
            break
        for d in data:
            out.append([int(d[0]), d[1], d[2], d[3], d[4], d[5]])
        after = data[-1][0]
        if len(out) >= limit:
            break
        time.sleep(0.06)
    out.sort(key=lambda x: x[0])
    return normalise_candles(out[-limit:])


def okx_inst_id(symbol):
    if "-" in symbol:
        return symbol.upper()
    s = symbol.replace("/", "").upper()
    if s.endswith("USDT"):
        return s[:-4] + "-USDT"
    if s.endswith("USDC"):
        return s[:-4] + "-USDC"
    return s + "-USDT"


def coinbase_candles(symbol, interval, limit):
    gran = CB_GRAN.get(interval)
    if not gran:
        raise ValueError("interval")
    prod = symbol if "-" in symbol else None
    if prod is None:
        s = symbol.replace("/", "").upper()
        prod = (s[:-4] + "-USD") if s.endswith("USDT") else (s[:-4] + "-USD" if s.endswith("USDC") else s + "-USD")
    end = int(time.time())
    out = []
    for _ in range(min(10, (limit + 299) // 300)):
        start = end - gran * 300
        url = (f"https://api.exchange.coinbase.com/products/{prod}/candles"
               f"?granularity={gran}&start={time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(start))}"
               f"&end={time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(end))}")
        j = http_get(url, timeout=18)
        if not isinstance(j, list) or not j:
            break
        for row in j:  # [time, low, high, open, close, volume]
            out.append([int(row[0]) * 1000, row[3], row[2], row[1], row[4], row[5]])
        end = start - 1
        if len(out) >= limit:
            break
        time.sleep(0.05)
    out.sort(key=lambda x: x[0])
    return normalise_candles(out[-limit:])


def kraken_pair(symbol):
    s = symbol.replace("/", "").replace("-", "").upper()
    if s.endswith("USDT"):
        base = s[:-4]
        return (base if base != "BTC" else "XBT") + "USDT"
    base = s[:-3] if s.endswith("USD") else s
    return (base if base != "BTC" else "XBT") + "USD"


def kraken_candles(symbol, interval, limit):
    iv = KRAKEN_INT.get(interval)
    if not iv:
        raise ValueError("interval")
    url = f"https://api.kraken.com/0/public/OHLC?pair={kraken_pair(symbol)}&interval={iv}"
    j = http_get(url, timeout=18)
    res = j.get("result") or {}
    key = [k for k in res if k != "last"]
    if not key:
        raise ValueError("no data")
    rows = res[key[0]]
    out = [[int(r[0]) * 1000, r[1], r[2], r[3], r[4], r[6]] for r in rows]
    return normalise_candles(out)[-limit:]


def coingecko_candles(symbol, interval, limit):
    coin = symbol.replace("/", "").replace("-USDT", "").lower()
    names = {"btc": "bitcoin", "eth": "ethereum", "sol": "solana", "xrp": "ripple",
             "bnb": "binancecoin", "doge": "dogecoin", "ada": "cardano", "avax": "avalanche-2",
             "link": "chainlink", "dot": "polkadot", "ltc": "litecoin", "trx": "tron",
             "ton": "the-open-network", "sui": "sui", "near": "near", "apt": "aptos",
             "arb": "arbitrum", "op": "optimism", "uni": "uniswap", "atom": "cosmos",
             "fil": "filecoin", "icp": "internet-computer", "inj": "injective-protocol",
             "sei": "sei-network", "tia": "celestia", "pepe": "pepe", "shib": "shiba-inu",
             "etc": "ethereum-classic", "xlm": "stellar", "algo": "algorand",
             "sand": "the-sandbox", "mana": "decentraland", "grt": "the-graph",
             "hbar": "hedera-hashgraph", "vet": "vechain", "aave": "aave", "mkr": "maker",
             "usdt": "tether"}
    cid = names.get(coin, coin)
    sec = INTERVAL_SECONDS.get(interval, 3600)
    days = max(1, min(365, (limit * sec) // 86400 + 1))
    if days <= 1:
        d = "1"
    elif days <= 7:
        d = "7"
    elif days <= 30:
        d = "30"
    elif days <= 90:
        d = "90"
    elif days <= 180:
        d = "180"
    elif days <= 365:
        d = "365"
    else:
        d = "max"
    j = http_get(f"https://api.coingecko.com/api/v3/coins/{cid}/ohlc?vs_currency=usd&days={d}", timeout=20)
    rows = [[int(r[0]), r[1], r[2], r[3], r[4], 0] for r in (j or [])]
    # resample to requested interval
    if sec > 1800 and rows:
        buckets = {}
        for ts, o, h, l, c, v in rows:
            b = ts - (ts % (sec * 1000))
            if b not in buckets:
                buckets[b] = [b, o, h, l, c, 0]
            else:
                e = buckets[b]
                e[2] = max(e[2], h); e[3] = min(e[3], l); e[4] = c
        rows = [buckets[k] for k in sorted(buckets)]
    return normalise_candles(rows)[-limit:]


CRYPTO_PROVIDERS = [
    ("okx", lambda s, i, n: okx_candles(okx_inst_id(s), i, n)),
    ("coinbase", coinbase_candles),
    ("kraken", kraken_candles),
    ("coingecko", coingecko_candles),
]


def fetch_crypto(symbol, interval, limit):
    errs = []
    for name, fn in CRYPTO_PROVIDERS:
        try:
            c = fn(symbol, interval, limit)
            if c and len(c) >= 3:
                return c, name
        except Exception as e:
            errs.append(f"{name}:{e}")
    raise RuntimeError("all crypto providers failed: " + "; ".join(errs[:4]))


# ===================================================================== #
#  YAHOO PROVIDER (forex / stocks / indices / commodities)              #
# ===================================================================== #
def yahoo_candles(yid, interval, limit):
    m = YH_MAP.get(interval)
    if not m:
        raise ValueError("interval")
    iv, merge = m
    base_sec = {"1m": 60, "3m": 180, "5m": 300, "15m": 900, "30m": 1800,
                "60m": 3600, "1d": 86400, "1wk": 604800, "1mo": 2592000}[iv]
    span = int(limit * merge * base_sec * 1.5) + 86400 * 3
    if iv == "1m":
        span = min(span, 7 * 86400)
    elif iv in ("3m", "5m", "15m", "30m"):
        span = min(span, 59 * 86400)
    elif iv == "60m":
        span = min(span, 725 * 86400)
    need = limit * merge
    end = int(time.time())
    start = end - span
    url = (f"https://query1.finance.yahoo.com/v8/finance/chart/{urllib.parse.quote(yid)}"
           f"?interval={iv}&period1={start}&period2={end}&includePrePost=false&events=div%2Csplit")
    j = http_get(url, timeout=22, headers={"Accept": "application/json"})
    res = ((j.get("chart") or {}).get("result") or [])
    if not res:
        raise RuntimeError("yahoo: empty result")
    r = res[0]
    ts = (r.get("timestamp") or [])
    q = (r.get("indicators") or {}).get("quote") or [{}]
    q = q[0]
    o, h, l, c, v = q.get("open") or [], q.get("high") or [], q.get("low") or [], q.get("close") or [], q.get("volume") or []
    rows = []
    for i in range(len(ts)):
        try:
            if None in (o[i], h[i], l[i], c[i]):
                continue
            rows.append([ts[i] * 1000, o[i], h[i], l[i], c[i], (v[i] if i < len(v) and v[i] else 0)])
        except Exception:
            continue
    out = normalise_candles(rows)
    if not out:
        raise RuntimeError("yahoo: no candles")
    # collapse base bars into the requested timeframe (2h/4h/6h/12h/3d)
    if merge > 1:
        # align merge windows to the interval boundary so candles start correctly
        step = base_sec * merge * 1000
        buckets = {}
        for cd in out:
            b = cd["t"] - (cd["t"] % step)
            e = buckets.get(b)
            if e is None:
                buckets[b] = {"t": b, "o": cd["o"], "h": cd["h"], "l": cd["l"], "c": cd["c"], "v": cd["v"]}
            else:
                e["h"] = max(e["h"], cd["h"]); e["l"] = min(e["l"], cd["l"])
                e["c"] = cd["c"]; e["v"] = round(e["v"] + cd["v"], 6)
        out = [buckets[k] for k in sorted(buckets)]
    return out[-limit:]


def yahoo_quote(yid):
    url = (f"https://query1.finance.yahoo.com/v8/finance/chart/{urllib.parse.quote(yid)}"
           f"?interval=1d&range=5d")
    j = http_get(url, timeout=18, headers={"Accept": "application/json"})
    res = ((j.get("chart") or {}).get("result") or [])
    if not res:
        raise RuntimeError("no data")
    m = res[0].get("meta") or {}
    price = m.get("regularMarketPrice")
    prev = m.get("chartPreviousClose") or m.get("previousClose")
    q = ((res[0].get("indicators") or {}).get("quote") or [{}])[0]
    closes = [x for x in (q.get("close") or []) if x]
    if prev is None and len(closes) > 1:
        prev = closes[-2]
    chg = (price - prev) if (price is not None and prev) else None
    chgp = (chg / prev * 100) if (chg is not None and prev) else None
    return {"price": price, "prevClose": prev, "change": chg, "changePct": chgp,
            "dayHigh": m.get("regularMarketDayHigh"), "dayLow": m.get("regularMarketDayLow"),
            "currency": m.get("currency"), "name": m.get("longName") or m.get("shortName"),
            "volume": m.get("regularMarketVolume"), "time": (m.get("regularMarketTime") or 0) * 1000,
            "yearHigh": m.get("fiftyTwoWeekHigh"), "yearLow": m.get("fiftyTwoWeekLow")}


# ===================================================================== #
#  PUBLIC DATA API                                                      #
# ===================================================================== #
class UpstreamUnavailable(RuntimeError):
    """Raised when a market-data provider cannot supply a usable response."""

def get_klines(market, symbol, interval, limit):
    market = market if market in MARKETS else "crypto"
    interval = interval if interval in INTERVAL_SECONDS else "1h"
    limit = max(30, min(int(limit or 500), 2000))
    ck = ("klines", market, symbol, interval, limit)
    hit = cache.get(*ck)
    if hit:
        return hit
    item = resolve(market, symbol)
    src = "yahoo"
    try:
        if market == "crypto":
            candles, src = fetch_crypto(item["id"], interval, limit)
        else:
            candles = yahoo_candles(item["id"], interval, limit)
            src = "yahoo"
    except Exception as exc:
        # A valid request with an offline/rate-limited provider is not an
        # internal server error. Keep the distinction visible to the client.
        raise UpstreamUnavailable(
            f"market data provider unavailable: {type(exc).__name__}: {exc}"
        ) from exc
    payload = {"market": market, "symbol": item["symbol"], "id": item["id"],
               "name": item.get("name", item["symbol"]), "interval": interval,
               "provider": src, "dp": item.get("dp", 4), "candles": candles,
               "fetchedAt": int(time.time() * 1000)}
    cache.set(payload, ttl_for(interval), *ck)
    return payload


def get_quote(market, symbol):
    item = resolve(market, symbol)
    ck = ("quote", market, item["id"])
    hit = cache.get(*ck)
    if hit:
        return hit
    q = None
    if market == "crypto":
        try:
            j = http_get(f"https://www.okx.com/api/v5/market/ticker?instId={okx_inst_id(item['id'])}", timeout=15)
            d = (j.get("data") or [{}])[0]
            last = float(d.get("last") or 0)
            op = float(d.get("open24h") or 0)
            q = {"price": last, "prevClose": op,
                 "change": last - op, "changePct": ((last - op) / op * 100) if op else 0,
                 "dayHigh": float(d.get("high24h") or 0), "dayLow": float(d.get("low24h") or 0),
                 "volume": float(d.get("volCcy24h") or 0), "currency": "USDT",
                 "name": item.get("name"), "time": int(d.get("ts") or time.time() * 1000),
                 "provider": "okx"}
        except Exception:
            try:
                cid = symbol.replace("/", "").replace("-USDT", "").lower()
                j = http_get(f"https://api.coingecko.com/api/v3/simple/price?ids={cid}&vs_currencies=usd&include_24hr_change=true&include_24hr_vol=true", timeout=15)
                d = list(j.values())[0] if j else {}
                q = {"price": d.get("usd"), "changePct": d.get("usd_24h_change"),
                     "volume": d.get("usd_24h_vol"), "currency": "USD",
                     "name": item.get("name"), "time": int(time.time() * 1000), "provider": "coingecko"}
            except Exception:
                q = None
    else:
        try:
            q = yahoo_quote(item["id"])
            q["provider"] = "yahoo"
            q["name"] = q.get("name") or item.get("name")
        except Exception:
            q = None
    if q is None:
        return None
    q["symbol"] = item["symbol"]
    q["dp"] = item.get("dp", 4)
    cache.set(q, 25, *ck)
    return q


def market_overview():
    ck = ("overview",)
    hit = cache.get(*ck)
    if hit:
        return hit

    def build():
        out = {"ts": int(time.time() * 1000)}
        # crypto global + fear&greed
        try:
            g = http_get("https://api.coingecko.com/api/v3/global", timeout=15)
            d = g.get("data") or {}
            out["global"] = {
                "totalMcapUsd": d.get("total_market_cap", {}).get("usd"),
                "mcapChange24h": d.get("market_cap_change_percentage_24h_usd"),
                "totalVolumeUsd": d.get("total_volume", {}).get("usd"),
                "btcDominance": d.get("market_cap_percentage", {}).get("btc"),
                "ethDominance": d.get("market_cap_percentage", {}).get("eth"),
                "activeCryptos": d.get("active_cryptocurrencies"),
            }
        except Exception as e:
            out["global"] = {"error": str(e)}
        try:
            f = http_get("https://api.alternative.me/fng/?limit=2", timeout=15)
            arr = f.get("data") or []
            out["fearGreed"] = {"value": int(arr[0]["value"]), "label": arr[0]["value_classification"],
                                "prev": int(arr[1]["value"]) if len(arr) > 1 else None}
        except Exception as e:
            out["fearGreed"] = {"error": str(e)}
        # movers
        movers = []

        def one(mk, it):
            try:
                q = get_quote(mk, it["id"])
                if q and q.get("price"):
                    movers.append({"market": mk, "id": it["id"], "symbol": it["symbol"],
                                   "name": it.get("name"), "price": q["price"],
                                   "changePct": q.get("changePct"), "dp": it.get("dp", 4),
                                   "volume": q.get("volume")})
            except Exception:
                pass
        pick = ([("crypto", i) for i in MARKETS["crypto"]["items"][:18]] +
                [("forex", i) for i in MARKETS["forex"]["items"][:8]] +
                [("indices", i) for i in MARKETS["indices"]["items"][:7]] +
                [("commodities", i) for i in MARKETS["commodities"]["items"][:5]] +
                [("stocks", i) for i in MARKETS["stocks"]["items"][:12]])
        with ThreadPoolExecutor(max_workers=10) as ex:
            list(ex.map(lambda p: one(*p), pick))
        out["quotes"] = movers
        return out
    return cache.get_or_set(45, build, *ck)


# ===================================================================== #
#  NEWS (RSS)                                                           #
# ===================================================================== #
NEWS_FEEDS = {
    "markets": [("https://feeds.finance.yahoo.com/rss/2.0/headline?s=^GSPC&region=US&lang=en-US", "Yahoo Finance"),
                ("https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=100003114", "CNBC Top News"),
                ("https://www.investing.com/rss/news.rss", "Investing.com")],
    "crypto":  [("https://feeds.finance.yahoo.com/rss/2.0/headline?s=BTC-USD&region=US&lang=en-US", "Yahoo Crypto"),
                ("https://www.coindesk.com/arc/outboundfeeds/rss/", "CoinDesk")],
    "forex":   [("https://www.investing.com/rss/news_301.rss", "Investing Forex"),
                ("https://feeds.finance.yahoo.com/rss/2.0/headline?s=EURUSD=X&region=US&lang=en-US", "Yahoo FX")],
    "economy": [("https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=20910258", "CNBC Economy"),
                ("https://www.investing.com/rss/economic_indicators.rss", "Investing Economic")],
}


def parse_rss(xml_text, source):
    items = []
    try:
        root = ET.fromstring(xml_text)
    except Exception:
        return items
    for it in root.iter("item"):
        def g(tag):
            e = it.find(tag)
            return (e.text or "").strip() if e is not None and e.text else ""
        title, link, desc, pub = g("title"), g("link"), g("description"), g("pubDate")
        if not title:
            continue
        desc = re.sub(r"<[^>]+>", "", desc)[:300]
        items.append({"title": title, "link": link, "desc": desc, "date": pub, "source": source})
    return items


def get_news(cat="markets", limit=25):
    ck = ("news", cat)
    hit = cache.get(*ck)
    if hit:
        return hit

    def build():
        feeds = NEWS_FEEDS.get(cat, NEWS_FEEDS["markets"])
        allit = []

        def fetch(f):
            url, src = f
            try:
                return parse_rss(http_get(url, timeout=15, as_json=False), src)
            except Exception:
                return []
        with ThreadPoolExecutor(max_workers=4) as ex:
            for r in ex.map(fetch, feeds):
                allit.extend(r)
        seen, out = set(), []
        for i in allit:
            if i["title"] in seen:
                continue
            seen.add(i["title"])
            out.append(i)
        return out[:limit]
    return cache.get_or_set(300, build, *ck)


# ===================================================================== #
#  YOUTUBE RESEARCH API                                                 #
# ===================================================================== #
def _yt_local(query, lang, max_results):
    """Fallback: search the pre-researched offline index (data/videos.json).
    Used when YouTube rate-limits the sandbox."""
    vids = load_json("videos.json", []) or []
    det = {d.get("id"): d for d in (load_json("video_details.json", []) or [])}
    q = query.lower()
    toks = [w for w in re.split(r"[\s,،]+", q) if len(w) > 2]
    scored = []
    for r in vids:
        for v in r["videos"]:
            title = (v.get("title") or "").lower()
            d = det.get(v["id"]) or {}
            views = d.get("views_exact") or 0
            if not views:
                mm = re.search(r"([\d,\.]+)", v.get("views") or "")
                if mm:
                    try: views = int(float(mm.group(1).replace(",", "")))
                    except Exception: views = 0
            hit = sum(1 for tk in toks if tk in title)
            if r["key"] and any(tk in r["query"].lower() for tk in toks):
                hit += 1
            if hit == 0:
                continue
            langbonus = 1.25 if r["lang"] == lang else 1.0
            scored.append((hit * langbonus * (1 + views / 1e7), {
                **v, "author": d.get("author") or v.get("channel"),
                "viewsExact": views, "lengthSeconds": d.get("length_seconds") or 0,
                "chapters": (d.get("chapters") or [])[:30], "offline": True}))
    scored.sort(key=lambda x: -x[0])
    seen, out = set(), []
    for _, v in scored:
        if v["id"] in seen:
            continue
        seen.add(v["id"]); out.append(v)
        if len(out) >= max_results:
            break
    return out


def yt_search(query, lang="en", max_results=18):
    ck = ("yt", query, lang)
    hit = cache.get(*ck)
    if hit:
        return hit

    def build():
        try:
            live = _yt_live(query, lang, max_results)
            if live:
                return live
        except Exception as e:
            sys.stderr.write(f"[yt] live search failed ({e}) -> offline index\n")
        return _yt_local(query, lang, max_results)
    return cache.get_or_set(3600, build, *ck)


def _yt_live(query, lang, max_results):
        hl = "ar" if lang == "ar" else "en"
        url = ("https://www.youtube.com/results?search_query=" + urllib.parse.quote(query) +
               f"&hl={hl}&gl=" + ("EG" if lang == "ar" else "US"))
        html = http_get(url, timeout=25, as_json=False)
        m = re.search(r"var ytInitialData = (\{.*?\});</script>", html, re.S)
        if not m:
            m = re.search(r'ytInitialData"?\s*[=:]\s*(\{.*?\})\s*;\s*</script>', html, re.S)
        if not m:
            return []

        def txt(n):
            if not isinstance(n, dict):
                return ""
            if "simpleText" in n:
                return n["simpleText"]
            return "".join(r.get("text", "") for r in (n.get("runs") or []) if isinstance(r, dict))
        vids = []

        def walk(o):
            if isinstance(o, dict):
                vr = o.get("videoRenderer")
                if isinstance(vr, dict) and vr.get("videoId"):
                    th = ((vr.get("thumbnail") or {}).get("thumbnails") or [{}])[-1].get("url", "")
                    vids.append({"id": vr["videoId"], "title": txt(vr.get("title")),
                                 "channel": txt(vr.get("ownerText")),
                                 "views": txt(vr.get("viewCountText")),
                                 "published": txt(vr.get("publishedTimeText")),
                                 "duration": txt(vr.get("lengthText")),
                                 "thumbnail": th})
                for v in o.values():
                    walk(v)
            elif isinstance(o, list):
                for v in o:
                    walk(v)
        walk(json.loads(m.group(1)))
        seen, out = set(), []
        for v in vids:
            if v["id"] in seen or not v["title"]:
                continue
            seen.add(v["id"])
            out.append(v)
            if len(out) >= max_results:
                break
        return out


def load_json(name, default=None):
    p = os.path.join(DATA, name)
    if not os.path.exists(p):
        return default
    try:
        with open(p, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


def get_curriculum():
    ck = ("curriculum",)
    hit = cache.get(*ck)
    if hit:
        return hit

    def build():
        vids = load_json("videos.json", []) or []
        det = load_json("video_details.json", []) or []
        cur = load_json("chapters_curated.json", {}) or {}
        dmap = {d["id"]: d for d in det if d.get("id")}
        for cid, c in cur.items():
            if cid.startswith("_"):
                continue
            e = dmap.setdefault(cid, {"id": cid})
            e["chapters"] = c.get("chapters") or e.get("chapters") or []
            e["author"] = c.get("author") or e.get("author")
            e["views_exact"] = c.get("views") or e.get("views_exact")
            e["length_seconds"] = c.get("length") or e.get("length_seconds")
        by_topic = {}
        for r in vids:
            lst = []
            for v in r["videos"][:10]:
                d = dmap.get(v["id"], {})
                views = d.get("views_exact") or 0
                if not views:
                    mm = re.search(r"([\d,\.]+)", v.get("views") or "")
                    if mm:
                        try:
                            views = int(float(mm.group(1).replace(",", "")))
                        except Exception:
                            views = 0
                secs = d.get("length_seconds") or 0
                lst.append({**v,
                            "viewsExact": views,
                            "lengthSeconds": secs,
                            "chapters": (d.get("chapters") or [])[:30],
                            "author": d.get("author") or v.get("channel"),
                            "topicLang": r["lang"], "query": r["query"]})
            lst.sort(key=lambda x: -x["viewsExact"])
            key = f'{r["lang"]}:{r["key"]}'
            by_topic.setdefault(key, [])
            for v in lst:
                if not any(x["id"] == v["id"] for x in by_topic[key]):
                    by_topic[key].append(v)
        stats = load_json("curriculum_stats.json", {}) or {}
        return {"byTopic": {k: v[:8] for k, v in by_topic.items()},
                "studied": len(det), "found": stats.get("videos_found", 0),
                "chapters": stats.get("chapters_extracted", 0),
                "channels": stats.get("channels", []),
                "topConcepts": stats.get("top_concepts", [])[:60],
                "generatedAt": int(time.time() * 1000)}
    return cache.get_or_set(1800, build, *ck)


# ===================================================================== #
#  HTTP SERVER                                                          #
# ===================================================================== #
MIME = {".html": "text/html; charset=utf-8", ".js": "application/javascript; charset=utf-8",
        ".css": "text/css; charset=utf-8", ".json": "application/json; charset=utf-8",
        ".svg": "image/svg+xml", ".png": "image/png", ".jpg": "image/jpeg",
        ".ico": "image/x-icon", ".woff2": "font/woff2", ".map": "application/json"}


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "TradeMasterPro/1.0"

    def log_message(self, fmt, *args):
        sys.stderr.write("· %s\n" % (fmt % args)[:180])

    # ---------- helpers ----------
    def _send(self, code, body, ctype="application/json; charset=utf-8", extra=None):
        if isinstance(body, (dict, list)):
            body = json.dumps(body, ensure_ascii=False).encode("utf-8")
        elif isinstance(body, str):
            body = body.encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Referrer-Policy", "strict-origin-when-cross-origin")
        self.send_header("Permissions-Policy", "camera=(), microphone=(), geolocation=()")
        for k, v in (extra or {}).items():
            self.send_header(k, v)
        self.end_headers()
        try:
            self.wfile.write(body)
        except Exception:
            pass

    def _json(self, obj, code=200):
        self._send(code, obj)

    def _err(self, msg, code=400):
        self._json({"error": msg}, code)

    # ---------- routes ----------
    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path
        qs = {k: v[0] for k, v in urllib.parse.parse_qs(parsed.query).items()}
        try:
            if path.startswith("/api/"):
                return self.api(path, qs)
            return self.static(path)
        except UpstreamUnavailable as e:
            return self._err(str(e), 503)
        except Exception as e:
            return self._err(f"{type(e).__name__}: {e}", 500)

    def do_OPTIONS(self):
        # The UI and its API are same-origin; do not enable broad cross-origin access.
        self.send_response(204)
        self.send_header("Content-Length", "0")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.end_headers()

    def static(self, path):
        if path in ("/", ""):
            path = "/index.html"
        public_root = os.path.realpath(PUBLIC)
        fp = os.path.realpath(os.path.join(public_root, path.lstrip("/")))
        try:
            if os.path.commonpath((public_root, fp)) != public_root:
                return self._err("forbidden", 403)
        except ValueError:
            return self._err("forbidden", 403)
        if os.path.isdir(fp):
            fp = os.path.join(fp, "index.html")
        if not os.path.exists(fp):
            return self._err("not found: " + path, 404)
        ext = os.path.splitext(fp)[1].lower()
        with open(fp, "rb") as f:
            body = f.read()
        self._send(200, body, MIME.get(ext, "application/octet-stream"))

    def api(self, path, qs):
        # ---- YouTube thumbnail proxy (the preview iframe cannot reach i.ytimg.com)
        if path == "/api/thumb":
            vid = (qs.get("id") or "").strip()
            if not re.match(r"^[A-Za-z0-9_\-]{6,20}$", vid):
                return self._err("bad id", 400)
            kind = qs.get("kind") or "hq"
            if kind not in ("hq", "mq", "max", "sd"):
                kind = "hq"
            name = {"hq": "hqdefault", "mq": "mqdefault", "max": "maxresdefault", "sd": "sddefault"}[kind]
            cached = cache.get("thumb", vid, kind)
            if cached:
                body, ctype = cached
                return self._send(200, body, ctype, {"Cache-Control": "public, max-age=86400"})
            for url in (f"https://i.ytimg.com/vi/{vid}/{name}.jpg",
                        f"https://i.ytimg.com/vi/{vid}/mqdefault.jpg",
                        f"https://i.ytimg.com/vi/{vid}/default.jpg"):
                body, ctype = http_get_bytes(url)
                if body and len(body) > 2000:         # i.ytimg serves a ~1KB grey placeholder for missing thumbs
                    cache.set((body, ctype or "image/jpeg"), 86400, "thumb", vid, kind)
                    return self._send(200, body, ctype or "image/jpeg",
                                      {"Cache-Control": "public, max-age=86400"})
            return self._err("thumbnail unavailable", 404)

        # ---- universe of tradable symbols
        if path == "/api/universe":
            mk = qs.get("market")
            if mk and mk in MARKETS:
                return self._json({"market": mk, **{k: v for k, v in MARKETS[mk].items()}})
            return self._json({"markets": MARKETS,
                               "intervals": [{"id": i, "s": s} for i, s in INTERVAL_SECONDS.items()]})

        # ---- candles
        if path == "/api/klines":
            market = qs.get("market", "crypto")
            symbol = qs.get("symbol", "BTC-USDT")
            interval = qs.get("interval", "1h")
            limit = int(qs.get("limit", "600"))
            data = get_klines(market, symbol, interval, limit)
            if qs.get("compact") == "1":
                data = {**data, "candles": [[c["t"], c["o"], c["h"], c["l"], c["c"], c["v"]] for c in data["candles"]]}
            return self._json(data)

        # ---- single / batch quotes
        if path == "/api/quote":
            q = get_quote(qs.get("market", "crypto"), qs.get("symbol", "BTC-USDT"))
            if q is None:
                return self._err("market quote unavailable from configured providers", 503)
            return self._json(q)

        if path == "/api/quotes":
            pairs = qs.get("pairs", "")  # "crypto:BTC-USDT,forex:EURUSD=X"
            items = []
            for p in [x for x in pairs.split(",") if x]:
                if ":" in p:
                    mk, sy = p.split(":", 1)
                    items.append((mk, sy))

            def one(it):
                try:
                    q = get_quote(*it)
                    return q
                except Exception:
                    return None
            # Preserve request order and failed entries (None). The client associates
            # each quote with the requested position by index; filtering failures
            # silently shifts later prices onto the wrong positions.
            with ThreadPoolExecutor(max_workers=8) as ex:
                res = list(ex.map(one, items))
            return self._json({"quotes": res})

        # ---- dashboard overview
        if path == "/api/overview":
            return self._json(market_overview())

        # ---- screener
        if path == "/api/screener":
            market = qs.get("market", "crypto")
            interval = qs.get("interval", "4h")
            limit = int(qs.get("limit", "400"))
            syms = [s for s in qs.get("symbols", "").split(",") if s]
            if not syms:
                syms = [it["id"] for it in MARKETS.get(market, MARKETS["crypto"])["items"][:20]]
            syms = syms[:40]

            def one(s):
                try:
                    k = get_klines(market, s, interval, limit)
                    return scan_one(k)
                except Exception:
                    return None
            with ThreadPoolExecutor(max_workers=6) as ex:
                rows = [r for r in ex.map(one, syms) if r]
            rows.sort(key=lambda r: -abs(r.get("score", 0)))
            return self._json({"market": market, "interval": interval, "rows": rows,
                               "ts": int(time.time() * 1000)})

        # ---- youtube research
        if path == "/api/youtube":
            q = qs.get("q", "trading course")
            lang = qs.get("lang", "en")
            return self._json({"query": q, "lang": lang,
                               "videos": yt_search(q, lang, int(qs.get("max", "18")))})

        if path == "/api/curriculum":
            return self._json(get_curriculum())

        # ---- news
        if path == "/api/news":
            return self._json({"items": get_news(qs.get("cat", "markets"), int(qs.get("limit", "25")))})

        if path == "/api/health":
            return self._json({"ok": True, "time": int(time.time() * 1000),
                               "providers": {"crypto": "OKX/Coinbase/Kraken/CoinGecko",
                                             "other": "Yahoo Finance",
                                             "sentiment": "alternative.me",
                                             "education": "YouTube"},
                               "requests": HAVE_REQUESTS})
        return self._err("unknown endpoint " + path, 404)


# --------------------------------------------------------------------- #
#  Lightweight server-side scanner (used by /api/screener)              #
# --------------------------------------------------------------------- #
def _sma(vals, n):
    if len(vals) < n:
        return None
    return sum(vals[-n:]) / n


def _ema_series(vals, n):
    if len(vals) < n:
        return []
    k = 2 / (n + 1)
    e = [sum(vals[:n]) / n]
    for v in vals[n:]:
        e.append(v * k + e[-1] * (1 - k))
    return e


def _rsi(closes, n=14):
    if len(closes) <= n:
        return None
    g = l = 0.0
    for i in range(1, n + 1):
        d = closes[i] - closes[i - 1]
        g += max(d, 0); l += max(-d, 0)
    ag, al = g / n, l / n
    for i in range(n + 1, len(closes)):
        d = closes[i] - closes[i - 1]
        ag = (ag * (n - 1) + max(d, 0)) / n
        al = (al * (n - 1) + max(-d, 0)) / n
    if al == 0:
        return 100.0
    return 100 - 100 / (1 + ag / al)


def _atr(candles, n=14):
    if len(candles) <= n:
        return None
    trs = []
    for i in range(1, len(candles)):
        h, l, pc = candles[i]["h"], candles[i]["l"], candles[i - 1]["c"]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    a = sum(trs[:n]) / n
    for t in trs[n:]:
        a = (a * (n - 1) + t) / n
    return a


def scan_one(k):
    c = k["candles"]
    if len(c) < 60:
        return None
    closes = [x["c"] for x in c]
    last = c[-1]
    e20 = _ema_series(closes, 20)
    e50 = _ema_series(closes, 50)
    e200 = _ema_series(closes, 200) if len(closes) >= 200 else []
    rsi = _rsi(closes)
    atr = _atr(c)
    price = last["c"]
    score = 0
    sig = []
    if e20 and e50:
        if e20[-1] > e50[-1]:
            score += 1; sig.append("EMA20>EMA50")
        else:
            score -= 1; sig.append("EMA20<EMA50")
        # cross detection
        if len(e20) > 2 and len(e50) > 2:
            if e20[-2] <= e50[-2] and e20[-1] > e50[-1]:
                score += 2; sig.append("GoldenCross")
            if e20[-2] >= e50[-2] and e20[-1] < e50[-1]:
                score -= 2; sig.append("DeathCross")
    if e200:
        score += 1 if price > e200[-1] else -1
        sig.append("Above200" if price > e200[-1] else "Below200")
    if rsi is not None:
        if rsi >= 70:
            score -= 1; sig.append(f"RSI{rsi:.0f} OB")
        elif rsi <= 30:
            score += 1; sig.append(f"RSI{rsi:.0f} OS")
        else:
            sig.append(f"RSI{rsi:.0f}")
    hi20 = max(x["h"] for x in c[-21:-1])
    lo20 = min(x["l"] for x in c[-21:-1])
    if price > hi20:
        score += 2; sig.append("Breakout20H")
    if price < lo20:
        score -= 2; sig.append("Breakdown20L")
    chg24 = (price / closes[-2] - 1) * 100 if len(closes) > 1 else 0
    rng = (max(x["h"] for x in c[-20:]) - min(x["l"] for x in c[-20:]))
    vol = [x["v"] for x in c if x["v"]]
    volratio = (last["v"] / (sum(vol[-21:-1]) / 20)) if len(vol) > 21 and sum(vol[-21:-1]) else 1
    return {"symbol": k["symbol"], "id": k["id"], "name": k.get("name"),
            "price": price, "dp": k.get("dp", 4), "changePct": round(chg24, 3),
            "rsi": round(rsi, 1) if rsi else None,
            "atrPct": round(atr / price * 100, 3) if atr else None,
            "trend20": round((price / _sma(closes, 20) - 1) * 100, 2) if _sma(closes, 20) else None,
            "score": score, "signals": sig, "volRatio": round(volratio, 2),
            "range20Pct": round(rng / price * 100, 2) if price else None,
            "candles": len(c), "provider": k.get("provider")}


def main():
    srv = ThreadingHTTPServer((HOST, PORT), Handler)
    srv.daemon_threads = True
    print("=" * 66)
    print("  TradeMaster Pro backend")
    print(f"  listening on http://{HOST}:{PORT}")
    print("  providers: OKX / Coinbase / Kraken / CoinGecko / Yahoo / YouTube")
    print("=" * 66, flush=True)
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        print("\nbye")


if __name__ == "__main__":
    main()
