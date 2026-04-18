#!/usr/bin/env python3
"""
Market Sector Screener v3
- S&P 500 Sector Screen (vs SPY)
- Themen-Screen: Photonics / Optoelectronics (global, vs SPY + QQQ)
- Top + Bottom Stocks, Firmennamen, Wochentag-Breakdown
- 1T / 1W / 1M / 1Q Zeitrahmen
"""

import yfinance as yf
import json
import os
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings("ignore")

# Finviz scraper (optional — läuft auch ohne)
try:
    from finviz_scraper import get_hot_stocks
    FINVIZ_AVAILABLE = True
except ImportError:
    FINVIZ_AVAILABLE = False

# ─── SEKTOR ETFs ───────────────────────────────────────────
SECTOR_ETFS = {
    "Technology":        "XLK",
    "Healthcare":        "XLV",
    "Financials":        "XLF",
    "Consumer Discret.": "XLY",
    "Industrials":       "XLI",
    "Energy":            "XLE",
    "Materials":         "XLB",
    "Utilities":         "XLU",
    "Real Estate":       "XLRE",
    "Consumer Staples":  "XLP",
    "Communication":     "XLC",
}

SECTOR_STOCKS = {
    "Technology":        ["AAPL", "MSFT", "NVDA", "AVGO", "AMD", "ORCL", "CRM", "ADBE", "QCOM", "TXN"],
    "Healthcare":        ["LLY", "UNH", "JNJ", "ABBV", "MRK", "TMO", "ABT", "DHR", "BMY", "AMGN"],
    "Financials":        ["BRK-B", "JPM", "V", "MA", "BAC", "WFC", "GS", "MS", "BLK", "AXP"],
    "Consumer Discret.": ["AMZN", "TSLA", "HD", "MCD", "NKE", "SBUX", "TGT", "LOW", "BKNG", "CMG"],
    "Industrials":       ["CAT", "UPS", "HON", "BA", "GE", "LMT", "RTX", "DE", "MMM", "ETN"],
    "Energy":            ["XOM", "CVX", "COP", "EOG", "SLB", "MPC", "PSX", "VLO", "OXY", "HAL"],
    "Materials":         ["LIN", "APD", "ECL", "NEM", "FCX", "NUE", "ALB", "VMC", "MLM", "CF"],
    "Utilities":         ["NEE", "DUK", "SO", "D", "AEP", "EXC", "SRE", "XEL", "ES", "PEG"],
    "Real Estate":       ["PLD", "AMT", "EQIX", "PSA", "O", "WELL", "DLR", "SPG", "CCI", "EQR"],
    "Consumer Staples":  ["PG", "KO", "PEP", "COST", "WMT", "MO", "PM", "MDLZ", "CL", "KHC"],
    "Communication":     ["META", "GOOGL", "NFLX", "DIS", "CMCSA", "T", "VZ", "CHTR", "TMUS", "EA"],
}

# ─── THEMEN-SCREEN: Photonics / Optoelectronics ────────────
# Yahoo Finance Suffixes: .DE = Frankfurt, .PA = Paris, .ST = Stockholm
THEME_GROUPS = {
    "Photonics & Laser": {
        "COHR":      "Coherent Corp",
        "LITE":      "Lumentum",
        "IPGP":      "IPG Photonics",
        "VIAV":      "Viavi Solutions",
        "JEN.DE":    "Jenoptik",
        "POET":      "POET Technologies",
    },
    "Optoelectronics & Compound Semi": {
        "AAOI":      "Applied Optoelectronics",
        "OLED":      "Universal Display",
        "AIXA.DE":   "Aixtron",
        "ALRIB.PA": "Riber",
        "SOI.PA":   "Soitec",
        "SIVE.ST":  "Sivers Semiconductors",
    },
    "Photonics Infrastructure & Networks": {
        "CIEN":      "Ciena",
        "TSEM":      "Tower Semiconductor",
        "NBIS":     "Nebius Group",
        "ONTO":      "Onto Innovation",
        "MKSI":      "MKS Instruments",
        "COHU":      "Cohu",
    },
    "Semi Equipment (Enabler)": {
        "AMAT":      "Applied Materials",
        "KLAC":      "KLA Corp",
        "NVTS":      "Navitas Semiconductor",
    },
}

# Flat lookup: ticker -> name (used by sector screen too)
COMPANY_NAMES = {
    # Technology
    "AAPL": "Apple", "MSFT": "Microsoft", "NVDA": "Nvidia", "AVGO": "Broadcom",
    "AMD": "AMD", "ORCL": "Oracle", "CRM": "Salesforce", "ADBE": "Adobe",
    "QCOM": "Qualcomm", "TXN": "Texas Instruments",
    # Healthcare
    "LLY": "Eli Lilly", "UNH": "UnitedHealth", "JNJ": "Johnson & Johnson",
    "ABBV": "AbbVie", "MRK": "Merck", "TMO": "Thermo Fisher", "ABT": "Abbott",
    "DHR": "Danaher", "BMY": "Bristol-Myers", "AMGN": "Amgen",
    # Financials
    "BRK-B": "Berkshire", "JPM": "JPMorgan", "V": "Visa", "MA": "Mastercard",
    "BAC": "Bank of America", "WFC": "Wells Fargo", "GS": "Goldman Sachs",
    "MS": "Morgan Stanley", "BLK": "BlackRock", "AXP": "Amex",
    # Consumer Discretionary
    "AMZN": "Amazon", "TSLA": "Tesla", "HD": "Home Depot", "MCD": "McDonald's",
    "NKE": "Nike", "SBUX": "Starbucks", "TGT": "Target", "LOW": "Lowe's",
    "BKNG": "Booking", "CMG": "Chipotle",
    # Industrials
    "CAT": "Caterpillar", "UPS": "UPS", "HON": "Honeywell", "BA": "Boeing",
    "GE": "GE Aerospace", "LMT": "Lockheed", "RTX": "RTX Corp", "DE": "Deere",
    "MMM": "3M", "ETN": "Eaton",
    # Energy
    "XOM": "ExxonMobil", "CVX": "Chevron", "COP": "ConocoPhillips", "EOG": "EOG Resources",
    "SLB": "SLB", "MPC": "Marathon Petroleum", "PSX": "Phillips 66",
    "VLO": "Valero", "OXY": "Occidental", "HAL": "Halliburton",
    # Materials
    "LIN": "Linde", "APD": "Air Products", "ECL": "Ecolab", "NEM": "Newmont",
    "FCX": "Freeport-McMoRan", "NUE": "Nucor", "ALB": "Albemarle",
    "VMC": "Vulcan Materials", "MLM": "Martin Marietta", "CF": "CF Industries",
    # Utilities
    "NEE": "NextEra Energy", "DUK": "Duke Energy", "SO": "Southern Co",
    "D": "Dominion", "AEP": "Am. Electric Power", "EXC": "Exelon",
    "SRE": "Sempra", "XEL": "Xcel Energy", "ES": "Eversource", "PEG": "PSEG",
    # Real Estate
    "PLD": "Prologis", "AMT": "American Tower", "EQIX": "Equinix", "PSA": "Public Storage",
    "O": "Realty Income", "WELL": "Welltower", "DLR": "Digital Realty",
    "SPG": "Simon Property", "CCI": "Crown Castle", "EQR": "Equity Residential",
    # Consumer Staples
    "PG": "Procter & Gamble", "KO": "Coca-Cola", "PEP": "PepsiCo", "COST": "Costco",
    "WMT": "Walmart", "MO": "Altria", "PM": "Philip Morris", "MDLZ": "Mondelez",
    "CL": "Colgate", "KHC": "Kraft Heinz",
    # Communication
    "META": "Meta", "GOOGL": "Alphabet", "NFLX": "Netflix", "DIS": "Disney",
    "CMCSA": "Comcast", "T": "AT&T", "VZ": "Verizon", "CHTR": "Charter",
    "TMUS": "T-Mobile", "EA": "Electronic Arts",
}

# Add theme names to global lookup
for group in THEME_GROUPS.values():
    COMPANY_NAMES.update(group)

# ─── HELPERS ───────────────────────────────────────────────
# Exchange/Country info for theme stocks
STOCK_EXCHANGE = {
    # US Nasdaq
    "COHR":    ("USA", "Nasdaq"),
    "LITE":    ("USA", "Nasdaq"),
    "IPGP":    ("USA", "Nasdaq"),
    "VIAV":    ("USA", "Nasdaq"),
    "POET":    ("USA", "Nasdaq"),
    "AAOI":    ("USA", "Nasdaq"),
    "OLED":    ("USA", "Nasdaq"),
    "CIEN":    ("USA", "NYSE"),
    "TSEM":    ("Israel", "Nasdaq"),
    "NBIS":    ("Netherlands", "Nasdaq"),
    "ONTO":    ("USA", "NYSE"),
    "MKSI":    ("USA", "Nasdaq"),
    "COHU":    ("USA", "Nasdaq"),
    "AMAT":    ("USA", "Nasdaq"),
    "KLAC":    ("USA", "Nasdaq"),
    "NVTS":    ("USA", "Nasdaq"),
    # European
    "JEN.DE":  ("Germany", "Frankfurt"),
    "AIXA.DE": ("Germany", "Frankfurt"),
    "ALRIB.PA":("France",  "Euronext Paris"),
    "SOI.PA":  ("France",  "Euronext Paris"),
    "SIVE.ST": ("Sweden",  "Stockholm"),
}


def to_scalar(val):
    try:
        if hasattr(val, 'iloc'): val = val.iloc[0]
        if hasattr(val, 'item'): return float(val.item())
        return float(val)
    except:
        return None

def get_history(ticker, days=100):
    try:
        end   = datetime.now()
        start = end - timedelta(days=days)
        data  = yf.download(ticker, start=start, end=end, progress=False, auto_adjust=True)
        if len(data) < 2: return None
        return data["Close"].dropna()
    except:
        return None

def pct_over_n_days(close, n):
    if close is None or len(close) < 2: return None
    n = min(n, len(close))
    v_end   = to_scalar(close.iloc[-1])
    v_start = to_scalar(close.iloc[-n])
    if v_end is None or v_start is None or v_start == 0: return None
    return round((v_end / v_start - 1) * 100, 2)

def get_last_5_days(close):
    if close is None or len(close) < 6: return []
    result = []
    daily = close.pct_change().dropna() * 100
    for dt, val in daily.iloc[-5:].items():
        try:
            date_str = dt.strftime("%a %d.%m") if hasattr(dt, 'strftime') else str(dt)[:10]
            result.append({"date": date_str, "pct": round(float(val), 2)})
        except:
            pass
    return result

def get_volume_ratio(ticker):
    try:
        data = yf.download(ticker, period="1mo", progress=False, auto_adjust=True)
        if len(data) < 5: return None
        vol   = data["Volume"].dropna()
        avg   = to_scalar(vol.iloc[:-1].mean())
        today = to_scalar(vol.iloc[-1])
        if avg is None or today is None or avg == 0: return None
        return round(today / avg, 2)
    except:
        return None

def get_current_price(ticker):
    try:
        price = to_scalar(yf.Ticker(ticker).fast_info.last_price)
        return round(price, 2) if price else None
    except:
        return None

def stock_entry(ticker, name=""):
    close = get_history(ticker, days=30)
    p1d   = pct_over_n_days(close, 1)
    p5d   = pct_over_n_days(close, 5)
    vol   = get_volume_ratio(ticker)
    price = get_current_price(ticker)
    if p1d is None and p5d is None: return None
    score = (p5d or 0) * 0.6 + (p1d or 0) * 0.4
    exch = STOCK_EXCHANGE.get(ticker, ("USA", "Nasdaq"))
    return {
        "ticker":    ticker,
        "name":      name or COMPANY_NAMES.get(ticker, ""),
        "country":   exch[0],
        "exchange":  exch[1],
        "price":     price,
        "perf_1d":   p1d,
        "perf_5d":   p5d,
        "vol_ratio": vol,
        "score":     round(score, 2),
    }

# ─── MAIN ──────────────────────────────────────────────────
def main():
    print(f"\n{'='*55}")
    print(f"  Market Screener v3 — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"{'='*55}\n")

    # ── Benchmarks ──
    print("Fetching benchmarks (SPY, QQQ)...")
    spy_close = get_history("SPY", days=100)
    qqq_close = get_history("QQQ", days=100)

    def bm_stats(close):
        return {
            "perf_1d":  pct_over_n_days(close, 1),
            "perf_5d":  pct_over_n_days(close, 5),
            "perf_1m":  pct_over_n_days(close, 21),
            "perf_1q":  pct_over_n_days(close, 63),
            "daily":    get_last_5_days(close),
        }

    spy_stats = bm_stats(spy_close)
    qqq_stats = bm_stats(qqq_close)
    print(f"  SPY: 1d={spy_stats['perf_1d']}%  5d={spy_stats['perf_5d']}%")
    print(f"  QQQ: 1d={qqq_stats['perf_1d']}%  5d={qqq_stats['perf_5d']}%")

    spy_1d = spy_stats["perf_1d"]; spy_5d = spy_stats["perf_5d"]
    spy_1m = spy_stats["perf_1m"]; spy_1q = spy_stats["perf_1q"]
    spy_days = spy_stats["daily"]

    # ── Sector Screen ──
    print("\nFetching sector ETFs...")
    sectors = []
    for name, etf in SECTOR_ETFS.items():
        print(f"  {name} ({etf})...")
        close = get_history(etf, days=100)
        p1d = pct_over_n_days(close, 1); p5d = pct_over_n_days(close, 5)
        p1m = pct_over_n_days(close, 21); p1q = pct_over_n_days(close, 63)

        etf_days = get_last_5_days(close)
        daily_vs = []
        for i, d in enumerate(etf_days):
            spy_d = spy_days[i]["pct"] if i < len(spy_days) else 0
            daily_vs.append({"date": d["date"], "sector": d["pct"], "spy": spy_d, "vs_spy": round(d["pct"] - spy_d, 2)})

        sectors.append({
            "name": name, "etf": etf,
            "perf_1d": p1d, "perf_5d": p5d, "perf_1m": p1m, "perf_1q": p1q,
            "vs_spy_1d":  round(p1d  - spy_1d,  2) if p1d  and spy_1d  else None,
            "vs_spy_5d":  round(p5d  - spy_5d,  2) if p5d  and spy_5d  else None,
            "vs_spy_1m":  round(p1m  - spy_1m,  2) if p1m  and spy_1m  else None,
            "vs_spy_1q":  round(p1q  - spy_1q,  2) if p1q  and spy_1q  else None,
            "daily_vs_spy": daily_vs,
        })

    sectors.sort(key=lambda x: x["vs_spy_5d"] if x["vs_spy_5d"] is not None else -999, reverse=True)

    print("\nFetching sector stocks...")
    for sector in sectors:
        tickers = SECTOR_STOCKS.get(sector["name"], [])
        print(f"  {sector['name']}...")
        stock_data = [e for t in tickers if (e := stock_entry(t)) is not None]
        stock_data.sort(key=lambda x: x["score"], reverse=True)
        sector["top_stocks"]    = stock_data[:5]
        sector["bottom_stocks"] = stock_data[-5:][::-1]

    # ── Theme Screen ──
    print("\nFetching theme stocks (Photonics/Optoelectronics)...")
    qqq_1d = qqq_stats["perf_1d"]; qqq_5d = qqq_stats["perf_5d"]
    qqq_1m = qqq_stats["perf_1m"]; qqq_1q = qqq_stats["perf_1q"]

    themes = []
    for group_name, stocks in THEME_GROUPS.items():
        print(f"  {group_name}...")
        entries = []
        for ticker, name in stocks.items():
            print(f"    {ticker} ({name})...")
            e = stock_entry(ticker, name)
            if e is None:
                print(f"    ⚠ No data for {ticker}")
                continue
            # vs SPY
            e["vs_spy_1d"] = round(e["perf_1d"] - spy_1d, 2) if e["perf_1d"] and spy_1d else None
            e["vs_spy_5d"] = round(e["perf_5d"] - spy_5d, 2) if e["perf_5d"] and spy_5d else None
            # vs QQQ
            e["vs_qqq_1d"] = round(e["perf_1d"] - qqq_1d, 2) if e["perf_1d"] and qqq_1d else None
            e["vs_qqq_5d"] = round(e["perf_5d"] - qqq_5d, 2) if e["perf_5d"] and qqq_5d else None
            entries.append(e)

        entries.sort(key=lambda x: x["score"], reverse=True)

        # Group summary: avg performance
        valid_1d = [e["perf_1d"] for e in entries if e["perf_1d"] is not None]
        valid_5d = [e["perf_5d"] for e in entries if e["perf_5d"] is not None]
        avg_1d = round(sum(valid_1d)/len(valid_1d), 2) if valid_1d else None
        avg_5d = round(sum(valid_5d)/len(valid_5d), 2) if valid_5d else None

        themes.append({
            "name":     group_name,
            "avg_1d":   avg_1d,
            "avg_5d":   avg_5d,
            "vs_spy_5d": round(avg_5d - spy_5d, 2) if avg_5d and spy_5d else None,
            "vs_qqq_5d": round(avg_5d - qqq_5d, 2) if avg_5d and qqq_5d else None,
            "stocks":   entries,
        })

    themes.sort(key=lambda x: x["vs_qqq_5d"] if x["vs_qqq_5d"] is not None else -999, reverse=True)

    # ── Finviz Hot Stocks ──
    hot_stocks = []
    if FINVIZ_AVAILABLE:
        print("\nScraping Finviz Hot Stocks (MCap >= 100M, via yfinance-Check)...")
        try:
            raw_hot = get_hot_stocks(max_pages=5)
            # Enrich with yfinance data (perf_5d, vol_ratio, market_cap)
            print(f"  Enriching {len(raw_hot)} stocks with yfinance data...")
            MIN_MCAP = 100_000_000  # $100M Mindest-Market-Cap
            enriched = []
            for s in raw_hot:
                t = s["ticker"]
                # Market Cap prüfen (zuverlässiger als Finviz-Filter)
                try:
                    mcap = yf.Ticker(t).fast_info.market_cap
                except Exception:
                    mcap = None
                if mcap is not None and mcap < MIN_MCAP:
                    print(f"    ✗ {t} übersprungen (MCap ${mcap/1e6:.1f}M < $100M)")
                    continue
                close = get_history(t, days=10)
                s["perf_1d"]    = s.get("change_1d")
                s["perf_5d"]    = pct_over_n_days(close, 5)
                s["vol_ratio"]  = get_volume_ratio(t)
                s["market_cap"] = mcap
                s["score"]      = (s["perf_5d"] or 0)*0.6 + (s["perf_1d"] or 0)*0.4
                enriched.append(s)
            hot_stocks = sorted(enriched, key=lambda x: x["score"], reverse=True)
            print(f"  ✓ {len(hot_stocks)} Hot Stocks bereit (nach MCap-Filter)")
        except Exception as e:
            print(f"  ⚠ Finviz error: {e}")
    else:
        print("\n⚠ finviz_scraper.py nicht gefunden — Hot Stocks übersprungen")

    # ── Output ──
    output = {
        "generated_at": datetime.now().isoformat(),
        "benchmark": {
            "spy": {"name": "S&P 500 (SPY)", **spy_stats},
            "qqq": {"name": "Nasdaq 100 (QQQ)", **qqq_stats},
        },
        "sectors": sectors,
        "themes":  themes,
        "hot_stocks": hot_stocks,
    }

    out_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "screener_data.json")
    with open(out_path, "w") as f:
        json.dump(output, f, indent=2)

    print(f"\n✓ Saved to {out_path}")
    print(f"\nTop 3 Sektoren vs SPY (5T):")
    for s in sectors[:3]:
        print(f"  {s['name']:20s}  {s.get('perf_5d') or 0:+.2f}%  vs SPY: {s.get('vs_spy_5d') or 0:+.2f}%")
    print(f"\nThemen-Gruppen vs QQQ (5T):")
    for t in themes:
        print(f"  {t['name']:35s}  avg 5T: {t.get('avg_5d') or 0:+.2f}%  vs QQQ: {t.get('vs_qqq_5d') or 0:+.2f}%")

if __name__ == "__main__":
    main()
