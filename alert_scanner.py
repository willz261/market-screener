#!/usr/bin/env python3
"""
Alert Scanner — Breit-Scanner für institutionelle Flow-Anomalien

Scannt ~60 Sektor/Themen-ETFs über deine Watchlist hinaus.
Erkennt drei Hedge-Fund-Patterns:
  1. DISTRIBUTION TOPPING — Sektor mit WEAK_RALLY / Distribution + positive Performance
  2. RATIO DIVERGENCE — Widersprüchliche Regime-Signale zwischen Ratio-Paaren
  3. CREDIT EARLY WARNING — HYG/LQD kippt bevor Equity reagiert

Plus: Outlier-Flows in Themen die nicht im Haupt-Dashboard sind.

Output: alerts.json (wird vom Dashboard geladen)
Runtime: ~3-5 Minuten (yfinance, ~60 Ticker + Ratios)
"""

import yfinance as yf
import json
import math
import os
from datetime import datetime, timedelta
from statistics import median
import warnings
warnings.filterwarnings("ignore")


def clean_nan(obj):
    """Recursively replace NaN/Inf with None for valid JSON output."""
    if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
        return None
    if isinstance(obj, dict):
        return {k: clean_nan(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [clean_nan(v) for v in obj]
    return obj

# ══════════════════════════════════════════════════════════
# BROAD SCAN UNIVERSE
# Themen-ETFs die NICHT im Haupt-Dashboard sind
# ══════════════════════════════════════════════════════════

BROAD_THEME_ETFS = {
    # ── Themen die du nicht trackst ──
    "AI & Cloud":           {"VGT": "Vanguard IT", "CLOU": "Cloud Computing", "BOTZ": "Robotics & AI", "ARKK": "ARK Innovation"},
    "Biotech & Pharma":     {"XBI": "Biotech SPDR", "IBB": "iShares Biotech", "LABU": "3x Bull Biotech"},
    "Clean Energy":         {"ICLN": "iShares Clean Energy", "TAN": "Solar ETF", "QCLN": "Clean Edge Green"},
    "Semiconductors":       {"SMH": "VanEck Semis", "SOXX": "iShares Semi", "PSI": "Dynamic Semis"},
    "Cybersecurity":        {"CIBR": "Cybersecurity ETF", "HACK": "ETFMG Cyber", "BUG": "Global X Cyber"},
    "China & EM Tech":      {"KWEB": "China Internet", "CQQQ": "China Tech", "EWT": "Taiwan"},
    "Infrastructure":       {"PAVE": "US Infra", "IFRA": "iShares Infra", "IGF": "Global Infra"},
    "Cannabis":             {"MSOS": "US Cannabis", "MJ": "Global Cannabis"},
    "Uranium & Nuclear":    {"URA": "Uranium ETF", "URNM": "Uranium Miners", "NLR": "Nuclear Energy"},
    "Shipping & Transport": {"BDRY": "Dry Bulk", "BOAT": "SonicShares Shipping", "IYT": "Transport"},
    "Regional Banks":       {"KRE": "Regional Banks", "KBE": "Bank ETF"},
    "REITs Specialized":    {"XLRE": "Real Estate SPDR", "VNQ": "Vanguard REIT", "MORT": "Mortgage REIT"},
    "Commodities Broad":    {"DBC": "Commodities Tracking", "GSG": "iShares Commodities", "PDBC": "Diversified Cmdty"},
    "Lithium & Battery":    {"LIT": "Lithium & Battery", "BATT": "Battery Tech"},
    "Space & Aerospace":    {"UFO": "Space ETF", "ARKX": "ARK Space"},
    "India":                {"INDA": "iShares India", "EPI": "WisdomTree India"},
    "Japan":                {"EWJ": "iShares Japan", "DXJ": "Hedged Japan"},
    "Agriculture":          {"DBA": "Agriculture ETF", "MOO": "Agribusiness", "WEAT": "Wheat"},
}

# ── Ratio-Paare für Divergenz-Erkennung ──
RATIO_PAIRS = {
    "HYG/LQD":    ("HYG", "LQD",  "Credit Risk Appetite"),
    "IWM/SPY":    ("IWM", "SPY",  "Small vs Large Cap"),
    "COPPER/GOLD":("CPER","GLD",  "Growth vs Safety"),
    "XLY/XLP":    ("XLY", "XLP",  "Cyclical vs Defensive"),
    "TLT/SHY":    ("TLT", "SHY",  "Duration Appetite"),
    "GLD/SPY":    ("GLD", "SPY",  "Safe Haven Demand"),
}

# ── Thresholds ──
OUTLIER_FLOW_THRESHOLD = 40     # ±40% Flow vs. Vorwoche = Outlier
OUTLIER_OBV_THRESHOLD  = 3.0    # OBV-Trend > 3x avg daily vol = stark
WEAK_RALLY_PERF_MIN    = 2.0    # Mindest-Performance für Weak Rally Warnung
DISTRIBUTION_FLOW_MIN  = -15.0  # Mindest-negativer Flow für Distribution
RATIO_DIVERGENCE_PCT   = 1.5    # Ratio-Änderung 1W > 1.5% = signifikant


# ══════════════════════════════════════════════════════════
# HELPERS (lightweight — reuse from flow_screener logic)
# ══════════════════════════════════════════════════════════

def to_scalar(val):
    try:
        if hasattr(val, 'iloc'): val = val.iloc[0]
        if hasattr(val, 'item'): return float(val.item())
        return float(val)
    except: return None

def get_historical(ticker, days=40):
    """Lightweight: nur 40 Tage für schnelle Scans."""
    try:
        end = datetime.now()
        start = end - timedelta(days=days + 10)
        df = yf.download(ticker, start=start, end=end, progress=False, auto_adjust=True)
        if df is None or len(df) < 5: return []
        hist = []
        for date, row in df.iterrows():
            try:
                hist.append({
                    "date": str(date)[:10],
                    "close": to_scalar(row["Close"]),
                    "high": to_scalar(row["High"]),
                    "low": to_scalar(row["Low"]),
                    "volume": to_scalar(row["Volume"]),
                })
            except: pass
        return hist
    except: return []

def pct_change(hist, n):
    if not hist or len(hist) < n: return None
    try:
        e, s = hist[-1]["close"], hist[-n]["close"]
        if not e or not s or s == 0: return None
        return round((e / s - 1) * 100, 2)
    except: return None

def dollar_vol_change(hist, n):
    """Flow: Dollar Volume aktuelle n Tage vs. vorherige n Tage."""
    if not hist or len(hist) < n * 2: return None
    def dvol(bars):
        t = 0
        for b in bars:
            h, l, c, v = b.get("high") or 0, b.get("low") or 0, b.get("close") or 0, b.get("volume") or 0
            if h and l and c and v: t += ((h+l+c)/3) * v
        return t
    cur = dvol(hist[-n:])
    prev = dvol(hist[-(n*2):-n])
    if not cur or not prev or prev == 0: return None
    return round((cur / prev - 1) * 100, 2)

def obv_trend(hist, n):
    if not hist or len(hist) < n + 1: return None
    seg = hist[-(n+1):]
    obv, obv_start = 0, None
    for i in range(1, len(seg)):
        pc, cc, v = seg[i-1]["close"] or 0, seg[i]["close"] or 0, seg[i]["volume"] or 0
        if cc > pc: obv += v
        elif cc < pc: obv -= v
        if i == 1: obv_start = obv
    if obv_start is None: return None
    avg_v = sum(b["volume"] or 0 for b in seg[1:]) / max(len(seg)-1, 1)
    if avg_v == 0: return None
    return round((obv - obv_start) / avg_v, 2)

def ad_trend(hist, n):
    """A/D Line (Chaikin) — intraday-sensitiv, normalisiert auf Ø-Volumen."""
    if not hist or len(hist) < n + 1: return None
    seg = hist[-(n+1):]
    ad, ad_start = 0, None
    for i in range(1, len(seg)):
        h = seg[i].get("high") or 0
        l = seg[i].get("low") or 0
        c = seg[i].get("close") or 0
        v = seg[i].get("volume") or 0
        rng = h - l
        if rng > 0 and v > 0:
            mfm = ((c - l) - (h - c)) / rng
            ad += mfm * v
        if i == 1: ad_start = ad
    if ad_start is None: return None
    avg_v = sum(b.get("volume") or 0 for b in seg[1:]) / max(len(seg)-1, 1)
    if avg_v == 0: return None
    return round((ad - ad_start) / avg_v, 2)

def flow_confidence(obv, ad):
    """Confidence 0–100 für OBV-Direction basierend auf A/D-Übereinstimmung."""
    if obv is None or ad is None: return None
    agree = (obv >= 0) == (ad >= 0)
    strength = min(1.0, (abs(obv) + abs(ad)) / 4.0)
    return int(round(50 + 50 * strength if agree else 50 - 50 * strength))

def flow_direction(obv_1w, perf_1w):
    if obv_1w is None or perf_1w is None: return "UNKNOWN"
    if obv_1w > 0 and perf_1w > 0:  return "ACCUMULATION"
    if obv_1w < 0 and perf_1w < 0:  return "DISTRIBUTION"
    if obv_1w > 0 and perf_1w < 0:  return "STEALTH_ACCUMULATION"
    if obv_1w < 0 and perf_1w > 0:  return "WEAK_RALLY"
    return "NEUTRAL"


# ══════════════════════════════════════════════════════════
# SCAN FUNCTIONS
# ══════════════════════════════════════════════════════════

def scan_ticker(ticker, name=""):
    """Scan eines Tickers: 1W + 1M Flow, OBV, Perf, Direction über beide Zeitrahmen."""
    hist = get_historical(ticker, days=80)  # 80 Tage für 1M OBV + Baseline
    if not hist: return None
    perf_1w  = pct_change(hist, 5)
    perf_1m  = pct_change(hist, 21)
    flow_1w  = dollar_vol_change(hist, 5)
    flow_1m  = dollar_vol_change(hist, 21)
    obv_1w   = obv_trend(hist, 5)
    obv_1m   = obv_trend(hist, 21)
    ad_1w    = ad_trend(hist, 5)
    ad_1m    = ad_trend(hist, 21)
    dir_1w   = flow_direction(obv_1w, perf_1w)
    dir_1m   = flow_direction(obv_1m, perf_1m)
    conf_1w  = flow_confidence(obv_1w, ad_1w)
    conf_1m  = flow_confidence(obv_1m, ad_1m)
    price = hist[-1]["close"] if hist else None
    return {
        "ticker": ticker, "name": name, "price": price,
        "perf_1W": perf_1w, "perf_1M": perf_1m,
        "flow_1W": flow_1w, "flow_1M": flow_1m,
        "obv_1W": obv_1w, "obv_1M": obv_1m,
        "ad_1W":  ad_1w,  "ad_1M":  ad_1m,
        "confidence_1W": conf_1w, "confidence_1M": conf_1m,
        "flow_direction": dir_1w,       # 1W direction (akut)
        "flow_direction_1M": dir_1m,    # 1M direction (strukturell)
    }

def scan_ratio(t1, t2):
    """Ratio-Performance 1W und 1M."""
    h1 = get_historical(t1, days=40)
    h2 = get_historical(t2, days=40)
    if not h1 or not h2: return None
    d1 = {b["date"]: b["close"] for b in h1 if b["close"]}
    d2 = {b["date"]: b["close"] for b in h2 if b["close"]}
    common = sorted(set(d1) & set(d2))
    if len(common) < 10: return None
    ratios = [{"date": d, "val": d1[d]/d2[d]} for d in common if d2[d]]
    def rchg(n):
        if len(ratios) < n: return None
        e, s = ratios[-1]["val"], ratios[-n]["val"]
        if not s: return None
        return round((e/s - 1) * 100, 2)
    return {"ratio_chg_1W": rchg(5), "ratio_chg_1M": rchg(21), "current": round(ratios[-1]["val"], 4) if ratios else None}


# ══════════════════════════════════════════════════════════
# ALERT DETECTION
# ══════════════════════════════════════════════════════════

def detect_alerts(scan_results, ratio_results, flow_data=None, spy_scan=None):
    """Analysiere Scan-Ergebnisse und generiere Alerts."""
    alerts = []

    # Collect all tickers flat for cross-referencing
    all_tickers = []
    for theme, tickers in scan_results.items():
        for t in tickers:
            if t: all_tickers.append({**t, "_theme": theme})

    # ─── Market-Level Checks (from flow_data.json + SPY scan) ───
    if flow_data and flow_data.get("sectors"):
        sectors = flow_data["sectors"]

        # Multi-timeframe direction counts
        def tf_counts(period):
            accum=dist=weak=stealth=0
            for s in sectors:
                obv = s.get(f"obv_{period}")
                perf = s.get(f"perf_{period}")
                if obv is None or perf is None: continue
                if obv>0 and perf>0: accum+=1
                elif obv<0 and perf<0: dist+=1
                elif obv<0 and perf>0: weak+=1
                elif obv>0 and perf<0: stealth+=1
            return {"accum":accum,"dist":dist,"weak":weak,"stealth":stealth}

        tf1w = tf_counts("1W")
        tf1m = tf_counts("1M")
        tf2m = tf_counts("2M")

        short_bullish = tf1w["accum"] >= 7
        med_bearish = tf2m["dist"] >= 5
        month_bearish = tf1m["dist"] >= 4 or tf1m["weak"] >= 3

        # SPY volume declining while price rising?
        spy_weak = spy_scan and spy_scan.get("flow_direction") == "WEAK_RALLY"
        spy_vol_declining = spy_scan and (spy_scan.get("flow_1W") or 0) < -15

        # Shares divergence from flow_data
        spy_shares_div = False
        shares = flow_data.get("shares_tracking", {})
        if shares.get("SPY", {}).get("signal") == "DIVERGENCE":
            spy_shares_div = True

        # ── BEAR MARKET RALLY ──
        if short_bullish and med_bearish:
            vol_note = ""
            if spy_weak:
                vol_note = " SPY selbst zeigt WEAK_RALLY — Rallye ohne Überzeugung."
            elif spy_vol_declining:
                vol_note = f" SPY-Volumen fällt {spy_scan.get('flow_1W'):+.1f}% vs. Vorwoche."
            elif spy_shares_div:
                vol_note = " SPY Shares Outstanding: DIVERGENCE — institutionelle Redemptions."

            alerts.append({
                "type": "BEAR_MARKET_RALLY",
                "severity": "HIGH",
                "ticker": "MARKT",
                "name": "Multi-Timeframe Widerspruch",
                "theme": "Makro",
                "message": f"🐻 BÄRENMARKTRALLYE: Kurzfristig (1W) akkumulieren {tf1w['accum']}/11 Sektoren. "
                           f"Aber strukturell (2M) sind {tf2m['dist']}/11 in Distribution. "
                           f"Typisches Muster einer technischen Erholung im Abwärtstrend.{vol_note}",
                "data": {
                    "1W_accum": tf1w["accum"], "1W_dist": tf1w["dist"],
                    "2M_accum": tf2m["accum"], "2M_dist": tf2m["dist"],
                    "1M_accum": tf1m["accum"], "1M_dist": tf1m["dist"],
                    "spy_direction": spy_scan.get("flow_direction") if spy_scan else None,
                    "spy_shares_divergence": spy_shares_div,
                },
            })

        # ── BROAD DISTRIBUTION ──
        elif tf1w["dist"] >= 6 and tf1m["dist"] >= 4:
            alerts.append({
                "type": "BROAD_DISTRIBUTION",
                "severity": "HIGH",
                "ticker": "MARKT",
                "name": "Breite Distribution",
                "theme": "Makro",
                "message": f"🔴 BROAD DISTRIBUTION: 1W {tf1w['dist']}/11 und 1M {tf1m['dist']}/11 Sektoren in Distribution. "
                           f"Multi-Timeframe bestätigt. Breiter Kapitalabzug.",
                "data": {"1W_dist": tf1w["dist"], "1M_dist": tf1m["dist"]},
            })

        # ── SPY WEAK RALLY (market-level) ──
        if spy_scan and spy_scan.get("flow_direction") == "WEAK_RALLY":
            spy_perf = spy_scan.get("perf_1W") or 0
            spy_obv = spy_scan.get("obv_1W") or 0
            alerts.append({
                "type": "MARKET_WEAK_RALLY",
                "severity": "HIGH",
                "ticker": "SPY",
                "name": "S&P 500 Weak Rally",
                "theme": "Makro",
                "message": f"⚠️ SPY WEAK RALLY: S&P 500 steigt {spy_perf:+.1f}% aber OBV fällt ({spy_obv:+.1f}). "
                           f"Gesamtmarkt-Rallye nicht durch Volumen bestätigt.",
                "data": {"perf_1W": spy_perf, "obv_1W": spy_obv, "flow_1W": spy_scan.get("flow_1W")},
            })

        # ── SPY SHARES DIVERGENCE ──
        if spy_shares_div:
            div_val = shares.get("SPY", {}).get("price_volume_divergence", 0)
            alerts.append({
                "type": "SHARES_DIVERGENCE",
                "severity": "MEDIUM",
                "ticker": "SPY",
                "name": "Shares Outstanding Divergence",
                "theme": "Makro",
                "message": f"📉 SPY SHARES DIVERGENCE: Institutionelle Investoren geben ETF-Anteile zurück "
                           f"(Divergenz: {div_val:+.1f}%) während Preise steigen. Echte Kapitalabflüsse.",
                "data": {"divergence": div_val},
            })

    # ─── Pattern 1: Distribution Topping (Multi-Timeframe) ───
    # Das wichtigste Signal: WEAK_RALLY über 1W UND 1M = strukturelles Topping
    for t in all_tickers:
        dir_1w = t.get("flow_direction", "")
        dir_1m = t.get("flow_direction_1M", "")
        perf_1w = t.get("perf_1W") or 0
        perf_1m = t.get("perf_1M") or 0
        obv_1w = t.get("obv_1W")
        obv_1m = t.get("obv_1M")
        flow_1w = t.get("flow_1W")

        # ── HÖCHSTE PRIORITÄT: Multi-Timeframe WEAK RALLY ──
        # 1W UND 1M zeigen beide OBV↓ bei Preis↑ — Smart Money steigt über Wochen aus
        if dir_1w == "WEAK_RALLY" and dir_1m == "WEAK_RALLY":
            alerts.append({
                "type": "DISTRIBUTION_TOPPING",
                "severity": "HIGH",
                "ticker": t["ticker"],
                "name": t.get("name", ""),
                "theme": t["_theme"],
                "message": f"🔴 TOPPING: {t['ticker']} zeigt WEAK RALLY über 1W UND 1M. Preis steigt (1W:{perf_1w:+.1f}%, 1M:{perf_1m:+.1f}%) aber OBV fällt auf beiden Zeitrahmen (1W:{obv_1w:+.1f}, 1M:{obv_1m:+.1f}). Smart Money steigt systematisch aus.",
                "data": {"perf_1W": perf_1w, "perf_1M": perf_1m, "obv_1W": obv_1w, "obv_1M": obv_1m, "flow_1W": flow_1w, "pattern": "MULTI_TF_WEAK_RALLY"},
            })

        # ── Einzelner Zeitrahmen WEAK RALLY (weniger stark aber beachtenswert) ──
        elif dir_1w == "WEAK_RALLY" and perf_1w >= WEAK_RALLY_PERF_MIN:
            # Nur 1W — könnte Rauschen sein, aber bei starker Performance flaggen
            sev = "MEDIUM" if dir_1m != "ACCUMULATION" else "LOW"
            alerts.append({
                "type": "DISTRIBUTION_TOPPING",
                "severity": sev,
                "ticker": t["ticker"],
                "name": t.get("name", ""),
                "theme": t["_theme"],
                "message": f"⚠️ WEAK RALLY: {t['ticker']} steigt {perf_1w:+.1f}% (1W) aber OBV fällt ({obv_1w:+.1f}). 1M-Direction: {dir_1m}. {'Noch nicht strukturell bestätigt.' if dir_1m != 'WEAK_RALLY' else ''}",
                "data": {"perf_1W": perf_1w, "perf_1M": perf_1m, "obv_1W": obv_1w, "obv_1M": obv_1m, "direction_1M": dir_1m},
            })

        # ── DISTRIBUTION über 1M bei flacher/positiver 1M-Performance ──
        elif dir_1m == "WEAK_RALLY" and perf_1m > 3:
            alerts.append({
                "type": "DISTRIBUTION_TOPPING",
                "severity": "MEDIUM",
                "ticker": t["ticker"],
                "name": t.get("name", ""),
                "theme": t["_theme"],
                "message": f"📉 1M DISTRIBUTION: {t['ticker']} zeigt WEAK RALLY auf Monatsbasis. Preis 1M:{perf_1m:+.1f}% aber OBV 1M fällt ({obv_1m:+.1f}). Strukturelle Schwäche unter der Oberfläche.",
                "data": {"perf_1W": perf_1w, "perf_1M": perf_1m, "obv_1M": obv_1m, "direction_1M": dir_1m},
            })

        # ── STEALTH ACCUMULATION — Smart Money kauft bei Schwäche ──
        if dir_1w == "STEALTH_ACCUMULATION" and perf_1w < -2:
            sev = "HIGH" if dir_1m == "STEALTH_ACCUMULATION" else "MEDIUM"
            multi = " Multi-Timeframe bestätigt!" if dir_1m == "STEALTH_ACCUMULATION" else ""
            alerts.append({
                "type": "STEALTH_ACCUMULATION",
                "severity": sev,
                "ticker": t["ticker"],
                "name": t.get("name", ""),
                "theme": t["_theme"],
                "message": f"🟣 STEALTH BUY: {t['ticker']} fällt {perf_1w:+.1f}% aber OBV steigt ({obv_1w:+.1f}). Akkumulation bei Schwäche.{multi}",
                "data": {"perf_1W": perf_1w, "perf_1M": perf_1m, "obv_1W": obv_1w, "obv_1M": obv_1m, "direction_1M": dir_1m},
            })

    # ─── Pattern 2: Outlier Flows ───
    for t in all_tickers:
        flow = t.get("flow_1W")
        if flow is not None and abs(flow) > OUTLIER_FLOW_THRESHOLD:
            emoji = "🔥" if flow > 0 else "🧊"
            alerts.append({
                "type": "OUTLIER_FLOW",
                "severity": "HIGH" if abs(flow) > 80 else "MEDIUM",
                "ticker": t["ticker"],
                "name": t.get("name", ""),
                "theme": t["_theme"],
                "message": f"{emoji} OUTLIER: {t['ticker']} Flow 1W = {flow:+.1f}% (Threshold: ±{OUTLIER_FLOW_THRESHOLD}%). Perf: {(t.get('perf_1W') or 0):+.1f}%. Direction: {t.get('flow_direction','')}",
                "data": {"flow_1W": flow, "perf_1W": t.get("perf_1W"), "obv_1W": t.get("obv_1W"), "direction": t.get("flow_direction")},
            })

    # ─── Pattern 3: Ratio Divergences + Credit Early Warning ───
    hyg_lqd = ratio_results.get("HYG/LQD", {})
    iwm_spy = ratio_results.get("IWM/SPY", {})
    copper_gold = ratio_results.get("COPPER/GOLD", {})
    gld_spy = ratio_results.get("GLD/SPY", {})
    tlt_shy = ratio_results.get("TLT/SHY", {})
    xly_xlp = ratio_results.get("XLY/XLP", {})

    hyg_chg = hyg_lqd.get("ratio_chg_1W")
    iwm_chg = iwm_spy.get("ratio_chg_1W")
    cu_chg  = copper_gold.get("ratio_chg_1W")
    gld_chg = gld_spy.get("ratio_chg_1W")
    tlt_chg = tlt_shy.get("ratio_chg_1W")
    xly_chg = xly_xlp.get("ratio_chg_1W")

    # ── Credit Early Warning: HYG/LQD fällt ──
    if hyg_chg is not None and hyg_chg < -RATIO_DIVERGENCE_PCT:
        # Cross-reference: sind Equity-Flows noch positiv? Dann ist das ein Frühwarnsignal
        equity_still_ok = any(t.get("perf_1W", 0) > 0 for t in all_tickers if t.get("_theme") in ["AI & Cloud", "Semiconductors"])
        extra = " Equities zeigen noch keine Schwäche — klassisches Frühwarnsignal!" if equity_still_ok else ""
        alerts.append({
            "type": "CREDIT_EARLY_WARNING",
            "severity": "HIGH",
            "ticker": "HYG/LQD",
            "name": "Credit Risk Appetite",
            "theme": "Makro",
            "message": f"⚠️ CREDIT STRESS: HYG/LQD fällt {hyg_chg:+.2f}% diese Woche. High Yield unter Druck — historisch einer der zuverlässigsten Vorlaufindikatoren für Equity-Drawdowns.{extra}",
            "data": {"hyg_lqd_1W": hyg_chg, "iwm_spy_1W": iwm_chg, "equity_still_positive": equity_still_ok},
        })

    # ── Growth vs Breadth Divergence ──
    if cu_chg is not None and iwm_chg is not None:
        if cu_chg < -RATIO_DIVERGENCE_PCT and iwm_chg > RATIO_DIVERGENCE_PCT:
            alerts.append({
                "type": "RATIO_DIVERGENCE",
                "severity": "MEDIUM",
                "ticker": "COPPER/GOLD vs IWM/SPY",
                "name": "Growth vs. Breadth",
                "theme": "Makro",
                "message": f"DIVERGENZ: Copper/Gold fällt {cu_chg:+.2f}% (Rezessionserwartung) aber Small Caps outperformen {iwm_chg:+.2f}% (Risk-On). Widersprüchlich — einer liegt falsch.",
                "data": {"copper_gold_1W": cu_chg, "iwm_spy_1W": iwm_chg},
            })
        if cu_chg > RATIO_DIVERGENCE_PCT and iwm_chg < -RATIO_DIVERGENCE_PCT:
            alerts.append({
                "type": "RATIO_DIVERGENCE",
                "severity": "MEDIUM",
                "ticker": "COPPER/GOLD vs IWM/SPY",
                "name": "Growth vs. Breadth",
                "theme": "Makro",
                "message": f"DIVERGENZ: Copper/Gold steigt {cu_chg:+.2f}% (Wachstum) aber Small Caps fallen {iwm_chg:+.2f}% (Risk-Off). Industrielle Nachfrage ohne Equity-Bestätigung.",
                "data": {"copper_gold_1W": cu_chg, "iwm_spy_1W": iwm_chg},
            })

    # ── Safe Haven Paradox ──
    if gld_chg is not None and xly_chg is not None:
        if gld_chg > RATIO_DIVERGENCE_PCT and xly_chg > RATIO_DIVERGENCE_PCT:
            alerts.append({
                "type": "RATIO_DIVERGENCE",
                "severity": "MEDIUM",
                "ticker": "GLD/SPY vs XLY/XLP",
                "name": "Safe Haven Paradox",
                "theme": "Makro",
                "message": f"PARADOX: Gold outperformt Equities ({gld_chg:+.2f}%) UND Cyclicals outperformen Defensives ({xly_chg:+.2f}%). Markt hedgt und kauft gleichzeitig — Unsicherheit.",
                "data": {"gld_spy_1W": gld_chg, "xly_xlp_1W": xly_chg},
            })

    # ── Duration Shift ──
    if tlt_chg is not None and abs(tlt_chg) > 2.0:
        direction = "Rate-Cut-Wette (Rezessionsangst)" if tlt_chg > 0 else "Inflationsangst / Higher-for-Longer"
        alerts.append({
            "type": "DURATION_SHIFT",
            "severity": "HIGH" if abs(tlt_chg) > 3.0 else "MEDIUM",
            "ticker": "TLT/SHY",
            "name": "Duration Appetite",
            "theme": "Makro",
            "message": f"DURATION SHIFT: TLT/SHY {tlt_chg:+.2f}% diese Woche → {direction}. Starke Bond-Rotation.",
            "data": {"tlt_shy_1W": tlt_chg},
        })

    # Sort: HIGH first, then by data significance
    severity_order = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}
    alerts.sort(key=lambda a: (severity_order.get(a["severity"], 9), -abs(a.get("data", {}).get("flow_1W") or a.get("data", {}).get("obv_1W") or 0)))

    return alerts


# ══════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════

def main():
    print(f"\n{'='*60}")
    print(f"  Alert Scanner v2 — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"  Broad ETF Scan + Pattern Detection + Market-Level Checks")
    print(f"{'='*60}")

    all_scan_results = {}

    # ── Load flow_data.json for cross-reference ──
    flow_data = None
    flow_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "flow_data.json")
    if os.path.exists(flow_path):
        try:
            with open(flow_path) as f:
                flow_data = json.load(f)
            print(f"  Cross-Reference: flow_data.json geladen (v{flow_data.get('version','?')})")
        except:
            print(f"  ⚠ flow_data.json nicht lesbar")
    else:
        print(f"  ⚠ flow_data.json nicht gefunden — Markt-Level Alerts eingeschränkt")

    # ── Scan SPY for market-level volume check ──
    print(f"\n  Scanne SPY für Markt-Level Checks...")
    spy_scan = scan_ticker("SPY", "S&P 500 ETF")
    if spy_scan:
        d1w = spy_scan.get("flow_direction","")
        print(f"    SPY: perf={spy_scan.get('perf_1W')}% flow={spy_scan.get('flow_1W')}% obv={spy_scan.get('obv_1W')} [{d1w}]")

    # ── Scan Broad Theme ETFs ──
    total_tickers = sum(len(v) for v in BROAD_THEME_ETFS.values())
    print(f"\n  Scanne {total_tickers} ETFs über {len(BROAD_THEME_ETFS)} Themen...\n")

    for theme_name, tickers in BROAD_THEME_ETFS.items():
        print(f"  {theme_name}:")
        results = []
        for ticker, name in tickers.items():
            print(f"    {ticker}...", end=" ", flush=True)
            r = scan_ticker(ticker, name)
            if r:
                d1w = r.get("flow_direction", "")
                d1m = r.get("flow_direction_1M", "")
                print(f"✓ perf={r.get('perf_1W')}% flow={r.get('flow_1W')}% [1W:{d1w} 1M:{d1m}]")
                results.append(r)
            else:
                print("⚠")
        all_scan_results[theme_name] = results

    # ── Scan Ratios ──
    print(f"\n  Scanne {len(RATIO_PAIRS)} Ratio-Signale...")
    ratio_results = {}
    for name, (t1, t2, desc) in RATIO_PAIRS.items():
        print(f"    {name}...", end=" ", flush=True)
        r = scan_ratio(t1, t2)
        if r:
            r["name"] = desc
            ratio_results[name] = r
            print(f"✓ 1W={r.get('ratio_chg_1W','—')}%")
        else:
            print("⚠")

    # ── Detect Alerts ──
    print(f"\n  Analysiere Patterns...")
    alerts = detect_alerts(all_scan_results, ratio_results, flow_data=flow_data, spy_scan=spy_scan)

    # ── Build Output ──
    # Theme summaries for dashboard
    theme_summaries = {}
    from collections import Counter
    for theme, tickers in all_scan_results.items():
        if not tickers: continue
        perfs_1w = [t["perf_1W"] for t in tickers if t.get("perf_1W") is not None]
        flows_1w = [t["flow_1W"] for t in tickers if t.get("flow_1W") is not None]
        obvs_1w  = [t["obv_1W"]  for t in tickers if t.get("obv_1W") is not None]
        dirs_1w  = [t["flow_direction"] for t in tickers if t.get("flow_direction") and t["flow_direction"] != "UNKNOWN"]

        perfs_1m = [t["perf_1M"] for t in tickers if t.get("perf_1M") is not None]
        flows_1m = [t["flow_1M"] for t in tickers if t.get("flow_1M") is not None]
        obvs_1m  = [t["obv_1M"]  for t in tickers if t.get("obv_1M") is not None]
        dirs_1m  = [t["flow_direction_1M"] for t in tickers if t.get("flow_direction_1M") and t["flow_direction_1M"] != "UNKNOWN"]

        dom_dir_1w = Counter(dirs_1w).most_common(1)[0][0] if dirs_1w else "UNKNOWN"
        dom_dir_1m = Counter(dirs_1m).most_common(1)[0][0] if dirs_1m else "UNKNOWN"

        theme_summaries[theme] = {
            "etf_count": len(tickers),
            "avg_perf_1W": round(median(perfs_1w), 2) if perfs_1w else None,
            "avg_flow_1W": round(median(flows_1w), 2) if flows_1w else None,
            "avg_obv_1W": round(median(obvs_1w), 2) if obvs_1w else None,
            "dominant_direction": dom_dir_1w,
            "avg_perf_1M": round(median(perfs_1m), 2) if perfs_1m else None,
            "avg_flow_1M": round(median(flows_1m), 2) if flows_1m else None,
            "avg_obv_1M": round(median(obvs_1m), 2) if obvs_1m else None,
            "dominant_direction_1M": dom_dir_1m,
            "tickers": tickers,
        }

    output = {
        "generated_at": datetime.now().isoformat(),
        "scan_universe": total_tickers,
        "themes_scanned": len(BROAD_THEME_ETFS),
        "alerts": alerts,
        "alert_count": len(alerts),
        "alert_count_high": len([a for a in alerts if a["severity"] == "HIGH"]),
        "theme_summaries": theme_summaries,
        "ratio_signals": ratio_results,
    }

    out_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "alerts.json")
    with open(out_path, "w") as f:
        json.dump(clean_nan(output), f, indent=2)

    # ── Print Summary ──
    n_high = output["alert_count_high"]
    n_total = output["alert_count"]
    print(f"\n{'='*60}")
    print(f"  ✓ {out_path}")
    print(f"  {n_total} Alerts ({n_high} HIGH severity)")
    print(f"  {total_tickers} ETFs gescannt über {len(BROAD_THEME_ETFS)} Themen")

    if alerts:
        print(f"\n  🚨 TOP ALERTS:")
        for a in alerts[:10]:
            sev = "🔴" if a["severity"] == "HIGH" else "🟡"
            print(f"    {sev} [{a['type']}] {a['message']}")

    # Theme overview
    print(f"\n  📊 THEMEN-ÜBERSICHT (nicht im Haupt-Dashboard):")
    sorted_themes = sorted(theme_summaries.items(),
                          key=lambda x: abs(x[1].get("avg_flow_1W") or 0), reverse=True)
    for tname, ts in sorted_themes[:10]:
        flow = ts.get("avg_flow_1W") or 0
        perf = ts.get("avg_perf_1W") or 0
        d = ts.get("dominant_direction", "")
        print(f"    {tname:25s}  Flow:{flow:+.1f}%  Perf:{perf:+.1f}%  [{d}]")

    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
