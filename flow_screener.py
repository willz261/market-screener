#!/usr/bin/env python3
"""
Flow Screener v3 — Institutional Flow Tracking + History

Kapitalfluss-Analyse per Sektor, Thema und Makro-Regime.
Neu in v3:
- Tages-Deltas: vergleicht aktuellen Lauf mit dem vorherigen
- flow_history.json: tägliche Snapshots für Trendanalyse
- Direction-Streaks: wie lange hält ein Signal schon an
- Score-Trend: 5-Tage gleitender Durchschnitt der Flow-Scores

Bestehend (v2):
- OBV, Makro-Regime, Ratio-Signale, Shares Outstanding
- Flow-Score Capping ±200%, Median-Aggregation für Themen

Datenquelle: yfinance (kostenlos, kein API-Key nötig)
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

# FRED API (optional — für M2 Money Supply, NFCI)
try:
    from config import FRED_API_KEY
except ImportError:
    FRED_API_KEY = ""

# ══════════════════════════════════════════════════════════
# CONFIGURATION
# ══════════════════════════════════════════════════════════

FLOW_SCORE_CAP = 200.0  # ±200% Maximum für Einzelwerte

# ── Sektor ETFs ───────────────────────────────────────────
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

# ── Themen-Aktien ─────────────────────────────────────────
THEME_STOCKS = {
    "Photonics & Laser": ["COHR", "LITE", "IPGP", "VIAV", "POET"],
    "Optoelectronics":   ["AAOI", "OLED", "TSEM", "ONTO", "MKSI"],
    "Networks & Infra":  ["CIEN", "NBIS", "COHU", "AMAT", "KLAC"],
    "European Photonics":["JEN.DE", "AIXA.DE", "ALRIB.PA", "SOI.PA", "SIVE.ST"],
    "AI & Data Center":  ["NVDA", "AMD", "SMCI", "ARM", "AVGO", "MRVL"],
    "Defense & Aero":    ["LMT", "RTX", "GD", "NOC", "LHX", "BA"],
}

# ── Kapitalfluss-Ziele ───────────────────────────────────
CAPITAL_FLOWS = {
    "Bonds & Rates": {
        "TLT":  "US Treasuries 20Y+",
        "IEF":  "US Treasuries 7-10Y",
        "SHY":  "US Treasuries 1-3Y",
        "HYG":  "High Yield Bonds",
        "LQD":  "Investment Grade Corp.",
        "TIP":  "Inflation-Linked Bonds",
    },
    "Gold & Commodities": {
        "GLD":  "Gold ETF",
        "IAU":  "Gold (iShares)",
        "SLV":  "Silver",
        "PDBC": "Broad Commodities",
        "USO":  "Oil (WTI)",
        "BZ=F": "Brent Crude",
        "WEAT": "Wheat",
    },
    "Cash & Geldmarkt": {
        "BIL":  "T-Bills (1-3M)",
        "SGOV": "T-Bills (0-3M)",
        "JPST": "JPM Ultra-Short Bond",
    },
    "Währungen & FX": {
        "FXY":  "Japanischer Yen",
        "FXF":  "Schweizer Franken",
        "UUP":  "US Dollar Index",
        "FXE":  "Euro",
    },
    "Defensive Aktien": {
        "XLU":  "Utilities ETF",
        "XLP":  "Consumer Staples ETF",
        "XLV":  "Healthcare ETF",
        "KO":   "Coca-Cola",
        "JNJ":  "Johnson & Johnson",
    },
    "Alternative Assets": {
        "BTC-USD": "Bitcoin",
        "GDX":     "Gold Miner ETF",
        "GDXJ":    "Junior Gold Miners",
        "VNQ":     "US Real Estate (REIT)",
    },
}

# ── Makro-Regime Indikatoren ──────────────────────────────
MACRO_TICKERS = {
    "VIX":      {"ticker": "^VIX",     "name": "CBOE Volatility Index"},
    "VIX9D":    {"ticker": "^VIX9D",   "name": "CBOE 9-Day VIX"},
    "VIX3M":    {"ticker": "^VIX3M",   "name": "CBOE 3-Month VIX"},
    "VIX6M":    {"ticker": "^VIX6M",   "name": "CBOE 6-Month VIX"},
    "DXY":      {"ticker": "DX-Y.NYB", "name": "US Dollar Index"},
    "US10Y":    {"ticker": "^TNX",     "name": "US 10Y Treasury Yield"},
    "US02Y":    {"ticker": "^IRX",     "name": "US 13-Week T-Bill Rate"},
    "BRENT":    {"ticker": "BZ=F",     "name": "Brent Crude Oil"},
    "WTI":      {"ticker": "CL=F",     "name": "WTI Crude Oil"},
}

# ── Ratio-Signale ─────────────────────────────────────────
RATIO_SIGNALS = {
    "HYG/LQD": {
        "numerator": "HYG", "denominator": "LQD",
        "name": "Credit Risk Appetite",
        "interpretation": "Steigend = Risk-On (HY > IG), Fallend = Risk-Off",
    },
    "GLD/SPY": {
        "numerator": "GLD", "denominator": "SPY",
        "name": "Safe Haven vs. Equities",
        "interpretation": "Steigend = Flucht in Gold, Fallend = Equity-Preference",
    },
    "IWM/SPY": {
        "numerator": "IWM", "denominator": "SPY",
        "name": "Small Cap vs. Large Cap",
        "interpretation": "Steigend = Risk-On/Breadth, Fallend = Mega-Cap-Konzentration",
    },
    "TLT/SHY": {
        "numerator": "TLT", "denominator": "SHY",
        "name": "Duration Appetite",
        "interpretation": "Steigend = Rate-Cut-Wette, Fallend = Inflationsangst",
    },
    "XLY/XLP": {
        "numerator": "XLY", "denominator": "XLP",
        "name": "Cyclical vs. Defensive",
        "interpretation": "Steigend = Konjunkturoptimismus, Fallend = Defensive Rotation",
    },
    "COPPER/GOLD": {
        "numerator": "CPER", "denominator": "GLD",
        "name": "Dr. Copper vs. Gold",
        "interpretation": "Steigend = Wachstum, Fallend = Stagflation/Rezession",
    },
}

# ── Shares Outstanding Tracking ──────────────────────────
SHARES_TRACKING_ETFS = [
    "SPY", "QQQ", "IWM",
    "XLK", "XLE", "XLF", "XLV", "XLI", "XLU",
    "TLT", "HYG", "LQD", "GLD", "SLV",
    "XBI",
]

# ── Zeitfenster ───────────────────────────────────────────
PERIODS = {
    "1W":  5,
    "2W":  10,
    "1M":  21,
    "2M":  42,
    "1Q":  63,
}


# ══════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════

def to_scalar(val):
    try:
        if hasattr(val, 'iloc'): val = val.iloc[0]
        if hasattr(val, 'item'): return float(val.item())
        return float(val)
    except:
        return None

def cap_flow(val):
    """Cap flow score to ±FLOW_SCORE_CAP."""
    if val is None: return None
    return round(max(-FLOW_SCORE_CAP, min(FLOW_SCORE_CAP, val)), 2)

def safe_median(values):
    if not values: return None
    return round(median(values), 2)

def safe_mean(values):
    if not values: return None
    return round(sum(values) / len(values), 2)

def get_historical(ticker, days=160):
    """Hole OHLCV als Liste von Dicts, älteste zuerst."""
    try:
        end   = datetime.now()
        start = end - timedelta(days=days + 30)
        df    = yf.download(ticker, start=start, end=end,
                            progress=False, auto_adjust=True)
        if df is None or len(df) < 5:
            return []
        hist = []
        for date, row in df.iterrows():
            try:
                hist.append({
                    "date":   str(date)[:10],
                    "open":   to_scalar(row["Open"]),
                    "high":   to_scalar(row["High"]),
                    "low":    to_scalar(row["Low"]),
                    "close":  to_scalar(row["Close"]),
                    "volume": to_scalar(row["Volume"]),
                })
            except:
                pass
        return hist
    except Exception as e:
        print(f"⚠ yfinance error ({ticker}): {e}")
        return []


# ══════════════════════════════════════════════════════════
# FLOW CALCULATIONS
# ══════════════════════════════════════════════════════════

def dollar_volume_for_period(hist, n_days):
    if not hist or len(hist) < n_days: return None
    total = 0
    for bar in hist[-n_days:]:
        try:
            h, l, c, v = bar["high"] or 0, bar["low"] or 0, bar["close"] or 0, bar["volume"] or 0
            if h and l and c and v:
                total += ((h + l + c) / 3) * v
        except: pass
    return total if total > 0 else None

def flow_score_vs_prev(hist, n_days):
    if not hist or len(hist) < n_days * 2: return None
    current  = dollar_volume_for_period(hist[-n_days:], n_days)
    previous = dollar_volume_for_period(hist[-(n_days*2):-n_days], n_days)
    if not current or not previous or previous == 0: return None
    return round((current / previous - 1) * 100, 2)

def flow_score_vs_avg(hist, n_days, avg_days=20):
    if not hist or len(hist) < avg_days + n_days: return None
    baseline_bars = hist[-(avg_days + n_days):-n_days]
    if not baseline_bars: return None
    daily_dvols = []
    for bar in baseline_bars:
        try:
            h, l, c, v = bar["high"] or 0, bar["low"] or 0, bar["close"] or 0, bar["volume"] or 0
            if h and l and c and v:
                daily_dvols.append(((h + l + c) / 3) * v)
        except: pass
    if not daily_dvols: return None
    baseline_total = (sum(daily_dvols) / len(daily_dvols)) * n_days
    current = dollar_volume_for_period(hist[-n_days:], n_days)
    if not current or baseline_total == 0: return None
    return round((current / baseline_total - 1) * 100, 2)

def pct_change(hist, n_days):
    if not hist or len(hist) < n_days: return None
    try:
        v_end, v_start = hist[-1]["close"], hist[-n_days]["close"]
        if not v_end or not v_start or v_start == 0: return None
        return round((v_end / v_start - 1) * 100, 2)
    except: return None


# ══════════════════════════════════════════════════════════
# OBV — On-Balance Volume
# ══════════════════════════════════════════════════════════

def obv_trend(hist, n_days):
    """
    OBV-Trend: positiv = Akkumulation, negativ = Distribution.
    Normalisiert auf durchschnittliches Tagesvolumen.
    """
    if not hist or len(hist) < n_days + 1: return None
    segment = hist[-(n_days + 1):]
    obv = 0
    obv_start = None
    for i in range(1, len(segment)):
        prev_c = segment[i-1]["close"] or 0
        curr_c = segment[i]["close"] or 0
        vol    = segment[i]["volume"] or 0
        if curr_c > prev_c:   obv += vol
        elif curr_c < prev_c: obv -= vol
        if i == 1: obv_start = obv
    if obv_start is None: return None
    avg_vol = sum(bar["volume"] or 0 for bar in segment[1:]) / max(len(segment) - 1, 1)
    if avg_vol == 0: return None
    return round((obv - obv_start) / avg_vol, 2)


def ad_trend(hist, n_days):
    """
    A/D-Line-Trend (Chaikin Accumulation/Distribution): positiv = Close nahe
    Tageshoch = Akkumulation, negativ = Close nahe Tagestief = Distribution.
    Im Gegensatz zu OBV intraday-sensitiv (Money Flow Multiplier).
    Normalisiert auf durchschnittliches Tagesvolumen.
    """
    if not hist or len(hist) < n_days + 1: return None
    segment = hist[-(n_days + 1):]
    ad = 0
    ad_start = None
    for i in range(1, len(segment)):
        h = segment[i].get("high") or 0
        l = segment[i].get("low") or 0
        c = segment[i].get("close") or 0
        v = segment[i].get("volume") or 0
        rng = h - l
        if rng > 0 and v > 0:
            mfm = ((c - l) - (h - c)) / rng  # Money Flow Multiplier ∈ [-1, 1]
            ad += mfm * v
        if i == 1: ad_start = ad
    if ad_start is None: return None
    avg_vol = sum(bar.get("volume") or 0 for bar in segment[1:]) / max(len(segment) - 1, 1)
    if avg_vol == 0: return None
    return round((ad - ad_start) / avg_vol, 2)


def flow_confidence(obv, ad):
    """
    Confidence (0–100) dass die OBV-basierte flow_direction verlässlich ist.
    Prinzip: A/D (intraday-sensitiv) wird als Bestätigungssignal herangezogen.

      - Beide in gleiche Richtung (gleiches Vorzeichen) → Confidence > 50,
        skaliert mit kombinierter Stärke bis max. 100.
      - Divergenz zwischen OBV und A/D → Confidence < 50,
        skaliert mit Stärke des Widerspruchs bis min. 0.
      - Base-Wert 50 = keine Aussage möglich (beide 0 bzw. fehlt).

    Bedeutung:
      >75  hohe Konviktion (Signal doppelt bestätigt)
      50-75 solide (schwache Bestätigung)
      25-50 wackelig (leichte Divergenz)
      <25  widersprüchlich (verstecktes Gegensignal)
    """
    if obv is None or ad is None: return None
    agree = (obv >= 0) == (ad >= 0)
    strength = min(1.0, (abs(obv) + abs(ad)) / 4.0)  # 4.0 = starkes Doppelsignal
    return int(round(50 + 50 * strength if agree else 50 - 50 * strength))


# ══════════════════════════════════════════════════════════
# SHARES OUTSTANDING DELTA
# ══════════════════════════════════════════════════════════

def get_shares_outstanding_delta(ticker):
    """Shares Outstanding + Price-Volume Divergence als Flow-Proxy."""
    try:
        info = yf.Ticker(ticker).info
        current_shares = info.get('sharesOutstanding')
        if not current_shares: return None
        hist = get_historical(ticker, days=30)
        if not hist or len(hist) < 10: return None
        perf_1w = pct_change(hist, 5)
        flow_1w = flow_score_vs_prev(hist, 5)
        if perf_1w is not None and flow_1w is not None:
            divergence = flow_1w if perf_1w >= 0 else -flow_1w
            return {
                "shares_outstanding": current_shares,
                "price_volume_divergence": round(divergence, 2),
                "signal": "CONFIRMED" if divergence > 0 else "DIVERGENCE",
            }
        return None
    except: return None


# ══════════════════════════════════════════════════════════
# RATIO ANALYSIS
# ══════════════════════════════════════════════════════════

def analyze_ratio(num_ticker, den_ticker):
    """Ratio-Performance: steigende Ratio = Numerator outperformt."""
    num_hist = get_historical(num_ticker, days=160)
    den_hist = get_historical(den_ticker, days=160)
    if not num_hist or not den_hist: return None

    num_by_date = {b["date"]: b["close"] for b in num_hist if b["close"]}
    den_by_date = {b["date"]: b["close"] for b in den_hist if b["close"]}
    common = sorted(set(num_by_date) & set(den_by_date))
    if len(common) < 10: return None

    ratio_hist = [{"date": d, "close": num_by_date[d] / den_by_date[d]}
                  for d in common if den_by_date[d] != 0]

    result = {}
    for pname, n in PERIODS.items():
        if len(ratio_hist) >= n:
            try:
                end_val, start_val = ratio_hist[-1]["close"], ratio_hist[-n]["close"]
                if start_val and start_val != 0:
                    result[f"ratio_chg_{pname}"] = round((end_val / start_val - 1) * 100, 2)
            except: pass
    if ratio_hist:
        result["current_ratio"] = round(ratio_hist[-1]["close"], 4)
    return result if result else None


# ══════════════════════════════════════════════════════════
# MACRO REGIME SNAPSHOT
# ══════════════════════════════════════════════════════════

def get_macro_snapshot():
    print("\n  Makro-Regime Indikatoren:")
    snapshot = {}
    for key, cfg in MACRO_TICKERS.items():
        ticker, name = cfg["ticker"], cfg["name"]
        print(f"    {key} ({ticker})...", end=" ", flush=True)
        hist = get_historical(ticker, days=160)
        if not hist:
            print("⚠ keine Daten"); continue
        entry = {"name": name, "current": hist[-1]["close"] if hist else None}
        for pname, n in PERIODS.items():
            entry[f"chg_{pname}"] = pct_change(hist, n)
        snapshot[key] = entry
        c = entry['current']
        print(f"✓  current={c:.2f}" if c else "✓")

    # Yield Spread
    if "US10Y" in snapshot and "US02Y" in snapshot:
        y10 = snapshot["US10Y"].get("current")
        y3m = snapshot["US02Y"].get("current")
        if y10 is not None and y3m is not None:
            spread = round(y10 - y3m, 2)
            snapshot["YIELD_SPREAD"] = {
                "name": "10Y-3M Yield Spread",
                "current": spread,
                "signal": "NORMAL" if spread > 0 else "INVERTED",
            }
            print(f"    YIELD_SPREAD: {spread:.2f}bp → {'NORMAL' if spread > 0 else 'INVERTED'}")

    # VIX Term Structure
    vix_keys = ["VIX", "VIX9D", "VIX3M", "VIX6M"]
    vix_vals = {k: snapshot[k].get("current") for k in vix_keys if k in snapshot and snapshot[k].get("current") is not None}
    if "VIX" in vix_vals and "VIX3M" in vix_vals:
        vix      = vix_vals["VIX"]
        vix3m    = vix_vals["VIX3M"]
        vix9d    = vix_vals.get("VIX9D")
        vix6m    = vix_vals.get("VIX6M")

        # Primary ratio: VIX / VIX3M — >1 = backwardation (stress), <1 = contango (normal)
        ratio_3m = round(vix / vix3m, 4) if vix3m else None
        # Short-term tension: VIX9D / VIX — >1 = acute near-term fear spike
        ratio_9d = round(vix9d / vix, 4) if vix9d and vix else None

        # Build term curve array (ascending maturity)
        curve = []
        for k in ["VIX9D", "VIX", "VIX3M", "VIX6M"]:
            if k in vix_vals:
                curve.append({"tenor": k, "value": round(vix_vals[k], 2)})

        # Determine structure shape
        if ratio_3m is not None:
            if ratio_3m > 1.05:
                shape = "BACKWARDATION"      # Stress — short > long
                risk  = "RISK_OFF"
            elif ratio_3m > 0.95:
                shape = "FLAT"               # Transitional
                risk  = "NEUTRAL"
            else:
                shape = "CONTANGO"           # Normal — short < long
                risk  = "RISK_ON"
        else:
            shape = "UNKNOWN"
            risk  = "UNKNOWN"

        # Steepness: spread between shortest and longest available
        vals_ordered = [vix_vals.get(k) for k in ["VIX9D", "VIX", "VIX3M", "VIX6M"] if vix_vals.get(k) is not None]
        steepness = round(vals_ordered[-1] - vals_ordered[0], 2) if len(vals_ordered) >= 2 else None

        snapshot["VIX_TERM"] = {
            "name":         "VIX Term Structure",
            "curve":        curve,
            "ratio_3m":     ratio_3m,
            "ratio_9d":     ratio_9d,
            "shape":        shape,
            "risk_signal":  risk,
            "steepness":    steepness,
        }
        print(f"    VIX_TERM: VIX/VIX3M={ratio_3m} shape={shape} → {risk}" +
              (f"  VIX9D/VIX={ratio_9d}" if ratio_9d else ""))

    # ═══ FRED DATA (M2, NFCI) ═══
    if FRED_API_KEY:
        try:
            import requests as fred_req
            fred_base = "https://api.stlouisfed.org/fred/series/observations"

            # M2 Money Supply (WM2NS — weekly, seasonally adjusted, billions)
            print("    FRED M2...", end=" ", flush=True)
            m2_params = {
                "series_id": "WM2NS", "api_key": FRED_API_KEY,
                "file_type": "json", "sort_order": "desc", "limit": 13,
                "observation_start": (datetime.now() - timedelta(days=120)).strftime("%Y-%m-%d"),
            }
            m2_resp = fred_req.get(fred_base, params=m2_params, timeout=15)
            m2_obs = [o for o in m2_resp.json().get("observations", []) if o["value"] != "."]
            if len(m2_obs) >= 2:
                m2_current = float(m2_obs[0]["value"])
                m2_prev = float(m2_obs[-1]["value"])
                m2_chg = round((m2_current / m2_prev - 1) * 100, 2)
                m2_trend = "EXPANDING" if m2_chg > 0.2 else "CONTRACTING" if m2_chg < -0.2 else "FLAT"
                snapshot["M2"] = {
                    "name": "M2 Money Supply",
                    "current": round(m2_current, 1),
                    "change_3m_pct": m2_chg,
                    "trend": m2_trend,
                    "unit": "Mrd USD",
                    "date": m2_obs[0]["date"],
                }
                print(f"✓ ${m2_current:.0f}B ({m2_chg:+.2f}%) → {m2_trend}")
            else:
                print("⚠ zu wenig Datenpunkte")

            # NFCI — Chicago Fed National Financial Conditions Index
            print("    FRED NFCI...", end=" ", flush=True)
            nfci_params = {
                "series_id": "NFCI", "api_key": FRED_API_KEY,
                "file_type": "json", "sort_order": "desc", "limit": 5,
                "observation_start": (datetime.now() - timedelta(days=60)).strftime("%Y-%m-%d"),
            }
            nfci_resp = fred_req.get(fred_base, params=nfci_params, timeout=15)
            nfci_obs = [o for o in nfci_resp.json().get("observations", []) if o["value"] != "."]
            if nfci_obs:
                nfci_val = float(nfci_obs[0]["value"])
                nfci_signal = "LOOSE" if nfci_val < -0.2 else "TIGHT" if nfci_val > 0.2 else "NEUTRAL"
                snapshot["NFCI"] = {
                    "name": "Chicago Fed NFCI",
                    "current": round(nfci_val, 3),
                    "signal": nfci_signal,
                    "date": nfci_obs[0]["date"],
                }
                print(f"✓ {nfci_val:.3f} → {nfci_signal}")
            else:
                print("⚠ keine Daten")

        except Exception as e:
            print(f"    ⚠ FRED API Fehler: {e}")
    else:
        print("    FRED: kein API-Key — M2/NFCI übersprungen")

    # ═══ COMPOSITE LIQUIDITY SIGNAL ═══
    # Combines: VIX term structure + DXY trend + M2 trend + NFCI
    liq_score = 0  # Positive = risk-on, negative = risk-off
    liq_parts = []

    # VIX Term Structure
    vt = snapshot.get("VIX_TERM")
    if vt:
        if vt["risk_signal"] == "RISK_ON":
            liq_score += 1; liq_parts.append("VIX:Contango")
        elif vt["risk_signal"] == "RISK_OFF":
            liq_score -= 1; liq_parts.append("VIX:Backwardation")
        else:
            liq_parts.append("VIX:Flat")

    # DXY: Falling = liquidity easing, Rising = tightening
    dxy = snapshot.get("DXY")
    if dxy and dxy.get("chg_1W") is not None:
        if dxy["chg_1W"] < -0.5:
            liq_score += 1; liq_parts.append("DXY:Schwächer")
        elif dxy["chg_1W"] > 0.5:
            liq_score -= 1; liq_parts.append("DXY:Stärker")
        else:
            liq_parts.append("DXY:Stabil")

    # M2
    m2 = snapshot.get("M2")
    if m2:
        if m2.get("trend") == "EXPANDING":
            liq_score += 1; liq_parts.append("M2:Expanding")
        elif m2.get("trend") == "CONTRACTING":
            liq_score -= 1; liq_parts.append("M2:Contracting")
        else:
            liq_parts.append("M2:Flat")

    # NFCI
    nfci = snapshot.get("NFCI")
    if nfci:
        if nfci.get("signal") == "LOOSE":
            liq_score += 1; liq_parts.append("NFCI:Loose")
        elif nfci.get("signal") == "TIGHT":
            liq_score -= 1; liq_parts.append("NFCI:Tight")
        else:
            liq_parts.append("NFCI:Neutral")

    if liq_parts:
        if liq_score >= 2:
            liq_signal = "RISK_ON"
        elif liq_score <= -2:
            liq_signal = "RISK_OFF"
        else:
            liq_signal = "NEUTRAL"
        snapshot["LIQUIDITY"] = {
            "name": "Liquiditäts-Composite",
            "signal": liq_signal,
            "score": liq_score,
            "components": " · ".join(liq_parts),
        }
        print(f"    LIQUIDITY: Score={liq_score} → {liq_signal} ({', '.join(liq_parts)})")

    return snapshot


# ══════════════════════════════════════════════════════════
# TICKER ANALYSIS (enhanced)
# ══════════════════════════════════════════════════════════

def analyze_ticker(ticker, name=""):
    print(f"    {ticker}...", end=" ", flush=True)
    hist = get_historical(ticker, days=160)
    if not hist:
        print("⚠ keine Daten"); return None

    result = {"ticker": ticker, "name": name}
    for pname, n in PERIODS.items():
        result[f"perf_{pname}"]      = pct_change(hist, n)
        result[f"flow_{pname}"]      = cap_flow(flow_score_vs_prev(hist, n))
        result[f"flow_raw_{pname}"]  = flow_score_vs_prev(hist, n)
        result[f"flow_avg_{pname}"]  = cap_flow(flow_score_vs_avg(hist, n))
        result[f"dvol_{pname}"]      = dollar_volume_for_period(hist, n)
        result[f"obv_{pname}"]       = obv_trend(hist, n)
        result[f"ad_{pname}"]        = ad_trend(hist, n)

    try:
        result["price"]  = hist[-1]["close"]
        result["volume"] = hist[-1]["volume"]
    except:
        result["price"] = result["volume"] = None

    # Aggregated scores
    flows     = [result[f"flow_{p}"]     for p in PERIODS if result.get(f"flow_{p}") is not None]
    flows_avg = [result[f"flow_avg_{p}"] for p in PERIODS if result.get(f"flow_avg_{p}") is not None]
    obvs      = [result[f"obv_{p}"]      for p in PERIODS if result.get(f"obv_{p}") is not None]
    result["flow_score"]     = safe_mean(flows)
    result["flow_score_avg"] = safe_mean(flows_avg)
    result["obv_signal"]     = safe_mean(obvs)

    # Flow Direction
    obv_1w  = result.get("obv_1W")
    perf_1w = result.get("perf_1W")
    ad_1w   = result.get("ad_1W")
    if obv_1w is not None and perf_1w is not None:
        if   obv_1w > 0 and perf_1w > 0:  result["flow_direction"] = "ACCUMULATION"
        elif obv_1w < 0 and perf_1w < 0:  result["flow_direction"] = "DISTRIBUTION"
        elif obv_1w > 0 and perf_1w < 0:  result["flow_direction"] = "STEALTH_ACCUMULATION"
        elif obv_1w < 0 and perf_1w > 0:  result["flow_direction"] = "WEAK_RALLY"
        else:                              result["flow_direction"] = "NEUTRAL"
    else:
        result["flow_direction"] = "UNKNOWN"

    # Confidence: OBV + A/D Agreement
    result["confidence_1W"] = flow_confidence(obv_1w, ad_1w)
    result["confidence_1M"] = flow_confidence(result.get("obv_1M"), result.get("ad_1M"))

    p1w = result.get('perf_1W'); f1w = result.get('flow_1W'); obv = result.get('obv_1W')
    d = result.get('flow_direction', '')
    print(f"✓  perf={p1w}%  flow={f1w}%  obv={obv}  [{d}]")
    return result


# ══════════════════════════════════════════════════════════
# AGGREGATION (Median for themes, Mean for capital flows)
# ══════════════════════════════════════════════════════════

def aggregate_group(stock_results, use_median=True):
    agg_fn = safe_median if use_median else safe_mean
    entry = {}
    for period in PERIODS:
        flows     = [s[f"flow_{period}"]     for s in stock_results if s.get(f"flow_{period}") is not None]
        flows_avg = [s[f"flow_avg_{period}"] for s in stock_results if s.get(f"flow_avg_{period}") is not None]
        perfs     = [s[f"perf_{period}"]     for s in stock_results if s.get(f"perf_{period}") is not None]
        dvols     = [s[f"dvol_{period}"]     for s in stock_results if s.get(f"dvol_{period}") is not None]
        obvs      = [s[f"obv_{period}"]      for s in stock_results if s.get(f"obv_{period}") is not None]
        entry[f"avg_flow_{period}"]     = agg_fn(flows)
        entry[f"avg_flow_avg_{period}"] = agg_fn(flows_avg)
        entry[f"avg_perf_{period}"]     = agg_fn(perfs)
        entry[f"total_dvol_{period}"]   = sum(dvols) if dvols else None
        entry[f"avg_obv_{period}"]      = agg_fn(obvs)

    all_f     = [entry[f"avg_flow_{p}"]     for p in PERIODS if entry.get(f"avg_flow_{p}") is not None]
    all_f_avg = [entry[f"avg_flow_avg_{p}"] for p in PERIODS if entry.get(f"avg_flow_avg_{p}") is not None]
    entry["flow_score"]     = safe_mean(all_f)
    entry["flow_score_avg"] = safe_mean(all_f_avg)

    # Dominant direction
    from collections import Counter
    dirs = [s.get("flow_direction") for s in stock_results if s.get("flow_direction") and s["flow_direction"] != "UNKNOWN"]
    if dirs:
        entry["dominant_direction"] = Counter(dirs).most_common(1)[0][0]
    return entry


# ══════════════════════════════════════════════════════════
# HISTORY & DELTA TRACKING
# ══════════════════════════════════════════════════════════

HISTORY_FILE = "flow_history.json"
MAX_HISTORY_DAYS = 90  # Keep ~3 months of daily snapshots

def get_script_dir():
    return os.path.dirname(os.path.abspath(__file__))

def load_previous_run():
    """Load the previous flow_data.json for delta computation."""
    path = os.path.join(get_script_dir(), "flow_data.json")
    if not os.path.exists(path):
        return None
    try:
        with open(path) as f:
            return json.load(f)
    except:
        return None

def load_history():
    """Load flow_history.json — list of daily snapshots."""
    path = os.path.join(get_script_dir(), HISTORY_FILE)
    if not os.path.exists(path):
        return []
    try:
        with open(path) as f:
            return json.load(f)
    except:
        return []

def save_history(history):
    """Save history, trimmed to MAX_HISTORY_DAYS."""
    history = history[-MAX_HISTORY_DAYS:]
    path = os.path.join(get_script_dir(), HISTORY_FILE)
    with open(path, "w") as f:
        json.dump(clean_nan(history), f, indent=1)

def build_snapshot(sectors, themes, macro):
    """Build a compact daily snapshot for history storage."""
    today = datetime.now().strftime("%Y-%m-%d")
    snap = {"date": today, "sectors": {}, "themes": {}, "macro": {}}

    for s in sectors:
        snap["sectors"][s["name"]] = {
            "score": s.get("flow_score"),
            "score_avg": s.get("flow_score_avg"),
            "dir": s.get("flow_direction"),
            "perf_1W": s.get("perf_1W"),
            "obv_1W": s.get("obv_1W"),
            "flow_1W": s.get("flow_1W"),
        }

    for t in themes:
        snap["themes"][t["name"]] = {
            "score": t.get("flow_score"),
            "score_avg": t.get("flow_score_avg"),
            "dir": t.get("dominant_direction"),
        }

    for key in ["VIX", "YIELD_SPREAD"]:
        if key in macro:
            snap["macro"][key] = macro[key].get("current")

    return snap

def append_history(history, snapshot):
    """Append snapshot, replacing if same date already exists."""
    today = snapshot["date"]
    history = [h for h in history if h.get("date") != today]
    history.append(snapshot)
    history.sort(key=lambda h: h["date"])
    return history

def compute_deltas(current_sectors, previous_data):
    """Compare current run with previous: direction changes, score shifts."""
    if not previous_data or "sectors" not in previous_data:
        return None

    prev_by_name = {s["name"]: s for s in previous_data["sectors"]}
    deltas = {"direction_changes": [], "score_shifts": [], "new_signals": []}

    for s in current_sectors:
        name = s["name"]
        prev = prev_by_name.get(name)
        if not prev:
            continue

        cur_dir = s.get("flow_direction", "UNKNOWN")
        prev_dir = prev.get("flow_direction", "UNKNOWN")

        # Direction change
        if cur_dir != prev_dir and cur_dir != "UNKNOWN" and prev_dir != "UNKNOWN":
            is_warning = cur_dir in ("WEAK_RALLY", "DISTRIBUTION")
            deltas["direction_changes"].append({
                "sector": name,
                "from": prev_dir,
                "to": cur_dir,
                "warning": is_warning,
            })

        # Significant score shift (>10% absolute change)
        cur_score = s.get("flow_score_avg")
        prev_score = prev.get("flow_score_avg")
        if cur_score is not None and prev_score is not None:
            shift = cur_score - prev_score
            if abs(shift) > 10:
                deltas["score_shifts"].append({
                    "sector": name,
                    "prev_score": prev_score,
                    "cur_score": cur_score,
                    "shift": round(shift, 1),
                })

        # New warning signals (wasn't WEAK_RALLY/DISTRIBUTION before)
        if cur_dir in ("WEAK_RALLY", "STEALTH_ACCUMULATION") and prev_dir not in (cur_dir,):
            deltas["new_signals"].append({
                "sector": name,
                "signal": cur_dir,
                "prev_dir": prev_dir,
            })

    return deltas

def compute_streaks(history, current_sectors):
    """Compute how many consecutive days each sector held its current direction."""
    if not history:
        return {}

    streaks = {}
    for s in current_sectors:
        name = s["name"]
        cur_dir = s.get("flow_direction", "UNKNOWN")
        if cur_dir == "UNKNOWN":
            continue

        # Walk history backwards
        streak = 0
        for snap in reversed(history):
            sec_data = snap.get("sectors", {}).get(name)
            if not sec_data:
                break
            if sec_data.get("dir") == cur_dir:
                streak += 1
            else:
                break

        # Score trend: average of last 5 readings
        recent_scores = []
        for snap in history[-5:]:
            sec_data = snap.get("sectors", {}).get(name)
            if sec_data and sec_data.get("score_avg") is not None:
                recent_scores.append(sec_data["score_avg"])

        cur_score = s.get("flow_score_avg")
        score_trend = None
        if recent_scores and cur_score is not None:
            avg_recent = sum(recent_scores) / len(recent_scores)
            score_trend = round(cur_score - avg_recent, 1)

        streaks[name] = {
            "direction": cur_dir,
            "days": streak + 1,  # +1 for today
            "score_trend": score_trend,  # positive = improving, negative = fading
            "stable": streak >= 3,  # 3+ days = stable signal
        }

    return streaks


# ══════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════

def main():
    print(f"\n{'='*60}")
    print(f"  Flow Screener v3 — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"  OBV + Macro + Ratios + History + Deltas | Cap ±{FLOW_SCORE_CAP}%")
    print(f"{'='*60}")

    # ── Load previous run for deltas ──
    previous = load_previous_run()
    history = load_history()
    if previous:
        prev_date = previous.get("generated_at", "?")[:16]
        print(f"  Vorheriger Lauf: {prev_date}")
    else:
        print(f"  Kein vorheriger Lauf — Deltas übersprungen")
    print(f"  History: {len(history)} Tage gespeichert")

    output = {
        "generated_at": datetime.now().isoformat(),
        "version": 3,
        "periods": list(PERIODS.keys()),
        "flow_cap": FLOW_SCORE_CAP,
    }

    # ── Macro ──
    print("\n═══ MAKRO-REGIME ═══")
    output["macro"] = get_macro_snapshot()

    # ── Ratios ──
    print("\n═══ RATIO-SIGNALE ═══")
    ratios = {}
    for rname, cfg in RATIO_SIGNALS.items():
        print(f"  {rname} ({cfg['name']})...", end=" ", flush=True)
        r = analyze_ratio(cfg["numerator"], cfg["denominator"])
        if r:
            r["name"] = cfg["name"]
            r["interpretation"] = cfg["interpretation"]
            ratios[rname] = r
            print(f"✓  1W={r.get('ratio_chg_1W','—')}%  1M={r.get('ratio_chg_1M','—')}%")
        else:
            print("⚠")
    output["ratios"] = ratios

    # ── Sektoren ──
    print("\n═══ SEKTOR-FLOWS ═══")
    sectors = []
    for name, etf in SECTOR_ETFS.items():
        print(f"  {name} ({etf}):")
        r = analyze_ticker(etf, name=name)
        if r:
            r["name"] = name; r["etf"] = etf
            sectors.append(r)
    sectors.sort(key=lambda x: x.get("flow_score") or -999, reverse=True)
    output["sectors"] = sectors

    # ── SPY Benchmark ──
    print("\n═══ BENCHMARK ═══")
    print("  S&P 500 (SPY):")
    spy_data = analyze_ticker("SPY", name="S&P 500")
    if spy_data:
        spy_data["name"] = "S&P 500"
        spy_data["etf"] = "SPY"
        output["benchmark_spy"] = spy_data

    # ── Themen (Median) ──
    print("\n═══ THEMEN-FLOWS ═══")
    themes = []
    for tname, tickers in THEME_STOCKS.items():
        print(f"  {tname}:")
        stocks = [r for t in tickers if (r := analyze_ticker(t))]
        if not stocks: continue
        entry = {"name": tname, "stocks": stocks}
        entry.update(aggregate_group(stocks, use_median=True))
        themes.append(entry)
    themes.sort(key=lambda x: x.get("flow_score") or -999, reverse=True)
    output["themes"] = themes

    # ── Capital Flows (Mean) ──
    print("\n═══ KAPITALFLUSS-ZIELE ═══")
    capital = []
    for gname, tickers in CAPITAL_FLOWS.items():
        print(f"  {gname}:")
        stocks = [r for t, n in tickers.items() if (r := analyze_ticker(t, name=n))]
        if not stocks: continue
        entry = {"name": gname, "stocks": stocks}
        entry.update(aggregate_group(stocks, use_median=False))
        capital.append(entry)
    capital.sort(key=lambda x: x.get("flow_score_avg") or -999, reverse=True)
    output["capital_flows"] = capital

    # ── Shares Tracking ──
    print("\n═══ SHARES OUTSTANDING ═══")
    shares = {}
    for etf in SHARES_TRACKING_ETFS:
        print(f"  {etf}...", end=" ", flush=True)
        d = get_shares_outstanding_delta(etf)
        if d:
            shares[etf] = d
            print(f"✓  signal={d['signal']}")
        else:
            print("⚠")
    output["shares_tracking"] = shares

    # ── Deltas (vs. previous run) ──
    print("\n═══ DELTAS & HISTORY ═══")
    deltas = compute_deltas(sectors, previous)
    if deltas:
        output["deltas"] = deltas
        n_dir = len(deltas.get("direction_changes", []))
        n_score = len(deltas.get("score_shifts", []))
        n_new = len(deltas.get("new_signals", []))
        print(f"  {n_dir} Direction-Wechsel | {n_score} Score-Shifts | {n_new} neue Signale")
        for dc in deltas.get("direction_changes", []):
            warn = " ⚠️" if dc["warning"] else ""
            print(f"    {dc['sector']:20s}  {dc['from']} → {dc['to']}{warn}")
    else:
        output["deltas"] = None
        print("  Keine Deltas (erster Lauf)")

    # ── Streaks (from history) ──
    streaks = compute_streaks(history, sectors)
    output["streaks"] = streaks
    stable = [(n, s) for n, s in streaks.items() if s["stable"]]
    fading = [(n, s) for n, s in streaks.items() if s.get("score_trend") is not None and s["score_trend"] < -5]
    print(f"  {len(stable)} stabile Signale (3+ Tage) | {len(fading)} fading (Score-Trend < -5)")
    for name, st in sorted(streaks.items(), key=lambda x: -x[1]["days"]):
        trend = f"  trend:{st['score_trend']:+.1f}" if st['score_trend'] is not None else ""
        marker = " 📉" if st.get('score_trend') is not None and st['score_trend'] < -5 else ""
        print(f"    {name:20s}  [{st['direction']}] {st['days']}T{' ✓' if st['stable'] else ''}{trend}{marker}")

    # ── Append to history ──
    snapshot = build_snapshot(sectors, themes, output.get("macro", {}))
    history = append_history(history, snapshot)
    save_history(history)
    print(f"  History: {len(history)} Tage gespeichert → {HISTORY_FILE}")

    # ── Save flow_data.json ──
    out_path = os.path.join(get_script_dir(), "flow_data.json")
    with open(out_path, "w") as f:
        json.dump(clean_nan(output), f, indent=2)

    print(f"\n{'='*60}")
    print(f"  ✓ {out_path}")
    print(f"  {len(sectors)} Sektoren | {len(themes)} Themen | {len(capital)} Capital-Gruppen")
    print(f"  {len(ratios)} Ratios | {len(shares)} Shares-Tracking | {len(output['macro'])} Makro")

    print(f"\n  Top Sektoren:")
    for s in sectors[:5]:
        streak_info = streaks.get(s['name'], {})
        days = streak_info.get('days', '?')
        print(f"    {s['name']:20s}  Score:{s.get('flow_score') or 0:+.1f}%  OBV:{s.get('obv_1W') or 0:+.1f}  [{s.get('flow_direction','')}] {days}T")

    print(f"\n  Ratios:")
    for rn, rd in ratios.items():
        print(f"    {rn:15s}  1W:{rd.get('ratio_chg_1W',0) or 0:+.2f}%  → {rd['interpretation'][:55]}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
