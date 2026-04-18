#!/usr/bin/env python3
"""
ISM Regime Patch — liest ism_data.json, berechnet Makro-Regime, patcht flow_data.json.

Nutzung:
  1. ism_data.json neben flow_data.json ablegen
  2. Nach jedem ISM-Release (1. Geschäftstag/Monat, 16:00 CET):
     Neue Zeile in ism_data.json eintragen, z.B.:
     "2026-04": { "new_orders": 54.2, "prices_paid": 75.1 }
  3. python ism_regime_patch.py
  4. Dashboard refreshen — Makro-Regime Tab zeigt die neuen Daten

Kann auch am Ende von flow_screener.py importiert werden:
  from ism_regime_patch import patch_flow_data
  patch_flow_data()
"""

import json
import os

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
ISM_PATH = os.path.join(SCRIPT_DIR, "ism_data.json")
FLOW_PATH = os.path.join(SCRIPT_DIR, "flow_data.json")


def classify_regime(no, pp):
    """Klassifiziert basierend auf New Orders + Prices Paid."""
    if no > 52 and pp < 58:
        return "GOLDILOCKS"
    elif no > 52 and pp >= 58:
        return "INFLATIONARY_GROWTH"
    elif no <= 52 and pp >= 58:
        return "STAGFLATION"
    elif no <= 52 and pp < 50:
        return "DEFLATION"
    else:
        return "TRANSITION"


def compute_regime():
    """Liest ISM-Daten, berechnet Regime + Trend."""
    if not os.path.exists(ISM_PATH):
        print(f"⚠ {ISM_PATH} nicht gefunden")
        return None

    with open(ISM_PATH, "r") as f:
        ism = json.load(f)

    data = ism.get("data", {})
    if not data:
        return None

    sorted_months = sorted(data.keys())
    history = []
    for m in sorted_months:
        d = data[m]
        no = d["new_orders"]
        pp = d["prices_paid"]
        regime = classify_regime(no, pp)
        spread = round(no - pp, 1)
        history.append({
            "month": m,
            "new_orders": no,
            "prices_paid": pp,
            "spread": spread,
            "regime": regime,
        })

    current = history[-1]

    # Trend über letzte 3 Monate
    recent = history[-3:] if len(history) >= 3 else history
    no_trend = round(recent[-1]["new_orders"] - recent[0]["new_orders"], 1)
    pp_trend = round(recent[-1]["prices_paid"] - recent[0]["prices_paid"], 1)
    spread_trend = round(recent[-1]["spread"] - recent[0]["spread"], 1)

    # Regime-Stabilität
    recent_regimes = [h["regime"] for h in recent]
    regime_consistency = recent_regimes.count(current["regime"]) / len(recent_regimes)

    if regime_consistency >= 1.0:
        confidence = "HOCH"
    elif regime_consistency >= 0.67:
        confidence = "MITTEL"
    else:
        confidence = "NIEDRIG"

    # Frühwarnsignale
    warnings = []
    if current["regime"] == "INFLATIONARY_GROWTH" and no_trend < -2:
        warnings.append("New Orders fallend bei Inflationary Growth — Stagflations-Risiko steigt")
    if current["regime"] == "GOLDILOCKS" and pp_trend > 5:
        warnings.append("Prices Paid beschleunigt — Übergang zu Inflationary Growth möglich")
    if current["regime"] == "INFLATIONARY_GROWTH" and current["prices_paid"] > 75:
        warnings.append(f"Prices Paid extrem hoch ({current['prices_paid']}) — historisch nicht nachhaltig")
    if current["spread"] < -20:
        warnings.append(f"NO-PP Spread bei {current['spread']} — starke Kosten-Schere")

    return {
        "current_regime": current["regime"],
        "current_month": current["month"],
        "new_orders": current["new_orders"],
        "prices_paid": current["prices_paid"],
        "spread": current["spread"],
        "trend_3m": {
            "new_orders_delta": no_trend,
            "prices_paid_delta": pp_trend,
            "spread_delta": spread_trend,
        },
        "confidence": confidence,
        "regime_consistency": round(regime_consistency, 2),
        "warnings": warnings,
        "history": history[-12:],
        "next_release": ism.get("next_release", "unbekannt"),
        "last_updated": ism.get("last_updated", "unbekannt"),
    }


def patch_flow_data():
    """Fügt ISM-Regime in flow_data.json ein."""
    regime = compute_regime()
    if not regime:
        return

    if not os.path.exists(FLOW_PATH):
        print(f"⚠ {FLOW_PATH} nicht gefunden — erstelle neue Datei")
        output = {"ism_regime": regime}
    else:
        with open(FLOW_PATH, "r") as f:
            output = json.load(f)
        output["ism_regime"] = regime

    with open(FLOW_PATH, "w") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    r = regime
    print(f"\n{'═' * 50}")
    print(f"  ISM REGIME: {r['current_regime']}")
    print(f"  Monat: {r['current_month']}  |  Konfidenz: {r['confidence']}")
    print(f"  New Orders: {r['new_orders']}  |  Prices Paid: {r['prices_paid']}")
    print(f"  Spread: {r['spread']:+.1f}  |  3M-Trend: {r['trend_3m']['spread_delta']:+.1f}")
    if r["warnings"]:
        print(f"  ⚠ {' | '.join(r['warnings'])}")
    print(f"  Nächstes Release: {r['next_release']}")
    print(f"{'═' * 50}\n")


if __name__ == "__main__":
    patch_flow_data()
