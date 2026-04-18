#!/usr/bin/env python3
"""
Runner — führt alle Screener-Scripts in der richtigen Reihenfolge aus.
Wird von GitHub Actions und optional lokal per Cron aufgerufen.
"""
import subprocess
import sys
import os

os.chdir(os.path.dirname(os.path.abspath(__file__)))

SCRIPTS = [
    ("flow_screener.py",  "Flow Screener + Macro Regime"),
    ("alert_scanner.py",  "Alert Scanner"),
]

failed = []
for script, label in SCRIPTS:
    print(f"\n{'='*60}")
    print(f"  ▶ {label} ({script})")
    print(f"{'='*60}\n")
    result = subprocess.run([sys.executable, script], capture_output=False)
    if result.returncode != 0:
        print(f"\n  ⚠ {script} beendet mit Code {result.returncode}")
        failed.append(script)
    else:
        print(f"\n  ✓ {script} erfolgreich")

print(f"\n{'='*60}")
if failed:
    print(f"  ⚠ Fehler in: {', '.join(failed)}")
    sys.exit(1)
else:
    print("  ✓ Alle Scripts erfolgreich durchgelaufen")
    sys.exit(0)
