#!/usr/bin/env python3
"""
Finviz Hot Stock Scraper
Liest Finviz Screener-Ergebnisse ohne Account/Subscription.
Kriterien: Rel. Volume > 2, Change > 3%, Avg Volume > 500k, Market Cap > 50M (Micro Cap+, nächste Stufe zu >100M in Finviz Free)
Sortiert nach relativem Volumen (stärkste Ausreißer zuerst).
"""

import requests
from bs4 import BeautifulSoup
import time

SCREENER_URL = (
    "https://finviz.com/screener.ashx"
    "?v=111"
    "&f=sh_avgvol_o500,sh_relvol_o2,ta_change_u3,sh_mcap_microover"
    "&o=-relativevolume"
)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Referer": "https://finviz.com/",
}

# Finviz Spalten in v=111 (Overview)
COLUMNS = [
    "No", "Ticker", "Company", "Sector", "Industry", "Country",
    "Market Cap", "P/E", "Price", "Change", "Volume", "Exchange"
]

def scrape_finviz_page(url):
    """Scrape eine Finviz-Seite und gib Zeilen zurück."""
    try:
        r = requests.get(url, headers=HEADERS, timeout=20)
        r.raise_for_status()
    except Exception as e:
        print(f"  ⚠ Fetch error: {e}")
        return []

    soup = BeautifulSoup(r.text, 'html.parser')

    # Finviz verwendet unterschiedliche Klassen je nach Version
    table = (
        soup.find('table', {'class': 'styled-table-new'}) or
        soup.find('table', id='screener-views-table') or
        soup.find('table', {'class': 'table-light'})
    )

    if not table:
        # Fallback: suche nach Ticker-Links
        tickers = []
        for a in soup.find_all('a', {'class': 'screener-link-primary'}):
            tickers.append(a.get_text(strip=True))
        if tickers:
            print(f"  Fallback: found {len(tickers)} tickers via links")
            return [{"Ticker": t} for t in tickers]
        print("  ⚠ No table found on page")
        return []

    rows = []
    all_rows = table.find_all('tr')

    # Header-Zeile finden
    header_row = None
    for row in all_rows:
        ths = row.find_all('th')
        if ths:
            header_row = [th.get_text(strip=True) for th in ths]
            break

    if not header_row:
        header_row = COLUMNS

    for row in all_rows:
        tds = row.find_all('td')
        if not tds or len(tds) < 5:
            continue
        cells = [td.get_text(strip=True) for td in tds]
        if cells[0].isdigit():  # Nur Datenzeilen (erste Spalte = Nummer)
            entry = {}
            for i, col in enumerate(header_row):
                if i < len(cells):
                    entry[col] = cells[i]
            rows.append(entry)

    return rows

def scrape_all_pages(max_pages=5):
    """Scrape bis zu max_pages Seiten (20 Ergebnisse pro Seite)."""
    all_results = []
    for page in range(max_pages):
        start = page * 20 + 1
        url = SCREENER_URL + f"&r={start}"
        print(f"  Scraping Seite {page+1} (ab #{start})...")
        rows = scrape_finviz_page(url)
        if not rows:
            break
        all_results.extend(rows)
        if len(rows) < 20:
            break  # Letzte Seite
        time.sleep(1.5)  # Finviz nicht überlasten

    return all_results

def parse_results(rows):
    """Normalisiere Finviz-Ergebnisse in einheitliches Format."""
    results = []
    for r in rows:
        ticker  = r.get('Ticker', '').strip()
        if not ticker:
            continue
        company = r.get('Company', '')
        sector  = r.get('Sector', '')
        change  = r.get('Change', '').replace('%','').replace('+','')
        price   = r.get('Price', '')
        volume  = r.get('Volume', '').replace(',','')

        try: change_f = float(change)
        except: change_f = None
        try: price_f = float(price)
        except: price_f = None
        try: volume_i = int(volume)
        except: volume_i = None

        country = r.get('Country', '')
        exchange = r.get('Exchange', '')

        results.append({
            "ticker":    ticker,
            "name":      company,
            "sector":    sector,
            "country":   country,
            "exchange":  exchange,
            "price":     price_f,
            "change_1d": change_f,
            "volume":    volume_i,
        })
    return results

def get_hot_stocks(max_pages=5):
    """Hauptfunktion: gibt Liste von Hot Stocks zurück."""
    print("Scraping Finviz Hot Stocks (RVOL>2, Change>3%, AvgVol>500k, MCap>50M / Micro Cap+)...")
    raw = scrape_all_pages(max_pages)
    if not raw:
        print("  ⚠ Keine Ergebnisse von Finviz")
        return []
    parsed = parse_results(raw)
    print(f"  ✓ {len(parsed)} Hot Stocks gefunden")
    return parsed

if __name__ == "__main__":
    stocks = get_hot_stocks()
    print(f"\nTop 10 Hot Stocks:")
    for s in stocks[:10]:
        print(f"  {s['ticker']:8s} {s['name'][:25]:25s} {s['sector']:20s} {s.get('change_1d') or 0:+.1f}%")
