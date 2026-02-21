#!/usr/bin/env python3
"""
Debug-Script: Speichert rohen HTML-Output von Race-Seite
→ Damit können wir die richtigen CSS-Selektoren identifizieren

Usage:
  python wettstar_debug.py --race-id 2492829
"""

import asyncio
import re
import sys
import argparse
from pathlib import Path


async def debug_race(race_id: int, output_dir: str = './debug_output/'):
    from playwright.async_api import async_playwright
    from bs4 import BeautifulSoup

    Path(output_dir).mkdir(parents=True, exist_ok=True)
    url = f'https://wettstar-pferdewetten.de/race/{race_id}'

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0'
        )
        page = await context.new_page()

        print(f"Lade {url} ...")
        await page.goto(url, wait_until='networkidle', timeout=30000)

        # Kurz warten damit JS vollständig rendert
        await asyncio.sleep(3)

        html = await page.content()

        # 1. Rohen HTML speichern
        html_path = f'{output_dir}/race_{race_id}.html'
        with open(html_path, 'w', encoding='utf-8') as f:
            f.write(html)
        print(f"✅ HTML gespeichert: {html_path} ({len(html)} chars)")

        # 2. Struktur-Analyse
        soup = BeautifulSoup(html, 'html.parser')

        print("\n=== STRUKTUR-ANALYSE ===")

        # Tables
        tables = soup.find_all('table')
        print(f"\nTables gefunden: {len(tables)}")
        for i, t in enumerate(tables):
            headers = [th.get_text(strip=True) for th in t.find_all('th')]
            rows    = len(t.find_all('tr'))
            print(f"  Table {i}: {rows} Zeilen, Headers: {headers[:6]}")

        # Alle CSS-Klassen die 'result', 'starter', 'race', 'horse' enthalten
        print("\nRelevante CSS-Klassen (Auswahl):")
        all_classes = set()
        for tag in soup.find_all(True):
            for cls in tag.get('class', []):
                if any(k in cls.lower() for k in ['result','starter','race','horse','finish',
                                                    'ev','quote','platz','sieg','jockey',
                                                    'pferd','row','item','card']):
                    all_classes.add(cls)
        for cls in sorted(all_classes)[:30]:
            print(f"  .{cls}")

        # Alle data-* Attribute
        print("\ndata-* Attribute (Auswahl):")
        data_attrs = set()
        for tag in soup.find_all(True):
            for attr in tag.attrs:
                if attr.startswith('data-'):
                    data_attrs.add(attr)
        for attr in sorted(data_attrs)[:20]:
            print(f"  {attr}")

        # Suche nach Schlüsselwörtern im HTML
        print("\nSchlüsselwörter im HTML:")
        keywords = ['Nightdance', 'Against All Odds', 'Ev.-Quote', 'Ev.Quote', 'ev_quote',
                    'ERGEBNIS', 'Ergebnis', 'finish', 'F.Sieg', 'F.Platz', 'Krefeld',
                    'evQuote', 'ev-quote', 'fsieg', 'fplatz', 'finishPosition']
        for kw in keywords:
            found = kw in html
            print(f"  {'✅' if found else '❌'} '{kw}'")

        # JSON-Daten im HTML? (React/Next.js injiziert oft __NEXT_DATA__ oder ähnliches)
        print("\nJSON-Blöcke im HTML:")
        json_scripts = soup.find_all('script', type=re.compile(r'application/json|__NEXT'))
        for s in json_scripts[:5]:
            preview = s.string[:200] if s.string else '(leer)'
            print(f"  Script: {preview}...")

        # window.__data__ oder ähnliche globale Variablen
        all_scripts = soup.find_all('script')
        print(f"\nScript-Tags gesamt: {len(all_scripts)}")
        for s in all_scripts:
            if s.string and any(k in s.string for k in ['race', 'horse', 'result', 'starter', 'ev']):
                preview = s.string[:300].replace('\n', ' ')
                print(f"  → {preview}...")
                break

        # Text-Extraktion Test
        print("\n=== TEXT-INHALT (erste 2000 Zeichen) ===")
        text = soup.get_text(separator=' ', strip=True)
        print(text[:2000])

        await browser.close()
        print(f"\n📁 Alle Debug-Dateien in: {output_dir}")
        print("→ Bitte race_{id}.html als Artefakt hochladen!")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--race-id', type=int, default=2492829)
    parser.add_argument('--output',  type=str, default='./debug_output/')
    args = parser.parse_args()
    asyncio.run(debug_race(args.race_id, args.output))


if __name__ == '__main__':
    main()
