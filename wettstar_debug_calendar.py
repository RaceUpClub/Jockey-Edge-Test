#!/usr/bin/env python3
"""
Debug: Kalenderseiten-Struktur analysieren
Usage: python wettstar_debug_calendar.py
"""
import asyncio, re, json, argparse
from bs4 import BeautifulSoup
from pathlib import Path


async def debug_calendar(date_str: str, output_dir: str = './debug_output/'):
    from playwright.async_api import async_playwright

    Path(output_dir).mkdir(parents=True, exist_ok=True)
    url = f'https://wettstar-pferdewetten.de/races/{date_str}'

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page    = await (await browser.new_context(
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0'
        )).new_page()

        print(f'Lade {url} ...')
        await page.goto(url, wait_until='networkidle', timeout=30000)
        await asyncio.sleep(2)
        html = await page.content()

        # HTML speichern
        out = f'{output_dir}/calendar_{date_str}.html'
        with open(out, 'w', encoding='utf-8') as f:
            f.write(html)
        print(f'✅ HTML gespeichert: {out} ({len(html)} chars)')

        soup = BeautifulSoup(html, 'html.parser')

        # Alle /race/ Links
        print('\n=== ALLE /race/ LINKS ===')
        all_race_links = []
        for a in soup.find_all('a', href=re.compile(r'/race/\d+')):
            href = a.get('href', '')
            text = a.get_text(strip=True)[:60]
            parent_text = a.parent.get_text(strip=True)[:80] if a.parent else ''
            all_race_links.append((href, text, parent_text))
            print(f'  href={href:<30} text="{text}" parent="{parent_text}"')

        print(f'\nTotal /race/ Links: {len(all_race_links)}')
        ids = list(set(re.findall(r'/race/(\d+)', html)))
        print(f'Unique Race-IDs: {len(ids)} → {sorted(ids)[:20]}')

        # CSS-Klassen auf der Kalenderseite
        print('\n=== KALENDER-SPEZIFISCHE KLASSEN ===')
        all_cls = set(
            c for tag in soup.find_all(True)
            for c in tag.get('class', [])
            if any(k in c.lower() for k in ['race','meet','event','card','day','list','item','calendar'])
        )
        for c in sorted(all_cls)[:30]:
            print(f'  .{c}')

        # Seitentext (erste 1500 Zeichen)
        print('\n=== SEITEN-TEXT (erste 1500 Zeichen) ===')
        print(soup.get_text(separator=' ', strip=True)[:1500])

        await browser.close()
        print(f'\n📁 Debug gespeichert: {output_dir}')


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--date',   default='2025-04-27', help='Datum mit bekannten Rennen (YYYY-MM-DD)')
    p.add_argument('--output', default='./debug_output/')
    args = p.parse_args()
    asyncio.run(debug_calendar(args.date, args.output))


if __name__ == '__main__':
    main()
