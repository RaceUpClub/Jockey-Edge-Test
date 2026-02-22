#!/usr/bin/env python3
"""
Debug: Kalenderseiten-Struktur analysieren (2024 vs 2025)
Usage: python wettstar_debug_calendar.py --date 2024-06-15
"""
import asyncio, re, argparse
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
        await asyncio.sleep(3)
        html = await page.content()

        out = f'{output_dir}/calendar_{date_str}.html'
        with open(out, 'w', encoding='utf-8') as f:
            f.write(html)
        print(f'HTML: {len(html)} chars → {out}')

        soup = BeautifulSoup(html, 'html.parser')

        # Deutschland-Meetings
        print('\n=== DEUTSCHLAND-MEETINGS ===')
        for cb in soup.find_all(class_='ttml__country'):
            name_el = cb.find(class_='ttml__country__name')
            if not name_el or 'Deutschland' not in name_el.get_text():
                continue
            meetings = cb.find_all(class_=re.compile(r'meeting-id--\d+'))
            print(f'  DE-Meetings gefunden: {len(meetings)}')
            for m in meetings:
                cls_str = str(m)
                ven_el  = m.find(class_='ttml__meeting__title--subject')
                venue   = ven_el.get_text(strip=True) if ven_el else '?'
                mid     = re.search(r'meeting-id--(\d+)', ' '.join(m.get('class',[])))
                # Alle Icon-Klassen zeigen
                icons   = re.findall(r'icon--r-\w+|icon__\w+|fixcourse|pmu', cls_str)
                print(f'    ID={mid.group(1) if mid else "?":<8} Venue="{venue}"')
                print(f'    Icons: {icons}')
                print(f'    "fixcourse" im HTML: {"JA" if "fixcourse" in cls_str else "NEIN"}')
                print(f'    "icon--r-gallop": {"JA" if "icon--r-gallop" in cls_str else "NEIN"}')
                print(f'    "icon--r-trot":   {"JA" if "icon--r-trot" in cls_str else "NEIN"}')
                print()

        # Alle ttml__meeting__icon Klassen auf der ganzen Seite
        print('\n=== ALLE ttml__meeting__icon KLASSEN (unique) ===')
        icon_classes = set()
        for el in soup.find_all(class_=re.compile(r'ttml__meeting__icon')):
            for c in el.get('class', []):
                icon_classes.add(c)
        for c in sorted(icon_classes):
            print(f'  {c}')

        await browser.close()


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--date',   default='2024-06-15')
    p.add_argument('--output', default='./debug_output/')
    args = p.parse_args()
    asyncio.run(debug_calendar(args.date, args.output))


if __name__ == '__main__':
    main()
