#!/usr/bin/env python3
"""
Debug: Meeting-Seite analysieren
Usage: python wettstar_debug_meeting.py --date 2025-04-27 --meeting-id 274472
"""
import asyncio, re, argparse
from bs4 import BeautifulSoup
from pathlib import Path


async def debug_meeting(date_str: str, meeting_id: int, output_dir: str = './debug_output/'):
    from playwright.async_api import async_playwright

    Path(output_dir).mkdir(parents=True, exist_ok=True)
    url = f'https://wettstar-pferdewetten.de/races/{date_str}?meeting={meeting_id}'

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page    = await (await browser.new_context(
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0'
        )).new_page()

        print(f'Lade {url} ...')
        await page.goto(url, wait_until='networkidle', timeout=30000)
        await asyncio.sleep(3)
        html = await page.content()

        out = f'{output_dir}/meeting_{meeting_id}.html'
        with open(out, 'w', encoding='utf-8') as f:
            f.write(html)
        print(f'✅ HTML gespeichert: {out} ({len(html)} chars)')

        soup = BeautifulSoup(html, 'html.parser')

        # 1. Alle /race/ Links
        print('\n=== ALLE /race/ LINKS ===')
        for a in soup.find_all('a', href=re.compile(r'/race/\d+')):
            print(f"  href='{a.get('href','')}' cls='{a.get('class',[])}' text='{a.get_text(strip=True)[:40]}'")

        # 2. Race-IDs im HTML (alle Methoden)
        all_race_ids = re.findall(r'["\'/]race[/\-_](\d{6,8})', html)
        print(f'\n=== RACE-IDs IM HTML (alle Patterns) ===')
        print(f'  Unique: {sorted(set(all_race_ids))}')

        # 3. R1, R2 Tabs/Buttons
        print('\n=== RACE-TABS/BUTTONS ===')
        for el in soup.find_all(class_=re.compile(r'race.*tab|tab.*race|race.*btn|r\d+.*tab', re.I)):
            print(f"  tag={el.name} cls={el.get('class',[])} text='{el.get_text(strip=True)[:40]}'")

        # 4. Data-Attribute
        print('\n=== DATA-* ATTRIBUTE MIT RACE-IDs ===')
        for tag in soup.find_all(True):
            for attr, val in tag.attrs.items():
                if isinstance(val, str) and re.search(r'\d{6,8}', val) and 'race' in attr.lower():
                    print(f"  <{tag.name} {attr}='{val}'>")

        # 5. JSON/Script mit Race-Daten
        print('\n=== SCRIPTS MIT RACE-DATEN ===')
        for s in soup.find_all('script'):
            content = s.string or ''
            if 'race' in content.lower() and re.search(r'\d{6,8}', content):
                ids_in_script = re.findall(r'["\']?race[_\-]?id["\']?\s*[:=]\s*["\']?(\d{6,8})', content, re.I)
                if ids_in_script:
                    print(f'  Script race IDs: {ids_in_script[:10]}')
                    print(f'  Preview: {content[:300].replace(chr(10)," ")}')

        # 6. Alle R1-R9 Selektoren
        print('\n=== R1-R9 ELEMENTE ===')
        for tag in soup.find_all(class_=re.compile(r'\bR[1-9]\b')):
            print(f"  {tag.name} cls={tag.get('class',[])} href='{tag.get('href','')}' text='{tag.get_text(strip=True)[:30]}'")

        # 7. Seitentext
        print('\n=== SEITENTEXT (erste 3000 Zeichen) ===')
        print(soup.get_text(separator=' | ', strip=True)[:3000])

        await browser.close()


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--date',       default='2025-04-27')
    p.add_argument('--meeting-id', type=int, default=274472)
    p.add_argument('--output',     default='./debug_output/')
    args = p.parse_args()
    asyncio.run(debug_meeting(args.date, args.meeting_id, args.output))


if __name__ == '__main__':
    main()
