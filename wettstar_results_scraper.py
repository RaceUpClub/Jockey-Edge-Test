#!/usr/bin/env python3
"""
Wettstar Historical Results Scraper
=====================================
Scrapt Rennergebnisse von wettstar-pferdewetten.de/race/{ID}

Output: CSV mit allen Startern + Ergebnissen + Quoten

Usage:
  python wettstar_results_scraper.py --race-id 2492829          # Einzeltest
  python wettstar_results_scraper.py --start-id 2400000 --end-id 2492829
  python wettstar_results_scraper.py --from-date 2024-01-01

Dependencies:
  pip install playwright beautifulsoup4
  playwright install chromium
  playwright install-deps chromium
"""

import asyncio
import re
import csv
import os
import sys
import json
import argparse
from datetime import datetime, timedelta
from pathlib import Path
from bs4 import BeautifulSoup

# ── CSV-Schema ────────────────────────────────────────────────────────────────
FIELDNAMES = [
    # Race-Level
    'race_id', 'race_date', 'venue', 'race_nr',
    'start_time', 'distance_m', 'prize_eur', 'surface',
    'race_class', 'age_restriction', 'field_size',
    # Starter
    'start_nr', 'box_nr', 'horse_name',
    'age', 'gender', 'weight_kg',
    'jockey', 'trainer',
    # Quoten
    'ml_quote',       # Morgenpost (aus Quotenverlauf-Tabelle)
    'sieg_toto',      # Toto Siegquote (Endquote)
    'fsieg_bm',       # Buchmacher F.Sieg
    'fplatz_bm',      # Buchmacher F.Platz
    'ev_quote',       # Ev.-Quote (Markt-Implied) ← Key für EV
    # Ergebnis
    'finish_position',
    'finish_distance',
    # Labels + Berechnungen
    'implied_prob',   # 1 / ev_quote
    'won',            # 1 wenn Sieger
    'placed',         # 1 wenn Top3
    # Pools
    'zweier_combo', 'zweier_quote',
    'dreier_combo', 'dreier_quote',
]


# ── Extraktion ────────────────────────────────────────────────────────────────

def parse_race_page(html: str, race_id: int) -> list[dict]:
    """Parst eine einzelne Race-HTML-Seite → Liste von Starter-Dicts."""
    soup = BeautifulSoup(html, 'html.parser')

    # Prüfe ob Ergebnis vorhanden (noch nicht gelaufene Rennen überspringen)
    if 'Ergebnis' not in html:
        return []

    meta    = extract_race_meta(soup)
    meta['race_id'] = race_id

    ev_data  = extract_ev_table(soup)      # horse_name → {finish_pos, ev_quote, finish_dist}
    pools    = extract_pools(soup)

    starters_raw = extract_starter_rows(soup)
    if not starters_raw:
        return []

    results = []
    for s in starters_raw:
        horse = s['horse_name']
        ev    = ev_data.get(horse, {})

        finish_pos = ev.get('finish_position', s.get('finish_position', ''))
        ev_q       = ev.get('ev_quote', '')

        row = {**meta, **pools}
        row.update({
            'start_nr':        s.get('start_nr', ''),
            'box_nr':          s.get('box_nr', ''),
            'horse_name':      horse,
            'age':             s.get('age', ''),
            'gender':          s.get('gender', ''),
            'weight_kg':       s.get('weight_kg', ''),
            'jockey':          s.get('jockey', ''),
            'trainer':         s.get('trainer', ''),
            'ml_quote':        s.get('ml_quote', ''),
            'sieg_toto':       s.get('sieg_toto', ''),
            'fsieg_bm':        s.get('fsieg_bm', ''),
            'fplatz_bm':       s.get('fplatz_bm', ''),
            'ev_quote':        ev_q,
            'finish_position': finish_pos,
            'finish_distance': ev.get('finish_distance', s.get('finish_distance', '')),
            'implied_prob':    round(1 / pf(ev_q), 4) if pf(ev_q) else '',
            'won':             1 if str(finish_pos) == '1' else 0,
            'placed':          1 if str(finish_pos) in ['1', '2', '3'] else 0,
        })
        results.append(row)

    return results


def extract_race_meta(soup) -> dict:
    meta = {}

    # Datum, Venue, Race-Nr aus Breadcrumb
    for label, css_cls in [('race_date', '-breadcrumb-date'),
                            ('venue',     '-breadcrumb-name'),
                            ('race_nr',   '-breadcrumb-race')]:
        el = soup.find(class_=lambda c: c and css_cls in c if c else False)
        meta[label] = el.get_text(strip=True) if el else ''

    # Datum normalisieren: "27.04.25" → "2025-04-27"
    if meta.get('race_date'):
        try:
            meta['race_date'] = datetime.strptime(
                meta['race_date'], '%d.%m.%y'
            ).strftime('%Y-%m-%d')
        except ValueError:
            try:
                meta['race_date'] = datetime.strptime(
                    meta['race_date'], '%d.%m.%Y'
                ).strftime('%Y-%m-%d')
            except ValueError:
                pass

    # Race-Nr: "R1" → 1
    if meta.get('race_nr'):
        rn = re.search(r'R(\d+)', meta['race_nr'])
        meta['race_nr'] = int(rn.group(1)) if rn else meta['race_nr']

    # Metadaten aus Infobox (li-Elemente mit class ttml__race__info__)
    text = soup.get_text(separator=' ')

    m = re.search(r'(\d{3,4})\s*m', text)
    meta['distance_m'] = int(m.group(1)) if m else ''

    m = re.search(r'Preisgeld\D{0,10}([\d.]+)\s*€', text)
    meta['prize_eur'] = int(m.group(1).replace('.', '')) if m else ''

    m = re.search(r'Starter\D{0,5}(\d+)', text)
    meta['field_size'] = int(m.group(1)) if m else ''

    m = re.search(r'(\d{2}:\d{2})\s*Uhr', text)
    meta['start_time'] = m.group(1) if m else ''

    m = re.search(r'Kategorie\s+([A-Z])', text)
    meta['race_class'] = m.group(1) if m else ''

    m = re.search(r'Alter:\s*(\d+)', text)
    meta['age_restriction'] = m.group(1) if m else ''

    meta['surface'] = 'Flach' if 'Flach' in text else ('Sand' if 'Sand' in text else '')

    return meta


def extract_starter_rows(soup) -> list[dict]:
    """Extrahiert alle Starter aus den race__grid__row--is-starter Divs."""
    rows = soup.find_all(class_=lambda c: c and '--rg-is-starter' in c if c else False)
    starters = []

    for row in rows:
        s = {}

        # Nr + Name + Box
        name_div = row.find(class_='race__grid__row__name')
        if not name_div:
            continue
        strongs = name_div.find_all('strong')
        spans   = name_div.find_all('span')
        s['start_nr']   = strongs[0].get_text(strip=True).rstrip('.') if strongs else ''
        s['horse_name'] = strongs[1].get_text(strip=True) if len(strongs) > 1 else ''
        box_m = re.search(r'\((\d+)\)', spans[0].get_text() if spans else '')
        s['box_nr'] = box_m.group(1) if box_m else ''

        # Alter + Geschlecht + Gewicht
        pills = row.find_all(class_='race__grid__row__vars__pills')
        age_gender = pills[0].get_text(strip=True) if pills else ''
        weight_str = pills[1].get_text(strip=True) if len(pills) > 1 else ''
        age_m = re.match(r'(\d+)j\.\s*([A-Z])', age_gender)
        s['age']    = int(age_m.group(1)) if age_m else ''
        s['gender'] = age_m.group(2) if age_m else ''
        wm = re.search(r'([\d.]+)\s*kg', weight_str)
        s['weight_kg'] = float(wm.group(1)) if wm else ''

        # Jockey + Trainer
        j = row.find(class_='race__grid__row__humans__jockey')
        t = row.find(class_='race__grid__row__humans__trainer')
        s['jockey']  = j.get_text(strip=True) if j else ''
        s['trainer'] = re.sub(r'^\(|\)$', '', t.get_text(strip=True)) if t else ''

        # Quoten
        s['sieg_toto'] = _get_odd(row, 'tote')
        s['fsieg_bm']  = _get_odd(row, 'fix')
        s['fplatz_bm'] = _get_odd(row, 'plcodd_fix')  # Korrektes CSS-Suffix

        # ML-Quote aus Quotenverlauf-Tabelle (erste Zelle der Quote-Zeile)
        trend_table = row.find('table', class_='trendTrendsTable')
        if trend_table:
            trows = trend_table.find_all('tr')
            quote_rows = [r for r in trows
                          if not r.find(class_='trendTrendsTable__row__divider')]
            if len(quote_rows) > 1:
                ml_td = quote_rows[1].find('td', class_='ml')
                s['ml_quote'] = pf(ml_td.get_text(strip=True)) if ml_td else ''
            else:
                s['ml_quote'] = ''
        else:
            s['ml_quote'] = ''

        # Finish-Position + Abstand
        finish_div = row.find(class_='race__grid__row__finish')
        if finish_div:
            strong = finish_div.find('strong')
            dist_div = finish_div.find(class_=lambda c: c and 'font-size' in c if c else False)
            finish_text = strong.get_text(strip=True) if strong else ''
            fp = re.search(r'(\d+)', finish_text)
            s['finish_position'] = int(fp.group(1)) if fp else ''
            s['finish_distance'] = dist_div.get_text(strip=True) if dist_div else ''
        else:
            s['finish_position'] = ''
            s['finish_distance'] = ''

        starters.append(s)

    return starters


def _get_odd(row, odd_type: str) -> float | str:
    d = row.find(class_=lambda c: c and f'type-{odd_type}' in c if c else False)
    if not d:
        return ''
    v = d.find(class_='c-runner-odd__value')
    if not v:
        return ''
    return pf(re.sub(r'[^\d,\.]', '', v.get_text(strip=True)))


def extract_ev_table(soup) -> dict:
    """Tabelle mit # | Nr | Pferd | Ev.-Quote | Jockey | Info → dict by horse_name."""
    tables = soup.find_all('table')
    ev_table = next((t for t in tables if 'Ev.-Quote' in t.get_text()), None)
    result = {}
    if not ev_table:
        return result
    for row in ev_table.find_all('tr')[1:]:
        cols = [c.get_text(strip=True) for c in row.find_all(['td', 'th'])]
        if len(cols) >= 4:
            horse = cols[2].strip()
            result[horse] = {
                'finish_position': int(cols[0]) if cols[0].isdigit() else cols[0],
                'ev_quote':        pf(cols[3]),
                'finish_distance': cols[5] if len(cols) > 5 else '',
            }
    return result


def extract_pools(soup) -> dict:
    """Zweier + Dreier Kombination und Quote."""
    tables = soup.find_all('table')
    pools  = {}
    for t in tables:
        text = t.get_text(separator=' ', strip=True)
        # Format: "7 - 3  9,50"
        m2 = re.match(r'^(\d+\s*-\s*\d+)\s+([\d,\.]+)$', text)
        if m2:
            pools['zweier_combo'] = m2.group(1).replace(' ', '')
            pools['zweier_quote'] = pf(m2.group(2))
            continue
        m3 = re.match(r'^(\d+\s*-\s*\d+\s*-\s*\d+)\s+([\d,\.]+)$', text)
        if m3:
            pools['dreier_combo'] = m3.group(1).replace(' ', '')
            pools['dreier_quote'] = pf(m3.group(2))
    return pools


def pf(s) -> float | str:
    try:
        return float(str(s).replace(',', '.').strip())
    except Exception:
        return ''


# ── Playwright-Scraping ───────────────────────────────────────────────────────

async def fetch_html(page, url: str) -> str:
    """Lädt eine Seite mit Playwright und gibt den gerenderten HTML zurück."""
    try:
        await page.goto(url, wait_until='networkidle', timeout=30000)
        await asyncio.sleep(1)  # Kurz warten für vollständiges Rendering
        return await page.content()
    except Exception as e:
        print(f'  ⚠️  {url}: {e}')
        return ''


async def get_race_ids_for_date(page, date_str: str) -> list[int]:
    """Scrapt Kalenderseite für ein Datum → Liste von Race-IDs."""
    url  = f'https://wettstar-pferdewetten.de/races/{date_str}'
    html = await fetch_html(page, url)
    if not html:
        return []
    ids = re.findall(r'/race/(\d{6,8})', html)
    return list(set(int(i) for i in ids))


# ── Main Loop ─────────────────────────────────────────────────────────────────

async def run(args):
    from playwright.async_api import async_playwright

    out_dir = Path(args.output)
    out_dir.mkdir(parents=True, exist_ok=True)

    # Checkpoint: bereits verarbeitete IDs nicht nochmal scrapen
    cp_file    = out_dir / 'scraped_ids.json'
    done_ids   = set()
    if cp_file.exists():
        with open(cp_file) as f:
            done_ids = set(json.load(f))
        print(f'📋 Checkpoint: {len(done_ids)} bereits verarbeitet')

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        ctx     = await browser.new_context(
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0'
        )
        page = await ctx.new_page()

        # ── Race-IDs bestimmen ────────────────────────────────────────────────
        if args.race_id:
            race_ids = [args.race_id]

        elif args.start_id:
            end_id   = args.end_id or args.start_id
            race_ids = list(range(args.start_id, end_id + 1))
            print(f'📋 Range: {args.start_id} → {end_id} ({len(race_ids)} IDs)')

        elif args.from_date:
            print(f'📅 Kalender: {args.from_date} → heute')
            race_ids = []
            current  = datetime.strptime(args.from_date, '%Y-%m-%d')
            today    = datetime.today()
            while current <= today:
                ds   = current.strftime('%Y-%m-%d')
                ids  = await get_race_ids_for_date(page, ds)
                if ids:
                    print(f'  {ds}: {len(ids)} Rennen')
                    race_ids.extend(ids)
                current += timedelta(days=1)
                await asyncio.sleep(0.3)
            print(f'✅ {len(race_ids)} Race-IDs gefunden')

        else:
            print('❌ Kein Modus. Nutze --race-id, --start-id, oder --from-date')
            sys.exit(1)

        # ── Scraping ──────────────────────────────────────────────────────────
        todo        = [rid for rid in sorted(set(race_ids)) if rid not in done_ids]
        all_results = []
        new_done    = []

        print(f'\n🏇 Scraping {len(todo)} Rennen...\n')

        for i, race_id in enumerate(todo, 1):
            try:
                url  = f'https://wettstar-pferdewetten.de/race/{race_id}'
                html = await fetch_html(page, url)

                if html:
                    rows = parse_race_page(html, race_id)
                    if rows:
                        all_results.extend(rows)
                        r0 = rows[0]
                        print(
                            f'  [{i:>4}/{len(todo)}] ✅ {race_id} | '
                            f'{r0.get("race_date","")} {r0.get("venue","")} '
                            f'R{r0.get("race_nr","")} | {len(rows)} Starter'
                        )
                    else:
                        print(f'  [{i:>4}/{len(todo)}] ⏭️  {race_id} | kein Ergebnis')

                new_done.append(race_id)

                # Checkpoint + CSV alle 100 Rennen
                if i % 100 == 0:
                    _save_cp(cp_file, done_ids | set(new_done))
                    _write_csv(all_results, out_dir, 'partial')
                    print(f'\n  💾 Checkpoint bei {i} Rennen\n')

                await asyncio.sleep(0.8)  # Rate-Limiting

            except KeyboardInterrupt:
                print('\n⚠️  Abgebrochen – speichere...')
                break
            except Exception as e:
                print(f'  [{i:>4}/{len(todo)}] ❌ {race_id}: {e}')
                new_done.append(race_id)

        await browser.close()

    # ── Final output ──────────────────────────────────────────────────────────
    _save_cp(cp_file, done_ids | set(new_done))

    if all_results:
        tag      = str(args.race_id or args.from_date or f'{args.start_id}-{args.end_id}')
        csv_path = _write_csv(all_results, out_dir, tag)
        print(f'\n📊 CSV: {len(all_results)} Starter-Ergebnisse → {csv_path}')
        _print_summary(all_results)
    else:
        print('\n⚠️  Keine Daten extrahiert.')


def _write_csv(results: list, out_dir: Path, tag: str) -> Path:
    path = out_dir / f'race_results_{tag}.csv'
    with open(path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(results)
    return path


def _save_cp(path: Path, ids: set):
    with open(path, 'w') as f:
        json.dump(sorted(ids), f)


def _print_summary(results: list):
    from collections import Counter
    venues = Counter(r.get('venue', '') for r in results)
    print(f'\n📈 Summary:')
    print(f'  Rennen: {len(set((r["race_id"],r["race_nr"]) for r in results))}')
    print(f'  Starter gesamt: {len(results)}')
    print(f'  Venues: {dict(venues.most_common(5))}')
    filled_ev = sum(1 for r in results if r.get('ev_quote'))
    print(f'  Ev.-Quote gefüllt: {filled_ev}/{len(results)} ({100*filled_ev//len(results)}%)')
    filled_fp = sum(1 for r in results if r.get('finish_position'))
    print(f'  Finish-Pos gefüllt: {filled_fp}/{len(results)} ({100*filled_fp//len(results)}%)')


# ── CLI ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description='Wettstar Results Scraper')
    group  = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--race-id',   type=int, help='Einzelnes Rennen (Test)')
    group.add_argument('--start-id',  type=int, help='Start-ID für Range')
    group.add_argument('--from-date', type=str, help='Ab Datum via Kalender (YYYY-MM-DD)')
    parser.add_argument('--end-id',   type=int, default=None)
    parser.add_argument('--output',   type=str, default='./race_results/')
    args = parser.parse_args()
    asyncio.run(run(args))


if __name__ == '__main__':
    main()
