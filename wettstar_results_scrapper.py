#!/usr/bin/env python3
"""
Wettstar Historical Results Scraper
=====================================
Scrapet Rennergebnisse von wettstar-pferdewetten.de/race/{ID}
für alle deutschen Galopp-Rennen.

Output CSV pro Renntag mit allen Startern + Ergebnissen.

Usage:
  # Einzel-Rennen testen:
  python wettstar_results_scraper.py --race-id 2492829

  # Range scrapen:
  python wettstar_results_scraper.py --start-id 2300000 --end-id 2492829

  # Ab bestimmtem Datum (ID-Discovery via Kalender):
  python wettstar_results_scraper.py --from-date 2024-01-01

Dependencies:
  pip install playwright beautifulsoup4 requests
  playwright install chromium
"""

import asyncio
import re
import csv
import os
import sys
import json
import time
import argparse
from datetime import datetime, timedelta
from pathlib import Path

# ── CSV-Schema ────────────────────────────────────────────────────────────────
FIELDNAMES = [
    # Race-Level
    'race_id', 'race_date', 'venue', 'race_nr', 'race_name',
    'start_time', 'distance_m', 'prize_eur', 'surface',
    'race_class', 'age_restriction', 'field_size',
    # Starter-Level
    'start_nr', 'box_nr', 'horse_name',
    'age', 'gender', 'weight_kg',
    'jockey', 'trainer',
    # Quoten
    'ev_quote',          # Ev.-Quote = Markt-Implied (key für EV)
    'sieg_toto',         # Toto Siegquote
    'fsieg_bm',          # Buchmacher F.Sieg
    'fplatz_bm',         # Buchmacher F.Platz
    # Ergebnis
    'finish_position',   # 1 = Sieg, 2 = Platz, etc.
    'finish_distance',   # Abstand zum Sieger (Längen)
    # Abgeleitete EV-Features
    'implied_prob',      # 1 / ev_quote (wenn vorhanden)
    'won',               # 1 wenn finish_position == 1
    'placed',            # 1 wenn finish_position <= 3
    # Pool-Ergebnisse (Race-Level, wiederholt pro Starter)
    'zweier_combo', 'zweier_quote',
    'dreier_combo', 'dreier_quote',
]


# ── Playwright-basierter Scraper ──────────────────────────────────────────────

async def scrape_race(page, race_id: int) -> list[dict]:
    """Scrapt eine einzelne Race-Seite, gibt Liste von Starter-Dicts zurück."""
    url = f'https://wettstar-pferdewetten.de/race/{race_id}'
    
    try:
        await page.goto(url, wait_until='networkidle', timeout=30000)
    except Exception as e:
        print(f'  ⚠️  Timeout/Error race {race_id}: {e}')
        return []

    # Prüfen ob Ergebnis vorhanden (kein Ergebnis = noch nicht gelaufen oder 404)
    content = await page.content()
    if 'ERGEBNIS' not in content and 'Ergebnis' not in content:
        return []
    # Nur deutsche Rennen
    if 'Flachrennen' not in content and 'Galopprennen' not in content:
        # Trotzdem versuchen – nicht alle haben explizit "Flachrennen"
        pass

    from bs4 import BeautifulSoup
    soup = BeautifulSoup(content, 'html.parser')

    # ── Race-Metadaten ────────────────────────────────────────────────────────
    meta = extract_race_meta(soup, race_id)
    if not meta:
        return []

    # ── Ev.-Quote Tabelle (obere Tabelle: #, Nr, Pferd, Ev.-Quote, Jockey, Info) ──
    ev_data = extract_ev_table(soup)      # dict: horse_name → {ev_quote, finish_pos, distance}

    # ── Starter-Detail Tabelle (untere Tabelle: Sieg, F.Sieg, F.Platz, Platz) ──
    starter_data = extract_starter_table(soup)   # dict: start_nr → {jockey, trainer, weight, ...}

    # ── Pools ─────────────────────────────────────────────────────────────────
    pools = extract_pools(soup)

    # ── Merge ─────────────────────────────────────────────────────────────────
    starters = []
    for start_nr, sd in starter_data.items():
        horse = sd.get('horse_name', '')
        ev    = ev_data.get(horse, {})

        finish_pos = ev.get('finish_position', sd.get('finish_position', ''))
        ev_quote   = ev.get('ev_quote', '')

        row = {**meta}
        row.update({
            'start_nr':        start_nr,
            'box_nr':          sd.get('box_nr', ''),
            'horse_name':      horse,
            'age':             sd.get('age', ''),
            'gender':          sd.get('gender', ''),
            'weight_kg':       sd.get('weight_kg', ''),
            'jockey':          sd.get('jockey', ''),
            'trainer':         sd.get('trainer', ''),
            'ev_quote':        ev_quote,
            'sieg_toto':       sd.get('sieg_toto', ''),
            'fsieg_bm':        sd.get('fsieg_bm', ''),
            'fplatz_bm':       sd.get('fplatz_bm', ''),
            'finish_position': finish_pos,
            'finish_distance': ev.get('finish_distance', ''),
            'implied_prob':    round(1/float(ev_quote), 4) if ev_quote else '',
            'won':             1 if str(finish_pos) == '1' else 0,
            'placed':          1 if str(finish_pos) in ['1','2','3'] else 0,
            'zweier_combo':    pools.get('zweier_combo', ''),
            'zweier_quote':    pools.get('zweier_quote', ''),
            'dreier_combo':    pools.get('dreier_combo', ''),
            'dreier_quote':    pools.get('dreier_quote', ''),
        })
        starters.append(row)

    return starters


def extract_race_meta(soup, race_id):
    """Extrahiert Race-Header-Daten."""
    meta = {'race_id': race_id}
    
    # Datum aus Breadcrumb: "27.04.25" oder Header
    date_m = re.search(r'(\d{2}\.\d{2}\.\d{2,4})', soup.get_text())
    if date_m:
        raw = date_m.group(1)
        try:
            if len(raw) == 8:  # 27.04.25 → 2025
                meta['race_date'] = datetime.strptime(raw, '%d.%m.%y').strftime('%Y-%m-%d')
            else:
                meta['race_date'] = datetime.strptime(raw, '%d.%m.%Y').strftime('%Y-%m-%d')
        except:
            meta['race_date'] = raw

    # Venue aus Breadcrumb/Header
    # Suche nach bekannten deutschen Rennbahnen
    VENUES = ['Krefeld','Dortmund','Hamburg','München','Köln','Baden-Baden','Hannover',
              'Berlin','Frankfurt','Düsseldorf','Hoppegarten','Dresden','Saarbrücken',
              'Mülheim','Magdeburg','Halle','Bad Harzburg','Neuss','Mannheim',
              'Straubing','Regensburg','Bremen','Karlshorst']
    text = soup.get_text()
    for v in VENUES:
        if v in text:
            meta['venue'] = v
            break

    # Race Nr: R1, R2, ...
    rn_m = re.search(r'R(\d+)', soup.get_text())
    meta['race_nr'] = int(rn_m.group(1)) if rn_m else ''

    # Details aus Infobox
    full_text = soup.get_text()
    
    dist_m = re.search(r'(\d{3,4})\s*m', full_text)
    meta['distance_m'] = int(dist_m.group(1)) if dist_m else ''

    prize_m = re.search(r'([\d.]+)\s*€', full_text)
    meta['prize_eur'] = int(prize_m.group(1).replace('.','')) if prize_m else ''

    meta['surface'] = 'Flach' if 'Flach' in full_text else ('Sand' if 'Sand' in full_text else '')

    # Klasse und Altersangabe
    class_m = re.search(r'Klasse\s+Kategorie\s+([A-Z])', full_text)
    meta['race_class'] = class_m.group(1) if class_m else ''

    age_m = re.search(r'Alter:\s*(\d+)', full_text)
    meta['age_restriction'] = age_m.group(1) if age_m else ''

    # Starter-Anzahl
    starter_m = re.search(r'Starter\s+(\d+)', full_text)
    meta['field_size'] = int(starter_m.group(1)) if starter_m else ''

    # Race-Name
    name_m = re.search(r'Rennen\s+(?:des|der|vom|von)\s+(.+?)(?:\n|,|\|)', full_text)
    meta['race_name'] = name_m.group(1).strip() if name_m else ''

    # Start-Zeit
    time_m = re.search(r'(\d{2}:\d{2})\s*Uhr', full_text)
    meta['start_time'] = time_m.group(1) if time_m else ''

    return meta


def extract_ev_table(soup):
    """
    Obere Ergebnis-Tabelle: # | Nr | Pferd | Ev.-Quote | Jockey | Info (Längen)
    Returns: dict {horse_name: {finish_position, ev_quote, finish_distance}}
    """
    result = {}
    
    # Suche Tabelle mit "Ev.-Quote" Header
    tables = soup.find_all('table')
    ev_table = None
    for t in tables:
        if 'Ev.-Quote' in t.get_text() or 'Ev.Quote' in t.get_text():
            ev_table = t
            break

    # Fallback: suche nach spezifischen CSS-Klassen oder div-Strukturen
    if not ev_table:
        # Moderne React-Apps nutzen oft keine <table>, sondern divs
        divs = soup.find_all('div', class_=re.compile(r'row|result|starter|race', re.I))
        # Hier müssen wir nach dem Pattern suchen: Nr | Name | Quote | Jockey | Info
        pass

    if ev_table:
        rows = ev_table.find_all('tr')
        for row in rows[1:]:  # Skip header
            cols = [c.get_text(strip=True) for c in row.find_all(['td','th'])]
            if len(cols) >= 5:
                try:
                    finish_pos  = cols[0].replace('#','').strip()
                    # start_nr  = cols[1]
                    horse_name  = cols[2].strip()
                    ev_quote_str= cols[3].replace(',','.')
                    jockey      = cols[4].strip()
                    distance    = cols[5].strip() if len(cols) > 5 else ''
                    
                    result[horse_name] = {
                        'finish_position': int(finish_pos) if finish_pos.isdigit() else finish_pos,
                        'ev_quote':        float(ev_quote_str) if ev_quote_str else '',
                        'finish_distance': distance,
                    }
                except (ValueError, IndexError):
                    continue

    return result


def extract_starter_table(soup):
    """
    Untere Detail-Tabelle: Nr Name(Box) | Gewicht/Alter | Jockey(Trainer) | Sieg | F.Sieg | F.Platz | Platz
    Returns: dict {start_nr: {horse_name, box_nr, age, gender, weight_kg, jockey, trainer, ...}}
    """
    result = {}
    
    tables = soup.find_all('table')
    detail_table = None
    for t in tables:
        th_text = t.get_text()
        if 'F.Sieg' in th_text or 'F.Platz' in th_text:
            detail_table = t
            break

    if detail_table:
        rows = detail_table.find_all('tr')
        for row in rows[1:]:
            cols = [c.get_text(strip=True) for c in row.find_all(['td','th'])]
            if len(cols) < 5:
                continue
            try:
                # Format: "1. Cuban Lynx (7) (SB)"
                name_col = cols[0]
                nr_m     = re.match(r'^(\d+)\.\s+(.+?)(?:\s+\((\d+)\))?(?:\s+\(SB\))?$', name_col)
                start_nr = int(nr_m.group(1)) if nr_m else ''
                horse    = nr_m.group(2).strip() if nr_m else name_col
                box_nr   = int(nr_m.group(3)) if (nr_m and nr_m.group(3)) else ''

                # Format: "3j. W. | 58.0 kg" oder "3j. S. | 56.5 kg"
                age_col  = cols[1] if len(cols) > 1 else ''
                age_m    = re.search(r'(\d+)j\.\s+([A-Z])', age_col)
                weight_m = re.search(r'([\d.]+)\s*kg', age_col)
                age      = int(age_m.group(1)) if age_m else ''
                gender   = age_m.group(2) if age_m else ''
                weight   = float(weight_m.group(1)) if weight_m else ''

                # Format: "Jockey Name (Trainer Name)"
                jt_col   = cols[2] if len(cols) > 2 else ''
                jt_m     = re.match(r'^(.+?)\s*\((.+?)\)$', jt_col)
                jockey   = jt_m.group(1).strip() if jt_m else jt_col
                trainer  = jt_m.group(2).strip() if jt_m else ''

                # Quoten (können leer sein wenn kein Ergebnis)
                sieg_toto = pf(cols[3]) if len(cols) > 3 else ''
                fsieg_bm  = pf(cols[4]) if len(cols) > 4 else ''
                fplatz_bm = pf(cols[5]) if len(cols) > 5 else ''

                # Finish-Position aus letzter Spalte
                platz_col = cols[-1]
                finish_m  = re.search(r'#\s*(\d+)', platz_col)
                finish_pos= int(finish_m.group(1)) if finish_m else ''

                result[start_nr] = {
                    'horse_name':      horse,
                    'box_nr':          box_nr,
                    'age':             age,
                    'gender':          gender,
                    'weight_kg':       weight,
                    'jockey':          jockey,
                    'trainer':         trainer,
                    'sieg_toto':       sieg_toto,
                    'fsieg_bm':        fsieg_bm,
                    'fplatz_bm':       fplatz_bm,
                    'finish_position': finish_pos,
                }
            except (ValueError, IndexError, AttributeError):
                continue

    return result


def extract_pools(soup):
    """Zweier und Dreier Combo + Quote."""
    pools = {}
    text  = soup.get_text()

    zwei_m  = re.search(r'(\d+\s*-\s*\d+)\s*\n.*?([\d,]+)', text)
    drei_m  = re.search(r'(\d+\s*-\s*\d+\s*-\s*\d+)\s*\n.*?([\d,]+)', text)

    if zwei_m:
        pools['zweier_combo'] = zwei_m.group(1).replace(' ','')
        pools['zweier_quote'] = pf(zwei_m.group(2))
    if drei_m:
        pools['dreier_combo'] = drei_m.group(1).replace(' ','')
        pools['dreier_quote'] = pf(drei_m.group(2))

    return pools


def pf(s):
    try: return float(str(s).replace(',','.').strip())
    except: return ''


# ── ID-Discovery ──────────────────────────────────────────────────────────────

async def find_start_id_for_date(page, target_date: str, known_id: int = 2492829) -> int:
    """
    Binary Search: findet erste Race-ID für target_date.
    known_id = 2492829 entspricht 27.04.2025
    
    Deutsche Rennen: ~4-6 Rennen/Tag, ~80-100 Renntage/Jahr
    → ~500 Rennen/Jahr → 2024-Start-ID ≈ 2492829 - 500 ≈ 2492300
    Aber IDs sind nicht lückenlos (int'l Rennen dazwischen)
    → besser: Kalender-Seite scrapen
    """
    target = datetime.strptime(target_date, '%Y-%m-%d')
    
    # Grobe Schätzung: ~10 IDs pro Tag (DE + Int'l)
    days_back = (datetime(2025, 4, 27) - target).days
    estimated_start = known_id - (days_back * 10)
    
    print(f"  Geschätzte Start-ID für {target_date}: ~{estimated_start}")
    return max(estimated_start, 1000000)


async def scrape_race_calendar(page, from_date: str, to_date: str = None) -> list[int]:
    """
    Scrapt Kalender-Seite um alle deutschen Race-IDs zu finden.
    URL-Muster: wettstar-pferdewetten.de/races/{date}
    """
    if not to_date:
        to_date = datetime.today().strftime('%Y-%m-%d')
    
    race_ids = []
    current  = datetime.strptime(from_date, '%Y-%m-%d')
    end      = datetime.strptime(to_date, '%Y-%m-%d')

    while current <= end:
        date_str = current.strftime('%Y-%m-%d')
        url = f'https://wettstar-pferdewetten.de/races/{date_str}'
        
        try:
            await page.goto(url, wait_until='networkidle', timeout=20000)
            content = await page.content()
            
            # Race-Links extrahieren: /race/XXXXXXX
            ids = re.findall(r'/race/(\d{6,8})', content)
            german_ids = []
            
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(content, 'html.parser')
            
            # Filter: nur deutsche Meetings (DE oder bekannte Venues)
            # Wettstar zeigt auch österreichische und schweizer Rennen
            links = soup.find_all('a', href=re.compile(r'/race/\d+'))
            for link in links:
                href = link.get('href', '')
                rid  = re.search(r'/race/(\d+)', href)
                if not rid:
                    continue
                # Prüfe ob Kontext "DE" oder deutsche Venue enthält
                parent_text = link.parent.get_text() if link.parent else ''
                if any(v in parent_text for v in ['DE', 'Deutschland', 'Dortmund', 'Hamburg', 
                        'Köln', 'München', 'Berlin', 'Hannover', 'Krefeld', 'Mülheim',
                        'Düsseldorf', 'Hoppegarten', 'Baden-Baden', 'Dresden']):
                    german_ids.append(int(rid.group(1)))
                elif not parent_text.strip():
                    # Ohne Kontext: alle aufnehmen, später filtern
                    german_ids.append(int(rid.group(1)))
            
            if german_ids:
                print(f"  {date_str}: {len(german_ids)} Rennen → IDs {min(german_ids)}-{max(german_ids)}")
                race_ids.extend(german_ids)
            
        except Exception as e:
            print(f"  ⚠️  {date_str}: {e}")
        
        current += timedelta(days=1)
        await asyncio.sleep(0.5)  # Rate-Limiting

    return list(set(race_ids))


# ── Main Scraping Loop ────────────────────────────────────────────────────────

async def run_scraper(args):
    from playwright.async_api import async_playwright

    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Checkpoint-File: bereits gescrapte IDs nicht nochmal bearbeiten
    checkpoint_file = output_dir / 'scraped_ids.json'
    scraped_ids = set()
    if checkpoint_file.exists():
        with open(checkpoint_file) as f:
            scraped_ids = set(json.load(f))
        print(f"📋 Checkpoint: {len(scraped_ids)} bereits gescrapte Rennen")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0'
        )
        page = await context.new_page()

        # ── Modus 1: Einzelnes Rennen testen ──
        if args.race_id:
            race_ids = [args.race_id]

        # ── Modus 2: ID-Range ──
        elif args.start_id and args.end_id:
            race_ids = list(range(args.start_id, args.end_id + 1))
            print(f"📋 Range: {args.start_id} → {args.end_id} ({len(race_ids)} IDs)")

        # ── Modus 3: Datum-Range (via Kalender) ──
        elif args.from_date:
            print(f"📅 Kalender scrapen: {args.from_date} → heute")
            race_ids = await scrape_race_calendar(page, args.from_date)
            print(f"✅ {len(race_ids)} Race-IDs gefunden")
        else:
            print("❌ Kein Modus angegeben. Nutze --race-id, --start-id/--end-id, oder --from-date")
            sys.exit(1)

        # ── Scraping ──────────────────────────────────────────────────────────
        all_results = []
        new_scraped = []
        errors      = []

        todo = [rid for rid in sorted(race_ids) if rid not in scraped_ids]
        print(f"\n🏇 Scraping {len(todo)} Rennen (überspringe {len(race_ids)-len(todo)} bekannte)...")

        for i, race_id in enumerate(todo, 1):
            try:
                starters = await scrape_race(page, race_id)
                if starters:
                    all_results.extend(starters)
                    new_scraped.append(race_id)
                    venue = starters[0].get('venue','?')
                    date  = starters[0].get('race_date','?')
                    rnr   = starters[0].get('race_nr','?')
                    print(f"  [{i}/{len(todo)}] ✅ {race_id}: {date} {venue} R{rnr} ({len(starters)} Starter)")
                else:
                    print(f"  [{i}/{len(todo)}] ⏭️  {race_id}: kein Ergebnis / nicht DE")
                    new_scraped.append(race_id)  # Auch leere als "bearbeitet" markieren

                # Checkpoint alle 50 Rennen
                if i % 50 == 0:
                    _save_checkpoint(checkpoint_file, scraped_ids | set(new_scraped))
                    _write_csv(all_results, output_dir, 'partial')
                    print(f"  💾 Checkpoint gespeichert ({i} Rennen)")

                # Rate-Limiting: 1-2s zwischen Requests
                await asyncio.sleep(1.0)

            except KeyboardInterrupt:
                print("\n⚠️  Abgebrochen. Speichere...")
                break
            except Exception as e:
                errors.append((race_id, str(e)))
                print(f"  [{i}/{len(todo)}] ❌ {race_id}: {e}")

        await browser.close()

        # ── Output ────────────────────────────────────────────────────────────
        if all_results:
            date_tag = args.from_date or args.race_id or f"{args.start_id}-{args.end_id}"
            csv_path = _write_csv(all_results, output_dir, date_tag)
            print(f"\n📊 CSV: {len(all_results)} Starter-Ergebnisse → {csv_path}")

        _save_checkpoint(checkpoint_file, scraped_ids | set(new_scraped))

        if errors:
            print(f"\n⚠️  {len(errors)} Fehler:")
            for rid, err in errors[:10]:
                print(f"  {rid}: {err}")

        print(f"\n🎉 Fertig: {len(new_scraped)} Rennen bearbeitet, {len(all_results)} Ergebnisse")


def _write_csv(results, output_dir, tag):
    path = output_dir / f'race_results_{tag}.csv'
    with open(path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(results)
    return path


def _save_checkpoint(path, ids):
    with open(path, 'w') as f:
        json.dump(list(ids), f)


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description='Wettstar Historical Results Scraper')
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--race-id',   type=int, help='Einzelnes Rennen scrapen (Test)')
    group.add_argument('--start-id',  type=int, help='Start-ID für Range-Scraping')
    group.add_argument('--from-date', type=str, help='Ab Datum (YYYY-MM-DD), via Kalender')
    parser.add_argument('--end-id',   type=int, default=2500000, help='End-ID für Range-Scraping')
    parser.add_argument('--output',   type=str, default='./race_results/', help='Output-Verzeichnis')
    args = parser.parse_args()

    asyncio.run(run_scraper(args))


if __name__ == '__main__':
    main()
