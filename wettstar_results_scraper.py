#!/usr/bin/env python3
"""
Wettstar Historical Results Scraper
=====================================
Scrapt deutsche Galopp-Ergebnisse von wettstar-pferdewetten.de

Usage:
  python wettstar_results_scraper.py --race-id 2492829
  python wettstar_results_scraper.py --from-date 2024-01-01 --to-date 2024-12-31
  python wettstar_results_scraper.py --from-date 2025-01-01

Dependencies:
  pip install playwright beautifulsoup4
  playwright install chromium && playwright install-deps chromium
"""

import asyncio, re, csv, os, sys, json, argparse
from datetime import datetime, timedelta
from pathlib import Path
from bs4 import BeautifulSoup

# Deutsche Galopp-Rennbahnen (Filter für nicht-DE Rennen)
DE_VENUES = {
    'Dortmund','Hamburg','Köln','München','Berlin','Hannover','Krefeld',
    'Baden-Baden','Hoppegarten','Dresden','Saarbrücken','Mülheim','Magdeburg',
    'Halle','Neuss','Mannheim','Düsseldorf','Bad Harzburg','Straubing',
    'Regensburg','Bremen','Karlshorst','Frankfurt','Gelsenkirchen',
    'Dusseldorf','Koeln','Muenchen'
}

FIELDNAMES = [
    'race_id','race_date','venue','race_nr','race_name',
    'start_time','distance_m','prize_eur','surface','race_class',
    'age_restriction','field_size',
    'start_nr','box_nr','horse_name','age','gender','weight_kg',
    'jockey','trainer',
    'ml_quote','sieg_toto','fsieg_bm','fplatz_bm','ev_quote',
    'finish_position','finish_distance',
    'implied_prob','won','placed',
    'zweier_combo','zweier_quote','dreier_combo','dreier_quote',
]


# ── Parser ────────────────────────────────────────────────────────────────────

def parse_race_page(html: str, race_id: int) -> list[dict]:
    if 'Ergebnis' not in html:
        return []
    soup = BeautifulSoup(html, 'html.parser')

    meta = extract_race_meta(soup)
    meta['race_id'] = race_id

    # Nur deutsche Galopp-Rennen
    if meta.get('venue') not in DE_VENUES:
        return []

    ev_data  = extract_ev_table(soup)
    pools    = extract_pools(soup)
    starters = extract_starter_rows(soup)
    if not starters:
        return []

    results = []
    for s in starters:
        horse = s['horse_name']
        ev    = ev_data.get(horse, {})
        fp    = ev.get('finish_position', s.get('finish_position', ''))
        ev_q  = ev.get('ev_quote', '')

        row = {**meta, **pools}
        row.update({
            'start_nr':        s.get('start_nr',''),
            'box_nr':          s.get('box_nr',''),
            'horse_name':      horse,
            'age':             s.get('age',''),
            'gender':          s.get('gender',''),
            'weight_kg':       s.get('weight_kg',''),
            'jockey':          s.get('jockey',''),
            'trainer':         s.get('trainer',''),
            'ml_quote':        s.get('ml_quote',''),
            'sieg_toto':       s.get('sieg_toto',''),
            'fsieg_bm':        s.get('fsieg_bm',''),
            'fplatz_bm':       s.get('fplatz_bm',''),
            'ev_quote':        ev_q,
            'finish_position': fp,
            'finish_distance': ev.get('finish_distance', s.get('finish_distance','')),
            'implied_prob':    round(1/pf(ev_q), 4) if pf(ev_q) else '',
            'won':             1 if str(fp)=='1' else 0,
            'placed':          1 if str(fp) in ['1','2','3'] else 0,
        })
        results.append(row)
    return results


def extract_race_meta(soup) -> dict:
    meta = {}
    for label, cls in [('race_date','-breadcrumb-date'),
                        ('venue',    '-breadcrumb-name'),
                        ('race_nr',  '-breadcrumb-race')]:
        el = soup.find(class_=lambda c: c and cls in c if c else False)
        meta[label] = el.get_text(strip=True) if el else ''

    # Datum normalisieren
    if meta.get('race_date'):
        for fmt in ('%d.%m.%y', '%d.%m.%Y'):
            try:
                meta['race_date'] = datetime.strptime(meta['race_date'], fmt).strftime('%Y-%m-%d')
                break
            except ValueError:
                pass

    # Race-Nr: "R1" → 1
    rn = re.search(r'R(\d+)', meta.get('race_nr',''))
    meta['race_nr'] = int(rn.group(1)) if rn else ''

    text = soup.get_text(separator=' ')

    for pattern, key, cast in [
        (r'(\d{3,4})\s*m',            'distance_m',      int),
        (r'Preisgeld\D{0,10}([\d.]+)\s*€', 'prize_eur', lambda x: int(x.replace('.','')))  ,
        (r'Starter\D{0,5}(\d+)',       'field_size',      int),
        (r'(\d{2}:\d{2})\s*Uhr',       'start_time',      str),
        (r'Kategorie\s+([A-Z])',        'race_class',      str),
        (r'Alter:\s*(\d+)',             'age_restriction', str),
    ]:
        m = re.search(pattern, text)
        try:
            meta[key] = cast(m.group(1)) if m else ''
        except Exception:
            meta[key] = ''

    meta['surface'] = 'Flach' if 'Flach' in text else ('Sand' if 'Sand' in text else '')

    # Renntitel
    name_m = re.search(r'Rennen\s+(?:des|der|vom|von)\s+(.+?)(?:\n|,|\|)', text)
    meta['race_name'] = name_m.group(1).strip() if name_m else ''

    return meta


def extract_starter_rows(soup) -> list[dict]:
    rows = soup.find_all(class_=lambda c: c and '--rg-is-starter' in c if c else False)
    result = []
    for row in rows:
        s = {}
        name_div = row.find(class_='race__grid__row__name')
        if not name_div: continue
        strongs = name_div.find_all('strong')
        spans   = name_div.find_all('span')
        s['start_nr']   = strongs[0].get_text(strip=True).rstrip('.') if strongs else ''
        s['horse_name'] = strongs[1].get_text(strip=True) if len(strongs)>1 else ''
        bm = re.search(r'\((\d+)\)', spans[0].get_text() if spans else '')
        s['box_nr'] = bm.group(1) if bm else ''

        pills = row.find_all(class_='race__grid__row__vars__pills')
        ag    = pills[0].get_text(strip=True) if pills else ''
        wt    = pills[1].get_text(strip=True) if len(pills)>1 else ''
        am    = re.match(r'(\d+)j\.\s*([A-Z])', ag)
        s['age']    = int(am.group(1)) if am else ''
        s['gender'] = am.group(2) if am else ''
        wm = re.search(r'([\d.]+)\s*kg', wt)
        s['weight_kg'] = float(wm.group(1)) if wm else ''

        j = row.find(class_='race__grid__row__humans__jockey')
        t = row.find(class_='race__grid__row__humans__trainer')
        s['jockey']  = j.get_text(strip=True) if j else ''
        s['trainer'] = re.sub(r'^\(|\)$','', t.get_text(strip=True)) if t else ''

        s['sieg_toto'] = _odd(row, 'tote')
        s['fsieg_bm']  = _odd(row, 'fix')
        s['fplatz_bm'] = _odd(row, 'plcodd_fix')

        # ML aus Quotenverlauf
        tt = row.find('table', class_='trendTrendsTable')
        s['ml_quote'] = ''
        if tt:
            trows = [r for r in tt.find_all('tr')
                     if not r.find(class_='trendTrendsTable__row__divider')]
            if len(trows) > 1:
                ml_td = trows[1].find('td', class_='ml')
                s['ml_quote'] = pf(ml_td.get_text(strip=True)) if ml_td else ''

        # Finish
        fd = row.find(class_='race__grid__row__finish')
        if fd:
            strong = fd.find('strong')
            dist   = fd.find(class_=lambda c: c and 'font-size' in c if c else False)
            fp     = re.search(r'(\d+)', strong.get_text(strip=True) if strong else '')
            s['finish_position'] = int(fp.group(1)) if fp else ''
            s['finish_distance'] = dist.get_text(strip=True) if dist else ''
        else:
            s['finish_position'] = s['finish_distance'] = ''

        result.append(s)
    return result


def _odd(row, t):
    d = row.find(class_=lambda c: c and f'type-{t}' in c if c else False)
    if not d: return ''
    v = d.find(class_='c-runner-odd__value')
    return pf(re.sub(r'[^\d,\.]','', v.get_text(strip=True))) if v else ''


def extract_ev_table(soup) -> dict:
    t = next((t for t in soup.find_all('table') if 'Ev.-Quote' in t.get_text()), None)
    if not t: return {}
    result = {}
    for row in t.find_all('tr')[1:]:
        cols = [c.get_text(strip=True) for c in row.find_all(['td','th'])]
        if len(cols) >= 4:
            result[cols[2]] = {
                'finish_position': int(cols[0]) if cols[0].isdigit() else cols[0],
                'ev_quote':        pf(cols[3]),
                'finish_distance': cols[5] if len(cols)>5 else '',
            }
    return result


def extract_pools(soup) -> dict:
    pools = {}
    for t in soup.find_all('table'):
        text = t.get_text(separator=' ', strip=True)
        m2 = re.match(r'^(\d+\s*-\s*\d+)\s+([\d,\.]+)$', text)
        if m2:
            pools['zweier_combo'] = m2.group(1).replace(' ','')
            pools['zweier_quote'] = pf(m2.group(2))
        m3 = re.match(r'^(\d+\s*-\s*\d+\s*-\s*\d+)\s+([\d,\.]+)$', text)
        if m3:
            pools['dreier_combo'] = m3.group(1).replace(' ','')
            pools['dreier_quote'] = pf(m3.group(2))
    return pools


def pf(s):
    try: return float(str(s).replace(',','.').strip())
    except: return ''


# ── Playwright ────────────────────────────────────────────────────────────────

async def fetch(page, url: str) -> str:
    try:
        await page.goto(url, wait_until='networkidle', timeout=30000)
        await asyncio.sleep(0.5)
        return await page.content()
    except Exception as e:
        print(f'  ⚠️  {url}: {e}')
        return ''


async def get_race_ids_for_date(page, date_str: str) -> list[int]:
    html = await fetch(page, f'https://wettstar-pferdewetten.de/races/{date_str}')
    if not html: return []
    soup = BeautifulSoup(html, 'html.parser')
    ids  = []
    for a in soup.find_all('a', href=re.compile(r'/race/\d+')):
        m = re.search(r'/race/(\d+)', a.get('href',''))
        if m:
            # Nur deutsche Rennen: Prüfe ob Venue im Kontext steht
            parent_text = ''
            for p in [a.parent, a.parent.parent if a.parent else None]:
                if p: parent_text += p.get_text()
            # Wenn "DE" im Link-Kontext oder keine AT/CH Kennzeichnung
            if not any(x in parent_text for x in ['AUT','SUI','CHE','FRA','ENG','IRE','HUN']):
                ids.append(int(m.group(1)))
    return list(set(ids))


# ── Main ──────────────────────────────────────────────────────────────────────

async def run(args):
    from playwright.async_api import async_playwright

    out_dir = Path(args.output)
    out_dir.mkdir(parents=True, exist_ok=True)

    cp_file  = out_dir / 'scraped_ids.json'
    done_ids = set()
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

        elif args.from_date:
            to_date  = args.to_date or datetime.today().strftime('%Y-%m-%d')
            print(f'📅 Kalender scannen: {args.from_date} → {to_date}')
            race_ids = []
            current  = datetime.strptime(args.from_date, '%Y-%m-%d')
            end      = datetime.strptime(to_date, '%Y-%m-%d')
            total_days = (end - current).days + 1
            day_count  = 0

            while current <= end:
                ds  = current.strftime('%Y-%m-%d')
                ids = await get_race_ids_for_date(page, ds)
                if ids:
                    print(f'  {ds}: {len(ids)} Rennen gefunden')
                    race_ids.extend(ids)
                day_count += 1
                if day_count % 30 == 0:
                    print(f'  ... {day_count}/{total_days} Tage gescannt, {len(race_ids)} IDs bisher')
                current += timedelta(days=1)
                await asyncio.sleep(0.3)

            race_ids = sorted(set(race_ids))
            print(f'✅ {len(race_ids)} Race-IDs gefunden\n')

        else:
            print('❌ Nutze --race-id oder --from-date')
            sys.exit(1)

        # ── Scraping ──────────────────────────────────────────────────────────
        todo        = [rid for rid in race_ids if rid not in done_ids]
        all_results = []
        new_done    = []
        skipped_de  = 0

        print(f'🏇 Scraping {len(todo)} Rennen ({len(done_ids)} bereits bekannt)...\n')

        for i, race_id in enumerate(todo, 1):
            try:
                html = await fetch(page, f'https://wettstar-pferdewetten.de/race/{race_id}')
                rows = parse_race_page(html, race_id) if html else []

                if rows:
                    all_results.extend(rows)
                    r0 = rows[0]
                    print(f'  [{i:>4}/{len(todo)}] ✅ {race_id} | '
                          f'{r0.get("race_date","")} {r0.get("venue","")} '
                          f'R{r0.get("race_nr","")} | {len(rows)} Starter')
                elif html and 'Ergebnis' in html:
                    skipped_de += 1
                    # Kein DE-Rennen – still checkpoint
                else:
                    pass  # Kein Ergebnis vorhanden

                new_done.append(race_id)

                # Checkpoint + CSV alle 200 Rennen
                if i % 200 == 0:
                    _save_cp(cp_file, done_ids | set(new_done))
                    _write_csv(all_results, out_dir, f'{args.from_date or args.race_id}_partial')
                    print(f'\n  💾 Checkpoint: {i}/{len(todo)} | {len(all_results)} Starter gespeichert\n')

                await asyncio.sleep(0.8)

            except KeyboardInterrupt:
                print('\n⚠️  Abgebrochen – speichere...')
                break
            except Exception as e:
                print(f'  [{i:>4}/{len(todo)}] ❌ {race_id}: {e}')
                new_done.append(race_id)

        await browser.close()

    # ── Output ────────────────────────────────────────────────────────────────
    _save_cp(cp_file, done_ids | set(new_done))

    if all_results:
        tag      = args.from_date or str(args.race_id)
        if args.to_date: tag += f'_to_{args.to_date}'
        csv_path = _write_csv(all_results, out_dir, tag)
        print(f'\n📊 CSV: {len(all_results)} Starter → {csv_path}')
        _summary(all_results, skipped_de)
    else:
        print('\n⚠️  Keine deutschen Galopp-Ergebnisse gefunden.')


def _write_csv(results, out_dir, tag):
    path = out_dir / f'race_results_{tag}.csv'
    with open(path, 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=FIELDNAMES, extrasaction='ignore')
        w.writeheader()
        w.writerows(results)
    return path


def _save_cp(path, ids):
    with open(path, 'w') as f:
        json.dump(sorted(ids), f)


def _summary(results, skipped):
    from collections import Counter
    venues  = Counter(r.get('venue','') for r in results)
    races   = len(set((r['race_id'], r['race_nr']) for r in results))
    ev_ok   = sum(1 for r in results if r.get('ev_quote'))
    fin_ok  = sum(1 for r in results if r.get('finish_position'))
    winners = sum(1 for r in results if r.get('won')==1)
    print(f'\n📈 Summary:')
    print(f'  Rennen:            {races}')
    print(f'  Starter gesamt:    {len(results)}')
    print(f'  Nicht-DE gefiltert:{skipped}')
    print(f'  Winners:           {winners} ({100*winners//len(results)}% Siegrate)')
    print(f'  Ev-Quote gefüllt:  {ev_ok}/{len(results)} ({100*ev_ok//len(results)}%)')
    print(f'  Finish gefüllt:    {fin_ok}/{len(results)} ({100*fin_ok//len(results)}%)')
    print(f'\n  Top Venues:')
    for v, n in venues.most_common(8):
        print(f'    {v:<20}: {n} Starter')


def main():
    p = argparse.ArgumentParser()
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument('--race-id',   type=int)
    g.add_argument('--from-date', type=str, help='YYYY-MM-DD')
    p.add_argument('--to-date',   type=str, default=None, help='YYYY-MM-DD (optional)')
    p.add_argument('--output',    type=str, default='./race_results/')
    args = p.parse_args()
    asyncio.run(run(args))


if __name__ == '__main__':
    main()
