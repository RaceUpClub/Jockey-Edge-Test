#!/usr/bin/env python3
"""
Wettstar Historical Results Scraper
=====================================
Korrekte 2-Stufen-Strategie:
  1. Kalenderseite → Deutschland-Meetings (meeting-id--XXXXXX)
  2. Meeting-Seite → Race-IDs für jedes Rennen
  3. Race-Seite → Ergebnisse + Quoten

Usage:
  python wettstar_results_scraper.py --race-id 2492829
  python wettstar_results_scraper.py --from-date 2024-01-01 --to-date 2024-12-31
  python wettstar_results_scraper.py --from-date 2025-01-01

Dependencies:
  pip install playwright beautifulsoup4
  playwright install chromium && playwright install-deps chromium
"""

import asyncio, re, csv, sys, json, argparse
from datetime import datetime, timedelta
from pathlib import Path
from bs4 import BeautifulSoup

BASE = 'https://wettstar-pferdewetten.de'

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


# ── Stufe 1: Kalender → DE-Meeting-IDs ───────────────────────────────────────

def get_de_meetings_from_calendar(soup) -> list[dict]:
    """
    Extrahiert alle deutschen Galopp-Meetings aus der Kalenderseite.
    Returns: [{meeting_id, venue, n_races}, ...]
    """
    result = []
    for cb in soup.find_all(class_='ttml__country'):
        name_el = cb.find(class_='ttml__country__name')
        if not name_el or 'Deutschland' not in name_el.get_text():
            continue
        for m in cb.find_all(class_=re.compile(r'meeting-id--\d+')):
            cls_str = ' '.join(m.get('class', []))
            mid     = re.search(r'meeting-id--(\d+)', cls_str)
            ven_el  = m.find(class_='ttml__meeting__title--subject')
            venue   = ven_el.get_text(strip=True) if ven_el else ''
            n_races = re.search(r'\((\d+)\)', venue)

            # Nur Galopp-Hauptmeeting – funktioniert für 2024 + 2025:
            #   icon--r-gallop = Galopp  (beide Jahre)
            #   icon--r-trot   = Trab    → skip
            #   pmu-int        = PMU-Duplikat (2025) → skip (gleiche Rennen)
            meeting_html = str(m)
            if 'icon--r-gallop' not in meeting_html:
                continue
            if 'pmu-int' in meeting_html:
                continue

            if mid:
                result.append({
                    'meeting_id': int(mid.group(1)),
                    'venue':      venue,
                    'n_races':    int(n_races.group(1)) if n_races else 8,
                })
    return result


# ── Stufe 2: Meeting-Seite → Race-IDs ────────────────────────────────────────

def get_race_ids_from_meeting(soup) -> list[int]:
    """
    Extrahiert Race-IDs aus einer Meeting-Seite.
    Nutzt class='meetinginfo__racenumber' – das sind die echten Meeting-Rennen.
    Ignoriert nextraces__race Links (= internationale Nächste-Rennen-Navigation).
    """
    ids = []
    for a in soup.find_all('a', class_='meetinginfo__racenumber'):
        m = re.search(r'/race/(\d+)', a.get('href', ''))
        if m:
            ids.append(int(m.group(1)))
    return sorted(set(ids))


# ── Stufe 3: Race-Seite → Ergebnisse ─────────────────────────────────────────

def parse_race_page(html: str, race_id: int) -> list[dict]:
    if 'Ergebnis' not in html:
        return []
    soup     = BeautifulSoup(html, 'html.parser')
    meta     = extract_race_meta(soup)
    meta['race_id'] = race_id
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
        row   = {**meta, **pools}
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
            'finish_position': fp,
            'finish_distance': ev.get('finish_distance', s.get('finish_distance', '')),
            'implied_prob':    round(1 / pf(ev_q), 4) if pf(ev_q) else '',
            'won':             1 if str(fp) == '1' else 0,
            'placed':          1 if str(fp) in ['1', '2', '3'] else 0,
        })
        results.append(row)
    return results


def extract_race_meta(soup) -> dict:
    meta = {}
    for label, cls in [('race_date', '-breadcrumb-date'),
                        ('venue',     '-breadcrumb-name'),
                        ('race_nr',   '-breadcrumb-race')]:
        el = soup.find(class_=lambda c: c and cls in c if c else False)
        meta[label] = el.get_text(strip=True) if el else ''

    if meta.get('race_date'):
        for fmt in ('%d.%m.%y', '%d.%m.%Y'):
            try:
                meta['race_date'] = datetime.strptime(
                    meta['race_date'], fmt).strftime('%Y-%m-%d')
                break
            except ValueError:
                pass

    rn = re.search(r'R(\d+)', meta.get('race_nr', ''))
    meta['race_nr'] = int(rn.group(1)) if rn else ''

    text = soup.get_text(separator=' ')
    for pattern, key, cast in [
        (r'(\d{3,4})\s*m',                  'distance_m',      int),
        (r'Preisgeld\D{0,10}([\d.]+)\s*€',  'prize_eur',       lambda x: int(x.replace('.', ''))),
        (r'Starter\D{0,5}(\d+)',             'field_size',      int),
        (r'(\d{2}:\d{2})\s*Uhr',            'start_time',      str),
        (r'Kategorie\s+([A-Z])',             'race_class',      str),
        (r'Alter:\s*(\d+)',                  'age_restriction', str),
    ]:
        m = re.search(pattern, text)
        try:
            meta[key] = cast(m.group(1)) if m else ''
        except Exception:
            meta[key] = ''

    meta['surface']   = 'Flach' if 'Flach' in text else ('Sand' if 'Sand' in text else '')
    nm = re.search(r'Rennen\s+(?:des|der|vom|von)\s+(.+?)(?:\n|,|\|)', text)
    meta['race_name'] = nm.group(1).strip() if nm else ''
    return meta


def extract_starter_rows(soup) -> list[dict]:
    rows   = soup.find_all(class_=lambda c: c and '--rg-is-starter' in c if c else False)
    result = []
    for row in rows:
        s        = {}
        name_div = row.find(class_='race__grid__row__name')
        if not name_div:
            continue
        strongs = name_div.find_all('strong')
        spans   = name_div.find_all('span')
        s['start_nr']   = strongs[0].get_text(strip=True).rstrip('.') if strongs else ''
        s['horse_name'] = strongs[1].get_text(strip=True) if len(strongs) > 1 else ''
        bm = re.search(r'\((\d+)\)', spans[0].get_text() if spans else '')
        s['box_nr'] = bm.group(1) if bm else ''

        pills = row.find_all(class_='race__grid__row__vars__pills')
        ag    = pills[0].get_text(strip=True) if pills else ''
        wt    = pills[1].get_text(strip=True) if len(pills) > 1 else ''
        am    = re.match(r'(\d+)j\.\s*([A-Z])', ag)
        s['age']    = int(am.group(1)) if am else ''
        s['gender'] = am.group(2) if am else ''
        wm = re.search(r'([\d.]+)\s*kg', wt)
        s['weight_kg'] = float(wm.group(1)) if wm else ''

        j = row.find(class_='race__grid__row__humans__jockey')
        t = row.find(class_='race__grid__row__humans__trainer')
        s['jockey']  = j.get_text(strip=True) if j else ''
        s['trainer'] = re.sub(r'^\(|\)$', '', t.get_text(strip=True)) if t else ''

        s['sieg_toto'] = _odd(row, 'tote')
        s['fsieg_bm']  = _odd(row, 'fix')
        s['fplatz_bm'] = _odd(row, 'plcodd_fix')

        tt = row.find('table', class_='trendTrendsTable')
        s['ml_quote'] = ''
        if tt:
            trows = [r for r in tt.find_all('tr')
                     if not r.find(class_='trendTrendsTable__row__divider')]
            if len(trows) > 1:
                ml_td = trows[1].find('td', class_='ml')
                s['ml_quote'] = pf(ml_td.get_text(strip=True)) if ml_td else ''

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
    if not d:
        return ''
    v = d.find(class_='c-runner-odd__value')
    return pf(re.sub(r'[^\d,\.]', '', v.get_text(strip=True))) if v else ''


def extract_ev_table(soup) -> dict:
    t = next((t for t in soup.find_all('table') if 'Ev.-Quote' in t.get_text()), None)
    if not t:
        return {}
    out = {}
    for row in t.find_all('tr')[1:]:
        cols = [c.get_text(strip=True) for c in row.find_all(['td', 'th'])]
        if len(cols) >= 4:
            out[cols[2]] = {
                'finish_position': int(cols[0]) if cols[0].isdigit() else cols[0],
                'ev_quote':        pf(cols[3]),
                'finish_distance': cols[5] if len(cols) > 5 else '',
            }
    return out


def extract_pools(soup) -> dict:
    pools = {}
    for t in soup.find_all('table'):
        text = t.get_text(separator=' ', strip=True)
        m2 = re.match(r'^(\d+\s*-\s*\d+)\s+([\d,\.]+)$', text)
        if m2:
            pools['zweier_combo'] = m2.group(1).replace(' ', '')
            pools['zweier_quote'] = pf(m2.group(2))
        m3 = re.match(r'^(\d+\s*-\s*\d+\s*-\s*\d+)\s+([\d,\.]+)$', text)
        if m3:
            pools['dreier_combo'] = m3.group(1).replace(' ', '')
            pools['dreier_quote'] = pf(m3.group(2))
    return pools


def pf(s):
    try:
        return float(str(s).replace(',', '.').strip())
    except Exception:
        return ''


# ── Playwright ────────────────────────────────────────────────────────────────

async def fetch(page, url: str) -> str:
    try:
        await page.goto(url, wait_until='networkidle', timeout=30000)
        await asyncio.sleep(0.5)
        return await page.content()
    except Exception as e:
        print(f'  ⚠️  {url}: {e}')
        return ''


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
            to_date   = args.to_date or datetime.today().strftime('%Y-%m-%d')
            print(f'📅 Sammle DE-Rennen: {args.from_date} → {to_date}\n')

            race_ids  = []
            current   = datetime.strptime(args.from_date, '%Y-%m-%d')
            end       = datetime.strptime(to_date, '%Y-%m-%d')
            total_days = (end - current).days + 1
            scanned    = 0

            while current <= end:
                ds = current.strftime('%Y-%m-%d')

                # STUFE 1: Kalenderseite
                cal_html = await fetch(page, f'{BASE}/races/{ds}')
                if not cal_html:
                    current += timedelta(days=1)
                    continue

                cal_soup = BeautifulSoup(cal_html, 'html.parser')
                de_meetings = get_de_meetings_from_calendar(cal_soup)

                if de_meetings:
                    day_race_ids = []
                    for meet in de_meetings:
                        mid   = meet['meeting_id']
                        venue = meet['venue']

                        # STUFE 2: Meeting-Seite → Race-IDs
                        meet_html = await fetch(
                            page, f'{BASE}/races/{ds}?meeting={mid}'
                        )
                        if meet_html:
                            meet_soup = BeautifulSoup(meet_html, 'html.parser')
                            rids      = get_race_ids_from_meeting(meet_soup)

                            if rids:
                                day_race_ids.extend(rids)
                                print(f'  {ds} | {venue} (mid={mid}): '
                                      f'{len(rids)} Rennen → IDs {rids}')
                            else:
                                print(f'  {ds} | {venue} (mid={mid}): '
                                      f'⚠️  keine Race-IDs in Meeting-Seite')

                        await asyncio.sleep(0.3)

                    race_ids.extend(day_race_ids)

                scanned += 1
                if scanned % 30 == 0:
                    print(f'\n  ··· {scanned}/{total_days} Tage | '
                          f'{len(race_ids)} IDs gesamt ···\n')

                current += timedelta(days=1)
                await asyncio.sleep(0.2)

            race_ids = sorted(set(race_ids))
            print(f'\n✅ {len(race_ids)} Race-IDs gesammelt\n')

        else:
            print('❌ Nutze --race-id oder --from-date')
            sys.exit(1)

        # ── STUFE 3: Race-Seiten scrapen ──────────────────────────────────────
        todo        = [rid for rid in race_ids if rid not in done_ids]
        all_results = []
        new_done    = []
        stats       = {'ok': 0, 'no_result': 0, 'no_starter': 0}

        print(f'🏇 Scraping {len(todo)} Rennen '
              f'({len(race_ids) - len(todo)} bereits bekannt)...\n')

        for i, race_id in enumerate(todo, 1):
            try:
                html = await fetch(page, f'{BASE}/race/{race_id}')
                if not html:
                    new_done.append(race_id)
                    continue

                if 'Ergebnis' not in html:
                    stats['no_result'] += 1
                    new_done.append(race_id)
                    await asyncio.sleep(0.5)
                    continue

                rows = parse_race_page(html, race_id)
                if rows:
                    all_results.extend(rows)
                    stats['ok'] += 1
                    r0 = rows[0]
                    print(f'  [{i:>4}/{len(todo)}] ✅ {race_id} | '
                          f'{r0["race_date"]} {r0["venue"]} '
                          f'R{r0["race_nr"]} | {len(rows)} Starter')
                else:
                    stats['no_starter'] += 1
                    print(f'  [{i:>4}/{len(todo)}] ⚠️  {race_id} | '
                          f'Ergebnis vorhanden aber keine Starter-Rows')

                new_done.append(race_id)

                if i % 200 == 0:
                    _save_cp(cp_file, done_ids | set(new_done))
                    _write_csv(all_results, out_dir,
                               f'{args.from_date or args.race_id}_partial')
                    print(f'\n  💾 Checkpoint: {i}/{len(todo)} | '
                          f'ok={stats["ok"]} noResult={stats["no_result"]}\n')

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
    print(f'\n📊 Stats: ok={stats["ok"]} | noResult={stats["no_result"]} | '
          f'noStarter={stats["no_starter"]}')

    if all_results:
        tag = args.from_date or str(args.race_id)
        if args.to_date:
            tag += f'_to_{args.to_date}'
        csv_path = _write_csv(all_results, out_dir, tag)
        print(f'✅ CSV: {len(all_results)} Starter → {csv_path}')
        _summary(all_results)
    else:
        print('⚠️  Keine Ergebnisse – prüfe Logs oben.')
        sys.exit(0)


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


def _summary(results):
    from collections import Counter
    venues  = Counter(r.get('venue', '') for r in results)
    races   = len(set((r['race_id'], r['race_nr']) for r in results))
    winners = sum(1 for r in results if r.get('won') == 1)
    ev_ok   = sum(1 for r in results if r.get('ev_quote'))
    print(f'\n📈 Summary:')
    print(f'  Rennen:         {races}')
    print(f'  Starter:        {len(results)}')
    print(f'  Win-Rate:       {100*winners//len(results)}%')
    print(f'  EV-Quote:       {ev_ok}/{len(results)} gefüllt')
    print(f'\n  Top Venues:')
    for v, n in venues.most_common(10):
        print(f'    {v:<25}: {n:>5} Starter')


def main():
    p = argparse.ArgumentParser()
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument('--race-id',   type=int)
    g.add_argument('--from-date', type=str)
    p.add_argument('--to-date',   type=str, default=None)
    p.add_argument('--output',    type=str, default='./race_results/')
    args = p.parse_args()
    asyncio.run(run(args))


if __name__ == '__main__':
    main()
