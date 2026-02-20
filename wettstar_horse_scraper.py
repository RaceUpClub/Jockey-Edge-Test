#!/usr/bin/env python3
"""
Wettstar Pferde-Scraper
=======================
Parst Wettstar-PDFs → CSV mit allen Starter-Features für das Pferde-Edge-Modell.

Usage:
  # Lokal (Test):
  python wettstar_horse_scraper.py --local ./mypdf.pdf --date 2026-02-01

  # Web (Produktion):
  python wettstar_horse_scraper.py --date 2026-02-01 --output ./horse_data/

Dependencies:
  pip install pdfplumber requests
"""

import pdfplumber
import re
import csv
import sys
import os
import argparse
from datetime import datetime
from collections import Counter

# ── CSV-Schema ────────────────────────────────────────────────────────────────
FIELDNAMES = [
    'meeting_date','venue','race_nr','race_time','race_name','distance_m','prize_eur','surface','field_size',
    'start_nr','horse_name','age','gender','color','sire','dam','trainer','owner','breeder','jockey',
    'weight_kg','box_nr','ml_odds',
    'starts_2025','wins_2025','places_2025','prize_2025','win_pct_2025','place_pct_2025',
    'starts_2024','wins_2024','places_2024','prize_2024','win_pct_2024','place_pct_2024',
    'total_starts','total_wins','career_win_pct','career_roi_approx',
    'r1_date','r1_venue','r1_place','r1_weight','r1_distance','r1_prize','r1_odds','r1_jockey',
    'r2_date','r2_venue','r2_place','r2_weight','r2_distance','r2_prize','r2_odds','r2_jockey',
    'r3_date','r3_venue','r3_place','r3_weight','r3_distance','r3_prize','r3_odds','r3_jockey',
    'r4_date','r4_venue','r4_place','r4_weight','r4_distance','r4_prize','r4_odds','r4_jockey',
    'r5_date','r5_venue','r5_place','r5_weight','r5_distance','r5_prize','r5_odds','r5_jockey',
    'days_since_last_run','avg_place_last5','distance_diff_m','weight_diff_kg',
    'venue_repeat','jockey_change','trainer_jockey_combo',
]

# Plätze/Platz (Umlaut und Singular)
STATS_PAT = re.compile(
    r'(\d{4}):\s*(\d+)\s+Starts?\s*-\s*(\d+)\s+Sieg[e]?\s*-\s*(\d+)\s+(?:Plätze|Platz)\s*([\d.]+)\s*€'
)

# Formrennen: DD.MMVenue PlaceWeight Dist Prize Quote Rest
FORM_PAT = re.compile(
    r'(\d{2}\.\d{2})([A-Za-zäöüÄÖÜß\-]+)\s+'
    r'(\d{1,2})(\d{2}\.\d+)\s+'
    r'(\d{3,4})\s+'
    r'([\d.]+)\s*'
    r'([\d,]+)\s+'
    r'(.+?)(?=\s*\d{2}\.\d{2}[A-Za-z]|\s*$)'
)


# ── Hilfsfunktionen ───────────────────────────────────────────────────────────

def jockey_from_rest(rest):
    """
    Format nach Quote: "Vorname Nachname HorseName1, HorseName2, ..."
    → erste 2 Tokens = Vor- und Nachname (deckt 98% der Fälle ab).
    Sonderfall 3-teilige Namen (z.B. Esentur Turganaaly Uulu): erste 2 Tokens
    sind nahe genug für jockey_change-Vergleich.
    """
    tokens = rest.strip().split()
    if len(tokens) >= 2:
        return f"{tokens[0]} {tokens[1]}"
    return tokens[0] if tokens else ''


def parse_box_ml(line):
    """Scrambled "B M o L x : : ML_INT BOX ,ML_DEC ..." → (ml_odds, box_nr)"""
    m = re.search(r':\s*(\d+)\s+(\d+)\s*,(\d)\s*(\d*)', line)
    if not m:
        return None, None
    ml  = float(f"{m.group(1)}.{m.group(3)}")
    box = int(m.group(2) + m.group(4)) if m.group(4) else int(m.group(2))
    return ml, box


def split_blocks(lines):
    """
    Zwei Starter-Muster:
    A) Normal:     "N HorseName 2025: X Starts..."
    B) Zweizeilig: "HorseName"  /  "N"  /  Abstammungszeile   (z.B. Beautiful Dawn)
    """
    PAT_A    = re.compile(
        r'^(\d{1,2})\s+([A-ZÄÖÜ][A-Za-zäöüÄÖÜß\s\'\-]+?)\s+'
        r'(?:[\d.]+\s*€\s+)?\d{4}:\s*\d+\s+Starts?'
    )
    PAT_NAME = re.compile(r'^([A-ZÄÖÜ][A-Za-zäöüÄÖÜß\s\'\-]+)$')
    PAT_NR   = re.compile(r'^(\d{1,2})$')

    blocks, current, i = [], None, 0
    while i < len(lines):
        line = lines[i]
        if PAT_A.match(line):
            if current:
                blocks.append(current)
            current = [line]
            i += 1
        elif (PAT_NAME.match(line)
              and i+1 < len(lines) and PAT_NR.match(lines[i+1])
              and i+2 < len(lines) and re.match(r'^\d+j\.', lines[i+2])):
            if current:
                blocks.append(current)
            current = [f"{lines[i+1].strip()} {line.strip()}", lines[i+2]]
            i += 3
        else:
            if current:
                current.append(line)
            i += 1
    if current:
        blocks.append(current)
    return blocks


def ef(text, label):
    m = re.search(rf'{label}:\s*(.+)', text)
    return m.group(1).strip() if m else ''


def pf(s):
    try:
        return float(str(s).replace(',', '.'))
    except Exception:
        return None


def calc_days(last_str, meeting_str):
    if not last_str or not meeting_str:
        return ''
    try:
        mt = datetime.strptime(meeting_str, '%Y-%m-%d')
        dd, mm = int(last_str[:2]), int(last_str[3:5])
        yr = mt.year if mm <= mt.month else mt.year - 1
        return (mt - datetime(yr, mm, dd)).days
    except Exception:
        return ''


# ── Kern-Parser ───────────────────────────────────────────────────────────────

def parse_wettstar_pdf(pdf_path, race_date):
    all_starters = []
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            text = page.extract_text()
            if not text:
                continue
            lines = text.split('\n')

            hm = re.match(
                r'\d{2}\.\d{2}\.\d{4}\s*-\s*([A-Za-zäöüÄÖÜß\-]+)\s+Rennen\s*#\s*(\d+)',
                lines[0]
            )
            if not hm:
                continue

            venue      = hm.group(1).strip()
            race_nr    = int(hm.group(2))
            dist_m     = re.search(r'(\d{3,4})\s*m', lines[1] if len(lines) > 1 else '')
            prize_m    = re.search(r'([\d.]+)\s*€', lines[2] if len(lines) > 2 else '')
            surface    = lines[4].strip() if len(lines) > 4 else 'Flach'
            race_time  = lines[5].strip() if len(lines) > 5 else ''
            race_name  = re.sub(r'^\d+\s+\d+\s*m\s*', '', lines[1]).strip() if len(lines) > 1 else ''
            distance_m = int(dist_m.group(1)) if dist_m else 0
            prize_eur  = int(prize_m.group(1).replace('.', '')) if prize_m else 0

            blocks = split_blocks(lines)
            for block in blocks:
                s = parse_starter(
                    block, venue, race_nr, race_time, race_name,
                    distance_m, prize_eur, surface, len(blocks), race_date
                )
                if s:
                    all_starters.append(s)
    return all_starters


def parse_starter(lines, venue, race_nr, race_time, race_name,
                  distance_m, prize_eur, surface, field_size, race_date):
    full  = '\n'.join(lines)
    line0 = lines[0]
    s = {
        'meeting_date': race_date, 'venue': venue, 'race_nr': race_nr,
        'race_time': race_time, 'race_name': race_name, 'distance_m': distance_m,
        'prize_eur': prize_eur, 'surface': surface, 'field_size': field_size,
    }

    # Startnummer + Pferdename
    m = re.match(r'^(\d{1,2})\s+([A-ZÄÖÜ][A-Za-zäöüÄÖÜß\s\'\-]+)', line0)
    if not m:
        return None
    s['start_nr']   = int(m.group(1))
    s['horse_name'] = m.group(2).strip()

    # Jahresstats 2025 + 2024
    stats = {2025: (0,0,0,0), 2024: (0,0,0,0)}
    for sm in STATS_PAT.finditer(line0):
        y = int(sm.group(1))
        if y in stats:
            stats[y] = (
                int(sm.group(2)), int(sm.group(3)),
                int(sm.group(4)), int(sm.group(5).replace('.', ''))
            )
    s25, w25, p25, pr25 = stats[2025]
    s24, w24, p24, pr24 = stats[2024]
    s.update({
        'starts_2025': s25, 'wins_2025': w25, 'places_2025': p25, 'prize_2025': pr25,
        'starts_2024': s24, 'wins_2024': w24, 'places_2024': p24, 'prize_2024': pr24,
        'win_pct_2025':    round(w25/s25,4) if s25 else 0,
        'place_pct_2025':  round(p25/s25,4) if s25 else 0,
        'win_pct_2024':    round(w24/s24,4) if s24 else 0,
        'place_pct_2024':  round(p24/s24,4) if s24 else 0,
        'total_starts':    s25+s24, 'total_wins': w25+w24,
        'career_win_pct':  round((w25+w24)/(s25+s24),4) if (s25+s24) else 0,
        'career_roi_approx': round((pr25+pr24)/(s25+s24),2) if (s25+s24) else 0,
    })

    # Abstammung (Alter, Geschlecht, Farbe, Vater, Mutter)
    bl_src = next((l for l in lines[1:4] if re.match(r'\d+j\.', l)), '')
    bl = re.match(r'(\d+)j\.\s+(\w+)\s+([A-Z])\s+\(([^-]+)\s*-\s*([^)]+)\)', bl_src)
    s.update({
        'age':    int(bl.group(1)) if bl else '',
        'color':  bl.group(2) if bl else '',
        'gender': bl.group(3) if bl else '',
        'sire':   bl.group(4).strip() if bl else '',
        'dam':    bl.group(5).strip() if bl else '',
    })

    s['trainer'] = ef(full, 'Trainer')
    s['owner']   = ef(full, 'Besitzer')
    s['breeder'] = ef(full, 'Züchter')

    # Gewicht
    wm = re.search(r'^(\d{2}\.\d{2})\s+Besitzer:', full, re.MULTILINE)
    s['weight_kg'] = float(wm.group(1)) if wm else None

    # Box + ML (aus scrambled BML-Zeile, ca. 42% Abdeckung – Rest über Seitenumbruch verloren)
    bml = next((l for l in lines if re.search(r'B\s+M\s+o\s+L', l)), '')
    s['ml_odds'], s['box_nr'] = parse_box_ml(bml)

    # ── Jockey: heutiges Rennen aus Form-Zeile, vollständiger Name (2 Tokens) ──
    today    = race_date[8:10] + '.' + race_date[5:7]   # "01.02"
    all_form = []
    today_jockey = ''

    for line in lines[1:]:
        for fm in FORM_PAT.finditer(line):
            date_str = fm.group(1)
            jockey   = jockey_from_rest(fm.group(8))   # immer 2 Tokens

            if date_str == today:
                today_jockey = jockey
                continue   # Heute-Rennen nicht in Formrennen aufnehmen
            if len(all_form) < 5:
                all_form.append({
                    'date':     date_str,
                    'venue':    fm.group(2),
                    'place':    int(fm.group(3)),
                    'weight':   float(fm.group(4)),
                    'distance': int(fm.group(5)),
                    'prize':    int(fm.group(6).replace('.', '')),
                    'odds':     pf(fm.group(7)),
                    'jockey':   jockey,
                })

    # Fallback: wenn heute nicht gefunden (Nachmeldung), r1_jockey nehmen
    s['jockey'] = today_jockey or (all_form[0]['jockey'] if all_form else '')

    # Formrennen r1–r5
    for i in range(5):
        r  = all_form[i] if i < len(all_form) else {}
        px = f'r{i+1}'
        for k in ['date','venue','place','weight','distance','prize','odds','jockey']:
            s[f'{px}_{k}'] = r.get(k, '')

    # ── Berechnete Features ────────────────────────────────────────────────────
    s['days_since_last_run'] = calc_days(
        all_form[0]['date'] if all_form else None, race_date
    )
    vp = [r['place'] for r in all_form if r.get('place', '') != '' and r['place'] < 20]
    s['avg_place_last5']  = round(sum(vp)/len(vp), 2) if vp else ''
    s['distance_diff_m']  = distance_m - all_form[0]['distance'] if all_form else ''
    s['weight_diff_kg']   = (
        round(s['weight_kg'] - all_form[0]['weight'], 1)
        if s.get('weight_kg') and all_form else ''
    )
    s['venue_repeat']     = 1 if any(r.get('venue') == venue for r in all_form) else 0

    # jockey_change: Nachname-Vergleich (letztes Token = robuster gegen Abkürzungen)
    main_last = s['jockey'].split()[-1] if s['jockey'] else ''
    r1_last   = s.get('r1_jockey', '').split()[-1] if s.get('r1_jockey') else ''
    s['jockey_change']        = (0 if main_last == r1_last else 1) if (main_last and r1_last) else ''
    s['trainer_jockey_combo'] = f"{s['trainer']}|{s['jockey']}"
    return s


# ── Web-Download ───────────────────────────────────────────────────────────────

def fetch_pdf_urls(race_date):
    """Scrape Wettstar-Seite nach PDF-Links für das Datum."""
    import requests
    from urllib.parse import urljoin
    base = 'https://wettstar-pferdewetten.de'
    url  = f'{base}/races/{race_date}'
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0'}
    try:
        resp = requests.get(url, headers=headers, timeout=20)
        links = re.findall(r'href=["\']([^"\']*\.pdf[^"\']*)["\']', resp.text, re.IGNORECASE)
        return list({l if l.startswith('http') else urljoin(base, l) for l in links})
    except Exception as e:
        print(f'❌ Fehler beim Abrufen: {e}')
        return []


def download_pdf(url, dest_dir):
    import requests
    fname = os.path.basename(url.split('?')[0])
    dest  = os.path.join(dest_dir, fname)
    if os.path.exists(dest):
        print(f'📄 Bereits vorhanden: {fname}')
        return dest
    resp = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=30)
    resp.raise_for_status()
    with open(dest, 'wb') as f:
        f.write(resp.content)
    print(f'⬇️  Heruntergeladen: {fname}')
    return dest


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description='Wettstar Pferde-Scraper')
    parser.add_argument('--date',   default=datetime.today().strftime('%Y-%m-%d'),
                        help='Renntag (YYYY-MM-DD)')
    parser.add_argument('--local',  default=None,
                        help='Lokales PDF (Test-Modus, kein Web-Scraping)')
    parser.add_argument('--output', default='./horse_data/',
                        help='Ausgabe-Verzeichnis für CSV')
    args = parser.parse_args()

    os.makedirs(args.output, exist_ok=True)

    all_starters = []

    if args.local:
        if not os.path.exists(args.local):
            print(f'❌ PDF nicht gefunden: {args.local}')
            sys.exit(1)
        print(f'📄 Verarbeite: {args.local}')
        all_starters = parse_wettstar_pdf(args.local, args.date)
        print(f'✅ {len(all_starters)} Starter | '
              f'{len(set(s["race_nr"] for s in all_starters))} Rennen')
    else:
        pdf_dir = os.path.join(args.output, 'pdfs')
        os.makedirs(pdf_dir, exist_ok=True)
        print(f'🌐 Suche PDFs für {args.date} ...')
        urls = fetch_pdf_urls(args.date)
        if not urls:
            print('⚠️  Keine PDF-Links gefunden.')
            sys.exit(1)
        for url in urls:
            try:
                pdf_path = download_pdf(url, pdf_dir)
                starters = parse_wettstar_pdf(pdf_path, args.date)
                print(f'  ✅ {len(starters)} Starter aus {os.path.basename(pdf_path)}')
                all_starters.extend(starters)
            except Exception as e:
                print(f'  ❌ {url}: {e}')

    if not all_starters:
        print('⚠️  Keine Daten.')
        sys.exit(1)

    csv_path = os.path.join(args.output, f'horse_starters_{args.date}.csv')
    with open(csv_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(all_starters)

    print(f'📊 CSV: {len(all_starters)} Zeilen → {csv_path}')


if __name__ == '__main__':
    main()
