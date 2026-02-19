#!/usr/bin/env node
/**
 * Wettstar Pferde-Scraper für Pferde-Edge Modell
 * ------------------------------------------------
 * 1. Öffnet wettstar-pferdewetten.de/races/DATUM via Puppeteer
 * 2. Findet alle PDF-Links des Renntages
 * 3. Lädt PDFs herunter
 * 4. Parst Starter-Daten via pdf-parse + Regex
 * 5. Exportiert CSV für Modell-Pipeline
 *
 * Installation:
 *   npm install puppeteer pdf-parse csv-writer axios
 *
 * Usage:
 *   node wettstar_horse_scraper.js --date 2026-02-20
 *   node wettstar_horse_scraper.js --date 2026-02-20 --output ./data/
 */

const puppeteer  = require('puppeteer');
const pdfParse   = require('pdf-parse');
const { createObjectCsvWriter } = require('csv-writer');
const axios      = require('axios');
const fs         = require('fs');
const path       = require('path');

// ─── CLI-Argumente ────────────────────────────────────────────────────────────
const args       = process.argv.slice(2);
const dateArg    = args[args.indexOf('--date') + 1]   || getTodayStr();
const outputDir  = args[args.indexOf('--output') + 1] || './horse_data/';
const BASE_URL   = 'https://wettstar-pferdewetten.de';

function getTodayStr() {
  return new Date().toISOString().slice(0, 10);
}

// ─── CSV Schema ───────────────────────────────────────────────────────────────
// Vollständiges Feature-Set für Pferde-Edge Modell
const CSV_HEADER = [
  // Meeting-Metadaten
  { id: 'meeting_date',   title: 'meeting_date'   },
  { id: 'venue',          title: 'venue'          },
  { id: 'race_nr',        title: 'race_nr'        },
  { id: 'race_time',      title: 'race_time'      },
  { id: 'race_name',      title: 'race_name'      },
  { id: 'distance_m',     title: 'distance_m'     },
  { id: 'prize_eur',      title: 'prize_eur'      },
  { id: 'surface',        title: 'surface'        },
  { id: 'field_size',     title: 'field_size'     },

  // Starter-Stammdaten
  { id: 'start_nr',       title: 'start_nr'       },
  { id: 'horse_name',     title: 'horse_name'     },
  { id: 'age',            title: 'age'            },
  { id: 'gender',         title: 'gender'         },
  { id: 'color',          title: 'color'          },
  { id: 'sire',           title: 'sire'           },
  { id: 'dam',            title: 'dam'            },
  { id: 'trainer',        title: 'trainer'        },
  { id: 'owner',          title: 'owner'          },
  { id: 'breeder',        title: 'breeder'        },
  { id: 'jockey',         title: 'jockey'         },
  { id: 'weight_kg',      title: 'weight_kg'      },
  { id: 'box_nr',         title: 'box_nr'         },
  { id: 'ml_odds',        title: 'ml_odds'        },  // Morning Line = Buchmacher-Basispreis

  // Jahresstatistiken 2025
  { id: 'starts_2025',    title: 'starts_2025'    },
  { id: 'wins_2025',      title: 'wins_2025'      },
  { id: 'places_2025',    title: 'places_2025'    },
  { id: 'prize_2025',     title: 'prize_2025'     },
  { id: 'win_pct_2025',   title: 'win_pct_2025'   },  // berechnete Feature
  { id: 'place_pct_2025', title: 'place_pct_2025' },

  // Jahresstatistiken 2024
  { id: 'starts_2024',    title: 'starts_2024'    },
  { id: 'wins_2024',      title: 'wins_2024'      },
  { id: 'places_2024',    title: 'places_2024'    },
  { id: 'prize_2024',     title: 'prize_2024'     },
  { id: 'win_pct_2024',   title: 'win_pct_2024'   },
  { id: 'place_pct_2024', title: 'place_pct_2024' },

  // Kombinierte Statistiken (für Modell-Features)
  { id: 'total_starts',   title: 'total_starts'   },
  { id: 'total_wins',     title: 'total_wins'     },
  { id: 'career_win_pct', title: 'career_win_pct' },
  { id: 'career_roi_approx', title: 'career_roi_approx' }, // crude form-Indikator

  // Letzte 5 Rennen (flach, für Sequenz-Features)
  { id: 'r1_date',        title: 'r1_date'        },
  { id: 'r1_venue',       title: 'r1_venue'       },
  { id: 'r1_place',       title: 'r1_place'       },
  { id: 'r1_weight',      title: 'r1_weight'      },
  { id: 'r1_distance',    title: 'r1_distance'    },
  { id: 'r1_prize',       title: 'r1_prize'       },
  { id: 'r1_odds',        title: 'r1_odds'        },
  { id: 'r1_jockey',      title: 'r1_jockey'      },

  { id: 'r2_date',        title: 'r2_date'        },
  { id: 'r2_venue',       title: 'r2_venue'       },
  { id: 'r2_place',       title: 'r2_place'       },
  { id: 'r2_weight',      title: 'r2_weight'      },
  { id: 'r2_distance',    title: 'r2_distance'    },
  { id: 'r2_prize',       title: 'r2_prize'       },
  { id: 'r2_odds',        title: 'r2_odds'        },
  { id: 'r2_jockey',      title: 'r2_jockey'      },

  { id: 'r3_date',        title: 'r3_date'        },
  { id: 'r3_venue',       title: 'r3_venue'       },
  { id: 'r3_place',       title: 'r3_place'       },
  { id: 'r3_weight',      title: 'r3_weight'      },
  { id: 'r3_distance',    title: 'r3_distance'    },
  { id: 'r3_prize',       title: 'r3_prize'       },
  { id: 'r3_odds',        title: 'r3_odds'        },
  { id: 'r3_jockey',      title: 'r3_jockey'      },

  { id: 'r4_date',        title: 'r4_date'        },
  { id: 'r4_venue',       title: 'r4_venue'       },
  { id: 'r4_place',       title: 'r4_place'       },
  { id: 'r4_weight',      title: 'r4_weight'      },
  { id: 'r4_distance',    title: 'r4_distance'    },
  { id: 'r4_prize',       title: 'r4_prize'       },
  { id: 'r4_odds',        title: 'r4_odds'        },
  { id: 'r4_jockey',      title: 'r4_jockey'      },

  { id: 'r5_date',        title: 'r5_date'        },
  { id: 'r5_venue',       title: 'r5_venue'       },
  { id: 'r5_place',       title: 'r5_place'       },
  { id: 'r5_weight',      title: 'r5_weight'      },
  { id: 'r5_distance',    title: 'r5_distance'    },
  { id: 'r5_prize',       title: 'r5_prize'       },
  { id: 'r5_odds',        title: 'r5_odds'        },
  { id: 'r5_jockey',      title: 'r5_jockey'      },

  // Berechnete Form-Features (wichtig für EV-Kalkulation)
  { id: 'days_since_last_run',   title: 'days_since_last_run'   },
  { id: 'avg_place_last5',       title: 'avg_place_last5'       },
  { id: 'distance_diff_m',       title: 'distance_diff_m'       }, // Distanzwechsel vs. letztes Rennen
  { id: 'weight_diff_kg',        title: 'weight_diff_kg'        }, // Gewichtsänderung
  { id: 'venue_repeat',          title: 'venue_repeat'          }, // Wiederholung Ort (0/1)
  { id: 'jockey_change',         title: 'jockey_change'         }, // Jockeywechsel vs. letztes Rennen (0/1)
  { id: 'trainer_jockey_combo',  title: 'trainer_jockey_combo'  }, // Trainer+Jockey-Kombi-Key
];

// ─── Step 1: PDF-URLs scrapen ─────────────────────────────────────────────────
async function scrapePdfUrls(date) {
  console.log(`🌐 Öffne Wettstar Rennkarte: ${date}`);

  const browser = await puppeteer.launch({
    headless: true,
    args: ['--no-sandbox', '--disable-setuid-sandbox']
  });

  const page = await browser.newPage();
  await page.setUserAgent(
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 ' +
    '(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
  );

  await page.goto(`${BASE_URL}/races/${date}`, {
    waitUntil: 'networkidle2',
    timeout: 30000
  });

  // Warte auf dynamisch geladene Inhalte
  await new Promise(r => setTimeout(r, 3000));

  // Extrahiere alle PDF-Links (verschiedene mögliche Selektoren)
  const pdfLinks = await page.evaluate((baseUrl) => {
    const links = [];
    const anchors = document.querySelectorAll('a[href*=".pdf"], a[href*="pdf"]');

    anchors.forEach(a => {
      const href = a.href;
      if (href && href.toLowerCase().includes('.pdf')) {
        links.push(href.startsWith('http') ? href : baseUrl + href);
      }
    });

    // Fallback: Suche nach Buttons/Links mit PDF-Text
    if (links.length === 0) {
      document.querySelectorAll('a').forEach(a => {
        const text = a.textContent.toLowerCase();
        if (text.includes('pdf') || text.includes('programm') || text.includes('download')) {
          const href = a.href;
          if (href) links.push(href);
        }
      });
    }

    return [...new Set(links)]; // Deduplizierung
  }, BASE_URL);

  // Fallback: Suche nach Meeting-ID im Seitenquelltext und konstruiere PDF-URL
  if (pdfLinks.length === 0) {
    console.log('⚠️  Keine direkten PDF-Links gefunden – versuche Meeting-ID-Extraktion');

    const pageContent = await page.content();
    const meetingMatches = pageContent.match(/ExtID[:\s=]*(\d{5,7})/gi) || [];
    const extIds = meetingMatches.map(m => m.match(/(\d{5,7})/)?.[1]).filter(Boolean);

    // Wettstar PDF-URL-Muster (aus Beispiel-PDF bekannt)
    // Format: /DE_G_{Ort}_{ExtID}.pdf
    extIds.forEach(id => {
      // Konstruiere mögliche URL basierend auf bekanntem Muster
      const dateFormatted = date.replace(/-/g, '');
      pdfLinks.push(`${BASE_URL}/pdf/${dateFormatted}_DE_G_unknown_${id}.pdf`);
    });
  }

  await browser.close();
  console.log(`✅ PDF-Links gefunden: ${pdfLinks.length}`);
  return pdfLinks;
}

// ─── Step 2: PDF herunterladen ────────────────────────────────────────────────
async function downloadPdf(url, destDir) {
  const filename = path.basename(url.split('?')[0]);
  const destPath = path.join(destDir, filename);

  if (fs.existsSync(destPath)) {
    console.log(`📄 PDF bereits vorhanden: ${filename}`);
    return destPath;
  }

  console.log(`⬇️  Lade herunter: ${filename}`);
  const response = await axios.get(url, {
    responseType: 'arraybuffer',
    timeout: 30000,
    headers: {
      'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0'
    }
  });

  fs.writeFileSync(destPath, response.data);
  console.log(`✅ Gespeichert: ${destPath}`);
  return destPath;
}

// ─── Step 3: PDF parsen ───────────────────────────────────────────────────────
async function parsePdf(pdfPath) {
  const dataBuffer = fs.readFileSync(pdfPath);
  const data = await pdfParse(dataBuffer);
  return data.text;
}

// ─── Step 4: Rennen-Metadaten aus PDF-Text extrahieren ────────────────────────
function extractMeetingMeta(text) {
  // Datum und Ort aus Kopfzeile: "01.02.2026 - Dortmund"
  const metaMatch = text.match(/(\d{2}\.\d{2}\.\d{4})\s*[-–]\s*([A-Za-zäöüÄÖÜß\s\-]+?)(?:\s+©|\s+Rennen)/);
  const date   = metaMatch?.[1] || '';
  const venue  = metaMatch?.[2]?.trim() || '';

  return { date, venue };
}

// ─── Step 5: Einzelne Pferde-Blöcke aus Rennen-Seiten parsen ─────────────────
function parseRacePages(text, meetingMeta) {
  const allStarters = [];

  // Rennen-Blöcke identifizieren: "Rennen # N" als Separator
  // Jede Rennen-Seite beginnt mit "01.02.2026 - Dortmund Rennen # N"
  const racePagePattern = /\d{2}\.\d{2}\.\d{4}\s*-\s*[A-Za-zäöüÄÖÜß]+\s+Rennen\s*#\s*(\d+)/g;

  // Text in Rennen-Abschnitte aufteilen
  const raceBlocks = splitIntoRaceBlocks(text);

  raceBlocks.forEach(block => {
    const starters = parseRaceBlock(block, meetingMeta);
    allStarters.push(...starters);
  });

  // field_size nachträglich setzen
  const raceMap = {};
  allStarters.forEach(s => {
    const key = `${s.race_nr}`;
    if (!raceMap[key]) raceMap[key] = 0;
    raceMap[key]++;
  });
  allStarters.forEach(s => {
    s.field_size = raceMap[`${s.race_nr}`] || 0;
  });

  return allStarters;
}

function splitIntoRaceBlocks(text) {
  // Trenne bei "Rennen # N  Seite N" oder ähnlichen Seitenmarkierungen
  // Wettstar PDF hat Rennen-Übersicht (Seite 1+2) und dann pro Rennen 1-2 Seiten
  const blocks = [];

  // Suche nach Rennkopf-Pattern: Rennnummer + Zeit + Distanz + Preisgeld
  // Format: "1\n10:35\n1950 m\n6.000 €"
  const raceHeaderPattern = /\n(\d{1,2})\n(\d{2}:\d{2})\n(\d{3,4})\s*m\n([\d.]+)\s*€\n(\w+)\n(.+?)\nRennpreis:/gs;

  let match;
  const positions = [];

  while ((match = raceHeaderPattern.exec(text)) !== null) {
    positions.push({ index: match.index, match });
  }

  for (let i = 0; i < positions.length; i++) {
    const start = positions[i].index;
    const end   = i + 1 < positions.length ? positions[i + 1].index : text.length;
    blocks.push({
      text:        text.slice(start, end),
      headerMatch: positions[i].match,
    });
  }

  return blocks;
}

function parseRaceBlock(block, meetingMeta) {
  const starters = [];
  const { text, headerMatch } = block;

  if (!headerMatch) return starters;

  // Rennen-Metadaten
  const raceNr   = parseInt(headerMatch[1]);
  const raceTime = headerMatch[2];
  const distM    = parseInt(headerMatch[3]);
  const prizeRaw = headerMatch[4].replace(/\./g, '');
  const prizeEur = parseInt(prizeRaw) || 0;
  const surface  = headerMatch[5].trim();  // "Flach"
  const raceName = headerMatch[6].trim();

  // Rennname aus nächster Zeile wenn nötig
  const raceNameFull = extractRaceName(text) || raceName;

  // Starter-Blöcke finden
  // Jeder Starter beginnt mit: Startnummer (allein auf Zeile), dann Box/ML
  const starterBlocks = splitIntoStarterBlocks(text);

  starterBlocks.forEach(starterText => {
    const starter = parseStarterBlock(starterText, {
      meeting_date: meetingMeta.date,
      venue:        meetingMeta.venue,
      race_nr:      raceNr,
      race_time:    raceTime,
      race_name:    raceNameFull,
      distance_m:   distM,
      prize_eur:    prizeEur,
      surface:      surface,
    });

    if (starter && starter.horse_name) {
      starters.push(starter);
    }
  });

  return starters;
}

function extractRaceName(text) {
  // Rennname liegt zwischen Preisgeld-Zeile und "Rennpreis:"
  const match = text.match(/€\n(\w[^\n]{3,80})\nRennpreis:/);
  return match?.[1]?.trim() || null;
}

function splitIntoStarterBlocks(text) {
  // Starter beginnen mit isolierter Zahl (Startnummer), dann "Box:"
  // Trennmuster: "\nN\nBox:" oder "\nN\nB\nBox:" (mit Bemerkung wie "B" für Blinkered)
  const blocks = [];
  const pattern = /\n(\d{1,2})\n(?:[A-Z]\n)?Box:\s*\d+/g;

  let match;
  const positions = [];

  while ((match = pattern.exec(text)) !== null) {
    positions.push(match.index);
  }

  for (let i = 0; i < positions.length; i++) {
    const start = positions[i];
    const end   = i + 1 < positions.length ? positions[i + 1] : text.length;
    blocks.push(text.slice(start, end));
  }

  return blocks;
}

function parseStarterBlock(text, raceMeta) {
  const starter = { ...raceMeta };

  // Startnummer
  const startNrMatch = text.match(/^\n?(\d{1,2})\n/);
  starter.start_nr = startNrMatch ? parseInt(startNrMatch[1]) : null;

  // Box und ML
  const boxMatch = text.match(/Box:\s*(\d+)/);
  const mlMatch  = text.match(/ML:\s*([\d,]+)/);
  starter.box_nr  = boxMatch  ? parseInt(boxMatch[1]) : null;
  starter.ml_odds = mlMatch   ? parseFloat(mlMatch[1].replace(',', '.')) : null;

  // Gewicht (steht oft als "62.00" oder "57.50" allein auf einer Zeile nach ML)
  const weightMatch = text.match(/ML:\s*[\d,]+\n([\d]+\.[\d]+)\n/);
  starter.weight_kg = weightMatch ? parseFloat(weightMatch[1]) : null;

  // Pferdename (erste Großschriftzeile nach Gewicht)
  const horseMatch = text.match(/\n([\d]+\.[\d]+)\n([A-Z][A-Za-zäöüÄÖÜß\s'\-]+)\n/);
  starter.horse_name = horseMatch ? horseMatch[2].trim() : null;

  // Abstammung: "6j. b W (Elzaam - Preach)" oder "4j. f S (Ardad - Azita)"
  const bloodlineMatch = text.match(/(\d+)j\.\s+(\w+)\s+([A-Z])\s+\(([^)]+)\)/);
  if (bloodlineMatch) {
    starter.age    = parseInt(bloodlineMatch[1]);
    starter.color  = bloodlineMatch[2]; // b, db, f, rsch etc.
    starter.gender = bloodlineMatch[3]; // W=Wallach, S=Stute, H=Hengst
    const parents  = bloodlineMatch[4].split(' - ');
    starter.sire   = parents[0]?.trim() || '';
    starter.dam    = parents[1]?.trim() || '';
  }

  // Trainer, Besitzer, Züchter
  const trainerMatch  = text.match(/Trainer:\s*(.+)/);
  const ownerMatch    = text.match(/Besitzer:\s*(.+)/);
  const breederMatch  = text.match(/Züchter:\s*(.+)/);
  starter.trainer = trainerMatch?.[1]?.trim() || '';
  starter.owner   = ownerMatch?.[1]?.trim()   || '';
  starter.breeder = breederMatch?.[1]?.trim() || '';

  // Jockey: Zeile nach Züchter (letzte Textzeile vor Jahresstats)
  const jockeyMatch = text.match(/Züchter:\s*.+\n(.+)\n.*\d{4}:/);
  starter.jockey = jockeyMatch?.[1]?.trim() || '';

  // Jahresstats 2025
  const stats2025 = text.match(/(\d{4}):\s*(\d+)\s+Starts?\s*-\s*(\d+)\s+Sieg[e]?\s*-\s*(\d+)\s+Plät[zs]e\s*([\d.]+)\s*€/);
  if (stats2025 && stats2025[1] === '2025') {
    starter.starts_2025 = parseInt(stats2025[2]);
    starter.wins_2025   = parseInt(stats2025[3]);
    starter.places_2025 = parseInt(stats2025[4]);
    starter.prize_2025  = parseInt(stats2025[5].replace(/\./g, ''));
  } else {
    starter.starts_2025 = 0;
    starter.wins_2025   = 0;
    starter.places_2025 = 0;
    starter.prize_2025  = 0;
  }

  // Jahresstats 2024
  const allStats = [...text.matchAll(/(\d{4}):\s*(\d+)\s+Starts?\s*-\s*(\d+)\s+Sieg[e]?\s*-\s*(\d+)\s+Plät[zs]e\s*([\d.]+)\s*€/g)];
  const stat2024 = allStats.find(m => m[1] === '2024');
  if (stat2024) {
    starter.starts_2024 = parseInt(stat2024[2]);
    starter.wins_2024   = parseInt(stat2024[3]);
    starter.places_2024 = parseInt(stat2024[4]);
    starter.prize_2024  = parseInt(stat2024[5].replace(/\./g, ''));
  } else {
    starter.starts_2024 = 0;
    starter.wins_2024   = 0;
    starter.places_2024 = 0;
    starter.prize_2024  = 0;
  }

  // Berechnete Jahres-Features
  starter.win_pct_2025   = starter.starts_2025 > 0 ? +(starter.wins_2025   / starter.starts_2025).toFixed(4) : 0;
  starter.place_pct_2025 = starter.starts_2025 > 0 ? +(starter.places_2025 / starter.starts_2025).toFixed(4) : 0;
  starter.win_pct_2024   = starter.starts_2024 > 0 ? +(starter.wins_2024   / starter.starts_2024).toFixed(4) : 0;
  starter.place_pct_2024 = starter.starts_2024 > 0 ? +(starter.places_2024 / starter.starts_2024).toFixed(4) : 0;

  // Kumulierte Karriere-Features
  starter.total_starts   = (starter.starts_2025 || 0) + (starter.starts_2024 || 0);
  starter.total_wins     = (starter.wins_2025   || 0) + (starter.wins_2024   || 0);
  starter.career_win_pct = starter.total_starts > 0
    ? +(starter.total_wins / starter.total_starts).toFixed(4)
    : 0;

  // Crude ROI-Approximation: Gewinne verglichen mit Preisgeld-Anteil
  // Kein echter ROI ohne Quoten-Historik, aber Preisgeld/Starts als Proxy
  const totalPrize  = (starter.prize_2025 || 0) + (starter.prize_2024 || 0);
  starter.career_roi_approx = starter.total_starts > 0
    ? +(totalPrize / starter.total_starts).toFixed(2)
    : 0;

  // Letzte 5 Rennen parsen
  // Format: "01.02 Dortmund 10 62.0 1950 6.000 7,0 Matthew-S. Johnson Maharani, Imaginary, ..."
  const formPattern = /(\d{2}\.\d{2})\s+([A-Za-zäöüÄÖÜß\-]+)\s+(\d{1,2}|U)\s+([\d]+\.[\d]+)\s+(\d{3,4})\s+([\d.]+)\s+([\d,]+)\s+(.+?)(?=\d{2}\.\d{2}\s+[A-Z]|\nWettstar|$)/gs;

  const formRaces = [];
  let fMatch;
  while ((fMatch = formPattern.exec(text)) !== null && formRaces.length < 5) {
    formRaces.push({
      date:     fMatch[1],
      venue:    fMatch[2].trim(),
      place:    fMatch[3] === 'U' ? 99 : parseInt(fMatch[3]),  // U = Unplaced
      weight:   parseFloat(fMatch[4]),
      distance: parseInt(fMatch[5]),
      prize:    parseInt(fMatch[6].replace(/\./g, '')),
      odds:     parseFloat(fMatch[7].replace(',', '.')),
      jockey:   fMatch[8].trim().split('\n')[0].trim(),
    });
  }

  // Formrennen in CSV-Felder mappen
  for (let i = 0; i < 5; i++) {
    const r = formRaces[i];
    const prefix = `r${i + 1}`;
    starter[`${prefix}_date`]     = r?.date     || '';
    starter[`${prefix}_venue`]    = r?.venue    || '';
    starter[`${prefix}_place`]    = r?.place    ?? '';
    starter[`${prefix}_weight`]   = r?.weight   ?? '';
    starter[`${prefix}_distance`] = r?.distance ?? '';
    starter[`${prefix}_prize`]    = r?.prize    ?? '';
    starter[`${prefix}_odds`]     = r?.odds     ?? '';
    starter[`${prefix}_jockey`]   = r?.jockey   || '';
  }

  // Berechnete Form-Features
  starter.days_since_last_run = calcDaysSinceLastRun(formRaces[0]?.date, raceMeta.meeting_date);
  starter.avg_place_last5     = calcAvgPlace(formRaces);
  starter.distance_diff_m     = formRaces[0] ? raceMeta.distance_m - formRaces[0].distance : null;
  starter.weight_diff_kg      = formRaces[0] && starter.weight_kg
    ? +(starter.weight_kg - formRaces[0].weight).toFixed(1) : null;
  starter.venue_repeat        = formRaces.some(r => r?.venue === raceMeta.venue) ? 1 : 0;
  starter.jockey_change       = formRaces[0] && starter.jockey
    ? (formRaces[0].jockey !== starter.jockey ? 1 : 0) : null;
  starter.trainer_jockey_combo = `${starter.trainer}|${starter.jockey}`;

  return starter;
}

// ─── Hilfsfunktionen ──────────────────────────────────────────────────────────
function calcDaysSinceLastRun(lastDateStr, meetingDateStr) {
  if (!lastDateStr || !meetingDateStr) return null;

  // lastDateStr: "28.12" (kein Jahr!) → anhand meeting_date Jahr erschließen
  const meetingDate  = parseMeetingDate(meetingDateStr);
  if (!meetingDate) return null;

  const [dd, mm]    = lastDateStr.split('.').map(Number);
  let year           = meetingDate.getFullYear();
  // Wenn Monat in der Vergangenheit relativ zum Meeting, selbes Jahr; sonst Vorjahr
  if (mm > meetingDate.getMonth() + 1) year--;

  const lastDate    = new Date(year, mm - 1, dd);
  const diffMs      = meetingDate - lastDate;
  return Math.round(diffMs / (1000 * 60 * 60 * 24));
}

function parseMeetingDate(dateStr) {
  // Format: "01.02.2026" oder "2026-02-01"
  if (!dateStr) return null;
  if (dateStr.includes('-')) return new Date(dateStr);
  const [dd, mm, yyyy] = dateStr.split('.').map(Number);
  return new Date(yyyy, mm - 1, dd);
}

function calcAvgPlace(formRaces) {
  const valid = formRaces.filter(r => r?.place && r.place < 99);
  if (!valid.length) return null;
  return +(valid.reduce((sum, r) => sum + r.place, 0) / valid.length).toFixed(2);
}

// ─── Step 6: CSV schreiben ────────────────────────────────────────────────────
async function writeCsv(starters, outputPath) {
  const writer = createObjectCsvWriter({
    path:   outputPath,
    header: CSV_HEADER
  });

  await writer.writeRecords(starters);
  console.log(`📊 CSV gespeichert: ${outputPath} (${starters.length} Starter)`);
}

// ─── Lokales PDF verarbeiten (Fallback / Testing) ─────────────────────────────
async function processLocalPdf(pdfPath, date) {
  console.log(`📄 Verarbeite lokales PDF: ${pdfPath}`);
  const text = await parsePdf(pdfPath);
  const meetingMeta = extractMeetingMeta(text);
  if (!meetingMeta.date && date) meetingMeta.date = date;
  const starters = parseRacePages(text, meetingMeta);
  return starters;
}

// ─── Hauptpipeline ────────────────────────────────────────────────────────────
async function main() {
  console.log('🏇 Wettstar Pferde-Scraper gestartet');
  console.log(`📅 Datum: ${dateArg}`);

  // Ausgabeverzeichnis anlegen
  if (!fs.existsSync(outputDir)) {
    fs.mkdirSync(outputDir, { recursive: true });
  }

  const pdfDir     = path.join(outputDir, 'pdfs');
  if (!fs.existsSync(pdfDir)) fs.mkdirSync(pdfDir);

  let allStarters = [];

  try {
    // PDF-Links scrapen
    const pdfUrls = await scrapePdfUrls(dateArg);

    if (pdfUrls.length === 0) {
      console.log('⚠️  Keine PDFs gefunden. Prüfe ob Datum korrekt oder Renntag vorhanden.');
      process.exit(0);
    }

    // PDFs verarbeiten
    for (const url of pdfUrls) {
      try {
        const pdfPath  = await downloadPdf(url, pdfDir);
        const text     = await parsePdf(pdfPath);
        const meta     = extractMeetingMeta(text);
        if (!meta.date) meta.date = dateArg;

        const starters = parseRacePages(text, meta);
        console.log(`✅ ${starters.length} Starter aus ${path.basename(pdfPath)} extrahiert`);
        allStarters.push(...starters);
      } catch (err) {
        console.error(`❌ Fehler bei PDF ${url}: ${err.message}`);
      }
    }

  } catch (err) {
    console.error(`❌ Scraping-Fehler: ${err.message}`);
  }

  if (allStarters.length === 0) {
    console.log('⚠️  Keine Starter-Daten extrahiert.');
    process.exit(1);
  }

  // CSV ausgeben
  const csvPath = path.join(outputDir, `horse_starters_${dateArg}.csv`);
  await writeCsv(allStarters, csvPath);

  console.log('\n🎉 Pipeline abgeschlossen');
  console.log(`📊 Gesamt: ${allStarters.length} Starter in ${csvPath}`);
}

// ─── Export für direktes PDF-Testing ─────────────────────────────────────────
// Ermöglicht: node wettstar_horse_scraper.js --local ./myfile.pdf --date 2026-02-01
if (args.includes('--local')) {
  const localPdf = args[args.indexOf('--local') + 1];
  if (!localPdf || !fs.existsSync(localPdf)) {
    console.error('❌ Lokales PDF nicht gefunden:', localPdf);
    process.exit(1);
  }

  if (!fs.existsSync(outputDir)) fs.mkdirSync(outputDir, { recursive: true });

  processLocalPdf(localPdf, dateArg)
    .then(async (starters) => {
      console.log(`✅ ${starters.length} Starter extrahiert`);
      const csvPath = path.join(outputDir, `horse_starters_${dateArg}.csv`);
      await writeCsv(starters, csvPath);
      console.log('🎉 Fertig');
    })
    .catch(err => {
      console.error('❌ Fehler:', err.message);
      process.exit(1);
    });

} else {
  main().catch(err => {
    console.error('❌ Unbehandelter Fehler:', err);
    process.exit(1);
  });
}
