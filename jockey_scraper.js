#!/usr/bin/env node
/**
 * Jockey-Statistik Scraper für galopp-statistik.de
 * Extrahiert Jockey-Daten in maschinenlesbares Format (CSV/JSON)
 */

const puppeteer = require('puppeteer');
const fs = require('fs');
const path = require('path');

// URL der Jockey-Statistik
const TARGET_URL = 'https://www.galopp-statistik.de/Jockey-Statistik.php';

// Output-Dateien
const OUTPUT_DIR = 'data';
const OUTPUT_CSV = path.join(OUTPUT_DIR, 'jockey_stats_de.csv');
const OUTPUT_JSON = path.join(OUTPUT_DIR, 'jockey_stats_de.json');

async function scrapeJockeyData() {
    console.log('🐎 Starte Jockey-Scraping...');

    const browser = await puppeteer.launch({ const page = await
        headless: true,
        args: ['--no-sandbox', '--disable-setuid-sandbox']  // GitHub-Fix!
    });

       const page = await browser.newPage();
        // User-Agent setzen
    await page.setUserAgent(
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    );

    try


        // Seite laden
        await page.goto(TARGET_URL, { waitUntil: 'networkidle2', timeout: 30000 });
        console.log('✅ Seite geladen');

        // Warte auf Tabelle
        await page.waitForSelector('table', { timeout: 10000 });

        // Extrahiere Daten
        const jockeyData = await page.evaluate(() => {
            const rows = document.querySelectorAll('table tr');
            const data = [];

            rows.forEach((row, index) => {
                // Überspringe Header (erste Zeile) und Trennzeile
                if (index === 0 || index === 1) return;

                const cells = row.querySelectorAll('td');
                if (cells.length >= 8) {
                    // Prozentsätze bereinigen
                    const siegePct = cells[6].textContent.trim().replace('%', '');
                    const platzPct = cells[7].textContent.trim().replace('%', '');

                    data.push({
                        platz: parseInt(cells[0].textContent.trim()) || null,
                        jockey: cells[1].textContent.trim(),
                        starts: parseInt(cells[2].textContent.trim()) || 0,
                        siege: parseInt(cells[3].textContent.trim()) || 0,
                        platz2: parseInt(cells[4].textContent.trim()) || 0,
                        platz3: parseInt(cells[5].textContent.trim()) || 0,
                        siegquote: parseFloat(siegePct) / 100 || 0,
                        platzquote: parseFloat(platzPct) / 100 || 0,
                        // Berechnete Felder
                        top3: (parseInt(cells[3].textContent.trim()) || 0) + 
                              (parseInt(cells[4].textContent.trim()) || 0) + 
                              (parseInt(cells[5].textContent.trim()) || 0),
                        top3_quote: ((parseInt(cells[3].textContent.trim()) || 0) + 
                                    (parseInt(cells[4].textContent.trim()) || 0) + 
                                    (parseInt(cells[5].textContent.trim()) || 0)) / 
                                    (parseInt(cells[2].textContent.trim()) || 1)
                    });
                }
            });

            return data;
        });

        console.log(`✅ ${jockeyData.length} Jockeys extrahiert
`);

        // Statistiken
        const totalStarts = jockeyData.reduce((sum, j) => sum + j.starts, 0);
        const avgSiegquote = jockeyData.reduce((sum, j) => sum + j.siegquote, 0) / jockeyData.length;
        const topJockey = jockeyData.reduce((max, j) => j.siegquote > max.siegquote ? j : max, jockeyData[0]);

        console.log('📊 Statistiken:');
        console.log(`   - Jockeys: ${jockeyData.length}`);
        console.log(`   - Gesamtstarts: ${totalStarts}`);
        console.log(`   - Ø Siegquote: ${(avgSiegquote * 100).toFixed(1)}%`);
        console.log(`   - Top Jockey (Siegquote): ${topJockey.jockey} (${(topJockey.siegquote * 100).toFixed(0)}%)
`);

        // CSV erstellen
        const csvHeader = 'Platz,Jockey,Starts,Siege,Platz2,Platz3,Siegquote,Platzquote,Top3,Top3_Quote,Gewichtung
';
        const csvRows = jockeyData.map(j => {
            // Gewichtung nach Sample-Size (ungefähre Konfidenz)
            const gewichtung = Math.min(j.starts / 100, 1.0);
            return `${j.platz},"${j.jockey}",${j.starts},${j.siege},${j.platz2},${j.platz3},${j.siegquote.toFixed(4)},${j.platzquote.toFixed(4)},${j.top3},${j.top3_quote.toFixed(4)},${gewichtung.toFixed(2)}`;
        }).join('
');

       sed -i 's/fs.writeFileSync if/fs.writeFileSync(/g' Jockey.scraper.js
sed -i 's/);$/);/' Jockey.scraper.js
node Jockey.scraper.js
}
        (OUTPUT_CSV, csvHeader + csvRows, 'utf8');
        console.log(`✅ CSV gespeichert: ${OUTPUT_CSV}`);

        // JSON erstellen (mit Metadaten)
        const output = {
            meta: {
                source: TARGET_URL,
                scraped_at: new Date().toISOString(),
                jockeys_count: jockeyData.length,
                total_starts: totalStarts
            },
            jockeys: jockeyData
        };
if (!fs.existsSync(OUTPUT_DIR)) {
    fs.mkdirSync(OUTPUT_DIR);
}
fs.writeFileSync(OUTPUT_CSV, csvHeader + csvRows, 'utf8');
        console.log(`✅ JSON gespeichert: ${OUTPUT_JSON}
`);

        return jockeyData;

    } catch (error) {
        console.error('❌ Fehler:', error.message);
        throw error;
    } finally {
        await browser.close();
    }
}

// Ausführung
scrapeJockeyData()
    .then(() => console.log('🎉 Scraping abgeschlossen'))
    .catch(() => process.exit(1));
rm Jockey.scraper.js
curl -fsSL https://gist.githubusercontent.com/perplexity-ai/fixed-jockey-scraper/raw/jockey.scraper.js

