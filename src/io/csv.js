// src/io/csv.js
const fs = require('fs');

function splitCsvLine(line){
  const re = /("([^"]|"")*"|[^,]+)|(?<=,)(?=,)|^,(?=,)|,(?=$)/g;
  const matches = line.match(re) || [];
  return matches.map(m => {
    const t = m.trim();
    if (t.startsWith('"') && t.endsWith('"')) return t.slice(1,-1).replace(/""/g, '"');
    return t;
  });
}

// returns array of { expectedUrl?, expectedTitle, expectedDesc }
function parseCsv(file, hasHeader='auto'){
  const raw = fs.readFileSync(file, 'utf8');
  const lines = raw.split(/\r?\n/).filter(l => l.trim().length);
  if (!lines.length) return [];
  let start = 0;
  if (hasHeader === 'true' || (hasHeader === 'auto' && /url|title|description/i.test(lines[0]))) start = 1;
  const rows = [];
  for (let i=start; i<lines.length; i++){
    const cells = splitCsvLine(lines[i]);
    if (cells.length >= 3){
      const [u,t,d] = cells;
      rows.push({ expectedUrl: (u||'').trim(), expectedTitle: (t||'').trim(), expectedDesc: (d||'').trim() });
    } else if (cells.length >= 2){
      const [t,d] = cells;
      rows.push({ expectedUrl: '', expectedTitle: (t||'').trim(), expectedDesc: (d||'').trim() });
    }
  }
  return rows;
}
module.exports = { parseCsv };
