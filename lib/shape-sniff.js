// src/lib/shape-sniff.js
// UTF-8 BOM-safe, delimiter auto-detect, URL-ish heuristics, shape summary.

const URLISH_RE = /^(?:https?:)?\/\/[^\s]+|^\/(?!\/)/i; // absolute or slash-rooted
const BOM_RE = /^\uFEFF/;
const DELIMS = [",", "\t", ";"];

function stripBOM(s) {
  return s.replace(BOM_RE, "");
}

function detectDelimiter(sampleText) {
  // Try the 3 candidates; pick by max stdev of splits across first 50 lines.
  const lines = sampleText.split(/\r?\n/).slice(0, 50).map(l => l.trim()).filter(Boolean);
  if (lines.length === 0) return ",";
  let best = { delim: ",", score: -1 };
  for (const d of DELIMS) {
    const counts = lines.map(l => l.split(d).length);
    const avg = counts.reduce((a,b)=>a+b,0)/counts.length;
    const varc = counts.reduce((a,b)=>a + Math.pow(b-avg,2),0)/counts.length;
    const score = varc; // higher variance means delimiter is segmenting consistently
    if (score > best.score) best = { delim: d, score };
  }
  return best.delim;
}

function splitRow(row, delim) {
  // naive split: your pipeline can swap to a stronger CSV parser later
  return row.split(delim).map(s => s.trim());
}

function isUrlish(s) {
  return URLISH_RE.test(String(s || "").trim());
}

function avgLen(arr) {
  const a = arr.map(x => (x || "").length);
  return a.length ? a.reduce((p,c)=>p+c,0)/a.length : 0;
}

function sniffCsvShape(text) {
  const raw = stripBOM(text || "");
  const delim = detectDelimiter(raw);
  const rows = raw.split(/\r?\n/).map(l => l.trim()).filter(Boolean)
    .map(l => splitRow(l, delim));
  const first50 = rows.slice(0, 50);
  const nonEmpty = first50.filter(r => r.some(c => String(c||"").trim().length>0));
  const cols = nonEmpty.length ? Math.max(...nonEmpty.map(r => r.length)) : 0;

  // Normalize each row to 'cols' length
  const padded = nonEmpty.map(r => r.concat(Array(Math.max(0, cols - r.length)).fill("")));
  const firstColUrlShare = padded.length
    ? padded.filter(r => isUrlish(r[0])).length / padded.length
    : 0;
  const firstRowIsUrl = padded.length ? isUrlish(padded[0][0]) : false;

  // Inference by column count
  const inferredRoles = [];
  if (cols >= 3) {
    inferredRoles.push(["url","title","description"]);
  } else if (cols === 2) {
    const col1Url = firstColUrlShare >= 0.6;
    const col2Avg = avgLen(padded.map(r => r[1]));
    const col1Avg = avgLen(padded.map(r => r[0]));
    if (col1Url) {
      if (col2Avg < 120) inferredRoles.push(["url","title"]);
      else inferredRoles.push(["url","description"]);
    } else {
      inferredRoles.push(["title","description"]);
    }
  } else if (cols === 1) {
    const urlShare = firstColUrlShare;
    if (urlShare >= 0.6) {
      inferredRoles.push(["url"]);
    } else {
      const avg = avgLen(padded.map(r => r[0]));
      if (avg < 120) inferredRoles.push(["title"]); else inferredRoles.push(["description"]);
    }
  }

  return {
    exists: Boolean(rows.length),
    cols,
    delim,
    firstColUrlShare,
    firstRowIsUrl,
    inferredRoles
  };
}

module.exports = {
  stripBOM,
  detectDelimiter,
  splitRow,
  isUrlish,
  avgLen,
  sniffCsvShape,
};
