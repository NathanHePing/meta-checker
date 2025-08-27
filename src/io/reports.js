// src/io/reports.js
const fs = require('fs');
const path = require('path');
const { normalizeText } = require('../match/normalize');
const { findByPrefixThenSimilarity } = require('../match/matcher');

const MAX_TITLE = 60;
const MAX_DESC  = 160;

// --- CSV helpers (Excel-friendly but configurable) ---
function csvEscape(val, sep) {
  let s = val == null ? '' : String(val);
  s = s.replace(/\r?\n/g, ' ');
  if (sep === '\t') return s;
  return /[",]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
}
function writeCsv(file, headers, rows, opts = {}) {
  const sep = (opts.delimiter === 'tab') ? '\t' : ',';
  const includeBom = opts.includeBom !== false;
  const line = (arr) => arr.map(h => csvEscape(h, sep)).join(sep);
  const head = line(headers);
  const body = rows.map(r => line(headers.map(h => r[h]))).join('\r\n');
  const out  = head + (rows.length ? '\r\n' + body : '') + '\r\n';
  const final = (includeBom ? '\uFEFF' : '') + out;
  fs.writeFileSync(file, final, 'utf8');
}

const withinLimit = (s, max) => (s || '').length <= max;

async function writeReports(cfg, pages, rows) {
  const only = Array.isArray(cfg.onlyReports) && cfg.onlyReports.length ? new Set(cfg.onlyReports) : null;
  const allow = (k) => (!only || only.has(k));
  const needCompare = allow('comparison_csv');
  const needLinks   = allow('internal_links');

  const out = (f) => path.join(cfg.outDir, f);
  const csvOpts = { delimiter: cfg.excelDelimiter, includeBom: true };

  // Debug catalog dump
  if (needCompare) {
    const pagesCsv = [...pages].sort((a,b)=> a.url.localeCompare(b.url));
    fs.writeFileSync(out('titles-debug.csv'),
      pagesCsv.map(p => `"${(p.title||'').replace(/"/g,'""')}","${p.url}"`).join('\n'),
      'utf8'
    );
  }

  // Duplicate titles
  const titleMap = new Map();
  for (const p of pages) {
    if (!titleMap.has(p.titleN)) titleMap.set(p.titleN, []);
    titleMap.get(p.titleN).push(p.url);
  }
  const duplicateRows = [];
  for (const [t, list] of titleMap.entries()) {
    if (list.length > 1) duplicateRows.push({ title_normalized: t, url_count: list.length, urls: list.join(' | ') });
  }

  // Internal links JSON + CSV
  const linksMap = pages.map(p => ({ url: p.url, links: Array.isArray(p.links) ? p.links : [] }));

  const internalLinkRows = [];
  for (const p of linksMap) {
    for (const l of p.links) {
      let rel = '';
      try { const u = new URL(l.url); rel = u.pathname + (u.search || ''); } catch { rel = l.url; }
      internalLinkRows.push({
        page_url: p.url,
        link_url: l.url,
        link_path: rel,
        label: (l.text || '').trim(),
        kind: l.kind || l.type || ''
      });
    }
  }

  if (needLinks) {
    writeCsv(out('internal-links.csv'), ['page_url','link_url','link_path','label','kind'], internalLinkRows, csvOpts);
    fs.writeFileSync(out('internal-links.json'), JSON.stringify(linksMap, null, 2), 'utf8');
  }

  // Extras (pages on site not in input)
  const expectedTitles = new Set(rows.map(r => normalizeText(r.expectedTitle)));
  const expectedPairs  = new Set(rows.map(r => `${normalizeText(r.expectedTitle)}|||${normalizeText(r.expectedDesc)}`));
  const extrasRows = [];
  for (const p of pages) {
    const tN = normalizeText(p.title);
    const dN = normalizeText(p.description || '');
    const key = `${tN}|||${dN}`;
    const present = cfg.extrasMode === 'pair' ? expectedPairs.has(key) : expectedTitles.has(tN);
    if (!present) {
      extrasRows.push({ url: p.url, title_normalized: tN, description_normalized: dN, title_len: tN.length, desc_len: dN.length, mode: cfg.extrasMode });
    }
  }

  // Matching & classification
  const pageByUrl = new Map(pages.map(p => [p.url, p]));

  const correctRows = [];
  const mismatchDescOnlyRows = [];
  const mismatchOtherRows = [];
  const notFoundRows  = [];
  const ambiguousRows = [];

  const baseRow = ({ url, matched_by, expected_title, found_title, expected_desc, found_desc }) => {
    const expected_title_len = (expected_title || '').length;
    const found_title_len    = (found_title || '').length;
    const expected_desc_len  = (expected_desc  || '').length;
    const found_desc_len     = (found_desc     || '').length;

    const title_len_ok = (found_title_len <= MAX_TITLE)  && (expected_title_len <= MAX_TITLE)  ? 'OK'
                         : (found_title_len  > MAX_TITLE ? `found>${MAX_TITLE}` : 'expected>60');

    const desc_len_ok  = (found_desc_len  <= MAX_DESC)   && (expected_desc_len  <= MAX_DESC)   ? 'OK'
                         : (found_desc_len   > MAX_DESC  ? `found>${MAX_DESC}`  : 'expected>160');

    return {
      url, matched_by,
      expected_title, found_title,
      expected_desc,  found_desc,
      expected_title_len, found_title_len,
      expected_desc_len,  found_desc_len,
      title_len_ok, desc_len_ok
    };
  };

  for (const row of rows) {
    const expectedTitleN = normalizeText(row.expectedTitle);
    const expectedDescN  = normalizeText(row.expectedDesc || '');
    const explicitUrl    = (row.expectedUrl || '').trim();

    const expectedTitleLenOK = withinLimit(expectedTitleN, MAX_TITLE);
    const expectedDescLenOK  = withinLimit(expectedDescN,  MAX_DESC);

    if (explicitUrl) {
      const p = pageByUrl.get(explicitUrl);
      if (!p) {
        notFoundRows.push({ type: 'url', expected_url: explicitUrl, expected_title: expectedTitleN, expected_desc: expectedDescN, note: 'Page not discovered/fetched or outside scope' });
        continue;
      }
      const foundTitleN = p.titleN;
      const foundDescN  = normalizeText(p.description || '');
      const titleMatch  = expectedTitleN === foundTitleN;
      const descMatch   = expectedDescN  === foundDescN;

      const base = baseRow({
        url: p.url, matched_by: 'explicit',
        expected_title: expectedTitleN, found_title: foundTitleN,
        expected_desc: expectedDescN,   found_desc: foundDescN
      });

      if (titleMatch && descMatch && expectedTitleLenOK && expectedDescLenOK && foundTitleN.length <= MAX_TITLE && foundDescN.length <= MAX_DESC) {
        correctRows.push(base);
      } else if (titleMatch && !descMatch) {
        mismatchDescOnlyRows.push(base);
      } else {
        mismatchOtherRows.push({ ...base, note: (!titleMatch ? 'title mismatch; ' : '') + (!descMatch ? 'desc mismatch; ' : '') });
      }
      continue;
    }

    // title based
    const match = findByPrefixThenSimilarity(pages, expectedTitleN, { prefixWords: cfg.prefixWords, fuzzyThreshold: cfg.fuzzyThreshold });
    if (match.type === 'none') {
      notFoundRows.push({ type: 'title', expected_url: '', expected_title: expectedTitleN, expected_desc: expectedDescN, note: 'No page matched this title (even with prefix/fuzzy)' });
      continue;
    }
    if (match.items.length > 1) {
      ambiguousRows.push({ match_type: match.type, score: match.score != null ? match.score.toFixed(3) : '', expected_title: expectedTitleN, candidates: match.items.map(x => x.url).join(' | ') });
      continue;
    }

    const p = pageByUrl.get(match.items[0].url);
    const foundTitleN = p.titleN;
    const foundDescN  = normalizeText(p.description || '');
    const titleMatch  = expectedTitleN === foundTitleN;
    const descMatch   = expectedDescN  === foundDescN;
    const matchedBy   = match.type + (match.score != null ? `:${match.score.toFixed(3)}` : '');

    const base = baseRow({
      url: p.url, matched_by: matchedBy,
      expected_title: expectedTitleN, found_title: foundTitleN,
      expected_desc: expectedDescN,   found_desc: foundDescN
    });

    if (titleMatch && descMatch && expectedTitleLenOK && expectedDescLenOK && foundTitleN.length <= MAX_TITLE && foundDescN.length <= MAX_DESC) {
      correctRows.push(base);
    } else if (titleMatch && !descMatch) {
      mismatchDescOnlyRows.push(base);
    } else {
      mismatchOtherRows.push({ ...base, note: (!titleMatch ? 'title mismatch; ' : '') + (!descMatch ? 'desc mismatch; ' : '') });
    }
  }

  // CSV outputs → dist
if (needCompare ) {
  writeCsv(out('duplicate-titles.csv'),      ['title_normalized','url_count','urls'], duplicateRows, csvOpts);
  writeCsv(out('extras-not-in-input.csv'),   ['url','title_normalized','description_normalized','title_len','desc_len','mode'], extrasRows, csvOpts);
  writeCsv(out('not-found.csv'),             ['type','expected_url','expected_title','expected_desc','note'], notFoundRows, csvOpts);
  writeCsv(out('ambiguous-fuzzy.csv'),       ['match_type','score','expected_title','candidates'], ambiguousRows, csvOpts);
  writeCsv(out('correct.csv'),               ['url','matched_by','expected_title','found_title','expected_title_len','found_title_len','expected_desc','found_desc','expected_desc_len','found_desc_len','title_len_ok','desc_len_ok'], correctRows, csvOpts);
  writeCsv(out('mismatches-desc-only.csv'),  ['url','matched_by','expected_title','found_title','expected_title_len','found_title_len','expected_desc','found_desc','expected_desc_len','found_desc_len','title_len_ok','desc_len_ok'], mismatchDescOnlyRows, csvOpts);
  writeCsv(out('mismatches-other.csv'),      ['url','matched_by','expected_title','found_title','expected_title_len','found_title_len','expected_desc','found_desc','expected_desc_len','found_desc_len','title_len_ok','desc_len_ok','note'], mismatchOtherRows, csvOpts);

  }

  if (needLinks) {
    writeCsv(out('internal-links.csv'), ['page_url','link_url','link_path','label','kind'], internalLinkRows, csvOpts);
    fs.writeFileSync(out('internal-links.json'), JSON.stringify(linksMap, null, 2), 'utf8');
  }


  // Summary (only list what we actually wrote)
  const lines = [
    '=== SUMMARY ==='
  ];

  if (needCompare ) {
    lines.push(
      `Correct rows: ${correctRows.length}`,
      `Titles not found: ${notFoundRows.length}`,
      `Ambiguous/fuzzy matches: ${ambiguousRows.length}`,
      `Mismatches (desc only): ${mismatchDescOnlyRows.length}`,
      `Mismatches (other): ${mismatchOtherRows.length}`,
      `Duplicate titles detected: ${duplicateRows.length}`,
      `Extras (pages on site not in input by ${cfg.extrasMode}): ${extrasRows.length}`,
      '',
      `correct.csv:               ${out('correct.csv')}`,
      `mismatches-desc-only.csv:  ${out('mismatches-desc-only.csv')}`,
      `mismatches-other.csv:      ${out('mismatches-other.csv')}`,
      `not-found.csv:             ${out('not-found.csv')}`,
      `ambiguous-fuzzy.csv:       ${out('ambiguous-fuzzy.csv')}`,
      `duplicate-titles.csv:      ${out('duplicate-titles.csv')}`,
      `extras-not-in-input.csv:   ${out('extras-not-in-input.csv')}`
    );
    if (typeof titlesDebugRows !== 'undefined') {
      lines.push(`titles-debug.csv:          ${out('titles-debug.csv')}`);
    }
  }

  if (needLinks) {
    lines.push(
      '',
      `internal-links.csv:        ${out('internal-links.csv')}`
    );
    if (fs.existsSync(out('internal-links.json'))) {
      lines.push(`internal-links.json:       ${out('internal-links.json')}`);
    }
  }

  lines.push(''); // trailing newline
  fs.writeFileSync(out('summary.txt'), lines.join('\n'), 'utf8');


}

// ---- Hierarchical tree report (added) -------------------------------------
function __buildTree(allUrls, pathPrefix) {
  const root = { name: pathPrefix || '/', children: new Map(), urls: [] };
  const norm = (u) => {
    try {
      const url = new URL(u);
      url.hash = '';
      // keep only ?page param if present; drop others
      const params = new URLSearchParams(url.search);
      const q = new URLSearchParams();
      if (params.has('page')) q.set('page', params.get('page'));
      url.search = q.toString() ? `?${q}` : '';
      return url.toString().replace(/\/+$/, '');
    } catch { return null; }
  };
  const segsOf = (u) => {
    const n = norm(u);
    if (!n) return null;
    const { pathname } = new URL(n);
    if (pathPrefix && !pathname.startsWith(pathPrefix)) return null;
    const rest = pathPrefix ? pathname.slice(pathPrefix.length) : pathname;
    const segs = String(rest || '').replace(/^\/+/, '').split('/').filter(Boolean);
    return segs;
  };

  for (const raw of allUrls || []) {
    const n = norm(raw);
    if (!n) continue;
    const segs = segsOf(n);
    if (segs === null) continue;

    let node = root;
    node.urls.push(n);
    for (const s of segs) {
      if (!node.children.has(s)) {
        node.children.set(s, { name: s, children: new Map(), urls: [] });
      }
      node = node.children.get(s);
      node.urls.push(n);
    }
  }
  return root;
}

function __renderTree(node, prefix = '', isLast = true) {
  const line = prefix + (prefix ? (isLast ? '└─ ' : '├─ ') : '') + node.name;
  const children = [...node.children.keys()].sort().map(k => node.children.get(k));
  const lines = [line];
  const nextPrefix = prefix + (prefix ? (isLast ? '   ' : '│  ') : '');
  children.forEach((child, i) => {
    const last = i === children.length - 1;
    lines.push(...__renderTree(child, nextPrefix, last));
  });
  return lines;
}

/**
 * writeTreeReport(outDir, allUrls, pathPrefix)
 * Produces:
 *   - <outDir>/tree.txt (ASCII hierarchy)
 *   - <outDir>/tree-examples.md (sample reconstructable URLs per branch)
 */
function writeTreeReport(outDir, allUrls, pathPrefix) {
  const fs = require('fs');
  const path = require('path');

  try {
    const tree = __buildTree(allUrls, pathPrefix);
    const ascii = __renderTree(tree, '', true).join('\n');
    fs.writeFileSync(path.join(outDir, 'tree.txt'), ascii, 'utf8');

    const examples = [];
    const visit = (n, segs) => {
      const base = (pathPrefix || '') + (segs.length ? '/' + segs.join('/') : '');
      const sample = (n.urls || []).slice(0, 3);
      if (sample.length) {
        examples.push(['# ' + base, ...sample.map(s => '- ' + s)].join('\n'));
      }
      for (const k of [...n.children.keys()].sort()) visit(n.children.get(k), [...segs, k]);
    };
    visit(tree, []);
    const md = examples.join('\n\n');
    fs.writeFileSync(path.join(outDir, 'tree-examples.md'), md, 'utf8');
  } catch (e) {
    console.warn('[reports] writeTreeReport failed:', e && e.message ? e.message : e);
  }
}
// ---------------------------------------------------------------------------


module.exports = { writeReports, writeTreeReport };

