// src/run.js
'use strict';

const { chromium } = require('playwright');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const tmod = require('./utils/telemetry');
const telemetry = tmod.telemetry || tmod;
// Check the stop flag that the telemetry server writes
function stopRequested(outDir) {
  try {
    const f = path.join(outDir || 'dist', 'telemetry', 'stop.flag');
    return fs.existsSync(f);
  } catch { return false; }
}


const { extractInternalLinks } = require('./extract/links');
const { parseCsv } = require('./io/csv');
const { writeReports, writeTreeReport } = require('./io/reports');

const { loadCache, saveCache } = require('./cache/file-cache');
const { nowIso, daysSince } = require('./utils/time');
const { normalizeText, normalizeUrl } = require('./match/normalize');

const { discoverBySitemap } = require('./discover/sitemap');
const { crawlSite } = require('./discover/crawler');
const { makeLogger } = require('./utils/log');


function sha1(s) { return crypto.createHash('sha1').update(String(s)).digest('hex'); }

// simple file-lock per URL; returns { file, release() } or null if already claimed
function claimUrl(locksDir, key) {
  try {
    fs.mkdirSync(locksDir, { recursive: true });
    const f = path.join(locksDir, `${sha1(key)}.lock`);
    const fd = fs.openSync(f, 'wx'); // exclusive create
    fs.writeFileSync(fd, JSON.stringify({ pid: process.pid, key, at: new Date().toISOString() }) + '\n');
    return { file: f, release: () => { try { fs.closeSync(fd); fs.unlinkSync(f); } catch {} } };
  } catch (e) {
    if (e && e.code === 'EEXIST') return null;
    return null;
  }
}

// ------------------- Input-shape sniffers -------------------
const isUrlish = (s) => typeof s === 'string' && (/^(https?:)?\/\//i.test(s) || s.startsWith('/'));
const wordCount = (s) => (String(s || '').trim().split(/\s+/).filter(Boolean).length);

function firstCell(row) {
  if (Array.isArray(row)) return (row[0] ?? '').toString();
  if (row && typeof row === 'object') {
    const k = Object.keys(row)[0];
    return (row[k] ?? '').toString();
  }
  return '';
}

function secondCell(row) {
  if (Array.isArray(row)) return (row[1] ?? '').toString();
  if (row && typeof row === 'object') {
    const ks = Object.keys(row);
    return (row[ks[1]] ?? '').toString();
  }
  return '';
}

function sniffInputShape(rows) {
  const n = rows.length;
  if (!n) return { mode: 'no-input', cols: 0, firstColUrlShare: 0, firstRowIsUrl: false };
  const sample = rows[0];
  const cols = Array.isArray(sample) ? sample.length : Object.keys(sample || {}).length;
  const firstCol = rows.map(firstCell);
  const firstColUrlShare = (firstCol.filter(isUrlish).length) / Math.max(1, firstCol.length);
  const firstRowIsUrl = isUrlish(firstCol[0]);
  return { mode: 'has-input', cols, firstColUrlShare, firstRowIsUrl };
}

function sniffInputForExplicitUrls(inputPath, origin, prefix) {
  try {
    const rows = parseCsv(inputPath, 'auto');
    if (!rows || rows.length === 0) return { explicit: false, urls: [] };

    const isObj = rows.length && !Array.isArray(rows[0]);
    const headers = isObj ? Object.keys(rows[0] || {}) : [];
    const hasUrlHeader = headers.some(k => /^url$/i.test(k));

    const firstCol = rows.map(r =>
      Array.isArray(r) ? String(r[0] ?? '').trim()
      : r && typeof r === 'object' ? String(r[headers[0]] ?? '').trim()
      : ''
    ).filter(Boolean);

    const abs = firstCol.filter(s => /^https?:\/\//i.test(s));
    const ratio = firstCol.length ? abs.length / firstCol.length : 0;

    const looksExplicit = hasUrlHeader || ratio >= 0.9;
    if (!looksExplicit) return { explicit: false, urls: [] };

    const normalized = Array.from(new Set(abs.map(h => {
      try {
        const u0 = new URL(h, origin);
        if (u0.origin !== origin) return null;
        const u = new URL(u0.pathname + u0.search, origin).toString().replace(/\/+$/, '');
        if (prefix && !u0.pathname.startsWith(prefix)) return null;
        return u;
      } catch { return null; }
    }).filter(Boolean)));

    if (normalized.length === 0) return { explicit: false, urls: [] };

    return { explicit: true, urls: normalized };
  } catch {
    return { explicit: false, urls: [] };
  }
}

// Decide whether a free-text column is more like titles or descriptions.
function inferRoleForSingleText(rows, pages) {
  // quick heuristic by average word count
  const sampleN = Math.min(50, rows.length);
  const avgWords = rows.slice(0, sampleN).map(firstCell).map(wordCount).reduce((a, b) => a + b, 0) / Math.max(1, sampleN);
  let role = (avgWords <= 12 ? 'title' : 'description');

  // optional refinement based on small sample overlap with crawled content
  try {
    if (pages && pages.length) {
      let titleHits = 0, descHits = 0, considered = 0;
      for (const r of rows) {
        const q = normalizeText(firstCell(r));
        if (!q) continue;
        considered++;
        const anyTitle = pages.some(p => (p.titleN || '').includes(q));
        const anyDesc = pages.some(p => normalizeText(p.description || '').includes(q));
        if (anyTitle) titleHits++;
        if (anyDesc) descHits++;
        if (considered >= 8) break;
      }
      if (titleHits >= descHits + 2) role = 'title';
      else if (descHits >= titleHits + 2) role = 'description';
    }
  } catch {}
  return role; // 'title' | 'description'
}

// helpers for the existence report
const writeCsv = (file, headers, rows) => {
  const sep = ',';
  const esc = (v) => {
    const s = (v == null ? '' : String(v));
    return /[",\r\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
  };
  fs.writeFileSync(file, [headers.join(sep), ...rows.map(r => headers.map(h => esc(r[h])).join(sep))].join('\n'), 'utf8');
};
const writeJson = (file, obj) => fs.writeFileSync(file, JSON.stringify(obj, null, 2), 'utf8');

async function run(cfg) {
  const argvModeMatch = process.argv.join(' ').match(/--mode\s+(\S+)/);
  const argvMode = argvModeMatch ? argvModeMatch[1] : '';
  const runMode = String((cfg && cfg.mode) || argvMode || '').toLowerCase();
  const forceFrontier = runMode === 'frontier';
  // sensible defaults for paths used below
  const tag = (cfg.workerId ?? cfg.shardIndex ?? 1);
  const cacheDir = path.join(cfg.outDir || 'dist', 'cache');
  const locksDir = cfg.locksDir || path.join(cfg.outDir || 'dist', 'locks');
  cfg.cachePath = cfg.cachePath || path.join(cacheDir, `fetch-cache.part${tag}.json`);
  cfg.locksDir = locksDir;

  // telemetry: mark worker row
  telemetry.threadStatus({ workerId: cfg.workerId, phase: 'init', url: '' });

  const t0 = Date.now();
  const log = makeLogger({
    shardIndex: (cfg.workerId ?? cfg.shardIndex) || 1,
    shards: (cfg.workerTotal ?? cfg.shards) || 1
  });

  fs.mkdirSync(cfg.outDir, { recursive: true });
  fs.mkdirSync(path.dirname(cfg.cachePath), { recursive: true });

  const baseOrigin = new URL(cfg.base).origin;
  const maxCrawlPagesRaw = Number.isFinite(cfg.maxCrawlPages) ? Number(cfg.maxCrawlPages) : 50000;
    // Interpret 0 or negative as "no cap"
    const maxCrawlPages = (maxCrawlPagesRaw <= 0 ? Number.POSITIVE_INFINITY : maxCrawlPagesRaw);

  log.info('start', {
    base: cfg.base,
    pathPrefix: cfg.pathPrefix,
    keepPageParam: !!cfg.keepPageParam,
    sameOrigin: cfg.sameOrigin !== false,
    rebuildLinks: !!cfg.rebuildLinks,
    dropCache: !!cfg.dropCache
  });

  // Optional: drop cache + reset locks
  if (cfg.dropCache) {
    try { fs.unlinkSync(cfg.cachePath); log.info('cache dropped', { cache: cfg.cachePath }); } catch {}
    try { fs.rmSync(cfg.locksDir, { recursive: true, force: true }); } catch {}
    fs.mkdirSync(cfg.locksDir, { recursive: true });
  }

  // Load CSV if it exists (some modes run with no input).
  let rows = [];
  let inputExists = false;
  try {
    if (cfg.input && fs.existsSync(cfg.input)) {
      inputExists = true;
      rows = parseCsv(cfg.input, 'auto'); // tolerant header handling
      // --- UI: show the first line of the input so users can confirm ---
    try {
      const first = rows && rows.length ? rows[0] : null;
      const preview = first
        ? Array.isArray(first)
            ? first.slice(0, 3).join(' | ')
            : Object.values(first).slice(0, 3).join(' | ')
        : '(empty)';
      telemetry.threadStatus({ workerId: cfg.workerId, phase: 'input-confirm', url: `Input: ${path.basename(cfg.input)} — ${preview}` });
    } catch {}

    }
  } catch (e) {
    log.warn('Failed to read input CSV; continuing as no-input', { err: String(e && e.message ? e.message : e) });
  }

  // If orchestrator passed a urlsFile, use it (highest priority)
  let urls = [];
  const usingUrlsFile = (cfg.urlsFile && fs.existsSync(cfg.urlsFile));
  if (usingUrlsFile) {
    urls = JSON.parse(fs.readFileSync(cfg.urlsFile, 'utf8'));
    log.info('Loaded shard URLs from file', { file: cfg.urlsFile, count: urls.length });
  }

  // ------------------- Input mode resolution -------------------
  let sitemapMode = false;            // explicit root-URL list (3+ col or 1-col URLs)
  let explicitInputUrls = [];
  let comparisonEnabled = true;       // some modes skip comparison
  let rowsForReports = rows;          // may be transformed for reporter
  let roleHint = null;                // 'title' | 'description' | 'auto-second-col' | null

  let shape = { mode: 'no-input', cols: 0, firstColUrlShare: 0, firstRowIsUrl: false };
  if (!usingUrlsFile) {
    const { explicit, urls: sniffedUrls } =
      sniffInputForExplicitUrls(cfg.input, baseOrigin, cfg.pathPrefix);

    if (!forceFrontier && explicit) {
      sitemapMode = true;
      explicitInputUrls = sniffedUrls;
      comparisonEnabled = false; // skip comparison for explicit URL list
    } else {
      shape = sniffInputShape(rows);
    }

    if (!inputExists || shape.mode === 'no-input') {
      // NEW MODE A: no input file -> discovery-only; skip comparison
      comparisonEnabled = false;
    } else {
      if (!forceFrontier && shape.cols >= 3) {
        // existing behavior: treat first column as explicit URL list
        sitemapMode = true;
        explicitInputUrls = rows.map(firstCell).filter(Boolean);
      } else if (shape.cols === 1) {
        const firstCol = rows.map(firstCell).filter(Boolean);
        if (!forceFrontier && shape.firstColUrlShare >= 0.6) {
          // NEW MODE B: 1 column of URLs -> explicit list; skip comparison
          sitemapMode = true;
          comparisonEnabled = false;
          explicitInputUrls = firstCol;
        } else {
          // NEW MODE C: 1 column of text -> treat like 2-col by inferring role
          roleHint = inferRoleForSingleText(rows, []);
          rowsForReports = rows.map(r => {
            const v = firstCell(r);
            return (roleHint === 'title')
              ? { expectedTitle: v, expectedDescription: '' }
              : { expectedTitle: '', expectedDescription: v };
          });
        }
      } else if (shape.cols === 2) {
           if (!forceFrontier && (shape.firstRowIsUrl || shape.firstColUrlShare >= 0.6)) {
          // NEW MODE D: 2-col where col-1 looks like URLs -> explicit; infer col-2 role
          sitemapMode = true;
          explicitInputUrls = rows.map(firstCell).filter(Boolean);
          roleHint = 'auto-second-col';
        } else {
          // Original 2-col: title, description
          rowsForReports = rows.map(r => ({
            expectedTitle: firstCell(r),
            expectedDescription: secondCell(r)
          }));
        }
      }
    }
  }

  try {
    telemetry.event({
      type: 'mode',
      sitemapMode: !!sitemapMode,
      usingUrlsFile: !!usingUrlsFile,
      urlCount: Array.isArray(urls) ? urls.length : 0
    });
  } catch {}

  // Inform telemetry about the mode
  try {
    telemetry.setMode(!inputExists ? 'no-input' : sitemapMode ? 'explicit-urls' : 'discovery');
    telemetry.step('discover');
    telemetry.event({ type: 'base', base: cfg.base, pathPrefix: cfg.pathPrefix || '/' });
  } catch {}

  // === LOG: mode resolution snapshot ===
  log.info('mode resolution', {
    usingUrlsFile,
    sitemapMode,
    inputExists,
    shape,
    explicitInputUrls: (explicitInputUrls || []).length,
    roleHint
  });

  // === LOG: mode resolution snapshot ===
  log.info('mode resolution', {
    usingUrlsFile,
    sitemapMode,
    inputExists,
    shape,
    explicitInputUrls: (explicitInputUrls || []).length,
    roleHint
  });

  if (!usingUrlsFile && sitemapMode) {
  console.log('Explicit URL input detected → using first column as explicit URL list; skipping discovery.', {
    rows: rows?.length ?? 0,
    uniqueCandidates: explicitInputUrls?.length ?? 0
  });

  // normalize + same-origin + pathPrefix + canonicalize
  urls = Array.from(new Set(explicitInputUrls.map(h => {
    try {
      const u0 = new URL(h, baseOrigin);
      if (u0.origin !== baseOrigin) return null; // same-site only
      const norm = normalizeUrl(u0.toString(), { keepPageParam: cfg.keepPageParam });
      const u = new URL(norm);
      if (cfg.pathPrefix && !u.pathname.startsWith(cfg.pathPrefix)) return null;
      return norm.replace(/\/+$/, '');
    } catch { return null; }
  }).filter(Boolean)));

  fs.writeFileSync(path.join(cfg.outDir, 'urls-from-input.txt'), urls.join('\n'), 'utf8');
  log.info('explicit-urls/normalize/done', { kept: urls.length, sample: urls.slice(0,5) });
}


  // If still no URLs, do normal discovery: sitemap → (optional) fallback crawl
if (!usingUrlsFile && !sitemapMode) {
  log.info('discover/sitemap/start', { base: cfg.base, pathPrefix: cfg.pathPrefix || '' });
     let discovered = [];
  try {
    discovered = await discoverBySitemap(cfg.base);
    log.info('discover/sitemap/done', { discovered: discovered.length, sample: (discovered || []).slice(0,5) });
    
    try { telemetry.event({ type: 'sitemap/discovered', count: discovered.length }); } catch {}
  } catch (e) {
    log.warn('sitemap discovery failed', { err: String(e?.message || e) });
    try { telemetry.event({ type: 'sitemap/error', msg: String(e?.message || e) }); } catch {}
  }

  fs.writeFileSync(
    path.join(cfg.outDir, 'urls-raw.txt'),
    (discovered || []).join('\n'),
    'utf8'
  );

  if (!discovered.length) {
    log.info('discover/crawl/start', {
      msg: 'No usable pages from sitemap; crawling (fallback)…',
      partIndex: cfg.partIndex, partTotal: cfg.partTotal, bucketParts: cfg.bucketParts
    });

    const browser2 = await chromium.launch({ headless: true, args: ['--disable-dev-shm-usage'] });
    const context2 = await browser2.newContext({ ignoreHTTPSErrors: true, locale: 'en-US' });
    context2.setDefaultTimeout(10000);
    await context2.route('**/*', (route) => {
      const u = route.request().url();
      if (/\.(png|jpe?g|gif|webp|svg|ico|woff2?|ttf|otf|mp4|webm|avi|mov)(\?|$)/i.test(u)) return route.abort();
      if (/(googletagmanager|google-analytics|doubleclick|facebook|segment|mixpanel|hotjar)\./i.test(u)) return route.abort();
      return route.continue();
    });

    // (optional) mark crawl start so the UI can show a spinner
    try { telemetry.event({ type: 'crawl/start' }); } catch {}

    try {
      discovered = await crawlSite(context2, cfg.base, {
        pathPrefix: cfg.pathPrefix,
        maxPages: maxCrawlPages,
        keepPageParam: cfg.keepPageParam,
        logger: log,
        frontierFile: cfg.frontierFile,
        frontierDir: cfg.frontierDir,
        discoLocks: cfg.discoLocks,
        partIndex: Number(cfg.partIndex || 0),
        partTotal: Number(cfg.partTotal || 1),
        bucketParts: cfg.bucketParts,
        outDir: cfg.outDir,
        workerId: cfg.workerId,
        workerTotal: cfg.workerTotal,
      });

      // emit *after* crawlSite returns, with the actual count
      log.info('discover/crawl/done', { discovered: discovered.length, sample: (discovered || []).slice(0,5) });
      
      try { telemetry.event({ type: 'crawl/discovered', count: discovered.length }); } catch {}
    } catch (e) {
      log.warn('crawl fallback failed', { err: String(e?.message || e) });
      try { telemetry.event({ type: 'crawl/error', msg: String(e?.message || e) }); } catch {}
    } finally {
      await context2.close().catch(() => {});
      await browser2.close().catch(() => {});
    }
  }

    urls = Array.from(new Set((discovered || []).map(u => {
      try {
        const abs = new URL(u, baseOrigin).toString();
        const norm = normalizeUrl(abs, { keepPageParam: cfg.keepPageParam });
        const uu = new URL(norm);
        if (cfg.sameOrigin !== false && uu.origin !== baseOrigin) return null;
        if (cfg.pathPrefix && !uu.pathname.startsWith(cfg.pathPrefix)) return null;
        return norm.replace(/\/+$/, '');
      } catch { return null; }
    }).filter(Boolean)));

    // Final scope-to-prefix for output/reporting only
    if (cfg.pathPrefix) {
      urls = urls.filter(u => {
        try { return new URL(u).pathname.startsWith(cfg.pathPrefix); } catch { return false; }
      });

    } 
    log.info('discover/normalize+scope', { postScope: urls.length, pathPrefix: cfg.pathPrefix || '', sample: urls.slice(0,5) });
    try { fs.writeFileSync(path.join(cfg.outDir, 'urls-after-scope.txt'), urls.join('\n'), 'utf8'); } catch {}
     
    try { telemetry.event({ type: 'urls/normalized', count: urls.length }); } catch {}
  } 

  // ── /ANCHOR: frontier_mode_top ─────────────────────────────────────────────

  // Detect "existence only" from META_ONLY_REPORTS and short-circuit fetch.
  const onlyReports = String(process.env.META_ONLY_REPORTS || '')
    .split(',').map(s => s.trim()).filter(Boolean);
  const existenceOnly = sitemapMode &&
    onlyReports.length === 1 &&
    (onlyReports[0] === 'existence_csv' || onlyReports[0] === 'existence');

  if (existenceOnly) {
    // Minimal: HTTP probe only; no rendering, no link extraction.
    const { writeFileSync } = require('fs');
    const existenceProbe = new Map();
    const req = await chromium.request.newContext({ ignoreHTTPSErrors: true });
    try {
      for (const inputUrl of urls) {
        if (stopRequested(cfg.outDir)) break;
        let ok = false, status = 0, finalUrl = '';
        try {
          const res = await req.get(inputUrl, { timeout: 12000, maxRedirects: 5 });
          ok = res.ok();
          status = res.status();
          finalUrl = res.url();
       } catch {}
        existenceProbe.set(inputUrl, { exists: !!ok, status, final_url: finalUrl || '' });
      }
    } finally {
      try { await req.dispose(); } catch {}
    }

    // Emit the same existence artifacts the crawler path writes.
    // (csv/json + working/not-working lists)
    const normalizedInputs = Array.from(new Set(urls));
    const rows = normalizedInputs.map(u => {
      const hit = existenceProbe.get(u);
      return {
        input_url: u,
        exists: hit && hit.exists ? 'true' : 'false',
        http_status: hit ? hit.status : 0,
        final_url: hit ? hit.final_url : ''
      };
    });
    const okCount = rows.filter(r => r.exists === 'true').length;
    const badCount = rows.length - okCount;
    try { telemetry.event({ type: 'existence/summary', ok: okCount, bad: badCount }); } catch {}

    const tag = (cfg.workerId ?? cfg.shardIndex ?? 1);
    const outCsv  = path.join(cfg.outDir, `url-existence.part${tag}.csv`);
    const outJson = path.join(cfg.outDir, `url-existence.part${tag}.json`);
    const toCsv = (headers, rs) => {
      const esc = s => /[",\r\n]/.test(s) ? `"${String(s).replace(/"/g,'""')}"` : String(s);
     const head = headers.join(',');
      const body = rs.map(r => headers.map(h => esc(r[h] ?? '')).join(',')).join('\n');
      return head + (body ? '\n' + body : '') + '\n';
    };
    writeFileSync(outCsv, toCsv(['input_url','exists','http_status','final_url'], rows), 'utf8');
    writeFileSync(outJson, JSON.stringify(rows, null, 2), 'utf8');

    // working/not-working master + per-shard lists
    const workingLines   = rows.filter(r => r.exists === 'true')
                              .map(r => `${r.final_url || r.input_url},${r.http_status}`);
    const notWorkingLines= rows.filter(r => r.exists !== 'true')
                               .map(r => `${r.input_url},${r.http_status}`);
    if (workingLines.length)
      writeFileSync(path.join(cfg.outDir, 'working-urls.txt'), workingLines.join('\n') + '\n', 'utf8');
    if (notWorkingLines.length)
      writeFileSync(path.join(cfg.outDir, 'not-working-urls.txt'), notWorkingLines.join('\n') + '\n', 'utf8');
    writeFileSync(path.join(cfg.outDir, `working-urls.part${tag}.txt`),
      rows.filter(r => r.exists === 'true' && (r.final_url || r.input_url))
          .map(r => r.final_url || r.input_url).join('\n') + '\n', 'utf8');
    writeFileSync(path.join(cfg.outDir, `not-working-urls.part${tag}.txt`),
      rows.filter(r => r.exists !== 'true').map(r => r.input_url).join('\n') + '\n', 'utf8');

   // done — skip the heavy fetch/crawl entirely
    try { telemetry.threadStatus({ workerId: cfg.workerId, phase: 'done', url: '' }); } catch {}
    return;
  }
  // ── /ANCHOR: existence_only_gate_top ─────────────────────────────────────────

  // Shard slice (if needed)
  if (cfg.shards > 1) {
    urls.sort();
    const per = Math.ceil(urls.length / cfg.shards);
    const start = per * (cfg.shardIndex - 1);
    urls = urls.slice(start, start + per);
    log.info('Sharded URL slice', { shard: `${cfg.shardIndex}/${cfg.shards}`, count: urls.length });
    try {
      telemetry.event({
        type: 'urls/sharded',
        shard: cfg.shardIndex,
        shards: cfg.shards,
        count: urls.length
      });
    } catch {}
  }

  // Emit this worker's (sharded) URL list for orchestrator, if requested
  if (cfg.urlsOutFile) {
    try {
      fs.writeFileSync(cfg.urlsOutFile, JSON.stringify(urls, null, 0), 'utf8');
      log.info('Wrote urlsOutFile', { file: cfg.urlsOutFile, count: urls.length });
      telemetry.bump('urlsFound', urls.length);
    } catch (e) {
      log.warn('Failed to write urlsOutFile', { file: cfg.urlsOutFile, err: String(e && e.message ? e.message : e) });
    }
  }

  log.info('URLs ready', { count: urls.length, sample: urls.slice(0, 5) });

  // Empty slice → write empty cache and finish
  if (!urls.length) {
    saveCache(cfg.cachePath, {});
    log.warn('empty-url-slice', {
      usingUrlsFile,
      sitemapMode,
      inputExists,
     explicitInputUrls: (explicitInputUrls || []).length
    });
    log.warn('Empty URL slice → wrote empty cache part and exited OK.');
    return;
  }

  // ---------- FETCH STAGE ----------
  telemetry.step('fetch');
  telemetry.threadStatus({ workerId: cfg.workerId, phase: 'fetch', url: '' });

  const browser = await chromium.launch({ headless: true, args: ['--disable-dev-shm-usage'] });
  const context = await browser.newContext({
    ignoreHTTPSErrors: true,
    locale: 'en-US',
    userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119 Safari/537.36'
  });
  await context.route('**/*', (route) => {
    const u = route.request().url();
    if (/\.(png|jpe?g|gif|webp|svg|ico|woff2?|ttf|otf|mp4|webm|avi|mov)(\?|$)/i.test(u)) return route.abort();
    if (/(googletagmanager|google-analytics|doubleclick|facebook|segment|mixpanel|hotjar)\./i.test(u)) return route.abort();
    return route.continue();
  });

  // fast HTTP reachability probe (no rendering)
  async function httpProbe(url, timeoutMs = 12000) {
    try {
      const res = await context.request.get(url, { timeout: timeoutMs, maxRedirects: 5 });
      return { ok: res.ok(), status: res.status(), finalUrl: res.url() };
    } catch {
      return { ok: false, status: 0, finalUrl: '' };
    }
  }

  const cache = loadCache(cfg.cachePath);
  let toFetch = [];
  let fromCache = [];

  if (cfg.rebuildLinks) {
    toFetch = urls.slice();
    fromCache = [];
    try {
    telemetry.event({ type: 'fetch/plan', toFetch: toFetch.length, fromCache: fromCache.length });
  } catch {}
    log.info('REBUILD_LINKS active → fetching all URLs');
  } else {
    for (const u of urls) {
      const c = cache[u];
      if (cfg.forceRefresh || !c || daysSince(c.lastFetched) > cfg.cacheTtlDays) toFetch.push(u);
      else fromCache.push({ url: u, ...c });
    }
    log.info('Catalog reuse', { cached: fromCache.length, toFetch: toFetch.length });
    try {
      telemetry.event({ type: 'fetch/plan', toFetch: toFetch.length, fromCache: fromCache.length });
    } catch {}
    log.info('fetch/plan/details', {
      toFetchSample: toFetch.slice(0, 5),
      fromCacheSample: fromCache.slice(0, 5).map(p => p.url)
    });
  }

  // For existence report when in sitemapMode
  const existenceProbe = new Map(); // normalized_input_url -> { exists, status, final_url }

  const out = [];
  const conc = Math.max(1, Number(cfg.concurrency || 4));
  for (let i = 0; i < toFetch.length; i += conc) {
  if (stopRequested(cfg.outDir)) {
    try { telemetry.threadStatus({ workerId: cfg.workerId, phase: 'stopped', url: '' }); } catch {}
    break;
  }
    const slice = toFetch.slice(i, i + conc);
    await Promise.all(slice.map(async (seedUrl) => {
      if (stopRequested(cfg.outDir)) return;
      const seedKey = normalizeUrl(seedUrl, { keepPageParam: cfg.keepPageParam });
      const seedClaim = claimUrl(cfg.locksDir, seedKey);
      if (!seedClaim) return; // someone else has it

      let finalClaim;
      const page = await context.newPage();
      // === LOG: Playwright signal for hard failures ===
      page.on('requestfailed', req => {
        try { log.warn('requestfailed', { url: req.url(), failure: req.failure()?.errorText }); } catch {}
      });
      page.on('response', res => {
        try {
          const s = res.status();
          if (s >= 400) log.warn('http', { url: res.url(), status: s });
        } catch {}
      });
      page.on('console', msg => {
       try {
          if (msg.type() === 'error') log.warn('console.error', { text: msg.text() });
        } catch {}
      });
      try {
        let final = seedUrl;
        let status = 0;
        let ok = false;
        let meta = { title: '', description: '' };

        // Update live thread status for visualizer
        try { telemetry.threadStatus({ workerId: (cfg.workerId ?? cfg.shardIndex ?? 1), phase: 'fetch', url: seedUrl }); } catch {}

        // quick commit-first nav, then escalate to DOM if needed
        for (let attempt = 1; attempt <= 2; attempt++) {
          try {
            const nav1 = await page.goto(seedUrl, { waitUntil: 'commit', timeout: 8000 });
            status = nav1 ? nav1.status() : 0;
            ok = !!nav1 && nav1.ok();
            if (ok) {
              try { await page.waitForLoadState('domcontentloaded', { timeout: 7000 }); } catch {}
              try { await page.waitForLoadState('networkidle', { timeout: 1500 }); } catch {}
            }
            meta = await page.evaluate(() => {
              const by = (sel) => document.querySelector(sel)?.getAttribute('content') || '';
              const title =
                by('meta[name="title"]') ||
                by('meta[property="og:title"]') ||
                document.title || '';
              const description =
                by('meta[name="description"]') ||
                by('meta[property="og:description"]') ||
                '';
              return { title, description };
            });
            final = page.url();
            // --- UI: contribute to the live Tree view ---
            try {
              const u = new URL(final);
              const segs = u.pathname.replace(/^\/+|\/+$/g, '').split('/').filter(Boolean);
              if (segs.length && telemetry.treeAdd) telemetry.treeAdd(segs);
            } catch {}

            break;
          } catch (e) {
            if (attempt === 2) {
              // fall through to HTTP probe
            } else {
              await page.waitForTimeout(600 * attempt);
            }
          }
        }

        // If nav timed out (status 0 or !ok), run a cheap HTTP probe
        if (!ok || status === 0) {
          const probe = await httpProbe(seedUrl, 12000);
          if (probe.ok && probe.status > 0) {
            ok = true;
            status = probe.status;
            final = probe.finalUrl || final;
          }
        }

        // Upgrade claim to final URL (after redirects)
        const finalKey = normalizeUrl(final, { keepPageParam: cfg.keepPageParam });
        if (finalKey !== seedKey) {
          finalClaim = claimUrl(cfg.locksDir, finalKey);
          if (!finalClaim) return; // someone else already processed final URL
        }

        // pathPrefix guard after redirects
        if (cfg.pathPrefix && !new URL(finalKey).pathname.startsWith(cfg.pathPrefix)) {
          if (sitemapMode) existenceProbe.set(seedKey, { exists: false, status, final_url: finalKey });
          return;
        }

        const titleN = normalizeText(meta.title);

        // Extract internal links once we have a good page
        const links = await extractInternalLinks(page, {
          origin: baseOrigin,
          pathPrefix: cfg.pathPrefix,
          sameOrigin: cfg.sameOrigin !== false,
          keepPageParam: cfg.keepPageParam,
          max: 50000,
          interactive: true,
        });

        try {
          telemetry.event({ type: 'visited', url: finalKey });
          telemetry.bump('internalEdges', Array.isArray(links) ? links.length : 0);
        } catch {}

        const linksArr = Array.isArray(links) ? links : [];
        const normLinks = linksArr.map(l => (typeof l === 'string'
          ? { url: l, text: '', kind: 'extracted' }
          : l));
        const rec = { url: finalKey, title: meta.title, description: meta.description, titleN, links: normLinks };
        out.push(rec);
        cache[finalKey] = { ...rec, lastFetched: nowIso() };

        if (sitemapMode) {
          existenceProbe.set(seedKey, { exists: !!ok, status, final_url: finalKey });
        }
      } catch (e) {
        if (sitemapMode) existenceProbe.set(seedKey, { exists: false, status: 0, final_url: '' });
        log.warn('fetch fail', { url: seedUrl, err: String(e && e.message ? e.message : e) });
      } finally {
        try { await page.close(); } catch {}
        try { if (finalClaim) finalClaim.release(); } catch {}
        try { seedClaim.release(); } catch {}
        try { telemetry.threadStatus({ workerId: (cfg.workerId ?? cfg.shardIndex ?? 1), phase: 'fetched', url: final || seedUrl }); } catch {}
      }
    }));

    // Persist after each batch
    saveCache(cfg.cachePath, cache);
    const done = Math.min(i + conc, toFetch.length);
    log.info('batch saved', { done, total: toFetch.length });
    try { telemetry.event({ type: 'fetch/progress', done, total: toFetch.length }); } catch {}
  }

  // Merge reused cache entries into out
  for (const p of fromCache) {
    if (!cache[p.url]) cache[p.url] = { ...p, lastFetched: p.lastFetched || nowIso() };
    out.push({ url: p.url, title: p.title, description: p.description, titleN: p.titleN, links: p.links || [] });
  }

  // If we deferred role inference for “2 cols but first cell is URL”, do it now and transform rows.
  if (roleHint === 'auto-second-col') {
    const role = inferRoleForSingleText(rows.map(r => [secondCell(r)]), out);
    rowsForReports = rows.map(r => {
      const u = firstCell(r);
      const t = secondCell(r);
      return (role === 'title')
        ? { expectedUrl: u, expectedTitle: t, expectedDescription: '' }
        : { expectedUrl: u, expectedTitle: '', expectedDescription: t };
    });
  }

  // emit the hierarchical tree files now that `out` is complete
  await writeTreeReport(cfg.outDir, out.map(p => p.url), cfg.pathPrefix || '');


  // ---------- REPORTS ----------
  telemetry.step('reports');

  // If initial single-col text guess was made before fetching, refine with page data
  if (!sitemapMode && roleHint && (roleHint === 'title' || roleHint === 'description')) {
    const role = inferRoleForSingleText(rows, out);
    rowsForReports = rows.map(r => {
      const v = firstCell(r);
      return (role === 'title')
        ? { expectedTitle: v, expectedDescription: '' }
        : { expectedTitle: '', expectedDescription: v };
    });
  }

  if (comparisonEnabled) {
    await writeReports({ ...cfg, onlyReports }, out, rowsForReports);
  } else {
    log.info('comparison skipped for this input mode');
    // emit lightweight site_catalog when comparison is off
    const cat = out.map(p => ({
      url: p.url,
      title: p.title || '',
      description: p.description || ''
    }));
    if (cat.length) {
      writeCsv(
        path.join(cfg.outDir, `site_catalog.part${tag}.csv`),
        ['url', 'title', 'description'],
        cat
      );
      log.info('report', { file: `site_catalog.part${tag}.csv`, rows: cat.length });
    }
  }

  // Also emit a lightweight internal-links file from the extracted links
  try {
    const ilPath = path.join(cfg.outDir, `internal-links.part${tag}.ndjson`);
      for (const p of out) {
        const links = Array.isArray(p.links) ? p.links : [];
        for (const l of links) {
          fs.appendFileSync(ilPath, JSON.stringify({
            page_url: p.url,
            link_url: l.url || String(l),
            link_text: l.text || '',
            kind: l.kind || 'extracted'
          }) + '\n', 'utf8');
        }
      }

    log.info('report', { file: path.basename(ilPath), edges: out.reduce((n, p) => n + (p.links?.length || 0), 0) });
  } catch (e) {
    log.warn('internal-links emit failed', { err: String(e && e.message ? e.message : e) });
  }

  // Existence report + working/not-working lists (only in explicit list mode)
  if (sitemapMode) {
    const normalizedInputs = Array.from(new Set((explicitInputUrls || []).map(h => {
      try { return normalizeUrl(new URL(h, baseOrigin).toString(), { keepPageParam: cfg.keepPageParam }); }
      catch { return null; }
    }).filter(Boolean)));

    const existenceRows = normalizedInputs.map(inputUrl => {
      const hit = existenceProbe.get(inputUrl);
      return {
        input_url: inputUrl,
        exists: hit ? (hit.exists ? 'true' : 'false') : 'false',
        http_status: hit ? hit.status : 0,
        final_url: hit ? hit.final_url : ''
      };
    });

    const okCount = existenceRows.filter(r => r.exists === 'true').length;
    const badCount = existenceRows.length - okCount;
    try { telemetry.event({ type: 'existence/summary', ok: okCount, bad: badCount }); } catch {}

    const outCsv = path.join(cfg.outDir, `url-existence.part${tag}.csv`);
    const outJson = path.join(cfg.outDir, `url-existence.part${tag}.json`);
    writeCsv(outCsv, ['input_url', 'exists', 'http_status', 'final_url'], existenceRows);
    writeJson(outJson, existenceRows);
    log.info('report', { file: outCsv, rows: existenceRows.length });

    // also write working/not-working master lists WITH status (url,status)
    try {
      const workingLines = existenceRows
        .filter(r => r.exists === 'true')
        .map(r => `${r.final_url || r.input_url},${r.http_status}`);
      const brokenLines = existenceRows
        .filter(r => r.exists !== 'true')
        .map(r => `${r.input_url},${r.http_status}`);
      if (workingLines.length)
        fs.writeFileSync(path.join(cfg.outDir, 'working-urls.txt'), workingLines.join('\n') + '\n', 'utf8');
      if (brokenLines.length)
        fs.writeFileSync(path.join(cfg.outDir, 'not-working-urls.txt'), brokenLines.join('\n') + '\n', 'utf8');
      log.info('report', { file: 'working-urls.txt', count: workingLines.length });
      log.info('report', { file: 'not-working-urls.txt', count: brokenLines.length });
    } catch (e) {
      log.warn('writing working/not-working failed', { err: String(e && e.message ? e.message : e) });
    }

    // per-shard working / not-working lists
    const working = Array.from(new Set(
      existenceRows.filter(r => r.exists === 'true' && r.final_url).map(r => r.final_url)
    ));
    const notWorking = Array.from(new Set(
      existenceRows.filter(r => r.exists !== 'true').map(r => r.input_url)
    ));
    if (working.length) fs.writeFileSync(path.join(cfg.outDir, `working-urls.part${tag}.txt`), working.join('\n') + '\n', 'utf8');
    if (notWorking.length) fs.writeFileSync(path.join(cfg.outDir, `not-working-urls.part${tag}.txt`), notWorking.join('\n') + '\n', 'utf8');
    log.info('report', { working: working.length, not_working: notWorking.length });
  }

  await context.close().catch(() => {});
  await browser.close().catch(() => {});

  const ms = Date.now() - t0;
  const hh = Math.floor(ms / 3600000);
  const mm = Math.floor((ms % 3600000) / 60000);
  const ss = Math.floor((ms % 60000) / 1000);
  const hms = `${hh}h ${mm}m ${ss}s`;

  try {
    telemetry.event({
      type: 'worker/done',
      fetched: out.length - fromCache.length,
      reused: fromCache.length
    });
  } catch {}


  log.info('worker finished', { elapsed_ms: ms, elapsed: hms });
  log.info('done');
  telemetry.threadStatus({ workerId: cfg.workerId, phase: 'done', url: '' });
}

module.exports = { run };
