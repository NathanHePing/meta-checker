// scripts/shard-run.js
'use strict';

(async () => {

const minimist = require('minimist');
const { fork, spawn } = require('child_process');
const fs = require('fs');
const path = require('path');
const os = require('os');
const { chromium } = require('playwright');

const { seedBuckets, appendToBuckets } = require('../src/discover/frontier');
const { makeLogger } = require('../src/utils/log');
const { sanitizePathPrefix } = require('../src/config');
const { parseCsv } = require('../src/io/csv');

const tmod = require('../src/utils/telemetry');
const telemetry = tmod.telemetry || tmod;

// ----------------------- argv -----------------------
const argv = minimist(process.argv.slice(2), {
  boolean: ['keepPageParam','dropCache','rebuildLinks','open','headless','override'],
  string:  ['input','base','pathPrefix','excelDelimiter','followup','outDir','telemetryPort'],
  default: {
    pathPrefix: '/',
    excelDelimiter: 'comma',
    bucketParts: 1,
    shards: os.cpus().length,
    shardCap: 0,
    concurrency: 4,
    telemetryPort: process.env.TELEMETRY_PORT || 7077,
    open: true,
  }
});

// Resolve paths early so we can share with telemetry
const inputPath = argv.input ? path.resolve(String(argv.input)) : '';
const outDirAbs = path.resolve(String(argv.outDir || 'dist'));
const cfgPath = path.join(outDirAbs, 'telemetry', 'config.json');

// Let telemetry know what we’re launching with (for /preflight CSV sniffing)
if (typeof tmod.setLaunchContext === 'function') {
  tmod.setLaunchContext({ inputPath, outDir: outDirAbs });
}

// -------------------- start telemetry --------------------
const requestedPort = Number(argv.telemetryPort || process.env.TELEMETRY_PORT || 7077);
const TELEMETRY_PORT = telemetry.startServer({ port: requestedPort, autoOpen: !!argv.open }) || requestedPort;
process.env.TELEMETRY_PORT = String(TELEMETRY_PORT);

  function readConfigFile() {
    try { return JSON.parse(fs.readFileSync(cfgPath, 'utf8')); } catch { return null; }
  }

// wait loop in shard-run.js
async function sleep(ms){ return new Promise(r => setTimeout(r, ms)); }
async function stopRequested() {
  const port = Number(process.env.TELEMETRY_PORT || 0);
  if (!port) return false;
  try {
    const r = await fetch(`http://127.0.0.1:${port}/stop-state`, { cache: 'no-store' });
    if (!r.ok) return false;
    const j = await r.json();
    return !!(j && j.stop);
  } catch { return false; }
}

console.log('[orchestrator] waiting for output selection in the web UI…');
while (true) {
  try {
    const r = await fetch(`http://127.0.0.1:${TELEMETRY_PORT}/preflight`, { cache: 'no-store' });
    if (!r.ok) throw 0;
    const j = await r.json();
    if (Array.isArray(j.outputs) && j.outputs.length) {
      if (j.started === true) {
        process.env.META_ONLY_REPORTS = j.outputs.join(',');
        const m = j.meta || {};

        if (m.base) argv.base = m.base;
        if (m.prefix != null) argv.pathPrefix = m.prefix;
        if (m.outDir) argv.outDir = m.outDir;
        if (typeof m.keepPageParam === 'boolean') argv.keepPageParam = m.keepPageParam;

        // NEW: prod + maxShards
        if (typeof m.prod === 'boolean') argv.prod = m.prod;
        if (typeof m.maxShards === 'boolean') argv.maxShards = m.maxShards;

        if (+m.shards > 0) argv.shards = +m.shards;
        if (+m.bucketParts > 0) argv.bucketParts = +m.bucketParts;
        if (m.inputPath) argv.input = m.inputPath;

        if (typeof m.headless === 'boolean') {
          process.env.PLAYWRIGHT_HEADLESS = m.headless ? '1' : '';
        }
        break;
      }
    }
  } catch {}
  await sleep(300);
}

//Keep!!!
const cfg = {
  base: argv.base,
  pathPrefix: sanitizePathPrefix(argv.pathPrefix || ''),
  keepPageParam: !!argv.keepPageParam,
  shards: Math.min(+argv.shardCap || Infinity, Math.max(1, +argv.shards || os.cpus().length)),
  outDir: path.resolve(String(argv.outDir || 'dist')),
  childEntry: path.resolve('../index.js'),
};

// --- Apply "maxShards" if requested by UI
if (argv.maxShards === true || String(argv.maxShards) === 'true') {
  cfg.shards = Math.max(1, os.cpus().length);
}

// buckets: 2*shards if shards>1, else 1 (unless explicitly provided)
let bucketParts = Number(argv.bucketParts || 0);
if (!bucketParts) bucketParts = (cfg.shards > 1 ? (2 * cfg.shards) : 1);

// --- Prod (safe) mode policy
if (argv.prod === true || String(argv.prod) === 'true') {
  // Lower pressure:
  //  - reduce child concurrency to 1
  //  - optional: cap shards to half the CPUs (min 1)
  //  - add a global polite delay for frontier claims (used by frontier.js)
  cfg.shards = 1;                 // prod = single shard
  argv.concurrency = 1;           // and single-page concurrency
  process.env.MC_POLITE_DELAY_MS = process.env.MC_POLITE_DELAY_MS || '400';
  process.env.MC_USER_AGENT = process.env.MC_USER_AGENT || 'MetaChecker/1.0 (safe mode; contact=you@example.com)';
  bucketParts = 1;                // one bucket
  // also force page-level concurrency 1 in children below
  argv.concurrency = 1;
}

//debug log to confirm everything
console.log('[orchestrator] final meta:', {
  base: cfg.base,
  pathPrefix: cfg.pathPrefix,
  outDir: cfg.outDir,
  shards: cfg.shards,
  bucketParts,
  keepPageParam: cfg.keepPageParam,
  prod: !!argv.prod,
  maxShards: !!argv.maxShards,
  input: argv.input || ''
});



// optional: start the terminal TUI that reads dist/telemetry/state.json
try {
  const tui = spawn(process.execPath, [path.resolve(__dirname, 'viz-tui.js'), outDirAbs], {
    stdio: 'inherit',
    env: { ...process.env, TELEMETRY_PORT: String(TELEMETRY_PORT) }
  });
  console.log(`[orchestrator] viz-tui started (port ${TELEMETRY_PORT})`);
  tui.on('exit', (code) => console.log(`[orchestrator] viz-tui exited ${code}`));
} catch (e) {
  console.warn('[orchestrator] failed to launch viz-tui:', e && e.message ? e.message : e);
}

// --- keep your viz-tui try/catch above as-is ---

function parseFirstColumn(filePath) {
  try {
    if (!filePath) return { urls: [], explicit: false };
    const rows = parseCsv(filePath, 'auto');
    const firstCell = (row) => Array.isArray(row)
      ? String(row[0] ?? '')
      : row && typeof row === 'object'
        ? String(row[Object.keys(row)[0]] ?? '')
        : '';
    const first = rows.map(firstCell).map(s => s.trim()).filter(Boolean);
    const looksUrl = (s) => /^https?:\/\//i.test(s);
    const good = first.filter(looksUrl);
    return { urls: Array.from(new Set(good)), explicit: first.length > 0 && (good.length / first.length) >= 0.8 };
  } catch {
    return { urls: [], explicit: false };
  }
}

// ---- apply Control Panel meta overrides from telemetry/config.json ----
const file = readConfigFile() || { meta:{} };
const meta = file.meta || {};

// allow UI to override CLI (when provided)
if (typeof meta.base === 'string' && meta.base)                 argv.base = meta.base;
if (typeof meta.prefix === 'string')                            argv.pathPrefix = meta.prefix;
if (typeof meta.outDir === 'string' && meta.outDir)             argv.outDir = meta.outDir;
if (typeof meta.keepPageParam === 'boolean')                    argv.keepPageParam = meta.keepPageParam;
if (typeof meta.shards === 'number' && meta.shards > 0)         argv.shards = meta.shards;
if (typeof meta.bucketParts === 'number' && meta.bucketParts > 0) argv.bucketParts = meta.bucketParts;

// let telemetry know what we're launching (for /preflight CSV sniffing, etc.)
(tmod.setLaunchContext || (()=>{}))({
  inputPath,
  outDir: cfg.outDir,
  base: cfg.base || '',
  pathPrefix: String(argv.pathPrefix || ''),
  bucketParts: Number(bucketParts || 1),
  shardCap: Number(argv.shardCap || cfg.shards || 1),
  concurrency: Number(argv.concurrency || 1),
});

// now proceed…
const T0 = Date.now();

telemetry.setStepper(['seed-scan','bootstrap-frontier','spawn-workers','merge-urls','dedupe','cleanup','done'], 0);
telemetry.step(0);

if (!cfg.base) { console.error('✖ Missing required flag: --base'); process.exit(1); }
fs.mkdirSync(cfg.outDir, { recursive: true });

const origin   = new URL(cfg.base).origin;
const prefix   = String(cfg.pathPrefix || '').replace(/^["']|["']$/g, '').replace(/\/+$/, '');
const followup = String(argv.followup || 'none').toLowerCase();

const sniff = parseFirstColumn(inputPath);
const sitemapMode = sniff.explicit && sniff.urls.length > 0;

const log = makeLogger({ shardIndex: 'orchestrator', shards: cfg.shards });
console.log(`[orchestrator] ${new Date().toLocaleTimeString()} CPU=${os.cpus().length} → shards=${cfg.shards}`, {});


  // Common outputs
  const master = path.join(cfg.outDir, 'urls-final.txt');
  try { fs.unlinkSync(master); } catch {}
  for (const f of fs.readdirSync(cfg.outDir)) {
    if (/^urls-final\.part\d+\.json$/i.test(f)) {
      try { fs.unlinkSync(path.join(cfg.outDir, f)); } catch {}
    }
  }


  // ============== 3-col sitemap mode (explicit list) ==============
  if (sitemapMode) {
    console.log('[orchestrator] 3-column input detected → using first column as explicit URL list; skipping discovery.');
    // normalize to absolute within site + optional pathPrefix filter
    const normalized = dedupe(sniff.urls.map(h => {
      try {
        const u0 = new URL(h, origin);
        if (u0.origin !== origin) return null;
        const u = new URL(u0.pathname + u0.search, origin).toString().replace(/\/+$/, '');
        if (prefix && !u0.pathname.startsWith(prefix)) return null;
        return u;
      } catch { return null; }
    }).filter(Boolean));

    // write the explicit list to a single source file
    const urlsFile = path.join(cfg.outDir, `urls-final.source.json`);
    fs.writeFileSync(urlsFile, JSON.stringify(normalized, null, 0), 'utf8');

    telemetry.step('spawn-workers');

    // fan out across N shards; each child slices via --shards/--shardIndex
    const parts = cfg.shards;
    let finished = 0;

    const children = new Set();
    for (let i = 0; i < parts; i++) {
      if (await stopRequested()) break;
      const partOut = path.join(cfg.outDir, `urls-final.part${i + 1}.json`);
      const childArgs = [
        cfg.childEntry,
        '--base', cfg.base,
        '--input', argv.input || 'input.csv', 
        '--concurrency', String(argv.concurrency || 4),
        '--keepPageParam', String(cfg.keepPageParam),
        '--outDir', cfg.outDir,
        '--rebuildLinks', argv.rebuildLinks ? 'true' : 'false',
        '--dropCache',   argv.dropCache   ? 'true' : 'false',
        '--headless', String(!!argv.headless),
        '--mode', 'root-urls',
        '--urlsFile', urlsFile,
        '--pathPrefix', prefix || '/',
        '--workerId', String(i + 1),
        '--workerTotal', String(parts),
        '--urlsOutFile', partOut,
        '--shards', String(parts),
        '--shardIndex', String(i + 1),
      ];

      console.log('[orchestrator] spawn root-urls', childArgs.slice(1).join(' '));
      telemetry.threadStatus({ workerId: i + 1, phase: 'spawn:root-urls', url: '' });

      const child = fork(childArgs[0], childArgs.slice(1), {
        stdio: 'inherit',
        env: { ...process.env, TELEMETRY_PORT: String(TELEMETRY_PORT) }
      });
      children.add(child);
      child.on('exit', async (code) => {
        children.delete(child);
        finished++;
        telemetry.threadStatus({ workerId: i + 1, phase: 'exit:root-urls', done: true, code });
        if (code !== 0) console.error(`Error: worker for root-urls shard ${i+1}/${parts} exited ${code}`);

        // append this shard’s output
        try {
          if (fs.existsSync(partOut)) {
            const urls = JSON.parse(fs.readFileSync(partOut, 'utf8'));
            fs.appendFileSync(master, urls.map(u => `${u}\n`).join(''), 'utf8');
            console.log('[append]', path.basename(partOut), '→', path.relative(process.cwd(), master), `(+${urls.length})`);
          }
        } catch (e) {
          console.warn('[append warn]', String(e && e.message ? e.message : e));
        }

        if (await stopRequested()) {
          for (const c of children) { try { c.kill('SIGINT'); } catch {} }
          console.log('[orchestrator] stop requested — halting sitemap mode');
          return; // leaves telemetry running; user can New Run
}

        if (finished === parts) await finalizeSitemap();
      });
      
    }

    async function finalizeSitemap() {
      telemetry.step('merge-urls');
      const uniqueCount = mergeManifestDedup(master, cfg.outDir);
      telemetry.bump('urlsFound', uniqueCount);

      // Merge existence & working/not-working parts (3-col mode)
      try {
        // working/not-working .part*.txt -> merged .txt
        const collectTxt = (re) =>
          fs.readdirSync(cfg.outDir)
            .filter(f => re.test(f))
            .flatMap(f => fs.readFileSync(path.join(cfg.outDir, f), 'utf8').split(/\r?\n/).filter(Boolean));
        const uniqLines = (arr) => Array.from(new Set(arr));

        const workingAll = uniqLines(collectTxt(/^working-urls\.part\d+\.txt$/i));
        const notWorkingAll = uniqLines(collectTxt(/^not-working-urls\.part\d+\.txt$/i));

        if (workingAll.length) {
          fs.writeFileSync(path.join(cfg.outDir, 'working-urls.txt'), workingAll.join('\n') + '\n', 'utf8');
          console.log('[merge] working-urls.txt', workingAll.length);
        }
        if (notWorkingAll.length) {
          fs.writeFileSync(path.join(cfg.outDir, 'not-working-urls.txt'), notWorkingAll.join('\n') + '\n', 'utf8');
          console.log('[merge] not-working-urls.txt', notWorkingAll.length);
        }

        // url-existence.part*.csv -> url-existence.csv
        const csvParts = fs.readdirSync(cfg.outDir).filter(f => /^url-existence\.part\d+\.csv$/i.test(f));
        if (csvParts.length) {
          const all = [];
          for (const f of csvParts) {
            const lines = fs.readFileSync(path.join(cfg.outDir, f), 'utf8').split(/\r?\n/).filter(Boolean);
            all.push(lines);
          }
          const header = all[0][0];
          const body = new Set(all.flatMap(lines => lines.slice(1)));
          fs.writeFileSync(
            path.join(cfg.outDir, 'url-existence.csv'),
            [header, ...Array.from(body)].join('\n') + '\n',
            'utf8'
          );
          console.log('[merge] url-existence.csv', body.size);
        }

        // url-existence.part*.json -> url-existence.json
        const jsonParts = fs.readdirSync(cfg.outDir).filter(f => /^url-existence\.part\d+\.json$/i.test(f));
        if (jsonParts.length) {
          const map = new Map();
          for (const f of jsonParts) {
            const arr = JSON.parse(fs.readFileSync(path.join(cfg.outDir, f), 'utf8'));
            for (const row of arr) map.set(row.input_url, row);
          }
          const merged = Array.from(map.values());
          fs.writeFileSync(
            path.join(cfg.outDir, 'url-existence.json'),
            JSON.stringify(merged, null, 0),
            'utf8'
          );
          console.log('[merge] url-existence.json', merged.length);

        }
      } catch (e) {
        console.warn('[merge warn]', String(e && e.message ? e.message : e));
      }

      telemetry.step('cleanup');
      const shouldClean = argv.cleanArtifacts !== 'false'; // default true
      if (shouldClean) {
        const out = cfg.outDir;
        const rm = (p) => { try { fs.rmSync(p, { recursive: true, force: true }); console.log('[clean]', path.relative(process.cwd(), p)); } catch (e) { console.warn('[clean warn]', e.message); } };
        for (const f of fs.readdirSync(out)) if (/^urls-final\.part\d+\.json$/i.test(f)) rm(path.join(out, f));
        rm(path.join(out, 'urls-final.txt'));
        rm(path.join(out, 'frontier'));
        rm(path.join(out, 'disco-locks'));
        rm(path.join(out, 'locks'));
        rm(path.join(out, 'frontier.ndjson'));
        if (argv.dropCache === true || argv.dropCache === 'true') {
          try { for (const f of fs.readdirSync(out)) if (/^site_catalog\.part\d+\.json$/i.test(f)) rm(path.join(out, f)); } catch {}
        }
      }

      telemetry.step('done');
      const MS = Date.now() - T0;
      const HH = Math.floor(MS/3600000);
      const MM = Math.floor((MS%3600000)/60000);
      const SS = Math.floor((MS%60000)/1000);
      console.log(`[orchestrator] done in ${HH}h ${MM}m ${SS}s (${MS.toLocaleString()} ms)`);
      // Wait for reset, then allow another run
      await waitForResetThenPreflight();
      return;
    }

    return;
  }

  // ============== Discovery path (bucketed frontier) ==============
  const frontierDir  = path.join(cfg.outDir, 'frontier');
  const discoLocks   = path.join(cfg.outDir, 'disco-locks');
  // Clean previous discovery artifacts to avoid stale .offset / owner files
  try { fs.rmSync(frontierDir, { recursive: true, force: true }); } catch {}
  try { fs.rmSync(discoLocks, { recursive: true, force: true }); } catch {}
  fs.mkdirSync(frontierDir, { recursive: true });
  fs.mkdirSync(discoLocks, { recursive: true });

  const seedUrl0 = new URL((String(cfg.pathPrefix||'').replace(/^["']|["']$/g,'') || '/'), cfg.base).toString();
  seedBuckets(frontierDir, [seedUrl0], bucketParts);

  // --- 1) seed-scan to discover first-level sections ---
  const baseHost = new URL(cfg.base).hostname;
  const etld1 = (host) => host.split('.').slice(-2).join('.'); // good enough for ping.com
  const sameSite = (u) => etld1(u.hostname) === etld1(baseHost);

  const sectionKeyFromPathname = (pn) => {
    const rest = prefix && pn.startsWith(prefix) ? pn.slice(prefix.length) : pn;
    const seg = rest.split('/').filter(Boolean)[0];
    return seg || ''; // '' means "root under /en-us"
  };

  const seedUrl = new URL(prefix || '/', cfg.base).toString();

  telemetry.step('seed-scan');
  console.log(`[orchestrator] seed-scan ${seedUrl} to find first-level sections…`);
  const browser = await chromium.launch({
    headless: process.env.PLAYWRIGHT_HEADLESS === '1',
    args: ['--disable-dev-shm-usage']
  });
  const context = await browser.newContext({
    ignoreHTTPSErrors: true,
    userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119 Safari/537.36',
    locale: 'en-US'
  });
  context.setDefaultNavigationTimeout(30000);
  context.setDefaultTimeout(10000);

  await context.route('**/*', (route) => {
    const u = route.request().url();
    if (/\.(png|jpe?g|gif|webp|svg|ico|woff2?|ttf|otf|mp4|webm|avi|mov)(\?|$)/i.test(u)) return route.abort();
    if (/(googletagmanager|google-analytics|doubleclick|facebook|segment|mixpanel|hotjar)\./i.test(u)) return route.abort();
    return route.continue();
  });

  let sections = [];
  let rootUrls = [];
  let filtered = [];

  try {
    const page = await context.newPage();

    // retry seed navigation up to 3 times with exponential backoff
    for (let attempt = 1; attempt <= 3; attempt++) {
      try {
        await page.goto(seedUrl, { waitUntil: 'domcontentloaded', timeout: 30000, referer: cfg.base });
        break;
      } catch (e) {
        if (attempt === 3) throw e;
        await page.waitForTimeout(800 * attempt);
      }
    }
    try { await page.waitForLoadState('networkidle', { timeout: 3000 }); } catch {}

    // lightweight “reveal”
    const clickMs = 120, hoverMs = 120;
    const clickSafe = async (h) => { try { await h.click({ timeout: 500 }); await page.waitForTimeout(clickMs); } catch {} };
    const hoverSafe = async (h) => { try { await h.hover({ timeout: 500 }); await page.waitForTimeout(hoverMs); } catch {} };
    const menuButtons = await page.$$(
      'button[aria-expanded="false"],button[aria-controls],' +
      '[role="button"][aria-expanded="false"],[data-toggle],[data-action*="menu"],[data-action*="expand"]'
    );
    for (const btn of menuButtons.slice(0, 8)) await clickSafe(btn);
    const hoverables = await page.$$(
      'nav [aria-haspopup="true"], nav .dropdown, nav .menu, .nav [aria-expanded]'
    );
    for (const el of hoverables.slice(0, 16)) await hoverSafe(el);
    try { await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight)); } catch {}
    await page.waitForTimeout(200);

    const links = await page.evaluate(() => {
      const toAbs = (h) => { try { return new URL(h, location.href).href; } catch { return ''; } };
      const bad = /^(javascript:|mailto:|tel:|data:)/i;
      const fromOnclick = (code) => {
        if (!code) return '';
        const m =
          code.match(/(?:window\.open|location\.assign|location\.replace)\(\s*['"]([^'"]+)['"]\s*\)/i) ||
          code.match(/(?:window\.location|location)\s*=\s*['"]([^'"]+)['"]/i) ||
          code.match(/location\.href\s*=\s*['"]([^'"]+)['"]/i);
        return m ? m[1] : '';
      };

      const nodes = Array.from(document.querySelectorAll(
        'a[href], [role="link"], button, [role="button"], [data-href], [data-url], [onclick]'
      ));
      const out = [];
      for (const el of nodes) {
        if (el.hasAttribute('href')) {
          const raw = el.getAttribute('href') || '';
          if (!raw || bad.test(raw)) continue;
          out.push(toAbs(raw.split('#')[0]));
        } else if (el.hasAttribute('data-href') || el.hasAttribute('data-url')) {
          const raw = el.getAttribute('data-href') || el.getAttribute('data-url') || '';
          if (!raw || bad.test(raw)) continue;
          out.push(toAbs(raw.split('#')[0]));
        } else if (el.hasAttribute('onclick')) {
          const dest = fromOnclick(el.getAttribute('onclick') || '');
          if (dest && !bad.test(dest)) out.push(toAbs(dest.split('#')[0]));
        }
      }
      return out;
    });

    filtered = links.map(href => {
      try {
        const u = new URL(href);
        if (!sameSite(u)) return null;
        const n = new URL(u.pathname + u.search, origin);
        return n.toString();
      } catch { return null; }
    }).filter(Boolean).filter(href => {
      try { const u = new URL(href); return !prefix || u.pathname.startsWith(prefix); }
      catch { return false; }
    });

    const secSet = new Set();
    for (const href of filtered) {
      const pn = new URL(href).pathname;
      const key = sectionKeyFromPathname(pn);
      if (key) secSet.add(key);
      else rootUrls.push(new URL(href).toString().split('#')[0]);
    }
    sections = Array.from(secSet);
    rootUrls = dedupe(rootUrls);
  } catch (e) {
    console.warn('[orchestrator] seed-scan failed, continuing with root-only crawl:', String(e && e.message ? e.message : e));
    sections = [];
    rootUrls = [seedUrl];
    filtered = [];
  } finally {
    try { await context.close(); } catch {}
    try { await browser.close(); } catch {}
  }

  if (!sections.length && !rootUrls.length) rootUrls = [seedUrl];

  console.log(`[orchestrator] sections found: ${sections.length} ${JSON.stringify(sections.slice(0, 20))}${sections.length>20?'…':''}`);
  if (rootUrls.length) console.log(`[orchestrator] root URLs under ${prefix}: ${rootUrls.length}`);

  // --- bootstrap frontier ---
  const sectionUrls = sections.map(s => new URL(`${prefix}/${s}`.replace(/\/+/g,'/'), cfg.base).toString());
  const bootstrapSeeds = Array.from(new Set([seedUrl, ...rootUrls, ...sectionUrls, ...filtered]));
  if (bootstrapSeeds.length) {
    seedBuckets(frontierDir, [seedUrl0], bucketParts);
    telemetry.step('bootstrap-frontier');
    console.log('[orchestrator] bootstrap frontier +', bootstrapSeeds.length, 'seeds');
  }

  // --- tasks: N frontier workers ---
  const parts = cfg.shards;
  const children = new Set();
  const tasks = Array.from({ length: parts }, (_, i) => ({
    kind: 'frontier',
    workerId: i + 1,
    argv: [
      '--mode', 'frontier',
      '--pathPrefix', String(cfg.pathPrefix||'').replace(/^["']|["']$/g,''),
      '--frontierDir', frontierDir,
      '--discoLocks', discoLocks,
      '--partIndex', String(i),
      '--partTotal', String(parts),
      '--bucketParts', String(bucketParts),
      '--workerId', String(i + 1),
      '--workerTotal', String(parts),
      '--urlsOutFile', path.join(cfg.outDir, `urls-final.part${i+1}.json`),
    ]
  }));


  // optional: section follow-up
  if (followup === 'sections' && sections.length) {
    let partCounter = parts;
    for (const sec of sections) {
      partCounter++;
      const sectionPrefix = `${prefix}/${sec}`.replace(/\/+/g, '/');
      const urlsOutFile = path.join(cfg.outDir, `urls-final.part${partCounter}.json`);
      tasks.push({
      kind: 'section', section: sec, workerId: partCounter, argv: [
        '--mode', 'frontier',
        '--pathPrefix', sectionPrefix,
        '--frontierDir', frontierDir,
        '--discoLocks', discoLocks,
        '--bucketParts', String(bucketParts),
        '--partIndex', String(0),
        '--partTotal', String(1),
        '--urlsOutFile', urlsOutFile,
        '--workerId', String(partCounter),
        '--workerTotal', String(sections.length + parts),
      ]
    });
    }
  }

  telemetry.step('spawn-workers');

  // --- pool ---
  const maxParallel = Math.min(cfg.shards, tasks.length);
  let running = 0, idx = 0;

  async function launchNext() {
    if (await stopRequested()) return;
    if (idx >= tasks.length) return;
    const t = tasks[idx++];
    running++;

    const childArgs = [
    cfg.childEntry,
    '--base', cfg.base,
    '--input', argv.input || 'input.csv',
    '--concurrency', String(argv.concurrency || 4),
    '--keepPageParam', String(cfg.keepPageParam),
    '--outDir', cfg.outDir,
    '--rebuildLinks', argv.rebuildLinks ? 'true' : 'false',
    '--dropCache',   argv.dropCache   ? 'true' : 'false',
    ...t.argv
  ];

    console.log('[orchestrator] spawn', t.kind, t.section ? t.section : '', childArgs.slice(1).join(' '));
    telemetry.threadStatus({ workerId: t.workerId, phase: `spawn:${t.kind}`, url: t.section || '' });

    const child = fork(childArgs[0], childArgs.slice(1), {
      stdio: 'inherit',
      env: { ...process.env, TELEMETRY_PORT: String(TELEMETRY_PORT) }
    });
    children.add(child);

    child.on('exit', async (code) => {
      children.delete(child);
      running--;
      telemetry.threadStatus({ workerId: t.workerId, phase: `exit:${t.kind}`, done: true, code });
      if (code !== 0) console.error(`Error: worker for ${t.kind}${t.section?`:${t.section}`:''} exited ${code}`);

      // Append this worker's URLs
      try {
          const outArgIdx = t.argv.findIndex(a => a === '--urlsOutFile');
          const fileArgIdx = t.argv.findIndex(a => a === '--urlsFile');
          const file = outArgIdx >= 0 ? t.argv[outArgIdx + 1]
                    : fileArgIdx >= 0 ? t.argv[fileArgIdx + 1]
                          : null;
        if (file && fs.existsSync(file)) {
          const urls = JSON.parse(fs.readFileSync(file, 'utf8'));
          fs.appendFileSync(master, urls.map(u => `${u}\n`).join(''), 'utf8');
          console.log('[append]', path.basename(file), '→', path.relative(process.cwd(), master), `(+${urls.length})`);
        }
      } catch (e) {
        console.warn('[append warn]', String(e && e.message ? e.message : e));
      }

      if (idx < tasks.length) {
        launchNext();
      } else if (running === 0) {

        if (await stopRequested()) {
          for (const c of children) { try { c.kill('SIGINT'); } catch {} }
          console.log('[orchestrator] stop requested — skipping merge/cleanup');
          telemetry.step('done');
          console.log('[orchestrator] stop requested — halted');
          await waitForResetThenPreflight();
          return;
        }

        telemetry.step('merge-urls');
        const uniqueCount = mergeManifestDedup(master, cfg.outDir);

        telemetry.bump('urlsFound', uniqueCount);

        console.log('[orchestrator] done');

        telemetry.step('cleanup');
        const shouldClean = argv.cleanArtifacts !== 'false'; // default true
        if (shouldClean) {
          const out = cfg.outDir;
          const rm = (p) => { try { fs.rmSync(p, { recursive: true, force: true }); console.log('[clean]', path.relative(process.cwd(), p)); } catch (e) { console.warn('[clean warn]', e.message); } };

          for (const f of fs.readdirSync(out)) {
            if (/^urls-final\.part\d+\.json$/i.test(f)) rm(path.join(out, f));
          }
          try { fs.unlinkSync(master); } catch {}

          rm(path.join(out, 'frontier'));
          rm(path.join(out, 'disco-locks'));
          rm(path.join(out, 'locks'));
          rm(path.join(out, 'frontier.ndjson'));

          if (argv.dropCache === true || argv.dropCache === 'true') {
            try {
              for (const f of fs.readdirSync(out)) {
                if (/^site_catalog\.part\d+\.json$/i.test(f)) rm(path.join(out, f));
              }
            } catch {}
          }
        }

        telemetry.step('done');
        const MS = Date.now() - T0;
        const HH = Math.floor(MS/3600000);
        const MM = Math.floor((MS%3600000)/60000);
        const SS = Math.floor((MS%60000)/1000);
        console.log(`[orchestrator] done in ${HH}h ${MM}m ${SS}s (${MS.toLocaleString()} ms)`);
        await waitForResetThenPreflight();
      }
    });
  }

  for (let i = 0; i < maxParallel; i++) launchNext();
})().catch(e => {
  console.error('orchestrator fatal:', (e && e.stack) || e);
  process.exit(1);
});

async function waitForResetThenPreflight() {
  const port = Number(process.env.TELEMETRY_PORT || 0);
  if (!port) return;
  const sleep = (ms) => new Promise(r => setTimeout(r, ms));
  let needResetAck = true;
  console.log('[orchestrator] idle — click "New Run" in the UI when ready');
  while (needResetAck) {
    try {
      const r = await fetch(`http://127.0.0.1:${port}/reset-wait`, { cache: 'no-store' });
      if (r && r.ok) needResetAck = false;
    } catch {}
    await sleep(600);
  }
  await waitForPreflightReady();
}
