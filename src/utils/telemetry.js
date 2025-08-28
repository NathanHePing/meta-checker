// src/utils/telemetry.js
'use strict';

const http = require('http');
const os   = require('os');
const { exec } = require('child_process');
const fs   = require('fs');
const path = require('path');

// === Pre-flight selection state & validators =================================
let APPLIED = false;
let STARTED = false;
let LAUNCH = { inputPath: '', outDir: 'dist' };  // set by shard-run via setLaunchContext()
const CONFIG_FILE = () => path.join(LAUNCH.outDir || 'dist', 'telemetry', 'config.json');

const OUTPUTS = [
  { key: 'urls',            label: 'urls',              needs: 'none'   },
  { key: 'site_catalog',    label: 'site_catalog',      needs: 'none'   },
  { key: 'internal_links',  label: 'internal_links',    needs: 'none'   },
  { key: 'tree',            label: 'tree',              needs: 'none'   },
  { key: 'existence_csv',   label: 'existence.csv',     needs: 'any'    }, // needs *any* CSV
  { key: 'comparison_csv',  label: 'comparison.csv',    needs: 'compare'}  // needs 2-col CSV
];

function sniffCsvShape(filePath) {
  try {
    // Harden: coerce to string, trim, and bail if empty or missing on disk
    const p = String(filePath || '').trim();
    if (!p) {
      return { exists: false, cols: 0, firstColUrlShare: 0, firstRowIsUrl: false, inferredRoles: [] };
    }
    try {
      if (!fs.existsSync(p)) {
        return { exists: false, cols: 0, firstColUrlShare: 0, firstRowIsUrl: false, inferredRoles: [] };
      }
    } catch {
      return { exists: false, cols: 0, firstColUrlShare: 0, firstRowIsUrl: false, inferredRoles: [] };
    }

    // Read and trim BOM
    const raw = fs.readFileSync(p, 'utf8').replace(/^\uFEFF/, '');

    // Keep first 50 non-empty rows
    const lines = raw.split(/\r?\n/).filter(l => l.trim()).slice(0, 50);
    if (!lines.length) {
      return { exists: false, cols: 0, firstColUrlShare: 0, firstRowIsUrl: false, inferredRoles: [] };
    }

    // Auto-detect delimiter (comma, tab, semicolon)
    const delims = [',', '\t', ';'];
    let bestDelim = ',', maxCols = 0;
    for (const d of delims) {
      const c = lines[0].split(d).length;
      if (c > maxCols) { maxCols = c; bestDelim = d; }
    }

    const rows = lines.map(l => l.split(bestDelim));
    const cols = maxCols;

    const isUrl = (s) => {
      if (!s) return false;
      const t = String(s).trim();
      return /^(https?:)?\/\//i.test(t) || t.startsWith('/');
    };

    const firstColUrlShare = rows.filter(r => isUrl(r[0] || '')).length / rows.length;
    const firstRowIsUrl    = isUrl(rows[0][0] || '');

    // Infer roles
    let roles = [];
    if (cols === 3) {
      roles = ['url', 'title', 'description'];
    } else if (cols === 2) {
      const col1Url = rows.filter(r => isUrl(r[0] || '')).length / rows.length;
      const col2Url = rows.filter(r => isUrl(r[1] || '')).length / rows.length;
      if (col1Url > 0.6 && col2Url < 0.3) {
        const avgLen = rows.reduce((s, r) => s + String(r[1] || '').length, 0) / rows.length;
        roles = ['url', avgLen < 120 ? 'title' : 'description'];
      } else if (col1Url < 0.3 && col2Url < 0.3) {
        roles = ['title', 'description'];
      } else {
        // Fallback: ambiguous 2-col → no roles (preflight will warn)
        roles = [];
      }
    } else if (cols === 1) {
      if (firstColUrlShare >= 0.6) {
        roles = ['url'];
      } else {
        const avg = rows.reduce((s, r) => s + String(r[0] || '').length, 0) / rows.length;
        roles = [avg < 120 ? 'title' : 'description'];
      }
    }

    return { exists: true, cols, firstColUrlShare, firstRowIsUrl, inferredRoles: roles };
  } catch {
    return { exists: false, cols: 0, firstColUrlShare: 0, firstRowIsUrl: false, inferredRoles: [] };
  }
}


function comparisonEnabledByShape(shape) {
  // allow 2-col CSVs; 1-col cannot support comparison; 3+ col treated as explicit lists (no compare)
  if (!shape.exists) return false;
  if (shape.cols === 2) return true;
  return false;
}

function validateOutputs(selected, shape) {
  const errors = [];
  const sel = Array.isArray(selected) ? selected : [];
  const hasInput = !!(shape && shape.exists);
  const roles = (shape && Array.isArray(shape.inferredRoles)) ? shape.inferredRoles : [];

  // Always-allowed (urls/site_catalog/internal_links/tree) need no checks

  // existence_csv requires input with URL-ish first column (>=60% URL-ish)
  if (sel.includes('existence_csv')) {
    if (!hasInput) {
      errors.push({ key: 'existence_csv', reason: 'No input file found for existence check.' });
    } else if (!roles.includes('url') && !(shape.firstColUrlShare >= 0.6)) {
      errors.push({ key: 'existence_csv', reason: 'First column must look like URLs (>=60%).' });
    }
  }

  // comparison_csv requires input AND at least title or description detected
  if (sel.includes('comparison_csv')) {
    if (!hasInput) {
      errors.push({ key: 'comparison_csv', reason: 'Comparison requires an input file.' });
    } else if (!roles.includes('title') && !roles.includes('description')) {
      errors.push({ key: 'comparison_csv', reason: 'Need a title and/or description column detected.' });
    }
  }

  // If no input at all, proactively forbid those two even if selected
  if (!hasInput) {
    if (sel.includes('existence_csv')) errors.push({ key: 'existence_csv', reason: 'Forbidden when no input file is provided.' });
    if (sel.includes('comparison_csv')) errors.push({ key: 'comparison_csv', reason: 'Forbidden when no input file is provided.' });
  }

  return errors;
}


function setLaunchContext(ctx) {
  LAUNCH = { ...LAUNCH, ...ctx };
  try { snapshot(); } catch {}
}
// ============================================================================


const STATE = {
  startedAt: 0,
  mode: 'init',
  stepper: { steps: ['discover', 'fetch', 'compare'], currentIndex: 0 },
  totals: { urlsFound: 0, internalEdges: 0 },
  threads: {},  // id -> { workerId?, pid?, phase?, url?, idleSwaps?, idleLimit?, seen? }
  buckets: {},  // r -> { owner?, pending?, processed?, last? }
  tree: {},     // depth -> Set(['/seg', '/seg2', ...])
  events: []
};

// ----- remote client (child -> orchestrator) -----
const REMOTE_PORT = Number(process.env.TELEMETRY_PORT || 0);
function postToParent(type, payload) {
  if (!REMOTE_PORT || server) return false; // no remote, or we ARE the server
  try {
    const data = Buffer.from(JSON.stringify({ type, ...payload }), 'utf8');
    const req = http.request(
      { hostname: '127.0.0.1', port: REMOTE_PORT, path: '/update', method: 'POST',
        headers: { 'content-type': 'application/json', 'content-length': data.length } },
      res => { res.resume(); }
    );
    req.on('error', () => {});
    req.write(data); req.end();
    return true;
  } catch { return false; }
}

function step(name) {
  if (postToParent('step', { step: name })) return;
  if (typeof name === 'number') {
    STATE.stepper.currentIndex = Math.max(0, Math.min(name, STATE.stepper.steps.length - 1));
    return;
  }
  if (typeof name === 'string' && name) {
    const idx = STATE.stepper.steps.indexOf(name);
    if (idx >= 0) STATE.stepper.currentIndex = idx;
    else {
      STATE.stepper.steps.push(name);
      STATE.stepper.currentIndex = STATE.stepper.steps.length - 1;
    }
  }
}

// replace your threadStatus with this
function threadStatus(info = {}) {
  if (postToParent('thread', { info })) return;
  if (info == null || (info.workerId == null && info.pid == null)) return;
  const id = info.workerId != null ? String(info.workerId) : String(info.pid);
  const prev = STATE.threads[id] || {};

  const phase = info.phase != null ? info.phase
             : info.stage != null ? info.stage
             : prev.phase || '';

  STATE.threads[id] = {
    ...prev,
    info,
    phase,
    url: info.url || prev.url || '',
  };
}


function bucketOwner(a, b) {
  if (typeof a === 'object' && a && a.bucket != null) {
    const data = { owner: a.owner };
    if (postToParent('bucket', { bucket: a.bucket, data })) return;
    return bucketUpdate(String(a.bucket), data);
  }
  const bucket = a, owner = b;
  if (postToParent('bucket', { bucket, data: { owner } })) return;
  bucketUpdate(bucket, { owner });
}

function bucketProgress(a, b, c) {
  if (typeof a === 'object' && a && a.bucket != null) {
    const bucket = String(a.bucket);
    const data = {};
    if (a.cursor != null && a.size != null) {
      data.processed = Number(a.cursor) || 0;
      data.pending   = Math.max(0, Number(a.size) - Number(a.cursor));
      data.bytes     = Number(a.size) || 0;
      data.cursor    = Number(a.cursor) || 0;
    }
    if (a.processed != null) data.processed = Number(a.processed) || 0;
    if (a.pending   != null) data.pending   = Number(a.pending)   || 0;
    if (a.last) data.last = String(a.last);
    if (postToParent('bucket', { bucket, data })) return;
    return bucketUpdate(bucket, data);
  }

  const bucket = String(a);
  const processed = Number(b || 0);
  const pending   = Number(c || 0);
  if (postToParent('bucket', { bucket, data: { processed, pending } })) return;
  bucketUpdate(bucket, { processed, pending });
}




let server = null;
let PORT   = 0;

function nowHMS() {
  const ms = STATE.startedAt ? (Date.now() - STATE.startedAt) : 0;
  const hh = Math.floor(ms/3600000);
  const mm = Math.floor((ms%3600000)/60000);
  const ss = Math.floor((ms%60000)/1000);
  return `${hh}:${String(mm).padStart(2,'0')}:${String(ss).padStart(2,'0')}`;
}


// ---------- public API ----------
function init(opts = {}) {
  if (server) return PORT;
  PORT = Number(process.env.TELEMETRY_PORT || opts.port || 0); // 0 -> random
  server = http.createServer(handleHttp);
  server.listen(PORT, '127.0.0.1', () => {
    try {
      if (!global.__telemetrySnapshotTimer) {
        global.__telemetrySnapshotTimer = setInterval(() => {
          try { snapshot(); } catch {}
        }, 700);
      }
    } catch {}
    PORT = server.address().port;
    logEvent('telemetry/started', { port: PORT, host: '127.0.0.1' });
    const auto = (opts.open !== false) && (opts.autoOpen !== false); // support either flag
    if (auto) openBrowser(`http://127.0.0.1:${PORT}/`);
  });
  return PORT;
}
const startServer = init;

function stop() {
  try { server && server.close(); } catch {}
  server = null;
}

function setMode(m) { STATE.mode = String(m || ''); }
function setStepper(steps, currentIndex = 0) {
  if (Array.isArray(steps) && steps.length) STATE.stepper.steps = steps.map(String);
  STATE.stepper.currentIndex = Math.max(0, Math.min(currentIndex, STATE.stepper.steps.length - 1));
}
function setPhase(phase) {
  // convenience for single-process runs
  STATE.threads['self'] = { ...(STATE.threads['self']||{}), pid: process.pid, phase: String(phase||'') };
}
function bump(metric, n = 1) {
  if (postToParent('bump', { metric, delta: Number(n)||0 })) return;
  if (!STATE.totals[metric]) STATE.totals[metric] = 0;
  STATE.totals[metric] += Number(n) || 0;
}
function event(obj) { 
  if (postToParent('event', obj || {})) return;
  logEvent(obj && obj.type ? obj.type : 'event', obj || {}); }

function threadHeartbeat(info = {}) {
  const id = info.workerId != null ? String(info.workerId)
           : info.pid ? String(info.pid)
           : 'self';
  const prev = STATE.threads[id] || {};
  STATE.threads[id] = {
    ...prev,
    info,
    url: info.url || prev.url || '',
    phase: info.phase || prev.phase || ''
  };
}

function bucketUpdate(r, data = {}) {
  if (r == null) return;
  const key = String(r);
  if (!key || key === 'undefined' || key === 'NaN') return;
  const prev = STATE.buckets[key] || {};
  STATE.buckets[key] = { ...prev, ...data };
}

function treeAdd(pathSegs) {
  if (postToParent('tree', { pathSegs })) return;
  if (!Array.isArray(pathSegs) || !pathSegs.length) return;

  // Normalize and build cumulative paths: ['en-us','legal','privacy'] =>
  // depth 0: '/en-us'
  // depth 1: '/en-us/legal'
  // depth 2: '/en-us/legal/privacy'
  const clean = pathSegs.map(s => String(s || '').replace(/^\/+|\/+$/g, '')).filter(Boolean);
  let cumulative = '';
  for (let depth = 0; depth < clean.length; depth++) {
    cumulative = depth === 0 ? `/${clean[0]}` : `${cumulative}/${clean[depth]}`;
    if (!STATE.tree[depth]) STATE.tree[depth] = new Set();
    STATE.tree[depth].add(cumulative);
  }
}


function snapshot() {
  const tree = {};
  for (const [k, set] of Object.entries(STATE.tree)) tree[k] = Array.from(set);

  // Pull latest meta so UI links always use the current base/prefix
  let meta = { base: '', prefix: '' };
  try {
    const file = readConfigFile();
    if (file && file.meta) meta = { base: String(file.meta.base || ''), prefix: String(file.meta.prefix || '') };
  } catch {}

  const snap = { ...STATE, upTime: nowHMS(), tree, meta };

  try {
    const outDir = path.resolve((LAUNCH.outDir || 'dist'), 'telemetry');
    fs.mkdirSync(outDir, { recursive: true });
    fs.writeFileSync(path.join(outDir, 'state.json'), JSON.stringify(snap, null, 2));
  } catch (e) {}

  return snap;
}



// ---------- HTTP server & UI ----------
function handleHttp(req, res) {
  if (req.method === 'POST' && req.url === '/update') {
    let body = ''; req.on('data', c => body += c);
    req.on('end', () => {
      try {
        const msg = JSON.parse(body || '{}');
        switch (msg.type) {
          case 'thread': threadStatus(msg.info || {}); break;
          case 'bucket': bucketUpdate(msg.bucket, msg.data || {}); break;
          case 'tree':   if (Array.isArray(msg.pathSegs)) treeAdd(msg.pathSegs); break;
          case 'bump':   bump(msg.metric, msg.delta || 1); break;
          case 'step':   step(msg.step); break;
          case 'mode':   setMode(msg.mode); break;
        }
        res.writeHead(204); res.end();
      } catch { res.writeHead(400); res.end('bad'); }
    });
    return;
  }

  if (req.method === 'GET' && req.url === '/') {
    res.writeHead(200, { 'content-type': 'text/html; charset=utf-8' });
    res.end(PAGE_HTML);
    return;
  }
  if (req.method === 'GET' && req.url === '/snapshot') {
    const js = JSON.stringify(snapshot());
    res.writeHead(200, { 'content-type': 'application/json; charset=utf-8', 'cache-control': 'no-store' });
    res.end(js);
    return;
  }

    // POST /upload-input?name=some.csv  (body = raw file bytes)
  if (req.method === 'POST' && req.url.startsWith('/upload-input')) {
    const q = new URL(req.url, 'http://x').searchParams;
    const name = (q.get('name') || 'input.csv').replace(/[^\w.\-]+/g, '_');
    const base = LAUNCH.outDir || 'dist';
    const dir  = path.join(base, 'telemetry', 'uploads');
    fs.mkdirSync(dir, { recursive: true });
    const abs  = path.join(dir, name);
    const chunks = [];
    req.on('data', c => chunks.push(c));
    req.on('end', () => {
      try {
        fs.writeFileSync(abs, Buffer.concat(chunks));
        LAUNCH.inputPath = abs; // let shard-run sniff this on next read
        snapshot();             // persist to state.json
        return sendJson(res, 200, { ok: true, path: abs });
      } catch (e) {
        return sendJson(res, 500, { ok: false, error: String(e && e.message || e) });
      }
    });
    return;
  }


// GET /preflight — report current shape + selection + flags
   if (req.method === 'GET' && req.url === '/preflight') {

    // load current config.json if it exists (for meta + selected outputs)
      let cfg = {};
      try {
        const p = path.join((LAUNCH.outDir || 'dist'), 'telemetry', 'config.json');
        cfg = JSON.parse(fs.readFileSync(p, 'utf8'));
      } catch {}

     // Prefer the last applied config's meta.inputPath if present, else fall back to LAUNCH
      const cfgForPre = readConfigFile() || {};
      const safeInputPath =
        (cfgForPre.meta && typeof cfgForPre.meta.inputPath === 'string' && cfgForPre.meta.inputPath.trim()) ||
        (typeof LAUNCH.inputPath === 'string' && LAUNCH.inputPath.trim()) ||
        '';
      const shape = sniffCsvShape(safeInputPath);

     const file = readConfigFile();
     const selected = (file && Array.isArray(file.outputs)) ? file.outputs : [];
     const meta = (file && file.meta) ? file.meta : {
      base: '',
      prefix: '',
      outDir: LAUNCH.outDir || 'dist',
     keepPageParam: false,
      headless: true,
      shards: 0,
      bucketParts: 0,
     override: false,
      multiShards: false
    };
    const startedFlag = (file && typeof file.started === 'boolean') ? file.started : (STARTED === true);
    shape.inferredRoles = shape.inferredRoles || [];

     return sendJson(res, 200, {
      shape,
      options: OUTPUTS,
      selected,
      outputs: selected,
      meta,
      applied: APPLIED === true,
      started: startedFlag
    });

   }

// POST /config (Apply ONLY — no start here)
if (req.method === 'POST' && req.url === '/config') {
  let body = '';
  req.on('data', c => body += c);
  req.on('end', () => {
    try {
      const j = JSON.parse(body || '{}');
      const outputs = Array.isArray(j.outputs) ? j.outputs : [];
      const meta = j.meta || {};
      const outDir = String(meta.outDir || LAUNCH.outDir || 'dist').trim();

      const errors = [];

      const ensureHttpUrl = (u) => {
        const s = String(u || '').trim();
        if (!s) return '';
        return /^[a-z]+:\/\//i.test(s) ? s : `https://${s}`;
      };
      const normPrefix = (p) => {
        const s = String(p || '').trim();
        if (!s) return '/';
        const withSlash = s.startsWith('/') ? s : `/${s}`;
        return withSlash.length > 1 ? withSlash.replace(/\/+$/, '') : '/';
      };
      const toInt = (n) => Number(n || 0) || 0;

      // validate base
      const normBase = ensureHttpUrl(meta.base);
      if (!normBase) errors.push({ key: 'base', reason: 'Base URL is required (e.g., https://stage.example.com).' });

      // outDir must exist
      try {
        const st = fs.statSync(outDir);
        if (!st.isDirectory()) errors.push({ key: 'outDir', reason: 'Not a folder.' });
      } catch {
        errors.push({ key: 'outDir', reason: 'Folder does not exist.' });
      }

      // CSV gating for URL-based outputs
      const shape = sniffCsvShape(meta.inputPath || LAUNCH.inputPath);
      errors.push(...validateOutputs(outputs, shape));

      const valid = errors.length === 0;
      APPLIED = valid;
      if (!valid) return sendJson(res, 200, { valid:false, errors });

      // persist config.json
      LAUNCH.outDir = outDir; // keep UI + server in sync
      STARTED = false;
      const dir = path.join(outDir, 'telemetry');
      fs.mkdirSync(dir, { recursive: true });

      const cfg = {
        valid: true,
        started: false,             // never auto-start on apply
        outputs,
        meta: {
          base: normBase,
          prefix: normPrefix(meta.prefix),
          outDir,
          keepPageParam: !!meta.keepPageParam,
          headless: !!meta.headless,
          prod: !!meta.prod,
          maxShards: !!meta.maxShards,
          multi: !!meta.multiShards,
          shards: toInt(meta.shards),
          bucketParts: toInt(meta.bucketParts),
          inputPath: String(meta.inputPath || LAUNCH.inputPath || '')
        }
      };

      fs.writeFileSync(path.join(dir, 'config.json'), JSON.stringify(cfg, null, 2), 'utf8');
      sendJson(res, 200, { valid:true, errors:[], meta: cfg.meta });
    } catch (e) {
      sendJson(res, 500, { valid:false, errors:[{ key:'exception', reason:String(e && e.message || e) }] });
    }
  });
  return;
}




  // POST /start (separate Start button in Control Panel)
  if (req.method === 'POST' && req.url === '/start') {
    const file = readConfigFile() || { valid:false, outputs:[], meta:{}, started:false };
    // Only allow start if config is valid and has at least one output
    if (file && file.valid && Array.isArray(file.outputs) && file.outputs.length) {
      // clear any lingering stop.flag
      try {
        const stopFile = path.join((LAUNCH.outDir || 'dist'), 'telemetry', 'stop.flag');
        if (fs.existsSync(stopFile)) fs.unlinkSync(stopFile);
      } catch {}
      file.started = true;
      fs.mkdirSync(path.dirname(CONFIG_FILE()), { recursive: true });
      fs.writeFileSync(CONFIG_FILE(), JSON.stringify(file));

      STOP_REQUESTED = false; // clear any prior stop request
      markStart();
      return sendJson(res, 200, { ok:true });
    }
    return sendJson(res, 400, { ok:false, reason:'No valid config applied yet' });
  }


  // --- Files listing since the run started ---
  if (req.method === 'GET' && req.url.startsWith('/files')) {
    const base = LAUNCH.outDir || 'dist';
    const list = [];
    const start = STATE.startedAt || 0;
    function walk(dir) {
      try {
        for (const name of fs.readdirSync(dir)) {
          const p = path.join(dir, name);
          const st = fs.statSync(p);
          if (st.isDirectory()) walk(p);
          else if (st.mtimeMs >= start) {
            list.push({ path: p, rel: path.relative(base, p), size: st.size, mtimeMs: st.mtimeMs });
          }
        }
      } catch {}
    }
    walk(base);
    list.sort((a,b) => a.rel.localeCompare(b.rel));
    res.writeHead(200, { 'content-type': 'application/json' });
    res.end(JSON.stringify({ files: list }, null, 0));
    return;
  }

  // --- Download (restrict to outDir) ---
  if (req.method === 'GET' && req.url.startsWith('/download?')) {
    const q = new URL(req.url, 'http://x').searchParams;
    const rel = (q.get('file') || '').replace(/^\.*[\\/]+/,'');
    const base = LAUNCH.outDir || 'dist';
    const abs = path.resolve(base, rel);
    if (!abs.startsWith(path.resolve(base))) { res.writeHead(403); res.end('forbidden'); return; }
    try {
      const data = fs.readFileSync(abs);
      res.writeHead(200, { 'content-type': 'application/octet-stream',
                           'content-disposition': `attachment; filename="${path.basename(abs)}"` });
      res.end(data);
    } catch { res.writeHead(404); res.end('not found'); }
    return;
  }

  // --- Reset / New Run ---
  if (req.method === 'POST' && req.url === '/reset') {
    STATE.startedAt = Date.now();
    try { markStart(); } catch {}
    STATE.mode = 'init';
    STATE.stepper.currentIndex = 0;
    STATE.threads = {};
    STATE.buckets = {};
    STATE.tree = {};
    STATE.events = [];
    APPLIED = false;
    STARTED = false;
    try { fs.unlinkSync(CONFIG_FILE()); } catch {}
    try {
      const stopFile = path.join((LAUNCH.outDir || 'dist'), 'telemetry', 'stop.flag');
      if (fs.existsSync(stopFile)) fs.unlinkSync(stopFile);
    } catch {}
    res.writeHead(204); res.end();
    return;
  }
  if (req.method === 'GET' && req.url === '/reset-wait') {
    // simple ack endpoint polled by the orchestrator to detect that reset happened
    res.writeHead(200); res.end('ok'); return;
  }

  // --- Stop signal (graceful)
  if (req.method === 'POST' && req.url === '/stop') {
    try {
      const stopFile = path.join((LAUNCH.outDir || 'dist'), 'telemetry', 'stop.flag');
      fs.mkdirSync(path.dirname(stopFile), { recursive: true });
      fs.writeFileSync(stopFile, String(Date.now()));

      // Flip in-process flag so orchestrator sees it immediately
      STOP_REQUESTED = true;

      // Also mark config.started=false so nothing auto-restarts
      const cfg = readConfigFile() || { valid:false, outputs:[], meta:{}, started:false };
      cfg.started = false;
      fs.mkdirSync(path.dirname(CONFIG_FILE()), { recursive: true });
      fs.writeFileSync(CONFIG_FILE(), JSON.stringify(cfg));
    } catch {}
    // Do not wipe caches or telemetry; manual "New Run" does that
    res.writeHead(204); res.end(); return;
  }



  if (req.method === 'GET' && req.url === '/stop-state') {
    try {
      const stopFile = path.join((LAUNCH.outDir || 'dist'), 'telemetry', 'stop.flag');
      const exists = fs.existsSync(stopFile);
      res.writeHead(200, { 'content-type': 'application/json' });
      res.end(JSON.stringify({ stop: !!exists }));
    } catch {
      res.writeHead(200, { 'content-type': 'application/json' });
      res.end(JSON.stringify({ stop: false }));
    }
    return;
  }



  if (req.method === 'GET' && req.url === '/favicon.ico') { res.writeHead(204); res.end(); return; }
  res.writeHead(404); res.end('not found');
}

const PAGE_HTML = `<!doctype html>
<html>
<head>
  <meta charset="utf-8"/>
  <title>Meta Check — Telemetry</title>
  <meta name="viewport" content="width=device-width,initial-scale=1"/>
  <style>
    :root { --bg:#0b0f14; --fg:#e6edf3; --muted:#8b949e; --ok:#2ea043; --warn:#f2cc60; --bad:#f85149; --card:#151b23; --pill:#1f2630; }
    * { box-sizing: border-box; font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial, Noto Sans, Apple Color Emoji, Segoe UI Emoji; }
    body { margin: 0; padding: 16px; background: var(--bg); color: var(--fg); }
    h1 { margin: 0 0 8px 0; font-size: 20px; }
    .row { display: flex; gap: 12px; flex-wrap: wrap; align-items: stretch; }
    .card { background: var(--card); border: 1px solid #222a35; border-radius: 10px; padding: 12px; }
    .metrics { display: grid; grid-template-columns: repeat(4, minmax(160px, 1fr)); gap: 10px; }
    .metric { background: var(--pill); padding: 10px; border-radius: 8px; }
    .metric .v { font-weight: 700; font-size: 18px; }
    table { width: 100%; border-collapse: collapse; }
    th, td { padding: 6px 8px; border-bottom: 1px solid #223; text-align: left; vertical-align: top; }
    th { color: var(--muted); font-weight: 500; }
    td.url { max-width: 560px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
    .buckets { display: grid; grid-template-columns: repeat(auto-fill, minmax(160px, 1fr)); gap: 10px; }
    .bucket { background: var(--pill); border-radius: 8px; padding: 8px; border: 1px solid #223; }
    .pill { display:inline-block; background:#1b212d; padding: 2px 8px; border-radius: 999px; margin-right: 6px; color: var(--muted); }
    .ok { color: var(--ok); } .bad { color: var(--bad); } .warn { color: var(--warn); }
    .stepper { display:flex; gap:8px; align-items:center; flex-wrap:wrap; }
    .step { padding: 4px 8px; border-radius: 999px; background: var(--pill); border: 1px solid #223; color: var(--muted); }
    .step.active { color: var(--fg); border-color: #395; }
    .tree { display:block; }
    .node { display:inline-block; background:#242628; border-radius:999px; padding:6px 14px; font-size:14px; margin:4px 0; text-decoration:none; color:var(--fg); }
    .node:hover { background:#2a2f34; }
    .node.depth-0 { margin-left: 0;      font-weight:600; }
    .node.depth-1 { margin-left: 24px; }
    .node.depth-2 { margin-left: 48px; }
    .node.depth-3 { margin-left: 72px; }
    .node.depth-4 { margin-left: 96px; }

    /* Files panel links — subtle by default, clearer on hover */
    #files-list a { color:#b8c1cc; text-decoration:underline; text-decoration-color:#3a4556; }
    #files-list a:hover { color:#eef2f7; text-decoration-color:#b8c1cc; }

    /* Control Panel */
    .cp-grid { display:grid; grid-template-columns: repeat(2, minmax(240px, 1fr)); gap: 10px; }
    .cp-row { display:flex; align-items:center; gap:10px; }
    .cp-col { display:flex; flex-direction:column; gap:6px; }
    .cp-k { color: var(--muted); font-size: 12px; }
    .cp-v, input[type="text"], input[type="number"], select { background:#0f1319; border:1px solid #253042; color:var(--fg); border-radius:8px; padding:8px 10px; }
    .cp-help { color: var(--muted); font-size: 12px; }
    .cp-badge { display:inline-block; padding:2px 8px; border-radius:999px; background:#1b2230; border:1px solid #223; color:#b8c1cc; font-size:12px; }
    .muted { color: var(--muted); }
    .btn { background:#1e2633; border:1px solid #2a3447; color:#e6edf3; padding:8px 12px; border-radius:8px; cursor:pointer; }
    .btn.secondary { background:#151b23; }
    .btn:active { transform: translateY(1px); }
    .mono { font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace; }
    .pre { white-space: pre-wrap; word-break: break-word; background:#0a0d12; border:1px solid #223; border-radius:10px; padding:10px; font-size:12px; }
    .hint { font-size: 11px; color: var(--muted); }
    .spacer { height: 6px; }
    .hover-help { text-decoration: dotted underline; cursor: help; }
  </style>
</head>
<body>

  <!-- ====================== CONTROL PANEL ====================== -->
  <div class="card" id="control-panel" style="margin-bottom:12px;">
    <div style="display:flex; align-items:center; justify-content:space-between; gap:10px; margin-bottom:10px;">
      <div style="font-weight:600;">Control Panel</div>
      <div class="cp-badge">builds a CLI for scripts/shard-run.js</div>
    </div>

    <div class="cp-grid">
      <div class="cp-col">
        <label class="cp-k" for="cp-base">Base URL</label>
        <input id="cp-base" type="text" />
        <div class="cp-help">e.g., https://stage.ping.com</div>
      </div>

      <div class="cp-col">
        <label class="cp-k" for="cp-prefix">Path Prefix</label>
        <input id="cp-prefix" type="text" />
        <div class="cp-help">Scope the crawl to this path. Keep as <span class="mono">/</span> to crawl all.</div>
      </div>

      <div class="cp-col">
        <label class="cp-k">Keep the ?page= query in links</label>
        <div class="cp-row">
          <input id="cp-keeppage" type="checkbox" />
          <span class="cp-help">Don’t strip <span class="mono">?page=</span> from URLs.</span>
        </div>
      </div>

      <div class="cp-col">
        <label class="cp-k">Run browser in the background</label>
        <div class="cp-row">
          <input id="cp-headless" type="checkbox" checked />
          <span class="cp-help">Sets <span class="mono">PLAYWRIGHT_HEADLESS=1</span> in the CLI.</span>
        </div>
      </div>

      <div class="cp-col">
        <label class="cp-k">Multiple parallel workers</label>
        <div class="cp-row">
          <input id="cp-multi" type="checkbox" />
          <span class="cp-help">Enable parallel shards (workers).</span>
        </div>
      </div>

      <div class="cp-col">
        <label class="cp-k">Safe mode (gentle crawl)</label>
        <div class="cp-row">
          <input id="cp-prod" type="checkbox" />
          <span class="cp-help">Throttles workers & adds polite delays to reduce server load.</span>
        </div>
      </div>

      <div class="cp-col">
        <label class="cp-k">Max shards (auto)</label>
        <div class="cp-row">
          <input id="cp-maxshards" type="checkbox" />
          <span class="cp-help">Sets shards to your CPU cores; buckets auto = <span class="mono">2 × shards</span>.</span>
        </div>
      </div>


      <div class="cp-col" id="cp-shard-col" style="display:none;">
        <label class="cp-k" for="cp-shards">Shards</label>
        <div class="cp-row">
          <select id="cp-shards"></select>
          <span class="cp-badge" title="Auto buckets = 2 × shards when shards > 1">
            buckets: <span id="cp-buckets">1</span>
          </span>
        </div>
        <div class="cp-help">Buckets auto-calc to <span class="mono">2 × shards</span> (min 1).</div>
      </div>

      <div class="cp-col">
        <label class="cp-k" for="cp-outdir">Output folder (local)</label>
        <div class="cp-row">
          <input id="cp-outdir" type="text"/>
          <button class="btn secondary" id="cp-browse">Choose folder…</button>
          <input id="cp-dirpicker" type="file" style="display:none" webkitdirectory directory />
        </div>
        <div class="cp-help">This is a local <b>write</b> location (nothing uploads). Default is <span class="mono">./dist</span>.</div>
      </div>


      <div class="cp-col">
        <label class="cp-k">Override existing outputs</label>
        <div class="cp-row">
          <input id="cp-override" type="checkbox" />
          <span class="cp-help">If off, tool may suffix folder name (n).</span>
        </div>
      </div>

      <div class="cp-col">
        <label class="cp-k">Input file (optional)</label>
        <div class="cp-row" style="align-items:center;">
          <input id="cp-file" type="file" accept=".csv,.tsv,.txt" />
          <span class="hint" id="cp-file-name"></span>
        </div>
        <div class="cp-help">If omitted, orchestrator will discover via frontier.</div>
      </div>

      <div class="cp-col">
        <div class="cp-help">Outputs are selected in the <b>Preflight</b> panel below.</div>
      </div>
    </div>

    <div class="spacer"></div>

    <div class="cp-col">
      <div class="cp-k">Generated CLI</div>
      <div class="pre mono" id="cp-cli"></div>
      <div class="cp-row" style="margin-top:8px;">
        <button class="btn" id="cp-copy">Copy CLI</button>
        <button class="btn" id="cp-start">Start</button>
        <span class="hint">Run this from your project root.</span>
      </div>
    </div>
  </div>

  <div class="cp-row" style="margin-top:8px; gap:10px;">
    <button class="btn" id="cp-stop">Stop Run</button>
    <button class="btn secondary" id="cp-newrun">New Run</button>
    <span class="hint">Stop halts the current run gracefully; New Run resets telemetry.</span>
  </div>

  <!-- ==================== END CONTROL PANEL ==================== -->


  <!-- ==================== PREFLIGHT (gate) ==================== -->
  <div id="preflight" style="border:1px solid #223; border-radius:12px; padding:12px; margin:12px 0; background:#131518;">
    <div style="font-weight:600; margin-bottom:8px;">Choose outputs to produce</div>
    <form id="pf-form" style="display:flex; flex-wrap:wrap; gap:10px;">
      <label title="Write merged URL list with deduplication."><input type="checkbox" name="outputs" value="urls" checked> urls</label>
      <label title="Write hierarchical site catalog / tree files."><input type="checkbox" name="outputs" value="site_catalog" checked> site_catalog</label>
      <label title="Emit internal link edges as NDJSON/CSV."><input type="checkbox" name="outputs" value="internal_links" checked> internal_links</label>
      <label title="Emit the discovery tree JSON for the TUI."><input type="checkbox" name="outputs" value="tree" checked> tree</label>
      <label title="Check URL existence/status for rows in your CSV."><input type="checkbox" name="outputs" value="existence_csv"> existence.csv</label>
      <label title="Compare actual vs expected meta when CSV has 2 columns."><input type="checkbox" name="outputs" value="comparison_csv"> comparison.csv</label>
    </form>
    <div id="pf-errors" style="color:#ff8989; margin-top:6px;"></div>
    <button id="pf-apply" style="margin-top:8px;">Apply and start</button>

    <div class="card" id="files-panel" style="margin-top:12px; display:none;">
      <h3 style="margin:0 0 8px 0">Files</h3>
      <div id="files-list" class="mono" style="font-size:12px;"></div>
      <div class="cp-row" style="margin-top:10px;">
        <button class="btn" id="btn-newrun">New Run</button>
        <span class="hint">Resets telemetry and shows the control panel again.</span>
      </div>
    </div>
  </div>
  <!-- ================= END PREFLIGHT ================= -->


  <h1>Meta Check — Telemetry <span class="pill" id="mode"></span> <span class="pill" id="elapsed"></span></h1>

  <div class="row">
    <div class="card" style="flex:1;">
      <div class="metrics">
        <div class="metric"><div class="k">URLs found</div><div class="v" id="m-urls">0</div></div>
        <div class="metric"><div class="k">Internal links</div><div class="v" id="m-edges">0</div></div>
        <div class="metric"><div class="k">Threads</div><div class="v" id="m-threads">0</div></div>
        <div class="metric"><div class="k">Buckets</div><div class="v" id="m-buckets">0</div></div>
      </div>
      <div style="height:10px"></div>
      <div class="stepper" id="stepper"></div>
    </div>
  </div>

  <div class="row" style="margin-top:12px">
    <div class="card" style="flex:2; min-width:520px;">
      <h3 style="margin:0 0 8px 0">Threads</h3>
      <table>
        <thead><tr><th>ID</th><th>Phase</th><th>URL</th><th>Idle</th></tr></thead>
        <tbody id="threadsBody"></tbody>
      </table>
    </div>
    <div class="card" style="flex:1; min-width:320px;">
      <h3 style="margin:0 0 8px 0">Buckets</h3>
      <div class="buckets" id="buckets"></div>
    </div>
  </div>

  <div class="card" style="margin-top:12px;">
    <h3 style="margin:0 0 8px 0">Discovery Tree (by path depth)</h3>
    <div class="tree" id="tree"></div>
  </div>

<script>
  try { localStorage.removeItem('mc_ctrl'); } catch {}
  const $ = (id) => document.getElementById(id);

  // Electron detection + folder dialog hookup
  const isElectron = !!(window.electronAPI);

  // Global meta the UI uses to build full links
  let __mc_meta = { base: '', prefix: '' };

  // ----------------------- CONTROL PANEL -----------------------
  (function(){
    // Populate shards dropdown
    const shardsSel = $('cp-shards');
    for (let i = 1; i <= 32; i++) {
      const opt = document.createElement('option');
      opt.value = String(i);
      opt.textContent = String(i);
      shardsSel.appendChild(opt);
    }
    shardsSel.value = '1';

    function state(){
      const multi = $('cp-multi').checked;
      // Prod and Multi are mutually exclusive — if Prod is on, force Multi off
      const prod = $('cp-prod').checked;
      const effectiveMulti = prod ? false : multi;

      // Max shards checkbox only meaningful when multi is enabled
      const maxAuto = effectiveMulti ? $('cp-maxshards').checked : false;

      const hw = (navigator.hardwareConcurrency || 1);
      const shardsSelVal = Math.max(1, parseInt(($('cp-shards').value || '1'), 10));
      const shards = maxAuto ? hw : (effectiveMulti ? shardsSelVal : 1);
      const buckets = (effectiveMulti && shards > 1) ? (shards * 2) : 1;

      return {
        base: $('cp-base').value.trim(),
        prefix: $('cp-prefix').value.trim(),
        keepPageParam: $('cp-keeppage').checked,
        headless: $('cp-headless').checked,
        // persist original toggles so the server knows user's intent
        prod: prod,
        multi: effectiveMulti,
        maxShards: maxAuto,
        shards,
        buckets,
        outDir: $('cp-outdir').value.trim() || './dist',
        override: $('cp-override').checked,
        fileName: $('cp-file-name').textContent.replace(/^Selected:\s*/,'') || '',
        inputPath: (JSON.parse(localStorage.getItem('mc_ctrl') || '{}').inputPath || '')
      };
}

    window.__mc_state = state; // expose for preflight handler

    function save(){ localStorage.setItem('mc_ctrl', JSON.stringify(state())); }

    function recompute(){
    const s = state();

    // Toggle visibility of shard picker when multi is on
    $('cp-shard-col').style.display = $('cp-multi').checked ? '' : 'none';

    // Mutual exclusivity: safe mode disables multi
    $('cp-prod').onchange = () => {
      if ($('cp-prod').checked) {
        $('cp-multi').checked = false;
        $('cp-shard-col').style.display = 'none';
      }
      recompute();
    };
    $('cp-multi').onchange = () => {
      if ($('cp-multi').checked) {
        $('cp-prod').checked = false;
      }
      $('cp-shard-col').style.display = $('cp-multi').checked ? '' : 'none';
      recompute();
    };

    // Buckets auto = 2 × shards when multi is on and shards > 1
    const hw = (navigator.hardwareConcurrency || 1);
    const shards = Math.max(1, parseInt(($('cp-shards').value || '1'), 10));
    const buckets = ($('cp-multi').checked && shards > 1) ? (2 * shards) : 1;
    $('cp-buckets').textContent = String(buckets);

      if (s.meta && (s.meta.base || s.meta.prefix)) {
        __mc_meta = { base: s.meta.base || '', prefix: s.meta.prefix || '' };
      }
      $('cp-shard-col').style.display = s.multi ? '' : 'none';

      // Show/Hide “Max shards (auto)” checkbox with multi only
      $('cp-maxshards').closest('.cp-col').style.display = s.multi ? '' : 'none';

      // If auto, reflect computed shards in the dropdown (cap 32 just for UI)
      if (s.maxShards) $('cp-shards').value = String(Math.min(32, Math.max(1, s.shards)));

      const envPrefix = s.headless ? 'set PLAYWRIGHT_HEADLESS=1 && ' : '';
      const parts = ['node','scripts/shard-run.js'];
      if (s.fileName) parts.push('--input', s.fileName);
      if (s.base) parts.push('--base', s.base);
      if (s.prefix) parts.push('--pathPrefix', s.prefix);
      if (s.keepPageParam) parts.push('--keepPageParam', 'true');

      const shardCap = s.multi ? s.shards : 1;
      const concurrency = s.multi ? s.shards : 1;
      parts.push('--bucketParts', String(s.buckets));
      parts.push('--shardCap', String(shardCap));
      parts.push('--concurrency', String(concurrency));
      parts.push('--outDir', s.outDir);

      const cli = envPrefix + parts.map(p => p.includes(' ') ? '"' + p + '"' : p).join(' ');
      $('cp-cli').textContent = cli;
      save();
    }

    ['cp-base','cp-prefix','cp-keeppage','cp-headless','cp-multi','cp-shards','cp-outdir','cp-override']
      .forEach(id => $(id).addEventListener('input', recompute));
    shardsSel.addEventListener('change', recompute);

    $('cp-file').addEventListener('change', async function(){
      const f = this.files && this.files[0];
      $('cp-file-name').textContent = f ? ('Selected: ' + f.name) : '';
      recompute();
      if (!f) return;
      try {
        const buf = await f.arrayBuffer();
        const r = await fetch('/upload-input?name=' + encodeURIComponent(f.name), {
          method: 'POST',
          headers: { 'content-type': 'application/octet-stream' },
          body: buf
        });
        const j = await r.json();
        if (j && j.ok && j.path) {
          // tuck the server path into our persisted CP state
          const saved = JSON.parse(localStorage.getItem('mc_ctrl') || '{}');
          saved.inputPath = j.path;
          saved.fileName  = f.name;
          localStorage.setItem('mc_ctrl', JSON.stringify(saved));
        }
      } catch {}
    });


    $('cp-copy').addEventListener('click', async function(){
      const text = $('cp-cli').textContent || '';
      try { await navigator.clipboard.writeText(text); this.textContent = 'Copied!'; setTimeout(()=> this.textContent='Copy CLI', 900); } catch {}
    });

    // Start button: separate from Preflight Apply
    $('cp-start').addEventListener('click', async ()=>{
      try {
        const r = await fetch('/start', { method:'POST' });
        if (!r.ok) return alert('Start failed — apply config first.');
       alert('Started!');
      } catch (e) {
        alert('Start failed: ' + (e && e.message ? e.message : e));
      }
    });

    // Primary: Electron native folder dialog
    $('cp-browse').addEventListener('click', async () => {
      if (isElectron) {
        const res = await window.electronAPI.selectFolder();
        if (!res || res.canceled) return;
        $('cp-outdir').value = res.path;
        return;
      }
      // Fallback: web directory input
      $('cp-dirpicker').click();
    });

    $('cp-dirpicker').addEventListener('change', function(){
      const f = this.files && this.files[0];
      if (f && f.webkitRelativePath) {
        const root = f.webkitRelativePath.split('/')[0] || 'dist';
        $('cp-outdir').value = (f.path && f.path.trim()) ? f.path : ('./' + root);
      }
    });


    $('cp-newrun').addEventListener('click', async function(){
      try { await fetch('/reset-wait'); } catch {}
      location.reload();
    });

    $('cp-stop').addEventListener('click', async function(){
      try {
        await fetch('/stop', { method: 'POST' });
        alert('Stop requested — halting workers safely.');
      } catch (e) {
        alert('Stop failed: ' + (e && e.message ? e.message : e));
      }
    });

    $('cp-prod').addEventListener('change', () => {
      if ($('cp-prod').checked) $('cp-multi').checked = false;
      recompute();
    });
    $('cp-multi').addEventListener('change', () => {
      if ($('cp-multi').checked) $('cp-prod').checked = false;
      recompute();
    });

    recompute();
  })();
  // --------------------- END CONTROL PANEL ----------------------


  // ---------------------- FILES PANEL (once) --------------------
  let __filesIntervalStarted = false;
  async function refreshFiles(){
    try {
      const res = await fetch('/files', { cache: 'no-store' });
      if (!res.ok) return;
      const { files } = await res.json();
      const box = $('files-panel');
      const list = $('files-list');
      if (!files || !files.length) { box.style.display = 'none'; return; }
      box.style.display = '';
      list.innerHTML = '';
      for (const f of files) {
        const a = document.createElement('a');
        a.href = '/download?file=' + encodeURIComponent(f.rel);
        a.textContent = f.rel + '  (' + f.size + ' bytes)';
        a.style.display = 'block';
        list.appendChild(a);
      }
    } catch {}
  }
  // -------------------- END FILES PANEL (once) ------------------


  // -------------------- TELEMETRY REFRESH LOOP ------------------
  async function tick(){
    try{
      const res = await fetch('/snapshot', { cache: 'no-store' });
      if (!res.ok) return;
      const s = await res.json();
      if (s.meta && (s.meta.base || s.meta.prefix)) {
        __mc_meta = { base: s.meta.base || '', prefix: s.meta.prefix || '' };
      }

      $('mode').textContent = s.mode || '';
      $('elapsed').textContent = 'elapsed ' + (s.upTime || '0:00');

      $('m-urls').textContent = String((s.totals && s.totals.urlsFound) || 0);
      $('m-edges').textContent = String((s.totals && s.totals.internalEdges) || 0);
      const threadList = Object.values(s.threads || {});
      const liveThreads = (Object.values(s.threads || {})).filter(t =>
        t && t.info && (t.info.workerId != null || t.info.pid != null) && t.phase !== 'input-confirm'
      );
      $('m-threads').textContent = String(liveThreads.length);
      const bucketKeys = Object.keys(s.buckets || {}).filter(k => k && k !== 'undefined' && k !== 'NaN');
      $('m-buckets').textContent = String(bucketKeys.length);


      const stepper = $('stepper'); stepper.innerHTML = '';
      const steps = (s.stepper && s.stepper.steps) || [];
      const idx   = (s.stepper && s.stepper.currentIndex) || 0;
      steps.forEach((name, i) => {
        const span = document.createElement('span');
        span.className = 'step' + (i === idx ? ' active' : '');
        span.textContent = name;
        stepper.appendChild(span);
      });

      // Files refresher — ensure single interval
      if (!__filesIntervalStarted) {
        __filesIntervalStarted = true;
        setInterval(refreshFiles, 1200);
        refreshFiles();
      }

      // New Run button inside Files panel
      if (!$('btn-newrun').__bound) {
        $('btn-newrun').addEventListener('click', async () => {
          try { await fetch('/reset', { method: 'POST' }); } catch {}
          $('files-panel').style.display = 'none';
          $('preflight').style.display = '';
        });
        $('btn-newrun').__bound = true;
      }

      // Threads table
      const tb = $('threadsBody'); tb.innerHTML = '';
        liveThreads
          .sort((a,b) => (a.workerId||a.pid||0) - (b.workerId||b.pid||0))
          .forEach(t => {
            const tr = document.createElement('tr');
            const id = t.workerId != null ? ('W' + t.workerId) : String(t.pid || '');
            tr.innerHTML =
              '<td>' + id + '</td>' +
              '<td>' + (t.phase || '') + '</td>' +
              '<td class="url" title="' + (t.url || '') + '">' + (t.url || '') + '</td>' +
              '<td>' + String(t.idleSwaps || 0) + (t.idleLimit ? ('/' + t.idleLimit) : '') + '</td>';
            tb.appendChild(tr);
          });

      // Buckets grid
      const bk = $('buckets'); bk.innerHTML = '';
      Object.entries(s.buckets || {})
        .sort((a,b) => Number(a[0]) - Number(b[0]))
        .forEach(([r, b]) => {
          const div = document.createElement('div');
          const done = Number(b.processed || 0), pend = Number(b.pending || 0);
          div.className = 'bucket';
          div.innerHTML =
            '<div><b>Bucket ' + r + '</b></div>' +
            '<div class="pill">owner ' + (b.owner || '-') + '</div>' +
            '<div>processed ' + done + ' · pending ' + pend + '</div>' +
            (b.last ? ('<div class="url" title="' + b.last + '">' + b.last + '</div>') : '');
          bk.appendChild(div);
        });

      // Discovery Tree
      const trc = $('tree'); trc.innerHTML = '';
      const byDepth = {};
      Object.keys(s.tree || {}).forEach(d => byDepth[+d] = (s.tree[d] || []).slice().sort());
      if (!byDepth[0] || !byDepth[0].length) return;

      const all = []; Object.values(byDepth).forEach(arr => all.push(...arr));
      const kids = new Map();
      let maxDepth = 0;
      for (const p of all) {
        const depth = p.split('/').filter(Boolean).length - 1;
        if (depth > maxDepth) maxDepth = depth;
        if (depth === 0) continue;
        const parent = p.slice(0, p.lastIndexOf('/'));
        if (!kids.has(parent)) kids.set(parent, []);
        kids.get(parent).push(p);
      }
      for (const [k, arr] of kids) arr.sort();

      const grid = document.createElement('div');
      grid.style.display = 'grid';
      grid.style.gridTemplateColumns = 'repeat(' + (maxDepth + 1) + ', minmax(220px, 1fr))';
      grid.style.gap = '8px 14px';

      function pillLink(seg, fullPath) {
        const a = document.createElement('a');
        a.className = 'node';
        a.textContent = seg || '/';
        const base = (__mc_meta && __mc_meta.base) || '';
        let href = fullPath.startsWith('/') ? fullPath : '/' + fullPath;
        try {
          href = new URL(href, base).toString();
        } catch {}
        a.href = href;
        a.target = '_blank';
        a.rel = 'noopener';

        if (isElectron && window.electronAPI && window.electronAPI.openExternal) {
          a.addEventListener('click', (e) => {
            e.preventDefault();
            window.electronAPI.openExternal(href);
          });
        }
        return a;
      }

      function emitRow(depth, fullPath) {
        for (let c = 0; c <= maxDepth; c++) {
          grid.appendChild(c === depth ? pillLink(fullPath.split('/').pop(), fullPath) : document.createElement('div'));
        }
      }

      function walk(parent, depth) {
        const children = kids.get(parent) || [];
        for (const c of children) { emitRow(depth, c); walk(c, depth + 1); }
      }

      for (const root of byDepth[0]) { emitRow(0, root); walk(root, 1); }
      trc.appendChild(grid);

    } catch(e) { /* ignore */ }
  }
  // ------------------ END TELEMETRY REFRESH LOOP -----------------


  // ------------------------ PREFLIGHT GATE -----------------------
  let pfInit = false;
  async function initPreflight(){
    if (pfInit) return; pfInit = true;

    const box  = $('preflight');
    const form = $('pf-form');
    const errs = $('pf-errors');
    const btn  = $('pf-apply');

    try {
      const r = await fetch('/preflight');
      const j = await r.json();
      __mc_meta = j.meta || { base: '', prefix: '' };
      const selected = new Set(j.outputs || []);
      for (const el of form.querySelectorAll('input[name="outputs"]')) {
        if (selected.size) el.checked = selected.has(el.value);
      }
    } catch {}

    function readControlPanelMeta(){
      const $ = (id) => document.getElementById(id);
      // Adjust these IDs to whatever you used in the Control Panel
      const shardsSel = $('cp-shards');           // <select> or <input>
      const outDirInp = $('cp-outdir');           // <input type="text">
      const prefixInp = $('cp-prefix');           // <input>
     const baseInp   = $('cp-base');             // <input>
      const headless  = $('cp-headless')?.checked;
      const multi     = $('cp-multi')?.checked;
      const keepPage  = $('cp-keeppage')?.checked;
     const shards    = shardsSel ? Number(shardsSel.value) : 0;
      // buckets rule: if multi-shards and shards>1 → 2*shards, else 1
      const bucketParts = (multi && shards > 1) ? (2 * shards) : 1;
      return {
       base: (baseInp?.value || '').trim(),
        prefix: (prefixInp?.value || '').trim(),
        outDir: (outDirInp?.value || '').trim(),
        keepPageParam: !!keepPage,
        headless: !!headless,
       shards: isFinite(shards) ? shards : 0,
        bucketParts,
        override: !!$('cp-override')?.checked,
       multiShards: !!multi
      };
   }

    btn.onclick = async () => {
      const outs = Array.from(form.querySelectorAll('input[name="outputs"]:checked'))
        .map(el => el.value);
      errs.textContent = '';

      // Pull state from your CP (make sure __mc_state() returns these keys)
      const s = (window.__mc_state && window.__mc_state()) || {};
      const meta = {
        base: (s.base || '').trim(),
        prefix: (s.prefix || '').trim(),
        outDir: (s.outDir || '').trim() || './dist',
        keepPageParam: !!s.keepPageParam,
        headless: !!s.headless,
        prod: !!s.prod,
        maxShards: !!s.maxShards,
        multiShards: !!s.multi,
        shards: Number(s.shards || 0) || 0,
        bucketParts: (s.multi && Number(s.shards) > 1) ? (2 * Number(s.shards)) : 1,
        override: !!s.override,
        inputPath: (s.inputPath || '').trim()
      };


      const prevLabel = btn.textContent;
      btn.disabled = true;
      btn.textContent = 'Applying…';
      try {
        // Apply (save config.json)
        const r = await fetch('/config', {
          method: 'POST',
          headers: { 'content-type': 'application/json' },
          body: JSON.stringify({ outputs: outs, meta })
        });
        const j = await r.json();
        if (!r.ok || !j.valid) {
          errs.innerHTML = (j.errors || []).map(e => '• ' + e.key + ': ' + e.reason).join('<br>');
          return;
        }

        // Make sure UI has the same meta for link building, CLI preview, etc.
        window.__mc_meta = j.meta || meta;

        // “Apply and start”: immediately POST /start
        const r2 = await fetch('/start', { method: 'POST' });
        if (!r2.ok) {
          errs.textContent = 'Apply succeeded but Start failed — press Start.';
          return;
        }

        // Hide panel once we’ve started
        box.style.display = 'none';
      } catch (e) {
        errs.textContent = 'Failed to apply selection: ' + (e && e.message ? e.message : e);
      } finally {
        btn.disabled = false;
        btn.textContent = prevLabel;
      }
    };


}

  initPreflight();
  setInterval(tick, 700);
  tick();
  // ---------------------- END PREFLIGHT GATE ---------------------
</script>
</body>
</html>`;



// ---------- helpers ----------
function readConfigFile() {
   try { return JSON.parse(fs.readFileSync(CONFIG_FILE(), 'utf8')); } catch { return null; }
 }
 
// Small helpers to respond exactly once
 function sendJson(res, status, obj) {
   if (res.writableEnded) return;
   res.writeHead(status, { 'content-type': 'application/json' });
   res.end(JSON.stringify(obj));
 }
function sendText(res, status, text, mime = 'text/plain; charset=utf-8') {
   if (res.writableEnded) return;
   res.writeHead(status, { 'content-type': mime });
   res.end(text);
 }

function openBrowser(url) {
  const plat = os.platform();
  try {
    if (plat === 'win32') exec(`start "" "${url}"`);
    else if (plat === 'darwin') exec(`open "${url}"`);
    else exec(`xdg-open "${url}"`);
  } catch {}
}

function logEvent(type, payload) {
  try {
    STATE.events.push({ t: Date.now(), type, payload });
    if (STATE.events.length > 1000) STATE.events.splice(0, STATE.events.length - 1000);
  } catch {}
}

// ---------- export ----------
const API = {
  startServer: init,
  stop,
  event,
  bump,
  setMode,
  setStepper,
  step,
  setStep: step,
  threadStatus,
  workerUpdate: (...a) => threadStatus(
    typeof a[0] === 'number' ? { workerId: a[0], ...(a[1] || {}) } : (a[0] || {})
  ),
  bucketOwner,
  bucketProgress,
  treeAdd,
  snapshot,
};


API.waitForApplyAndStart = waitForApplyAndStart;
API.markApplied = markApplied;
API.markStart = markStart;
API.requestStop = requestStop;
API.stopRequested = stopRequested;


function remote() {
  const port = Number(process.env.TELEMETRY_PORT || 0);
  const send = (msg) => {
    if (!port) return;
    try {
      const data = Buffer.from(JSON.stringify(msg));
      const req = require('http').request(
        { method:'POST', hostname:'127.0.0.1', port, path:'/update',
          headers:{'content-type':'application/json','content-length':data.length}},
        r => r.resume()
      );
      req.on('error', ()=>{}); req.write(data); req.end();
    } catch {}
  };
  return {
    thread: (info) => send({ type:'thread', info }),
    bucket: (bucket, data) => send({ type:'bucket', bucket, data }),
    tree:   (pathSegs) => send({ type:'tree', pathSegs }),
    bump:   (metric, delta=1) => send({ type:'bump', metric, delta }),
    step:   (s) => send({ type:'step', step:s }),
    mode:   (m) => send({ type:'mode', mode:m }),
  };
}

// —— Telemetry run-state (single-session init; no auto resets) ——
let STOP_REQUESTED = false;

// Ensure we only create the session file once per app session
function sessionFile(){
  const dir = path.join(LAUNCH.outDir || 'dist', 'telemetry');
  fs.mkdirSync(dir, { recursive: true });
  return path.join(dir, 'session.json');
}
function ensureSessionOnce(){
  const f = sessionFile();
  if (!fs.existsSync(f)) {
    fs.writeFileSync(f, JSON.stringify({ initialized:true, startedRuns:0 }, null, 2));
    // NOTE: If you ever want "cold-start" cleanups, put them here.
  }
}

// These three flags already exist at the top of the file; keep using them.
function markApplied(){ APPLIED = true; }
function markStart(){
  STARTED = true; STOP_REQUESTED = false;
  // start wall-clock timer for UI
  try { STATE.startedAt = Date.now(); } catch {}
  const f = sessionFile();
  const s = fs.existsSync(f) ? JSON.parse(fs.readFileSync(f,'utf8')) : { initialized:true, startedRuns:0 };
  s.startedRuns = (s.startedRuns||0)+1;
  fs.writeFileSync(f, JSON.stringify(s,null,2));
}
function requestStop(){ STOP_REQUESTED = true; }
function stopRequested(){ return !!STOP_REQUESTED; }

/**
 * Wait until the user has pressed “Apply and start” in the telemetry UI.
 * This polls the persisted telemetry/config.json that your UI writes:
 *   { valid:true, started:true, ... }
 */
async function waitForApplyAndStart(){
  while (true) {
    try {
      const cfg = readConfigFile();
      if (cfg && cfg.valid === true && cfg.started === true && cfg.meta && cfg.meta.base) {
        return;
      }
    } catch {}
    await new Promise(r => setTimeout(r, 200));
  }
}


// Export a single, stable surface
module.exports = {
  ...API,                // startServer/stop/event/bump/... etc
  telemetry: API,        // legacy alias
  remote,                // child->parent http poster
  setLaunchContext,      // lets shard-run tell us input/outDir early
  markApplied,
  markStart,
  requestStop,
  stopRequested,
  waitForApplyAndStart
};

module.exports.setLaunchContext = setLaunchContext;
