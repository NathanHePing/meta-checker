// src/discover/crawler.js
'use strict';

// --- telemetry (safe wrapper over utils/telemetry) ---
let tele;
try { const tmod = require('../utils/telemetry'); tele = tmod.telemetry || tmod; } catch {}
const T = {
  thread(info)      { try { tele?.threadStatus?.(info); }           catch {} },
  owner(r, owner)   { try { tele?.bucketOwner?.(r, owner); }        catch {} },
  progress(r,d,p)   { try { tele?.bucketProgress?.(r,d,p); }        catch {} },
  event(obj)        { try { tele?.event?.(obj); }                   catch {} },
  step(s)           { try { tele?.step?.(s); }                      catch {} },
  tree(segs)        { try { tele?.treeAdd?.(segs); }                catch {} },
  bump(k,n)         { try { tele?.bump?.(k,n); }                    catch {} },
};

const fs   = require('fs');
const path = require('path');

// --- tiny retry for Windows/OneDrive EBUSY/EPERM during reads ---
function sleepMs(ms){ const sab = new SharedArrayBuffer(4); Atomics.wait(new Int32Array(sab), 0, 0, ms); }
function readFileWithRetry(p, enc='utf8', tries=40){
  let last;
  for (let i=0;i<tries;i++){
    try { return fs.readFileSync(p, enc); }
    catch (e){ if (e && (e.code==='EBUSY' || e.code==='EPERM')) { last=e; sleepMs(20+i*10); continue; } throw e; }
  }
  throw last || new Error(`Failed to read after ${tries} attempts: ${p}`);
}

// ---------- tiny helpers for quiescence + lock counts ----------
function countLocks(dir) {
  try {
    return fs.readdirSync(dir).filter(f => f.endsWith('.lock')).length;
  } catch {
    return 0;
  }
}

function frontierSnapshot(frontierDir, discoLocks) {
  try {
    let bytes = 0;
    let newest = 0;
    for (const f of fs.readdirSync(frontierDir)) {
      if (!/^bucket\.\d+\.ndjson$/i.test(f)) continue;
      const p = path.join(frontierDir, f);
      const st = fs.statSync(p);
      bytes += st.size;
      if (st.mtimeMs > newest) newest = st.mtimeMs;
    }
    const locks = countLocks(discoLocks);
    return `${bytes}:${Math.floor(newest)}:${locks}`;
  } catch {
    return 'ERR';
  }
}

// content-based emptiness check (ignores mtime flapping)
function bucketStats(frontierDir) {
  try {
    let lines = 0, bytes = 0;
    for (const f of fs.readdirSync(frontierDir)) {
      if (!/^bucket\.\d+\.ndjson$/i.test(f)) continue;
      const p = path.join(frontierDir, f);
      const txt = readFileWithRetry(p, 'utf8');
      bytes += Buffer.byteLength(txt);
      lines += txt.split(/\r?\n/).filter(Boolean).length;
    }
    return { lines, bytes };
  } catch {
    return { lines: 0, bytes: 0 };
  }
}

const {
  appendToFrontier,
  claimNext,
  appendToBuckets,
  claimNextBucket,
  acquireBucketOwner
} = require('./frontier');

/**
 * Crawl the site and return a de-duplicated array of normalized URLs.
 *
 * Modes:
 *  1) Bucketed frontier (preferred):    frontierDir + discoLocks + partIndex/partTotal (+ bucketParts)
 *  2) Legacy single frontier file:      frontierFile + discoLocks + partIndex/partTotal
 *  3) Fallback BFS (single-process):    no frontier args
 *
 * Also writes an internal-links NDJSON per worker into outDir:
 *   internal-links.part{workerId}.ndjson
 */
async function crawlSite(context, base, opts) {
  opts = opts || {};
  const {
    // scope
    pathPrefix     = '/en-us',
    maxPages       = 50000,
    keepPageParam  = false,

    // logging
    logger,

    // legacy frontier
    frontierFile,

    // bucketed frontier
    frontierDir,
    discoLocks,
    partIndex      = 0,
    partTotal      = 1,
    bucketParts, // defaulted later to W

    // file outputs
    outDir         = 'dist',
    workerId,
    workerTotal
    } = opts;
    const log = logger || {
      info:  console.log.bind(console),
      warn:  console.warn.bind(console),
      error: console.error.bind(console),
      debug: console.log.bind(console),
    };
    log.info('telemetry env', { TELEMETRY_PORT: process.env.TELEMETRY_PORT || '' });

  // identifiers for logs/files
  const W  = Number(partTotal) || 1;
  const me = Number(partIndex) || 0;
  const myWorkerId  = Number.isFinite(workerId) ? Number(workerId) : (me + 1);
  const myWorkerTot = Number.isFinite(workerTotal) ? Number(workerTotal) : W;
  T.thread({ workerId: myWorkerId, phase: 'ready' });

  // per-worker file for internal-links rows
  fs.mkdirSync(outDir, { recursive: true });
  const linksPartFile = path.join(outDir, `internal-links.part${myWorkerId}.ndjson`);

  // base/origin helpers
  const baseUrl  = new URL(base);
  const origin   = baseUrl.origin;
  const baseHost = baseUrl.hostname;

  // crude eTLD+1 (good enough for ping.com)
  const etld1 = (host) => host.split('.').slice(-2).join('.'); // "stage.ping.com" -> "ping.com"
  const sameSite = (u) => etld1(u.hostname) === etld1(baseHost);

  // normalize query params and trailing slashes
  function normalizeQuery(u) {
    const q = new URLSearchParams(u.search);
    // keep only ?page= if keepPageParam=true; else drop all
    for (const k of Array.from(q.keys())) {
      const keep = keepPageParam ? /^page$/i.test(k) : false;
      if (!keep) q.delete(k);
    }
    u.search = q.toString() ? `?${q.toString()}` : '';
    if (u.pathname.length > 1) u.pathname = u.pathname.replace(/\/+$/, '');
  }

  // strict normalizer used for frontier participation and visited list
  function acceptAndNormalize(rawUrl, baseForRel) {
    try {
      const u0 = new URL(rawUrl, baseForRel || origin);
      if (!sameSite(u0)) return null; // only same-site in the crawl frontier

      // rewrite to base origin, keep path+search
      const u = new URL(u0.pathname + u0.search, origin);

      // scope to prefix
      if (pathPrefix && !u.pathname.startsWith(pathPrefix)) return null;

      // drop obvious non-HTML assets
      if (/\.(png|jpe?g|gif|webp|svg|ico|pdf|zip|mp4|webm|css|js|woff2?|ttf|otf)(\?|$)/i.test(u.toString())) return null;

      normalizeQuery(u);
      return u.toString();
    } catch {
      return null;
    }
  }

  // looser normalizer for the internal-links “edge” rows (include external links as well)
  function normalizeForEdge(rawUrl, baseForRel) {
    try {
      const u0 = new URL(rawUrl, baseForRel || origin);
      if (!/^https?:$/i.test(u0.protocol)) return null;

      // ignore non-HTML-ish assets
      if (/\.(png|jpe?g|gif|webp|svg|ico|pdf|zip|mp4|webm|css|js|woff2?|ttf|otf)(\?|$)/i.test(u0.pathname)) return null;

      const internal = sameSite(u0);
      let u = u0;

      if (internal) {
        // rewrite to base origin, normalize query to match our internal canonical form
        u = new URL(u0.pathname + u0.search, origin);
        normalizeQuery(u);
      }
      return { url: u.toString(), internal };
    } catch {
      return null;
    }
  }

  // throttle heavy/analytics
  try {
    await context.route('**/*', (route) => {
      const u = route.request().url();
      if (/\.(png|jpe?g|gif|webp|svg|ico|woff2?|ttf|otf|mp4|webm|avi|mov)(\?|$)/i.test(u)) return route.abort();
      if (/(googletagmanager|google-analytics|doubleclick|facebook|segment|mixpanel|hotjar)\./i.test(u)) return route.abort();
      return route.continue();
    });
  } catch {
    // route probably already installed by orchestrator—ignore
  }

  // ---------- in-page instrumentation ----------

  // minimal SPA recorder (prevents real nav during probe)
  async function installSpaRecorder(page) {
    await page.evaluate(() => {
      if (window.__navRecorderInstalled) return;
      window.__navRecorderInstalled = true;
      window.__navs = [];
      const record = (u) => {
        try { window.__navs.push(new URL(u, location.href).href.split('#')[0]); } catch {}
      };

      const hp = history.pushState.bind(history);
      const hr = history.replaceState.bind(history);
      history.pushState    = function (s,t,u){ if (u) record(u); return hp(s,t,u); };
      history.replaceState = function (s,t,u){ if (u) record(u); return hr(s,t,u); };

      const la = location.assign.bind(location);
      const lr = location.replace.bind(location);
      location.assign      = function (u){ if (u) record(u); /* swallow during probe */ };
      location.replace     = function (u){ if (u) record(u); /* swallow during probe */ };

      const wo = window.open.bind(window);
      window.open          = function (u){ if (u) record(u); return null; };

      document.addEventListener('click', (e) => {
        const a = e.target && e.target.closest && e.target.closest('a[href]');
        if (a) e.preventDefault();
      }, true);
    });
  }

  // collect DOM/link-like candidates (anchors, role=link, data-href/url, simple onclick)
  async function collectDomCandidates(page) {
    return await page.evaluate(() => {
      const toAbs = (h) => { try { return new URL(h, location.href).href; } catch { return ''; } };
      const bad   = /^(javascript:|mailto:|tel:|data:)/i;
      const fromOnclick = (code) => {
        if (!code) return '';
        const m =
          code.match(/(?:window\.open|location\.(?:assign|replace))\(\s*['"]([^'"]+)['"]\s*\)/i) ||
          code.match(/(?:window\.location|location)\s*=\s*['"]([^'"]+)['"]/i) ||
          code.match(/location\.href\s*=\s*['"]([^'"]+)['"]/i);
        return m ? m[1] : '';
      };

      const nodes = Array.from(document.querySelectorAll(
        'a[href], [role="link"], button, [role="button"], [data-href], [data-url], [onclick], [role="menuitem"]'
      ));
      const out = [];
      for (const el of nodes) {
        if (el.hasAttribute && el.hasAttribute('href')) {
          const raw = el.getAttribute('href') || '';
          if (!raw || bad.test(raw)) continue;
          out.push(toAbs(raw.split('#')[0]));
        } else if (el.getAttribute) {
          const raw = el.getAttribute('data-href') || el.getAttribute('data-url') || '';
          if (raw && !bad.test(raw)) out.push(toAbs(raw.split('#')[0]));
          else if (el.hasAttribute && el.hasAttribute('onclick')) {
            const dest = fromOnclick(el.getAttribute('onclick') || '');
            if (dest && !bad.test(dest)) out.push(toAbs(dest.split('#')[0]));
          }
        }
      }
      return Array.from(new Set(out));
    });
  }

  // trigger client routers without leaving the page
  async function probeSpa(page, limit = 60) {
    const clickables = await page.$$('a[href], [role="link"], [data-href], [data-url], [role="menuitem"]');
    for (const el of clickables.slice(0, limit)) {
      try { await el.click({ timeout: 300 }); } catch {}
    }
    try { await page.waitForTimeout(150); } catch {}
    return await page.evaluate(() => Array.from(new Set(window.__navs || [])));
  }

  // build a map of target URL -> text/label once per page (O(N) instead of O(N²))
  async function buildHrefTextMap(page) {
    return await page.evaluate(() => {
      const toAbs = (h) => { try { return new URL(h, location.href).href.split('#')[0]; } catch { return ''; } };
      const nodes = Array.from(document.querySelectorAll('a[href], [role="link"], [data-href], [data-url], [role="menuitem"]'));
      const map = {};
      for (const el of nodes) {
        let target = '';
        if (el.hasAttribute('href')) {
          target = toAbs(el.getAttribute('href') || '');
        } else if (el.hasAttribute('data-href') || el.hasAttribute('data-url')) {
          target = toAbs(el.getAttribute('data-href') || el.getAttribute('data-url') || '');
        }
        if (!target) continue;
        const txt = (el.getAttribute('aria-label') ||
                     el.textContent ||
                     el.getAttribute('title') ||
                     (el.querySelector && el.querySelector('img') && el.querySelector('img').getAttribute('alt')) ||
                     '' ).trim().replace(/\s+/g, ' ').slice(0, 200);
        if (!map[target]) map[target] = txt;
      }
      return map;
    });
  }

  // emit internal-links rows for this page (includes internal & external edges)
  async function emitInternalLinks(page, mergedCandidates) {
    try {
      const pageNorm = acceptAndNormalize(page.url(), page.url());
      if (!pageNorm) return;

      const p = new URL(pageNorm);
      const seg = p.pathname.replace(/\/+$/, '').replace(/^\/+/, '').split('/')[0] || '';

      const hrefToTextMap = await buildHrefTextMap(page);

      const lines = [];
      for (const raw of mergedCandidates) {
        const edge = normalizeForEdge(raw, page.url());
        if (!edge) continue;
        const label = hrefToTextMap[edge.url] || '';
        lines.push(JSON.stringify({
          page_url: pageNorm,
          link_url: edge.url,
          link_text: label,
          path_label: seg,
          link_type: edge.internal ? 'internal' : 'external',
          kind: 'candidate'
        }));
      }
      if (lines.length) fs.appendFileSync(linksPartFile, lines.join('\n') + '\n', 'utf8');
      T.bump('internalEdges', mergedCandidates.length || 0);
    } catch (e) {
      log.warn('[internal-links] ' + (e && e.message ? e.message : String(e)));
    }
  }

  // ===================== BUCKETED FRONTIER (preferred) =====================
  if (frontierDir && discoLocks) {
    const B = Number(bucketParts || W);
    log.info(`Frontier crawl (parts=${W}, me=${me}, buckets=${B}) starting…`);

    try {
    telemetry.event({
      type: 'frontier/start',
      parts: opts.bucketParts || 1,
      me:    opts.partIndex || 0,
      buckets: opts.bucketParts || 1
    });
  } catch {}


    const results   = new Set();
    let idleCycles  = 0;

    // preferred buckets for this worker: i, i+W, i+2W, …
    const preferred  = [];
    for (let r = me; r < B; r += W) preferred.push(r);
    const allBuckets = Array.from({ length: B }, (_, r) => r);

    // process a single bucket r with ownership lock
    const tryOneBucket = async (r) => {
      try { T.owner(r, myWorkerId); } catch {}
      const owner = acquireBucketOwner(frontierDir, r, `S${myWorkerId}/${myWorkerTot}`);
      if (!owner) return false; // another worker holds this bucket

      let claimedAny = false;
      let localIdle  = 0;

      try {
        while (results.size < maxPages) {
          T.thread({ workerId: myWorkerId, phase: 'bucket', bucket: r });

          const claim = claimNextBucket(
            frontierDir,
            discoLocks,
            r,
            B,
            (raw) => !!acceptAndNormalize(raw, origin)
          );

          if (!claim) {
            localIdle++;
            if (localIdle >= 6) break;               // release bucket so others can try it
            await new Promise(res => setTimeout(res, 100));
            continue;
          }

          localIdle = 0;
          claimedAny = true;

          const { url, release, complete } = claim;
          const page = await context.newPage();
          try {
            T.thread({ workerId: myWorkerId, phase: 'fetch', url });
            await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 15000 });
            try { await page.waitForLoadState('networkidle', { timeout: 3000 }); } catch {}
            const finalNorm = acceptAndNormalize(page.url(), origin);
            if (finalNorm) {
              results.add(finalNorm);
              try {
                const segs = new URL(finalNorm).pathname.replace(/^\/+|\/+$/g, '').split('/').filter(Boolean);
                if (segs.length) T.tree(segs);
              } catch {}
            }

            await installSpaRecorder(page);
            const domCandidates = await collectDomCandidates(page);
            const spaNavs       = await probeSpa(page);
            const merged        = Array.from(new Set([...domCandidates, ...spaNavs]));

            // emit internal-links edges
            if (merged.length) await emitInternalLinks(page, merged);

            // discoveries -> correct buckets (appendToBuckets routes by hash % B)
            const discoveries = [];
            for (const h of merged) {
              const n = acceptAndNormalize(h, page.url());
              if (n) discoveries.push(n);
            }
            if (discoveries.length) {
              const unique = Array.from(new Set(discoveries));
              appendToBuckets(frontierDir, unique, B);
              T.thread({ workerId: myWorkerId, phase: 'discover', added: unique.length });
              log.info('discoveries', { from: url, added: unique.length, totalSeen: results.size });
              try {
                telemetry.event({
                  type: 'frontier/discoveries',
                  from: url,
                  added: unique.length,
                  totalSeen: results.size
                });
              } catch {}
            }


            try { complete(); } catch {}
          } catch (e) {
            log.warn('X error on ' + url + ': ' + String((e && e.message) || e).split('\n')[0]);
          } finally {
            try { await page.close(); } catch {}
            try { release(); } catch {}
          }
        }
      } finally {
        try { owner.release(); } catch {}
      }
      T.owner(r, `W${myWorkerId}`);
      return claimedAny;
    };

    // main work/idle loop with global quiescence detection
    let stableCycles = 0;
    let lastSnap = '';

    while (results.size < maxPages) {
      // 1) work preferred buckets
      let didWork = false;
      for (const r of preferred) {
        const used = await tryOneBucket(r);
        if (used) didWork = true;
      }
      if (didWork) { idleCycles = 0; stableCycles = 0; continue; }

      // 2) opportunistic steal: try any bucket
      for (const r of allBuckets) {
        const used = await tryOneBucket(r);
        if (used) { didWork = true; break; }
      }
      if (didWork) { idleCycles = 0; stableCycles = 0; continue; }

      // 3) nothing to do; check if frontier is globally quiet
      idleCycles++;
      const snap = frontierSnapshot(frontierDir, discoLocks);
      stableCycles = (snap && snap === lastSnap) ? (stableCycles + 1) : 0;
      lastSnap = snap;

      // hard quiescence — all buckets empty and no locks => we are done
      const pending  = bucketStats(frontierDir);
      const locksNow = countLocks(discoLocks);
      if (pending.lines === 0 && locksNow === 0) {
        if (stableCycles >= 5 || idleCycles >= 50) {
          log.info('frontier empty — exiting', { me, seen: results.size, bytes: pending.bytes, stableCycles, idleCycles });
          break;
        }
      }

      if (idleCycles % 50 === 0) {
        const locks = countLocks(discoLocks);
        log.info('frontier idle', { me, seen: results.size, locks });
        T.event({ type: 'bucket/switch', workerId: myWorkerId });
      }
      if (stableCycles >= 60) {
        const locks = countLocks(discoLocks);
        log.info('frontier settled — exiting', { me, seen: results.size, locks });
        break;
      }

      await new Promise(res => setTimeout(res, 200));
    }

    log.info(`Crawl done. Collected ${results.size} URLs.`);
    return Array.from(results);
  }

  // ===================== LEGACY FRONTIER (single queue file) =====================
  if (frontierFile && discoLocks) {
    log.info(`Frontier crawl (parts=${W}, me=${me}) starting…`);
    const results  = new Set();
    let idleCycles = 0;

    while (results.size < maxPages) {
      const claim = claimNext(
        frontierFile,
        discoLocks,
        me,
        W,
        (raw) => !!acceptAndNormalize(raw, origin)
      );

      if (!claim) {
        idleCycles++;
        if (idleCycles % 50 === 0) log.info('frontier idle', { me, seen: results.size });
        if (idleCycles >= 250) {
          T.event({ type: 'bucket/switch', workerId: myWorkerId });
          log.info('frontier settled — exiting', { me, seen: results.size });
          break;
        }
        await new Promise(res => setTimeout(res, 200));
        continue;
      }

      idleCycles = 0;

      const { url, release, complete } = claim; // complete is a no-op in legacy
      const page = await context.newPage();
      try {
        T.thread({ workerId: myWorkerId, phase: 'fetch', url });
        await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 15000 });
        try { await page.waitForLoadState('networkidle', { timeout: 3000 }); } catch {}

        const finalNorm = acceptAndNormalize(page.url(), origin);
        if (finalNorm) {
          results.add(finalNorm);
          try {
            const segs = new URL(finalNorm).pathname.replace(/^\/+|\/+$/g, '').split('/').filter(Boolean);
            if (segs.length) T.tree(segs);
          } catch {}
        }

        await installSpaRecorder(page);
        const domCandidates = await collectDomCandidates(page);
        const spaNavs       = await probeSpa(page);
        const merged        = Array.from(new Set([...domCandidates, ...spaNavs]));

        if (merged.length) await emitInternalLinks(page, merged);

        const discoveries = [];
        for (const h of merged) {
          const n = acceptAndNormalize(h, page.url());
          if (n) discoveries.push(n);
        }
        if (discoveries.length) {
          const unique = Array.from(new Set(discoveries));
          appendToFrontier(frontierFile, unique);
          T.thread({ workerId: myWorkerId, phase: 'discover', added: unique.length });
          if (unique.length) {
            try { T.event({ type:'discovered', from: url, count: unique.length }); } catch {}
          }
        }

        try { complete && complete(); } catch {}
      } catch (e) {
        log.warn('X error on ' + url + ': ' + String((e && e.message) || e).split('\n')[0]);
      } finally {
        try { await page.close(); } catch {}
        try { release && release(); } catch {}
      }
    }

    log.info(`Crawl done. Collected ${results.size} URLs.`);
    return Array.from(results);
  }

  // ===================== FALLBACK BFS (single-process) =====================
  const seed   = new URL(String(pathPrefix || '/').replace(/^\/*/, '/'), origin).toString().replace(/\/$/, '');
  const home   = new URL('/', origin).toString().replace(/\/$/, '');
  const starts = pathPrefix ? [seed] : [home, seed];

  const queue    = starts.slice();
  const seen     = new Set();
  const enqueued = new Set();
  const urls     = [];
  let visited    = 0;

  log.info(`Crawling (maxPages=${maxPages}) starting at: ${JSON.stringify(starts)} ...`);

  while (queue.length && urls.length < maxPages) {
    const next = queue.shift();
    if (!next || seen.has(next)) continue;
    seen.add(next);

    const page = await context.newPage();
    try {
      T.thread({ workerId: myWorkerId, phase: 'fetch', url: next });
      await page.goto(next, { waitUntil: 'domcontentloaded', timeout: 15000 });
      try { await page.waitForLoadState('networkidle', { timeout: 3000 }); } catch {}
      visited++;

      await installSpaRecorder(page);

      // record final URL
      const finalNorm = acceptAndNormalize(page.url(), origin);
      if (finalNorm && !urls.includes(finalNorm)) {
        urls.push(finalNorm);
        try {
          const segs = new URL(finalNorm).pathname.replace(/^\/+|\/+$/g, '').split('/').filter(Boolean);
          if (segs.length) T.tree(segs);
        } catch {}
      }

      // collect & normalize candidates
      const domCandidates = await collectDomCandidates(page);
      const spaNavs       = await probeSpa(page);
      const merged        = Array.from(new Set([...domCandidates, ...spaNavs]));

      if (merged.length) await emitInternalLinks(page, merged);

      for (const h of merged) {
        const n = acceptAndNormalize(h, page.url());
        if (!n) continue;
        if (!seen.has(n) && !enqueued.has(n) && (urls.length + queue.length) < maxPages) {
          queue.push(n);
          enqueued.add(n);
        }
      }

      if (visited % 5 === 0) {
        log.info(`Progress: visited=${visited} | collected=${urls.length} | inQueue=${queue.length}`);
      }
    } catch (e) {
      log.warn(`X error on ${next}: ${String((e && e.message) || e).split('\n')[0]}`);
    } finally {
      try { await page.close(); } catch {}
    }
  }

  log.info(`Crawl done. Collected ${urls.length} URLs.`);
  return Array.from(new Set(urls)).filter(u => {
    try { const uu = new URL(u); return !pathPrefix || uu.pathname.startsWith(pathPrefix); }
    catch { return false; }
  });
}

module.exports = { crawlSite };
