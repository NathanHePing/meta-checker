// scripts/viz-tui.js
'use strict';

const fs = require('fs');
const path = require('path');
const readline = require('readline');

const OUT = process.argv[2] || 'dist';
const STATE = path.join(OUT, 'telemetry', 'state.json');

function fmtMs(ms){ const s = Math.floor(ms/1000); const h= (s/3600)|0; const m=((s%3600)/60)|0; const ss=s%60; return `${h}h ${m}m ${ss}s`; }
function clip(s, n){ s = String(s||''); return s.length>n ? s.slice(0,n-1)+'…' : s; }

async function draw(){
  if (process.stdout.isTTY) {
    readline.cursorTo(process.stdout, 0, 0);
    readline.clearScreenDown(process.stdout);
  }


  let state = null;
  try { state = JSON.parse(fs.readFileSync(STATE, 'utf8')); } catch {}
  if (!state){ console.log('waiting for telemetry…'); return; }

  const startedAt = state.startedAt || Date.now();
  const dur = fmtMs(Date.now() - startedAt);
  const steps = (state.stepper && state.stepper.steps) || [];
  const STEP_HELP = {
    'seed-scan':   'Scan base URL to find top-level sections (first seeds).',
    'discover':    'Crawl site to discover URLs (respects pathPrefix, robots, and de-dupes).',
    'fetch':       'Open pages in headless browser and collect meta/title/links.',
    'compare':     'Compare fetched meta to your input CSV/TSV titles and descriptions.',
    'merge-urls':  'Merge per-worker URL parts into a unique final URL list.',
    'cleanup':     'Remove temp artifacts and write final reports.',
    'done':        'Run completed. Reports are ready in the output folder.',
  };
  const cur   = (state.stepper && state.stepper.currentIndex) || 0;


  console.log(`== Meta-checker Viz ==   mode: ${(state && state.mode) || ''}   elapsed: ${dur}`);

  console.log(`Step ${cur+1}/${steps.length}: ${steps.map((s,i)=> i===cur ? `[${s}]` : s).join(' > ')}`);
  const stepName = steps[cur] || '';
  const help = STEP_HELP[stepName] || '';
  if (help) console.log('→ ' + help);
  console.log('');

  const urlsFound      = (state.totals && state.totals.urlsFound)     || 0;
  const internalLinks  = (state.totals && state.totals.internalEdges) || 0;
  const threadCount    = Object.values(state.threads || {}).filter(t => {
    const hasId = (t.info && (t.info.workerId != null || t.info.pid != null)) || t.pid != null;
    const phase = t.phase || (t.info && t.info.phase) || '';
    return hasId && phase !== 'input-confirm';
  }).length;
  const bucketCount    = Object.keys(state.buckets || {}).filter(k => k && k !== 'undefined' && k !== 'NaN').length;

  console.log(`Summary: urls=${urlsFound}  internalLinks=${internalLinks}  threads=${threadCount}  buckets=${bucketCount}`);
  console.log('');


  // Flatten and filter threads to what's actually “live”
  const threads = Object.entries(state.threads || {})
    .map(([id, w]) => ({ id, ...w }))
    .filter(w => {
      const hasId = (w.info && (w.info.workerId != null || w.info.pid != null)) || w.pid != null;
      const phase = w.phase || (w.info && w.info.phase) || '';
      return hasId && phase !== 'input-confirm';
    })
    .sort((a, b) => (+(a.info?.workerId ?? a.id) - +(b.info?.workerId ?? b.id)));

  console.log('Workers:');
  console.log(' id  pid      phase        bucket  sw/idle   url');

  for (const w of threads){
    const id     = (w.info?.workerId != null) ? String(w.info.workerId) : String(w.id);
    const pid    = String(w.info?.pid ?? w.pid ?? '');
    const phase  = String(w.phase || w.info?.phase || '');
    const bucket = (w.info && (w.info.bucket ?? w.bucket)) ?? '-';
    const swIdle = String(((w.switches||0)) + '/' + ((w.idle||0)));
    const url    = String(w.url || w.info?.url || '');

    console.log(
      `${id.padStart(2)}  ${pid.padEnd(8)}  ${clip(phase,12).padEnd(12)}  ${String(bucket).padStart(6)}  ` +
      `${swIdle.padEnd(7)}  ${clip(url, 80)}`
    );
  }

  console.log('');

  // Buckets
  const bid = Object.keys(state.buckets || {}).sort((a,b)=> (+a)-(+b));
  console.log('Buckets:');
  console.log(' r   owner        processed  pending   bytes     cursor');
  for (const r of bid){
    const b = (state.buckets || {})[r] || {};
    const owner = clip(b.owner||'-', 12);
    const processed = Number(b.processed || b.cursor || 0);
    const pending   = Number(b.pending   || 0);
    const bytes     = Number(b.bytes     || 0);
    const cursor    = Number(b.cursor    || 0);
    console.log(
      `${String(r).padStart(2)}  ${owner.padEnd(12)}  ${String(processed).padStart(9)}  ${String(pending).padStart(8)}  ${String(bytes).padStart(10)}  ${String(cursor).padStart(8)}`
    );
  }

}

function loop(){
  draw().catch(()=>{});
  setTimeout(loop, 500);
}

console.clear();
loop();
