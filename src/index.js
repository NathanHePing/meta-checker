// src/index.js
'use strict';

const minimist = require('minimist');
const path = require('path');
const fs = require('fs');
const { run } = require('./run');
const { sanitizePathPrefix } = require('./config');

const asInt = (v, d) => {
  const n = parseInt(v, 10);
  return Number.isFinite(n) ? n : d;
};

(async () => {
  const argv = minimist(process.argv.slice(2));

  if (!argv.base) {
    console.error('✖ Missing required flag: --base');
    process.exit(1);
  }

  // Shard info (for URL slicing) — 1-based shardIndex like 1..shards
  const shardIndex = asInt(argv.shardIndex, 1);
  const shards     = asInt(argv.shards, 1);

  // Optional “partitioned discovery” indices (legacy/advanced)
  const partIndex = argv.partIndex != null ? asInt(argv.partIndex, 0) : undefined; // 0-based
  const partTotal = argv.partTotal != null ? asInt(argv.partTotal, 1) : undefined;

  // Worker identity for filenames/logs; default to partIndex+1 if present, else 1
  const workerId = argv.workerId != null
    ? asInt(argv.workerId, 1)
    : (partIndex != null ? partIndex + 1 : 1);

  const workerTotal = argv.workerTotal != null
    ? asInt(argv.workerTotal, 1)
    : (partTotal != null ? partTotal : 1);

  const outDir = path.resolve(argv.outDir || 'dist');

  // Use workerId when available, otherwise fall back to shardIndex for per-worker artifacts
  const cachePart = (workerId != null ? workerId : shardIndex);

  const cfg = {
    // inputs
    input: argv.input || 'input.csv',
    base: argv.base,
    pathPrefix: sanitizePathPrefix(argv.pathPrefix || ''),
    keepPageParam: !!argv.keepPageParam,
    sameOrigin: argv.sameOrigin !== 'false',
    outDir,
    excelDelimiter: (argv.excelDelimiter || 'comma').toLowerCase(), // 'comma' | 'tab'
    hasHeader: (argv.hasHeader || 'auto').toLowerCase(),
    mode: (argv.mode || '').toLowerCase(),            // ✅ keep only this one

    // sharding/meta
    shardIndex,
    shards,

    // frontier / partitioned-discovery (optional / legacy)
    frontierFile: argv.frontierFile ? path.resolve(argv.frontierFile) : undefined,
    frontierDir:  argv.frontierDir  ? path.resolve(argv.frontierDir)  : undefined,
    discoLocks:   argv.discoLocks   ? path.resolve(argv.discoLocks)   : undefined,
    partIndex,
    partTotal,
    bucketParts:  argv.bucketParts ? asInt(argv.bucketParts, undefined) : undefined,

    // worker tag for logs
    workerId,
    workerTotal,

    // explicit URL list IO (3‑column mode)
    urlsFile:    argv.urlsFile    ? path.resolve(argv.urlsFile)    : undefined,
    urlsOutFile: argv.urlsOutFile ? path.resolve(argv.urlsOutFile) : undefined,

    // behavior flags
    rebuildLinks: argv.rebuildLinks === true || argv.rebuildLinks === 'true',
    dropCache:    argv.dropCache    === true || argv.dropCache    === 'true',
    reportsOnly:  argv.reportsOnly  === true || argv.reportsOnly  === 'true',
    forceRefresh: argv.forceRefresh === true || argv.forceRefresh === 'true',

    // performance
    concurrency:   asInt(argv.concurrency, 4),
    maxCrawlPages: asInt(argv.maxPages, 50000),
    cacheTtlDays:  asInt(argv.cacheTtlDays, 7),

    // per-worker artifacts
    // 🔁 align with run.js' fetch cache naming to avoid collisions with reports
    cachePath: path.join(outDir, `fetch-cache.part${cachePart}.json`),
    locksDir:  path.join(outDir, 'locks'),
  };

  try { fs.mkdirSync(cfg.outDir, { recursive: true }); } catch {}

  await run(cfg);
})().catch(err => {
  console.error(err && err.stack ? err.stack : err);
  process.exit(1);
});
