#!/usr/bin/env node
'use strict';

const path = require('path');
const { run } = require('./run');

// sensible defaults; allow flags to override
const argv = require('minimist')(process.argv.slice(2));
const cfg = {
  base: argv.base || process.env.BASE,
  outDir: path.resolve(argv.outDir || 'dist'),
  input: argv.input || '',
  pathPrefix: argv.prefix || '',
  keepPageParam: !!argv.keepPageParam,
  sameOrigin: argv.sameOrigin !== 'false',
  cacheTtlDays: Number(argv.cacheTtlDays || 7),
  concurrency: Number(argv.concurrency || 4),
  workerId: Number(argv.workerId || 1),
  workerTotal: Number(argv.workerTotal || 1),
  // (add any other flags you use)
};

if (!cfg.base) {
  console.error('Usage: crawler --base https://stage.example.com [--outDir out]');
  process.exit(2);
}

run(cfg).catch((e) => {
  console.error('[fatal]', e && e.stack || e);
  process.exit(1);
});
