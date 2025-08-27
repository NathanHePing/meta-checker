// src/config.js
const fs = require('fs');
const path = require('path');
const os = require('os');

function getArg(name, def) {
  const idx = process.argv.findIndex(a => a === `--${name}`);
  if (idx >= 0 && idx < process.argv.length - 1) return process.argv[idx + 1];
  const eq = process.argv.find(a => a.startsWith(`--${name}=`));
  if (eq) return eq.split('=').slice(1).join('=');
  return def;
}

function sanitizePathPrefix(raw){
  if (raw == null) return '';
  let s = String(raw).trim();
  if (/^https?:\/\//i.test(s)){
    try{ return new URL(s).pathname || ''; }catch{}
  }
  s = s.replace(/\\/g, '/');
  if (/^[A-Za-z]:\//.test(s)){
    const m = s.match(/\/[a-z]{2}-[a-z]{2}(?:\/.*)?$/i);
    if (m) s = m[0];
    else {
      const parts = s.split('/').filter(Boolean);
      s = '/' + parts.slice(-2).join('/');
    }
  }
  if (s === '/' || s === '') return '';
  if (!s.startsWith('/')) s = '/' + s;
  return s;
}

const cpuCount = os.cpus().length;

function ensureDir(d){
  fs.mkdirSync(d, { recursive: true });
  return d;
}

function loadConfig(){
  const configPath = path.resolve('meta-check.config.json');
  let fileCfg = {};
  try {
    if (fs.existsSync(configPath)) {
      fileCfg = JSON.parse(fs.readFileSync(configPath, 'utf8'));
    }
  } catch {}

  const outDir = path.resolve(getArg('outDir', fileCfg.outDir || 'dist'));
  ensureDir(outDir);

  const cfg = {
    base:            getArg('base', fileCfg.base || ''),
    input:           getArg('input', fileCfg.input || ''),
    pathPrefix:      sanitizePathPrefix(getArg('pathPrefix', fileCfg.pathPrefix ?? '/en-us')),
    sameOrigin:      String(getArg('sameOrigin', fileCfg.sameOrigin ?? 'true')).toLowerCase() !== 'false',
    keepPageParam:   String(getArg('keepPageParam', fileCfg.keepPageParam ?? 'false')).toLowerCase() === 'true',
    forceRefresh:    String(getArg('force', fileCfg.forceRefresh ?? 'false')).toLowerCase() === 'true',
    rebuildLinks:    String(getArg('rebuildLinks', fileCfg.rebuildLinks ?? 'true')).toLowerCase() !== 'false',
    dropCache:       String(getArg('dropCache', fileCfg.dropCache ?? 'false')).toLowerCase() === 'true',
    maxCrawlPages:   parseInt(getArg('maxPages', fileCfg.maxCrawlPages ?? '50000'), 10),
    concurrency:     parseInt(getArg('concurrency', fileCfg.concurrency ?? '6'), 10),
    cacheTtlDays:    parseInt(getArg('cacheTtlDays', fileCfg.cacheTtlDays ?? '7'), 10),
    hasHeader:       (getArg('hasHeader', fileCfg.hasHeader ?? 'auto') || 'auto').toLowerCase(),
    prefixWords:     parseInt(getArg('prefixWords', fileCfg.prefixWords ?? '4'), 10),
    fuzzyThreshold:  parseFloat(getArg('fuzzyThreshold', fileCfg.fuzzyThreshold ?? '0.6')),
    extrasMode:      (getArg('extrasMode', fileCfg.extrasMode ?? 'title') || 'title').toLowerCase(),
    excelDelimiter:  (getArg('excelDelimiter', fileCfg.excelDelimiter || 'comma') || 'comma').toLowerCase(),
    // File locations
    outDir,
    cachePath:       path.resolve(getArg('cachePath', fileCfg.cachePath || path.join(outDir, 'site_catalog.json'))),
    locksDir:        path.join(outDir, 'locks'),
    // Shards
    shards:          parseInt(getArg('shards', '1'), 10),
    shardIndex:      parseInt(getArg('shardIndex', '1'), 10),
    autoShards:      String(getArg('autoShards', 'false')).toLowerCase() === 'true',
    reportsOnly:     String(getArg('reportsOnly', 'false')).toLowerCase() === 'true',
    cpuCount
  };

  if (!cfg.base || !cfg.input) {
    throw new Error('Missing required flags: --base and --input');
  }

  // ensure dirs
  ensureDir(cfg.outDir);
  ensureDir(cfg.locksDir);

  return cfg;
}

module.exports = {
  loadConfig,
  getArg,
  sanitizePathPrefix
};
