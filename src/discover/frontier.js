// src/discover/frontier.js
'use strict';

const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const http = require('http');
const os   = require('os');
const { exec } = require('child_process');



// --- optional telemetry (safe no-op if module not present) ---
let telemetry = null;
try {
  const mod = require('../utils/telemetry');
  telemetry = mod && (mod.telemetry || mod.default || mod);
} catch {}
const tEmit = (event, ...args) => {
  try { telemetry && typeof telemetry[event] === 'function' && telemetry[event](...args); } catch {}
};

function sha1(s){ return crypto.createHash('sha1').update(String(s)).digest('hex'); }
function u32(s){ const b = Buffer.isBuffer(s) ? s : Buffer.from(s); return b.readUInt32BE(0); }
function hashUrl(url){ return u32(crypto.createHash('sha1').update(url).digest()); }

// -------- Windows/OneDrive hardening (retry on EBUSY/EPERM) --------
function sleepMs(ms){ const sab = new SharedArrayBuffer(4); Atomics.wait(new Int32Array(sab), 0, 0, ms); }

function readFileWithRetry(p, encOrOpts = null, tries = 40) {
  let last;
  for (let i = 0; i < tries; i++) {
    try { return fs.readFileSync(p, encOrOpts); }
    catch (e) {
      if (e && (e.code === 'EBUSY' || e.code === 'EPERM')) { last = e; sleepMs(20 + i * 10); continue; }
      throw e;
    }
  }
  throw last || new Error(`Failed to read after ${tries} attempts: ${p}`);
}
function appendFileWithRetry(p, data, tries = 40) {
  let last;
  for (let i = 0; i < tries; i++) {
    try { fs.appendFileSync(p, data, 'utf8'); return; }
    catch (e) {
      if (e && (e.code === 'EBUSY' || e.code === 'EPERM')) { last = e; sleepMs(20 + i * 10); continue; }
      throw e;
    }
  }
  throw last || new Error(`Failed to append after ${tries} attempts: ${p}`);
}
function writeFileWithRetry(p, data, enc = 'utf8', tries = 40) {
  let last;
  for (let i = 0; i < tries; i++) {
    try { fs.writeFileSync(p, data, enc); return; }
    catch (e) {
      if (e && (e.code === 'EBUSY' || e.code === 'EPERM')) { last = e; sleepMs(20 + i * 10); continue; }
      throw e;
    }
  }
  throw last || new Error(`Failed to write after ${tries} attempts: ${p}`);
}
function renameWithRetry(from, to, tries = 40) {
  let last;
  for (let i = 0; i < tries; i++) {
    try { fs.renameSync(from, to); return; }
    catch (e) {
      if (e && (e.code === 'EBUSY' || e.code === 'EPERM')) { last = e; sleepMs(20 + i * 10); continue; }
      throw e;
    }
  }
  throw last || new Error(`Failed to rename after ${tries} attempts: ${from} -> ${to}`);
}

// Small sleep primitive (used for lock retry)
function sleepMs(ms){
  try {
    const sab = new SharedArrayBuffer(4);
    Atomics.wait(new Int32Array(sab), 0, 0, Math.max(0, ms|0));
  } catch {
    const start = Date.now(); while (Date.now() - start < ms) {}
  }
}

// ---------- Legacy (single-file) helpers kept for backward compat ----------
function seedFrontier(file, urls) {
  fs.mkdirSync(path.dirname(file), { recursive: true });
  if (!fs.existsSync(file)) writeFileWithRetry(file, urls.map(u => u + '\n').join(''), 'utf8');
}
function appendToFrontier(file, urls) {
  if (!urls?.length) return;
  appendFileWithRetry(file, urls.map(u => u + '\n').join(''));
}
function* readFrontier(file) {
  if (!fs.existsSync(file)) return;
  const lines = readFileWithRetry(file, 'utf8').split(/\r?\n/);
  for (const line of lines) { const u = line.trim(); if (u) yield u; }
}

function tryClaim(locksDir, url) {
  fs.mkdirSync(locksDir, { recursive: true });
  const id = sha1(url);
  const lockPath = path.join(locksDir, `${id}.lock`);
  const donePath = path.join(locksDir, `${id}.done`);
  if (fs.existsSync(donePath)) return null; // already processed
  // Robust exclusive create with retries (Windows/OneDrive/AV can return EBUSY/EPERM transiently)
  let fd = -1;
  const MAX_TRIES = parseInt(process.env.MC_LOCK_TRIES || '60', 10); // ~6s worst-case
  const SLEEP_MS  = parseInt(process.env.MC_LOCK_SLEEP || '100', 10);
  for (let i = 0; i < MAX_TRIES; i++) {
    try {
      fd = fs.openSync(lockPath, 'wx'); // exclusive create
      break; // success
    } catch (e) {
      // If another worker already claimed (lock file exists), treat as "someone else owns it"
     if (e && (e.code === 'EEXIST')) return null;
      // Transient Windows/OneDrive/AV conditions — retry
      if (e && (e.code === 'EBUSY' || e.code === 'EPERM' || e.code === 'EACCES' || e.code === 'EPROTO')) {
        sleepMs(SLEEP_MS);
        continue;
      }
      // Unknown hard error — give up on this URL (behave like already claimed)
      return null;
    }
  }
  if (fd < 0) {
   // Still couldn't create after retries — skip this URL
    return null;
  }
  try {
    fs.writeFileSync(
      fd,
      JSON.stringify({ url, at: new Date().toISOString(), pid: process.pid }) + '\n'
    );
    const complete = () => {
      try { fs.closeSync(fd); renameWithRetry(lockPath, donePath); } catch {}
   };
    // Opportunistic GC of old .done files (best-effort)
      try {
        const dir = path.dirname(lockPath);
        const files = fs.readdirSync(dir).filter(f => f.endsWith('.done'));
        const maxDone = parseInt(process.env.MC_MAX_DONE || '5000', 10);
        if (files.length > maxDone) {
          const victims = files.slice(0, files.length - maxDone);
          for (const v of victims) try { fs.unlinkSync(path.join(dir, v)); } catch {}
        }
      } catch {}

    const release  = () => { try { fs.closeSync(fd); fs.unlinkSync(lockPath); } catch {} };
    return { lockPath, donePath, complete, release };
   } catch {
    try { if (fd >= 0) { fs.closeSync(fd); fs.unlinkSync(lockPath); } } catch {}
    return null; // treat as unclaimable this pass
  }
}

// Legacy claim (modulo on claim) — still exported for compat
function claimNext(frontierFile, locksDir, partIndex, partTotal, acceptFn) {
  for (const url of readFrontier(frontierFile)) {
    if (acceptFn && !acceptFn(url)) continue;
    if ((hashUrl(url) % partTotal) !== partIndex) continue;
    const claim = tryClaim(locksDir, url);
    if (claim) return { url, ...claim };
  }
  return null;
}

// ---------- Bucketed frontier (fast path) ----------
function bucketFiles(frontierDir, r){
  fs.mkdirSync(frontierDir, { recursive: true });
  return {
    file: path.join(frontierDir, `bucket.${r}.ndjson`),
    offset: path.join(frontierDir, `bucket.${r}.offset`)
  };
}

// Bytes left in a single bucket (non-negative)
function bucketPendingBytes(frontierDir, r){
  const { file, offset } = bucketFiles(frontierDir, r);
  try {
    const size = fs.existsSync(file) ? fs.statSync(file).size : 0;
    const pos  = fs.existsSync(offset) ? parseInt(readFileWithRetry(offset, 'utf8').trim() || '0', 10) || 0 : 0;
    return Math.max(0, size - pos);
  } catch { return 0; }
}

// Snapshot of all buckets' pending bytes
function allBucketPending(frontierDir, parts){
  const list = [];
  for (let r = 0; r < parts; r++){
    list.push({ r, pending: bucketPendingBytes(frontierDir, r) });
  }
  return list;
}


// ----- Per-bucket owner lock: only one worker scans a bucket at a time -----
function acquireBucketOwner(frontierDir, r, ownerTag = '') {
  const assignDir = path.join(frontierDir, 'assign');
  fs.mkdirSync(assignDir, { recursive: true });
  const ownerPath = path.join(assignDir, `bucket.${r}.owner`);
  try {
    const fd = fs.openSync(ownerPath, 'wx'); // exclusive
    fs.writeFileSync(
      fd,
      JSON.stringify({ r, owner: ownerTag, pid: process.pid, at: new Date().toISOString() }) + '\n'
    );
    tEmit('bucketOwner', { bucket: r, owner: ownerTag, phase: 'acquired' });
    const release = () => {
      try { fs.closeSync(fd); fs.unlinkSync(ownerPath); } catch {}
      tEmit('bucketOwner', { bucket: r, owner: ownerTag, phase: 'released' });
    };
    return { release };
  } catch {
    return null; // someone else owns it right now
  }
}

function seedBuckets(frontierDir, urls, parts){
  if (!urls?.length) return;
  parts = Math.max(1, (parseInt(parts, 10) || 0));
  fs.mkdirSync(frontierDir, { recursive: true });
  // ensure files exist
  for (let r=0; r<parts; r++){
    const { file } = bucketFiles(frontierDir, r); 

    if (!fs.existsSync(file)) writeFileWithRetry(file, '', 'utf8');
  }
  appendToBuckets(frontierDir, urls, parts);
}

function appendToBuckets(frontierDir, urls, parts){
  if (!urls?.length) return;
  parts = Math.max(1, (parseInt(parts, 10) || 0));
  const perBucket = new Map();
  for (const raw of urls){
    if (!raw) continue;
    const r = hashUrl(raw) % parts;
    if (!perBucket.has(r)) perBucket.set(r, []);
    perBucket.get(r).push(raw);
  }
  for (const [r, list] of perBucket){
    const { file } = bucketFiles(frontierDir, r);
    try {
      const st = fs.existsSync(file) ? fs.statSync(file) : null;
      const max = parseInt(process.env.MC_BUCKET_MAX_BYTES || '134217728', 10); // 128MB
    if (st && st.size > max) {
        const rotated = file.replace(/\.ndjson$/, `.${Date.now()}.ndjson`);
        renameWithRetry(file, rotated);
        writeFileWithRetry(file, '', 'utf8');
      }
    } catch {}
    appendFileWithRetry(file, list.map(u => u + '\n').join(''));
    tEmit('bucketAppend', { bucket: r, count: list.length });
  }
}

/**
 * Claim next URL for bucket r by reading ONLY new bytes in bucket.r since last cursor.
 * Returns { url, complete(), release() } or null if no new work right now.
 */
function claimNextBucket(frontierDir, locksDir, r, parts, acceptFn){
  const { file, offset } = bucketFiles(frontierDir, r);
  fs.mkdirSync(path.dirname(file), { recursive: true });
  if (!fs.existsSync(file)) writeFileWithRetry(file, '', 'utf8');

  let pos = 0;
  try { pos = parseInt(readFileWithRetry(offset, 'utf8').trim() || '0', 10) || 0; } catch {}

  // Read raw and normalize (strip BOM, normalize CRLF)
  let buf = readFileWithRetry(file);
  // Defensive: BOM at the very beginning can poison first split
  if (buf.length >= 3 && buf[0] === 0xEF && buf[1] === 0xBB && buf[2] === 0xBF) {
    buf = Buffer.from(buf.slice(3));
  }
    // Defensive: if offset is beyond EOF (truncate/rotate), clamp it
    if (pos > buf.length) {
      try { writeFileWithRetry(offset, String(buf.length), 'utf8'); } catch {}
      pos = buf.length;
    }
  if (pos >= buf.length) {
    tEmit('bucketProgress', { bucket: r, cursor: pos, size: buf.length });
    return null; // nothing new
  }

  // Normalize CRLF → LF before slicing to lines
  const tail = buf.toString('utf8', pos).replace(/\r\n/g, '\n');
  const lines = tail.split('\n');
  let claimed = false;

  for (let i=0; i<lines.length; i++){
    const line = lines[i];
    const withNl = i < lines.length - 1 ? line + '\n' : line; // last item may not end with NL
    const inc = Buffer.byteLength(withNl, 'utf8');
    const url = (line || '').trim();
    advanced += inc;

    if (!url) continue;
    if (acceptFn && !acceptFn(url)) continue;

    const claim = tryClaim(locksDir, url);
    if (claim) {
      // advance cursor THROUGH the claimed line
      const newPos = pos + advanced;
      try { writeFileWithRetry(offset, String(newPos), 'utf8'); } catch {}
      tEmit('bucketProgress', { bucket: r, cursor: newPos, size: buf.length, claimed: true });
      claimed = true;
      const delayMs = parseInt(process.env.MC_POLITE_DELAY_MS || '0', 10) || 0;
      if (delayMs > 0) { try { sleepMs(delayMs); } catch {} }
      return { url, ...claim };
    }
  }

  // consumed all new lines, advance cursor to EOF
  try { writeFileWithRetry(offset, String(pos), 'utf8'); } catch {}
  tEmit('bucketProgress', { bucket: r, cursor: pos, size: buf.length, claimed: false });
  return null;
}

/**
 * Try current bucket first, then steal from other buckets that have the most pending bytes.
 * startR is the worker's "home" bucket index. Returns same shape as claimNextBucket().
 */
function claimNextAnyBucket(frontierDir, locksDir, startR, parts, acceptFn){
  // 1) Try home bucket
  const first = claimNextBucket(frontierDir, locksDir, startR, parts, acceptFn);
  if (first) return first;

  // 2) Build a sorted donor list by pending bytes (desc), excluding startR
  const pend = allBucketPending(frontierDir, parts)
    .filter(x => x.r !== startR && x.pending > 0)
    .sort((a,b) => b.pending - a.pending);

  for (const { r } of pend) {
    const got = claimNextBucket(frontierDir, locksDir, r, parts, acceptFn);
    if (got) {
      tEmit('bucketOwner', { bucket: r, owner: `steal->${startR}` });
      return got;
    }
  }
  return null;
}


module.exports = {
  // legacy
  seedFrontier,
  appendToFrontier,
  claimNext,
  hashUrl,

  // bucketed
  seedBuckets,
  appendToBuckets,
  claimNextBucket,
  claimNextAnyBucket,
  acquireBucketOwner,

  // stats
  bucketPendingBytes,
  allBucketPending,
};

