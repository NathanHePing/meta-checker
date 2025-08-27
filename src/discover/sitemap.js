// src/discover/sitemap.js
const https = require('https');
const http = require('http');

function fetchText(url, timeoutMs = 10000) {
  return new Promise((resolve, reject) => {
    const mod = url.startsWith('https') ? https : http;
    const u = new URL(url);
    const req = mod.request(
      {
        protocol: u.protocol,
        hostname: u.hostname,
        port: u.port || (u.protocol === 'https:' ? 443 : 80),
        path: u.pathname + (u.search || ''),
        method: 'GET',
        headers: {
          'user-agent': 'meta-check-bot/1.0 (+https://github.com/)'
        },
      },
      res => {
        // follow redirects
        if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
          const next = new URL(res.headers.location, url).toString();
          fetchText(next, timeoutMs).then(resolve, reject);
          return;
        }
        let data = '';
        res.setEncoding('utf8');
        res.on('data', chunk => (data += chunk));
        res.on('end', () => resolve(data));
      }
    );
    req.setTimeout(timeoutMs, () => {
      req.destroy(new Error('timeout'));
    });
    req.on('error', reject);
    req.end();
  });
}

function extractLocs(xml) {
  // Grab all <loc> elements, normalize whitespace
  const locs = Array.from(xml.matchAll(/<loc>([\s\S]*?)<\/loc>/gi)).map(m => m[1].trim());
  // Basic sanity
  return locs.filter(u => /^https?:\/\//i.test(u));
}

async function expandSitemap(url, depth = 0, seen = new Set()) {
  if (depth > 3) return []; // avoid deep recursion
  if (seen.has(url)) return [];
  seen.add(url);

  let xml = '';
  try {
    xml = await fetchText(url, 12000);
  } catch {
    return [];
  }
  if (!xml || xml.length < 10) return [];

  const locs = extractLocs(xml);
  if (!locs.length) return [];

  // If a <loc> ends with .xml or contains 'sitemap', recurse; else treat as page URL
  const pages = [];
  for (const loc of locs) {
    const lower = loc.toLowerCase();
    if (lower.endsWith('.xml') || lower.includes('sitemap')) {
      const more = await expandSitemap(loc, depth + 1, seen);
      if (more.length) pages.push(...more);
    } else {
      pages.push(loc);
    }
  }
  return [...new Set(pages)];
}

async function discoverBySitemap(base) {
  try {
    const origin = new URL(base).origin;
    const seeds = [
      origin + '/sitemap.xml',
      origin + '/sitemap_index.xml',
      origin + '/sitemap-index.xml',
      origin + '/sitemap-default.xml',
    ];

    const all = new Set();
    for (const s of seeds) {
      try {
        const pages = await expandSitemap(s, 0, new Set());
        for (const p of pages) all.add(p);
      } catch {
        // ignore this seed
      }
    }

    // Return only non-XML pages
    return [...all].filter(u => !u.toLowerCase().endsWith('.xml'));
  } catch {
    return [];
  }
}

module.exports = { discoverBySitemap };
