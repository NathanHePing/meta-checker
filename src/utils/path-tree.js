'use strict';

// tiny URL normalizer that matches your keepPageParam/sameOrigin/pathPrefix rules
function normalizeUrl(u) {
  try {
    const url = new URL(u);
    url.hash = '';
    // NOTE: keepPageParam=true: we keep ?page and drop others; tweak if needed
    const params = new URLSearchParams(url.search);
    const q = new URLSearchParams();
    if (params.has('page')) q.set('page', params.get('page'));
    url.search = q.toString() ? `?${q}` : '';
    return url.toString().replace(/\/+$/, ''); // strip trailing slash
  } catch { return null; }
}

function segmentize(u, pathPrefix = '/en-us') {
  const n = normalizeUrl(u);
  if (!n) return null;
  const { pathname } = new URL(n);
  if (!pathname.startsWith(pathPrefix)) return null;
  const rest = pathname.slice(pathPrefix.length).replace(/^\/+/, '');
  const segs = rest ? rest.split('/').filter(Boolean) : [];
  return segs;
}

function buildTree(urls, pathPrefix = '/en-us') {
  const tree = { name: pathPrefix, children: new Map(), urls: [] };
  for (const raw of urls) {
    const n = normalizeUrl(raw);
    if (!n) continue;
    const segs = segmentize(n, pathPrefix);
    if (segs === null) continue;
    let node = tree;
    node.urls.push(n);
    for (const s of segs) {
      if (!node.children.has(s)) node.children.set(s, { name: s, children: new Map(), urls: [] });
      node = node.children.get(s);
      node.urls.push(n);
    }
  }
  return tree;
}

function renderTree(node, prefix = '', isLast = true) {
  const line = prefix + (prefix ? (isLast ? '└─ ' : '├─ ') : '') + node.name;
  const childArr = [...node.children.keys()].sort().map(k => node.children.get(k));
  const lines = [line];
  const nextPrefix = prefix + (prefix ? (isLast ? '   ' : '│  ') : '');
  childArr.forEach((child, idx) => {
    const last = idx === childArr.length - 1;
    lines.push(...renderTree(child, nextPrefix, last));
  });
  return lines;
}

function flattenTreeToReconstructableExamples(node, pathPrefix = '/en-us', maxPerLevel = 3) {
  // returns a small sample showing that each branch forms valid URLs
  const out = [];
  function walk(n, segs) {
    // show up to N example URLs from this node to prove reconstructability
    const sample = n.urls.slice(0, maxPerLevel);
    if (sample.length) {
      out.push([`${pathPrefix}${segs.length ? '/' + segs.join('/') : ''}`, sample]);
    }
    for (const k of [...n.children.keys()].sort()) {
      walk(n.children.get(k), [...segs, k]);
    }
  }
  walk(node, []);
  return out;
}

module.exports = { buildTree, renderTree, flattenTreeToReconstructableExamples, normalizeUrl };
