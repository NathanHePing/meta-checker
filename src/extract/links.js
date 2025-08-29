// src/extract/links.js
async function collectLinksOnce(page) {
  return await page.evaluate(() => {
    const bad = /^(javascript:|mailto:|tel:|data:)/i;

    const bestLabel = (el) => {
      const txt = (el.innerText || '').replace(/\s+/g, ' ').trim();
      if (txt) return txt;
      const aria = el.getAttribute && (el.getAttribute('aria-label') || el.getAttribute('title') || '');
      if (aria) return aria.trim();
      const img = el.querySelector && el.querySelector('img[alt]');
      if (img) return (img.getAttribute('alt') || '').trim();
      return '';
    };
    const toAbs = (href, baseHref) => { try { return new URL(href, baseHref || location.href).href; } catch { return ''; } };

    const collectFromRoot = (root, baseHref) => {
      const out = [];
      const stack = [root];
      while (stack.length) {
        const node = stack.pop();
        const list = node.querySelectorAll
          ? node.querySelectorAll('a[href], [role="link"], button, [role="button"], [data-href], [data-url], [onclick], [role="menuitem"], [tabindex]')
          : [];
        for (const el of list) {
          let href = '';
          let kind = el.tagName ? el.tagName.toLowerCase() : 'node';

          if (el.hasAttribute && el.hasAttribute('href')) {
            href = el.getAttribute('href') || '';
            kind = 'a';
          } else if (el.getAttribute) {
            href = el.getAttribute('data-href') || el.getAttribute('data-url') || '';
            if (!href && el.hasAttribute && el.hasAttribute('onclick')) {
              const code = el.getAttribute('onclick') || '';
              const m =
                code.match(/(?:window\.open|location\.(?:assign|replace))\(\s*['"]([^'"]+)['"]\s*\)/i) ||
                code.match(/(?:window\.location|location)\s*=\s*['"]([^'"]+)['"]/i) ||
                code.match(/location\.href\s*=\s*['"]([^'"]+)['"]/i);
              if (m) href = m[1];
            }
            if (href) kind = (kind === 'node' ? (el.getAttribute('role') || 'button') : kind);
          }

          if (href && !bad.test(href)) {
            const abs = toAbs(href, baseHref);
            if (abs) out.push({ url: abs.split('#')[0], text: bestLabel(el), kind });
          }
        }
        if (node.shadowRoot) stack.push(node.shadowRoot);
        if (node.children) for (const c of node.children) if (c.shadowRoot) stack.push(c.shadowRoot);
      }
      return out;
    };

    const items = collectFromRoot(document, location.href);
    const seen = new Set(), out = [];
    for (const it of items) {
      const k = `${it.url}|${(it.text||'').toLowerCase()}|${it.kind||''}`;
      if (!seen.has(k)) { seen.add(k); out.push(it); }
    }
    return out;
  });
}

async function revealLikelyLinks(page, { hoverMs = 150, clickMs = 150, afterScrollMs = 150 } = {}) {
  const clickSafe = async (h) => { try { await h.click({ timeout: 500 }); await page.waitForTimeout(clickMs); } catch {} };
  const hoverSafe = async (h) => { try { await h.hover({ timeout: 500 }); await page.waitForTimeout(hoverMs); } catch {} };

  const menuButtons = await page.$$(
    'button[aria-expanded="false"],button[aria-controls],' +
    '[role="button"][aria-expanded="false"],[data-toggle],[data-action*="menu"],[data-action*="expand"]'
  );
  for (const btn of menuButtons.slice(0, 10)) await clickSafe(btn);

  const hoverables = await page.$$(
    'nav [aria-haspopup="true"], nav .dropdown, nav .menu, .nav [aria-expanded]'
  );
  for (const el of hoverables.slice(0, 20)) await hoverSafe(el);

  const moreButtons = await page.$$('button, [role="button"]');
  for (const el of moreButtons.slice(0, 30)) {
    try {
      const text = ((await el.textContent()) || '').trim().toLowerCase();
      if (/^(see|show|view)\s+more$/.test(text) || /expand/.test(text)) await clickSafe(el);
    } catch {}
  }

  const details = await page.$$('details:not([open])');
  for (const d of details.slice(0, 20)) { try { await d.evaluate(el => el.open = true); } catch {} }

  for (let i = 0; i < 30; i++) { try { await page.keyboard.press('Tab'); } catch {} }
  await page.waitForTimeout(hoverMs);

  try { await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight)); } catch {}
  await page.waitForTimeout(afterScrollMs);
}

async function installNavRecorder(page) {
  // install into the current document (we don’t navigate away)
  await page.evaluate(() => {
    if (window.__navRecorderInstalled) return;
    window.__navRecorderInstalled = true;
    window.__navs = [];
    const record = (u) => { try { window.__navs.push(new URL(u, location.href).href.split('#')[0]); } catch {} };

    const hp = history.pushState.bind(history);
    const hr = history.replaceState.bind(history);
    history.pushState = function (s,t,u){ if (u) record(u); return hp(s,t,u); };
    history.replaceState = function (s,t,u){ if (u) record(u); return hr(s,t,u); };

    const la = location.assign.bind(location);
    const lr = location.replace.bind(location);
    location.assign = function (u){ record(u); /* swallow */ };
    location.replace = function (u){ record(u); /* swallow */ };

    const wo = window.open.bind(window);
    window.open = function (u){ if (u) record(u); return null; };
  });
}

function filterAndNormalize(items, { origin, pathPrefix, keepPageParam }) {
  const seen = new Set(), out = [];
  const assetRe = /\.(?:png|jpe?g|gif|webp|svg|ico|pdf|zip|mp4|webm|css|js|woff2?|ttf|otf)(?:\?|$)/i;
  for (const it of items) {
    try {
      const u = new URL(it.url, origin);
      if (u.origin !== origin) continue;
      if (pathPrefix && !u.pathname.startsWith(pathPrefix)) continue;
      if (assetRe.test(u.pathname)) continue;
      if (!keepPageParam) {
        const q = new URLSearchParams(u.search);
        for (const k of Array.from(q.keys())) if (!/^page$/i.test(k)) q.delete(k);
        u.search = q.toString() ? '?' + q.toString() : '';
      }
      if (u.pathname.length > 1) u.pathname = u.pathname.replace(/\/+$/, '');
      const url = u.toString();
      const text = (it.text || '').trim();
      const kind = it.kind || '';
      const key = `${url}|${text.toLowerCase()}|${kind}`;
      if (!seen.has(key)) { seen.add(key); out.push({ url, text, kind }); }
    } catch {}
  }
  return out;
}

async function extractInternalLinks(page, opts) {
  const {
    origin,
    pathPrefix = '',
   keepPageParam = false,
    max = 50000,
    interactive = true,
    waitAfterLoadMs = 300,
    allowSubdomains = false
  } = opts || {};
  try { await page.setViewportSize({ width: 1920, height: 1600 }); } catch {}
  try { await page.waitForLoadState('networkidle', { timeout: 3000 }); } catch {}
  if (waitAfterLoadMs) await page.waitForTimeout(waitAfterLoadMs);

  await installNavRecorder(page);

  let items = await collectLinksOnce(page);
  if (interactive && items.length < max) {
    try { await revealLikelyLinks(page); } catch {}
    const more = await collectLinksOnce(page);
    items = items.concat(more);
  }
  const navs = await page.evaluate(() => Array.from(new Set(window.__navs || [])));
  if (navs.length) items.push(...navs.map(u => ({ url: u, text: '', kind: 'spa' })));

  if (!allowSubdomains) {
    return filterAndNormalize(items, { origin, pathPrefix, keepPageParam }).slice(0, max);
  } else {
    const baseHost = new URL(origin).hostname;
   const etld1 = (h) => h.split('.').slice(-2).join('.');
    const registrable = etld1(baseHost);
    const sameSiteItems = items.filter(it => {
      try { return etld1(new URL(it.url, origin).hostname) === registrable; } catch { return false; }
    });
    return filterAndNormalize(sameSiteItems, { origin, pathPrefix, keepPageParam }).slice(0, max);
  }
}

module.exports = { extractInternalLinks };
