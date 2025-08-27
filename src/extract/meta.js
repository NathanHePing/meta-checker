async function getMetaFromPage(page, url) {
  let lastErr;
  for (let attempt = 1; attempt <= 2; attempt++) {
    try {
      const res = await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 15000 });
      if (!res || !res.ok()) throw new Error(`status ${res ? res.status() : 'no response'}`);
      try { await page.waitForLoadState('networkidle', { timeout: 2000 }); } catch {}
      // Robust meta extraction matching run.js behavior
      const meta = await page.evaluate(() => {
        const by = (sel) => document.querySelector(sel)?.getAttribute('content') || '';
        const title =
          by('meta[name="title"]') ||
          by('meta[property="og:title"]') ||
          document.title ||
          '';
       const description =
          by('meta[name="description"]') ||
          by('meta[property="og:description"]') ||
          '';
        return { title, description };
      });
      return { title: meta.title || '', description: meta.description || '' };
   } catch (e) {
      lastErr = e;
      await page.waitForTimeout(400 * attempt);
    }
  }
  throw lastErr;
}
module.exports = { getMetaFromPage };
