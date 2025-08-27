function normalizeText(s){
  return (s || '')
    .toLowerCase()
    .replace(/[\u2019\u2018’']/g, "'")  // curly/smart quotes → '
    .replace(/\s+/g, ' ')
    .trim();
}
function normalizeUrl(u, opts = {}){
  const keepPageParam = !!opts.keepPageParam;
  const url = new URL(u);
  url.hash = '';
  const drop = new Set(['utm_source','utm_medium','utm_campaign','utm_term','utm_content','gclid','fbclid']);
  for (const k of [...url.searchParams.keys()]) if (drop.has(k.toLowerCase())) url.searchParams.delete(k);
  if (!keepPageParam && url.searchParams.has('page')) url.searchParams.delete('page');
  url.host = url.host.toLowerCase();
  if ([...url.searchParams.keys()].length === 0) url.search = '';
  if (url.pathname.length > 1 && url.pathname.endsWith('/')) url.pathname = url.pathname.slice(0,-1);
  return url.toString();
}
module.exports = { normalizeText, normalizeUrl };
