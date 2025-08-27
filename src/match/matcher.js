const { normalizeText } = require('./normalize');

function tokens(s){
  return new Set(normalizeText(s).split(/[^a-z0-9]+/).filter(Boolean));
}
function jaccard(a, b){
  const A = tokens(a), B = tokens(b);
  let inter=0; for(const t of A) if(B.has(t)) inter++;
  const uni = A.size + B.size - inter;
  return uni ? inter/uni : 0;
}

/**
 * @param {{titleN:string, url:string}[]} pages
 * @param {string} expectedTitleN
 * @param {{prefixWords:number, fuzzyThreshold:number}} opts
 * @returns {{type:'none'|'exact'|'prefix'|'fuzzy', items:{url:string,titleN:string}[], score?:number}}
 */
function findByPrefixThenSimilarity(pages, expectedTitleN, opts){
  const { prefixWords=4, fuzzyThreshold=0.6 } = opts || {};
  // exact
  let hits = pages.filter(p => p.titleN === expectedTitleN);
  if (hits.length === 1) return { type:'exact', items:hits };
  if (hits.length > 1)   return { type:'exact', items:hits }; // ambiguous

  // prefix by N words
  const prefix = expectedTitleN.split(' ').slice(0, prefixWords).join(' ');
  hits = pages.filter(p => p.titleN.startsWith(prefix));
  if (hits.length === 1) return { type:'prefix', items:hits };
  if (hits.length > 1)   return { type:'prefix', items:hits }; // ambiguous

  // fuzzy by jaccard
  const scored = pages.map(p => ({ p, s: jaccard(p.titleN, expectedTitleN) }))
                      .filter(x => x.s >= fuzzyThreshold)
                      .sort((a,b) => b.s - a.s);
  if (scored.length === 0) return { type:'none', items:[] };
  const top = scored.filter(x => x.s === scored[0].s).map(x => ({ url:x.p.url, titleN:x.p.titleN }));
  return { type:'fuzzy', items: top, score: scored[0].s };
}

module.exports = { findByPrefixThenSimilarity };
