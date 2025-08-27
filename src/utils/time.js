function nowIso(){ return new Date().toISOString(); }
function daysSince(iso){
  if(!iso) return Infinity;
  const d = (Date.now() - new Date(iso).getTime()) / (1000*60*60*24);
  return d;
}
module.exports = { nowIso, daysSince };
