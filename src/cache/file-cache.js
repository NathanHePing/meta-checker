const fs = require('fs');
const { writeFileAtomic } = require('../utils/fs-atomic');

function loadCache(path){
  try{
    const raw = fs.readFileSync(path, 'utf8');
    return JSON.parse(raw);
  }catch{ return {}; }
}
function saveCache(path, obj){
  writeFileAtomic(path, JSON.stringify(obj, null, 2));
}
module.exports = { loadCache, saveCache };
