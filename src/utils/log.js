// src/utils/log.js
function makeLogger({ shardIndex = 1, shards = 1, pid = process.pid, label = '' } = {}) {
  const tag = `[S${shardIndex}/${shards} pid=${pid}${label ? ' ' + label : ''}]`;
  const ts  = () => new Date().toISOString().slice(11, 19); // HH:MM:SS
  const fmt = (level, args) => [`[${ts()}] ${tag} ${level}`, ...args];
  return {
    info:  (...args) => console.log (...fmt('ℹ', args)),
    warn:  (...args) => console.warn(...fmt('⚠', args)),
    error: (...args) => console.error(...fmt('✖', args)),
    debug: (...args) => console.log (...fmt('·', args)),
    child(sublabel) { return makeLogger({ shardIndex, shards, pid, label: sublabel }); }
  };
}

module.exports = { makeLogger };
