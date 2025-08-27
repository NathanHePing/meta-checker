function ts() {
  const d = new Date();
  return new Intl.DateTimeFormat('en-US', {
    hour12: false, hour: '2-digit', minute: '2-digit', second: '2-digit'
  }).format(d);
}
module.exports = {
  info: (msg, extra) => console.log(`[${ts()}] ℹ ${msg}`, extra || ''),
  warn: (msg, extra) => console.warn(`[${ts()}] ⚠ ${msg}`, extra || ''),
  error: (msg, extra) => console.error(`[${ts()}] ✖ ${msg}`, extra || '')
};
