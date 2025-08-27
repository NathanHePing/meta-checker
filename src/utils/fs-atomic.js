// src/utils/fs-atomic.js
const fs = require('fs');
function writeFileAtomic(filePath, data) {
  const tmp = `${filePath}.${process.pid}.tmp`;
  fs.writeFileSync(tmp, data, 'utf8');
  fs.renameSync(tmp, filePath);
}
module.exports = { writeFileAtomic };
