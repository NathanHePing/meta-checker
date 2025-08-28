// electron/main.js
const { app, BrowserWindow, ipcMain, dialog, shell } = require('electron');
const path = require('path');

const argPortFlag = process.argv.find(a => a === '--telemetry-port');
const TELEMETRY_PORT = argPortFlag ? Number(process.argv[process.argv.indexOf(argPortFlag) + 1]) : Number(process.env.TELEMETRY_PORT || 0);
const START_URL = TELEMETRY_PORT ? `http://127.0.0.1:${TELEMETRY_PORT}/` : 'http://127.0.0.1:7077/';

let win;
async function createWindow() {
  win = new BrowserWindow({
    width: 1280,
    height: 900,
    backgroundColor: '#0b0f14',
    title: 'Meta Checker',
    webPreferences: {
      preload: path.join(__dirname, 'preload.js'),
      nodeIntegration: false,
      contextIsolation: true
    }
  });
  await win.loadURL(START_URL);
  win.on('closed', () => { win = null; });
}

app.on('ready', createWindow);
app.on('window-all-closed', () => { if (process.platform !== 'darwin') app.quit(); });
app.on('activate', () => { if (win === null) createWindow(); });

// IPC handlers
ipcMain.handle('select-folder', async () => {
  const res = await dialog.showOpenDialog({ properties: ['openDirectory', 'createDirectory'] });
  if (res.canceled || !res.filePaths || !res.filePaths[0]) return { canceled: true };
  return { canceled: false, path: res.filePaths[0] };
});

ipcMain.handle('open-folder', async (_evt, folderPath) => {
  if (!folderPath) return { ok: false, error: 'No path' };
  try {
    if (process.platform === 'win32') {
      // On Windows, open the folder
      await shell.openPath(folderPath);
    } else {
      await shell.openExternal('file://' + folderPath.replace(/ /g, '%20'));
    }
    return { ok: true };
  } catch (e) {
    return { ok: false, error: String(e) };
  }
});

ipcMain.handle('open-external', async (_evt, url) => {
  try { await shell.openExternal(url); return { ok: true }; }
  catch (e) { return { ok: false, error: String(e) }; }
});
