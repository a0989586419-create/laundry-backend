/**
 * LINE Rich Menu Setup Script for YPURE Cloud Butler
 *
 * Usage:
 *   node scripts/setup-rich-menu.js
 *   LINE_CHANNEL_ACCESS_TOKEN=xxx node scripts/setup-rich-menu.js
 *   node scripts/setup-rich-menu.js --token=xxx
 *
 * Creates a 2x3 Rich Menu, generates the image with node-canvas,
 * uploads it, and sets it as the default rich menu.
 */

require('dotenv').config();
const { createCanvas } = require('canvas');
const https = require('https');
const http = require('http');

// --- Resolve token ---
let TOKEN = process.env.LINE_CHANNEL_ACCESS_TOKEN || '';
const tokenArg = process.argv.find(a => a.startsWith('--token='));
if (tokenArg) TOKEN = tokenArg.split('=')[1];

if (!TOKEN) {
  console.error('ERROR: LINE_CHANNEL_ACCESS_TOKEN is required.');
  console.error('Set it as env var or pass --token=<value>');
  process.exit(1);
}

// --- Constants ---
const LIFF_BASE = 'https://liff.line.me/2009552592-xkDKSJ1Y';
const WIDTH = 2500;
const HEIGHT = 1686;
const COLS = 2;
const ROWS = 3;
const CELL_W = WIDTH / COLS;   // 1250
const CELL_H = HEIGHT / ROWS;  // 562

const BG_COLOR = '#3A3A8C';
const GRID_COLOR = '#2A2A6C';
const GOLD = '#E5B94C';
const WHITE = '#FFFFFF';

const panels = [
  { icon: '\uD83D\uDD0D', label: '查空機',   action: { type: 'uri', uri: `${LIFF_BASE}?tab=wash` } },
  { icon: '\uD83D\uDCB0', label: '線上儲值', action: { type: 'uri', uri: `${LIFF_BASE}?tab=wallet` } },
  { icon: '\uD83C\uDF81', label: '領優惠',   action: { type: 'uri', uri: `${LIFF_BASE}?tab=coupons` } },
  { icon: '\uD83D\uDCCB', label: '使用紀錄', action: { type: 'uri', uri: `${LIFF_BASE}?tab=history` } },
  { icon: '\uD83D\uDD27', label: '故障報修', action: { type: 'message', text: '故障報修' } },
  { icon: '\uD83D\uDCDE', label: '聯繫客服', action: { type: 'uri', uri: 'https://line.me/R/ti/p/@016kcwrh' } },
];

// --- Helper: HTTPS request returning a promise ---
function request(url, options, body) {
  return new Promise((resolve, reject) => {
    const lib = url.startsWith('https') ? https : http;
    const req = lib.request(url, options, (res) => {
      const chunks = [];
      res.on('data', c => chunks.push(c));
      res.on('end', () => {
        const raw = Buffer.concat(chunks);
        const contentType = res.headers['content-type'] || '';
        let data;
        if (contentType.includes('application/json')) {
          try { data = JSON.parse(raw.toString()); } catch { data = raw.toString(); }
        } else {
          data = raw.toString();
        }
        if (res.statusCode >= 200 && res.statusCode < 300) {
          resolve({ status: res.statusCode, data });
        } else {
          reject(new Error(`HTTP ${res.statusCode}: ${typeof data === 'object' ? JSON.stringify(data) : data}`));
        }
      });
    });
    req.on('error', reject);
    if (body) req.write(body);
    req.end();
  });
}

// --- Step 1: Create Rich Menu ---
async function createRichMenu() {
  console.log('[1/4] Creating rich menu...');

  const areas = [];
  for (let row = 0; row < ROWS; row++) {
    for (let col = 0; col < COLS; col++) {
      const idx = row * COLS + col;
      const panel = panels[idx];
      const bounds = {
        x: col * CELL_W,
        y: row * CELL_H,
        width: CELL_W,
        height: CELL_H,
      };
      let action;
      if (panel.action.type === 'uri') {
        action = { type: 'uri', label: panel.label, uri: panel.action.uri };
      } else {
        action = { type: 'message', label: panel.label, text: panel.action.text };
      }
      areas.push({ bounds, action });
    }
  }

  const richMenuBody = {
    size: { width: WIDTH, height: HEIGHT },
    selected: true,
    name: 'YPURE Rich Menu',
    chatBarText: '開啟選單',
    areas,
  };

  const bodyStr = JSON.stringify(richMenuBody);
  const url = 'https://api.line.me/v2/bot/richmenu';
  const res = await request(url, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${TOKEN}`,
      'Content-Length': Buffer.byteLength(bodyStr),
    },
  }, bodyStr);

  const richMenuId = res.data.richMenuId;
  console.log(`   Rich menu created: ${richMenuId}`);
  return richMenuId;
}

// --- Step 2: Generate image with node-canvas ---
function generateImage() {
  console.log('[2/4] Generating rich menu image (2500x1686)...');

  const canvas = createCanvas(WIDTH, HEIGHT);
  const ctx = canvas.getContext('2d');

  // Background
  ctx.fillStyle = BG_COLOR;
  ctx.fillRect(0, 0, WIDTH, HEIGHT);

  // Grid lines
  ctx.strokeStyle = GRID_COLOR;
  ctx.lineWidth = 4;
  // Vertical line (center)
  ctx.beginPath();
  ctx.moveTo(CELL_W, 0);
  ctx.lineTo(CELL_W, HEIGHT);
  ctx.stroke();
  // Horizontal lines
  for (let r = 1; r < ROWS; r++) {
    ctx.beginPath();
    ctx.moveTo(0, r * CELL_H);
    ctx.lineTo(WIDTH, r * CELL_H);
    ctx.stroke();
  }

  // Draw each panel
  for (let row = 0; row < ROWS; row++) {
    for (let col = 0; col < COLS; col++) {
      const idx = row * COLS + col;
      const panel = panels[idx];
      const cx = col * CELL_W + CELL_W / 2;
      const cy = row * CELL_H + CELL_H / 2;

      // Subtle rounded rectangle highlight
      const pad = 30;
      const rx = col * CELL_W + pad;
      const ry = row * CELL_H + pad;
      const rw = CELL_W - pad * 2;
      const rh = CELL_H - pad * 2;
      const radius = 24;
      ctx.fillStyle = 'rgba(255,255,255,0.05)';
      ctx.beginPath();
      ctx.moveTo(rx + radius, ry);
      ctx.lineTo(rx + rw - radius, ry);
      ctx.arcTo(rx + rw, ry, rx + rw, ry + radius, radius);
      ctx.lineTo(rx + rw, ry + rh - radius);
      ctx.arcTo(rx + rw, ry + rh, rx + rw - radius, ry + rh, radius);
      ctx.lineTo(rx + radius, ry + rh);
      ctx.arcTo(rx, ry + rh, rx, ry + rh - radius, radius);
      ctx.lineTo(rx, ry + radius);
      ctx.arcTo(rx, ry, rx + radius, ry, radius);
      ctx.closePath();
      ctx.fill();

      // Icon (emoji as text -- fallback to symbol characters)
      const iconMap = {
        '\uD83D\uDD0D': '\u2315',  // search
        '\uD83D\uDCB0': '$',       // money
        '\uD83C\uDF81': '\u2605',  // gift/star
        '\uD83D\uDCCB': '\u2630',  // clipboard/trigram
        '\uD83D\uDD27': '\u2699',  // wrench/gear
        '\uD83D\uDCDE': '\u260E',  // phone
      };
      const displayIcon = iconMap[panel.icon] || panel.icon;

      ctx.fillStyle = GOLD;
      ctx.font = 'bold 120px sans-serif';
      ctx.textAlign = 'center';
      ctx.textBaseline = 'middle';
      ctx.fillText(displayIcon, cx, cy - 60);

      // Label
      ctx.fillStyle = WHITE;
      ctx.font = 'bold 64px sans-serif';
      ctx.textAlign = 'center';
      ctx.textBaseline = 'middle';
      ctx.fillText(panel.label, cx, cy + 80);
    }
  }

  // Outer border
  ctx.strokeStyle = GOLD;
  ctx.lineWidth = 6;
  ctx.strokeRect(3, 3, WIDTH - 6, HEIGHT - 6);

  return canvas.toBuffer('image/png');
}

// --- Step 3: Upload image ---
async function uploadImage(richMenuId, imageBuffer) {
  console.log(`[3/4] Uploading image to rich menu ${richMenuId}...`);

  const url = `https://api-data.line.me/v2/bot/richmenu/${richMenuId}/content`;
  await request(url, {
    method: 'POST',
    headers: {
      'Content-Type': 'image/png',
      'Authorization': `Bearer ${TOKEN}`,
      'Content-Length': imageBuffer.length,
    },
  }, imageBuffer);

  console.log('   Image uploaded successfully.');
}

// --- Step 4: Set as default ---
async function setDefault(richMenuId) {
  console.log(`[4/4] Setting rich menu ${richMenuId} as default...`);

  const url = `https://api.line.me/v2/bot/user/all/richmenu/${richMenuId}`;
  await request(url, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${TOKEN}`,
      'Content-Length': 0,
    },
  }, '');

  console.log('   Default rich menu set successfully!');
}

// --- Main ---
async function main() {
  console.log('=== YPURE Rich Menu Setup ===\n');
  try {
    const richMenuId = await createRichMenu();
    const imageBuffer = generateImage();
    console.log(`   Image size: ${(imageBuffer.length / 1024).toFixed(1)} KB`);
    await uploadImage(richMenuId, imageBuffer);
    await setDefault(richMenuId);
    console.log('\nDone! Rich menu is now active.');
    console.log(`Rich Menu ID: ${richMenuId}`);
  } catch (err) {
    console.error('\nFailed:', err.message);
    process.exit(1);
  }
}

main();
