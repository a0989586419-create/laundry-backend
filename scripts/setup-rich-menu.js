/**
 * LINE Rich Menu Setup Script — Per-Audience (3 menus)
 *
 * Usage:
 *   node scripts/setup-rich-menu.js --type=all
 *   node scripts/setup-rich-menu.js --type=default
 *   node scripts/setup-rich-menu.js --type=b2c
 *   node scripts/setup-rich-menu.js --type=b2b
 *
 * Options:
 *   --token=xxx     LINE Channel Access Token (or use env LINE_CHANNEL_ACCESS_TOKEN)
 *   --type=all      Create all 3 menus (default)
 *   --set-default   Set the 'default' menu as the default for all users
 */

require('dotenv').config();
const fs = require('fs');
const path = require('path');

// --- Resolve token ---
let TOKEN = process.env.LINE_CHANNEL_ACCESS_TOKEN || '';
const tokenArg = process.argv.find(a => a.startsWith('--token='));
if (tokenArg) TOKEN = tokenArg.split('=')[1];
if (!TOKEN) { console.error('ERROR: LINE_CHANNEL_ACCESS_TOKEN required.'); process.exit(1); }

const typeArg = process.argv.find(a => a.startsWith('--type='));
const menuType = typeArg ? typeArg.split('=')[1] : 'all';

// --- Constants ---
const LIFF_BASE = 'https://liff.line.me/2009552592-xkDKSJ1Y';
const OFFICIAL_WEBSITE = 'https://cloudmonster-website.vercel.app';
const LINE_OA_CHAT_URL = 'https://line.me/R/ti/p/@016kcwrh';

// --- Menu Definitions ---
const MENUS = {
  default: {
    name: 'YPURE Default Menu (Identity Selection)',
    chatBarText: '選擇身份',
    size: { width: 2500, height: 843 },
    image: path.join(__dirname, '..', '..', 'line-oa-assets', 'richmenu-default.png'),
    areas: [
      { bounds: { x: 0, y: 0, width: 833, height: 843 }, action: { type: 'postback', label: '我是店主', data: 'action=welcome_business', displayText: '我是店主' } },
      { bounds: { x: 833, y: 0, width: 834, height: 843 }, action: { type: 'postback', label: '我是顧客', data: 'action=welcome_customer', displayText: '我是顧客' } },
      { bounds: { x: 1667, y: 0, width: 833, height: 843 }, action: { type: 'uri', label: '了解更多', uri: OFFICIAL_WEBSITE } },
    ],
  },
  b2c: {
    name: 'YPURE B2C Consumer Menu',
    chatBarText: '開啟選單',
    size: { width: 2500, height: 1686 },
    image: path.join(__dirname, '..', '..', 'line-oa-assets', 'richmenu-b2c.png'),
    areas: [
      { bounds: { x: 0, y: 0, width: 833, height: 843 }, action: { type: 'uri', label: '開始洗衣', uri: `${LIFF_BASE}?tab=wash` } },
      { bounds: { x: 833, y: 0, width: 834, height: 843 }, action: { type: 'uri', label: '我的帳戶', uri: `${LIFF_BASE}?tab=profile` } },
      { bounds: { x: 1667, y: 0, width: 833, height: 843 }, action: { type: 'message', label: '門市資訊', text: '門市' } },
      { bounds: { x: 0, y: 843, width: 833, height: 843 }, action: { type: 'message', label: '洗衣教學', text: '教學' } },
      { bounds: { x: 833, y: 843, width: 834, height: 843 }, action: { type: 'message', label: '聯繫客服', text: '客服' } },
      { bounds: { x: 1667, y: 843, width: 833, height: 843 }, action: { type: 'message', label: '故障報修', text: '故障報修' } },
    ],
  },
  b2b: {
    name: 'YPURE B2B Business Menu',
    chatBarText: '開啟選單',
    size: { width: 2500, height: 1686 },
    image: path.join(__dirname, '..', '..', 'line-oa-assets', 'richmenu-b2b.png'),
    areas: [
      { bounds: { x: 0, y: 0, width: 833, height: 843 }, action: { type: 'message', label: '方案報價', text: '方案' } },
      { bounds: { x: 833, y: 0, width: 834, height: 843 }, action: { type: 'uri', label: '免費體驗', uri: `${LIFF_BASE}` } },
      { bounds: { x: 1667, y: 0, width: 833, height: 843 }, action: { type: 'postback', label: '填寫問卷', data: 'action=start_survey', displayText: '填寫諮詢表' } },
      { bounds: { x: 0, y: 843, width: 833, height: 843 }, action: { type: 'message', label: '成功案例', text: '成功案例' } },
      { bounds: { x: 833, y: 843, width: 834, height: 843 }, action: { type: 'uri', label: '瀏覽官網', uri: OFFICIAL_WEBSITE } },
      { bounds: { x: 1667, y: 843, width: 833, height: 843 }, action: { type: 'uri', label: '聯繫顧問', uri: LINE_OA_CHAT_URL } },
    ],
  },
};

// --- API Helper ---
async function lineApi(method, urlPath, body, contentType = 'application/json') {
  const baseUrl = urlPath.includes('api-data') ? '' : 'https://api.line.me';
  const url = urlPath.startsWith('http') ? urlPath : `${baseUrl}${urlPath}`;

  const headers = { 'Authorization': `Bearer ${TOKEN}` };
  const options = { method, headers };

  if (body) {
    if (contentType === 'application/json') {
      headers['Content-Type'] = 'application/json';
      options.body = JSON.stringify(body);
    } else {
      headers['Content-Type'] = contentType;
      options.body = body;
    }
  }

  const res = await fetch(url, options);
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(`HTTP ${res.status}: ${JSON.stringify(data)}`);
  return data;
}

// --- Create, Upload, Return ID ---
async function setupMenu(type) {
  const menu = MENUS[type];
  console.log(`\n--- Setting up ${type} menu: ${menu.name} ---`);

  // 1. Create rich menu
  console.log('[1/3] Creating rich menu...');
  const richMenuBody = {
    size: menu.size,
    selected: type !== 'default', // default menu starts collapsed
    name: menu.name,
    chatBarText: menu.chatBarText,
    areas: menu.areas,
  };
  const result = await lineApi('POST', '/v2/bot/richmenu', richMenuBody);
  const richMenuId = result.richMenuId;
  console.log(`   Created: ${richMenuId}`);

  // 2. Upload image
  console.log('[2/3] Uploading image...');
  if (!fs.existsSync(menu.image)) {
    throw new Error(`Image not found: ${menu.image}`);
  }
  const imageBuffer = fs.readFileSync(menu.image);
  console.log(`   Image size: ${(imageBuffer.length / 1024).toFixed(1)} KB`);

  const uploadUrl = `https://api-data.line.me/v2/bot/richmenu/${richMenuId}/content`;
  await lineApi('POST', uploadUrl, imageBuffer, 'image/png');
  console.log('   Image uploaded.');

  // 3. Set as default if it's the 'default' type
  if (type === 'default') {
    console.log('[3/3] Setting as default for all users...');
    await lineApi('POST', `/v2/bot/user/all/richmenu/${richMenuId}`);
    console.log('   Default menu set.');
  } else {
    console.log('[3/3] Skipped (per-audience menu, not default).');
  }

  return richMenuId;
}

// --- Delete all existing rich menus ---
async function cleanupExisting() {
  console.log('Cleaning up existing rich menus...');
  try {
    const list = await lineApi('GET', '/v2/bot/richmenu/list');
    if (list.richmenus && list.richmenus.length > 0) {
      for (const rm of list.richmenus) {
        await lineApi('DELETE', `/v2/bot/richmenu/${rm.richMenuId}`);
        console.log(`   Deleted: ${rm.richMenuId} (${rm.name})`);
      }
    } else {
      console.log('   No existing menus found.');
    }
  } catch (e) {
    console.log(`   Cleanup skipped: ${e.message}`);
  }
}

// --- Main ---
async function main() {
  console.log('=== YPURE Rich Menu Setup (Per-Audience) ===');

  // Clean up existing menus first
  await cleanupExisting();

  const results = {};

  if (menuType === 'all') {
    for (const type of ['default', 'b2c', 'b2b']) {
      results[type] = await setupMenu(type);
    }
  } else if (MENUS[menuType]) {
    results[menuType] = await setupMenu(menuType);
  } else {
    console.error(`Unknown type: ${menuType}. Use: default, b2c, b2b, or all`);
    process.exit(1);
  }

  console.log('\n=== Results ===');
  for (const [type, id] of Object.entries(results)) {
    console.log(`${type}: ${id}`);
  }

  console.log('\n Set these in Railway environment variables:');
  if (results.default) console.log(`RICHMENU_DEFAULT_ID=${results.default}`);
  if (results.b2c) console.log(`RICHMENU_B2C_ID=${results.b2c}`);
  if (results.b2b) console.log(`RICHMENU_B2B_ID=${results.b2b}`);

  console.log('\nDone!');
}

main().catch(err => { console.error('Failed:', err.message); process.exit(1); });
