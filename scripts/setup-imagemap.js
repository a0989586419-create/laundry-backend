#!/usr/bin/env node
/**
 * Imagemap Message Setup Script for LINE OA
 *
 * This script outputs the imagemap message JSON that can be used in broadcasts.
 * The image must be hosted at a public HTTPS URL.
 * LINE requires the image to be available at: baseUrl/{width}
 * (e.g., baseUrl/1040, baseUrl/700, baseUrl/460, baseUrl/300, baseUrl/240)
 *
 * Usage: node scripts/setup-imagemap.js
 */

const ASSETS_BASE = 'https://laundry-backend-production-efa4.up.railway.app/assets';
const LIFF_URL = 'https://liff.line.me/2009552592-xkDKSJ1Y';
const WEBSITE_URL = 'https://cloudmonster-website.vercel.app';
const LINE_OA_URL = 'https://line.me/R/ti/p/@016kcwrh';

// Main imagemap (1040x1040, 2 click areas: top=system overview, bottom-left=wash, bottom-right=profile)
const mainImagemap = {
  type: 'imagemap',
  baseUrl: `${ASSETS_BASE}/imagemap-main`,
  altText: '雲管家 — IoT 智慧洗衣解決方案',
  baseSize: { width: 1040, height: 1040 },
  actions: [
    { type: 'uri', linkUri: WEBSITE_URL, area: { x: 0, y: 0, width: 1040, height: 624 } },
    { type: 'uri', linkUri: `${LIFF_URL}?tab=wash`, area: { x: 0, y: 624, width: 520, height: 416 } },
    { type: 'uri', linkUri: `${LIFF_URL}?tab=profile`, area: { x: 520, y: 624, width: 520, height: 416 } }
  ]
};

// Grid imagemap (1040x1040, 4 areas 2x2)
const gridImagemap = {
  type: 'imagemap',
  baseUrl: `${ASSETS_BASE}/imagemap-grid`,
  altText: '雲管家服務 — 店內消費 | 線上儲值 | 洗衣門市 | 代洗烘折',
  baseSize: { width: 1040, height: 1040 },
  actions: [
    { type: 'uri', linkUri: `${LIFF_URL}?tab=wash`, area: { x: 0, y: 0, width: 520, height: 520 } },
    { type: 'uri', linkUri: `${LIFF_URL}?tab=profile`, area: { x: 520, y: 0, width: 520, height: 520 } },
    { type: 'uri', linkUri: `${LIFF_URL}?tab=wash`, area: { x: 0, y: 520, width: 520, height: 520 } },
    { type: 'uri', linkUri: LINE_OA_URL, area: { x: 520, y: 520, width: 520, height: 520 } }
  ]
};

console.log('=== Main Imagemap (Hero + 2 Bottom Actions) ===');
console.log(JSON.stringify(mainImagemap, null, 2));
console.log('\n=== Grid Imagemap (2x2 Grid Actions) ===');
console.log(JSON.stringify(gridImagemap, null, 2));

console.log('\n=== Usage Instructions ===');
console.log('1. Place your 1040x1040 PNG images in: public/assets/');
console.log('   - imagemap-main.png (for the main imagemap)');
console.log('   - imagemap-grid.png (for the grid imagemap)');
console.log('2. Deploy to Railway so images are served at the ASSETS_BASE URL');
console.log('3. Use these JSON objects in the broadcast API body as messages');
console.log('\nExample broadcast curl:');
console.log(`curl -X POST https://laundry-backend-production-efa4.up.railway.app/api/admin/broadcast \\`);
console.log(`  -H "Content-Type: application/json" \\`);
console.log(`  -d '{"adminUserId":"YOUR_ADMIN_ID","messages":[${JSON.stringify(mainImagemap)}]}'`);
