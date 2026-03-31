require('dotenv').config();
const express = require('express');
const mqtt = require('mqtt');
const { Pool } = require('pg');
const cors = require('cors');
const { v4: uuid } = require('uuid');
const crypto = require('crypto');
const https = require('https');

const path = require('path');
const fs = require('fs');
const helmet = require('helmet');
const rateLimit = require('express-rate-limit');
const axios = require('axios');

const app = express();
app.set('trust proxy', 1);
app.use(helmet({ contentSecurityPolicy: false }));
// Save raw body for LINE webhook signature verification
app.use(express.json({
  verify: (req, res, buf) => { req.rawBody = buf; }
}));
app.use(cors());

// Rate limiting
app.use('/api/', rateLimit({ windowMs: 60000, max: 200, message: { error: 'Too many requests' } }));
app.use('/api/payment/', rateLimit({ windowMs: 60000, max: 10, message: { error: 'Too many payment requests' } }));
app.use('/api/wallet/', rateLimit({ windowMs: 60000, max: 20, message: { error: 'Too many wallet requests' } }));
app.use('/api/notifications/', rateLimit({ windowMs: 60000, max: 5, message: { error: 'Too many notification requests' } }));

// 資料庫
const db = new Pool({
  connectionString: process.env.DB_URL,
  max: 20,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 5000,
});
db.on('error', (err) => console.error('DB pool error:', err));

// ===== MQTT =====
const mqttClient = process.env.MQTT_URL ? mqtt.connect(process.env.MQTT_URL, {
  username: process.env.MQTT_USER,
  password: process.env.MQTT_PASS,
  clientId: 'server-' + Date.now(),
  reconnectPeriod: 5000,
}) : null;
if (mqttClient) {
  mqttClient.on('error', (err) => console.error('MQTT error:', err.message));
  mqttClient.on('reconnect', () => console.log('MQTT reconnecting...'));
  mqttClient.on('offline', () => console.warn('MQTT offline'));
}

const machineCache = {};

if (mqttClient) {
mqttClient.on('connect', () => {
  console.log('MQTT connected');
  mqttClient.subscribe('laundry/+/+/status', { qos: 1 });
});

mqttClient.on('message', async (topic, payload) => {
  try {
    const parts = topic.split('/');
    const machineId = parts[2];
    const type = parts[3];
    const data = JSON.parse(payload.toString());
    if (type === 'status') {
      machineCache[machineId] = { ...data, updatedAt: new Date().toISOString() };
      if (data.state === 'done') {
        // Find running orders for this machine to notify users
        const runningOrders = await db.query(
          `SELECT o.id, o.store_id, mb.line_user_id, s.name as store_name, m.name as machine_name, s.group_id
           FROM orders o
           JOIN members mb ON o.member_id = mb.id
           JOIN stores s ON o.store_id = s.id
           JOIN machines m ON o.machine_id = m.id
           WHERE o.machine_id = $1 AND o.status = 'running'`, [machineId]
        );
        await db.query(`UPDATE orders SET status='done', completed_at=NOW() WHERE machine_id=$1 AND status='running'`, [machineId]);
        // Lookup support URL for the store group
        let mqttSupportUrl = '';
        let mqttSupportPhone = '';
        if (runningOrders.rows.length > 0 && runningOrders.rows[0].group_id) {
          const sgRes = await db.query('SELECT support_url, support_phone FROM store_groups WHERE id=$1', [runningOrders.rows[0].group_id]);
          mqttSupportUrl = sgRes.rows[0]?.support_url || '';
          mqttSupportPhone = sgRes.rows[0]?.support_phone || '';
        }
        // Send LINE push to each user with a running order on this machine
        for (const ord of runningOrders.rows) {
          sendLineFlexMessage(ord.line_user_id, '洗衣完成通知', buildCompleteFlexMessage(ord.store_name, ord.machine_name, mqttSupportUrl, mqttSupportPhone, ord.group_id, ord.store_id), { pushType: 'auto_complete', storeId: parts[1], description: `MQTT洗衣完成 ${ord.store_name} ${ord.machine_name}` }).catch(e => console.error('[LINE Push] MQTT done notify error:', e.message));
        }
      }
    }
  } catch (e) { console.error(e.message); }
});
} // end if (mqttClient)

// ===== IoT API Key middleware for device endpoints =====
const IOT_API_KEY = process.env.IOT_API_KEY || (() => { console.warn('[SECURITY] IOT_API_KEY using fallback - set env var!'); return 'ypure-iot-2026-default-key'; })();
function requireIotApiKey(req, res, next) {
  const key = req.headers['x-api-key'] || req.query.apiKey;
  if (key !== IOT_API_KEY) return res.status(401).json({ error: 'Invalid or missing API key' });
  next();
}

// ===== ThingsBoard RPC Helper =====
let tbToken = null;
let tbTokenExpiry = 0;

const TB_BASE = process.env.TB_BASE_URL || 'http://vps3.monsterstore.tw:8090';

async function getTBToken() {
  if (tbToken && Date.now() < tbTokenExpiry) return tbToken;
  try {
    const { data } = await axios.post(`${TB_BASE}/api/auth/login`, {
      username: process.env.TB_USERNAME || (() => { console.warn('[SECURITY] TB_USERNAME using fallback - set env var!'); return 'sl6963693171@gmail.com'; })(),
      password: process.env.TB_PASSWORD || (() => { console.warn('[SECURITY] TB_PASSWORD using fallback - set env var!'); return 's85010805'; })(),
    }, { timeout: 10000 });
    tbToken = data.token;
    tbTokenExpiry = Date.now() + 3600000;
    console.log('[TB] Login OK, token acquired');
    return tbToken;
  } catch (e) {
    console.error('[TB Login FAIL]', e.response?.status, e.response?.data || e.message);
    throw e;
  }
}

function tbHeaders(token) {
  return { 'X-Authorization': `Bearer ${token}`, 'Authorization': `Bearer ${token}` };
}

// Cache device IDs to avoid repeated lookups
const tbDeviceIdCache = {};

async function getTBDeviceId(deviceName) {
  if (tbDeviceIdCache[deviceName]) return tbDeviceIdCache[deviceName];
  const token = await getTBToken();
  try {
    const { data } = await axios.get(
      `${TB_BASE}/api/tenant/devices?pageSize=1&page=0&textSearch=${encodeURIComponent(deviceName)}&sortProperty=name&sortOrder=ASC`,
      { headers: tbHeaders(token), timeout: 10000 }
    );
    const id = data.data?.[0]?.id?.id || null;
    if (id) {
      tbDeviceIdCache[deviceName] = id;
      console.log(`[TB] Device ${deviceName} → ${id}`);
    } else {
      console.warn(`[TB] Device ${deviceName} NOT FOUND`);
    }
    return id;
  } catch (e) {
    console.error(`[TB DeviceId FAIL] ${deviceName}:`, e.response?.status, e.response?.data || e.message);
    throw e;
  }
}

async function sendThingsBoardRPC(deviceName, method, params = {}) {
  const token = await getTBToken();
  const deviceId = await getTBDeviceId(deviceName);
  if (!deviceId) throw new Error(`Device ${deviceName} not found in ThingsBoard`);
  await axios.post(`${TB_BASE}/api/rpc/oneway/${deviceId}`, { method, params }, {
    headers: { ...tbHeaders(token), 'Content-Type': 'application/json' }, timeout: 10000,
  });
  return true;
}

// Generic TB GET helper (used by telemetry/attribute endpoints)
async function tbGet(path) {
  const token = await getTBToken();
  try {
    const { data } = await axios.get(`${TB_BASE}${path}`, {
      headers: tbHeaders(token), timeout: 15000,
    });
    return data;
  } catch (e) {
    console.error(`[TB GET FAIL] ${path}:`, e.response?.status, JSON.stringify(e.response?.data || e.message).slice(0, 300));
    throw e;
  }
}

// ===== LINE Messaging API =====
const LINE_CHANNEL_ACCESS_TOKEN = process.env.LINE_CHANNEL_ACCESS_TOKEN || '';
const LINE_CHANNEL_SECRET = process.env.LINE_CHANNEL_SECRET || '';

async function sendLinePush(userId, messages, options = {}) {
  // options: { groupId, storeId, pushType, triggeredBy, description }
  if (!LINE_CHANNEL_ACCESS_TOKEN) {
    console.log('[LINE Push] No token configured, skipping push');
    return false;
  }
  let success = false;
  try {
    const res = await fetch('https://api.line.me/v2/bot/message/push', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${LINE_CHANNEL_ACCESS_TOKEN}`,
      },
      body: JSON.stringify({ to: userId, messages }),
    });
    const data = await res.json().catch(() => ({}));
    if (!res.ok) console.error('[LINE Push] Error:', data);
    success = res.ok;
  } catch (e) {
    console.error('[LINE Push] Failed:', e.message);
    success = false;
  }

  // Record push log
  try {
    await db.query(`
      INSERT INTO push_logs (group_id, store_id, push_type, recipient_count, message_count, triggered_by, target_user_id, description, success)
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
    `, [
      options.groupId || null,
      options.storeId || null,
      options.pushType || 'unknown',
      1,
      messages.length,
      options.triggeredBy || 'system',
      userId,
      options.description || '',
      success
    ]);
  } catch (e) { console.error('[Push Log] Error:', e.message); }

  return success;
}

async function sendLineText(userId, text, options = {}) {
  return sendLinePush(userId, [{ type: 'text', text }], options);
}

async function sendLineFlexMessage(userId, altText, contents, options = {}) {
  return sendLinePush(userId, [{ type: 'flex', altText, contents }], options);
}

// ===== Flex Message Template Builders (ODay-inspired clean style) =====
const LIFF_URL = 'https://liff.line.me/2009552592-xkDKSJ1Y';
const LIFF_WASH = LIFF_URL + '?tab=wash';
const LIFF_PROFILE = LIFF_URL + '?tab=profile';
const LINE_OA_CHAT_URL = 'https://line.me/R/ti/p/@016kcwrh';
const OFFICIAL_WEBSITE = 'https://cloudmonster-website.vercel.app';
const CONTACT_PHONE = '0800-018-888';
const CONTACT_EMAIL = 'contact@cloudmonster.com.tw';
const DEFAULT_SUPPORT_PHONE = '0800-018-888';
const BRAND_PRIMARY = '#3A3A8C';
const BRAND_GOLD = '#E5B94C';
const GREEN = '#2ECC71';
const RED = '#E74C3C';

// Helper: build support footer buttons (LINE OA chat + phone)
function buildSupportFooterButtons(supportUrl, supportPhone) {
  const chatUrl = supportUrl || LINE_OA_CHAT_URL;
  const phone = supportPhone || DEFAULT_SUPPORT_PHONE;
  return [
    { type: 'button', action: { type: 'uri', label: 'LINE 聯繫客服', uri: chatUrl }, style: 'link', height: 'sm' },
    { type: 'button', action: { type: 'uri', label: `撥打客服 ${phone}`, uri: `tel:${phone}` }, style: 'link', height: 'sm' },
  ];
}

function buildPaymentFlexMessage({ storeName, machineName, modeName, amount, discount, finalAmount, orderId, paymentMethod, minutes, supportUrl, supportPhone, groupId, storeId }) {
  return {
    type: 'bubble',
    body: {
      type: 'box', layout: 'vertical', paddingAll: '20px', spacing: 'md',
      contents: [
        { type: 'text', text: '收據', color: GREEN, size: 'sm', weight: 'bold' },
        { type: 'text', text: `${storeName || '雲管家'}`, size: 'lg', weight: 'bold' },
        { type: 'text', text: machineName || '', color: '#888888', size: 'sm' },
        { type: 'separator', margin: 'lg' },
        { type: 'box', layout: 'horizontal', margin: 'lg', contents: [
          { type: 'text', text: modeName || '洗衣+烘乾(標準)', flex: 5, size: 'md', weight: 'bold' },
          { type: 'text', text: `$ ${amount || 0}`, flex: 3, size: 'md', align: 'end' },
        ]},
        { type: 'box', layout: 'horizontal', contents: [
          { type: 'text', text: '使用店家點數折抵', flex: 5, size: 'sm', color: GREEN },
          { type: 'text', text: `-${discount || 0}`, flex: 3, size: 'sm', align: 'end', color: GREEN },
        ]},
        { type: 'separator', margin: 'md' },
        { type: 'box', layout: 'horizontal', margin: 'md', contents: [
          { type: 'text', text: '付款金額', flex: 5, size: 'md', weight: 'bold' },
          { type: 'text', text: `NT $ ${finalAmount || amount || 0}`, flex: 3, size: 'lg', align: 'end', weight: 'bold', color: GREEN },
        ]},
        { type: 'separator', margin: 'lg' },
        { type: 'box', layout: 'vertical', margin: 'md', spacing: 'sm', contents: [
          { type: 'box', layout: 'horizontal', contents: [
            { type: 'text', text: '訂單編號', size: 'xs', color: '#AAAAAA', flex: 3 },
            { type: 'text', text: `#${orderId || '---'}`, size: 'xs', color: '#AAAAAA', flex: 5, align: 'end' },
          ]},
          { type: 'box', layout: 'horizontal', contents: [
            { type: 'text', text: '交易時間', size: 'xs', color: '#AAAAAA', flex: 3 },
            { type: 'text', text: new Date().toLocaleString('zh-TW', { timeZone: 'Asia/Taipei' }), size: 'xs', color: '#AAAAAA', flex: 5, align: 'end' },
          ]},
          { type: 'box', layout: 'horizontal', contents: [
            { type: 'text', text: '付款方式', size: 'xs', color: '#AAAAAA', flex: 3 },
            { type: 'text', text: paymentMethod || '錢包付款', size: 'xs', color: '#AAAAAA', flex: 5, align: 'end' },
          ]},
        ]},
        { type: 'separator', margin: 'md' },
        { type: 'box', layout: 'horizontal', margin: 'md', contents: [
          { type: 'text', text: `預計 ${minutes || 65} 分鐘完成`, size: 'sm', color: RED, weight: 'bold' },
        ]},
      ]
    },
    footer: {
      type: 'box', layout: 'vertical', paddingAll: '16px', spacing: 'sm',
      contents: [
        { type: 'button', action: { type: 'uri', label: '運轉剩餘時間', uri: groupId ? `${LIFF_WASH}&group=${groupId}&store=${storeId || ''}` : LIFF_WASH }, style: 'primary', color: BRAND_GOLD, height: 'sm' },
        { type: 'button', action: { type: 'uri', label: '查看會員點數', uri: LIFF_PROFILE }, style: 'link', height: 'sm' },
        ...buildSupportFooterButtons(supportUrl, supportPhone),
      ]
    }
  };
}

function buildCompleteFlexMessage(storeName, machineName, supportUrl, supportPhone, groupId, storeId) {
  const machineStatusUrl = groupId ? `${LIFF_WASH}&group=${groupId}&store=${storeId || ''}` : LIFF_WASH;
  return {
    type: 'bubble',
    body: {
      type: 'box', layout: 'vertical', paddingAll: '20px', spacing: 'md',
      contents: [
        { type: 'box', layout: 'horizontal', spacing: 'md', contents: [
          { type: 'text', text: '!', size: 'xxl', color: '#E67E22', weight: 'bold' },
          { type: 'text', text: '洗衣完成', size: 'lg', weight: 'bold', color: '#E67E22', gravity: 'center' },
        ]},
        { type: 'separator', margin: 'lg' },
        { type: 'text', text: '您的衣服已經洗乾淨，可以過來領取囉！！', size: 'md', wrap: true, margin: 'lg' },
        { type: 'separator', margin: 'lg' },
        { type: 'box', layout: 'vertical', margin: 'md', spacing: 'sm', contents: [
          { type: 'box', layout: 'horizontal', contents: [
            { type: 'text', text: '門市', size: 'sm', color: '#888888', flex: 2 },
            { type: 'text', text: storeName || '門市', size: 'sm', flex: 3, align: 'end', weight: 'bold' },
          ]},
          { type: 'box', layout: 'horizontal', contents: [
            { type: 'text', text: '機器', size: 'sm', color: '#888888', flex: 2 },
            { type: 'text', text: machineName || '機器', size: 'sm', flex: 3, align: 'end' },
          ]},
        ]},
        { type: 'text', text: '提醒：本訊息內容由店家發送。', size: 'xs', color: '#AAAAAA', margin: 'lg' },
      ]
    },
    footer: {
      type: 'box', layout: 'vertical', paddingAll: '16px', spacing: 'sm',
      contents: [
        { type: 'button', action: { type: 'uri', label: '查看機器狀態', uri: machineStatusUrl }, style: 'primary', color: BRAND_GOLD, height: 'sm' },
        ...buildSupportFooterButtons(supportUrl, supportPhone),
      ]
    }
  };
}

function buildTopupFlexMessage({ groupName, amount, balance, supportUrl, supportPhone }) {
  return {
    type: 'bubble',
    body: {
      type: 'box', layout: 'vertical', paddingAll: '20px', spacing: 'md',
      contents: [
        { type: 'text', text: '儲值成功', color: GREEN, size: 'sm', weight: 'bold' },
        { type: 'text', text: groupName || '雲管家', size: 'lg', weight: 'bold' },
        { type: 'separator', margin: 'md' },
        { type: 'box', layout: 'horizontal', margin: 'md', contents: [
          { type: 'text', text: '儲值金額', flex: 3, size: 'md' },
          { type: 'text', text: `+NT$ ${amount}`, flex: 3, size: 'lg', align: 'end', weight: 'bold', color: GREEN },
        ]},
        { type: 'separator', margin: 'md' },
        { type: 'box', layout: 'horizontal', margin: 'md', contents: [
          { type: 'text', text: '目前餘額', flex: 3, size: 'md', weight: 'bold' },
          { type: 'text', text: `NT$ ${balance || 0}`, flex: 3, size: 'lg', align: 'end', weight: 'bold' },
        ]},
        { type: 'box', layout: 'horizontal', margin: 'md', contents: [
          { type: 'text', text: '交易時間', size: 'xs', color: '#AAA', flex: 3 },
          { type: 'text', text: new Date().toLocaleString('zh-TW', { timeZone: 'Asia/Taipei' }), size: 'xs', color: '#AAA', flex: 5, align: 'end' },
        ]},
      ]
    },
    footer: {
      type: 'box', layout: 'vertical', paddingAll: '16px', spacing: 'sm',
      contents: [
        { type: 'button', action: { type: 'uri', label: '查看會員點數', uri: LIFF_PROFILE }, style: 'primary', color: BRAND_GOLD, height: 'sm' },
        ...buildSupportFooterButtons(supportUrl, supportPhone),
      ]
    }
  };
}

function buildAlmostDoneFlexMessage(storeName, machineName, remainMin, supportUrl, supportPhone, groupId, storeId) {
  return {
    type: 'bubble',
    body: {
      type: 'box', layout: 'vertical', paddingAll: '20px', spacing: 'md',
      contents: [
        { type: 'box', layout: 'horizontal', spacing: 'md', contents: [
          { type: 'text', text: '!', size: 'xxl', color: '#E67E22', weight: 'bold' },
          { type: 'text', text: '即將完成提醒', size: 'lg', weight: 'bold', color: '#E67E22', gravity: 'center' },
        ]},
        { type: 'separator', margin: 'md' },
        { type: 'text', text: '您的洗衣即將完成，可以準備前往取衣了！', size: 'md', wrap: true, margin: 'md' },
        { type: 'box', layout: 'vertical', margin: 'md', spacing: 'sm', contents: [
          { type: 'box', layout: 'horizontal', contents: [
            { type: 'text', text: '門市', size: 'sm', color: '#888888', flex: 2 },
            { type: 'text', text: storeName || '門市', size: 'sm', flex: 3, align: 'end', weight: 'bold' },
          ]},
          { type: 'box', layout: 'horizontal', contents: [
            { type: 'text', text: '機器', size: 'sm', color: '#888888', flex: 2 },
            { type: 'text', text: machineName || '機器', size: 'sm', flex: 3, align: 'end' },
          ]},
        ]},
        { type: 'box', layout: 'horizontal', margin: 'md', paddingAll: '12px', backgroundColor: '#FFF3CD', cornerRadius: '8px', contents: [
          { type: 'text', text: '剩餘時間', size: 'sm', color: '#856404', flex: 2, weight: 'bold' },
          { type: 'text', text: `約 ${remainMin} 分鐘`, size: 'lg', color: '#856404', flex: 3, align: 'end', weight: 'bold' },
        ]},
      ]
    },
    footer: {
      type: 'box', layout: 'vertical', paddingAll: '16px', spacing: 'sm',
      contents: [
        { type: 'button', action: { type: 'uri', label: '運轉剩餘時間', uri: groupId ? `${LIFF_WASH}&group=${groupId}&store=${storeId || ''}` : LIFF_WASH }, style: 'primary', color: BRAND_GOLD, height: 'sm' },
        ...buildSupportFooterButtons(supportUrl, supportPhone),
      ]
    }
  };
}

function buildPromoFlexMessage(message, options = {}) {
  const { title, imageUrl } = options;
  const hero = imageUrl ? { type: 'image', url: imageUrl, size: 'full', aspectRatio: '20:13', aspectMode: 'cover' } : null;
  const bubble = {
    type: 'bubble',
    body: {
      type: 'box', layout: 'vertical', paddingAll: '20px', spacing: 'md',
      contents: [
        { type: 'text', text: title || '雲管家通知', size: 'lg', weight: 'bold' },
        { type: 'separator', margin: 'md' },
        { type: 'text', text: message, wrap: true, size: 'md', color: '#555555', margin: 'md' },
      ]
    },
    footer: {
      type: 'box', layout: 'vertical', paddingAll: '16px', spacing: 'sm',
      contents: [
        { type: 'button', action: { type: 'uri', label: '立即查看', uri: LIFF_URL }, style: 'primary', color: BRAND_GOLD, height: 'sm' },
        { type: 'button', action: { type: 'uri', label: '開啟雲管家', uri: LIFF_URL }, style: 'link', height: 'sm' },
      ]
    }
  };
  if (hero) bubble.hero = hero;
  return bubble;
}

// ===== MQTT Publish Start Command (ThingsBoard primary, HiveMQ fallback) =====
async function publishStartCommand(order) {
  const { id: orderId, store_id, machine_id, mode, temp, total_amount } = order;
  const washModeMap = { standard: 1, small: 2, washonly: 3, soft: 4, strong: 5, dryonly: 0 };
  const dryModeMap = { high: 1, mid: 2, low: 3 };
  const washMode = washModeMap[mode] ?? 1;
  const dryMode = mode === 'washonly' ? 0 : (dryModeMap[temp] ?? 1);
  const coins = Math.ceil(total_amount / 10);
  const mqttPayload = { cmd: 'start', orderId, mode_id: mode, wash_mode: washMode, dry_mode: dryMode, coins, amount: total_amount, temp: temp || 'high' };

  try {
    // Primary: Send via ThingsBoard RPC (with mode_id for program_map lookup)
    await sendThingsBoardRPC(machine_id, 'startMachine', {
      orderId,
      mode_id: mode,
      wash_program: washMode,
      dry_program: dryMode,
      temp: temp || 'high',
      coins,
      amount: total_amount,
    });
    console.log(`[TB RPC] startMachine sent to ${machine_id}`);
    // Also keep HiveMQ as fallback
    if (mqttClient?.connected) {
      const topic = `laundry/${store_id}/${machine_id}/command`;
      mqttClient.publish(topic, JSON.stringify(mqttPayload), { qos: 1 });
    }
  } catch (e) {
    console.error('[TB RPC] Error:', e.message);
    // Fallback to HiveMQ only
    if (mqttClient?.connected) {
      const topic = `laundry/${store_id}/${machine_id}/command`;
      mqttClient.publish(topic, JSON.stringify(mqttPayload), { qos: 1 }, (err) => {
        if (err) console.error('MQTT publish error:', err);
        else console.log(`MQTT fallback start -> ${topic}`);
      });
    }
  }
}

// ===== LINE Pay =====
const LINE_PAY_CHANNEL_ID = process.env.LINE_PAY_CHANNEL_ID;
const LINE_PAY_CHANNEL_SECRET = process.env.LINE_PAY_CHANNEL_SECRET;
const LINE_PAY_ENV = process.env.LINE_PAY_ENV || 'sandbox';
const LINE_PAY_API_HOST = LINE_PAY_ENV === 'sandbox' ? 'sandbox-api-pay.line.me' : 'api-pay.line.me';

function linePayRequest(method, path, body) {
  return new Promise((resolve, reject) => {
    const nonce = uuid();
    const bodyStr = body ? JSON.stringify(body) : '';
    const message = LINE_PAY_CHANNEL_SECRET + path + bodyStr + nonce;
    const signature = crypto.createHmac('sha256', LINE_PAY_CHANNEL_SECRET).update(message).digest('base64');
    const options = {
      hostname: LINE_PAY_API_HOST, path, method,
      headers: {
        'Content-Type': 'application/json',
        'X-LINE-ChannelId': LINE_PAY_CHANNEL_ID,
        'X-LINE-Authorization-Nonce': nonce,
        'X-LINE-Authorization': signature,
      },
    };
    const req = https.request(options, (res) => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => { try { resolve(JSON.parse(data)); } catch (e) { reject(e); } });
    });
    req.on('error', reject);
    if (body) req.write(bodyStr);
    req.end();
  });
}

// ===== Helper: get or create member =====
async function getOrCreateMember(lineUserId) {
  let row = (await db.query('SELECT * FROM members WHERE line_user_id=$1', [lineUserId])).rows[0];
  if (!row) {
    const id = uuid();
    await db.query('INSERT INTO members (id, line_user_id, created_at) VALUES ($1,$2,NOW())', [id, lineUserId]);
    row = { id, line_user_id: lineUserId, wallet: 0 };
  }
  return row;
}

// ===== Helper: get user role =====
async function getUserRole(lineUserId) {
  const r = await db.query('SELECT role, group_id, permissions FROM user_roles WHERE line_user_id=$1', [lineUserId]);
  if (r.rows.length === 0) return { role: 'consumer', groupId: null, permissions: {} };
  // super_admin has group_id = NULL
  const adminRow = r.rows.find(row => row.role === 'super_admin');
  if (adminRow) return { role: 'super_admin', groupId: null, allGroups: r.rows, permissions: { revenue: true, coupons: true, members: true, machines: true, topup: true, news: true, roles: true, monitor: true } };
  const storeAdminRow = r.rows.find(row => row.role === 'store_admin');
  if (storeAdminRow) return { role: 'store_admin', groupId: storeAdminRow.group_id, allGroups: r.rows, permissions: storeAdminRow.permissions || {} };
  return { role: 'consumer', groupId: null, allGroups: r.rows, permissions: {} };
}

// ═══════════════════════════════════════
//  API: User Profile & Role
// ═══════════════════════════════════════
app.get('/api/user/profile', async (req, res) => {
  try {
    const lineUserId = req.query.userId;
    const entryGroupId = req.query.groupId; // From URL param ?group=sg2
    if (!lineUserId) return res.status(400).json({ error: 'userId required' });

    const member = await getOrCreateMember(lineUserId);
    // Save profile data if provided
    const { displayName, pictureUrl } = req.query;
    if (displayName) {
      await db.query(`UPDATE members SET display_name=$1, picture_url=$2, last_login=NOW() WHERE line_user_id=$3`,
        [displayName, pictureUrl || '', lineUserId]).catch(() => {});
    }
    // Auto-assign super_admin if this is the admin user
    const SUPER_ADMIN_ID = process.env.SUPER_ADMIN_LINE_ID || 'Ubdcdd269e115bf9ac492288adbc0115e';
    if (lineUserId === SUPER_ADMIN_ID) {
      const chk = await db.query('SELECT id FROM user_roles WHERE line_user_id=$1 AND role=$2 AND group_id IS NULL', [lineUserId, 'super_admin']);
      if (chk.rows.length === 0) {
        await db.query('INSERT INTO user_roles (line_user_id, role, group_id) VALUES ($1, $2, NULL)', [lineUserId, 'super_admin']);
      }
    }
    const roleInfo = await getUserRole(lineUserId);

    // If consumer entered via a group link, auto-associate them
    if (entryGroupId && roleInfo.role === 'consumer') {
      await db.query(`
        INSERT INTO wallets (line_user_id, group_id, balance) VALUES ($1, $2, 0)
        ON CONFLICT (line_user_id, group_id) DO NOTHING
      `, [lineUserId, entryGroupId]);
    }

    // Get groups this user has access to
    let groups = [];
    if (roleInfo.role === 'super_admin') {
      const gr = await db.query('SELECT * FROM store_groups ORDER BY name');
      groups = gr.rows;
    } else if (roleInfo.role === 'store_admin') {
      const gr = await db.query('SELECT * FROM store_groups WHERE id=$1', [roleInfo.groupId]);
      groups = gr.rows;
    } else {
      // Consumer - get only groups they have a wallet for
      const gr = await db.query(`
        SELECT sg.* FROM store_groups sg
        INNER JOIN wallets w ON w.group_id = sg.id AND w.line_user_id = $1
        ORDER BY sg.name
      `, [lineUserId]);
      groups = gr.rows;
      // If no groups yet and no entry group, return empty
      // Consumer must enter via a group link to get associated
    }

    // Get stores for each group
    for (const g of groups) {
      const sr = await db.query('SELECT * FROM stores WHERE group_id=$1 ORDER BY name', [g.id]);
      g.stores = sr.rows;
    }

    // Get wallets
    const wr = await db.query('SELECT group_id, balance FROM wallets WHERE line_user_id=$1', [lineUserId]);
    const wallets = {};
    wr.rows.forEach(w => { wallets[w.group_id] = w.balance; });

    // Get member level info
    const memberLevel = await getMemberLevel(lineUserId);

    res.json({
      memberId: member.id,
      lineUserId,
      role: roleInfo.role,
      managedGroupId: roleInfo.groupId,
      groups,
      wallets,
      permissions: roleInfo.permissions,
      memberLevel,
    });
  } catch (e) {
    console.error('profile error:', e);
    res.status(500).json({ error: e.message });
  }
});

// ═══════════════════════════════════════
//  API: User Order History
// ═══════════════════════════════════════
app.get('/api/user/orders', async (req, res) => {
  try {
    const userId = req.query.userId;
    if (!userId) return res.status(400).json({ error: 'userId required' });

    const limit = Math.min(Math.max(parseInt(req.query.limit) || 20, 1), 100);
    const offset = Math.max(parseInt(req.query.offset) || 0, 0);

    // Build dynamic WHERE clause for filters
    let whereConditions = [`mb.line_user_id = $1`, `COALESCE(o.type, 'online') != 'topup'`];
    const params = [userId];
    let paramIdx = 2;

    // Filter by order type (e.g., 'online', 'coin_payment')
    if (req.query.type) {
      whereConditions.push(`COALESCE(o.type, 'online') = $${paramIdx}`);
      params.push(req.query.type);
      paramIdx++;
    }
    // Filter by status (e.g., 'paid', 'done', 'pending')
    if (req.query.status) {
      whereConditions.push(`o.status = $${paramIdx}`);
      params.push(req.query.status);
      paramIdx++;
    }
    // Filter by date range
    if (req.query.startDate) {
      whereConditions.push(`o.created_at >= $${paramIdx}::date`);
      params.push(req.query.startDate);
      paramIdx++;
    }
    if (req.query.endDate) {
      whereConditions.push(`o.created_at < ($${paramIdx}::date + INTERVAL '1 day')`);
      params.push(req.query.endDate);
      paramIdx++;
    }

    const whereClause = whereConditions.join(' AND ');

    // Get total count
    const countResult = await db.query(
      `SELECT COUNT(*) FROM orders o
       JOIN members mb ON o.member_id = mb.id
       WHERE ${whereClause}`,
      params
    );
    const total = parseInt(countResult.rows[0].count);

    // Get paginated orders with store, machine, and coupon info
    const ordersResult = await db.query(
      `SELECT o.id, o.store_id, o.machine_id, o.mode, o.addons, o.extend_min, o.temp,
              o.total_amount as amount, o.status, o.created_at, o.paid_at, o.completed_at,
              COALESCE(o.payment_method, 'wallet') as payment_method,
              COALESCE(o.type, 'online') as order_type,
              COALESCE(o.coupon_id, NULL) as coupon_id,
              COALESCE(o.discount_amount, 0) as discount_amount,
              s.name as store_name,
              m.name as machine_name,
              ac.name as coupon_name
       FROM orders o
       JOIN members mb ON o.member_id = mb.id
       LEFT JOIN stores s ON o.store_id = s.id
       LEFT JOIN machines m ON o.machine_id = m.id
       LEFT JOIN user_coupons uc ON o.coupon_id = uc.id
       LEFT JOIN admin_coupons ac ON uc.coupon_id = ac.id
       WHERE ${whereClause}
       ORDER BY o.created_at DESC
       LIMIT $${paramIdx} OFFSET $${paramIdx + 1}`,
      [...params, limit, offset]
    );

    res.json({
      orders: ordersResult.rows,
      total,
      hasMore: offset + limit < total,
    });
  } catch (e) {
    console.error('user orders error:', e);
    res.status(500).json({ error: e.message });
  }
});

// GET single order detail
app.get('/api/user/orders/:orderId', async (req, res) => {
  try {
    const { orderId } = req.params;
    const userId = req.query.userId;
    if (!userId) return res.status(400).json({ error: 'userId required' });

    const result = await db.query(
      `SELECT o.id, o.store_id, o.machine_id, o.mode, o.addons, o.extend_min, o.temp,
              o.total_amount as amount, o.status, o.created_at, o.paid_at, o.completed_at,
              o.pulses, o.duration_sec,
              COALESCE(o.payment_method, 'wallet') as payment_method,
              COALESCE(o.type, 'online') as order_type,
              COALESCE(o.coupon_id, NULL) as coupon_id,
              COALESCE(o.discount_amount, 0) as discount_amount,
              s.name as store_name, s.address as store_address,
              m.name as machine_name,
              ac.name as coupon_name, ac.discount as coupon_discount, ac.type as coupon_type
       FROM orders o
       JOIN members mb ON o.member_id = mb.id
       LEFT JOIN stores s ON o.store_id = s.id
       LEFT JOIN machines m ON o.machine_id = m.id
       LEFT JOIN user_coupons uc ON o.coupon_id = uc.id
       LEFT JOIN admin_coupons ac ON uc.coupon_id = ac.id
       WHERE o.id = $1 AND mb.line_user_id = $2`,
      [orderId, userId]
    );

    if (result.rows.length === 0) {
      return res.status(404).json({ error: '訂單不存在' });
    }

    const order = result.rows[0];
    // Compute original amount before discount for display
    order.original_amount = order.amount + (order.discount_amount || 0);

    res.json({ order });
  } catch (e) {
    console.error('user order detail error:', e);
    res.status(500).json({ error: e.message });
  }
});

// ═══════════════════════════════════════
//  API: Stores & Store Groups
// ═══════════════════════════════════════
app.get('/api/store-groups', async (req, res) => {
  const { userId } = req.query;
  if (!userId) return res.status(400).json({ error: 'Missing userId parameter' });
  try {
    const gr = await db.query('SELECT * FROM store_groups ORDER BY name');
    for (const g of gr.rows) {
      const sr = await db.query('SELECT * FROM stores WHERE group_id=$1 ORDER BY name', [g.id]);
      g.stores = sr.rows;
    }
    res.json(gr.rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/store-groups/:groupId', async (req, res) => {
  try {
    const gr = await db.query('SELECT * FROM store_groups WHERE id=$1', [req.params.groupId]);
    if (gr.rows.length === 0) return res.status(404).json({ error: 'not found' });
    const group = gr.rows[0];
    const sr = await db.query('SELECT * FROM stores WHERE group_id=$1 ORDER BY name', [group.id]);
    group.stores = sr.rows;
    res.json(group);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/stores', async (req, res) => {
  try {
    const r = await db.query('SELECT s.*, sg.name as group_name, sg.type as group_type FROM stores s LEFT JOIN store_groups sg ON s.group_id=sg.id ORDER BY s.name');
    res.json(r.rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ═══════════════════════════════════════
//  API: Wallet (per user per group)
// ═══════════════════════════════════════
app.get('/api/wallet/:groupId', async (req, res) => {
  try {
    const lineUserId = req.query.userId;
    if (!lineUserId) return res.status(400).json({ error: 'userId required' });
    const r = await db.query('SELECT balance FROM wallets WHERE line_user_id=$1 AND group_id=$2', [lineUserId, req.params.groupId]);
    res.json({ balance: r.rows[0]?.balance || 0 });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/wallet/topup', async (req, res) => {
  const client = await db.connect();
  try {
    const { userId, groupId, amount, _internal, source } = req.body;
    if (!userId || !groupId || !amount) return res.status(400).json({ error: 'userId, groupId, amount required' });
    if (amount <= 0) return res.status(400).json({ error: 'amount must be positive' });
    // Only allow topup from trusted sources
    const allowedSources = ['linepay_confirm', 'admin_manual'];
    if (!source || !allowedSources.includes(source)) {
      return res.status(403).json({ error: 'Unauthorized topup source. Use LINE Pay or admin panel.' });
    }

    await client.query('BEGIN');
    // Upsert wallet
    await client.query(`
      INSERT INTO wallets (line_user_id, group_id, balance) VALUES ($1, $2, $3)
      ON CONFLICT (line_user_id, group_id) DO UPDATE SET balance = wallets.balance + $3
    `, [userId, groupId, amount]);
    // Record transaction
    await client.query(`
      INSERT INTO transactions (line_user_id, group_id, type, amount, description, created_at)
      VALUES ($1, $2, 'topup', $3, $4, NOW())
    `, [userId, groupId, amount, `Topup +${amount}`]);
    await client.query('COMMIT');

    const r = await db.query('SELECT balance FROM wallets WHERE line_user_id=$1 AND group_id=$2', [userId, groupId]);
    res.json({ success: true, balance: r.rows[0].balance });
  } catch (e) {
    await client.query('ROLLBACK').catch(() => {});
    res.status(500).json({ error: e.message });
  } finally { client.release(); }
});

app.post('/api/wallet/deduct', async (req, res) => {
  const client = await db.connect();
  try {
    const { userId, groupId, amount, description } = req.body;
    if (!userId || !groupId || !amount) return res.status(400).json({ error: 'missing params' });
    // Validate userId is a valid LINE user ID (starts with U, length 33)
    if (typeof userId !== 'string' || !userId.startsWith('U') || userId.length !== 33) {
      return res.status(403).json({ error: 'Invalid userId format' });
    }

    await client.query('BEGIN');
    // Atomic deduct with balance check in single statement
    const deductResult = await client.query(
      `UPDATE wallets SET balance = balance - $3 WHERE line_user_id=$1 AND group_id=$2 AND balance >= $3 RETURNING balance`,
      [userId, groupId, amount]
    );
    if (deductResult.rows.length === 0) {
      await client.query('ROLLBACK');
      const wr = await db.query('SELECT balance FROM wallets WHERE line_user_id=$1 AND group_id=$2', [userId, groupId]);
      return res.status(400).json({ error: 'insufficient balance', balance: wr.rows[0]?.balance || 0 });
    }
    // Record transaction
    await client.query(`
      INSERT INTO transactions (line_user_id, group_id, type, amount, description, created_at)
      VALUES ($1, $2, 'payment', $3, $4, NOW())
    `, [userId, groupId, -amount, description || 'Payment']);
    await client.query('COMMIT');

    res.json({ success: true, balance: deductResult.rows[0].balance });
  } catch (e) {
    await client.query('ROLLBACK').catch(() => {});
    res.status(500).json({ error: e.message });
  } finally { client.release(); }
});

// ═══════════════════════════════════════
//  API: Transactions
// ═══════════════════════════════════════
app.get('/api/transactions', async (req, res) => {
  try {
    const { userId, groupId, type, limit: lim } = req.query;
    let query = 'SELECT * FROM transactions WHERE 1=1';
    const params = [];
    let idx = 1;

    if (userId) { query += ` AND line_user_id=$${idx++}`; params.push(userId); }
    if (groupId) { query += ` AND group_id=$${idx++}`; params.push(groupId); }
    if (type) { query += ` AND type=$${idx++}`; params.push(type); }
    query += ` ORDER BY created_at DESC LIMIT $${idx++}`;
    params.push(parseInt(lim) || 50);

    const r = await db.query(query, params);
    res.json(r.rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ═══════════════════════════════════════
//  API: Admin Dashboard
// ═══════════════════════════════════════
app.get('/api/admin/dashboard', async (req, res) => {
  try {
    const { userId, groupId } = req.query;
    if (!userId) return res.status(400).json({ error: 'userId required' });

    const roleInfo = await getUserRole(userId);
    if (roleInfo.role !== 'super_admin' && roleInfo.role !== 'store_admin') {
      return res.status(403).json({ error: 'forbidden' });
    }

    // Scope to group if store_admin
    const scopeGroupId = roleInfo.role === 'store_admin' ? roleInfo.groupId : groupId;

    let storeFilter = '';
    let txFilter = '';
    const params = [];

    if (scopeGroupId) {
      storeFilter = `AND s.group_id = $1`;
      txFilter = `AND t.group_id = $1`;
      params.push(scopeGroupId);
    }

    // Revenue stats
    const revenueQuery = `
      SELECT
        COALESCE(SUM(CASE WHEN o.created_at >= CURRENT_DATE THEN o.total_amount ELSE 0 END), 0) as today_revenue,
        COALESCE(SUM(CASE WHEN o.created_at >= CURRENT_DATE - INTERVAL '7 days' THEN o.total_amount ELSE 0 END), 0) as week_revenue,
        COALESCE(SUM(CASE WHEN o.created_at >= CURRENT_DATE - INTERVAL '30 days' THEN o.total_amount ELSE 0 END), 0) as month_revenue,
        COUNT(CASE WHEN o.created_at >= CURRENT_DATE THEN 1 END) as today_orders,
        COUNT(CASE WHEN o.created_at >= CURRENT_DATE - INTERVAL '30 days' THEN 1 END) as month_orders
      FROM orders o
      JOIN stores s ON o.store_id = s.id
      WHERE o.status IN ('paid','done','running','completed') ${storeFilter}
    `;
    const revenue = (await db.query(revenueQuery, params)).rows[0];

    // Machine status (calculate remaining time based on elapsed since last update)
    const machineQuery = `
      SELECT m.id, m.store_id, m.name, m.size, s.name as store_name,
        CASE
          WHEN cs.state = 'running' AND cs.remain_sec - EXTRACT(EPOCH FROM (NOW() - cs.updated_at))::int <= 0 THEN 'idle'
          WHEN cs.state = 'running' THEN 'running'
          WHEN cs.state IS NULL OR cs.state = 'unknown' THEN 'idle'
          ELSE cs.state
        END as state,
        CASE
          WHEN cs.state = 'running' THEN GREATEST(0, cs.remain_sec - EXTRACT(EPOCH FROM (NOW() - cs.updated_at))::int)
          ELSE 0
        END as remain_sec,
        cs.updated_at as last_seen
      FROM machines m
      JOIN stores s ON m.store_id = s.id
      LEFT JOIN machine_current_state cs ON m.id = cs.machine_id
      WHERE m.active = true ${storeFilter}
      ORDER BY s.name, m.sort_order
    `;
    const machines = (await db.query(machineQuery, params)).rows;

    // Consumer count
    const consumerQuery = scopeGroupId
      ? `SELECT COUNT(DISTINCT line_user_id) as count FROM wallets WHERE group_id = $1`
      : `SELECT COUNT(DISTINCT line_user_id) as count FROM wallets`;
    const consumers = (await db.query(consumerQuery, scopeGroupId ? [scopeGroupId] : [])).rows[0];

    // Recent transactions
    const txQuery = `
      SELECT t.* FROM transactions t
      WHERE 1=1 ${txFilter}
      ORDER BY t.created_at DESC LIMIT 20
    `;
    const recentTx = (await db.query(txQuery, params)).rows;

    // Topup total (stored value)
    const topupQuery = scopeGroupId
      ? `SELECT COALESCE(SUM(amount), 0) as total FROM transactions WHERE group_id = $1 AND type = 'topup'`
      : `SELECT COALESCE(SUM(amount), 0) as total FROM transactions WHERE type = 'topup'`;
    const topupTotal = (await db.query(topupQuery, scopeGroupId ? [scopeGroupId] : [])).rows[0];

    // Store groups with support info
    const groupsQuery = scopeGroupId
      ? `SELECT id, name, type, phone, topup_enabled, support_url, support_phone FROM store_groups WHERE id = $1`
      : `SELECT id, name, type, phone, topup_enabled, support_url, support_phone FROM store_groups ORDER BY name`;
    const storeGroups = (await db.query(groupsQuery, scopeGroupId ? [scopeGroupId] : [])).rows;

    res.json({
      revenue: {
        today: parseInt(revenue.today_revenue),
        week: parseInt(revenue.week_revenue),
        month: parseInt(revenue.month_revenue),
        todayOrders: parseInt(revenue.today_orders),
        monthOrders: parseInt(revenue.month_orders),
      },
      machines,
      consumerCount: parseInt(consumers.count),
      recentTransactions: recentTx,
      totalTopup: parseInt(topupTotal.total),
      storeGroups,
    });
  } catch (e) {
    console.error('dashboard error:', e);
    res.status(500).json({ error: e.message });
  }
});

// ═══════════════════════════════════════
//  API: Admin Store Group Support Settings
// ═══════════════════════════════════════
app.put('/api/admin/store-group/:id/support', async (req, res) => {
  try {
    const { userId, supportUrl, supportPhone } = req.body;
    if (!userId) return res.status(400).json({ error: 'userId required' });
    const roleInfo = await getUserRole(userId);
    if (roleInfo.role !== 'super_admin' && roleInfo.role !== 'store_admin') return res.status(403).json({ error: 'forbidden' });
    await db.query('UPDATE store_groups SET support_url=$1, support_phone=$2 WHERE id=$3', [supportUrl || '', supportPhone || '', req.params.id]);
    res.json({ success: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ═══════════════════════════════════════
//  API: Admin User Management (super_admin only)
// ═══════════════════════════════════════
app.get('/api/admin/users', async (req, res) => {
  try {
    const { userId, search } = req.query;
    const roleInfo = await getUserRole(userId);
    if (roleInfo.role !== 'super_admin') return res.status(403).json({ error: 'forbidden' });
    let query = `
      SELECT ur.id, ur.line_user_id, ur.role, ur.group_id, sg.name as group_name,
        ur.display_name, ur.phone, ur.notes, m.picture_url
      FROM user_roles ur
      LEFT JOIN store_groups sg ON ur.group_id = sg.id
      LEFT JOIN members m ON ur.line_user_id = m.line_user_id
      WHERE 1=1
    `;
    const params = [];
    if (search) {
      query += ` AND (ur.line_user_id ILIKE $1 OR ur.display_name ILIKE $1 OR ur.phone ILIKE $1)`;
      params.push(`%${search}%`);
    }
    query += ' ORDER BY ur.role DESC, sg.name';
    const r = await db.query(query, params);
    res.json(r.rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/admin/users', async (req, res) => {
  try {
    const { userId, targetUserId, role, groupId, displayName, phone, notes, permissions } = req.body;
    const roleInfo = await getUserRole(userId);
    if (roleInfo.role !== 'super_admin') return res.status(403).json({ error: 'forbidden' });
    if (!targetUserId || !role) return res.status(400).json({ error: 'targetUserId and role required' });
    const gid = role === 'super_admin' ? null : (groupId || null);
    await db.query(`
      INSERT INTO user_roles (line_user_id, role, group_id, display_name, phone, notes, permissions)
      VALUES ($1, $2, $3, $4, $5, $6, $7)
      ON CONFLICT (line_user_id, role, COALESCE(group_id, '__NULL__')) DO UPDATE
      SET display_name=COALESCE($4, user_roles.display_name), phone=COALESCE($5, user_roles.phone), notes=COALESCE($6, user_roles.notes), permissions=COALESCE($7, user_roles.permissions)
    `, [targetUserId, role, gid, displayName || null, phone || null, notes || null, JSON.stringify(permissions || {})]);
    res.json({ success: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.delete('/api/admin/users/:id', async (req, res) => {
  try {
    const { userId } = req.query;
    const roleInfo = await getUserRole(userId);
    if (roleInfo.role !== 'super_admin') return res.status(403).json({ error: 'forbidden' });
    await db.query('DELETE FROM user_roles WHERE id=$1', [req.params.id]);
    res.json({ success: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ═══════════════════════════════════════
//  API: Admin Store Topup Management
// ═══════════════════════════════════════
// Get topup overview per group
app.get('/api/admin/topup-overview', async (req, res) => {
  try {
    const { userId } = req.query;
    const roleInfo = await getUserRole(userId);
    if (roleInfo.role !== 'super_admin' && roleInfo.role !== 'store_admin') return res.status(403).json({ error: 'forbidden' });

    const scopeGroupId = roleInfo.role === 'store_admin' ? roleInfo.groupId : null;
    let query = `
      SELECT sg.id, sg.name, sg.type, sg.phone,
        COALESCE(sg.topup_enabled, true) as topup_enabled,
        COALESCE(w.total_balance, 0) as total_balance,
        COALESCE(w.consumer_count, 0) as consumer_count,
        COALESCE(t.total_topup, 0) as total_topup,
        COALESCE(t.total_spent, 0) as total_spent
      FROM store_groups sg
      LEFT JOIN (
        SELECT group_id, SUM(balance) as total_balance, COUNT(DISTINCT line_user_id) as consumer_count
        FROM wallets GROUP BY group_id
      ) w ON w.group_id = sg.id
      LEFT JOIN (
        SELECT group_id,
          COALESCE(SUM(CASE WHEN type='topup' THEN amount ELSE 0 END), 0) as total_topup,
          COALESCE(SUM(CASE WHEN type='payment' THEN ABS(amount) ELSE 0 END), 0) as total_spent
        FROM transactions GROUP BY group_id
      ) t ON t.group_id = sg.id
    `;
    const qParams = []; if (scopeGroupId) { qParams.push(scopeGroupId); query += ` WHERE sg.id = $${qParams.length}`; }
    query += ' ORDER BY sg.name';
    const r = await db.query(query, qParams);
    res.json(r.rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Toggle topup enabled/disabled for a group
app.post('/api/admin/topup-toggle', async (req, res) => {
  try {
    const { userId, groupId, enabled } = req.body;
    const roleInfo = await getUserRole(userId);
    if (roleInfo.role !== 'super_admin') return res.status(403).json({ error: 'forbidden' });
    // Add topup_enabled column if not exists
    await db.query('ALTER TABLE store_groups ADD COLUMN IF NOT EXISTS topup_enabled BOOLEAN DEFAULT true').catch(() => {});
    await db.query('UPDATE store_groups SET topup_enabled=$1 WHERE id=$2', [enabled, groupId]);
    res.json({ success: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Manual topup/deduct for a consumer
app.post('/api/admin/manual-topup', async (req, res) => {
  try {
    const { userId, targetUserId, groupId, amount, description } = req.body;
    const roleInfo = await getUserRole(userId);
    if (roleInfo.role !== 'super_admin' && roleInfo.role !== 'store_admin') return res.status(403).json({ error: 'forbidden' });
    if (roleInfo.role === 'store_admin' && roleInfo.groupId !== groupId) return res.status(403).json({ error: 'not your group' });
    if (!targetUserId || !groupId || !amount) return res.status(400).json({ error: 'missing params' });

    // Upsert wallet
    await db.query(`
      INSERT INTO wallets (line_user_id, group_id, balance) VALUES ($1, $2, $3)
      ON CONFLICT (line_user_id, group_id) DO UPDATE SET balance = GREATEST(0, wallets.balance + $3)
    `, [targetUserId, groupId, parseInt(amount)]);

    // Record transaction
    const type = parseInt(amount) > 0 ? 'topup' : 'adjustment';
    await db.query(`
      INSERT INTO transactions (line_user_id, group_id, type, amount, description, created_at)
      VALUES ($1, $2, $3, $4, $5, NOW())
    `, [targetUserId, groupId, type, parseInt(amount), description || `管理員${parseInt(amount) > 0 ? '儲值' : '扣點'}`]);

    const r = await db.query('SELECT balance FROM wallets WHERE line_user_id=$1 AND group_id=$2', [targetUserId, groupId]);
    res.json({ success: true, balance: r.rows[0]?.balance || 0 });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Search consumers
app.get('/api/admin/consumers', async (req, res) => {
  try {
    const { userId, groupId, search } = req.query;
    const roleInfo = await getUserRole(userId);
    if (roleInfo.role !== 'super_admin' && roleInfo.role !== 'store_admin') return res.status(403).json({ error: 'forbidden' });
    const scopeGroupId = roleInfo.role === 'store_admin' ? roleInfo.groupId : groupId;
    // Group by unique member, aggregate wallets
    let query = `
      SELECT m.line_user_id, m.display_name, m.picture_url, m.created_at as registered_at, m.last_login,
        COALESCE(SUM(w.balance), 0) as total_balance,
        COUNT(w.id) as group_count,
        json_agg(json_build_object('group_id', w.group_id, 'group_name', sg.name, 'balance', w.balance)) as wallets
      FROM members m
      LEFT JOIN wallets w ON w.line_user_id = m.line_user_id
      LEFT JOIN store_groups sg ON w.group_id = sg.id
      WHERE w.id IS NOT NULL
    `;
    const params = [];
    let idx = 1;
    if (scopeGroupId) { query += ` AND w.group_id=$${idx++}`; params.push(scopeGroupId); }
    if (search) { query += ` AND (m.line_user_id ILIKE $${idx} OR m.display_name ILIKE $${idx})`; params.push(`%${search}%`); idx++; }
    query += ' GROUP BY m.line_user_id, m.display_name, m.picture_url, m.created_at, m.last_login';
    query += ' ORDER BY m.last_login DESC NULLS LAST LIMIT 50';
    const r = await db.query(query, params);
    res.json(r.rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Get a specific consumer's detail with transaction history
app.get('/api/admin/consumer/:lineUserId', async (req, res) => {
  try {
    const { userId } = req.query;
    const roleInfo = await getUserRole(userId);
    if (roleInfo.role !== 'super_admin' && roleInfo.role !== 'store_admin') return res.status(403).json({ error: 'forbidden' });
    const targetId = req.params.lineUserId;

    // Member info
    const memberR = await db.query('SELECT * FROM members WHERE line_user_id=$1', [targetId]);
    const member = memberR.rows[0] || {};

    // Wallets
    const walletsR = await db.query(`
      SELECT w.group_id, w.balance, sg.name as group_name
      FROM wallets w JOIN store_groups sg ON w.group_id = sg.id
      WHERE w.line_user_id=$1 ORDER BY sg.name
    `, [targetId]);

    // Transactions
    let txQuery = 'SELECT t.*, sg.name as group_name FROM transactions t LEFT JOIN store_groups sg ON t.group_id = sg.id WHERE t.line_user_id=$1';
    const txParams = [targetId];
    if (roleInfo.role === 'store_admin') {
      txQuery += ' AND t.group_id=$2';
      txParams.push(roleInfo.groupId);
    }
    txQuery += ' ORDER BY t.created_at DESC LIMIT 50';
    const txR = await db.query(txQuery, txParams);

    // Orders
    let orderQuery = `
      SELECT o.*, s.name as store_name, m2.name as machine_name
      FROM orders o
      JOIN members mb ON o.member_id = mb.id
      JOIN stores s ON o.store_id = s.id
      JOIN machines m2 ON o.machine_id = m2.id
      WHERE mb.line_user_id=$1
    `;
    const orderParams = [targetId];
    if (roleInfo.role === 'store_admin') {
      orderQuery += ' AND s.group_id=$2';
      orderParams.push(roleInfo.groupId);
    }
    orderQuery += ' ORDER BY o.created_at DESC LIMIT 30';
    const orderR = await db.query(orderQuery, orderParams);

    res.json({
      member: { lineUserId: targetId, displayName: member.display_name, pictureUrl: member.picture_url, registeredAt: member.created_at, lastLogin: member.last_login },
      wallets: walletsR.rows,
      transactions: txR.rows,
      orders: orderR.rows,
    });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ═══════════════════════════════════════
//  API: Machines (existing, enhanced)
// ═══════════════════════════════════════
app.get('/api/machines/:storeId', async (req, res) => {
  try {
    const r = await db.query(`
      SELECT m.*,
      CASE
        WHEN cs.state = 'running' AND cs.remain_sec - EXTRACT(EPOCH FROM (NOW() - cs.updated_at))::int <= 0 THEN 'idle'
        ELSE COALESCE(cs.state,'unknown')
      END as state,
      CASE
        WHEN cs.state = 'running' THEN GREATEST(0, cs.remain_sec - EXTRACT(EPOCH FROM (NOW() - cs.updated_at))::int)
        ELSE 0
      END as remain_sec,
      COALESCE(cs.progress,0) as progress,
      cs.updated_at as last_seen
      FROM machines m LEFT JOIN machine_current_state cs ON m.id=cs.machine_id
      WHERE m.store_id=$1 AND m.active=true ORDER BY m.sort_order,m.name
    `, [req.params.storeId]);
    res.json(r.rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ═══════════════════════════════════════
//  API: Orders (existing, enhanced)
// ═══════════════════════════════════════
app.post('/api/orders/create', async (req, res) => {
  try {
    const { lineUserId, storeId, machineId, mode, addons, extendMin, temp, totalAmount } = req.body;
    let member = (await db.query('SELECT id FROM members WHERE line_user_id=$1', [lineUserId])).rows[0];
    if (!member) {
      const id = uuid();
      await db.query('INSERT INTO members (id,line_user_id,created_at) VALUES ($1,$2,NOW())', [id, lineUserId]);
      member = { id };
    }
    const orderId = 'ORD' + Date.now() + '-' + Math.random().toString(36).slice(2, 6);
    const pulses = Math.ceil(totalAmount / 10);
    const modeDur = { standard:65, small:50, washonly:35, soft:65, strong:75, dryonly:40 };
    const durationSec = ((modeDur[mode]||65) + parseInt(extendMin||0)) * 60;
    await db.query(
      `INSERT INTO orders (id,member_id,store_id,machine_id,mode,addons,extend_min,temp,total_amount,pulses,duration_sec,status,created_at)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,'pending',NOW())`,
      [orderId, member.id, storeId, machineId, mode, JSON.stringify(addons||[]), extendMin||0, temp, totalAmount, pulses, durationSec]
    );
    res.json({ orderId, totalAmount, pulses });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/orders/:lineUserId', async (req, res) => {
  const r = await db.query(`
    SELECT o.*,s.name as store_name,m.name as machine_name,
    cs.remain_sec,cs.progress FROM orders o
    JOIN members mb ON o.member_id=mb.id
    JOIN stores s ON o.store_id=s.id JOIN machines m ON o.machine_id=m.id
    LEFT JOIN machine_current_state cs ON o.machine_id=cs.machine_id
    WHERE mb.line_user_id=$1 ORDER BY o.created_at DESC LIMIT 20
  `, [req.params.lineUserId]);
  res.json(r.rows);
});

// ═══════════════════════════════════════
//  LINE Pay API (existing)
// ═══════════════════════════════════════
app.post('/api/payment/request', async (req, res) => {
  try {
    const { orderId, amount, orderName } = req.body;
    const SERVER_URL = process.env.SERVER_URL || 'https://laundry-backend-production-efa4.up.railway.app';
    const FRONTEND_URL = 'https://laundry-frontend-chi.vercel.app';
    const body = {
      amount, currency: 'TWD', orderId,
      packages: [{ id: orderId, amount, name: orderName || '雲管家洗衣服務', products: [{ name: orderName || '洗衣服務', quantity: 1, price: amount }] }],
      redirectUrls: {
        confirmUrl: `${SERVER_URL}/api/payment/confirm`,
        cancelUrl: `${FRONTEND_URL}/?status=cancel&orderId=${orderId}`,
      },
    };
    const result = await linePayRequest('POST', '/v3/payments/request', body);
    if (result.returnCode === '0000') {
      await db.query(`UPDATE orders SET status='waiting_payment' WHERE id=$1`, [orderId]);
      res.json({ success: true, paymentUrl: result.info.paymentUrl.web, transactionId: result.info.transactionId });
    } else {
      res.status(400).json({ success: false, error: result.returnMessage });
    }
  } catch (e) { res.status(500).json({ success: false, error: e.message }); }
});

app.get('/api/payment/confirm', async (req, res) => {
  try {
    const { transactionId, orderId } = req.query;
    const FRONTEND_URL = 'https://laundry-frontend-chi.vercel.app';
    const orderResult = await db.query('SELECT * FROM orders WHERE id=$1 AND status NOT IN ($2, $3)', [orderId, 'paid', 'completed']);
    if (orderResult.rows.length === 0) return res.redirect(`${FRONTEND_URL}/?status=success&orderId=${orderId}&already_confirmed=1`);
    const order = orderResult.rows[0];
    const body = { amount: order.total_amount, currency: 'TWD' };
    const result = await linePayRequest('POST', `/v3/payments/${transactionId}/confirm`, body);
    if (result.returnCode === '0000') {
      await db.query(`UPDATE orders SET status='paid', paid_at=NOW(), payment_method='linepay' WHERE id=$1`, [orderId]);
      // Update machine state in DB
      await db.query(`
        INSERT INTO machine_current_state (machine_id, state, remain_sec, progress, updated_at)
        VALUES ($1, 'running', $2, 0, NOW())
        ON CONFLICT (machine_id) DO UPDATE SET state='running', remain_sec=$2, progress=0, updated_at=NOW()
      `, [order.machine_id, order.duration_sec || 3600]);
      await publishStartCommand(order);
      // LINE Push: payment success
      const memberRow = (await db.query('SELECT line_user_id FROM members WHERE id=$1', [order.member_id])).rows[0];
      if (memberRow) {
        // Auto-bind user to store
        try {
          await db.query(`INSERT INTO user_store_bindings (line_user_id, store_id, bound_at) VALUES ($1, $2, NOW()) ON CONFLICT (line_user_id, store_id) DO UPDATE SET bound_at = NOW()`, [memberRow.line_user_id, order.store_id]);
        } catch(e) {}
        const storeRow = (await db.query('SELECT name, group_id FROM stores WHERE id=$1', [order.store_id])).rows[0];
        const sName = storeRow?.name || order.store_id;
        let paymentSupportUrl = '';
        let paymentSupportPhone = '';
        if (storeRow?.group_id) {
          const sgr = (await db.query('SELECT support_url, support_phone FROM store_groups WHERE id=$1', [storeRow.group_id])).rows[0];
          paymentSupportUrl = sgr?.support_url || '';
          paymentSupportPhone = sgr?.support_phone || '';
        }
        const mName = order.machine_id.includes('-d') ? `烘乾機` : `洗脫烘`;
        const mins = Math.ceil((order.duration_sec || 3600) / 60);
        const modeLabels = { standard: '標準洗衣', small: '少量快洗', washonly: '單洗程', soft: '柔洗', strong: '強力洗', dryonly: '單烘乾', dryextend: '加烘' };
        sendLineFlexMessage(memberRow.line_user_id, '付款成功', buildPaymentFlexMessage({
          storeName: sName, machineName: mName, modeName: modeLabels[order.mode] || order.mode || '標準洗衣',
          amount: order.total_amount, discount: 0, finalAmount: order.total_amount,
          orderId: orderId, paymentMethod: 'LINE Pay', minutes: mins, supportUrl: paymentSupportUrl, supportPhone: paymentSupportPhone, groupId: storeRow?.group_id, storeId: order.store_id
        }), { pushType: 'auto_payment', storeId: order.store_id, description: `LINE Pay付款成功 ${sName} ${mName}` }).catch(() => {});
      }
      res.redirect(`${FRONTEND_URL}/?status=success&orderId=${orderId}`);
    } else {
      res.redirect(`${FRONTEND_URL}/?status=fail&msg=${encodeURIComponent(result.returnMessage)}`);
    }
  } catch (e) {
    res.redirect(`https://laundry-frontend-chi.vercel.app/?status=error&msg=${encodeURIComponent(e.message)}`);
  }
});

app.get('/api/payment/cancel', async (req, res) => {
  const { orderId } = req.query;
  if (orderId) await db.query(`UPDATE orders SET status='cancelled' WHERE id=$1`, [orderId]);
  res.redirect(`https://laundry-frontend-chi.vercel.app/?status=cancel&orderId=${orderId || ''}`);
});

// ═══════════════════════════════════════
//  API: Unified Payment Create (frontend calls this)
// ═══════════════════════════════════════
app.post('/api/payment/create', async (req, res) => {
  try {
    const { userId, storeId, machineId, machineNum, mode, amount, minutes, dryTemp, couponId } = req.body;
    if (!userId || !storeId || !machineId || !amount) {
      return res.status(400).json({ success: false, error: 'Missing required fields' });
    }

    // Create member if not exists
    let member = (await db.query('SELECT id FROM members WHERE line_user_id=$1', [userId])).rows[0];
    if (!member) {
      const id = uuid();
      await db.query('INSERT INTO members (id,line_user_id,created_at) VALUES ($1,$2,NOW())', [id, userId]);
      member = { id };
    }

    // Coupon validation
    let discountAmount = 0;
    let validatedCouponId = null;
    let finalAmount = amount;

    if (couponId) {
      try {
        console.log(`[Coupon] Validating coupon userCouponId=${couponId} for userId=${userId}`);
        const ucRes = await db.query(
          `SELECT uc.*, c.discount, c.type as coupon_type, c.min_spend, c.name as coupon_name
           FROM user_coupons uc
           JOIN admin_coupons c ON uc.coupon_id = c.id
           WHERE uc.id = $1 AND uc.line_user_id = $2 AND uc.status = 'active' AND uc.uses_remaining > 0`,
          [couponId, userId]
        );
        if (ucRes.rows.length === 0) {
          return res.status(400).json({ success: false, error: '優惠券無效、已使用完畢或不屬於此帳號' });
        }
        const uc = ucRes.rows[0];
        // Check expiry
        if (uc.expiry_date && new Date(uc.expiry_date) < new Date()) {
          return res.status(400).json({ success: false, error: '優惠券已過期' });
        }
        // Check min_spend
        if (uc.min_spend && amount < uc.min_spend) {
          return res.status(400).json({ success: false, error: `未達最低消費 NT$${uc.min_spend}` });
        }
        // Calculate discount
        if (uc.coupon_type === 'percent') {
          discountAmount = Math.floor(amount * uc.discount / 100);
        } else if (uc.coupon_type === 'free') {
          discountAmount = amount;
        } else {
          // fixed discount
          discountAmount = Math.min(uc.discount, amount);
        }
        finalAmount = Math.max(amount - discountAmount, 0);
        validatedCouponId = couponId;
        console.log(`[Coupon] Applied "${uc.coupon_name}": original=${amount}, discount=${discountAmount}, final=${finalAmount}`);
      } catch (couponErr) {
        console.error('[Coupon] Validation error:', couponErr.message);
        return res.status(500).json({ success: false, error: '優惠券驗證失敗' });
      }
    }

    // Create order (with coupon deduction in transaction)
    const orderId = 'ORD' + Date.now() + '-' + Math.random().toString(36).slice(2, 6);
    const pulses = Math.ceil(amount / 10);
    const modeDur = { standard: 65, small: 50, washonly: 35, soft: 65, strong: 75, dryonly: 40, dryextend: 0 };
    const baseDur = modeDur[mode] || 0;
    const durationSec = (baseDur + (minutes || 0)) * 60;

    const txClient = await db.connect();
    try {
      await txClient.query('BEGIN');
      await txClient.query(
        `INSERT INTO orders (id,member_id,store_id,machine_id,mode,addons,extend_min,temp,total_amount,pulses,duration_sec,status,coupon_id,discount_amount,created_at)
         VALUES ($1,$2,$3,$4,$5,'[]',$6,$7,$8,$9,$10,'pending',$11,$12,NOW())`,
        [orderId, member.id, storeId, machineId, mode, minutes || 0, dryTemp || 'high', finalAmount, pulses, durationSec, validatedCouponId, discountAmount]
      );
      // Deduct coupon uses atomically
      if (validatedCouponId) {
        const couponDeduct = await txClient.query(
          `UPDATE user_coupons SET uses_remaining = uses_remaining - 1, status = CASE WHEN uses_remaining - 1 <= 0 THEN 'used' ELSE status END WHERE id = $1 AND uses_remaining > 0 RETURNING id`,
          [validatedCouponId]
        );
        if (couponDeduct.rows.length === 0) {
          throw new Error('優惠券已用完');
        }
        console.log(`[Coupon] Deducted 1 use from userCouponId=${validatedCouponId}`);
      }
      await txClient.query('COMMIT');
    } catch (txErr) {
      await txClient.query('ROLLBACK').catch(() => {});
      throw txErr;
    } finally {
      txClient.release();
    }

    // Try LINE Pay
    const SERVER_URL = process.env.SERVER_URL || 'https://laundry-backend-production-efa4.up.railway.app';
    const FRONTEND_URL = 'https://laundry-frontend-chi.vercel.app';
    try {
      const storeName = (await db.query('SELECT name FROM stores WHERE id=$1', [storeId])).rows[0]?.name || '洗衣服務';
      const machineName = machineId.includes('-d') ? `烘乾${machineNum}號` : `洗脫烘${machineNum}號(${mode})`;
      const orderName = `${storeName} - ${machineName}`;
      const payBody = {
        amount: finalAmount, currency: 'TWD', orderId,
        packages: [{ id: orderId, amount: finalAmount, name: orderName, products: [{ name: orderName, quantity: 1, price: finalAmount }] }],
        redirectUrls: {
          confirmUrl: `${SERVER_URL}/api/payment/confirm`,
          cancelUrl: `${FRONTEND_URL}/?status=cancel&orderId=${orderId}`,
        },
      };
      const result = await linePayRequest('POST', '/v3/payments/request', payBody);
      if (result.returnCode === '0000') {
        await db.query(`UPDATE orders SET status='waiting_payment' WHERE id=$1`, [orderId]);
        return res.json({ success: true, paymentUrl: result.info.paymentUrl.web, transactionId: result.info.transactionId, orderId, discountAmount, finalAmount });
      }
    } catch (linePayErr) {
      console.warn('LINE Pay unavailable, using demo mode:', linePayErr.message);
    }

    // Demo mode: LINE Pay unavailable, complete payment directly
    await db.query(`UPDATE orders SET status='paid', paid_at=NOW() WHERE id=$1`, [orderId]);

    // Update machine_current_state in DB so admin dashboard can see it
    const totalSec = durationSec || (minutes || 0) * 60;
    await db.query(`
      INSERT INTO machine_current_state (machine_id, state, remain_sec, progress, updated_at)
      VALUES ($1, 'running', $2, 0, NOW())
      ON CONFLICT (machine_id) DO UPDATE SET state='running', remain_sec=$2, progress=0, updated_at=NOW()
    `, [machineId, totalSec]);

    // Record transaction
    const store = (await db.query('SELECT group_id FROM stores WHERE id=$1', [storeId])).rows[0];
    if (store?.group_id) {
      await db.query(`
        INSERT INTO transactions (line_user_id, group_id, type, amount, description, created_at)
        VALUES ($1, $2, 'payment', $3, $4, NOW())
      `, [userId, store.group_id, -finalAmount, `${storeId} - ${mode}${discountAmount > 0 ? ` (折扣 NT$${discountAmount})` : ''}`]);
    }

    // Try MQTT start command
    try { await publishStartCommand({ id: orderId, store_id: storeId, machine_id: machineId, mode, temp: dryTemp || 'high', total_amount: amount }); } catch (e) {}

    // LINE Push: payment success (demo mode)
    {
      const sName = (await db.query('SELECT name FROM stores WHERE id=$1', [storeId])).rows[0]?.name || storeId;
      let demoSupportUrl = '';
      let demoSupportPhone = '';
      if (store?.group_id) {
        const sgr = (await db.query('SELECT support_url, support_phone FROM store_groups WHERE id=$1', [store.group_id])).rows[0];
        demoSupportUrl = sgr?.support_url || '';
        demoSupportPhone = sgr?.support_phone || '';
      }
      const mName = machineId.includes('-d') ? `烘乾${machineNum || ''}號` : `洗脫烘${machineNum || ''}號`;
      const mins = Math.ceil((durationSec || (minutes || 0) * 60) / 60);
      const modeLabels = { standard: '標準洗衣', small: '少量快洗', washonly: '單洗程', soft: '柔洗', strong: '強力洗', dryonly: '單烘乾', dryextend: '加烘' };
      sendLineFlexMessage(userId, '付款成功', buildPaymentFlexMessage({
        storeName: sName, machineName: mName, modeName: modeLabels[mode] || mode || '標準洗衣',
        amount: amount, discount: discountAmount, finalAmount: finalAmount,
        orderId: orderId, paymentMethod: '錢包付款', minutes: mins, supportUrl: demoSupportUrl, supportPhone: demoSupportPhone, groupId: store?.group_id, storeId: storeId
      }), { pushType: 'auto_payment', storeId: storeId, groupId: store?.group_id, description: `Demo付款成功 ${sName} ${mName}` }).catch(() => {});
      // Auto-bind user to store
      try {
        await db.query(`INSERT INTO user_store_bindings (line_user_id, store_id, bound_at) VALUES ($1, $2, NOW()) ON CONFLICT (line_user_id, store_id) DO UPDATE SET bound_at = NOW()`, [userId, storeId]);
      } catch(e) {}
    }

    res.json({ success: true, orderId, demoMode: true, discountAmount, finalAmount });
  } catch (e) {
    console.error('payment/create error:', e);
    res.status(500).json({ success: false, error: e.message });
  }
});

// API: Report machine state from frontend (for cross-user visibility)
app.post('/api/machines/state', requireIotApiKey, async (req, res) => {
  try {
    const { machineId, state, remainSec } = req.body;
    if (!machineId) return res.status(400).json({ error: 'machineId required' });
    await db.query(`
      INSERT INTO machine_current_state (machine_id, state, remain_sec, progress, updated_at)
      VALUES ($1, $2, $3, 0, NOW())
      ON CONFLICT (machine_id) DO UPDATE SET state=$2, remain_sec=$3, progress=0, updated_at=NOW()
    `, [machineId, state || 'running', remainSec || 0]);
    res.json({ success: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ═══════════════════════════════════════
//  API: LINE Pay Topup
// ═══════════════════════════════════════
app.post('/api/topup/linepay', async (req, res) => {
  try {
    const { userId, groupId, amount } = req.body;
    if (!userId || !groupId || !amount || amount <= 0) return res.status(400).json({ error: 'missing params' });
    const orderId = 'TOP' + Date.now() + '-' + Math.random().toString(36).slice(2, 6);
    const SERVER_URL = process.env.SERVER_URL || 'https://laundry-backend-production-efa4.up.railway.app';
    const FRONTEND_URL = 'https://laundry-frontend-chi.vercel.app';
    // Get group name
    const gr = await db.query('SELECT name FROM store_groups WHERE id=$1', [groupId]);
    const groupName = gr.rows[0]?.name || '洗衣服務';

    try {
      // Store topup order in DB for tamper-proof confirm
      await db.query(`INSERT INTO orders (id, store_id, member_id, machine_id, mode, total_amount, status, created_at) VALUES ($1, 'topup', $2, $3, 'topup', $4, 'pending', NOW()) ON CONFLICT DO NOTHING`, [orderId, userId, groupId, amount]);
      const payBody = {
        amount, currency: 'TWD', orderId,
        packages: [{ id: orderId, amount, name: `${groupName} 儲值`, products: [{ name: `${groupName} 點數儲值 ${amount}點`, quantity: 1, price: amount }] }],
        redirectUrls: {
          confirmUrl: `${SERVER_URL}/api/topup/confirm?orderId=${orderId}`,
          cancelUrl: `${FRONTEND_URL}/?status=cancel`,
        },
      };
      const result = await linePayRequest('POST', '/v3/payments/request', payBody);
      if (result.returnCode === '0000') {
        return res.json({ success: true, paymentUrl: result.info.paymentUrl.web, transactionId: result.info.transactionId, orderId });
      }
    } catch (linePayErr) {
      console.error('LINE Pay topup error:', linePayErr.message);
      return res.status(500).json({ success: false, error: 'LINE Pay 服務暫時無法使用，請稍後再試' });
    }
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/topup/confirm', async (req, res) => {
  try {
    const { transactionId, orderId, userId: legacyUserId, groupId: legacyGroupId, amount: legacyAmount } = req.query;
    const FRONTEND_URL = process.env.FRONTEND_URL || 'https://laundry-frontend-chi.vercel.app';

    // Retrieve order from DB (tamper-proof) or fallback to legacy URL params
    let userId, groupId, amountInt;
    if (orderId) {
      const orderRow = (await db.query('SELECT member_id, machine_id, total_amount FROM orders WHERE id=$1 AND status NOT IN ($2, $3)', [orderId, 'paid', 'completed'])).rows[0];
      if (!orderRow) return res.redirect(`${FRONTEND_URL}/?status=topup_success&already_confirmed=1`);
      userId = orderRow.member_id;
      groupId = orderRow.machine_id; // stored groupId in machine_id field for topup orders
      amountInt = orderRow.total_amount;
      await db.query('UPDATE orders SET status=$1 WHERE id=$2', ['confirming', orderId]);
    } else {
      // Legacy fallback for old URLs
      userId = legacyUserId; groupId = legacyGroupId; amountInt = parseInt(legacyAmount);
    }

    const body = { amount: amountInt, currency: 'TWD' };
    const result = await linePayRequest('POST', `/v3/payments/${transactionId}/confirm`, body);
    if (result.returnCode === '0000') {
      // Add points to wallet
      await db.query(`
        INSERT INTO wallets (line_user_id, group_id, balance) VALUES ($1, $2, $3)
        ON CONFLICT (line_user_id, group_id) DO UPDATE SET balance = wallets.balance + $3
      `, [userId, groupId, amountInt]);
      await db.query(`
        INSERT INTO transactions (line_user_id, group_id, type, amount, description, created_at)
        VALUES ($1, $2, 'topup', $3, $4, NOW())
      `, [userId, groupId, amountInt, `LINE Pay 儲值 +${amountInt}`]);
      // LINE Push: topup success
      const walletRow = (await db.query('SELECT balance FROM wallets WHERE line_user_id=$1 AND group_id=$2', [userId, groupId])).rows[0];
      const grRow = (await db.query('SELECT name, support_url, support_phone FROM store_groups WHERE id=$1', [groupId])).rows[0];
      const grName = grRow?.name || '';
      const linePayTopupSupportUrl = grRow?.support_url || '';
      const linePayTopupSupportPhone = grRow?.support_phone || '';
      sendLineFlexMessage(userId, '儲值成功', buildTopupFlexMessage({ groupName: grName, amount: amountInt, balance: walletRow?.balance || amountInt, supportUrl: linePayTopupSupportUrl, supportPhone: linePayTopupSupportPhone }), { pushType: 'auto_topup', groupId: groupId, description: `LINE Pay儲值成功 ${grName} NT$${amountInt}` }).catch(() => {});
      res.redirect(`${FRONTEND_URL}/?status=topup_success&amount=${amountInt}`);
    } else {
      res.redirect(`${FRONTEND_URL}/?status=topup_fail`);
    }
  } catch (e) {
    res.redirect(`https://laundry-frontend-chi.vercel.app/?status=topup_fail`);
  }
});

// ═══════════════════════════════════════
//  API: Admin Machine Control
// ═══════════════════════════════════════
app.post('/api/admin/machine/control', async (req, res) => {
  try {
    const { userId, machineId, storeId, action, durationMin } = req.body;
    const roleInfo = await getUserRole(userId);
    if (roleInfo.role !== 'super_admin' && roleInfo.role !== 'store_admin') return res.status(403).json({ error: 'forbidden' });

    // Skip step - send via ThingsBoard RPC (HiveMQ fallback)
    if (action === 'skip_step') {
      const sid = storeId || 'unknown';
      try {
        await sendThingsBoardRPC(machineId, 'skipStep', {});
        console.log(`[TB RPC] skipStep sent to ${machineId}`);
      } catch (e) {
        console.error('[TB RPC] skipStep error:', e.message);
      }
      // HiveMQ fallback
      if (mqttClient?.connected) {
        const topic = `laundry/${sid}/${machineId}/command`;
        mqttClient.publish(topic, JSON.stringify({ cmd: 'skip_step' }), { qos: 1 });
      }
      return res.json({ success: true, message: '已發送跳步指令' });
    }

    // Clear coins - record coin count and reset via ThingsBoard RPC
    if (action === 'clear_coins') {
      const sid = storeId || 'unknown';
      const cached = machineCache[machineId] || {};
      const previousCount = cached.coinCount || 0;
      await db.query(
        `INSERT INTO coin_records (store_id, machine_id, coin_count, cleared_by, cleared_at) VALUES ($1, $2, $3, $4, NOW())`,
        [sid, machineId, previousCount, userId]
      );
      try {
        await sendThingsBoardRPC(machineId, 'clearCoins', {});
        console.log(`[TB RPC] clearCoins sent to ${machineId}`);
      } catch (e) {
        console.error('[TB RPC] clearCoins error:', e.message);
      }
      // HiveMQ fallback
      if (mqttClient?.connected) {
        const topic = `laundry/${sid}/${machineId}/command`;
        mqttClient.publish(topic, JSON.stringify({ cmd: 'clear_coins' }), { qos: 1 });
      }
      return res.json({ success: true, message: '錢箱已清零', previousCount });
    }

    // Start/stop logic via ThingsBoard RPC
    const state = action === 'start' ? 'running' : 'idle';
    const remainSec = action === 'start' ? (durationMin || 60) * 60 : 0;
    try {
      const rpcMethod = action === 'start' ? 'startMachine' : 'stopMachine';
      await sendThingsBoardRPC(machineId, rpcMethod, { minutes: durationMin || 60 });
      console.log(`[TB RPC] ${rpcMethod} sent to ${machineId}`);
    } catch (e) {
      console.error(`[TB RPC] ${action} error:`, e.message);
      // HiveMQ fallback
      if (mqttClient?.connected) {
        const sid = storeId || 'unknown';
        const topic = `laundry/${sid}/${machineId}/command`;
        mqttClient.publish(topic, JSON.stringify({ cmd: action, durationMin }), { qos: 1 });
      }
    }
    await db.query(`
      INSERT INTO machine_current_state (machine_id, state, remain_sec, progress, updated_at)
      VALUES ($1, $2, $3, 0, NOW())
      ON CONFLICT (machine_id) DO UPDATE SET state=$2, remain_sec=$3, progress=0, updated_at=NOW()
    `, [machineId, state, remainSec]);
    res.json({ success: true, state, remainSec });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ═══════════════════════════════════════
//  API: Machine State Update (from IoT / ThingsBoard webhook)
// ═══════════════════════════════════════
app.post('/api/machines/state/update', requireIotApiKey, async (req, res) => {
  const { storeId, machineId, status, remaining, mode, temperature, rpm, power, waterLevel, coinCount, coinTotal } = req.body;
  if (!machineId) return res.status(400).json({ error: 'machineId required' });
  try {
    await db.query(`
      INSERT INTO machine_current_state (store_id, machine_id, status, remaining, mode, temperature, rpm, power, water_level, coin_count, coin_total, updated_at)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,NOW())
      ON CONFLICT (store_id, machine_id) DO UPDATE SET
        status=EXCLUDED.status, remaining=EXCLUDED.remaining, mode=EXCLUDED.mode,
        temperature=EXCLUDED.temperature, rpm=EXCLUDED.rpm, power=EXCLUDED.power,
        water_level=EXCLUDED.water_level, coin_count=EXCLUDED.coin_count, coin_total=EXCLUDED.coin_total,
        updated_at=NOW()
    `, [storeId || '', machineId, status || 'unknown', remaining || 0, mode || '', temperature || 0, rpm || 0, power || 0, waterLevel || 0, coinCount || 0, coinTotal || 0]);
    // Also update legacy machine_id-only row for backward compatibility
    await db.query(`
      INSERT INTO machine_current_state (machine_id, state, remain_sec, progress, updated_at)
      VALUES ($1, $2, $3, 0, NOW())
      ON CONFLICT (machine_id) DO UPDATE SET state=$2, remain_sec=$3, updated_at=NOW()
    `, [machineId, status || 'unknown', remaining || 0]).catch(() => {});
    res.json({ success: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ═══════════════════════════════════════
//  API: ThingsBoard Webhook (machine_done notification)
// ═══════════════════════════════════════
const TB_WEBHOOK_TOKEN = process.env.TB_WEBHOOK_TOKEN || (() => { console.warn('[SECURITY] TB_WEBHOOK_TOKEN using fallback - set env var!'); return 'e002afa25100c2fa5033db4d839e777d400d701c932d138598a372cf5d492819'; })();

app.post('/api/thingsboard/webhook', async (req, res) => {
  const token = req.headers['x-tb-webhook-token'];
  if (token !== TB_WEBHOOK_TOKEN) {
    return res.status(401).json({ error: 'Invalid webhook token' });
  }

  const { event, device, store_id } = req.body;
  console.log(`[TB Webhook] event=${event} device=${device} store_id=${store_id}`);

  if (event === 'machine_done') {
    try {
      // Update machine state to idle
      await db.query(`
        UPDATE machine_current_state SET status='idle', remaining=0, updated_at=NOW()
        WHERE machine_id=$1
      `, [device]).catch(() => {});

      // Find active order for this machine and send LINE notification
      const orderRes = await db.query(`
        SELECT o.id, mb.line_user_id, o.store_id, o.machine_id, o.mode, s.name as store_name
        FROM orders o
        JOIN members mb ON o.member_id = mb.id
        LEFT JOIN stores s ON o.store_id = s.id
        WHERE o.machine_id=$1 AND o.status IN ('paid','running')
        ORDER BY o.created_at DESC LIMIT 1
      `, [device]);

      if (orderRes.rows.length > 0) {
        const order = orderRes.rows[0];
        // Mark order completed
        await db.query(`UPDATE orders SET status='completed', completed_at=NOW() WHERE id=$1`, [order.id]);

        // Send LINE push notification if user has line_user_id
        if (order.line_user_id && process.env.LINE_CHANNEL_ACCESS_TOKEN) {
          const storeName = order.store_name || store_id;
          const machineLabel = device.replace(/-/g, ' ').toUpperCase();

          try {
            await fetch('https://api.line.me/v2/bot/message/push', {
              method: 'POST',
              headers: {
                'Content-Type': 'application/json',
                'Authorization': `Bearer ${process.env.LINE_CHANNEL_ACCESS_TOKEN}`,
              },
              body: JSON.stringify({
                to: order.line_user_id,
                messages: [{
                  type: 'text',
                  text: `✅ 您的洗衣已完成！\n📍 ${storeName}\n🔧 ${machineLabel}\n請盡快取回衣物 🧺`,
                }],
              }),
            });
            console.log(`[TB Webhook] LINE notification sent to ${order.line_user_id}`);
          } catch (lineErr) {
            console.error('[TB Webhook] LINE push error:', lineErr.message);
          }
        }
      }

      res.json({ success: true, event, device });
    } catch (e) {
      console.error('[TB Webhook] Error:', e.message);
      res.status(500).json({ error: e.message });
    }
  } else {
    res.json({ success: true, event, message: 'event received' });
  }
});

// GET /api/tb/diag — ThingsBoard connectivity diagnostic
app.get('/api/tb/diag', async (req, res) => {
  const steps = { tb_base: TB_BASE };
  try {
    // Step 1: Login
    const loginRes = await axios.post(`${TB_BASE}/api/auth/login`, {
      username: process.env.TB_USERNAME || (() => { console.warn('[SECURITY] TB_USERNAME using fallback - set env var!'); return 'sl6963693171@gmail.com'; })(),
      password: process.env.TB_PASSWORD || (() => { console.warn('[SECURITY] TB_PASSWORD using fallback - set env var!'); return 's85010805'; })(),
    }, { timeout: 10000 });
    steps.login = { status: loginRes.status, hasToken: !!loginRes.data?.token };
    const token = loginRes.data.token;

    // Step 2: Device lookup
    const devRes = await axios.get(`${TB_BASE}/api/tenant/devices?pageSize=1&page=0&textSearch=s1-m1&sortProperty=name&sortOrder=ASC`, {
      headers: tbHeaders(token), timeout: 10000,
    });
    const deviceId = devRes.data?.data?.[0]?.id?.id;
    steps.device_lookup = { status: devRes.status, deviceId, found: !!deviceId };

    // Step 3: Telemetry query
    if (deviceId) {
      const telRes = await axios.get(`${TB_BASE}/api/plugins/telemetry/DEVICE/${deviceId}/values/timeseries?keys=state`, {
        headers: tbHeaders(token), timeout: 10000,
      });
      steps.telemetry = { status: telRes.status, data: telRes.data };
    }

    steps.overall = 'OK';
    res.json(steps);
  } catch (e) {
    steps.error = {
      step: steps.login ? (steps.device_lookup ? 'telemetry' : 'device_lookup') : 'login',
      status: e.response?.status,
      data: e.response?.data,
      message: e.message,
    };
    res.status(500).json(steps);
  }
});

// ═══════════════════════════════════════
//  API: ThingsBoard Telemetry Query (Path B)
// ═══════════════════════════════════════

// GET /api/tb/telemetry/:deviceName — 查詢單台機器最新遙測
app.get('/api/tb/telemetry/:deviceName', async (req, res) => {
  try {
    const { deviceName } = req.params;
    const keys = req.query.keys || 'state,door,remain_sec,temperature,rpm,wash_program,dry_program,current_step,required_coins,current_coins,fault,warning';
    const deviceId = await getTBDeviceId(deviceName);
    if (!deviceId) return res.status(404).json({ error: `Device ${deviceName} not found` });

    const data = await tbGet(`/api/plugins/telemetry/DEVICE/${deviceId}/values/timeseries?keys=${keys}`);

    // Flatten: { "state": [{"ts":..,"value":"idle"}] } → { "state": "idle", ... }
    const flat = {};
    for (const [k, arr] of Object.entries(data)) {
      if (arr && arr.length > 0) {
        let v = arr[0].value;
        if (v === 'true') v = true;
        else if (v === 'false') v = false;
        else if (!isNaN(v) && v !== '') v = Number(v);
        flat[k] = v;
        flat[k + '_ts'] = arr[0].ts;
      }
    }
    flat.device = deviceName;
    flat.source = 'thingsboard';
    res.json(flat);
  } catch (e) {
    console.error('[TB Telemetry]', e.message);
    res.status(500).json({ error: e.message });
  }
});

// GET /api/tb/telemetry/store/:storeId — 查詢整店所有機器最新遙測
app.get('/api/tb/telemetry/store/:storeId', async (req, res) => {
  try {
    const { storeId } = req.params;
    const keys = 'state,door,remain_sec,temperature,rpm,wash_program,dry_program,current_step,fault,warning';

    // Get all machines for this store from DB
    const machinesRes = await db.query(
      'SELECT id, name, size FROM machines WHERE store_id=$1 AND active=true ORDER BY sort_order, name',
      [storeId]
    );

    const results = [];
    for (const machine of machinesRes.rows) {
      const deviceName = machine.id;  // e.g. "s1-m1"
      try {
        const deviceId = await getTBDeviceId(deviceName);
        if (!deviceId) {
          results.push({ device: deviceName, name: machine.name, source: 'thingsboard', error: 'not_found' });
          continue;
        }
        const data = await tbGet(`/api/plugins/telemetry/DEVICE/${deviceId}/values/timeseries?keys=${keys}`);
        const flat = { device: deviceName, name: machine.name, size: machine.size, source: 'thingsboard' };
        for (const [k, arr] of Object.entries(data)) {
          if (arr && arr.length > 0) {
            let v = arr[0].value;
            if (v === 'true') v = true;
            else if (v === 'false') v = false;
            else if (!isNaN(v) && v !== '') v = Number(v);
            flat[k] = v;
            flat[k + '_ts'] = arr[0].ts;
          }
        }
        results.push(flat);
      } catch (innerErr) {
        results.push({ device: deviceName, name: machine.name, source: 'thingsboard', error: innerErr.message });
      }
    }

    res.json({ store_id: storeId, machines: results, source: 'thingsboard', timestamp: new Date().toISOString() });
  } catch (e) {
    console.error('[TB Store Telemetry]', e.message);
    res.status(500).json({ error: e.message });
  }
});

// GET /api/tb/history/:deviceName — 查詢歷史遙測（時間序列）
app.get('/api/tb/history/:deviceName', async (req, res) => {
  try {
    const { deviceName } = req.params;
    const keys = req.query.keys || 'state,temperature,rpm';
    const startTs = req.query.startTs || (Date.now() - 24 * 3600 * 1000); // default: last 24h
    const endTs = req.query.endTs || Date.now();
    const limit = Math.min(parseInt(req.query.limit) || 100, 1000);
    const interval = req.query.interval || 0; // 0 = no aggregation
    const agg = req.query.agg || 'NONE'; // NONE, AVG, MIN, MAX, COUNT, SUM

    const deviceId = await getTBDeviceId(deviceName);
    if (!deviceId) return res.status(404).json({ error: `Device ${deviceName} not found` });

    let path = `/api/plugins/telemetry/DEVICE/${deviceId}/values/timeseries?keys=${keys}&startTs=${startTs}&endTs=${endTs}&limit=${limit}`;
    if (interval > 0 && agg !== 'NONE') {
      path += `&interval=${interval}&agg=${agg}`;
    }

    const data = await tbGet(path);

    res.json({ device: deviceName, source: 'thingsboard', startTs: Number(startTs), endTs: Number(endTs), data });
  } catch (e) {
    console.error('[TB History]', e.message);
    res.status(500).json({ error: e.message });
  }
});

// GET /api/tb/attributes/:deviceName — 查詢設備屬性（shared + client）
app.get('/api/tb/attributes/:deviceName', async (req, res) => {
  try {
    const { deviceName } = req.params;
    const deviceId = await getTBDeviceId(deviceName);
    if (!deviceId) return res.status(404).json({ error: `Device ${deviceName} not found` });

    // Fetch both shared and client attributes in parallel
    const [shared, client] = await Promise.all([
      tbGet(`/api/plugins/telemetry/DEVICE/${deviceId}/values/attributes/SHARED_SCOPE`).catch(() => []),
      tbGet(`/api/plugins/telemetry/DEVICE/${deviceId}/values/attributes/CLIENT_SCOPE`).catch(() => []),
    ]);

    // Convert array format to object: [{key, value}] → {key: value}
    const sharedObj = {};
    for (const item of shared) sharedObj[item.key] = item.value;
    const clientObj = {};
    for (const item of client) clientObj[item.key] = item.value;

    res.json({ device: deviceName, source: 'thingsboard', shared: sharedObj, client: clientObj });
  } catch (e) {
    console.error('[TB Attributes]', e.message);
    res.status(500).json({ error: e.message });
  }
});

// GET /api/tb/config/:storeId — 查詢觸控屏程式配置（價格、洗程、時間）
// 從 TB shared attributes 讀取，工控機每 5 分鐘自動更新
app.get('/api/tb/config/:storeId', async (req, res) => {
  try {
    const { storeId } = req.params;
    // Read config from first machine of this store (all machines share same config)
    const deviceName = `${storeId}-m1`;
    const deviceId = await getTBDeviceId(deviceName);
    if (!deviceId) return res.status(404).json({ error: `Device ${deviceName} not found` });

    const attrs = await tbGet(`/api/plugins/telemetry/DEVICE/${deviceId}/values/attributes/SHARED_SCOPE`);

    // Convert array [{key, value}] to object
    const config = {};
    for (const item of attrs) config[item.key] = item.value;

    // Build structured program list for frontend
    const programs = [];
    for (let i = 1; i <= (config.num_programs || 4); i++) {
      programs.push({
        program: i,
        coins: config[`prog${i}_coins`] || config[`P${i}_coins`] || 0,
        price_nt: config[`prog${i}_price_nt`] || config[`P${i}_price_nt`] || 0,
        total_min: config[`prog${i}_total_min`] || 0,
        wash_program: config[`prog${i}_wash`] || i,
        dry_program: config[`prog${i}_dry`] || 0,
      });
    }

    res.json({
      store_id: storeId,
      source: 'thingsboard',
      config_updated_at: config.config_updated_at || null,
      num_programs: config.num_programs || 4,
      coin_value_nt: config.coin_value_nt || 10,
      currency: config.currency || 3,
      payment_mode: config.payment_mode || 1,
      programs,
      raw: config,
    });
  } catch (e) {
    console.error('[TB Config]', e.message);
    res.status(500).json({ error: e.message });
  }
});

// GET /api/store/modes/:storeId — 取得店家洗衣模式（TB 動態 + 預設 fallback）
app.get('/api/store/modes/:storeId', async (req, res) => {
  try {
    const { storeId } = req.params;

    // Default modes (fallback)
    const DEFAULT_MODES = [
      { id: 'standard', name: '洗衣+烘衣(標準)', price: 170, minutes: 72, color: '#FF3B30', desc: '標準洗+烘衣', dryMin: 35, wash_program: 1 },
      { id: 'small',    name: '洗衣+烘衣(少量)', price: 150, minutes: 58, color: '#FF6B6B', desc: '少量洗+烘衣', dryMin: 25, wash_program: 2 },
      { id: 'washonly', name: '洗衣',            price: 120, minutes: 34, color: '#007AFF', desc: '純洗不烘',     dryMin: 0,  wash_program: 3 },
      { id: 'dryonly',  name: '烘衣',            price: 10,  minutes: 5,  color: '#FFCC00', desc: '純烘乾',       dryMin: 5,  wash_program: 0 },
    ];

    const MODE_META = {
      1: { id: 'standard', color: '#FF3B30', desc: '標準洗+烘衣' },
      2: { id: 'small',    color: '#FF6B6B', desc: '少量洗+烘衣' },
      3: { id: 'washonly', color: '#007AFF', desc: '純洗不烘' },
      0: { id: 'dryonly',  color: '#FFCC00', desc: '純烘乾' },
    };

    // Try TB config first
    try {
      const deviceName = `${storeId}-m1`;
      const deviceId = await getTBDeviceId(deviceName);
      if (deviceId) {
        const attrs = await tbGet(`/api/plugins/telemetry/DEVICE/${deviceId}/values/attributes/SHARED_SCOPE`);
        const config = {};
        for (const item of attrs) config[item.key] = item.value;

        if (config.num_programs) {
          const modes = [];
          for (let i = 1; i <= config.num_programs; i++) {
            const washProg = Number(config[`prog${i}_wash`]) || i;
            const dryProg = Number(config[`prog${i}_dry`]) || 0;
            const totalMin = Number(config[`prog${i}_total_min`]) || 0;
            const priceNt = Number(config[`prog${i}_price_nt`]) || Number(config[`P${i}_price_nt`]) || 0;
            const coins = Number(config[`prog${i}_coins`]) || Number(config[`P${i}_coins`]) || 0;
            const coinValue = Number(config.coin_value_nt) || 10;
            const price = priceNt || (coins * coinValue);

            // Determine name from wash/dry program combo
            const hasDry = dryProg > 0 || (config[`prog${i}_dry_min`] && Number(config[`prog${i}_dry_min`]) > 0);
            const dryMin = Number(config[`prog${i}_dry_min`]) || 0;
            const washMin = totalMin - dryMin;

            let name, desc, id, color;
            if (washProg === 0 && hasDry) {
              name = '烘衣'; desc = '純烘乾'; id = `dryonly_${i}`; color = '#FFCC00';
            } else if (washProg > 0 && !hasDry) {
              name = '洗衣'; desc = '純洗不烘'; id = `washonly_${i}`; color = '#007AFF';
            } else {
              name = config[`prog${i}_name`] || DEFAULT_MODES[i-1]?.name || `洗衣+烘衣 (P${i})`;
              desc = config[`prog${i}_desc`] || DEFAULT_MODES[i-1]?.desc || `程式 ${i}`;
              id = DEFAULT_MODES[i-1]?.id || `prog${i}`;
              color = DEFAULT_MODES[i-1]?.color || '#FF3B30';
            }

            modes.push({
              id,
              name: config[`prog${i}_name`] || name,
              price: price || DEFAULT_MODES[i-1]?.price || 0,
              minutes: totalMin || DEFAULT_MODES[i-1]?.minutes || 0,
              color,
              desc: config[`prog${i}_desc`] || desc,
              dryMin: dryMin || DEFAULT_MODES[i-1]?.dryMin || 0,
              wash_program: washProg,
            });
          }

          return res.json({
            store_id: storeId,
            source: 'thingsboard',
            config_updated_at: config.config_updated_at || null,
            modes,
          });
        }
      }
    } catch (tbErr) {
      console.warn(`[Store Modes] TB unavailable for ${storeId}:`, tbErr.message);
    }

    // Fallback: check if store has custom modes in DB
    try {
      const dbModes = await db.query(
        'SELECT * FROM store_modes WHERE store_id=$1 ORDER BY sort_order',
        [storeId]
      );
      if (dbModes.rows.length > 0) {
        return res.json({
          store_id: storeId,
          source: 'database',
          modes: dbModes.rows.map(r => ({
            id: r.mode_id, name: r.name, price: r.price, minutes: r.minutes,
            color: r.color, desc: r.description, dryMin: r.dry_min || 0, wash_program: r.wash_program || 0,
          })),
        });
      }
    } catch (dbErr) {
      // store_modes table might not exist, that's OK
    }

    // Final fallback: default modes
    res.json({
      store_id: storeId,
      source: 'default',
      modes: DEFAULT_MODES,
    });
  } catch (e) {
    console.error('[Store Modes]', e.message);
    res.status(500).json({ error: e.message });
  }
});

// GET /api/monitor/store/:storeId — 機器監控面板（需 monitor 權限）
app.get('/api/monitor/store/:storeId', async (req, res) => {
  try {
    const userId = req.query.userId;
    if (!userId) return res.status(400).json({ error: 'userId required' });

    // Permission check
    const { role, permissions } = await getUserRole(userId);
    if (role !== 'super_admin' && !permissions.monitor) {
      return res.status(403).json({ error: '無機器監控權限' });
    }

    const { storeId } = req.params;
    const keys = 'state,door,remain_sec,temperature,rpm,wash_program,dry_program,current_step,required_coins,current_coins,fault,warning';

    // Get machines from DB
    const machinesRes = await db.query(
      'SELECT id, name, size, store_id FROM machines WHERE store_id=$1 AND active=true ORDER BY sort_order, name',
      [storeId]
    );

    const results = [];
    let tbAvailable = true;

    for (const machine of machinesRes.rows) {
      const deviceName = machine.id;
      const entry = {
        device: deviceName, name: machine.name, size: machine.size,
        store_id: machine.store_id, source: 'database'
      };

      // Try TB telemetry
      try {
        const deviceId = await getTBDeviceId(deviceName);
        if (deviceId && tbAvailable) {
          const data = await tbGet(`/api/plugins/telemetry/DEVICE/${deviceId}/values/timeseries?keys=${keys}`);
          entry.source = 'thingsboard';
          for (const [k, arr] of Object.entries(data)) {
            if (arr && arr.length > 0) {
              let v = arr[0].value;
              if (v === 'true') v = true;
              else if (v === 'false') v = false;
              else if (!isNaN(v) && v !== '') v = Number(v);
              entry[k] = v;
              entry[k + '_ts'] = arr[0].ts;
            }
          }
        }
      } catch (tbErr) {
        tbAvailable = false;
        console.warn(`[Monitor] TB unavailable for ${deviceName}:`, tbErr.message);
      }

      // Fallback: get state from DB (machine_current_state table)
      if (entry.source === 'database') {
        try {
          const stateRes = await db.query(
            'SELECT state, remain_sec, updated_at FROM machine_current_state WHERE machine_id=$1',
            [deviceName]
          );
          if (stateRes.rows.length > 0) {
            entry.state = stateRes.rows[0].state;
            entry.remain_sec = stateRes.rows[0].remain_sec;
            entry.state_ts = new Date(stateRes.rows[0].updated_at).getTime();
          }
        } catch (dbErr) {
          // ignore
        }
      }

      results.push(entry);
    }

    res.json({
      store_id: storeId,
      machines: results,
      tb_available: tbAvailable,
      timestamp: new Date().toISOString()
    });
  } catch (e) {
    console.error('[Monitor]', e.message);
    res.status(500).json({ error: e.message });
  }
});

// GET /api/monitor/overview — 全店監控總覽（需 monitor 權限）
app.get('/api/monitor/overview', async (req, res) => {
  try {
    const userId = req.query.userId;
    if (!userId) return res.status(400).json({ error: 'userId required' });

    const { role, permissions } = await getUserRole(userId);
    if (role !== 'super_admin' && !permissions.monitor) {
      return res.status(403).json({ error: '無機器監控權限' });
    }

    // Get all stores
    const storesRes = await db.query('SELECT id, name FROM stores ORDER BY id');
    const stores = [];

    for (const store of storesRes.rows) {
      const machinesRes = await db.query(
        `SELECT m.id, m.name,
          COALESCE(cs.state, 'idle') as state,
          COALESCE(cs.remain_sec, 0) as remain_sec
        FROM machines m
        LEFT JOIN machine_current_state cs ON m.id = cs.machine_id
        WHERE m.store_id=$1 AND m.active=true
        ORDER BY m.sort_order, m.name`,
        [store.id]
      );

      const running = machinesRes.rows.filter(m => m.state === 'running').length;
      const idle = machinesRes.rows.filter(m => m.state === 'idle' || m.state === 'done' || m.state === 'unknown').length;
      const fault = machinesRes.rows.filter(m => m.state === 'fault').length;

      stores.push({
        store_id: store.id,
        store_name: store.name,
        total: machinesRes.rows.length,
        running,
        idle,
        fault,
        machines: machinesRes.rows.map(m => ({
          id: m.id, name: m.name, state: m.state, remain_sec: parseInt(m.remain_sec) || 0
        }))
      });
    }

    res.json({ stores, timestamp: new Date().toISOString() });
  } catch (e) {
    console.error('[Monitor Overview]', e.message);
    res.status(500).json({ error: e.message });
  }
});

// GET /api/machines/:storeId/live — 混合查詢：DB + ThingsBoard（最佳數據源）
app.get('/api/machines/:storeId/live', async (req, res) => {
  try {
    const { storeId } = req.params;

    // 1. Get DB state (always available)
    const dbRes = await db.query(`
      SELECT m.id, m.name, m.size,
      CASE
        WHEN cs.state = 'running' AND cs.remain_sec - EXTRACT(EPOCH FROM (NOW() - cs.updated_at))::int <= 0 THEN 'idle'
        ELSE COALESCE(cs.state,'unknown')
      END as state,
      CASE
        WHEN cs.state = 'running' THEN GREATEST(0, cs.remain_sec - EXTRACT(EPOCH FROM (NOW() - cs.updated_at))::int)
        ELSE 0
      END as remain_sec,
      cs.temperature, cs.rpm, cs.mode,
      cs.updated_at as last_seen
      FROM machines m LEFT JOIN machine_current_state cs ON m.id=cs.machine_id
      WHERE m.store_id=$1 AND m.active=true ORDER BY m.sort_order, m.name
    `, [storeId]);

    const machines = dbRes.rows;

    // 2. Try to enrich with ThingsBoard telemetry (best-effort)
    try {
      const tbKeys = 'state,door,remain_sec,temperature,rpm,fault,warning';

      await Promise.all(machines.map(async (machine) => {
        try {
          const deviceId = await getTBDeviceId(machine.id);
          if (!deviceId) return;
          const data = await tbGet(`/api/plugins/telemetry/DEVICE/${deviceId}/values/timeseries?keys=${tbKeys}`);

          // Only use TB data if it's fresher than DB data
          const tbTs = data.state?.[0]?.ts || 0;
          const dbTs = machine.last_seen ? new Date(machine.last_seen).getTime() : 0;

          if (tbTs > dbTs) {
            machine.state = data.state?.[0]?.value || machine.state;
            machine.remain_sec = Number(data.remain_sec?.[0]?.value) || machine.remain_sec;
            machine.temperature = Number(data.temperature?.[0]?.value) || machine.temperature;
            machine.rpm = Number(data.rpm?.[0]?.value) || machine.rpm;
            machine.door = data.door?.[0]?.value || 'unknown';
            machine.fault = data.fault?.[0]?.value === 'true';
            machine.warning = data.warning?.[0]?.value === 'true';
            machine.source = 'thingsboard';
            machine.tb_ts = tbTs;
          } else {
            machine.source = 'database';
          }
        } catch (innerErr) {
          machine.source = 'database';
        }
      }));
    } catch (tbErr) {
      // TB unavailable, all machines use DB data
      console.warn('[Live] TB unavailable, using DB only:', tbErr.message);
      machines.forEach(m => { m.source = 'database'; });
    }

    res.json(machines);
  } catch (e) {
    console.error('[Live Machines]', e.message);
    res.status(500).json({ error: e.message });
  }
});

// ═══════════════════════════════════════
//  Machine Management CRUD
// ═══════════════════════════════════════

// Machine type definitions
const MACHINE_TYPES = [
  { id: 'combo', name: '洗脫烘一體機', description: '單機洗+脫+烘', protocol: 'sx176005a' },
  { id: 'dual_dryer', name: '雙烘機', description: '兩個烘乾單元', protocol: 'sx176005a' },
  { id: 'stack', name: '上烘下洗機', description: '上層烘乾、下層洗衣', protocol: 'sx176005a' },
  { id: 'washer', name: '單洗機', description: '純洗衣', protocol: 'sx176005a' },
  { id: 'dryer', name: '單烘機', description: '純烘乾', protocol: 'sx176005a' },
];

// GET /api/admin/machine-types
app.get('/api/admin/machine-types', (req, res) => {
  res.json(MACHINE_TYPES);
});

// POST /api/admin/machines
app.post('/api/admin/machines', async (req, res) => {
  try {
    const { userId, storeId, name, size, machineType, protocol, modbusSlave, tbDevice, description } = req.body;
    if (!userId) return res.status(400).json({ error: 'userId required' });
    const { role } = await getUserRole(userId);
    if (role !== 'super_admin') return res.status(403).json({ error: '權限不足' });
    if (!storeId || !name) return res.status(400).json({ error: 'storeId and name required' });

    // Auto-generate machine ID
    const existing = await db.query('SELECT id FROM machines WHERE store_id=$1 ORDER BY id', [storeId]);
    const nextNum = existing.rows.length + 1;
    let machineId = `${storeId}-m${nextNum}`;

    // Find next available ID if collision
    let n = nextNum;
    while (true) {
      const check = await db.query('SELECT id FROM machines WHERE id=$1', [machineId]);
      if (check.rows.length === 0) break;
      n++;
      machineId = `${storeId}-m${n}`;
    }

    res.json(await createMachine(machineId, storeId, name, size, machineType, protocol, modbusSlave, tbDevice, description, n));
  } catch (e) {
    console.error('[Machine Create]', e.message);
    res.status(500).json({ error: e.message });
  }
});

async function createMachine(machineId, storeId, name, size, machineType, protocol, modbusSlave, tbDevice, description, sortOrder) {
  const result = await db.query(
    `INSERT INTO machines (id, store_id, name, size, machine_type, protocol, modbus_slave, tb_device, description, sort_order, active)
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, true)
     RETURNING *`,
    [machineId, storeId, name, size || '中型', machineType || 'combo', protocol || 'sx176005a', modbusSlave || 0, tbDevice || machineId, description || '', sortOrder]
  );
  return result.rows[0];
}

// PUT /api/admin/machines/:machineId
app.put('/api/admin/machines/:machineId', async (req, res) => {
  try {
    const { userId, name, size, machineType, protocol, modbusSlave, tbDevice, description, active, sortOrder } = req.body;
    if (!userId) return res.status(400).json({ error: 'userId required' });
    const { role } = await getUserRole(userId);
    if (role !== 'super_admin') return res.status(403).json({ error: '權限不足' });

    const { machineId } = req.params;
    const result = await db.query(
      `UPDATE machines SET
        name = COALESCE($1, name),
        size = COALESCE($2, size),
        machine_type = COALESCE($3, machine_type),
        protocol = COALESCE($4, protocol),
        modbus_slave = COALESCE($5, modbus_slave),
        tb_device = COALESCE($6, tb_device),
        description = COALESCE($7, description),
        active = COALESCE($8, active),
        sort_order = COALESCE($9, sort_order)
      WHERE id = $10 RETURNING *`,
      [name, size, machineType, protocol, modbusSlave, tbDevice, description, active, sortOrder, machineId]
    );

    if (result.rows.length === 0) return res.status(404).json({ error: 'Machine not found' });
    res.json(result.rows[0]);
  } catch (e) {
    console.error('[Machine Update]', e.message);
    res.status(500).json({ error: e.message });
  }
});

// DELETE /api/admin/machines/:machineId (soft delete)
app.delete('/api/admin/machines/:machineId', async (req, res) => {
  try {
    const userId = req.query.userId;
    if (!userId) return res.status(400).json({ error: 'userId required' });
    const { role } = await getUserRole(userId);
    if (role !== 'super_admin') return res.status(403).json({ error: '權限不足' });

    const { machineId } = req.params;
    const result = await db.query(
      'UPDATE machines SET active = false WHERE id = $1 RETURNING id, name',
      [machineId]
    );

    if (result.rows.length === 0) return res.status(404).json({ error: 'Machine not found' });
    res.json({ success: true, message: `已停用 ${result.rows[0].name}`, machine: result.rows[0] });
  } catch (e) {
    console.error('[Machine Delete]', e.message);
    res.status(500).json({ error: e.message });
  }
});

// GET /api/admin/machines/all
app.get('/api/admin/machines/all', async (req, res) => {
  try {
    const userId = req.query.userId;
    if (!userId) return res.status(400).json({ error: 'userId required' });
    const { role } = await getUserRole(userId);
    if (role !== 'super_admin') return res.status(403).json({ error: '權限不足' });

    const storeId = req.query.storeId;
    let query = `SELECT m.*, s.name as store_name FROM machines m
                 LEFT JOIN stores s ON m.store_id = s.id`;
    const params = [];
    if (storeId) {
      query += ' WHERE m.store_id = $1';
      params.push(storeId);
    }
    query += ' ORDER BY m.store_id, m.sort_order, m.id';

    const result = await db.query(query, params);
    res.json(result.rows);
  } catch (e) {
    console.error('[Machine List]', e.message);
    res.status(500).json({ error: e.message });
  }
});

// ═══════════════════════════════════════
//  API: Machine Notify (queue notification)
// ═══════════════════════════════════════
app.post('/api/machine/notify', requireIotApiKey, async (req, res) => {
  try {
    const { machineId, storeId, remaining, status } = req.body;
    const resolvedStoreId = storeId || machineId?.split('-')[0];
    // Find store name and group support URL
    const storeRes = await db.query('SELECT s.name, s.group_id FROM stores s WHERE s.id=$1', [resolvedStoreId]);
    const storeName = storeRes.rows[0]?.name || resolvedStoreId;
    const storeGroupId = storeRes.rows[0]?.group_id;
    let supportUrl = '';
    let supportPhone = '';
    if (storeGroupId) {
      const sgRes = await db.query('SELECT support_url, support_phone FROM store_groups WHERE id=$1', [storeGroupId]);
      supportUrl = sgRes.rows[0]?.support_url || '';
      supportPhone = sgRes.rows[0]?.support_phone || '';
    }
    const machineNum = machineId?.includes('-d') ? `烘乾${machineId.split('-d')[1]}號` : `洗脫烘${machineId.split('-m')[1]}號`;

    console.log(`[NOTIFY] ${storeName} ${machineNum} remaining: ${remaining}s status: ${status}`);

    // Update machine_current_state so frontend reflects actual status
    const dbState = (status === 'done' || parseInt(remaining) <= 0) ? 'idle' : (status || 'running');
    const dbRemain = (status === 'done' || parseInt(remaining) <= 0) ? 0 : (parseInt(remaining) || 0);
    await db.query(`
      INSERT INTO machine_current_state (machine_id, state, remain_sec, progress, updated_at)
      VALUES ($1, $2, $3, 0, NOW())
      ON CONFLICT (machine_id) DO UPDATE SET state=$2, remain_sec=$3, progress=0, updated_at=NOW()
    `, [machineId, dbState, dbRemain]).catch(e => console.error('[NOTIFY] DB state update error:', e.message));

    // Find users who have recent orders (last 2 hours) at this store for this machine
    const recentUsers = await db.query(
      `SELECT DISTINCT mb.line_user_id
       FROM orders o
       JOIN members mb ON o.member_id = mb.id
       WHERE o.machine_id = $1
         AND o.status IN ('paid', 'running')
         AND o.created_at > NOW() - INTERVAL '2 hours'`,
      [machineId]
    );

    // Fallback: if no recent users found, push to super_admin for testing
    let targetUsers = recentUsers.rows;
    let fallbackUsed = false;
    if (targetUsers.length === 0) {
      const SUPER_ADMIN_ID = process.env.SUPER_ADMIN_LINE_ID || 'Ubdcdd269e115bf9ac492288adbc0115e';
      targetUsers = [{ line_user_id: SUPER_ADMIN_ID }];
      fallbackUsed = true;
      console.log(`[NOTIFY] No recent users found, fallback to super_admin: ${SUPER_ADMIN_ID}`);
    }

    let pushedCount = 0;
    const remainMin = Math.ceil((parseInt(remaining) || 0) / 60);

    if (status === 'done' || parseInt(remaining) <= 0) {
      for (const u of targetUsers) {
        const ok = await sendLineFlexMessage(u.line_user_id, '洗衣完成通知', buildCompleteFlexMessage(storeName, machineNum, supportUrl, supportPhone, storeGroupId, resolvedStoreId), { pushType: 'auto_complete', storeId: resolvedStoreId, description: `洗衣完成 ${storeName} ${machineNum}${fallbackUsed ? ' (fallback)' : ''}` });
        if (ok) pushedCount++;
      }
    } else if (remainMin > 0 && remainMin <= 5) {
      for (const u of targetUsers) {
        const ok = await sendLineFlexMessage(u.line_user_id, '即將完成提醒', buildAlmostDoneFlexMessage(storeName, machineNum, remainMin, supportUrl, supportPhone, storeGroupId, resolvedStoreId), { pushType: 'auto_reminder', storeId: resolvedStoreId, description: `即將完成提醒 ${storeName} ${machineNum} 剩${remainMin}分${fallbackUsed ? ' (fallback)' : ''}` });
        if (ok) pushedCount++;
      }
    }

    res.json({ success: true, message: `通知已記錄: ${storeName} ${machineNum}`, pushedCount, usersFound: recentUsers.rows.length, fallbackUsed });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ═══════════════════════════════════════
//  API: Admin Coupons CRUD
// ═══════════════════════════════════════
app.get('/api/admin/coupons', async (req, res) => {
  try {
    const { userId, groupId } = req.query;
    const roleInfo = await getUserRole(userId);
    if (roleInfo.role !== 'super_admin' && roleInfo.role !== 'store_admin') return res.status(403).json({ error: 'forbidden' });
    const scopeGroupId = roleInfo.role === 'store_admin' ? roleInfo.groupId : groupId;
    let query = 'SELECT c.*, sg.name as group_name FROM admin_coupons c LEFT JOIN store_groups sg ON c.group_id = sg.id WHERE 1=1';
    const params = [];
    if (scopeGroupId) { query += ' AND c.group_id=$1'; params.push(scopeGroupId); }
    query += ' ORDER BY c.created_at DESC';
    const r = await db.query(query, params);
    res.json(r.rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/admin/coupons', async (req, res) => {
  try {
    const { userId, groupId, name, discount, type, minSpend, price, expiry, description, timeSlot, deviceLimit, validity, storeScope, maxUses, category, autoDistribute, notes, refundPolicy, redeemCode } = req.body;
    const roleInfo = await getUserRole(userId);
    if (roleInfo.role !== 'super_admin' && roleInfo.role !== 'store_admin') return res.status(403).json({ error: 'forbidden' });
    const couponRes = await db.query(`
      INSERT INTO admin_coupons (group_id, name, discount, type, min_spend, price, expiry, active, description, time_slot, device_limit, validity, store_scope, max_uses, category, notes, refund_policy, redeem_code, created_at)
      VALUES ($1,$2,$3,$4,$5,$6,$7,true,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,NOW()) RETURNING id
    `, [groupId||null, name, discount, type||'fixed', minSpend||0, price||0, expiry||'2026-12-31',
        description||'', timeSlot||'全時段可用', deviceLimit||'全機型適用', validity||'自購買起90日內有效',
        storeScope||'全台門市', maxUses||0, category||'gift', notes||'', refundPolicy||'', redeemCode||'']);
    const couponId = couponRes.rows[0].id;
    // Auto-distribute to all members of the group
    if (autoDistribute) {
      const effectiveGroupId = groupId || null;
      let membersQuery;
      let membersParams;
      if (effectiveGroupId) {
        membersQuery = 'SELECT DISTINCT line_user_id FROM wallets WHERE group_id = $1';
        membersParams = [effectiveGroupId];
      } else {
        membersQuery = 'SELECT DISTINCT line_user_id FROM members';
        membersParams = [];
      }
      const members = await db.query(membersQuery, membersParams);
      let expiryDate = expiry || '2026-12-31';
      const validityMatch = (validity || '').match(/(\d+)日/);
      if (validityMatch) {
        expiryDate = new Date(Date.now() + parseInt(validityMatch[1]) * 86400000).toISOString().split('T')[0];
      }
      for (const m of members.rows) {
        await db.query(`
          INSERT INTO user_coupons (line_user_id, coupon_id, group_id, uses_remaining, expiry_date, status)
          VALUES ($1, $2, $3, $4, $5, 'active')
        `, [m.line_user_id, couponId, effectiveGroupId, maxUses || 1, expiryDate]);
      }
      res.json({ success: true, distributed: members.rows.length });
    } else {
      res.json({ success: true });
    }
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.put('/api/admin/coupons/:id', async (req, res) => {
  try {
    const { userId, name, discount, type, minSpend, expiry, active, price, description, timeSlot, deviceLimit, validity, storeScope, maxUses, category, groupId, notes, refundPolicy, redeemCode } = req.body;
    const roleInfo = await getUserRole(userId);
    if (roleInfo.role !== 'super_admin' && roleInfo.role !== 'store_admin') return res.status(403).json({ error: 'forbidden' });
    await db.query(`
      UPDATE admin_coupons SET name=COALESCE($2,name), discount=COALESCE($3,discount), type=COALESCE($4,type),
      min_spend=COALESCE($5,min_spend), expiry=COALESCE($6,expiry), active=COALESCE($7,active),
      price=COALESCE($8,price), description=COALESCE($9,description), time_slot=COALESCE($10,time_slot),
      device_limit=COALESCE($11,device_limit), validity=COALESCE($12,validity), store_scope=COALESCE($13,store_scope),
      max_uses=COALESCE($14,max_uses), category=COALESCE($15,category), group_id=COALESCE($16,group_id),
      notes=COALESCE($17,notes), refund_policy=COALESCE($18,refund_policy), redeem_code=COALESCE($19,redeem_code)
      WHERE id=$1
    `, [req.params.id, name, discount, type, minSpend, expiry, active, price, description, timeSlot, deviceLimit, validity, storeScope, maxUses, category, groupId, notes, refundPolicy, redeemCode]);
    res.json({ success: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.delete('/api/admin/coupons/:id', async (req, res) => {
  try {
    const { userId } = req.query;
    const roleInfo = await getUserRole(userId);
    if (roleInfo.role !== 'super_admin' && roleInfo.role !== 'store_admin') return res.status(403).json({ error: 'forbidden' });
    await db.query('DELETE FROM admin_coupons WHERE id=$1', [req.params.id]);
    res.json({ success: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ========== Announcements API ==========

// Public: get published announcements
app.get('/api/announcements', async (req, res) => {
  try {
    const { groupId } = req.query;
    let query = 'SELECT * FROM announcements WHERE published = true';
    const params = [];
    if (groupId) {
      params.push(groupId);
      query += ` AND (group_id = $${params.length} OR group_id IS NULL)`;
    }
    query += ' ORDER BY sort_order ASC, created_at DESC';
    const result = await db.query(query, params);
    res.json(result.rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Admin: get all announcements (including unpublished)
app.get('/api/admin/announcements', async (req, res) => {
  try {
    const { userId, groupId } = req.query;
    const roleInfo = await getUserRole(userId);
    if (roleInfo.role !== 'super_admin' && roleInfo.role !== 'store_admin') return res.status(403).json({ error: 'forbidden' });
    let query = 'SELECT * FROM announcements';
    const params = [];
    if (groupId) {
      params.push(groupId);
      query += ` WHERE group_id = $${params.length}`;
    }
    query += ' ORDER BY sort_order ASC, created_at DESC';
    const result = await db.query(query, params);
    res.json(result.rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Admin: create announcement
app.post('/api/admin/announcements', async (req, res) => {
  try {
    const { userId, title, content, imageUrl, linkUrl, groupId, published, sortOrder, tag } = req.body;
    const roleInfo = await getUserRole(userId);
    if (roleInfo.role !== 'super_admin' && roleInfo.role !== 'store_admin') return res.status(403).json({ error: 'forbidden' });
    if (!title) return res.status(400).json({ error: 'title is required' });
    const result = await db.query(`
      INSERT INTO announcements (group_id, title, content, image_url, link_url, published, sort_order, tag)
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
      RETURNING *
    `, [groupId || null, title, content || '', imageUrl || '', linkUrl || '', published !== false, sortOrder || 0, tag || '']);
    res.json(result.rows[0]);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Admin: update announcement
app.put('/api/admin/announcements/:id', async (req, res) => {
  try {
    const { userId, title, content, imageUrl, linkUrl, groupId, published, sortOrder, tag } = req.body;
    const roleInfo = await getUserRole(userId);
    if (roleInfo.role !== 'super_admin' && roleInfo.role !== 'store_admin') return res.status(403).json({ error: 'forbidden' });
    const result = await db.query(`
      UPDATE announcements SET
        title = COALESCE($2, title),
        content = COALESCE($3, content),
        image_url = COALESCE($4, image_url),
        link_url = COALESCE($5, link_url),
        group_id = $6,
        published = COALESCE($7, published),
        sort_order = COALESCE($8, sort_order),
        tag = COALESCE($9, tag),
        updated_at = NOW()
      WHERE id = $1
      RETURNING *
    `, [req.params.id, title, content, imageUrl, linkUrl, groupId || null, published, sortOrder, tag]);
    if (result.rows.length === 0) return res.status(404).json({ error: 'announcement not found' });
    res.json(result.rows[0]);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Admin: delete announcement
app.delete('/api/admin/announcements/:id', async (req, res) => {
  try {
    const { userId } = req.query;
    const roleInfo = await getUserRole(userId);
    if (roleInfo.role !== 'super_admin' && roleInfo.role !== 'store_admin') return res.status(403).json({ error: 'forbidden' });
    const result = await db.query('DELETE FROM announcements WHERE id=$1 RETURNING id', [req.params.id]);
    if (result.rows.length === 0) return res.status(404).json({ error: 'announcement not found' });
    res.json({ success: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Redeem coupon by code
app.post('/api/coupons/redeem', async (req, res) => {
  try {
    const { userId, code, groupId } = req.body;
    if (!userId || !code) return res.status(400).json({ error: 'userId and code required' });
    // Find coupon by redeem_code
    const couponRes = await db.query(
      `SELECT * FROM admin_coupons WHERE UPPER(redeem_code) = UPPER($1) AND active = true AND expiry >= CURRENT_DATE AND redeem_code != ''`,
      [code.trim()]
    );
    if (couponRes.rows.length === 0) return res.status(404).json({ error: '無效的兌換碼' });
    const coupon = couponRes.rows[0];
    // Check if already redeemed by this user
    const existing = await db.query(
      'SELECT id FROM user_coupons WHERE line_user_id = $1 AND coupon_id = $2',
      [userId, coupon.id]
    );
    if (existing.rows.length > 0) return res.status(400).json({ error: '您已經兌換過此優惠券' });
    // Calculate expiry
    let expiryDate = coupon.expiry;
    const validityMatch = (coupon.validity || '').match(/(\d+)日/);
    if (validityMatch) {
      expiryDate = new Date(Date.now() + parseInt(validityMatch[1]) * 86400000).toISOString().split('T')[0];
    }
    const effectiveGroupId = groupId || coupon.group_id;
    // Insert user_coupon
    await db.query(`
      INSERT INTO user_coupons (line_user_id, coupon_id, group_id, uses_remaining, expiry_date, status)
      VALUES ($1, $2, $3, $4, $5, 'active')
    `, [userId, coupon.id, effectiveGroupId, coupon.max_uses || 1, expiryDate]);
    res.json({
      success: true,
      coupon: {
        name: coupon.name, discount: coupon.discount, type: coupon.type,
        expiry: expiryDate, category: coupon.category || 'coupon',
      },
    });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ═══════════════════════════════════════
//  API: Public Coupons (user-facing)
// ═══════════════════════════════════════

// List available coupons for purchase (no admin required)
app.get('/api/coupons', async (req, res) => {
  try {
    const { groupId } = req.query;
    let query = `SELECT c.*, sg.name as group_name FROM admin_coupons c
      LEFT JOIN store_groups sg ON c.group_id = sg.id
      WHERE c.active = true AND c.expiry >= CURRENT_DATE`;
    const params = [];
    if (groupId) { query += ` AND (c.group_id = $1 OR c.group_id IS NULL)`; params.push(groupId); }
    query += ' ORDER BY c.category, c.created_at DESC';
    const r = await db.query(query, params);
    // Auto-generate description template for each coupon
    const coupons = r.rows.map(c => {
      const autoDesc = c.type === 'fixed'
        ? `當實付點數達到${c.min_spend || 0}點，可使用該卷抵扣${c.discount}點`
        : `該卷將全額抵扣應付點數`;
      return {
        id: String(c.id),
        name: c.name,
        discount: c.type === 'fixed' ? `${Math.round((1 - c.price / (c.price + c.discount * (c.max_uses || 1))) * 100)}%OFF` : `${c.discount}%OFF`,
        price: c.price || 0,
        originalPrice: c.type === 'fixed' ? (c.price || 0) + (c.discount * (c.max_uses || 1)) : Math.round((c.price || 0) / (1 - c.discount / 100)),
        type: c.type === 'fixed' ? '滿減卷' : '折扣券',
        desc: c.description || autoDesc,
        timeSlot: c.time_slot || '全時段可用',
        device: c.device_limit ? `${c.device_limit}，共可用${c.max_uses || 0}次` : `全機型適用，共可用${c.max_uses || 0}次`,
        validity: c.validity || '自購買起90日內有效',
        stores: c.store_scope ? `雲管家洗衣【${c.store_scope}】` : '雲管家洗衣【全台直營門市】',
        usage: '於會員系統中使用『店內付款』功能，選擇該優惠卷，系統將自動扣抵點數',
        purchaseLimit: '不限制購買次數',
        category: c.category || 'gift',
        groupId: c.group_id,
        groupName: c.group_name,
        // Raw fields for frontend use
        rawDiscount: c.discount,
        rawType: c.type,
        minSpend: c.min_spend || 0,
        maxUses: c.max_uses || 0,
        expiry: c.expiry,
        notes: c.notes || '',
        refundPolicy: c.refund_policy || '',
      };
    });
    res.json(coupons);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Purchase a coupon (deducts points OR LINE Pay, creates user_coupon)
app.post('/api/coupons/purchase', async (req, res) => {
  try {
    const { userId, couponId, groupId, paymentMethod } = req.body;
    if (!userId || !couponId) return res.status(400).json({ error: 'userId and couponId required' });
    // Verify coupon exists and is active
    const couponRes = await db.query('SELECT * FROM admin_coupons WHERE id=$1 AND active=true AND expiry >= CURRENT_DATE', [couponId]);
    if (couponRes.rows.length === 0) return res.status(404).json({ error: 'Coupon not found or expired' });
    const coupon = couponRes.rows[0];
    const price = coupon.price || 0;
    const effectiveGroupId = groupId || coupon.group_id;

    // === LINE Pay flow ===
    if (paymentMethod === 'linepay') {
      if (price <= 0) return res.status(400).json({ error: 'Free coupons do not need LINE Pay' });
      // Create a coupon_purchase order
      const orderId = 'CPORD' + Date.now() + '-' + Math.random().toString(36).slice(2, 6);
      const member = (await db.query('SELECT id FROM members WHERE line_user_id=$1', [userId])).rows[0];
      const memberId = member ? member.id : null;
      await db.query(
        `INSERT INTO orders (id, member_id, store_id, machine_id, mode, addons, extend_min, temp, total_amount, pulses, duration_sec, status, created_at)
         VALUES ($1, $2, 'coupon_purchase', $3, 'coupon_purchase', '[]', 0, '', $4, 0, 0, 'pending', NOW())`,
        [orderId, memberId, couponId, price]
      );
      // Call LINE Pay Request API
      const SERVER_URL = process.env.SERVER_URL || 'https://laundry-backend-production-efa4.up.railway.app';
      const FRONTEND_URL = 'https://laundry-frontend-chi.vercel.app';
      const orderName = `購買優惠券: ${coupon.name}`;
      const payBody = {
        amount: price, currency: 'TWD', orderId,
        packages: [{ id: orderId, amount: price, name: orderName, products: [{ name: orderName, quantity: 1, price }] }],
        redirectUrls: {
          confirmUrl: `${SERVER_URL}/api/coupons/purchase/confirm`,
          cancelUrl: `${FRONTEND_URL}/?status=cancel&orderId=${orderId}`,
        },
      };
      const result = await linePayRequest('POST', '/v3/payments/request', payBody);
      if (result.returnCode === '0000') {
        await db.query(`UPDATE orders SET status='waiting_payment' WHERE id=$1`, [orderId]);
        return res.json({
          success: true,
          paymentMethod: 'linepay',
          paymentUrl: result.info.paymentUrl.web,
          transactionId: result.info.transactionId,
          orderId,
        });
      } else {
        return res.status(400).json({ success: false, error: result.returnMessage });
      }
    }

    // === Wallet (points) flow (default) — wrapped in transaction ===
    const walletClient = await db.connect();
    try {
      await walletClient.query('BEGIN');
      // Check wallet balance and deduct atomically
      if (price > 0 && effectiveGroupId) {
        const deductRes = await walletClient.query(
          'UPDATE wallets SET balance = balance - $1 WHERE line_user_id=$2 AND group_id=$3 AND balance >= $1 RETURNING balance',
          [price, userId, effectiveGroupId]
        );
        if (deductRes.rows.length === 0) {
          await walletClient.query('ROLLBACK');
          const wr = await db.query('SELECT balance FROM wallets WHERE line_user_id=$1 AND group_id=$2', [userId, effectiveGroupId]);
          return res.status(400).json({ error: 'Insufficient balance', balance: wr.rows[0]?.balance || 0, required: price });
        }
        // Record transaction
        await walletClient.query('INSERT INTO transactions (line_user_id, group_id, type, amount, description) VALUES ($1,$2,$3,$4,$5)',
          [userId, effectiveGroupId, 'payment', -price, `購買優惠券: ${coupon.name}`]);
      }
      // Calculate expiry date from validity
      let expiryDate = coupon.expiry;
      const validityMatch = (coupon.validity || '').match(/(\d+)日/);
      if (validityMatch) {
        const days = parseInt(validityMatch[1]);
        expiryDate = new Date(Date.now() + days * 86400000).toISOString().split('T')[0];
      }
      // Insert user_coupon
      const ucRes = await walletClient.query(`
        INSERT INTO user_coupons (line_user_id, coupon_id, group_id, uses_remaining, expiry_date, status)
        VALUES ($1, $2, $3, $4, $5, 'active') RETURNING id
      `, [userId, couponId, effectiveGroupId, coupon.max_uses || 1, expiryDate]);
      await walletClient.query('COMMIT');
      // Fetch new balance
      let newBalance;
      if (price > 0 && effectiveGroupId) {
        const newBal = await db.query('SELECT balance FROM wallets WHERE line_user_id=$1 AND group_id=$2', [userId, effectiveGroupId]);
        newBalance = newBal.rows[0]?.balance || 0;
      }
      res.json({ success: true, paymentMethod: 'wallet', userCouponId: ucRes.rows[0].id, newBalance });
    } catch (walletErr) {
      await walletClient.query('ROLLBACK').catch(() => {});
      throw walletErr;
    } finally {
      walletClient.release();
    }
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// LINE Pay confirm for coupon purchase
app.get('/api/coupons/purchase/confirm', async (req, res) => {
  try {
    const { transactionId, orderId } = req.query;
    const FRONTEND_URL = 'https://laundry-frontend-chi.vercel.app';
    if (!transactionId || !orderId) return res.redirect(`${FRONTEND_URL}/?status=error&msg=missing_params`);

    // Fetch the coupon_purchase order (idempotency: skip if already paid/completed)
    const orderResult = await db.query('SELECT * FROM orders WHERE id=$1 AND status NOT IN ($2, $3)', [orderId, 'paid', 'completed']);
    if (orderResult.rows.length === 0) return res.redirect(`${FRONTEND_URL}/?status=success&type=coupon_purchase&orderId=${orderId}&already_confirmed=1`);
    const order = orderResult.rows[0];

    // Confirm with LINE Pay
    const body = { amount: order.total_amount, currency: 'TWD' };
    const result = await linePayRequest('POST', `/v3/payments/${transactionId}/confirm`, body);
    if (result.returnCode !== '0000') {
      return res.redirect(`${FRONTEND_URL}/?status=fail&msg=${encodeURIComponent(result.returnMessage)}`);
    }

    // Mark order as paid
    await db.query(`UPDATE orders SET status='paid', paid_at=NOW(), payment_method='linepay' WHERE id=$1`, [orderId]);

    // The couponId is stored in machine_id field of the coupon_purchase order
    const couponId = order.machine_id;
    const couponRes = await db.query('SELECT * FROM admin_coupons WHERE id=$1', [couponId]);
    if (couponRes.rows.length === 0) {
      return res.redirect(`${FRONTEND_URL}/?status=error&msg=coupon_not_found`);
    }
    const coupon = couponRes.rows[0];
    const effectiveGroupId = coupon.group_id;

    // Get userId from member
    const memberRow = order.member_id ? (await db.query('SELECT line_user_id FROM members WHERE id=$1', [order.member_id])).rows[0] : null;
    const userId = memberRow?.line_user_id;

    if (!userId) {
      return res.redirect(`${FRONTEND_URL}/?status=error&msg=user_not_found`);
    }

    // Calculate expiry date from validity
    let expiryDate = coupon.expiry;
    const validityMatch = (coupon.validity || '').match(/(\d+)日/);
    if (validityMatch) {
      const days = parseInt(validityMatch[1]);
      expiryDate = new Date(Date.now() + days * 86400000).toISOString().split('T')[0];
    }

    // Insert user_coupon
    await db.query(`
      INSERT INTO user_coupons (line_user_id, coupon_id, group_id, uses_remaining, expiry_date, status)
      VALUES ($1, $2, $3, $4, $5, 'active')
    `, [userId, couponId, effectiveGroupId, coupon.max_uses || 1, expiryDate]);

    // Record transaction
    if (effectiveGroupId) {
      await db.query('INSERT INTO transactions (line_user_id, group_id, type, amount, description) VALUES ($1,$2,$3,$4,$5)',
        [userId, effectiveGroupId, 'payment', -order.total_amount, `LINE Pay 購買優惠券: ${coupon.name}`]);
    }

    console.log(`[Coupon] LINE Pay purchase confirmed: userId=${userId}, coupon=${coupon.name}, amount=${order.total_amount}`);
    res.redirect(`${FRONTEND_URL}/?status=success&type=coupon_purchase&orderId=${orderId}`);
  } catch (e) {
    console.error('[Coupon] LINE Pay confirm error:', e.message);
    res.redirect(`https://laundry-frontend-chi.vercel.app/?status=error&msg=${encodeURIComponent(e.message)}`);
  }
});

// Get user's owned coupons
app.get('/api/coupons/mine', async (req, res) => {
  try {
    const { userId, groupId } = req.query;
    if (!userId) return res.status(400).json({ error: 'userId required' });
    let query = `
      SELECT uc.id as user_coupon_id, uc.uses_remaining, uc.expiry_date, uc.status, uc.purchased_at,
        c.name, c.discount, c.type, c.min_spend, c.description, c.time_slot, c.device_limit,
        c.validity, c.store_scope, c.max_uses, c.category, c.group_id,
        c.notes, c.refund_policy,
        sg.name as group_name
      FROM user_coupons uc
      JOIN admin_coupons c ON uc.coupon_id = c.id
      LEFT JOIN store_groups sg ON uc.group_id = sg.id
      WHERE uc.line_user_id = $1
    `;
    const params = [userId];
    if (groupId) { query += ` AND (uc.group_id = $2 OR uc.group_id IS NULL)`; params.push(groupId); }
    query += ' ORDER BY uc.purchased_at DESC';
    const r = await db.query(query, params);
    const coupons = r.rows.map(c => {
      const isExpired = new Date(c.expiry_date) < new Date();
      const isUsedUp = c.uses_remaining <= 0;
      return {
        id: `uc-${c.user_coupon_id}`,
        userCouponId: c.user_coupon_id,
        name: c.name,
        discount: c.discount,
        type: c.type,
        minSpend: c.min_spend || 0,
        expiry: c.expiry_date ? new Date(c.expiry_date).toISOString().split('T')[0] : '',
        used: isUsedUp,
        category: c.category || 'coupon',
        usesRemaining: c.uses_remaining,
        maxUses: c.max_uses,
        desc: c.description || (c.type === 'fixed'
          ? `當實付點數達到${c.min_spend || 0}點，可使用該卷抵扣${c.discount}點`
          : `該卷將全額抵扣應付點數`),
        timeSlot: c.time_slot || '全時段可用',
        deviceLimit: c.device_limit || '全機型適用',
        validity: c.validity || '自購買起90日內有效',
        storeScope: c.store_scope || '全台門市',
        notes: c.notes || '',
        refundPolicy: c.refund_policy || '',
        status: isExpired ? 'expired' : isUsedUp ? 'used' : 'active',
        groupName: c.group_name,
        purchasedAt: c.purchased_at,
      };
    });
    res.json(coupons);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ═══════════════════════════════════════
//  API: Coin Insert (called by edge controller)
// ═══════════════════════════════════════
app.post('/api/machine/coin-insert', requireIotApiKey, async (req, res) => {
  try {
    const { storeId, machineId, amount, coinType } = req.body;
    if (!storeId || !machineId || !amount) return res.status(400).json({ error: 'storeId, machineId, amount required' });
    const coinVal = parseInt(amount) || 0;
    const ct = coinType || '10';
    // Record as coin_payment order
    const orderId = 'COIN-' + Date.now() + '-' + Math.random().toString(36).slice(2, 6);
    await db.query(
      `INSERT INTO orders (id, store_id, machine_id, total_amount, status, type, payment_method, paid_at, created_at) VALUES ($1,$2,$3,$4,'paid','coin_payment','coin',NOW(),NOW())`,
      [orderId, storeId, machineId, coinVal]
    );
    // Update machine cache coin counters
    if (!machineCache[machineId]) machineCache[machineId] = {};
    machineCache[machineId].coinCount = (machineCache[machineId].coinCount || 0) + (ct === '50' ? 5 : 1);
    machineCache[machineId].coinTotal = (machineCache[machineId].coinTotal || 0) + coinVal;
    console.log(`Coin insert: ${storeId}/${machineId} +${coinVal} (${ct}元)`);
    res.json({ success: true, orderId, coinCount: machineCache[machineId].coinCount, coinTotal: machineCache[machineId].coinTotal });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ═══════════════════════════════════════
//  API: Coin Revenue Statistics
// ═══════════════════════════════════════
app.get('/api/admin/coin-revenue', async (req, res) => {
  try {
    const { userId, days, groupId, storeId } = req.query;
    const roleInfo = await getUserRole(userId);
    if (roleInfo.role !== 'super_admin' && roleInfo.role !== 'store_admin') return res.status(403).json({ error: 'forbidden' });
    const scopeGroupId = roleInfo.role === 'store_admin' ? roleInfo.groupId : groupId;
    const numDays = Math.min(Math.max(parseInt(days) || 7, 1), 365);

    // Build WHERE clause
    const params = [numDays];
    let where = `o.type = 'coin_payment' AND o.created_at >= CURRENT_DATE - ($1 || ' days')::INTERVAL`;
    if (storeId) {
      params.push(storeId);
      where += ` AND o.store_id = $${params.length}`;
    } else if (scopeGroupId) {
      params.push(scopeGroupId);
      where += ` AND s.group_id = $${params.length}`;
    }

    // Total coins and amount
    const totalQ = await db.query(
      `SELECT COUNT(*) as total_coins, COALESCE(SUM(o.total_amount), 0) as total_amount FROM orders o LEFT JOIN stores s ON o.store_id = s.id WHERE ${where}`, params
    );
    const totalCoins = parseInt(totalQ.rows[0].total_coins) || 0;
    const totalAmount = parseInt(totalQ.rows[0].total_amount) || 0;

    // By store
    const byStoreQ = await db.query(
      `SELECT s.name as store_name, COUNT(*) as coins, COALESCE(SUM(o.total_amount), 0) as amount FROM orders o LEFT JOIN stores s ON o.store_id = s.id WHERE ${where} GROUP BY s.name ORDER BY amount DESC`, params
    );

    // By date
    const byDateQ = await db.query(
      `SELECT DATE(o.created_at) as date, COUNT(*) as coins, COALESCE(SUM(o.total_amount), 0) as amount FROM orders o LEFT JOIN stores s ON o.store_id = s.id WHERE ${where} GROUP BY DATE(o.created_at) ORDER BY date`, params
    );

    res.json({
      totalCoins,
      totalAmount,
      byStore: byStoreQ.rows.map(r => ({ storeName: r.store_name, coins: parseInt(r.coins), amount: parseInt(r.amount) })),
      byDate: byDateQ.rows.map(r => ({
        date: r.date instanceof Date ? r.date.toISOString().split('T')[0] : String(r.date).split('T')[0],
        coins: parseInt(r.coins),
        amount: parseInt(r.amount)
      }))
    });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ═══════════════════════════════════════
//  API: Revenue Chart
// ═══════════════════════════════════════
app.get('/api/admin/revenue-chart', async (req, res) => {
  try {
    const { userId, days, groupId, startDate, endDate, storeId } = req.query;
    const roleInfo = await getUserRole(userId);
    if (roleInfo.role !== 'super_admin' && roleInfo.role !== 'store_admin') return res.status(403).json({ error: 'forbidden' });
    const scopeGroupId = roleInfo.role === 'store_admin' ? roleInfo.groupId : groupId;
    const params = [];
    let dateFilter, txDateFilter;
    if (startDate && endDate) {
      params.push(startDate, endDate);
      dateFilter = `o.created_at >= $1::date AND o.created_at < ($2::date + INTERVAL '1 day')`;
      txDateFilter = `t.created_at >= $1::date AND t.created_at < ($2::date + INTERVAL '1 day')`;
    } else {
      const numDays = Math.min(Math.max(parseInt(days) || 7, 1), 365);
      params.push(numDays);
      dateFilter = `o.created_at >= CURRENT_DATE - ($1 || ' days')::INTERVAL`;
      txDateFilter = `t.created_at >= CURRENT_DATE - ($1 || ' days')::INTERVAL`;
    }
    // Orders (machine revenue + single-pay via LINE Pay)
    // When storeId is provided, filter orders by store_id (specific store)
    // Otherwise when scopeGroupId exists, filter orders by group_id (all stores in group)
    const orderParams = [...params];
    let orderQuery = `
      SELECT DATE(o.created_at) as date, COALESCE(SUM(o.total_amount), 0) as revenue, COUNT(*) as orders
      FROM orders o JOIN stores s ON o.store_id = s.id
      WHERE o.status IN ('paid','done','running','completed') AND ${dateFilter}
    `;
    if (storeId) {
      orderParams.push(storeId);
      orderQuery += ` AND o.store_id = $${orderParams.length}`;
    } else if (scopeGroupId) {
      orderParams.push(scopeGroupId);
      orderQuery += ` AND s.group_id = $${orderParams.length}`;
    }
    orderQuery += ' GROUP BY DATE(o.created_at) ORDER BY date';
    // Transactions (topup/payment)
    // Transactions always filter by group_id (never storeId) because topup balance is shared across group
    const txParams = [...params];
    let txQuery = `
      SELECT DATE(t.created_at) as date,
        COALESCE(SUM(CASE WHEN t.type='topup' THEN t.amount ELSE 0 END), 0) as topup,
        COALESCE(SUM(CASE WHEN t.type='payment' THEN ABS(t.amount) ELSE 0 END), 0) as consumption,
        COALESCE(SUM(CASE WHEN t.type='adjustment' AND t.amount > 0 THEN t.amount ELSE 0 END), 0) as manual_topup,
        COALESCE(SUM(CASE WHEN t.type='adjustment' AND t.amount < 0 THEN ABS(t.amount) ELSE 0 END), 0) as manual_deduct,
        COUNT(CASE WHEN t.type='topup' THEN 1 END) as topup_count,
        COUNT(CASE WHEN t.type='payment' THEN 1 END) as payment_count
      FROM transactions t
      WHERE ${txDateFilter}
    `;
    if (scopeGroupId) {
      txParams.push(scopeGroupId);
      txQuery += ` AND t.group_id = $${txParams.length}`;
    }
    txQuery += ' GROUP BY DATE(t.created_at) ORDER BY date';
    // Coin revenue query (coin_payment orders)
    const coinParams = [...params];
    let coinDateFilter = dateFilter.replace(/o\./g, 'co.');
    let coinQuery = `
      SELECT DATE(co.created_at) as date, COALESCE(SUM(co.total_amount), 0) as coin_revenue, COUNT(*) as coin_count
      FROM orders co LEFT JOIN stores cs ON co.store_id = cs.id
      WHERE co.type = 'coin_payment' AND ${coinDateFilter}
    `;
    if (storeId) {
      coinParams.push(storeId);
      coinQuery += ` AND co.store_id = $${coinParams.length}`;
    } else if (scopeGroupId) {
      coinParams.push(scopeGroupId);
      coinQuery += ` AND cs.group_id = $${coinParams.length}`;
    }
    coinQuery += ' GROUP BY DATE(co.created_at) ORDER BY date';
    // Single-pay query (LINE Pay / Apple Pay direct payment, not wallet)
    const spParams = [...params];
    let spDateFilter = dateFilter.replace(/o\./g, 'sp.');
    let spQuery = `
      SELECT DATE(sp.created_at) as date, COALESCE(SUM(sp.total_amount), 0) as single_pay, COUNT(*) as single_pay_count
      FROM orders sp LEFT JOIN stores ss ON sp.store_id = ss.id
      WHERE sp.status IN ('paid','done','running','completed')
        AND sp.payment_method IN ('linepay','applepay','creditcard')
        AND COALESCE(sp.type, 'online') != 'coin_payment'
        AND ${spDateFilter}
    `;
    if (storeId) {
      spParams.push(storeId);
      spQuery += ` AND sp.store_id = $${spParams.length}`;
    } else if (scopeGroupId) {
      spParams.push(scopeGroupId);
      spQuery += ` AND ss.group_id = $${spParams.length}`;
    }
    spQuery += ' GROUP BY DATE(sp.created_at) ORDER BY date';
    const [orderRes, txRes, coinRes, spRes] = await Promise.all([
      db.query(orderQuery, orderParams),
      db.query(txQuery, txParams),
      db.query(coinQuery, coinParams),
      db.query(spQuery, spParams),
    ]);
    // Merge by date
    const emptyDay = () => ({ revenue: 0, orders: 0, topup: 0, consumption: 0, manualTopup: 0, manualDeduct: 0, topupCount: 0, paymentCount: 0, coinRevenue: 0, coinCount: 0, singlePay: 0, singlePayCount: 0 });
    const dateMap = {};
    const toD = r => r.date instanceof Date ? r.date.toISOString().split('T')[0] : String(r.date).split('T')[0];
    orderRes.rows.forEach(r => {
      const d = toD(r);
      dateMap[d] = { ...emptyDay(), date: d, revenue: parseInt(r.revenue) || 0, orders: parseInt(r.orders) || 0 };
    });
    txRes.rows.forEach(r => {
      const d = toD(r);
      if (!dateMap[d]) dateMap[d] = { ...emptyDay(), date: d };
      dateMap[d].topup = parseInt(r.topup) || 0;
      dateMap[d].consumption = parseInt(r.consumption) || 0;
      dateMap[d].manualTopup = parseInt(r.manual_topup) || 0;
      dateMap[d].manualDeduct = parseInt(r.manual_deduct) || 0;
      dateMap[d].topupCount = parseInt(r.topup_count) || 0;
      dateMap[d].paymentCount = parseInt(r.payment_count) || 0;
    });
    coinRes.rows.forEach(r => {
      const d = toD(r);
      if (!dateMap[d]) dateMap[d] = { ...emptyDay(), date: d };
      dateMap[d].coinRevenue = parseInt(r.coin_revenue) || 0;
      dateMap[d].coinCount = parseInt(r.coin_count) || 0;
    });
    spRes.rows.forEach(r => {
      const d = toD(r);
      if (!dateMap[d]) dateMap[d] = { ...emptyDay(), date: d };
      dateMap[d].singlePay = parseInt(r.single_pay) || 0;
      dateMap[d].singlePayCount = parseInt(r.single_pay_count) || 0;
    });
    const result = Object.values(dateMap).sort((a, b) => a.date.localeCompare(b.date));
    res.json(result);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Revenue CSV export with store breakdown + financial summary
app.get('/api/admin/revenue-export', async (req, res) => {
  try {
    const { userId, days, groupId, startDate, endDate, storeId } = req.query;
    const roleInfo = await getUserRole(userId);
    if (roleInfo.role !== 'super_admin' && roleInfo.role !== 'store_admin') return res.status(403).json({ error: 'forbidden' });
    const scopeGroupId = roleInfo.role === 'store_admin' ? roleInfo.groupId : groupId;
    const params = [];
    let dateFilter, txDateFilter;
    if (startDate && endDate) {
      params.push(startDate, endDate);
      dateFilter = `o.created_at >= $1::date AND o.created_at < ($2::date + INTERVAL '1 day')`;
      txDateFilter = `t.created_at >= $1::date AND t.created_at < ($2::date + INTERVAL '1 day')`;
    } else {
      const numDays = Math.min(Math.max(parseInt(days) || 7, 1), 365);
      params.push(numDays);
      dateFilter = `o.created_at >= CURRENT_DATE - ($1 || ' days')::INTERVAL`;
      txDateFilter = `t.created_at >= CURRENT_DATE - ($1 || ' days')::INTERVAL`;
    }
    // Store-level orders (with machine breakdown)
    let orderQuery = `
      SELECT DATE(o.created_at) as date, s.name as store_name,
        COALESCE(o.machine_id, 'unknown') as machine_id,
        COUNT(*) as orders, COALESCE(SUM(o.total_amount), 0) as revenue
      FROM orders o JOIN stores s ON o.store_id = s.id
      WHERE o.status IN ('paid','done','running','completed') AND ${dateFilter}
    `;
    const orderExtraParams = [];
    if (scopeGroupId) { orderQuery += ` AND s.group_id = $${params.length + orderExtraParams.length + 1}`; orderExtraParams.push(scopeGroupId); }
    if (storeId) { orderQuery += ` AND o.store_id = $${params.length + orderExtraParams.length + 1}`; orderExtraParams.push(storeId); }
    orderQuery += ' GROUP BY DATE(o.created_at), s.name, o.machine_id ORDER BY date, s.name, o.machine_id';
    // Daily transaction summary
    let txQuery = `
      SELECT DATE(t.created_at) as date,
        COALESCE(SUM(CASE WHEN t.type='topup' THEN t.amount ELSE 0 END), 0) as topup,
        COALESCE(SUM(CASE WHEN t.type='payment' THEN ABS(t.amount) ELSE 0 END), 0) as consumption,
        COALESCE(SUM(CASE WHEN t.type='adjustment' AND t.amount > 0 THEN t.amount ELSE 0 END), 0) as manual_topup,
        COALESCE(SUM(CASE WHEN t.type='adjustment' AND t.amount < 0 THEN ABS(t.amount) ELSE 0 END), 0) as manual_deduct,
        COUNT(CASE WHEN t.type='topup' THEN 1 END) as topup_count,
        COUNT(CASE WHEN t.type='payment' THEN 1 END) as payment_count
      FROM transactions t WHERE ${txDateFilter}
    `;
    if (scopeGroupId) { txQuery += ` AND t.group_id = $${params.length + 1}`; }
    txQuery += ' GROUP BY DATE(t.created_at) ORDER BY date';

    // Coin revenue query for export
    const coinExportParams = [...params];
    let coinExportDateFilter = dateFilter.replace(/o\./g, 'co.');
    let coinExportQuery = `
      SELECT DATE(co.created_at) as date, COALESCE(SUM(co.total_amount), 0) as coin_revenue, COUNT(*) as coin_count
      FROM orders co LEFT JOIN stores cs ON co.store_id = cs.id
      WHERE co.type = 'coin_payment' AND ${coinExportDateFilter}
    `;
    if (scopeGroupId) { coinExportQuery += ` AND cs.group_id = $${coinExportParams.length + 1}`; coinExportParams.push(scopeGroupId); }
    if (storeId) { coinExportQuery += ` AND co.store_id = $${coinExportParams.length + 1}`; coinExportParams.push(storeId); }
    coinExportQuery += ' GROUP BY DATE(co.created_at) ORDER BY date';

    // Single-pay query for export (LINE Pay / Apple Pay direct payment)
    const spExportParams = [...params];
    let spExportDateFilter = dateFilter.replace(/o\./g, 'sp.');
    let spExportQuery = `
      SELECT DATE(sp.created_at) as date, COALESCE(SUM(sp.total_amount), 0) as single_pay, COUNT(*) as single_pay_count
      FROM orders sp LEFT JOIN stores ss ON sp.store_id = ss.id
      WHERE sp.status IN ('paid','done','running','completed')
        AND sp.payment_method IN ('linepay','applepay','creditcard')
        AND COALESCE(sp.type, 'online') != 'coin_payment'
        AND ${spExportDateFilter}
    `;
    if (scopeGroupId) { spExportQuery += ` AND ss.group_id = $${spExportParams.length + 1}`; spExportParams.push(scopeGroupId); }
    if (storeId) { spExportQuery += ` AND sp.store_id = $${spExportParams.length + 1}`; spExportParams.push(storeId); }
    spExportQuery += ' GROUP BY DATE(sp.created_at) ORDER BY date';

    const orderParams = [...params, ...orderExtraParams];
    const txParams = scopeGroupId ? [...params, scopeGroupId] : params;
    const [orderRes, txRes, coinExportRes, spExportRes] = await Promise.all([
      db.query(orderQuery, orderParams),
      db.query(txQuery, txParams),
      db.query(coinExportQuery, coinExportParams),
      db.query(spExportQuery, spExportParams),
    ]);
    const toD = r => r.date instanceof Date ? r.date.toISOString().split('T')[0] : String(r.date).split('T')[0];
    const txMap = {};
    txRes.rows.forEach(r => { txMap[toD(r)] = r; });
    const coinMap = {};
    coinExportRes.rows.forEach(r => { coinMap[toD(r)] = r; });
    const spMap = {};
    spExportRes.rows.forEach(r => { spMap[toD(r)] = r; });

    // Compute totals
    const totalRevenue = orderRes.rows.reduce((s, r) => s + (parseInt(r.revenue) || 0), 0);
    const totalOrders = orderRes.rows.reduce((s, r) => s + (parseInt(r.orders) || 0), 0);
    const totalTopup = txRes.rows.reduce((s, r) => s + (parseInt(r.topup) || 0), 0);
    const totalConsumption = txRes.rows.reduce((s, r) => s + (parseInt(r.consumption) || 0), 0);
    const totalManualTopup = txRes.rows.reduce((s, r) => s + (parseInt(r.manual_topup) || 0), 0);
    const totalManualDeduct = txRes.rows.reduce((s, r) => s + (parseInt(r.manual_deduct) || 0), 0);
    const totalTopupCount = txRes.rows.reduce((s, r) => s + (parseInt(r.topup_count) || 0), 0);
    const totalPaymentCount = txRes.rows.reduce((s, r) => s + (parseInt(r.payment_count) || 0), 0);
    const totalCoinRevenue = coinExportRes.rows.reduce((s, r) => s + (parseInt(r.coin_revenue) || 0), 0);
    const totalCoinCount = coinExportRes.rows.reduce((s, r) => s + (parseInt(r.coin_count) || 0), 0);
    const totalSinglePay = spExportRes.rows.reduce((s, r) => s + (parseInt(r.single_pay) || 0), 0);
    const totalSinglePayCount = spExportRes.rows.reduce((s, r) => s + (parseInt(r.single_pay_count) || 0), 0);

    const BOM = '\uFEFF';
    const periodLabel = startDate && endDate ? `${startDate} ~ ${endDate}` : `近 ${params[0]} 天`;
    const genTime = new Date().toLocaleString('zh-TW', { timeZone: 'Asia/Taipei' });
    let csv = BOM;
    csv += '=== 雲管家營收報表 ===\n';
    csv += `報表期間:,${periodLabel}\n`;
    csv += `產生時間:,${genTime}\n\n`;

    csv += '=== 營收摘要 ===\n';
    csv += `項目,金額\n`;
    csv += `機台營業總額,$${totalRevenue}\n`;
    csv += `儲值點數消費,$${totalConsumption}\n`;
    csv += `單次線上支付,$${totalSinglePay}\n`;
    csv += `現金投幣收入,$${totalCoinRevenue}\n`;
    csv += `線上儲值總額,$${totalTopup}\n`;
    csv += `手動加值,$${totalManualTopup}\n`;
    csv += `手動扣款,$${totalManualDeduct}\n`;
    csv += `淨儲值,$${totalTopup - totalConsumption + totalManualTopup - totalManualDeduct}\n`;
    csv += `總交易筆數,${totalOrders}\n`;
    csv += `儲值筆數,${totalTopupCount}\n`;
    csv += `消費筆數(點數),${totalPaymentCount}\n`;
    csv += `投幣筆數,${totalCoinCount}\n`;
    csv += `單次支付筆數,${totalSinglePayCount}\n\n`;

    // Daily transaction summary
    csv += '=== 每日明細 ===\n';
    csv += '日期,機台營收,儲值消費,單次支付,投幣金額,線上儲值,儲值筆數,消費筆數,投幣筆數,單次支付筆數\n';
    const allDates = new Set([
      ...orderRes.rows.map(r => toD(r)),
      ...txRes.rows.map(r => toD(r)),
      ...coinExportRes.rows.map(r => toD(r)),
      ...spExportRes.rows.map(r => toD(r)),
    ]);
    [...allDates].sort().forEach(d => {
      const dayOrders = orderRes.rows.filter(r => toD(r) === d);
      const dayRev = dayOrders.reduce((s, r) => s + (parseInt(r.revenue) || 0), 0);
      const tx = txMap[d] || {};
      const coin = coinMap[d] || {};
      const sp = spMap[d] || {};
      csv += `${d},${dayRev},${parseInt(tx.consumption)||0},${parseInt(sp.single_pay)||0},${parseInt(coin.coin_revenue)||0},${parseInt(tx.topup)||0},${parseInt(tx.topup_count)||0},${parseInt(tx.payment_count)||0},${parseInt(coin.coin_count)||0},${parseInt(sp.single_pay_count)||0}\n`;
    });
    csv += '\n';

    // Store breakdown (with machine detail)
    csv += '=== 機台明細 ===\n';
    csv += '日期,店舖名稱,機台編號,交易筆數,營收金額\n';
    orderRes.rows.forEach(row => {
      csv += `${toD(row)},"${row.store_name}","${row.machine_id}",${row.orders},${row.revenue}\n`;
    });
    csv += '\n';

    // Per-machine revenue breakdown (aggregate across dates)
    let machineQuery = `
      SELECT o.machine_id, s.name as store_name, COUNT(*) as orders, COALESCE(SUM(o.total_amount), 0) as revenue
      FROM orders o JOIN stores s ON o.store_id = s.id
      WHERE o.status IN ('paid','done','running','completed') AND ${dateFilter}
    `;
    const machineExtraParams = [];
    if (scopeGroupId) { machineQuery += ` AND s.group_id = $${params.length + machineExtraParams.length + 1}`; machineExtraParams.push(scopeGroupId); }
    if (storeId) { machineQuery += ` AND o.store_id = $${params.length + machineExtraParams.length + 1}`; machineExtraParams.push(storeId); }
    machineQuery += ' GROUP BY o.machine_id, s.name ORDER BY s.name, o.machine_id';
    try {
      const machineRes = await db.query(machineQuery, [...params, ...machineExtraParams]);
      if (machineRes.rows.length > 0) {
        csv += '=== 機台營收彙總 ===\n';
        csv += '機台編號,所屬門市,交易筆數,營收金額\n';
        machineRes.rows.forEach(row => {
          csv += `"${row.machine_id || '未知'}","${row.store_name}",${row.orders},${row.revenue}\n`;
        });
      }
    } catch (e) { /* machine_id column might not exist */ }

    const today = new Date().toISOString().split('T')[0];
    res.setHeader('Content-Type', 'text/csv; charset=utf-8');
    res.setHeader('Content-Disposition', `attachment; filename="revenue-report-${today}.csv"`);
    res.send(csv);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ═══════════════════════════════════════
//  API: Member Level System
// ═══════════════════════════════════════

// Helper: calculate member level from total spent
async function getMemberLevel(lineUserId) {
  try {
    // Sum all spending transactions (type = 'payment' or 'deduct' are negative amounts, topup are positive)
    const spentResult = await db.query(
      `SELECT COALESCE(SUM(ABS(amount)), 0) as total_spent
       FROM transactions WHERE line_user_id = $1 AND type IN ('payment', 'deduct')`,
      [lineUserId]
    );
    const totalSpent = parseInt(spentResult.rows[0].total_spent) || 0;

    // Get all levels ordered by min_points desc to find the highest matching level
    const levels = await db.query('SELECT * FROM member_levels ORDER BY min_points DESC');
    let currentLevel = levels.rows[levels.rows.length - 1]; // default: lowest level
    let nextLevel = null;

    for (let i = 0; i < levels.rows.length; i++) {
      if (totalSpent >= levels.rows[i].min_points) {
        currentLevel = levels.rows[i];
        nextLevel = i > 0 ? levels.rows[i - 1] : null;
        break;
      }
    }

    const pointsToNext = nextLevel ? nextLevel.min_points - totalSpent : 0;

    return {
      level: {
        name: currentLevel.name,
        icon: currentLevel.icon,
        discount_percent: currentLevel.discount_percent,
        benefits: currentLevel.benefits,
      },
      totalSpent,
      nextLevel: nextLevel ? { name: nextLevel.name, icon: nextLevel.icon, min_points: nextLevel.min_points } : null,
      pointsToNext: Math.max(0, pointsToNext),
    };
  } catch (e) {
    return {
      level: { name: '普通會員', icon: '🌱', discount_percent: 0, benefits: '基本會員權益' },
      totalSpent: 0,
      nextLevel: { name: '銅牌會員', icon: '🥉', min_points: 500 },
      pointsToNext: 500,
    };
  }
}

app.get('/api/member/level', async (req, res) => {
  try {
    const { userId } = req.query;
    if (!userId) return res.status(400).json({ error: 'userId required' });
    const result = await getMemberLevel(userId);
    res.json(result);
  } catch (e) {
    console.error('member level error:', e);
    res.status(500).json({ error: e.message });
  }
});

// ═══════════════════════════════════════
//  API: Referral Code System
// ═══════════════════════════════════════

// Helper: generate referral code YP + 6 random alphanumeric
function generateReferralCode() {
  const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789'; // removed confusing chars 0OI1
  let code = 'YP';
  for (let i = 0; i < 6; i++) {
    code += chars.charAt(Math.floor(Math.random() * chars.length));
  }
  return code;
}

app.get('/api/member/referral', async (req, res) => {
  try {
    const { userId } = req.query;
    if (!userId) return res.status(400).json({ error: 'userId required' });

    // Get or create referral code
    let codeRow = await db.query('SELECT * FROM referral_codes WHERE line_user_id = $1', [userId]);
    if (codeRow.rows.length === 0) {
      // Generate unique code with retry
      let code;
      let attempts = 0;
      while (attempts < 10) {
        code = generateReferralCode();
        try {
          await db.query(
            'INSERT INTO referral_codes (line_user_id, code) VALUES ($1, $2)',
            [userId, code]
          );
          break;
        } catch (e) {
          attempts++;
          if (attempts >= 10) throw new Error('Failed to generate unique referral code');
        }
      }
      codeRow = await db.query('SELECT * FROM referral_codes WHERE line_user_id = $1', [userId]);
    }

    const referralCode = codeRow.rows[0];

    // Get referral records
    const records = await db.query(
      `SELECT rr.referred_id, rr.reward_given, rr.created_at,
              m.display_name
       FROM referral_records rr
       LEFT JOIN members m ON m.line_user_id = rr.referred_id
       WHERE rr.referrer_id = $1
       ORDER BY rr.created_at DESC`,
      [userId]
    );

    const totalReward = referralCode.uses * referralCode.reward_points;

    res.json({
      code: referralCode.code,
      uses: referralCode.uses,
      rewardPoints: referralCode.reward_points,
      totalReward,
      records: records.rows.map(r => ({
        referredId: r.referred_id,
        displayName: r.display_name || '匿名用戶',
        rewardGiven: r.reward_given,
        createdAt: r.created_at,
      })),
    });
  } catch (e) {
    console.error('referral get error:', e);
    res.status(500).json({ error: e.message });
  }
});

app.post('/api/member/referral/use', async (req, res) => {
  try {
    const { userId, code } = req.body;
    if (!userId || !code) return res.status(400).json({ error: 'userId and code required' });

    // Find the referral code
    const codeResult = await db.query('SELECT * FROM referral_codes WHERE code = $1', [code.toUpperCase()]);
    if (codeResult.rows.length === 0) {
      return res.status(404).json({ error: '推薦碼不存在' });
    }

    const referralCode = codeResult.rows[0];

    // Cannot refer yourself
    if (referralCode.line_user_id === userId) {
      return res.status(400).json({ error: '不能使用自己的推薦碼' });
    }

    // Check if already used by this user
    const existing = await db.query(
      'SELECT id FROM referral_records WHERE referred_id = $1',
      [userId]
    );
    if (existing.rows.length > 0) {
      return res.status(400).json({ error: '您已經使用過推薦碼了' });
    }

    const rewardPoints = referralCode.reward_points;
    const referrerId = referralCode.line_user_id;

    // Record the referral
    await db.query(
      'INSERT INTO referral_records (referrer_id, referred_id, code, reward_given) VALUES ($1, $2, $3, true)',
      [referrerId, userId, code.toUpperCase()]
    );

    // Increment uses
    await db.query('UPDATE referral_codes SET uses = uses + 1 WHERE code = $1', [code.toUpperCase()]);

    // Reward both users: add to all their wallets
    // Get all group wallets for referrer
    const referrerWallets = await db.query('SELECT group_id FROM wallets WHERE line_user_id = $1', [referrerId]);
    for (const w of referrerWallets.rows) {
      await db.query('UPDATE wallets SET balance = balance + $1 WHERE line_user_id = $2 AND group_id = $3',
        [rewardPoints, referrerId, w.group_id]);
      await db.query(
        `INSERT INTO transactions (line_user_id, group_id, type, amount, description)
         VALUES ($1, $2, 'referral_reward', $3, $4)`,
        [referrerId, w.group_id, rewardPoints, `推薦獎勵 (被推薦人使用推薦碼 ${code.toUpperCase()})`]
      );
    }

    // Get all group wallets for referred user
    const referredWallets = await db.query('SELECT group_id FROM wallets WHERE line_user_id = $1', [userId]);
    for (const w of referredWallets.rows) {
      await db.query('UPDATE wallets SET balance = balance + $1 WHERE line_user_id = $2 AND group_id = $3',
        [rewardPoints, userId, w.group_id]);
      await db.query(
        `INSERT INTO transactions (line_user_id, group_id, type, amount, description)
         VALUES ($1, $2, 'referral_bonus', $3, $4)`,
        [userId, w.group_id, rewardPoints, `新用戶推薦碼獎勵 (NT$${rewardPoints})`]
      );
    }

    res.json({
      success: true,
      message: `推薦碼使用成功！您和推薦人各獲得 NT$${rewardPoints} 獎勵`,
      reward: rewardPoints,
    });
  } catch (e) {
    console.error('referral use error:', e);
    res.status(500).json({ error: e.message });
  }
});

// ═══════════════════════════════════════
//  API: Admin - Member Levels
// ═══════════════════════════════════════

app.get('/api/admin/member-levels', async (req, res) => {
  try {
    const { userId } = req.query;
    if (!userId) return res.status(400).json({ error: 'userId required' });
    const roleInfo = await getUserRole(userId);
    if (roleInfo.role !== 'super_admin' && roleInfo.role !== 'store_admin') {
      return res.status(403).json({ error: 'forbidden' });
    }
    const result = await db.query('SELECT * FROM member_levels ORDER BY min_points ASC');
    res.json(result.rows);
  } catch (e) {
    console.error('admin member-levels error:', e);
    res.status(500).json({ error: e.message });
  }
});

app.put('/api/admin/member-levels/:id', async (req, res) => {
  try {
    const { userId } = req.body;
    if (!userId) return res.status(400).json({ error: 'userId required' });
    const roleInfo = await getUserRole(userId);
    if (roleInfo.role !== 'super_admin' && roleInfo.role !== 'store_admin') {
      return res.status(403).json({ error: 'forbidden' });
    }
    const { name, min_points, discount_percent, benefits, icon } = req.body;
    const result = await db.query(
      `UPDATE member_levels SET
        name = COALESCE($1, name),
        min_points = COALESCE($2, min_points),
        discount_percent = COALESCE($3, discount_percent),
        benefits = COALESCE($4, benefits),
        icon = COALESCE($5, icon)
       WHERE id = $6 RETURNING *`,
      [name, min_points, discount_percent, benefits, icon, req.params.id]
    );
    if (result.rows.length === 0) return res.status(404).json({ error: 'Level not found' });
    res.json(result.rows[0]);
  } catch (e) {
    console.error('admin update member-level error:', e);
    res.status(500).json({ error: e.message });
  }
});

// API: Admin - Update Referral Reward Points
app.put('/api/admin/referral-reward', async (req, res) => {
  try {
    const { userId, rewardPoints } = req.body;
    if (!userId) return res.status(400).json({ error: 'userId required' });
    const roleInfo = await getUserRole(userId);
    if (roleInfo.role !== 'super_admin' && roleInfo.role !== 'store_admin') {
      return res.status(403).json({ error: 'forbidden' });
    }
    if (!rewardPoints || rewardPoints < 0) return res.status(400).json({ error: 'invalid rewardPoints' });
    // Update all future referral codes' reward (existing codes keep their original reward)
    await db.query('UPDATE referral_codes SET reward_points = $1 WHERE uses = 0', [rewardPoints]);
    res.json({ success: true, message: `Referral reward updated to ${rewardPoints} points for unused codes` });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ═══════════════════════════════════════
//  API: Admin - Referral Stats
// ═══════════════════════════════════════

app.get('/api/admin/referral-stats', async (req, res) => {
  try {
    const { userId } = req.query;
    if (!userId) return res.status(400).json({ error: 'userId required' });
    const roleInfo = await getUserRole(userId);
    if (roleInfo.role !== 'super_admin' && roleInfo.role !== 'store_admin') {
      return res.status(403).json({ error: 'forbidden' });
    }

    const totalReferrals = await db.query('SELECT COUNT(*) FROM referral_records');
    const totalRewards = await db.query(
      `SELECT COALESCE(SUM(rc.uses * rc.reward_points), 0) as total
       FROM referral_codes rc WHERE rc.uses > 0`
    );
    const topReferrers = await db.query(
      `SELECT rc.line_user_id, rc.code, rc.uses, rc.reward_points,
              (rc.uses * rc.reward_points) as total_reward,
              m.display_name
       FROM referral_codes rc
       LEFT JOIN members m ON m.line_user_id = rc.line_user_id
       WHERE rc.uses > 0
       ORDER BY rc.uses DESC
       LIMIT 20`
    );

    res.json({
      totalReferrals: parseInt(totalReferrals.rows[0].count),
      totalRewards: parseInt(totalRewards.rows[0].total),
      topReferrers: topReferrers.rows.map(r => ({
        lineUserId: r.line_user_id,
        displayName: r.display_name || '匿名用戶',
        code: r.code,
        uses: r.uses,
        rewardPoints: r.reward_points,
        totalReward: parseInt(r.total_reward),
      })),
    });
  } catch (e) {
    console.error('admin referral-stats error:', e);
    res.status(500).json({ error: e.message });
  }
});

// ═══════════════════════════════════════
//  API: Admin Manual Push Notification
// ═══════════════════════════════════════
app.post('/api/notifications/send', async (req, res) => {
  try {
    const { userId: adminId, targetUserId, targetGroupId, message, type, title, imageUrl } = req.body;
    if (!adminId || !message) return res.status(400).json({ error: 'Missing adminId or message' });

    // Verify admin permission
    const roleInfo = await getUserRole(adminId);
    if (roleInfo.role !== 'super_admin' && roleInfo.role !== 'store_admin') {
      return res.status(403).json({ error: 'forbidden' });
    }

    // store_admin can only send to their own managed group
    if (roleInfo.role === 'store_admin') {
      if (targetGroupId && targetGroupId !== roleInfo.groupId) {
        return res.status(403).json({ error: 'store_admin can only send notifications to their own group' });
      }
      if (targetUserId) {
        const walletCheck = await db.query(
          'SELECT 1 FROM wallets WHERE line_user_id = $1 AND group_id = $2 LIMIT 1',
          [targetUserId, roleInfo.groupId]
        );
        if (walletCheck.rows.length === 0) {
          return res.status(403).json({ error: 'target user is not in your managed group' });
        }
      }
    }

    let targets = [];
    if (targetUserId) {
      // Single user push
      targets = [targetUserId];
    } else if (targetGroupId) {
      // Group push: find all users who have a wallet in this group (active consumers)
      const usersRes = await db.query(
        'SELECT DISTINCT line_user_id FROM wallets WHERE group_id = $1 AND balance >= 0',
        [targetGroupId]
      );
      targets = usersRes.rows.map(r => r.line_user_id);
    } else {
      return res.status(400).json({ error: 'Provide targetUserId or targetGroupId' });
    }

    let successCount = 0;
    let failCount = 0;

    const pushType = type === 'promo' ? 'manual_promo' : 'manual_text';
    for (const uid of targets) {
      let ok = false;
      const pushOpts = { pushType, groupId: targetGroupId || null, triggeredBy: adminId, description: message.substring(0, 200) };
      if (type === 'promo') {
        // Promotional Flex Message
        ok = await sendLineFlexMessage(uid, title || message, buildPromoFlexMessage(message, { title, imageUrl }), pushOpts);
      } else {
        ok = await sendLineText(uid, message, pushOpts);
      }
      if (ok) successCount++; else failCount++;
    }

    res.json({ success: true, totalTargets: targets.length, successCount, failCount });
  } catch (e) {
    console.error('notifications/send error:', e);
    res.status(500).json({ error: e.message });
  }
});

// ═══════════════════════════════════════
//  API: Admin Push Stats (LINE billing tracking)
// ═══════════════════════════════════════

// LINE Messaging API pricing (Taiwan 2026)
const LINE_PUSH_PRICING = {
  plans: [
    { name: 'free', monthlyFee: 0, included: 200, overageRate: 0 },
    { name: 'light', monthlyFee: 800, included: 4000, overageRate: 0.2 },
    { name: 'standard', monthlyFee: 4000, included: 25000, overageRate: 0.16 },
    { name: 'pro', monthlyFee: 10000, included: 100000, overageRate: 0.1 },
  ],
  defaultPlan: 'light',
};

const PUSH_TYPE_LABELS = {
  auto_payment: '付款通知',
  auto_complete: '完成通知',
  auto_reminder: '取衣提醒',
  auto_topup: '儲值通知',
  manual_text: '手動文字',
  manual_promo: '促銷推播',
  store_manual: '店家手動推播',
  unknown: '其他',
};

app.get('/api/admin/push-stats', async (req, res) => {
  try {
    const { userId, days, groupId } = req.query;
    if (!userId) return res.status(400).json({ error: 'userId required' });
    const roleInfo = await getUserRole(userId);
    if (roleInfo.role !== 'super_admin' && roleInfo.role !== 'store_admin') {
      return res.status(403).json({ error: 'forbidden' });
    }

    const scopeGroupId = roleInfo.role === 'store_admin' ? roleInfo.groupId : (groupId || null);
    const numDays = Math.min(Math.max(parseInt(days) || 30, 1), 365);

    // Build WHERE clause
    const params = [numDays];
    let where = `pl.created_at >= CURRENT_DATE - ($1 || ' days')::INTERVAL`;
    if (scopeGroupId) {
      params.push(scopeGroupId);
      where += ` AND pl.group_id = $${params.length}`;
    }

    // Summary totals
    const summaryQ = await db.query(`
      SELECT
        COUNT(*) as total_pushes,
        COALESCE(SUM(pl.message_count), 0) as total_messages,
        COUNT(CASE WHEN pl.push_type LIKE 'auto_%' THEN 1 END) as auto_count,
        COUNT(CASE WHEN pl.push_type LIKE 'manual_%' THEN 1 END) as manual_count
      FROM push_logs pl
      WHERE ${where}
    `, params);
    const summary = summaryQ.rows[0];
    const totalMessages = parseInt(summary.total_messages) || 0;

    // Calculate estimated cost based on current plan
    const plan = LINE_PUSH_PRICING.plans.find(p => p.name === LINE_PUSH_PRICING.defaultPlan);
    const freeQuota = plan.included;
    const usedQuota = totalMessages;
    const remainingFree = Math.max(0, freeQuota - usedQuota);
    const paidMessages = Math.max(0, usedQuota - freeQuota);
    const overageCost = paidMessages * plan.overageRate;
    // Pro-rate monthly fee across groups if needed
    const estimatedCost = overageCost;

    // By group
    const byGroupQ = await db.query(`
      SELECT pl.group_id, sg.name as group_name,
        COUNT(*) as pushes,
        COALESCE(SUM(pl.message_count), 0) as messages
      FROM push_logs pl
      LEFT JOIN store_groups sg ON pl.group_id = sg.id
      WHERE ${where}
      GROUP BY pl.group_id, sg.name
      ORDER BY messages DESC
    `, params);

    // Calculate cost per group (pro-rate by message count)
    const byGroup = byGroupQ.rows.map(r => {
      const msgs = parseInt(r.messages) || 0;
      const ratio = totalMessages > 0 ? msgs / totalMessages : 0;
      const groupMonthlyShare = Math.round(plan.monthlyFee * ratio);
      const groupOverage = Math.round(overageCost * ratio);
      return {
        groupId: r.group_id,
        groupName: r.group_name || '(未歸屬)',
        pushes: parseInt(r.pushes),
        messages: msgs,
        cost: groupMonthlyShare + groupOverage,
      };
    });

    // By type
    const byTypeQ = await db.query(`
      SELECT pl.push_type, COUNT(*) as count
      FROM push_logs pl
      WHERE ${where}
      GROUP BY pl.push_type
      ORDER BY count DESC
    `, params);
    const byType = byTypeQ.rows.map(r => ({
      type: r.push_type,
      count: parseInt(r.count),
      label: PUSH_TYPE_LABELS[r.push_type] || r.push_type,
    }));

    // By date
    const byDateQ = await db.query(`
      SELECT DATE(pl.created_at) as date,
        COUNT(*) as pushes,
        COALESCE(SUM(pl.message_count), 0) as messages
      FROM push_logs pl
      WHERE ${where}
      GROUP BY DATE(pl.created_at)
      ORDER BY date
    `, params);
    const byDate = byDateQ.rows.map(r => ({
      date: r.date instanceof Date ? r.date.toISOString().split('T')[0] : String(r.date).split('T')[0],
      pushes: parseInt(r.pushes),
      messages: parseInt(r.messages),
    }));

    res.json({
      summary: {
        totalPushes: parseInt(summary.total_pushes) || 0,
        totalMessages,
        autoCount: parseInt(summary.auto_count) || 0,
        manualCount: parseInt(summary.manual_count) || 0,
        estimatedCost: Math.round(estimatedCost),
      },
      byGroup,
      byType,
      byDate,
      pricing: {
        plan: LINE_PUSH_PRICING.defaultPlan,
        monthlyFee: plan.monthlyFee,
        freeQuota,
        usedQuota,
        remainingFree,
        paidMessages,
        pricePerMessage: plan.overageRate,
        overageCost: Math.round(overageCost),
      },
    });
  } catch (e) {
    console.error('push-stats error:', e);
    res.status(500).json({ error: e.message });
  }
});

// Push usage CSV export
app.get('/api/admin/push-export', async (req, res) => {
  try {
    const { userId, days, groupId } = req.query;
    if (!userId) return res.status(400).json({ error: 'userId required' });
    const roleInfo = await getUserRole(userId);
    if (roleInfo.role !== 'super_admin' && roleInfo.role !== 'store_admin') {
      return res.status(403).json({ error: 'forbidden' });
    }

    const scopeGroupId = roleInfo.role === 'store_admin' ? roleInfo.groupId : (groupId || null);
    const numDays = Math.min(Math.max(parseInt(days) || 30, 1), 365);

    const params = [numDays];
    let where = `pl.created_at >= CURRENT_DATE - ($1 || ' days')::INTERVAL`;
    if (scopeGroupId) {
      params.push(scopeGroupId);
      where += ` AND pl.group_id = $${params.length}`;
    }

    // Daily breakdown by group and type
    const detailQ = await db.query(`
      SELECT DATE(pl.created_at) as date,
        pl.group_id, sg.name as group_name,
        pl.push_type,
        COUNT(*) as pushes,
        COALESCE(SUM(pl.message_count), 0) as messages,
        COUNT(CASE WHEN pl.success = true THEN 1 END) as success_count,
        COUNT(CASE WHEN pl.success = false THEN 1 END) as fail_count
      FROM push_logs pl
      LEFT JOIN store_groups sg ON pl.group_id = sg.id
      WHERE ${where}
      GROUP BY DATE(pl.created_at), pl.group_id, sg.name, pl.push_type
      ORDER BY date, sg.name, pl.push_type
    `, params);

    const plan = LINE_PUSH_PRICING.plans.find(p => p.name === LINE_PUSH_PRICING.defaultPlan);

    const BOM = '\uFEFF';
    const today = new Date().toISOString().split('T')[0];
    const genTime = new Date().toLocaleString('zh-TW', { timeZone: 'Asia/Taipei' });
    let csv = BOM;
    csv += '=== LINE 推播用量報表 ===\n';
    csv += `報表期間:,近 ${numDays} 天\n`;
    csv += `產生時間:,${genTime}\n`;
    csv += `方案:,${LINE_PUSH_PRICING.defaultPlan} (月費 NT$${plan.monthlyFee}, 含 ${plan.included} 則, 超額 NT$${plan.overageRate}/則)\n\n`;

    // Summary
    const totalMessages = detailQ.rows.reduce((s, r) => s + (parseInt(r.messages) || 0), 0);
    const paidMessages = Math.max(0, totalMessages - plan.included);
    csv += '=== 摘要 ===\n';
    csv += `總推播則數,${totalMessages}\n`;
    csv += `免費額度,${plan.included}\n`;
    csv += `已用額度,${totalMessages}\n`;
    csv += `剩餘免費,${Math.max(0, plan.included - totalMessages)}\n`;
    csv += `超額則數,${paidMessages}\n`;
    csv += `超額費用,NT$${Math.round(paidMessages * plan.overageRate)}\n\n`;

    // Detail
    csv += '=== 每日明細 ===\n';
    csv += '日期,門市集團,推播類型,推播次數,訊息則數,成功,失敗,預估費用\n';
    detailQ.rows.forEach(r => {
      const d = r.date instanceof Date ? r.date.toISOString().split('T')[0] : String(r.date).split('T')[0];
      const msgs = parseInt(r.messages) || 0;
      const ratio = totalMessages > 0 ? msgs / totalMessages : 0;
      const cost = Math.round((paidMessages * plan.overageRate) * ratio);
      const typeLabel = PUSH_TYPE_LABELS[r.push_type] || r.push_type;
      csv += `${d},"${r.group_name || '(未歸屬)'}","${typeLabel}",${r.pushes},${msgs},${r.success_count},${r.fail_count},NT$${cost}\n`;
    });

    res.setHeader('Content-Type', 'text/csv; charset=utf-8');
    res.setHeader('Content-Disposition', `attachment; filename="push-usage-report-${today}.csv"`);
    res.send(csv);
  } catch (e) {
    console.error('push-export error:', e);
    res.status(500).json({ error: e.message });
  }
});

// ═══════════════════════════════════════
//  API: Store Owner Push Notifications
// ═══════════════════════════════════════

// Preview: get recipient count, balance, estimated cost
app.get('/api/admin/store-push/preview', async (req, res) => {
  try {
    const { userId, groupId } = req.query;
    if (!userId) return res.status(400).json({ error: 'userId required' });

    const roleInfo = await getUserRole(userId);
    if (roleInfo.role !== 'super_admin' && roleInfo.role !== 'store_admin') {
      return res.status(403).json({ error: 'forbidden' });
    }

    const targetGroupId = roleInfo.role === 'store_admin' ? roleInfo.groupId : (groupId || null);
    if (!targetGroupId) return res.status(400).json({ error: 'groupId required' });

    // Recipient count
    const recipientRes = await db.query(
      'SELECT COUNT(DISTINCT line_user_id) as cnt FROM wallets WHERE group_id = $1',
      [targetGroupId]
    );
    const recipientCount = parseInt(recipientRes.rows[0].cnt) || 0;

    // Balance info
    const balanceRes = await db.query(
      'SELECT balance, per_push_rate, monthly_manual_limit FROM store_push_balance WHERE group_id = $1',
      [targetGroupId]
    );
    const balanceRow = balanceRes.rows[0] || { balance: 0, per_push_rate: 0.16, monthly_manual_limit: 500 };
    const balance = parseFloat(balanceRow.balance) || 0;
    const perPushRate = parseFloat(balanceRow.per_push_rate) || 0.16;
    const monthlyLimit = parseInt(balanceRow.monthly_manual_limit) || 500;

    // Monthly manual push count
    const monthlyRes = await db.query(
      `SELECT COUNT(*) as cnt FROM push_logs WHERE group_id = $1 AND push_type LIKE '%manual%' AND created_at >= date_trunc('month', CURRENT_DATE)`,
      [targetGroupId]
    );
    const monthlyUsed = parseInt(monthlyRes.rows[0].cnt) || 0;

    const estimatedCost = Math.round(recipientCount * perPushRate * 100) / 100;

    res.json({
      recipientCount,
      balance,
      perPushRate,
      estimatedCost,
      monthlyUsed,
      monthlyLimit,
    });
  } catch (e) {
    console.error('store-push/preview error:', e);
    res.status(500).json({ error: e.message });
  }
});

// Send store push notification
app.post('/api/admin/store-push', async (req, res) => {
  try {
    const { userId, message, title, imageUrl, groupId } = req.body;
    if (!userId || !message) return res.status(400).json({ error: 'userId and message required' });

    const roleInfo = await getUserRole(userId);
    if (roleInfo.role !== 'super_admin' && roleInfo.role !== 'store_admin') {
      return res.status(403).json({ error: 'forbidden' });
    }

    const targetGroupId = roleInfo.role === 'store_admin' ? roleInfo.groupId : (groupId || null);
    if (!targetGroupId) return res.status(400).json({ error: 'groupId required' });

    // store_admin can only push to their own group
    if (roleInfo.role === 'store_admin' && targetGroupId !== roleInfo.groupId) {
      return res.status(403).json({ error: 'store_admin can only push to their own group' });
    }

    // Fetch recipients
    const recipientRes = await db.query(
      'SELECT DISTINCT line_user_id FROM wallets WHERE group_id = $1',
      [targetGroupId]
    );
    const targets = recipientRes.rows.map(r => r.line_user_id);
    if (targets.length === 0) return res.status(400).json({ error: 'No recipients found in this group' });

    // Fetch balance
    const balanceRes = await db.query(
      'SELECT balance, per_push_rate, monthly_manual_limit FROM store_push_balance WHERE group_id = $1',
      [targetGroupId]
    );
    const balanceRow = balanceRes.rows[0] || { balance: 0, per_push_rate: 0.16, monthly_manual_limit: 500 };
    const perPushRate = parseFloat(balanceRow.per_push_rate) || 0.16;
    const monthlyLimit = parseInt(balanceRow.monthly_manual_limit) || 500;
    const currentBalance = parseFloat(balanceRow.balance) || 0;
    const totalCost = Math.round(targets.length * perPushRate * 100) / 100;

    // Check monthly limit
    const monthlyRes = await db.query(
      `SELECT COUNT(*) as cnt FROM push_logs WHERE group_id = $1 AND push_type LIKE '%manual%' AND created_at >= date_trunc('month', CURRENT_DATE)`,
      [targetGroupId]
    );
    const monthlyUsed = parseInt(monthlyRes.rows[0].cnt) || 0;
    if (monthlyUsed + targets.length > monthlyLimit) {
      return res.status(429).json({ error: '已超過本月推播上限', monthlyUsed, monthlyLimit });
    }

    // Check balance
    if (currentBalance < totalCost) {
      return res.status(402).json({ error: '推播餘額不足', balance: currentBalance, cost: totalCost });
    }

    // Deduct balance atomically
    const deductRes = await db.query(
      'UPDATE store_push_balance SET balance = balance - $1, updated_at = NOW() WHERE group_id = $2 AND balance >= $1 RETURNING balance',
      [totalCost, targetGroupId]
    );
    if (deductRes.rows.length === 0) {
      return res.status(402).json({ error: '推播餘額不足（扣款失敗）' });
    }
    const remainingBalance = parseFloat(deductRes.rows[0].balance);

    // Send messages
    let successCount = 0;
    let failCount = 0;
    const costPerRecipient = Math.round((totalCost / targets.length) * 100) / 100;

    for (const uid of targets) {
      let ok = false;
      const pushOpts = {
        pushType: 'store_manual',
        groupId: targetGroupId,
        triggeredBy: userId,
        description: message.substring(0, 200),
      };

      if (title && imageUrl) {
        ok = await sendLineFlexMessage(uid, title || message, buildPromoFlexMessage(message, { title, imageUrl }), pushOpts);
      } else {
        ok = await sendLineText(uid, message, pushOpts);
      }

      // Update push_logs cost for this push
      try {
        await db.query(
          `UPDATE push_logs SET cost = $1 WHERE target_user_id = $2 AND push_type = 'store_manual' AND group_id = $3 ORDER BY created_at DESC LIMIT 1`,
          [costPerRecipient, uid, targetGroupId]
        );
      } catch (costErr) {
        console.error('[store-push] cost update error:', costErr.message);
      }

      if (ok) successCount++; else failCount++;
    }

    res.json({
      success: true,
      totalTargets: targets.length,
      successCount,
      failCount,
      cost: totalCost,
      remainingBalance,
    });
  } catch (e) {
    console.error('store-push error:', e);
    res.status(500).json({ error: e.message });
  }
});

// Get store push balance & topup history
app.get('/api/admin/store-push/balance', async (req, res) => {
  try {
    const { userId, groupId } = req.query;
    if (!userId) return res.status(400).json({ error: 'userId required' });

    const roleInfo = await getUserRole(userId);
    if (roleInfo.role !== 'super_admin' && roleInfo.role !== 'store_admin') {
      return res.status(403).json({ error: 'forbidden' });
    }

    const targetGroupId = roleInfo.role === 'store_admin' ? roleInfo.groupId : (groupId || null);
    if (!targetGroupId) return res.status(400).json({ error: 'groupId required' });

    const balanceRes = await db.query(
      'SELECT balance, per_push_rate, monthly_manual_limit, updated_at FROM store_push_balance WHERE group_id = $1',
      [targetGroupId]
    );
    const balanceRow = balanceRes.rows[0] || { balance: 0, per_push_rate: 0.16, monthly_manual_limit: 500, updated_at: null };

    const topupRes = await db.query(
      'SELECT id, amount, method, note, created_by, created_at FROM store_push_topup WHERE group_id = $1 ORDER BY created_at DESC LIMIT 20',
      [targetGroupId]
    );

    res.json({
      groupId: targetGroupId,
      balance: parseFloat(balanceRow.balance) || 0,
      perPushRate: parseFloat(balanceRow.per_push_rate) || 0.16,
      monthlyManualLimit: parseInt(balanceRow.monthly_manual_limit) || 500,
      updatedAt: balanceRow.updated_at,
      topupHistory: topupRes.rows.map(r => ({
        id: r.id,
        amount: parseFloat(r.amount),
        method: r.method,
        note: r.note,
        createdBy: r.created_by,
        createdAt: r.created_at,
      })),
    });
  } catch (e) {
    console.error('store-push/balance error:', e);
    res.status(500).json({ error: e.message });
  }
});

// Topup store push balance (super_admin only)
app.post('/api/admin/store-push/topup', async (req, res) => {
  try {
    const { userId, groupId, amount, note } = req.body;
    if (!userId || !groupId || !amount) return res.status(400).json({ error: 'userId, groupId, and amount required' });

    const roleInfo = await getUserRole(userId);
    if (roleInfo.role !== 'super_admin') {
      return res.status(403).json({ error: 'Only super_admin can topup push balance' });
    }

    const numAmount = parseFloat(amount);
    if (isNaN(numAmount) || numAmount <= 0) return res.status(400).json({ error: 'Invalid amount' });

    // Record topup
    await db.query(
      'INSERT INTO store_push_topup (group_id, amount, method, note, created_by) VALUES ($1, $2, $3, $4, $5)',
      [groupId, numAmount, 'manual', note || '', userId]
    );

    // Update balance (upsert)
    const updateRes = await db.query(
      `INSERT INTO store_push_balance (group_id, balance, updated_at)
       VALUES ($1, $2, NOW())
       ON CONFLICT (group_id) DO UPDATE SET balance = store_push_balance.balance + $2, updated_at = NOW()
       RETURNING balance`,
      [groupId, numAmount]
    );

    res.json({
      success: true,
      groupId,
      addedAmount: numAmount,
      newBalance: parseFloat(updateRes.rows[0].balance),
    });
  } catch (e) {
    console.error('store-push/topup error:', e);
    res.status(500).json({ error: e.message });
  }
});

// Update store push settings (super_admin only)
app.put('/api/admin/store-push/settings', async (req, res) => {
  try {
    const { userId, groupId, perPushRate, monthlyManualLimit } = req.body;
    if (!userId || !groupId) return res.status(400).json({ error: 'userId and groupId required' });

    const roleInfo = await getUserRole(userId);
    if (roleInfo.role !== 'super_admin') {
      return res.status(403).json({ error: 'Only super_admin can update push settings' });
    }

    const updates = [];
    const params = [];
    let paramIdx = 1;

    if (perPushRate !== undefined) {
      const rate = parseFloat(perPushRate);
      if (isNaN(rate) || rate < 0) return res.status(400).json({ error: 'Invalid perPushRate' });
      updates.push(`per_push_rate = $${paramIdx++}`);
      params.push(rate);
    }
    if (monthlyManualLimit !== undefined) {
      const limit = parseInt(monthlyManualLimit);
      if (isNaN(limit) || limit < 0) return res.status(400).json({ error: 'Invalid monthlyManualLimit' });
      updates.push(`monthly_manual_limit = $${paramIdx++}`);
      params.push(limit);
    }

    if (updates.length === 0) return res.status(400).json({ error: 'No fields to update' });

    updates.push(`updated_at = NOW()`);
    params.push(groupId);

    const result = await db.query(
      `UPDATE store_push_balance SET ${updates.join(', ')} WHERE group_id = $${paramIdx} RETURNING *`,
      params
    );

    if (result.rows.length === 0) {
      return res.status(404).json({ error: 'Group push balance record not found' });
    }

    const row = result.rows[0];
    res.json({
      success: true,
      groupId: row.group_id,
      balance: parseFloat(row.balance),
      perPushRate: parseFloat(row.per_push_rate),
      monthlyManualLimit: parseInt(row.monthly_manual_limit),
      updatedAt: row.updated_at,
    });
  } catch (e) {
    console.error('store-push/settings error:', e);
    res.status(500).json({ error: e.message });
  }
});

// ═══ Push Plans Management ═══

// GET /api/admin/push-plans — 取得所有推播方案
app.get('/api/admin/push-plans', async (req, res) => {
  try {
    const result = await db.query('SELECT * FROM push_plans WHERE is_active=true ORDER BY sort_order');
    res.json(result.rows);
  } catch (e) {
    console.error('[Push Plans]', e.message);
    res.status(500).json({ error: e.message });
  }
});

// PUT /api/admin/push-plans/:planKey — 更新方案
app.put('/api/admin/push-plans/:planKey', async (req, res) => {
  try {
    const userId = req.body.userId;
    if (!userId) return res.status(400).json({ error: 'userId required' });
    const { role } = await getUserRole(userId);
    if (role !== 'super_admin') return res.status(403).json({ error: '權限不足' });

    const { planKey } = req.params;
    const { label, monthlyFee, includedMessages, extraRate, canExceed } = req.body;

    const result = await db.query(
      `UPDATE push_plans SET
        label = COALESCE($1, label),
        monthly_fee = COALESCE($2, monthly_fee),
        included_messages = COALESCE($3, included_messages),
        extra_rate = COALESCE($4, extra_rate),
        can_exceed = COALESCE($5, can_exceed),
        updated_at = NOW()
      WHERE plan_key = $6 RETURNING *`,
      [label, monthlyFee, includedMessages, extraRate, canExceed, planKey]
    );

    if (result.rows.length === 0) return res.status(404).json({ error: 'Plan not found' });
    res.json(result.rows[0]);
  } catch (e) {
    console.error('[Push Plans Update]', e.message);
    res.status(500).json({ error: e.message });
  }
});

// POST /api/admin/push-plans — 新增方案
app.post('/api/admin/push-plans', async (req, res) => {
  try {
    const userId = req.body.userId;
    if (!userId) return res.status(400).json({ error: 'userId required' });
    const { role } = await getUserRole(userId);
    if (role !== 'super_admin') return res.status(403).json({ error: '權限不足' });

    const { planKey, label, monthlyFee, includedMessages, extraRate, canExceed } = req.body;
    if (!planKey || !label) return res.status(400).json({ error: 'planKey and label required' });

    const maxSort = await db.query('SELECT COALESCE(MAX(sort_order), 0) + 1 as next FROM push_plans');
    const result = await db.query(
      `INSERT INTO push_plans (plan_key, label, monthly_fee, included_messages, extra_rate, can_exceed, sort_order)
       VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING *`,
      [planKey, label, monthlyFee || 0, includedMessages || 0, extraRate || 0, canExceed !== false, maxSort.rows[0].next]
    );
    res.json(result.rows[0]);
  } catch (e) {
    console.error('[Push Plans Create]', e.message);
    res.status(500).json({ error: e.message });
  }
});

// POST /api/admin/push-plans/select — 業主選擇方案
app.post('/api/admin/push-plans/select', async (req, res) => {
  try {
    const { userId, groupId, planKey } = req.body;
    if (!userId || !groupId || !planKey) return res.status(400).json({ error: 'userId, groupId, planKey required' });

    // Verify plan exists
    const planRes = await db.query('SELECT * FROM push_plans WHERE plan_key=$1 AND is_active=true', [planKey]);
    if (planRes.rows.length === 0) return res.status(404).json({ error: 'Plan not found' });

    // Update store's selected plan and per_push_rate
    const plan = planRes.rows[0];
    await db.query(
      `INSERT INTO store_push_balance (group_id, selected_plan, per_push_rate, monthly_manual_limit)
       VALUES ($1, $2, $3, $4)
       ON CONFLICT (group_id) DO UPDATE SET selected_plan=$2, per_push_rate=$3, monthly_manual_limit=$4, updated_at=NOW()`,
      [groupId, planKey, plan.extra_rate, plan.included_messages]
    );

    res.json({ success: true, plan: plan, groupId });
  } catch (e) {
    console.error('[Push Plan Select]', e.message);
    res.status(500).json({ error: e.message });
  }
});

// GET /api/admin/push-plans/store/:groupId — 取得店家已選方案
app.get('/api/admin/push-plans/store/:groupId', async (req, res) => {
  try {
    const { groupId } = req.params;
    const result = await db.query(
      `SELECT spb.selected_plan, pp.*
       FROM store_push_balance spb
       LEFT JOIN push_plans pp ON spb.selected_plan = pp.plan_key
       WHERE spb.group_id = $1`,
      [groupId]
    );
    if (result.rows.length === 0) {
      // Default to light plan
      const defaultPlan = await db.query('SELECT * FROM push_plans WHERE plan_key = $1', ['light']);
      return res.json({ selected_plan: 'light', plan: defaultPlan.rows[0] || null });
    }
    res.json({ selected_plan: result.rows[0].selected_plan, plan: result.rows[0] });
  } catch (e) {
    console.error('[Push Plan Store]', e.message);
    res.status(500).json({ error: e.message });
  }
});

app.get('/health', (req, res) => res.json({ ok: true, time: new Date() }));

// Admin: cleanup duplicate roles
app.post('/api/admin/cleanup', async (req, res) => {
  try {
    const { userId } = req.body;
    const roleInfo = await getUserRole(userId);
    if (roleInfo.role !== 'super_admin') return res.status(403).json({ error: 'forbidden' });
    const result = await db.query(`
      DELETE FROM user_roles WHERE id NOT IN (
        SELECT MIN(id) FROM user_roles GROUP BY line_user_id, role, COALESCE(group_id, '__NULL__')
      )
    `);
    res.json({ success: true, deleted: result.rowCount });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ═══════════════════════════════════════
//  API: Member Level (spending-based)
// ═══════════════════════════════════════

const MEMBER_LEVELS = [
  { name: '鑽石', threshold: 8000, discount: 8 },
  { name: '金卡', threshold: 3000, discount: 5 },
  { name: '銀卡', threshold: 1000, discount: 2 },
  { name: '銅卡', threshold: 0, discount: 0 },
];

function calculateMemberLevel(totalSpent) {
  for (const lvl of MEMBER_LEVELS) {
    if (totalSpent >= lvl.threshold) {
      const currentIndex = MEMBER_LEVELS.indexOf(lvl);
      const nextLevel = currentIndex > 0 ? MEMBER_LEVELS[currentIndex - 1] : null;
      return {
        level: lvl.name,
        discount: lvl.discount,
        totalSpent,
        nextLevel: nextLevel ? nextLevel.name : null,
        nextThreshold: nextLevel ? nextLevel.threshold : null,
        progress: nextLevel
          ? Math.min(100, Math.round(((totalSpent - lvl.threshold) / (nextLevel.threshold - lvl.threshold)) * 100))
          : 100,
      };
    }
  }
  // Fallback (should not reach here)
  return { level: '銅卡', discount: 0, totalSpent, nextLevel: '銀卡', nextThreshold: 1000, progress: 0 };
}

app.get('/api/user/member-level', async (req, res) => {
  try {
    const { userId } = req.query;
    if (!userId) return res.status(400).json({ error: 'userId required' });

    const result = await db.query(
      `SELECT COALESCE(SUM(o.total_amount), 0) as total_spent
       FROM orders o
       JOIN members m ON o.member_id = m.id
       WHERE m.line_user_id = $1 AND o.status = 'completed'`,
      [userId]
    );

    const totalSpent = parseInt(result.rows[0].total_spent) || 0;
    const levelInfo = calculateMemberLevel(totalSpent);

    res.json(levelInfo);
  } catch (e) {
    console.error('member-level error:', e);
    res.status(500).json({ error: e.message });
  }
});

// ═══════════════════════════════════════
//  API: Referral System (LINE ID based)
// ═══════════════════════════════════════

app.post('/api/referral/apply', async (req, res) => {
  const client = await db.connect();
  try {
    const { userId, referralCode } = req.body;
    if (!userId || !referralCode) return res.status(400).json({ error: 'userId and referralCode required' });

    const code = referralCode.toUpperCase().trim();

    // Find referrer by last 6 chars of LINE user ID
    const referrerResult = await db.query(
      `SELECT line_user_id FROM members WHERE UPPER(RIGHT(line_user_id, 6)) = $1`,
      [code]
    );
    if (referrerResult.rows.length === 0) {
      return res.status(400).json({ error: '推薦碼無效' });
    }

    const referrerUserId = referrerResult.rows[0].line_user_id;

    // Cannot refer yourself
    if (referrerUserId === userId) {
      return res.status(400).json({ error: '不能使用自己的推薦碼' });
    }

    // Check if user has already been referred
    const existing = await db.query(
      'SELECT 1 FROM referrals WHERE referred_user_id = $1',
      [userId]
    );
    if (existing.rows.length > 0) {
      return res.status(400).json({ error: '您已使用過推薦碼' });
    }

    const rewardAmount = 30;

    await client.query('BEGIN');

    // Insert referral record
    await client.query(
      'INSERT INTO referrals (referrer_user_id, referred_user_id, reward_amount) VALUES ($1, $2, $3)',
      [referrerUserId, userId, rewardAmount]
    );

    // Reward referrer: add to all their group wallets
    const referrerWallets = await client.query(
      'SELECT group_id FROM wallets WHERE line_user_id = $1',
      [referrerUserId]
    );
    for (const w of referrerWallets.rows) {
      await client.query(
        `INSERT INTO wallets (line_user_id, group_id, balance) VALUES ($1, $2, $3)
         ON CONFLICT (line_user_id, group_id) DO UPDATE SET balance = wallets.balance + $3`,
        [referrerUserId, w.group_id, rewardAmount]
      );
      await client.query(
        `INSERT INTO transactions (line_user_id, group_id, type, amount, description, created_at)
         VALUES ($1, $2, 'referral_reward', $3, $4, NOW())`,
        [referrerUserId, w.group_id, rewardAmount, `推薦獎勵 (推薦碼 ${code})`]
      );
    }

    // Reward referred user: add to all their group wallets
    const referredWallets = await client.query(
      'SELECT group_id FROM wallets WHERE line_user_id = $1',
      [userId]
    );
    for (const w of referredWallets.rows) {
      await client.query(
        `INSERT INTO wallets (line_user_id, group_id, balance) VALUES ($1, $2, $3)
         ON CONFLICT (line_user_id, group_id) DO UPDATE SET balance = wallets.balance + $3`,
        [userId, w.group_id, rewardAmount]
      );
      await client.query(
        `INSERT INTO transactions (line_user_id, group_id, type, amount, description, created_at)
         VALUES ($1, $2, 'referral_bonus', $3, $4, NOW())`,
        [userId, w.group_id, rewardAmount, `新用戶推薦獎勵 (NT$${rewardAmount})`]
      );
    }

    await client.query('COMMIT');

    res.json({
      success: true,
      message: `推薦碼使用成功！您和推薦人各獲得 NT$${rewardAmount} 獎勵`,
      reward: rewardAmount,
    });
  } catch (e) {
    await client.query('ROLLBACK').catch(() => {});
    console.error('referral apply error:', e);
    res.status(500).json({ error: e.message });
  } finally {
    client.release();
  }
});

app.get('/api/referral/status', async (req, res) => {
  try {
    const { userId } = req.query;
    if (!userId) return res.status(400).json({ error: 'userId required' });

    // My referral code = last 6 chars of LINE user ID (uppercase)
    const myCode = userId.slice(-6).toUpperCase();

    // Has this user been referred?
    const referred = await db.query(
      'SELECT 1 FROM referrals WHERE referred_user_id = $1',
      [userId]
    );
    const hasBeenReferred = referred.rows.length > 0;

    // How many people has this user referred?
    const countResult = await db.query(
      'SELECT COUNT(*) as cnt, COALESCE(SUM(reward_amount), 0) as total_rewards FROM referrals WHERE referrer_user_id = $1',
      [userId]
    );
    const referralCount = parseInt(countResult.rows[0].cnt) || 0;
    const totalRewards = parseInt(countResult.rows[0].total_rewards) || 0;

    res.json({
      myCode,
      hasBeenReferred,
      referralCount,
      totalRewards,
    });
  } catch (e) {
    console.error('referral status error:', e);
    res.status(500).json({ error: e.message });
  }
});

// ═══════════════════════════════════════
//  Store Owner Admin Endpoints
// ═══════════════════════════════════════

// Check if user is a store owner
app.get('/api/owner/check', async (req, res) => {
  const { userId } = req.query;
  if (!userId) return res.status(400).json({ error: 'Missing userId' });
  try {
    const r = await db.query('SELECT * FROM store_owners WHERE user_id = $1 AND status = $2', [userId, 'active']);
    if (r.rows.length > 0) {
      const owner = r.rows[0];
      res.json({ isOwner: true, storeId: owner.store_id, ownerName: owner.owner_name, role: owner.role });
    } else {
      res.json({ isOwner: false });
    }
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Owner dashboard data (only their store)
app.get('/api/owner/dashboard', async (req, res) => {
  const { userId } = req.query;
  if (!userId) return res.status(400).json({ error: 'Missing userId' });
  try {
    const ownerR = await db.query('SELECT store_id FROM store_owners WHERE user_id = $1 AND status = $2', [userId, 'active']);
    if (ownerR.rows.length === 0) return res.status(403).json({ error: 'Not authorized' });
    const storeId = ownerR.rows[0].store_id;

    // Today's revenue
    const todayR = await db.query(
      `SELECT COALESCE(SUM(total_amount),0) as total, COUNT(*) as count FROM orders WHERE store_id = $1 AND created_at >= CURRENT_DATE AND status != 'cancelled'`,
      [storeId]
    );

    // This month's revenue
    const monthR = await db.query(
      `SELECT COALESCE(SUM(total_amount),0) as total, COUNT(*) as count FROM orders WHERE store_id = $1 AND created_at >= DATE_TRUNC('month', CURRENT_DATE) AND status != 'cancelled'`,
      [storeId]
    );

    // Machine states
    const machinesR = await db.query(
      `SELECT * FROM machine_current_state WHERE machine_id LIKE $1 ORDER BY machine_id`,
      [storeId + '%']
    );

    // Recent orders (last 20)
    const ordersR = await db.query(
      `SELECT * FROM orders WHERE store_id = $1 ORDER BY created_at DESC LIMIT 20`,
      [storeId]
    );

    // Unique customers this month
    const customersR = await db.query(
      `SELECT COUNT(DISTINCT member_id) as count FROM orders WHERE store_id = $1 AND created_at >= DATE_TRUNC('month', CURRENT_DATE)`,
      [storeId]
    );

    res.json({
      storeId,
      today: { revenue: parseInt(todayR.rows[0].total), orders: parseInt(todayR.rows[0].count) },
      month: { revenue: parseInt(monthR.rows[0].total), orders: parseInt(monthR.rows[0].count) },
      customers: parseInt(customersR.rows[0].count),
      machines: machinesR.rows,
      recentOrders: ordersR.rows
    });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Owner's machines only
app.get('/api/owner/machines', async (req, res) => {
  const { userId } = req.query;
  if (!userId) return res.status(400).json({ error: 'Missing userId' });
  try {
    const ownerR = await db.query('SELECT store_id FROM store_owners WHERE user_id = $1 AND status = $2', [userId, 'active']);
    if (ownerR.rows.length === 0) return res.status(403).json({ error: 'Not authorized' });
    const storeId = ownerR.rows[0].store_id;
    const r = await db.query(
      `SELECT * FROM machine_current_state WHERE machine_id LIKE $1 ORDER BY machine_id`,
      [storeId + '%']
    );
    res.json({ machines: r.rows, storeId });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Owner revenue report
app.get('/api/owner/revenue', async (req, res) => {
  const { userId, period = 'daily', days = 30 } = req.query;
  if (!userId) return res.status(400).json({ error: 'Missing userId' });
  try {
    // Check owner or admin
    let storeId = null;
    const ownerR = await db.query('SELECT store_id FROM store_owners WHERE user_id = $1 AND status = $2', [userId, 'active']);
    if (ownerR.rows.length > 0) {
      storeId = ownerR.rows[0].store_id;
    }
    // If not owner, check if super_admin (storeId stays null = all stores)

    const safeDays = parseInt(days) || 30;
    let dateGroup;
    if (period === 'weekly') {
      dateGroup = `DATE_TRUNC('week', created_at)`;
    } else if (period === 'monthly') {
      dateGroup = `DATE_TRUNC('month', created_at)`;
    } else {
      dateGroup = `DATE(created_at)`;
    }

    const params = [safeDays];
    let pidx = 2;
    let query = `SELECT ${dateGroup} as date, COALESCE(SUM(total_amount),0) as revenue, COUNT(*) as orders
      FROM orders WHERE created_at >= NOW() - make_interval(days => $1::int) AND status != 'cancelled'`;
    if (storeId) {
      query += ` AND store_id = $${pidx}`;
      params.push(storeId);
      pidx++;
    }
    query += ` GROUP BY ${dateGroup} ORDER BY date ASC`;

    const r = await db.query(query, params);

    // Top modes
    const modeParams = [safeDays];
    let mpidx = 2;
    let modeQuery = `SELECT mode, COUNT(*) as count, COALESCE(SUM(total_amount),0) as revenue FROM orders WHERE created_at >= NOW() - make_interval(days => $1::int) AND status != 'cancelled'`;
    if (storeId) {
      modeQuery += ` AND store_id = $${mpidx}`;
      modeParams.push(storeId);
      mpidx++;
    }
    modeQuery += ` GROUP BY mode ORDER BY count DESC LIMIT 5`;
    const modeR = await db.query(modeQuery, modeParams);

    // Peak hours
    const peakParams = [safeDays];
    let ppidx = 2;
    let peakQuery = `SELECT EXTRACT(HOUR FROM created_at) as hour, COUNT(*) as count FROM orders WHERE created_at >= NOW() - make_interval(days => $1::int) AND status != 'cancelled'`;
    if (storeId) {
      peakQuery += ` AND store_id = $${ppidx}`;
      peakParams.push(storeId);
      ppidx++;
    }
    peakQuery += ` GROUP BY hour ORDER BY hour`;
    const peakR = await db.query(peakQuery, peakParams);

    res.json({
      period,
      data: r.rows.map(row => ({ date: row.date, revenue: parseInt(row.revenue), orders: parseInt(row.orders) })),
      topModes: modeR.rows,
      peakHours: peakR.rows.map(row => ({ hour: parseInt(row.hour), count: parseInt(row.count) })),
      storeId
    });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Super admin: add a store owner
app.post('/api/admin/store-owners', async (req, res) => {
  const { userId, storeId, ownerName, phone, adminUserId } = req.body;
  if (!userId || !storeId) return res.status(400).json({ error: 'Missing userId or storeId' });
  try {
    // Require super_admin to create/update store owners
    const roleInfo = await getUserRole(adminUserId || req.query.userId);
    if (roleInfo.role !== 'super_admin') return res.status(403).json({ error: 'Forbidden: super_admin only' });
    await db.query(
      `INSERT INTO store_owners (user_id, store_id, owner_name, phone)
       VALUES ($1,$2,$3,$4)
       ON CONFLICT (user_id) DO UPDATE SET store_id=$2, owner_name=$3, phone=$4`,
      [userId, storeId, ownerName || null, phone || null]
    );
    res.json({ success: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Super admin: list all store owners
app.get('/api/admin/store-owners', async (req, res) => {
  try {
    const r = await db.query('SELECT * FROM store_owners ORDER BY created_at DESC');
    res.json({ owners: r.rows });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Super admin: revenue report (with optional storeId filter)
app.get('/api/admin/revenue', async (req, res) => {
  const { period = 'daily', days = 30, storeId } = req.query;
  try {
    const safeDays = parseInt(days) || 30;
    let dateGroup;
    if (period === 'weekly') {
      dateGroup = `DATE_TRUNC('week', created_at)`;
    } else if (period === 'monthly') {
      dateGroup = `DATE_TRUNC('month', created_at)`;
    } else {
      dateGroup = `DATE(created_at)`;
    }

    const params = [safeDays];
    let pidx = 2;
    let query = `SELECT ${dateGroup} as date, COALESCE(SUM(total_amount),0) as revenue, COUNT(*) as orders
      FROM orders WHERE created_at >= NOW() - make_interval(days => $1::int) AND status != 'cancelled'`;
    if (storeId) {
      query += ` AND store_id = $${pidx}`;
      params.push(storeId);
      pidx++;
    }
    query += ` GROUP BY ${dateGroup} ORDER BY date ASC`;
    const r = await db.query(query, params);

    // Per-store breakdown
    const storeParams = [safeDays];
    let spidx = 2;
    let storeQuery = `SELECT store_id, COALESCE(SUM(total_amount),0) as revenue, COUNT(*) as orders
      FROM orders WHERE created_at >= NOW() - make_interval(days => $1::int) AND status != 'cancelled'`;
    if (storeId) {
      storeQuery += ` AND store_id = $${spidx}`;
      storeParams.push(storeId);
      spidx++;
    }
    storeQuery += ` GROUP BY store_id ORDER BY revenue DESC`;
    const storeR = await db.query(storeQuery, storeParams);

    res.json({
      period,
      data: r.rows.map(row => ({ date: row.date, revenue: parseInt(row.revenue), orders: parseInt(row.orders) })),
      storeBreakdown: storeR.rows.map(row => ({ storeId: row.store_id, revenue: parseInt(row.revenue), orders: parseInt(row.orders) })),
      storeId: storeId || 'all'
    });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ═══════════════════════════════════════
//  MQTT WebSocket Config Endpoint
// ═══════════════════════════════════════

app.get('/api/mqtt/config', (req, res) => {
  const { userId } = req.query;
  // Validate userId is a valid LINE user ID format (U + 32 hex chars)
  if (!userId || !/^U[0-9a-f]{32}$/.test(userId)) {
    return res.status(401).json({ error: 'Valid userId required' });
  }
  res.json({
    host: process.env.MQTT_HOST || 'f29e89cd32414b9c826381e76ef8baaf.s1.eu.hivemq.cloud',
    port: 8884,
    protocol: 'wss',
    username: process.env.MQTT_USER || 'ypure-iot',
    // Do not expose raw password; client should use read-only topics only
  });
});

// ═══════════════════════════════════════
//  Database Initialization
// ═══════════════════════════════════════
async function initDB() {
  // Original tables
  await db.query(`
    CREATE TABLE IF NOT EXISTS stores (id VARCHAR(20) PRIMARY KEY, name VARCHAR(100), address VARCHAR(200), phone VARCHAR(20), group_id VARCHAR(20));
    CREATE TABLE IF NOT EXISTS machines (id VARCHAR(30) PRIMARY KEY, store_id VARCHAR(20), name VARCHAR(50), size VARCHAR(10), sort_order INT DEFAULT 0, active BOOLEAN DEFAULT true);
    CREATE TABLE IF NOT EXISTS machine_current_state (machine_id VARCHAR(30) PRIMARY KEY, state VARCHAR(20) DEFAULT 'unknown', remain_sec INT DEFAULT 0, progress INT DEFAULT 0, wifi_rssi INT DEFAULT 0, updated_at TIMESTAMPTZ DEFAULT NOW());
    CREATE TABLE IF NOT EXISTS members (id VARCHAR(40) PRIMARY KEY, line_user_id VARCHAR(60) UNIQUE, wallet INT DEFAULT 0, is_admin BOOLEAN DEFAULT false, created_at TIMESTAMPTZ DEFAULT NOW());
    CREATE TABLE IF NOT EXISTS orders (id VARCHAR(30) PRIMARY KEY, member_id VARCHAR(40), store_id VARCHAR(20), machine_id VARCHAR(30), mode VARCHAR(20), addons JSONB DEFAULT '[]', extend_min INT DEFAULT 0, temp VARCHAR(10), total_amount INT, pulses INT, duration_sec INT, status VARCHAR(20) DEFAULT 'pending', paid_at TIMESTAMPTZ, completed_at TIMESTAMPTZ, created_at TIMESTAMPTZ DEFAULT NOW());
  `);

  // Add machine management columns
  await db.query(`
    ALTER TABLE machines ADD COLUMN IF NOT EXISTS machine_type VARCHAR(30) DEFAULT 'combo';
    ALTER TABLE machines ADD COLUMN IF NOT EXISTS protocol VARCHAR(30) DEFAULT 'sx176005a';
    ALTER TABLE machines ADD COLUMN IF NOT EXISTS modbus_slave INT DEFAULT 0;
    ALTER TABLE machines ADD COLUMN IF NOT EXISTS tb_device VARCHAR(30) DEFAULT '';
    ALTER TABLE machines ADD COLUMN IF NOT EXISTS description TEXT DEFAULT '';
  `);

  // New multi-tenant tables
  await db.query(`
    CREATE TABLE IF NOT EXISTS store_groups (
      id VARCHAR(20) PRIMARY KEY,
      name VARCHAR(100) NOT NULL,
      type VARCHAR(20) NOT NULL DEFAULT 'independent',
      phone VARCHAR(20) DEFAULT '0800-018-888',
      created_at TIMESTAMPTZ DEFAULT NOW()
    );
    CREATE TABLE IF NOT EXISTS wallets (
      id SERIAL PRIMARY KEY,
      line_user_id VARCHAR(60) NOT NULL,
      group_id VARCHAR(20) NOT NULL REFERENCES store_groups(id),
      balance INT DEFAULT 0,
      UNIQUE(line_user_id, group_id)
    );
    CREATE TABLE IF NOT EXISTS user_roles (
      id SERIAL PRIMARY KEY,
      line_user_id VARCHAR(60) NOT NULL,
      role VARCHAR(20) NOT NULL DEFAULT 'consumer',
      group_id VARCHAR(20) REFERENCES store_groups(id),
      UNIQUE(line_user_id, role, group_id)
    );
    CREATE TABLE IF NOT EXISTS transactions (
      id SERIAL PRIMARY KEY,
      line_user_id VARCHAR(60),
      group_id VARCHAR(20),
      type VARCHAR(20) NOT NULL,
      amount INT NOT NULL,
      description TEXT,
      created_at TIMESTAMPTZ DEFAULT NOW()
    );
  `);

  // Add group_id column to stores if not exists
  try {
    await db.query(`ALTER TABLE stores ADD COLUMN IF NOT EXISTS group_id VARCHAR(20)`);
    await db.query('ALTER TABLE orders ADD COLUMN IF NOT EXISTS machine_id VARCHAR(20)').catch(() => {});
    await db.query(`ALTER TABLE members ADD COLUMN IF NOT EXISTS display_name VARCHAR(100)`).catch(() => {});
    await db.query(`ALTER TABLE members ADD COLUMN IF NOT EXISTS picture_url TEXT`).catch(() => {});
    await db.query(`ALTER TABLE members ADD COLUMN IF NOT EXISTS last_login TIMESTAMPTZ`).catch(() => {});
    // machine_current_state: add new IoT telemetry columns
    await db.query(`ALTER TABLE machine_current_state ADD COLUMN IF NOT EXISTS store_id VARCHAR(20) DEFAULT ''`).catch(() => {});
    await db.query(`ALTER TABLE machine_current_state ADD COLUMN IF NOT EXISTS status VARCHAR(20) DEFAULT 'unknown'`).catch(() => {});
    await db.query(`ALTER TABLE machine_current_state ADD COLUMN IF NOT EXISTS remaining INT DEFAULT 0`).catch(() => {});
    await db.query(`ALTER TABLE machine_current_state ADD COLUMN IF NOT EXISTS mode VARCHAR(30) DEFAULT ''`).catch(() => {});
    await db.query(`ALTER TABLE machine_current_state ADD COLUMN IF NOT EXISTS temperature NUMERIC DEFAULT 0`).catch(() => {});
    await db.query(`ALTER TABLE machine_current_state ADD COLUMN IF NOT EXISTS rpm INT DEFAULT 0`).catch(() => {});
    await db.query(`ALTER TABLE machine_current_state ADD COLUMN IF NOT EXISTS power NUMERIC DEFAULT 0`).catch(() => {});
    await db.query(`ALTER TABLE machine_current_state ADD COLUMN IF NOT EXISTS water_level NUMERIC DEFAULT 0`).catch(() => {});
    await db.query(`ALTER TABLE machine_current_state ADD COLUMN IF NOT EXISTS coin_count INT DEFAULT 0`).catch(() => {});
    await db.query(`ALTER TABLE machine_current_state ADD COLUMN IF NOT EXISTS coin_total INT DEFAULT 0`).catch(() => {});
    // Admin user role extra fields
    await db.query(`ALTER TABLE user_roles ADD COLUMN IF NOT EXISTS display_name VARCHAR(100)`).catch(() => {});
    await db.query(`ALTER TABLE user_roles ADD COLUMN IF NOT EXISTS phone VARCHAR(20)`).catch(() => {});
    await db.query(`ALTER TABLE user_roles ADD COLUMN IF NOT EXISTS notes TEXT`).catch(() => {});
    await db.query(`ALTER TABLE user_roles ADD COLUMN IF NOT EXISTS permissions JSONB DEFAULT '{}'`).catch(() => {});
    // Admin coupons table
    await db.query(`
      CREATE TABLE IF NOT EXISTS admin_coupons (
        id SERIAL PRIMARY KEY,
        group_id VARCHAR(20) REFERENCES store_groups(id),
        name VARCHAR(100) NOT NULL,
        discount INT NOT NULL DEFAULT 0,
        type VARCHAR(20) NOT NULL DEFAULT 'fixed',
        min_spend INT DEFAULT 0,
        price INT DEFAULT 0,
        expiry DATE DEFAULT '2026-12-31',
        active BOOLEAN DEFAULT true,
        description TEXT DEFAULT '',
        time_slot VARCHAR(100) DEFAULT '全時段可用',
        device_limit VARCHAR(100) DEFAULT '全機型適用',
        validity VARCHAR(100) DEFAULT '自購買起90日內有效',
        store_scope VARCHAR(200) DEFAULT '全台門市',
        max_uses INT DEFAULT 0,
        category VARCHAR(20) DEFAULT 'gift',
        created_at TIMESTAMPTZ DEFAULT NOW()
      )
    `).catch(() => {});
    // Add new columns if not exist
    await db.query(`ALTER TABLE admin_coupons ADD COLUMN IF NOT EXISTS notes TEXT DEFAULT ''`).catch(() => {});
    await db.query(`ALTER TABLE admin_coupons ADD COLUMN IF NOT EXISTS refund_policy TEXT DEFAULT ''`).catch(() => {});
    await db.query(`ALTER TABLE admin_coupons ADD COLUMN IF NOT EXISTS redeem_code VARCHAR(50) DEFAULT ''`).catch(() => {});
    // User coupons table (tracks purchased coupons per user)
    await db.query(`
      CREATE TABLE IF NOT EXISTS user_coupons (
        id SERIAL PRIMARY KEY,
        line_user_id VARCHAR(100) NOT NULL,
        coupon_id INT REFERENCES admin_coupons(id) ON DELETE CASCADE,
        group_id VARCHAR(20),
        uses_remaining INT DEFAULT 0,
        expiry_date DATE,
        status VARCHAR(20) DEFAULT 'active',
        purchased_at TIMESTAMPTZ DEFAULT NOW()
      )
    `).catch(() => {});
    // Add new coupon columns if not exist
    for (const col of ['price INT DEFAULT 0','description TEXT','time_slot VARCHAR(100)','device_limit VARCHAR(100)','validity VARCHAR(100)','store_scope VARCHAR(200)','max_uses INT DEFAULT 0','category VARCHAR(20) DEFAULT \'gift\'']) {
      await db.query(`ALTER TABLE admin_coupons ADD COLUMN IF NOT EXISTS ${col}`).catch(() => {});
    }
    await db.query(`ALTER TABLE store_groups ADD COLUMN IF NOT EXISTS topup_enabled BOOLEAN DEFAULT true`).catch(() => {});
    await db.query(`ALTER TABLE store_groups ADD COLUMN IF NOT EXISTS support_url VARCHAR(500) DEFAULT ''`).catch(() => {});
    await db.query(`ALTER TABLE store_groups ADD COLUMN IF NOT EXISTS support_phone VARCHAR(30) DEFAULT ''`).catch(() => {});
    // Announcements table
    await db.query(`
      CREATE TABLE IF NOT EXISTS announcements (
        id SERIAL PRIMARY KEY,
        group_id VARCHAR(20) REFERENCES store_groups(id),
        title VARCHAR(200) NOT NULL,
        content TEXT DEFAULT '',
        image_url TEXT DEFAULT '',
        link_url TEXT DEFAULT '',
        published BOOLEAN DEFAULT true,
        sort_order INT DEFAULT 0,
        created_at TIMESTAMPTZ DEFAULT NOW(),
        updated_at TIMESTAMPTZ DEFAULT NOW()
      )
    `).catch(() => {});
    await db.query(`ALTER TABLE announcements ADD COLUMN IF NOT EXISTS tag VARCHAR(20) DEFAULT ''`).catch(() => {});

    // Seed default announcements if table is empty
    await db.query(`
      INSERT INTO announcements (title, content, tag, published, sort_order, created_at)
      SELECT '我們一直都在',
             E'在生活節奏越來越快的都市中，自助洗衣已成為許多人生活中不可或缺的一環。雲管家深知，對顧客來說「乾淨」從來不只是衣服的狀態，更是整體洗衣體驗的基礎。\n\n為了提供最安心、舒適的自助洗衣空間，我們雲管家洗衣團隊每週都會到店巡檢，默默守護大家的洗衣時光。我們定期補充洗衣用品、清潔環境、維護設備、檢查濾網、確認洗衣機運作狀況，只為了讓每位來洗衣的你都能感受到一種被照顧的安心。\n\n我們相信，洗衣不是把衣服丟進機器就好，而是日常中一種「讓生活更有秩序」的儀式感。我們會繼續努力，成為你生活裡最可靠的洗衣夥伴。',
             '原創', true, 1, '2026-03-20'
      WHERE NOT EXISTS (SELECT 1 FROM announcements LIMIT 1)
    `).catch(() => {});
    await db.query(`
      INSERT INTO announcements (title, content, tag, published, sort_order, created_at)
      SELECT '升級會員洗衣更輕鬆',
             E'雲管家全面升級會員系統，打造更智慧、便利的洗衣體驗！新系統支援點數儲值、優惠券管理、機器狀態即時查詢等功能，讓你的洗衣生活更加輕鬆便利。\n\n透過LINE官方帳號即可快速註冊，享受專屬會員福利。',
             '系統', true, 2, '2026-03-18'
      WHERE NOT EXISTS (SELECT 1 FROM announcements LIMIT 1)
    `).catch(() => {});
    await db.query(`
      INSERT INTO announcements (title, content, tag, published, sort_order, created_at)
      SELECT '專屬你的洗衣錢包',
             E'雲管家全新線上會員功能來囉！會員可透過LINE官方帳號管理點數、查看交易紀錄、領取優惠券。\n\n首次加入會員即贈50元洗衣折扣券，立即加入享受專屬優惠！',
             '功能', true, 3, '2026-03-15'
      WHERE NOT EXISTS (SELECT 1 FROM announcements LIMIT 1)
    `).catch(() => {});
    await db.query(`
      INSERT INTO announcements (title, content, tag, published, sort_order, created_at)
      SELECT '夜猫洗衣全年最划算',
             E'你也是「夜猫洗衣族」嗎？深夜洗衣機台不用排隊，空間獨享更自在！\n\n夜猫計畫提供01:00-07:00時段專屬優惠，年卡方案每次洗衣只要65點，是一般價格的31折！立即購買夜猫計畫，享受最划算的深夜洗衣體驗。',
             '優惠', true, 4, '2026-03-10'
      WHERE NOT EXISTS (SELECT 1 FROM announcements LIMIT 1)
    `).catch(() => {});

    // Coin records table for tracking coin box clears
    await db.query(`
      CREATE TABLE IF NOT EXISTS coin_records (
        id SERIAL PRIMARY KEY,
        store_id VARCHAR(20),
        machine_id VARCHAR(30),
        coin_count INT DEFAULT 0,
        cleared_by VARCHAR(100),
        cleared_at TIMESTAMPTZ DEFAULT NOW()
      )
    `).catch(() => {});
    // Add type column to orders for distinguishing coin_payment vs normal orders
    await db.query(`ALTER TABLE orders ADD COLUMN IF NOT EXISTS type VARCHAR(30) DEFAULT 'online'`).catch(() => {});
    // Add payment_method column to orders for distinguishing wallet vs linepay vs coin
    await db.query(`ALTER TABLE orders ADD COLUMN IF NOT EXISTS payment_method VARCHAR(30) DEFAULT 'wallet'`).catch(() => {});
    // Add coupon columns to orders for coupon system
    await db.query(`ALTER TABLE orders ADD COLUMN IF NOT EXISTS coupon_id INT DEFAULT NULL`).catch(() => {});
    await db.query(`ALTER TABLE orders ADD COLUMN IF NOT EXISTS discount_amount INT DEFAULT 0`).catch(() => {});

    // Member levels table
    await db.query(`
      CREATE TABLE IF NOT EXISTS member_levels (
        id SERIAL PRIMARY KEY,
        name VARCHAR(50) NOT NULL,
        min_points INT DEFAULT 0,
        discount_percent INT DEFAULT 0,
        benefits TEXT DEFAULT '',
        icon VARCHAR(10) DEFAULT '',
        created_at TIMESTAMPTZ DEFAULT NOW()
      )
    `).catch(() => {});

    // Referral codes table
    await db.query(`
      CREATE TABLE IF NOT EXISTS referral_codes (
        id SERIAL PRIMARY KEY,
        line_user_id VARCHAR(100) NOT NULL,
        code VARCHAR(20) UNIQUE NOT NULL,
        uses INT DEFAULT 0,
        reward_points INT DEFAULT 50,
        created_at TIMESTAMPTZ DEFAULT NOW()
      )
    `).catch(() => {});

    // Referral records table
    await db.query(`
      CREATE TABLE IF NOT EXISTS referral_records (
        id SERIAL PRIMARY KEY,
        referrer_id VARCHAR(100) NOT NULL,
        referred_id VARCHAR(100) NOT NULL,
        code VARCHAR(20) NOT NULL,
        reward_given BOOLEAN DEFAULT false,
        created_at TIMESTAMPTZ DEFAULT NOW()
      )
    `).catch(() => {});

    // Push logs table (LINE push billing tracking)
    await db.query(`
      CREATE TABLE IF NOT EXISTS push_logs (
        id SERIAL PRIMARY KEY,
        group_id VARCHAR(20),
        store_id VARCHAR(20),
        push_type VARCHAR(30) NOT NULL,
        recipient_count INT DEFAULT 1,
        message_count INT DEFAULT 1,
        triggered_by VARCHAR(100),
        target_user_id VARCHAR(100),
        description TEXT,
        success BOOLEAN DEFAULT true,
        created_at TIMESTAMPTZ DEFAULT NOW()
      )
    `).catch(() => {});
    // Index for faster queries on push_logs
    await db.query(`CREATE INDEX IF NOT EXISTS idx_push_logs_created_at ON push_logs (created_at)`).catch(() => {});
    await db.query(`CREATE INDEX IF NOT EXISTS idx_push_logs_group_id ON push_logs (group_id)`).catch(() => {});

    // Scheduled broadcasts table
    await db.query(`
      CREATE TABLE IF NOT EXISTS scheduled_broadcasts (
        id SERIAL PRIMARY KEY,
        messages JSONB NOT NULL,
        target_tags TEXT[],
        exclude_tags TEXT[],
        scheduled_at TIMESTAMPTZ NOT NULL,
        status TEXT DEFAULT 'pending',
        sent_count INT DEFAULT 0,
        created_by TEXT NOT NULL,
        created_at TIMESTAMPTZ DEFAULT NOW(),
        sent_at TIMESTAMPTZ
      )
    `).catch(() => {});
    await db.query(`ALTER TABLE scheduled_broadcasts ADD COLUMN IF NOT EXISTS target_store_ids TEXT[]`).catch(() => {});

    // Store push balance & topup tables
    await db.query(`
      CREATE TABLE IF NOT EXISTS store_push_balance (
        id SERIAL PRIMARY KEY,
        group_id VARCHAR(20) NOT NULL UNIQUE,
        balance NUMERIC(10,2) DEFAULT 0,
        per_push_rate NUMERIC(6,4) DEFAULT 0.16,
        monthly_manual_limit INT DEFAULT 500,
        updated_at TIMESTAMPTZ DEFAULT NOW()
      )
    `).catch(() => {});
    await db.query(`
      CREATE TABLE IF NOT EXISTS store_push_topup (
        id SERIAL PRIMARY KEY,
        group_id VARCHAR(20) NOT NULL,
        amount NUMERIC(10,2) NOT NULL,
        method VARCHAR(30) DEFAULT 'manual',
        note TEXT DEFAULT '',
        created_by VARCHAR(100),
        created_at TIMESTAMPTZ DEFAULT NOW()
      )
    `).catch(() => {});
    await db.query(`ALTER TABLE push_logs ADD COLUMN IF NOT EXISTS cost NUMERIC(10,2) DEFAULT 0`).catch(() => {});

    // Push plans table
    await db.query(`
      CREATE TABLE IF NOT EXISTS push_plans (
        id SERIAL PRIMARY KEY,
        plan_key VARCHAR(30) UNIQUE NOT NULL,
        label VARCHAR(50) NOT NULL,
        monthly_fee NUMERIC(10,0) DEFAULT 0,
        included_messages INT DEFAULT 0,
        extra_rate NUMERIC(6,2) DEFAULT 0,
        can_exceed BOOLEAN DEFAULT true,
        is_active BOOLEAN DEFAULT true,
        sort_order INT DEFAULT 0,
        created_at TIMESTAMP DEFAULT NOW(),
        updated_at TIMESTAMP DEFAULT NOW()
      )
    `).catch(() => {});

    // Add selected_plan column to store_push_balance
    await db.query(`ALTER TABLE store_push_balance ADD COLUMN IF NOT EXISTS selected_plan VARCHAR(30) DEFAULT 'light'`).catch(() => {});

    // Referrals table (simple referral tracking by LINE user ID)
    await db.query(`
      CREATE TABLE IF NOT EXISTS referrals (
        id SERIAL PRIMARY KEY,
        referrer_user_id VARCHAR(100) NOT NULL,
        referred_user_id VARCHAR(100) NOT NULL UNIQUE,
        reward_amount INT DEFAULT 30,
        status VARCHAR(20) DEFAULT 'completed',
        created_at TIMESTAMPTZ DEFAULT NOW()
      )
    `).catch(() => {});

    // Store owners table for owner admin portal
    await db.query(`
      CREATE TABLE IF NOT EXISTS store_owners (
        id SERIAL PRIMARY KEY,
        user_id VARCHAR(100) UNIQUE NOT NULL,
        store_id VARCHAR(20) NOT NULL,
        owner_name VARCHAR(100),
        phone VARCHAR(20),
        role VARCHAR(20) DEFAULT 'owner',
        status VARCHAR(20) DEFAULT 'active',
        created_at TIMESTAMP DEFAULT NOW()
      )
    `).catch(() => {});
    // User tags table (LINE OA auto-tagging & manual tagging)
    await db.query(`
      CREATE TABLE IF NOT EXISTS user_tags (
        id SERIAL PRIMARY KEY,
        line_user_id VARCHAR(100) NOT NULL,
        tag VARCHAR(50) NOT NULL,
        source VARCHAR(30) DEFAULT 'auto',
        created_at TIMESTAMPTZ DEFAULT NOW(),
        UNIQUE(line_user_id, tag)
      )
    `).catch(() => {});
    await db.query(`CREATE INDEX IF NOT EXISTS idx_user_tags_user ON user_tags (line_user_id)`).catch(() => {});
    await db.query(`CREATE INDEX IF NOT EXISTS idx_user_tags_tag ON user_tags (tag)`).catch(() => {});

    // User-store bindings table (auto-bound on payment)
    await db.query(`
      CREATE TABLE IF NOT EXISTS user_store_bindings (
        id SERIAL PRIMARY KEY,
        line_user_id VARCHAR(100) NOT NULL,
        store_id VARCHAR(20) NOT NULL,
        bound_at TIMESTAMPTZ DEFAULT NOW(),
        UNIQUE(line_user_id, store_id)
      )
    `).catch(() => {});
    await db.query(`CREATE INDEX IF NOT EXISTS idx_user_store_bindings_user ON user_store_bindings (line_user_id)`).catch(() => {});
    await db.query(`CREATE INDEX IF NOT EXISTS idx_user_store_bindings_store ON user_store_bindings (store_id)`).catch(() => {});

    // Customer service sessions table
    await db.query(`
      CREATE TABLE IF NOT EXISTS customer_service_sessions (
        id SERIAL PRIMARY KEY,
        customer_line_id VARCHAR(100) NOT NULL,
        store_id VARCHAR(20) NOT NULL,
        store_admin_line_id VARCHAR(100),
        status VARCHAR(20) DEFAULT 'active',
        created_at TIMESTAMPTZ DEFAULT NOW(),
        ended_at TIMESTAMPTZ
      )
    `).catch(() => {});
    await db.query(`CREATE INDEX IF NOT EXISTS idx_cs_sessions_customer ON customer_service_sessions (customer_line_id, status)`).catch(() => {});
    await db.query(`CREATE INDEX IF NOT EXISTS idx_cs_sessions_admin ON customer_service_sessions (store_admin_line_id, status)`).catch(() => {});

    // B2B inquiries table
    await db.query(`
      CREATE TABLE IF NOT EXISTS b2b_inquiries (
        id SERIAL PRIMARY KEY,
        line_user_id VARCHAR(100) NOT NULL,
        display_name VARCHAR(100),
        contact_name VARCHAR(100),
        phone VARCHAR(30),
        plan_type VARCHAR(30),
        has_store BOOLEAN,
        store_name VARCHAR(200),
        store_address VARCHAR(500),
        machine_count INT,
        machine_brand VARCHAR(200),
        has_protocol BOOLEAN,
        protocol_info TEXT,
        store_size VARCHAR(50),
        monthly_revenue VARCHAR(50),
        timeline VARCHAR(100),
        notes TEXT,
        status VARCHAR(20) DEFAULT 'new',
        created_at TIMESTAMPTZ DEFAULT NOW()
      )
    `).catch(() => {});

    // Survey progress table
    await db.query(`
      CREATE TABLE IF NOT EXISTS survey_progress (
        line_user_id VARCHAR(100) PRIMARY KEY,
        current_step VARCHAR(30) DEFAULT 'name',
        data JSONB DEFAULT '{}',
        started_at TIMESTAMPTZ DEFAULT NOW(),
        updated_at TIMESTAMPTZ DEFAULT NOW()
      )
    `).catch(() => {});

  } catch (e) { /* column might already exist */ }

  // Seed store groups
  await db.query(`
    INSERT INTO store_groups (id, name, type, phone) VALUES
      ('sg1', '悠洗自助洗衣', 'independent', '0800-018-888'),
      ('sg2', '吼你洗自助洗衣', 'chain', '0800-018-888'),
      ('sg3', '熊愛洗自助洗衣', 'independent', '0800-018-888'),
      ('sg4', '上好洗自助洗衣', 'independent', '0800-018-888')
    ON CONFLICT (id) DO UPDATE SET name=EXCLUDED.name, type=EXCLUDED.type
  `);

  // Seed store push balance for each group
  await db.query(`
    INSERT INTO store_push_balance (group_id, balance, per_push_rate, monthly_manual_limit) VALUES
      ('sg1', 0, 0.16, 500),
      ('sg2', 0, 0.16, 500),
      ('sg3', 0, 0.16, 500),
      ('sg4', 0, 0.16, 500),
      ('sg5', 0, 0.16, 500)
    ON CONFLICT (group_id) DO NOTHING
  `).catch(() => {});

  // Seed default push plans
  await db.query(`
    INSERT INTO push_plans (plan_key, label, monthly_fee, included_messages, extra_rate, can_exceed, sort_order)
    VALUES
      ('free', '免費', 0, 200, 0, false, 1),
      ('light', '輕用量', 800, 4000, 0.2, true, 2),
      ('standard', '中用量', 4000, 25000, 0.16, true, 3),
      ('pro', '高用量', 10000, 100000, 0.1, true, 4)
    ON CONFLICT (plan_key) DO NOTHING
  `).catch(() => {});

  // Seed stores with group mapping
  await db.query(`DELETE FROM machines WHERE store_id IN ('s1','s2','s3','s4','s5')`);
  await db.query(`DELETE FROM stores WHERE id IN ('s1','s2','s3','s4','s5')`);
  await db.query(`
    INSERT INTO stores (id, name, address, phone, group_id) VALUES
      ('s1', '悠洗自助洗衣',           '嘉義市東區文雅街181號',       '0800-018-888', 'sg1'),
      ('s2', '吼你洗自助洗衣(玉清店)', '苗栗縣苗栗市玉清路51號',      '0800-018-888', 'sg2'),
      ('s3', '吼你洗自助洗衣(農會店)', '苗栗縣苗栗市為公路290號',     '0800-018-888', 'sg2'),
      ('s4', '熊愛洗自助洗衣',         '台中市西屯區福聯街22巷2號',    '0800-018-888', 'sg3'),
      ('s5', '上好洗自助洗衣',         '高雄市鳳山區北平路214號',      '0800-018-888', 'sg4')
    ON CONFLICT (id) DO UPDATE SET name=EXCLUDED.name, address=EXCLUDED.address, group_id=EXCLUDED.group_id
  `);

  // Seed machines (6 washers + 2 dryers per store)
  for (const sid of ['s1','s2','s3','s4','s5']) {
    for (let i = 1; i <= 6; i++) {
      const size = i <= 2 ? '大型' : i <= 5 ? '中型' : '小型';
      await db.query(
        `INSERT INTO machines (id, store_id, name, size, sort_order) VALUES ($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING`,
        [`${sid}-m${i}`, sid, `洗脫烘${i}號`, size, i]
      );
    }
    for (let i = 1; i <= 2; i++) {
      await db.query(
        `INSERT INTO machines (id, store_id, name, size, sort_order) VALUES ($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING`,
        [`${sid}-d${i}`, sid, `烘乾${i}號`, i === 1 ? '上' : '下', 10 + i]
      );
    }
  }

  // Create unique index that handles NULL group_id properly
  await db.query(`
    CREATE UNIQUE INDEX IF NOT EXISTS idx_user_roles_unique
    ON user_roles (line_user_id, role, COALESCE(group_id, '__NULL__'))
  `).catch(() => {});

  // Clean up duplicate super_admin entries
  await db.query(`
    DELETE FROM user_roles WHERE id NOT IN (
      SELECT MIN(id) FROM user_roles GROUP BY line_user_id, role, COALESCE(group_id, '__NULL__')
    )
  `).catch(() => {});

  // Seed super admin
  const SUPER_ADMIN_LINE_ID = process.env.SUPER_ADMIN_LINE_ID || 'Ubdcdd269e115bf9ac492288adbc0115e';
  const existing = await db.query('SELECT id FROM user_roles WHERE line_user_id=$1 AND role=$2 AND group_id IS NULL', [SUPER_ADMIN_LINE_ID, 'super_admin']);
  if (existing.rows.length === 0) {
    await db.query('INSERT INTO user_roles (line_user_id, role, group_id) VALUES ($1, $2, NULL)', [SUPER_ADMIN_LINE_ID, 'super_admin']);
  }

  // Seed member levels (if empty)
  const levelCount = await db.query('SELECT COUNT(*) FROM member_levels');
  if (parseInt(levelCount.rows[0].count) === 0) {
    await db.query(`
      INSERT INTO member_levels (name, min_points, discount_percent, benefits, icon) VALUES
        ('普通會員', 0, 0, '基本會員權益', '🌱'),
        ('銅牌會員', 500, 3, '消費享3%折扣', '🥉'),
        ('銀牌會員', 2000, 5, '消費享5%折扣、生日優惠', '🥈'),
        ('金牌會員', 5000, 8, '消費享8%折扣、生日優惠、專屬客服', '🥇'),
        ('鑽石會員', 10000, 12, '消費享12%折扣、生日優惠、專屬客服、免費烘乾券', '💎')
    `).catch(() => {});
  }

  console.log('DB initialized with multi-tenant tables');
}

// ===== LINE Webhook & Auto-Reply System (v2 - Comprehensive) =====

// ---- Core Utilities ----

// Reply to LINE message (uses reply token, free of charge)
async function lineReply(replyToken, messages) {
  if (!LINE_CHANNEL_ACCESS_TOKEN || !replyToken) return false;
  try {
    const res = await fetch('https://api.line.me/v2/bot/message/reply', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${LINE_CHANNEL_ACCESS_TOKEN}` },
      body: JSON.stringify({ replyToken, messages: messages.slice(0, 5) }),
    });
    if (!res.ok) console.error('[LINE Reply] Error:', await res.json().catch(() => ({})));
    return res.ok;
  } catch (e) { console.error('[LINE Reply] Failed:', e.message); return false; }
}

// Get LINE user profile
async function getLineProfile(userId) {
  try {
    const res = await fetch(`https://api.line.me/v2/bot/profile/${userId}`, {
      headers: { 'Authorization': `Bearer ${LINE_CHANNEL_ACCESS_TOKEN}` }
    });
    if (!res.ok) return null;
    return await res.json();
  } catch(e) { return null; }
}

// ---- Rich Menu Per-Audience Assignment ----
const RICH_MENU_IDS = {
  default: process.env.RICHMENU_DEFAULT_ID || '',
  b2c: process.env.RICHMENU_B2C_ID || '',
  b2b: process.env.RICHMENU_B2B_ID || '',
};

async function linkRichMenuToUser(userId, menuType) {
  const richMenuId = RICH_MENU_IDS[menuType];
  if (!richMenuId || !LINE_CHANNEL_ACCESS_TOKEN) {
    console.log(`[RichMenu] Skip: no ${menuType} menu ID configured`);
    return false;
  }
  try {
    const res = await fetch(`https://api.line.me/v2/bot/user/${userId}/richmenu/${richMenuId}`, {
      method: 'POST',
      headers: { 'Authorization': `Bearer ${LINE_CHANNEL_ACCESS_TOKEN}` },
    });
    if (!res.ok) {
      const err = await res.json().catch(() => ({}));
      console.error(`[RichMenu] Error linking ${menuType}:`, err);
    }
    console.log(`[RichMenu] Linked ${menuType} menu to ${userId}`);
    return res.ok;
  } catch (e) {
    console.error(`[RichMenu] Failed:`, e.message);
    return false;
  }
}

// Helper: add tag to user
async function addUserTag(lineUserId, tag, source = 'auto') {
  try {
    await db.query(
      `INSERT INTO user_tags (line_user_id, tag, source) VALUES ($1, $2, $3) ON CONFLICT (line_user_id, tag) DO NOTHING`,
      [lineUserId, tag, source]
    );
    return true;
  } catch (e) { console.error('[Tag] Error:', e.message); return false; }
}

// Helper: remove tag from user
async function removeUserTag(lineUserId, tag) {
  try {
    await db.query(`DELETE FROM user_tags WHERE line_user_id = $1 AND tag = $2`, [lineUserId, tag]);
    return true;
  } catch (e) { return false; }
}

// Track user interaction and auto-upgrade tags based on engagement
async function trackInteraction(userId, type) {
  try {
    // Upsert interaction counter
    await db.query(`
      INSERT INTO user_tags (line_user_id, tag, source)
      VALUES ($1, '_interaction_count_1', 'system')
      ON CONFLICT (line_user_id, tag) DO NOTHING
    `, [userId]);

    // Count current interactions by counting _interaction_count_ tags
    const countResult = await db.query(
      `SELECT COUNT(*) as cnt FROM user_tags WHERE line_user_id = $1 AND tag LIKE '_interaction_count_%'`,
      [userId]
    );
    const currentCount = parseInt(countResult.rows[0]?.cnt || '0');
    const newCount = currentCount + 1;

    // Add new interaction marker
    await db.query(`
      INSERT INTO user_tags (line_user_id, tag, source)
      VALUES ($1, $2, 'system')
      ON CONFLICT (line_user_id, tag) DO NOTHING
    `, [userId, `_interaction_count_${newCount}`]);

    console.log(`[Track] ${userId} interaction #${newCount} (${type})`);

    // Auto-upgrade logic: franchise observer -> evaluator
    if (newCount >= 5) {
      const hasObserve = await db.query(
        `SELECT 1 FROM user_tags WHERE line_user_id = $1 AND tag = $2`, [userId, '加盟_觀望']
      );
      if (hasObserve.rows.length > 0) {
        await removeUserTag(userId, '加盟_觀望');
        await addUserTag(userId, '加盟_評估中', 'auto_upgrade');
        console.log(`[Track] Auto-upgrade: ${userId} 加盟_觀望 -> 加盟_評估中`);
        // Send follow-up push
        try {
          await sendLinePush(userId, [{
            type: 'text',
            text: '嗨！看你對自助洗衣加盟持續關注中，我們整理了一份獲利分析報告，也許對你有幫助。\n\n回覆「獲利分析」即可查看，或直接預約免費 Demo！'
          }]);
        } catch (pushErr) {
          console.error('[Track] Push failed:', pushErr.message);
        }
      }
    }

    // Auto-upgrade logic: new member -> active member
    if (newCount >= 3) {
      const hasNew = await db.query(
        `SELECT 1 FROM user_tags WHERE line_user_id = $1 AND tag = $2`, [userId, '新會員']
      );
      if (hasNew.rows.length > 0) {
        await removeUserTag(userId, '新會員');
        await addUserTag(userId, '活躍會員', 'auto_upgrade');
        console.log(`[Track] Auto-upgrade: ${userId} 新會員 -> 活躍會員`);
      }
    }
  } catch (e) {
    console.error('[Track] Error:', e.message);
  }
}

// ---- Flex Message Builders ----

function buildWelcomeFlexMessage() {
  return {
    type: 'flex', altText: '歡迎來到 YPURE 雲管家！',
    contents: {
      type: 'bubble', size: 'mega',
      styles: {
        header: { backgroundColor: '#1A1A3A' },
        body: { backgroundColor: '#0D0D1A' },
        footer: { backgroundColor: '#0D0D1A', separator: false }
      },
      header: {
        type: 'box', layout: 'vertical',
        paddingAll: '24px', paddingBottom: '16px',
        contents: [
          {
            type: 'box', layout: 'vertical', backgroundColor: '#252547',
            cornerRadius: '12px', paddingAll: '16px', margin: 'none',
            contents: [
              { type: 'text', text: 'YPURE 雲管家', color: BRAND_GOLD, weight: 'bold', size: 'lg', align: 'center' },
              { type: 'text', text: 'IoT 智慧洗衣解決方案', color: '#999999', size: 'sm', align: 'center', margin: 'sm' }
            ]
          }
        ]
      },
      body: {
        type: 'box', layout: 'vertical', spacing: 'lg', paddingAll: '20px',
        contents: [
          { type: 'text', text: '歡迎！你是哪一種身份？', size: 'md', wrap: true, weight: 'bold', color: '#FFFFFF' },
          { type: 'text', text: '請選擇最符合你的選項，我們會提供最適合的資訊。', size: 'sm', wrap: true, color: '#999999', margin: 'sm' }
        ]
      },
      footer: {
        type: 'box', layout: 'vertical', spacing: 'md', paddingAll: '20px',
        contents: [
          {
            type: 'button', style: 'primary', color: '#E5B94C', height: 'md',
            action: { type: 'postback', label: '💼 我是店主／想開店', data: 'action=welcome_business', displayText: '我是店主／想開店' }
          },
          {
            type: 'button', style: 'primary', color: '#5B5BD6', height: 'md',
            action: { type: 'postback', label: '🧺 我是洗衣顧客', data: 'action=welcome_customer', displayText: '我是洗衣顧客' }
          },
          {
            type: 'button', style: 'primary', color: '#3A3A8C', height: 'md',
            action: { type: 'postback', label: '🤝 我想了解加盟合作', data: 'action=welcome_franchise', displayText: '我想了解加盟合作' }
          }
        ]
      }
    }
  };
}

// Customer welcome: returns ARRAY of messages (max 5)
function buildCustomerWelcomeReply() {
  const featureCard = {
    type: 'flex', altText: '歡迎使用雲管家洗衣服務！',
    contents: {
      type: 'bubble', size: 'mega',
      styles: {
        header: { backgroundColor: '#1A1A3A' },
        body: { backgroundColor: '#0D0D1A' },
        footer: { backgroundColor: '#0D0D1A', separator: false }
      },
      header: {
        type: 'box', layout: 'vertical', paddingAll: '20px',
        contents: [
          { type: 'text', text: '🧺 歡迎使用雲管家！', color: '#FFFFFF', weight: 'bold', size: 'lg' },
          { type: 'text', text: '全台 5 間門市為您服務', color: BRAND_GOLD, size: 'xs', margin: 'sm' }
        ]
      },
      body: {
        type: 'box', layout: 'vertical', spacing: 'lg', paddingAll: '20px',
        contents: [
          { type: 'text', text: '四大貼心功能', size: 'md', weight: 'bold', wrap: true, color: '#FFFFFF' },
          {
            type: 'box', layout: 'horizontal', margin: 'lg', spacing: 'md',
            contents: [
              { type: 'box', layout: 'vertical', flex: 1, spacing: 'xs', alignItems: 'center', contents: [
                { type: 'box', layout: 'vertical', backgroundColor: '#1E3A5F', cornerRadius: '8px', width: '40px', height: '40px', justifyContent: 'center', alignItems: 'center', contents: [
                  { type: 'text', text: '🔍', size: 'lg', align: 'center' }
                ]},
                { type: 'text', text: '查空機', size: 'xs', align: 'center', weight: 'bold', color: '#FFFFFF', margin: 'xs' },
                { type: 'text', text: '出門前先看', size: 'xxs', align: 'center', color: '#999999', wrap: true }
              ]},
              { type: 'box', layout: 'vertical', flex: 1, spacing: 'xs', alignItems: 'center', contents: [
                { type: 'box', layout: 'vertical', backgroundColor: '#1E5F3A', cornerRadius: '8px', width: '40px', height: '40px', justifyContent: 'center', alignItems: 'center', contents: [
                  { type: 'text', text: '📱', size: 'lg', align: 'center' }
                ]},
                { type: 'text', text: '手機付款', size: 'xs', align: 'center', weight: 'bold', color: '#FFFFFF', margin: 'xs' },
                { type: 'text', text: '免帶零錢', size: 'xxs', align: 'center', color: '#999999', wrap: true }
              ]},
              { type: 'box', layout: 'vertical', flex: 1, spacing: 'xs', alignItems: 'center', contents: [
                { type: 'box', layout: 'vertical', backgroundColor: '#5F3A1E', cornerRadius: '8px', width: '40px', height: '40px', justifyContent: 'center', alignItems: 'center', contents: [
                  { type: 'text', text: '🔔', size: 'lg', align: 'center' }
                ]},
                { type: 'text', text: '洗好通知', size: 'xs', align: 'center', weight: 'bold', color: '#FFFFFF', margin: 'xs' },
                { type: 'text', text: 'LINE提醒', size: 'xxs', align: 'center', color: '#999999', wrap: true }
              ]},
              { type: 'box', layout: 'vertical', flex: 1, spacing: 'xs', alignItems: 'center', contents: [
                { type: 'box', layout: 'vertical', backgroundColor: '#5F1E3A', cornerRadius: '8px', width: '40px', height: '40px', justifyContent: 'center', alignItems: 'center', contents: [
                  { type: 'text', text: '🎁', size: 'lg', align: 'center' }
                ]},
                { type: 'text', text: '優惠券', size: 'xs', align: 'center', weight: 'bold', color: '#FFFFFF', margin: 'xs' },
                { type: 'text', text: '專屬折扣', size: 'xxs', align: 'center', color: '#999999', wrap: true }
              ]}
            ]
          },
          { type: 'box', layout: 'vertical', height: '1px', backgroundColor: '#333355', margin: 'lg' },
          { type: 'text', text: '💡 儲值滿 NT$200 再送 NT$30，比投幣更划算！', size: 'sm', wrap: true, color: BRAND_GOLD, margin: 'md', weight: 'bold' }
        ]
      }
    }
  };

  const quickActionCard = {
    type: 'flex', altText: '快速操作',
    contents: {
      type: 'bubble', size: 'mega',
      styles: {
        body: { backgroundColor: '#0D0D1A' },
        footer: { backgroundColor: '#0D0D1A', separator: false }
      },
      body: {
        type: 'box', layout: 'vertical', spacing: 'md', paddingAll: '20px',
        contents: [
          { type: 'text', text: '快速開始', size: 'md', weight: 'bold', color: '#FFFFFF' },
          { type: 'text', text: '點選下方按鈕，馬上體驗！', size: 'sm', color: '#999999', margin: 'sm' }
        ]
      },
      footer: {
        type: 'box', layout: 'vertical', spacing: 'sm', paddingAll: '15px',
        contents: [
          { type: 'button', style: 'primary', color: '#5B5BD6', action: { type: 'uri', label: '立即查空機', uri: LIFF_WASH } },
          { type: 'button', style: 'primary', color: '#4CAF50', action: { type: 'postback', label: '首次儲值送 $20', data: 'action=topup_intro', displayText: '我想儲值' } },
          { type: 'button', style: 'link', color: BRAND_GOLD, action: { type: 'postback', label: '查看附近門市', data: 'action=show_stores', displayText: '門市在哪裡？' } }
        ]
      }
    }
  };

  return [featureCard, quickActionCard];
}

// Business welcome: returns ARRAY of messages (B2B focused)
function buildBusinessWelcomeReply() {
  const overviewCard = {
    type: 'flex', altText: '歡迎了解雲管家系統',
    contents: {
      type: 'bubble', size: 'mega',
      styles: {
        header: { backgroundColor: '#1A1A3A' },
        body: { backgroundColor: '#0D0D1A' },
        footer: { backgroundColor: '#0D0D1A', separator: false }
      },
      header: {
        type: 'box', layout: 'vertical', paddingAll: '20px',
        contents: [
          { type: 'text', text: '💼 歡迎了解雲管家系統', color: BRAND_GOLD, weight: 'bold', size: 'lg' }
        ]
      },
      body: {
        type: 'box', layout: 'vertical', spacing: 'md', paddingAll: '20px',
        contents: [
          { type: 'text', text: '雲管家是專為自助洗衣店打造的 IoT 管理系統', size: 'sm', wrap: true, color: '#E0E0E0', weight: 'bold' },
          {
            type: 'box', layout: 'horizontal', margin: 'lg', contents: [
              { type: 'box', layout: 'vertical', backgroundColor: '#1E3A5F', cornerRadius: '8px', width: '36px', height: '36px', justifyContent: 'center', alignItems: 'center', flex: 0, contents: [
                { type: 'text', text: '📱', size: 'md', align: 'center' }
              ]},
              { type: 'box', layout: 'vertical', flex: 8, paddingStart: '12px', contents: [
                { type: 'text', text: 'LINE Pay 行動支付', size: 'sm', weight: 'bold', color: '#FFFFFF' },
                { type: 'text', text: '營收自動入帳', size: 'xs', color: '#999999', wrap: true }
              ]}
            ]
          },
          {
            type: 'box', layout: 'horizontal', margin: 'md', contents: [
              { type: 'box', layout: 'vertical', backgroundColor: '#1E5F3A', cornerRadius: '8px', width: '36px', height: '36px', justifyContent: 'center', alignItems: 'center', flex: 0, contents: [
                { type: 'text', text: '📊', size: 'md', align: 'center' }
              ]},
              { type: 'box', layout: 'vertical', flex: 8, paddingStart: '12px', contents: [
                { type: 'text', text: '雲端即時報表', size: 'sm', weight: 'bold', color: '#FFFFFF' },
                { type: 'text', text: '手機隨時看營收', size: 'xs', color: '#999999', wrap: true }
              ]}
            ]
          },
          {
            type: 'box', layout: 'horizontal', margin: 'md', contents: [
              { type: 'box', layout: 'vertical', backgroundColor: '#5F3A1E', cornerRadius: '8px', width: '36px', height: '36px', justifyContent: 'center', alignItems: 'center', flex: 0, contents: [
                { type: 'text', text: '🔔', size: 'md', align: 'center' }
              ]},
              { type: 'box', layout: 'vertical', flex: 8, paddingStart: '12px', contents: [
                { type: 'text', text: '異常自動通知', size: 'sm', weight: 'bold', color: '#FFFFFF' },
                { type: 'text', text: '遠端管理免到場', size: 'xs', color: '#999999', wrap: true }
              ]}
            ]
          },
          {
            type: 'box', layout: 'horizontal', margin: 'md', contents: [
              { type: 'box', layout: 'vertical', backgroundColor: '#5F1E3A', cornerRadius: '8px', width: '36px', height: '36px', justifyContent: 'center', alignItems: 'center', flex: 0, contents: [
                { type: 'text', text: '🎯', size: 'md', align: 'center' }
              ]},
              { type: 'box', layout: 'vertical', flex: 8, paddingStart: '12px', contents: [
                { type: 'text', text: '會員+優惠券', size: 'sm', weight: 'bold', color: '#FFFFFF' },
                { type: 'text', text: '回購率提升 40%', size: 'xs', color: '#999999', wrap: true }
              ]}
            ]
          },
          { type: 'box', layout: 'vertical', height: '1px', backgroundColor: '#333355', margin: 'lg' },
          { type: 'text', text: '💰 月租方案 NT$5,000 起｜最快 7 天上線', size: 'sm', align: 'center', color: BRAND_GOLD, weight: 'bold', margin: 'md', wrap: true }
        ]
      },
      footer: {
        type: 'box', layout: 'vertical', spacing: 'sm', paddingAll: '15px',
        contents: [
          { type: 'button', style: 'primary', color: BRAND_PRIMARY, action: { type: 'postback', label: '📋 查看方案與報價', data: 'action=show_plans', displayText: '查看方案與報價' } },
          { type: 'button', style: 'primary', color: BRAND_GOLD, action: { type: 'uri', label: '📞 預約免費 Demo', uri: LINE_OA_CHAT_URL } }
        ]
      }
    }
  };

  const keywordGuide = {
    type: 'text',
    text: '📩 也可以直接輸入關鍵字：\n\n「方案」→ 查看三種方案詳情\n「成功案例」→ 看其他店主的成效\n「比較」→ 傳統 vs 智慧洗衣對比\n「免費Demo」→ 預約線上展示\n「獲利分析」→ 投報率試算'
  };

  return [overviewCard, keywordGuide];
}

// Keep backward compatibility alias
function buildFranchiseWelcomeReply() {
  return buildBusinessWelcomeReply();
}

// ---- Customer Keyword Flex Builders ----

function buildPriceGuideCard() {
  return {
    type: 'flex', altText: '洗衣價格參考',
    contents: {
      type: 'bubble', size: 'mega',
      styles: {
        header: { backgroundColor: '#1A1A3A' },
        body: { backgroundColor: '#0D0D1A' },
        footer: { backgroundColor: '#0D0D1A', separator: false }
      },
      header: {
        type: 'box', layout: 'vertical', paddingAll: '20px',
        contents: [
          { type: 'text', text: '💰 洗衣價格參考', color: '#FFFFFF', weight: 'bold', size: 'lg' }
        ]
      },
      body: {
        type: 'box', layout: 'vertical', spacing: 'md', paddingAll: '20px',
        contents: [
          { type: 'text', text: '各模式參考價格', size: 'md', weight: 'bold', color: '#FFFFFF' },
          { type: 'box', layout: 'vertical', height: '1px', backgroundColor: '#333355', margin: 'md' },
          { type: 'box', layout: 'horizontal', margin: 'lg', contents: [
            { type: 'text', text: '🧺 單洗', size: 'sm', flex: 4, color: '#E0E0E0' },
            { type: 'text', text: 'NT$40 起', size: 'sm', weight: 'bold', flex: 4, align: 'end', color: BRAND_GOLD }
          ]},
          { type: 'box', layout: 'horizontal', margin: 'sm', contents: [
            { type: 'text', text: '🌀 洗脫', size: 'sm', flex: 4, color: '#E0E0E0' },
            { type: 'text', text: 'NT$50 起', size: 'sm', weight: 'bold', flex: 4, align: 'end', color: BRAND_GOLD }
          ]},
          { type: 'box', layout: 'horizontal', margin: 'sm', contents: [
            { type: 'text', text: '✨ 洗脫烘', size: 'sm', flex: 4, color: '#E0E0E0' },
            { type: 'text', text: 'NT$60 起', size: 'sm', weight: 'bold', flex: 4, align: 'end', color: BRAND_GOLD }
          ]},
          { type: 'box', layout: 'horizontal', margin: 'sm', contents: [
            { type: 'text', text: '🔥 烘衣', size: 'sm', flex: 4, color: '#E0E0E0' },
            { type: 'text', text: 'NT$30 起', size: 'sm', weight: 'bold', flex: 4, align: 'end', color: BRAND_GOLD }
          ]},
          { type: 'box', layout: 'vertical', height: '1px', backgroundColor: '#333355', margin: 'lg' },
          { type: 'text', text: '* 價格依機型大小而異，大型機器價格略高', size: 'xxs', color: '#999999', margin: 'md', wrap: true },
          { type: 'text', text: '💡 儲值 NT$200 送 NT$30，每次洗衣更省！', size: 'xs', color: BRAND_GOLD, margin: 'md', weight: 'bold', wrap: true }
        ]
      },
      footer: {
        type: 'box', layout: 'vertical', spacing: 'sm', paddingAll: '15px',
        contents: [
          { type: 'button', style: 'primary', color: BRAND_PRIMARY, action: { type: 'uri', label: '立即查看空機', uri: LIFF_WASH } }
        ]
      }
    }
  };
}

function buildTopupBenefitsCard() {
  return {
    type: 'flex', altText: '儲值好處多多',
    contents: {
      type: 'bubble', size: 'mega',
      styles: {
        header: { backgroundColor: '#1A1A3A' },
        body: { backgroundColor: '#0D0D1A' },
        footer: { backgroundColor: '#0D0D1A', separator: false }
      },
      header: {
        type: 'box', layout: 'vertical', paddingAll: '20px',
        contents: [
          { type: 'text', text: '💳 錢包儲值優惠', color: BRAND_GOLD, weight: 'bold', size: 'lg' }
        ]
      },
      body: {
        type: 'box', layout: 'vertical', spacing: 'md', paddingAll: '20px',
        contents: [
          { type: 'text', text: '儲值三大好處', size: 'md', weight: 'bold', color: '#FFFFFF' },
          { type: 'box', layout: 'vertical', height: '1px', backgroundColor: '#333355', margin: 'md' },
          {
            type: 'box', layout: 'horizontal', margin: 'lg', contents: [
              { type: 'box', layout: 'vertical', backgroundColor: '#5F1E3A', cornerRadius: '8px', width: '36px', height: '36px', justifyContent: 'center', alignItems: 'center', flex: 0, contents: [
                { type: 'text', text: '🎁', size: 'md', align: 'center' }
              ]},
              { type: 'box', layout: 'vertical', flex: 8, paddingStart: '12px', contents: [
                { type: 'text', text: '儲 NT$200 送 NT$30', size: 'sm', weight: 'bold', color: '#FFFFFF' },
                { type: 'text', text: '等於打85折，越洗越省', size: 'xs', color: '#999999', wrap: true }
              ]}
            ]
          },
          {
            type: 'box', layout: 'horizontal', margin: 'md', contents: [
              { type: 'box', layout: 'vertical', backgroundColor: '#1E3A5F', cornerRadius: '8px', width: '36px', height: '36px', justifyContent: 'center', alignItems: 'center', flex: 0, contents: [
                { type: 'text', text: '⚡', size: 'md', align: 'center' }
              ]},
              { type: 'box', layout: 'vertical', flex: 8, paddingStart: '12px', contents: [
                { type: 'text', text: '付款秒速完成', size: 'sm', weight: 'bold', color: '#FFFFFF' },
                { type: 'text', text: '錢包扣款免等，比投幣快3倍', size: 'xs', color: '#999999', wrap: true }
              ]}
            ]
          },
          {
            type: 'box', layout: 'horizontal', margin: 'md', contents: [
              { type: 'box', layout: 'vertical', backgroundColor: '#1E5F3A', cornerRadius: '8px', width: '36px', height: '36px', justifyContent: 'center', alignItems: 'center', flex: 0, contents: [
                { type: 'text', text: '📋', size: 'md', align: 'center' }
              ]},
              { type: 'box', layout: 'vertical', flex: 8, paddingStart: '12px', contents: [
                { type: 'text', text: '消費紀錄一目了然', size: 'sm', weight: 'bold', color: '#FFFFFF' },
                { type: 'text', text: '所有交易紀錄都在手機裡', size: 'xs', color: '#999999', wrap: true }
              ]}
            ]
          }
        ]
      },
      footer: {
        type: 'box', layout: 'vertical', spacing: 'sm', paddingAll: '15px',
        contents: [
          { type: 'button', style: 'primary', color: '#4CAF50', action: { type: 'uri', label: '立即儲值', uri: LIFF_PROFILE } }
        ]
      }
    }
  };
}

function buildPromotionsCard() {
  return {
    type: 'flex', altText: '最新優惠活動',
    contents: {
      type: 'bubble', size: 'mega',
      header: {
        type: 'box', layout: 'vertical', backgroundColor: RED, paddingAll: '20px',
        contents: [
          { type: 'text', text: '🎉 最新優惠活動', color: '#FFFFFF', weight: 'bold', size: 'lg' }
        ]
      },
      body: {
        type: 'box', layout: 'vertical', spacing: 'md', paddingAll: '20px',
        contents: [
          {
            type: 'box', layout: 'vertical', backgroundColor: '#FFF5F5', cornerRadius: '8px', paddingAll: '12px', margin: 'none',
            contents: [
              { type: 'text', text: '🔥 新會員首儲禮', size: 'sm', weight: 'bold', color: RED },
              { type: 'text', text: '首次儲值任意金額，即送 NT$20 洗衣金', size: 'xs', wrap: true, margin: 'sm' }
            ]
          },
          {
            type: 'box', layout: 'vertical', backgroundColor: '#F5FFF5', cornerRadius: '8px', paddingAll: '12px', margin: 'md',
            contents: [
              { type: 'text', text: '💰 儲值加碼', size: 'sm', weight: 'bold', color: GREEN },
              { type: 'text', text: '儲值 NT$200 再送 NT$30，最高回饋15%', size: 'xs', wrap: true, margin: 'sm' }
            ]
          },
          {
            type: 'box', layout: 'vertical', backgroundColor: '#F5F5FF', cornerRadius: '8px', paddingAll: '12px', margin: 'md',
            contents: [
              { type: 'text', text: '🎁 推薦好友', size: 'sm', weight: 'bold', color: BRAND_PRIMARY },
              { type: 'text', text: '推薦朋友加入，雙方各得 NT$10 洗衣金', size: 'xs', wrap: true, margin: 'sm' }
            ]
          }
        ]
      },
      footer: {
        type: 'box', layout: 'vertical', spacing: 'sm', paddingAll: '15px',
        contents: [
          { type: 'button', style: 'primary', color: BRAND_PRIMARY, action: { type: 'uri', label: '馬上去洗衣', uri: LIFF_URL } }
        ]
      }
    }
  };
}

function buildDryerGuideCard() {
  return {
    type: 'flex', altText: '烘衣小指南',
    contents: {
      type: 'bubble', size: 'mega',
      header: {
        type: 'box', layout: 'vertical', backgroundColor: '#FF8C00', paddingAll: '20px',
        contents: [
          { type: 'text', text: '🔥 烘衣小指南', color: '#FFFFFF', weight: 'bold', size: 'lg' }
        ]
      },
      body: {
        type: 'box', layout: 'vertical', spacing: 'md', paddingAll: '20px',
        contents: [
          { type: 'text', text: '溫度選擇建議', size: 'md', weight: 'bold' },
          { type: 'separator', margin: 'md' },
          { type: 'box', layout: 'horizontal', margin: 'lg', contents: [
            { type: 'text', text: '低溫 40°C', size: 'sm', flex: 4 },
            { type: 'text', text: '內衣褲、絲質衣物', size: 'sm', flex: 6, wrap: true, color: '#555555' }
          ]},
          { type: 'box', layout: 'horizontal', margin: 'sm', contents: [
            { type: 'text', text: '中溫 60°C', size: 'sm', flex: 4 },
            { type: 'text', text: '一般衣物、T恤、褲子', size: 'sm', flex: 6, wrap: true, color: '#555555' }
          ]},
          { type: 'box', layout: 'horizontal', margin: 'sm', contents: [
            { type: 'text', text: '高溫 80°C', size: 'sm', flex: 4 },
            { type: 'text', text: '毛巾、床單、厚棉衣', size: 'sm', flex: 6, wrap: true, color: '#555555' }
          ]},
          { type: 'separator', margin: 'lg' },
          { type: 'text', text: '💡 小技巧', size: 'sm', weight: 'bold', margin: 'md' },
          { type: 'text', text: '1. 衣物不要塞太滿，七分滿最佳\n2. 加烘衣球可減少20%烘乾時間\n3. 牛仔褲建議反面烘乾避免褪色', size: 'xs', wrap: true, margin: 'sm', color: '#555555' }
        ]
      }
    }
  };
}

function buildFirstWashGuideCard() {
  return {
    type: 'flex', altText: '第一次洗衣教學',
    contents: {
      type: 'bubble', size: 'mega',
      header: {
        type: 'box', layout: 'vertical', backgroundColor: BRAND_PRIMARY, paddingAll: '20px',
        contents: [
          { type: 'text', text: '📖 第一次使用教學', color: '#FFFFFF', weight: 'bold', size: 'lg' }
        ]
      },
      body: {
        type: 'box', layout: 'vertical', spacing: 'md', paddingAll: '20px',
        contents: [
          { type: 'text', text: '只要 4 步驟，超簡單！', size: 'md', weight: 'bold' },
          { type: 'separator', margin: 'md' },
          {
            type: 'box', layout: 'horizontal', margin: 'lg', contents: [
              {
                type: 'box', layout: 'vertical', flex: 1, alignItems: 'center',
                contents: [
                  {
                    type: 'box', layout: 'vertical', width: '32px', height: '32px',
                    backgroundColor: BRAND_PRIMARY, cornerRadius: '16px', justifyContent: 'center', alignItems: 'center',
                    contents: [{ type: 'text', text: '1', color: '#FFFFFF', size: 'sm', align: 'center', weight: 'bold' }]
                  }
                ]
              },
              { type: 'box', layout: 'vertical', flex: 7, contents: [
                { type: 'text', text: '查空機', size: 'sm', weight: 'bold' },
                { type: 'text', text: '在 LINE 上查看附近門市空機狀態', size: 'xs', color: '#888888', wrap: true }
              ]}
            ]
          },
          {
            type: 'box', layout: 'horizontal', margin: 'md', contents: [
              {
                type: 'box', layout: 'vertical', flex: 1, alignItems: 'center',
                contents: [
                  {
                    type: 'box', layout: 'vertical', width: '32px', height: '32px',
                    backgroundColor: BRAND_PRIMARY, cornerRadius: '16px', justifyContent: 'center', alignItems: 'center',
                    contents: [{ type: 'text', text: '2', color: '#FFFFFF', size: 'sm', align: 'center', weight: 'bold' }]
                  }
                ]
              },
              { type: 'box', layout: 'vertical', flex: 7, contents: [
                { type: 'text', text: '放入衣物', size: 'sm', weight: 'bold' },
                { type: 'text', text: '到店後選機器，放入衣物並關好機門', size: 'xs', color: '#888888', wrap: true }
              ]}
            ]
          },
          {
            type: 'box', layout: 'horizontal', margin: 'md', contents: [
              {
                type: 'box', layout: 'vertical', flex: 1, alignItems: 'center',
                contents: [
                  {
                    type: 'box', layout: 'vertical', width: '32px', height: '32px',
                    backgroundColor: BRAND_PRIMARY, cornerRadius: '16px', justifyContent: 'center', alignItems: 'center',
                    contents: [{ type: 'text', text: '3', color: '#FFFFFF', size: 'sm', align: 'center', weight: 'bold' }]
                  }
                ]
              },
              { type: 'box', layout: 'vertical', flex: 7, contents: [
                { type: 'text', text: '選模式付款', size: 'sm', weight: 'bold' },
                { type: 'text', text: '選擇洗衣模式，用 LINE Pay 或錢包付款', size: 'xs', color: '#888888', wrap: true }
              ]}
            ]
          },
          {
            type: 'box', layout: 'horizontal', margin: 'md', contents: [
              {
                type: 'box', layout: 'vertical', flex: 1, alignItems: 'center',
                contents: [
                  {
                    type: 'box', layout: 'vertical', width: '32px', height: '32px',
                    backgroundColor: GREEN, cornerRadius: '16px', justifyContent: 'center', alignItems: 'center',
                    contents: [{ type: 'text', text: '✓', color: '#FFFFFF', size: 'sm', align: 'center', weight: 'bold' }]
                  }
                ]
              },
              { type: 'box', layout: 'vertical', flex: 7, contents: [
                { type: 'text', text: '等通知取衣', size: 'sm', weight: 'bold' },
                { type: 'text', text: '洗好後 LINE 會通知你，回來取衣就好', size: 'xs', color: '#888888', wrap: true }
              ]}
            ]
          }
        ]
      },
      footer: {
        type: 'box', layout: 'vertical', spacing: 'sm', paddingAll: '15px',
        contents: [
          { type: 'button', style: 'primary', color: BRAND_PRIMARY, action: { type: 'uri', label: '立即開始', uri: LIFF_WASH } }
        ]
      }
    }
  };
}

function buildStoreListCarousel() {
  const stores = [
    { name: '悠洗自助洗衣', addr: '嘉義市東區文雅街181號', id: 's1' },
    { name: '吼你洗 玉清店', addr: '苗栗市玉清路51號', id: 's2' },
    { name: '吼你洗 農會店', addr: '苗栗市為公路290號', id: 's3' },
    { name: '熊愛洗自助洗衣', addr: '台中市西屯區福聯街22巷2號', id: 's4' },
    { name: '上好洗自助洗衣', addr: '高雄市鳳山區北平路214號', id: 's5' }
  ];

  const bubbles = stores.map((s, i) => ({
    type: 'bubble', size: 'kilo',
    header: {
      type: 'box', layout: 'vertical', backgroundColor: BRAND_PRIMARY, paddingAll: '14px',
      contents: [
        { type: 'text', text: `📍 ${s.name}`, color: '#FFFFFF', weight: 'bold', size: 'md', wrap: true }
      ]
    },
    body: {
      type: 'box', layout: 'vertical', spacing: 'sm', paddingAll: '14px',
      contents: [
        { type: 'text', text: s.addr, size: 'sm', wrap: true, color: '#555555' },
        { type: 'text', text: '⏰ 24小時營業・全年無休', size: 'xs', color: '#888888', margin: 'md' }
      ]
    },
    footer: {
      type: 'box', layout: 'vertical', spacing: 'sm', paddingAll: '10px',
      contents: [
        { type: 'button', style: 'primary', color: BRAND_PRIMARY, height: 'sm', action: { type: 'uri', label: '查看空機', uri: LIFF_WASH } },
        { type: 'button', style: 'link', height: 'sm', action: { type: 'uri', label: '導航前往', uri: `https://www.google.com/maps/search/?api=1&query=${encodeURIComponent(s.addr)}` } }
      ]
    }
  }));

  return {
    type: 'flex', altText: '全台 5 間門市地址',
    contents: { type: 'carousel', contents: bubbles }
  };
}

function buildMemberLevelCard() {
  return {
    type: 'flex', altText: '會員等級說明',
    contents: {
      type: 'bubble', size: 'mega',
      header: {
        type: 'box', layout: 'vertical', backgroundColor: BRAND_GOLD, paddingAll: '20px',
        contents: [
          { type: 'text', text: '🏅 會員等級制度', color: '#FFFFFF', weight: 'bold', size: 'lg' }
        ]
      },
      body: {
        type: 'box', layout: 'vertical', spacing: 'md', paddingAll: '20px',
        contents: [
          { type: 'text', text: '消費累積點數，享受更多折扣！', size: 'sm', wrap: true, color: '#555555' },
          { type: 'separator', margin: 'md' },
          { type: 'box', layout: 'horizontal', margin: 'lg', contents: [
            { type: 'text', text: '🌱 普通會員', size: 'sm', flex: 5 },
            { type: 'text', text: '0 點起', size: 'xs', flex: 3, color: '#888888', align: 'end' }
          ]},
          { type: 'box', layout: 'horizontal', margin: 'sm', contents: [
            { type: 'text', text: '🥉 銅牌會員', size: 'sm', flex: 5 },
            { type: 'text', text: '500 點 / 折3%', size: 'xs', flex: 3, color: '#888888', align: 'end' }
          ]},
          { type: 'box', layout: 'horizontal', margin: 'sm', contents: [
            { type: 'text', text: '🥈 銀牌會員', size: 'sm', flex: 5 },
            { type: 'text', text: '2000 點 / 折5%', size: 'xs', flex: 3, color: '#888888', align: 'end' }
          ]},
          { type: 'box', layout: 'horizontal', margin: 'sm', contents: [
            { type: 'text', text: '🥇 金牌會員', size: 'sm', flex: 5 },
            { type: 'text', text: '5000 點 / 折8%', size: 'xs', flex: 3, color: '#888888', align: 'end' }
          ]},
          { type: 'box', layout: 'horizontal', margin: 'sm', contents: [
            { type: 'text', text: '💎 鑽石會員', size: 'sm', flex: 5 },
            { type: 'text', text: '10000 點 / 折12%', size: 'xs', flex: 3, color: '#888888', align: 'end' }
          ]},
          { type: 'separator', margin: 'lg' },
          { type: 'text', text: '💡 每消費 NT$1 = 1 點，點數永久有效', size: 'xs', color: BRAND_GOLD, margin: 'md', wrap: true, weight: 'bold' }
        ]
      },
      footer: {
        type: 'box', layout: 'vertical', spacing: 'sm', paddingAll: '15px',
        contents: [
          { type: 'button', style: 'primary', color: BRAND_PRIMARY, action: { type: 'uri', label: '查看我的等級', uri: LIFF_PROFILE } }
        ]
      }
    }
  };
}

function buildLaundryTipsCarousel() {
  const tips = [
    {
      title: '衣物標籤解密',
      emoji: '🏷️',
      color: '#4A90D9',
      items: [
        '🔵 水洗符號：盆子代表可水洗',
        '🔺 三角形：漂白相關指示',
        '⬜ 正方形：烘乾方式指示',
        '💡 看不懂就選低溫柔洗最安全'
      ]
    },
    {
      title: '自助洗衣殺菌真相',
      emoji: '🦠',
      color: '#2ECC71',
      items: [
        '60°C 以上可殺死多數細菌',
        '烘衣高溫更能有效滅菌',
        '自助洗衣機定期專業清洗',
        '💡 建議：內衣褲用高溫洗+烘'
      ]
    },
    {
      title: '烘衣球的科學',
      emoji: '⚽',
      color: '#FF8C00',
      items: [
        '減少衣物纏繞，均勻受熱',
        '縮短約20%烘乾時間',
        '減少靜電與皺褶產生',
        '💡 沒有烘衣球？網球也行'
      ]
    },
    {
      title: '換季收納必洗',
      emoji: '📦',
      color: '#9B59B6',
      items: [
        '收納前一定要洗乾淨+烘乾',
        '殘留汗漬會氧化產生黃斑',
        '防蟲蛀：確保衣物完全乾燥',
        '💡 羽絨衣建議專業洗+低溫烘'
      ]
    }
  ];

  const bubbles = tips.map(t => ({
    type: 'bubble', size: 'kilo',
    header: {
      type: 'box', layout: 'vertical', backgroundColor: t.color, paddingAll: '14px',
      contents: [
        { type: 'text', text: `${t.emoji} ${t.title}`, color: '#FFFFFF', weight: 'bold', size: 'md', wrap: true }
      ]
    },
    body: {
      type: 'box', layout: 'vertical', spacing: 'sm', paddingAll: '14px',
      contents: t.items.map((item, i) => ({
        type: 'text', text: item, size: 'xs', wrap: true, color: i === t.items.length - 1 ? BRAND_GOLD : '#555555',
        weight: i === t.items.length - 1 ? 'bold' : 'regular', margin: i === 0 ? 'none' : 'sm'
      }))
    }
  }));

  return {
    type: 'flex', altText: '洗衣小知識',
    contents: { type: 'carousel', contents: bubbles }
  };
}

// ---- Franchise Keyword Flex Builders ----

function buildSuccessCasesCarousel() {
  const cases = [
    {
      name: '林老闆',
      store: '苗栗玉清店',
      period: '加入 8 個月',
      quote: '導入雲管家系統後，月營收成長 20%，遠端管理讓我不用天天跑店，省時省力！',
      stats: [
        { label: '月營收成長', value: '+20%' },
        { label: '管理時間', value: '每天15分鐘' },
        { label: '回購率', value: '68%' }
      ]
    },
    {
      name: '張小姐',
      store: '台中福聯店',
      period: '加入 6 個月',
      quote: '行動支付佔比已達 60%，會員回購率提升 34%，年輕客群明顯增加。',
      stats: [
        { label: '行動支付佔比', value: '60%' },
        { label: '回購率提升', value: '+34%' },
        { label: '回收期', value: '16個月' }
      ]
    },
    {
      name: '陳先生',
      store: '高雄北平店',
      period: '加入 1 年',
      quote: '從傳統投幣升級到智慧系統，客訴減少 70%，遠端故障排除太方便了。',
      stats: [
        { label: '客訴減少', value: '-70%' },
        { label: '月淨利', value: 'NT$6萬+' },
        { label: '設備異常處理', value: '遠端秒排除' }
      ]
    }
  ];

  const bubbles = cases.map(c => ({
    type: 'bubble', size: 'mega',
    header: {
      type: 'box', layout: 'vertical', backgroundColor: BRAND_GOLD, paddingAll: '16px',
      contents: [
        { type: 'text', text: `⭐ ${c.name} — ${c.store}`, color: '#FFFFFF', weight: 'bold', size: 'md', wrap: true },
        { type: 'text', text: c.period, color: '#FFFFFFCC', size: 'xs', margin: 'xs' }
      ]
    },
    body: {
      type: 'box', layout: 'vertical', spacing: 'md', paddingAll: '16px',
      contents: [
        { type: 'text', text: `"${c.quote}"`, size: 'sm', wrap: true, style: 'italic', color: '#444444' },
        { type: 'separator', margin: 'lg' },
        ...c.stats.map(s => ({
          type: 'box', layout: 'horizontal', margin: 'sm',
          contents: [
            { type: 'text', text: s.label, size: 'sm', color: '#888888', flex: 5 },
            { type: 'text', text: s.value, size: 'sm', weight: 'bold', flex: 4, align: 'end', color: BRAND_PRIMARY }
          ]
        }))
      ]
    },
    footer: {
      type: 'box', layout: 'vertical', spacing: 'sm', paddingAll: '10px',
      contents: [
        { type: 'button', style: 'primary', color: BRAND_GOLD, height: 'sm', action: { type: 'uri', label: '我也想導入雲管家', uri: LINE_OA_CHAT_URL } }
      ]
    }
  }));

  return {
    type: 'flex', altText: '雲管家客戶成功案例',
    contents: { type: 'carousel', contents: bubbles }
  };
}

function buildTraditionalVsSmartCard() {
  return {
    type: 'flex', altText: '傳統投幣 vs 雲管家智慧系統比較',
    contents: {
      type: 'bubble', size: 'mega',
      header: {
        type: 'box', layout: 'vertical', backgroundColor: BRAND_PRIMARY, paddingAll: '20px',
        contents: [
          { type: 'text', text: '⚡ 傳統投幣 vs 雲管家智慧系統', color: '#FFFFFF', weight: 'bold', size: 'md', wrap: true }
        ]
      },
      body: {
        type: 'box', layout: 'vertical', spacing: 'sm', paddingAll: '16px',
        contents: [
          // Header row
          { type: 'box', layout: 'horizontal', margin: 'none', contents: [
            { type: 'text', text: '項目', size: 'xs', weight: 'bold', flex: 3, color: '#888888' },
            { type: 'text', text: '傳統投幣', size: 'xs', weight: 'bold', flex: 3, align: 'center', color: '#888888' },
            { type: 'text', text: '雲管家', size: 'xs', weight: 'bold', flex: 3, align: 'center', color: BRAND_PRIMARY }
          ]},
          { type: 'separator', margin: 'sm' },
          // Rows
          { type: 'box', layout: 'horizontal', margin: 'md', contents: [
            { type: 'text', text: '付款方式', size: 'xs', flex: 3 },
            { type: 'text', text: '僅投幣', size: 'xs', flex: 3, align: 'center', color: '#888888' },
            { type: 'text', text: '多元支付', size: 'xs', flex: 3, align: 'center', color: GREEN, weight: 'bold' }
          ]},
          { type: 'box', layout: 'horizontal', margin: 'sm', contents: [
            { type: 'text', text: '管理方式', size: 'xs', flex: 3 },
            { type: 'text', text: '天天到場', size: 'xs', flex: 3, align: 'center', color: '#888888' },
            { type: 'text', text: '遠端15分鐘', size: 'xs', flex: 3, align: 'center', color: GREEN, weight: 'bold' }
          ]},
          { type: 'box', layout: 'horizontal', margin: 'sm', contents: [
            { type: 'text', text: '客單價', size: 'xs', flex: 3 },
            { type: 'text', text: 'NT$40-50', size: 'xs', flex: 3, align: 'center', color: '#888888' },
            { type: 'text', text: 'NT$55-70', size: 'xs', flex: 3, align: 'center', color: GREEN, weight: 'bold' }
          ]},
          { type: 'box', layout: 'horizontal', margin: 'sm', contents: [
            { type: 'text', text: '會員經營', size: 'xs', flex: 3 },
            { type: 'text', text: '無', size: 'xs', flex: 3, align: 'center', color: RED },
            { type: 'text', text: '完整CRM', size: 'xs', flex: 3, align: 'center', color: GREEN, weight: 'bold' }
          ]},
          { type: 'box', layout: 'horizontal', margin: 'sm', contents: [
            { type: 'text', text: '設備監控', size: 'xs', flex: 3 },
            { type: 'text', text: '人工巡店', size: 'xs', flex: 3, align: 'center', color: '#888888' },
            { type: 'text', text: 'IoT即時', size: 'xs', flex: 3, align: 'center', color: GREEN, weight: 'bold' }
          ]},
          { type: 'box', layout: 'horizontal', margin: 'sm', contents: [
            { type: 'text', text: '數據分析', size: 'xs', flex: 3 },
            { type: 'text', text: '手記帳', size: 'xs', flex: 3, align: 'center', color: '#888888' },
            { type: 'text', text: '自動報表', size: 'xs', flex: 3, align: 'center', color: GREEN, weight: 'bold' }
          ]},
          { type: 'box', layout: 'horizontal', margin: 'sm', contents: [
            { type: 'text', text: '回購率', size: 'xs', flex: 3 },
            { type: 'text', text: '~30%', size: 'xs', flex: 3, align: 'center', color: '#888888' },
            { type: 'text', text: '~65%', size: 'xs', flex: 3, align: 'center', color: GREEN, weight: 'bold' }
          ]},
          { type: 'separator', margin: 'lg' },
          { type: 'text', text: '💡 雲管家智慧系統平均提升營收 25-40%', size: 'xs', color: BRAND_GOLD, weight: 'bold', margin: 'md', wrap: true }
        ]
      },
      footer: {
        type: 'box', layout: 'vertical', spacing: 'sm', paddingAll: '15px',
        contents: [
          { type: 'button', style: 'primary', color: BRAND_GOLD, action: { type: 'postback', label: '查看成功案例', data: 'action=success_cases', displayText: '成功案例' } },
          { type: 'button', style: 'link', action: { type: 'uri', label: '🌐 瀏覽官網了解更多', uri: OFFICIAL_WEBSITE } }
        ]
      }
    }
  };
}

// ---- Plans Card (B2B Carousel) ----
function buildPlansCard() {
  const monthlyBubble = {
    type: 'bubble', size: 'mega',
    styles: {
      header: { backgroundColor: '#1A1A3A' },
      body: { backgroundColor: '#0D0D1A' },
      footer: { backgroundColor: '#0D0D1A', separator: false }
    },
    header: {
      type: 'box', layout: 'vertical', paddingAll: '20px',
      contents: [
        { type: 'text', text: '月租方案', color: '#FFFFFF', weight: 'bold', size: 'lg' }
      ]
    },
    body: {
      type: 'box', layout: 'vertical', spacing: 'md', paddingAll: '20px',
      contents: [
        { type: 'text', text: 'NT$ 5,000 /月', size: 'xl', weight: 'bold', color: BRAND_GOLD },
        { type: 'box', layout: 'vertical', height: '1px', backgroundColor: '#333355', margin: 'lg' },
        { type: 'text', text: '適合：剛起步、想先試水溫', size: 'xs', color: '#999999', margin: 'sm', wrap: true },
        { type: 'box', layout: 'vertical', height: '1px', backgroundColor: '#333355', margin: 'lg' },
        { type: 'text', text: '✅ LINE Pay 支付', size: 'sm', margin: 'lg', wrap: true, color: '#E0E0E0' },
        { type: 'text', text: '✅ 會員儲值系統', size: 'sm', margin: 'sm', wrap: true, color: '#E0E0E0' },
        { type: 'text', text: '✅ 營收報表', size: 'sm', margin: 'sm', wrap: true, color: '#E0E0E0' },
        { type: 'text', text: '✅ 機台狀態監控', size: 'sm', margin: 'sm', wrap: true, color: '#E0E0E0' },
        { type: 'text', text: '✅ 洗好推播通知', size: 'sm', margin: 'sm', wrap: true, color: '#E0E0E0' },
        { type: 'box', layout: 'vertical', height: '1px', backgroundColor: '#333355', margin: 'lg' },
        { type: 'text', text: '✅ 7天快速上線', size: 'sm', color: '#4CAF50', weight: 'bold', margin: 'md', wrap: true }
      ]
    },
    footer: {
      type: 'box', layout: 'vertical', spacing: 'sm', paddingAll: '15px',
      contents: [
        { type: 'button', style: 'primary', color: '#3A3A8C', action: { type: 'uri', label: '立即諮詢', uri: LINE_OA_CHAT_URL } },
        { type: 'button', style: 'secondary', height: 'sm', action: { type: 'postback', label: '📋 填寫諮詢表', data: 'action=start_survey', displayText: '填寫諮詢表' } }
      ]
    }
  };

  const annualBubble = {
    type: 'bubble', size: 'mega',
    styles: {
      header: { backgroundColor: '#252547' },
      body: { backgroundColor: '#0D0D1A' },
      footer: { backgroundColor: '#0D0D1A', separator: false }
    },
    header: {
      type: 'box', layout: 'horizontal', paddingAll: '20px', justifyContent: 'space-between', alignItems: 'center',
      contents: [
        { type: 'text', text: '年租方案', color: '#FFFFFF', weight: 'bold', size: 'lg', flex: 0 },
        { type: 'box', layout: 'vertical', backgroundColor: BRAND_GOLD, cornerRadius: '4px', paddingAll: '4px', paddingStart: '8px', paddingEnd: '8px', flex: 0, contents: [
          { type: 'text', text: '最熱門', size: 'xs', weight: 'bold', color: '#0D0D1A', align: 'center' }
        ]}
      ]
    },
    body: {
      type: 'box', layout: 'vertical', spacing: 'md', paddingAll: '20px',
      contents: [
        { type: 'text', text: 'NT$ 50,000 /年', size: 'xl', weight: 'bold', color: BRAND_GOLD },
        { type: 'text', text: '等於月付 $4,167，省下兩個月！', size: 'sm', weight: 'bold', color: '#FF6B6B', margin: 'sm', wrap: true },
        { type: 'text', text: '適合：確定長期經營的店主', size: 'xs', color: '#999999', margin: 'sm', wrap: true },
        { type: 'box', layout: 'vertical', height: '1px', backgroundColor: '#333355', margin: 'lg' },
        { type: 'text', text: '✅ 月租全部功能', size: 'sm', margin: 'lg', wrap: true, color: '#E0E0E0' },
        { type: 'text', text: '✅ 優惠券系統', size: 'sm', margin: 'sm', wrap: true, color: '#E0E0E0' },
        { type: 'text', text: '✅ 多店管理', size: 'sm', margin: 'sm', wrap: true, color: '#E0E0E0' },
        { type: 'text', text: '✅ 數據分析報告', size: 'sm', margin: 'sm', wrap: true, color: '#E0E0E0' },
        { type: 'text', text: '✅ 優先技術支援', size: 'sm', margin: 'sm', wrap: true, color: '#E0E0E0' }
      ]
    },
    footer: {
      type: 'box', layout: 'vertical', spacing: 'sm', paddingAll: '15px',
      contents: [
        { type: 'button', style: 'primary', color: BRAND_GOLD, action: { type: 'uri', label: '立即諮詢', uri: LINE_OA_CHAT_URL } },
        { type: 'button', style: 'secondary', height: 'sm', action: { type: 'postback', label: '📋 填寫諮詢表', data: 'action=start_survey', displayText: '填寫諮詢表' } }
      ]
    }
  };

  const customBubble = {
    type: 'bubble', size: 'mega',
    styles: {
      header: { backgroundColor: '#1A1A3A' },
      body: { backgroundColor: '#0D0D1A' },
      footer: { backgroundColor: '#0D0D1A', separator: false }
    },
    header: {
      type: 'box', layout: 'vertical', paddingAll: '20px',
      contents: [
        { type: 'text', text: '客製化 / 買斷', color: '#FFFFFF', weight: 'bold', size: 'lg' }
      ]
    },
    body: {
      type: 'box', layout: 'vertical', spacing: 'md', paddingAll: '20px',
      contents: [
        { type: 'text', text: '依需求報價', size: 'xl', weight: 'bold', color: '#5B5BD6' },
        { type: 'box', layout: 'vertical', height: '1px', backgroundColor: '#333355', margin: 'lg' },
        { type: 'text', text: '適合：多店連鎖、特殊整合需求', size: 'xs', color: '#999999', margin: 'sm', wrap: true },
        { type: 'box', layout: 'vertical', height: '1px', backgroundColor: '#333355', margin: 'lg' },
        { type: 'text', text: '✅ 年租全部功能', size: 'sm', margin: 'lg', wrap: true, color: '#E0E0E0' },
        { type: 'text', text: '✅ API 客製整合', size: 'sm', margin: 'sm', wrap: true, color: '#E0E0E0' },
        { type: 'text', text: '✅ 專屬品牌介面', size: 'sm', margin: 'sm', wrap: true, color: '#E0E0E0' },
        { type: 'text', text: '✅ 硬體客製', size: 'sm', margin: 'sm', wrap: true, color: '#E0E0E0' },
        { type: 'text', text: '✅ 專案經理服務', size: 'sm', margin: 'sm', wrap: true, color: '#E0E0E0' }
      ]
    },
    footer: {
      type: 'box', layout: 'vertical', spacing: 'sm', paddingAll: '15px',
      contents: [
        { type: 'button', style: 'primary', color: '#5B5BD6', action: { type: 'uri', label: '預約評估', uri: LINE_OA_CHAT_URL } },
        { type: 'button', style: 'secondary', height: 'sm', action: { type: 'postback', label: '📋 填寫諮詢表', data: 'action=start_survey', displayText: '填寫諮詢表' } }
      ]
    }
  };

  return {
    type: 'flex', altText: '雲管家方案價格',
    contents: { type: 'carousel', contents: [monthlyBubble, annualBubble, customBubble] }
  };
}

// Keep backward compatibility alias
function buildPricingCard() {
  return buildPlansCard();
}

// ---- Keyword Auto-Reply Responses ----

const KEYWORD_REPLIES = {
  // === Franchise keywords ===
  '我要創業': {
    tags: ['加盟_興趣'],
    messages: [{
      type: 'flex', altText: '《自助洗衣創業避坑指南》',
      contents: {
        type: 'bubble', size: 'mega',
        header: { type: 'box', layout: 'vertical', backgroundColor: BRAND_PRIMARY, paddingAll: '20px', contents: [
          { type: 'text', text: '📘 創業避坑指南', color: '#FFFFFF', weight: 'bold', size: 'lg' }
        ]},
        body: { type: 'box', layout: 'vertical', spacing: 'md', paddingAll: '20px', contents: [
          { type: 'text', text: '5 個開店前必看的關鍵指標：', size: 'md', weight: 'bold', wrap: true },
          { type: 'text', text: '1️⃣ 選址密度比 — 半徑500m住戶÷競業數≥800', size: 'sm', wrap: true, margin: 'md' },
          { type: 'text', text: '2️⃣ 設備投報率 — 單機日均使用≥4次才健康', size: 'sm', wrap: true },
          { type: 'text', text: '3️⃣ 金流結構 — 行動支付客單價比投幣高18%', size: 'sm', wrap: true },
          { type: 'text', text: '4️⃣ 遠端管理 — 每天只需15分鐘管整間店', size: 'sm', wrap: true },
          { type: 'text', text: '5️⃣ 會員經營 — 有會員系統的店終身價值高2.3倍', size: 'sm', wrap: true },
          { type: 'separator', margin: 'lg' },
          { type: 'text', text: '想進一步了解？回覆「獲利分析」領取投報率試算表', size: 'xs', wrap: true, color: '#888888', margin: 'md' }
        ]},
        footer: { type: 'box', layout: 'vertical', spacing: 'sm', paddingAll: '15px', contents: [
          { type: 'button', style: 'primary', color: BRAND_GOLD, action: { type: 'postback', label: '📊 領取獲利分析表', data: 'action=keyword_profit', displayText: '獲利分析' } }
        ]}
      }
    }]
  },
  '獲利分析': {
    tags: ['加盟_評估中'],
    messages: [{
      type: 'flex', altText: '加盟獲利分析',
      contents: {
        type: 'bubble', size: 'mega',
        header: { type: 'box', layout: 'vertical', backgroundColor: BRAND_GOLD, paddingAll: '20px', contents: [
          { type: 'text', text: '📊 獲利分析概覽', color: '#FFFFFF', weight: 'bold', size: 'lg' }
        ]},
        body: { type: 'box', layout: 'vertical', spacing: 'md', paddingAll: '20px', contents: [
          { type: 'text', text: '以 6 台機器標準店為例：', size: 'md', weight: 'bold', wrap: true },
          { type: 'box', layout: 'horizontal', margin: 'lg', contents: [
            { type: 'text', text: '月均營收', size: 'sm', color: '#888888', flex: 4 },
            { type: 'text', text: 'NT$ 8-15 萬', size: 'sm', weight: 'bold', flex: 6, align: 'end' }
          ]},
          { type: 'box', layout: 'horizontal', contents: [
            { type: 'text', text: '月淨利', size: 'sm', color: '#888888', flex: 4 },
            { type: 'text', text: 'NT$ 4-8 萬', size: 'sm', weight: 'bold', flex: 6, align: 'end' }
          ]},
          { type: 'box', layout: 'horizontal', contents: [
            { type: 'text', text: '預估回收期', size: 'sm', color: '#888888', flex: 4 },
            { type: 'text', text: '14-20 個月', size: 'sm', weight: 'bold', flex: 6, align: 'end' }
          ]},
          { type: 'separator', margin: 'lg' },
          { type: 'text', text: '雲管家加盟優勢：\n📊 數據分析精準投放\n🎯 會員系統拉高回購\n🔧 遠端管理省人力', size: 'sm', wrap: true, margin: 'md' },
          { type: 'text', text: '想要客製化評估？回覆「區域評估」', size: 'xs', wrap: true, color: '#888888', margin: 'md' }
        ]},
        footer: { type: 'box', layout: 'vertical', spacing: 'sm', paddingAll: '15px', contents: [
          { type: 'button', style: 'primary', color: BRAND_PRIMARY, action: { type: 'uri', label: '預約免費 Demo', uri: LINE_OA_CHAT_URL } }
        ]}
      }
    }]
  },
  '區域評估': {
    tags: ['加盟_熱leads'],
    messages: [{ type: 'text', text: '📍 感謝你的興趣！\n\n請提供以下資訊，我們將在 24 小時內免費為你評估：\n\n1️⃣ 你預計開店的地址或區域\n2️⃣ 預計的店面坪數\n3️⃣ 你的預算範圍\n4️⃣ 是否已有店面或還在找？\n\n直接回覆以上資訊即可，真人顧問會盡快聯繫你！' }]
  },
  '成功案例': {
    tags: [],
    messages: [buildSuccessCasesCarousel()]
  },
  '傳統比較': {
    tags: [],
    messages: [buildTraditionalVsSmartCard()]
  },
  '方案': {
    tags: ['B2B_方案查詢'],
    messages: [buildPlansCard()]
  },
  'Demo': {
    tags: ['B2B_Demo預約'],
    messages: [{ type: 'text', text: '🎯 太好了！預約免費 Demo 只需要 30 秒：\n\n請回覆以下資訊：\n1️⃣ 你的姓名\n2️⃣ 目前身份（準備開店 / 已有洗衣店 / 連鎖業者）\n3️⃣ 方便的聯繫時間\n4️⃣ 聯絡電話或 Email\n\n我們會在 24 小時內安排專人與你聯繫！\n\n📞 急件可撥：' + CONTACT_PHONE + '\n📧 ' + CONTACT_EMAIL }]
  },
  '免費Demo': {
    tags: ['B2B_Demo預約'],
    messages: [{ type: 'text', text: '🎯 太好了！預約免費 Demo 只需要 30 秒：\n\n請回覆以下資訊：\n1️⃣ 你的姓名\n2️⃣ 目前身份（準備開店 / 已有洗衣店 / 連鎖業者）\n3️⃣ 方便的聯繫時間\n4️⃣ 聯絡電話或 Email\n\n我們會在 24 小時內安排專人與你聯繫！\n\n📞 急件可撥：' + CONTACT_PHONE + '\n📧 ' + CONTACT_EMAIL }]
  },
  '報價': {
    tags: ['B2B_報價需求'],
    messages: [{ type: 'text', text: '💼 感謝你的報價需求！\n\n為了提供最精準的報價，請提供以下資訊：\n\n1️⃣ 您的姓名 / 公司名稱\n2️⃣ 聯絡電話\n3️⃣ 門市數量與地點\n4️⃣ 目前機台數量與品牌\n5️⃣ 需要的客製化功能\n6️⃣ 預計導入時間\n\n或直接撥打 ' + CONTACT_PHONE + '\n📧 ' + CONTACT_EMAIL + '\n🌐 ' + OFFICIAL_WEBSITE }]
  },

  // === Customer keywords ===
  '價格': {
    tags: [],
    messages: [buildPriceGuideCard()]
  },
  '儲值': {
    tags: [],
    messages: [buildTopupBenefitsCard()]
  },
  '優惠': {
    tags: [],
    messages: [buildPromotionsCard()]
  },
  '烘衣': {
    tags: [],
    messages: [buildDryerGuideCard()]
  },
  '營業時間': {
    tags: [],
    messages: [{ type: 'text', text: '⏰ 我們所有門市皆為 24 小時自助營業，全年無休！\n\n不管凌晨還是深夜，隨時都能來洗衣。出門前記得先查空機狀態：\n👉 ' + LIFF_WASH }]
  },
  '教學': {
    tags: [],
    messages: [buildFirstWashGuideCard()]
  },
  '門市': {
    tags: [],
    messages: [buildStoreListCarousel()]
  },
  // '客服' is now handled as special logic in handleTextMessage (customer service forwarding system)
  '會員': {
    tags: [],
    messages: [buildMemberLevelCard()]
  },
  '洗衣知識': {
    tags: [],
    messages: [buildLaundryTipsCarousel()]
  },

  // === Customer Q&A keywords (legacy) ===
  '系統不穩怎麼辦': {
    tags: [],
    messages: [{ type: 'text', text: '您好！若遇到系統異常，請先確認網路連線是否正常。\n\n若問題持續，請直接點選下方「LINE 聯繫客服」，我們工程師將在 30 分鐘內回覆您。\n\n雲管家支援遠端診斷，多數問題免到場即可排除！' }]
  },
  '行動支付': {
    tags: [],
    messages: [{ type: 'text', text: '💳 雲管家支援 LINE Pay 行動支付！\n\n使用方式超簡單：\n1️⃣ 掃機台 QR Code\n2️⃣ 選擇洗衣模式\n3️⃣ LINE Pay 付款\n\n也可以先儲值到錢包，儲值滿 NT$200 送 NT$30 更划算！\n\n👉 不用帶零錢，不用找兌幣機' }]
  },
  '空機': {
    tags: [],
    messages: [{ type: 'text', text: '🔍 想查看空機狀態？\n\n點選下方連結，即可查看各店即時空機情況：\n👉 ' + LIFF_WASH + '\n\n出門前先看，不用白跑一趟！' }]
  },
  '故障報修': {
    tags: [],
    messages: [{
      type: 'flex', altText: '故障報修',
      contents: {
        type: 'bubble', size: 'mega',
        header: { type: 'box', layout: 'vertical', backgroundColor: '#E74C3C', paddingAll: '20px', contents: [
          { type: 'text', text: '🔧 故障報修', color: '#FFFFFF', weight: 'bold', size: 'lg' }
        ]},
        body: { type: 'box', layout: 'vertical', spacing: 'md', paddingAll: '20px', contents: [
          { type: 'text', text: '遇到機器問題了嗎？請提供以下資訊，我們會盡快處理：', size: 'sm', wrap: true },
          { type: 'separator', margin: 'lg' },
          { type: 'text', text: '請回覆以下格式：', size: 'sm', weight: 'bold', margin: 'lg', wrap: true },
          { type: 'text', text: '1️⃣ 哪家門市？（例如：悠洗自助洗衣）\n2️⃣ 哪台機器？（例如：洗衣機2號）\n3️⃣ 什麼問題？（例如：無法啟動）\n4️⃣ 是否有投幣/付款？', size: 'sm', wrap: true, margin: 'sm', color: '#555555' },
          { type: 'separator', margin: 'lg' },
          { type: 'text', text: '💡 常見快速排除：', size: 'sm', weight: 'bold', margin: 'md', wrap: true },
          { type: 'text', text: '• 門沒關好 → 確認機門完全關閉再按啟動\n• 投幣沒反應 → 試試用 LINE Pay 付款\n• 洗到一半停了 → 可能斷電，等2分鐘會自動恢復', size: 'xs', wrap: true, color: '#888888', margin: 'sm' }
        ]},
        footer: { type: 'box', layout: 'vertical', spacing: 'sm', paddingAll: '15px', contents: [
          { type: 'button', style: 'primary', color: '#E74C3C', action: { type: 'uri', label: '直接聯繫客服', uri: LINE_OA_CHAT_URL } },
          { type: 'button', style: 'link', action: { type: 'uri', label: '查看門市地址', uri: LIFF_WASH } }
        ]}
      }
    }]
  },
  '退款': {
    tags: [],
    messages: [{ type: 'text', text: '💳 退款政策說明：\n\n1️⃣ 機器故障導致未啟動 → 全額退回錢包\n2️⃣ 洗程中斷 → 依剩餘時間比例退回\n3️⃣ 儲值退款 → 聯繫客服處理\n\n⏰ 退款處理時間：1-3 個工作天\n\n需要申請退款？請回覆「客服」聯繫真人客服。' }]
  },
  '棉被': {
    tags: [],
    messages: [{ type: 'text', text: '🛏️ 大型衣物洗滌指南：\n\n✅ 可洗：棉被、毛毯、窗簾、大浴巾\n❌ 不建議：羽絨被（建議乾洗）、電毯\n\n💡 洗滌建議：\n1. 選擇「大物洗」模式\n2. 一次只洗一件大型物品\n3. 建議加購洗劑效果更好\n4. 烘乾選中溫 60°C，時間加長\n\n價格：大物洗 NT$90 起\n\n👉 查看空機：' + LIFF_WASH }]
  },
  '推薦碼': {
    tags: [],
    messages: [{ type: 'text', text: '🎁 推薦好友計畫：\n\n分享你的推薦碼，好友首次消費時輸入：\n👉 雙方各得 NT$10 洗衣金！\n\n如何取得推薦碼？\n1. 開啟雲管家 → 我的帳戶\n2. 點選「推薦好友」\n3. 複製推薦碼分享給朋友\n\n📱 立即查看：' + LIFF_PROFILE }]
  },
  '合約': {
    tags: ['B2B_合約查詢'],
    messages: [{ type: 'text', text: '📄 雲管家合約說明：\n\n📋 月租方案：\n• 最短 3 個月起簽\n• 每月自動續約\n• 提前 30 天通知可終止\n\n📋 年租方案：\n• 一年期合約\n• 年付享 2 個月折扣\n• 含免費系統教學\n\n📋 客製化：\n• 依專案需求議定\n• 含 SLA 服務保障\n\n📞 詳細合約內容請聯繫顧問：\n' + CONTACT_PHONE + '\n📧 ' + CONTACT_EMAIL }]
  },
  '安裝': {
    tags: ['B2B_安裝查詢'],
    messages: [{ type: 'text', text: '🔧 安裝與導入流程：\n\n📅 標準時程：7-14 個工作天\n\n1️⃣ 簽約後 Day 1-2\n   → 現場場勘 + 網路環境確認\n\n2️⃣ Day 3-5\n   → 工控機安裝 + 通訊線路佈建\n\n3️⃣ Day 5-7\n   → 系統設定 + 機台串接測試\n\n4️⃣ Day 7-14\n   → 試營運 + 店主教育訓練\n\n✅ 全程免費安裝\n✅ 不需更換現有機器\n✅ 遠端支援 + 到場服務\n\n📞 預約場勘：' + CONTACT_PHONE }]
  },
};

// ---- Franchise secondary segmentation ----
function buildFranchiseSegmentReply() {
  return {
    type: 'flex', altText: '請問你目前的狀態是？',
    contents: {
      type: 'bubble', size: 'mega',
      body: {
        type: 'box', layout: 'vertical', spacing: 'md', paddingAll: '20px',
        contents: [
          { type: 'text', text: '想更了解你的需求，請問目前的狀態是？', size: 'md', wrap: true, weight: 'bold' }
        ]
      },
      footer: {
        type: 'box', layout: 'vertical', spacing: 'md', paddingAll: '15px',
        contents: [
          { type: 'button', style: 'primary', color: '#AAAAAA', height: 'sm', action: { type: 'postback', label: '🅰️ 還在觀望，想多了解', data: 'action=franchise_observe', displayText: '還在觀望，想多了解' } },
          { type: 'button', style: 'primary', color: BRAND_GOLD, height: 'sm', action: { type: 'postback', label: '🅱️ 已有看好地點，認真評估', data: 'action=franchise_hot', displayText: '已有看好地點，認真評估中' } },
          { type: 'button', style: 'primary', color: BRAND_PRIMARY, height: 'sm', action: { type: 'postback', label: '🅾️ 已有洗衣店，想升級系統', data: 'action=franchise_upgrade', displayText: '已有洗衣店，想升級系統' } }
        ]
      }
    }
  };
}

// ---- Webhook Signature Verification ----
function verifyLineSignature(rawBody, signature) {
  if (!LINE_CHANNEL_SECRET) return false;
  const hash = crypto.createHmac('SHA256', LINE_CHANNEL_SECRET).update(rawBody).digest('base64');
  return hash === signature;
}

// ---- Enhanced Fuzzy Matching Map ----
const FUZZY_MAP = [
  // Franchise / B2B entry
  { keywords: ['創業', '開店', '加盟', '店主', '業主'], reply: '我要創業' },
  { keywords: ['獲利', '賺錢', '投報率', 'ROI', '回收'], reply: '獲利分析' },
  { keywords: ['區域', '地點', '選址'], reply: '區域評估' },
  { keywords: ['成功案例', '案例'], reply: '成功案例' },
  { keywords: ['傳統', '比較', '差別'], reply: '傳統比較' },
  // B2B - pricing plans
  { keywords: ['方案', '價格', '費用', '多少錢', '月租', '年租', '訂閱', '系統費用'], reply: '方案' },
  // B2B - demo
  { keywords: ['Demo', 'demo', 'DEMO', '展示', '預約', '試用', '預約Demo', '免費Demo', '免費demo'], reply: '免費Demo' },
  // B2B - quote
  { keywords: ['報價', '客製', '客製化', '買斷'], reply: '報價' },
  // Customer - pricing (use more specific terms to avoid overlap with B2B '方案')
  { keywords: ['洗衣價格', '洗衣多少錢', '洗一次', '收費', '洗衣費用'], reply: '價格' },
  // Customer - topup
  { keywords: ['儲值', '加值', '充值'], reply: '儲值' },
  // Customer - promotions
  { keywords: ['優惠', '折扣', '特價', '優惠券', '促銷'], reply: '優惠' },
  // Customer - dryer
  { keywords: ['烘衣', '烘乾', '烘衣機'], reply: '烘衣' },
  // Customer - hours
  { keywords: ['營業時間', '幾點', '幾點開', '幾點關', '24小時'], reply: '營業時間' },
  // Customer - tutorial
  { keywords: ['怎麼用', '教學', '第一次', '使用方法', '新手'], reply: '教學' },
  // Customer - stores
  { keywords: ['地址', '在哪', '怎麼去', '門市', '哪裡'], reply: '門市' },
  // Customer - support
  { keywords: ['客服', '真人', '人工', '找人'], reply: '__customer_service__' },
  // Customer - membership
  { keywords: ['會員', '等級', '點數', '積分'], reply: '會員' },
  // Customer - tips
  { keywords: ['洗衣知識', '小常識', '洗衣技巧', '小撇步', '知識'], reply: '洗衣知識' },
  // Legacy
  { keywords: ['不穩', '當機'], reply: '系統不穩怎麼辦' },
  { keywords: ['故障', '壞了', '報修', '維修', '沒反應', '不能用'], reply: '故障報修' },
  { keywords: ['支付', '付款', 'LINE Pay', 'linepay'], reply: '行動支付' },
  { keywords: ['空機', '有沒有位', '還有機器', '有位子'], reply: '空機' },
  { keywords: ['退款', '退費', '退錢', '退回'], reply: '退款' },
  { keywords: ['棉被', '被子', '大型', '窗簾', '毛毯'], reply: '棉被' },
  { keywords: ['推薦碼', '邀請碼', '推薦好友', '分享碼'], reply: '推薦碼' },
  { keywords: ['合約', '簽約', '合同', '契約'], reply: '合約' },
  { keywords: ['安裝', '施工', '工期', '場勘', '導入'], reply: '安裝' },
];

// ===== Static Assets for LINE Imagemap =====
// Serve imagemap images: GET /assets/:name/:width
// LINE requires: baseUrl/240, baseUrl/300, baseUrl/460, baseUrl/700, baseUrl/1040
app.get('/assets/:name/:width', (req, res) => {
  const { name, width } = req.params;
  const validWidths = ['240', '300', '460', '700', '1040'];
  if (!validWidths.includes(width)) {
    return res.status(400).json({ error: 'Invalid width. Valid: 240, 300, 460, 700, 1040' });
  }
  // Sanitize name to prevent path traversal
  const safeName = name.replace(/[^a-zA-Z0-9_-]/g, '');
  const filePath = path.join(__dirname, 'public', 'assets', `${safeName}.png`);
  if (fs.existsSync(filePath)) {
    res.setHeader('Content-Type', 'image/png');
    res.setHeader('Cache-Control', 'public, max-age=86400');
    fs.createReadStream(filePath).pipe(res);
  } else {
    res.status(404).json({ error: 'Asset not found' });
  }
});

// ---- LINE Webhook Endpoint ----
app.post('/api/line/webhook', async (req, res) => {
  // Verify signature
  const signature = req.headers['x-line-signature'];
  if (!signature || !verifyLineSignature(req.rawBody, signature)) {
    console.warn('[Webhook] Invalid signature');
    return res.status(401).json({ error: 'Invalid signature' });
  }

  res.status(200).json({ ok: true }); // Reply immediately to LINE

  const events = req.body?.events || [];
  for (const event of events) {
    try {
      const userId = event.source?.userId;
      if (!userId) continue;

      switch (event.type) {
        case 'follow': await handleFollow(event, userId); break;
        case 'unfollow': await handleUnfollow(userId); break;
        case 'postback': await handlePostback(event, userId); break;
        case 'message': {
          if (event.message?.type === 'text') {
            await handleTextMessage(event, userId, event.message.text.trim());
          } else if (event.message?.type === 'location') {
            await handleLocationMessage(event, userId, event.message);
          } else if (['image', 'sticker', 'audio', 'video'].includes(event.message?.type)) {
            await lineReply(event.replyToken, [{
              type: 'text',
              text: '感謝您的訊息！目前我只能處理文字喔 📝\n\n試試輸入關鍵字：\n🔍 空機 | 📍 門市 | 💰 價格\n📖 教學 | 🎁 優惠 | 🔧 故障報修'
            }]);
          }
          break;
        }
      }
    } catch (e) {
      console.error('[Webhook] Event error:', e.message);
    }
  }
});

// ---- Event Handlers ----

async function handleFollow(event, userId) {
  console.log(`[Webhook] Follow: ${userId}`);
  await addUserTag(userId, '新好友');

  // Check if returning user (has existing tags)
  const existingTags = await db.query('SELECT tag FROM user_tags WHERE line_user_id = $1 AND tag NOT IN ($2, $3)', [userId, '新好友', '已封鎖']);
  if (existingTags.rows.some(r => ['顧客', '新會員', '活躍會員'].includes(r.tag))) {
    // Returning customer
    await linkRichMenuToUser(userId, 'b2c');
    const msgs = buildCustomerWelcomeReply();
    await lineReply(event.replyToken, msgs);
  } else if (existingTags.rows.some(r => ['B2B_業主', '業主_興趣', '加盟_興趣'].includes(r.tag))) {
    // Returning business user
    await linkRichMenuToUser(userId, 'b2b');
    const msgs = buildBusinessWelcomeReply();
    await lineReply(event.replyToken, msgs);
  } else {
    // New user - show identity selection
    await linkRichMenuToUser(userId, 'default');
    await lineReply(event.replyToken, [buildWelcomeFlexMessage()]);
  }
}

async function handleUnfollow(userId) {
  console.log(`[Webhook] Unfollow: ${userId}`);
  await addUserTag(userId, '已封鎖', 'system');
}

async function handleLocationMessage(event, userId, location) {
  console.log(`[Webhook] Location: ${userId} → lat:${location.latitude}, lng:${location.longitude}`);

  const stores = [
    { name: '悠洗自助洗衣', lat: 23.4800, lng: 120.4491, addr: '嘉義市東區文雅街181號' },
    { name: '吼你洗 玉清店', lat: 24.5700, lng: 120.8200, addr: '苗栗市玉清路51號' },
    { name: '吼你洗 農會店', lat: 24.5650, lng: 120.8180, addr: '苗栗市為公路290號' },
    { name: '熊愛洗自助洗衣', lat: 24.1810, lng: 120.6460, addr: '台中市西屯區福聯街22巷2號' },
    { name: '上好洗自助洗衣', lat: 22.6273, lng: 120.3560, addr: '高雄市鳳山區北平路214號' },
  ];

  // Calculate distances and find nearest
  function calcDist(lat1, lng1, lat2, lng2) {
    const R = 6371;
    const dLat = (lat2 - lat1) * Math.PI / 180;
    const dLng = (lng2 - lng1) * Math.PI / 180;
    const a = Math.sin(dLat/2)**2 + Math.cos(lat1*Math.PI/180) * Math.cos(lat2*Math.PI/180) * Math.sin(dLng/2)**2;
    return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
  }

  const sorted = stores.map(s => ({
    ...s,
    dist: calcDist(location.latitude, location.longitude, s.lat, s.lng)
  })).sort((a, b) => a.dist - b.dist);

  const nearest = sorted[0];
  const second = sorted[1];

  await lineReply(event.replyToken, [{
    type: 'text',
    text: `📍 離您最近的門市：\n\n🥇 ${nearest.name}\n📍 ${nearest.addr}\n📏 約 ${nearest.dist.toFixed(1)} 公里\n\n🥈 ${second.name}\n📍 ${second.addr}\n📏 約 ${second.dist.toFixed(1)} 公里\n\n🔍 查看空機狀態：\n${LIFF_WASH}\n\n📱 Google Maps 導航：\nhttps://www.google.com/maps/search/?api=1&query=${encodeURIComponent(nearest.addr)}`
  }]);
}

async function handlePostback(event, userId) {
  const data = new URLSearchParams(event.postback?.data || '');
  const action = data.get('action');
  console.log(`[Webhook] Postback: ${userId} → ${action}`);

  // Track interaction
  await trackInteraction(userId, 'postback');

  switch (action) {
    case 'welcome_customer': {
      await addUserTag(userId, '顧客');
      await addUserTag(userId, '新會員');
      await removeUserTag(userId, '新好友');
      await linkRichMenuToUser(userId, 'b2c');
      const msgs = buildCustomerWelcomeReply();
      await lineReply(event.replyToken, msgs);
      break;
    }
    case 'welcome_business': {
      await addUserTag(userId, 'B2B_業主');
      await removeUserTag(userId, '新好友');
      await linkRichMenuToUser(userId, 'b2b');
      const bizMsgs = buildBusinessWelcomeReply();
      await lineReply(event.replyToken, bizMsgs);
      break;
    }
    case 'show_plans': {
      await addUserTag(userId, 'B2B_方案查詢');
      await lineReply(event.replyToken, [buildPlansCard()]);
      break;
    }
    case 'welcome_franchise': {
      await addUserTag(userId, '業主_興趣');
      await removeUserTag(userId, '新好友');
      await linkRichMenuToUser(userId, 'b2b');
      const msgs = buildFranchiseWelcomeReply();
      msgs.push(buildFranchiseSegmentReply());
      await lineReply(event.replyToken, msgs);
      break;
    }
    case 'keyword_startup': {
      const entry = KEYWORD_REPLIES['我要創業'];
      for (const tag of entry.tags) await addUserTag(userId, tag);
      await lineReply(event.replyToken, entry.messages);
      break;
    }
    case 'keyword_profit': {
      const entry = KEYWORD_REPLIES['獲利分析'];
      for (const tag of entry.tags) await addUserTag(userId, tag);
      await lineReply(event.replyToken, entry.messages);
      break;
    }
    case 'success_cases': {
      await lineReply(event.replyToken, [buildSuccessCasesCarousel()]);
      break;
    }
    case 'topup_intro': {
      await lineReply(event.replyToken, [buildTopupBenefitsCard()]);
      break;
    }
    case 'show_stores': {
      await lineReply(event.replyToken, [buildStoreListCarousel()]);
      break;
    }
    case 'franchise_observe': {
      await addUserTag(userId, '加盟_觀望');
      await lineReply(event.replyToken, [{ type: 'text', text: '沒問題！我們會持續分享成功案例和經營知識給你。\n\n有任何問題隨時問我，也可以回覆「成功案例」看看其他老闆的經驗。' }]);
      break;
    }
    case 'franchise_hot': {
      await addUserTag(userId, '加盟_熱leads');
      await lineReply(event.replyToken, [{ type: 'text', text: '太好了！\n\n我們的業務顧問會在 24 小時內聯繫你，幫你做完整的選址分析報告。\n\n請先提供：\n1️⃣ 你的姓名\n2️⃣ 看好的地點地址\n3️⃣ 方便聯繫的電話\n\n直接回覆即可！' }]);
      break;
    }
    case 'franchise_upgrade': {
      await addUserTag(userId, '加盟_升級');
      await lineReply(event.replyToken, [{ type: 'text', text: '很高興你已經在洗衣業了！\n\n雲管家可以幫你現有的店升級智慧系統，不需要換機器。\n\n我們會安排專人為你做免費的系統 Demo 和報價。\n\n請提供：\n1️⃣ 你的店名和地址\n2️⃣ 目前機器數量和品牌\n3️⃣ 方便聯繫的時間\n\n直接回覆即可！' }]);
      break;
    }
    case 'start_survey': {
      // Check if already in progress
      const existing = await db.query('SELECT * FROM survey_progress WHERE line_user_id = $1', [userId]);
      if (existing.rows.length > 0) {
        await db.query('DELETE FROM survey_progress WHERE line_user_id = $1', [userId]);
      }

      await db.query(
        `INSERT INTO survey_progress (line_user_id, current_step, data, started_at, updated_at) VALUES ($1, 'name', '{}', NOW(), NOW())`,
        [userId]
      );

      await lineReply(event.replyToken, [{
        type: 'flex', altText: '雲管家諮詢表',
        contents: {
          type: 'bubble',
          styles: { body: { backgroundColor: '#0D0D1A' } },
          body: {
            type: 'box', layout: 'vertical', paddingAll: '20px', spacing: 'md',
            contents: [
              { type: 'text', text: '📋 雲管家諮詢表', color: '#E5B94C', weight: 'bold', size: 'lg' },
              { type: 'box', layout: 'vertical', height: '1px', backgroundColor: '#333355', margin: 'md' },
              { type: 'text', text: '我們需要了解您的基本資訊，以便提供最適合的方案。', color: '#E0E0E0', size: 'sm', wrap: true, margin: 'md' },
              { type: 'text', text: '📝 共 8-10 個問題，約 2 分鐘完成', color: '#999999', size: 'xs', margin: 'sm' },
              { type: 'box', layout: 'vertical', height: '1px', backgroundColor: '#333355', margin: 'md' },
              { type: 'text', text: '👤 請輸入您的姓名：', color: '#FFFFFF', weight: 'bold', size: 'md', margin: 'md' }
            ]
          }
        }
      }]);
      break;
    }
    case 'survey_answer': {
      const answerStep = data.get('step');
      const answerValue = data.get('value');

      const surveyRow = await db.query('SELECT * FROM survey_progress WHERE line_user_id = $1', [userId]);
      if (surveyRow.rows.length === 0) {
        await lineReply(event.replyToken, [{ type: 'text', text: '⚠️ 問卷已過期，請重新開始。輸入「方案」查看方案。' }]);
        break;
      }

      const surveyData = surveyRow.rows[0].data || {};
      let nextStep = null;
      let nextQuestion = null;

      switch (answerStep) {
        case 'plan': {
          const planLabels = { monthly: '月租方案', annual: '年租方案', custom: '客製化', undecided: '未決定' };
          surveyData.plan_type = planLabels[answerValue] || answerValue;
          nextStep = 'has_store';
          nextQuestion = '🏪 您目前是否已有洗衣店？\n\n請回覆「有」或「沒有」';
          break;
        }
        case 'has_protocol': {
          if (answerValue === 'yes_know') {
            surveyData.has_protocol = true;
            nextStep = 'protocol_info';
            nextQuestion = '📡 請輸入通訊協議/控制板的資訊：\n（型號、品牌、介面類型等）';
          } else if (answerValue === 'yes_unknown') {
            surveyData.has_protocol = true;
            surveyData.protocol_info = '有，但不確定型號';
            nextStep = 'timeline';
          } else {
            surveyData.has_protocol = false;
            nextStep = 'timeline';
          }
          break;
        }
        case 'store_size': {
          const sizeLabels = { small: '小型(3-5台)', medium: '中型(6-10台)', large: '大型(11+台)' };
          surveyData.store_size = sizeLabels[answerValue] || answerValue;
          nextStep = 'machine_brand';
          nextQuestion = '🏭 預計使用什麼品牌/型號的機器？\n（例如：海爾、LG、自組機、還沒決定 等）';
          break;
        }
        case 'timeline': {
          const timeLabels = { '1month': '1個月內', '1-3months': '1-3個月', '3months+': '3個月以上', exploring: '只是先了解' };
          surveyData.timeline = timeLabels[answerValue] || answerValue;
          nextStep = 'notes';
          nextQuestion = '💬 還有什麼想告訴我們的嗎？\n（特殊需求、問題等，沒有請輸入「無」）';
          break;
        }
        default:
          break;
      }

      if (nextStep) {
        await db.query(
          'UPDATE survey_progress SET current_step = $1, data = $2, updated_at = NOW() WHERE line_user_id = $3',
          [nextStep, JSON.stringify(surveyData), userId]
        );
      }

      // If nextStep needs postback buttons (timeline from has_protocol)
      if (nextStep === 'timeline' && answerStep !== 'timeline') {
        await lineReply(event.replyToken, [{
          type: 'flex', altText: '預計上線時間',
          contents: {
            type: 'bubble',
            styles: { body: { backgroundColor: '#0D0D1A' }, footer: { backgroundColor: '#0D0D1A', separator: false } },
            body: { type: 'box', layout: 'vertical', paddingAll: '16px', contents: [{ type: 'text', text: '⏱ 預計何時要上線？', color: '#E5B94C', weight: 'bold', size: 'md' }] },
            footer: {
              type: 'box', layout: 'vertical', paddingAll: '16px', spacing: 'sm',
              contents: [
                { type: 'button', style: 'primary', color: '#3A3A8C', height: 'sm', action: { type: 'postback', label: '1 個月內', data: 'action=survey_answer&step=timeline&value=1month', displayText: '1個月內' } },
                { type: 'button', style: 'primary', color: '#3A3A8C', height: 'sm', action: { type: 'postback', label: '1-3 個月', data: 'action=survey_answer&step=timeline&value=1-3months', displayText: '1-3個月' } },
                { type: 'button', style: 'primary', color: '#3A3A8C', height: 'sm', action: { type: 'postback', label: '3 個月以上', data: 'action=survey_answer&step=timeline&value=3months+', displayText: '3個月以上' } },
                { type: 'button', style: 'secondary', height: 'sm', action: { type: 'postback', label: '只是先了解', data: 'action=survey_answer&step=timeline&value=exploring', displayText: '先了解' } }
              ]
            }
          }
        }]);
        break;
      }

      // If nextStep needs store_size buttons (from store_size answer to machine_brand)
      if (nextStep === 'machine_brand' && answerStep === 'store_size') {
        await lineReply(event.replyToken, [{ type: 'text', text: nextQuestion }]);
        break;
      }

      if (nextQuestion) {
        await lineReply(event.replyToken, [{ type: 'text', text: nextQuestion }]);
      }
      break;
    }
    default:
      console.log(`[Webhook] Unknown postback action: ${action}`);
  }
}

// Helper: get time-based greeting for Taiwan timezone (UTC+8)
function getTimeGreeting() {
  const now = new Date();
  const h = (now.getUTCHours() + 8) % 24;
  if (h >= 5 && h < 12) return '早安';
  if (h >= 12 && h < 18) return '午安';
  return '晚安';
}

// Greeting keywords
const GREETINGS = ['你好', '哈囉', 'hi', 'hello', '嗨', '安安', '早安', '午安', '晚安', '嘿', 'hey'];
// Thank-you keywords
const THANKS = ['謝謝', '感謝', '感恩', 'thanks', 'thx', '3q', 'thank you', '多謝'];

// === Customer Service Trigger Handler ===
async function handleCustomerServiceTrigger(event, userId) {
  try {
    // Find user's most recently bound store
    const boundStore = await db.query(
      `SELECT usb.store_id, s.name, s.group_id FROM user_store_bindings usb
       JOIN stores s ON usb.store_id = s.id
       WHERE usb.line_user_id = $1 ORDER BY usb.bound_at DESC LIMIT 1`,
      [userId]
    );

    if (boundStore.rows.length === 0) {
      await lineReply(event.replyToken, [{ type: 'text', text: '⚠️ 您尚未綁定任何門市。\n請先掃描門市 QR Code 綁定後再使用客服功能。' }]);
      return;
    }

    const storeInfo = boundStore.rows[0];

    // Find store admin for this store's group
    const storeAdmin = await db.query(
      `SELECT line_user_id FROM user_roles WHERE group_id = $1 AND role = 'store_admin' LIMIT 1`,
      [storeInfo.group_id]
    );
    const adminLineId = storeAdmin.rows.length > 0 ? storeAdmin.rows[0].line_user_id : null;

    // Create customer service session
    await db.query(
      `INSERT INTO customer_service_sessions (customer_line_id, store_id, store_admin_line_id, status, created_at)
       VALUES ($1, $2, $3, 'active', NOW())`,
      [userId, storeInfo.store_id, adminLineId]
    );

    await lineReply(event.replyToken, [{
      type: 'flex', altText: '已連線門市客服',
      contents: {
        type: 'bubble',
        styles: { body: { backgroundColor: '#0D0D1A' } },
        body: {
          type: 'box', layout: 'vertical', paddingAll: '20px', spacing: 'md',
          contents: [
            { type: 'text', text: '💬 門市客服', color: '#E5B94C', weight: 'bold', size: 'lg' },
            { type: 'box', layout: 'vertical', height: '1px', backgroundColor: '#333355', margin: 'md' },
            { type: 'text', text: `已連線至 ${storeInfo.name} 客服`, color: '#FFFFFF', size: 'md', margin: 'md', wrap: true },
            { type: 'text', text: '請直接輸入您的問題，我們會盡快為您回覆。', color: '#999999', size: 'sm', margin: 'sm', wrap: true },
            { type: 'box', layout: 'vertical', height: '1px', backgroundColor: '#333355', margin: 'md' },
            { type: 'text', text: '⏱ 客服對話 30 分鐘內有效', color: '#666666', size: 'xxs', margin: 'sm' },
            { type: 'text', text: '💡 輸入「結束客服」可結束對話', color: '#666666', size: 'xxs' }
          ]
        }
      }
    }]);
  } catch (e) {
    console.error('[CS Trigger]', e.message);
    await lineReply(event.replyToken, [{ type: 'text', text: '⚠️ 客服系統暫時無法使用，請稍後再試。' }]);
  }
}

async function handleTextMessage(event, userId, text) {
  console.log(`[Webhook] Message: ${userId} → "${text}"`);

  // Track interaction
  await trackInteraction(userId, 'message');

  const lowerText = text.toLowerCase();

  // === (1) Check if user is in active customer service session ===
  try {
    const activeCS = await db.query(
      `SELECT cs.*, s.name as store_name FROM customer_service_sessions cs
       JOIN stores s ON cs.store_id = s.id
       WHERE cs.customer_line_id = $1 AND cs.status = 'active'
       AND cs.created_at > NOW() - INTERVAL '30 minutes'
       ORDER BY cs.created_at DESC LIMIT 1`,
      [userId]
    );

    if (activeCS.rows.length > 0) {
      const session = activeCS.rows[0];

      // Check if user wants to end
      if (['結束客服', '結束', '離開客服'].includes(text.trim())) {
        await db.query(`UPDATE customer_service_sessions SET status='ended', ended_at=NOW() WHERE id=$1`, [session.id]);
        await lineReply(event.replyToken, [{ type: 'text', text: `✅ 客服對話已結束。\n感謝您的諮詢，如需再次聯繫門市客服，請輸入「客服」。` }]);
        return;
      }

      // Forward message to store admin
      if (session.store_admin_line_id) {
        const profile = await getLineProfile(userId);
        const customerName = profile?.displayName || '顧客';
        await sendLinePush(session.store_admin_line_id, [{
          type: 'flex', altText: `${session.store_name} 客服訊息`,
          contents: {
            type: 'bubble',
            styles: { body: { backgroundColor: '#0D0D1A' } },
            body: {
              type: 'box', layout: 'vertical', paddingAll: '16px', spacing: 'sm',
              contents: [
                { type: 'text', text: `💬 ${session.store_name} 客服`, color: '#E5B94C', weight: 'bold', size: 'sm' },
                { type: 'box', layout: 'vertical', height: '1px', backgroundColor: '#333355', margin: 'sm' },
                { type: 'text', text: `來自：${customerName}`, color: '#999999', size: 'xs', margin: 'sm' },
                { type: 'text', text: text, color: '#FFFFFF', size: 'md', wrap: true, margin: 'md' },
                { type: 'box', layout: 'vertical', height: '1px', backgroundColor: '#333355', margin: 'md' },
                { type: 'text', text: `回覆方式：@回覆 ${customerName} 您的回覆內容`, color: '#666666', size: 'xxs', wrap: true, margin: 'sm' }
              ]
            }
          }
        }]);
        await lineReply(event.replyToken, [{ type: 'text', text: `📨 訊息已轉發給 ${session.store_name} 客服，請稍候回覆。\n\n輸入「結束客服」可結束對話。` }]);
      } else {
        await lineReply(event.replyToken, [{ type: 'text', text: `⚠️ 此門市尚未設定客服人員，請直接撥打門市電話。\n\n輸入「結束客服」可結束對話。` }]);
      }
      return;
    }
  } catch (e) { console.error('[CS Session Check]', e.message); }

  // === (2) Check if store admin is replying to customer ===
  if (text.startsWith('@回覆 ') || text.startsWith('@回覆')) {
    try {
      const roleInfo = await getUserRole(userId);
      if (roleInfo.role === 'store_admin' || roleInfo.role === 'super_admin') {
        const match = text.match(/^@回覆\s+(\S+)\s+([\s\S]+)/);
        if (match) {
          const targetName = match[1];
          const replyContent = match[2].trim();

          const adminSession = await db.query(
            `SELECT cs.*, s.name as store_name FROM customer_service_sessions cs
             JOIN stores s ON cs.store_id = s.id
             WHERE cs.store_admin_line_id = $1 AND cs.status = 'active'
             AND cs.created_at > NOW() - INTERVAL '30 minutes'
             ORDER BY cs.created_at DESC LIMIT 1`,
            [userId]
          );

          if (adminSession.rows.length > 0) {
            const sess = adminSession.rows[0];
            await sendLinePush(sess.customer_line_id, [{
              type: 'flex', altText: `${sess.store_name} 客服回覆`,
              contents: {
                type: 'bubble',
                styles: { body: { backgroundColor: '#0D0D1A' } },
                body: {
                  type: 'box', layout: 'vertical', paddingAll: '16px', spacing: 'sm',
                  contents: [
                    { type: 'text', text: `💬 ${sess.store_name} 客服回覆`, color: '#4CAF50', weight: 'bold', size: 'sm' },
                    { type: 'box', layout: 'vertical', height: '1px', backgroundColor: '#333355', margin: 'sm' },
                    { type: 'text', text: replyContent, color: '#FFFFFF', size: 'md', wrap: true, margin: 'md' }
                  ]
                }
              }
            }]);
            // Refresh session timeout
            await db.query(`UPDATE customer_service_sessions SET created_at=NOW() WHERE id=$1`, [sess.id]);
            await lineReply(event.replyToken, [{ type: 'text', text: `✅ 已回覆顧客「${targetName}」` }]);
          } else {
            await lineReply(event.replyToken, [{ type: 'text', text: '⚠️ 沒有找到進行中的客服對話。' }]);
          }
          return;
        }
      }
    } catch (e) { console.error('[CS Admin Reply]', e.message); }
  }

  // === (3) Check if user is in survey progress ===
  try {
    const surveyProgress = await db.query(
      `SELECT * FROM survey_progress WHERE line_user_id = $1 AND updated_at > NOW() - INTERVAL '1 hour'`,
      [userId]
    );

    if (surveyProgress.rows.length > 0) {
      const progress = surveyProgress.rows[0];
      const surveyData = progress.data || {};
      const step = progress.current_step;
      const userInput = text.trim();

      let nextStep = null;
      let nextQuestion = null;

      switch (step) {
        case 'name':
          surveyData.contact_name = userInput;
          nextStep = 'phone';
          nextQuestion = '📱 請輸入您的聯絡電話：';
          break;
        case 'phone':
          surveyData.phone = userInput;
          nextStep = 'plan';
          nextQuestion = null;
          break;
        case 'has_store':
          if (userInput === '是' || userInput === '有') {
            surveyData.has_store = true;
            nextStep = 'store_name';
            nextQuestion = '🏪 請輸入您洗衣店的名稱：';
          } else {
            surveyData.has_store = false;
            nextStep = 'store_size';
            nextQuestion = null;
          }
          break;
        case 'store_name':
          surveyData.store_name = userInput;
          nextStep = 'store_address';
          nextQuestion = '📍 請輸入洗衣店的地址：';
          break;
        case 'store_address':
          surveyData.store_address = userInput;
          nextStep = 'machine_count';
          nextQuestion = '🔢 目前有幾台機器？（輸入數字）';
          break;
        case 'machine_count':
          surveyData.machine_count = parseInt(userInput) || 0;
          nextStep = 'machine_brand';
          nextQuestion = '🏭 機器品牌/型號？（例如：海爾、LG、自組機 等）';
          break;
        case 'machine_brand':
          surveyData.machine_brand = userInput;
          nextStep = 'has_protocol';
          nextQuestion = null;
          break;
        case 'protocol_info':
          surveyData.protocol_info = userInput;
          nextStep = 'timeline';
          nextQuestion = null;
          break;
        case 'notes':
          surveyData.notes = userInput;
          nextStep = 'done';
          break;
        default:
          nextStep = 'done';
          break;
      }

      if (nextStep === 'done') {
        // Save completed survey
        const profile = await getLineProfile(userId);
        await db.query(
          `INSERT INTO b2b_inquiries (line_user_id, display_name, contact_name, phone, plan_type, has_store, store_name, store_address, machine_count, machine_brand, has_protocol, protocol_info, store_size, timeline, notes, status, created_at)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,'new',NOW())`,
          [userId, profile?.displayName, surveyData.contact_name, surveyData.phone, surveyData.plan_type,
           surveyData.has_store || false, surveyData.store_name, surveyData.store_address,
           surveyData.machine_count, surveyData.machine_brand, surveyData.has_protocol || false,
           surveyData.protocol_info, surveyData.store_size, surveyData.timeline, surveyData.notes]
        );

        await db.query('DELETE FROM survey_progress WHERE line_user_id = $1', [userId]);

        // Notify super admin
        const SUPER_ADMIN_ID = process.env.SUPER_ADMIN_LINE_ID || 'Ubdcdd269e115bf9ac492288adbc0115e';
        await sendLinePush(SUPER_ADMIN_ID, [{
          type: 'flex', altText: '📋 新買家諮詢表',
          contents: {
            type: 'bubble',
            styles: { header: { backgroundColor: '#1A1A3A' }, body: { backgroundColor: '#0D0D1A' } },
            header: {
              type: 'box', layout: 'vertical', paddingAll: '16px',
              contents: [{ type: 'text', text: '📋 新買家諮詢表', color: '#E5B94C', weight: 'bold', size: 'lg' }]
            },
            body: {
              type: 'box', layout: 'vertical', paddingAll: '16px', spacing: 'sm',
              contents: [
                { type: 'text', text: `👤 ${surveyData.contact_name || '未填'}`, color: '#FFFFFF', size: 'md' },
                { type: 'text', text: `📱 ${surveyData.phone || '未填'}`, color: '#E0E0E0', size: 'sm' },
                { type: 'text', text: `📦 方案：${surveyData.plan_type || '未選'}`, color: '#E0E0E0', size: 'sm' },
                { type: 'text', text: `🏪 ${surveyData.has_store ? '有店' : '尚無店面'}：${surveyData.store_name || '-'}`, color: '#E0E0E0', size: 'sm', wrap: true },
                { type: 'text', text: `📍 ${surveyData.store_address || '-'}`, color: '#999999', size: 'xs', wrap: true },
                { type: 'text', text: `🔧 機器：${surveyData.machine_count || 0}台 ${surveyData.machine_brand || '-'}`, color: '#E0E0E0', size: 'sm', wrap: true },
                { type: 'text', text: `📡 通訊協議：${surveyData.has_protocol ? surveyData.protocol_info || '有' : '無/不確定'}`, color: '#E0E0E0', size: 'sm', wrap: true },
                { type: 'text', text: `⏱ 預計：${surveyData.timeline || '-'}`, color: '#E0E0E0', size: 'sm' },
                { type: 'text', text: `💬 備註：${surveyData.notes || '-'}`, color: '#999999', size: 'xs', wrap: true },
                { type: 'text', text: `LINE: ${profile?.displayName || userId}`, color: '#666666', size: 'xxs', margin: 'md' }
              ]
            }
          }
        }]);

        await lineReply(event.replyToken, [{
          type: 'flex', altText: '感謝您填寫諮詢表！',
          contents: {
            type: 'bubble',
            styles: { body: { backgroundColor: '#0D0D1A' } },
            body: {
              type: 'box', layout: 'vertical', paddingAll: '20px', spacing: 'md',
              contents: [
                { type: 'text', text: '✅ 諮詢表已送出', color: '#4CAF50', weight: 'bold', size: 'lg' },
                { type: 'box', layout: 'vertical', height: '1px', backgroundColor: '#333355', margin: 'md' },
                { type: 'text', text: '感謝您的填寫！我們的顧問會在 1-2 個工作天內與您聯繫。', color: '#E0E0E0', size: 'sm', wrap: true, margin: 'md' },
                { type: 'text', text: '如有急需，可直接聯繫：\n📞 0800-018-888\n📧 contact@cloudmonster.com.tw', color: '#999999', size: 'xs', wrap: true, margin: 'md' }
              ]
            }
          }
        }]);
        return;
      }

      // Save progress and ask next question
      await db.query(
        `UPDATE survey_progress SET current_step = $1, data = $2, updated_at = NOW() WHERE line_user_id = $3`,
        [nextStep, JSON.stringify(surveyData), userId]
      );

      // Questions that need postback buttons
      if (nextStep === 'plan') {
        await lineReply(event.replyToken, [{
          type: 'flex', altText: '請選擇方案',
          contents: {
            type: 'bubble',
            styles: { body: { backgroundColor: '#0D0D1A' }, footer: { backgroundColor: '#0D0D1A', separator: false } },
            body: {
              type: 'box', layout: 'vertical', paddingAll: '16px',
              contents: [{ type: 'text', text: '📦 您感興趣的方案？', color: '#E5B94C', weight: 'bold', size: 'md' }]
            },
            footer: {
              type: 'box', layout: 'vertical', paddingAll: '16px', spacing: 'sm',
              contents: [
                { type: 'button', style: 'primary', color: '#3A3A8C', height: 'sm', action: { type: 'postback', label: '月租 NT$5,000/月', data: 'action=survey_answer&step=plan&value=monthly', displayText: '月租方案' } },
                { type: 'button', style: 'primary', color: '#3A3A8C', height: 'sm', action: { type: 'postback', label: '年租 NT$50,000/年', data: 'action=survey_answer&step=plan&value=annual', displayText: '年租方案' } },
                { type: 'button', style: 'primary', color: '#3A3A8C', height: 'sm', action: { type: 'postback', label: '客製化 / 買斷', data: 'action=survey_answer&step=plan&value=custom', displayText: '客製化方案' } },
                { type: 'button', style: 'secondary', height: 'sm', action: { type: 'postback', label: '還沒決定', data: 'action=survey_answer&step=plan&value=undecided', displayText: '還沒決定' } }
              ]
            }
          }
        }]);
        return;
      }

      if (nextStep === 'has_protocol') {
        await lineReply(event.replyToken, [{
          type: 'flex', altText: '機器通訊協議',
          contents: {
            type: 'bubble',
            styles: { body: { backgroundColor: '#0D0D1A' }, footer: { backgroundColor: '#0D0D1A', separator: false } },
            body: {
              type: 'box', layout: 'vertical', paddingAll: '16px',
              contents: [
                { type: 'text', text: '📡 機器是否有通訊協議/控制板？', color: '#E5B94C', weight: 'bold', size: 'md' },
                { type: 'text', text: '例如：Modbus RTU、RS485、觸控螢幕控制板 等', color: '#999999', size: 'xs', wrap: true, margin: 'sm' }
              ]
            },
            footer: {
              type: 'box', layout: 'vertical', paddingAll: '16px', spacing: 'sm',
              contents: [
                { type: 'button', style: 'primary', color: '#3A3A8C', height: 'sm', action: { type: 'postback', label: '有，我知道型號', data: 'action=survey_answer&step=has_protocol&value=yes_know', displayText: '有通訊協議' } },
                { type: 'button', style: 'primary', color: '#3A3A8C', height: 'sm', action: { type: 'postback', label: '有，但不確定型號', data: 'action=survey_answer&step=has_protocol&value=yes_unknown', displayText: '有但不確定' } },
                { type: 'button', style: 'secondary', height: 'sm', action: { type: 'postback', label: '沒有 / 不清楚', data: 'action=survey_answer&step=has_protocol&value=no', displayText: '沒有通訊協議' } }
              ]
            }
          }
        }]);
        return;
      }

      if (nextStep === 'store_size') {
        await lineReply(event.replyToken, [{
          type: 'flex', altText: '預計店面規模',
          contents: {
            type: 'bubble',
            styles: { body: { backgroundColor: '#0D0D1A' }, footer: { backgroundColor: '#0D0D1A', separator: false } },
            body: {
              type: 'box', layout: 'vertical', paddingAll: '16px',
              contents: [{ type: 'text', text: '📐 預計的店面規模？', color: '#E5B94C', weight: 'bold', size: 'md' }]
            },
            footer: {
              type: 'box', layout: 'vertical', paddingAll: '16px', spacing: 'sm',
              contents: [
                { type: 'button', style: 'primary', color: '#3A3A8C', height: 'sm', action: { type: 'postback', label: '小型（3-5台）', data: 'action=survey_answer&step=store_size&value=small', displayText: '小型 3-5台' } },
                { type: 'button', style: 'primary', color: '#3A3A8C', height: 'sm', action: { type: 'postback', label: '中型（6-10台）', data: 'action=survey_answer&step=store_size&value=medium', displayText: '中型 6-10台' } },
                { type: 'button', style: 'primary', color: '#3A3A8C', height: 'sm', action: { type: 'postback', label: '大型（11台以上）', data: 'action=survey_answer&step=store_size&value=large', displayText: '大型 11台以上' } }
              ]
            }
          }
        }]);
        return;
      }

      if (nextStep === 'timeline') {
        await lineReply(event.replyToken, [{
          type: 'flex', altText: '預計上線時間',
          contents: {
            type: 'bubble',
            styles: { body: { backgroundColor: '#0D0D1A' }, footer: { backgroundColor: '#0D0D1A', separator: false } },
            body: {
              type: 'box', layout: 'vertical', paddingAll: '16px',
              contents: [{ type: 'text', text: '⏱ 預計何時要上線？', color: '#E5B94C', weight: 'bold', size: 'md' }]
            },
            footer: {
              type: 'box', layout: 'vertical', paddingAll: '16px', spacing: 'sm',
              contents: [
                { type: 'button', style: 'primary', color: '#3A3A8C', height: 'sm', action: { type: 'postback', label: '1 個月內', data: 'action=survey_answer&step=timeline&value=1month', displayText: '1個月內' } },
                { type: 'button', style: 'primary', color: '#3A3A8C', height: 'sm', action: { type: 'postback', label: '1-3 個月', data: 'action=survey_answer&step=timeline&value=1-3months', displayText: '1-3個月' } },
                { type: 'button', style: 'primary', color: '#3A3A8C', height: 'sm', action: { type: 'postback', label: '3 個月以上', data: 'action=survey_answer&step=timeline&value=3months+', displayText: '3個月以上' } },
                { type: 'button', style: 'secondary', height: 'sm', action: { type: 'postback', label: '只是先了解', data: 'action=survey_answer&step=timeline&value=exploring', displayText: '先了解' } }
              ]
            }
          }
        }]);
        return;
      }

      // Text input question
      if (nextQuestion) {
        await lineReply(event.replyToken, [{ type: 'text', text: nextQuestion }]);
      }
      return;
    }
  } catch (e) { console.error('[Survey Check]', e.message); }

  // --- Greeting detection ---
  if (GREETINGS.some(g => lowerText.includes(g.toLowerCase()))) {
    let displayName = '你';
    try {
      const profileRes = await fetch(`https://api.line.me/v2/bot/profile/${userId}`, {
        headers: { 'Authorization': `Bearer ${LINE_CHANNEL_ACCESS_TOKEN}` }
      });
      if (profileRes.ok) {
        const profile = await profileRes.json();
        displayName = profile.displayName || '你';
      }
    } catch (e) { /* ignore profile fetch errors */ }

    // Check user tags to personalize greeting
    let isB2B = false;
    try {
      const tagResult = await db.query('SELECT tag FROM user_tags WHERE line_user_id = $1', [userId]);
      const tags = tagResult.rows.map(r => r.tag);
      isB2B = tags.includes('B2B_業主') || tags.includes('加盟_興趣') || tags.includes('加盟_熱leads');
    } catch (e) { /* ignore */ }

    const timeGreeting = getTimeGreeting();
    let greeting = '';
    if (isB2B) {
      greeting = `${displayName} 老闆${timeGreeting}！\n\n我是雲管家智慧洗衣系統的專屬顧問。\n\n想了解什麼呢？\n📋 回覆「方案」查看報價\n📊 回覆「成功案例」看實際成效\n🎯 回覆「免費Demo」預約展示`;
    } else {
      greeting = `${displayName} ${timeGreeting}！\n\n歡迎使用雲管家洗衣服務！\n\n我可以幫你：\n🔍 回覆「空機」查看即時空位\n💰 回覆「儲值」了解優惠\n📍 回覆「門市」查看地址\n❓ 回覆「教學」看使用指南`;
    }

    await lineReply(event.replyToken, [{ type: 'text', text: greeting }]);
    return;
  }

  // --- Thank-you detection ---
  if (THANKS.some(t => lowerText.includes(t.toLowerCase()))) {
    await lineReply(event.replyToken, [{
      type: 'text',
      text: `不客氣！很高興能幫到您 😊\n\n有任何問題隨時問我，祝您洗衣愉快！`
    }]);
    return;
  }

  // Check exact keyword match first
  // Special handling for '客服' keyword (customer service forwarding)
  if (text === '客服') {
    await handleCustomerServiceTrigger(event, userId);
    return;
  }
  const entry = KEYWORD_REPLIES[text];
  if (entry) {
    for (const tag of (entry.tags || [])) await addUserTag(userId, tag);
    await lineReply(event.replyToken, entry.messages);
    return;
  }

  // Fuzzy keyword matching
  for (const { keywords, reply } of FUZZY_MAP) {
    if (keywords.some(k => lowerText.includes(k.toLowerCase()))) {
      // Special handling for customer service fuzzy match
      if (reply === '__customer_service__') {
        await handleCustomerServiceTrigger(event, userId);
        return;
      }
      const matched = KEYWORD_REPLIES[reply];
      if (matched) {
        for (const tag of (matched.tags || [])) await addUserTag(userId, tag);
        await lineReply(event.replyToken, matched.messages);
        return;
      }
    }
  }

  // --- Smart fallback for unmatched messages ---
  // Try to find partial matches and suggest closest keywords
  const suggestions = [];
  if (text.length >= 2) {
    const prefix = text.substring(0, 2);
    for (const { keywords, reply } of FUZZY_MAP) {
      for (const k of keywords) {
        if (k.includes(prefix) || prefix.includes(k.substring(0, 2))) {
          if (!suggestions.includes(reply)) suggestions.push(reply);
        }
      }
    }
  }

  if (suggestions.length > 0) {
    const suggestText = suggestions.slice(0, 3).map(s => `「${s}」`).join('、');
    await lineReply(event.replyToken, [{
      type: 'text',
      text: `抱歉，我不太確定您的需求 🤔\n\n您是不是想問：\n${suggestText}\n\n或者直接輸入以下關鍵字：\n🔍 空機、門市、價格\n💰 儲值、優惠、會員\n📖 教學、烘衣、客服\n💼 方案、Demo、報價`
    }]);
  } else {
    await lineReply(event.replyToken, [{
      type: 'text',
      text: `感謝您的訊息！\n\n您可以試試以下關鍵字：\n\n🧺 顧客服務：\n空機 | 門市 | 價格 | 儲值 | 優惠 | 教學\n\n💼 業主諮詢：\n方案 | Demo | 成功案例 | 獲利分析\n\n需要真人客服？請直接回覆「客服」👋`
    }]);
  }
}

// ---- Scheduled Follow-up API ----

// POST /api/admin/scheduled-followup - Trigger follow-up push messages
app.post('/api/admin/scheduled-followup', async (req, res) => {
  const { tag, days_since, message_type, adminUserId } = req.body;
  if (!tag || !days_since) return res.status(400).json({ error: 'tag and days_since required' });

  try {
    // Verify admin
    if (adminUserId) {
      const role = await db.query('SELECT role FROM user_roles WHERE line_user_id = $1 AND role IN ($2, $3)', [adminUserId, 'super_admin', 'store_admin']);
      if (role.rows.length === 0) return res.status(403).json({ error: 'Admin only' });
    }

    // Find users with the tag created X+ days ago who haven't received a follow-up push
    const result = await db.query(`
      SELECT ut.line_user_id, ut.created_at
      FROM user_tags ut
      WHERE ut.tag = $1
        AND ut.created_at <= NOW() - INTERVAL '1 day' * $2
        AND ut.line_user_id NOT IN (
          SELECT line_user_id FROM user_tags WHERE tag = $3
        )
      ORDER BY ut.created_at ASC
      LIMIT 100
    `, [tag, days_since, `_followup_sent_${tag}`]);

    const users = result.rows;
    let sentCount = 0;

    for (const user of users) {
      try {
        let pushMessages = [];

        // Determine which message to send based on tag or message_type
        if (message_type === 'success_cases' || tag === '加盟_興趣') {
          pushMessages = [buildSuccessCasesCarousel()];
        } else if (message_type === 'laundry_tips' || tag === '新會員') {
          pushMessages = [buildLaundryTipsCarousel()];
        } else if (message_type === 'topup' || tag === '活躍會員') {
          pushMessages = [buildTopupBenefitsCard()];
        } else if (tag === '加盟_觀望') {
          pushMessages = [buildTraditionalVsSmartCard()];
        } else if (tag === '加盟_評估中') {
          pushMessages = [{ type: 'text', text: '嗨！之前你看過我們的獲利分析，有任何問題嗎？\n\n回覆「區域評估」可以免費評估你看好的地點，或直接預約 Demo 讓我們為你詳細說明。' }];
        } else {
          pushMessages = [{ type: 'text', text: '嗨！好久不見，最近有洗衣需求嗎？\n\n回覆「優惠」查看最新活動，或直接點選下方開始洗衣：\n👉 ' + LIFF_WASH }];
        }

        await sendLinePush(user.line_user_id, pushMessages);
        await addUserTag(user.line_user_id, `_followup_sent_${tag}`, 'scheduled');
        sentCount++;
      } catch (pushErr) {
        console.error(`[Followup] Push failed for ${user.line_user_id}:`, pushErr.message);
      }
    }

    res.json({
      success: true,
      candidates: users.length,
      sent: sentCount,
      tag,
      days_since
    });
  } catch (e) {
    console.error('[Followup] Error:', e.message);
    res.status(500).json({ error: e.message });
  }
});

// GET /api/admin/followup-candidates - Preview who would receive follow-ups
app.get('/api/admin/followup-candidates', async (req, res) => {
  const { tag, days_since } = req.query;
  if (!tag || !days_since) return res.status(400).json({ error: 'tag and days_since required' });

  try {
    const result = await db.query(`
      SELECT ut.line_user_id, ut.created_at, m.display_name, m.picture_url
      FROM user_tags ut
      LEFT JOIN members m ON m.line_user_id = ut.line_user_id
      WHERE ut.tag = $1
        AND ut.created_at <= NOW() - INTERVAL '1 day' * $2
        AND ut.line_user_id NOT IN (
          SELECT line_user_id FROM user_tags WHERE tag = $3
        )
      ORDER BY ut.created_at ASC
      LIMIT 100
    `, [tag, parseInt(days_since), `_followup_sent_${tag}`]);

    res.json({
      success: true,
      count: result.rows.length,
      users: result.rows
    });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ---- User Tag Management API ----

// Get user tags
app.get('/api/user/tags', async (req, res) => {
  const { userId } = req.query;
  if (!userId) return res.status(400).json({ error: 'userId required' });
  try {
    const result = await db.query('SELECT tag, source, created_at FROM user_tags WHERE line_user_id = $1 ORDER BY created_at', [userId]);
    res.json({ success: true, tags: result.rows });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Add tag manually (admin)
app.post('/api/user/tags', async (req, res) => {
  const { userId, tag, adminUserId } = req.body;
  if (!userId || !tag) return res.status(400).json({ error: 'userId and tag required' });
  // Verify admin
  const role = await db.query('SELECT role FROM user_roles WHERE line_user_id = $1 AND role IN ($2, $3)', [adminUserId, 'super_admin', 'store_admin']);
  if (role.rows.length === 0) return res.status(403).json({ error: 'Admin only' });
  const ok = await addUserTag(userId, tag, 'manual');
  res.json({ success: ok });
});

// Remove tag (admin)
app.delete('/api/user/tags', async (req, res) => {
  const { userId, tag, adminUserId } = req.body;
  if (!userId || !tag) return res.status(400).json({ error: 'userId and tag required' });
  const role = await db.query('SELECT role FROM user_roles WHERE line_user_id = $1 AND role IN ($2, $3)', [adminUserId, 'super_admin', 'store_admin']);
  if (role.rows.length === 0) return res.status(403).json({ error: 'Admin only' });
  const ok = await removeUserTag(userId, tag);
  res.json({ success: ok });
});

// ===== Store QR Code Binding APIs =====

// POST /api/user/bind-store - Bind user to a store via QR Code
app.post('/api/user/bind-store', async (req, res) => {
  const { lineUserId, storeId } = req.body;
  if (!lineUserId || !storeId) return res.status(400).json({ error: 'lineUserId and storeId required' });

  try {
    // Verify store exists
    const store = await db.query('SELECT id, name, group_id FROM stores WHERE id = $1', [storeId]);
    if (store.rows.length === 0) return res.status(404).json({ error: 'Store not found' });

    // Bind user to store
    await db.query(
      `INSERT INTO user_store_bindings (line_user_id, store_id, bound_at)
       VALUES ($1, $2, NOW())
       ON CONFLICT (line_user_id, store_id) DO UPDATE SET bound_at = NOW()`,
      [lineUserId, storeId]
    );

    // Auto-tag as customer
    await addUserTag(lineUserId, '顧客');

    // Switch to B2C menu
    await linkRichMenuToUser(lineUserId, 'b2c');

    // Send welcome push with store name
    const storeName = store.rows[0].name;
    try {
      await sendLinePush(lineUserId, [{
        type: 'flex', altText: `已綁定 ${storeName}`,
        contents: {
          type: 'bubble',
          styles: { body: { backgroundColor: '#0D0D1A' }, footer: { backgroundColor: '#0D0D1A', separator: false } },
          body: {
            type: 'box', layout: 'vertical', paddingAll: '24px', spacing: 'md',
            contents: [
              { type: 'text', text: '✅ 門市綁定成功', color: '#4CAF50', weight: 'bold', size: 'lg' },
              { type: 'box', layout: 'vertical', height: '1px', backgroundColor: '#333355', margin: 'lg' },
              { type: 'box', layout: 'horizontal', margin: 'lg', contents: [
                { type: 'text', text: '綁定門市', size: 'sm', color: '#999999', flex: 3 },
                { type: 'text', text: storeName, size: 'sm', color: '#FFFFFF', flex: 5, align: 'end', weight: 'bold' }
              ]},
              { type: 'text', text: '您現在可以使用以下功能：', size: 'sm', color: '#999999', margin: 'lg', wrap: true },
              { type: 'text', text: '🔍 查空機  💰 線上付款  🔔 洗好通知', size: 'sm', color: '#E5B94C', margin: 'sm', wrap: true },
              { type: 'text', text: '💬 輸入「客服」可聯繫門市客服', size: 'xs', color: '#999999', margin: 'lg', wrap: true }
            ]
          },
          footer: {
            type: 'box', layout: 'vertical', paddingAll: '16px', spacing: 'sm',
            contents: [
              { type: 'button', style: 'primary', color: '#3A3A8C', height: 'sm',
                action: { type: 'uri', label: '立即查空機', uri: `https://liff.line.me/2009552592-xkDKSJ1Y?tab=wash` } }
            ]
          }
        }
      }]);
    } catch(e) { console.error('[Bind] Push error:', e.message); }

    res.json({ success: true, storeName, storeId });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// GET /api/user/bound-stores - Get user's bound stores
app.get('/api/user/bound-stores', async (req, res) => {
  const lineUserId = req.query.lineUserId;
  if (!lineUserId) return res.status(400).json({ error: 'lineUserId required' });
  try {
    const result = await db.query(
      `SELECT usb.store_id, s.name as store_name, s.address, usb.bound_at
       FROM user_store_bindings usb
       JOIN stores s ON usb.store_id = s.id
       WHERE usb.line_user_id = $1
       ORDER BY usb.bound_at DESC`,
      [lineUserId]
    );
    res.json({ stores: result.rows });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// GET /api/admin/inquiries - List B2B inquiries
app.get('/api/admin/inquiries', async (req, res) => {
  try {
    const result = await db.query(
      `SELECT * FROM b2b_inquiries ORDER BY created_at DESC LIMIT 50`
    );
    res.json({ inquiries: result.rows });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// List all tags with user count (admin analytics)
app.get('/api/admin/tags', async (req, res) => {
  try {
    const result = await db.query(`
      SELECT tag, COUNT(DISTINCT line_user_id) as user_count, MAX(created_at) as last_used
      FROM user_tags GROUP BY tag ORDER BY user_count DESC
    `);
    res.json({ success: true, tags: result.rows });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// List users by tag (admin)
app.get('/api/admin/tags/:tag/users', async (req, res) => {
  try {
    const result = await db.query(`
      SELECT ut.line_user_id, ut.source, ut.created_at, m.display_name, m.picture_url
      FROM user_tags ut
      LEFT JOIN members m ON m.line_user_id = ut.line_user_id
      WHERE ut.tag = $1
      ORDER BY ut.created_at DESC
    `, [req.params.tag]);
    res.json({ success: true, users: result.rows });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ===== Broadcast API (群發訊息) =====

// Helper: Build a VOOM-style flex message with hero image
function buildVoomFlexPost({ imageUrl, title, description, linkUrl, linkLabel }) {
  return {
    type: 'flex',
    altText: title || '雲管家最新消息',
    contents: {
      type: 'bubble', size: 'mega',
      hero: {
        type: 'image',
        url: imageUrl,
        size: 'full',
        aspectRatio: '1:1',
        aspectMode: 'cover'
      },
      body: {
        type: 'box', layout: 'vertical', spacing: 'md', paddingAll: '20px',
        contents: [
          { type: 'text', text: title || '', weight: 'bold', size: 'lg', wrap: true },
          { type: 'text', text: description || '', size: 'sm', color: '#888888', wrap: true, margin: 'md' }
        ]
      },
      footer: {
        type: 'box', layout: 'vertical', spacing: 'sm', paddingAll: '15px',
        contents: [
          { type: 'button', style: 'primary', color: BRAND_PRIMARY,
            action: { type: 'uri', label: linkLabel || '了解更多', uri: linkUrl || OFFICIAL_WEBSITE }
          }
        ]
      }
    }
  };
}

// Helper: Execute a broadcast (used by both immediate and scheduled)
async function executeBroadcast(messages, targetTags, excludeTags, createdBy, targetStoreIds) {
  let sentCount = 0;

  try {
    if (targetStoreIds && targetStoreIds.length > 0) {
      // Store-specific broadcast: query users bound to these stores
      let query = `SELECT DISTINCT usb.line_user_id FROM user_store_bindings usb WHERE usb.store_id = ANY($1)`;
      const params = [targetStoreIds];

      // Also apply tag filters if provided
      if (targetTags && targetTags.length > 0) {
        query = `SELECT DISTINCT usb.line_user_id FROM user_store_bindings usb
                 INNER JOIN user_tags ut ON usb.line_user_id = ut.line_user_id
                 WHERE usb.store_id = ANY($1) AND ut.tag = ANY($2)`;
        params.push(targetTags);
      }

      if (excludeTags && excludeTags.length > 0) {
        query += ` AND usb.line_user_id NOT IN (SELECT DISTINCT line_user_id FROM user_tags WHERE tag = ANY($${params.length + 1}))`;
        params.push(excludeTags);
      }

      const result = await db.query(query, params);
      const userIds = result.rows.map(r => r.line_user_id);

      if (userIds.length === 0) {
        return { success: true, sentCount: 0, message: 'No users found for these stores' };
      }

      // Multicast in batches of 500
      for (let i = 0; i < userIds.length; i += 500) {
        const batch = userIds.slice(i, i + 500);
        try {
          const res = await fetch('https://api.line.me/v2/bot/message/multicast', {
            method: 'POST',
            headers: {
              'Content-Type': 'application/json',
              'Authorization': `Bearer ${LINE_CHANNEL_ACCESS_TOKEN}`,
            },
            body: JSON.stringify({ to: batch, messages }),
          });
          const data = await res.json().catch(() => ({}));
          if (res.ok) {
            sentCount += batch.length;
          } else {
            console.error(`[Broadcast] Store multicast batch error:`, data);
          }
        } catch (batchErr) {
          console.error(`[Broadcast] Store multicast batch failed:`, batchErr.message);
        }
      }
      console.log(`[Broadcast] Store-filtered multicast sent to ${sentCount} users for stores: ${targetStoreIds.join(', ')}`);
    } else if (!targetTags || targetTags.length === 0) {
      // No tag filter: use LINE broadcast API (sends to all followers)
      const res = await fetch('https://api.line.me/v2/bot/message/broadcast', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${LINE_CHANNEL_ACCESS_TOKEN}`,
        },
        body: JSON.stringify({ messages }),
      });
      const data = await res.json().catch(() => ({}));
      if (!res.ok) {
        console.error('[Broadcast] LINE API error:', data);
        return { success: false, error: data.message || 'LINE API error', sentCount: 0 };
      }
      sentCount = -1; // broadcast API doesn't return count; -1 means "all followers"
      console.log('[Broadcast] Sent to all followers');
    } else {
      // Tag filter: query users and use multicast API
      let query = `SELECT DISTINCT line_user_id FROM user_tags WHERE tag = ANY($1)`;
      const params = [targetTags];

      if (excludeTags && excludeTags.length > 0) {
        query += ` AND line_user_id NOT IN (SELECT DISTINCT line_user_id FROM user_tags WHERE tag = ANY($2))`;
        params.push(excludeTags);
      }

      const result = await db.query(query, params);
      const userIds = result.rows.map(r => r.line_user_id);

      if (userIds.length === 0) {
        return { success: true, sentCount: 0, message: 'No users matched the tag filter' };
      }

      // Multicast in batches of 500
      for (let i = 0; i < userIds.length; i += 500) {
        const batch = userIds.slice(i, i + 500);
        try {
          const res = await fetch('https://api.line.me/v2/bot/message/multicast', {
            method: 'POST',
            headers: {
              'Content-Type': 'application/json',
              'Authorization': `Bearer ${LINE_CHANNEL_ACCESS_TOKEN}`,
            },
            body: JSON.stringify({ to: batch, messages }),
          });
          const data = await res.json().catch(() => ({}));
          if (res.ok) {
            sentCount += batch.length;
          } else {
            console.error(`[Broadcast] Multicast batch error:`, data);
          }
        } catch (batchErr) {
          console.error(`[Broadcast] Multicast batch failed:`, batchErr.message);
        }
      }
      console.log(`[Broadcast] Multicast sent to ${sentCount} users`);
    }

    // Log to push_logs
    try {
      await db.query(`
        INSERT INTO push_logs (push_type, recipient_count, message_count, triggered_by, description, success)
        VALUES ($1, $2, $3, $4, $5, $6)
      `, [
        'broadcast',
        sentCount === -1 ? 0 : sentCount,
        messages.length,
        createdBy,
        targetTags ? `Tags: ${targetTags.join(', ')}` : 'All followers',
        true
      ]);
    } catch (logErr) { console.error('[Broadcast] Push log error:', logErr.message); }

    return { success: true, sentCount };

  } catch (e) {
    console.error('[Broadcast] Error:', e.message);
    return { success: false, error: e.message, sentCount: 0 };
  }
}

// GET /api/admin/store-bindings - Get user counts per store
app.get('/api/admin/store-bindings', async (req, res) => {
  try {
    const result = await db.query(`
      SELECT usb.store_id, s.name as store_name, COUNT(DISTINCT usb.line_user_id) as user_count
      FROM user_store_bindings usb
      LEFT JOIN stores s ON usb.store_id = s.id
      GROUP BY usb.store_id, s.name
      ORDER BY user_count DESC
    `);
    res.json({ stores: result.rows });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// POST /api/admin/broadcast - Send broadcast messages
app.post('/api/admin/broadcast', async (req, res) => {
  const { adminUserId, messages, targetTags, excludeTags, targetStoreIds, schedule } = req.body;

  if (!adminUserId) return res.status(400).json({ error: 'adminUserId required' });
  if (!messages || !Array.isArray(messages) || messages.length === 0) {
    return res.status(400).json({ error: 'messages array required (1-5 LINE message objects)' });
  }
  if (messages.length > 5) {
    return res.status(400).json({ error: 'Maximum 5 messages per broadcast' });
  }

  try {
    // Verify super_admin
    const roleCheck = await db.query(
      'SELECT role FROM user_roles WHERE line_user_id = $1 AND role = $2',
      [adminUserId, 'super_admin']
    );
    if (roleCheck.rows.length === 0) {
      return res.status(403).json({ error: 'Only super_admin can broadcast' });
    }

    // Scheduled broadcast
    if (schedule) {
      const scheduledAt = new Date(schedule);
      if (isNaN(scheduledAt.getTime())) {
        return res.status(400).json({ error: 'Invalid schedule date format. Use ISO 8601.' });
      }
      if (scheduledAt <= new Date()) {
        return res.status(400).json({ error: 'Schedule time must be in the future' });
      }

      const insertResult = await db.query(`
        INSERT INTO scheduled_broadcasts (messages, target_tags, exclude_tags, scheduled_at, created_by, target_store_ids)
        VALUES ($1, $2, $3, $4, $5, $6)
        RETURNING id, scheduled_at
      `, [
        JSON.stringify(messages),
        targetTags || null,
        excludeTags || null,
        scheduledAt,
        adminUserId,
        targetStoreIds || null
      ]);

      return res.json({
        success: true,
        scheduled: true,
        broadcastId: insertResult.rows[0].id,
        scheduledAt: insertResult.rows[0].scheduled_at
      });
    }

    // Immediate broadcast
    const result = await executeBroadcast(messages, targetTags, excludeTags, adminUserId, targetStoreIds);
    res.json(result);

  } catch (e) {
    console.error('[Broadcast API] Error:', e.message);
    res.status(500).json({ error: e.message });
  }
});

// GET /api/admin/broadcast/history - View past broadcasts
app.get('/api/admin/broadcast/history', async (req, res) => {
  const { adminUserId, limit: queryLimit } = req.query;

  try {
    if (adminUserId) {
      const roleCheck = await db.query(
        'SELECT role FROM user_roles WHERE line_user_id = $1 AND role IN ($2, $3)',
        [adminUserId, 'super_admin', 'store_admin']
      );
      if (roleCheck.rows.length === 0) {
        return res.status(403).json({ error: 'Admin only' });
      }
    }

    const rowLimit = Math.min(parseInt(queryLimit) || 50, 200);

    // Get push_logs for broadcast type
    const pushLogs = await db.query(`
      SELECT id, push_type, recipient_count, message_count, triggered_by, description, success, created_at
      FROM push_logs
      WHERE push_type = 'broadcast'
      ORDER BY created_at DESC
      LIMIT $1
    `, [rowLimit]);

    // Get scheduled broadcasts
    const scheduled = await db.query(`
      SELECT id, target_tags, exclude_tags, scheduled_at, status, sent_count, created_by, created_at, sent_at
      FROM scheduled_broadcasts
      ORDER BY created_at DESC
      LIMIT $1
    `, [rowLimit]);

    res.json({
      success: true,
      broadcasts: pushLogs.rows,
      scheduled: scheduled.rows
    });
  } catch (e) {
    console.error('[Broadcast History] Error:', e.message);
    res.status(500).json({ error: e.message });
  }
});

// DELETE /api/admin/broadcast/scheduled/:id - Cancel a scheduled broadcast
app.delete('/api/admin/broadcast/scheduled/:id', async (req, res) => {
  const { adminUserId } = req.body;
  const broadcastId = req.params.id;

  try {
    const roleCheck = await db.query(
      'SELECT role FROM user_roles WHERE line_user_id = $1 AND role = $2',
      [adminUserId, 'super_admin']
    );
    if (roleCheck.rows.length === 0) {
      return res.status(403).json({ error: 'Only super_admin can cancel broadcasts' });
    }

    const result = await db.query(
      `UPDATE scheduled_broadcasts SET status = 'cancelled' WHERE id = $1 AND status = 'pending' RETURNING id`,
      [broadcastId]
    );

    if (result.rows.length === 0) {
      return res.status(404).json({ error: 'Scheduled broadcast not found or already processed' });
    }

    res.json({ success: true, cancelled: true, broadcastId: parseInt(broadcastId) });
  } catch (e) {
    console.error('[Cancel Broadcast] Error:', e.message);
    res.status(500).json({ error: e.message });
  }
});

// ===== LINE VOOM Post API =====

// POST /api/admin/voom-post - Send a VOOM-style flex message broadcast
app.post('/api/admin/voom-post', async (req, res) => {
  const { adminUserId, imageUrl, title, description, linkUrl, linkLabel, altText } = req.body;

  if (!adminUserId) return res.status(400).json({ error: 'adminUserId required' });
  if (!imageUrl || !title) return res.status(400).json({ error: 'imageUrl and title required' });

  try {
    // Verify super_admin
    const roleCheck = await db.query(
      'SELECT role FROM user_roles WHERE line_user_id = $1 AND role = $2',
      [adminUserId, 'super_admin']
    );
    if (roleCheck.rows.length === 0) {
      return res.status(403).json({ error: 'Only super_admin can post VOOM messages' });
    }

    const flexMessage = buildVoomFlexPost({
      imageUrl,
      title,
      description: description || '',
      linkUrl: linkUrl || OFFICIAL_WEBSITE,
      linkLabel: linkLabel || '了解更多'
    });

    // Broadcast to all followers
    const broadcastRes = await fetch('https://api.line.me/v2/bot/message/broadcast', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${LINE_CHANNEL_ACCESS_TOKEN}`,
      },
      body: JSON.stringify({ messages: [flexMessage] }),
    });

    const data = await broadcastRes.json().catch(() => ({}));

    if (!broadcastRes.ok) {
      console.error('[VOOM Post] LINE API error:', data);
      return res.status(500).json({ error: data.message || 'LINE API error' });
    }

    // Log to push_logs
    try {
      await db.query(`
        INSERT INTO push_logs (push_type, recipient_count, message_count, triggered_by, description, success)
        VALUES ($1, $2, $3, $4, $5, $6)
      `, ['voom_post', 0, 1, adminUserId, `VOOM: ${title}`, true]);
    } catch (logErr) { console.error('[VOOM Post] Push log error:', logErr.message); }

    console.log(`[VOOM Post] Broadcast sent: "${title}"`);
    res.json({ success: true, title, message: 'VOOM-style post broadcasted to all followers' });

  } catch (e) {
    console.error('[VOOM Post] Error:', e.message);
    res.status(500).json({ error: e.message });
  }
});

// ===== Scheduled Broadcast Processor =====
// Runs every 60 seconds to check and execute pending scheduled broadcasts
setInterval(async () => {
  try {
    const pending = await db.query(`
      SELECT id, messages, target_tags, exclude_tags, created_by, target_store_ids
      FROM scheduled_broadcasts
      WHERE status = 'pending' AND scheduled_at <= NOW()
      ORDER BY scheduled_at ASC
      LIMIT 5
    `);

    for (const broadcast of pending.rows) {
      try {
        console.log(`[Scheduler] Processing broadcast #${broadcast.id}`);
        const messages = typeof broadcast.messages === 'string'
          ? JSON.parse(broadcast.messages)
          : broadcast.messages;

        const result = await executeBroadcast(
          messages,
          broadcast.target_tags,
          broadcast.exclude_tags,
          broadcast.created_by,
          broadcast.target_store_ids
        );

        await db.query(`
          UPDATE scheduled_broadcasts
          SET status = $1, sent_count = $2, sent_at = NOW()
          WHERE id = $3
        `, [
          result.success ? 'sent' : 'failed',
          result.sentCount || 0,
          broadcast.id
        ]);

        console.log(`[Scheduler] Broadcast #${broadcast.id} ${result.success ? 'sent' : 'failed'}`);
      } catch (broadcastErr) {
        console.error(`[Scheduler] Broadcast #${broadcast.id} error:`, broadcastErr.message);
        await db.query(
          `UPDATE scheduled_broadcasts SET status = 'failed' WHERE id = $1`,
          [broadcast.id]
        ).catch(() => {});
      }
    }
  } catch (e) {
    // Silently ignore scheduler errors to avoid polluting logs
  }
}, 60000);

const PORT = process.env.PORT || 3000;
app.listen(PORT, async () => {
  await initDB();
  console.log(`Server running on port ${PORT}`);
});
