require('dotenv').config();
const express = require('express');
const mqtt = require('mqtt');
const { Pool } = require('pg');
const cors = require('cors');
const { v4: uuid } = require('uuid');
const crypto = require('crypto');
const https = require('https');

const helmet = require('helmet');
const rateLimit = require('express-rate-limit');

const app = express();
app.set('trust proxy', 1);
app.use(helmet({ contentSecurityPolicy: false }));
app.use(express.json());
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
const IOT_API_KEY = process.env.IOT_API_KEY || 'ypure-iot-2026-default-key';
function requireIotApiKey(req, res, next) {
  const key = req.headers['x-api-key'] || req.query.apiKey;
  if (key !== IOT_API_KEY) return res.status(401).json({ error: 'Invalid or missing API key' });
  next();
}

// ===== ThingsBoard RPC Helper =====
let tbToken = null;
let tbTokenExpiry = 0;

async function getTBToken() {
  if (tbToken && Date.now() < tbTokenExpiry) return tbToken;
  const res = await fetch('http://vps3.monsterstore.tw:8080/api/auth/login', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ username: process.env.TB_USERNAME || 'sl6963693171@gmail.com', password: process.env.TB_PASSWORD || 's85010805' }),
  });
  const data = await res.json();
  tbToken = data.token;
  tbTokenExpiry = Date.now() + 3600000; // 1 hour
  return tbToken;
}

async function getTBDeviceId(deviceName) {
  const token = await getTBToken();
  const res = await fetch(`http://vps3.monsterstore.tw:8080/api/tenant/devices?pageSize=1&textSearch=${deviceName}`, {
    headers: { 'X-Authorization': `Bearer ${token}` },
  });
  const data = await res.json();
  return data.data?.[0]?.id?.id || null;
}

async function sendThingsBoardRPC(deviceName, method, params = {}) {
  const token = await getTBToken();
  const deviceId = await getTBDeviceId(deviceName);
  if (!deviceId) throw new Error(`Device ${deviceName} not found in ThingsBoard`);
  const res = await fetch(`http://vps3.monsterstore.tw:8080/api/rpc/oneway/${deviceId}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'X-Authorization': `Bearer ${token}` },
    body: JSON.stringify({ method, params }),
  });
  if (!res.ok) throw new Error(`TB RPC failed: ${res.status}`);
  return true;
}

// ===== LINE Messaging API Push Notifications =====
const LINE_CHANNEL_ACCESS_TOKEN = process.env.LINE_CHANNEL_ACCESS_TOKEN || '';

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
  const mqttPayload = { cmd: 'start', orderId, wash_mode: washMode, dry_mode: dryMode, coins, amount: total_amount };

  try {
    // Primary: Send via ThingsBoard RPC
    await sendThingsBoardRPC(machine_id, 'startMachine', {
      orderId,
      mode: washMode,
      dry_mode: dryMode,
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
  if (adminRow) return { role: 'super_admin', groupId: null, allGroups: r.rows, permissions: { revenue: true, coupons: true, members: true, machines: true, topup: true, news: true, roles: true } };
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

    // Get total count
    const countResult = await db.query(
      `SELECT COUNT(*) FROM orders o
       JOIN members mb ON o.member_id = mb.id
       WHERE mb.line_user_id = $1 AND COALESCE(o.type, 'online') != 'topup'`,
      [userId]
    );
    const total = parseInt(countResult.rows[0].count);

    // Get paginated orders with store and machine names
    const ordersResult = await db.query(
      `SELECT o.id, o.store_id, o.machine_id, o.mode, o.addons, o.extend_min, o.temp,
              o.total_amount as amount, o.status, o.created_at, o.paid_at, o.completed_at,
              COALESCE(o.payment_method, 'wallet') as payment_method,
              COALESCE(o.type, 'online') as order_type,
              s.name as store_name,
              m.name as machine_name
       FROM orders o
       JOIN members mb ON o.member_id = mb.id
       LEFT JOIN stores s ON o.store_id = s.id
       LEFT JOIN machines m ON o.machine_id = m.id
       WHERE mb.line_user_id = $1 AND COALESCE(o.type, 'online') != 'topup'
       ORDER BY o.created_at DESC
       LIMIT $2 OFFSET $3`,
      [userId, limit, offset]
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

// ═══════════════════════════════════════
//  API: Stores & Store Groups
// ═══════════════════════════════════════
app.get('/api/store-groups', async (req, res) => {
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
    const { userId, groupId, amount, _internal } = req.body;
    if (!userId || !groupId || !amount) return res.status(400).json({ error: 'userId, groupId, amount required' });
    if (amount <= 0) return res.status(400).json({ error: 'amount must be positive' });

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
});

// ═══════════════════════════════════════
//  API: Orders (existing, enhanced)
// ═══════════════════════════════════════
app.post('/api/orders/create', async (req, res) => {
  const { lineUserId, storeId, machineId, mode, addons, extendMin, temp, totalAmount } = req.body;
  let member = (await db.query('SELECT id FROM members WHERE line_user_id=$1', [lineUserId])).rows[0];
  if (!member) {
    const id = uuid();
    await db.query('INSERT INTO members (id,line_user_id,created_at) VALUES ($1,$2,NOW())', [id, lineUserId]);
    member = { id };
  }
  const orderId = 'ORD' + Date.now();
  const pulses = Math.ceil(totalAmount / 10);
  const modeDur = { standard:65, small:50, washonly:35, soft:65, strong:75, dryonly:40 };
  const durationSec = ((modeDur[mode]||65) + parseInt(extendMin||0)) * 60;
  await db.query(
    `INSERT INTO orders (id,member_id,store_id,machine_id,mode,addons,extend_min,temp,total_amount,pulses,duration_sec,status,created_at)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,'pending',NOW())`,
    [orderId, member.id, storeId, machineId, mode, JSON.stringify(addons||[]), extendMin||0, temp, totalAmount, pulses, durationSec]
  );
  res.json({ orderId, totalAmount, pulses });
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
    const orderResult = await db.query('SELECT * FROM orders WHERE id=$1', [orderId]);
    if (orderResult.rows.length === 0) return res.redirect(`${FRONTEND_URL}/?status=error&msg=order_not_found`);
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
      publishStartCommand(order);
      // LINE Push: payment success
      const memberRow = (await db.query('SELECT line_user_id FROM members WHERE id=$1', [order.member_id])).rows[0];
      if (memberRow) {
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

    // Create order
    const orderId = 'ORD' + Date.now();
    const pulses = Math.ceil(amount / 10);
    const modeDur = { standard: 65, small: 50, washonly: 35, soft: 65, strong: 75, dryonly: 40, dryextend: 0 };
    const baseDur = modeDur[mode] || 0;
    const durationSec = (baseDur + (minutes || 0)) * 60;

    await db.query(
      `INSERT INTO orders (id,member_id,store_id,machine_id,mode,addons,extend_min,temp,total_amount,pulses,duration_sec,status,created_at)
       VALUES ($1,$2,$3,$4,$5,'[]',$6,$7,$8,$9,$10,'pending',NOW())`,
      [orderId, member.id, storeId, machineId, mode, minutes || 0, dryTemp || 'high', amount, pulses, durationSec]
    );

    // Try LINE Pay
    const SERVER_URL = process.env.SERVER_URL || 'https://laundry-backend-production-efa4.up.railway.app';
    const FRONTEND_URL = 'https://laundry-frontend-chi.vercel.app';
    try {
      const storeName = (await db.query('SELECT name FROM stores WHERE id=$1', [storeId])).rows[0]?.name || '洗衣服務';
      const machineName = machineId.includes('-d') ? `烘乾${machineNum}號` : `洗脫烘${machineNum}號(${mode})`;
      const orderName = `${storeName} - ${machineName}`;
      const payBody = {
        amount, currency: 'TWD', orderId,
        packages: [{ id: orderId, amount, name: orderName, products: [{ name: orderName, quantity: 1, price: amount }] }],
        redirectUrls: {
          confirmUrl: `${SERVER_URL}/api/payment/confirm`,
          cancelUrl: `${FRONTEND_URL}/?status=cancel&orderId=${orderId}`,
        },
      };
      const result = await linePayRequest('POST', '/v3/payments/request', payBody);
      if (result.returnCode === '0000') {
        await db.query(`UPDATE orders SET status='waiting_payment' WHERE id=$1`, [orderId]);
        return res.json({ success: true, paymentUrl: result.info.paymentUrl.web, transactionId: result.info.transactionId, orderId });
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
      `, [userId, store.group_id, -amount, `${storeId} - ${mode}`]);
    }

    // Try MQTT start command
    try { publishStartCommand({ id: orderId, store_id: storeId, machine_id: machineId, mode, temp: dryTemp || 'high', total_amount: amount }); } catch (e) {}

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
        amount: amount, discount: 0, finalAmount: amount,
        orderId: orderId, paymentMethod: '錢包付款', minutes: mins, supportUrl: demoSupportUrl, supportPhone: demoSupportPhone, groupId: store?.group_id, storeId: storeId
      }), { pushType: 'auto_payment', storeId: storeId, groupId: store?.group_id, description: `Demo付款成功 ${sName} ${mName}` }).catch(() => {});
    }

    res.json({ success: true, orderId, demoMode: true });
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
    const orderId = 'TOP' + Date.now();
    const SERVER_URL = process.env.SERVER_URL || 'https://laundry-backend-production-efa4.up.railway.app';
    const FRONTEND_URL = 'https://laundry-frontend-chi.vercel.app';
    // Get group name
    const gr = await db.query('SELECT name FROM store_groups WHERE id=$1', [groupId]);
    const groupName = gr.rows[0]?.name || '洗衣服務';

    try {
      // Store topup order in DB for tamper-proof confirm
      await db.query(`INSERT INTO orders (id, store_id, member_id, machine_id, mode, amount, status, created_at) VALUES ($1, 'topup', $2, $3, 'topup', $4, 'pending', NOW()) ON CONFLICT DO NOTHING`, [orderId, userId, groupId, amount]);
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
      console.warn('LINE Pay topup unavailable, using demo mode:', linePayErr.message);
    }

    // Demo mode fallback: add points directly
    await db.query(`
      INSERT INTO wallets (line_user_id, group_id, balance) VALUES ($1, $2, $3)
      ON CONFLICT (line_user_id, group_id) DO UPDATE SET balance = wallets.balance + $3
    `, [userId, groupId, amount]);
    await db.query(`
      INSERT INTO transactions (line_user_id, group_id, type, amount, description, created_at)
      VALUES ($1, $2, 'topup', $3, $4, NOW())
    `, [userId, groupId, amount, `儲值 +${amount}`]);
    const wr = await db.query('SELECT balance FROM wallets WHERE line_user_id=$1 AND group_id=$2', [userId, groupId]);
    // LINE Push: topup success
    let topupSupportUrl = '';
    let topupSupportPhone = '';
    if (groupId) {
      const sgr = (await db.query('SELECT support_url, support_phone FROM store_groups WHERE id=$1', [groupId])).rows[0];
      topupSupportUrl = sgr?.support_url || '';
      topupSupportPhone = sgr?.support_phone || '';
    }
    sendLineFlexMessage(userId, '儲值成功', buildTopupFlexMessage({ groupName: groupName, amount: amount, balance: wr.rows[0]?.balance || 0, supportUrl: topupSupportUrl, supportPhone: topupSupportPhone }), { pushType: 'auto_topup', groupId: groupId, description: `Demo儲值成功 ${groupName} NT$${amount}` }).catch(() => {});
    res.json({ success: true, demoMode: true, balance: wr.rows[0]?.balance || 0 });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/topup/confirm', async (req, res) => {
  try {
    const { transactionId, orderId, userId: legacyUserId, groupId: legacyGroupId, amount: legacyAmount } = req.query;
    const FRONTEND_URL = process.env.FRONTEND_URL || 'https://laundry-frontend-chi.vercel.app';

    // Retrieve order from DB (tamper-proof) or fallback to legacy URL params
    let userId, groupId, amountInt;
    if (orderId) {
      const orderRow = (await db.query('SELECT member_id, machine_id, amount FROM orders WHERE id=$1 AND status=$2', [orderId, 'pending'])).rows[0];
      if (!orderRow) return res.redirect(`${FRONTEND_URL}/?status=topup_fail&reason=order_not_found`);
      userId = orderRow.member_id;
      groupId = orderRow.machine_id; // stored groupId in machine_id field for topup orders
      amountInt = orderRow.amount;
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

// Purchase a coupon (deducts points, creates user_coupon)
app.post('/api/coupons/purchase', async (req, res) => {
  try {
    const { userId, couponId, groupId } = req.body;
    if (!userId || !couponId) return res.status(400).json({ error: 'userId and couponId required' });
    // Verify coupon exists and is active
    const couponRes = await db.query('SELECT * FROM admin_coupons WHERE id=$1 AND active=true AND expiry >= CURRENT_DATE', [couponId]);
    if (couponRes.rows.length === 0) return res.status(404).json({ error: 'Coupon not found or expired' });
    const coupon = couponRes.rows[0];
    const price = coupon.price || 0;
    const effectiveGroupId = groupId || coupon.group_id;
    // Check wallet balance
    if (price > 0 && effectiveGroupId) {
      const walletRes = await db.query('SELECT balance FROM wallets WHERE line_user_id=$1 AND group_id=$2', [userId, effectiveGroupId]);
      const balance = walletRes.rows[0]?.balance || 0;
      if (balance < price) return res.status(400).json({ error: 'Insufficient balance', balance, required: price });
      // Deduct points
      await db.query('UPDATE wallets SET balance = balance - $1 WHERE line_user_id=$2 AND group_id=$3', [price, userId, effectiveGroupId]);
      // Record transaction
      await db.query('INSERT INTO transactions (line_user_id, group_id, type, amount, description) VALUES ($1,$2,$3,$4,$5)',
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
    const ucRes = await db.query(`
      INSERT INTO user_coupons (line_user_id, coupon_id, group_id, uses_remaining, expiry_date, status)
      VALUES ($1, $2, $3, $4, $5, 'active') RETURNING id
    `, [userId, couponId, effectiveGroupId, coupon.max_uses || 1, expiryDate]);
    res.json({ success: true, userCouponId: ucRes.rows[0].id, newBalance: price > 0 ? undefined : undefined });
    // Return new balance if deducted
    if (price > 0 && effectiveGroupId) {
      const newBal = await db.query('SELECT balance FROM wallets WHERE line_user_id=$1 AND group_id=$2', [userId, effectiveGroupId]);
      // Note: response already sent, but frontend can re-fetch
    }
  } catch (e) { res.status(500).json({ error: e.message }); }
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

    let query = `SELECT ${dateGroup} as date, COALESCE(SUM(total_amount),0) as revenue, COUNT(*) as orders
      FROM orders WHERE created_at >= NOW() - INTERVAL '${safeDays} days' AND status != 'cancelled'`;
    const params = [];
    if (storeId) {
      query += ` AND store_id = $1`;
      params.push(storeId);
    }
    query += ` GROUP BY ${dateGroup} ORDER BY date ASC`;

    const r = await db.query(query, params);

    // Top modes
    let modeQuery = `SELECT mode, COUNT(*) as count, COALESCE(SUM(total_amount),0) as revenue FROM orders WHERE created_at >= NOW() - INTERVAL '${safeDays} days' AND status != 'cancelled'`;
    const modeParams = [];
    if (storeId) {
      modeQuery += ` AND store_id = $1`;
      modeParams.push(storeId);
    }
    modeQuery += ` GROUP BY mode ORDER BY count DESC LIMIT 5`;
    const modeR = await db.query(modeQuery, modeParams);

    // Peak hours
    let peakQuery = `SELECT EXTRACT(HOUR FROM created_at) as hour, COUNT(*) as count FROM orders WHERE created_at >= NOW() - INTERVAL '${safeDays} days' AND status != 'cancelled'`;
    const peakParams = [];
    if (storeId) {
      peakQuery += ` AND store_id = $1`;
      peakParams.push(storeId);
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
  const { userId, storeId, ownerName, phone } = req.body;
  if (!userId || !storeId) return res.status(400).json({ error: 'Missing userId or storeId' });
  try {
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

    let query = `SELECT ${dateGroup} as date, COALESCE(SUM(total_amount),0) as revenue, COUNT(*) as orders
      FROM orders WHERE created_at >= NOW() - INTERVAL '${safeDays} days' AND status != 'cancelled'`;
    const params = [];
    if (storeId) {
      query += ` AND store_id = $1`;
      params.push(storeId);
    }
    query += ` GROUP BY ${dateGroup} ORDER BY date ASC`;
    const r = await db.query(query, params);

    // Per-store breakdown
    let storeQuery = `SELECT store_id, COALESCE(SUM(total_amount),0) as revenue, COUNT(*) as orders
      FROM orders WHERE created_at >= NOW() - INTERVAL '${safeDays} days' AND status != 'cancelled'`;
    const storeParams = [];
    if (storeId) {
      storeQuery += ` AND store_id = $1`;
      storeParams.push(storeId);
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
  res.json({
    host: process.env.MQTT_HOST || 'f29e89cd32414b9c826381e76ef8baaf.s1.eu.hivemq.cloud',
    port: 8884,
    protocol: 'wss',
    username: process.env.MQTT_USER || 'ypure-iot',
    password: process.env.MQTT_PASS || 'Ypure2025!'
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

const PORT = process.env.PORT || 3000;
app.listen(PORT, async () => {
  await initDB();
  console.log(`Server running on port ${PORT}`);
});
