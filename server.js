require('dotenv').config();
const express = require('express');
const mqtt = require('mqtt');
const { Pool } = require('pg');
const cors = require('cors');
const { v4: uuid } = require('uuid');
const crypto = require('crypto');
const https = require('https');

const app = express();
app.use(express.json());
app.use(cors());

// 資料庫
const db = new Pool({ connectionString: process.env.DB_URL });

// MQTT
const mqttClient = mqtt.connect(process.env.MQTT_URL, {
  username: process.env.MQTT_USER,
  password: process.env.MQTT_PASS,
  clientId: 'server-' + Date.now(),
});

const machineCache = {};

mqttClient.on('connect', () => {
  console.log('✅ MQTT 已連線');
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
        await db.query(`UPDATE orders SET status='done', completed_at=NOW() WHERE machine_id=$1 AND status='running'`, [machineId]);
      }
    }
  } catch (e) { console.error(e.message); }
});

// ===== MQTT 發布啟動指令 =====
function publishStartCommand(order) {
  const { id: orderId, store_id, machine_id, mode, addons, temp, total_amount } = order;

  // 洗衣程序對應（模式 -> 程序號）
  const washModeMap = {
    standard: 1,
    small:    2,
    washonly: 3,
    soft:     4,
    strong:   5,
    dryonly:  0,  // 只烘乾不洗
  };

  // 烘乾程序對應（溫度 -> 程序號）
  const dryModeMap = {
    '高溫': 1,
    '中溫': 2,
    '低溫': 3,
    '輕柔': 4,
  };

  const washMode = washModeMap[mode] ?? 1;
  const dryMode  = mode === 'washonly' ? 0 : (dryModeMap[temp] ?? 1);
  const coins    = Math.ceil(total_amount / 10);

  const topic   = `laundry/${store_id}/${machine_id}/command`;
  const payload = JSON.stringify({
    cmd:       'start',
    orderId,
    wash_mode: washMode,
    dry_mode:  dryMode,
    coins,
    amount:    total_amount,
  });

  mqttClient.publish(topic, payload, { qos: 1 }, (err) => {
    if (err) console.error('❌ MQTT 發布失敗:', err);
    else console.log(`✅ MQTT 啟動指令已發布 → ${topic}`, payload);
  });
}

// ===== LINE Pay 工具函式 =====
const LINE_PAY_CHANNEL_ID     = process.env.LINE_PAY_CHANNEL_ID;
const LINE_PAY_CHANNEL_SECRET = process.env.LINE_PAY_CHANNEL_SECRET;
const LINE_PAY_ENV            = process.env.LINE_PAY_ENV || 'sandbox';
const LINE_PAY_API_HOST       = LINE_PAY_ENV === 'sandbox'
  ? 'sandbox-api-pay.line.me'
  : 'api-pay.line.me';

function linePayRequest(method, path, body) {
  return new Promise((resolve, reject) => {
    const nonce   = uuid();
    const bodyStr = body ? JSON.stringify(body) : '';
    const message = LINE_PAY_CHANNEL_SECRET + path + bodyStr + nonce;
    const signature = crypto.createHmac('sha256', LINE_PAY_CHANNEL_SECRET)
      .update(message).digest('base64');

    const options = {
      hostname: LINE_PAY_API_HOST,
      path,
      method,
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
      res.on('end', () => {
        try { resolve(JSON.parse(data)); }
        catch (e) { reject(e); }
      });
    });
    req.on('error', reject);
    if (body) req.write(bodyStr);
    req.end();
  });
}

// ===== LINE Pay API =====

// 1. 建立付款請求
app.post('/api/payment/request', async (req, res) => {
  try {
    const { orderId, amount, orderName } = req.body;
    const SERVER_URL   = process.env.SERVER_URL || 'https://laundry-backend-production-efa4.up.railway.app';
    const FRONTEND_URL = 'https://laundry-frontend-chi.vercel.app';

    const body = {
      amount,
      currency: 'TWD',
      orderId,
      packages: [{
        id: orderId,
        amount,
        name: orderName || '悠洗洗衣服務',
        products: [{ name: orderName || '洗衣服務', quantity: 1, price: amount }],
      }],
      redirectUrls: {
        confirmUrl: `${SERVER_URL}/api/payment/confirm`,
        cancelUrl:  `${FRONTEND_URL}/payment-result?status=cancel&orderId=${orderId}`,
      },
    };

    const result = await linePayRequest('POST', '/v3/payments/request', body);
    console.log('LINE Pay request:', result.returnCode, result.returnMessage);

    if (result.returnCode === '0000') {
      await db.query(`UPDATE orders SET status='waiting_payment' WHERE id=$1`, [orderId]);
      res.json({
        success: true,
        paymentUrl:    result.info.paymentUrl.web,
        transactionId: result.info.transactionId,
      });
    } else {
      res.status(400).json({ success: false, error: result.returnMessage, code: result.returnCode });
    }
  } catch (e) {
    console.error('LINE Pay request error:', e);
    res.status(500).json({ success: false, error: e.message });
  }
});

// 2. 確認付款（LINE Pay redirect 回來）→ 付款成功後發 MQTT 啟動指令
app.get('/api/payment/confirm', async (req, res) => {
  try {
    const { transactionId, orderId } = req.query;
    const FRONTEND_URL = 'https://laundry-frontend-chi.vercel.app';

    // 查訂單
    const orderResult = await db.query('SELECT * FROM orders WHERE id=$1', [orderId]);
    if (orderResult.rows.length === 0) {
      return res.redirect(`${FRONTEND_URL}/payment-result?status=error&msg=訂單不存在`);
    }
    const order = orderResult.rows[0];

    // 確認付款
    const body   = { amount: order.total_amount, currency: 'TWD' };
    const result = await linePayRequest('POST', `/v3/payments/${transactionId}/confirm`, body);
    console.log('LINE Pay confirm:', result.returnCode, result.returnMessage);

    if (result.returnCode === '0000') {
      // 更新訂單狀態為已付款
      await db.query(
        `UPDATE orders SET status='paid', paid_at=NOW() WHERE id=$1`,
        [orderId]
      );

      // ✅ 發布 MQTT 啟動指令給樹莓派
      publishStartCommand(order);

      res.redirect(`${FRONTEND_URL}/payment-result?status=success&orderId=${orderId}`);
    } else {
      res.redirect(`${FRONTEND_URL}/payment-result?status=fail&msg=${encodeURIComponent(result.returnMessage)}`);
    }
  } catch (e) {
    console.error('LINE Pay confirm error:', e);
    res.redirect(`https://laundry-frontend-chi.vercel.app/payment-result?status=error&msg=${encodeURIComponent(e.message)}`);
  }
});

// 3. 取消付款
app.get('/api/payment/cancel', async (req, res) => {
  const { orderId } = req.query;
  const FRONTEND_URL = 'https://laundry-frontend-chi.vercel.app';
  if (orderId) {
    await db.query(`UPDATE orders SET status='cancelled' WHERE id=$1`, [orderId]);
  }
  res.redirect(`${FRONTEND_URL}/payment-result?status=cancel&orderId=${orderId || ''}`);
});

// ===== 原有 API =====
app.get('/api/stores', async (req, res) => {
  const r = await db.query('SELECT * FROM stores ORDER BY name');
  res.json(r.rows);
});

app.get('/api/machines/:storeId', async (req, res) => {
  const r = await db.query(`
    SELECT m.*, COALESCE(cs.state,'unknown') as state,
    COALESCE(cs.remain_sec,0) as remain_sec, COALESCE(cs.progress,0) as progress,
    cs.updated_at as last_seen
    FROM machines m LEFT JOIN machine_current_state cs ON m.id=cs.machine_id
    WHERE m.store_id=$1 AND m.active=true ORDER BY m.sort_order,m.name
  `, [req.params.storeId]);
  res.json(r.rows);
});

app.post('/api/orders/create', async (req, res) => {
  const { lineUserId, storeId, machineId, mode, addons, extendMin, temp, totalAmount } = req.body;
  let member = (await db.query('SELECT id FROM members WHERE line_user_id=$1', [lineUserId])).rows[0];
  if (!member) {
    const id = uuid();
    await db.query('INSERT INTO members (id,line_user_id,created_at) VALUES ($1,$2,NOW())', [id, lineUserId]);
    member = { id };
  }
  const orderId = 'ORD' + Date.now();
  const pulses  = Math.ceil(totalAmount / 10);
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

app.get('/health', (req, res) => res.json({ ok: true, time: new Date() }));

async function initDB() {
  await db.query(`
    CREATE TABLE IF NOT EXISTS stores (id VARCHAR(20) PRIMARY KEY, name VARCHAR(100), address VARCHAR(200), phone VARCHAR(20));
    CREATE TABLE IF NOT EXISTS machines (id VARCHAR(30) PRIMARY KEY, store_id VARCHAR(20), name VARCHAR(50), size VARCHAR(10), sort_order INT DEFAULT 0, active BOOLEAN DEFAULT true);
    CREATE TABLE IF NOT EXISTS machine_current_state (machine_id VARCHAR(30) PRIMARY KEY, state VARCHAR(20) DEFAULT 'unknown', remain_sec INT DEFAULT 0, progress INT DEFAULT 0, wifi_rssi INT DEFAULT 0, updated_at TIMESTAMPTZ DEFAULT NOW());
    CREATE TABLE IF NOT EXISTS members (id VARCHAR(40) PRIMARY KEY, line_user_id VARCHAR(60) UNIQUE, wallet INT DEFAULT 0, is_admin BOOLEAN DEFAULT false, created_at TIMESTAMPTZ DEFAULT NOW());
    CREATE TABLE IF NOT EXISTS orders (id VARCHAR(30) PRIMARY KEY, member_id VARCHAR(40), store_id VARCHAR(20), machine_id VARCHAR(30), mode VARCHAR(20), addons JSONB DEFAULT '[]', extend_min INT DEFAULT 0, temp VARCHAR(10), total_amount INT, pulses INT, duration_sec INT, status VARCHAR(20) DEFAULT 'pending', paid_at TIMESTAMPTZ, completed_at TIMESTAMPTZ, created_at TIMESTAMPTZ DEFAULT NOW());
  `);

  // 清除舊店家資料，重新插入
  await db.query(`DELETE FROM machines WHERE store_id IN ('s1','s2','s3','s4','s5')`);
  await db.query(`DELETE FROM stores WHERE id IN ('s1','s2','s3','s4','s5')`);
  await db.query(`INSERT INTO stores (id,name,address,phone) VALUES
    ('s1','悠洗自助洗衣','嘉義市東區文雅街181號',''),
    ('s2','吼你洗自助洗衣(玉清店)','苗栗縣苗栗市玉清路51號',''),
    ('s3','吼你洗自助洗衣(農會店)','苗栗縣苗栗市為公路290號',''),
    ('s4','熊愛洗自助洗衣','台中市西屯區福聯街22巷2號',''),
    ('s5','上好洗自助洗衣','高雄市鳳山區北平路214號','')
    ON CONFLICT (id) DO UPDATE SET name=EXCLUDED.name, address=EXCLUDED.address`);
  for (const sid of ['s1','s2','s3','s4','s5']) {
    for (let i=1;i<=6;i++) {
      const size = i<=2?'大型':i<=5?'中型':'小型';
      await db.query(
        `INSERT INTO machines (id,store_id,name,size,sort_order) VALUES ($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING`,
        [`${sid}-m${i}`, sid, `洗脫烘${i}號`, size, i]
      );
    }
  }
  console.log('✅ 資料庫初始化完成');
}

const PORT = process.env.PORT || 3000;
app.listen(PORT, async () => {
  await initDB();
  console.log(`🚀 伺服器啟動：port ${PORT}`);
});
