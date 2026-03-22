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

// ===== MQTT =====
const mqttClient = mqtt.connect(process.env.MQTT_URL, {
  username: process.env.MQTT_USER,
  password: process.env.MQTT_PASS,
  clientId: 'server-' + Date.now(),
});

const machineCache = {};

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
        await db.query(`UPDATE orders SET status='done', completed_at=NOW() WHERE machine_id=$1 AND status='running'`, [machineId]);
      }
    }
  } catch (e) { console.error(e.message); }
});

// ===== MQTT Publish Start Command =====
function publishStartCommand(order) {
  const { id: orderId, store_id, machine_id, mode, temp, total_amount } = order;
  const washModeMap = { standard: 1, small: 2, washonly: 3, soft: 4, strong: 5, dryonly: 0 };
  const dryModeMap = { high: 1, mid: 2, low: 3 };
  const washMode = washModeMap[mode] ?? 1;
  const dryMode = mode === 'washonly' ? 0 : (dryModeMap[temp] ?? 1);
  const coins = Math.ceil(total_amount / 10);
  const topic = `laundry/${store_id}/${machine_id}/command`;
  const payload = JSON.stringify({ cmd: 'start', orderId, wash_mode: washMode, dry_mode: dryMode, coins, amount: total_amount });
  mqttClient.publish(topic, payload, { qos: 1 }, (err) => {
    if (err) console.error('MQTT publish error:', err);
    else console.log(`MQTT start → ${topic}`, payload);
  });
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
  const r = await db.query('SELECT role, group_id FROM user_roles WHERE line_user_id=$1', [lineUserId]);
  if (r.rows.length === 0) return { role: 'consumer', groupId: null };
  // super_admin has group_id = NULL
  const adminRow = r.rows.find(row => row.role === 'super_admin');
  if (adminRow) return { role: 'super_admin', groupId: null, allGroups: r.rows };
  const storeAdminRow = r.rows.find(row => row.role === 'store_admin');
  if (storeAdminRow) return { role: 'store_admin', groupId: storeAdminRow.group_id, allGroups: r.rows };
  return { role: 'consumer', groupId: null, allGroups: r.rows };
}

// ═══════════════════════════════════════
//  API: User Profile & Role
// ═══════════════════════════════════════
app.get('/api/user/profile', async (req, res) => {
  try {
    const lineUserId = req.query.userId;
    if (!lineUserId) return res.status(400).json({ error: 'userId required' });

    const member = await getOrCreateMember(lineUserId);
    const roleInfo = await getUserRole(lineUserId);

    // Get groups this user has access to
    let groups = [];
    if (roleInfo.role === 'super_admin') {
      const gr = await db.query('SELECT * FROM store_groups ORDER BY name');
      groups = gr.rows;
    } else if (roleInfo.role === 'store_admin') {
      const gr = await db.query('SELECT * FROM store_groups WHERE id=$1', [roleInfo.groupId]);
      groups = gr.rows;
    } else {
      // Consumer - get groups from their wallets (any group they've interacted with)
      const gr = await db.query(`
        SELECT DISTINCT sg.* FROM store_groups sg
        LEFT JOIN wallets w ON w.group_id = sg.id AND w.line_user_id = $1
        ORDER BY sg.name
      `, [lineUserId]);
      groups = gr.rows;
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

    res.json({
      memberId: member.id,
      lineUserId,
      role: roleInfo.role,
      managedGroupId: roleInfo.groupId,
      groups,
      wallets,
    });
  } catch (e) {
    console.error('profile error:', e);
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
  const r = await db.query('SELECT s.*, sg.name as group_name, sg.type as group_type FROM stores s LEFT JOIN store_groups sg ON s.group_id=sg.id ORDER BY s.name');
  res.json(r.rows);
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
  try {
    const { userId, groupId, amount } = req.body;
    if (!userId || !groupId || !amount) return res.status(400).json({ error: 'userId, groupId, amount required' });
    if (amount <= 0) return res.status(400).json({ error: 'amount must be positive' });

    // Upsert wallet
    await db.query(`
      INSERT INTO wallets (line_user_id, group_id, balance) VALUES ($1, $2, $3)
      ON CONFLICT (line_user_id, group_id) DO UPDATE SET balance = wallets.balance + $3
    `, [userId, groupId, amount]);

    // Record transaction
    await db.query(`
      INSERT INTO transactions (line_user_id, group_id, type, amount, description, created_at)
      VALUES ($1, $2, 'topup', $3, $4, NOW())
    `, [userId, groupId, amount, `Topup +${amount}`]);

    const r = await db.query('SELECT balance FROM wallets WHERE line_user_id=$1 AND group_id=$2', [userId, groupId]);
    res.json({ success: true, balance: r.rows[0].balance });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/wallet/deduct', async (req, res) => {
  try {
    const { userId, groupId, amount, description } = req.body;
    if (!userId || !groupId || !amount) return res.status(400).json({ error: 'missing params' });

    // Check balance
    const wr = await db.query('SELECT balance FROM wallets WHERE line_user_id=$1 AND group_id=$2', [userId, groupId]);
    const balance = wr.rows[0]?.balance || 0;
    if (balance < amount) return res.status(400).json({ error: 'insufficient balance', balance });

    // Deduct
    await db.query(`UPDATE wallets SET balance = balance - $3 WHERE line_user_id=$1 AND group_id=$2`, [userId, groupId, amount]);

    // Record transaction
    await db.query(`
      INSERT INTO transactions (line_user_id, group_id, type, amount, description, created_at)
      VALUES ($1, $2, 'payment', $3, $4, NOW())
    `, [userId, groupId, -amount, description || 'Payment']);

    const r = await db.query('SELECT balance FROM wallets WHERE line_user_id=$1 AND group_id=$2', [userId, groupId]);
    res.json({ success: true, balance: r.rows[0].balance });
  } catch (e) { res.status(500).json({ error: e.message }); }
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

    // Machine status
    const machineQuery = `
      SELECT m.id, m.store_id, m.name, m.size, s.name as store_name,
        COALESCE(cs.state, 'unknown') as state, COALESCE(cs.remain_sec, 0) as remain_sec
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
    });
  } catch (e) {
    console.error('dashboard error:', e);
    res.status(500).json({ error: e.message });
  }
});

// ═══════════════════════════════════════
//  API: Machines (existing, enhanced)
// ═══════════════════════════════════════
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
        cancelUrl: `${FRONTEND_URL}/payment-result?status=cancel&orderId=${orderId}`,
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
    if (orderResult.rows.length === 0) return res.redirect(`${FRONTEND_URL}/payment-result?status=error&msg=order_not_found`);
    const order = orderResult.rows[0];
    const body = { amount: order.total_amount, currency: 'TWD' };
    const result = await linePayRequest('POST', `/v3/payments/${transactionId}/confirm`, body);
    if (result.returnCode === '0000') {
      await db.query(`UPDATE orders SET status='paid', paid_at=NOW() WHERE id=$1`, [orderId]);
      publishStartCommand(order);
      res.redirect(`${FRONTEND_URL}/payment-result?status=success&orderId=${orderId}`);
    } else {
      res.redirect(`${FRONTEND_URL}/payment-result?status=fail&msg=${encodeURIComponent(result.returnMessage)}`);
    }
  } catch (e) {
    res.redirect(`https://laundry-frontend-chi.vercel.app/payment-result?status=error&msg=${encodeURIComponent(e.message)}`);
  }
});

app.get('/api/payment/cancel', async (req, res) => {
  const { orderId } = req.query;
  if (orderId) await db.query(`UPDATE orders SET status='cancelled' WHERE id=$1`, [orderId]);
  res.redirect(`https://laundry-frontend-chi.vercel.app/payment-result?status=cancel&orderId=${orderId || ''}`);
});

app.get('/health', (req, res) => res.json({ ok: true, time: new Date() }));

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

  // Seed super admin (your LINE userId — update this with your actual LINE userId)
  // You can find it in the LIFF profile. For now we seed a placeholder.
  const SUPER_ADMIN_LINE_ID = process.env.SUPER_ADMIN_LINE_ID || 'Ubdcdd269e115bf9ac492288adbc0115e';
  await db.query(`
    INSERT INTO user_roles (line_user_id, role, group_id) VALUES ($1, 'super_admin', NULL)
    ON CONFLICT (line_user_id, role, group_id) DO NOTHING
  `, [SUPER_ADMIN_LINE_ID]);

  console.log('DB initialized with multi-tenant tables');
}

const PORT = process.env.PORT || 3000;
app.listen(PORT, async () => {
  await initDB();
  console.log(`Server running on port ${PORT}`);
});
