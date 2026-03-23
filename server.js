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
    const { userId, targetUserId, role, groupId, displayName, phone, notes } = req.body;
    const roleInfo = await getUserRole(userId);
    if (roleInfo.role !== 'super_admin') return res.status(403).json({ error: 'forbidden' });
    if (!targetUserId || !role) return res.status(400).json({ error: 'targetUserId and role required' });
    const gid = role === 'super_admin' ? null : (groupId || null);
    await db.query(`
      INSERT INTO user_roles (line_user_id, role, group_id, display_name, phone, notes)
      VALUES ($1, $2, $3, $4, $5, $6)
      ON CONFLICT (line_user_id, role, COALESCE(group_id, '__NULL__')) DO UPDATE
      SET display_name=COALESCE($4, user_roles.display_name), phone=COALESCE($5, user_roles.phone), notes=COALESCE($6, user_roles.notes)
    `, [targetUserId, role, gid, displayName || null, phone || null, notes || null]);
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
    if (scopeGroupId) query += ` WHERE sg.id = '${scopeGroupId}'`;
    query += ' ORDER BY sg.name';
    const r = await db.query(query);
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
      await db.query(`UPDATE orders SET status='paid', paid_at=NOW() WHERE id=$1`, [orderId]);
      // Update machine state in DB
      await db.query(`
        INSERT INTO machine_current_state (machine_id, state, remain_sec, progress, updated_at)
        VALUES ($1, 'running', $2, 0, NOW())
        ON CONFLICT (machine_id) DO UPDATE SET state='running', remain_sec=$2, progress=0, updated_at=NOW()
      `, [order.machine_id, order.duration_sec || 3600]);
      publishStartCommand(order);
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

    res.json({ success: true, orderId, demoMode: true });
  } catch (e) {
    console.error('payment/create error:', e);
    res.status(500).json({ success: false, error: e.message });
  }
});

// API: Report machine state from frontend (for cross-user visibility)
app.post('/api/machines/state', async (req, res) => {
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
      const payBody = {
        amount, currency: 'TWD', orderId,
        packages: [{ id: orderId, amount, name: `${groupName} 儲值`, products: [{ name: `${groupName} 點數儲值 ${amount}點`, quantity: 1, price: amount }] }],
        redirectUrls: {
          confirmUrl: `${SERVER_URL}/api/topup/confirm?userId=${encodeURIComponent(userId)}&groupId=${groupId}&amount=${amount}`,
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
    res.json({ success: true, demoMode: true, balance: wr.rows[0]?.balance || 0 });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/topup/confirm', async (req, res) => {
  try {
    const { transactionId, userId, groupId, amount } = req.query;
    const FRONTEND_URL = 'https://laundry-frontend-chi.vercel.app';
    const amountInt = parseInt(amount);
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
    const { userId, machineId, action, durationMin } = req.body;
    const roleInfo = await getUserRole(userId);
    if (roleInfo.role !== 'super_admin' && roleInfo.role !== 'store_admin') return res.status(403).json({ error: 'forbidden' });
    const state = action === 'start' ? 'running' : 'idle';
    const remainSec = action === 'start' ? (durationMin || 60) * 60 : 0;
    await db.query(`
      INSERT INTO machine_current_state (machine_id, state, remain_sec, progress, updated_at)
      VALUES ($1, $2, $3, 0, NOW())
      ON CONFLICT (machine_id) DO UPDATE SET state=$2, remain_sec=$3, progress=0, updated_at=NOW()
    `, [machineId, state, remainSec]);
    res.json({ success: true, state, remainSec });
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
    const { userId, groupId, name, discount, type, minSpend, price, expiry, description, timeSlot, deviceLimit, validity, storeScope, maxUses, category } = req.body;
    const roleInfo = await getUserRole(userId);
    if (roleInfo.role !== 'super_admin' && roleInfo.role !== 'store_admin') return res.status(403).json({ error: 'forbidden' });
    await db.query(`
      INSERT INTO admin_coupons (group_id, name, discount, type, min_spend, price, expiry, active, description, time_slot, device_limit, validity, store_scope, max_uses, category, created_at)
      VALUES ($1,$2,$3,$4,$5,$6,$7,true,$8,$9,$10,$11,$12,$13,$14,NOW())
    `, [groupId||null, name, discount, type||'fixed', minSpend||0, price||0, expiry||'2026-12-31',
        description||'', timeSlot||'全時段可用', deviceLimit||'全機型適用', validity||'自購買起90日內有效',
        storeScope||'全台門市', maxUses||0, category||'gift']);
    res.json({ success: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.put('/api/admin/coupons/:id', async (req, res) => {
  try {
    const { userId, name, discount, type, minSpend, expiry, active, price, description, timeSlot, deviceLimit, validity, storeScope, maxUses, category, groupId } = req.body;
    const roleInfo = await getUserRole(userId);
    if (roleInfo.role !== 'super_admin' && roleInfo.role !== 'store_admin') return res.status(403).json({ error: 'forbidden' });
    await db.query(`
      UPDATE admin_coupons SET name=COALESCE($2,name), discount=COALESCE($3,discount), type=COALESCE($4,type),
      min_spend=COALESCE($5,min_spend), expiry=COALESCE($6,expiry), active=COALESCE($7,active),
      price=COALESCE($8,price), description=COALESCE($9,description), time_slot=COALESCE($10,time_slot),
      device_limit=COALESCE($11,device_limit), validity=COALESCE($12,validity), store_scope=COALESCE($13,store_scope),
      max_uses=COALESCE($14,max_uses), category=COALESCE($15,category), group_id=COALESCE($16,group_id)
      WHERE id=$1
    `, [req.params.id, name, discount, type, minSpend, expiry, active, price, description, timeSlot, deviceLimit, validity, storeScope, maxUses, category, groupId]);
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
        status: isExpired ? 'expired' : isUsedUp ? 'used' : 'active',
        groupName: c.group_name,
        purchasedAt: c.purchased_at,
      };
    });
    res.json(coupons);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ═══════════════════════════════════════
//  API: Revenue Chart
// ═══════════════════════════════════════
app.get('/api/admin/revenue-chart', async (req, res) => {
  try {
    const { userId, days, groupId, startDate, endDate } = req.query;
    const roleInfo = await getUserRole(userId);
    if (roleInfo.role !== 'super_admin' && roleInfo.role !== 'store_admin') return res.status(403).json({ error: 'forbidden' });
    const scopeGroupId = roleInfo.role === 'store_admin' ? roleInfo.groupId : groupId;
    const params = [];
    let dateFilter;
    if (startDate && endDate) {
      params.push(startDate, endDate);
      dateFilter = `o.created_at >= $1::date AND o.created_at < ($2::date + INTERVAL '1 day')`;
    } else {
      const numDays = Math.min(Math.max(parseInt(days) || 7, 1), 365);
      params.push(numDays);
      dateFilter = `o.created_at >= CURRENT_DATE - ($1 || ' days')::INTERVAL`;
    }
    let query = `
      SELECT DATE(o.created_at) as date, COALESCE(SUM(o.total_amount), 0) as revenue, COUNT(*) as orders
      FROM orders o JOIN stores s ON o.store_id = s.id
      WHERE o.status IN ('paid','done','running','completed') AND ${dateFilter}
    `;
    if (scopeGroupId) { params.push(scopeGroupId); query += ` AND s.group_id = $${params.length}`; }
    query += ' GROUP BY DATE(o.created_at) ORDER BY date';
    const r = await db.query(query, params);
    res.json(r.rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Revenue CSV export with store breakdown
app.get('/api/admin/revenue-export', async (req, res) => {
  try {
    const { userId, days, groupId, startDate, endDate } = req.query;
    const roleInfo = await getUserRole(userId);
    if (roleInfo.role !== 'super_admin' && roleInfo.role !== 'store_admin') return res.status(403).json({ error: 'forbidden' });
    const scopeGroupId = roleInfo.role === 'store_admin' ? roleInfo.groupId : groupId;
    const params = [];
    let dateFilter;
    if (startDate && endDate) {
      params.push(startDate, endDate);
      dateFilter = `o.created_at >= $1::date AND o.created_at < ($2::date + INTERVAL '1 day')`;
    } else {
      const numDays = Math.min(Math.max(parseInt(days) || 7, 1), 365);
      params.push(numDays);
      dateFilter = `o.created_at >= CURRENT_DATE - ($1 || ' days')::INTERVAL`;
    }
    let query = `
      SELECT DATE(o.created_at) as date, s.name as store_name,
        COUNT(*) as orders, COALESCE(SUM(o.total_amount), 0) as revenue
      FROM orders o JOIN stores s ON o.store_id = s.id
      WHERE o.status IN ('paid','done','running','completed') AND ${dateFilter}
    `;
    if (scopeGroupId) { params.push(scopeGroupId); query += ` AND s.group_id = $${params.length}`; }
    query += ' GROUP BY DATE(o.created_at), s.name ORDER BY date, s.name';
    const r = await db.query(query, params);
    const BOM = '\uFEFF';
    let csv = BOM + '日期,店舖名稱,交易筆數,總金額\n';
    r.rows.forEach(row => {
      const date = row.date instanceof Date ? row.date.toISOString().split('T')[0] : String(row.date).split('T')[0];
      csv += `${date},"${row.store_name}",${row.orders},${row.revenue}\n`;
    });
    const today = new Date().toISOString().split('T')[0];
    res.setHeader('Content-Type', 'text/csv; charset=utf-8');
    res.setHeader('Content-Disposition', `attachment; filename="revenue-report-${today}.csv"`);
    res.send(csv);
  } catch (e) { res.status(500).json({ error: e.message }); }
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
    await db.query(`ALTER TABLE members ADD COLUMN IF NOT EXISTS display_name VARCHAR(100)`).catch(() => {});
    await db.query(`ALTER TABLE members ADD COLUMN IF NOT EXISTS picture_url TEXT`).catch(() => {});
    await db.query(`ALTER TABLE members ADD COLUMN IF NOT EXISTS last_login TIMESTAMPTZ`).catch(() => {});
    // Admin user role extra fields
    await db.query(`ALTER TABLE user_roles ADD COLUMN IF NOT EXISTS display_name VARCHAR(100)`).catch(() => {});
    await db.query(`ALTER TABLE user_roles ADD COLUMN IF NOT EXISTS phone VARCHAR(20)`).catch(() => {});
    await db.query(`ALTER TABLE user_roles ADD COLUMN IF NOT EXISTS notes TEXT`).catch(() => {});
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

  console.log('DB initialized with multi-tenant tables');
}

const PORT = process.env.PORT || 3000;
app.listen(PORT, async () => {
  await initDB();
  console.log(`Server running on port ${PORT}`);
});
