require('dotenv').config();
const express = require('express');
const mqtt = require('mqtt');
const { Pool } = require('pg');
const cors = require('cors');
const { v4: uuid } = require('uuid');

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

// API
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
  const pulses = Math.ceil(totalAmount / 10);
  const modeDur = { standard:65, small:50, washonly:35, soft:65, strong:75, dryonly:40 };
  const durationSec = ((modeDur[mode]||65) + parseInt(extendMin||0)) * 60;
  await db.query(`INSERT INTO orders (id,member_id,store_id,machine_id,mode,addons,extend_min,temp,total_amount,pulses,duration_sec,status,created_at)
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,'pending',NOW())`,
    [orderId, member.id, storeId, machineId, mode, JSON.stringify(addons||[]), extendMin||0, temp, totalAmount, pulses, durationSec]);
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
  await db.query(`INSERT INTO stores (id,name,address,phone) VALUES ('s1','三重和平店','新北市三重區和平路88號','02-2288-8888'),('s2','信義嘉興店','台北市信義區嘉興街120號','02-2777-7777') ON CONFLICT DO NOTHING`);
  for (const sid of ['s1','s2']) {
    for (let i=1;i<=6;i++) {
      const size = i<=2?'大型':i<=5?'中型':'小型';
      await db.query(`INSERT INTO machines (id,store_id,name,size,sort_order) VALUES ($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING`, [`${sid}-m${i}`,sid,`洗脫烘${i}號`,size,i]);
    }
  }
  console.log('✅ 資料庫初始化完成');
}

const PORT = process.env.PORT || 3000;
app.listen(PORT, async () => {
  await initDB();
  console.log(`🚀 伺服器啟動：port ${PORT}`);
});
