#!/usr/bin/env python3
"""
Cloud Monster IPC Controller
Production-ready control script for laundry machine IPC units.

Connects to HiveMQ Cloud via MQTT (TLS), listens for start/stop commands
from the backend, and controls machines via Modbus RTU over RS485.

Uses raw serial (pyserial) for Modbus RTU to avoid pymodbus API compatibility issues.

MQTT Topic Schema (matches backend server.js):
  Subscribe: laundry/{STORE_ID}/+/command
  Publish:   laundry/{STORE_ID}/{machine_id}/status
  Heartbeat: laundry/{STORE_ID}/ipc/heartbeat

Command payload from backend:
  { "cmd": "start", "orderId": "...", "wash_mode": 1, "dry_mode": 1, "coins": 4, "amount": 40 }
  { "cmd": "stop" }
  { "cmd": "skip_step" }
  { "cmd": "clear_coins" }
"""

import json
import time
import ssl
import signal
import sys
import os
import struct
import threading
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime, timezone

try:
    import paho.mqtt.client as mqtt
except ImportError:
    sys.exit("[FATAL] paho-mqtt not installed. Run: pip3 install paho-mqtt")

try:
    import serial
except ImportError:
    sys.exit("[FATAL] pyserial not installed. Run: pip3 install pyserial")


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

MQTT_HOST = os.environ.get("MQTT_HOST", "1eb78bf5e78d4a50852846906854bec1.s1.eu.hivemq.cloud")
MQTT_PORT = int(os.environ.get("MQTT_PORT", "8883"))
MQTT_USER = os.environ.get("MQTT_USER", "laundry")
MQTT_PASS = os.environ.get("MQTT_PASS", "Phchen1108")
STORE_ID = os.environ.get("STORE_ID", "s1")

SERIAL_PORT = os.environ.get("SERIAL_PORT", "/dev/ttyUSB0")
SERIAL_BAUD = int(os.environ.get("SERIAL_BAUD", "9600"))
SERIAL_TIMEOUT = 1  # seconds

LOG_PATH = os.environ.get("LOG_PATH", "/home/ypure/cloud_monster.log")
LOG_MAX_BYTES = 5 * 1024 * 1024  # 5 MB
LOG_BACKUP_COUNT = 3

STATUS_INTERVAL = 10   # seconds between status reads
HEARTBEAT_INTERVAL = 30  # seconds between heartbeat publishes
RECONNECT_BASE = 5     # base seconds for exponential backoff
RECONNECT_MAX = 300    # max backoff cap

# ---------------------------------------------------------------------------
# SX176005A Touchscreen Modbus Protocol — Register Map
# ---------------------------------------------------------------------------
# Write Registers (function code 0x06 single register):
#   Address 0: Fault reset / mute command
#   Address 1: Start machine (write 1)
#   Address 2: Skip step (write 1)
#   Address 3: Force stop (write 1)
#   Address 4: Paid coins / amount (0-65535)
#   Address 5: Select wash program (0=none, 1-29)
#   Address 6: Select dry program (0=none, 1=high, 2=mid, 3=low, 4=soft)
#
# Read Registers (function code 0x03, start address 20, count 24):
#   Address 20: Machine state (0=power_on, 1=standby, 3=auto_running, 4=manual)
#   Address 21: Door state (0=idle, 1=open, 2=closed, 3=locked, 4=error, 5=locking, 6=unlocking)
#   Address 22: Fault state (bit0=fault, bit1=warning)
#   Address 23: Step remaining time (minutes)
#   Address 24: Step remaining time (seconds)
#   Address 25: Auto program total remaining (hours)
#   Address 26: Auto program total remaining (minutes)
#   Address 27: Auto program total remaining (seconds)
#   Address 28: Current water level (cm)
#   Address 29: Set water level
#   Address 30: Current temperature (deg C)
#   Address 31: Set temperature
#   Address 32: Current RPM
#   Address 33: Set RPM
#   Address 38: Current wash program number
#   Address 39: Current dry program number
#   Address 40: Current step number
#   Address 41: Required coins
#   Address 42: Current coin count
# ---------------------------------------------------------------------------

REG_WRITE_FAULT_RESET  = 0   # Write: fault reset / mute
REG_WRITE_START        = 1   # Write: 1 = start machine
REG_WRITE_SKIP_STEP    = 2   # Write: 1 = skip step
REG_WRITE_FORCE_STOP   = 3   # Write: 1 = force stop
REG_WRITE_COINS        = 4   # Write: paid coins / amount
REG_WRITE_WASH_PROG    = 5   # Write: wash program (0=none, 1-29)
REG_WRITE_DRY_PROG     = 6   # Write: dry program (0=none, 1-4)

REG_READ_START         = 20  # Read start address
REG_READ_COUNT         = 24  # Number of registers to read (20..43)

# Machine mapping -- machine_id -> Modbus slave + metadata
MACHINE_MAP = {
    "s1_washer_1": {"slave": 1, "type": "washer", "name": "Wash-Dry 1 (Large)"},
    "s1_washer_2": {"slave": 2, "type": "washer", "name": "Wash-Dry 2 (Medium)"},
    "s1_washer_3": {"slave": 3, "type": "washer", "name": "Wash-Dry 3 (Medium)"},
    "s1_dryer_1":  {"slave": 4, "type": "dryer",  "name": "Dryer 1"},
    "s1_dryer_2":  {"slave": 5, "type": "dryer",  "name": "Dryer 2"},
    "s1_dryer_3":  {"slave": 6, "type": "dryer",  "name": "Dryer 3"},
}

# Machine state mapping (address 20)
STATE_MAP = {0: "power_on", 1: "idle", 3: "running", 4: "manual"}

# Door state mapping (address 21)
DOOR_MAP = {0: "idle", 1: "open", 2: "closed", 3: "locked", 4: "error", 5: "locking", 6: "unlocking"}

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def setup_logging():
    log_dir = os.path.dirname(LOG_PATH)
    if log_dir and not os.path.exists(log_dir):
        try:
            os.makedirs(log_dir, exist_ok=True)
        except OSError:
            pass

    logger = logging.getLogger("cloud_monster")
    logger.setLevel(logging.DEBUG)

    fmt = logging.Formatter(
        "[%(asctime)s] %(levelname)-7s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # File handler with rotation
    try:
        fh = RotatingFileHandler(
            LOG_PATH, maxBytes=LOG_MAX_BYTES, backupCount=LOG_BACKUP_COUNT
        )
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(fmt)
        logger.addHandler(fh)
    except (OSError, PermissionError) as exc:
        print(f"[WARN] Cannot write to {LOG_PATH}: {exc}. Logging to stdout only.")

    # Console handler
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    return logger


log = setup_logging()


# ---------------------------------------------------------------------------
# Raw Modbus RTU helpers (using pyserial directly)
# ---------------------------------------------------------------------------

def crc16_modbus(data: bytes) -> int:
    """Calculate CRC16/Modbus over a byte sequence."""
    crc = 0xFFFF
    for byte in data:
        crc ^= byte
        for _ in range(8):
            if crc & 0x0001:
                crc = (crc >> 1) ^ 0xA001
            else:
                crc >>= 1
    return crc


def build_read_request(slave: int, address: int, count: int) -> bytes:
    """Build Modbus RTU read holding registers (FC 0x03) request."""
    pdu = struct.pack('>BBHH', slave, 0x03, address, count)
    crc = crc16_modbus(pdu)
    return pdu + struct.pack('<H', crc)


def build_write_request(slave: int, address: int, value: int) -> bytes:
    """Build Modbus RTU write single register (FC 0x06) request."""
    pdu = struct.pack('>BBHH', slave, 0x06, address, value)
    crc = crc16_modbus(pdu)
    return pdu + struct.pack('<H', crc)


def hex_str(data: bytes) -> str:
    return ' '.join(f'{b:02X}' for b in data)


# ---------------------------------------------------------------------------
# CloudMonsterIPC
# ---------------------------------------------------------------------------

class CloudMonsterIPC:
    """Main controller: bridges MQTT commands to Modbus RTU machines."""

    def __init__(self):
        self.running = True
        self.mqtt_client: mqtt.Client | None = None
        self.serial_port: serial.Serial | None = None
        self.modbus_lock = threading.Lock()
        self.machine_states: dict[str, dict] = {}
        self._reconnect_attempts = 0
        self._pending_coins: dict[str, int] = {}
        self._pending_dry: dict[str, int] = {}

    # ------------------------------------------------------------------
    # MQTT
    # ------------------------------------------------------------------

    def setup_mqtt(self):
        client_id = f"ipc-{STORE_ID}-{int(time.time())}"
        self.mqtt_client = mqtt.Client(
            client_id=client_id,
            protocol=mqtt.MQTTv311,
            transport="tcp",
        )
        self.mqtt_client.username_pw_set(MQTT_USER, MQTT_PASS)

        # TLS for HiveMQ Cloud
        ctx = ssl.create_default_context()
        self.mqtt_client.tls_set_context(ctx)

        # Callbacks
        self.mqtt_client.on_connect = self._on_connect
        self.mqtt_client.on_disconnect = self._on_disconnect
        self.mqtt_client.on_message = self._on_message

        # Last Will: offline heartbeat
        will_topic = f"laundry/{STORE_ID}/ipc/heartbeat"
        will_payload = json.dumps({
            "store_id": STORE_ID,
            "status": "offline",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })
        self.mqtt_client.will_set(will_topic, will_payload, qos=1, retain=True)

        log.info("Connecting MQTT -> %s:%d as %s", MQTT_HOST, MQTT_PORT, client_id)
        self.mqtt_client.connect_async(MQTT_HOST, MQTT_PORT, keepalive=60)
        self.mqtt_client.loop_start()

    def _on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            self._reconnect_attempts = 0
            log.info("MQTT connected (rc=%d)", rc)
            topic = f"laundry/{STORE_ID}/+/command"
            client.subscribe(topic, qos=1)
            log.info("Subscribed to %s", topic)
        else:
            log.error("MQTT connect failed rc=%d", rc)

    def _on_disconnect(self, client, userdata, rc):
        if rc != 0:
            log.warning("MQTT unexpected disconnect (rc=%d). Will auto-reconnect.", rc)
            self._reconnect_attempts += 1
            backoff = min(RECONNECT_BASE * (2 ** self._reconnect_attempts), RECONNECT_MAX)
            log.info("Reconnect backoff: %ds", backoff)
            self.mqtt_client.reconnect_delay_set(
                min_delay=backoff, max_delay=backoff + 5
            )

    def _on_message(self, client, userdata, msg):
        """Handle incoming MQTT command messages."""
        topic = msg.topic
        try:
            payload = json.loads(msg.payload.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            log.error("Bad payload on %s: %s", topic, exc)
            return

        log.info("MQTT << %s : %s", topic, json.dumps(payload, ensure_ascii=False))

        parts = topic.split("/")
        if len(parts) < 4 or parts[3] != "command":
            log.warning("Ignoring unexpected topic: %s", topic)
            return

        machine_id = parts[2]
        if machine_id not in MACHINE_MAP:
            log.warning("Unknown machine_id=%s, ignoring", machine_id)
            return

        cmd = payload.get("cmd", "")
        try:
            if cmd == "start":
                self._handle_start(machine_id, payload)
            elif cmd == "stop":
                self._handle_stop(machine_id)
            elif cmd == "skip_step":
                self._handle_skip_step(machine_id)
            elif cmd == "clear_coins":
                self._handle_clear_coins(machine_id)
            else:
                log.warning("Unknown cmd=%s for %s", cmd, machine_id)
        except Exception as exc:
            log.exception("Error handling cmd=%s for %s: %s", cmd, machine_id, exc)

    # ------------------------------------------------------------------
    # Command handlers
    # ------------------------------------------------------------------

    def _handle_start(self, machine_id: str, payload: dict):
        wash_mode = int(payload.get("wash_mode", 1))
        dry_mode = int(payload.get("dry_mode", 0))
        coins = int(payload.get("coins", 4))
        order_id = payload.get("orderId", "unknown")

        self._pending_coins[machine_id] = coins
        self._pending_dry[machine_id] = dry_mode

        log.info(
            "START %s order=%s wash_prog=%d dry_prog=%d coins=%d",
            machine_id, order_id, wash_mode, dry_mode, coins,
        )

        success = self.start_machine(machine_id, wash_mode, 0)

        if success:
            self.machine_states[machine_id] = {
                "state": "running",
                "remain_sec": 0,
                "wash_program": wash_mode,
                "dry_program": dry_mode,
                "order_id": order_id,
                "started_at": time.time(),
            }
            self._publish_status(machine_id)
        else:
            log.error("Failed to start %s via Modbus", machine_id)
            self.machine_states[machine_id] = {
                "state": "error",
                "remain_sec": 0,
                "wash_program": wash_mode,
                "dry_program": dry_mode,
                "order_id": order_id,
                "started_at": time.time(),
            }
            self._publish_status(machine_id)

    def _handle_stop(self, machine_id: str):
        log.info("STOP %s", machine_id)
        success = self.stop_machine(machine_id)
        if success:
            self.machine_states[machine_id] = {
                "state": "idle",
                "remain_sec": 0,
            }
            self._publish_status(machine_id)

    def _handle_skip_step(self, machine_id: str):
        log.info("SKIP_STEP %s", machine_id)
        slave = MACHINE_MAP[machine_id]["slave"]
        with self.modbus_lock:
            ok = self._modbus_write_register(slave, REG_WRITE_SKIP_STEP, 1)
            if ok:
                log.info("Skip step sent to slave=%d", slave)
            else:
                log.error("skip_step Modbus write failed for %s", machine_id)

    def _handle_clear_coins(self, machine_id: str):
        log.info("CLEAR_COINS %s", machine_id)
        slave = MACHINE_MAP[machine_id]["slave"]
        with self.modbus_lock:
            ok = self._modbus_write_register(slave, REG_WRITE_COINS, 0)
            if ok:
                log.info("Clear coins sent to slave=%d", slave)
            else:
                log.error("clear_coins Modbus write failed for %s", machine_id)

    # ------------------------------------------------------------------
    # Raw Modbus RTU over pyserial
    # ------------------------------------------------------------------

    def setup_modbus(self):
        """Initialize serial port for Modbus RTU."""
        try:
            self.serial_port = serial.Serial(
                port=SERIAL_PORT,
                baudrate=SERIAL_BAUD,
                bytesize=8,
                parity='N',
                stopbits=1,
                timeout=SERIAL_TIMEOUT,
            )
            log.info("Modbus connected on %s @ %d baud", SERIAL_PORT, SERIAL_BAUD)
        except (serial.SerialException, OSError) as exc:
            log.error("Modbus connection failed on %s: %s", SERIAL_PORT, exc)
            self.serial_port = None

    def _ensure_serial(self) -> bool:
        """Reconnect serial if needed. Returns True if connected."""
        if self.serial_port is not None and self.serial_port.is_open:
            return True

        log.info("Serial reconnecting...")
        try:
            if self.serial_port:
                self.serial_port.close()
            self.serial_port = serial.Serial(
                port=SERIAL_PORT,
                baudrate=SERIAL_BAUD,
                bytesize=8,
                parity='N',
                stopbits=1,
                timeout=SERIAL_TIMEOUT,
            )
            log.info("Serial reconnected on %s", SERIAL_PORT)
            return True
        except (serial.SerialException, OSError) as exc:
            log.error("Serial reconnect failed: %s", exc)
            self.serial_port = None
            return False

    def _serial_send_receive(self, request: bytes, timeout: float = 1.0) -> bytes:
        """Send Modbus RTU request and read response."""
        if not self.serial_port:
            return b''

        self.serial_port.reset_input_buffer()
        self.serial_port.write(request)
        time.sleep(0.05)  # inter-frame gap

        end_time = time.time() + timeout
        response = b''
        while time.time() < end_time:
            if self.serial_port.in_waiting > 0:
                response += self.serial_port.read(self.serial_port.in_waiting)
                time.sleep(0.05)
            else:
                if response:
                    break  # got data, no more coming
                time.sleep(0.02)
        return response

    def _modbus_write_register(self, slave: int, address: int, value: int) -> bool:
        """Write single register (FC 0x06). Returns True on success."""
        if not self._ensure_serial():
            log.error("Modbus WRITE failed: serial not connected")
            return False

        req = build_write_request(slave, address, value)
        resp = self._serial_send_receive(req)

        log.debug("Modbus WRITE slave=%d addr=%d val=%d TX[%s] RX[%s]",
                  slave, address, value, hex_str(req), hex_str(resp))

        if resp and len(resp) >= 8 and resp[1] == 0x06:
            # FC 0x06 echo response confirms write
            log.info("Modbus WRITE OK slave=%d addr=%d val=%d", slave, address, value)
            return True
        elif resp and len(resp) >= 3 and resp[1] & 0x80:
            log.error("Modbus WRITE exception slave=%d addr=%d code=%d", slave, address, resp[2])
            return False
        else:
            log.error("Modbus WRITE no valid response slave=%d addr=%d RX[%s]",
                      slave, address, hex_str(resp))
            return False

    def _modbus_read_registers(self, slave: int, address: int, count: int) -> list | None:
        """Read holding registers (FC 0x03). Returns list of int values or None."""
        if not self._ensure_serial():
            log.warning("Modbus READ failed: serial not connected")
            return None

        req = build_read_request(slave, address, count)
        expected_bytes = 3 + count * 2 + 2  # slave + fc + bytecount + data + crc
        resp = self._serial_send_receive(req, timeout=1.5)

        log.debug("Modbus READ slave=%d addr=%d count=%d TX[%s] RX[%s]",
                  slave, address, count, hex_str(req), hex_str(resp))

        if not resp:
            log.warning("Modbus READ no response slave=%d addr=%d", slave, address)
            return None

        if len(resp) < 5:
            log.warning("Modbus READ response too short slave=%d len=%d", slave, len(resp))
            return None

        if resp[1] & 0x80:
            log.warning("Modbus READ exception slave=%d addr=%d code=%d", slave, address, resp[2])
            return None

        if resp[1] != 0x03:
            log.warning("Modbus READ unexpected FC=0x%02X slave=%d", resp[1], slave)
            return None

        byte_count = resp[2]
        if len(resp) < 3 + byte_count:
            log.warning("Modbus READ incomplete slave=%d expected=%d got=%d",
                        slave, 3 + byte_count, len(resp))
            return None

        # Parse register values
        regs = []
        for i in range(count):
            offset = 3 + i * 2
            if offset + 2 <= len(resp):
                val = struct.unpack('>H', resp[offset:offset + 2])[0]
                regs.append(val)
        return regs

    def start_machine(self, machine_id: str, mode: int, duration_min: int) -> bool:
        """Send SX176005A Modbus start sequence to a machine."""
        info = MACHINE_MAP.get(machine_id)
        if not info:
            log.error("start_machine: unknown machine %s", machine_id)
            return False

        slave = info["slave"]
        wash_prog = mode
        dry_prog = self._pending_dry.get(machine_id, 0)
        coins = self._pending_coins.get(machine_id, 4)

        with self.modbus_lock:
            # Step 1: Write wash program (address 5)
            if not self._modbus_write_register(slave, REG_WRITE_WASH_PROG, wash_prog):
                log.error("start_machine: failed to write wash_prog for %s", machine_id)
                return False
            time.sleep(0.1)

            # Step 2: Write dry program (address 6)
            if not self._modbus_write_register(slave, REG_WRITE_DRY_PROG, dry_prog):
                log.error("start_machine: failed to write dry_prog for %s", machine_id)
                return False
            time.sleep(0.1)

            # Step 3: Write coins/amount (address 4)
            if not self._modbus_write_register(slave, REG_WRITE_COINS, coins):
                log.error("start_machine: failed to write coins for %s", machine_id)
                return False
            time.sleep(0.1)

            # Step 4: Start machine (address 1, value 1)
            if not self._modbus_write_register(slave, REG_WRITE_START, 1):
                log.error("start_machine: failed to write start for %s", machine_id)
                return False

            log.info(
                "Machine %s (slave=%d) started: wash_prog=%d dry_prog=%d coins=%d",
                machine_id, slave, wash_prog, dry_prog, coins,
            )
            return True

    def stop_machine(self, machine_id: str) -> bool:
        """Send force-stop command via SX176005A protocol."""
        info = MACHINE_MAP.get(machine_id)
        if not info:
            log.error("stop_machine: unknown machine %s", machine_id)
            return False

        slave = info["slave"]
        with self.modbus_lock:
            if self._modbus_write_register(slave, REG_WRITE_FORCE_STOP, 1):
                log.info("Machine %s (slave=%d) force-stopped", machine_id, slave)
                return True
            else:
                log.error("stop_machine Modbus write failed for %s", machine_id)
                return False

    def read_machine_status(self, machine_id: str) -> dict | None:
        """Read current status from a single machine via SX176005A protocol."""
        info = MACHINE_MAP.get(machine_id)
        if not info:
            return None

        slave = info["slave"]
        with self.modbus_lock:
            regs = self._modbus_read_registers(slave, REG_READ_START, REG_READ_COUNT)

        if regs is None or len(regs) < REG_READ_COUNT:
            return None

        # Parse registers (index = address - 20)
        machine_state_raw = regs[0]   # Address 20
        door_state_raw    = regs[1]   # Address 21
        fault_state       = regs[2]   # Address 22
        step_remain_min   = regs[3]   # Address 23
        step_remain_sec   = regs[4]   # Address 24
        total_remain_hr   = regs[5]   # Address 25
        total_remain_min  = regs[6]   # Address 26
        total_remain_sec  = regs[7]   # Address 27
        current_temp      = regs[10]  # Address 30
        current_rpm       = regs[12]  # Address 32
        wash_prog         = regs[18]  # Address 38
        dry_prog          = regs[19]  # Address 39
        current_step      = regs[20]  # Address 40
        required_coins    = regs[21]  # Address 41
        current_coins     = regs[22]  # Address 42

        state = STATE_MAP.get(machine_state_raw, "unknown")
        door = DOOR_MAP.get(door_state_raw, "unknown")
        total_remain_seconds = total_remain_hr * 3600 + total_remain_min * 60 + total_remain_sec

        return {
            "machine_id": machine_id,
            "state": state,
            "door": door,
            "remain_sec": total_remain_seconds,
            "step_remain": step_remain_min * 60 + step_remain_sec,
            "temperature": current_temp,
            "rpm": current_rpm,
            "wash_program": wash_prog,
            "dry_program": dry_prog,
            "current_step": current_step,
            "required_coins": required_coins,
            "current_coins": current_coins,
            "fault": bool(fault_state & 0x01),
            "warning": bool(fault_state & 0x02),
            "store_id": STORE_ID,
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
        }

    # ------------------------------------------------------------------
    # MQTT publish helpers
    # ------------------------------------------------------------------

    def _publish_status(self, machine_id: str):
        """Publish a single machine's status to MQTT."""
        if not self.mqtt_client or not self.mqtt_client.is_connected():
            log.warning("Cannot publish status: MQTT not connected")
            return

        cached = self.machine_states.get(machine_id, {})
        state = cached.get("state", "idle")
        remain_sec = cached.get("remain_sec", 0)

        topic = f"laundry/{STORE_ID}/{machine_id}/status"
        payload = {
            "machine_id": machine_id,
            "state": state,
            "remain_sec": remain_sec,
            "door": cached.get("door", "unknown"),
            "step_remain": cached.get("step_remain", 0),
            "temperature": cached.get("temperature", 0),
            "rpm": cached.get("rpm", 0),
            "wash_program": cached.get("wash_program", 0),
            "dry_program": cached.get("dry_program", 0),
            "current_step": cached.get("current_step", 0),
            "required_coins": cached.get("required_coins", 0),
            "current_coins": cached.get("current_coins", 0),
            "fault": cached.get("fault", False),
            "warning": cached.get("warning", False),
            "store_id": STORE_ID,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        self.mqtt_client.publish(topic, json.dumps(payload), qos=1)
        log.debug("MQTT >> %s : state=%s remain=%ds", topic, state, remain_sec)

    def _publish_heartbeat(self):
        """Publish IPC heartbeat."""
        if not self.mqtt_client or not self.mqtt_client.is_connected():
            return

        topic = f"laundry/{STORE_ID}/ipc/heartbeat"
        payload = {
            "store_id": STORE_ID,
            "status": "online",
            "machines": len(MACHINE_MAP),
            "serial_connected": (
                self.serial_port is not None and self.serial_port.is_open
            ),
            "uptime_sec": int(time.time() - self._start_time),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        self.mqtt_client.publish(topic, json.dumps(payload), qos=1, retain=True)
        log.debug("Heartbeat published")

    # ------------------------------------------------------------------
    # Background loops
    # ------------------------------------------------------------------

    def status_loop(self):
        """Periodically read all machine statuses via Modbus and publish."""
        log.info("Status loop started (interval=%ds)", STATUS_INTERVAL)
        while self.running:
            for machine_id in MACHINE_MAP:
                if not self.running:
                    break
                try:
                    hw_status = self.read_machine_status(machine_id)
                    if hw_status:
                        cached = self.machine_states.get(machine_id, {})
                        order_id = cached.get("order_id")
                        cached.update(hw_status)
                        if order_id:
                            cached["order_id"] = order_id
                        self.machine_states[machine_id] = cached
                        self._publish_status(machine_id)
                    else:
                        log.debug("No status response from %s", machine_id)
                except Exception as exc:
                    log.error("Status read error for %s: %s", machine_id, exc)

            # Sleep in small increments so we can exit quickly
            for _ in range(STATUS_INTERVAL * 10):
                if not self.running:
                    break
                time.sleep(0.1)

    def heartbeat_loop(self):
        """Send heartbeat every HEARTBEAT_INTERVAL seconds."""
        log.info("Heartbeat loop started (interval=%ds)", HEARTBEAT_INTERVAL)
        while self.running:
            try:
                self._publish_heartbeat()
            except Exception as exc:
                log.error("Heartbeat error: %s", exc)

            for _ in range(HEARTBEAT_INTERVAL * 10):
                if not self.running:
                    break
                time.sleep(0.1)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def run(self):
        """Main entry point."""
        self._start_time = time.time()
        log.info("=" * 60)
        log.info("Cloud Monster IPC starting  store=%s", STORE_ID)
        log.info("MQTT=%s:%d  Serial=%s@%d", MQTT_HOST, MQTT_PORT, SERIAL_PORT, SERIAL_BAUD)
        log.info("Machines: %s", ", ".join(MACHINE_MAP.keys()))
        log.info("=" * 60)

        # Setup connections
        self.setup_mqtt()
        self.setup_modbus()

        # Initialize machine states
        for mid in MACHINE_MAP:
            self.machine_states[mid] = {"state": "idle", "remain_sec": 0, "mode": 0}

        # Start background threads
        status_thread = threading.Thread(target=self.status_loop, name="status", daemon=True)
        heartbeat_thread = threading.Thread(target=self.heartbeat_loop, name="heartbeat", daemon=True)
        status_thread.start()
        heartbeat_thread.start()

        log.info("Cloud Monster IPC running. Press Ctrl+C to stop.")

        try:
            while self.running:
                time.sleep(1)
        except KeyboardInterrupt:
            log.info("KeyboardInterrupt received")
        finally:
            self.shutdown()

    def shutdown(self):
        """Graceful shutdown."""
        if not self.running:
            return
        self.running = False
        log.info("Shutting down Cloud Monster IPC...")

        # Publish offline heartbeat
        if self.mqtt_client and self.mqtt_client.is_connected():
            topic = f"laundry/{STORE_ID}/ipc/heartbeat"
            payload = json.dumps({
                "store_id": STORE_ID,
                "status": "offline",
                "timestamp": datetime.now(timezone.utc).isoformat(),
            })
            self.mqtt_client.publish(topic, payload, qos=1, retain=True)
            time.sleep(0.5)
            self.mqtt_client.loop_stop()
            self.mqtt_client.disconnect()
            log.info("MQTT disconnected")

        # Close serial
        if self.serial_port and self.serial_port.is_open:
            try:
                self.serial_port.close()
                log.info("Serial closed")
            except Exception:
                pass

        log.info("Cloud Monster IPC stopped.")


# ---------------------------------------------------------------------------
# Signal handlers
# ---------------------------------------------------------------------------

_ipc_instance: CloudMonsterIPC | None = None


def _signal_handler(signum, frame):
    sig_name = signal.Signals(signum).name if hasattr(signal, "Signals") else str(signum)
    log.info("Signal %s received", sig_name)
    if _ipc_instance:
        _ipc_instance.shutdown()
    sys.exit(0)


# ---------------------------------------------------------------------------
# Systemd service file generator
# ---------------------------------------------------------------------------

SYSTEMD_UNIT = """\
[Unit]
Description=Cloud Monster IPC Controller
Documentation=https://github.com/a0989586419-create/laundry-backend
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=ypure
Group=dialout
WorkingDirectory=/opt/cloud_monster
ExecStart=/usr/bin/python3 /opt/cloud_monster/cloud_monster.py
Restart=always
RestartSec=10
WatchdogSec=120
Environment=STORE_ID={store_id}
Environment=SERIAL_PORT={serial_port}
StandardOutput=journal
StandardError=journal
SyslogIdentifier=cloud-monster

# Hardening
ProtectSystem=strict
ReadWritePaths=/home/ypure /opt/cloud_monster
PrivateTmp=true
NoNewPrivileges=true

[Install]
WantedBy=multi-user.target
"""


def generate_systemd_service(output_path: str = "/tmp/cloud-monster.service"):
    content = SYSTEMD_UNIT.format(store_id=STORE_ID, serial_port=SERIAL_PORT)
    with open(output_path, "w") as f:
        f.write(content)
    print(f"Systemd service file written to {output_path}")
    print("Install with:")
    print(f"  sudo cp {output_path} /etc/systemd/system/cloud-monster.service")
    print("  sudo systemctl daemon-reload")
    print("  sudo systemctl enable --now cloud-monster")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--generate-service":
        out = sys.argv[2] if len(sys.argv) > 2 else "/tmp/cloud-monster.service"
        generate_systemd_service(out)
        sys.exit(0)

    _ipc_instance = CloudMonsterIPC()

    signal.signal(signal.SIGTERM, _signal_handler)
    signal.signal(signal.SIGINT, _signal_handler)

    _ipc_instance.run()
