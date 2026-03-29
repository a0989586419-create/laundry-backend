#!/usr/bin/env python3
"""
Modbus RTU Slave 模擬器 — 模擬背板 PLC 回應觸控屏的輪詢請求

使用場景：
  觸控屏作為 Modbus Master，持續以 ~3.5 次/秒 的速率輪詢 slave address 01。
  已知輪詢內容：FC 0x03, 起始位址 252 (0x00FC), 數量 97 個暫存器。
  本工具作為 Modbus Slave (address 1) 回應這些請求，
  讓觸控屏以為正在與真正的 PLC 通訊。

環境變數：
  SERIAL_PORT   — 串口路徑   (預設 /dev/ttyUSB0)
  SERIAL_BAUD   — 波特率     (預設 9600)
  SLAVE_ADDR    — 從站位址   (預設 1)

用法：
  python3 test_modbus_slave.py
  python3 test_modbus_slave.py --simulate    # 每 10 秒自動變更暫存器值模擬狀態變化
  SERIAL_PORT=/dev/ttyUSB1 python3 test_modbus_slave.py

按 Ctrl+C 可停止。
"""

import os
import sys
import time
import struct
import argparse
from datetime import datetime

try:
    import serial
except ImportError:
    sys.exit("請先安裝: pip3 install pyserial")

# ─── 設定 ─────────────────────────────────────────────────────────────
SERIAL_PORT = os.environ.get("SERIAL_PORT", "/dev/ttyUSB0")
SERIAL_BAUD = int(os.environ.get("SERIAL_BAUD", "9600"))
SLAVE_ADDR = int(os.environ.get("SLAVE_ADDR", "1"))

# 3.5 字元時間作為訊框分隔（Modbus RTU 規範）
# 每字元 = 11 bit (1 start + 8 data + 0 parity + 1 stop)
# 在 9600 baud: 1 字元 ≈ 1.146 ms，3.5 字元 ≈ 4.01 ms
CHAR_TIME = 11.0 / SERIAL_BAUD          # 單一字元傳輸時間（秒）
FRAME_GAP = CHAR_TIME * 3.5             # 訊框間隔（秒）
FRAME_GAP_MS = FRAME_GAP * 1000         # 轉換為毫秒

# 確保最小間隔不低於 2ms（避免極高波特率下的誤判）
if FRAME_GAP < 0.002:
    FRAME_GAP = 0.002
    FRAME_GAP_MS = 2.0

# 回應前的最小延遲（Modbus 規範要求至少 3.5 字元時間的 turnaround）
TURNAROUND_DELAY = max(FRAME_GAP, 0.002)


# ─── CRC16 Modbus ─────────────────────────────────────────────────────
def crc16(data):
    """計算 Modbus CRC16（與其他腳本相同的實作）"""
    crc = 0xFFFF
    for b in data:
        crc ^= b
        for _ in range(8):
            if crc & 1:
                crc = (crc >> 1) ^ 0xA001
            else:
                crc >>= 1
    return crc


def verify_crc(frame):
    """驗證訊框的 CRC16，回傳 (is_valid, calculated_crc, received_crc)"""
    if len(frame) < 4:
        return False, 0, 0
    payload = frame[:-2]
    received_crc = struct.unpack('<H', frame[-2:])[0]
    calculated_crc = crc16(payload)
    return (calculated_crc == received_crc), calculated_crc, received_crc


def append_crc(data):
    """在 data 後面附加 CRC16 低位元在前"""
    c = crc16(data)
    return data + struct.pack('<H', c)


# ─── 工具函數 ─────────────────────────────────────────────────────────
def hex_str(data):
    """將 bytes 轉為十六進位字串"""
    return ' '.join(f'{b:02X}' for b in data)


def timestamp_str():
    """取得當前時間戳記字串"""
    return datetime.now().strftime('%H:%M:%S.%f')[:-3]


# ─── 暫存器地圖 ───────────────────────────────────────────────────────

# 已知暫存器名稱對照表（觸控屏讀取範圍 252-348）
REGISTER_NAMES = {
    252: "狀態字 0",
    253: "狀態字 1",
    254: "狀態字 2",
    255: "狀態字 3",
    256: "狀態字 4",
    260: "機器狀態",
    261: "門鎖狀態",
    262: "故障碼",
    270: "剩餘時間(分)",
    271: "剩餘時間(秒)",
    280: "溫度",
    290: "轉速",
    300: "程式編號",
    310: "投幣金額",
    320: "水位",
}


def get_register_name(addr):
    """嘗試取得暫存器的中文名稱"""
    if addr in REGISTER_NAMES:
        return REGISTER_NAMES[addr]
    return ""


class RegisterMap:
    """
    Modbus 暫存器地圖 — 內部以 Python dict 儲存。
    暫存器值為 16-bit unsigned (0-65535)。
    """

    def __init__(self):
        self._regs = {}
        # 初始化觸控屏輪詢範圍的暫存器 (252-348) 為 0
        for addr in range(252, 349):
            self._regs[addr] = 0

        # 設定有意義的預設值，模擬待機/閒置狀態
        # 這些值可能讓觸控屏顯示為「待機中」
        self._regs[252] = 0x0001   # 狀態字 0: 表示有通訊/連線
        self._regs[260] = 1        # 機器狀態: 1=standby
        self._regs[261] = 0        # 門鎖狀態: 0=idle
        self._regs[262] = 0        # 故障碼: 0=無故障
        self._regs[270] = 0        # 剩餘時間(分): 0
        self._regs[271] = 0        # 剩餘時間(秒): 0
        self._regs[280] = 25       # 溫度: 25 度（室溫）
        self._regs[290] = 0        # 轉速: 0
        self._regs[300] = 0        # 程式編號: 0（未選擇）
        self._regs[310] = 0        # 投幣金額: 0
        self._regs[320] = 0        # 水位: 0

    def read(self, start_addr, count):
        """
        讀取連續暫存器，回傳 list of int。
        未定義的暫存器回傳 0。
        """
        values = []
        for addr in range(start_addr, start_addr + count):
            values.append(self._regs.get(addr, 0))
        return values

    def write(self, addr, value):
        """寫入單一暫存器（自動限制為 16-bit）"""
        self._regs[addr] = value & 0xFFFF

    def write_multiple(self, start_addr, values):
        """寫入多個連續暫存器"""
        for i, val in enumerate(values):
            self._regs[start_addr + i] = val & 0xFFFF

    def dump(self, start_addr, count):
        """印出暫存器內容（用於除錯）"""
        lines = []
        for addr in range(start_addr, start_addr + count):
            val = self._regs.get(addr, 0)
            name = get_register_name(addr)
            if val != 0 or name:
                suffix = f"  -- {name}" if name else ""
                lines.append(f"  REG[{addr}] = {val} (0x{val:04X}){suffix}")
        return '\n'.join(lines)


# ─── Modbus 回應建構 ──────────────────────────────────────────────────

def build_fc03_response(slave, register_values):
    """
    建構 FC 0x03 讀取保持暫存器回應。
    格式: [slave] [0x03] [byte_count] [data_hi data_lo ...] [CRC_lo CRC_hi]
    """
    byte_count = len(register_values) * 2
    pdu = struct.pack('>BBB', slave, 0x03, byte_count)
    for val in register_values:
        pdu += struct.pack('>H', val & 0xFFFF)
    return append_crc(pdu)


def build_fc06_response(slave, reg_addr, reg_value):
    """
    建構 FC 0x06 寫入單一暫存器回應（echo back）。
    格式: [slave] [0x06] [addr_hi] [addr_lo] [val_hi] [val_lo] [CRC_lo CRC_hi]
    """
    pdu = struct.pack('>BBHH', slave, 0x06, reg_addr, reg_value)
    return append_crc(pdu)


def build_fc10_response(slave, start_addr, reg_count):
    """
    建構 FC 0x10 寫入多個暫存器回應。
    格式: [slave] [0x10] [addr_hi] [addr_lo] [count_hi] [count_lo] [CRC_lo CRC_hi]
    """
    pdu = struct.pack('>BBHH', slave, 0x10, start_addr, reg_count)
    return append_crc(pdu)


def build_exception_response(slave, fc, exception_code):
    """
    建構例外回應。
    格式: [slave] [FC|0x80] [exception_code] [CRC_lo CRC_hi]
    """
    pdu = struct.pack('>BBB', slave, fc | 0x80, exception_code)
    return append_crc(pdu)


# ─── 訊框處理 ─────────────────────────────────────────────────────────

def process_frame(frame, reg_map, stats, verbose=True):
    """
    解析收到的 Modbus 請求訊框，產生回應。
    回傳 (response_bytes, log_message) 或 (None, log_message)。
    """
    ts = timestamp_str()

    # 基本長度檢查
    if len(frame) < 4:
        stats['errors'] += 1
        return None, f"[{ts}] 訊框太短 ({len(frame)} bytes): {hex_str(frame)}"

    # CRC 驗證
    crc_ok, calc_crc, recv_crc = verify_crc(frame)
    if not crc_ok:
        stats['crc_errors'] += 1
        return None, (f"[{ts}] CRC 錯誤: 算出 0x{calc_crc:04X}, "
                      f"收到 0x{recv_crc:04X} | RAW: {hex_str(frame)}")

    slave = frame[0]
    fc = frame[1]

    # 不是我們的位址 — 忽略（可能是其他 slave 的請求）
    if slave != SLAVE_ADDR:
        stats['ignored'] += 1
        if verbose:
            return None, (f"[{ts}] 忽略: Slave={slave} (非本站 {SLAVE_ADDR}) "
                          f"FC=0x{fc:02X} | RAW: {hex_str(frame)}")
        return None, None

    # ── FC 0x03: Read Holding Registers ──────────────────────────
    if fc == 0x03 and len(frame) == 8:
        start_addr = struct.unpack('>H', frame[2:4])[0]
        count = struct.unpack('>H', frame[4:6])[0]

        # 安全檢查：最多讀 125 個暫存器（Modbus 規範）
        if count < 1 or count > 125:
            stats['errors'] += 1
            resp = build_exception_response(SLAVE_ADDR, fc, 3)  # ILLEGAL DATA VALUE
            return resp, (f"[{ts}] FC=0x03 讀取數量超出範圍: {count}")

        # 讀取暫存器值
        values = reg_map.read(start_addr, count)
        resp = build_fc03_response(SLAVE_ADDR, values)
        stats['reads'] += 1

        # 每 100 次只印一次，避免刷屏（因為觸控屏每秒輪詢 3.5 次）
        if stats['reads'] % 100 == 1 or verbose:
            log = (f"[{ts}] FC=0x03 讀取: 位址={start_addr} (0x{start_addr:04X}), "
                   f"數量={count}, 回應={len(resp)} bytes "
                   f"[累計讀取 #{stats['reads']}]")
        else:
            log = None

        return resp, log

    # ── FC 0x06: Write Single Register ───────────────────────────
    elif fc == 0x06 and len(frame) == 8:
        reg_addr = struct.unpack('>H', frame[2:4])[0]
        reg_value = struct.unpack('>H', frame[4:6])[0]

        # 儲存到暫存器地圖
        reg_map.write(reg_addr, reg_value)
        stats['writes'] += 1

        # Echo back（標準 Modbus 行為）
        resp = build_fc06_response(SLAVE_ADDR, reg_addr, reg_value)

        reg_name = get_register_name(reg_addr)
        name_str = f" ({reg_name})" if reg_name else ""
        log = (f"[{ts}] FC=0x06 寫入: 位址={reg_addr} (0x{reg_addr:04X}){name_str}, "
               f"值={reg_value} (0x{reg_value:04X})")
        # 寫入命令較少見，每次都印
        print(f"  >>> 觸控屏寫入指令: REG[{reg_addr}] = {reg_value}{name_str}")

        return resp, log

    # ── FC 0x10: Write Multiple Registers ────────────────────────
    elif fc == 0x10 and len(frame) >= 9:
        start_addr = struct.unpack('>H', frame[2:4])[0]
        reg_count = struct.unpack('>H', frame[4:6])[0]
        byte_count = frame[6]
        expected_len = 7 + byte_count + 2  # header + data + CRC

        if len(frame) != expected_len:
            stats['errors'] += 1
            resp = build_exception_response(SLAVE_ADDR, fc, 3)
            return resp, (f"[{ts}] FC=0x10 訊框長度不符: "
                          f"預期={expected_len}, 實際={len(frame)}")

        if reg_count < 1 or reg_count > 123:
            stats['errors'] += 1
            resp = build_exception_response(SLAVE_ADDR, fc, 3)
            return resp, (f"[{ts}] FC=0x10 寫入數量超出範圍: {reg_count}")

        # 解析寫入的值
        values = []
        for i in range(reg_count):
            val = struct.unpack('>H', frame[7 + i*2 : 9 + i*2])[0]
            values.append(val)

        # 儲存到暫存器地圖
        reg_map.write_multiple(start_addr, values)
        stats['writes'] += 1

        # 建構回應
        resp = build_fc10_response(SLAVE_ADDR, start_addr, reg_count)

        values_str = ', '.join(f'{v}' for v in values)
        log = (f"[{ts}] FC=0x10 批次寫入: 起始位址={start_addr} (0x{start_addr:04X}), "
               f"數量={reg_count}, 值=[{values_str}]")
        print(f"  >>> 觸控屏批次寫入: REG[{start_addr}..{start_addr+reg_count-1}] = [{values_str}]")

        return resp, log

    # ── 不支援的功能碼 ────────────────────────────────────────────
    else:
        stats['errors'] += 1
        resp = build_exception_response(SLAVE_ADDR, fc, 1)  # ILLEGAL FUNCTION
        return resp, (f"[{ts}] 不支援的 FC=0x{fc:02X}, 長度={len(frame)} | "
                      f"RAW: {hex_str(frame)}")


# ─── 狀態模擬 ─────────────────────────────────────────────────────────

SIMULATION_STEPS = [
    # (延遲秒數, 要修改的暫存器, 說明)
    (10, {260: 1, 261: 0, 262: 0, 300: 0, 310: 0},
     "待機狀態: machine=standby, door=idle"),
    (10, {310: 30, 300: 1},
     "投幣完成: coins=30, program=1"),
    (10, {260: 3, 261: 3, 270: 25, 271: 0, 280: 40, 290: 800, 320: 15},
     "運轉中: machine=running, door=locked, 25min, 40C, 800RPM, water=15cm"),
    (10, {270: 20, 271: 30, 280: 55, 290: 1200, 320: 20},
     "運轉中: 20:30 remaining, 55C, 1200RPM, water=20cm"),
    (10, {270: 10, 271: 0, 280: 60, 290: 400, 320: 10},
     "脫水階段: 10:00 remaining, 60C, 400RPM, water=10cm"),
    (10, {270: 0, 271: 0, 260: 1, 261: 0, 280: 25, 290: 0, 320: 0, 310: 0, 300: 0},
     "完成回到待機: machine=standby, 0RPM, 25C"),
]


# ─── 主程式 ───────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Modbus RTU Slave 模擬器 — 回應觸控屏輪詢")
    parser.add_argument('--simulate', action='store_true',
                        help='每 10 秒自動變更暫存器值模擬狀態變化')
    parser.add_argument('--verbose', action='store_true',
                        help='顯示所有讀取請求（預設每 100 次印一次）')
    parser.add_argument('--set', nargs=2, action='append', metavar=('ADDR', 'VALUE'),
                        help='啟動時設定暫存器值, 例如: --set 260 3 --set 280 50')
    args = parser.parse_args()

    print("=" * 65)
    print("  Modbus RTU Slave 模擬器 — 模擬背板 PLC")
    print("=" * 65)
    print(f"  串口:       {SERIAL_PORT}")
    print(f"  波特率:     {SERIAL_BAUD}")
    print(f"  設定:       8N1 (no parity)")
    print(f"  從站位址:   {SLAVE_ADDR}")
    print(f"  訊框間隔:   {FRAME_GAP_MS:.2f} ms (3.5 字元時間)")
    print(f"  回應延遲:   {TURNAROUND_DELAY*1000:.2f} ms (turnaround)")
    print(f"  狀態模擬:   {'開啟' if args.simulate else '關閉'}")
    print(f"  詳細模式:   {'開啟' if args.verbose else '關閉 (每 100 次讀取印一次)'}")
    print(f"  按 Ctrl+C 停止")
    print("=" * 65)
    print()

    # 初始化暫存器地圖
    reg_map = RegisterMap()

    # 處理命令列設定的暫存器值
    if args.set:
        print("  自訂暫存器初始值:")
        for addr_str, val_str in args.set:
            addr = int(addr_str)
            val = int(val_str)
            reg_map.write(addr, val)
            name = get_register_name(addr)
            name_str = f" ({name})" if name else ""
            print(f"    REG[{addr}]{name_str} = {val} (0x{val:04X})")
        print()

    # 印出初始暫存器狀態（僅非零值）
    print("  初始暫存器狀態 (非零值):")
    dump = reg_map.dump(252, 97)
    if dump:
        print(dump)
    else:
        print("    (全部為 0)")
    print()

    # 開啟串口
    try:
        ser = serial.Serial(
            port=SERIAL_PORT,
            baudrate=SERIAL_BAUD,
            bytesize=8,
            parity='N',
            stopbits=1,
            timeout=0,  # 非阻塞模式
        )
    except Exception as e:
        sys.exit(f"無法開啟串口 {SERIAL_PORT}: {e}")

    print(f"  串口已開啟，開始監聽並回應...\n")

    # 統計資料
    stats = {
        'reads': 0,        # FC 0x03 讀取次數
        'writes': 0,       # FC 0x06/0x10 寫入次數
        'errors': 0,       # 錯誤次數
        'crc_errors': 0,   # CRC 驗證失敗次數
        'ignored': 0,      # 忽略的訊框（非本站位址）
        'responses': 0,    # 已送出的回應次數
    }

    start_time = time.time()
    last_byte_time = 0
    frame_buffer = bytearray()
    last_stats_time = start_time       # 上次印統計的時間
    stats_interval = 10                 # 每 10 秒印一次統計

    # 狀態模擬相關
    simulate = args.simulate
    sim_step_index = 0
    sim_last_change = start_time

    try:
        while True:
            now = time.time()

            # ── 讀取串口資料 ──────────────────────────────────────
            available = ser.in_waiting
            if available > 0:
                data = ser.read(available)
                now = time.time()

                # 如果距離上次收到 byte 超過訊框間隔，且 buffer 有資料
                # 代表前一個訊框已結束，先處理它
                if frame_buffer and last_byte_time > 0:
                    gap = now - last_byte_time
                    if gap >= FRAME_GAP:
                        _handle_frame(
                            ser, bytes(frame_buffer), reg_map, stats,
                            args.verbose
                        )
                        frame_buffer = bytearray()

                frame_buffer.extend(data)
                last_byte_time = now

            else:
                # 沒有新資料，檢查 buffer 中的訊框是否已經完成
                if frame_buffer and last_byte_time > 0:
                    gap = now - last_byte_time
                    if gap >= FRAME_GAP:
                        _handle_frame(
                            ser, bytes(frame_buffer), reg_map, stats,
                            args.verbose
                        )
                        frame_buffer = bytearray()

                # 短暫休眠避免 CPU 空轉（必須遠小於訊框間隔）
                time.sleep(0.0003)

            # ── 定期印出統計 ──────────────────────────────────────
            if now - last_stats_time >= stats_interval:
                elapsed = now - start_time
                total = stats['reads'] + stats['writes']
                rate = total / elapsed if elapsed > 0 else 0
                print(f"\n  --- 統計 [T+{elapsed:.0f}s] ---")
                print(f"  讀取: {stats['reads']}  寫入: {stats['writes']}  "
                      f"回應: {stats['responses']}  速率: {rate:.1f}/s")
                print(f"  CRC錯誤: {stats['crc_errors']}  其他錯誤: {stats['errors']}  "
                      f"忽略: {stats['ignored']}")
                print()
                last_stats_time = now

            # ── 狀態模擬（可選）────────────────────────────────────
            if simulate and sim_step_index < len(SIMULATION_STEPS):
                delay, regs_to_set, description = SIMULATION_STEPS[sim_step_index]
                if now - sim_last_change >= delay:
                    print(f"\n  [模擬] 步驟 {sim_step_index + 1}/{len(SIMULATION_STEPS)}: {description}")
                    for addr, val in regs_to_set.items():
                        reg_map.write(addr, val)
                        name = get_register_name(addr)
                        name_str = f" ({name})" if name else ""
                        print(f"    REG[{addr}]{name_str} = {val}")
                    print()
                    sim_step_index += 1
                    sim_last_change = now

            # 模擬循環結束後重新開始
            if simulate and sim_step_index >= len(SIMULATION_STEPS):
                if now - sim_last_change >= 10:
                    print(f"\n  [模擬] 循環完成，重新開始...\n")
                    sim_step_index = 0
                    sim_last_change = now

    except KeyboardInterrupt:
        elapsed = time.time() - start_time
        print(f"\n\n  使用者中斷，已運行 {elapsed:.1f} 秒。")

    finally:
        ser.close()
        print(f"  串口已關閉。")

    # ─── 列印最終統計 ─────────────────────────────────────────────
    _print_summary(stats, time.time() - start_time)


def _handle_frame(ser, frame, reg_map, stats, verbose):
    """
    處理收到的一個完整訊框：解析、產生回應、送出。
    """
    resp, log = process_frame(frame, reg_map, stats, verbose)

    # 印出 log（如果有的話）
    if log:
        print(f"  {log}")

    # 送出回應
    if resp:
        # Modbus RTU turnaround 延遲（至少 3.5 字元時間）
        time.sleep(TURNAROUND_DELAY)

        ser.write(resp)
        # 確保所有資料已送出（半雙工 RS485 需要等待傳輸完成再切回接收）
        ser.flush()
        # 額外等待資料完全離開硬體緩衝區
        # 計算回應的傳輸時間: bytes * 11 bits / baud
        tx_time = len(resp) * 11.0 / SERIAL_BAUD
        time.sleep(tx_time + 0.0005)

        stats['responses'] += 1

        if verbose and resp:
            print(f"    TX [{hex_str(resp)}] ({len(resp)} bytes)")


def _print_summary(stats, duration):
    """列印運行結束後的統計摘要"""
    print()
    print("=" * 65)
    print("  Modbus Slave 模擬器 — 運行摘要")
    print("=" * 65)
    print(f"  總運行時間:       {duration:.1f} 秒")
    print(f"  讀取請求 (FC03):  {stats['reads']}")
    print(f"  寫入命令 (FC06/10): {stats['writes']}")
    print(f"  已送出回應:       {stats['responses']}")
    print(f"  CRC 錯誤:         {stats['crc_errors']}")
    print(f"  其他錯誤:         {stats['errors']}")
    print(f"  忽略 (非本站):    {stats['ignored']}")

    total = stats['reads'] + stats['writes']
    if total > 0 and duration > 0:
        rate = total / duration
        print(f"  平均請求速率:     {rate:.1f} 次/秒")

    if stats['reads'] == 0 and stats['writes'] == 0:
        print()
        print("  未收到任何有效請求，請確認：")
        print(f"    1. RS485 A/B 接線正確（可嘗試交換 A/B）")
        print(f"    2. 波特率 {SERIAL_BAUD} 是否與觸控屏一致")
        print(f"    3. 觸控屏是否正在運作並送出輪詢指令")
        print(f"    4. 從站位址 {SLAVE_ADDR} 是否與觸控屏設定一致")

    print("=" * 65)


if __name__ == "__main__":
    main()
