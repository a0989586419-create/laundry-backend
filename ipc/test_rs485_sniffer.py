#!/usr/bin/env python3
"""
RS485 匯流排監聽/嗅探工具 — 用於除錯觸控屏 (Modbus Master) 與 PLC 之間的通訊

使用場景：
  觸控屏作為 Modbus Master，持續輪詢 slave address 01 的背板 PLC。
  本工具被動監聽 RS485 匯流排上的所有訊框，解析並顯示通訊內容。

環境變數：
  SERIAL_PORT  — 串口路徑 (預設 /dev/ttyUSB0)
  SERIAL_BAUD  — 波特率   (預設 9600)
  SNIFF_DURATION — 監聽秒數 (預設 30)

用法：
  python3 test_rs485_sniffer.py
  SERIAL_PORT=/dev/ttyUSB1 SERIAL_BAUD=19200 SNIFF_DURATION=60 python3 test_rs485_sniffer.py
"""

import os
import sys
import time
import struct
from collections import OrderedDict
from datetime import datetime

try:
    import serial
except ImportError:
    sys.exit("請先安裝: pip3 install pyserial")

# ─── 設定 ─────────────────────────────────────────────────────────────
SERIAL_PORT = os.environ.get("SERIAL_PORT", "/dev/ttyUSB0")
SERIAL_BAUD = int(os.environ.get("SERIAL_BAUD", "9600"))
SNIFF_DURATION = int(os.environ.get("SNIFF_DURATION", "30"))

# 3.5 字元時間作為訊框分隔（Modbus RTU 規範）
# 每字元 = 11 bit (1 start + 8 data + 1 parity/none + 1 stop)
# 在 9600 baud: 1 字元 ≈ 1.146 ms，3.5 字元 ≈ 4.01 ms
CHAR_TIME = 11.0 / SERIAL_BAUD          # 單一字元傳輸時間（秒）
FRAME_GAP = CHAR_TIME * 3.5             # 訊框間隔（秒）
FRAME_GAP_MS = FRAME_GAP * 1000         # 轉換為毫秒

# 確保最小間隔不低於 2ms（避免極高波特率下的誤判）
if FRAME_GAP < 0.002:
    FRAME_GAP = 0.002
    FRAME_GAP_MS = 2.0


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


# ─── 工具函數 ─────────────────────────────────────────────────────────
def hex_str(data):
    """將 bytes 轉為十六進位字串"""
    return ' '.join(f'{b:02X}' for b in data)


def timestamp_str():
    """取得當前時間戳記字串"""
    return datetime.now().strftime('%H:%M:%S.%f')[:-3]


# ─── 訊框解析 ─────────────────────────────────────────────────────────

# 已知暫存器名稱對照表（可依實際情況擴充）
REGISTER_NAMES = {
    0:  "機器狀態",
    1:  "運轉模式",
    2:  "剩餘時間",
    3:  "錯誤碼",
    4:  "投幣金額",
    5:  "目標金額",
    10: "啟動命令",
    20: "程式編號",
    30: "溫度設定",
    40: "洗劑選擇",
}


def get_register_name(addr):
    """嘗試取得暫存器的中文名稱"""
    if addr in REGISTER_NAMES:
        return REGISTER_NAMES[addr]
    return f"REG_{addr}"


def parse_frame(frame):
    """
    嘗試將 bytes 解析為 Modbus RTU 訊框。
    回傳 (frame_type, description, pattern_key) 或 None
    frame_type: 'REQUEST', 'RESPONSE', 'UNKNOWN'
    """
    if len(frame) < 4:
        return None

    crc_ok, calc_crc, recv_crc = verify_crc(frame)
    slave = frame[0]
    fc = frame[1]

    # ── FC 0x03: Read Holding Registers ──────────────────────────
    if fc == 0x03:
        # 請求格式: [slave, 0x03, addr_hi, addr_lo, count_hi, count_lo, crc_lo, crc_hi] = 8 bytes
        if len(frame) == 8:
            start_addr = struct.unpack('>H', frame[2:4])[0]
            count = struct.unpack('>H', frame[4:6])[0]
            crc_status = "CRC OK" if crc_ok else f"CRC FAIL (算出 {calc_crc:04X}, 收到 {recv_crc:04X})"
            reg_name = get_register_name(start_addr)
            desc = (
                f"[REQUEST] FC=0x03 讀取保持暫存器\n"
                f"         Slave: {slave:02X} ({slave})\n"
                f"         起始位址: {start_addr} (0x{start_addr:04X}) — {reg_name}\n"
                f"         數量: {count} 個暫存器\n"
                f"         {crc_status}"
            )
            pattern_key = f"FC03_S{slave:02X}_R{start_addr:04X}_N{count}"
            return ('REQUEST', desc, pattern_key)

        # 回應格式: [slave, 0x03, byte_count, data..., crc_lo, crc_hi]
        elif len(frame) >= 5:
            byte_count = frame[2]
            expected_len = 3 + byte_count + 2
            if len(frame) == expected_len:
                data_bytes = frame[3:3 + byte_count]
                # 解析為 16-bit 暫存器值
                values = []
                for i in range(0, len(data_bytes), 2):
                    if i + 1 < len(data_bytes):
                        val = struct.unpack('>H', data_bytes[i:i+2])[0]
                        values.append(val)
                crc_status = "CRC OK" if crc_ok else f"CRC FAIL (算出 {calc_crc:04X}, 收到 {recv_crc:04X})"
                values_str = ', '.join(f'{v} (0x{v:04X})' for v in values)
                desc = (
                    f"[RESPONSE] FC=0x03 讀取保持暫存器回應\n"
                    f"         Slave: {slave:02X} ({slave})\n"
                    f"         資料長度: {byte_count} bytes ({len(values)} 個暫存器)\n"
                    f"         數值: [{values_str}]\n"
                    f"         {crc_status}"
                )
                pattern_key = f"FC03_RESP_S{slave:02X}_B{byte_count}"
                return ('RESPONSE', desc, pattern_key)

    # ── FC 0x06: Write Single Register ───────────────────────────
    elif fc == 0x06 and len(frame) == 8:
        reg_addr = struct.unpack('>H', frame[2:4])[0]
        reg_value = struct.unpack('>H', frame[4:6])[0]
        crc_status = "CRC OK" if crc_ok else f"CRC FAIL (算出 {calc_crc:04X}, 收到 {recv_crc:04X})"
        reg_name = get_register_name(reg_addr)
        desc = (
            f"[REQUEST] FC=0x06 寫入單一暫存器\n"
            f"         Slave: {slave:02X} ({slave})\n"
            f"         暫存器位址: {reg_addr} (0x{reg_addr:04X}) — {reg_name}\n"
            f"         寫入值: {reg_value} (0x{reg_value:04X})\n"
            f"         {crc_status}"
        )
        pattern_key = f"FC06_S{slave:02X}_R{reg_addr:04X}"
        return ('REQUEST', desc, pattern_key)

    # ── FC 0x10: Write Multiple Registers ────────────────────────
    elif fc == 0x10:
        # 請求格式: [slave, 0x10, addr_hi, addr_lo, count_hi, count_lo, byte_count, data..., crc_lo, crc_hi]
        if len(frame) >= 9:
            start_addr = struct.unpack('>H', frame[2:4])[0]
            reg_count = struct.unpack('>H', frame[4:6])[0]
            byte_count = frame[6]
            expected_len = 7 + byte_count + 2
            if len(frame) == expected_len:
                data_bytes = frame[7:7 + byte_count]
                values = []
                for i in range(0, len(data_bytes), 2):
                    if i + 1 < len(data_bytes):
                        val = struct.unpack('>H', data_bytes[i:i+2])[0]
                        values.append(val)
                crc_status = "CRC OK" if crc_ok else f"CRC FAIL (算出 {calc_crc:04X}, 收到 {recv_crc:04X})"
                reg_name = get_register_name(start_addr)
                values_str = ', '.join(f'{v} (0x{v:04X})' for v in values)
                desc = (
                    f"[REQUEST] FC=0x10 寫入多個暫存器\n"
                    f"         Slave: {slave:02X} ({slave})\n"
                    f"         起始位址: {start_addr} (0x{start_addr:04X}) — {reg_name}\n"
                    f"         數量: {reg_count} 個暫存器\n"
                    f"         寫入值: [{values_str}]\n"
                    f"         {crc_status}"
                )
                pattern_key = f"FC10_S{slave:02X}_R{start_addr:04X}_N{reg_count}"
                return ('REQUEST', desc, pattern_key)

            # FC 0x10 回應: [slave, 0x10, addr_hi, addr_lo, count_hi, count_lo, crc_lo, crc_hi] = 8 bytes
            elif len(frame) == 8:
                crc_status = "CRC OK" if crc_ok else f"CRC FAIL (算出 {calc_crc:04X}, 收到 {recv_crc:04X})"
                desc = (
                    f"[RESPONSE] FC=0x10 寫入多個暫存器回應\n"
                    f"         Slave: {slave:02X} ({slave})\n"
                    f"         起始位址: {start_addr} (0x{start_addr:04X})\n"
                    f"         數量: {reg_count}\n"
                    f"         {crc_status}"
                )
                pattern_key = f"FC10_RESP_S{slave:02X}_R{start_addr:04X}"
                return ('RESPONSE', desc, pattern_key)

    # ── 例外回應 (FC | 0x80) ─────────────────────────────────────
    elif fc & 0x80 and len(frame) == 5:
        original_fc = fc & 0x7F
        exception_code = frame[2]
        exception_names = {
            1: "ILLEGAL FUNCTION（不支援的功能碼）",
            2: "ILLEGAL DATA ADDRESS（無效的暫存器位址）",
            3: "ILLEGAL DATA VALUE（無效的資料值）",
            4: "SLAVE DEVICE FAILURE（從站裝置故障）",
            5: "ACKNOWLEDGE（確認，處理中）",
            6: "SLAVE DEVICE BUSY（從站忙碌）",
        }
        exc_name = exception_names.get(exception_code, f"UNKNOWN ({exception_code})")
        crc_status = "CRC OK" if crc_ok else f"CRC FAIL"
        desc = (
            f"[EXCEPTION] 例外回應\n"
            f"         Slave: {slave:02X} ({slave})\n"
            f"         原始 FC: 0x{original_fc:02X}\n"
            f"         例外碼: {exc_name}\n"
            f"         {crc_status}"
        )
        pattern_key = f"EXC_S{slave:02X}_FC{original_fc:02X}_E{exception_code}"
        return ('EXCEPTION', desc, pattern_key)

    # ── 無法辨識 ─────────────────────────────────────────────────
    crc_status = "CRC OK" if crc_ok else "CRC FAIL"
    desc = (
        f"[UNKNOWN] 無法辨識的訊框\n"
        f"         首位元組: 0x{frame[0]:02X}, FC?: 0x{frame[1]:02X}\n"
        f"         長度: {len(frame)} bytes\n"
        f"         {crc_status}"
    )
    pattern_key = f"UNK_{len(frame)}B_0x{frame[0]:02X}{frame[1]:02X}"
    return ('UNKNOWN', desc, pattern_key)


# ─── 主程式 ───────────────────────────────────────────────────────────
def main():
    print("=" * 65)
    print("  RS485 匯流排監聽工具 — 觸控屏 Modbus Master 嗅探器")
    print("=" * 65)
    print(f"  串口:     {SERIAL_PORT}")
    print(f"  波特率:   {SERIAL_BAUD}")
    print(f"  設定:     8N1")
    print(f"  訊框間隔: {FRAME_GAP_MS:.2f} ms (3.5 字元時間)")
    print(f"  監聽時間: {SNIFF_DURATION} 秒")
    print(f"  按 Ctrl+C 可提前結束")
    print("=" * 65)
    print()

    # 開啟串口
    try:
        ser = serial.Serial(
            port=SERIAL_PORT,
            baudrate=SERIAL_BAUD,
            bytesize=8,
            parity='N',
            stopbits=1,
            timeout=0,  # 非阻塞模式，用於持續監聽
        )
    except Exception as e:
        sys.exit(f"無法開啟串口 {SERIAL_PORT}: {e}")

    print(f"  串口已開啟，開始監聽...\n")

    # 統計資料
    frame_count = 0                    # 總訊框數
    crc_ok_count = 0                   # CRC 正確的訊框數
    crc_fail_count = 0                 # CRC 錯誤的訊框數
    pattern_stats = OrderedDict()      # 每種 pattern 的出現次數與時間
    start_time = time.time()
    last_byte_time = 0                 # 上一次收到 byte 的時間
    frame_buffer = bytearray()         # 當前訊框的 buffer

    try:
        while True:
            now = time.time()
            elapsed = now - start_time

            # 檢查是否超過監聽時間
            if elapsed >= SNIFF_DURATION:
                print(f"\n  監聽時間 {SNIFF_DURATION} 秒已到，停止監聽。")
                break

            # 讀取可用的 bytes
            available = ser.in_waiting
            if available > 0:
                data = ser.read(available)
                now = time.time()

                # 如果距離上次收到 byte 超過訊框間隔，且 buffer 有資料
                # 代表前一個訊框已結束，先處理它
                if frame_buffer and last_byte_time > 0:
                    gap = now - last_byte_time
                    if gap >= FRAME_GAP:
                        # 處理前一個訊框
                        _process_frame(
                            bytes(frame_buffer), frame_count,
                            elapsed, pattern_stats
                        )
                        frame_count += 1
                        crc_ok, _, _ = verify_crc(bytes(frame_buffer))
                        if crc_ok:
                            crc_ok_count += 1
                        else:
                            crc_fail_count += 1
                        frame_buffer = bytearray()

                frame_buffer.extend(data)
                last_byte_time = now

            else:
                # 沒有新資料，檢查 buffer 中的訊框是否已經完成（靜默間隔已過）
                if frame_buffer and last_byte_time > 0:
                    gap = now - last_byte_time
                    if gap >= FRAME_GAP:
                        _process_frame(
                            bytes(frame_buffer), frame_count,
                            elapsed, pattern_stats
                        )
                        frame_count += 1
                        crc_ok, _, _ = verify_crc(bytes(frame_buffer))
                        if crc_ok:
                            crc_ok_count += 1
                        else:
                            crc_fail_count += 1
                        frame_buffer = bytearray()

                # 短暫休眠避免 CPU 空轉（必須遠小於訊框間隔）
                time.sleep(0.0005)

    except KeyboardInterrupt:
        elapsed = time.time() - start_time
        print(f"\n\n  使用者中斷，已監聽 {elapsed:.1f} 秒。")

    finally:
        ser.close()
        print(f"  串口已關閉。\n")

    # ─── 列印摘要 ─────────────────────────────────────────────────
    _print_summary(frame_count, crc_ok_count, crc_fail_count, pattern_stats,
                   time.time() - start_time)


def _process_frame(frame, index, elapsed, pattern_stats):
    """解析並印出一個訊框"""
    ts = timestamp_str()
    raw = hex_str(frame)

    print(f"--- 訊框 #{index:04d} [{ts}] T+{elapsed:.3f}s  長度={len(frame)} ---")
    print(f"  RAW: {raw}")

    result = parse_frame(frame)
    if result:
        frame_type, description, pattern_key = result
        print(f"  {description}")

        # 更新統計
        if pattern_key not in pattern_stats:
            pattern_stats[pattern_key] = {
                'count': 0,
                'first_seen': elapsed,
                'last_seen': elapsed,
                'description': description.split('\n')[0],
            }
        pattern_stats[pattern_key]['count'] += 1
        pattern_stats[pattern_key]['last_seen'] = elapsed
    else:
        print(f"  [!] 無法解析（資料太短: {len(frame)} bytes）")

    print()


def _print_summary(total_frames, crc_ok, crc_fail, patterns, duration):
    """列印監聽結束後的統計摘要"""
    print()
    print("=" * 65)
    print("  監聽摘要")
    print("=" * 65)
    print(f"  總監聽時間:     {duration:.1f} 秒")
    print(f"  偵測到的訊框:   {total_frames}")
    print(f"  CRC 正確:       {crc_ok}")
    print(f"  CRC 錯誤:       {crc_fail}")

    if total_frames > 0 and duration > 0:
        fps = total_frames / duration
        print(f"  平均訊框速率:   {fps:.1f} frames/sec")

    if not patterns:
        print(f"\n  未偵測到任何有效訊框。")
        print(f"  請確認：")
        print(f"    1. RS485 A/B 接線正確（可嘗試交換 A/B）")
        print(f"    2. 波特率 {SERIAL_BAUD} 是否與觸控屏一致")
        print(f"    3. 觸控屏是否正在運作並送出輪詢指令")
        print("=" * 65)
        return

    print(f"\n  偵測到 {len(patterns)} 種不同的通訊模式：")
    print("-" * 65)
    print(f"  {'模式':<35s}  {'次數':>6s}  {'頻率':>10s}  {'首次':>8s}")
    print("-" * 65)

    for key, info in sorted(patterns.items(), key=lambda x: -x[1]['count']):
        count = info['count']
        first = info['first_seen']
        last = info['last_seen']
        desc_short = info['description']

        # 計算輪詢頻率
        if count > 1 and (last - first) > 0:
            interval = (last - first) / (count - 1)
            freq_str = f"{interval*1000:.0f}ms/次"
        else:
            freq_str = "—"

        print(f"  {key:<35s}  {count:>6d}  {freq_str:>10s}  T+{first:.1f}s")

    print("-" * 65)

    # 分析觸控屏輪詢的暫存器範圍
    print(f"\n  觸控屏輪詢分析：")
    read_patterns = [k for k in patterns if k.startswith("FC03_S")]
    write_patterns = [k for k in patterns if k.startswith("FC06_") or k.startswith("FC10_S")]

    if read_patterns:
        print(f"    讀取請求 (FC 0x03):")
        for key in sorted(read_patterns):
            # 從 pattern key 解析出 slave, register, count
            parts = key.split('_')
            slave_hex = parts[1][1:]   # e.g. "01"
            reg_hex = parts[2][1:]     # e.g. "0000"
            count_str = parts[3][1:] if len(parts) > 3 else "?"
            reg_int = int(reg_hex, 16)
            reg_name = get_register_name(reg_int)
            print(f"      Slave 0x{slave_hex} | "
                  f"暫存器 {reg_int} (0x{reg_hex}) — {reg_name} | "
                  f"數量 {count_str} | "
                  f"出現 {patterns[key]['count']} 次")

    if write_patterns:
        print(f"    寫入請求 (FC 0x06/0x10):")
        for key in sorted(write_patterns):
            print(f"      {key} | 出現 {patterns[key]['count']} 次")

    if not read_patterns and not write_patterns:
        print(f"    僅偵測到回應或無法辨識的訊框。")

    print()
    print("=" * 65)
    print("  提示：若要監聽更長時間，設定環境變數 SNIFF_DURATION=120")
    print("  提示：暫存器名稱對照表可在腳本中 REGISTER_NAMES 字典自訂")
    print("=" * 65)


if __name__ == "__main__":
    main()
