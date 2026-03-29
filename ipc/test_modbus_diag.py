#!/usr/bin/env python3
"""
Modbus RTU 診斷工具
測試 RS485 通訊，逐步讀取觸控屏暫存器。
用法：python3 test_modbus_diag.py
"""

import struct
import time
import sys
import os

try:
    import serial
except ImportError:
    sys.exit("請先安裝: pip3 install pyserial")

SERIAL_PORT = os.environ.get("SERIAL_PORT", "/dev/ttyUSB0")
SERIAL_BAUD = int(os.environ.get("SERIAL_BAUD", "9600"))
SLAVE_ADDR = int(os.environ.get("SLAVE", "1"))


def crc16_modbus(data: bytes) -> int:
    crc = 0xFFFF
    for byte in data:
        crc ^= byte
        for _ in range(8):
            if crc & 0x0001:
                crc = (crc >> 1) ^ 0xA001
            else:
                crc >>= 1
    return crc


def hex_str(data: bytes) -> str:
    return ' '.join(f'{b:02X}' for b in data)


def build_read_request(slave, address, count):
    """FC 0x03 Read Holding Registers"""
    pdu = struct.pack('>BBHH', slave, 0x03, address, count)
    crc = crc16_modbus(pdu)
    return pdu + struct.pack('<H', crc)


def build_write_request(slave, address, value):
    """FC 0x06 Write Single Register"""
    pdu = struct.pack('>BBHH', slave, 0x06, address, value)
    crc = crc16_modbus(pdu)
    return pdu + struct.pack('<H', crc)


def send_and_receive(ser, request, timeout=1.0):
    """Send request and read response."""
    ser.reset_input_buffer()
    ser.write(request)
    ser.flush()
    time.sleep(0.1)

    # Wait for response
    start = time.time()
    response = b''
    while time.time() - start < timeout:
        if ser.in_waiting > 0:
            response += ser.read(ser.in_waiting)
            time.sleep(0.05)  # Wait for more bytes
        else:
            if response:
                break
            time.sleep(0.01)

    return response


def parse_read_response(response, expected_count):
    """Parse FC 0x03 response."""
    if len(response) < 5:
        return None, f"回應太短 ({len(response)} bytes)"

    slave = response[0]
    fc = response[1]

    # Exception response
    if fc & 0x80:
        exc_code = response[2] if len(response) > 2 else 0
        exc_names = {1: 'ILLEGAL FUNCTION', 2: 'ILLEGAL DATA ADDRESS', 3: 'ILLEGAL DATA VALUE', 4: 'SLAVE DEVICE FAILURE'}
        return None, f"例外回應: {exc_names.get(exc_code, f'code {exc_code}')}"

    if fc != 0x03:
        return None, f"非預期的 function code: 0x{fc:02X}"

    byte_count = response[2]
    expected_bytes = expected_count * 2
    if byte_count != expected_bytes:
        return None, f"資料長度不符: 預期 {expected_bytes} bytes, 收到 {byte_count}"

    # Parse register values
    values = []
    for i in range(expected_count):
        offset = 3 + i * 2
        if offset + 2 <= len(response) - 2:  # -2 for CRC
            val = struct.unpack('>H', response[offset:offset+2])[0]
            values.append(val)

    return values, None


def main():
    print(f"\n{'='*60}")
    print(f"  Modbus RTU 診斷工具")
    print(f"  Serial: {SERIAL_PORT} @ {SERIAL_BAUD}")
    print(f"  Slave:  {SLAVE_ADDR}")
    print(f"{'='*60}\n")

    try:
        ser = serial.Serial(
            port=SERIAL_PORT,
            baudrate=SERIAL_BAUD,
            bytesize=serial.EIGHTBITS,
            parity=serial.PARITY_NONE,
            stopbits=serial.STOPBITS_ONE,
            timeout=1,
        )
        print(f"  串口已開啟: {SERIAL_PORT}\n")
    except Exception as e:
        print(f"  串口開啟失敗: {e}")
        sys.exit(1)

    # ─── Test 1: 基本連通測試（讀 1 個暫存器）───
    print(f"{'─'*60}")
    print(f"  Test 1: 讀取 Address 20 (機器狀態) × 1 個暫存器")
    print(f"{'─'*60}")
    req = build_read_request(SLAVE_ADDR, 20, 1)
    print(f"  TX: {hex_str(req)}")
    resp = send_and_receive(ser, req)
    print(f"  RX: {hex_str(resp)} ({len(resp)} bytes)")
    values, err = parse_read_response(resp, 1)
    if err:
        print(f"  結果: ❌ {err}")
    else:
        state_map = {0: "power_on", 1: "idle/standby", 3: "running", 4: "manual"}
        print(f"  結果: ✅ Address 20 = {values[0]} ({state_map.get(values[0], '未知')})")
    print()

    time.sleep(0.3)

    # ─── Test 2: 讀取門狀態 ───
    print(f"{'─'*60}")
    print(f"  Test 2: 讀取 Address 21 (門狀態) × 1 個暫存器")
    print(f"{'─'*60}")
    req = build_read_request(SLAVE_ADDR, 21, 1)
    print(f"  TX: {hex_str(req)}")
    resp = send_and_receive(ser, req)
    print(f"  RX: {hex_str(resp)} ({len(resp)} bytes)")
    values, err = parse_read_response(resp, 1)
    if err:
        print(f"  結果: ❌ {err}")
    else:
        door_map = {0: "idle", 1: "open", 2: "closed", 3: "locked", 4: "error"}
        print(f"  結果: ✅ Address 21 = {values[0]} ({door_map.get(values[0], '未知')})")
    print()

    time.sleep(0.3)

    # ─── Test 3: 讀 4 個暫存器 (20-23) ───
    print(f"{'─'*60}")
    print(f"  Test 3: 讀取 Address 20-23 × 4 個暫存器")
    print(f"{'─'*60}")
    req = build_read_request(SLAVE_ADDR, 20, 4)
    print(f"  TX: {hex_str(req)}")
    resp = send_and_receive(ser, req)
    print(f"  RX: {hex_str(resp)} ({len(resp)} bytes)")
    values, err = parse_read_response(resp, 4)
    if err:
        print(f"  結果: ❌ {err}")
    else:
        labels = ["機器狀態", "門狀態", "故障狀態", "步驟剩餘(分)"]
        for i, v in enumerate(values):
            print(f"  Address {20+i}: {v:5d}  ({labels[i]})")
    print()

    time.sleep(0.3)

    # ─── Test 4: 讀完整 24 個暫存器 (20-43) ───
    print(f"{'─'*60}")
    print(f"  Test 4: 讀取 Address 20-43 × 24 個暫存器 (完整)")
    print(f"{'─'*60}")
    req = build_read_request(SLAVE_ADDR, 20, 24)
    print(f"  TX: {hex_str(req)}")
    resp = send_and_receive(ser, req)
    print(f"  RX: {hex_str(resp)} ({len(resp)} bytes)")
    values, err = parse_read_response(resp, 24)
    if err:
        print(f"  結果: ❌ {err}")
        # 如果 24 個失敗，嘗試分批讀
        print(f"\n  → 嘗試分批讀取 (12+12)...")
        time.sleep(0.3)
        req1 = build_read_request(SLAVE_ADDR, 20, 12)
        resp1 = send_and_receive(ser, req1)
        values1, err1 = parse_read_response(resp1, 12)
        if err1:
            print(f"  Address 20-31: ❌ {err1}")
        else:
            print(f"  Address 20-31: ✅ {values1}")

        time.sleep(0.3)
        req2 = build_read_request(SLAVE_ADDR, 32, 12)
        resp2 = send_and_receive(ser, req2)
        values2, err2 = parse_read_response(resp2, 12)
        if err2:
            print(f"  Address 32-43: ❌ {err2}")
        else:
            print(f"  Address 32-43: ✅ {values2}")
    else:
        labels = {
            20: "機器狀態", 21: "門狀態", 22: "故障狀態",
            23: "步驟剩餘(分)", 24: "步驟剩餘(秒)",
            25: "總剩餘(時)", 26: "總剩餘(分)", 27: "總剩餘(秒)",
            28: "當前水位", 29: "設定水位",
            30: "當前溫度", 31: "設定溫度",
            32: "當前轉速", 33: "設定轉速",
            38: "洗衣程式", 39: "烘乾程式",
            40: "當前步驟", 41: "需要幣數", 42: "當前幣數",
        }
        print(f"  結果: ✅ 讀取成功！")
        for i, v in enumerate(values):
            addr = 20 + i
            label = labels.get(addr, f"暫存器{addr}")
            print(f"  [{addr:2d}] {label:12s} = {v}")
    print()

    time.sleep(0.3)

    # ─── Test 5: 讀取價格暫存器 (440-443) ───
    print(f"{'─'*60}")
    print(f"  Test 5: 讀取價格 Address 440-443 × 4 個暫存器")
    print(f"{'─'*60}")
    req = build_read_request(SLAVE_ADDR, 440, 4)
    print(f"  TX: {hex_str(req)}")
    resp = send_and_receive(ser, req)
    print(f"  RX: {hex_str(resp)} ({len(resp)} bytes)")
    values, err = parse_read_response(resp, 4)
    if err:
        print(f"  結果: ❌ {err}")
    else:
        for i, v in enumerate(values):
            print(f"  P{i+1} 價格: {v} 幣 (= NT${v * 10})")
    print()

    time.sleep(0.3)

    # ─── Test 6: 讀取設定暫存器 (412-414) ───
    print(f"{'─'*60}")
    print(f"  Test 6: 讀取設定 Address 412-414 × 3 個暫存器")
    print(f"{'─'*60}")
    req = build_read_request(SLAVE_ADDR, 412, 3)
    print(f"  TX: {hex_str(req)}")
    resp = send_and_receive(ser, req)
    print(f"  RX: {hex_str(resp)} ({len(resp)} bytes)")
    values, err = parse_read_response(resp, 3)
    if err:
        print(f"  結果: ❌ {err}")
    else:
        payment_modes = {0: "免費", 1: "投幣", 2: "卡片", 3: "投幣+卡片"}
        print(f"  [412] 付款模式: {values[0]} ({payment_modes.get(values[0], '未知')})")
        print(f"  [413] 幣值(NT$): {values[1]}")
        print(f"  [414] 程式數量:  {values[2]}")
    print()

    # ─── Summary ───
    print(f"{'='*60}")
    print(f"  診斷完成！")
    print(f"  如果 Test 1-2 成功但 Test 4 失敗，")
    print(f"  代表觸控屏不支援一次讀 24 個暫存器，")
    print(f"  需要分批讀取。")
    print(f"{'='*60}\n")

    ser.close()


if __name__ == "__main__":
    main()
