#!/usr/bin/env python3
"""
RS485 波特率自動掃描工具
測試不同 baud rate，找到觸控屏正確的通訊參數。
用法：python3 test_baud_scan.py
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
SLAVE_ADDR = int(os.environ.get("SLAVE", "1"))

BAUD_RATES = [9600, 19200, 38400, 57600, 115200, 4800, 2400]
PARITIES = [
    ("NONE", serial.PARITY_NONE),
    ("EVEN", serial.PARITY_EVEN),
    ("ODD", serial.PARITY_ODD),
]


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
    pdu = struct.pack('>BBHH', slave, 0x03, address, count)
    crc = crc16_modbus(pdu)
    return pdu + struct.pack('<H', crc)


def is_valid_modbus_response(resp, slave_addr):
    """檢查是否為合法的 Modbus RTU 回應"""
    if len(resp) < 5:
        return False, "太短"

    # 檢查 slave address
    if resp[0] != slave_addr:
        return False, f"slave 不符 (收到 0x{resp[0]:02X}, 預期 0x{slave_addr:02X})"

    fc = resp[1]

    # Exception response (FC | 0x80)
    if fc & 0x80:
        exc_code = resp[2] if len(resp) > 2 else 0
        exc_names = {1: 'ILLEGAL FUNCTION', 2: 'ILLEGAL DATA ADDRESS', 3: 'ILLEGAL DATA VALUE', 4: 'SLAVE DEVICE FAILURE'}
        return True, f"例外回應: {exc_names.get(exc_code, f'code {exc_code}')}"

    # Normal FC 0x03 response
    if fc == 0x03:
        byte_count = resp[2]
        expected_len = 3 + byte_count + 2  # header + data + CRC
        if len(resp) >= expected_len:
            # Verify CRC
            crc_received = struct.unpack('<H', resp[expected_len-2:expected_len])[0]
            crc_calc = crc16_modbus(resp[:expected_len-2])
            if crc_received == crc_calc:
                return True, f"FC 0x03 OK, {byte_count} bytes data, CRC 正確!"
            else:
                return True, f"FC 0x03, {byte_count} bytes, CRC 不符 (收到 {crc_received:04X}, 算出 {crc_calc:04X})"
        return False, f"FC 0x03 但長度不足 (預期 {expected_len}, 收到 {len(resp)})"

    return False, f"非預期 FC: 0x{fc:02X}"


def test_config(port, baud, parity_name, parity_val, slave):
    """測試一個特定的 baud + parity 組合"""
    try:
        ser = serial.Serial(
            port=port,
            baudrate=baud,
            bytesize=serial.EIGHTBITS,
            parity=parity_val,
            stopbits=serial.STOPBITS_ONE,
            timeout=0.5,
        )
    except Exception as e:
        return None, f"開啟失敗: {e}"

    # 清空緩衝區
    ser.reset_input_buffer()
    time.sleep(0.1)
    ser.reset_input_buffer()

    # 先讀一下看有沒有持續廣播的數據
    noise = ser.read(100)

    # 發送 Modbus 讀取請求 (Address 20, count 1)
    req = build_read_request(slave, 20, 1)
    ser.reset_input_buffer()
    ser.write(req)
    ser.flush()

    # 等待回應
    time.sleep(0.15)  # Modbus RTU 標準間隔

    response = b''
    start = time.time()
    while time.time() - start < 0.8:
        chunk = ser.read(ser.in_waiting or 1)
        if chunk:
            response += chunk
            if len(response) >= 7:  # FC 0x03, 1 register = 7 bytes
                break
            time.sleep(0.02)
        else:
            if response:
                break
            time.sleep(0.01)

    ser.close()

    if not response:
        return None, "無回應"

    # 限制顯示前 20 bytes
    display = hex_str(response[:20])
    if len(response) > 20:
        display += f" ... (共 {len(response)} bytes)"

    valid, detail = is_valid_modbus_response(response, slave)
    return valid, f"RX: {display} → {detail}"


def main():
    print(f"\n{'='*70}")
    print(f"  RS485 波特率自動掃描")
    print(f"  Port:  {SERIAL_PORT}")
    print(f"  Slave: {SLAVE_ADDR}")
    print(f"  測試:  FC 0x03, Address 20, Count 1")
    print(f"{'='*70}\n")

    found = []

    for baud in BAUD_RATES:
        for parity_name, parity_val in PARITIES:
            label = f"{baud:>6} bps, Parity={parity_name:<4}"
            sys.stdout.write(f"  測試 {label} ... ")
            sys.stdout.flush()

            valid, detail = test_config(SERIAL_PORT, baud, parity_name, parity_val, SLAVE_ADDR)

            if valid is True:
                print(f"✅ {detail}")
                found.append((baud, parity_name, detail))
            elif valid is None:
                print(f"— {detail}")
            else:
                print(f"❌ {detail}")

            time.sleep(0.2)  # 切換 baud rate 之間等一下

    print(f"\n{'='*70}")
    if found:
        print(f"  找到 {len(found)} 個可能的設定:")
        for baud, parity, detail in found:
            print(f"  ✅ {baud} bps, Parity={parity}")
            print(f"     {detail}")
        print(f"\n  建議使用第一個成功的設定。")
    else:
        print(f"  ❌ 所有組合都失敗！")
        print(f"")
        print(f"  可能原因:")
        print(f"  1. RS485 A/B 線接反 → 交換 A(+) 和 B(-) 再試")
        print(f"  2. 觸控屏 Modbus 未啟用 → 檢查觸控屏通訊設定")
        print(f"  3. Slave 地址不是 {SLAVE_ADDR} → 用 SLAVE=2 再試")
        print(f"  4. 接線問題 → 檢查接頭、確認 GND 有接")
    print(f"{'='*70}\n")


if __name__ == "__main__":
    main()
