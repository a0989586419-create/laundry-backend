#!/usr/bin/env python3
"""
遠端啟動測試 - 測試從「洗衣結束」畫面能否遠端啟動機器
"""
import serial, struct, time

def crc16(d):
    c = 0xFFFF
    for b in d:
        c ^= b
        for _ in range(8):
            c = (c >> 1) ^ 0xA001 if c & 1 else c >> 1
    return c

def write_reg(ser, addr, val, name):
    pdu = struct.pack('>BBHH', 1, 0x06, addr, val)
    req = pdu + struct.pack('<H', crc16(pdu))
    ser.reset_input_buffer()
    ser.write(req)
    time.sleep(0.1)
    r = b''
    t = time.time() + 1.5
    while time.time() < t:
        if ser.in_waiting:
            r += ser.read(ser.in_waiting)
            time.sleep(0.05)
        elif r:
            break
        else:
            time.sleep(0.02)
    ok = r and len(r) >= 8 and r[1] == 0x06
    print(f"  {'OK' if ok else 'FAIL'} - {name}")
    return ok

def read_state(ser):
    pdu = struct.pack('>BBHH', 1, 0x03, 20, 1)
    req = pdu + struct.pack('<H', crc16(pdu))
    ser.reset_input_buffer()
    ser.write(req)
    time.sleep(0.1)
    r = b''
    t = time.time() + 1.5
    while time.time() < t:
        if ser.in_waiting:
            r += ser.read(ser.in_waiting)
            time.sleep(0.05)
        elif r:
            break
        else:
            time.sleep(0.02)
    if r and len(r) >= 5:
        s = struct.unpack('>H', r[3:5])[0]
        names = {0: "power_on", 1: "standby", 3: "running", 4: "manual"}
        return s, names.get(s, "unknown")
    return -1, "no_response"

# ---- 開始測試 ----
print("=" * 50)
print("  遠端啟動測試")
print("=" * 50)
print()

ser = serial.Serial('/dev/ttyUSB0', 9600, bytesize=8, parity='N', stopbits=1, timeout=1)

# 先讀取目前狀態
state, name = read_state(ser)
print(f"目前機器狀態: {state} ({name})")
print()

input(">>> 請確認觸控屏在「洗衣結束」畫面，然後按 Enter 繼續...")
print()

# ===== 測試 A: 強制停機 =====
print("=" * 50)
print("測試 A: 強制停機")
print("=" * 50)
write_reg(ser, 3, 1, "force_stop")
time.sleep(2)
state, name = read_state(ser)
print(f"  機器狀態: {state} ({name})")
input(">>> 觸控屏畫面有變化嗎？按 Enter 繼續...")
print()

# ===== 測試 B: 設定「自動返回待機」參數 =====
print("=" * 50)
print("測試 B: 設定自動返回待機 (參數1020=1)")
print("=" * 50)
write_reg(ser, 720, 1, "auto_return_standby=1")
write_reg(ser, 7, 1, "save_params")
print("  等待 3 秒讓觸控屏載入...")
time.sleep(3)
state, name = read_state(ser)
print(f"  機器狀態: {state} ({name})")
input(">>> 觸控屏畫面有變化嗎？按 Enter 繼續...")
print()

# ===== 測試 C: 直接啟動 =====
print("=" * 50)
print("測試 C: 設定洗衣程式 + 啟動")
print("=" * 50)
write_reg(ser, 5, 1, "wash_prog=1")
time.sleep(0.2)
write_reg(ser, 1, 1, "start")
print("  等待 2 秒...")
time.sleep(2)
state, name = read_state(ser)
print(f"  機器狀態: {state} ({name})")
if state == 3:
    print("  >>> 成功! 機器已運轉!")
else:
    print("  >>> 未運轉")
input(">>> 觸控屏畫面有變化嗎？按 Enter 繼續...")
print()

# ===== 測試 D: 切常規模式再啟動 =====
if state != 3:
    print("=" * 50)
    print("測試 D: 切換常規模式 + 啟動")
    print("=" * 50)
    write_reg(ser, 709, 1, "machine_type=regular")
    write_reg(ser, 7, 1, "save_params")
    print("  等待 3 秒讓觸控屏載入...")
    time.sleep(3)
    input(">>> 觸控屏畫面有變化嗎？按 Enter 繼續...")

    write_reg(ser, 5, 1, "wash_prog=1")
    time.sleep(0.2)
    write_reg(ser, 1, 1, "start")
    print("  等待 2 秒...")
    time.sleep(2)
    state, name = read_state(ser)
    print(f"  機器狀態: {state} ({name})")
    if state == 3:
        print("  >>> 成功! 機器已運轉!")
    else:
        print("  >>> 未運轉")
    input(">>> 按 Enter 繼續...")
    print()

    # 切回自助模式
    print("切回自助模式...")
    write_reg(ser, 709, 0, "machine_type=self_service")
    write_reg(ser, 7, 1, "save_params")
    time.sleep(1)

# 還原自動返回設定
print("還原設定 (自動返回待機=0)...")
write_reg(ser, 720, 0, "auto_return_standby=0")
write_reg(ser, 7, 1, "save_params")

ser.close()
print()
print("=" * 50)
print("測試完成!")
print("=" * 50)
