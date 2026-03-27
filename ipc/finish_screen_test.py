#!/usr/bin/env python3
"""
測試：從「洗衣結束」畫面遠端啟動機器
嘗試所有可能的 Modbus 方法
"""
import serial, struct, time, sys

PORT = sys.argv[1] if len(sys.argv) > 1 else "/dev/ttyUSB0"

def crc16(d):
    c = 0xFFFF
    for b in d:
        c ^= b
        for _ in range(8):
            c = (c >> 1) ^ 0xA001 if c & 1 else c >> 1
    return c

def w06(ser, addr, val, name=""):
    pdu = struct.pack('>BBHH', 1, 0x06, addr, val)
    req = pdu + struct.pack('<H', crc16(pdu))
    ser.reset_input_buffer(); ser.write(req); time.sleep(0.1)
    r = b''; t = time.time() + 1.5
    while time.time() < t:
        if ser.in_waiting: r += ser.read(ser.in_waiting); time.sleep(0.05)
        elif r: break
        else: time.sleep(0.02)
    ok = r and len(r) >= 8 and r[1] == 0x06
    print(f"    {'OK' if ok else 'FAIL'} {name}")
    return ok

def w10(ser, addr, vals, name=""):
    cnt = len(vals); bc = cnt * 2
    pdu = struct.pack('>BBHHB', 1, 0x10, addr, cnt, bc)
    for v in vals: pdu += struct.pack('>H', v)
    req = pdu + struct.pack('<H', crc16(pdu))
    ser.reset_input_buffer(); ser.write(req); time.sleep(0.1)
    r = b''; t = time.time() + 1.5
    while time.time() < t:
        if ser.in_waiting: r += ser.read(ser.in_waiting); time.sleep(0.05)
        elif r: break
        else: time.sleep(0.02)
    ok = r and len(r) >= 8 and r[1] == 0x10
    print(f"    {'OK' if ok else 'FAIL'} {name}")
    return ok

def w16(ser, addr, and_mask, or_mask, name=""):
    """FC 0x16 (22) Mask Write Register"""
    pdu = struct.pack('>BBHHH', 1, 0x16, addr, and_mask, or_mask)
    req = pdu + struct.pack('<H', crc16(pdu))
    ser.reset_input_buffer(); ser.write(req); time.sleep(0.1)
    r = b''; t = time.time() + 1.5
    while time.time() < t:
        if ser.in_waiting: r += ser.read(ser.in_waiting); time.sleep(0.05)
        elif r: break
        else: time.sleep(0.02)
    ok = r and len(r) >= 8 and r[1] == 0x16
    fail_info = ""
    if r and len(r) >= 3 and r[1] == 0x96:  # error response
        fail_info = f" (exception code={r[2]})"
    print(f"    {'OK' if ok else 'FAIL'+fail_info} FC16 {name}")
    if r: print(f"      RX:[{' '.join(f'{b:02X}' for b in r)}]")
    return ok

def read_state(ser):
    pdu = struct.pack('>BBHH', 1, 0x03, 20, 24)
    req = pdu + struct.pack('<H', crc16(pdu))
    ser.reset_input_buffer(); ser.write(req); time.sleep(0.1)
    r = b''; t = time.time() + 1.5
    while time.time() < t:
        if ser.in_waiting: r += ser.read(ser.in_waiting); time.sleep(0.05)
        elif r: break
        else: time.sleep(0.02)
    if r and len(r) >= 51 and r[1] == 0x03:
        regs = [struct.unpack('>H', r[3+i*2:5+i*2])[0] for i in range(24)]
        states = {0: "power_on", 1: "standby", 3: "RUNNING", 4: "manual"}
        print(f"    state={regs[0]}({states.get(regs[0],'?')}) door={regs[1]} coins={regs[22]}")
        return regs[0]
    return -1

ser = serial.Serial(PORT, 9600, bytesize=8, parity='N', stopbits=1, timeout=1)

print("=" * 55)
print("  從「洗衣結束」畫面遠端啟動測試")
print("=" * 55)
print()
print("目前狀態:")
read_state(ser)
print()
input("確認觸控屏在「洗衣結束」畫面，按 Enter 開始...\n")

methods = [
    ("方法 1", "fault_reset=1", lambda: w06(ser, 0, 1, "fault_reset=1")),
    ("", "wait 1s + check screen", lambda: (time.sleep(1), read_state(ser))),

    ("方法 2", "fault_reset=0 then =1", lambda: (w06(ser, 0, 0, "reset=0"), time.sleep(0.2), w06(ser, 0, 1, "reset=1"))),
    ("", "wait 1s + check screen", lambda: (time.sleep(1), read_state(ser))),

    ("方法 3", "force_stop=1 then fault_reset=1", lambda: (w06(ser, 3, 1, "force_stop"), time.sleep(0.5), w06(ser, 0, 1, "fault_reset"))),
    ("", "wait 1s + check screen", lambda: (time.sleep(1), read_state(ser))),

    ("方法 4", "coins=3 + wash=1 + start=1 (from finish screen)", lambda: (w06(ser, 4, 3, "coins=3"), time.sleep(0.2), w06(ser, 5, 1, "wash=1"), time.sleep(0.2), w06(ser, 1, 1, "start"))),
    ("", "wait 2s + check", lambda: (time.sleep(2), read_state(ser))),

    ("方法 5", "FC10 batch addr 0-6 all at once", lambda: w10(ser, 0, [1, 1, 0, 0, 3, 1, 0], "reset+start+coins+wash")),
    ("", "wait 2s + check", lambda: (time.sleep(2), read_state(ser))),

    ("方法 6", "FC16 Mask Write start", lambda: w16(ser, 1, 0x0000, 0x0001, "start=1 via mask write")),
    ("", "wait 2s + check", lambda: (time.sleep(2), read_state(ser))),

    ("方法 7", "FC16 Mask Write fault_reset", lambda: w16(ser, 0, 0x0000, 0x0001, "fault_reset via mask write")),
    ("", "wait 1s + check", lambda: (time.sleep(1), read_state(ser))),

    ("方法 8", "write addr 0 with val=255 (full reset?)", lambda: w06(ser, 0, 255, "reset=255")),
    ("", "wait 2s + check", lambda: (time.sleep(2), read_state(ser))),

    ("方法 9", "write auto_return param + save (FC06 addr 720=1)", lambda: (w06(ser, 720, 1, "auto_return=1"), time.sleep(0.2), w06(ser, 7, 1, "save"))),
    ("", "wait 3s + check", lambda: (time.sleep(3), read_state(ser))),

    ("方法 10", "FC10 addr 720 auto_return=1 + save", lambda: (w10(ser, 720, [1], "auto_return=1 FC10"), time.sleep(0.2), w06(ser, 7, 1, "save"))),
    ("", "wait 3s + check", lambda: (time.sleep(3), read_state(ser))),
]

for label, desc, func in methods:
    if label:
        print(f"\n{'=' * 55}")
        print(f"  {label}: {desc}")
        print(f"{'=' * 55}")
    else:
        print(f"  >> {desc}")
    func()
    if label:
        ans = input("  觸控屏有變化嗎? (y=有/Enter=沒有): ").strip().lower()
        if ans == 'y':
            print(f"\n  !!! {label} 有效! 記錄下來 !!!")
            # Try to start if screen changed
            print("  嘗試啟動...")
            w06(ser, 4, 3, "coins=3")
            time.sleep(0.2)
            w06(ser, 5, 1, "wash=1")
            time.sleep(0.2)
            w06(ser, 1, 1, "start")
            time.sleep(2)
            s = read_state(ser)
            if s == 3:
                print("\n  RUNNING!")
            break

ser.close()
print()
print("=" * 55)
print("測試完成")
print("=" * 55)
