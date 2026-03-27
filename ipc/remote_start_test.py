#!/usr/bin/env python3
"""
自助模式遠端啟動測試 - 嘗試多種方式在自助模式下遠端啟動機器
"""
import serial, struct, time, sys

PORT = sys.argv[1] if len(sys.argv) > 1 else "/dev/ttyUSB0"
SLAVE = 1

def crc16(d):
    c = 0xFFFF
    for b in d:
        c ^= b
        for _ in range(8):
            c = (c >> 1) ^ 0xA001 if c & 1 else c >> 1
    return c

def write_single(ser, addr, val, name=""):
    """FC 0x06 Write Single Register"""
    pdu = struct.pack('>BBHH', SLAVE, 0x06, addr, val)
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
    print(f"    {'OK' if ok else 'FAIL'} addr={addr} val={val} ({name})")
    return ok

def write_multiple(ser, start_addr, values, name=""):
    """FC 0x10 Write Multiple Registers"""
    count = len(values)
    byte_count = count * 2
    # Build: slave + FC(0x10) + start_addr(2) + count(2) + byte_count(1) + data
    pdu = struct.pack('>BBHHB', SLAVE, 0x10, start_addr, count, byte_count)
    for v in values:
        pdu += struct.pack('>H', v)
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
    ok = r and len(r) >= 8 and r[1] == 0x10
    print(f"    {'OK' if ok else 'FAIL'} FC=0x10 addr={start_addr} values={values} ({name})")
    if not ok and r:
        print(f"    RX: [{' '.join(f'{b:02X}' for b in r)}]")
    return ok

def read_state(ser):
    pdu = struct.pack('>BBHH', SLAVE, 0x03, 20, 24)
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
    if r and len(r) >= 51 and r[1] == 0x03:
        regs = [struct.unpack('>H', r[3+i*2:5+i*2])[0] for i in range(24)]
        states = {0: "power_on", 1: "standby", 3: "RUNNING", 4: "manual"}
        print(f"    machine={regs[0]}({states.get(regs[0],'?')}) door={regs[1]} "
              f"wash_prog={regs[18]} dry_prog={regs[19]} "
              f"req_coins={regs[21]} cur_coins={regs[22]}")
        return regs
    print("    read failed")
    return None

def check_running(ser):
    time.sleep(2)
    regs = read_state(ser)
    if regs and regs[0] == 3:
        print("    >>> RUNNING! <<<")
        return True
    else:
        print("    >>> not running")
        return False

# ==== Main ====
print("=" * 55)
print("  自助模式遠端啟動測試")
print("  測試多種方法讓自助模式下的機器遠端啟動")
print("=" * 55)
print()

ser = serial.Serial(PORT, 9600, bytesize=8, parity='N', stopbits=1, timeout=1)

print("目前狀態:")
read_state(ser)
print()
input("確認觸控屏在自助模式，按 Enter 開始測試...")
print()

# ========================================
print("=" * 55)
print("方法 1: wash_prog -> coins(4) -> start")
print("  (之前測試過的方式)")
print("=" * 55)
write_single(ser, 5, 1, "wash_prog=1")
time.sleep(0.2)
write_single(ser, 4, 4, "coins=4")
time.sleep(0.2)
write_single(ser, 1, 1, "start")
if check_running(ser):
    print("\n方法 1 成功!")
    write_single(ser, 3, 1, "force_stop")
    time.sleep(2)
else:
    print()

input("按 Enter 繼續下一個方法...")
print()

# ========================================
print("=" * 55)
print("方法 2: coins(30) -> wash_prog -> start")
print("  (先投幣30元，再選程式)")
print("=" * 55)
write_single(ser, 4, 30, "coins=30")
time.sleep(0.5)
write_single(ser, 5, 1, "wash_prog=1")
time.sleep(0.2)
write_single(ser, 1, 1, "start")
if check_running(ser):
    print("\n方法 2 成功!")
    write_single(ser, 3, 1, "force_stop")
    time.sleep(2)
else:
    print()

input("按 Enter 繼續下一個方法...")
print()

# ========================================
print("=" * 55)
print("方法 3: 只投幣(30)，不發 start (看是否自動啟動)")
print("=" * 55)
write_single(ser, 5, 1, "wash_prog=1")
time.sleep(0.2)
write_single(ser, 4, 30, "coins=30")
print("  等待 5 秒看是否自動啟動...")
time.sleep(5)
regs = read_state(ser)
if regs and regs[0] == 3:
    print("    >>> AUTO START!")
    write_single(ser, 3, 1, "force_stop")
    time.sleep(2)
else:
    print("    >>> no auto start")
print()

input("按 Enter 繼續下一個方法...")
print()

# ========================================
print("=" * 55)
print("方法 4: FC 0x10 批次寫入 (addr 1-6 一次寫入)")
print("  start=1, skip=0, stop=0, coins=30, wash=1, dry=0")
print("=" * 55)
write_multiple(ser, 1, [1, 0, 0, 30, 1, 0], "batch: start+coins+prog")
if check_running(ser):
    print("\n方法 4 成功!")
    write_single(ser, 3, 1, "force_stop")
    time.sleep(2)
else:
    print()

input("按 Enter 繼續下一個方法...")
print()

# ========================================
print("=" * 55)
print("方法 5: FC 0x10 先設定(不start)，再單獨 start")
print("  addr 4-6: coins=30, wash=1, dry=0")
print("  然後 addr 1: start=1")
print("=" * 55)
write_multiple(ser, 4, [30, 1, 0], "batch: coins+prog")
time.sleep(0.5)
write_single(ser, 1, 1, "start")
if check_running(ser):
    print("\n方法 5 成功!")
    write_single(ser, 3, 1, "force_stop")
    time.sleep(2)
else:
    print()

print()

# ========================================
print("=" * 55)
print("方法 6: 故障重置 -> wash_prog -> coins(30) -> start")
print("=" * 55)
write_single(ser, 0, 1, "fault_reset")
time.sleep(0.5)
write_single(ser, 5, 1, "wash_prog=1")
time.sleep(0.2)
write_single(ser, 4, 30, "coins=30")
time.sleep(0.2)
write_single(ser, 1, 1, "start")
if check_running(ser):
    print("\n方法 6 成功!")
    write_single(ser, 3, 1, "force_stop")
    time.sleep(2)
else:
    print()

# Reset coins to 0
write_single(ser, 4, 0, "reset coins=0")

ser.close()
print()
print("=" * 55)
print("測試完成! 請回報哪個方法有效")
print("=" * 55)
