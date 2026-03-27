#!/usr/bin/env python3
"""
SX176005A 觸控屏參數恢復腳本
=============================
恢復被意外覆蓋的工廠參數：
  - param 1312 = 1 (付款方式: 自助模式)
  - param 1320 = 10 (幣值: 10)
  - param 1325 = 3 (幣種: NT$)
  - param 1326 = 4 (程式數量: 4 個)
  - param 1340-1343 = 1 (P1-P4 價格: 各 1 幣 = NT$10)
  - param 1019 = 0 (手動運行: 關閉)

地址對照：
  params 1300-1399 → WRITE addr 1000-1099, READ addr 400-499
  offset = param_number - 1300

用法：
  python3 restore_touchscreen.py [/dev/ttyUSB0] [slave_id]
"""

import sys
import os
import time
import struct
import glob

try:
    import serial
except ImportError:
    sys.exit("[FATAL] pyserial not installed. Run: pip3 install pyserial")

SLAVE = int(sys.argv[2]) if len(sys.argv) > 2 else 1

# ---- Modbus helpers (raw serial) ----

def crc16(data):
    crc = 0xFFFF
    for b in data:
        crc ^= b
        for _ in range(8):
            if crc & 1: crc = (crc >> 1) ^ 0xA001
            else: crc >>= 1
    return crc

def find_port():
    if len(sys.argv) > 1:
        p = sys.argv[1]
        if os.path.exists(p):
            return p
    for p in sorted(glob.glob('/dev/ttyUSB*')):
        return p
    return None

def open_serial(port=None):
    if port is None:
        port = find_port()
    if port is None:
        print("  !! 找不到 USB 串口")
        return None, None
    try:
        ser = serial.Serial(port=port, baudrate=9600, bytesize=8, parity='N', stopbits=1, timeout=1)
        print(f"  >> 已連接 {port}")
        return ser, port
    except Exception as e:
        print(f"  !! 無法開啟 {port}: {e}")
        return None, None

def build_read(slave, addr, count):
    pdu = struct.pack('>BBHH', slave, 0x03, addr, count)
    return pdu + struct.pack('<H', crc16(pdu))

def build_write(slave, addr, value):
    pdu = struct.pack('>BBHH', slave, 0x06, addr, value)
    return pdu + struct.pack('<H', crc16(pdu))

def send(ser, req, timeout=1.5):
    try:
        ser.reset_input_buffer()
        ser.write(req)
        time.sleep(0.05)
        end = time.time() + timeout
        resp = b''
        while time.time() < end:
            if ser.in_waiting:
                resp += ser.read(ser.in_waiting)
                time.sleep(0.05)
            else:
                if resp: break
                time.sleep(0.02)
        return resp
    except (serial.SerialException, OSError, IOError) as e:
        raise ConnectionError(f"USB disconnect: {e}")

def write_reg(ser, slave, addr, val, name=""):
    req = build_write(slave, addr, val)
    resp = send(ser, req)
    ok = resp and len(resp) >= 8 and resp[1] == 0x06
    status = "OK" if ok else "FAIL"
    print(f"  寫入 addr={addr:4d} val={val:5d}  ({name}) -> {status}")
    return ok

def read_reg(ser, slave, addr, count=1):
    req = build_read(slave, addr, count)
    resp = send(ser, req)
    if not resp or len(resp) < 3 + count * 2 or resp[1] != 0x03:
        return None
    regs = []
    for i in range(count):
        val = struct.unpack('>H', resp[3 + i*2 : 5 + i*2])[0]
        regs.append(val)
    return regs

# ---- Restoration parameters ----
# Format: (write_addr, value, read_addr, param_name)

RESTORE_PARAMS = [
    # params 1000-1099 block: write 700+offset, read 100+offset
    (719, 0, 119, "param 1019: 手動運行=關閉"),

    # params 1300-1399 block: write 1000+offset, read 400+offset
    (1012, 1, 412, "param 1312: 付款方式=自助(1)"),
    (1020, 10, 420, "param 1320: 幣值=10"),
    (1022, 1, 422, "param 1322: 付款後自動啟動=是(1)"),
    (1025, 3, 425, "param 1325: 幣種=NT$(3)"),
    (1026, 4, 426, "param 1326: 程式數量=4"),

    # P1-P4 prices: params 1340-1343
    (1040, 1, 440, "param 1340: P1 價格=1幣(NT$10)"),
    (1041, 1, 441, "param 1341: P2 價格=1幣(NT$10)"),
    (1042, 1, 442, "param 1342: P3 價格=1幣(NT$10)"),
    (1043, 1, 443, "param 1343: P4 價格=1幣(NT$10)"),
]

SAVE_ADDR = 7  # Write addr 7 with value 1 to save factory params


def step1_read_current(ser):
    """Step 1: Read current values before restoring."""
    print("\n" + "=" * 60)
    print("  STEP 1: 讀取目前參數值")
    print("=" * 60)
    issues = []
    for write_addr, expected, read_addr, name in RESTORE_PARAMS:
        regs = read_reg(ser, SLAVE, read_addr, 1)
        if regs is None:
            print(f"  讀取 addr={read_addr:4d}       -> FAIL (無回應)  [{name}]")
            issues.append(name)
        else:
            val = regs[0]
            match = "OK" if val == expected else f"!! 需修正 (目前={val}, 應為={expected})"
            if val != expected:
                issues.append(name)
            print(f"  讀取 addr={read_addr:4d} = {val:5d} -> {match}  [{name}]")
        time.sleep(0.15)

    if not issues:
        print("\n  所有參數已正確，不需要恢復!")
        return False
    else:
        print(f"\n  發現 {len(issues)} 個參數需要修正")
        return True


def step2_write_params(ser):
    """Step 2: Write correct values."""
    print("\n" + "=" * 60)
    print("  STEP 2: 寫入正確參數值")
    print("=" * 60)
    fail_count = 0
    for write_addr, value, read_addr, name in RESTORE_PARAMS:
        ok = write_reg(ser, SLAVE, write_addr, value, name)
        if not ok:
            fail_count += 1
        time.sleep(0.15)

    if fail_count:
        print(f"\n  !! {fail_count} 個寫入失敗")
    else:
        print("\n  全部寫入成功!")
    return fail_count == 0


def step3_save(ser):
    """Step 3: Save parameters (write addr 7 = 1)."""
    print("\n" + "=" * 60)
    print("  STEP 3: 儲存參數 (addr 7 = 1)")
    print("=" * 60)
    ok = write_reg(ser, SLAVE, SAVE_ADDR, 1, "儲存參數")
    if ok:
        print("  儲存指令已發送，等待觸控屏處理...")
        time.sleep(2)
    return ok


def step4_verify(ser):
    """Step 4: Read back and verify."""
    print("\n" + "=" * 60)
    print("  STEP 4: 驗證恢復結果")
    print("=" * 60)
    all_ok = True
    for write_addr, expected, read_addr, name in RESTORE_PARAMS:
        regs = read_reg(ser, SLAVE, read_addr, 1)
        if regs is None:
            print(f"  驗證 addr={read_addr:4d}       -> FAIL (無回應)  [{name}]")
            all_ok = False
        else:
            val = regs[0]
            if val == expected:
                print(f"  驗證 addr={read_addr:4d} = {val:5d} -> OK  [{name}]")
            else:
                print(f"  驗證 addr={read_addr:4d} = {val:5d} -> !! 仍不正確 (應為 {expected})  [{name}]")
                all_ok = False
        time.sleep(0.15)

    return all_ok


def main():
    print("=" * 60)
    print("  SX176005A 觸控屏參數恢復工具")
    print(f"  Slave ID: {SLAVE}")
    print("=" * 60)

    ser, port = open_serial()
    if not ser:
        sys.exit(1)

    try:
        # Step 1: Check current state
        needs_fix = step1_read_current(ser)

        if not needs_fix:
            print("\n完成! 不需要恢復。")
            return

        # Ask user confirmation
        print("\n  即將寫入以上修正值並儲存。")
        ans = input("  繼續? (y/n): ").strip().lower()
        if ans != 'y':
            print("  已取消。")
            return

        # Step 2: Write
        if not step2_write_params(ser):
            print("\n  !! 寫入有錯誤，請檢查連線後重試")
            return

        # Step 3: Save
        if not step3_save(ser):
            print("\n  !! 儲存失敗")
            return

        # Step 4: Verify
        all_ok = step4_verify(ser)

        print("\n" + "=" * 60)
        if all_ok:
            print("  恢復成功! 請檢查觸控屏是否顯示:")
            print("    - 4 個程式選項")
            print("    - 每個程式 NT$10 (1 幣)")
            print("    - 自助模式 (無手動按鈕)")
            print("    - 如需要重開機讓設定生效")
        else:
            print("  !! 部分參數未正確恢復，可能需要重開機後再驗證")
        print("=" * 60)

    except ConnectionError as e:
        print(f"\n  !! USB 斷線: {e}")
        print("  請重新插拔 USB-RS485 後重新執行腳本")
    except KeyboardInterrupt:
        print("\n  已中斷")
    finally:
        try:
            ser.close()
        except:
            pass


if __name__ == "__main__":
    main()
