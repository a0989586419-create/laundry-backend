#!/usr/bin/env python3
"""
SX176005A 觸控屏 — 洗程掃描腳本
==================================
掃描程序號 1-29，讀取每個程序的：
  - 是否有效（觸控屏是否接受）
  - 所需幣數（register 41）
  - 當前洗程序號（register 38）
  - 當前烘程序號（register 39）

同時讀取 P1-P30 價格參數（READ addr 440-469）

結果輸出到 program_scan_result.json

用法：
  python3 scan_programs.py [/dev/ttyUSB0] [slave_id]
"""

import sys
import os
import time
import struct
import json
import glob

try:
    import serial
except ImportError:
    sys.exit("[FATAL] pyserial not installed. Run: pip3 install pyserial")

SLAVE = int(sys.argv[2]) if len(sys.argv) > 2 else 1

# ---- Modbus helpers (raw serial, same as restore_touchscreen.py) ----

def crc16(data):
    crc = 0xFFFF
    for b in data:
        crc ^= b
        for _ in range(8):
            if crc & 1:
                crc = (crc >> 1) ^ 0xA001
            else:
                crc >>= 1
    return crc

def find_port():
    if len(sys.argv) > 1:
        p = sys.argv[1]
        if os.path.exists(p):
            return p
    for p in sorted(glob.glob('/dev/ttyUSB*')):
        return p
    for p in sorted(glob.glob('/dev/ttyS*')):
        if 'ttyS0' not in p:  # skip console
            return p
    return None

def read_registers(ser, slave, start_addr, count):
    """FC 0x03 — Read Holding Registers"""
    pdu = struct.pack('>BBHH', slave, 0x03, start_addr, count)
    frame = pdu + struct.pack('<H', crc16(pdu))
    ser.reset_input_buffer()
    ser.write(frame)
    time.sleep(0.15)
    resp = ser.read(5 + count * 2)
    if len(resp) < 5 + count * 2:
        return None
    if resp[0] != slave or resp[1] != 0x03:
        return None
    values = []
    for i in range(count):
        val = struct.unpack('>H', resp[3 + i * 2: 5 + i * 2])[0]
        values.append(val)
    return values

def write_register(ser, slave, addr, value):
    """FC 0x06 — Write Single Register"""
    pdu = struct.pack('>BBHH', slave, 0x06, addr, value)
    frame = pdu + struct.pack('<H', crc16(pdu))
    ser.reset_input_buffer()
    ser.write(frame)
    time.sleep(0.15)
    resp = ser.read(8)
    if len(resp) < 8:
        return False
    return resp[0] == slave and resp[1] == 0x06

# ---- Main scan logic ----

def main():
    port = find_port()
    if not port:
        sys.exit("[FATAL] No serial port found")

    print(f"=== SX176005A 洗程掃描 ===")
    print(f"Port: {port}, Slave: {SLAVE}")
    print()

    ser = serial.Serial(port, 9600, bytesize=8, parity='N', stopbits=1, timeout=1)
    time.sleep(0.5)

    # Step 1: Read current status (baseline)
    print("[1/4] 讀取當前狀態...")
    baseline = read_registers(ser, SLAVE, 20, 24)
    if not baseline:
        sys.exit("[FATAL] 無法讀取狀態暫存器 (addr 20-43)")

    machine_state = baseline[0]  # addr 20
    state_names = {0: "power_on", 1: "standby/idle", 3: "running", 4: "manual"}
    print(f"  機器狀態: {state_names.get(machine_state, f'unknown({machine_state})')}")
    print(f"  當前洗程: {baseline[18]}, 烘程: {baseline[19]}")  # addr 38, 39
    print(f"  所需幣數: {baseline[21]}, 已投幣: {baseline[22]}")  # addr 41, 42

    if machine_state == 3:
        sys.exit("[ABORT] 機器運轉中！請在待機狀態下掃描")

    # Step 2: Read P1-P30 prices (READ addr 440-469)
    print("\n[2/4] 讀取 P1-P30 價格...")
    prices = read_registers(ser, SLAVE, 440, 30)
    if prices:
        for i, p in enumerate(prices):
            if p > 0:
                print(f"  P{i+1} = {p} 幣")
    else:
        print("  [WARN] 無法讀取價格暫存器")
        prices = [0] * 30

    # Step 3: Scan wash programs 1-29
    print("\n[3/4] 掃描洗程序號 1-29...")
    wash_results = []

    for prog_num in range(1, 30):
        # Write wash program number
        ok = write_register(ser, SLAVE, 5, prog_num)
        if not ok:
            print(f"  程序 {prog_num:2d}: 寫入失敗")
            wash_results.append({"program": prog_num, "valid": False, "error": "write_failed"})
            continue

        time.sleep(0.2)  # give touchscreen time to process

        # Read back status
        status = read_registers(ser, SLAVE, 20, 24)
        if not status:
            print(f"  程序 {prog_num:2d}: 讀取失敗")
            wash_results.append({"program": prog_num, "valid": False, "error": "read_failed"})
            continue

        current_wash = status[18]   # addr 38
        current_dry = status[19]    # addr 39
        required_coins = status[21] # addr 41
        step_num = status[20]       # addr 40

        # Check if touchscreen accepted the program
        is_valid = (current_wash == prog_num) or (required_coins > 0)

        result = {
            "program": prog_num,
            "valid": is_valid,
            "wash_program_readback": current_wash,
            "dry_program_readback": current_dry,
            "required_coins": required_coins,
            "step": step_num,
            "price_nt": prices[prog_num - 1] * 10 if prog_num <= 30 else 0,
        }
        wash_results.append(result)

        marker = "✅" if is_valid else "—"
        print(f"  程序 {prog_num:2d}: {marker}  wash={current_wash}, dry={current_dry}, coins={required_coins}, P{prog_num}={prices[prog_num-1]}幣")

    # Step 4: Scan dry programs 1-4
    print("\n[4/4] 掃描烘程序號 1-4...")
    # Reset wash program first
    write_register(ser, SLAVE, 5, 0)
    time.sleep(0.2)

    dry_results = []
    dry_names = {1: "高溫烘衣", 2: "中溫烘衣", 3: "低溫烘衣", 4: "輕柔烘衣"}

    for dry_num in range(1, 5):
        ok = write_register(ser, SLAVE, 6, dry_num)
        if not ok:
            dry_results.append({"program": dry_num, "valid": False, "error": "write_failed"})
            continue

        time.sleep(0.2)
        status = read_registers(ser, SLAVE, 20, 24)
        if not status:
            dry_results.append({"program": dry_num, "valid": False, "error": "read_failed"})
            continue

        current_dry = status[19]
        required_coins = status[21]

        result = {
            "program": dry_num,
            "name": dry_names.get(dry_num, f"dry_{dry_num}"),
            "valid": True,
            "dry_program_readback": current_dry,
            "required_coins": required_coins,
        }
        dry_results.append(result)
        print(f"  烘程 {dry_num}: {dry_names.get(dry_num, '?')}  dry={current_dry}, coins={required_coins}")

    # Reset selections
    write_register(ser, SLAVE, 5, 0)
    write_register(ser, SLAVE, 6, 0)

    ser.close()

    # Build output
    output = {
        "scan_time": time.strftime("%Y-%m-%d %H:%M:%S"),
        "port": port,
        "slave_id": SLAVE,
        "prices_raw": {f"P{i+1}": p for i, p in enumerate(prices)},
        "wash_programs": [r for r in wash_results if r.get("valid")],
        "dry_programs": dry_results,
        "all_wash_scan": wash_results,
    }

    # Save result
    out_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "program_scan_result.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"\n{'='*50}")
    print(f"掃描完成！")
    print(f"有效洗程: {len([r for r in wash_results if r.get('valid')])} 個")
    print(f"有效烘程: {len([r for r in dry_results if r.get('valid')])} 個")
    print(f"結果儲存: {out_path}")
    print(f"{'='*50}")

    # Print suggested program_map
    valid_wash = [r for r in wash_results if r.get("valid")]
    if valid_wash:
        print("\n建議的 program_map（可貼到 config.json）:")
        print(json.dumps({
            f"wash_prog_{r['program']}": {
                "wash_program": r["program"],
                "dry_program": r.get("dry_program_readback", 1),
                "coins": r["required_coins"],
                "price_nt": r["required_coins"] * 10,
            }
            for r in valid_wash
        }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
