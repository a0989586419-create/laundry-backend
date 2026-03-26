#!/usr/bin/env python3
"""
Interactive coin simulation test for SX176005A touchscreen.
Step by step: read status → write coins → write start → read status.
"""

import sys
import time
import struct
import serial

PORT = sys.argv[1] if len(sys.argv) > 1 else "/dev/ttyUSB0"
SLAVE = int(sys.argv[2]) if len(sys.argv) > 2 else 1

def crc16(data):
    crc = 0xFFFF
    for b in data:
        crc ^= b
        for _ in range(8):
            if crc & 1: crc = (crc >> 1) ^ 0xA001
            else: crc >>= 1
    return crc

def build_read(slave, addr, count):
    pdu = struct.pack('>BBHH', slave, 0x03, addr, count)
    return pdu + struct.pack('<H', crc16(pdu))

def build_write(slave, addr, value):
    pdu = struct.pack('>BBHH', slave, 0x06, addr, value)
    return pdu + struct.pack('<H', crc16(pdu))

def send(ser, req, timeout=1.5):
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

def hex_str(d):
    return ' '.join(f'{b:02X}' for b in d)

def read_status(ser, slave):
    req = build_read(slave, 20, 24)
    resp = send(ser, req)
    if not resp or len(resp) < 51 or resp[1] != 0x03:
        print("  ❌ 讀取失敗")
        return None
    regs = [struct.unpack('>H', resp[3+i*2:5+i*2])[0] for i in range(24)]
    STATE = {0:"開機中", 1:"待機", 3:"運轉中", 4:"手動模式"}
    DOOR = {0:"待命", 1:"門開", 2:"門關", 3:"門鎖", 4:"門故障"}
    print(f"  機器狀態: {regs[0]} ({STATE.get(regs[0], '?')})")
    print(f"  門狀態:   {regs[1]} ({DOOR.get(regs[1], '?')})")
    print(f"  故障:     {regs[2]}")
    print(f"  剩餘時間: {regs[5]}h {regs[6]}m {regs[7]}s")
    print(f"  溫度:     {regs[10]}°C")
    print(f"  轉速:     {regs[12]} RPM")
    print(f"  洗衣程式: {regs[18]}")
    print(f"  烘乾程式: {regs[19]}")
    print(f"  當前步驟: {regs[20]}")
    print(f"  需要投幣: {regs[21]}")
    print(f"  已投幣數: {regs[22]}")
    return regs

def write_reg(ser, slave, addr, val, name=""):
    req = build_write(slave, addr, val)
    resp = send(ser, req)
    ok = resp and len(resp) >= 8 and resp[1] == 0x06
    status = "✅ OK" if ok else "❌ FAIL"
    print(f"  寫入 addr={addr} val={val} ({name}) → {status}")
    print(f"    TX [{hex_str(req)}]")
    print(f"    RX [{hex_str(resp)}]")
    return ok

# ---- Main ----
print(f"=== 投幣模擬測試 ===")
print(f"Port: {PORT}  Slave: {SLAVE}")
print()

ser = serial.Serial(port=PORT, baudrate=9600, bytesize=8, parity='N', stopbits=1, timeout=1)

while True:
    print("=" * 50)
    print("選擇操作:")
    print("  1 - 讀取機器狀態")
    print("  2 - 模擬投幣 (寫入 coins)")
    print("  3 - 選擇洗衣程式")
    print("  4 - 選擇烘乾程式")
    print("  5 - 啟動機器 (寫入 start=1)")
    print("  6 - 完整啟動流程 (程式→投幣→啟動)")
    print("  7 - 強制停止")
    print("  8 - 故障重置")
    print("  9 - 🔥 遠端啟動（自動模式切換，任何畫面都能啟動）")
    print("  0 - 離開")
    print("=" * 50)

    try:
        choice = input("輸入選項: ").strip()
    except (EOFError, KeyboardInterrupt):
        break

    if choice == "0":
        break

    elif choice == "1":
        print("\n📖 讀取狀態...")
        read_status(ser, SLAVE)

    elif choice == "2":
        try:
            coins = int(input("輸入投幣數量 (例: 4 = NT$40): ").strip())
        except (ValueError, EOFError):
            coins = 4
        print(f"\n💰 模擬投幣 {coins} 枚...")
        write_reg(ser, SLAVE, 4, coins, "coins")
        time.sleep(0.5)
        print("\n📖 讀取狀態確認...")
        read_status(ser, SLAVE)

    elif choice == "3":
        try:
            prog = int(input("洗衣程式 (1-29, 0=無): ").strip())
        except (ValueError, EOFError):
            prog = 1
        print(f"\n🧺 設定洗衣程式 {prog}...")
        write_reg(ser, SLAVE, 5, prog, "wash_prog")

    elif choice == "4":
        try:
            prog = int(input("烘乾程式 (1=高溫 2=中溫 3=低溫 4=柔風 0=無): ").strip())
        except (ValueError, EOFError):
            prog = 0
        print(f"\n🔥 設定烘乾程式 {prog}...")
        write_reg(ser, SLAVE, 6, prog, "dry_prog")

    elif choice == "5":
        print("\n▶️  啟動機器...")
        write_reg(ser, SLAVE, 1, 1, "start")
        time.sleep(1)
        print("\n📖 讀取狀態...")
        read_status(ser, SLAVE)

    elif choice == "6":
        try:
            wash = int(input("洗衣程式 (1-29): ").strip() or "1")
            dry = int(input("烘乾程式 (0-4, 0=不烘): ").strip() or "0")
            coins = int(input("投幣數量: ").strip() or "4")
        except (ValueError, EOFError):
            wash, dry, coins = 1, 0, 4

        print(f"\n🚀 完整啟動: 洗衣={wash} 烘乾={dry} 投幣={coins}")
        print()

        print("Step 1/4: 設定洗衣程式")
        write_reg(ser, SLAVE, 5, wash, "wash_prog")
        time.sleep(0.2)

        print("Step 2/4: 設定烘乾程式")
        write_reg(ser, SLAVE, 6, dry, "dry_prog")
        time.sleep(0.2)

        print("Step 3/4: 模擬投幣")
        write_reg(ser, SLAVE, 4, coins, "coins")
        time.sleep(0.2)

        print("Step 4/4: 啟動機器")
        write_reg(ser, SLAVE, 1, 1, "start")
        time.sleep(2)

        print("\n📖 啟動後狀態:")
        read_status(ser, SLAVE)

    elif choice == "7":
        print("\n⛔ 強制停止...")
        write_reg(ser, SLAVE, 3, 1, "force_stop")
        time.sleep(1)
        read_status(ser, SLAVE)

    elif choice == "8":
        print("\n🔄 故障重置...")
        write_reg(ser, SLAVE, 0, 1, "fault_reset")
        time.sleep(1)
        read_status(ser, SLAVE)

    elif choice == "9":
        try:
            wash = int(input("洗衣程式 (1-29): ").strip() or "1")
            dry = int(input("烘乾程式 (0-4, 0=不烘): ").strip() or "0")
        except (ValueError, EOFError):
            wash, dry = 1, 0

        print(f"\n🔥 遠端啟動（模式切換）: 洗衣={wash} 烘乾={dry}")
        print()

        # Step 1: 故障重置（清除錢盒門異常等）
        print("Step 1/7: 故障重置")
        write_reg(ser, SLAVE, 0, 1, "fault_reset")
        time.sleep(0.3)

        # Step 2: 切換為常規模式 (param 1009 → addr 709 = 1)
        print("Step 2/7: 切換為常規模式")
        write_reg(ser, SLAVE, 709, 1, "machine_type=regular")
        time.sleep(0.3)

        # Step 3: 儲存參數
        print("Step 3/7: 儲存參數")
        write_reg(ser, SLAVE, 7, 1, "save_params")
        time.sleep(1.5)  # 等待觸控屏重新載入

        # Step 4: 設定洗衣程式
        print("Step 4/7: 設定洗衣程式")
        write_reg(ser, SLAVE, 5, wash, "wash_prog")
        time.sleep(0.2)

        # Step 5: 設定烘乾程式
        print("Step 5/7: 設定烘乾程式")
        write_reg(ser, SLAVE, 6, dry, "dry_prog")
        time.sleep(0.2)

        # Step 6: 啟動機器
        print("Step 6/7: 啟動機器")
        write_reg(ser, SLAVE, 1, 1, "start")
        time.sleep(2)

        # 確認狀態
        print("\n📖 啟動後狀態:")
        regs = read_status(ser, SLAVE)
        running = regs and regs[0] == 3

        if running:
            print("\n✅ 機器已運轉！")
        else:
            print("\n⚠️  機器未運轉，狀態未變更")

        # Step 7: 切回自助模式
        print("\nStep 7/7: 切回自助模式")
        write_reg(ser, SLAVE, 709, 0, "machine_type=self_service")
        time.sleep(0.3)
        write_reg(ser, SLAVE, 7, 1, "save_params")
        time.sleep(1)
        print("✅ 已切回自助模式")

    print()

ser.close()
print("測試結束")
