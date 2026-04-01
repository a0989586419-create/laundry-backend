#!/usr/bin/env python3
"""
暫存器探測工具 — 逐一改變暫存器值，觀察觸控屏反應
用法：python3 test_reg_probe.py
"""
import os,sys,time,struct,threading
try:
    import serial
except ImportError:
    sys.exit("pip3 install pyserial")

PORT=os.environ.get("SERIAL_PORT","/dev/ttyUSB0")
BAUD=int(os.environ.get("SERIAL_BAUD","9600"))
SLAVE=1
GAP=max(0.002,(11.0/BAUD)*3.5)

def crc16(d):
    c=0xFFFF
    for b in d:
        c^=b
        for _ in range(8): c=(c>>1)^0xA001 if c&1 else c>>1
    return c

# Register map
regs = {i: 0 for i in range(0, 500)}
reads = 0
writes = 0
write_log = []
running = True

def slave_loop(ser):
    """Background thread: respond to touchscreen polling"""
    global reads, writes, running
    buf=bytearray(); last_t=0
    while running:
        try:
            n=ser.in_waiting
            if n>0:
                d=ser.read(n); now=time.time()
                if buf and last_t and now-last_t>=GAP:
                    process_frame(ser, bytes(buf))
                    buf=bytearray()
                buf.extend(d); last_t=now
            else:
                if buf and last_t and time.time()-last_t>=GAP:
                    process_frame(ser, bytes(buf))
                    buf=bytearray()
                time.sleep(0.0005)
        except:
            pass

def process_frame(ser, frame):
    global reads, writes, write_log
    if len(frame)<8: return
    rc=struct.unpack('<H',frame[-2:])[0]
    cc=crc16(frame[:-2])
    if rc!=cc or frame[0]!=SLAVE: return

    fc=frame[1]
    if fc==0x03 and len(frame)==8:
        addr=struct.unpack('>H',frame[2:4])[0]
        cnt=struct.unpack('>H',frame[4:6])[0]
        reads+=1
        bc=cnt*2
        resp=bytearray([SLAVE,0x03,bc])
        for i in range(cnt):
            resp+=struct.pack('>H', regs.get(addr+i, 0))
        resp+=struct.pack('<H', crc16(resp))
        time.sleep(GAP)
        ser.write(resp); ser.flush()
    elif fc==0x06 and len(frame)==8:
        addr=struct.unpack('>H',frame[2:4])[0]
        val=struct.unpack('>H',frame[4:6])[0]
        regs[addr]=val
        writes+=1
        write_log.append((time.strftime('%H:%M:%S'), addr, val))
        print(f"\n  🔔 觸控屏寫入! addr={addr}(0x{addr:04X}) = {val} (0x{val:04X})")
        time.sleep(GAP)
        ser.write(frame); ser.flush()
    elif fc==0x10 and len(frame)>=9:
        addr=struct.unpack('>H',frame[2:4])[0]
        cnt=struct.unpack('>H',frame[4:6])[0]
        bc=frame[6]
        if len(frame)==7+bc+2:
            for i in range(cnt):
                v=struct.unpack('>H',frame[7+i*2:9+i*2])[0]
                regs[addr+i]=v
                write_log.append((time.strftime('%H:%M:%S'), addr+i, v))
                print(f"\n  🔔 觸控屏寫入! addr={addr+i}(0x{addr+i:04X}) = {v}")
            writes+=1
            resp=frame[:6]+struct.pack('<H',crc16(frame[:6]))
            time.sleep(GAP)
            ser.write(resp); ser.flush()

def main():
    global running

    print(f"{'='*60}")
    print(f"  暫存器探測工具")
    print(f"  Port: {PORT} @ {BAUD} | Slave: {SLAVE}")
    print(f"{'='*60}")
    print()
    print("  指令說明:")
    print("  s <addr> <val>    — 設定單一暫存器 (例: s 252 1)")
    print("  r <addr> [count]  — 讀取暫存器值 (例: r 252 10)")
    print("  fill <val>        — 全部 252-348 填入同一個值")
    print("  idle              — 模擬待機狀態 (預設值)")
    print("  run               — 模擬運轉狀態")
    print("  probe <addr>      — 自動探測: 將該暫存器從 0 變到 5，每次停 5 秒")
    print("  scan              — 逐一掃描 252-280，每個暫存器設 1 持續 3 秒")
    print("  log               — 顯示觸控屏寫入紀錄")
    print("  status            — 顯示通訊統計")
    print("  q                 — 離開")
    print()

    ser=serial.Serial(port=PORT,baudrate=BAUD,bytesize=8,parity='N',stopbits=1,timeout=0)

    # Start slave response thread
    t=threading.Thread(target=slave_loop, args=(ser,), daemon=True)
    t.start()

    print(f"  Slave 已啟動，等待觸控屏輪詢...\n")
    time.sleep(3)
    print(f"  已收到 {reads} 個輪詢請求 ✅\n")

    try:
        while True:
            try:
                cmd = input("  > ").strip()
            except EOFError:
                break

            if not cmd:
                continue

            parts = cmd.split()
            op = parts[0].lower()

            if op == 'q':
                break

            elif op == 's' and len(parts) >= 3:
                addr = int(parts[1])
                val = int(parts[2])
                regs[addr] = val
                print(f"  ✅ 設定 reg[{addr}] = {val} (0x{val:04X})")

            elif op == 'r':
                addr = int(parts[1]) if len(parts) > 1 else 252
                cnt = int(parts[2]) if len(parts) > 2 else 10
                print(f"  暫存器 {addr}-{addr+cnt-1}:")
                for i in range(cnt):
                    a = addr + i
                    v = regs.get(a, 0)
                    mark = " ◀" if v != 0 else ""
                    print(f"    [{a:3d}] = {v:5d} (0x{v:04X}){mark}")

            elif op == 'fill':
                val = int(parts[1]) if len(parts) > 1 else 0
                for i in range(252, 349):
                    regs[i] = val
                print(f"  ✅ 暫存器 252-348 全部填入 {val}")

            elif op == 'idle':
                for i in range(252, 349):
                    regs[i] = 0
                regs[252] = 1  # state = idle
                print(f"  ✅ 設定為待機狀態 (reg[252]=1, 其餘=0)")

            elif op == 'run':
                for i in range(252, 349):
                    regs[i] = 0
                regs[252] = 3  # state = running
                regs[253] = 2  # door = closed
                regs[254] = 0  # no fault
                regs[255] = 30 # 30 min remaining
                regs[256] = 0  # 0 sec
                print(f"  ✅ 設定為運轉狀態 (state=3, door=2, time=30min)")

            elif op == 'probe' and len(parts) >= 2:
                addr = int(parts[1])
                print(f"  開始探測 reg[{addr}]，值 0→5，每次 5 秒...")
                print(f"  ⚠️  請觀察觸控屏畫面變化！")
                for val in range(6):
                    regs[addr] = val
                    print(f"    reg[{addr}] = {val}  (等待 5 秒...)", end='', flush=True)
                    time.sleep(5)
                    print(f"  done")
                regs[addr] = 0
                print(f"  探測完成，已重置為 0")

            elif op == 'scan':
                start = int(parts[1]) if len(parts) > 1 else 252
                end = int(parts[2]) if len(parts) > 2 else 280
                print(f"  掃描 reg[{start}]-reg[{end}]，每個設為 1 持續 3 秒")
                print(f"  ⚠️  請觀察觸控屏畫面變化！")
                for addr in range(start, end+1):
                    regs[addr] = 1
                    print(f"    reg[{addr}] = 1 ...", end='', flush=True)
                    time.sleep(3)
                    # Check if touchscreen wrote anything
                    if write_log:
                        last = write_log[-1]
                        print(f"  (觸控屏寫入: addr={last[1]} val={last[2]})")
                    else:
                        print()
                    regs[addr] = 0
                print(f"  掃描完成")

            elif op == 'log':
                if write_log:
                    print(f"  觸控屏寫入紀錄 ({len(write_log)} 筆):")
                    for ts, addr, val in write_log[-20:]:
                        print(f"    [{ts}] addr={addr}(0x{addr:04X}) = {val}(0x{val:04X})")
                else:
                    print(f"  尚無觸控屏寫入紀錄")

            elif op == 'status':
                print(f"  reads={reads} writes={writes}")
                print(f"  觸控屏寫入紀錄: {len(write_log)} 筆")

            else:
                print(f"  ❓ 未知指令: {cmd}")
                print(f"     試試: s 252 1 / probe 252 / scan / idle / run / log / q")

    except KeyboardInterrupt:
        pass

    running = False
    ser.close()

    print(f"\n{'='*60}")
    print(f"  結束! reads={reads} writes={writes}")
    if write_log:
        print(f"\n  觸控屏寫入紀錄:")
        for ts, addr, val in write_log:
            print(f"    [{ts}] addr={addr}(0x{addr:04X}) = {val}(0x{val:04X})")
    print(f"{'='*60}")

if __name__ == "__main__":
    main()
