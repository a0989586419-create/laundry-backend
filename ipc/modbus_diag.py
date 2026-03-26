#!/usr/bin/env python3
"""
Modbus RS485 Diagnostic Tool for SX176005A Touchscreen
Uses raw serial to bypass pymodbus API compatibility issues.
"""

import sys
import time
import struct
import serial

SERIAL_PORT = sys.argv[1] if len(sys.argv) > 1 else "/dev/ttyUSB0"
BAUD_RATE = int(sys.argv[2]) if len(sys.argv) > 2 else 9600

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

def build_read_request(slave, addr, count):
    """Build Modbus RTU read holding registers (FC 0x03) request."""
    pdu = struct.pack('>BBHH', slave, 0x03, addr, count)
    c = crc16(pdu)
    return pdu + struct.pack('<H', c)

def build_write_request(slave, addr, value):
    """Build Modbus RTU write single register (FC 0x06) request."""
    pdu = struct.pack('>BBHH', slave, 0x06, addr, value)
    c = crc16(pdu)
    return pdu + struct.pack('<H', c)

def send_and_receive(ser, request, timeout=1.0):
    """Send request and read response."""
    ser.reset_input_buffer()
    ser.write(request)
    time.sleep(0.05)

    end_time = time.time() + timeout
    response = b''
    while time.time() < end_time:
        if ser.in_waiting > 0:
            response += ser.read(ser.in_waiting)
            time.sleep(0.05)
        else:
            time.sleep(0.02)
    return response

def hex_str(data):
    return ' '.join(f'{b:02X}' for b in data)

print(f"=== Modbus RS485 Raw Diagnostic ===")
print(f"Port: {SERIAL_PORT}  Baud: {BAUD_RATE}")
print(f"Settings: 8N1, timeout=1s")
print()

try:
    ser = serial.Serial(
        port=SERIAL_PORT,
        baudrate=BAUD_RATE,
        bytesize=8,
        parity='N',
        stopbits=1,
        timeout=1,
    )
except Exception as e:
    sys.exit(f"Cannot open {SERIAL_PORT}: {e}")

print(f"Serial port opened OK")
print()

# === Test 1: Scan slave addresses ===
print("=" * 55)
print("TEST 1: Scanning slave addresses 1-10")
print("Sending: FC 0x03, Read register 20, count=1")
print("=" * 55)

found_slaves = []
for slave_id in range(1, 11):
    req = build_read_request(slave_id, 20, 1)
    print(f"  Slave {slave_id}: TX [{hex_str(req)}]", end="  ")

    resp = send_and_receive(ser, req)

    if resp:
        print(f"RX [{hex_str(resp)}]", end="  ")
        if len(resp) >= 5 and resp[1] == 0x03:
            byte_count = resp[2]
            value = struct.unpack('>H', resp[3:5])[0]
            print(f"=> RESPONDED! Value={value}")
            found_slaves.append(slave_id)
        elif len(resp) >= 3 and resp[1] & 0x80:
            print(f"=> EXCEPTION code={resp[2]}")
        else:
            print(f"=> Unknown response")
    else:
        print("RX [no response - timeout]")

    time.sleep(0.3)

print()

# === Test 1b: If nothing found, try register 0 ===
if not found_slaves:
    print("No response at register 20. Trying register 0...")
    print()
    for slave_id in range(1, 11):
        req = build_read_request(slave_id, 0, 1)
        print(f"  Slave {slave_id}: TX [{hex_str(req)}]", end="  ")
        resp = send_and_receive(ser, req)
        if resp:
            print(f"RX [{hex_str(resp)}]", end="  ")
            if len(resp) >= 5 and resp[1] == 0x03:
                value = struct.unpack('>H', resp[3:5])[0]
                print(f"=> RESPONDED! Value={value}")
                found_slaves.append(slave_id)
            elif len(resp) >= 3 and resp[1] & 0x80:
                print(f"=> EXCEPTION code={resp[2]}")
            else:
                print(f"=> Unknown response")
        else:
            print("RX [no response]")
        time.sleep(0.3)

# === Test 2: Full status read ===
if found_slaves:
    slave = found_slaves[0]
    print()
    print("=" * 55)
    print(f"TEST 2: Full register read from slave {slave}")
    print(f"Reading 24 registers at address 20-43")
    print("=" * 55)

    req = build_read_request(slave, 20, 24)
    print(f"  TX [{hex_str(req)}]")
    resp = send_and_receive(ser, req, timeout=2.0)
    print(f"  RX [{hex_str(resp)}]")

    if resp and len(resp) >= 51 and resp[1] == 0x03:
        byte_count = resp[2]
        regs = []
        for i in range(24):
            val = struct.unpack('>H', resp[3 + i*2 : 5 + i*2])[0]
            regs.append(val)

        print(f"  Raw: {regs}")
        print()
        STATE = {0:"power_on", 1:"standby", 3:"running", 4:"manual"}
        DOOR = {0:"idle", 1:"open", 2:"closed", 3:"locked", 4:"error", 5:"locking", 6:"unlocking"}
        print(f"  [20] Machine state:    {regs[0]} ({STATE.get(regs[0], '?')})")
        print(f"  [21] Door state:       {regs[1]} ({DOOR.get(regs[1], '?')})")
        print(f"  [22] Fault:            {regs[2]} (bit0=fault, bit1=warn)")
        print(f"  [23] Step remain min:  {regs[3]}")
        print(f"  [24] Step remain sec:  {regs[4]}")
        print(f"  [25] Total remain hr:  {regs[5]}")
        print(f"  [26] Total remain min: {regs[6]}")
        print(f"  [27] Total remain sec: {regs[7]}")
        print(f"  [28] Water level:      {regs[8]} cm")
        print(f"  [30] Temperature:      {regs[10]} C")
        print(f"  [32] RPM:              {regs[12]}")
        print(f"  [38] Wash program:     {regs[18]}")
        print(f"  [39] Dry program:      {regs[19]}")
        print(f"  [40] Current step:     {regs[20]}")
        print(f"  [41] Required coins:   {regs[21]}")
        print(f"  [42] Current coins:    {regs[22]}")
    else:
        print("  Read failed or incomplete response")

    # === Test 3: Write test ===
    print()
    print("=" * 55)
    print(f"TEST 3: Write coins=0 to slave {slave} addr=4")
    print("=" * 55)

    req = build_write_request(slave, 4, 0)
    print(f"  TX [{hex_str(req)}]")
    resp = send_and_receive(ser, req)
    print(f"  RX [{hex_str(resp)}]")
    if resp and len(resp) >= 8 and resp[1] == 0x06:
        print("  => Write SUCCESS (echo confirmed)")
    elif resp:
        print("  => Write response unexpected")
    else:
        print("  => No response to write")

# === Summary ===
print()
print("=" * 55)
print("DIAGNOSTIC COMPLETE")
if found_slaves:
    print(f"Found working slaves: {found_slaves}")
else:
    print()
    print("NO SLAVES RESPONDED! Check:")
    print("1. RS485 A/B wiring - TRY SWAPPING A and B wires")
    print("2. Touchscreen must be POWERED ON")
    print("3. Try different baud rate:")
    print(f"   python3 modbus_diag.py {SERIAL_PORT} 19200")
    print(f"   python3 modbus_diag.py {SERIAL_PORT} 38400")
    print("4. Check touchscreen slave address (default may not be 1)")
    print("5. Check RS485 termination resistor (120 ohm)")
print("=" * 55)

ser.close()
