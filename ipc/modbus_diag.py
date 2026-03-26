#!/usr/bin/env python3
"""
Modbus RS485 Diagnostic Tool for SX176005A Touchscreen
Tests communication with different slave addresses and operations.
"""

import sys
import time

try:
    from pymodbus.client import ModbusSerialClient
except ImportError:
    sys.exit("pip3 install pymodbus")

SERIAL_PORT = sys.argv[1] if len(sys.argv) > 1 else "/dev/ttyUSB0"
BAUD_RATE = int(sys.argv[2]) if len(sys.argv) > 2 else 9600

print(f"=== Modbus RS485 Diagnostic ===")
print(f"Port: {SERIAL_PORT}  Baud: {BAUD_RATE}")
print(f"Settings: 8N1, timeout=2s")
print()

client = ModbusSerialClient(
    port=SERIAL_PORT,
    baudrate=BAUD_RATE,
    bytesize=8,
    parity='N',
    stopbits=1,
    timeout=2,
)

if not client.connect():
    sys.exit(f"Cannot open {SERIAL_PORT}")

print(f"Serial port opened: {SERIAL_PORT}")
print()

# --- Test 1: Scan slave addresses 1-10 ---
print("=" * 50)
print("TEST 1: Scanning slave addresses 1-10")
print("Reading register 20 (machine state) from each")
print("=" * 50)

found_slaves = []
for slave_id in range(1, 11):
    try:
        result = client.read_holding_registers(address=20, count=1, slave=slave_id)
        if not result.isError():
            print(f"  Slave {slave_id}: RESPONDED! Value = {result.registers[0]}")
            found_slaves.append(slave_id)
        else:
            print(f"  Slave {slave_id}: Error - {result}")
    except TypeError:
        try:
            result = client.read_holding_registers(address=20, count=1, unit=slave_id)
            if not result.isError():
                print(f"  Slave {slave_id}: RESPONDED! Value = {result.registers[0]}")
                found_slaves.append(slave_id)
            else:
                print(f"  Slave {slave_id}: Error - {result}")
        except Exception as e:
            print(f"  Slave {slave_id}: Exception - {e}")
    except Exception as e:
        print(f"  Slave {slave_id}: Exception - {e}")
    time.sleep(0.3)

print()
if found_slaves:
    print(f"Found slaves: {found_slaves}")
else:
    print("No slaves found with address 20 read!")
    print()
    print("Trying broader scan: reading register 0 instead...")
    for slave_id in range(1, 11):
        try:
            result = client.read_holding_registers(address=0, count=1, slave=slave_id)
            if not result.isError():
                print(f"  Slave {slave_id}: RESPONDED! Register 0 = {result.registers[0]}")
                found_slaves.append(slave_id)
            else:
                print(f"  Slave {slave_id}: {result}")
        except TypeError:
            try:
                result = client.read_holding_registers(address=0, count=1, unit=slave_id)
                if not result.isError():
                    print(f"  Slave {slave_id}: RESPONDED! Register 0 = {result.registers[0]}")
                    found_slaves.append(slave_id)
                else:
                    print(f"  Slave {slave_id}: {result}")
            except Exception as e:
                print(f"  Slave {slave_id}: {e}")
        except Exception as e:
            print(f"  Slave {slave_id}: {e}")
        time.sleep(0.3)

# --- Test 2: If found, read full status ---
if found_slaves:
    print()
    print("=" * 50)
    print(f"TEST 2: Full register read from slave {found_slaves[0]}")
    print("Reading 24 registers starting at address 20")
    print("=" * 50)

    slave = found_slaves[0]
    try:
        result = client.read_holding_registers(address=20, count=24, slave=slave)
    except TypeError:
        result = client.read_holding_registers(20, count=24, unit=slave)

    if not result.isError():
        regs = result.registers
        print(f"  Raw registers: {regs}")
        print()
        STATE_MAP = {0: "power_on", 1: "standby", 3: "auto_running", 4: "manual"}
        DOOR_MAP = {0: "idle", 1: "open", 2: "closed", 3: "locked", 4: "error"}
        print(f"  [20] Machine state: {regs[0]} ({STATE_MAP.get(regs[0], 'unknown')})")
        print(f"  [21] Door state:    {regs[1]} ({DOOR_MAP.get(regs[1], 'unknown')})")
        print(f"  [22] Fault state:   {regs[2]} (bit0=fault, bit1=warning)")
        print(f"  [23] Step remain min: {regs[3]}")
        print(f"  [24] Step remain sec: {regs[4]}")
        print(f"  [25] Total remain hr: {regs[5]}")
        print(f"  [26] Total remain min: {regs[6]}")
        print(f"  [27] Total remain sec: {regs[7]}")
        print(f"  [28] Current water level: {regs[8]} cm")
        print(f"  [29] Set water level: {regs[9]}")
        print(f"  [30] Current temp:   {regs[10]} C")
        print(f"  [31] Set temp:       {regs[11]}")
        print(f"  [32] Current RPM:    {regs[12]}")
        print(f"  [33] Set RPM:        {regs[13]}")
        print(f"  [38] Wash program:   {regs[18]}")
        print(f"  [39] Dry program:    {regs[19]}")
        print(f"  [40] Current step:   {regs[20]}")
        print(f"  [41] Required coins: {regs[21]}")
        print(f"  [42] Current coins:  {regs[22]}")
    else:
        print(f"  Read error: {result}")

# --- Test 3: Write test ---
if found_slaves:
    print()
    print("=" * 50)
    print(f"TEST 3: Write test - clear coins (addr=4, val=0)")
    print("=" * 50)

    slave = found_slaves[0]
    try:
        result = client.write_register(address=4, value=0, slave=slave)
    except TypeError:
        try:
            result = client.write_register(4, 0, unit=slave)
        except TypeError:
            result = client.write_register(4, 0)

    if not result.isError():
        print(f"  Write SUCCESS: {result}")
    else:
        print(f"  Write FAILED: {result}")

print()
print("=" * 50)
print("DIAGNOSTIC COMPLETE")
if not found_slaves:
    print()
    print("TROUBLESHOOTING:")
    print("1. Check RS485 A/B wiring (try swapping A and B)")
    print("2. Check touchscreen slave address setting")
    print("3. Check baud rate matches touchscreen config")
    print("   Try: python3 modbus_diag.py /dev/ttyUSB0 19200")
    print("4. Ensure touchscreen is powered on")
    print("5. Check RS485 termination resistor")
print("=" * 50)

client.close()
