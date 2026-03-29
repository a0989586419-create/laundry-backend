#!/usr/bin/env python3
"""
Test A: MQTT 通訊測試
只測 MQTT 收發，不需要 RS485/觸控屏。
在 D660 上跑這個腳本，然後從手機 LIFF 或 curl 發送指令看是否收到。

用法：python3 test_mqtt_listener.py
"""

import json
import ssl
import time
import sys
import os

try:
    import paho.mqtt.client as mqtt
except ImportError:
    sys.exit("請先安裝: pip3 install paho-mqtt")

# === 設定 ===
MQTT_HOST = os.environ.get("MQTT_HOST", "1eb78bf5e78d4a50852846906854bec1.s1.eu.hivemq.cloud")
MQTT_PORT = int(os.environ.get("MQTT_PORT", "8883"))
MQTT_USER = os.environ.get("MQTT_USER", "laundry")
MQTT_PASS = os.environ.get("MQTT_PASS", "Phchen1108")
STORE_ID  = os.environ.get("STORE_ID", "s1")

# 後端 publish 的 topic 格式
SUBSCRIBE_TOPIC = f"laundry/{STORE_ID}/+/command"
# 模擬 IPC 回覆的 topic
STATUS_TOPIC = f"laundry/{STORE_ID}/{{machine_id}}/status"
HEARTBEAT_TOPIC = f"laundry/{STORE_ID}/ipc/heartbeat"

received_count = 0


def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        print(f"\n{'='*60}")
        print(f"  MQTT 連線成功!")
        print(f"  Broker: {MQTT_HOST}:{MQTT_PORT}")
        print(f"  Store:  {STORE_ID}")
        print(f"  訂閱:   {SUBSCRIBE_TOPIC}")
        print(f"{'='*60}")
        client.subscribe(SUBSCRIBE_TOPIC, qos=1)

        # 發送上線心跳
        heartbeat = json.dumps({
            "store_id": STORE_ID,
            "status": "online",
            "mode": "test_listener",
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        })
        client.publish(HEARTBEAT_TOPIC, heartbeat, qos=1, retain=True)
        print(f"\n  已發送上線心跳")
        print(f"\n  等待指令中... (從手機 LIFF 付款或用 curl 測試)")
        print(f"  按 Ctrl+C 停止\n")
    else:
        print(f"  MQTT 連線失敗! rc={rc}")
        if rc == 5:
            print("  → 帳號密碼錯誤")
        elif rc == 4:
            print("  → 連線被拒絕")


def on_message(client, userdata, msg):
    global received_count
    received_count += 1

    topic = msg.topic
    try:
        payload = json.loads(msg.payload.decode())
    except:
        payload = msg.payload.decode()

    # 解析 machine_id from topic: laundry/s1/s1_washer_1/command
    parts = topic.split("/")
    machine_id = parts[2] if len(parts) >= 3 else "unknown"

    print(f"\n{'─'*60}")
    print(f"  #{received_count} 收到指令!")
    print(f"  時間:     {time.strftime('%H:%M:%S')}")
    print(f"  Topic:    {topic}")
    print(f"  Machine:  {machine_id}")
    print(f"  Payload:  {json.dumps(payload, ensure_ascii=False, indent=2)}")

    if isinstance(payload, dict):
        cmd = payload.get("cmd", "?")
        print(f"\n  指令類型: {cmd}")
        if cmd == "start":
            print(f"  訂單ID:   {payload.get('orderId', '?')}")
            print(f"  洗程:     wash_mode={payload.get('wash_mode')}, dry_mode={payload.get('dry_mode')}")
            print(f"  投幣數:   {payload.get('coins')} (金額 NT${payload.get('amount')})")
            print(f"  溫度:     {payload.get('temp', '?')}")

            # 模擬回覆 status（讓後端知道工控機收到了）
            status_payload = json.dumps({
                "machine_id": machine_id,
                "state": "running",
                "door": "locked",
                "remain_sec": 300,
                "orderId": payload.get("orderId"),
                "source": "test_listener",
                "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            })
            status_topic = STATUS_TOPIC.format(machine_id=machine_id)
            client.publish(status_topic, status_payload, qos=1)
            print(f"\n  已回覆模擬 status → {status_topic}")

        elif cmd == "stop":
            print(f"  → 停止機器")
        elif cmd == "skip_step":
            print(f"  → 跳步")
        elif cmd == "clear_coins":
            print(f"  → 清零錢箱")

    print(f"{'─'*60}")


def on_disconnect(client, userdata, rc, properties=None):
    print(f"\n  MQTT 斷線 (rc={rc})，嘗試重連...")


def main():
    print(f"\n  Cloud Monster MQTT 測試工具")
    print(f"  ─────────────────────────")
    print(f"  正在連線到 HiveMQ Cloud...")

    client = mqtt.Client(
        client_id=f"test-listener-{STORE_ID}-{int(time.time())}",
        protocol=mqtt.MQTTv311,
    )
    client.username_pw_set(MQTT_USER, MQTT_PASS)

    ctx = ssl.create_default_context()
    client.tls_set_context(ctx)

    client.on_connect = on_connect
    client.on_message = on_message
    client.on_disconnect = on_disconnect

    try:
        client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
        client.loop_forever()
    except KeyboardInterrupt:
        print(f"\n\n  測試結束！共收到 {received_count} 個指令")
        # 發送離線心跳
        heartbeat = json.dumps({
            "store_id": STORE_ID,
            "status": "offline",
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        })
        client.publish(HEARTBEAT_TOPIC, heartbeat, qos=1, retain=True)
        client.disconnect()
    except Exception as e:
        print(f"\n  連線失敗: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
