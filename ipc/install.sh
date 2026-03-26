#!/bin/bash
# ============================================================
# Cloud Monster IPC -- Installation Script
# Run on the Industrial Mini PC: bash install.sh
# ============================================================
set -euo pipefail

INSTALL_DIR="/opt/cloud_monster"
SERVICE_NAME="cloud-monster"
SERVICE_FILE="/etc/systemd/system/${SERVICE_NAME}.service"
LOG_DIR="/home/ypure"
USER_NAME="ypure"

# Default config (override via environment variables)
STORE_ID="${STORE_ID:-s1}"
SERIAL_PORT="${SERIAL_PORT:-/dev/ttyUSB0}"

echo "==========================================="
echo " Cloud Monster IPC Installer"
echo " Store: ${STORE_ID}"
echo " Serial: ${SERIAL_PORT}"
echo "==========================================="
echo ""

# --- 1. Check root ---
if [ "$EUID" -ne 0 ]; then
    echo "[ERROR] Please run as root: sudo bash install.sh"
    exit 1
fi

# --- 2. Create user if not exists ---
if ! id "${USER_NAME}" &>/dev/null; then
    echo "[*] Creating user ${USER_NAME}..."
    useradd -r -m -d /home/${USER_NAME} -s /bin/bash ${USER_NAME}
fi

# Add user to dialout group for serial port access
usermod -aG dialout ${USER_NAME} 2>/dev/null || true

# --- 3. Install Python dependencies ---
echo "[*] Installing Python dependencies..."
apt-get update -qq
apt-get install -y -qq python3 python3-pip > /dev/null 2>&1
pip3 install --quiet paho-mqtt pymodbus pyserial

# --- 4. Create installation directory ---
echo "[*] Setting up ${INSTALL_DIR}..."
mkdir -p "${INSTALL_DIR}"
cp "$(dirname "$0")/cloud_monster.py" "${INSTALL_DIR}/cloud_monster.py"
chmod +x "${INSTALL_DIR}/cloud_monster.py"
chown -R ${USER_NAME}:${USER_NAME} "${INSTALL_DIR}"

# --- 5. Create log directory ---
mkdir -p "${LOG_DIR}"
chown ${USER_NAME}:${USER_NAME} "${LOG_DIR}"

# --- 6. Create environment file ---
echo "[*] Creating environment config..."
cat > "${INSTALL_DIR}/env.conf" << ENVEOF
# Cloud Monster IPC Configuration
# Edit these values for your store
STORE_ID=${STORE_ID}
SERIAL_PORT=${SERIAL_PORT}
SERIAL_BAUD=9600
MQTT_HOST=1eb78bf5e78d4a50852846906854bec1.s1.eu.hivemq.cloud
MQTT_PORT=8883
MQTT_USER=laundry
MQTT_PASS=Phchen1108
LOG_PATH=/home/ypure/cloud_monster.log
ENVEOF
chown ${USER_NAME}:${USER_NAME} "${INSTALL_DIR}/env.conf"
chmod 600 "${INSTALL_DIR}/env.conf"

# --- 7. Create systemd service ---
echo "[*] Installing systemd service..."
cat > "${SERVICE_FILE}" << SVCEOF
[Unit]
Description=Cloud Monster IPC Controller
Documentation=https://github.com/a0989586419-create/laundry-backend
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=${USER_NAME}
Group=dialout
WorkingDirectory=${INSTALL_DIR}
ExecStart=/usr/bin/python3 ${INSTALL_DIR}/cloud_monster.py
EnvironmentFile=${INSTALL_DIR}/env.conf
Restart=always
RestartSec=10
WatchdogSec=120
StandardOutput=journal
StandardError=journal
SyslogIdentifier=cloud-monster

# Hardening
ProtectSystem=strict
ReadWritePaths=/home/ypure ${INSTALL_DIR}
PrivateTmp=true
NoNewPrivileges=true

[Install]
WantedBy=multi-user.target
SVCEOF

# --- 8. Enable and start ---
echo "[*] Enabling and starting service..."
systemctl daemon-reload
systemctl enable "${SERVICE_NAME}"
systemctl start "${SERVICE_NAME}"

echo ""
echo "==========================================="
echo " Installation complete!"
echo "==========================================="
echo ""
echo " Service:  ${SERVICE_NAME}"
echo " Config:   ${INSTALL_DIR}/env.conf"
echo " Script:   ${INSTALL_DIR}/cloud_monster.py"
echo " Logs:     ${LOG_DIR}/cloud_monster.log"
echo ""
echo " Useful commands:"
echo "   systemctl status ${SERVICE_NAME}"
echo "   journalctl -u ${SERVICE_NAME} -f"
echo "   systemctl restart ${SERVICE_NAME}"
echo "   systemctl stop ${SERVICE_NAME}"
echo ""
echo " To change store config, edit:"
echo "   ${INSTALL_DIR}/env.conf"
echo " Then restart: systemctl restart ${SERVICE_NAME}"
echo "==========================================="
