#!/bin/bash

# --- CONFIG ---
REPO_URL="https://github.com/dismalict/dismalHAASETL.git"
TARGET_DIR="/home/administrator/Desktop/CNC/dismalHAASETL"
SERVICE_NAME="dismalHAASETL"
SERVICE_FILE="/etc/systemd/system/${SERVICE_NAME}.service"
PYTHON_BIN="/usr/bin/python3"
SCRIPT_FILE="$TARGET_DIR/cnc_etl.py"

# --- DEPLOY REPO ---
echo "Deploying ETL repo..."

if [ -d "$TARGET_DIR" ]; then
    echo "Repo exists. Pulling latest changes..."
    cd "$TARGET_DIR" || exit
    git reset --hard
    git pull origin main
else
    echo "Cloning repo..."
    git clone "$REPO_URL" "$TARGET_DIR"
fi

echo "Setting execute permissions..."
chmod +x "$TARGET_DIR"/*.sh
chmod +x "$SCRIPT_FILE"

# --- CREATE SYSTEMD SERVICE ---
echo "Creating systemd service..."

cat <<EOF | sudo tee "$SERVICE_FILE"
[Unit]
Description=Dismal HAASETL CNC ETL
After=network.target

[Service]
ExecStart=$PYTHON_BIN $SCRIPT_FILE
WorkingDirectory=$TARGET_DIR
User=root
Group=root
Restart=always
RestartSec=5
Environment=PYTHONUNBUFFERED=1
StandardOutput=append:$TARGET_DIR/output.log
StandardError=append:$TARGET_DIR/error.log

[Install]
WantedBy=multi-user.target
EOF

# --- RELOAD AND START SERVICE ---
echo "Reloading systemd..."
sudo systemctl daemon-reload

echo "Enabling and starting service..."
sudo systemctl enable "$SERVICE_NAME"
sudo systemctl restart "$SERVICE_NAME"

echo "Deployment and service setup complete."
