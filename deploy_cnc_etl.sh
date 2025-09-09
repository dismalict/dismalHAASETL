#!/bin/bash
# deploy_cnc_etl.sh
# Deploy CNC ETL script from GitHub to automation server

set -e  # exit on error

# Configuration
REPO_RAW_URL="https://raw.githubusercontent.com/dismalict/dismalHAASETL/main/cnc_etl.py"
DEST_DIR="/home/administrator/Desktop/CNC/dismalHAASETL"
SCRIPT_NAME="cnc_etl.py"
SERVICE_NAME="cnc_alert_etl.service"
SYSTEMD_PATH="/etc/systemd/system/$SERVICE_NAME"

# Create destination directory if it doesn't exist
mkdir -p "$DEST_DIR"

echo "Downloading latest ETL script..."
wget -O "$DEST_DIR/$SCRIPT_NAME" "$REPO_RAW_URL"

echo "Setting executable permissions..."
chmod +x "$DEST_DIR/$SCRIPT_NAME"

echo "Creating/updating systemd service..."
cat <<EOF | sudo tee "$SYSTEMD_PATH"
[Unit]
Description=Continuous CNC ETL for Alerting
After=network.target

[Service]
ExecStart=/usr/bin/python3 $DEST_DIR/$SCRIPT_NAME
WorkingDirectory=$DEST_DIR
User=root
Group=root
Restart=always
RestartSec=5
Environment=PYTHONUNBUFFERED=1
StandardOutput=append:$DEST_DIR/cnc_etl.log
StandardError=append:$DEST_DIR/cnc_etl_error.log

[Install]
WantedBy=multi-user.target
EOF

echo "Reloading systemd daemon..."
sudo systemctl daemon-reload

echo "Enabling and starting service..."
sudo systemctl enable "$SERVICE_NAME"
sudo systemctl restart "$SERVICE_NAME"

echo "Deployment complete. Check logs with:"
echo "sudo journalctl -u $SERVICE_NAME -f"
