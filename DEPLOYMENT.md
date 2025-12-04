# L2-Uploader Deployment Guide

## Quick Start (Linux with systemd)

### 1. Install Systemd Service

```bash
# Copy service file (replace USERNAME with your actual username)
sudo cp l2-uploader.service /etc/systemd/system/l2-uploader.service
sudo sed -i "s/%i/USERNAME/g" /etc/systemd/system/l2-uploader.service

# Or manually edit the service file:
sudo nano /etc/systemd/system/l2-uploader.service
# Change: User=%i → User=your_username
# Change: /home/%i/ → /home/your_username/
```

### 2. Configure Environment

Ensure `.env` file exists in project root:

```bash
cd /home/n39/L2-Uploader
cat > .env << EOF
AWS_ACCESS_KEY_ID=your_key_here
AWS_SECRET_ACCESS_KEY=your_secret_here
AWS_REGION=eu-central-1
S3_BUCKET_NAME=your-bucket-name
S3_PREFIX=binance/l2
SYMBOL=btcusdt
DEPTH_LEVEL=20
UPDATE_SPEED_MS=1000
BATCH_SECONDS=10
MAX_MESSAGES_PER_BATCH=5000
LOG_FILE=logs/l2_uploader.log
LOG_LEVEL=INFO
EOF

chmod 600 .env  # Protect credentials
```

### 3. Install Dependencies

```bash
cd /home/n39/L2-Uploader
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 4. Test Run (Manual)

```bash
cd /home/n39/L2-Uploader
source venv/bin/activate
python run_uploader.py
# Press Ctrl+C after verifying it connects and uploads
```

### 5. Enable and Start Service

```bash
sudo systemctl daemon-reload
sudo systemctl enable l2-uploader.service
sudo systemctl start l2-uploader.service
```

### 6. Verify Status

```bash
# Check service status
sudo systemctl status l2-uploader.service

# Follow logs
sudo journalctl -u l2-uploader -f

# Check application log (if LOG_FILE set)
tail -f logs/l2_uploader.log
```

---

## Alternative: macOS Deployment (launchd)

Since you mentioned a remote Mac server, here's a launchd plist for macOS:

### Create LaunchAgent

```bash
cat > ~/Library/LaunchAgents/com.l2uploader.plist << EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.l2uploader</string>
    <key>ProgramArguments</key>
    <array>
        <string>/Users/USERNAME/L2-Uploader/venv/bin/python</string>
        <string>/Users/USERNAME/L2-Uploader/run_uploader.py</string>
    </array>
    <key>WorkingDirectory</key>
    <string>/Users/USERNAME/L2-Uploader</string>
    <key>EnvironmentVariables</key>
    <dict>
        <key>PATH</key>
        <string>/Users/USERNAME/L2-Uploader/venv/bin:/usr/local/bin:/usr/bin:/bin</string>
    </dict>
    <key>StandardOutPath</key>
    <string>/Users/USERNAME/L2-Uploader/logs/stdout.log</string>
    <key>StandardErrorPath</key>
    <string>/Users/USERNAME/L2-Uploader/logs/stderr.log</string>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <true/>
    <key>ThrottleInterval</key>
    <integer>10</integer>
</dict>
</plist>
EOF

# Replace USERNAME with your actual username
# Load the service
launchctl load ~/Library/LaunchAgents/com.l2uploader.plist

# Check status
launchctl list | grep l2uploader

# View logs
tail -f ~/Library/LaunchAgents/com.l2uploader.plist
```

---

## Alternative: tmux (Simple, No Auto-Restart)

```bash
# Create tmux session
tmux new-session -d -s l2-uploader

# Run in tmux
tmux send-keys -t l2-uploader "cd /home/n39/L2-Uploader && source venv/bin/activate && python run_uploader.py" C-m

# Attach to session
tmux attach -t l2-uploader

# Detach: Ctrl+B, then D
```

**Note:** tmux doesn't auto-restart on crash. Use systemd or launchd for production.

---

## Verification Checklist

After deployment, verify:

- [ ] Service is running: `systemctl status l2-uploader` (Linux) or `launchctl list | grep l2uploader` (macOS)
- [ ] WebSocket connects: Check logs for "Connected successfully"
- [ ] S3 uploads work: Check logs for "Successfully uploaded"
- [ ] Files appear in S3: `aws s3 ls s3://your-bucket/binance/l2/` (or check AWS console)
- [ ] No errors in logs: `journalctl -u l2-uploader --since "10 minutes ago" | grep ERROR`
- [ ] Memory stable: `ps aux | grep run_uploader` (check MEM% over time)

---

## Troubleshooting

### Service Won't Start

```bash
# Check service status
sudo systemctl status l2-uploader.service

# Check logs
sudo journalctl -u l2-uploader -n 50

# Common issues:
# - Missing .env file
# - Wrong user in service file
# - Virtual environment not activated
# - Missing dependencies
```

### WebSocket Connection Fails

- Check network connectivity: `ping stream.binance.com`
- Verify stream name format in logs
- Check Binance API status
- Verify firewall rules allow WebSocket connections

### S3 Upload Fails

- Verify AWS credentials in `.env`
- Test credentials: `aws s3 ls s3://your-bucket/`
- Check IAM permissions (needs `s3:PutObject`)
- Verify bucket exists and is accessible
- Check network connectivity to AWS

### High Memory Usage

- Check batch size: Reduce `MAX_MESSAGES_PER_BATCH` if needed
- Check batch frequency: Reduce `BATCH_SECONDS` if needed
- Monitor for S3 upload failures (causes batch accumulation)

---

## Maintenance

### View Logs

```bash
# Systemd (Linux)
sudo journalctl -u l2-uploader -f

# Application log
tail -f logs/l2_uploader.log

# Last 100 lines
tail -n 100 logs/l2_uploader.log
```

### Restart Service

```bash
# Linux
sudo systemctl restart l2-uploader.service

# macOS
launchctl unload ~/Library/LaunchAgents/com.l2uploader.plist
launchctl load ~/Library/LaunchAgents/com.l2uploader.plist
```

### Stop Service

```bash
# Linux
sudo systemctl stop l2-uploader.service

# macOS
launchctl unload ~/Library/LaunchAgents/com.l2uploader.plist
```

### Update Code

```bash
# Pull latest changes
cd /home/n39/L2-Uploader
git pull  # or however you update

# Restart service
sudo systemctl restart l2-uploader.service
```

---

## Security Notes

1. **Protect .env file:** `chmod 600 .env`
2. **AWS IAM:** Use least-privilege IAM user with only S3 PutObject permission
3. **No withdrawals:** AWS keys should NOT have withdrawal permissions
4. **Log rotation:** Set up logrotate to prevent disk fill
5. **Monitor access:** Review S3 access logs periodically

---

## Performance Tuning

### Batch Configuration

- **Small batches (frequent uploads):** `BATCH_SECONDS=5`, `MAX_MESSAGES_PER_BATCH=1000`
- **Large batches (less frequent):** `BATCH_SECONDS=60`, `MAX_MESSAGES_PER_BATCH=10000`

### Memory Limits

Systemd service includes `MemoryMax=2G`. Adjust if needed:

```ini
[Service]
MemoryMax=4G  # Increase if needed
```

---

## Support

For issues, check:
1. `HEALTH_REPORT.md` - Detailed audit and known issues
2. Application logs: `logs/l2_uploader.log`
3. System logs: `journalctl -u l2-uploader`

