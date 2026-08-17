# Upload Agent - Install Guide
Deploy this on each store's back-office server. Same code every store, different config.json.

---

## What you need on each store server

- Python 3.8 or newer
- Internet access to reach AWS S3
- The folder path where Dumac drops TLOG files (confirm with client IT)

---

## Step 1 - Install Python (if not already installed)

Windows: download from https://www.python.org/downloads - check "Add Python to PATH" during install

Linux:
```
sudo apt install python3 python3-pip    # Ubuntu/Debian
sudo yum install python3 python3-pip    # CentOS/RHEL
```

---

## Step 2 - Install dependencies

Open a command prompt or terminal in the folder where you put upload_agent.py and run:

```
pip install boto3 watchdog
```

---

## Step 3 - Configure for this store

Edit config.json and fill in:

- store_id - a unique ID for this location, e.g. "store-007". Use the same ID for every store consistently.
- watch_folder - the local path where Dumac writes TLOG files. Get this from the client's IT team or the Dumac vendor.
  - Windows example: "C:\\RORC\\TLOGExport"
  - Linux example: "/var/rorc/export"
- file_pattern - the file extension Dumac uses. Update this once you know it from the vendor spec. Default is "*.tlog".
- s3_bucket - the name of the S3 bucket you created in AWS.
- aws_access_key and aws_secret_key - create one IAM user per store in AWS (named something like "upload-agent-store-007"), give it write access to S3, and paste the keys here.
- aws_region - the region your S3 bucket is in, e.g. "us-east-1".
- settle_seconds - how long to wait after a file appears before uploading it, to make sure Dumac has finished writing it. 3 seconds is a safe default.

---

## Step 4 - Test it manually

Run the agent once from the command line to confirm it connects and uploads:

```
python upload_agent.py
```

You should see log output like:
```
2026-08-06 14:22:01 [INFO] Starting POS upload agent for store: store-007
2026-08-06 14:22:01 [INFO] Watching folder: C:\RORC\TLOGExport
2026-08-06 14:22:01 [INFO] Scanning for existing files...
2026-08-06 14:22:02 [INFO] Uploaded: C:\RORC\TLOGExport\tx080626.tlog -> s3://yourcompany-pos-datalake/store-007/2026/08/06/tx080626.tlog
```

Check the S3 bucket in the AWS console to confirm the file arrived.

Press Ctrl+C to stop when done testing.

---

## Step 5 - Run as a background service

You want this running automatically, even after the server restarts.

### On Windows - using NSSM

NSSM (Non-Sucking Service Manager) turns any program into a Windows service.

1. Download NSSM from https://nssm.cc/download
2. Open a command prompt as Administrator
3. Run: `nssm install POSUploadAgent`
4. In the dialog that opens:
   - Path: full path to python.exe (e.g. C:\Python311\python.exe)
   - Startup directory: folder containing upload_agent.py
   - Arguments: upload_agent.py
5. Click Install service
6. Run: `nssm start POSUploadAgent`

The agent will now start automatically when Windows starts.

### On Linux - using systemd

Create a service file:

```
sudo nano /etc/systemd/system/pos-upload-agent.service
```

Paste this (update the paths to match your setup):

```
[Unit]
Description=POS TLOG Upload Agent
After=network.target

[Service]
Type=simple
User=root
WorkingDirectory=/opt/pos-upload-agent
ExecStart=/usr/bin/python3 /opt/pos-upload-agent/upload_agent.py
Restart=on-failure
RestartSec=10

[Install]
WantedBy=multi-user.target
```

Then enable and start it:

```
sudo systemctl daemon-reload
sudo systemctl enable pos-upload-agent
sudo systemctl start pos-upload-agent
```

Check it's running:
```
sudo systemctl status pos-upload-agent
```

---

## How it works day to day

Once running, the agent does three things automatically:

1. On startup - scans the watch folder and uploads any files already there. This catches anything that landed while the agent was offline.
2. Continuously - watches for new files and uploads them as they appear.
3. Duplicate prevention - keeps a local database (uploaded_files.db) so it never uploads the same file twice, even across restarts.

Logs are written monthly to the logs/ folder. Each store has its own log file.

---

## Deploying to multiple stores

The process is the same for every store - copy the same two files (upload_agent.py and config.json), change store_id and watch_folder in config.json, use that store's AWS access keys, and install as a service. Nothing else changes.
