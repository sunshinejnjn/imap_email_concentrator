# Email Concentrator

This project provides tools to download emails from an IMAP server (specifically optimized for 163.com/Netease), concentrate them into larger archives (bundling attachments and compressing using ZIP), and upload them back to a special folder on the server. This helps manage quota limits and organizes old emails.

## Prerequisites

- Python 3.x
- `pip install -r requirements.txt` (or install `imaplib`, `ollama` if needed, though most are standard lib)
- 7-Zip installed (referenced in `config.py` path) for splitting large archives.

## Configuration

1.  Copy `config.ini.sample` to `config.ini`.
2.  Fill in your IMAP credentials and 7-Zip path.

## Usage

All operations are managed through `main.py`.

### 1. Download Emails
Download emails from your Inbox to a local SQLite database and file storage.

```bash
# Download last 100 emails
python main.py download --limit 100

# Download specific month
python main.py download --month 2023-01

# Batch download (smart resume)
python main.py download --batch-mode --start-year 2010
```

It is recommonded to run the download 2 times. The first time we'll download email locally, and the 2nd time, we will remove the confirmed locally existing emails from the server. If we add a --remove switch to the command, the script will do the deletion at the end of each batch (month). You still want to manually "permanent remove" emails from the "Deleted with email client" folder after that to reclaim your email count quota after this. But you can be sure enough that the emails in there are already saved locally.

```bash
# Run this to download emails between 2003 and 2025. Run it twice to remove them remotely.
python main.py download --batch-mode --start-month 2003-01 --before 2026-01-01 --remove
# On the second (confirmation) run, we only pull the header from the server, so the bandwidth/quota consumption won't be very high.
```

# Due to 163.com's unfair limitation, IMAP have daily quota around 10GB/day which will reset on 12:00 AM CST. 
# If you exceed the quota, you will need to wait for the next day to download more. Simply run the (batch) download command again the next day. You can specify a start month to resume from there.

### 2. Concentrate Emails
Process downloaded emails, grouping them by sender/year, bundling attachments, and creating `.eml` archives.

```bash
# Concentrate all downloaded emails
python main.py concentrate

# Concentrate specific year range
python main.py concentrate --start-year 2011 --end-year 2012
```

If you want to re-run the concentration from start, simply run `python main.py clean --concentration` first.


### 3. Upload Concentrated Emails
Upload the generated concentrated archives to the `Concentrated_Emails` folder on the IMAP server.

```bash
# Upload pending files
python main.py upload
# Due to 163.com limitations, IMAP have daily quota around 10GB/day which will reset on 12:00 AM CST. 
# If you exceed the quota, you will need to wait for the next day to upload more. Simply run the upload command again the next day. The script will resume from where it left off.

# Retry all (resets status and uploads everything again)
python main.py upload --retry-all
```

### 4. Search
Search through the concentrated metadata.

```bash
python main.py search --query "invoice"
```

### 5. Maintenance / Clean
Tools to clean up local data or remote headers.

```bash
# Clean local concentration data (keeps raw downloads)
python main.py clean --concentration

# Clean ALL local data (fresh start)
python main.py clean
** very dangerous!! **

# Flush Remote Folder (DANGER: Deletes all emails in 'Concentrated_Emails' folder)
python main.py flush
```

### 6. Statistics
View local stats.

```bash
python main.py stats
```

## Structure

- `main.py`: Entry point CLI.
- `downloader.py`: Handles IMAP downloading.
- `concentrator.py`: Logic for grouping and creating archives.
- `uploader.py`: Handles uploading to IMAP.
- `db.py`: Database management (SQLite).
- `app.py`: (Primitive) Flask Web UI (run with `python app.py`) for email concentration management. 
