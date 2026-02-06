import os
import time
import argparse
import json
import sqlite3
import imaplib
import email
import email.utils
from email.message import EmailMessage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.header import decode_header
import datetime
import re
import shutil
import shutil
import ollama
import json
import subprocess
import glob

from config import load_config
from db import get_db_connection
from identity import get_cached_identity_full, update_cached_identity, get_better_name, get_email_address_and_name, decode_mime_words, process_identity

MAX_SIZE_BYTES = 49 * 1024 * 1024 # 49MB
SPLIT_THRESHOLD = 33 * 1024 * 1024 # 33MB
RAR_PART_SIZE = "16m"

# ... (omitted helper lines) ...

def mark_as_concentrated(email_ids, concentrated_id):
    conn = get_db_connection()
    c = conn.cursor()
    for eid in email_ids:
        c.execute("UPDATE emails SET is_concentrated = 1, concentrated_id = ? WHERE id = ?", (concentrated_id, eid))
    conn.commit()
    conn.close()

def save_concentrated_record(sender, file_path, metadata, uploaded=0):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute('''
        INSERT INTO concentrated_emails (sender, file_path, content_metadata, uploaded)
        VALUES (?, ?, ?, ?)
    ''', (sender, file_path, json.dumps(metadata), uploaded))
    new_id = c.lastrowid
    conn.commit()
    conn.close()
    return new_id

# ... (rest of helper functions) ...



def clean_filename(s):
    """Sanitize string to be cache-safe as a filename."""
    # Windows forbidden: < > : " / \ | ? *
    s = str(s).strip().replace('"', '').replace("'", "").replace("\n", "").replace("\r", "")
    # Allow brackets [] for the naming convention, but replace others
    return re.sub(r'[<>:"/\\|?*]', '_', s)



# duplicate function removed

def split_email_with_zip(file_path):
    """
    Split a large email file into ZIP parts using 7-Zip.
    Returns list of paths to generated parts.
    """
    config = load_config()
    seven_zip_exe = config.get('7z_path', r"C:\Program Files\7-Zip\7z.exe")
    
    if not os.path.exists(seven_zip_exe):
        print(f"7-Zip executable not found at {seven_zip_exe}. Cannot split file.")
        return [file_path] 
        
    temp_dir = os.path.join("data", "temp_zip")
    os.makedirs(temp_dir, exist_ok=True)
    
    basename = os.path.basename(file_path)
    # Output archive name
    zip_name = f"{basename}.zip"
    zip_dest = os.path.join(temp_dir, zip_name)
    
    # 7z a -tzip -v16m dest source
    cmd = [seven_zip_exe, 'a', '-tzip', f'-v{RAR_PART_SIZE}', zip_dest, file_path]
    
    try:
        # cleanup old parts for this file
        existing = glob.glob(os.path.join(temp_dir, f"{basename}.z*")) + glob.glob(os.path.join(temp_dir, f"{basename}.zip*"))
        for e in existing: 
            try: os.remove(e) 
            except: pass
            
        print(f"Splitting {basename} (>33MB) to ZIP...")
        subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        
        # Find generated parts
        # 7-Zip split zip naming is usually .zip.001, .zip.002 OR .zip, .z01, .z02 depending on version.
        # Let's catch all relevant
        parts = []
        # Check standard zip split (.z01, .z02, ... .zip)
        parts.extend(glob.glob(os.path.join(temp_dir, f"{basename}.z*")))
        parts.extend(glob.glob(os.path.join(temp_dir, f"{basename}.zip"))) # Main file
        
        # Check 7z generic split (.zip.001, .zip.002)
        parts.extend(glob.glob(os.path.join(temp_dir, f"{basename}.zip.*")))
        
        # Deduplicate and sort
        parts = sorted(list(set(parts)))
        
        if not parts:
            if os.path.exists(zip_dest):
                return [zip_dest]
            
        return parts
    except Exception as e:
        print(f"ZIP splitting failed: {e}")
        return [file_path]


# duplicate function removed

def get_unconcentrated_years():
    """Get list of distinct years present in unconcentrated emails."""
    conn = get_db_connection()
    c = conn.cursor()
    # We need to parse year from 'date' column. 
    # Since date format varies, this is tricky in pure SQL.
    # But usually we can get min/max date?
    # Or just fetch all dates and parse in python (lightweight compared to fetching all content)
    c.execute("SELECT date FROM emails WHERE is_concentrated = 0")
    rows = c.fetchall()
    conn.close()
    
    years = set()
    for r in rows:
        try:
            d = email.utils.parsedate_to_datetime(r['date'])
            years.add(d.year)
        except:
             years.add(datetime.datetime.now().year)
             
    return sorted(list(years))

def get_unconcentrated_emails_for_year(year):
    """Fetch emails for a specific year manually since SQL date parsing is hard."""
    # We have to fetch all and filter in python? NO, that defeats the purpose.
    # But SQLite doesn't have easy date parsing for our custom strings.
    # Wait, 99% of our dates should be standard. 
    # Actually, we can fetch ID + Date, filter in python, then fetch full rows by ID? 
    # That is memory efficient.
    
    conn = get_db_connection()
    c = conn.cursor()
    c.execute("SELECT id, date FROM emails WHERE is_concentrated = 0")
    all_rows = c.fetchall()
    
    target_ids = []
    
    for r in all_rows:
        try:
            d = email.utils.parsedate_to_datetime(r['date'])
            if d.year == year:
                target_ids.append(r['id'])
        except:
            if year == datetime.datetime.now().year: # Fallback year
                 target_ids.append(r['id'])
                 
    if not target_ids:
        conn.close()
        return []
        
    # Now fetch full rows
    # chunk it if too big? 
    placeholders = ','.join(['?'] * len(target_ids))
    query = f"SELECT * FROM emails WHERE id IN ({placeholders}) ORDER BY date"
    c.execute(query, target_ids)
    rows = c.fetchall()
    conn.close()
    return rows

def connect_imap():
    config = load_config()
    imap_server = config['imap_server']
    imap_port = int(config.get('imap_tls_port', 993))
    username = config['username']
    password = config['password']
    
    mail = imaplib.IMAP4_SSL(imap_server, imap_port)
    mail.login(username, password)
    
    # ID command
    name_val = '("name" "python-client" "version" "1.0")'
    try:
            mail.xatom('ID', name_val)
    except:
            pass
    return mail

def flush_remote_folder():
    print("Flushing remote 'Concentrated_Emails' folder...")
    try:
        mail = connect_imap()
        folder = "Concentrated_Emails"
        typ, data = mail.select(folder)
        if typ != 'OK':
            print(f"Folder {folder} not found or cannot be selected.")
            mail.logout()
            return

        # Search ALL
        typ, data = mail.search(None, 'ALL')
        msg_ids = data[0].split()
        
        if not msg_ids:
            print("Folder is already empty.")
        else:
            print(f"Deleting {len(msg_ids)} emails...")
            # Store flag \Deleted
            # Convert list of bytes to comma separated string (or batch)
            # IMAP commands can fail if too long, better batch it
            batch_size = 100
            ids = [i.decode('utf-8') for i in msg_ids]
            
            for i in range(0, len(ids), batch_size):
                batch = ",".join(ids[i:i+batch_size])
                mail.store(batch, '+FLAGS', '\\Deleted')
            
            mail.expunge()
            print("Expunged.")
            
        mail.logout()
    except Exception as e:
        print(f"Flush failed: {e}")

def ensure_remote_folder(mail, folder="Concentrated_Emails"):
    try:
        typ, data = mail.select(folder)
        if typ == 'OK':
            return True
            
        print(f"Folder '{folder}' not found. Creating...")
        typ, data = mail.create(folder)
        if typ == 'OK':
             mail.select(folder)
             return True
        else:
             print(f"Failed to create folder: {data}")
             return False
    except Exception as e:
        print(f"Folder Ensure Error: {e}")
        return False

def upload_to_imap(file_path, retry_interactive=True, mail_conn=None):
    should_close = False
    
    while True:
        try:
            if mail_conn:
                mail = mail_conn
            else:
                mail = connect_imap()
                should_close = True
            
            # Ensure folder exists (we assume connection is fresh or valid)
            # For batch usage, we might want to check this once outside, but double check is cheap-ish
            folder = "Concentrated_Emails"
            # Note: 163.com might fail select/create but append might work if folder exists but hidden?
            # Or if select fails, we might just try append anyway?
            
            # Try to select/create
            ensure_remote_folder(mail, folder)

            with open(file_path, 'rb') as f:
                msg_data = f.read()
                
            # print(f"Uploading {os.path.basename(file_path)}...") # Reduce log noise
            typ, data = mail.append(folder, None, imaplib.Time2Internaldate(time.time()), msg_data)
            
            if typ != 'OK':
                raise Exception(f"Append failed: {data}")
                
            # print("Upload successful.")
            
            if should_close:
                mail.logout()
            return True
            
        except Exception as e:
            print(f"Upload Error: {e}")
            if should_close:
                try: mail.logout() 
                except: pass
                
            if retry_interactive:
                user_input = input("Upload failed. Press Enter to retry, or type 's' to skip this file: ")
                if user_input.lower().strip() == 's':
                    return False
            else:
                # If we were provided a connection and it failed, maybe it timed out?
                # We raise so caller can decide to reconnect.
                raise e




def parse_attachments_metrics(msg):
    """Return count, size, and list of {name, size} for attachments."""
    count = 0
    total_size = 0
    details = []
    
    for part in msg.walk():
        if part.get_content_maintype() == 'multipart':
            continue
        if part.get('Content-Disposition') is None:
            continue
            
        filename = part.get_filename()
        if filename:
            filename = decode_mime_words(filename)
            payload = part.get_payload(decode=True)
            if payload:
                size = len(payload)
                count += 1
                total_size += size
                details.append({'name': filename, 'size': size})
                
    return count, total_size, details

def format_size(size_bytes):
    if size_bytes < 1024:
        return f"{size_bytes}B"
    elif size_bytes < 1024 * 1024:
        return f"{size_bytes/1024:.1f}K"
    else:
        return f"{size_bytes/(1024*1024):.1f}M"

def concentrate_emails(start_year_arg=None, end_year_arg=None):
    config = load_config()
    sender_for_new_email = config.get('concerntrated_email_sender', 'Concentrator <auto@local>')
    receipt_address = config.get('concerntrated_email_receipt')
    me_address_str = config.get('username')
    
    # 1. Identify Years to Process
    print("Scanning database for available years...")
    available_years = get_unconcentrated_years()
    if not available_years:
        print("No emails found to concentrate.")
        return

    # Filter years based on args
    target_years = []
    for y in available_years:
        if start_year_arg is not None and y < start_year_arg: continue
        if end_year_arg is not None and y > end_year_arg: continue
        target_years.append(y)
        
    print(f"Target Years: {target_years}")

    # 2. Iterate Year by Year
    for current_processing_year in target_years:
        print(f"\n=== Processing Year: {current_processing_year} ===")
        
        raw_emails = get_unconcentrated_emails_for_year(current_processing_year)
        if not raw_emails:
            print(f"No emails found for year {current_processing_year} (Checked)")
            continue
            
        print(f"Loaded {len(raw_emails)} emails for {current_processing_year}.")
        
        # Grouping for current year
        groups = {} # Key: other_party_email
        # We don't need nested year dict since we are processing one year
        
        total_emails = len(raw_emails)
        for i, row in enumerate(raw_emails):
            if i % 1000 == 0:
                print(f"Scanning {i}/{total_emails}...")
                
            sender_str = row['sender']
            real_name, email_addr = get_email_address_and_name(sender_str)
            
            # Identity Logic
            other_party_email = email_addr
            other_party_name = real_name
            current_source_type = 0 # 0 = Received/From Header
            
            # Simple check if I am the sender
            if me_address_str and me_address_str.lower() in email_addr.lower():
                try:
                    with open(row['local_path'], 'rb') as f:
                        msg = email.message_from_bytes(f.read())
                        to_header = decode_mime_words(msg.get('To', 'Unknown'))
                        t_name, t_email = get_email_address_and_name(to_header)
                        other_party_email = t_email
                        other_party_name = t_name if t_name else t_email
                        current_source_type = 1 # 1 = Sent/To Header
                except Exception as e:
                    # print(f"Error reading file {row['local_path']}: {e}")
                    pass
            
            # --- Name Persistence Logic ---
            process_identity(other_party_email, other_party_name, current_source_type)
            cached_name, _, _ = get_cached_identity_full(other_party_email)
            display_name = cached_name if cached_name else other_party_name
            # -----------------------------
            
            group_key = other_party_email
            if not group_key: group_key = "unknown"
            
            if group_key not in groups:
                 groups[group_key] = {'name': display_name, 'emails': []}
                 
            # Update name if valid (trust cache more)
            groups[group_key]['name'] = display_name
            groups[group_key]['emails'].append(row)
            
        # --- Aggregation Logic (Per Year) ---
        misc_emails = []
        keys_to_remove = []
        
        for email_key, data in groups.items():
            if len(data['emails']) <= 1:
                misc_emails.extend(data['emails'])
                keys_to_remove.append(email_key)
                
        for k in keys_to_remove:
            del groups[k]
            
        if misc_emails:
            groups['misc_singles'] = {'name': 'Miscellaneous Singles', 'emails': misc_emails}
            print(f"Aggregated {len(misc_emails)} sparse emails into 'misc_singles'")
            
        # --- Process Groups for this Year ---
        for email_key, data in groups.items():
            
            # Use the persisted best name
            key_name = data['name']
            if not key_name: key_name = email_key
            
            # Subject display: [Name <email>]
            party_display = f"{key_name} <{email_key}>"
            
            msg_rows = data['emails']
            
            # Sort by date
            # Sort by date
            def parse_date_sort(row):
                 try: 
                     dt = email.utils.parsedate_to_datetime(row['date'])
                     if dt.tzinfo is None:
                         dt = dt.replace(tzinfo=datetime.timezone.utc)
                     return dt
                 except: 
                     return datetime.datetime.min.replace(tzinfo=datetime.timezone.utc)
            msg_rows.sort(key=parse_date_sort)
            
            # Pre-calculate Chunks
            process_queue = []
            
            for row in msg_rows:
                fpath = row['local_path']
                if not os.path.exists(fpath):
                    continue
                size = os.path.getsize(fpath)
                
                if size > SPLIT_THRESHOLD:
                    parts = split_email_with_zip(fpath)
                    total_parts_count = len(parts)
                    for i, p_path in enumerate(parts):
                        process_queue.append({
                            'type': 'part',
                            'path': p_path,
                            'original_row': row,
                            'part_index': i + 1,
                            'total_parts': total_parts_count
                        })
                else:
                    process_queue.append({
                        'type': 'email',
                        'path': fpath,
                        'original_row': row
                    })

            chunks = []
            current_chunk = []
            current_encoded_size = 0
            
            for item in process_queue:
                fpath = item['path']
                size = os.path.getsize(fpath)
                estimated_encoded_size = int(size * 1.4)
                
                if current_encoded_size + estimated_encoded_size > MAX_SIZE_BYTES and current_chunk:
                    chunks.append(current_chunk)
                    current_chunk = []
                    current_encoded_size = 0
                
                current_chunk.append(item)
                current_encoded_size += estimated_encoded_size
                
            if current_chunk:
                chunks.append(current_chunk)
            
            total_parts = len(chunks)
            # print(f"Processing {email_key}: {len(msg_rows)} items -> {total_parts} chunks.")
            
            for idx, chunk_msgs in enumerate(chunks):
                part_num = idx + 1
                
                # Sort chunk by Date
                def parse_date_item(item):
                    row = item['original_row']
                    try:
                        dt = email.utils.parsedate_to_datetime(row['date'])
                        if dt.tzinfo is None: dt = dt.replace(tzinfo=datetime.timezone.utc)
                        return dt
                    except:
                        return datetime.datetime.min.replace(tzinfo=datetime.timezone.utc)
                
                chunk_msgs.sort(key=parse_date_item)

                # Metrics Calculation
                chunk_att_count = 0
                chunk_att_size = 0
                first_date = None
                last_date = None
                
                metadata_list = []
                
                # Collect Data
                for item in chunk_msgs:
                    path = item['path']
                    row = item['original_row']
                    original_filename = os.path.basename(path)
                    
                    is_part = (item['type'] == 'part')
                    
                    if is_part:
                        subject = f"[Part {item['part_index']}/{item['total_parts']}] {row['subject']}"
                        att_details = [{'name': os.path.basename(path), 'size': os.path.getsize(path)}]
                        
                        try:
                            # Try to get header from original file just for TO field consistency
                            # But if file is huge, maybe skip?
                            # 33MB is big. Let's try raw read only head?
                            # Optim: Just use Unknown for parts, or read row data if we had it.
                            # We can trust row['sender']? But we want 'To'.
                            to_h = "Unknown" 
                            cc_h = ""
                        except:
                            to_h = "Unknown"
                            cc_h = ""
                        
                        meta = {
                            'original_id': row['id'],
                            'subject': subject,
                            'date': row['date'],
                            'date_iso': row['date'], 
                            'message_id': row['message_id'],
                            'to': to_h,
                            'cc': cc_h,
                            'att_details': att_details,
                            'filename': original_filename,
                            'is_part': True
                        }
                    else:
                        # Normal Email
                        with open(path, 'rb') as f:
                            raw_bytes = f.read()
                        
                        msg = email.message_from_bytes(raw_bytes)
                        att_count, att_size, att_details = parse_attachments_metrics(msg)
                        
                        chunk_att_count += att_count
                        chunk_att_size += att_size
                        
                        # Date for Range
                        try:
                            d = email.utils.parsedate_to_datetime(row['date'])
                            if d.tzinfo is None: d = d.replace(tzinfo=datetime.timezone.utc)
                            if not first_date or d < first_date: first_date = d
                            if not last_date or d > last_date: last_date = d
                            date_str_iso = d.strftime('%Y-%m-%d %H:%M:%S')
                        except:
                            date_str_iso = row['date']
                        
                        meta = {
                            'original_id': row['id'],
                            'subject': row['subject'],
                            'date': row['date'],
                            'date_iso': date_str_iso,
                            'message_id': row['message_id'],
                            'to': decode_mime_words(msg.get('To', '')),
                            'cc': decode_mime_words(msg.get('Cc', '')),
                            'bcc': decode_mime_words(msg.get('Bcc', '')),
                            'att_details': att_details,
                            'filename': original_filename,
                            'is_part': False
                        }
                    metadata_list.append(meta)
                
                # Format Dates for Title: YYYYMMDD
                fd_str = first_date.strftime("%Y%m%d") if first_date else "00000000"
                ld_str = last_date.strftime("%Y%m%d") if last_date else "00000000"
                
                size_str = format_size(chunk_att_size)
                
                # Use current_processing_year for title
                title_str = f"{current_processing_year}_[{party_display}]_{part_num}/{total_parts}_{len(chunk_msgs)}-Emails_{chunk_att_count}-Files-{size_str.replace(' ', '')}_{fd_str}_{ld_str}"
                
                filename_base = clean_filename(title_str)
                filename = f"{filename_base}.eml"
                
                # Build MIME
                outer = MIMEMultipart()
                outer['Subject'] = title_str
                outer['From'] = sender_for_new_email
                if receipt_address:
                    if '<' in receipt_address: outer['To'] = receipt_address
                    else:
                        p_name, _ = get_email_address_and_name(party_display)
                        if not p_name: p_name = "Concentration"
                        outer['To'] = f"{p_name} <{receipt_address}>"
                else:
                    outer['To'] = party_display 
                    
                outer['Date'] = email.utils.formatdate(localtime=True)
                
                for item in chunk_msgs:
                    path = item['path']
                    with open(path, 'rb') as f: content = f.read()
                    
                    if item['type'] == 'part':
                        part = MIMEApplication(content, _subtype="zip")
                    else:
                        part = MIMEApplication(content, _subtype="rfc822")
                        
                    part.add_header('Content-Disposition', 'attachment', filename=os.path.basename(path))
                    outer.attach(part)
                
                # Summary Body
                summary_lines = []
                summary_lines.append(f"Concentrated Email Archive")
                summary_lines.append(f"Title: {title_str}")
                summary_lines.append("=" * 60)
                summary_lines.append("")
                
                for m in metadata_list:
                    summary_lines.append(f"Subject: {m['subject']}")
                    summary_lines.append(f"Date:    {m['date']}")
                    summary_lines.append(f"File:    {m['filename']}") 
                    summary_lines.append(f"To:      {m['to']}")
                    if m['cc']: summary_lines.append(f"Cc:      {m['cc']}")
                    
                    if m['att_details']:
                        summary_lines.append("  Attachments:")
                        for att in m['att_details']:
                            summary_lines.append(f"  - {att['name']} ({format_size(att['size'])})")
                    else:
                        summary_lines.append("  (No Attachments)")
                        
                    summary_lines.append("-" * 40)
                    summary_lines.append("")
                
                outer.attach(MIMEText("\n".join(summary_lines), 'plain', 'utf-8'))
                
                save_dir = os.path.join("data", "concentrated", str(current_processing_year)) # Subfolder by Year
                os.makedirs(save_dir, exist_ok=True)
                file_path = os.path.join(save_dir, filename)
                
                with open(file_path, 'wb') as f:
                    f.write(outer.as_bytes())
                
                print(f"Created: {filename}")
                
                cid = save_concentrated_record(party_display, file_path, metadata_list, uploaded=0)
                ids = [m['original_id'] for m in metadata_list]
                mark_as_concentrated(ids, cid)
                # print(f"Saved local archive {filename} (ID: {cid}).")
            


def upload_pending_concentrated_emails():
    """Uploads files from concentrated_emails table where uploaded=0."""
    print("Checking for pending uploads...")
    conn = get_db_connection()
    c = conn.cursor()
    c.execute("SELECT id, file_path, sender FROM concentrated_emails WHERE uploaded = 0 ORDER BY id")
    rows = c.fetchall()
    conn.close()
    
    if not rows:
        print("No pending uploads.")
        return

    print(f"Found {len(rows)} pending files.")
    
    mail = None
    try:
        print("Connecting to IMAP for batch upload...")
        mail = connect_imap()
        
        # Ensure folder once
        folder_ready = ensure_remote_folder(mail, "Concentrated_Emails")
        if not folder_ready:
            print("Warning: Could not select/create 'Concentrated_Emails' folder. Uploads might fail.")
            
    except Exception as e:
        print(f"Initial IMAP connection failed: {e}")
        return

    success_count = 0
    fail_count = 0
    
    for row in rows:
        record_id = row['id']
        file_path = row['file_path']
        
        if not os.path.exists(file_path):
            print(f"File missing: {file_path}. Skipping.")
            continue
            
        print(f"Uploading ID {record_id}: {os.path.basename(file_path)}...")
        
        try:
            # Pass usage of shared connection
            success = upload_to_imap(file_path, retry_interactive=False, mail_conn=mail)
            if success:
                conn = get_db_connection()
                c = conn.cursor()
                c.execute("UPDATE concentrated_emails SET uploaded = 1 WHERE id = ?", (record_id,))
                conn.commit()
                conn.close()
                success_count += 1
                # print("Marked as uploaded.")
        except Exception as e:
            print(f"Failed to upload {record_id}: {e}")
            fail_count += 1
            # Try to reconnect if broken pipe?
            try:
                mail.noop() # Check connection
            except:
                print("Reconnecting...")
                try:
                    mail = connect_imap()
                    ensure_remote_folder(mail, "Concentrated")
                except:
                    print("Reconnect failed. Aborting batch.")
                    break
    
    if mail:
        try: mail.logout()
        except: pass
        
    print(f"Batch upload complete. Success: {success_count}, Failed: {fail_count}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Concentrate emails for a specific year or range.")
    parser.add_argument("start_year", type=int, help="The start year to process (e.g. 2013)")
    parser.add_argument("end_year", type=int, nargs='?', help="The end year (optional, defaults to start_year)")
    
    args = parser.parse_args()
    
    s_year = args.start_year
    e_year = args.end_year if args.end_year is not None else s_year
    
    print(f"Running concentration for years {s_year} to {e_year}...")
    concentrate_emails(start_year_arg=s_year, end_year_arg=e_year)
    
    print("Concentration process complete. Files saved locally and DB updated.")
    # Upload is now handled by uploader.py

