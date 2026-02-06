import imaplib
import email
from email.header import decode_header
import os
import datetime
import calendar
import re
import time # Added for time.time()
from config import load_config
from config import load_config
from db import save_email_metadata, email_exists, get_db_connection, get_latest_email_date
from identity import process_identity, get_email_address_and_name, decode_mime_words

def clean_filename(s):
    """Sanitize string."""
    # Windows forbidden
    s = str(s).strip().replace('"', '').replace("'", "").replace("\n", "").replace("\r", "")
    s = str(s).strip().replace('"', '').replace("'", "").replace("\n", "").replace("\r", "")
    return re.sub(r'[<>:"/\\|?*]', '_', s)

def log_download_error(e, context={}):
    """Log error with context to download_errors.log"""
    log_file = "download_errors.log"
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    try:
        with open(log_file, "a", encoding="utf-8") as f:
            f.write(f"[{timestamp}] ERROR: {str(e)}\n")
            for k, v in context.items():
                f.write(f"  {k}: {v}\n")
            f.write("-" * 40 + "\n")
    except Exception as log_err:
        print(f"Failed to write to log: {log_err}")

# decode_mime_words is now imported from identity.py, so the local definition is removed.

def get_date_search_criteria(month_str=None, since_arg=None, before_arg=None):
    """
    Return (since_date, before_date) strings for IMAP.
    month_str: 'YYYY-MM'. If None, use current month.
    since_arg, before_arg: 'YYYY-MM-DD'. Overrides month if present.
    """
    today = datetime.date.today()
    
    # 1. Determine Requested Window
    if since_arg:
        try:
             req_start = datetime.datetime.strptime(since_arg, "%Y-%m-%d").date()
        except Exception as e:
             print(f"Invalid --since format: {e}. Using Today.")
             req_start = today
             
        if before_arg:
             try:
                 req_end = datetime.datetime.strptime(before_arg, "%Y-%m-%d").date()
             except:
                 req_end = req_start + datetime.timedelta(days=1)
        else:
             # Default end if not specified? Maybe just +1 day or infinite?
             # Let's say +30 days if only start is given, but user usually pairs them.
             # Or default to 'tomorrow' if they just say "since today".
             req_end = today + datetime.timedelta(days=1)
             
    elif month_str:
        try:
            year, month = map(int, month_str.split('-'))
            req_start = datetime.date(year, month, 1)
            # End date is 1st of next month
            if month == 12:
                req_end = datetime.date(year + 1, 1, 1)
            else:
                req_end = datetime.date(year, month + 1, 1)
        except ValueError:
            print("Invalid month format. Using current month.")
            req_start = datetime.date(today.year, today.month, 1)
            req_end = today + datetime.timedelta(days=1)
    else:
        # Default: Current Month
        req_start = datetime.date(today.year, today.month, 1)
        req_end = today + datetime.timedelta(days=1)
        
    # 2. Smart Resume Logic
    # Check if we have a latest date in DB that falls within this window
    # User Request: If month specified, do NOT resume from DB. Start from 1st.
    # We only auto-resume if we are running in default mode (no specific month args)
    # OR if we want to be safe, we can just disable this block if month_str is present.
    
    start_date = req_start
    
    if not month_str and not since_arg:
        # Only check DB for auto-resume if no specific date/month was requested
        latest_date_str = get_latest_email_date()
        
        if latest_date_str:
            try:
                # Parse "Thu, 30 Jan 2026..." or similar
                date_ref = email.utils.parsedate_to_datetime(latest_date_str).date()
                
                # Intersection Check
                if req_start <= date_ref < req_end:
                     resume_date = date_ref.replace(day=1)
                     print(f"Smart Resume: Found existing data up to {date_ref}. Resuming download from {resume_date} (Start of Month).")
                     start_date = resume_date
                     
            except Exception as e:
                print(f"Date parse error for resume '{latest_date_str}': {e}")

    # 3. Format for IMAP
    since_str = start_date.strftime("%d-%b-%Y")
    before_str = req_end.strftime("%d-%b-%Y")
    
    return since_str, before_str

def get_other_party_email(msg, source_folder):
    """
    Identify the counterpart email.
    If source is INBOX (Received): Sender.
    If source is Sent: Recipient (To).
    """
    from_header = decode_mime_words(msg.get("From", ""))
    to_header = decode_mime_words(msg.get("To", ""))
    
    # Heuristic for Sent folder
    # Common Sent folder names: "Sent", "Sent Items", "&XfJT0ZAB-" (163 specific)
    is_sent = False
    if "sent" in source_folder.lower() or "&xfjt0zab-" in source_folder.lower():
        is_sent = True
        
    if is_sent:
        # Recipient is the other party
        # Parse 'To' - might be multiple, take first
        name, addr = email.utils.parseaddr(to_header)
        return addr.lower() if addr else "unknown_recipient"
    else:
        # Sender is the other party
        name, addr = email.utils.parseaddr(from_header)
        return addr.lower() if addr else "unknown_sender"

def extract_date_from_received(msg):
    """
    Attempt to extract a valid date from 'Received' headers.
    Returns datetime object or None.
    """
    received_headers = msg.get_all('Received')
    if not received_headers:
        return None
        
    for hdr in received_headers:
        # Typical format: "from ... by ... ; Mon, 13 Sep 2010 14:12:45 +0800"
        # We look for the part after the last semicolon
        try:
            if ';' in hdr:
                date_part = hdr.split(';')[-1].strip()
                dt = email.utils.parsedate_to_datetime(date_part)
                if dt:
                    return dt
        except:
             continue
    return None

def download_emails(limit=None, month=None, since=None, before=None, remove_on_exist=False):
    config = load_config()
    imap_server = config['imap_server']
    imap_port = int(config.get('imap_tls_port', 993))
    username = config['username']
    password = config['password']
    
    total_processed = 0
    total_deleted = 0

    print(f"Connecting to {imap_server}...")
    mail = imaplib.IMAP4_SSL(imap_server, imap_port)
    
    try:
        mail.login(username, password)
        print("Logged in.")
        
        # ID Command
        name_val = '("name" "python-client" "version" "1.0")'
        try:
             mail.xatom('ID', name_val)
        except:
             pass

        # Identify folders
        target_folders = ["INBOX"]
        # Try to find Sent folder
        typ, mailboxes = mail.list()
        sent_folder = None
        for m in mailboxes:
            name = m.decode('utf-8')
            # 163 uses &XfJT0ZAB- for Sent Items usually, or "Sent Items"
            if "&XfJT0ZAB-" in name or "Sent" in name:
                # Extract name from string like: (\HasNoChildren) "/" "&XfJT0ZAB-"
                # Simple extraction
                if '"/"' in name:
                    folder_name = name.split('"/"')[-1].strip().replace('"', '')
                else:
                    folder_name = name.split()[-1].replace('"', '')
                
                # Check if it looks like Sent
                if "&XfJT0ZAB-" in folder_name or "Sent" in folder_name:
                    sent_folder = folder_name
                    break
        
        if sent_folder:
            print(f"Detected Sent folder: {sent_folder}")
            target_folders.append(sent_folder)
        else:
            print("Could not specific Sent folder automatically, checking standard 'Sent Items'...")
            target_folders.append("Sent Items") # Try standard

        # Date Criteria
        since_crit, before_crit = get_date_search_criteria(month, since, before)
        print(f"Date Range: SINCE {since_crit} BEFORE {before_crit}")
        
        # conn = get_db_connection() # No longer needed here, db functions handle their own connections
        # cursor = conn.cursor() # No longer needed here

        for folder in target_folders:
            print(f"Scanning folder: {folder}")
            typ, data = mail.select(folder)
            if typ != 'OK':
                print(f"  Skipping {folder} (Selected failed)")
                continue

            # Search with Date Range
            # SEARCH SINCE d-M-Y BEFORE d-M-Y
            search_crit = f'(SINCE "{since_crit}" BEFORE "{before_crit}")'
            typ, messages = mail.search(None, search_crit)
            
            if typ != 'OK':
                print("  Search failed.")
                continue
                
            email_ids = messages[0].split()
            print(f"  Found {len(email_ids)} emails in {folder}.")
            
            # Apply limit if strictly requested (though date range usually overrides)
            if limit and len(email_ids) > limit:
                print(f"  Limiting to last {limit}...")
                email_ids = email_ids[-limit:]

            consecutive_errors = 0
            for i, e_id in enumerate(email_ids):
                msg_id_str = e_id.decode() if isinstance(e_id, bytes) else str(e_id)
                print(f"[{i+1}/{len(email_ids)}] Fetching ID {msg_id_str}...")
                
                try:
                    # 1. Fetch Header for ID check (Optimization)
                    typ, header_data = mail.fetch(msg_id_str, '(RFC822.HEADER)')
                    if typ != 'OK': 
                        raise Exception("Error fetching header")
                        
                    header_content = None
                    if header_data and isinstance(header_data[0], tuple):
                        header_content = header_data[0][1]
                    
                    if header_content:
                        try:
                            msg_header = email.message_from_bytes(header_content)
                            message_id = msg_header.get('Message-ID', '').strip()
                            if not message_id:
                                 message_id = f"{msg_id_str}_{folder}_{int(time.time())}"
                        except:
                            message_id = f"{msg_id_str}_{folder}_{int(time.time())}"
                    else:
                        message_id = f"{msg_id_str}_{folder}_{int(time.time())}"
                    
                    # Resume/Duplicate Check
                    if email_exists(message_id):
                        print(f"Skipping {message_id} (Already exists)")
                        
                        if remove_on_exist:
                            try:
                                mail.store(msg_id_str, '+FLAGS', '\\Deleted')
                                print(f"  Marked {msg_id_str} for deletion.")
                                total_deleted += 1
                            except Exception as del_err:
                                print(f"  Deletion failed: {del_err}")
                                
                        consecutive_errors = 0 # Successful skip resets errors
                        continue

                    # 2. Fetch Full Content
                    typ, msg_data = mail.fetch(msg_id_str, '(RFC822)')
                    if typ != 'OK':
                        raise Exception("Error fetching body")
                        
                    if not msg_data or not isinstance(msg_data[0], tuple):
                         raise Exception(f"Invalid message body data received for ID {msg_id_str}")
                    raw_email = msg_data[0][1]
                    msg = email.message_from_bytes(raw_email)
                    
                    # Parse Basic Info
                    # Ensure all are strings for SQLite
                    subject = str(decode_mime_words(msg.get('Subject', 'No Subject')))
                    date_str = str(msg.get('Date') or '')
                    from_str = str(msg.get('From') or '')
                    to_str = str(msg.get('To', ''))
                    
                    # Message ID cleanup
                    message_id = str(message_id).strip()

                    # --- Identity Processing (Downloader Phase) ---
                    # Check Sender (Received Email -> Source 0)
                    from_name, from_email = get_email_address_and_name(from_str)
                    process_identity(from_email, from_name, source_type=0)
                    
                    # Check Recipient (Sent Email -> Source 1)
                    # Use my username to check if I am sender
                    me_address = config.get('username', '').lower()
                    is_from_me = (me_address in from_email.lower()) if me_address else False
                    
                    if is_from_me:
                        to_name, to_email = get_email_address_and_name(to_str)
                        # Use Source 1 (High Priority) for people I send TO
                        process_identity(to_email, to_name, source_type=1)
                    # ----------------------------------------------

                    if not date_str:
                        date_str = str(datetime.datetime.now())
                        
                    # Folder/File Logic
                    # Local Path: data/raw/YYYY/MM/OtherParty/filename
                    
                    # Determine Other Party (for folder organization)
                    if is_from_me:
                        # I sent it, so folder is the Recipient
                        # Simpler parsing for folder key
                        _, other_email = get_email_address_and_name(to_str)
                    else:
                        # I received it, so folder is Sender
                        other_email = from_email
                        
                    if not other_email: other_email = "unknown"
    
                    # Date Parsing with Fallback regarding "Received" header
                    final_date_obj = None
                    
                    # 1. Try Standard Date Header
                    try:
                        if date_str:
                            final_date_obj = email.utils.parsedate_to_datetime(date_str)
                            # Sanity Check: If year is way off (e.g. < 1990) or future?
                            # For now, just trust it if it parses.
                    except:
                        pass
                        
                    # 2. Fallback to Received Header if 1 failed
                    if not final_date_obj:
                        print(f"  Date header '{date_str}' invalid/missing. Trying 'Received' header...")
                        final_date_obj = extract_date_from_received(msg)
                        if final_date_obj:
                             # Update date_str for DB consistency
                             date_str = str(final_date_obj)
                             print(f"  Extracted date from Received: {date_str}")
                             
                    # 3. Fallback to Now
                    if not final_date_obj:
                        print("  Could not extract any date. using NOW.")
                        final_date_obj = datetime.datetime.now()
                        
                    year = str(final_date_obj.year)
                    month = f"{final_date_obj.month:02d}"
                        
                    save_dir = os.path.join("data", "raw", year, month, clean_filename(other_email))
                    os.makedirs(save_dir, exist_ok=True)
                    
                    filename = f"{clean_filename(subject[:50])}_{int(time.time())}.eml"
                    filepath = os.path.join(save_dir, filename)
                    
                    with open(filepath, 'wb') as f:
                        f.write(raw_email)
                        
                    save_email_metadata(message_id, subject, from_str, date_str, filepath)

                    total_processed += 1
                    consecutive_errors = 0 # Reset on success
                    
                except Exception as e:
                    print(f"Error processing email {i}: {e}")
                    
                    # Gather context for logging
                    err_ctx = {
                        "Query Since": since_crit,
                        "Query Before": before_crit,
                        "Current Folder": folder,
                        "Message Sequence": i,
                        "Last DB Date": get_latest_email_date(), # Re-query for specific error context
                    }
                    
                    # Try to add current email details if available
                    try:
                        if 'msg_id_str' in locals(): err_ctx["Msg ID"] = msg_id_str
                        if 'date_str' in locals() and date_str: err_ctx["Email Date"] = date_str
                        if 'subject' in locals() and subject: err_ctx["Subject"] = subject
                    except:
                        pass
                        
                    log_download_error(e, err_ctx)
                    
                    # If it's just a missing body (ghost email), don't count it as a connection/critical failure
                    if "Invalid message body data" in str(e):
                        print(f"  Warning: Skipping invalid email {msg_id_str} and continuing...")
                        #consecutive_errors = 0 # Reset or just don't increment? Resetting is safer to avoid accumulating mixed errors.
                    else:
                        consecutive_errors += 1
                        
                    if consecutive_errors >= 10:
                        print("TOO MANY CONSECUTIVE ERRORS (10). Stopping download safely.")
                        raise RuntimeError("Too many consecutive errors")
                    continue
        
    except Exception as e:
        print(f"IMAP Error: {e}")
        # Critical: If this was our intentional stop signal, re-raise it so main.py stops!
        if "Too many consecutive errors" in str(e):
            raise e
    finally:
        if remove_on_exist:
            try:
                print("Expunging deleted messages...")
                mail.expunge()
            except Exception as e:
                print(f"Expunge Error: {e}")
        mail.logout()
        
    print(f"Download complete. Processed {total_processed} emails. Deleted {total_deleted} emails.")
    return total_processed, total_deleted

if __name__ == "__main__":
    download_emails(limit=5)
