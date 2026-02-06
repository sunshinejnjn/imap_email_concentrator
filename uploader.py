import os
import time
import imaplib
import sqlite3
from config import load_config
from db import get_db_connection

TARGET_FOLDER = "Concentrated_Emails"

def connect_imap():
    config = load_config()
    imap_server = config['imap_server']
    username = config['username']
    password = config['password']
    
    # print(f"Connecting to {imap_server}...")
    mail = imaplib.IMAP4_SSL(imap_server)
    mail.login(username, password)
    return mail

def ensure_remote_folder(mail, folder=TARGET_FOLDER):
    try:
        # 163.com might return NO for select even if it exists in some states, 
        # or we might just want to be robust.
        typ, data = mail.select(folder)
        if typ == 'OK':
            return True
            
        # print(f"Folder '{folder}' not found (typ={typ}). Creating...")
        typ, data = mail.create(folder)
        if typ == 'OK':
             mail.select(folder)
             return True
        else:
             # Check if it failed because it exists
             # 163 error: [b'CREATE Folder exist']
             response_str = str(data)
             if b'Folder exist' in data[0] or "exist" in response_str.lower():
                 # It exists, try to select again or just return True (append doesn't strictly require select)
                 # print(f"Folder '{folder}' already exists.")
                 return True
                 
             print(f"Failed to create folder: {data}")
             return False
    except Exception as e:
        print(f"Folder Ensure Error: {e}")
        return False

def upload_to_imap(file_path, retry_interactive=True, mail_conn=None, check_folder=True):
    should_close = False
    
    while True:
        try:
            if mail_conn:
                mail = mail_conn
            else:
                mail = connect_imap()
                should_close = True
            
            # Ensure folder exists
            if check_folder:
                if not ensure_remote_folder(mail, TARGET_FOLDER):
                    raise Exception("Could not search or create destination folder.")

            with open(file_path, 'rb') as f:
                msg_data = f.read()
                
            # print(f"Uploading {os.path.basename(file_path)}...")
            typ, data = mail.append(TARGET_FOLDER, None, imaplib.Time2Internaldate(time.time()), msg_data)
            
            if typ != 'OK':
                raise Exception(f"Append failed: {data}")
            
            if should_close:
                mail.logout()
            return True
            
        except Exception as e:
            print(f"Upload Error: {e}")
            if should_close:
                try: mail.logout() 
                except: pass

            if "limit exceed" in str(e).lower() or "quota" in str(e).lower():
                print("CRITICAL: Upload limit exceeded. Aborting.")
                # Close connection if possible
                try: mail.logout()
                except: pass
                import sys
                sys.exit(1)
                
            if retry_interactive:
                user_input = input("Upload failed. Press Enter to retry, or type 's' to skip this file: ")
                if user_input.lower().strip() == 's':
                    return False
            else:
                 # Standard failure (network, etc) - log and continue to next
                 pass
            return False

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
        if not ensure_remote_folder(mail, TARGET_FOLDER):
            print("Failed to ensure target folder exists. Aborting batch.")
            return
            
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
        
        # Pass usage of shared connection, skip folder check since we did it once
        success = upload_to_imap(file_path, retry_interactive=False, mail_conn=mail, check_folder=False)
        if success:
            conn = get_db_connection()
            c = conn.cursor()
            c.execute("UPDATE concentrated_emails SET uploaded = 1 WHERE id = ?", (record_id,))
            conn.commit()
            conn.close()
            success_count += 1
        else:
            fail_count += 1
            # Reconnection logic is handled in upload_to_imap somewhat, but here we loop
            # If aborted due to limit, we exited already.
            # If network error, we might want to try reconnecting?
            # Existing code didn't handle reconnect well in loop in previous version
            pass

    if mail:
        try: mail.logout()
        except: pass
        
    print(f"Batch upload complete. Success: {success_count}, Failed: {fail_count}")

def flush_remote_folder():
    print(f"Flushing remote '{TARGET_FOLDER}' folder...")
    try:
        mail = connect_imap()
        typ, data = mail.select(TARGET_FOLDER)
        if typ != 'OK':
            print(f"Folder {TARGET_FOLDER} not found or cannot be selected.")
            mail.logout()
            return

        # Search ALL
        typ, data = mail.search(None, 'ALL')
        msg_ids = data[0].split()
        
        if not msg_ids:
            print("Folder is already empty.")
        else:
            print(f"Deleting {len(msg_ids)} emails...")
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

def reset_upload_status():
    print("Resetting local upload status (setting uploaded=0 for all)...")
    conn = get_db_connection()
    c = conn.cursor()
    c.execute("UPDATE concentrated_emails SET uploaded = 0")
    conn.commit()
    conn.close()
    print("Reset complete.")

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == '--flush':
        force = '--force' in sys.argv
        print("!!! WARNING: FLUSHING REMOTE FOLDER AND RE-UPLOADING EVERYTHING !!!")
        if not force:
            try:
                input("Press Enter to confirm, or Ctrl+C to cancel...")
            except KeyboardInterrupt:
                print("\nCancelled.")
                sys.exit(0)
            
        flush_remote_folder()
        reset_upload_status()
        upload_pending_concentrated_emails()
    else:
        upload_pending_concentrated_emails()
