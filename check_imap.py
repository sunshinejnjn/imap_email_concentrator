import imaplib
import email
from config import load_config
from email.header import decode_header

def decode_mime_words(s):
    if not s: return ""
    decoded_list = decode_header(s)
    text = ""
    for decoded_bytes, charset in decoded_list:
        if isinstance(decoded_bytes, bytes):
            if charset:
                try:
                    text += decoded_bytes.decode(charset)
                except:
                    text += decoded_bytes.decode('utf-8', errors='replace')
            else:
                text += decoded_bytes.decode('utf-8', errors='replace')
        else:
            text += str(decoded_bytes)
    return text

def check_remote():
    config = load_config()
    imap_server = config['imap_server']
    username = config['username']
    password = config['password']
    
    print(f"Connecting to {imap_server} as {username}...")
    mail = imaplib.IMAP4_SSL(imap_server)
    mail.login(username, password)
    
    print("Listing ALL folders (RAW):")
    typ, folders = mail.list()
    for f in folders:
        print(f)
        
    target_folder = "Concentrated_Emails"
    print(f"\nSelecting '{target_folder}'...")
    try:
        typ, data = mail.select(target_folder)
        print(f"Selection Result: {typ}, {data}")
    except Exception as e:
        print(f"Selection Exception: {e}")
        typ = 'NO'

    if typ != 'OK':
        print(f"Folder '{target_folder}' not found. Attempting to CREATE...")
        try:
            typ, data = mail.create(target_folder)
            print(f"Creation Result: {typ}, {data}")
            mail.select(target_folder)
        except Exception as e:
            print(f"Creation Failed: {e}")
            
    # Check again
    typ, data = mail.select(target_folder)
    print(f"Final Selection: {typ}, {data}")
    
    if typ == 'OK':
        num_msgs = data[0].decode('utf-8')
        print(f"Folder selected. Total messages: {num_msgs}")
        
        # List last 5 subjects
        typ, msg_ids = mail.search(None, 'ALL')
        ids = msg_ids[0].split()
        if ids:
            print("Last 5 messages:")
            for i in ids[-5:]:
                try:
                    typ, msg_data = mail.fetch(i, '(BODY.PEEK[HEADER.FIELDS (SUBJECT)])')
                    for response_part in msg_data:
                        if isinstance(response_part, tuple):
                            msg = email.message_from_bytes(response_part[1])
                            subj = decode_mime_words(msg['Subject'])
                            print(f"ID {i.decode()}: {subj}")
                except Exception as e:
                    print(f"Error fetching {i}: {e}")
    
    mail.logout()

if __name__ == "__main__":
    check_remote()
