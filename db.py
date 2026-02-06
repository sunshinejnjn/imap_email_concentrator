import sqlite3
import os

DB_FILE = 'data/emails.db'

def get_db_connection():
    if not os.path.exists('data'):
        os.makedirs('data')
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_db_connection()
    c = conn.cursor()
    
    # 1. Create Tables (Latest Schema)
    
    # Table to store original downloaded emails
    c.execute('''
        CREATE TABLE IF NOT EXISTS emails (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            message_id TEXT UNIQUE,
            sender TEXT,
            subject TEXT,
            date TEXT,
            local_path TEXT,
            is_concentrated BOOLEAN DEFAULT 0,
            concentrated_id INTEGER
        )
    ''')
    
    # Table to store concentrated emails
    c.execute('''
        CREATE TABLE IF NOT EXISTS concentrated_emails (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sender TEXT,
            file_path TEXT,
            remote_uid TEXT,
            content_metadata TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')    

    # Table to cache persistent identities (names) for email addresses
    c.execute('''
        CREATE TABLE IF NOT EXISTS email_identities (
            email TEXT PRIMARY KEY,
            name TEXT,
            seen_names TEXT,
            name_source INTEGER DEFAULT 0,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # 2. Migrations (For existing databases with old schemas)
    try:
        c.execute("SELECT concentrated_id FROM emails LIMIT 1")
    except sqlite3.OperationalError:
        print("Migrating: Adding concentrated_id column to emails table...")
        c.execute("ALTER TABLE emails ADD COLUMN concentrated_id INTEGER")
        
    try:
        c.execute("SELECT seen_names FROM email_identities LIMIT 1")
    except sqlite3.OperationalError:
        print("Migrating: Adding seen_names column to email_identities table...")
        c.execute("ALTER TABLE email_identities ADD COLUMN seen_names TEXT")
        
    try:
        c.execute("SELECT name_source FROM email_identities LIMIT 1")
    except sqlite3.OperationalError:
        print("Migrating: Adding name_source column to email_identities table...")
        c.execute("ALTER TABLE email_identities ADD COLUMN name_source INTEGER DEFAULT 0")

    try:
        c.execute("SELECT uploaded FROM concentrated_emails LIMIT 1")
    except sqlite3.OperationalError:
        print("Migrating: Adding uploaded column to concentrated_emails table...")
        c.execute("ALTER TABLE concentrated_emails ADD COLUMN uploaded BOOLEAN DEFAULT 0")
    
    conn.commit()
    conn.close()
    print(f"Database initialized at {DB_FILE}")

def email_exists(message_id):
    conn = get_db_connection()
    try:
        c = conn.cursor()
        c.execute("SELECT id FROM emails WHERE message_id = ?", (message_id,))
        exists = c.fetchone() is not None
        return exists
    finally:
        conn.close()

def save_email_metadata(message_id, subject, sender, date_str, local_path):
    conn = get_db_connection()
    try:
        c = conn.cursor()
        c.execute('''
            INSERT INTO emails (message_id, sender, subject, date, local_path)
            VALUES (?, ?, ?, ?, ?)
        ''', (message_id, sender, subject, date_str, local_path))
        conn.commit()
    finally:
        conn.close()

def get_latest_email_date():
    """Get the date of the most recent email in the DB."""
    conn = get_db_connection()
    try:
        c = conn.cursor()
        # Ordered by Date Descending.
        # Note: 'date' column is currently Text (IMAP string or ISO?). 
        # Downloader saves it as 'date_str' from header (e.g. "Thu, 30 Jan 2026..."). 
        # This is hard to sort SQL-wise directly unless we parsed it.
        # BUT, we also save 'filename' which has a sortable timestamp prefix if we updated it?
        # Or, we should trust the SQLite ID (usually inserted in order) as a heuristic, 
        # or rely on the python logic to parse.
        
        # Ideally, we should parse the dates to restart correctly.
        # Let's try to get the row with the max ID (assuming download order ~ time order).
        # Then parse that date.
        
        c.execute("SELECT date FROM emails ORDER BY id DESC LIMIT 1")
        row = c.fetchone()
        
        if row and row[0]:
            return row[0]
        return None
    finally:
        conn.close()

if __name__ == "__main__":
    init_db()
