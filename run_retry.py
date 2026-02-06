import sqlite3
from concentrator import upload_pending_concentrated_emails

if __name__ == "__main__":
    print("Resetting uploaded status in DB...")
    with sqlite3.connect('data/emails.db') as conn:
        cursor = conn.execute("UPDATE concentrated_emails SET uploaded=0")
        conn.commit()
        print(f"Reset {cursor.rowcount} records.")

    print("Starting upload retry...")
    upload_pending_concentrated_emails()
