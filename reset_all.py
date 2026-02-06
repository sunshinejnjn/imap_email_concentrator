import sqlite3
import os
import shutil
import time

def reset_all():
    print("Flushing ALL concentrated data...")
    db_path = 'data/emails.db'
    
    # DB
    try:
        # Increase timeout to handle potential locks from downloader
        conn = sqlite3.connect(db_path, timeout=30) 
        c = conn.cursor()
        
        print("Truncating concentrated_emails...")
        c.execute("DELETE FROM concentrated_emails")
        
        print("Resetting emails status...")
        c.execute("UPDATE emails SET is_concentrated = 0, concentrated_id = NULL")
        
        conn.commit()
        conn.close()
        print("DB Reset successful.")
    except Exception as e:
        print(f"DB Error: {e}")
        # Proceed to delete files anyway? No, DB sync is important.
        # But we can try files.

    # Files
    dir_path = os.path.join("data", "concentrated")
    if os.path.exists(dir_path):
        print(f"Cleaning directory {dir_path}...")
        for filename in os.listdir(dir_path):
            file_path = os.path.join(dir_path, filename)
            try:
                if os.path.isfile(file_path):
                    os.remove(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
            except Exception as e:
                print(f'Failed to delete {file_path}. Reason: {e}')
    else:
        print("Directory data/concentrated does not exist.")
        
    print("Flush complete.")

if __name__ == "__main__":
    reset_all()
