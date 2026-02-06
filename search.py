import argparse
import sqlite3
import json
from db import get_db_connection

def search_emails(query):
    print(f"Searching for '{query}'...")
    conn = get_db_connection()
    c = conn.cursor()
    
    # We'll just fetch all and filter in python for simplicity given it's a JSON field
    # For large datasets, SQLite's json_extract or a proper FTS would be better,
    # but for this "test run" simplicity is key.
    c.execute("SELECT * FROM concentrated_emails")
    rows = c.fetchall()
    
    found_count = 0
    query = query.lower()
    
    for row in rows:
        metadata_json = row['content_metadata']
        try:
            metadata = json.loads(metadata_json)
        except:
            continue
            
        matches_in_file = []
        for item in metadata:
            subject = item.get('subject', '').lower()
            date = item.get('date', '')
            
            if query in subject or query in date.lower():
                matches_in_file.append(item)
        
        if matches_in_file:
            print(f"Found {len(matches_in_file)} matches in '{row['file_path']}':")
            for m in matches_in_file:
                print(f"  - [{m['date']}] {m['subject']}")
            print("-" * 40)
            found_count += 1
            
    if found_count == 0:
        print("No matches found.")
        
    conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Search Concentrated Emails")
    parser.add_argument('--query', required=True, help='Keyword to search in metadata')
    args = parser.parse_args()
    
    search_emails(args.query)
