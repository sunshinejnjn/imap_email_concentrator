import os
import sqlite3
import datetime
import email.utils
from db import get_db_connection

def format_size(size_bytes):
    """Format bytes into human readable string."""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size_bytes < 1024.0:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.2f} PB"

def generate_statistics():
    """
    Connect to DB, fetch all emails, calculate stats by year.
    Priority for Year:
    1. Parse from 'date' column.
    2. Parse from 'local_path' (data/raw/YYYY/...).
    """
    conn = get_db_connection()
    try:
        c = conn.cursor()
        print("Fetching email metadata...")
        c.execute("SELECT id, date, local_path FROM emails")
        rows = c.fetchall()
        
        stats = {} # { year: {'count': 0, 'size': 0} }
        total_count = 0
        total_size = 0
        
        print(f"Analyzing {len(rows)} emails...")
        
        for row in rows:
            uid, date_str, local_path = row
            
            # Determine Year
            year = "Unknown"
            
            # Method 1: Path (Most reliable since downloader structures it)
            # data/raw/2013/05/...
            if local_path and "data" in local_path:
                parts = local_path.replace("\\", "/").split("/")
                # Find 'raw' index and next is year
                try:
                    if "raw" in parts:
                        idx = parts.index("raw")
                        if idx + 1 < len(parts):
                            year_candidate = parts[idx+1]
                            if year_candidate.isdigit() and len(year_candidate) == 4:
                                year = year_candidate
                except:
                    pass
            
            # Method 2: Date String Fallback
            if year == "Unknown" and date_str:
                try:
                    dt = email.utils.parsedate_to_datetime(date_str)
                    year = str(dt.year)
                except:
                    pass
            
            # Calculate Size
            size = 0
            if local_path and os.path.exists(local_path):
                try:
                    size = os.path.getsize(local_path)
                except:
                    pass
            
            # Update Stats
            if year not in stats:
                stats[year] = {'count': 0, 'size': 0}
            
            stats[year]['count'] += 1
            stats[year]['size'] += size
            
            total_count += 1
            total_size += size
            
        # Display Results
        print("\n=== Email Statistics by Year ===\n")
        print(f"{'Year':<10} | {'Count':<10} | {'Size':<15}")
        print("-" * 40)
        
        sorted_years = sorted(stats.keys())
        for y in sorted_years:
            count = stats[y]['count']
            size_str = format_size(stats[y]['size'])
            print(f"{y:<10} | {count:<10} | {size_str:<15}")
            
        print("-" * 40)
        print(f"{'TOTAL':<10} | {total_count:<10} | {format_size(total_size):<15}")
        print("\n")
        
    finally:
        conn.close()

if __name__ == "__main__":
    generate_statistics()
