import argparse
import sys
from db import init_db

from downloader import download_emails
import datetime
import email.utils

def handle_download(args):
    print(f"Starting download... Limit: {args.limit}")
    download_emails(limit=args.limit)

from concentrator import concentrate_emails, flush_remote_folder
from search import search_emails
from stats import generate_statistics
import os
import shutil
import datetime

def handle_clean(args):
    if args.concentration:
        print("Resetting concentration data only...")
        # 1. Remove data/concentrated
        conc_dir = os.path.join("data", "concentrated")
        if os.path.exists(conc_dir):
            try:
                shutil.rmtree(conc_dir)
                print(f"Removed {conc_dir}")
            except Exception as e:
                print(f"Error removing {conc_dir}: {e}")
        
        # 2. Reset DB
        try:
            from db import get_db_connection
            conn = get_db_connection()
            c = conn.cursor()
            # Clear concentrated_emails table
            c.execute("DELETE FROM concentrated_emails")
            # Reset emails status
            c.execute("UPDATE emails SET is_concentrated = 0, concentrated_id = NULL")
            conn.commit()
            conn.close()
            print("Database concentration records reset.")
        except Exception as e:
             print(f"Error resetting DB: {e}")
             
    else:
        print("Cleaning ALL local data...")
        if os.path.exists("data"):
            try:
                shutil.rmtree("data")
                print("Removed data/ directory.")
            except Exception as e:
                print(f"Error removing data: {e}")
        else:
            print("Data directory not found.")
            
        # Re-init DB just in case
        from db import init_db
        init_db()

def handle_flush(args):
    print("Flushing remote folder...")
    flush_remote_folder()

def handle_download(args):
    today = datetime.date.today()
    
    # Batch Mode Logic
    if args.batch_mode:
        start_year = args.start_year
        if args.start_from:
            try:
                 sy, sm = map(int, args.start_from.split('-'))
                 current_iter = datetime.date(sy, sm, 1)
            except:
                 print("Invalid --start-from format. Using start-year.")
                 current_iter = datetime.date(start_year, 1, 1)
        else:
            current_iter = datetime.date(start_year, 1, 1)
        
        # Smart Global Resume: Check DB
        # Only check DB if explicit start-from wasn't given OR if DB is AHEAD of start-from?
        # Usually smart resume is helpful. But if user says "start from 2011-08", they likely want to force it.
        # Let's say: If --start-from is provided, we respect result of that parsing (current_iter).
        # We STILL check DB, but only jump if DB > current_iter?
        # Example: User asks 2011-08. DB has 2015-01.
        # If we respect DB, we jump to 2015. User might be confused.
        # But if DB has 2015, we *should* skip 2011 unless we want duplicates check?
        # Let's trust the user override if provided. 
        # Actually, let's keep the logic: Jump only if DB > current_iter AND user didn't force strict? 
        # I'll stick to: If `start_from` is explicitly passed, disable automatic jump?
        # Or: If `start_from` is passed, `current_iter` is set.
        # The check `if iter_start_month > current_iter` handles it.
        # If DB is 2015, and request is 2011. 2015 > 2011. It jumps.
        # If user wants to FORCE re-download of 2011, they should clean or we need a flag.
        # Given "resume from 2011.08", assuming DB stopped there, so DB date ~ 2011-08.
        # If DB stopped at 2011-08, then `iter_start_month` (from DB) will be approx 2011-08.
        # `current_iter` (from arg) will be 2011-08.
        # `if 2011-08 > 2011-08`: False. No jump.
        # So it works naturally.
        
        from db import get_latest_email_date
        latest_str = get_latest_email_date()
        if latest_str and not args.start_from:
            try:
                lat_date = email.utils.parsedate_to_datetime(latest_str).date()
                iter_start_month = lat_date.replace(day=1)
                
                if iter_start_month > current_iter:
                    print(f"Batch Resume: Jumping from {current_iter} (Requested) to {iter_start_month} (DB Latest).")
                    current_iter = iter_start_month
            except:
                pass
                
        # Determine End Date
        end_date = today
        end_date_str = "Now"
        if args.before:
            try:
                # args.before is YYYY-MM-DD
                # User request: "--before should mean the end date is one day before it."
                ey, em, ed = map(int, args.before.split('-'))
                end_date = datetime.date(ey, em, ed) - datetime.timedelta(days=1)
                end_date_str = str(end_date)
            except:
                print("Invalid --before format. Using Today.")

        print(f"Starting Batch Download from {current_iter.strftime('%Y-%m')} to {end_date_str}...")
        
        total_downloaded = 0
        total_deleted = 0
        global_limit = args.limit
        
        # loop condition now unified: run while month-start is <= end_date
        while current_iter <= end_date:

            # Check Global Limit
            if global_limit and total_downloaded >= global_limit:
                print(f"Global limit of {global_limit} reached. Stopping batch.")
                break
                
            month_str = current_iter.strftime("%Y-%m")
            print(f"\n=== Processing Month: {month_str} | Downloaded: {total_downloaded}/{global_limit if global_limit else 'Inf'} | Deleted: {total_deleted} ===")
            
            # Calculate remaining limit for this batch
            current_batch_limit = None
            if global_limit:
                 current_batch_limit = global_limit - total_downloaded
            
            # Increment Month
            try:
                 count, del_count = download_emails(limit=current_batch_limit, month=month_str, remove_on_exist=args.remove)
                 total_downloaded += count
                 total_deleted += del_count
            except RuntimeError as e:
                 print(f"Batch stopping due to error: {e}")
                 break
            except Exception as e:
                 print(f"Batch interrupt: {e}")
                 break
            
            if current_iter.month == 12:
                current_iter = datetime.date(current_iter.year + 1, 1, 1)
            else:
                current_iter = datetime.date(current_iter.year, current_iter.month + 1, 1)
                
    else:
        # Standard Single Mode
        download_emails(limit=args.limit, month=args.month, since=args.since, before=args.before, remove_on_exist=args.remove)

def handle_concentrate(args):
    print("Starting concentration...")
    concentrate_emails(start_year_arg=args.start_year, end_year_arg=args.end_year)

def handle_search(args):
    search_emails(args.query)

def handle_stats(args):
    generate_statistics()


def main():
    parser = argparse.ArgumentParser(description="Email Concentrator CLI")
    subparsers = parser.add_subparsers(dest='command', help='Available commands')

    # Download command
    parser_download = subparsers.add_parser('download', help='Download emails from IMAP')
    parser_download.add_argument('--limit', type=int, default=10, help='Number of emails to fetch (default: 10)')
    parser_download.add_argument('--month', type=str, help='Download specific month (YYYY-MM)')
    parser_download.add_argument('--since', type=str, help='Start date (YYYY-MM-DD)')
    parser_download.add_argument('--before', type=str, help='End date (YYYY-MM-DD)')
    parser_download.add_argument('--start-year', type=int, default=2000, help='Start year for batch mode (Default: 2000)')
    parser_download.add_argument('--start-from', type=str, help='Start month for batch mode (YYYY-MM). Overrides start-year.')
    parser_download.add_argument('--batch-mode', action='store_true', help='Iterate month-by-month from Start Year to Now')
    parser_download.add_argument('--remove', action='store_true', help='Remove email from server if it already exists locally')
    
    parser_concentrate = subparsers.add_parser('concentrate', help='Concentrate emails')
    parser_concentrate.add_argument('--start-year', type=int, help='Start year (inclusive)')
    parser_concentrate.add_argument('--end-year', type=int, help='End year (inclusive)')
    
    parser_search = subparsers.add_parser('search', help='Search concentrated emails')
    parser_search.add_argument('--query', type=str, required=True, help='Search query')
    
    parser_clean = subparsers.add_parser('clean', help='Clean local data')
    parser_clean.add_argument('--concentration', action='store_true', help='Clean only concentration data (keep raw emails)')
    
    parser_flush = subparsers.add_parser('flush', help='Flush remote Concentrated folder')

    parser_upload = subparsers.add_parser('upload', help='Upload concentrated emails to IMAP')
    parser_upload.add_argument('--retry-all', action='store_true', help='Reset all upload status to 0 before uploading')

    parser_stats = subparsers.add_parser('stats', help='Show email statistics')
    
    args = parser.parse_args()

    if args.command == 'download':
        handle_download(args)
    elif args.command == 'concentrate':
        handle_concentrate(args)
    elif args.command == 'upload':
        from uploader import upload_pending_concentrated_emails, reset_upload_status
        if args.retry_all:
             reset_upload_status()
        upload_pending_concentrated_emails()
    elif args.command == 'search':
        handle_search(args)
    elif args.command == 'clean':
        handle_clean(args)
    elif args.command == 'flush':
        handle_flush(args)
    elif args.command == 'stats':
        handle_stats(args)
    else:
        parser.print_help()

if __name__ == "__main__":
    init_db() # Ensure DB is ready
    main()
