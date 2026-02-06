import os
import sqlite3
from flask import Flask, render_template, request, g
import datetime
from db import get_db_connection
from identity import get_email_address_and_name, get_cached_identity_full

app = Flask(__name__)

def get_db():
    if 'db' not in g:
        g.db = get_db_connection()
    return g.db

@app.teardown_appcontext
def close_db(error):
    db = g.pop('db', None)
    if db is not None:
        db.close()

def get_sidebar_data():
    """Fetch data for the sidebar: Year/Month tree and Top Senders."""
    db = get_db()
    c = db.cursor()
    
    # 1. Date Tree
    # Fix: Date column contains raw email date strings (e.g. 'Thu, 3 Feb...').
    # We must parse them in Python to group correctly. SQL substr won't work reliably.
    
    c.execute("SELECT date FROM emails")
    rows = c.fetchall()
    
    date_tree = {}
    
    from email.utils import parsedate_to_datetime
    
    for r in rows:
        d_str = r[0]
        if not d_str: continue
        try:
            # Parse RFC2822 date
            dt = parsedate_to_datetime(d_str)
            y = str(dt.year)
            m = f"{dt.month:02d}" # Zero pad
            
            if y not in date_tree: date_tree[y] = {}
            if m not in date_tree[y]: date_tree[y][m] = 0
            date_tree[y][m] += 1
        except:
             continue
    
    # Convert dict to sorted list structure
    sorted_tree = {}
    for y in sorted(date_tree.keys(), reverse=True):
        sorted_tree[y] = []
        for m in sorted(date_tree[y].keys(), reverse=True):
            sorted_tree[y].append({'month': m, 'count': date_tree[y][m]})
            
    # 2. Top Senders
        
    # 2. Top Senders
    # Aggregate by raw sender first
    c.execute("SELECT sender, count(*) as cnt FROM emails GROUP BY sender HAVING cnt > 0 ORDER BY cnt DESC LIMIT 500")
    raw_senders = c.fetchall()
    
    # Aggregate by clean email in Python
    sender_stats = {}
    for r in raw_senders:
        raw_s = r[0]
        cnt = r[1]
        name, email = get_email_address_and_name(raw_s)
        if not email: continue
        
        if email not in sender_stats:
            sender_stats[email] = {'count': 0, 'raw_name': name}
        sender_stats[email]['count'] += cnt
        
    # Sort by count
    sorted_emails = sorted(sender_stats.items(), key=lambda x: x[1]['count'], reverse=True)[:20]
    
    top_senders = []
    for email, data in sorted_emails:
        # Resolve best name (Cached Identity)
        c_name, _, _ = get_cached_identity_full(email)
        display_name = c_name if c_name else data['raw_name']
        if not display_name: display_name = email
        
        top_senders.append({'email': email, 'name': display_name, 'count': data['count']})
        
    return sorted_tree, top_senders
        




import re
def clean_filename(s):
    """Sanitize string (Duplicated from downloader.py for path matching)."""
    s = str(s).strip().replace('"', '').replace("'", "").replace("\n", "").replace("\r", "")
    s = str(s).strip().replace('"', '').replace("'", "").replace("\n", "").replace("\r", "")
    return re.sub(r'[<>:"/\\|?*]', '_', s)

@app.route('/')
def index():
    query = request.args.get('q', '').strip()
    start_date = request.args.get('start_date', '')
    end_date = request.args.get('end_date', '')
    sender_filter = request.args.get('sender_email', '').strip()
    
    limit = 100
    
    # Fetch all for Python-side sorting/filtering (Date format issue)
    # OR: Try to utilize SQLite if we can. 
    # But current 'date' column is messy text. Sorting 'Thu, ...' vs 'Wed, ...' is wrong.
    # To fix SORTING, we need to fetch all and sort in Python, or add a proper column.
    # For 100 limit, fetching all is okay if DB is small (<10k). 
    # If DB is large, this will be slow.
    # BEST FIX: Use 'id' DESC as proxy for date (assuming download order ~ date order). 
    # downloader.py usually downloads mostly in order or we can rely on it.
    # BUT user insisted on "descending order of dates".
    # Let's try to fetch a larger batch, sort in python, slice.
    
    sql = "SELECT * FROM emails WHERE 1=1"
    params = []
    
    if query:
        sql += " AND (subject LIKE ? OR sender LIKE ? OR message_id LIKE ?)"
        wildcard_query = f"%{query}%"
        params.extend([wildcard_query, wildcard_query, wildcard_query])
        
    if sender_filter:
        # FILTER SENDER & RECIPIENT (Conversation View)
        # Use local_path trick: the folder name contains the other party's email.
        # This covers both emails FROM them (folder=sender) and emails TO them (folder=recipient).
        clean_email = clean_filename(sender_filter)
        
        # We search if local_path contains that clean folder name
        # Windows path: ...\clean_email\filename.eml
        # Linux: .../clean_email/filename.eml
        # So we look for path separator + clean_email + path separator
        # match %sep%clean_email%sep%
        
        # Simpler: just "%clean_email%" but make sure it's valid
        if clean_email:
             sql += " AND local_path LIKE ?"
             params.append(f"%{clean_email}%")
        else:
             # Fallback to sender field if name cleaning fails
             sql += " AND sender LIKE ?"
             params.append(f"%{sender_filter}%")

        
    if start_date:
        sql += " AND date >= ?"
        params.append(start_date)
        
    # Remove SQL Order/Limit for now, do it in Python to fix Date Sort
    # sql += " ORDER BY date DESC LIMIT ?" 
    # params.append(limit)
    
    db = get_db()
    cursor = db.cursor()
    cursor.execute(sql, params)
    raw_emails = cursor.fetchall()
    
    # Python-side Date Parsing & Sorting
    from email.utils import parsedate_to_datetime
    
    all_results = []
    for r in raw_emails:
        # Convert row to dict for mutability
        item = dict(r)
        
        # Parse Date
        try:
             dt = parsedate_to_datetime(item['date'])
             
             # Ensure Timezone Awareness to prevent sort errors
             if dt.tzinfo is None:
                 dt = dt.replace(tzinfo=datetime.timezone.utc)
             
             # Filter by Date Range (if set) in Python since SQL format is raw text
             if start_date:
                  sd_obj = datetime.datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=dt.tzinfo)
                  if dt < sd_obj: continue
             if end_date:
                  ed_obj = datetime.datetime.strptime(end_date, "%Y-%m-%d").replace(tzinfo=dt.tzinfo)
                  # Inclusive of end day?
                  ed_obj = ed_obj + datetime.timedelta(days=1)
                  if dt >= ed_obj: continue
                  
             item['_dt'] = dt
             item['display_date'] = str(dt) # standardized display
        except:
             # Fallback
             item['_dt'] = datetime.datetime.min.replace(tzinfo=datetime.timezone.utc)
             item['display_date'] = item['date']
             
        all_results.append(item)
        
    # Sort with safety for mixed types (though we tried to fix above)
    all_results.sort(key=lambda x: x['_dt'], reverse=True)
    
    # Pagination / Limit
    display_slice = all_results[:limit]
    
    display_emails = []
    for r in display_slice:
        raw_s = r['sender']
        _, email_addr = get_email_address_and_name(raw_s)
        
        c_name, _, _ = get_cached_identity_full(email_addr)
        best_name = c_name
        
        if not best_name:
             n, _ = get_email_address_and_name(raw_s)
             best_name = n if n else email_addr
             
        display_emails.append({
            'id': r['id'],
            'date': r['display_date'], # Clean ISO-ish string
            'subject': r['subject'],
            'sender_name': best_name,
            'sender_email': email_addr,
            'local_path': r['local_path']
        })

    # Sidebar Data
    date_tree, top_senders = get_sidebar_data()
    
    return render_template('index.html', 
                           emails=display_emails, 
                           query=query, 
                           start_date=start_date, 
                           end_date=end_date,
                           sender_filter=sender_filter,
                           date_tree=date_tree,
                           top_senders=top_senders)

if __name__ == '__main__':
    print("Starting Flask with Sidebar & Identity...")
    app.run(debug=True, port=5000, host='0.0.0.0')
