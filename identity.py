import json
import sqlite3
import re
import email.utils
from email.header import decode_header
import ollama
from db import get_db_connection
from config import load_config

# In-Memory Cache
# Key: email_address, Value: (name, seen_names_list, source)
_IDENTITY_CACHE = {}

def decode_mime_words(s):
    """Decode MIME encoded words."""
    if not s:
        return ""
    decoded_list = decode_header(s)
    text = ""
    for decoded_bytes, charset in decoded_list:
        if isinstance(decoded_bytes, bytes):
            if charset:
                try:
                    text += decoded_bytes.decode(charset)
                except (LookupError, UnicodeDecodeError):
                    # Fallback logic
                    try:
                        # Common issue: gb2312 labels often contain gbk/gb18030 chars
                        if charset and 'gb' in charset.lower():
                            text += decoded_bytes.decode('gb18030', errors='replace')
                        else:
                            text += decoded_bytes.decode('utf-8', errors='replace')
                    except:
                         text += decoded_bytes.decode('utf-8', errors='replace')
            else:
                # No charset specified, usually ascii or utf-8
                text += decoded_bytes.decode('utf-8', errors='replace')
        else:
            text += str(decoded_bytes)
    return text

def get_email_address_and_name(sender_str):
    """Parse 'Name <email@domain.com>' into ('Name', 'email@domain.com')."""
    # decode likely mime encoded string first
    decoded_sender = decode_mime_words(sender_str)
    real_name, email_addr = email.utils.parseaddr(decoded_sender)
    
    # Normalization
    email_addr = email_addr.strip().lower()
    real_name = real_name.strip()
    
    # If name is empty, try to use the part before @ in email
    if not real_name and '@' in email_addr:
        real_name = email_addr.split('@')[0]
        
    return real_name, email_addr

def contains_chinese(text):
    """Check if the text contains any Chinese characters."""
    if not text:
        return False
    for char in text:
        if '\u4e00' <= char <= '\u9fff':
            return True
    return False

def call_ollama_decision(name1, name2):
    """
    Ask Ollama (qwen3) which name is more descriptive.
    Returns: The selected name (name1 or name2).
    """
    if not name1: return name2
    if not name2: return name1
    
    prompt = f"Which name is more descriptive for a person or entity? Option A: '{name1}', Option B: '{name2}'. Answer with only 'A' or 'B'."
    
    try:
        # Use ollama python library with specific host
        config = load_config()
        ollama_host = config.get('ollama_url', 'http://localhost:11434')
        client = ollama.Client(host=ollama_host)
        response = client.generate(model='qwen3', prompt=prompt)
        answer = response['response'].strip().upper()
            
        # Simple parsing of the answer
        if 'A' in answer and 'B' not in answer:
            return name1
        elif 'B' in answer and 'A' not in answer:
            return name2
        else:
            # Fallback: prefer longer name if ambiguous
            return name1 if len(name1) >= len(name2) else name2
    except Exception as e:
        print(f"Ollama call failed: {e}")
        # Fallback
        return name1 if len(name1) >= len(name2) else name2

def is_valid_name(name, email_addr):
    """
    Return False if name is just the email or the account part.
    """
    if not name:
        return False
    
    name_check = name.strip().lower()
    email_check = email_addr.strip().lower()
    
    if name_check == email_check:
        return False
        
    if '@' in email_check:
        account_part = email_check.split('@')[0]
        if name_check == account_part:
            return False
            
    return True

def get_better_name(current_name, candidate_name, email_addr, current_source=0, candidate_source=0):
    """
    Decide which name is better.
    Priority: Source 1 > Source 0 (Sent > Received).
    If sources equal:
      - Valid > Invalid
      - Chinese > Non-Chinese
      - LLM > Length
    """
    # 0. Source Check (Strict Priority for Sent Names)
    # Only if candidate is valid though. Invalid Sent name shouldn't overwrite valid Received name if it's junk.
    # But let's assume is_valid_name handles junk.
    
    cand_valid = is_valid_name(candidate_name, email_addr)
    curr_valid = is_valid_name(current_name, email_addr)
    
    if not cand_valid: return current_name
    if not curr_valid: return candidate_name
    
    # If sources differ
    if candidate_source > current_source:
        return candidate_name
    if current_source > candidate_source:
        return current_name
        
    # Sources are equal: Tie Breaking
    
    # 1. Exact Match
    if current_name.strip() == candidate_name.strip():
        return current_name
        
    # 2. Chinese Priority
    curr_cn = contains_chinese(current_name)
    cand_cn = contains_chinese(candidate_name)
    
    if cand_cn and not curr_cn:
        return candidate_name
    if curr_cn and not cand_cn:
        return current_name
        
    # 3. LLM Decision (if both are non-chinese or both are chinese)
    # Only call LLM if they are sufficiently different
    print(f"Asking LLM: '{current_name}' vs '{candidate_name}'")
    return call_ollama_decision(current_name, candidate_name)

def get_cached_identity_full(email):
    """Get name, seen_names, and name_source from DB or Memory Cache."""
    global _IDENTITY_CACHE
    
    if email in _IDENTITY_CACHE:
        return _IDENTITY_CACHE[email]
        
    conn = get_db_connection()
    c = conn.cursor()
    c.execute("SELECT name, seen_names, name_source FROM email_identities WHERE email = ?", (email,))
    row = c.fetchone()
    conn.close()
    
    if row:
        name = row[0]
        try:
            seen = json.loads(row[1]) if row[1] else []
        except:
            seen = []
        source = row[2] if row[2] is not None else 0
        
        # Populate Cache
        _IDENTITY_CACHE[email] = (name, seen, source)
        return name, seen, source
        
    return None, [], 0

def update_cached_identity(email, name, seen_names=None, name_source=0):
    """Update DB and Memory Cache."""
    global _IDENTITY_CACHE
    
    conn = get_db_connection()
    c = conn.cursor()
    
    # Ensure current seen_names/source are preserved or updated if passed as None (though caller usually passes full)
    # But since we use cache, we assume caller has latest.
    if seen_names is None:
        seen_names = []
            
    seen_json = json.dumps(seen_names)
    
    c.execute('''
        INSERT INTO email_identities (email, name, seen_names, name_source) 
        VALUES (?, ?, ?, ?)
        ON CONFLICT(email) DO UPDATE SET 
            name=excluded.name, 
            seen_names=excluded.seen_names, 
            name_source=excluded.name_source,
            updated_at=CURRENT_TIMESTAMP
    ''', (email, name, seen_json, name_source))
    conn.commit()
    conn.close()
    
    # Update Cache
    _IDENTITY_CACHE[email] = (name, seen_names, name_source)

def process_identity(email_addr, raw_name, source_type=0):
    """
    Main entry point to process an identity observation.
    source_type: 0 for Received (From), 1 for Sent (To)
    """
    if not email_addr: return
    
    cached_name, seen_names, cached_source = get_cached_identity_full(email_addr)
    
    # Logic to decide if we need to evaluate
    # If raw_name is present and new to us -> Evaluate
    # If source is better -> Evaluate
    # If it's a NEW email address we've never seen (cached_name is None), update/create it regardless of name presence?
    # The user requested "make sure all 'seen' email addresses are saved". 
    # So if it's new, we save it (even with empty name).
    
    is_new_email = (cached_name is None)
    
    should_evaluate = (raw_name and raw_name not in seen_names) or (source_type > cached_source)
    
    if should_evaluate or is_new_email:
        # Try to refine
        better_name = get_better_name(cached_name, raw_name, email_addr, cached_source, source_type)
        
        # Update source if we picked the new name
        # If better_name is same as raw_name (and raw_name is valid), usage new source
        # If better_name is same as cached_name, use cached_source
        
        new_source = cached_source
        
        # Fallback for None names
        if better_name is None: better_name = ""
        if cached_name is None: cached_name = ""
        
        if better_name == raw_name:
             new_source = source_type
        
        # record it as seen (if not already)
        if raw_name and raw_name not in seen_names:
            seen_names.append(raw_name)
        
        if better_name != cached_name:
            print(f"Updating identity for {email_addr}: {cached_name} (Src:{cached_source}) -> {better_name} (Src:{new_source})")
            cached_name = better_name
            cached_source = new_source
        elif is_new_email:
             # First time seen, just save
             cached_name = better_name
             cached_source = new_source
        
        # Save update (name + seen list + source)
        update_cached_identity(email_addr, cached_name, seen_names, cached_source)
