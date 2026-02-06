import imaplib

def decode_utf7(s):
    # Modified UTF-7 decoding for IMAP
    # & is used instead of +
    # , is used instead of /
    s_utf7 = s.replace('&', '+').replace(',', '/')
    try:
        # Convert to bytes (ascii) then decode using standard utf-7
        return s_utf7.encode('ascii').decode('utf-7')
    except Exception as e:
        return f"{s} (Error: {e})"

folders = [
    "&g0l6P3ux-", 
    "&XfJT0ZAB-", 
    "&XfJSIJZk-", 
    "&V4NXPpCuTvY-", 
    "&dcVr0mWHTvZZOQ-", 
    "&Xn9USpCuTvY-", 
    "&i6KWBZCuTvY-"
]

print("Decoding folder names:")
for f in folders:
    print(f"{f} -> {decode_utf7(f)}")
