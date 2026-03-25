#!/usr/bin/env python
import mailbox
import imaplib
import argparse
import email.utils
import email.policy
import re
import io
import sys
from datetime import datetime, timezone
from email.generator import BytesGenerator

def get_safe_bytes(msg):
    """
    Converts mbox message to bytes safely, bypassing ASCII codec errors.
    Uses BytesGenerator with 8-bit safe policy and UTF-8 encoding.
    """
    try:
        fp = io.BytesIO()
        # Use default policy (8-bit safe)
        g = BytesGenerator(fp, policy=email.policy.default)
        g.flatten(msg)
        return fp.getvalue()
    except UnicodeEncodeError:
        # Retry with UTF-8 forced
        fp = io.BytesIO()
        g = BytesGenerator(fp, policy=email.policy.SMTP)
        g.flatten(msg)
        return fp.getvalue()
    except Exception as e:
        print(f"[WARN] Could not flatten message: {e}")
        # fallback: encode headers & body manually
        try:
            return str(msg).encode("utf-8", errors="replace")
        except Exception:
            return b""


def get_safe_bytes(msg):
    """
    Uses the modern 'as_bytes' method which handles 
    non-ASCII characters and binary attachments correctly.
    """
    try:
        # msg.as_bytes() automatically uses the message's policy 
        # (or the default 8-bit safe policy) to generate raw bytes.
        return msg.as_bytes()
    except Exception as e:
        print(f"[WARN] Standard flattening failed: {e}")
        try:
            # Forced fallback for legacy objects
            return msg.as_string().encode('utf-8', errors='replace')
        except Exception:
            # Last resort: raw access to the message's internal buffer if available
            return b""




def parse_best_date(msg):
    """
    Finds the best timestamp for IMAP INTERNALDATE.
    Priority: mbox 'From ' (valid) -> Return-Path -> Date header -> Current.
    """
    # 1. Try mbox separator line
    #from_line = msg.get_from()

    try:
        #from_line = msg.get_from()
        from_line = msg.get_unixfrom()
    except UnicodeDecodeError:
        from_line = None

    if from_line:
        try:
            parts = from_line.strip().split()
            if len(parts) >= 5:
                date_str = " ".join(parts[-5:])
                dt = datetime.strptime(date_str, '%a %b %d %H:%M:%S %Y')
                if dt.year > 1900:
                    return dt.strftime('"%d-%b-%Y %H:%M:%S +0000"')
        except (ValueError, IndexError):
            pass

    # 2. Try Return-Path header (for your Oct 12 2006 case)
    return_path = msg.get('Return-Path')
    if return_path:
        match = re.search(r'([A-Z][a-z]{2}\s+[A-Z][a-z]{2}\s+\d{1,2}\s+\d{2}:\d{2}:\d{2}\s+\d{4})', return_path)
        if match:
            try:
                dt = datetime.strptime(match.group(1), '%a %b %d %H:%M:%S %Y')
                return dt.strftime('"%d-%b-%Y %H:%M:%S +0000"')
            except ValueError:
                pass

    # 3. Fallback to standard Date header
    date_hdr = msg.get('Date')
    if date_hdr:
        try:
            dt = email.utils.parsedate_to_datetime(date_hdr)
            return dt.strftime('"%d-%b-%Y %H:%M:%S %z"')
        except (ValueError, TypeError):
            pass

    # 4. Final fallback: Current time
    return datetime.now(timezone.utc).strftime('"%d-%b-%Y %H:%M:%S +0000"')


import email.parser

def migrate():
    parser = argparse.ArgumentParser(description="Reliable mbox to IMAP Migration Script")
    parser.add_argument("--mbox", required=True, help="Path to .mbox file")
    parser.add_argument("--host", required=True, help="IMAP server (e.g. imap.gmail.com)")
    parser.add_argument("--user", required=True, help="IMAP username")
    parser.add_argument("--password", required=True, help="IMAP password")
    parser.add_argument("--folder", default="INBOX", help="Target folder")
    args = parser.parse_args()

    try:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Reading mbox: {args.mbox}")
        parser = email.parser.BytesParser(policy=email.policy.default)
        mbox = mailbox.mbox(args.mbox, factory=parser.parse)
        
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Connecting to {args.host}...")
        server = imaplib.IMAP4_SSL(args.host)
        server.login(args.user, args.password)
        
        # Ensure folder exists
        server.create(args.folder)
        
        count = 0
        total = len(mbox)
        print(f"Starting migration of {total} messages...")

        for message in mbox:
            try:
                # 1. Get the best possible date string
                imap_date = parse_best_date(message)
                
                # 2. Get bytes safely (avoids 'ascii' codec errors)
                try:
#                    msg_bytes = get_safe_bytes(message)
                    msg_bytes = message.as_bytes()
                except Exception as e:
                    print(f"[!] Failed to get bytes for message {count}: {e}")
                    msg_bytes = b""


                # 3. APPEND with empty flags '()' to keep as Unseen
                status, response = server.append(args.folder, '()', imap_date, msg_bytes)
                
                if status != 'OK':
                    print(f"\n[!] Server rejected message {count}: {response}")
                
                count += 1
                if count % 25 == 0:
                    sys.stdout.write(f"\rProgress: {count}/{total} uploaded...")
                    sys.stdout.flush()

            except Exception as e:
                print(f"\n[!] Error on message {count}: {e}")

        print(f"\n\nMigration Complete!")
        print(f"Successfully uploaded {count} messages to '{args.folder}'.")
        server.logout()

    except Exception as e:
        print(f"\nCRITICAL ERROR: {e}")
        sys.exit(1)

if __name__ == "__main__":
    migrate()

