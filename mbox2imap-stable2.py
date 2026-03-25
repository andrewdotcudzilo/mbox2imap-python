#!/usr/bin/env python3

import mailbox
import imaplib
import argparse
import email.utils
import email.policy
import email.parser
import re
import io
import sys
from datetime import datetime, timezone


# -----------------------------
# IMAP helpers
# -----------------------------

def normalize_mailbox(name):
    """
    Encode mailbox name using IMAP Modified UTF-7 and quote it
    so spaces and non-ASCII characters are handled correctly.
    """
    try:
        encoded = name.encode("imap4-utf-7").decode("ascii")
    except Exception:
        encoded = name
    return f'"{encoded}"'

# -----------------------------
# Message serialization
# -----------------------------

def get_safe_bytes(msg):
    """
    Safely convert an email.message to raw bytes.
    """
    try:
        return msg.as_bytes()
    except Exception as e:
        print(f"[WARN] as_bytes() failed: {e}")
        try:
            return msg.as_string().encode("utf-8", errors="replace")
        except Exception:
            return b""


# -----------------------------
# Date handling
# -----------------------------

def parse_best_date(msg):
    """
    Determine the best INTERNALDATE for IMAP APPEND.
    Priority:
      1. mbox 'From ' line
      2. Return-Path header
      3. Date header
      4. Current UTC time
    """

    # 1. mbox From_ line
    try:
        from_line = msg.get_unixfrom()
    except Exception:
        from_line = None

    if from_line:
        try:
            parts = from_line.strip().split()
            if len(parts) >= 5:
                date_str = " ".join(parts[-5:])
                dt = datetime.strptime(date_str, "%a %b %d %H:%M:%S %Y")
                if dt.year > 1900:
                    return dt.strftime('"%d-%b-%Y %H:%M:%S +0000"')
        except Exception:
            pass

    # 2. Return-Path header
    return_path = msg.get("Return-Path")
    if return_path:
        match = re.search(
            r'([A-Z][a-z]{2}\s+[A-Z][a-z]{2}\s+\d{1,2}\s+\d{2}:\d{2}:\d{2}\s+\d{4})',
            return_path
        )
        if match:
            try:
                dt = datetime.strptime(match.group(1), "%a %b %d %H:%M:%S %Y")
                return dt.strftime('"%d-%b-%Y %H:%M:%S +0000"')
            except Exception:
                pass

    # 3. Date header
    date_hdr = msg.get("Date")
    if date_hdr:
        try:
            dt = email.utils.parsedate_to_datetime(date_hdr)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.strftime('"%d-%b-%Y %H:%M:%S %z"')
        except Exception:
            pass

    # 4. Fallback
    return datetime.now(timezone.utc).strftime('"%d-%b-%Y %H:%M:%S +0000"')


# -----------------------------
# Migration logic
# -----------------------------

def migrate():
    parser = argparse.ArgumentParser(description="Reliable mbox → IMAP migration")
    parser.add_argument("--mbox", required=True, help="Path to mbox file")
    parser.add_argument("--host", required=True, help="IMAP server")
    parser.add_argument("--user", required=True, help="IMAP username")
    parser.add_argument("--password", required=True, help="IMAP password")
    parser.add_argument("--folder", default="INBOX", help="Target IMAP folder")
    args = parser.parse_args()

    print(f"[{datetime.now().strftime('%H:%M:%S')}] Reading mbox: {args.mbox}")

    msg_parser = email.parser.BytesParser(policy=email.policy.default)
    mbox = mailbox.mbox(args.mbox, factory=msg_parser.parse)

    print(f"[{datetime.now().strftime('%H:%M:%S')}] Connecting to {args.host}")
    server = imaplib.IMAP4_SSL(args.host)
    server.login(args.user, args.password)

    imap_folder = normalize_mailbox(args.folder)

    print(f"[INFO] Using IMAP folder: {args.folder}")
    print(f"[INFO] Encoded folder name: {imap_folder}")

    # Create folder (ignore if it already exists)
    status, _ = server.create(imap_folder)
    if status != "OK":
        print("[INFO] Folder may already exist")

    total = len(mbox)
    count = 0

    print(f"Starting migration of {total} messages...")

    for message in mbox:
        try:
            imap_date = parse_best_date(message)
            msg_bytes = get_safe_bytes(message)

            status, response = server.append(
                imap_folder,
                "()",
                imap_date,
                msg_bytes
            )

            if status != "OK":
                print(f"\n[WARN] APPEND failed on message {count}: {response}")

            count += 1
            if count % 25 == 0:
                sys.stdout.write(f"\rProgress: {count}/{total}")
                sys.stdout.flush()

        except Exception as e:
            print(f"\n[ERROR] Message {count}: {e}")

    print(f"\n\nMigration complete.")
    print(f"Uploaded {count} messages to {args.folder}")

    server.logout()


# -----------------------------
# Entry point
# -----------------------------

if __name__ == "__main__":
    migrate()

