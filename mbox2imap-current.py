#!/usr/bin/env python3

import os
import mailbox
import imaplib
import argparse
import email.utils
import email.policy
import email.parser
import re
import sys
from datetime import datetime, timezone


# --------------------------------------------------
# IMAP helpers
# --------------------------------------------------

def normalize_mailbox(name):
    try:
        encoded = name.encode("imap4-utf-7").decode("ascii")
    except Exception:
        encoded = name
    return f'"{encoded}"'


def create_mailbox_tree(server, mailbox, dry_run=False):
    """
    Create mailbox and all parents. Subscribe each folder.
    """
    parts = mailbox.split('/')
    current = ""

    for part in parts:
        current = f"{current}/{part}" if current else part
        imap_box = normalize_mailbox(current)

        if dry_run:
            print(f"[DRY-RUN] CREATE {current}")
            print(f"[DRY-RUN] SUBSCRIBE {current}")
        else:
            server.create(imap_box)
            server.subscribe(imap_box)


# --------------------------------------------------
# Message handling
# --------------------------------------------------

def get_safe_bytes(msg):
    try:
        return msg.as_bytes()
    except Exception:
        try:
            return msg.as_string().encode("utf-8", errors="replace")
        except Exception:
            return b""


def parse_best_date(msg):
    try:
        from_line = msg.get_unixfrom()
    except Exception:
        from_line = None

    if from_line:
        try:
            parts = from_line.strip().split()
            if len(parts) >= 5:
                dt = datetime.strptime(
                    " ".join(parts[-5:]),
                    "%a %b %d %H:%M:%S %Y"
                )
                return dt.strftime('"%d-%b-%Y %H:%M:%S +0000"')
        except Exception:
            pass

    date_hdr = msg.get("Date")
    if date_hdr:
        try:
            dt = email.utils.parsedate_to_datetime(date_hdr)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.strftime('"%d-%b-%Y %H:%M:%S %z"')
        except Exception:
            pass

    return datetime.now(timezone.utc).strftime('"%d-%b-%Y %H:%M:%S +0000"')


# --------------------------------------------------
# Core migration logic
# --------------------------------------------------

def import_mbox(server, mbox_path, imap_folder, dry_run=False):
    parser = email.parser.BytesParser(policy=email.policy.default)
    mbox = mailbox.mbox(mbox_path, factory=parser.parse)

    print(f"\n[MAP] {mbox_path}")
    print(f"      → {imap_folder}")
    print(f"      Messages: {len(mbox)}")

    create_mailbox_tree(server, imap_folder, dry_run=dry_run)

    if dry_run:
        return

    imap_box = normalize_mailbox(imap_folder)

    for msg in mbox:
        try:
            date = parse_best_date(msg)
            data = get_safe_bytes(msg)
            server.append(imap_box, "()", date, data)
        except Exception as e:
            print(f"[WARN] Message failed: {e}")


def walk_mbox_tree(server, root_dir, imap_root, dry_run=False):
    for dirpath, _, filenames in os.walk(root_dir):
        rel = os.path.relpath(dirpath, root_dir)
        parts = [] if rel == "." else rel.split(os.sep)
        parts = [p for p in parts if p != "Mail"]

        for filename in filenames:
            if not filename.lower().endswith(".mbox"):
                continue

            mbox_name = os.path.splitext(filename)[0]

            # Avoid duplicate leaf folder
            if parts and parts[-1] == mbox_name:
                imap_parts = [imap_root] + parts
            else:
                imap_parts = [imap_root] + parts + [mbox_name]

            imap_folder = "/".join(imap_parts)
            full_path = os.path.join(dirpath, filename)

            import_mbox(server, full_path, imap_folder, dry_run=dry_run)


# --------------------------------------------------
# Entry point
# --------------------------------------------------

def main():
    ap = argparse.ArgumentParser(description="Recursive mbox → IMAP importer")
    ap.add_argument("--mbox-root", required=True)
    ap.add_argument("--imap-root", default="INBOX")
    ap.add_argument("--host", required=True)
    ap.add_argument("--user", required=True)
    ap.add_argument("--password", required=True)
    ap.add_argument("--dry-run", action="store_true", help="Show actions only")
    args = ap.parse_args()

    if args.dry_run:
        print("[INFO] DRY-RUN mode enabled")

    server = None
    if not args.dry_run:
        server = imaplib.IMAP4_SSL(args.host)
        server.login(args.user, args.password)

    walk_mbox_tree(
        server,
        args.mbox_root,
        args.imap_root,
        dry_run=args.dry_run
    )

    if server:
        server.logout()

    print("\nDone.")


if __name__ == "__main__":
    main()

