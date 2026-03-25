#!/usr/bin/env python3

import mailbox
import imaplib
import email
import sys
import ssl
from datetime import datetime

# ------------- CONFIGURATION -------------

IMAP_HOST = "imap.siteprotect.com"
IMAP_PORT = 993
IMAP_USER = "cbicorecom@andrewcudzilo.com"
IMAP_PASS = "r&PqFwhoUf9xC"
IMAP_FOLDER = "Trash"   # Target folder

MBOX_FILE = "/home/andrew/Downloads/tmp/temp/admin/Mail/Deleted Items/Deleted Items.mbox"

USE_SSL = True

# -----------------------------------------


def connect_imap():
    if USE_SSL:
        context = ssl.create_default_context()
        imap = imaplib.IMAP4_SSL(IMAP_HOST, IMAP_PORT, ssl_context=context)
    else:
        imap = imaplib.IMAP4(IMAP_HOST, IMAP_PORT)

    imap.login(IMAP_USER, IMAP_PASS)
    return imap


def parse_date(msg):
    """Return IMAP-compatible INTERNALDATE"""
    date_hdr = msg.get("Date")
    if not date_hdr:
        return None

    try:
        parsed = email.utils.parsedate_to_datetime(date_hdr)
        return imaplib.Time2Internaldate(parsed.timetuple())
    except Exception:
        return None


def import_mbox(imap):
    mbox = mailbox.mbox(MBOX_FILE, factory=None)
    total = len(mbox)
    imported = 0

    for i, msg in enumerate(mbox, 1):
        try:
            if isinstance(msg, mailbox.mboxMessage):
                msg = msg.as_bytes()
            else:
                msg = msg.as_bytes()

            internaldate = parse_date(email.message_from_bytes(msg))

            if internaldate:
                result = imap.append(IMAP_FOLDER, None, internaldate, msg)
            else:
                result = imap.append(IMAP_FOLDER, None, None, msg)

            if result[0] != "OK":
                print(f"[ERROR] Message {i}/{total} append failed: {result}")
            else:
                imported += 1

            if i % 100 == 0:
                print(f"Imported {i}/{total}")

        except Exception as e:
            print(f"[ERROR] Message {i}/{total}: {e}")

    print(f"\nDone. Imported {imported}/{total} messages.")


def main():
    try:
        imap = connect_imap()
        import_mbox(imap)
    finally:
        try:
            imap.logout()
        except Exception:
            pass


if __name__ == "__main__":
    main()

