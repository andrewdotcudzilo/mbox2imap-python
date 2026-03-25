#!/usr/bin/env python3

import argparse
import mailbox
import imaplib
import ssl
import sys
import email.utils

RETRY_APPEND = 1


def log(msg):
    print(msg, file=sys.stderr, flush=True)


def connect_imap(host, port, user, password, use_ssl):
    try:
        if use_ssl:
            ctx = ssl.create_default_context()
            imap = imaplib.IMAP4_SSL(host, port, ssl_context=ctx)
        else:
            imap = imaplib.IMAP4(host, port)

        imap.login(user, password)
        return imap
    except Exception as e:
        log(f"[FATAL] IMAP connection failed: {e}")
        sys.exit(1)


def safe_internaldate(raw_bytes):
    """Best-effort INTERNALDATE extraction"""
    try:
        msg = email.message_from_bytes(raw_bytes)
        date_hdr = msg.get("Date")
        if not date_hdr:
            return None

        dt = email.utils.parsedate_to_datetime(date_hdr)
        if not dt:
            return None

        return imaplib.Time2Internaldate(dt.timetuple())
    except Exception:
        return None


def safe_append(imap, folder, raw_msg, internaldate, reconnect_fn):
    for _ in range(RETRY_APPEND + 1):
        try:
            if internaldate:
                return imap.append(folder, None, internaldate, raw_msg)
            else:
                return imap.append(folder, None, None, raw_msg)

        except imaplib.IMAP4.abort:
            log("[WARN] IMAP aborted, reconnecting")
            imap = reconnect_fn()

        except imaplib.IMAP4.error as e:
            log(f"[ERROR] IMAP append failed: {e}")
            break

    return None


def import_mbox(imap, mbox_path, folder, reconnect_fn):
    try:
        mbox = mailbox.mbox(mbox_path, create=False)
    except Exception as e:
        log(f"[FATAL] Cannot open mbox file: {e}")
        sys.exit(1)

    keys = mbox.keys()
    total = len(keys)
    imported = 0
    failed = 0

    for idx, key in enumerate(keys, 1):
        try:
            # 🔑 RAW BYTES — NO DECODING
            raw = mbox.get_bytes(key)

            internaldate = safe_internaldate(raw)
            result = safe_append(imap, folder, raw, internaldate, reconnect_fn)

            if not result or result[0] != "OK":
                failed += 1
                log(f"[ERROR] Message {idx}: append failed")
                continue

            imported += 1

            if idx % 100 == 0:
                log(f"Imported {idx}/{total}")

        except Exception as e:
            failed += 1
            log(f"[ERROR] Message {idx}: {e}")

    log(f"\nDONE: imported={imported}, failed={failed}, total={total}")


def main():
    parser = argparse.ArgumentParser(
        description="Import an mbox file into a remote IMAP folder (Unicode-safe)"
    )

    parser.add_argument("--host", required=True, help="IMAP server hostname")
    parser.add_argument("--port", type=int, default=993, help="IMAP server port")
    parser.add_argument("--user", required=True, help="IMAP username")
    parser.add_argument("--password", required=True, help="IMAP password")
    parser.add_argument("--mbox", required=True, help="Local mbox file path")
    parser.add_argument("--folder", required=True, help="Remote IMAP folder")
    parser.add_argument("--ssl", action="store_true", help="Use IMAP over SSL")

    args = parser.parse_args()

    imap = None

    def reconnect():
        return connect_imap(
            args.host, args.port, args.user, args.password, args.ssl
        )

    try:
        imap = reconnect()
        import_mbox(imap, args.mbox, args.folder, reconnect)
    finally:
        try:
            if imap:
                imap.logout()
        except Exception:
            pass


if __name__ == "__main__":
    main()

