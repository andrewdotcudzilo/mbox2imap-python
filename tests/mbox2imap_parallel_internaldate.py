#!/usr/bin/env python3
import argparse
import mailbox
import imaplib
import ssl
import sys
import datetime
import email
import email.utils
from queue import Queue
from threading import Thread

RETRY_APPEND = 1


def log(msg):
    print(msg, file=sys.stderr, flush=True)


# ---------------- IMAP ----------------
def connect_imap(host, port, user, password, use_ssl):
    if use_ssl:
        ctx = ssl.create_default_context()
        imap = imaplib.IMAP4_SSL(host, port, ssl_context=ctx)
    else:
        imap = imaplib.IMAP4(host, port)
    imap.login(user, password)
    return imap


# -------- INTERNALDATE FROM Date: HEADER --------
def internaldate_from_date_header(raw_bytes):
    """
    Parse the Date: header and return RFC 3501 INTERNALDATE string
    Returns None if parsing fails (fallback to server default INTERNALDATE)
    """
    try:
        msg = email.message_from_bytes(raw_bytes)
        date_hdr = msg.get("Date")
        if not date_hdr:
            return None
        if not isinstance(date_hdr, str):
            date_hdr = str(date_hdr)

        # Strip comments like "(UTC)"
        date_hdr = email.utils.strip_comments(date_hdr)

        # Try modern parser first
        try:
            dt = email.utils.parsedate_to_datetime(date_hdr)
        except (TypeError, ValueError):
            dt = None

        # Fallback parser
        if dt is None:
            parsed = email.utils.parsedate_tz(date_hdr)
            if not parsed:
                return None
            ts = email.utils.mktime_tz(parsed)
            dt = datetime.datetime.fromtimestamp(ts, datetime.timezone.utc)

        # Ensure tz-aware
        if dt.tzinfo is None:
            dt = dt.astimezone()

        # RFC 3501 INTERNALDATE format
        return dt.strftime("%d-%b-%Y %H:%M:%S %z")

    except Exception:
        return None


# ---------------- WORKER ----------------
def worker(task_queue, args, mbox, stats):
    try:
        imap = connect_imap(args.host, args.port, args.user, args.password, args.ssl)
    except Exception as e:
        log(f"[FATAL] IMAP connection failed: {e}")
        return

    while True:
        item = task_queue.get()
        if item is None:
            break
        idx, key = item

        try:
            raw = mbox.get_bytes(key)
            internaldate = internaldate_from_date_header(raw)

            for _ in range(RETRY_APPEND + 1):
                try:
                    # Pass INTERNALDATE as-is, no manual quoting
                    r = imap.append(args.folder, None, internaldate, raw)
                    if r and r[0] == "OK":
                        stats["ok"] += 1
                        break
                except imaplib.IMAP4.abort:
                    log("[WARN] IMAP aborted, reconnecting...")
                    imap = connect_imap(args.host, args.port, args.user, args.password, args.ssl)
            else:
                stats["fail"] += 1
                log(f"[ERROR] Message {idx}: append failed")

        except Exception as e:
            stats["fail"] += 1
            log(f"[ERROR] Message {idx}: {e}")
        finally:
            task_queue.task_done()

    try:
        imap.logout()
    except Exception:
        pass


# ---------------- MAIN ----------------
def main():
    parser = argparse.ArgumentParser(
        description="Threaded mbox → IMAP importer (INTERNALDATE = Date header)"
    )
    parser.add_argument("--host", required=True)
    parser.add_argument("--port", type=int, default=993)
    parser.add_argument("--user", required=True)
    parser.add_argument("--password", required=True)
    parser.add_argument("--mbox", required=True)
    parser.add_argument("--folder", required=True)
    parser.add_argument("--ssl", action="store_true")
    parser.add_argument("--threads", type=int, default=4)
    args = parser.parse_args()

    try:
        mbox = mailbox.mbox(args.mbox, create=False)
    except Exception as e:
        log(f"[FATAL] Cannot open mbox: {e}")
        sys.exit(1)

    keys = mbox.keys()
    total = len(keys)
    task_queue = Queue(maxsize=args.threads * 2)
    stats = {"ok": 0, "fail": 0}

    threads = []
    for _ in range(args.threads):
        t = Thread(target=worker, args=(task_queue, args, mbox, stats), daemon=True)
        t.start()
        threads.append(t)

    for idx, key in enumerate(keys, 1):
        task_queue.put((idx, key))

    for _ in threads:
        task_queue.put(None)

    task_queue.join()
    log(f"\nDONE: imported={stats['ok']} failed={stats['fail']} total={total}")


if __name__ == "__main__":
    main()

