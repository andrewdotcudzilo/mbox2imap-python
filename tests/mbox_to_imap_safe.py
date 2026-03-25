#!/usr/bin/env python3

import argparse
import mailbox
import imaplib
import ssl
import sys
import email.utils
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue

RETRY_APPEND = 1


def log(msg):
    print(msg, file=sys.stderr, flush=True)


def connect_imap(host, port, user, password, use_ssl):
    if use_ssl:
        ctx = ssl.create_default_context()
        imap = imaplib.IMAP4_SSL(host, port, ssl_context=ctx)
    else:
        imap = imaplib.IMAP4(host, port)

    imap.login(user, password)
    return imap


def safe_internaldate(raw_bytes):
    try:
        msg = email.message_from_bytes(raw_bytes)
        date_hdr = msg.get("Date")
        if not date_hdr:
            return None
        dt = email.utils.parsedate_to_datetime(date_hdr)
        return imaplib.Time2Internaldate(dt.timetuple()) if dt else None
    except Exception:
        return None


def worker(task_queue, args, results):
    """One IMAP connection per worker"""
    try:
        imap = connect_imap(
            args.host, args.port, args.user, args.password, args.ssl
        )
    except Exception as e:
        log(f"[FATAL] Worker IMAP connect failed: {e}")
        return

    while True:
        item = task_queue.get()
        if item is None:
            break

        idx, raw = item
        try:
            internaldate = safe_internaldate(raw)

            for _ in range(RETRY_APPEND + 1):
                try:
                    if internaldate:
                        r = imap.append(args.folder, None, internaldate, raw)
                    else:
                        r = imap.append(args.folder, None, None, raw)

                    if r and r[0] == "OK":
                        results["ok"] += 1
                        break
                except imaplib.IMAP4.abort:
                    imap = connect_imap(
                        args.host, args.port, args.user, args.password, args.ssl
                    )
            else:
                results["fail"] += 1
                log(f"[ERROR] Message {idx}: append failed")

        except Exception as e:
            results["fail"] += 1
            log(f"[ERROR] Message {idx}: {e}")

        finally:
            task_queue.task_done()

    try:
        imap.logout()
    except Exception:
        pass


def main():
    parser = argparse.ArgumentParser(
        description="Parallel Unicode-safe mbox → IMAP importer"
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
    results = {"ok": 0, "fail": 0}

    workers = []
    for _ in range(args.threads):
        t = ThreadPoolExecutor(max_workers=1)
        workers.append(
            t.submit(worker, task_queue, args, results)
        )

    for idx, key in enumerate(keys, 1):
        raw = mbox.get_bytes(key)  # 🔑 NO DECODING EVER
        task_queue.put((idx, raw))

    for _ in range(args.threads):
        task_queue.put(None)

    task_queue.join()

    log(
        f"\nDONE: imported={results['ok']}, "
        f"failed={results['fail']}, total={total}"
    )


if __name__ == "__main__":
    main()

