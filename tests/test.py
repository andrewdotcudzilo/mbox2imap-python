import mailbox
import imaplib
import argparse
import email.utils
from datetime import datetime
import time
import sys


from datetime import datetime, timezone  # Add timezone to your imports

def parse_mbox_from_date(from_line):
    try:
        parts = from_line.strip().split()
        if len(parts) >= 5:
            date_str = " ".join(parts[-5:])
            # 1. Parse into a naive datetime
            dt = datetime.strptime(date_str, '%a %b %d %H:%M:%S %Y')
            # 2. Make it timezone-aware (set to UTC)
            return dt.replace(tzinfo=timezone.utc)
    except (ValueError, IndexError):
        return None


def migrate():
    parser = argparse.ArgumentParser(description="Migrate mbox to IMAP with date priority.")
    parser.add_argument("--mbox", required=True, help="Path to .mbox file")
    parser.add_argument("--host", required=True, help="IMAP server")
    parser.add_argument("--user", required=True, help="Username")
    parser.add_argument("--password", required=True, help="Password")
    parser.add_argument("--folder", default="INBOX", help="Target folder")
    args = parser.parse_args()

    try:
        mbox = mailbox.mbox(args.mbox)
        print(f"Connecting to {args.host}...")
        server = imaplib.IMAP4_SSL(args.host)
        server.login(args.user, args.password)
        server.create(args.folder)

        count = 0
        total = len(mbox)
        for message in mbox:
            try:
                # 1. Try 'From ' separator line first
                internal_dt = parse_mbox_from_date(message.get_from())
                
                # 2. Fallback to 'Date:' header if separator date fails
                if not internal_dt and message.get('Date'):
                    internal_dt = email.utils.parsedate_to_datetime(message.get('Date'))

                # Convert to IMAP INTERNALDATE format
                if internal_dt:
                    imap_date = imaplib.Time2Internaldate(internal_dt)
                else:
                    imap_date = imaplib.Time2Internaldate(time.time())

                # Upload with empty flags () to keep as Unseen
                server.append(args.folder, '()', imap_date, message.as_bytes())
                
                count += 1
                if count % 20 == 0:
                    print(f"Uploaded {count}/{total}...")
            except Exception as e:
                print(f"Error on message {count}: {e}")

        print(f"\nFinished. {count} messages migrated to {args.folder}.")
        server.logout()

    except Exception as e:
        print(f"Critical failure: {e}")
        sys.exit(1)

if __name__ == "__main__":
    migrate()

