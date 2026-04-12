#!/usr/bin/env python3

import argparse
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from shared.utils.log_utils import cleanup_old_logs


def main():
    parser = argparse.ArgumentParser(description="Delete old log files from logs/.")
    parser.add_argument("--days", type=int, default=7, help="Retain only the last N days of logs")
    args = parser.parse_args()
    deleted = cleanup_old_logs(retention_days=args.days)
    print(f"Deleted {deleted} old log file(s).")


if __name__ == "__main__":
    main()
