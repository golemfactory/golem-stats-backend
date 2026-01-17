#!/usr/bin/env python
import os
import sys
import time

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "core.settings")

import django
django.setup()

from django.core.management import call_command
from django.db import connection, OperationalError


LOCK_ID = 987654321  # Any fixed 64-bit integer, keep it constant


def acquire_lock(wait_seconds=300, retry_interval=5):
    """
    Acquire a PostgreSQL advisory lock.
    Waits until lock is acquired or timeout is reached.
    """
    deadline = time.time() + wait_seconds

    while time.time() < deadline:
        try:
            with connection.cursor() as cursor:
                cursor.execute("SELECT pg_try_advisory_lock(%s);", [LOCK_ID])
                locked = cursor.fetchone()[0]
                if locked:
                    print("✅ Acquired migration lock")
                    return True
                else:
                    print("⏳ Waiting for migration lock...")
        except OperationalError as e:
            print(f"⚠️ Database not ready yet: {e}")

        time.sleep(retry_interval)

    print("❌ Could not acquire migration lock (timeout)")
    return False


def release_lock():
    with connection.cursor() as cursor:
        cursor.execute("SELECT pg_advisory_unlock(%s);", [LOCK_ID])
        print("🔓 Released migration lock")


def main():
    if not acquire_lock():
        sys.exit(1)

    try:
        print("🚀 Running Django migrations")
        call_command("migrate", interactive=False)
    finally:
        release_lock()


if __name__ == "__main__":
    main()
