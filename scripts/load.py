"""
load.py — Stage 3: Database Loading
Creates SQLite database and tables, loads clean records using upsert logic,
and logs pipeline runs.
"""

import sqlite3
import os
from datetime import datetime


# Database path
DB_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "jobs.db")


def get_connection():
    """Create and return a database connection."""
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")  # Better concurrent access
    return conn


def create_tables(conn):
    """Create the database tables if they don't exist."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS job_postings (
            control_number TEXT PRIMARY KEY,
            position_title TEXT,
            organization_name TEXT,
            department_name TEXT,
            sub_agency TEXT,
            job_grade TEXT,
            pay_plan TEXT,
            salary_min REAL,
            salary_max REAL,
            salary_mid REAL,
            salary_interval TEXT,
            location_name TEXT,
            city TEXT,
            state TEXT,
            country TEXT,
            latitude REAL,
            longitude REAL,
            position_url TEXT,
            open_date TEXT,
            close_date TEXT,
            role_category TEXT,
            search_keyword TEXT,
            collected_date TEXT
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS pipeline_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_timestamp TEXT,
            records_collected INTEGER,
            records_after_dedup INTEGER,
            records_inserted INTEGER,
            records_skipped INTEGER,
            source TEXT
        )
    """)

    conn.commit()


def load_records(records, source="daily"):
    """
    Load clean records into the database.
    Uses INSERT OR IGNORE to skip duplicates already in the database.
    Returns a summary dict.
    """
    conn = get_connection()
    create_tables(conn)

    columns = [
        "control_number", "position_title", "organization_name",
        "department_name", "sub_agency", "job_grade", "pay_plan",
        "salary_min", "salary_max", "salary_mid", "salary_interval",
        "location_name", "city", "state", "country",
        "latitude", "longitude", "position_url",
        "open_date", "close_date", "role_category",
        "search_keyword", "collected_date"
    ]

    placeholders = ", ".join(["?"] * len(columns))
    column_names = ", ".join(columns)
    sql = f"INSERT OR IGNORE INTO job_postings ({column_names}) VALUES ({placeholders})"

    inserted = 0
    skipped = 0

    for record in records:
        values = tuple(record.get(col) for col in columns)
        cursor = conn.execute(sql, values)
        if cursor.rowcount > 0:
            inserted += 1
        else:
            skipped += 1

    # Log the pipeline run
    conn.execute("""
        INSERT INTO pipeline_log (run_timestamp, records_collected, records_after_dedup,
                                   records_inserted, records_skipped, source)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (
        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        len(records) + skipped,
        len(records),
        inserted,
        skipped,
        source
    ))

    conn.commit()

    # Get total count in database
    total_in_db = conn.execute("SELECT COUNT(*) FROM job_postings").fetchone()[0]
    conn.close()

    print(f"  Records provided: {len(records)}")
    print(f"  New records inserted: {inserted}")
    print(f"  Duplicates skipped: {skipped}")
    print(f"  Total records in database: {total_in_db}")

    return {
        "inserted": inserted,
        "skipped": skipped,
        "total_in_db": total_in_db
    }


# Allow running directly for testing
if __name__ == "__main__":
    from transform import transform_raw_file

    print("Running load test...\n")
    print("Step 1: Transforming raw data...")
    records = transform_raw_file()

    print(f"\nStep 2: Loading {len(records)} records into database...")
    summary = load_records(records)

    print(f"\nDatabase saved to: {DB_PATH}")