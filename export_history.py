"""
export_history.py
Exports the SQLite database tables to CSV files for external analysis.
"""
import sqlite3
import pandas as pd
import os
import logging

DB_PATH = "serp_data.db"
EXPORT_DIR = "exports"


def export_tables():
    if not os.path.exists(DB_PATH):
        print(f"Database {DB_PATH} not found.")
        return

    os.makedirs(EXPORT_DIR, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)

    tables = [
        "runs",
        "keywords",
        "serp_results",
        "url_features",
        "domain_features"
    ]

    print(f"Exporting tables to '{EXPORT_DIR}/'...")

    for table in tables:
        try:
            df = pd.read_sql(f"SELECT * FROM {table}", conn)
            if not df.empty:
                csv_path = os.path.join(EXPORT_DIR, f"{table}.csv")
                df.to_csv(csv_path, index=False)
                print(f"  - {table}: {len(df)} rows -> {csv_path}")
            else:
                print(f"  - {table}: [Empty]")
        except Exception as e:
            print(f"  - {table}: Error exporting ({e})")

    conn.close()
    print("Done.")


if __name__ == "__main__":
    export_tables()
