"""
verify_enrichment.py
Checks the SQLite database to verify that enrichment data (URL features, Domain features)
is being correctly populated.
"""
import sqlite3
import os
import sys

DB_PATH = "serp_data.db"


def verify_db():
    if not os.path.exists(DB_PATH):
        print(f"Error: Database {DB_PATH} not found. Run serp_audit.py first.")
        return

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    print("--- Verifying Database Population ---")

    # 1. Check Runs
    c.execute("SELECT count(*) FROM runs")
    run_count = c.fetchone()[0]
    print(f"Runs found: {run_count}")

    # 2. Check SERP Results
    c.execute("SELECT count(*) FROM serp_results")
    result_count = c.fetchone()[0]
    print(f"SERP Results found: {result_count}")

    # 3. Check Enriched URL Features
    c.execute("SELECT count(*) FROM url_features")
    url_feat_count = c.fetchone()[0]
    print(f"Enriched URLs found: {url_feat_count}")

    if url_feat_count > 0:
        c.execute(
            "SELECT url, content_type, word_count_est FROM url_features LIMIT 3")
        print("\nSample Enriched URLs:")
        for row in c.fetchall():
            print(f"  - {row[0][:50]}... | Type: {row[1]} | Words: {row[2]}")

    # 4. Check Domain Features
    c.execute("SELECT count(*) FROM domain_features")
    print(f"Classified Domains found: {c.fetchone()[0]}")


if __name__ == "__main__":
    verify_db()
