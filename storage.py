"""
storage.py
Manages SQLite database for SERP history and enriched features.
"""
import sqlite3
import json
import logging
from datetime import datetime


class SerpStorage:
    def __init__(self, db_path="serp_data.db"):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()

        # 1. Runs
        c.execute('''CREATE TABLE IF NOT EXISTS runs (
            run_id TEXT PRIMARY KEY,
            run_date TEXT,
            params_hash TEXT
        )''')

        # 2. Keywords
        c.execute('''CREATE TABLE IF NOT EXISTS keywords (
            keyword_id INTEGER PRIMARY KEY AUTOINCREMENT,
            keyword_text TEXT UNIQUE,
            locale TEXT
        )''')

        # 3. SERP Results
        c.execute('''CREATE TABLE IF NOT EXISTS serp_results (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT,
            keyword_text TEXT,
            result_type TEXT, -- organic, paid, local, etc.
            rank INTEGER,
            title TEXT,
            url TEXT,
            domain TEXT,
            snippet TEXT,
            features_json TEXT,
            FOREIGN KEY(run_id) REFERENCES runs(run_id)
        )''')

        # 4. URL Features (Enriched)
        c.execute('''CREATE TABLE IF NOT EXISTS url_features (
            url TEXT PRIMARY KEY,
            fetched_at TEXT,
            status_code INTEGER,
            content_type TEXT,
            schema_types TEXT,
            word_count_est INTEGER,
            evidence_json TEXT
        )''')

        # 5. Domain Features (Classified)
        c.execute('''CREATE TABLE IF NOT EXISTS domain_features (
            domain TEXT PRIMARY KEY,
            entity_type TEXT,
            domain_age_years INTEGER
        )''')

        # 6. Autocomplete Suggestions
        c.execute('''CREATE TABLE IF NOT EXISTS autocomplete_suggestions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT,
            source_keyword TEXT,
            suggestion TEXT,
            rank INTEGER,
            relevance INTEGER,
            type TEXT,
            FOREIGN KEY(run_id) REFERENCES runs(run_id)
        )''')

        conn.commit()
        conn.close()

    def save_run(self, run_id, params_hash):
        run_date = datetime.now().isoformat()
        with sqlite3.connect(self.db_path) as conn:
            # Use REPLACE to handle updates to params_hash if called multiple times
            conn.execute("INSERT OR REPLACE INTO runs (run_id, run_date, params_hash) VALUES (?, ?, ?)",
                         (run_id, run_date, params_hash))

    def save_serp_result(self, run_id, keyword, result_type, rank, title, url, domain, snippet, features=None):
        features_json = json.dumps(features) if features else "{}"
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('''INSERT INTO serp_results 
                            (run_id, keyword_text, result_type, rank, title, url, domain, snippet, features_json)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                         (run_id, keyword, result_type, rank, title, url, domain, snippet, features_json))

    def save_url_features(self, url, status_code, content_type, schema_types, word_count, evidence):
        fetched_at = datetime.now().isoformat()
        schema_json = json.dumps(schema_types)
        evidence_json = json.dumps(evidence)

        with sqlite3.connect(self.db_path) as conn:
            conn.execute('''INSERT OR REPLACE INTO url_features 
                            (url, fetched_at, status_code, content_type, schema_types, word_count_est, evidence_json)
                            VALUES (?, ?, ?, ?, ?, ?, ?)''',
                         (url, fetched_at, status_code, content_type, schema_json, word_count, evidence_json))

    def save_domain_features(self, domain, entity_type):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('''INSERT OR REPLACE INTO domain_features (domain, entity_type) VALUES (?, ?)''',
                         (domain, entity_type))

    def save_autocomplete_suggestion(self, run_id, source_keyword, suggestion, rank, relevance=None, type_=None):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('''INSERT INTO autocomplete_suggestions 
                            (run_id, source_keyword, suggestion, rank, relevance, type)
                            VALUES (?, ?, ?, ?, ?, ?)''',
                         (run_id, source_keyword, suggestion, rank, relevance, type_))
