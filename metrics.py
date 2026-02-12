"""
metrics.py
Calculates SEO metrics (Volatility, Dominance) from the SQLite history.
"""
import sqlite3
import pandas as pd
import os
import logging

DB_PATH = "serp_data.db"


def get_volatility_metrics(current_run_id):
    """
    Compares the current run against the immediate previous run to calculate rank volatility.
    Returns a dictionary of metrics or None if insufficient history.
    """
    if not os.path.exists(DB_PATH):
        return None

    try:
        conn = sqlite3.connect(DB_PATH)

        # 1. Get Run History
        runs_df = pd.read_sql(
            "SELECT run_id, run_date FROM runs ORDER BY run_date DESC", conn)
        if len(runs_df) < 2:
            conn.close()
            return {"status": "insufficient_history", "msg": "Need at least 2 runs to calculate volatility."}

        # Current is index 0 (if passed run_id matches), Previous is index 1
        # Verify current_run_id is actually the latest or find it

        # Simple approach: Get data for current_run_id and the one immediately preceding it in time
        current_run_date_row = runs_df[runs_df['run_id'] == current_run_id]
        if current_run_date_row.empty:
            conn.close()
            return None

        current_date = current_run_date_row.iloc[0]['run_date']

        # Find previous run
        prev_runs = runs_df[runs_df['run_date'] < current_date]
        if prev_runs.empty:
            conn.close()
            return {"status": "insufficient_history", "msg": "No prior run found."}

        prev_run_id = prev_runs.iloc[0]['run_id']

        # 2. Get Ranks for Current and Previous
        query = """
        SELECT 
            keyword_text, 
            url, 
            rank 
        FROM serp_results 
        WHERE run_id = ? AND result_type = 'organic'
        """
        curr_df = pd.read_sql(query, conn, params=(current_run_id,))
        prev_df = pd.read_sql(query, conn, params=(prev_run_id,))

        conn.close()

        if curr_df.empty or prev_df.empty:
            return {"status": "empty_data", "msg": "No organic results found in one of the runs."}

        # Ensure ranks are numeric (handle "N/A" or other artifacts)
        curr_df['rank'] = pd.to_numeric(curr_df['rank'], errors='coerce')
        prev_df['rank'] = pd.to_numeric(prev_df['rank'], errors='coerce')
        curr_df = curr_df.dropna(subset=['rank'])
        prev_df = prev_df.dropna(subset=['rank'])

        # Merge on keyword + url
        merged = pd.merge(curr_df, prev_df, on=[
                          'keyword_text', 'url'], suffixes=('_curr', '_prev'))

        # Calculate Delta
        # Positive means improved (rank went down numerically)
        merged['rank_delta'] = merged['rank_prev'] - merged['rank_curr']

        # Volatility Score (Average absolute change)
        volatility_score = merged['rank_delta'].abs().mean()

        # Winners & Losers
        winners = merged[merged['rank_delta'] > 0].sort_values(
            'rank_delta', ascending=False).head(5)
        losers = merged[merged['rank_delta'] < 0].sort_values(
            'rank_delta', ascending=True).head(5)

        return {
            "status": "success",
            "volatility_score": round(volatility_score, 2),
            "stable_urls_count": len(merged[merged['rank_delta'] == 0]),
            "total_compared": len(merged),
            "winners": winners[['keyword_text', 'url', 'rank_curr', 'rank_delta']].to_dict('records'),
            "losers": losers[['keyword_text', 'url', 'rank_curr', 'rank_delta']].to_dict('records')
        }

    except Exception as e:
        logging.error(f"Error calculating volatility: {e}")
        return None


def get_entity_dominance(run_id):
    """
    Aggregates Entity Type and Content Type dominance for the current run.
    """
    if not os.path.exists(DB_PATH):
        return {}

    try:
        conn = sqlite3.connect(DB_PATH)

        # Join serp_results with domain_features and url_features
        query = """
        SELECT 
            s.rank,
            d.entity_type,
            u.content_type
        FROM serp_results s
        LEFT JOIN domain_features d ON s.domain = d.domain
        LEFT JOIN url_features u ON s.url = u.url
        WHERE s.run_id = ? AND s.result_type = 'organic' AND s.rank <= 10
        """
        df = pd.read_sql(query, conn, params=(run_id,))
        conn.close()

        if df.empty:
            return {}

        entity_counts = df['entity_type'].value_counts(
            normalize=True).to_dict()
        content_counts = df['content_type'].value_counts(
            normalize=True).to_dict()

        return {
            "entity_dominance": {k: round(v * 100, 1) for k, v in entity_counts.items()},
            "content_dominance": {k: round(v * 100, 1) for k, v in content_counts.items()}
        }

    except Exception as e:
        logging.error(f"Error calculating dominance: {e}")
        return {}


def get_rank_deltas(current_run_id):
    """
    Returns a dictionary of {url: rank_delta} for the current run compared to the previous one.
    """
    if not os.path.exists(DB_PATH):
        return {}

    try:
        conn = sqlite3.connect(DB_PATH)
        runs_df = pd.read_sql(
            "SELECT run_id, run_date FROM runs ORDER BY run_date DESC", conn)

        if len(runs_df) < 2:
            conn.close()
            return {}

        # Identify previous run
        current_run_date_row = runs_df[runs_df['run_id'] == current_run_id]
        if current_run_date_row.empty:
            conn.close()
            return {}

        current_date = current_run_date_row.iloc[0]['run_date']
        prev_runs = runs_df[runs_df['run_date'] < current_date]

        if prev_runs.empty:
            conn.close()
            return {}

        prev_run_id = prev_runs.iloc[0]['run_id']

        # Get Deltas
        query = "SELECT url, rank FROM serp_results WHERE run_id = ? AND result_type = 'organic'"
        curr = pd.read_sql(query, conn, params=(
            current_run_id,)).set_index('url')['rank']
        prev = pd.read_sql(query, conn, params=(
            prev_run_id,)).set_index('url')['rank']

        # Ensure numeric types
        curr = pd.to_numeric(curr, errors='coerce')
        prev = pd.to_numeric(prev, errors='coerce')

        # Calculate delta (Prev - Curr). Positive = Improved.
        delta_series = prev - curr
        return delta_series.dropna().to_dict()

    except Exception as e:
        logging.error(f"Error getting rank deltas: {e}")
        return {}
