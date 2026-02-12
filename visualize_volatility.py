"""
visualize_volatility.py
Visualizes rank history for a specific keyword using matplotlib.
"""
import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import argparse
import sys

DB_PATH = "serp_data.db"


def get_keywords():
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql("SELECT DISTINCT keyword_text FROM serp_results", conn)
    conn.close()
    return df['keyword_text'].tolist()


def plot_history(keyword):
    conn = sqlite3.connect(DB_PATH)

    # Get data for this keyword
    query = """
    SELECT 
        r.run_date,
        s.rank,
        s.domain
    FROM serp_results s
    JOIN runs r ON s.run_id = r.run_id
    WHERE s.keyword_text = ? 
      AND s.result_type = 'organic'
      AND s.rank <= 10
    ORDER BY r.run_date ASC
    """
    df = pd.read_sql(query, conn, params=(keyword,))
    conn.close()

    if df.empty:
        print(f"No data found for keyword: {keyword}")
        return

    # Pivot for plotting: Index=Date, Columns=Domain, Values=Rank
    pivot = df.pivot_table(index='run_date', columns='domain', values='rank')

    # Plot
    plt.figure(figsize=(12, 6))

    for column in pivot.columns:
        plt.plot(pivot.index, pivot[column], marker='o', label=column)

    plt.gca().invert_yaxis()  # Rank 1 is at top
    plt.title(f"SERP History: {keyword}")
    plt.ylabel("Rank")
    plt.xlabel("Run Date")
    plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.tight_layout()

    filename = f"volatility_{keyword.replace(' ', '_')}.png"
    plt.savefig(filename)
    print(f"Chart saved to {filename}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--keyword", help="Keyword to visualize")
    parser.add_argument("--list", action="store_true",
                        help="List available keywords")
    args = parser.parse_args()

    if args.list:
        print("Available Keywords:")
        for k in get_keywords():
            print(f" - {k}")
    elif args.keyword:
        plot_history(args.keyword)
    else:
        print("Please specify --keyword or --list")
