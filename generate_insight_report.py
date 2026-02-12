#!/usr/bin/env python3
"""
generate_insight_report.py

Generates a Markdown summary report from the SERP analysis JSON.
Usage: python generate_insight_report.py --json market_analysis_v2.json --out report.md
"""
import argparse
import json
import sys
from datetime import datetime


def load_data(json_path):
    try:
        with open(json_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"Error loading JSON: {e}")
        sys.exit(1)


def generate_report(data):
    report = []

    # Extract Metadata
    overview = data.get("overview", [])
    run_id = overview[0].get("Run_ID", "Unknown") if overview else "Unknown"
    date = overview[0].get("Created_At", datetime.now(
    ).isoformat()) if overview else datetime.now().isoformat()

    report.append(f"# Market Intelligence Report")
    report.append(f"**Run ID:** {run_id} | **Date:** {date}\n")

    # 1. Overview & Opportunity
    report.append("## 1. Market Overview")
    if overview:
        total_vol = sum(int(o.get("Total_Results", 0) or 0) for o in overview)
        report.append(f"- **Keywords Analyzed:** {len(overview)}")
        report.append(
            f"- **Total Search Volume (Proxy):** {total_vol:,} results")

        # Top SERP Features
        features = [o.get("SERP_Features")
                    for o in overview if o.get("SERP_Features")]
        if features:
            unique_features = sorted(
                list(set(", ".join(features).split(", "))))
            report.append(
                f"- **Dominant SERP Features:** {', '.join(unique_features)}")
    report.append("\n")

    # 2. The "Anxiety Loop" (PAA Analysis)
    report.append("## 2. The 'Anxiety Loop' (User Intent)")
    report.append(
        "What users are frantically searching for (Problem-Awareness).")

    paa = data.get("paa_questions", [])
    if paa:
        # Group by category if available
        commercial = [q["Question"]
                      for q in paa if q.get("Category") == "Commercial"]
        distress = [q["Question"]
                    for q in paa if q.get("Category") == "Distress"]
        reactivity = [q["Question"]
                      for q in paa if q.get("Category") == "Reactivity"]

        if distress:
            report.append("\n### 🚨 High Distress Signals")
            for q in distress[:5]:
                report.append(f"- {q}")

        if reactivity:
            report.append("\n### 🔥 Reactivity & Blame")
            for q in reactivity[:5]:
                report.append(f"- {q}")

        if commercial:
            report.append("\n### 💰 Resource/Cost Anxiety")
            for q in commercial[:5]:
                report.append(f"- {q}")

        # General top questions if no categories matched
        if not (commercial or distress or reactivity):
            for q in paa[:5]:
                report.append(f"- {q['Question']}")
    else:
        report.append("_No PAA data found._")
    report.append("\n")

    # 3. The "Status Quo" (Competitor Language)
    report.append("## 3. The 'Status Quo' (Competitor Language)")
    report.append(
        "The dominant narrative in the market (Medical Model vs. Systemic).")

    # Handle key rename compatibility (serp_language_patterns vs bad_advice_patterns)
    patterns = data.get("serp_language_patterns") or data.get(
        "bad_advice_patterns", [])

    if patterns:
        report.append("\n### Top Recurring Phrases")
        # Filter for Bigrams/Trigrams and sort by count
        top_patterns = sorted(
            patterns, key=lambda x: x["Count"], reverse=True)[:10]
        for p in top_patterns:
            report.append(f"- **{p['Phrase']}** ({p['Count']} occurrences)")
    else:
        report.append("_No language patterns found._")
    report.append("\n")

    # 4. Strategic Bridge
    report.append("## 4. Strategic Recommendations (The Bridge)")
    report.append("How to differentiate using Bowen Theory.")

    recs = data.get("strategic_recommendations", [])
    if recs:
        for rec in recs:
            report.append(f"\n### 🌉 {rec.get('Pattern_Name', 'Opportunity')}")
            report.append(f"- **Status Quo:** {rec.get('Status_Quo_Message')}")
            report.append(
                f"- **The Reframe:** {rec.get('Bowen_Bridge_Reframe')}")
            report.append(f"- **Content Angle:** *{rec.get('Content_Angle')}*")
            if rec.get("Detected_Triggers") and rec.get("Detected_Triggers") != "N/A":
                report.append(
                    f"- *Triggers found:* {rec.get('Detected_Triggers')}")
    else:
        report.append("_No strategic recommendations generated._")

    return "\n".join(report)


def main():
    parser = argparse.ArgumentParser(
        description="Generate Marketing Insights Report")
    parser.add_argument("--json", required=True,
                        help="Path to serp_norm.json or market_analysis_v2.json")
    parser.add_argument("--out", required=True,
                        help="Output Markdown file path")
    args = parser.parse_args()

    data = load_data(args.json)
    report_content = generate_report(data)

    try:
        with open(args.out, 'w', encoding='utf-8') as f:
            f.write(report_content)
        print(f"Report generated: {args.out}")
    except Exception as e:
        print(f"Error writing report: {e}")


if __name__ == "__main__":
    main()
