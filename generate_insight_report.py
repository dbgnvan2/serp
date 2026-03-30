#!/usr/bin/env python3
"""
generate_insight_report.py
~~~~~~~~~~~~~~~~~~~~~~~~~~
Generates a Markdown summary report from the SERP analysis JSON.

Sections
--------
1. Market Overview
2. The 'Anxiety Loop' (PAA Analysis)
3. The 'Status Quo' (Competitor Language)
4. Strategic Recommendations (The Bridge)
5. SERP Composition (Entity + Content Dominance)
5b. Keyword Feasibility & Pivot Recommendations  ← NEW
6. Market Volatility

Usage
-----
::

    python generate_insight_report.py --json market_analysis_v2.json --out report.md
"""
import argparse
import json
import sys
from datetime import datetime

try:
    import metrics
    METRICS_AVAILABLE = True
except ImportError:
    METRICS_AVAILABLE = False


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
        seen_questions = set()
        deduped_paa = []
        for item in paa:
            question = str(item.get("Question", "")).strip()
            key = question.lower()
            if not question or key in seen_questions:
                continue
            seen_questions.add(key)
            deduped_paa.append(item)

        # Group by category if available
        commercial = [q["Question"]
                      for q in deduped_paa if q.get("Category") == "Commercial"]
        distress = [q["Question"]
                    for q in deduped_paa if q.get("Category") == "Distress"]
        reactivity = [q["Question"]
                      for q in deduped_paa if q.get("Category") == "Reactivity"]

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
            for q in deduped_paa[:5]:
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

    # 5. Advanced Metrics (Dominance) + 6. Volatility
    if METRICS_AVAILABLE:
        _overview = data.get("overview", [])
        _run_id = _overview[0].get("Run_ID") if _overview else None

        if _run_id:
            # Section 5 — SERP Composition
            dominance = metrics.get_entity_dominance(_run_id)
            if dominance:
                report.append("\n## 5. SERP Composition (Enriched Data)")

                ents = dominance.get("entity_dominance", {})
                if ents:
                    report.append("### Entity Dominance (Top 10)")
                    for k, v in sorted(ents.items(), key=lambda x: x[1], reverse=True):
                        report.append(f"- **{k}:** {v}%")

                conts = dominance.get("content_dominance", {})
                if conts:
                    report.append("\n### Content Type Dominance (Top 10)")
                    for k, v in sorted(conts.items(), key=lambda x: x[1], reverse=True):
                        report.append(f"- **{k}:** {v}%")
                report.append("\n")

    # 5b. Keyword Feasibility & Pivot Recommendations
    feasibility_rows = data.get("keyword_feasibility", [])
    if feasibility_rows:
        report.append("## 5b. Keyword Feasibility & Pivot Recommendations")
        report.append(
            "Domain Authority gap analysis for each keyword. "
            "Low Feasibility keywords include a hyper-local pivot suggestion "
            "where geographic relevance can substitute for domain strength.\n"
        )

        # Split primary and pivot rows
        primary_rows = [r for r in feasibility_rows if r.get("Query_Label") != "P"]
        pivot_rows   = {r.get("Source_Keyword", r.get("Keyword")): r
                        for r in feasibility_rows if r.get("Query_Label") == "P"}

        # Table header
        report.append("| Keyword | Client DA | Avg Comp DA | Gap | Status | Recommended Pivot |")
        report.append("|---------|-----------|-------------|-----|--------|-------------------|")

        STATUS_ICONS = {
            "High Feasibility":     "✅ High",
            "Moderate Feasibility": "⚠️ Moderate",
            "Low Feasibility":      "🔴 Low",
        }

        for row in primary_rows:
            kw       = row.get("Keyword") or row.get("original_keyword", "—")
            client_da = row.get("client_da", "—")
            avg_da   = row.get("avg_serp_da")
            gap      = row.get("gap")
            status   = STATUS_ICONS.get(row.get("feasibility_status", ""), row.get("feasibility_status", "—"))
            avg_da_str = f"{avg_da:.0f}" if avg_da is not None else "—"
            gap_str    = f"{gap:+.0f}" if gap is not None else "—"

            pivot_cell = "*(stay the course)*"
            if row.get("pivot_status") == "Pivoting to Hyper-Local":
                suggested = row.get("suggested_keyword", "")
                # Check if we have a pivot result with local pack data
                pivot_result = pivot_rows.get(kw)
                if pivot_result:
                    pack = pivot_result.get("Client_In_Local_Pack")
                    pack_str = " ✓ in local pack" if pack else " ✗ not in local pack"
                    pivot_feas = pivot_result.get("feasibility_status", "")
                    p_icon = STATUS_ICONS.get(pivot_feas, pivot_feas)
                    pivot_cell = f"**{suggested}** — {p_icon}{pack_str}"
                else:
                    pivot_cell = f"**{suggested}**"

            report.append(f"| {kw} | {client_da} | {avg_da_str} | {gap_str} | {status} | {pivot_cell} |")

        report.append("")

        # Pivot strategy explanations for Low Feasibility keywords
        low_feas = [r for r in primary_rows if r.get("feasibility_status") == "Low Feasibility"]
        if low_feas:
            report.append("### Pivot Strategy\n")
            report.append(
                "> **Why this works:** Geographic relevance is the equalizer for non-profits. "
                "A practitioner physically located in a neighbourhood can outrank a national "
                "directory for a user searching in that specific area, regardless of domain authority.\n"
            )
            for row in low_feas:
                strategy = row.get("strategy", "")
                if strategy and strategy != "Current keyword is feasible. No pivot required.":
                    kw = row.get("Keyword") or row.get("original_keyword", "")
                    report.append(f"**{kw}:** {strategy}\n")

        report.append("\n")

    # Section 6 — Market Volatility
    if METRICS_AVAILABLE:
        _overview = data.get("overview", [])
        _run_id = _overview[0].get("Run_ID") if _overview else None
        if _run_id:
            vol = metrics.get_volatility_metrics(_run_id)
            if vol and vol.get("status") == "success":
                report.append("## 6. Market Volatility")
                report.append(
                    f"**Volatility Score:** {vol['volatility_score']} (Avg rank change)")
                report.append(
                    f"**Stable URLs:** {vol['stable_urls_count']} / {vol['total_compared']}")
                if vol.get("comparability_warning"):
                    report.append(f"**Comparability Warning:** {vol['comparability_warning']}")

                if vol['winners']:
                    report.append("\n### 🚀 Top Movers (Winners)")
                    for w in vol['winners']:
                        report.append(
                            f"- **{w['url']}** (+{w['rank_delta']} positions) for '{w['keyword_text']}'")

                if vol['losers']:
                    report.append("\n### 🔻 Top Movers (Losers)")
                    for l in vol['losers']:
                        report.append(
                            f"- **{l['url']}** ({l['rank_delta']} positions) for '{l['keyword_text']}'")
                report.append("\n")

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
