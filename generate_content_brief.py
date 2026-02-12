#!/usr/bin/env python3
"""
generate_content_brief.py

Generates a detailed Content Brief/Blog Outline based on a selected Strategic Recommendation.
Usage:
  List options: python generate_content_brief.py --json market_analysis_v2.json --list
  Generate:     python generate_content_brief.py --json market_analysis_v2.json --out brief.md --index 0
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


def list_recommendations(data):
    recs = data.get("strategic_recommendations", [])
    if not recs:
        print("No strategic recommendations found.")
        return

    print(f"\nFound {len(recs)} Strategic Recommendations:\n")
    for i, rec in enumerate(recs):
        print(
            f"[{i}] {rec.get('Pattern_Name')} (Triggers: {rec.get('Detected_Triggers')})")
        print(f"    Angle: {rec.get('Content_Angle')}\n")


def generate_brief(data, rec_index=0):
    recs = data.get("strategic_recommendations", [])
    if not recs:
        return "No strategic recommendations found in the dataset."

    if rec_index >= len(recs):
        print(f"Index {rec_index} out of range. Using 0.")
        rec_index = 0

    rec = recs[rec_index]

    # Extract supporting data
    paa = data.get("paa_questions", [])
    organic = data.get("organic_results", [])

    # Filter PAA for relevance (simple keyword matching based on triggers)
    triggers = rec.get("Detected_Triggers", "").split(", ")
    relevant_paa = []

    # 1. Try to find PAA containing trigger words
    if triggers and triggers[0] != "N/A":
        for q in paa:
            q_text = q.get("Question", "").lower()
            if any(t in q_text for t in triggers):
                relevant_paa.append(q.get("Question"))

    # 2. If no specific matches, take top ranked PAA (often most relevant anyway)
    if not relevant_paa:
        relevant_paa = [q.get("Question") for q in paa[:5]]
    else:
        # Limit to 5
        relevant_paa = relevant_paa[:5]

    # Top Competitor Titles (Status Quo)
    top_competitors = [o.get("Title") for o in organic[:3]]

    # Build the Brief
    lines = []
    lines.append(f"# Content Brief: {rec.get('Content_Angle')}")
    lines.append(f"**Strategy:** {rec.get('Pattern_Name')}")
    lines.append(f"**Date:** {datetime.now().strftime('%Y-%m-%d')}\n")

    lines.append("## 1. The Core Conflict (The Hook)")
    lines.append(
        f"**The Status Quo (Bad Advice):** \"{rec.get('Status_Quo_Message')}\"")
    lines.append(
        f"**The Bowen Reframe (The Solution):** \"{rec.get('Bowen_Bridge_Reframe')}\"")
    lines.append(
        f"**Target Audience Pain Point:** They are searching for *{rec.get('Detected_Triggers')}* and feeling anxious/stuck.")
    lines.append("\n")

    lines.append("## 2. User Intent & Anxiety (PAA)")
    lines.append(
        "Address these specific questions to validate the reader's experience:")
    for q in relevant_paa:
        lines.append(f"- {q}")
    lines.append("\n")

    lines.append("## 3. The Competition (What to Differentiate Against)")
    lines.append(
        "The reader has likely already seen these headlines. Do NOT repeat them; challenge them.")
    for t in top_competitors:
        lines.append(f"- *{t}*")
    lines.append("\n")

    lines.append("## 4. Blog Outline")
    lines.append("### H1: [Draft Title based on Content Angle]")
    lines.append(f"*(e.g., {rec.get('Content_Angle')})*")

    lines.append("### Introduction: Validate & Pivot")
    lines.append(
        "- Acknowledge the search intent (e.g., \"You are looking for...\").")
    lines.append(
        f"- Call out the status quo trap: \"{rec.get('Status_Quo_Message')}\"")
    lines.append(
        "- Introduce the pivot: \"But what if the problem isn't [X], but [Y]?\"")

    lines.append("### Body Paragraph 1: The Cost of the Status Quo")
    lines.append(
        "- Explain why the standard advice fails (increases anxiety, creates dependency).")
    lines.append("- Use a generic scenario (no specific client details).")

    lines.append("### Body Paragraph 2: The Systemic View (The Bridge)")
    lines.append(f"- Introduce the concept: {rec.get('Bowen_Bridge_Reframe')}")
    lines.append(
        "- Explain how this shifts the focus from 'fixing' to 'growing'.")

    lines.append("### Body Paragraph 3: Actionable Steps (Differentiation)")
    lines.append("- Step 1: Observe your reaction.")
    lines.append("- Step 2: Pause before responding.")
    lines.append(
        "- Step 3: Choose a response based on principles, not feelings.")

    lines.append("### Conclusion: The Long Game")
    lines.append("- Reiterate that quick fixes don't last.")
    lines.append(
        "- Call to Action: Invite them to explore deeper work (consultation/resources).")

    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(
        description="Generate Content Brief from Strategic Recommendations")
    parser.add_argument("--json", required=True,
                        help="Path to market_analysis_v2.json")
    parser.add_argument(
        "--out", help="Output Markdown file path (required unless --list is used)")
    parser.add_argument("--index", type=int, default=0,
                        help="Index of the strategic recommendation to use (default: 0)")
    parser.add_argument("--list", action="store_true",
                        help="List available strategic recommendations and exit")
    args = parser.parse_args()

    data = load_data(args.json)

    if args.list:
        list_recommendations(data)
        return

    if not args.out:
        print("Error: --out is required unless --list is used.")
        sys.exit(1)

    brief_content = generate_brief(data, args.index)

    try:
        with open(args.out, 'w', encoding='utf-8') as f:
            f.write(brief_content)
        print(f"Content Brief generated: {args.out}")
    except Exception as e:
        print(f"Error writing brief: {e}")


if __name__ == "__main__":
    main()
