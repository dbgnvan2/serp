#!/usr/bin/env python3
"""
run_pipeline.py
Orchestrates the full SERP audit pipeline:
1. Run Audit (Fetch + Enrich + Store)
2. Validate Output (XLSX vs JSON)
3. Verify DB Enrichment
4. Generate Domain Override Review Candidates
"""
import subprocess
import sys
import os
import yaml


def run_command(cmd, description):
    print(f"\n--- {description} ---")
    print(f"Running: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=False)
    if result.returncode != 0:
        print(f"❌ {description} Failed! (Exit Code: {result.returncode})")
        sys.exit(result.returncode)
    print(f"✅ {description} Completed.")


def main():
    # Load Config
    config = {}
    if os.path.exists("config.yml"):
        with open("config.yml", "r") as f:
            config = yaml.safe_load(f) or {}
    files_cfg = config.get("files", {})

    # 1. Run SERP Audit
    run_command([sys.executable, "serp_audit.py"],
                "Step 1: SERP Audit & Enrichment")

    # Re-read config: serp_audit.py writes the actual output paths on completion.
    if os.path.exists("config.yml"):
        with open("config.yml", "r") as f:
            config = yaml.safe_load(f) or {}
    files_cfg = config.get("files", {})

    # 2. Validate Data Consistency
    xlsx_file = files_cfg.get("output_xlsx", "market_analysis_v2.xlsx")
    json_file = files_cfg.get("output_json", "market_analysis_v2.json")
    diff_file = "diff_report.json"

    if os.path.exists(xlsx_file) and os.path.exists(json_file):
        run_command([
            sys.executable, "validate_xlsx_vs_json.py",
            "--xlsx", xlsx_file,
            "--json", json_file,
            "--out", diff_file
        ], "Step 2: Data Validation")
    else:
        print("⚠️ Skipping validation: Output files not found.")

    # 3. Verify Database Enrichment
    run_command([sys.executable, "verify_enrichment.py"],
                "Step 3: DB Enrichment Verification")

    # 4. Generate Domain Override Review Candidates
    if os.path.exists(json_file):
        run_command([
            sys.executable, "generate_domain_override_candidates.py",
            "--json", json_file,
            "--overrides", files_cfg.get("domain_overrides", "domain_overrides.yml"),
            "--out", "domain_override_candidates.md",
        ], "Step 4: Domain Override Candidate Report")
    else:
        print("⚠️ Skipping domain override candidate report: JSON output not found.")

    print("\n🎉 Pipeline Finished Successfully!")
    print(f"   - Report: market_analysis_v2.md")
    print(f"   - Excel:  {xlsx_file}")
    print(f"   - JSON:   {json_file}")
    print(f"   - DB:     serp_data.db")
    print(f"   - Override Candidates: domain_override_candidates.md")


if __name__ == "__main__":
    main()
