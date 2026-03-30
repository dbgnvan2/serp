# SERP Intelligence Tool — CLAUDE.md

## Project Overview

A market intelligence tool built for Living Systems Counselling (livingsystems.ca), a small Bowen Family Systems Theory nonprofit in North Vancouver, BC. It scrapes Google SERPs via SerpAPI, enriches results with URL content analysis, uses Anthropic LLMs to generate strategic content recommendations using a "Bridge Strategy" — mapping Problem-Aware search queries to Solution-Aware counselling content — and scores keyword competitiveness using Domain Authority gap analysis.

## Environment Setup

```bash
# Always activate venv before running Python commands
source venv/bin/activate

# Required env vars (.env file)
SERPAPI_KEY=<your_key>
ANTHROPIC_API_KEY=<your_key>        # Only needed for content brief generation
DATAFORSEO_LOGIN=<your_email>       # Primary DA provider (pay-per-use)
DATAFORSEO_PASSWORD=<your_api_pw>   # From DataForSEO dashboard
MOZ_TOKEN=<your_token>              # Fallback DA provider (free tier: 50 rows/month)
```

## Running Tests

```bash
source venv/bin/activate
python -m pytest test_*.py -q
```

Tests do **not** require API keys — all external calls are mocked. The `test_serp_launcher.py` tests are skipped if tkinter is unavailable (headless environments).

Expected: 181 passing, 5 skipped, 0 errors.

## Key Entry Points

| Command | Purpose |
|---------|---------|
| `python serp-me.py` | GUI launcher (daily workflow) |
| `python serp_audit.py` | Fetch & parse SERPs, write JSON/XLSX/MD |
| `python run_pipeline.py` | Full pipeline (audit → validation → refresh) |
| `python run_feasibility.py --json <file>` | Standalone DA feasibility analysis |
| `python generate_content_brief.py --json <file> --list` | LLM content brief |
| `python refresh_analysis_outputs.py --json <file> --xlsx <file>` | Re-classify without re-fetching |
| `python export_history.py` | Export SQLite history to CSV |
| `python visualize_volatility.py --keyword <kw>` | Plot rank volatility |

## Architecture

```
keywords.csv
    └─► serp_audit.py (fetch + parse + enrich)
            ├─► raw/{run_id}/              raw JSON from SerpAPI
            ├─► market_analysis_*.json     aggregated data
            ├─► market_analysis_*.xlsx     Excel workbook
            ├─► market_analysis_*.md       markdown summary
            └─► serp_data.db               SQLite history

market_analysis_*.json
    ├─► run_feasibility.py (DA gap analysis — standalone, runs any time)
    │       ├─► dataforseo_client.py       domain rank lookup (primary)
    │       ├─► moz_client.py             domain rank lookup (fallback)
    │       ├─► feasibility.py            gap scoring + pivot suggestions
    │       └─► feasibility_*.md          standalone feasibility report
    │
    └─► generate_content_brief.py (LLM via Anthropic)
            ├─► content_opportunities_*.md
            └─► advisory_briefing_*.md
```

### Core Modules

| Module | Role |
|--------|------|
| `serp_audit.py` | Main SERP engine — fetches, parses, enriches, tags PAA intent |
| `generate_content_brief.py` | LLM report generator using Anthropic API |
| `run_feasibility.py` | Standalone DA feasibility analysis and pivot report |
| `classifiers.py` | Rule-based content & entity type classifiers |
| `intent_classifier.py` | Tags PAA questions as External Locus / Systemic / General |
| `feasibility.py` | DA gap scoring (`compute_feasibility`) + pivot suggestions (`generate_hyper_local_pivot`) |
| `dataforseo_client.py` | DataForSEO bulk_ranks API client with 30-day SQLite cache |
| `moz_client.py` | Moz Links API v2 client with 30-day SQLite cache (fallback) |
| `storage.py` | SQLite persistence layer |
| `metrics.py` | Volatility & entity dominance calculations |
| `url_enricher.py` | URL fetching & feature extraction |
| `serp-me.py` | Tkinter GUI launcher |
| `run_pipeline.py` | Pipeline orchestration |

### Prompt Templates

```
prompts/
├── main_report/system.md + user_template.md   → content_opportunities_*.md
├── advisory/system.md + user_template.md      → advisory_briefing_*.md
└── correction/user_template.md                → retry on validation failure
```

## Configuration

**`config.yml`** — all operational settings:
- `serpapi.*` — API params (engine, location, pagination, retries, modes)
- `files.*` — input/output file paths (auto-updated by GUI after each run)
- `enrichment.*` — URL enrichment settings
- `app.*` — API mode flags (`balanced_mode`, `deep_research_mode`)
- `moz.cache_ttl_days` — DA cache lifetime in days (default 30)
- `feasibility.*` — DA gap thresholds, client DA, neighbourhoods, pivot settings
- `analysis_report.*` — client context injected into LLM prompts

**`domain_overrides.yml`** — manual entity type overrides (e.g., `psychologytoday.com: directory`).

## Feasibility Scoring

Gap = avg competitor DA − client DA. Thresholds:

| Gap | Status | Meaning |
|-----|--------|---------|
| ≤ 5 | ✅ High Feasibility | Rankable with content alone |
| 6–15 | ⚠️ Moderate Feasibility | Requires local backlink building |
| > 15 | 🔴 Low Feasibility | Dominated by high-authority sites — pivot to neighbourhood variant |

**DA providers** (tried in order):
1. **DataForSEO** (`DATAFORSEO_LOGIN` + `DATAFORSEO_PASSWORD`) — `POST /v3/backlinks/bulk_ranks/live`, up to 1000 domains/call, pay-per-use
2. **Moz** (`MOZ_TOKEN`) — `POST /v2/url_metrics`, up to 50 URLs/call, free tier 50 rows/month

Both cache results in SQLite (`da_cache` and `moz_cache` tables) for 30 days. Re-running within the cache window costs nothing.

**Pivot logic:** Low Feasibility keywords get a neighbourhood variant suggestion (e.g. "Couples Counselling" → "Couples Counselling Lonsdale"). If `feasibility.pivot_serp_fetch: true`, a secondary SerpAPI Maps call checks whether the client appears in the local 3-pack for the pivot keyword.

## PAA Intent Classification

`intent_classifier.py` tags every PAA question with:
- **External Locus** — Medical model language (diagnosis, treatment, disorder, patient…)
- **Systemic** — Bowen Theory language (differentiation, emotional cutoff, triangulation…)
- **General** — Neither

Tags written to `market_analysis_*.json` (`Intent_Tag`, `Intent_Confidence` fields). Systemic-tagged questions are passed to the LLM as `bowen_reframe_faqs` in the content brief payload.

## API Modes

| Mode | Google Pages | Maps Pages | AI Follow-up |
|------|-------------|-----------|--------------|
| Low API | 1 | 1 | No |
| Balanced (default) | 3 | 1 | Defend/Strengthen only |
| Deep Research | configurable | configurable | Yes (up to 5 calls) |

Set via `config.yml` (`app.balanced_mode`, `app.deep_research_mode`) or env vars:
```bash
SERP_LOW_API_MODE=1
SERP_BALANCED_MODE=1
SERP_DEEP_RESEARCH_MODE=1
```

## Database

SQLite at `serp_data.db`. Key tables:

| Table | Contents |
|-------|----------|
| `runs` | Each audit run (run_id, date, params_hash) |
| `serp_results` | All ranked results per keyword per run |
| `url_features` | Enriched URL data including Moz DA/PA columns |
| `domain_features` | Entity type per domain |
| `autocomplete_suggestions` | Search autocomplete data |
| `keyword_feasibility` | DA gap scores, feasibility status, pivot variants per run |
| `da_cache` | DataForSEO domain rank cache (30-day TTL) |
| `moz_cache` | Moz DA/PA cache (30-day TTL) |

All schema changes use `CREATE TABLE IF NOT EXISTS` or `ALTER TABLE … ADD COLUMN` wrapped in `try/except OperationalError` — migrations run automatically on first use.

## GUI (serp-me.py) — Step Reference

| Step | Script | When to run |
|------|--------|-------------|
| 1. Full Pipeline | `run_pipeline.py` | Fresh SERP fetch for a keyword set |
| 2. Fetch SERPs Only | `serp_audit.py` | Fetch without pipeline validation |
| 3. Content Brief | `generate_content_brief.py` | After a pipeline run |
| 4. Refresh Outputs | `refresh_analysis_outputs.py` | Re-classify without re-fetching |
| 5. Export History | `export_history.py` | Export DB to CSV |
| 6. Domain Overrides | — | Review/approve entity type overrides |
| 7. Feasibility Analysis | `run_feasibility.py` | DA scoring from existing JSON (cached — free to re-run) |

## LLM Validation Strategy

`generate_content_brief.py` validates LLM outputs before writing:
1. Hard-fail (abort): AI Overview count mismatch vs. extracted data
2. Soft-fail (1 retry): Wording issues → sends correction prompt
3. Failed validations written to `*.validation.md` for inspection

## Development Notes

- **Naming convention**: output files follow `market_analysis_{topic}_{YYYYMMDD_HHMM}.json` — the GUI updates `config.yml` with the latest paths after each run. Topic slug is derived from the keyword CSV filename (lowercase, spaces → underscores).
- **Tkinter tests**: `test_serp_launcher.py` imports `serp-me.py` which requires tkinter. Tests are skipped automatically in headless environments.
- **No pytest installed by default**: `pytest` is not in `requirements.txt`; install it in the venv for development.
- **Client context**: `config.yml → analysis_report` section is injected verbatim into LLM prompts — keep it accurate and concise.
- **Entity classification precedence**: `domain_overrides.yml` > TLD signals > known-directory list > domain keywords > page content.
- **DA client priority**: DataForSEO is tried first if `DATAFORSEO_LOGIN` is set; Moz used as fallback if `MOZ_TOKEN` is set. If neither is available, feasibility scores are skipped (keywords marked "No DA Data").
- **Token costs**: Main content brief runs ~$0.08–0.40 depending on model (Sonnet vs Opus). DA lookups: DataForSEO ~$0.002/domain, Moz free tier 50 rows/month. Cache all DA results for 30 days to minimise repeat costs.
