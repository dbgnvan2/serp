## Implementation instructions for your coding agent (SerpApi, Google, English, Canada)

### 0) Goal and non-goals

- [Confirmed] Goal: capture **paid/sponsored**, **organic**, **local/map pack**, **PAA**, **PASF/related searches**, and **AI answers (AI Overviews)** for Google Canada English.
- [Confirmed] Non-goal: “force” Google to show PAA/AI/ads. Those blocks are **conditional SERP features**; you can only **detect + capture** when present.

---

## 1) Request strategy (two-tier, feature-complete)

### 1.1 Primary SERP request (always)

Make one canonical request per query:

- [Confirmed] `engine=google`
- [Confirmed] `q=<query>`
- [Confirmed] `gl=ca`
- [Confirmed] `hl=en`
- [Confirmed] `location=<City, Province, Canada>` **OR** `uule=<encoded>` (choose one; prefer `location` for simplicity)
- [Requires Verification] `device=desktop|mobile` (choose and keep consistent; mobile vs desktop changes SERP modules)
- [Confirmed] `no_cache=true` if you need “as-live-as-possible” captures; otherwise allow caching for cost control.

**Rationale:** This is the single response most likely to contain: organic, ads (if present), local pack (if present), PAA (if present), related searches (if present).

- [Confirmed] SerpApi parses these as they appear on the SERP. (Google Ads / Related Questions docs) ([serpapi.com](https://serpapi.com/google-ads?utm_source=chatgpt.com)) ([serpapi.com](https://serpapi.com/related-questions?utm_source=chatgpt.com))

### 1.2 AI Overview request (conditional)

- [Confirmed] If the primary response indicates an AI Overview exists but is not fully returned (or if your product requirement is “always attempt AI Overview”), then do a second call:
  - `engine=google_ai_overview`
  - same `q`, `gl`, `hl`, `location/uule`, `device`

- [Confirmed] SerpApi provides this endpoint specifically for AI Overview extraction. ([serpapi.com](https://serpapi.com/google-ai-overview-api?utm_source=chatgpt.com))

### 1.3 PAA expansion (optional but recommended if you want _all_ questions, not just visible)

- [Confirmed] The standard SERP response usually includes only the visible PAA set.
- [Confirmed] If you need more PAA items (expanded set), call:
  - `engine=google_related_questions`
  - same `q`, `gl`, `hl`, `location/uule`

- [Confirmed] This is SerpApi’s “Google Related Questions API” (PAA). ([serpapi.com](https://serpapi.com/google-related-questions-api?utm_source=chatgpt.com))

---

## 2) Data capture requirements (store raw + normalized)

### 2.1 Persist raw responses

For each query execution, persist:

- [Confirmed] `serpapi_google_raw.json` (primary `engine=google` response)
- [Confirmed] `serpapi_ai_overview_raw.json` (if called)
- [Confirmed] `serpapi_paa_raw.json` (if called)

Also persist request metadata:

- [Confirmed] `q`, `gl`, `hl`, `location/uule`, `device`, `timestamp_utc`, `no_cache`, `serpapi_search_metadata` (from response), and a stable `run_id`.

### 2.2 Normalize into a canonical schema you own

Create `serp_norm.json` with these top-level keys:

- `query_context`: `{q, gl, hl, location, device, captured_at_utc, run_id}`
- `paid`: `[{type, position, title, displayed_link, target_url, snippet, sitelinks[], extensions{...}, raw_ref}]`
- `organic`: `[{position, title, displayed_link, target_url, snippet, rich_snippets{...}, raw_ref}]`
- `local_pack`: `[{position, name, rating, reviews, category, address, phone, website, cid/place_id, raw_ref}]`
- `paa`: `[{question, answer_snippet?, source_title?, source_url?, raw_ref}]`
- `related_searches`: `[{query, raw_ref}]`
- `ai_overview`: `{text_blocks[], citations[{title,url}], raw_ref}`
- `feature_flags`: `{has_paid, has_local_pack, has_paa, has_related_searches, has_ai_overview}`
- `parsing_warnings`: `[{module, issue, raw_path}]`

**Rationale:** SerpApi field names and module shapes can vary; your analysis should consume your schema, not theirs.

- [Confirmed] SerpApi documentation and community reports indicate variability (esp. ads). ([serpapi.com](https://serpapi.com/google-ads?utm_source=chatgpt.com)) ([github.com](https://github.com/serpapi/public-roadmap/issues/2253?utm_source=chatgpt.com))

---

## 3) Module extraction rules (robust to naming/layout shifts)

### 3.1 Ads / Sponsored (paid)

- [Confirmed] Parse `ads` if present.
- [Requires Verification] Also check for variant paid modules (shopping units, local services ads) depending on query category; SerpApi may represent them under different keys.
- [Confirmed] If paid is a must-have, optionally run `engine=google_ads` as a verification/augmentation step and merge results by URL/title/position. ([serpapi.com](https://serpapi.com/google-ads?utm_source=chatgpt.com))

### 3.2 Local / Map pack

- [Confirmed] In `engine=google`, parse the local pack module when present.
- [Confirmed] If you require the “map list” beyond the 3-pack (more listings, richer fields), call `engine=google_maps` and store separately, then either:
  - keep as `maps_results` (distinct from `local_pack`), or
  - merge top N into your canonical view with provenance. ([serpapi.com](https://serpapi.com/maps-local-results?utm_source=chatgpt.com))

### 3.3 PAA

- [Confirmed] In `engine=google`, extract the PAA/related-questions block when present.
- [Confirmed] If you need deeper capture, call `engine=google_related_questions` and append unique questions (dedupe by normalized question text). ([serpapi.com](https://serpapi.com/google-related-questions-api?utm_source=chatgpt.com))

### 3.4 PASF / Related searches

- [Confirmed] Extract `related_searches` (or equivalent) when present; store the query strings as-is.

### 3.5 AI Overview

- [Confirmed] Prefer AI Overview content from:
  1. the primary SERP response if present and complete,
  2. otherwise `engine=google_ai_overview`.

- [Confirmed] Store citations/links separately so you can analyze “sources used by AI.” ([serpapi.com](https://serpapi.com/google-ai-overview-api?utm_source=chatgpt.com))

---

## 4) Parameter discipline for Canada English (stability)

- [Confirmed] Always set `gl=ca` and `hl=en`.
- [Confirmed] Always set a **specific city-level `location`** (e.g., “Vancouver, British Columbia, Canada”) to stabilize local pack + ads behavior.
- [Requires Verification] Fix `device` across runs (desktop OR mobile) to avoid mixing incomparable SERP layouts.
- [Confirmed] Log all parameters per run; treat parameter drift as a data quality error.

---

## 5) Reliability engineering (must implement)

### 5.1 Rate limits, retries, idempotency

- [Confirmed] Implement exponential backoff on 429/5xx.
- [Confirmed] Idempotency: same `(q, gl, hl, location, device, date_bucket)` should reuse cached result unless `no_cache=true`.

### 5.2 Dedupe and canonicalization

- [Confirmed] Dedupe URLs with a canonicalizer:
  - remove tracking params (`utm_*`, `gclid`, etc.)
  - normalize scheme/hostname

- [Confirmed] Dedupe PAA by normalized question string (trim, collapse whitespace, lowercase).

### 5.3 Audit trail

- [Confirmed] Keep raw JSON forever (or long retention) so you can reparse when schemas change.

---

## 6) Acceptance tests (agent must write these)

For a fixed set of test queries, assert:

1. [Confirmed] The pipeline always produces `serp_norm.json` with required keys.
2. [Confirmed] Feature flags reflect presence/absence of modules.
3. [Requires Verification] For “known ad-heavy query” and “known local intent query,” ensure paid/local_pack frequently appear; if not, alert on location/device drift.
4. [Confirmed] AI Overview path: if `has_ai_overview==true`, ensure `ai_overview` is populated from either primary or `google_ai_overview` call.
5. [Confirmed] Schema resilience: if SerpApi response changes, pipeline still stores raw and emits `parsing_warnings` rather than failing.

---

## 7) Legal/operational note (don’t ignore)

- [Confirmed] Google has sued SerpApi over scraping/resale of Google results; treat this as platform risk and plan contingencies (vendor substitution, fallbacks). ([theverge.com](https://www.theverge.com/news/848365/google-scraper-lawsuit-serpapi?utm_source=chatgpt.com))

---

## Minimal “done” definition

- [Confirmed] For each query, you reliably produce: paid (when present), organic, local pack (when present), PAA (visible + optionally expanded), related searches (when present), and AI overview (when present, including citations), with raw JSON preserved and normalized output stable.

If you want, paste one of your real SerpApi responses (redact the key) and I’ll specify the **exact JSON paths** your agent should map into the canonical schema (so they don’t guess field names).
