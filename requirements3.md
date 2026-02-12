Requirements for an AI coding agent to implement the 4 “upgraded” capture items (SerpApi, Google, EN, Canada)
Scope

[Confirmed] Extend the current Python SERP capture pipeline (the one producing market_analysis_v2.xlsx) to add four missing capability areas:

Local Pack listing capture (ranked businesses + key fields)

AI Overview citations capture (sources + URLs + domains)

SERP module order capture (layout/real-estate displacement proxy)

Rich feature presence capture (FAQ/HowTo/video/image/news/shopping/knowledge panel)

[Confirmed] Target: Google Canada (gl=ca), English (hl=en), fixed city-level location.

1. Local Pack listing capture
   Functional requirements

[Confirmed] For each query run, the agent must detect whether a Local Pack is present in the Google SERP response.

[Confirmed] If present, extract all available Local Pack listings with rank order.

[Confirmed] Store results in:

new Excel sheet Local_Pack

normalized JSON local_pack[]

Required fields per Local Pack item (minimum)

[Confirmed] run_id

[Confirmed] query

[Confirmed] location

[Confirmed] local_rank (1..N)

[Confirmed] name

[Requires Verification] rating (nullable)

[Requires Verification] reviews_count (nullable)

[Requires Verification] category (nullable)

[Requires Verification] address (nullable)

[Requires Verification] phone (nullable)

[Requires Verification] website (nullable)

[Requires Verification] place_id or cid (nullable)

[Confirmed] raw_path (pointer to the JSON node)

Additional depth (recommended)

[Confirmed] If Local Pack is present OR query is local-intent, call engine=google_maps and extract a deeper Maps_Results list.

[Confirmed] Store in Excel sheet Maps_Results and JSON maps_results[].

Robustness requirements

[Confirmed] Extraction must not crash if fields are missing; store nulls + emit warning.

[Confirmed] Any ambiguity in key names must be handled by a mapping layer (candidate key list).

2. AI Overview citations capture
   Functional requirements

[Confirmed] Detect AI Overview presence.

[Confirmed] Capture:

AI Overview text blocks (already present in your older format)

AI Overview citations with URLs and titles

[Confirmed] If the primary SERP response does not provide complete AI Overview/citations, perform a second call:

engine=google_ai_overview (same query + locale + location)

Required fields per citation

[Confirmed] run_id

[Confirmed] query

[Confirmed] citation_rank (order as presented)

[Confirmed] title (nullable)

[Confirmed] url

[Confirmed] domain (parsed from url)

[Requires Verification] publisher (nullable; only if provided explicitly)

[Confirmed] raw_path

Output requirements

[Confirmed] Excel sheet: AI_Overview_Citations

[Confirmed] JSON: ai_overview.citations[]

Data quality requirements

[Confirmed] Normalize URLs (strip tracking parameters) while preserving raw URL in a separate field if desired.

[Confirmed] Dedupe citations by canonical URL.

3. SERP module order capture
   Functional requirements

[Confirmed] For each query run, produce an ordered list of “modules” as they appear on the SERP.

[Confirmed] Modules to track (minimum set):

top_ads

ai_overview

local_pack

organic

paa

related_searches

bottom_ads

knowledge_panel (if present)

top_stories (if present)

video_pack / image_pack (if present)

shopping_results (if present)

Required output

[Confirmed] Excel sheet: SERP_Modules with rows:

run_id, query, module_name, order_index, present_bool, order_source, raw_path_or_basis

[Confirmed] JSON: modules[] list with:

module_name

order_index

present_bool

order_source = "explicit" | "inferred"

Constraints / realism

[Confirmed] If SerpApi provides explicit module ordering, use it.

[Confirmed] If explicit ordering is not available, infer ordering using:

presence of “top ads vs bottom ads arrays”

known SERP precedence

any provided “position” fields

[Confirmed] All inference must be labeled as order_source="inferred" and logged in warnings.

[Confirmed] Do not attempt pixel-level “above-the-fold” determinations (unless you add a rendering step, which is out of scope).

4. Rich feature presence capture
   Functional requirements

[Confirmed] Detect presence/absence of major rich SERP features and capture key items when feasible.

[Confirmed] At minimum, presence flags for:

knowledge_panel

featured_snippet

faq_rich_results

howto_rich_results

top_stories / news module

video_pack

image_pack

shopping_results

[Requires Verification] If present, capture a small list of representative items per feature (N=3–10), with rank if available.

Required fields per rich-feature item (when items exist)

[Confirmed] run_id

[Confirmed] query

[Confirmed] feature_type

[Requires Verification] rank_within_feature

[Requires Verification] title

[Requires Verification] url

[Requires Verification] source / domain

[Confirmed] raw_path

Output requirements

[Confirmed] Excel sheet: Rich_Features

[Confirmed] JSON: rich_features[] (feature objects with items)

Robustness requirements

[Confirmed] Feature detection must not be brittle to missing keys; implement key-candidate mapping and warnings.

[Confirmed] If only presence is detectable, still record the presence flag even if items can’t be parsed.

Cross-cutting requirements (apply to all 4 upgrades)
Logging and warnings

[Confirmed] Add parsing_warnings[] capturing:

module/feature name

what was expected

what was missing

which raw keys were checked

Traceability

[Confirmed] Every extracted row must carry:

run_id

query

gl/hl/location/device

raw_path (or equivalent pointer)

Storage

[Confirmed] Persist raw SERP JSON for each engine call:

raw/<run_id>\_google.json

raw/<run_id>\_ai_overview.json (if called)

raw/<run_id>\_maps.json (if called)

Tests (minimum)

[Confirmed] Add a test harness with 3 fixed queries:

local intent (should trigger local pack often)

informational (should trigger PAA often)

commercial (more likely to show ads)

[Confirmed] Tests assert:

new sheets are created

JSON normalized output includes required keys

code does not crash if modules absent

Definition of Done

[Confirmed] For a real run, the output must include:

Local_Pack (when local pack exists)

AI_Overview_Citations (when AI overview exists and citations are present)

SERP_Modules (always)

Rich_Features (always at least presence flags)

[Confirmed] The pipeline continues to produce the original sheets without breaking.

If you want, paste the folder/file structure of your script and I’ll format these into a single “agent ticket” with file-level implementation steps and acceptance criteria.
