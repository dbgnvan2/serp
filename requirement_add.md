Instructions for AI coding agent: Upgrade SERP capture completeness (SerpApi, Google, EN, Canada) 0) Scope

[Confirmed] Upgrade the existing Python script that generates market_analysis_v2.xlsx so it captures the missing SERP components: Local Pack / Map list, true PASF / Related Searches, SERP layout/structure, and richer paid + rich-result metadata, while preserving existing sheets (Overview, Organic_Results, PAA_Questions, Competitors_Ads, Strategy_Expansion).

[Confirmed] Target engine: SerpApi, Google, English, Canada.

1. Inputs and canonical request settings
   1.1 Primary SERP request (always run)

[Confirmed] Use engine=google

[Confirmed] Parameters:

q=<query>

gl=ca

hl=en

location=<City, Province, Canada> (must be explicit; do not rely on “Canada” only)

device=desktop (or mobile, but pick one and keep fixed across runs)

no_cache=true only when freshness is required; otherwise allow cache (cost control)

[Confirmed] Persist the entire raw JSON for each query-run to disk (e.g., raw/<run_id>\_google.json).

1.2 Conditional secondary requests (feature completion)

[Confirmed] AI Overview: if the Google SERP response indicates AI Overview exists but content is missing/partial, request:

engine=google_ai_overview with the same q/gl/hl/location/device.

[Confirmed] Map results depth: if Local Pack exists or if query intent is “local”, additionally request:

engine=google_maps (or SerpApi “Maps Local Results”) to capture a deeper “map list”.

[Confirmed] PAA expansion (optional but recommended if “all PAA” is a requirement):

engine=google_related_questions and merge additional questions.

Note: Do not “force” Google modules. We detect + capture what appears. [Confirmed]

2. New output artifacts (in addition to the XLS)

[Confirmed] In addition to writing Excel, output a normalized JSON per run (for audit + future-proofing):

normalized/<run_id>.serp_norm.json

[Confirmed] The JSON must include:

query_context: {q, gl, hl, location, device, captured_at_utc, run_id}

modules: ordered list of modules as they appear (see section 4)

paid, organic, local_pack, maps_results, paa, related_searches, ai_overview, rich_features

feature_flags

parsing_warnings

3. Add missing SERP components
   3.1 Local Pack / Map list (currently missing)
   3.1.1 Extract Local Pack from engine=google response

[Confirmed] Detect presence of Local Pack and extract a ranked list.

[Required fields] (minimum viable local pack record):

local_rank (1..N in pack)

name

rating (if present)

reviews_count (if present)

type/category (if present)

address (if present)

phone (if present)

website (if present)

place_id or cid (if present)

serpapi_result_id / raw pointer for traceability

[Requires Verification] Field names in SerpApi JSON vary across layouts; implement robust extraction by checking multiple candidate keys and emitting parsing_warnings when absent.

3.1.2 Extract “Map list” depth via engine=google_maps

[Confirmed] Capture more than the pack (where available).

[Required fields] for maps_results:

maps_rank

name, place_id/cid

rating, reviews_count

address, phone, website

category

latitude/longitude (if present)

[Confirmed] Write a new Excel sheet: Local_Pack and Maps_Results.

3.2 True PASF / Related searches (currently unclear/derived)
3.2.1 Extract Google-rendered related searches (PASF equivalent)

[Confirmed] From engine=google, extract the actual “related searches” block as shown by Google.

[Required fields]: related_rank

query_text

link (if present)

[Confirmed] Write a new sheet: Related_Searches.

[Confirmed] Rename the current Strategy_Expansion conceptually to “Derived_Expansions” (keep the sheet for backwards compatibility), and ensure the new sheet is clearly labeled as Google-rendered related searches.

3.3 SERP structure / layout (module order and presence)
3.3.1 Build a canonical ordered modules[] list

[Confirmed] Add an ordered list of SERP modules for each run:

e.g., ["top_ads", "ai_overview", "local_pack", "paa", "organic", "bottom_ads", "related_searches"]

[Requires Verification] SerpApi does not always provide an explicit “module order” array; if absent, infer order using available position metadata and known block precedence rules, and mark inferred order clearly:

modules[i].order_source = "explicit" | "inferred"

add parsing_warnings whenever inference is used.

[Confirmed] Add a new sheet: SERP_Modules with rows: {module_name, order_index, order_source, present_bool}.

3.3.2 Capture “position” fields for modules that support it

[Confirmed] For ads, organic, local, PAA, store position/rank within the module.

[Not Confirmed] Above-the-fold/below-the-fold cannot be reliably computed without pixel layout; do not fake it.

3.4 Rich SERP features (FAQ, video, images, knowledge panel, etc.)

[Confirmed] Add a Rich_Features sheet capturing presence and key items for:

knowledge_panel (if present)

image_pack

video_results

top_stories/news

shopping_results / product grids

sitelinks (organic sitelinks)

faq_rich_results / howto (if present)

[Requires Verification] Exact JSON keys vary; implement extraction via a mapping table + defensive checks; store raw pointer paths.

4. Improve “Sponsored / Ads” completeness (currently partial)
   4.1 Add ad position + ad block type

[Confirmed] For each ad record, add:

ad_rank_within_block

block = "top" | "bottom" | "unknown"

ad_type = "text" | "shopping" | "local_services" | "unknown"

[Requires Verification] If SerpApi response does not expose top vs bottom explicitly, infer cautiously using known keys (e.g., separate arrays) and mark inference.

4.2 Capture ad assets/extensions when present

[Confirmed] Store:

sitelinks[]

callouts[]

structured_snippets[]

phone (if present)

[Confirmed] Keep existing Competitors_Ads sheet but add columns rather than replacing.

5. AI Overview completeness upgrade
   5.1 Capture citations/provenance

[Confirmed] For AI Overviews, capture:

ai_overview_text (blocks/paragraphs)

citations[] each with {title, url, source_domain} when available

[Confirmed] Add sheet: AI_Overview_Citations.

5.2 Two-step retrieval

[Confirmed] Implement conditional retrieval via engine=google_ai_overview when the primary response contains incomplete AIO data.

6. Data quality, traceability, and robustness requirements
   6.1 Raw pointers / provenance fields

[Confirmed] Every extracted record must store:

source_engine (google / google_maps / google_ai_overview / related_questions)

run_id

raw_path (JSON path or a stable pointer string)

[Confirmed] If extraction fails for a module, do not crash; emit parsing_warnings and continue.

6.2 Dedupe rules

[Confirmed] Canonicalize URLs to dedupe:

strip utm\_\*, gclid, etc.

normalize scheme/hostname

[Confirmed] PAA dedupe by normalized question text (lowercase, trim, collapse spaces).

7. Excel changes (minimal disruption)

[Confirmed] Preserve existing sheets and columns.

[Confirmed] Add new sheets:

Local_Pack

Maps_Results

Related_Searches (Google-rendered)

SERP_Modules (layout)

Rich_Features

AI_Overview_Citations

[Confirmed] Keep Strategy_Expansion but clarify it is derived, not SERP-rendered, in the sheet header note.

8. Acceptance tests (must be added)

For a small fixed list of queries (include at least one local-intent query like “dentist near me”, one commercial query like “best crm software”, one informational query):

[Confirmed] Script runs end-to-end and produces:

XLS with all sheets

serp_norm.json with required keys

raw JSON files saved

[Confirmed] If Local Pack appears in the SERP, Local_Pack must have >= 1 row.

[Confirmed] If related searches appear, Related_Searches must have >= 1 row.

[Confirmed] If AI Overview appears, citations sheet populates when citations exist.

[Confirmed] No hard failures on missing modules; warnings logged instead.

9. Deliverables

[Confirmed] PR that adds:

modular extractor functions per module (ads/org/local/paa/related/aio/rich)

normalization layer producing serp_norm.json

Excel writer updated with new sheets + columns

test suite + small fixture runs

End of requirements
