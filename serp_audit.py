import pandas as pd
from serpapi import GoogleSearch
import time
import os
import re
import random
from dotenv import load_dotenv
import logging
import json
from datetime import datetime
from collections import Counter
from openpyxl import Workbook
import hashlib
import generate_insight_report
import generate_content_brief
import yaml
import metrics
from urllib.parse import urlparse
from classifiers import ContentClassifier, EntityClassifier
from url_enricher import UrlEnricher
from storage import SerpStorage

try:
    from textblob import TextBlob
    TEXTBLOB_AVAILABLE = True
except ImportError:
    TEXTBLOB_AVAILABLE = False
try:
    from wordcloud import WordCloud
    import matplotlib.pyplot as plt
    VISUALIZATION_AVAILABLE = True
except ImportError:
    VISUALIZATION_AVAILABLE = False

# --- CONFIGURATION ---
load_dotenv()
API_KEY = os.getenv("SERPAPI_KEY")

# Load Config
CONFIG = {}
if os.path.exists("config.yml"):
    with open("config.yml", "r") as f:
        CONFIG = yaml.safe_load(f) or {}

INPUT_FILE = CONFIG.get("files", {}).get("input_csv", "keywords.csv")
OUTPUT_FILE = CONFIG.get("files", {}).get(
    "output_xlsx", "market_analysis_v2.xlsx")
OUTPUT_JSON = CONFIG.get("files", {}).get(
    "output_json", "market_analysis_v2.json")
OUTPUT_MD = CONFIG.get("files", {}).get("output_md", "market_analysis_v2.md")

LOCATION = CONFIG.get("serpapi", {}).get(
    "location", "Vancouver, British Columbia, Canada")
GOOGLE_ENGINE = CONFIG.get("serpapi", {}).get("engine", "google")
GOOGLE_GL = CONFIG.get("serpapi", {}).get("gl", "ca")
GOOGLE_HL = CONFIG.get("serpapi", {}).get("hl", "en")
GOOGLE_DEVICE = CONFIG.get("serpapi", {}).get("device", "desktop")
GOOGLE_NUM = int(CONFIG.get("serpapi", {}).get("num", 100))
GOOGLE_MAX_PAGES = max(1, int(CONFIG.get("serpapi", {}).get("google_max_pages", 3)))
GOOGLE_MAX_RESULTS = max(10, int(CONFIG.get("serpapi", {}).get("google_max_results", 300)))
MAPS_MAX_PAGES = max(1, int(CONFIG.get("serpapi", {}).get("maps_max_pages", 3)))
RETRY_MAX_ATTEMPTS = max(1, int(CONFIG.get("serpapi", {}).get("retry_max_attempts", 3)))
RETRY_BACKOFF_SECONDS = float(CONFIG.get("serpapi", {}).get("retry_backoff_seconds", 1.0))
REQUEST_DELAY_SECONDS = float(CONFIG.get("serpapi", {}).get("request_delay_seconds", 0.2))
AI_FALLBACK_WITHOUT_LOCATION = bool(
    CONFIG.get("serpapi", {}).get("ai_fallback_without_location", True)
)
RELATED_QUESTIONS_AI_FOLLOWUP = bool(
    CONFIG.get("serpapi", {}).get("related_questions_ai_followup", True)
)
RELATED_QUESTIONS_AI_MAX_CALLS = max(
    0, int(CONFIG.get("serpapi", {}).get("related_questions_ai_max_calls", 5))
)
FORCE_LOCAL_INTENT = CONFIG.get("app", {}).get("force_local_intent", True)
ENRICHMENT_ENABLED = CONFIG.get("enrichment", {}).get("enabled", True)
MAX_URLS_TO_ENRICH = CONFIG.get(
    "enrichment", {}).get("max_urls_per_keyword", 5)

STOP_WORDS = {"the", "and", "to", "of", "a", "in", "is", "for", "on", "with", "as", "at", "by", "an", "be", "or", "are", "from", "that",
              "this", "it", "we", "our", "us", "can", "will", "your", "you", "my", "me", "not", "have", "has", "but", "so", "if", "their", "they",
              "vancouver", "bc", "british", "columbia", "canada", "north", "west", "counselling", "counseling", "therapy", "therapist",
              "counsellor", "counselor", "service", "services", "clinic", "centre", "center", "help", "support",
              "highlytrained"}


def _env_bool(name, default=False):
    """Read boolean env var with common truthy/falsey values."""
    raw = os.getenv(name)
    if raw is None:
        return default
    val = raw.strip().lower()
    if val in {"1", "true", "yes", "y", "on"}:
        return True
    if val in {"0", "false", "no", "n", "off"}:
        return False
    return default


AI_QUERY_ALTERNATIVES_ENABLED = _env_bool(
    "SERP_ENABLE_AI_QUERY_ALTERNATIVES",
    bool(CONFIG.get("app", {}).get("ai_query_alternatives_enabled", False))
)


def setup_logging(run_id):
    """Sets up logging for the script."""
    log_file = f"raw/{run_id}/serp_api.log"
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )


def _fetch_serp_api(params):
    """Internal function to query SerpApi with retry logic."""
    # Redact API Key for logging
    log_params = params.copy()
    if "api_key" in log_params:
        log_params["api_key"] = "REDACTED"
    logging.info(f"API Call Parameters: {json.dumps(log_params, indent=2)}")
    for attempt in range(1, RETRY_MAX_ATTEMPTS + 1):
        try:
            search = GoogleSearch(params)
            results = search.get_dict()
            logging.info(f"API Return Message: {json.dumps(results, indent=2)}")
            if "error" in results:
                logging.error(f"API Error (attempt {attempt}): {results['error']}")
                if attempt == RETRY_MAX_ATTEMPTS:
                    return None
            else:
                return results
        except Exception as e:
            logging.error(
                f"Fetch error (attempt {attempt}) for query '{params.get('q', 'N/A')}': {e}"
            )
            if attempt == RETRY_MAX_ATTEMPTS:
                logging.critical(
                    f"CRITICAL ERROR fetching with params {params.get('q')}: {e}"
                )
                return None
        sleep_s = RETRY_BACKOFF_SECONDS * (2 ** (attempt - 1)) + random.uniform(0, 0.2)
        time.sleep(sleep_s)
    return None


def _parse_start_from_pagination(results):
    """Returns the next `start` integer from serpapi_pagination.next, if present."""
    pagination = results.get("serpapi_pagination", {}) if isinstance(results, dict) else {}
    next_link = pagination.get("next")
    if not next_link:
        return None
    match = re.search(r"[?&]start=(\d+)", next_link)
    if not match:
        return None
    return int(match.group(1))


def _merge_google_pages(pages):
    """Merge selected paginated Google fields into a single response object."""
    if not pages:
        return {}

    merged = dict(pages[0])
    merged["organic_results"] = []
    merged["related_questions"] = []
    merged["related_searches"] = []
    merged["discussions_and_forums"] = []
    merged["pagination_pages_fetched"] = len(pages)

    seen_org = set()
    seen_paa = set()
    seen_rel = set()
    seen_forums = set()

    for page in pages:
        for item in page.get("organic_results", []) or []:
            key = item.get("link") or f"{item.get('title')}|{item.get('position')}"
            if key in seen_org:
                continue
            seen_org.add(key)
            merged["organic_results"].append(item)

        for item in page.get("related_questions", []) or []:
            key = item.get("question")
            if not key or key in seen_paa:
                continue
            seen_paa.add(key)
            merged["related_questions"].append(item)

        for item in page.get("related_searches", []) or []:
            key = item.get("query")
            if not key or key in seen_rel:
                continue
            seen_rel.add(key)
            merged["related_searches"].append(item)

        for item in page.get("discussions_and_forums", []) or []:
            key = item.get("link") or item.get("title")
            if not key or key in seen_forums:
                continue
            seen_forums.add(key)
            merged["discussions_and_forums"].append(item)

        if "ai_overview" not in merged and page.get("ai_overview"):
            merged["ai_overview"] = page["ai_overview"]

    return merged


def _merge_maps_pages(pages):
    """Merge paginated maps local results into a single response object."""
    if not pages:
        return {}
    merged = dict(pages[0])
    merged["local_results"] = []
    merged["pagination_pages_fetched"] = len(pages)

    seen_places = set()
    for page in pages:
        for place in page.get("local_results", []) or []:
            key = place.get("place_id") or place.get("data_id") or f"{place.get('title')}|{place.get('address')}"
            if key in seen_places:
                continue
            seen_places.add(key)
            merged["local_results"].append(place)
    return merged


def _extract_text_blocks_text(item):
    """Flattens AI related-question text blocks into a single snippet."""
    blocks = item.get("text_blocks", [])
    if not isinstance(blocks, list):
        return None
    parts = []
    for block in blocks:
        if not isinstance(block, dict):
            continue
        if block.get("text"):
            parts.append(block.get("text"))
        elif block.get("snippet"):
            parts.append(block.get("snippet"))
        if isinstance(block.get("list"), list):
            for li in block.get("list"):
                if isinstance(li, dict) and li.get("snippet"):
                    parts.append(li.get("snippet"))
    if not parts:
        return None
    return " ".join(parts)


def save_raw_json(run_id, engine, data):
    """Saves raw JSON output to a structured folder."""
    output_dir = f"raw/{run_id}"
    os.makedirs(output_dir, exist_ok=True)
    file_path = f"{output_dir}/{engine}_response.json"
    with open(file_path, 'w') as f:
        json.dump(data, f, indent=2)


def fetch_serp_data(keyword, run_id):
    """
    Orchestrates primary and secondary SerpApi calls for a given keyword.
    Returns a dictionary of raw results from different engines.
    """
    all_results = {}
    aio_log = {
        "Run_ID": run_id,
        "Keyword": keyword,
        "has_ai_overview": False,
        "ai_overview_mode": "not_present",
        "page_token_received_at": None,
        "followup_started_at": None,
        "followup_latency_ms": None,
        "google_pages_fetched": 0,
        "maps_pages_fetched": 0,
        "related_questions_ai_calls": 0,
        "error": None
    }

    # --- 1. Primary SERP Request ---
    if FORCE_LOCAL_INTENT and LOCATION.split(",")[0].lower() not in keyword.lower():
        query_term = f"{keyword} {LOCATION}"
    else:
        query_term = keyword

    # Calculate params_hash for auditability
    # We hash the params before the API key is added (or we rely on the fact that API_KEY is constant)
    # To be safe and consistent, we hash the dictionary structure excluding the API key if we wanted,
    # but here we just hash the definition before the call.
    primary_params = {
        "engine": GOOGLE_ENGINE,
        "q": query_term,
        "location": LOCATION,
        "hl": GOOGLE_HL,
        "gl": GOOGLE_GL,
        "api_key": API_KEY,
        "num": GOOGLE_NUM,
        "device": GOOGLE_DEVICE,
        "no_cache": True
    }

    # Create a stable hash of the parameters for audit trails
    params_hash = hashlib.md5(json.dumps(
        primary_params, sort_keys=True).encode()).hexdigest()

    logging.info(f"  - Fetching main SERP for '{query_term}'...")
    primary_results = _fetch_serp_api(primary_params)

    # Metadata capture
    created_at = datetime.now().isoformat()
    google_url = "N/A"

    if not primary_results:
        # Even on failure, we return metadata structure
        return {}, aio_log, {"run_id": run_id, "created_at": created_at, "google_url": "N/A", "params_hash": params_hash}

    google_url = primary_results.get(
        "search_metadata", {}).get("google_url", "N/A")

    # Bundle metadata for downstream processing
    query_metadata = {"run_id": run_id, "created_at": created_at,
                      "google_url": google_url, "params_hash": params_hash}

    # Fetch additional Google pages, then merge.
    google_pages = [primary_results]
    seen_starts = set([0])
    next_start = _parse_start_from_pagination(primary_results)

    while (
        next_start is not None
        and len(google_pages) < GOOGLE_MAX_PAGES
        and len(_merge_google_pages(google_pages).get("organic_results", [])) < GOOGLE_MAX_RESULTS
    ):
        if next_start in seen_starts:
            break
        seen_starts.add(next_start)
        page_params = dict(primary_params)
        page_params["start"] = next_start
        logging.info(f"  - Fetching Google page start={next_start}...")
        page_results = _fetch_serp_api(page_params)
        if not page_results:
            break
        google_pages.append(page_results)
        next_start = _parse_start_from_pagination(page_results)
        time.sleep(REQUEST_DELAY_SECONDS)

    primary_results = _merge_google_pages(google_pages)
    aio_log["google_pages_fetched"] = len(google_pages)
    query_metadata["google_pages_fetched"] = len(google_pages)

    # Log top-level keys for debugging
    logging.info(f"Main SERP Keys: {sorted(primary_results.keys())}")

    # Log module booleans for easier debugging
    module_flags = {
        "has_ai_overview": "ai_overview" in primary_results,
        "has_local_results": "local_results" in primary_results,
        "has_knowledge_panel": "knowledge_graph" in primary_results,
        "has_ads": "ads" in primary_results,
        "has_related_questions": "related_questions" in primary_results
    }
    logging.info(f"Module Flags: {json.dumps(module_flags, indent=2)}")

    all_results['google'] = primary_results
    all_results['google_pages'] = google_pages
    save_raw_json(run_id, 'google', primary_results)

    # Update AIO log with audit fields
    aio_log["created_at"] = created_at
    aio_log["google_url"] = google_url
    aio_log["params_hash"] = params_hash

    # --- 2. AI Overview Request (Conditional) ---
    # Logic: Only call if 'ai_overview' exists AND has a 'page_token'
    aio_data = primary_results.get("ai_overview")

    if not aio_data:
        aio_log["ai_overview_mode"] = "not_present"
        logging.info("AIO absent in main SERP.")

        if AI_FALLBACK_WITHOUT_LOCATION:
            logging.info("  - Running AI fallback probe without location bias...")
            fallback_params = {
                "engine": GOOGLE_ENGINE,
                "q": keyword,
                "hl": GOOGLE_HL,
                "gl": GOOGLE_GL,
                "device": GOOGLE_DEVICE,
                "num": 10,
                "api_key": API_KEY,
                "no_cache": True
            }
            fallback_results = _fetch_serp_api(fallback_params)
            if fallback_results and fallback_results.get("ai_overview"):
                all_results["google_ai_overview_probe"] = fallback_results.get("ai_overview", {})
                aio_log["has_ai_overview"] = True
                aio_log["ai_overview_mode"] = "fallback_without_location"
                save_raw_json(run_id, 'google_ai_overview_probe', fallback_results)
            else:
                logging.info("  - AI fallback probe also returned no ai_overview.")
    else:
        aio_log["has_ai_overview"] = True
        page_token = aio_data.get("page_token")

        if page_token:
            aio_log["ai_overview_mode"] = "token_followup"
            aio_log["page_token_received_at"] = datetime.now().isoformat()

            # Construct params for AIO call
            # Note: We do NOT send 'q' or 'location' again, just the token and engine.
            aio_params = {
                "engine": "google_ai_overview",
                "page_token": page_token,
                "api_key": API_KEY,
                "no_cache": True  # Maintain freshness preference
            }

            start_time = datetime.now()
            aio_log["followup_started_at"] = start_time.isoformat()

            logging.info(f"  - Fetching AI Overview (token found)...")
            aio_results = _fetch_serp_api(aio_params)

            end_time = datetime.now()
            aio_log["followup_latency_ms"] = (
                end_time - start_time).total_seconds() * 1000

            if aio_results:
                aio_log["ai_overview_mode"] = "token_followup_success"
                all_results['google_ai_overview'] = aio_results
                save_raw_json(run_id, 'google_ai_overview', aio_results)
            else:
                aio_log["ai_overview_mode"] = "token_followup_failed"
                aio_log["error"] = "API call returned None"
        else:
            # AI Overview is present but fully contained in the main response (no token needed)
            aio_log["ai_overview_mode"] = "direct_in_main"

    # --- 2b. Related Questions Follow-up (AI-overview type) ---
    if RELATED_QUESTIONS_AI_FOLLOWUP and RELATED_QUESTIONS_AI_MAX_CALLS > 0:
        related_pages = []
        token_queue = []
        seen_tokens = set()

        for item in primary_results.get("related_questions", []) or []:
            token = item.get("next_page_token")
            if token:
                token_queue.append(token)

        while token_queue and len(related_pages) < RELATED_QUESTIONS_AI_MAX_CALLS:
            token = token_queue.pop(0)
            if token in seen_tokens:
                continue
            seen_tokens.add(token)

            rq_params = {
                "engine": "google_related_questions",
                "next_page_token": token,
                "api_key": API_KEY,
                "no_cache": True
            }
            rq_results = _fetch_serp_api(rq_params)
            if not rq_results:
                continue

            related_pages.append(rq_results)
            for rq_item in rq_results.get("related_questions", []) or []:
                next_token = rq_item.get("next_page_token")
                if next_token and next_token not in seen_tokens:
                    token_queue.append(next_token)

            time.sleep(REQUEST_DELAY_SECONDS)

        if related_pages:
            all_results["google_related_questions"] = related_pages
            aio_log["related_questions_ai_calls"] = len(related_pages)
            save_raw_json(run_id, "google_related_questions", related_pages)

    # --- 3. Google Maps Request (Conditional) ---
    # Logic: Call if 'local_results' are present OR if local intent is forced.
    has_local_pack = "local_results" in primary_results

    if has_local_pack or FORCE_LOCAL_INTENT:
        logging.info(
            "  - Fetching Google Maps results (Local Pack detected or Forced)...")

        maps_params = {
            "engine": "google_maps",
            "q": query_term,
            "type": "search",
            "hl": GOOGLE_HL,
            "gl": GOOGLE_GL,
            "api_key": API_KEY,
            "no_cache": True
        }

        # Attempt to extract 'll' (latitude, longitude) from metadata to pin location
        # This ensures the maps view matches the SERP location context
        meta = primary_results.get("serpapi_search_metadata", {}) or primary_results.get("search_metadata", {})
        maps_url = meta.get("google_maps_url", "")
        ll_match = re.search(r"[?&]ll=([0-9\.\-]+,[0-9\.\-]+)", maps_url)

        if ll_match:
            maps_params["ll"] = ll_match.group(1)
        else:
            # Fallback to string location if coordinates not found
            maps_params["location"] = LOCATION
            # Required when using location (zoom level)
            maps_params["z"] = "14"

        maps_results = _fetch_serp_api(maps_params)
        if maps_results:
            maps_pages = [maps_results]
            seen_map_starts = set([0])
            next_map_start = _parse_start_from_pagination(maps_results)

            while next_map_start is not None and len(maps_pages) < MAPS_MAX_PAGES:
                if next_map_start in seen_map_starts:
                    break
                seen_map_starts.add(next_map_start)
                maps_page_params = dict(maps_params)
                maps_page_params["start"] = next_map_start
                logging.info(f"  - Fetching Google Maps page start={next_map_start}...")
                maps_page = _fetch_serp_api(maps_page_params)
                if not maps_page:
                    break
                maps_pages.append(maps_page)
                next_map_start = _parse_start_from_pagination(maps_page)
                time.sleep(REQUEST_DELAY_SECONDS)

            merged_maps = _merge_maps_pages(maps_pages)
            aio_log["maps_pages_fetched"] = len(maps_pages)
            query_metadata["maps_pages_fetched"] = len(maps_pages)
            all_results['google_maps'] = merged_maps
            all_results['google_maps_pages'] = maps_pages
            save_raw_json(run_id, 'google_maps', merged_maps)

    return all_results, aio_log, query_metadata


def parse_data(keyword, results, query_metadata):
    """
    The 'Vacuum' function: Sucks up PAA, Related Searches, and Ads.
    Now handles a dictionary of results from multiple engines.
    """
    primary_results = results.get('google', {})
    parsing_warnings = []

    # Common fields for every row in every sheet (Auditability)
    common_fields = {
        "Root_Keyword": keyword,
        "Run_ID": query_metadata["run_id"],
        "Created_At": query_metadata["created_at"],
        "Google_URL": query_metadata["google_url"],
        "Params_Hash": query_metadata["params_hash"]
    }

    if not primary_results:
        return {}, [], [], [], [], [], [], [], [], []

    # --- 0. SERP MODULES & RICH FEATURES ---
    serp_modules = []
    rich_features = []
    module_keys = ["top_ads", "ai_overview", "local_pack", "local_results", "related_questions", "organic_results",
                   "bottom_ads", "related_searches", "knowledge_graph", "inline_videos", "top_stories",
                   "image_pack", "shopping_results", "discussions_and_forums", "filters", "local_map"]
    for i, key in enumerate(module_keys):
        if key in primary_results:
            serp_modules.append({**common_fields, "Module": key,
                                "Order": i+1, "Present": True, "Order_Source": "inferred"})
            if key == "knowledge_graph":
                if not primary_results[key].get("title"):
                    parsing_warnings.append({**common_fields, "Module": "knowledge_graph",
                                            "Field": "title", "Message": "Knowledge Graph title not found"})
                rich_features.append({**common_fields,
                                     "Feature": "Knowledge Panel", "Details": primary_results[key].get("title")})
            if key == "inline_videos":
                rich_features.append({**common_fields,
                                     "Feature": "Video Carousel", "Details": f"{len(primary_results[key])} videos"})
            if key == "image_pack":
                rich_features.append({**common_fields,
                                     "Feature": "Image Pack", "Details": f"{len(primary_results[key])} images"})
            if key == "top_stories":
                rich_features.append({**common_fields,
                                     "Feature": "Top Stories", "Details": f"{len(primary_results[key])} stories"})
            if key == "shopping_results":
                rich_features.append({**common_fields,
                                     "Feature": "Shopping Results", "Details": f"{len(primary_results[key])} results"})

    # --- 1. OVERVIEW (Top Organic) ---
    organic = primary_results.get("organic_results") or []

    metrics = {**common_fields,
               "Search_Query_Used": primary_results.get("search_parameters", {}).get("q"),
               "Total_Results": primary_results.get("search_information", {}).get("total_results"),
               }

    # --- SERP TERRAIN ANALYSIS (What features exist?) ---
    features = []
    if "inline_videos" in primary_results:
        features.append("Video Carousel")
    if "knowledge_graph" in primary_results:
        features.append("Knowledge Panel")
    if "answer_box" in primary_results:
        features.append("Featured Snippet")
    if "local_results" in primary_results:
        features.append("Local Map Pack")
    if "shopping_results" in primary_results:
        features.append("Shopping")
    if "top_stories" in primary_results:
        features.append("Top Stories")
    if "image_pack" in primary_results:
        features.append("Image Pack")

    metrics["SERP_Features"] = ", ".join(
        features) if features else "Standard Organic"

    # --- FEATURED SNIPPET (Position 0) ---
    answer_box = primary_results.get("answer_box", {})
    if not answer_box.get("title"):
        parsing_warnings.append({**common_fields, "Module": "answer_box",
                                "Field": "title", "Message": "Featured Snippet title not found"})
    metrics["Featured_Snippet_Title"] = answer_box.get("title", "N/A")
    metrics["Featured_Snippet_Link"] = answer_box.get("link", "N/A")
    metrics["Featured_Snippet_Snippet"] = answer_box.get("snippet", "N/A")

    # --- AI OVERVIEW (SGE) ---
    ai_candidate = (
        results.get('google_ai_overview')
        or results.get('google_ai_overview_probe')
        or primary_results.get('ai_overview', {})
    ) or {}
    # google_ai_overview returns a full response envelope with ai_overview nested.
    ai_overview_data = ai_candidate.get("ai_overview", ai_candidate)

    related_ai_items = []
    for page in results.get("google_related_questions", []) or []:
        for rq_item in page.get("related_questions", []) or []:
            if rq_item.get("type") == "ai_overview":
                related_ai_items.append(rq_item)

    related_ai_text = None
    if related_ai_items:
        related_ai_text = _extract_text_blocks_text(related_ai_items[0])

    # B. Don't overload "Has_AI_Overview"
    metrics["Has_Main_AI_Overview"] = bool(ai_overview_data)

    ai_overview_text = ai_overview_data.get("snippet") or _extract_text_blocks_text(ai_overview_data)

    if not ai_overview_text:
        parsing_warnings.append({**common_fields, "Module": "ai_overview",
                                "Field": "snippet", "Message": "AI Overview snippet not found"})

    metrics["AI_Overview"] = ai_overview_text or related_ai_text or "N/A"
    metrics["AI_Reading_Level"] = calculate_reading_level(
        metrics["AI_Overview"])
    metrics["AI_Sentiment"] = calculate_sentiment(metrics["AI_Overview"])
    metrics["AI_Subjectivity"] = calculate_subjectivity(metrics["AI_Overview"])

    ai_citations = []
    citation_rows = ai_overview_data.get("citations") or ai_overview_data.get("references") or []
    for citation in citation_rows:
        if not isinstance(citation, dict):
            continue
        if not citation.get("link"):
            parsing_warnings.append({**common_fields,
                                     "Module": "ai_citations", "Field": "link", "Message": "Citation link not found"})
        ai_citations.append({**common_fields,
                             "Title": citation.get("title"),
                             "Link": citation.get("link"),
                             "Source": citation.get("source"),
                             })

    # Capture Top 3 Organic Results (as per Project Context)
    for i in range(3):
        rank = i + 1
        if i < len(organic):
            if not organic[i].get("title"):
                parsing_warnings.append({**common_fields, "Module": "organic_results",
                                        "Field": "title", "Message": f"Rank {rank} title not found"})
            # C. Source-of-truth row-level check
            metrics[f"Rank_{rank}_Title"] = organic[i].get("title", "N/A")
            metrics[f"Rank_{rank}_Link"] = organic[i].get("link", "N/A")
            metrics[f"Rank_{rank}_Snippet"] = organic[i].get("snippet", "N/A")
            metrics[f"Rank_{rank}_Position"] = organic[i].get(
                "position", "N/A")
        else:
            metrics[f"Rank_{rank}_Title"] = "N/A"
            metrics[f"Rank_{rank}_Link"] = "N/A"
            metrics[f"Rank_{rank}_Snippet"] = "N/A"
            metrics[f"Rank_{rank}_Position"] = "N/A"

    # --- ALL ORGANIC RESULTS ---
    organic_list = []
    for item in organic:
        organic_list.append({**common_fields,
                             "Rank": item.get("position", "N/A"),
                             "Title": item.get("title", "N/A"),
                             "Link": item.get("link", "N/A"),
                             "Snippet": item.get("snippet", "N/A"),
                             "Source": item.get("source", "N/A"),
                             "Content_Type": "N/A",
                             "Entity_Type": "N/A",
                             "Word_Count": "N/A",
                             "Rank_Delta": "N/A"
                             })

    # --- 2. PAA INTELLIGENCE (Questions) ---
    paa_list = []

    # Bridge Strategy Triggers
    trigger_map = {
        "Commercial": ["cost", "price", "how much", "fees"],
        "Distress": ["survive", "divorce", "infidelity", "leave", "separation"],
        "Reactivity": ["narcissist", "toxic", "signs", "mean", "angry", "cut off", "hate"]
    }

    metrics["Has_PAA_AI_Overview"] = False

    if "related_questions" in primary_results:
        for i, item in enumerate(primary_results["related_questions"]):
            if not item.get("question"):
                parsing_warnings.append({**common_fields, "Module": "related_questions",
                                        "Field": "question", "Message": "PAA question not found"})
            question_text = item.get("question", "")
            question_lower = question_text.lower() if question_text else ""

            category = "General"
            score = 1

            if question_lower:
                for cat, triggers in trigger_map.items():
                    if any(t in question_lower for t in triggers):
                        category = cat
                        score = 10
                        break

            # Check for AI Overview in PAA
            is_ai_paa = item.get("type") == "ai_overview"
            if is_ai_paa:
                metrics["Has_PAA_AI_Overview"] = True
                # Flatten text blocks if present
                if "text_blocks" in item:
                    # Simple flattening of text blocks
                    item["snippet"] = " ".join(
                        [b.get("text", "") for b in item.get("text_blocks", [])])

            paa_list.append({**common_fields,
                             "Rank": i + 1,
                             "Score": score,
                             "Category": category,
                             "Is_AI_Generated": is_ai_paa,
                             "Question": question_text,
                             "Snippet": item.get("snippet"),
                             "Link": item.get("link")
                             })

    if related_ai_items:
        metrics["Has_PAA_AI_Overview"] = True
        base_rank = len(paa_list)
        for idx, item in enumerate(related_ai_items, start=1):
            text_snippet = _extract_text_blocks_text(item)
            refs = item.get("references", [])
            first_ref = refs[0] if refs and isinstance(refs[0], dict) else {}
            paa_list.append({**common_fields,
                             "Rank": base_rank + idx,
                             "Score": 10,
                             "Category": "General",
                             "Is_AI_Generated": True,
                             "Question": item.get("question"),
                             "Snippet": text_snippet,
                             "Link": first_ref.get("link")
                             })

    # --- 3. STRATEGY EXPANSION (Related Searches & PASF) ---
    # This is the "Gold Mine" for new content ideas
    expansion_list = []

    # A. Standard "Related Searches" (Bottom of page)
    if "related_searches" in primary_results:
        for item in primary_results["related_searches"]:
            expansion_list.append({**common_fields,
                                   "Type": "Related Search",
                                   "Term": item.get("query"),
                                   "Link": item.get("link")
                                   })

    if "discussions_and_forums" in primary_results:
        for item in primary_results["discussions_and_forums"]:
            expansion_list.append({**common_fields,
                                   "Type": "Discussion/Forum",
                                   "Term": item.get("title"),
                                   "Link": item.get("link")
                                   })

    if "filters" in primary_results:
        for item in primary_results["filters"]:
            expansion_list.append({**common_fields,
                                   "Type": "SERP Filter",
                                   "Term": item.get("name"),
                                   "Link": item.get("link")
                                   })

    # B. "People Also Search For" (Often inside organic results)
    if "inline_people_also_search_for" in primary_results:
        for item in primary_results["inline_people_also_search_for"]:
            expansion_list.append({**common_fields,
                                   "Type": "PASF (Inline)",
                                   "Term": item.get("title"),
                                   "Link": item.get("link")
                                   })

    # C. "People Also Search For" (Knowledge Graph / Box)
    if "people_also_search_for" in primary_results:
        for item in primary_results["people_also_search_for"]:
            expansion_list.append({**common_fields,
                                   "Type": "PASF (Box)",
                                   "Term": item.get("name") or item.get("title"),
                                   "Link": item.get("link")
                                   })

    # --- 4. COMPETITOR RECON (Ads & Maps) ---
    competitor_list = []

    # Ads
    if "ads" in primary_results:
        for ad in primary_results["ads"]:
            if not ad.get("title"):
                parsing_warnings.append({**common_fields,
                                         "Module": "ads", "Field": "title", "Message": "Ad title not found"})
            competitor_list.append({**common_fields,
                                    "Type": "Paid Ad",
                                    "Block_Position": "top" if ad.get("block_position") == "top" else "bottom",
                                    "Name": ad.get("title"),
                                    "Snippet": ad.get("description"),
                                    "Position": ad.get("position"),
                                    "Link": ad.get("link"),
                                    "Sitelinks": json.dumps(ad.get("sitelinks")),
                                    "Callouts": json.dumps(ad.get("callouts"))
                                    })

    # --- 5. LOCAL PACK & MAPS RESULTS ---
    all_local_pack = []
    # a) From the main SERP
    if "local_results" in primary_results and "places" in primary_results["local_results"]:
        for i, place in enumerate(primary_results["local_results"]["places"]):
            if not place.get("title"):
                parsing_warnings.append({**common_fields, "Module": "local_results",
                                        "Field": "title", "Message": "Local Pack title not found"})
            website = place.get("links", {}).get(
                "website") or place.get("website")
            all_local_pack.append({**common_fields,
                                   "Source": "google_serp",
                                   "Rank": i + 1,
                                   "Name": place.get("title"),
                                   "Category": place.get("type"),
                                   "Rating": place.get("rating"),
                                   "Reviews": place.get("reviews"),
                                   "Address": place.get("address"),
                                   "Phone": place.get("phone"),
                                   "Website": website,
                                   "Place_ID": place.get("place_id")
                                   })

    # b) From the dedicated maps results
    maps_results = results.get('google_maps', {})
    if "local_results" in maps_results:
        for i, place in enumerate(maps_results["local_results"]):
            if not place.get("title"):
                parsing_warnings.append({**common_fields,
                                         "Module": "google_maps", "Field": "title", "Message": "Maps title not found"})
            all_local_pack.append({**common_fields,
                                   "Source": "google_maps",
                                   "Rank": i + 1,
                                   "Name": place.get("title"),
                                   "Category": place.get("type"),
                                   "Rating": place.get("rating"),
                                   "Reviews": place.get("reviews"),
                                   "Address": place.get("address"),
                                   "Phone": place.get("phone"),
                                   "Website": place.get("website"),
                                   "Place_ID": place.get("place_id")
                                   })

    return metrics, organic_list, paa_list, expansion_list, competitor_list, all_local_pack, ai_citations, serp_modules, rich_features, parsing_warnings


def get_ngrams(text, n):
    if not isinstance(text, str):
        return []
    # Clean: lowercase, replace non-alphanumeric with space (prevents "highly-trained" -> "highlytrained")
    text = re.sub(r'[^\w\s]', ' ', text.lower())
    words = [w for w in text.split() if w not in STOP_WORDS and len(w) > 2]
    return [" ".join(words[i:i+n]) for i in range(len(words)-n+1)]


def count_syllables(word):
    word = word.lower()
    count = 0
    vowels = "aeiouy"
    if len(word) == 0:
        return 0
    if word[0] in vowels:
        count += 1
    for index in range(1, len(word)):
        if word[index] in vowels and word[index - 1] not in vowels:
            count += 1
    if word.endswith("e"):
        count -= 1
    if count == 0:
        count += 1
    return count


def calculate_reading_level(text):
    if not text or not isinstance(text, str) or text == "N/A":
        return "N/A"
    # Basic cleaning and tokenization
    clean_text = re.sub(r'[^\w\s.?!]', '', text)
    sentences = [s for s in re.split(r'[.?!]+', clean_text) if s.strip()]
    words = clean_text.split()
    if not sentences or not words:
        return "N/A"
    num_syllables = sum(count_syllables(w) for w in words)
    # Flesch-Kincaid Grade Level Formula
    score = 0.39 * (len(words) / len(sentences)) + 11.8 * \
        (num_syllables / len(words)) - 15.59
    return round(score, 1)


def calculate_sentiment(text):
    if not TEXTBLOB_AVAILABLE or not text or not isinstance(text, str) or text == "N/A":
        return "N/A"
    try:
        # Returns a float between -1.0 (Negative) and 1.0 (Positive)
        return round(TextBlob(text).sentiment.polarity, 2)
    except Exception:
        return "N/A"


def calculate_subjectivity(text):
    if not TEXTBLOB_AVAILABLE or not text or not isinstance(text, str) or text == "N/A":
        return "N/A"
    try:
        # Returns a float between 0.0 (Objective) and 1.0 (Subjective)
        return round(TextBlob(text).sentiment.subjectivity, 2)
    except Exception:
        return "N/A"


def analyze_strategic_opportunities(ngram_results):
    """
    Maps detected N-Gram patterns to Bowen Theory strategic recommendations.
    Returns a list of dictionaries for the 'Strategic_Recommendations' sheet.
    """
    recommendations = []

    # Define the Knowledge Base (The "Bridge")
    strategies = [
        {
            "Pattern_Name": "The Medical Model Trap",
            "Triggers": ["clinical", "registered", "diagnosis", "disorder", "mental health", "patient", "treatment"],
            "Status_Quo_Message": "You are sick/broken and need an expert to fix you (External Locus of Control).",
            "Bowen_Bridge_Reframe": "Shift from pathology to functioning. You don't need a diagnosis; you need a map of your emotional system.",
            "Content_Angle": "Why treating your marriage like a medical condition keeps you stuck."
        },
        {
            "Pattern_Name": "The Fusion Trap",
            "Triggers": ["connection", "bond", "close", "intimacy", "communication", "save marriage", "stop fighting"],
            "Status_Quo_Message": "The goal is to merge, agree, and be 'close' at all costs.",
            "Bowen_Bridge_Reframe": "Real intimacy requires differentiation. Seeking closeness to reduce anxiety often increases reactivity.",
            "Content_Angle": "Why trying to get 'closer' might be pushing your partner away."
        },
        {
            "Pattern_Name": "The Resource Trap",
            "Triggers": ["free", "low cost", "sliding scale", "cheap", "affordable", "covered", "insurance"],
            "Status_Quo_Message": "High anxiety about resources/access. Seeking immediate symptom relief (venting).",
            "Bowen_Bridge_Reframe": "Address the anxiety driving the search. Cheap relief often delays real structural change.",
            "Content_Angle": "When 'free venting' costs you more than you think."
        },
        {
            "Pattern_Name": "The Blame/Reactivity Trap",
            "Triggers": ["narcissist", "toxic", "abusive", "mean", "angry", "hate", "deal with"],
            "Status_Quo_Message": "The problem is the other person (The Identified Patient).",
            "Bowen_Bridge_Reframe": "Focus on self-regulation. You cannot change them, only your response to them.",
            "Content_Angle": "Stop diagnosing your partner and start observing your reaction."
        }
    ]

    # Flatten ngrams for searching
    all_phrases = " ".join([item["Phrase"] for item in ngram_results]).lower()

    for strategy in strategies:
        found_triggers = [t for t in strategy["Triggers"] if t in all_phrases]
        if found_triggers:
            # Create a copy to avoid mutating the template if we were reusing it
            rec = strategy.copy()
            rec["Detected_Triggers"] = ", ".join(
                found_triggers[:5])  # Limit to top 5 matches
            recommendations.append(rec)

    if not recommendations:
        # Default fallback if no specific triggers found
        recommendations.append({
            "Pattern_Name": "General Differentiation",
            "Detected_Triggers": "N/A",
            "Status_Quo_Message": "Standard symptom-focused advice.",
            "Bowen_Bridge_Reframe": "Focus on defining a self within the system.",
            "Content_Angle": "How to be yourself in your important relationships."
        })

    return recommendations


def _autocomplete_query_variants(keyword):
    """Build fallback autocomplete queries for long/local phrases."""
    q = (keyword or "").strip()
    variants = [q]

    city = LOCATION.split(",")[0].strip().lower()
    lowered = q.lower()
    if city:
        for suffix in (f" in {city}", f" {city}"):
            if lowered.endswith(suffix):
                trimmed = q[:len(q) - len(suffix)].strip()
                if trimmed:
                    variants.append(trimmed)

    for prefix in ("help with ", "help for ", "need help with "):
        if lowered.startswith(prefix):
            core = q[len(prefix):].strip()
            if core:
                variants.append(core)
                variants.append(f"{core} help")
            break

    deduped = []
    seen = set()
    for item in variants:
        key = item.lower()
        if item and key not in seen:
            seen.add(key)
            deduped.append(item)
    return deduped


def _ai_query_alternatives(base_keyword):
    """Generate two AI-likely informational alternatives for a base query."""
    q = (base_keyword or "").strip()
    if not q:
        return []

    city = LOCATION.split(",")[0].strip()
    city_lower = city.lower()
    base = q
    base_lower = base.lower()

    # Remove obvious local suffixes (often suppress AI overviews).
    directional_city_pattern = rf"\s+in\s+(north|south|east|west)\s+{re.escape(city_lower)}$"
    if re.search(directional_city_pattern, base_lower):
        base = re.sub(directional_city_pattern, "", base, flags=re.I).strip()
        base_lower = base.lower()
    directional_city_pattern_2 = rf"\s+(north|south|east|west)\s+{re.escape(city_lower)}$"
    if re.search(directional_city_pattern_2, base_lower):
        base = re.sub(directional_city_pattern_2, "", base, flags=re.I).strip()
        base_lower = base.lower()

    for suffix in (f" in {city_lower}", f" near {city_lower}", f" {city_lower}"):
        if base_lower.endswith(suffix):
            base = base[:len(base) - len(suffix)].strip()
            base_lower = base.lower()
            break

    base = re.sub(r"^(best|top)\s+", "", base, flags=re.I).strip()
    base_lower = base.lower()
    if not base:
        return []

    service_like_tokens = (
        "counselling", "counseling", "counsellor", "counselor",
        "therapist", "therapy", "psychologist", "mental health"
    )
    if any(tok in base_lower for tok in service_like_tokens):
        alt1 = f"how to choose the right {base}?"
        alt2 = f"how much does {base} cost in {city}?"
    elif base_lower.startswith("help with "):
        topic = base[10:].strip()
        alt1 = f"what are effective ways to manage {topic}?"
        alt2 = f"where to get help for {topic} in {city}?"
    else:
        alt1 = f"what are the best options for {base}?"
        alt2 = f"how much does {base} cost in {city}?"

    out = []
    seen = set()
    for candidate in (alt1, alt2):
        normalized = candidate.strip()
        key = normalized.lower()
        if normalized and key != q.lower() and key not in seen:
            out.append(normalized)
            seen.add(key)
    return out


def expand_keywords_for_ai(keywords):
    """
    Build query execution list.
    Returns tuples: (query_text, source_keyword, query_label).
    """
    expanded = []
    for keyword in keywords:
        base = (keyword or "").strip()
        if not base:
            continue
        expanded.append((base, base, "A"))
        if not AI_QUERY_ALTERNATIVES_ENABLED:
            continue
        ai_alts = _ai_query_alternatives(base)[:2]
        for idx, alt in enumerate(ai_alts, start=1):
            expanded.append((alt, base, f"A.{idx}"))
    return expanded


def fetch_autocomplete(keyword):
    """Fetches Google Autocomplete suggestions with fallback variants."""
    variants = _autocomplete_query_variants(keyword)
    merged_suggestions = []
    seen = set()
    last_response = None

    for variant in variants:
        params = {
            "engine": "google_autocomplete",
            "q": variant,
            "gl": GOOGLE_GL,
            "hl": GOOGLE_HL,
            "api_key": API_KEY
        }
        logging.info(f"  - Fetching Autocomplete for '{variant}'...")
        response = _fetch_serp_api(params)
        if not response:
            continue
        last_response = response

        for s in response.get("suggestions", []) or []:
            val = s.get("value") if isinstance(s, dict) else s
            if not val:
                continue
            dedupe_key = val.strip().lower()
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)
            merged_suggestions.append(s)

        # Keep API usage bounded once we have suggestions.
        if merged_suggestions:
            break

    if not last_response:
        return None

    out = dict(last_response)
    out["suggestions"] = merged_suggestions
    out["query_variants_tried"] = variants
    return out


def build_help_rows():
    """Guidance rows for why specific sheets may be empty."""
    return [
        {
            "Tab": "AI_Overview_Citations",
            "Trigger": "Google returns AI Overview with references/citations.",
            "Likely_Query_Type": "Informational queries like 'how to choose a counsellor' or 'how much does counselling cost'.",
            "Why_Empty": "Local/commercial intent (e.g., 'best', 'near me', 'in [city]') often shows maps/ads/organic instead of AI Overview."
        },
        {
            "Tab": "Rich_Features",
            "Trigger": "SERP contains modules like videos, top stories, image pack, shopping, or knowledge graph.",
            "Likely_Query_Type": "News/video/image/product/entity intent queries.",
            "Why_Empty": "Local service intent often returns map pack + PAA without these modules."
        },
        {
            "Tab": "Autocomplete_Suggestions",
            "Trigger": "Autocomplete endpoint returns suggestion strings.",
            "Likely_Query_Type": "Shorter seed phrases like 'stress counselling' or 'help with stress'.",
            "Why_Empty": "Long-tail full phrases can return no autocomplete suggestions."
        },
        {
            "Tab": "AI_Query_Generator (GUI Option)",
            "Trigger": "Enable 'Run 2 AI-likely alternatives (A.1, A.2)' in the launcher.",
            "Likely_Query_Type": "Local seed query A plus two informational alternatives A.1/A.2.",
            "Why_Empty": "If disabled, only base query A runs; fewer chances to trigger AI Overview."
        }
    ]


def main():
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    setup_logging(run_id)

    if not API_KEY:
        logging.error(
            "SERPAPI_KEY environment variable not set. Please run: export SERPAPI_KEY='your_key'")
        return

    if not os.path.exists(INPUT_FILE):
        logging.error(f"{INPUT_FILE} not found.")
        return

    print(f"--- STARTING RUN: {run_id} ---")

    print("Reading keywords...")
    try:
        df_input = pd.read_csv(INPUT_FILE, header=None)
        keywords = df_input[0].tolist()
    except Exception as e:
        logging.error(f"Error reading CSV: {e}")
        return

    query_jobs = expand_keywords_for_ai(keywords)

    # Initialize Enrichment Modules
    if ENRICHMENT_ENABLED:
        enricher = UrlEnricher(user_agent=CONFIG.get("enrichment", {}).get("user_agent", "MarketIntelligenceBot/1.0"),
                               timeout=CONFIG.get("enrichment", {}).get("timeout_seconds", 10))
        content_classifier = ContentClassifier()
        entity_classifier = EntityClassifier(override_file=CONFIG.get(
            "files", {}).get("domain_overrides", "domain_overrides.yml"))
        storage = SerpStorage()
        # Params hash updated per keyword later, but run init here
        storage.save_run(run_id, "N/A")

    # Data Containers
    all_metrics = []
    all_organic = []
    all_paa = []
    all_expansion = []
    all_competitors = []
    all_local_pack = []
    all_ai_citations = []
    all_serp_modules = []
    all_rich_features = []
    all_parsing_warnings = []
    all_aio_logs = []
    all_autocomplete = []

    print(f"--- Base keywords: {len(keywords)} ---")
    print(f"--- AI query alternatives enabled: {AI_QUERY_ALTERNATIVES_ENABLED} ---")
    print(f"--- Queries to run: {len(query_jobs)} ---")
    print(f"--- FORCE LOCAL INTENT: {FORCE_LOCAL_INTENT} ---")
    print(f"--- BRIDGE STRATEGY: Mapping Symptoms -> Systems ---")

    for i, (keyword, source_keyword, query_label) in enumerate(query_jobs):
        print(f"\n{'='*60}")
        print(
            f"[{i+1}/{len(query_jobs)}] Analyzing ({query_label}) source='{source_keyword}' query='{keyword}'"
        )
        print(f"{'='*60}\n")

        raw_data_dict, aio_log, query_metadata = fetch_serp_data(
            keyword, run_id)
        aio_log["Source_Keyword"] = source_keyword
        aio_log["Query_Label"] = query_label
        aio_log["Executed_Query"] = keyword
        all_aio_logs.append(aio_log)

        if raw_data_dict:
            m, o, p, e, c, lp, ac, sm, rf, pw = parse_data(
                keyword, raw_data_dict, query_metadata)
            if m:  # Only append if parsing was successful
                m["Source_Keyword"] = source_keyword
                m["Query_Label"] = query_label
                m["Executed_Query"] = keyword
                all_metrics.append(m)
                for row in o:
                    row["Source_Keyword"] = source_keyword
                    row["Query_Label"] = query_label
                    row["Executed_Query"] = keyword
                all_organic.extend(o)
                for row in p:
                    row["Source_Keyword"] = source_keyword
                    row["Query_Label"] = query_label
                    row["Executed_Query"] = keyword
                all_paa.extend(p)
                for row in e:
                    row["Source_Keyword"] = source_keyword
                    row["Query_Label"] = query_label
                    row["Executed_Query"] = keyword
                all_expansion.extend(e)
                for row in c:
                    row["Source_Keyword"] = source_keyword
                    row["Query_Label"] = query_label
                    row["Executed_Query"] = keyword
                all_competitors.extend(c)
                for row in lp:
                    row["Source_Keyword"] = source_keyword
                    row["Query_Label"] = query_label
                    row["Executed_Query"] = keyword
                all_local_pack.extend(lp)
                for row in ac:
                    row["Source_Keyword"] = source_keyword
                    row["Query_Label"] = query_label
                    row["Executed_Query"] = keyword
                all_ai_citations.extend(ac)
                for row in sm:
                    row["Source_Keyword"] = source_keyword
                    row["Query_Label"] = query_label
                    row["Executed_Query"] = keyword
                all_serp_modules.extend(sm)
                for row in rf:
                    row["Source_Keyword"] = source_keyword
                    row["Query_Label"] = query_label
                    row["Executed_Query"] = keyword
                all_rich_features.extend(rf)
                for row in pw:
                    row["Source_Keyword"] = source_keyword
                    row["Query_Label"] = query_label
                    row["Executed_Query"] = keyword
                all_parsing_warnings.extend(pw)

                # --- ENRICHMENT LOOP ---
                if ENRICHMENT_ENABLED:
                    print(f"  - Enriching {len(o)} organic results...")
                    # Update run params hash in storage now that we have it
                    storage.save_run(run_id, query_metadata["params_hash"])

                    for item in o:
                        url = item.get("Link")
                        if not url or url == "N/A":
                            continue

                        # 1. Save basic SERP result to DB
                        domain = urlparse(url).netloc
                        storage.save_serp_result(
                            run_id, source_keyword, "organic", item.get("Rank"),
                            item.get("Title"), url, domain, item.get("Snippet")
                        )

                        # 2. Fetch & Enrich URL (Limit to top 5 to save time/bandwidth for now)
                        try:
                            rank_val = int(item.get("Rank", 99))
                        except (ValueError, TypeError):
                            rank_val = 99

                        if rank_val <= MAX_URLS_TO_ENRICH:
                            fetch_res = enricher.fetch_url(url)
                            if fetch_res:
                                features = enricher.extract_features(fetch_res)
                                soup = features.get('soup')

                                # Classify
                                c_type, c_conf, c_ev = content_classifier.classify(
                                    url, soup, fetch_res.get('headers'))
                                e_type, e_conf, e_ev = entity_classifier.classify(
                                    domain, soup)

                                # Save Features
                                storage.save_url_features(
                                    url, fetch_res['status_code'], c_type,
                                    features.get('schema_types', []),
                                    features.get('word_count_est', 0),
                                    c_ev
                                )
                                storage.save_domain_features(domain, e_type)

                                # Optional: Add to item dict for Excel output?
                                item['Content_Type'] = c_type
                                item['Entity_Type'] = e_type
                                item['Word_Count'] = features.get(
                                    'word_count_est', "N/A")

        # --- AUTOCOMPLETE ---
        ac_data = fetch_autocomplete(keyword)
        if ac_data and "suggestions" in ac_data:
            for idx, s in enumerate(ac_data["suggestions"]):
                # Handle both dict and string formats
                val = s.get("value") if isinstance(s, dict) else s
                rel = s.get("relevance") if isinstance(s, dict) else None
                typ = s.get("type") if isinstance(s, dict) else None

                all_autocomplete.append({
                    "Run_ID": run_id,
                    "Source_Keyword": source_keyword,
                    "Query_Label": query_label,
                    "Executed_Query": keyword,
                    "Rank": idx + 1,
                    "Suggestion": val,
                    "Relevance": rel,
                    "Type": typ
                })

                if ENRICHMENT_ENABLED:
                    storage.save_autocomplete_suggestion(
                        run_id, source_keyword, val, idx + 1, rel, typ)

        time.sleep(1.2)  # Polite delay

    # --- N-GRAM ANALYSIS (SERP Language Patterns) ---
    print("Running N-Gram Analysis (SERP Language Patterns)...")

    all_snippets = []

    # 1. Organic & Featured Snippets
    for m in all_metrics:
        keys = ["Featured_Snippet_Snippet", "AI_Overview", "Rank_1_Snippet",
                "Rank_2_Snippet", "Rank_3_Snippet"]
        for k in keys:
            val = m.get(k)
            if val and val != "N/A":
                all_snippets.append(val)

    # 2. Paid Ads (Skip Map Pack ratings as they are just numbers)
    for c in all_competitors:
        if c.get("Type") == "Paid Ad" and c.get("Snippet"):
            all_snippets.append(c["Snippet"])

    # 3. PASF & Related Searches (The Anxiety Loop)
    for e in all_expansion:
        if e.get("Term"):
            all_snippets.append(e["Term"])

    # 4. Autocomplete Suggestions
    for a in all_autocomplete:
        if a.get("Suggestion"):
            all_snippets.append(a["Suggestion"])

    bigrams = []
    trigrams = []

    for s in all_snippets:
        bigrams.extend(get_ngrams(s, 2))
        trigrams.extend(get_ngrams(s, 3))

    ngram_results = []
    for term, count in Counter(bigrams).most_common():
        ngram_results.append(
            {"Type": "Bigram", "Phrase": term, "Count": count})
    for term, count in Counter(trigrams).most_common():
        ngram_results.append(
            {"Type": "Trigram", "Phrase": term, "Count": count})

    # --- STRATEGIC ANALYSIS (The Bridge) ---
    print("Generating Strategic Recommendations...")
    strategic_recs = analyze_strategic_opportunities(ngram_results)
    print(f"Generated {len(strategic_recs)} strategic recommendations.")

    # --- MERGE RANK DELTAS (Volatility) ---
    if ENRICHMENT_ENABLED:
        print("Calculating Rank Deltas...")
        deltas = metrics.get_rank_deltas(run_id)
        if deltas:
            count = 0
            for item in all_organic:
                url = item.get("Link")
                if url in deltas:
                    item["Rank_Delta"] = int(deltas[url])
                    count += 1
            print(f"Updated {count} rows with rank changes.")
        else:
            print("No historical data for rank comparison yet.")

    # --- VISUALIZATION (Word Cloud) ---
    if VISUALIZATION_AVAILABLE:
        print("Generating Word Cloud...")
        frequencies = {item["Phrase"]: item["Count"] for item in ngram_results}

        if frequencies:
            wc = WordCloud(width=800, height=400,
                           background_color='white').generate_from_frequencies(frequencies)
            plt.figure(figsize=(10, 5))
            plt.imshow(wc, interpolation='bilinear')
            plt.axis("off")
            plt.title("SERP Language Patterns (Competitor Language)")
            plt.savefig("serp_language_wordcloud.png")
            plt.close()
    else:
        print("Skipping Word Cloud generation (libraries not installed).")

    # --- PREPARE DATA FOR JSON & EXCEL ---
    # Split Strategy_Expansion into Related_Searches and Derived_Expansions
    related_searches_data = [
        x for x in all_expansion if x.get("Type") == "Related Search"]
    derived_expansions_data = [
        x for x in all_expansion if x.get("Type") != "Related Search"]
    help_rows = build_help_rows()

    full_data = {
        "overview": all_metrics,
        "organic_results": all_organic,
        "paa_questions": all_paa,
        "related_searches": related_searches_data,
        "derived_expansions": derived_expansions_data,
        "competitors_ads": all_competitors,
        "serp_language_patterns": ngram_results,
        "strategic_recommendations": strategic_recs,
        "local_pack_and_maps": all_local_pack,
        "ai_overview_citations": all_ai_citations,
        "serp_modules": all_serp_modules,
        "rich_features": all_rich_features,
        "parsing_warnings": all_parsing_warnings,
        "aio_logs": all_aio_logs,
        "autocomplete_suggestions": all_autocomplete,
        "help_guide": help_rows
    }

    print(f"Saving JSON to {OUTPUT_JSON}...")
    try:
        with open(OUTPUT_JSON, 'w', encoding='utf-8') as f:
            json.dump(full_data, f, indent=2, ensure_ascii=False)

        # Save normalized audit trail (per requirements)
        norm_dir = "normalized"
        os.makedirs(norm_dir, exist_ok=True)
        norm_path = f"{norm_dir}/{run_id}.serp_norm.json"
        with open(norm_path, 'w', encoding='utf-8') as f:
            json.dump(full_data, f, indent=2, ensure_ascii=False)
        print(f"Saved normalized audit to {norm_path}")
    except Exception as e:
        logging.error(f"Error saving JSON file: {e}")

    print("Saving to Excel...")
    try:
        with pd.ExcelWriter(OUTPUT_FILE, engine='openpyxl') as writer:
            pd.DataFrame(all_metrics).to_excel(
                writer, sheet_name="Overview", index=False)
            pd.DataFrame(all_organic).to_excel(
                writer, sheet_name="Organic_Results", index=False)
            pd.DataFrame(all_paa).to_excel(
                writer, sheet_name="PAA_Questions", index=False)
            pd.DataFrame(related_searches_data).to_excel(
                writer, sheet_name="Related_Searches", index=False)
            pd.DataFrame(derived_expansions_data).to_excel(
                writer, sheet_name="Derived_Expansions", index=False)
            pd.DataFrame(all_competitors).to_excel(
                writer, sheet_name="Competitors_Ads", index=False)
            pd.DataFrame(ngram_results).to_excel(
                writer, sheet_name="SERP_Language_Patterns", index=False)
            pd.DataFrame(strategic_recs).to_excel(
                writer, sheet_name="Strategic_Recommendations", index=False)
            pd.DataFrame(all_local_pack).to_excel(
                writer, sheet_name="Local_Pack_and_Maps", index=False)
            pd.DataFrame(all_ai_citations).to_excel(
                writer, sheet_name="AI_Overview_Citations", index=False)
            pd.DataFrame(all_serp_modules).to_excel(
                writer, sheet_name="SERP_Modules", index=False)
            pd.DataFrame(all_rich_features).to_excel(
                writer, sheet_name="Rich_Features", index=False)
            pd.DataFrame(all_parsing_warnings).to_excel(
                writer, sheet_name="Parsing_Warnings", index=False)
            pd.DataFrame(all_aio_logs).to_excel(
                writer, sheet_name="AIO_Logs", index=False)
            pd.DataFrame(all_autocomplete).to_excel(
                writer, sheet_name="Autocomplete_Suggestions", index=False)
            pd.DataFrame(help_rows).to_excel(
                writer, sheet_name="Help", index=False)

        print(f"SUCCESS! Data saved to {OUTPUT_FILE}")
    except Exception as e:
        logging.error(f"Error saving Excel file (is it open?): {e}")

    print("Generating Markdown Report...")
    try:
        md_content = []

        # 1. Insight Report
        md_content.append(generate_insight_report.generate_report(full_data))
        md_content.append("\n---\n")

        # 2. Content Briefs
        recs = full_data.get("strategic_recommendations", [])
        if recs:
            md_content.append("# Content Briefs\n")
            for i in range(len(recs)):
                md_content.append(
                    f"## Brief {i+1}: {recs[i].get('Pattern_Name')}")
                md_content.append(
                    generate_content_brief.generate_brief(full_data, rec_index=i))
                md_content.append("\n---\n")

        with open(OUTPUT_MD, "w", encoding="utf-8") as f:
            f.write("\n".join(md_content))

        print(f"SUCCESS! Report saved to {OUTPUT_MD}")
    except Exception as e:
        logging.error(f"Error saving Markdown report: {e}")


if __name__ == "__main__":
    main()
