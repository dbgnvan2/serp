import pandas as pd
from serpapi import GoogleSearch
import time
import os
import re
from dotenv import load_dotenv
import logging
import json
from datetime import datetime
from collections import Counter
from openpyxl import Workbook
import hashlib

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
INPUT_FILE = "keywords.csv"
OUTPUT_FILE = "market_analysis_v2.xlsx"
LOCATION = "Vancouver, British Columbia, Canada"
FORCE_LOCAL_INTENT = True
STOP_WORDS = {"the", "and", "to", "of", "a", "in", "is", "for", "on", "with", "as", "at", "by", "an", "be", "or", "are", "from", "that",
              "this", "it", "we", "our", "us", "can", "will", "your", "you", "my", "me", "not", "have", "has", "but", "so", "if", "their", "they"}


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
    try:
        search = GoogleSearch(params)
        results = search.get_dict()
        logging.info(f"API Return Message: {json.dumps(results, indent=2)}")
        if "error" in results:
            logging.error(f"API Error: {results['error']}")
            return None
        return results
    except Exception as e:
        logging.critical(
            f"CRITICAL ERROR fetching with params {params.get('q')}: {e}")
        return None


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
        "engine": "google",
        "q": query_term,
        "location": LOCATION,
        "hl": "en",
        "gl": "ca",
        "api_key": API_KEY,
        "num": 100,
        "device": "desktop",
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
        logging.info("AIO absent in main SERP; skipping follow-up.")
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
            "hl": "en",
            "gl": "ca",
            "api_key": API_KEY,
            "no_cache": True
        }

        # Attempt to extract 'll' (latitude, longitude) from metadata to pin location
        # This ensures the maps view matches the SERP location context
        meta = primary_results.get("serpapi_search_metadata", {})
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
            all_results['google_maps'] = maps_results
            save_raw_json(run_id, 'google_maps', maps_results)

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
    module_keys = ["top_ads", "ai_overview", "local_pack", "related_questions", "organic_results", "bottom_ads",
                   "related_searches", "knowledge_graph", "inline_videos", "top_stories", "image_pack", "shopping_results"]
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
    ai_overview_data = results.get(
        'google_ai_overview') or primary_results.get('ai_overview', {})

    # B. Don't overload "Has_AI_Overview"
    metrics["Has_Main_AI_Overview"] = bool(ai_overview_data)

    if not ai_overview_data.get("snippet"):
        parsing_warnings.append({**common_fields, "Module": "ai_overview",
                                "Field": "snippet", "Message": "AI Overview snippet not found"})

    metrics["AI_Overview"] = ai_overview_data.get("snippet", "N/A")
    metrics["AI_Reading_Level"] = calculate_reading_level(
        metrics["AI_Overview"])
    metrics["AI_Sentiment"] = calculate_sentiment(metrics["AI_Overview"])
    metrics["AI_Subjectivity"] = calculate_subjectivity(metrics["AI_Overview"])

    ai_citations = []
    if "citations" in ai_overview_data:
        for citation in ai_overview_data["citations"]:
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
                             "Source": item.get("source", "N/A")
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
    # Clean: lowercase, remove non-alphanumeric (keep spaces)
    text = re.sub(r'[^\w\s]', '', text.lower())
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

    print(f"--- Analyzing {len(keywords)} keywords ---")
    print(f"--- FORCE LOCAL INTENT: {FORCE_LOCAL_INTENT} ---")

    for i, keyword in enumerate(keywords):
        print(f"\n{'='*60}")
        print(f"[{i+1}/{len(keywords)}] Analyzing: {keyword}")
        print(f"{'='*60}\n")

        raw_data_dict, aio_log, query_metadata = fetch_serp_data(
            keyword, run_id)
        all_aio_logs.append(aio_log)

        if raw_data_dict:
            m, o, p, e, c, lp, ac, sm, rf, pw = parse_data(
                keyword, raw_data_dict, query_metadata)
            if m:  # Only append if parsing was successful
                all_metrics.append(m)
                all_organic.extend(o)
                all_paa.extend(p)
                all_expansion.extend(e)
                all_competitors.extend(c)
                all_local_pack.extend(lp)
                all_ai_citations.extend(ac)
                all_serp_modules.extend(sm)
                all_rich_features.extend(rf)
                all_parsing_warnings.extend(pw)

        time.sleep(1.2)  # Polite delay

    # --- N-GRAM ANALYSIS (Bad Advice Detector) ---
    print("Running N-Gram Analysis (Bad Advice Detector)...")

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
            plt.title("Bad Advice Patterns (Competitor Language)")
            plt.savefig("bad_advice_wordcloud.png")
            plt.close()
    else:
        print("Skipping Word Cloud generation (libraries not installed).")

    print("Saving to Excel...")
    try:
        with pd.ExcelWriter(OUTPUT_FILE, engine='openpyxl') as writer:
            pd.DataFrame(all_metrics).to_excel(
                writer, sheet_name="Overview", index=False)
            pd.DataFrame(all_organic).to_excel(
                writer, sheet_name="Organic_Results", index=False)
            pd.DataFrame(all_paa).to_excel(
                writer, sheet_name="PAA_Questions", index=False)

            # Split Strategy_Expansion into Related_Searches and Derived_Expansions
            related_searches_data = [
                x for x in all_expansion if x.get("Type") == "Related Search"]
            derived_expansions_data = [
                x for x in all_expansion if x.get("Type") != "Related Search"]
            pd.DataFrame(related_searches_data).to_excel(
                writer, sheet_name="Related_Searches", index=False)
            pd.DataFrame(derived_expansions_data).to_excel(
                writer, sheet_name="Derived_Expansions", index=False)

            pd.DataFrame(all_competitors).to_excel(
                writer, sheet_name="Competitors_Ads", index=False)
            pd.DataFrame(ngram_results).to_excel(
                writer, sheet_name="Bad_Advice_Patterns", index=False)
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

        print(f"SUCCESS! Data saved to {OUTPUT_FILE}")
    except Exception as e:
        logging.error(f"Error saving Excel file (is it open?): {e}")


if __name__ == "__main__":
    main()
