#!/usr/bin/env python3
"""
generate_content_brief.py

Modes:
1) Improved content opportunity report (default for launcher list mode):
   python generate_content_brief.py --json market_analysis_v2.json --list

2) Legacy single brief mode:
   python generate_content_brief.py --json market_analysis_v2.json --out brief.md --index 0
"""
import argparse
import json
import os
import re
import sys
from collections import Counter, defaultdict
from datetime import datetime
from urllib.parse import urlparse
import yaml

try:
    import anthropic
    ANTHROPIC_AVAILABLE = True
except ImportError:
    anthropic = None
    ANTHROPIC_AVAILABLE = False


DEFAULT_CLIENT_CONTEXT = {
    "client_name": "Living Systems Counselling",
    "client_domain": "livingsystems.ca",
    "client_name_patterns": ["Living Systems"],
    "org_type": "Small nonprofit counselling organization",
    "location": "North Vancouver, BC, Canada",
    "framework_description": (
        "Bowen Family Systems Theory. Differentiation of self, emotional cutoff, "
        "triangles, and multigenerational family patterns."
    ),
    "content_focus": (
        "Counselling services and educational content grounded in Bowen Family Systems Theory."
    ),
    "additional_context": (
        "Prioritize practical recommendations a small nonprofit can execute. "
        "Avoid audiences outside counselling scope."
    ),
    "framework_terms": [
        "family systems", "bowen", "differentiation",
        "emotional cutoff", "triangles", "multigenerational",
        "nuclear family emotional", "societal emotional",
    ],
}

MAIN_REPORT_DEFAULT_MODEL = "claude-opus-4-6"
ADVISORY_DEFAULT_MODEL = "claude-sonnet-4-20250514"

SUPPORTED_REPORT_MODELS = [
    "claude-opus-4-6",
    "claude-sonnet-4-20250514",
    "claude-opus-4-1-20250805",
    "claude-opus-4-20250514",
    "claude-3-7-sonnet-20250219",
]

MAIN_REPORT_PROMPT_DEFAULT = os.path.join("prompts", "main_report")
ADVISORY_PROMPT_DEFAULT = os.path.join("prompts", "advisory")
CORRECTION_PROMPT_DEFAULT = os.path.join("prompts", "correction", "user_template.md")


BRIEF_PAA_THEMES = {
    "The Medical Model Trap": [
        "therapy", "therapist", "counselling", "counselor",
        "session", "diagnosis", "mental health", "treatment",
        "professional", "psychologist",
    ],
    "The Fusion Trap": [
        "reach out", "reconnect", "contact", "close",
        "relationship", "communicate", "talking",
        "stop reaching", "go no contact",
    ],
    "The Resource Trap": [
        "cost", "free", "afford", "pay", "price", "insurance",
        "covered", "sliding scale", "low cost", "how much",
    ],
    "The Blame/Reactivity Trap": [
        "toxic", "narcissist", "abusive", "signs", "fault",
        "blame", "anger", "deal with", "mean",
    ],
}

BRIEF_PAA_CATEGORIES = {
    "The Medical Model Trap": {"General", "Commercial"},
    "The Fusion Trap": {"General", "Distress"},
    "The Resource Trap": {"Commercial", "Distress"},
    "The Blame/Reactivity Trap": {"Reactivity", "Distress"},
}

BRIEF_KEYWORD_HINTS = {
    "The Medical Model Trap": ["therapy", "counselling", "counseling", "mental health"],
    "The Fusion Trap": ["estrangement", "adult child", "reach out", "contact"],
    "The Resource Trap": ["grief", "counselling", "therapy", "bc"],
    "The Blame/Reactivity Trap": ["estrangement", "toxic", "no-contact", "family member"],
}

def progress(message):
    print(message, flush=True)


def load_yaml_config(config_path):
    if not os.path.exists(config_path):
        return {}
    try:
        progress(f"[1/7] Loading config from {config_path}...")
        with open(config_path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    except Exception as e:
        print(f"Error loading config YAML ({config_path}): {e}")
        sys.exit(1)


def load_client_context_from_config(config):
    section = config.get("analysis_report", {}) if isinstance(config, dict) else {}

    context = {
        "client_name": section.get("client_name", DEFAULT_CLIENT_CONTEXT["client_name"]),
        "client_domain": section.get("client_domain", DEFAULT_CLIENT_CONTEXT["client_domain"]),
        "client_name_patterns": section.get(
            "client_name_patterns",
            DEFAULT_CLIENT_CONTEXT["client_name_patterns"]
        ),
        "org_type": section.get("org_type", DEFAULT_CLIENT_CONTEXT["org_type"]),
        "location": section.get(
            "location",
            config.get("serpapi", {}).get("location", DEFAULT_CLIENT_CONTEXT["location"])
        ),
        "framework_description": section.get(
            "framework_description", DEFAULT_CLIENT_CONTEXT["framework_description"]
        ),
        "content_focus": section.get("content_focus", DEFAULT_CLIENT_CONTEXT["content_focus"]),
        "additional_context": section.get(
            "additional_context", DEFAULT_CLIENT_CONTEXT["additional_context"]
        ),
        "framework_terms": section.get(
            "framework_terms", DEFAULT_CLIENT_CONTEXT["framework_terms"]
        ),
    }
    if isinstance(context["client_name_patterns"], str):
        context["client_name_patterns"] = [
            p.strip() for p in context["client_name_patterns"].split(",") if p.strip()
        ]
    return context


def load_data(json_path):
    try:
        progress(f"[2/7] Loading analysis JSON from {json_path}...")
        with open(json_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"Error loading JSON: {e}")
        sys.exit(1)


def _extract_domain(url):
    if not url:
        return ""
    try:
        return urlparse(str(url)).netloc.replace("www.", "").lower()
    except Exception:
        return str(url).lower()


def _safe_int(v, default=0):
    if v is None:
        return default
    if isinstance(v, int):
        return v
    if isinstance(v, float):
        return int(v)
    try:
        return int(str(v).strip())
    except Exception:
        return default


def _top_sources_for_keyword(organic_rows, source_keyword, max_n=5):
    ctr = Counter()
    for row in organic_rows:
        if row.get("Source_Keyword") != source_keyword:
            continue
        if row.get("Query_Label") != "A":
            continue
        src = row.get("Source")
        if src:
            ctr[src] += 1
    return ctr.most_common(max_n)


def _normalize_text(value):
    return re.sub(r"\s+", " ", str(value or "").strip().lower())


def _classify_entity_distribution(distribution):
    counts = Counter({
        entity: _safe_int(count)
        for entity, count in (distribution or {}).items()
        if _safe_int(count) > 0
    })
    if not counts:
        return None, "unclassified"

    ranked = counts.most_common()
    top_entity, top_count = ranked[0]
    second_count = ranked[1][1] if len(ranked) > 1 else 0
    classified_total = sum(counts.values())
    top_pct = top_count / classified_total if classified_total else 0.0

    if top_pct >= 0.60:
        return top_entity, f"dominated_by_{top_entity}"

    tied_or_close = [
        entity for entity, count in ranked
        if (top_count - count) <= 2
    ]
    if len(tied_or_close) >= 2:
        return top_entity, f"mixed_{'_'.join(sorted(tied_or_close))}"

    return top_entity, f"{top_entity}_plurality"


def _entity_label_reason_text(entity_label, dominant_type):
    label = str(entity_label or "")
    if label.startswith("dominated_by_"):
        return f"Dominated by {label.removeprefix('dominated_by_')} entities."
    if label.endswith("_plurality"):
        return f"{label.removesuffix('_plurality').replace('_', ' ')} plurality."
    if label.startswith("mixed_"):
        return f"Mixed or contested across {label.removeprefix('mixed_').replace('_', ', ')}."
    if dominant_type:
        return f"Leading entity type: {dominant_type}."
    return "Mixed entity distribution."


def _client_match_patterns(client_name_patterns):
    patterns = []
    for pattern in client_name_patterns or []:
        normalized = _normalize_text(pattern)
        if len(normalized.split()) >= 2:
            patterns.append(normalized)
    return patterns


def _contains_phrase(text, phrase):
    return phrase and phrase in _normalize_text(text)


def _extract_excerpt(text, phrase, radius=80):
    normalized_text = str(text or "")
    idx = _normalize_text(normalized_text).find(phrase)
    if idx == -1:
        return None
    start = max(0, idx - radius)
    end = min(len(normalized_text), idx + len(phrase) + radius)
    return normalized_text[start:end].strip()


def _parse_trigger_words(trigger_text):
    if isinstance(trigger_text, list):
        cleaned = []
        for item in trigger_text:
            if item is None:
                continue
            text = str(item).strip().lower()
            if text:
                cleaned.append(text)
        return cleaned
    return [part.strip().lower() for part in str(trigger_text or "").split(",") if part.strip()]


def _count_terms_in_texts(terms, texts):
    counts = {}
    for term in terms:
        total = 0
        pattern = re.compile(rf"\b{re.escape(term)}\b", flags=re.IGNORECASE)
        for text in texts:
            total += len(pattern.findall(str(text or "")))
        if total:
            counts[term] = total
    return counts


def _compute_strategic_flags(root_keywords, keyword_profiles, client_position, total_results_by_kw, paa_analysis):
    flags = {}

    client_organic = client_position.get("organic", [])
    summary = client_position.get("summary", {})
    declining = [
        item for item in client_organic
        if item.get("stability") == "declining"
    ]
    visible_kws = summary.get("keywords_with_any_visibility", [])

    if declining:
        worst = min(declining, key=lambda x: x.get("rank_delta") or 0)
        flags["defensive_urgency"] = "high"
        flags["defensive_detail"] = (
            f"Client's content '{worst.get('title', 'unknown')}' "
            f"dropped {abs(worst.get('rank_delta', 0))} positions "
            f"to rank #{worst.get('rank', '?')} for "
            f"'{worst.get('source_keyword', 'unknown')}'. "
            f"This page provides {summary.get('total_aio_citations', 0)} of the "
            f"client's AIO citations. If organic rank continues declining, "
            f"AIO citation loss is probable."
        )
    elif client_organic:
        flags["defensive_urgency"] = "low"
        flags["defensive_detail"] = "All client positions are stable or improving."
    else:
        flags["defensive_urgency"] = "none"
        flags["defensive_detail"] = "Client has no organic positions to defend."

    total_kws = len(root_keywords)
    visible_count = len(visible_kws)
    if visible_count == 0:
        flags["visibility_concentration"] = "absent"
        flags["concentration_detail"] = (
            f"Client has zero visibility across all {total_kws} tracked keywords."
        )
    elif visible_count == 1:
        flags["visibility_concentration"] = "critical"
        flags["concentration_detail"] = (
            f"Client visible for 1 of {total_kws} tracked keywords "
            f"('{visible_kws[0]}'). 100% of organic and AIO visibility depends on a single keyword cluster."
        )
    elif visible_count <= total_kws * 0.3:
        flags["visibility_concentration"] = "high"
        flags["concentration_detail"] = (
            f"Client visible for {visible_count} of {total_kws} tracked keywords."
        )
    else:
        flags["visibility_concentration"] = "distributed"
        flags["concentration_detail"] = (
            f"Client visible for {visible_count} of {total_kws} tracked keywords."
        )

    opportunity_scale = {}
    for kw in root_keywords:
        profile = keyword_profiles.get(kw, {})
        total_results = profile.get("total_results", total_results_by_kw.get(kw, 0))
        client_rank = profile.get("client_rank")
        client_delta = profile.get("client_rank_delta")
        client_visible = profile.get("client_visible", False)
        entity_dominant = profile.get("entity_dominant_type")
        entity_label = profile.get("entity_label")

        if client_visible and client_delta is not None and client_delta < 0:
            action = "defend"
            reason = (
                f"Client ranks #{client_rank}, declined {abs(client_delta)} positions. "
                f"Protect existing visibility before expanding elsewhere."
            )
        elif client_visible:
            trend_text = (
                "stable" if client_delta == 0 else
                "new (no history)" if client_delta is None else
                "improving"
            )
            action = "strengthen"
            reason = (
                f"Client ranks #{client_rank}. Position is {trend_text}. "
                f"Expand content depth to improve rank."
            )
        elif total_results < 200:
            action = "skip"
            reason = (
                f"Only {total_results} total results. Market too small to justify dedicated content investment."
            )
        elif entity_label in {"dominated_by_legal", "legal_plurality"}:
            action = "enter_cautiously"
            reason = (
                "Legal entities lead this SERP. Entry requires differentiated content "
                "that avoids competing on legal topics directly."
            )
        else:
            action = "enter"
            reason = (
                f"{total_results:,} total results. Client has no current visibility. "
                f"{_entity_label_reason_text(entity_label, entity_dominant)}"
            )

        opportunity_scale[kw] = {
            "total_results": total_results,
            "client_rank": client_rank,
            "client_trend": (
                "declining" if client_delta is not None and client_delta < 0
                else "improving" if client_delta is not None and client_delta > 0
                else "stable" if client_delta == 0
                else "new" if client_visible
                else None
            ),
            "action": action,
            "reason": reason,
        }

    flags["opportunity_scale"] = opportunity_scale

    priority_order = {"defend": 0, "strengthen": 1, "enter": 2, "enter_cautiously": 3, "skip": 4}
    priorities = []
    for kw in root_keywords:
        opp = opportunity_scale[kw]
        priorities.append({
            "action": opp["action"],
            "keyword": kw,
            "total_results": opp["total_results"],
            "reason": opp["reason"],
        })

    priorities.sort(key=lambda x: (priority_order.get(x["action"], 99), -x["total_results"]))
    flags["content_priorities"] = priorities

    cross = paa_analysis.get("cross_cluster", [])
    if cross:
        top_cross = cross[0]
        flags["top_cross_cluster_paa"] = {
            "question": top_cross["question"],
            "cluster_count": top_cross["cluster_count"],
            "combined_total_results": top_cross["combined_total_results"],
        }
    else:
        flags["top_cross_cluster_paa"] = None

    return flags


def _classify_paa_intent(paa_rows):
    """Group PAA questions by Intent_Tag written by serp_audit.py.

    Returns a dict with keys ``"External Locus"``, ``"Systemic"``, and
    ``"General"``, each containing a list of question strings.  Questions
    without an ``Intent_Tag`` fall into ``"General"``.
    """
    buckets = {"External Locus": [], "Systemic": [], "General": []}
    seen = set()
    for row in paa_rows:
        question = str(row.get("Question") or "").strip()
        if not question or question in seen:
            continue
        seen.add(question)
        tag = str(row.get("Intent_Tag") or "").strip()
        if tag in buckets:
            buckets[tag].append(question)
        else:
            buckets["General"].append(question)
    return buckets


def _build_feasibility_summary(feasibility_rows):
    """Compact summary of keyword feasibility data for the LLM payload.

    Returns a dict with:
    - ``low_feasibility``: list of {keyword, gap, suggested_pivot} for
      Low Feasibility primary keywords (query_label != "P").
    - ``moderate_feasibility``: list of keyword strings.
    - ``high_feasibility``: list of keyword strings.
    - ``pivot_serp_results``: list of {keyword, suggested_pivot, client_in_local_pack}
      for pivot keywords (query_label == "P") where pivot data was fetched.
    """
    primary = [r for r in feasibility_rows if r.get("Query_Label") != "P" and r.get("query_label") != "P"]
    pivots  = [r for r in feasibility_rows if r.get("Query_Label") == "P" or r.get("query_label") == "P"]

    low, moderate, high = [], [], []
    for row in primary:
        status = row.get("feasibility_status", "")
        kw = row.get("Keyword") or row.get("keyword_text") or ""
        gap = row.get("gap")
        suggested = row.get("suggested_keyword", "")
        if status == "Low Feasibility":
            low.append({"keyword": kw, "gap": gap, "suggested_pivot": suggested})
        elif status == "Moderate Feasibility":
            moderate.append(kw)
        elif status == "High Feasibility":
            high.append(kw)

    pivot_results = []
    for row in pivots:
        source = row.get("Source_Keyword") or row.get("source_keyword") or ""
        kw = row.get("Keyword") or row.get("keyword_text") or ""
        in_pack = row.get("Client_In_Local_Pack") if row.get("Client_In_Local_Pack") is not None else row.get("client_in_local_pack")
        pivot_results.append({
            "source_keyword": source,
            "pivot_keyword": kw,
            "client_in_local_pack": bool(in_pack) if in_pack is not None else None,
        })

    return {
        "low_feasibility": low,
        "moderate_feasibility": moderate,
        "high_feasibility": high,
        "pivot_serp_results": pivot_results,
    }


def extract_analysis_data_from_json(data, client_domain, client_name_patterns=None, framework_terms=None):
    """Build a compact, pre-verified analysis object from market_analysis_v2.json."""
    client_domain_lower = (client_domain or "").lower()
    client_name_patterns = client_name_patterns or []
    framework_terms = framework_terms or DEFAULT_CLIENT_CONTEXT["framework_terms"]
    client_phrase_patterns = _client_match_patterns(client_name_patterns)

    overview = data.get("overview", [])
    organic = data.get("organic_results", [])
    citations = data.get("ai_overview_citations", [])
    paa_rows = data.get("paa_questions", [])
    autocomplete = data.get("autocomplete_suggestions", [])
    related = data.get("related_searches", [])
    modules = data.get("serp_modules", [])
    local_pack = data.get("local_pack_and_maps", [])
    bigrams_trigrams = data.get("serp_language_patterns", [])
    recs = data.get("strategic_recommendations", [])
    ads = data.get("competitors_ads", [])

    if overview:
        first = overview[0]
        metadata = {
            "run_id": first.get("Run_ID"),
            "created_at": str(first.get("Created_At") or "unknown"),
            "google_url_sample": first.get("Google_URL"),
        }
    else:
        metadata = {"run_id": "unknown", "created_at": "unknown", "google_url_sample": None}

    root_keywords = sorted({r.get("Source_Keyword") for r in overview if r.get("Source_Keyword")})

    total_results_by_kw = {}
    queries = []
    overview_by_kw = defaultdict(list)
    for r in overview:
        source_kw = r.get("Source_Keyword")
        if not source_kw:
            continue
        overview_by_kw[source_kw].append(r)
        if r.get("Query_Label") == "A" or source_kw not in total_results_by_kw:
            total_results_by_kw[source_kw] = _safe_int(r.get("Total_Results"), 0)
        aio_text = str(r.get("AI_Overview") or "")
        client_mentioned = any(_contains_phrase(aio_text, phrase) for phrase in client_phrase_patterns)
        queries.append({
            "source_keyword": source_kw,
            "query_label": r.get("Query_Label"),
            "executed_query": r.get("Executed_Query"),
            "total_results": _safe_int(r.get("Total_Results"), 0),
            "serp_features": r.get("SERP_Features"),
            "has_ai_overview": bool(r.get("Has_Main_AI_Overview")),
            "client_mentioned_in_aio_text": client_mentioned,
            "rank_1": {
                "title": r.get("Rank_1_Title"),
                "source": _extract_domain(r.get("Rank_1_Link")),
            },
            "rank_2": {
                "title": r.get("Rank_2_Title"),
                "source": _extract_domain(r.get("Rank_2_Link")),
            },
            "rank_3": {
                "title": r.get("Rank_3_Title"),
                "source": _extract_domain(r.get("Rank_3_Link")),
            },
        })

    organic_rows_by_kw = defaultdict(list)
    entity_by_kw = defaultdict(Counter)
    entity_breakdown_with_na = defaultdict(Counter)
    content_counter = Counter()
    content_breakdown_by_kw = defaultdict(Counter)
    source_counter = Counter()
    rank_deltas = []
    total_organic_rows = 0
    entity_na = 0
    client_organic = []
    top_sources_by_kw_counter = defaultdict(lambda: defaultdict(lambda: {"appearances": 0, "best_rank": 999, "entity_types": Counter()}))

    for row in organic:
        source_kw = row.get("Source_Keyword")
        label = row.get("Query_Label")
        if not source_kw:
            continue
        total_organic_rows += 1
        rank = _safe_int(row.get("Rank"), 999)
        src = row.get("Source")
        entity = row.get("Entity_Type") or "N/A"
        content_type = row.get("Content_Type") or "N/A"
        snippet = str(row.get("Snippet") or "")
        title = str(row.get("Title") or "")
        link = str(row.get("Link") or "")

        if src:
            source_counter[src] += 1
        entity_breakdown_with_na[source_kw][entity] += 1
        if entity != "N/A":
            entity_by_kw[source_kw][entity] += 1
        else:
            entity_na += 1
        if content_type != "N/A":
            content_counter[content_type] += 1
            content_breakdown_by_kw[source_kw][content_type] += 1

        if label == "A":
            row_profile = {
                "rank": rank,
                "title": title,
                "source": src,
                "entity_type": entity,
                "content_type": content_type,
            }
            organic_rows_by_kw[source_kw].append(row_profile)
            if src:
                entry = top_sources_by_kw_counter[source_kw][src]
                entry["appearances"] += 1
                entry["best_rank"] = min(entry["best_rank"], rank)
                entry["entity_types"][entity] += 1

        if client_domain_lower and client_domain_lower in link.lower():
            delta_raw = row.get("Rank_Delta")
            delta = None if delta_raw in (None, "", "N/A") else _safe_int(delta_raw, 0)
            client_organic.append({
                "source_keyword": source_kw,
                "query_label": label,
                "rank": rank,
                "title": title,
                "link": link,
                "rank_delta": delta,
                "stability": (
                    "new" if delta is None else
                    "stable" if delta == 0 else
                    "improving" if delta > 0 else
                    "declining"
                ),
            })

        delta_raw = row.get("Rank_Delta")
        if delta_raw not in (None, "", "N/A"):
            delta = _safe_int(delta_raw, 0)
            if delta != 0:
                rank_deltas.append({
                    "source_keyword": source_kw,
                    "query_label": label,
                    "rank": rank,
                    "delta": delta,
                    "source": src,
                    "title": title,
                })

    aio_source_counter = Counter()
    aio_by_kw = defaultdict(Counter)
    client_aio_citations = []
    for row in citations:
        src = row.get("Source")
        source_kw = row.get("Source_Keyword")
        link = str(row.get("Link") or "")
        title = row.get("Title")
        if src:
            aio_source_counter[src] += 1
            aio_by_kw[source_kw][src] += 1
        if client_domain_lower and client_domain_lower in link.lower():
            client_aio_citations.append({
                "source_keyword": source_kw,
                "query_label": row.get("Query_Label"),
                "title": title,
                "link": link,
            })

    paa_unique = {}
    for row in paa_rows:
        question = row.get("Question")
        if not question:
            continue
        source_kw = row.get("Source_Keyword")
        record = paa_unique.setdefault(question, {
            "category": row.get("Category"),
            "score": _safe_int(row.get("Score"), 0),
            "source_keywords": [],
        })
        if source_kw and source_kw not in record["source_keywords"]:
            record["source_keywords"].append(source_kw)

    paa_cross_cluster = []
    paa_single_cluster = []
    for question, info in paa_unique.items():
        kws = sorted(info["source_keywords"])
        entry = {
            "question": question,
            "source_keywords": kws,
            "cluster_count": len(kws),
            "combined_total_results": sum(total_results_by_kw.get(kw, 0) for kw in kws),
            "category": info.get("category"),
        }
        if len(kws) >= 2:
            paa_cross_cluster.append(entry)
        else:
            paa_single_cluster.append(entry)
    paa_cross_cluster.sort(key=lambda item: (-item["cluster_count"], -item["combined_total_results"], item["question"]))
    paa_single_cluster.sort(key=lambda item: (-item["combined_total_results"], item["question"]))
    paa_analysis = {
        "cross_cluster": paa_cross_cluster,
        "single_cluster": paa_single_cluster,
        "summary": {
            "total_unique_questions": len(paa_unique),
            "cross_cluster_count": len(paa_cross_cluster),
            "single_cluster_count": len(paa_single_cluster),
        },
    }

    paa_by_intent = _classify_paa_intent(paa_rows)

    feasibility_rows = data.get("keyword_feasibility", [])
    feasibility_summary = _build_feasibility_summary(feasibility_rows) if feasibility_rows else None

    autocomplete_by_kw = defaultdict(list)
    autocomplete_texts = []
    for row in autocomplete:
        source_kw = row.get("Source_Keyword")
        suggestion = row.get("Suggestion")
        if source_kw and suggestion:
            autocomplete_by_kw[source_kw].append({
                "suggestion": suggestion,
                "relevance": row.get("Relevance"),
            })
            autocomplete_texts.append(str(suggestion))

    related_by_kw = defaultdict(list)
    related_texts = []
    for row in related:
        source_kw = row.get("Source_Keyword")
        term = row.get("Term")
        if source_kw and term and term not in related_by_kw[source_kw]:
            related_by_kw[source_kw].append(term)
            related_texts.append(str(term))

    bigrams = []
    trigrams = []
    for row in bigrams_trigrams:
        item = {"phrase": row.get("Phrase"), "count": _safe_int(row.get("Count"), 0)}
        if row.get("Type") == "Bigram":
            bigrams.append(item)
        elif row.get("Type") == "Trigram":
            trigrams.append(item)

    client_language_mentions = []
    for item in (bigrams + trigrams):
        phrase_l = _normalize_text(item.get("phrase"))
        if any(phrase in phrase_l for phrase in client_phrase_patterns):
            client_language_mentions.append(item)

    modules_by_kw = defaultdict(list)
    for row in modules:
        if row.get("Query_Label") == "A" and row.get("Present"):
            modules_by_kw[row.get("Source_Keyword")].append({
                "module": row.get("Module"),
                "order": _safe_int(row.get("Order"), 999),
            })

    serp_has_local = set()
    for kw in root_keywords:
        for mod in modules_by_kw.get(kw, []):
            if mod.get("module") in ("local_results", "local_map", "local_pack"):
                serp_has_local.add(kw)
                break

    local_pack_summary = {}
    client_local = []
    local_rows_by_kw = defaultdict(list)
    for row in local_pack:
        if row.get("Query_Label") != "A":
            continue
        source_kw = row.get("Source_Keyword")
        if source_kw:
            local_rows_by_kw[source_kw].append(row)

    for kw in root_keywords:
        rows = local_rows_by_kw.get(kw, [])
        if rows:
            category_counter = Counter(str(r.get("Category") or "Unknown") for r in rows)
            ratings = [float(r.get("Rating")) for r in rows if r.get("Rating") not in (None, "", "N/A")]
            client_present = False
            for row in rows:
                website = str(row.get("Website") or "")
                if client_domain_lower and client_domain_lower in website.lower():
                    client_present = True
                    client_local.append({
                        "source_keyword": kw,
                        "rank": row.get("Rank"),
                        "name": row.get("Name"),
                        "category": row.get("Category"),
                    })
            local_pack_summary[kw] = {
                "total_businesses": len(rows),
                "top_categories": category_counter.most_common(5),
                "avg_rating": round(sum(ratings) / len(ratings), 2) if ratings else None,
                "client_present": client_present,
                "on_serp": kw in serp_has_local,
            }
    local_pack_summary["serp_local_pack_confirmed"] = sorted(serp_has_local)
    local_pack_summary["serp_local_pack_absent"] = sorted(kw for kw in root_keywords if kw not in serp_has_local)

    market_language = {
        "top_20_bigrams": sorted(bigrams, key=lambda x: -x["count"])[:20],
        "top_10_trigrams": sorted(trigrams, key=lambda x: -x["count"])[:10],
        "client_mentions": client_language_mentions,
        "bowen_theory_terms": [],
    }

    aio_analysis = {}
    for kw in root_keywords:
        a_query = next((q for q in overview_by_kw.get(kw, []) if q.get("Query_Label") == "A"), None)
        aio_text = str((a_query or {}).get("AI_Overview") or "")
        opening_excerpt = aio_text[:400] if aio_text else None
        client_excerpt = None
        client_mentioned = False
        for phrase in client_phrase_patterns:
            if _contains_phrase(aio_text, phrase):
                client_mentioned = True
                client_excerpt = _extract_excerpt(aio_text, phrase)
                break
        top_sources = [source for source, _count in aio_by_kw.get(kw, Counter()).most_common(10)]
        sources_named = [source for source in top_sources if _contains_phrase(aio_text, _normalize_text(source))]
        key_phrases = []
        for item in (market_language["top_20_bigrams"] + market_language["top_10_trigrams"]):
            phrase = item["phrase"]
            if _contains_phrase(aio_text, _normalize_text(phrase)):
                key_phrases.append(phrase)
        aio_analysis[kw] = {
            "has_aio": bool((a_query or {}).get("Has_Main_AI_Overview")),
            "aio_length_chars": len(aio_text),
            "sources_named_in_text": sources_named[:10],
            "client_mentioned": client_mentioned,
            "client_excerpt": client_excerpt,
            "key_phrases": key_phrases[:12],
            "opening_excerpt": opening_excerpt,
        }

    # Framework language subset is computed after AIO/language extraction to keep zeros explicit.
    term_counts = []
    for term in framework_terms:
        total = 0
        for item in (bigrams + trigrams):
            if term in _normalize_text(item.get("phrase")):
                total += _safe_int(item.get("count"), 0)
        term_counts.append({"phrase": term, "count": total})
    market_language["bowen_theory_terms"] = term_counts

    tool_recommendations_verified = []
    all_trigger_words = set()
    organic_titles = [row.get("Title") for row in organic]
    organic_snippets = [row.get("Snippet") for row in organic]
    aio_texts = [row.get("AI_Overview") for row in overview]
    paa_texts = [row.get("Question") for row in paa_rows]
    for row in recs:
        triggers = _parse_trigger_words(row.get("Triggers"))
        all_trigger_words.update(triggers)
        found = {
            "in_paa_questions": _count_terms_in_texts(triggers, paa_texts),
            "in_organic_titles": _count_terms_in_texts(triggers, organic_titles),
            "in_organic_snippets": _count_terms_in_texts(triggers, organic_snippets),
            "in_aio_text": _count_terms_in_texts(triggers, aio_texts),
            "in_autocomplete": _count_terms_in_texts(triggers, autocomplete_texts),
            "in_related_searches": _count_terms_in_texts(triggers, related_texts),
        }
        source_totals = {
            name: sum(bucket.values())
            for name, bucket in found.items()
        }
        primary_source = max(source_totals.items(), key=lambda item: item[1])[0] if any(source_totals.values()) else "none"
        tool_recommendations_verified.append({
            "pattern_name": row.get("Pattern_Name"),
            "trigger_words_searched_for": triggers,
            "triggers_found": found,
            "content_angle": row.get("Content_Angle"),
            "status_quo_message": row.get("Status_Quo_Message"),
            "reframe": row.get("Bowen_Bridge_Reframe"),
            "verdict_inputs": {
                "any_paa_evidence": bool(found["in_paa_questions"]),
                "any_autocomplete_evidence": bool(found["in_autocomplete"]),
                "total_trigger_occurrences": sum(source_totals.values()),
                "primary_evidence_source": primary_source,
            },
        })

    autocomplete_summary = {
        "total_suggestions": len(autocomplete),
        "by_keyword": {kw: len(rows) for kw, rows in autocomplete_by_kw.items()},
        "trigger_word_hits": {
            trigger: sorted({
                row["suggestion"]
                for rows in autocomplete_by_kw.values()
                for row in rows
                if re.search(rf"\b{re.escape(trigger)}\b", str(row["suggestion"]), flags=re.IGNORECASE)
            })
            for trigger in sorted(all_trigger_words)
        },
    }

    competitive_landscape = {}
    keyword_profiles = {}
    for kw in root_keywords:
        kw_rows = sorted(organic_rows_by_kw.get(kw, []), key=lambda x: x["rank"])
        top_sources = []
        for source, info in sorted(
            top_sources_by_kw_counter.get(kw, {}).items(),
            key=lambda item: (-item[1]["appearances"], item[1]["best_rank"], item[0])
        )[:5]:
            entity_type = info["entity_types"].most_common(1)[0][0] if info["entity_types"] else "N/A"
            top_sources.append({
                "source": source,
                "appearances": info["appearances"],
                "entity_type": entity_type,
                "best_rank": info["best_rank"],
            })
        competitive_landscape[kw] = {
            "total_organic_results": len(kw_rows),
            "entity_breakdown": dict(entity_breakdown_with_na.get(kw, {})),
            "top_sources": top_sources,
            "content_type_breakdown": dict(content_breakdown_by_kw.get(kw, {})),
        }

        paa_for_kw = sorted([
            question for question, info in paa_unique.items()
            if kw in info["source_keywords"]
        ])
        local_summary = local_pack_summary.get(kw, {})
        kw_client_org = [item for item in client_organic if item["source_keyword"] == kw and item["query_label"] == "A"]
        kw_client_aio = [item for item in client_aio_citations if item["source_keyword"] == kw]
        modules_list = [
            f"{item['module']}:{item['order']}"
            for item in sorted(modules_by_kw.get(kw, []), key=lambda x: x["order"])
        ]
        dominant_type, entity_label = _classify_entity_distribution(entity_by_kw.get(kw, {}))
        keyword_profiles[kw] = {
            "total_results": total_results_by_kw.get(kw, 0),
            "serp_modules": modules_list,
            "has_ai_overview": aio_analysis.get(kw, {}).get("has_aio", False),
            "has_local_pack": kw in serp_has_local,
            "has_discussions_forums": any("discussions" in item["module"] or "forums" in item["module"] for item in modules_by_kw.get(kw, [])),
            "entity_distribution": dict(entity_by_kw.get(kw, {})),
            "entity_dominant_type": dominant_type,
            "entity_label": entity_label,
            "top5_organic": kw_rows[:5],
            "aio_citation_count": sum(aio_by_kw.get(kw, Counter()).values()),
            "aio_top_sources": aio_by_kw.get(kw, Counter()).most_common(5),
            "paa_questions": paa_for_kw,
            "paa_count": len(paa_for_kw),
            "autocomplete_top10": [item["suggestion"] for item in autocomplete_by_kw.get(kw, [])[:10]],
            "related_searches": related_by_kw.get(kw, [])[:10],
            "local_pack_count": local_summary.get("total_businesses", 0),
            "client_visible": bool(kw_client_org or kw_client_aio or local_summary.get("client_present")),
            "client_rank": kw_client_org[0]["rank"] if kw_client_org else None,
            "client_rank_delta": kw_client_org[0]["rank_delta"] if kw_client_org else None,
            "client_aio_cited": bool(kw_client_aio),
        }

    client_aio_text_mentions = []
    for kw, analysis in aio_analysis.items():
        if analysis.get("client_mentioned"):
            client_aio_text_mentions.append({
                "source_keyword": kw,
                "query_label": "A",
                "excerpt": analysis.get("client_excerpt") or analysis.get("opening_excerpt"),
            })

    client_position = {
        "organic": [],
        "aio_citations": [],
        "aio_text_mentions": client_aio_text_mentions,
        "local_pack": client_local,
        "language_pattern_mentions": client_language_mentions,
    }
    for item in client_organic:
        competitors_above = []
        for row in organic_rows_by_kw.get(item["source_keyword"], []):
            if row["rank"] < item["rank"]:
                competitors_above.append({
                    "rank": row["rank"],
                    "source": row["source"],
                    "entity_type": row["entity_type"],
                })
        enriched = dict(item)
        enriched["competitors_above"] = competitors_above
        client_position["organic"].append(enriched)
    for item in client_aio_citations:
        kw = item["source_keyword"]
        item_copy = dict(item)
        item_copy["also_mentioned_in_aio_text"] = any(m["source_keyword"] == kw for m in client_aio_text_mentions)
        item_copy["aio_text_excerpt"] = aio_analysis.get(kw, {}).get("client_excerpt")
        client_position["aio_citations"].append(item_copy)

    visible_keywords = sorted({
        item["source_keyword"] for item in client_position["organic"]
    } | {
        item["source_keyword"] for item in client_position["aio_citations"]
    } | {
        item["source_keyword"] for item in client_position["aio_text_mentions"]
    } | {
        item["source_keyword"] for item in client_position["local_pack"]
    })
    deltas = [item["rank_delta"] for item in client_position["organic"] if item["rank_delta"] is not None]
    client_position["summary"] = {
        "total_organic_appearances": len({(item["source_keyword"], item["query_label"]) for item in client_position["organic"]}),
        "total_aio_citations": len(client_position["aio_citations"]),
        "total_aio_text_mentions": len(client_position["aio_text_mentions"]),
        "total_local_pack": len(client_position["local_pack"]),
        "keywords_with_any_visibility": visible_keywords,
        "keywords_with_zero_visibility": [kw for kw in root_keywords if kw not in visible_keywords],
        "has_declining_positions": any((delta or 0) < 0 for delta in deltas),
        "worst_delta": min(deltas) if deltas else None,
    }

    strategic_flags = _compute_strategic_flags(
        root_keywords=root_keywords,
        keyword_profiles=keyword_profiles,
        client_position=client_position,
        total_results_by_kw=total_results_by_kw,
        paa_analysis=paa_analysis,
    )

    ads_out = []
    for row in ads:
        ads_out.append({
            "keyword": row.get("Source_Keyword") or row.get("Root_Keyword"),
            "advertiser": row.get("Name"),
            "position": row.get("Rank"),
            "link": row.get("Link"),
        })

    return {
        "metadata": metadata,
        "root_keywords": root_keywords,
        "queries": queries,
        "organic_summary": {
            "total_rows": total_organic_rows,
            "entity_classified_count": total_organic_rows - entity_na,
            "entity_unclassified_count": entity_na,
        },
        "source_frequency_top30": source_counter.most_common(30),
        "content_type_distribution": content_counter.most_common(10),
        "rank_deltas_top20": sorted(rank_deltas, key=lambda x: -abs(x["delta"]))[:20],
        "paa_analysis": paa_analysis,
        "paa_by_intent": paa_by_intent,
        "feasibility_summary": feasibility_summary,
        "tool_recommendations_verified": tool_recommendations_verified,
        "client_position": client_position,
        "keyword_profiles": keyword_profiles,
        "competitive_landscape": competitive_landscape,
        "aio_analysis": aio_analysis,
        "aio_citations_top25": aio_source_counter.most_common(25),
        "aio_total_citations": sum(aio_source_counter.values()),
        "aio_unique_sources": len(aio_source_counter),
        "autocomplete_by_keyword": dict(autocomplete_by_kw),
        "related_searches_by_keyword": dict(related_by_kw),
        "autocomplete_summary": autocomplete_summary,
        "local_pack_summary": local_pack_summary,
        "market_language": market_language,
        "competitor_ads": ads_out,
        "strategic_flags": strategic_flags,
    }


def validate_extraction(data):
    warnings = []
    if not data.get("root_keywords"):
        warnings.append("No root keywords extracted.")

    for kw in data.get("root_keywords", []):
        if kw not in data.get("keyword_profiles", {}):
            warnings.append(f"No keyword profile built for root keyword: {kw}")

    client_summary = data.get("client_position", {}).get("summary", {})
    if client_summary.get("total_organic_appearances", 0) == 0 and client_summary.get("total_aio_citations", 0) == 0 and client_summary.get("total_aio_text_mentions", 0) == 0 and client_summary.get("total_local_pack", 0) == 0:
        warnings.append("Client was not detected in organic, AIO citations, AIO text, or local pack.")

    queries_with_aio = sum(1 for q in data.get("queries", []) if q.get("has_ai_overview"))
    total_queries = len(data.get("queries", []))
    if total_queries > 0 and queries_with_aio == 0:
        warnings.append("No queries triggered AI Overviews.")

    if data.get("paa_analysis", {}).get("summary", {}).get("total_unique_questions", 0) < 5:
        warnings.append("Very low PAA coverage (<5 questions).")

    summary = data.get("organic_summary", {})
    total = summary.get("total_rows", 0)
    unclassified = summary.get("entity_unclassified_count", 0)
    if total > 0 and (unclassified / total) > 0.4:
        warnings.append(
            f"High unclassified entity share: {unclassified}/{total} ({unclassified/total:.0%})."
        )
    return warnings


def _extract_code_block_after_heading(markdown_text, heading_text):
    # Finds the first fenced code block after the given heading, allowing
    # explanatory text between the heading and the block.
    idx = markdown_text.find(heading_text)
    if idx == -1:
        return None
    tail = markdown_text[idx + len(heading_text):]
    match = re.search(r"```(?:[a-zA-Z0-9_]*)\n(.*?)\n```", tail, flags=re.DOTALL)
    if not match:
        return None
    return match.group(1).strip()


def _read_prompt_file(path):
    if not os.path.exists(path):
        return None
    with open(path, "r", encoding="utf-8") as f:
        return f.read().strip()


def load_prompt_blocks(prompt_source, progress_label="[5/7]", progress_name="prompt spec"):
    if not os.path.exists(prompt_source):
        return None, None
    progress(f"{progress_label} Loading {progress_name} from {prompt_source}...")
    if os.path.isdir(prompt_source):
        system_prompt = _read_prompt_file(os.path.join(prompt_source, "system.md"))
        user_template = _read_prompt_file(os.path.join(prompt_source, "user_template.md"))
        return system_prompt, user_template

    with open(prompt_source, "r", encoding="utf-8") as f:
        text = f.read()
    system_prompt = _extract_code_block_after_heading(text, "### System Prompt")
    user_template = _extract_code_block_after_heading(text, "### User Prompt Template")
    return system_prompt, user_template


def load_single_prompt(prompt_path, progress_label=None, progress_name="prompt template"):
    if progress_label:
        progress(f"{progress_label} Loading {progress_name} from {prompt_path}...")
    return _read_prompt_file(prompt_path)


def build_user_prompt(template, context, extracted_data, warnings):
    additional_context = context["additional_context"]
    if warnings:
        additional_context += "\n\nData extraction warnings:\n" + "\n".join(f"- {w}" for w in warnings)

    prompt_payload = build_main_report_payload(extracted_data)
    user_prompt = template
    replacements = {
        "{{CLIENT_NAME}}": context["client_name"],
        "{{CLIENT_DOMAIN}}": context["client_domain"],
        "{{ORG_TYPE}}": context["org_type"],
        "{{LOCATION}}": context["location"],
        "{{FRAMEWORK_DESCRIPTION}}": context["framework_description"],
        "{{CONTENT_FOCUS}}": context["content_focus"],
        "{{ADDITIONAL_CONTEXT}}": additional_context or "None provided.",
        "{{QUERY_COUNT}}": str(len(extracted_data.get("queries", []))),
        "{{ROOT_KEYWORD_COUNT}}": str(len(extracted_data.get("root_keywords", []))),
        "{{GEO_LOCATION}}": context["location"],
        "{{COLLECTION_DATE}}": extracted_data.get("metadata", {}).get("created_at", "unknown"),
        "{{EXTRACTED_DATA_JSON}}": json.dumps(prompt_payload, separators=(",", ":"), default=str),
    }
    for k, v in replacements.items():
        user_prompt = user_prompt.replace(k, v)
    return user_prompt


def build_main_report_payload(extracted_data):
    keyword_profiles = {}
    for keyword, profile in (extracted_data.get("keyword_profiles", {}) or {}).items():
        keyword_profiles[keyword] = {
            "total_results": profile.get("total_results"),
            "serp_modules": profile.get("serp_modules", [])[:8],
            "has_ai_overview": profile.get("has_ai_overview"),
            "has_local_pack": profile.get("has_local_pack"),
            "has_discussions_forums": profile.get("has_discussions_forums"),
            "entity_distribution": profile.get("entity_distribution", {}),
            "entity_dominant_type": profile.get("entity_dominant_type"),
            "entity_label": profile.get("entity_label"),
            "top5_organic": [
                {
                    "rank": row.get("rank"),
                    "title": row.get("title"),
                    "source": row.get("source"),
                    "entity_type": row.get("entity_type"),
                    "content_type": row.get("content_type"),
                }
                for row in profile.get("top5_organic", [])[:5]
            ],
            "aio_citation_count": profile.get("aio_citation_count"),
            "aio_top_sources": profile.get("aio_top_sources", [])[:5],
            "paa_questions": profile.get("paa_questions", [])[:8],
            "paa_count": profile.get("paa_count"),
            "autocomplete_top10": profile.get("autocomplete_top10", [])[:10],
            "related_searches": profile.get("related_searches", [])[:8],
            "local_pack_count": profile.get("local_pack_count"),
            "client_visible": profile.get("client_visible"),
            "client_rank": profile.get("client_rank"),
            "client_rank_delta": profile.get("client_rank_delta"),
            "client_aio_cited": profile.get("client_aio_cited"),
        }

    competitive_landscape = {}
    for keyword, landscape in (extracted_data.get("competitive_landscape", {}) or {}).items():
        competitive_landscape[keyword] = {
            "total_organic_results": landscape.get("total_organic_results"),
            "entity_breakdown": landscape.get("entity_breakdown", {}),
            "top_sources": landscape.get("top_sources", [])[:5],
            "content_type_breakdown": landscape.get("content_type_breakdown", {}),
        }

    payload = {
        "metadata": {
            "run_id": extracted_data.get("metadata", {}).get("run_id"),
            "created_at": extracted_data.get("metadata", {}).get("created_at"),
        },
        "root_keywords": extracted_data.get("root_keywords", []),
        "queries": [
            {
                "source_keyword": item.get("source_keyword"),
                "query_label": item.get("query_label"),
                "total_results": item.get("total_results"),
                "serp_features": item.get("serp_features", []),
                "has_ai_overview": item.get("has_ai_overview"),
                "client_mentioned_in_aio_text": item.get("client_mentioned_in_aio_text"),
            }
            for item in extracted_data.get("queries", [])
        ],
        "organic_summary": extracted_data.get("organic_summary", {}),
        "client_position": extracted_data.get("client_position", {}),
        "strategic_flags": extracted_data.get("strategic_flags", {}),
        "keyword_profiles": keyword_profiles,
        "competitive_landscape": competitive_landscape,
        "aio_analysis": extracted_data.get("aio_analysis", {}),
        "aio_citations_top25": extracted_data.get("aio_citations_top25", [])[:25],
        "aio_total_citations": extracted_data.get("aio_total_citations"),
        "aio_unique_sources": extracted_data.get("aio_unique_sources"),
        "paa_analysis": extracted_data.get("paa_analysis", {}),
        "bowen_reframe_faqs": (extracted_data.get("paa_by_intent") or {}).get("Systemic", [])[:10],
        "feasibility_summary": extracted_data.get("feasibility_summary"),
        "tool_recommendations_verified": extracted_data.get("tool_recommendations_verified", []),
        "autocomplete_by_keyword": {
            keyword: rows[:10]
            for keyword, rows in (extracted_data.get("autocomplete_by_keyword", {}) or {}).items()
        },
        "related_searches_by_keyword": {
            keyword: rows[:10]
            for keyword, rows in (extracted_data.get("related_searches_by_keyword", {}) or {}).items()
        },
        "autocomplete_summary": extracted_data.get("autocomplete_summary", {}),
        "local_pack_summary": extracted_data.get("local_pack_summary", {}),
        "market_language": extracted_data.get("market_language", {}),
        "competitor_ads": extracted_data.get("competitor_ads", []),
    }
    return payload


def build_correction_message(validation_issues, template_path=CORRECTION_PROMPT_DEFAULT):
    template = load_single_prompt(template_path)
    if not template:
        raise RuntimeError(f"Correction prompt template could not be loaded from {template_path}")
    issues_text = "\n".join(f"- {issue}" for issue in validation_issues)
    return template.replace("{{VALIDATION_ISSUES}}", issues_text)


def has_hard_validation_failures(validation_issues):
    hard_patterns = [
        "queries have ai overviews",
        "claims ai overviews appear for all",
        "cites 'toxic' as a high-volume autocomplete term",
        "cross-cutting 'toxic' opportunity",
        "marks '",
    ]
    for issue in validation_issues:
        normalized = _normalize_text(issue)
        if "despite zero verified trigger evidence" in normalized:
            return True
        if "but keyword_profiles shows" in normalized:
            return True
        if "but verified data shows" in normalized:
            return True
        if any(pattern in normalized for pattern in hard_patterns):
            return True
    return False


def partition_validation_issues(validation_issues):
    blocking = []
    notes = []
    for issue in validation_issues:
        normalized = _normalize_text(issue)
        if "contradicts keyword_profiles.entity_label" in normalized:
            notes.append(issue)
        else:
            blocking.append(issue)
    return blocking, notes


def append_interpretation_notes(report_text, note_issues):
    if not note_issues:
        return report_text

    note_lines = []
    seen = set()
    for issue in note_issues:
        match = re.search(
            r"for '([^']+)': ([^ ]+) should be described as ([^.]+)\.",
            issue,
            flags=re.IGNORECASE,
        )
        if match:
            keyword = match.group(1)
            entity_label = match.group(2)
            guidance = match.group(3)
            line = (
                f"For '{keyword}', the pre-computed entity label is `{entity_label}`. "
                f"Treat this SERP as {guidance} and interpret any single-category dominance wording cautiously."
            )
        else:
            line = issue
        if line not in seen:
            seen.add(line)
            note_lines.append(f"- {line}")

    section = "\n\n## Data Interpretation Notes\n" + "\n".join(note_lines) + "\n"
    if "## Data Interpretation Notes" in report_text:
        return report_text
    return report_text.rstrip() + section


def write_validation_artifact(output_path, title, validation_issues, draft_text):
    base, _ext = os.path.splitext(output_path)
    artifact_path = base + ".validation.md"
    lines = [
        f"# {title}",
        "",
        "## Rejected Claims",
        "",
    ]
    lines.extend(f"- {issue}" for issue in validation_issues)
    lines.extend([
        "",
        "## Rejected Draft",
        "",
        draft_text.strip(),
        "",
    ])
    with open(artifact_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    return artifact_path


def run_llm_report(system_prompt, user_prompt, model, max_tokens, prior_response=None, correction_message=None):
    if not ANTHROPIC_AVAILABLE:
        raise RuntimeError("anthropic package not installed.")
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        raise RuntimeError("ANTHROPIC_API_KEY is not set.")

    progress(f"[7/7] Calling Anthropic model {model}...")
    client = anthropic.Anthropic(api_key=api_key)
    messages = [{"role": "user", "content": user_prompt}]
    if prior_response is not None:
        if not correction_message:
            raise RuntimeError("correction_message is required when prior_response is provided.")
        messages = [
            {"role": "user", "content": user_prompt},
            {"role": "assistant", "content": prior_response},
            {"role": "user", "content": correction_message},
        ]
    response = client.messages.create(
        model=model,
        max_tokens=max_tokens,
        system=[
            {
                "type": "text",
                "text": system_prompt,
                "cache_control": {"type": "ephemeral"},
            }
        ],
        messages=messages,
    )
    chunks = []
    for block in response.content:
        text = getattr(block, "text", None)
        if text:
            chunks.append(text)
    return "\n".join(chunks).strip()


def _mixed_keyword_dominance_profiles(extracted_data):
    profiles = []
    for keyword, profile in (extracted_data.get("keyword_profiles", {}) or {}).items():
        distribution = profile.get("entity_distribution", {}) or {}
        ranked = sorted(
            [(entity, count) for entity, count in distribution.items() if count],
            key=lambda item: item[1],
            reverse=True,
        )
        if len(ranked) < 2:
            continue
        (top_entity, top_count), (_second_entity, second_count) = ranked[:2]
        classified_total = sum(count for _entity, count in ranked)
        if classified_total <= 0:
            continue
        top_share = top_count / classified_total
        if second_count >= 3 and top_share <= 0.60:
            profiles.append((keyword, top_entity, top_count, second_count, top_share))
    return profiles


def _label_requires_mixed(entity_label):
    return str(entity_label or "").startswith("mixed_")


def _label_requires_plurality(entity_label):
    return str(entity_label or "").endswith("_plurality")


def validate_llm_report(report_text, extracted_data):
    issues = []
    report_l = _normalize_text(report_text)
    query_count = len(extracted_data.get("queries", []))
    keyword_profiles = extracted_data.get("keyword_profiles", {}) or {}
    queries_with_aio = sum(
        1 for profile in keyword_profiles.values()
        if profile.get("has_ai_overview")
    ) or sum(1 for q in extracted_data.get("queries", []) if q.get("has_ai_overview"))

    speculative_patterns = [
        r"indicating (?:technical|content|data|system)\w*",
        r"suggesting (?:a |that |some )?(?:bug|issue|problem|filter)",
        r"possibly due to",
        r"likely (?:because|due|caused)",
        r"technical issues? or content filtering",
        r"content filtering",
    ]
    for pattern in speculative_patterns:
        if re.search(pattern, report_l):
            issues.append(
                f"Report contains speculative causal language matching pattern: {pattern}"
            )
            break

    paa_cross_questions = {
        _normalize_text(item.get("question"))
        for item in extracted_data.get("paa_analysis", {}).get("cross_cluster", [])
    }
    if "cross-cutting" in report_l and "toxic" in report_l:
        toxic_cross_cluster = any("toxic" in question for question in paa_cross_questions)
        toxic_autocomplete = bool(extracted_data.get("autocomplete_summary", {}).get("trigger_word_hits", {}).get("toxic", []))
        if not toxic_cross_cluster and not toxic_autocomplete:
            issues.append(
                "Report claims a cross-cutting 'toxic' opportunity, but verified PAA/autocomplete evidence is absent."
            )

    for rec in extracted_data.get("tool_recommendations_verified", []):
        pattern_name = rec.get("pattern_name", "")
        verdict = rec.get("verdict_inputs", {})
        total_hits = verdict.get("total_trigger_occurrences", 0)
        if not pattern_name:
            continue
        section_match = re.search(
            rf"\*\*{re.escape(pattern_name)}\*\*:\s*([A-Z ]+)\.",
            report_text,
            flags=re.IGNORECASE,
        )
        if total_hits == 0 and section_match:
            label = section_match.group(1).strip().upper()
            if label in {"SUPPORTED", "PARTIALLY SUPPORTED"}:
                issues.append(
                    f"Report marks '{pattern_name}' as {label}, but verified trigger evidence count is zero."
                )
            paragraph_match = re.search(
                rf"(\*\*{re.escape(pattern_name)}\*\*:[^\n]*(?:\n(?!\*\*).*)*)",
                report_text,
                flags=re.IGNORECASE,
            )
            paragraph_text = paragraph_match.group(1) if paragraph_match else ""
            if re.search(
                r"(appear frequently|multiple autocomplete suggestions include|trigger[s]? found|heavy presence)",
                paragraph_text,
                flags=re.IGNORECASE,
            ):
                issues.append(
                    f"Report cites specific trigger evidence for '{pattern_name}' despite zero verified trigger evidence."
                )

    toxic_hits = extracted_data.get("autocomplete_summary", {}).get("trigger_word_hits", {}).get("toxic", [])
    if not toxic_hits and "high search volume term from autocomplete data" in report_l and "toxic" in report_l:
        issues.append(
            "Report cites 'toxic' as a high-volume autocomplete term, but autocomplete_summary shows no toxic hits."
        )

    aio_all_patterns = [
        rf"ai overviews appear for all {query_count} queries",
        rf"ai overviews appear across all {query_count} queries",
        rf"all {query_count} queries trigger ai overviews",
    ]
    if query_count and queries_with_aio != query_count:
        for pattern in aio_all_patterns:
            if re.search(pattern, report_l):
                issues.append(
                    f"Report claims AI Overviews appear for all {query_count} queries, but verified data shows {queries_with_aio} of {query_count}."
                )
                break
    aio_count_match = re.search(
        r"(\d+)\s+of\s+(\d+)\s+quer(?:y|ies)\s+(?:feature|have|show|trigger)",
        report_text,
        flags=re.IGNORECASE,
    )
    if aio_count_match:
        reported_with_aio = int(aio_count_match.group(1))
        reported_total = int(aio_count_match.group(2))
        if query_count and reported_total == query_count and reported_with_aio != queries_with_aio:
            issues.append(
                f"Report says {reported_with_aio} of {reported_total} queries have AI Overviews, but keyword_profiles shows {queries_with_aio} of {query_count}."
            )

    if re.search(r"data collection issue|potential data collection issue|suggesting .*data collection issue", report_l):
        issues.append(
            "Report speculates about a data collection issue without verified extraction evidence."
        )

    if re.search(r"monthly searches|monthly search volume", report_l):
        issues.append(
            "Report describes total_results as monthly search volume, which is not supported by the extracted data."
        )

    estrangement_profile = extracted_data.get("keyword_profiles", {}).get("estrangement", {})
    entity_distribution = estrangement_profile.get("entity_distribution", {}) or {}
    counselling_count = entity_distribution.get("counselling", 0)
    legal_count = entity_distribution.get("legal", 0)
    if counselling_count >= 3 and legal_count >= 3:
        if "estrangement" in report_l and re.search(r"counselling-dominat|counselling services dominat", report_l):
            issues.append(
                "Report labels the broad estrangement landscape as counselling-dominant despite a mixed legal and counselling entity distribution."
            )

    for keyword, top_entity, top_count, second_count, top_share in _mixed_keyword_dominance_profiles(extracted_data):
        section_match = re.search(
            rf"\*\*{re.escape(keyword)} \([^\n]+\)\*\*(.*?)(?:\n\n\*\*|\n### |\Z)",
            report_text,
            flags=re.IGNORECASE | re.DOTALL,
        )
        if not section_match:
            continue
        section_text = section_match.group(1)
        if re.search(rf"\b{re.escape(top_entity)} (?:entities )?(?:heavily )?dominat", section_text, flags=re.IGNORECASE):
            issues.append(
                f"Report labels '{keyword}' as {top_entity}-dominant, but the classified entity mix is too close ({top_count} vs {second_count}; {top_share:.0%} share) and should be described as mixed or contested."
            )

    for keyword, profile in keyword_profiles.items():
        entity_label = profile.get("entity_label")
        if not entity_label:
            continue
        section_match = re.search(
            rf"\*\*{re.escape(keyword)} \([^\n]+\)\*\*(.*?)(?:\n\n\*\*|\n### |\Z)",
            report_text,
            flags=re.IGNORECASE | re.DOTALL,
        )
        if not section_match:
            continue
        section_l = _normalize_text(section_match.group(1))
        if _label_requires_mixed(entity_label) and re.search(r"\bdominat(?:e|ed|es|ing)\b", section_l):
            issues.append(
                f"Report contradicts keyword_profiles.entity_label for '{keyword}': {entity_label} should be described as mixed or contested, not dominant."
            )
        if _label_requires_plurality(entity_label) and re.search(r"\bdominat(?:e|ed|es|ing)\b", section_l):
            issues.append(
                f"Report contradicts keyword_profiles.entity_label for '{keyword}': {entity_label} should be described as a plurality, not dominant."
            )

    return issues


def validate_advisory_briefing(report_text, extracted_data):
    issues = []
    report_l = _normalize_text(report_text)
    strategic_flags = extracted_data.get("strategic_flags", {})
    priorities = strategic_flags.get("content_priorities", [])
    first_priority = priorities[0] if priorities else {}
    skip_keywords = {item.get("keyword") for item in priorities if item.get("action") == "skip"}

    if strategic_flags.get("defensive_urgency") == "high":
        headline = report_text.split("## Action 1", 1)[0]
        first_block = report_text.split("## Action 1", 1)[1] if "## Action 1" in report_text else report_text
        first_block = first_block.split("## Action 2", 1)[0]
        expected_keyword = _normalize_text(first_priority.get("keyword"))
        if expected_keyword and expected_keyword not in _normalize_text(headline + "\n" + first_block):
            issues.append(
                f"Advisory briefing does not make the top defensive keyword '{first_priority.get('keyword')}' the first action."
            )

    for kw in skip_keywords:
        if kw and _normalize_text(kw) in report_l and "stop thinking about" not in report_l:
            issues.append(
                f"Advisory briefing references skip keyword '{kw}' outside the stop-thinking section."
            )

    overconfident_patterns = [
        r"you'?ll lose your rank #\d+ position entirely",
        r"will lose .*ai overview citation",
        r"will disappear entirely",
    ]
    for pattern in overconfident_patterns:
        if re.search(pattern, report_l):
            issues.append(
                "Advisory briefing uses unsupported certainty language where the data supports risk, not certainty."
            )
            break

    if re.search(r"monthly searches|monthly search volume", report_l):
        issues.append(
            "Advisory briefing describes total_results as monthly search volume, which is not supported by the extracted data."
        )

    estrangement_profile = extracted_data.get("keyword_profiles", {}).get("estrangement", {})
    entity_distribution = estrangement_profile.get("entity_distribution", {}) or {}
    counselling_count = entity_distribution.get("counselling", 0)
    legal_count = entity_distribution.get("legal", 0)
    if counselling_count >= 3 and legal_count >= 3:
        if "estrangement" in report_l and re.search(r"counselling practices dominat|counselling services dominat", report_l):
            issues.append(
                "Advisory briefing overstates broad estrangement as counselling-dominant despite mixed legal and counselling signals."
            )

    if re.search(r"eliminate your digital presence entirely|complete loss of (?:your )?digital presence", report_l):
        issues.append(
            "Advisory briefing overstates the consequence scope beyond measured search visibility."
        )

    return issues


def _infer_intent_text(keyword, paa_questions, related_terms):
    text = " ".join(
        [keyword.lower()] + [q.lower() for q in paa_questions] + [t.lower() for t in related_terms]
    )
    if any(x in text for x in ["law", "lawyer", "custody", "will", "inheritance", "court"]):
        return "Legal-dominant intent"
    if any(x in text for x in ["cost", "price", "fees", "insurance", "free"]):
        return "Cost/access intent"
    if any(x in text for x in ["therap", "counsell", "counsel"]):
        return "Therapeutic service intent"
    if any(x in text for x in ["what is", "meaning", "definition", "stages"]):
        return "Informational/educational intent"
    return "Mixed informational and support intent"


def _score_keyword_opportunity(extracted_data, source_keyword):
    client_org = [
        r for r in extracted_data.get("client_position", {}).get("organic", [])
        if r.get("source_keyword") == source_keyword
    ]
    client_aio = [
        r for r in extracted_data.get("client_position", {}).get("aio_citations", [])
        if r.get("source_keyword") == source_keyword
    ]
    client_local = [
        r for r in extracted_data.get("client_position", {}).get("local_pack", [])
        if r.get("source_keyword") == source_keyword
    ]
    profile = extracted_data.get("keyword_profiles", {}).get(source_keyword, {})
    aio_count = profile.get("aio_citation_count", 0)
    organic_rows = profile.get("top5_organic", [])
    low_competition = len({r.get("source") for r in organic_rows[:10] if r.get("source")}) <= 5
    has_top10_client = any(_safe_int(r.get("rank"), 999) <= 10 for r in client_org)
    has_position = bool(client_org or client_aio or client_local)

    score = 0
    if has_position:
        score += 4
    if has_top10_client:
        score += 3
    score += min(aio_count, 3)
    if low_competition:
        score += 2
    if len(organic_rows) >= 20:
        score += 1
    return score


def generate_local_report(extracted_data, context, warnings):
    now = datetime.now().strftime("%Y-%m-%d")
    root_keywords = extracted_data.get("root_keywords", [])
    queries = extracted_data.get("queries", [])
    organic_summary = extracted_data.get("organic_summary", {})

    kw_scores = [
        (kw, _score_keyword_opportunity(extracted_data, kw))
        for kw in root_keywords
    ]
    kw_scores.sort(key=lambda x: x[1], reverse=True)

    lines = []
    lines.append("# Content Opportunity Report")
    lines.append(f"Date: {now}")
    lines.append(f"Run ID: {extracted_data.get('metadata', {}).get('run_id', 'unknown')}")
    lines.append("")
    lines.append("## Section 1: Data Summary")
    lines.append(
        f"This run analyzed {len(root_keywords)} root keywords and {len(queries)} total queries. "
        f"The dataset includes {organic_summary.get('total_rows', 0)} organic rows and "
        f"{extracted_data.get('aio_total_citations', 0)} AI Overview citations."
    )
    if warnings:
        lines.append(
            "Data quality caveats were detected: " + "; ".join(warnings) + ". "
            "Interpret entity distributions and recommendation confidence accordingly."
        )
    else:
        lines.append("No major extraction warnings were detected.")

    lines.append("")
    lines.append("## Section 2: Keyword Cluster Analysis")
    for kw, score in kw_scores:
        profile = extracted_data.get("keyword_profiles", {}).get(kw, {})
        landscape = extracted_data.get("competitive_landscape", {}).get(kw, {})
        top_sources = landscape.get("top_sources", [])
        top_sources_text = ", ".join(f"{s['source']} ({s['appearances']})" for s in top_sources[:3]) or "No dominant source pattern"
        paa_q = profile.get("paa_questions", [])[:5]
        related = profile.get("related_searches", [])[:5]
        intent = _infer_intent_text(kw, paa_q, related)
        modules_text = ", ".join(profile.get("serp_modules", [])[:6]) or "No module data"
        lines.append(
            f"For '{kw}', opportunity score is {score}. Dominant sources include {top_sources_text}. "
            f"Likely intent: {intent}. Common SERP modules: {modules_text}."
        )

    lines.append("")
    lines.append("## Section 3: Client Position Assessment")
    client_summary = extracted_data.get("client_position", {}).get("summary", {})
    lines.append(
        f"Client organic appearances: {client_summary.get('total_organic_appearances', 0)}. "
        f"Client AIO citations: {client_summary.get('total_aio_citations', 0)}. "
        f"Client local pack appearances: {client_summary.get('total_local_pack', 0)}. "
        f"Client AIO text mentions: {client_summary.get('total_aio_text_mentions', 0)}."
    )
    rank_deltas = extracted_data.get("rank_deltas_top20", [])
    if rank_deltas:
        biggest = rank_deltas[0]
        lines.append(
            f"Top rank movement sample: keyword '{biggest.get('source_keyword')}', "
            f"title '{biggest.get('title')}', delta {biggest.get('delta')}."
        )
    else:
        lines.append("No significant rank-delta history is available yet.")

    lines.append("")
    lines.append("## Section 4: AI Overview / GEO Opportunity Analysis")
    queries_with_aio = [q for q in queries if q.get("has_ai_overview")]
    lines.append(
        f"{len(queries_with_aio)} of {len(queries)} queries triggered AI Overview. "
        f"Top cited sources: {', '.join(s for s, _ in extracted_data.get('aio_citations_top25', [])[:5]) or 'none'}."
    )
    lines.append(
        "Best near-term GEO gains should target source-keywords where AI Overview appears and "
        "the client already has either organic presence or at least one citation foothold."
    )

    lines.append("")
    lines.append("## Section 5: Content Gap Analysis")
    cross_cutting = extracted_data.get("paa_analysis", {}).get("cross_cluster", [])[:8]
    if cross_cutting:
        lines.append(
            "Cross-cutting PAA gaps (appearing across multiple root keywords) indicate high-leverage content themes: "
            + "; ".join(p.get("question") for p in cross_cutting) + "."
        )
    else:
        lines.append("No strong cross-keyword PAA overlap was detected in this run.")

    lines.append("")
    lines.append("## Section 6: Evaluation of Tool-Generated Recommendations")
    tool_recs = extracted_data.get("tool_recommendations_verified", [])
    if not tool_recs:
        lines.append("No tool-generated recommendations were present.")
    else:
        for rec in tool_recs:
            verdict = rec.get("verdict_inputs", {})
            support = "supported" if verdict.get("total_trigger_occurrences", 0) > 0 else "weakly supported"
            lines.append(
                f"'{rec.get('pattern_name')}' is {support}. Trigger evidence source: {verdict.get('primary_evidence_source', 'none')}. "
                f"Angle: {rec.get('content_angle')}."
            )

    lines.append("")
    lines.append("## Section 7: Prioritized Content Recommendations")
    for kw, score in kw_scores[:7]:
        top_sources = extracted_data.get("competitive_landscape", {}).get(kw, {}).get("top_sources", [])
        dominant = ", ".join(s["source"] for s in top_sources[:2]) or "mixed sources"
        lines.append(
            f"Priority keyword cluster '{kw}' (score {score}): create one focused page that directly answers the "
            f"highest-scoring PAA questions for this cluster, framed using {context['framework_description']}. "
            f"Rationale: current SERP is dominated by {dominant}, and this cluster has measurable visibility runway. "
            "Success criteria: new AI Overview citations and organic rank gains in label-A queries. "
            "Avoid broad, generic content that does not match the observed intent profile."
        )

    lines.append("")
    lines.append("## Section 8: Keyword Expansion Recommendations")
    for kw in root_keywords[:]:
        ac = extracted_data.get("autocomplete_by_keyword", {}).get(kw, [])[:3]
        rel = extracted_data.get("related_searches_by_keyword", {}).get(kw, [])[:3]
        if not ac and not rel:
            continue
        ac_text = ", ".join((x.get("suggestion") or "") for x in ac if x.get("suggestion"))
        rel_text = ", ".join(rel)
        lines.append(
            f"For '{kw}', test expansion terms from autocomplete ({ac_text or 'none'}) and related searches "
            f"({rel_text or 'none'}). These terms should be queued for the next run when they sharpen intent "
            "segmentation (legal vs therapeutic vs cost-access)."
        )

    return "\n".join(lines).strip()


def score_paa_for_brief(question_text, theme_words):
    q_lower = str(question_text or "").lower()
    return sum(1 for word in theme_words if word in q_lower)


def get_relevant_paa(paa_questions, pattern_name, max_results=5):
    theme_words = BRIEF_PAA_THEMES.get(pattern_name, [])
    category_hints = BRIEF_PAA_CATEGORIES.get(pattern_name, set())
    keyword_hints = BRIEF_KEYWORD_HINTS.get(pattern_name, [])
    if not theme_words and not category_hints and not keyword_hints:
        return _dedupe_question_records(paa_questions)[:max_results]

    scored = []
    for idx, q in enumerate(_dedupe_question_records(paa_questions)):
        question = q.get("Question", "")
        category = q.get("Category")
        source_keyword = str(q.get("Source_Keyword", "")).lower()
        theme_score = score_paa_for_brief(question, theme_words)
        category_score = 1 if category in category_hints else 0
        keyword_score = sum(1 for hint in keyword_hints if hint in source_keyword)
        score = (theme_score * 3) + (category_score * 2) + keyword_score
        scored.append((q, score, idx))

    scored.sort(key=lambda item: (-item[1], item[2]))
    matched = [q for q, score, _idx in scored if score > 0][:max_results]

    if len(matched) < max_results:
        matched_texts = {q.get("Question") for q in matched}
        category_fill = [
            q for q, _score, _idx in scored
            if q.get("Category") in category_hints
            and q.get("Question") not in matched_texts
        ]
        matched.extend(category_fill[:max_results - len(matched)])

    if len(matched) < max_results:
        matched_texts = {q.get("Question") for q in matched}
        remaining = [
            q for q, _score, _idx in scored
            if q.get("Question") not in matched_texts
        ]
        matched.extend(remaining[:max_results - len(matched)])

    return matched[:max_results]


def get_relevant_competitors(organic_results, pattern_name, max_results=3):
    theme_words = BRIEF_PAA_THEMES.get(pattern_name, [])
    seen_titles = set()
    scored = []
    for idx, row in enumerate(organic_results):
        title = row.get("Title", "")
        if not title or title in seen_titles:
            continue
        seen_titles.add(title)
        combined = f"{title} {row.get('Snippet', '')}".lower()
        score = sum(1 for word in theme_words if word in combined)
        rank = _safe_int(row.get("Rank"), 999)
        scored.append((title, score, rank, idx))

    scored.sort(key=lambda item: (-item[1], item[2], item[3]))
    top = [title for title, score, _rank, _idx in scored if score > 0][:max_results]
    if len(top) < max_results:
        remaining = [
            title for title, _score, _rank, _idx in scored
            if title not in top
        ]
        top.extend(remaining[:max_results - len(top)])
    return top[:max_results]


def _dedupe_question_records(paa_questions):
    out = []
    seen = set()
    for q in paa_questions:
        question = q.get("Question")
        key = str(question or "").strip().lower()
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(q)
    return out


def list_recommendations(data, args):
    config = load_yaml_config(args.config)
    progress("[3/7] Resolving client context from YAML...")
    context = load_client_context_from_config(config)

    progress("[4/7] Extracting report data from market_analysis JSON...")
    extracted = extract_analysis_data_from_json(
        data,
        client_domain=context["client_domain"],
        client_name_patterns=context["client_name_patterns"],
        framework_terms=context.get("framework_terms"),
    )
    warnings = validate_extraction(extracted)
    progress(
        f"      Extracted {len(extracted.get('root_keywords', []))} root keywords, "
        f"{len(extracted.get('queries', []))} queries, "
        f"{extracted.get('aio_total_citations', 0)} AIO citations."
    )
    extracted_size = len(json.dumps(extracted, separators=(",", ":"), default=str))
    progress(f"      Extracted data object size: {extracted_size} chars.")

    if args.use_llm:
        if not ANTHROPIC_AVAILABLE:
            print("Error: anthropic package is not installed.")
            sys.exit(2)
        if not os.getenv("ANTHROPIC_API_KEY"):
            print("Error: ANTHROPIC_API_KEY is not set.")
            sys.exit(2)

        system_prompt, user_template = load_prompt_blocks(
            args.prompt_spec,
            progress_label="[5/7]",
            progress_name="main report prompt",
        )
        if not (system_prompt and user_template):
            print(f"Error: Prompt blocks could not be loaded from {args.prompt_spec}")
            sys.exit(2)
        try:
            progress("[6/7] Building LLM prompt payload...")
            prompt_payload = build_main_report_payload(extracted)
            prompt_payload_size = len(json.dumps(prompt_payload, separators=(",", ":"), default=str))
            progress(f"      Main report prompt payload size: {prompt_payload_size} chars.")
            user_prompt = build_user_prompt(user_template, context, extracted, warnings)
            report = run_llm_report(
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                model=args.llm_model,
                max_tokens=args.llm_max_tokens,
            )
            validation_issues = validate_llm_report(report, extracted)
            blocking_issues, note_issues = partition_validation_issues(validation_issues)
            if validation_issues and not args.allow_unverified_report:
                if not blocking_issues and note_issues:
                    progress("[note] LLM draft produced recoverable interpretation warnings. Writing report with notes.")
                    report = append_interpretation_notes(report, note_issues)
                    validation_issues = []
                elif has_hard_validation_failures(blocking_issues):
                    progress("[fail-fast] Initial LLM draft failed hard factual validation. Skipping retry.")
                else:
                    progress("[retry] Initial LLM draft failed evidence validation. Requesting one corrected revision...")
                    correction_msg = build_correction_message(
                        blocking_issues,
                        template_path=args.correction_prompt,
                    )
                    report = run_llm_report(
                        system_prompt=system_prompt,
                        user_prompt=user_prompt,
                        model=args.llm_model,
                        max_tokens=args.llm_max_tokens,
                        prior_response=report,
                        correction_message=correction_msg,
                    )
                    validation_issues = validate_llm_report(report, extracted)
                    blocking_issues, note_issues = partition_validation_issues(validation_issues)
                    if not blocking_issues and note_issues:
                        progress("[note] Corrected draft still has interpretation warnings. Writing report with notes.")
                        report = append_interpretation_notes(report, note_issues)
                        validation_issues = []
                if validation_issues:
                    artifact_path = write_validation_artifact(
                        args.report_out,
                        "Content Opportunity Report Validation Issues",
                        validation_issues,
                        report,
                    )
                    print("Error: LLM report failed evidence validation.")
                    for issue in validation_issues:
                        print(f"- {issue}")
                    print(f"- Validation artifact written to {artifact_path}")
                    sys.exit(2)
            progress("[done] LLM response received. Writing report to disk...")
        except Exception as e:
            print(f"Error: LLM report generation failed: {e}")
            sys.exit(2)
    else:
        progress("[6/7] Generating local heuristic report...")
        report = generate_local_report(extracted, context, warnings)
        progress("[done] Local report generated. Writing report to disk...")

    with open(args.report_out, "w", encoding="utf-8") as f:
        f.write(report + "\n")

    if args.advisory_briefing:
        if not args.use_llm:
            print("Warning: --advisory-briefing requires --use-llm. Skipping.")
        elif not ANTHROPIC_AVAILABLE or not os.getenv("ANTHROPIC_API_KEY"):
            print("Error: Advisory briefing requires anthropic package and API key.")
            sys.exit(2)
        else:
            progress("[advisory] Running second-pass strategic briefing...")
            advisory_model = args.advisory_model or args.llm_model
            advisory_system_prompt, advisory_user_template = load_prompt_blocks(
                args.advisory_prompt_dir,
                progress_label="[advisory]",
                progress_name="advisory prompt",
            )
            if not (advisory_system_prompt and advisory_user_template):
                print(f"Error: Advisory prompt files could not be loaded from {args.advisory_prompt_dir}")
                sys.exit(2)
            advisory_user = advisory_user_template.format(
                client_name=context["client_name"],
                client_domain=context["client_domain"],
                org_type=context["org_type"],
                location=context["location"],
                framework_description=context["framework_description"],
                content_focus=context["content_focus"],
                additional_context=context["additional_context"],
                strategic_flags_json=json.dumps(
                    extracted["strategic_flags"],
                    separators=(",", ":"),
                    default=str,
                ),
                market_report_text=report,
            )
            advisory_report = run_llm_report(
                system_prompt=advisory_system_prompt,
                user_prompt=advisory_user,
                model=advisory_model,
                max_tokens=4000,
            )
            advisory_issues = validate_advisory_briefing(advisory_report, extracted)
            if advisory_issues:
                correction_msg = build_correction_message(
                    advisory_issues,
                    template_path=args.correction_prompt,
                )
                advisory_report = run_llm_report(
                    system_prompt=advisory_system_prompt,
                    user_prompt=advisory_user,
                    model=advisory_model,
                    max_tokens=4000,
                    prior_response=advisory_report,
                    correction_message=correction_msg,
                )
                advisory_issues = validate_advisory_briefing(advisory_report, extracted)
                if advisory_issues:
                    artifact_path = write_validation_artifact(
                        args.advisory_out,
                        "Advisory Briefing Validation Issues",
                        advisory_issues,
                        advisory_report,
                    )
                    print("Error: Advisory briefing failed validation.")
                    for issue in advisory_issues:
                        print(f"- {issue}")
                    print(f"- Validation artifact written to {artifact_path}")
                    sys.exit(2)
            with open(args.advisory_out, "w", encoding="utf-8") as f:
                f.write(advisory_report + "\n")
            progress(f"[done] Advisory briefing written to {args.advisory_out}")

    print("\nContent Opportunity Report generated.\n")
    print(f"Output: {args.report_out}")
    print(f"Run ID: {extracted.get('metadata', {}).get('run_id', 'unknown')}")
    print(f"Root keywords: {len(extracted.get('root_keywords', []))}")
    print(f"Queries: {len(extracted.get('queries', []))}")
    print(f"AIO citations: {extracted.get('aio_total_citations', 0)}")
    print(f"Client organic appearances: {extracted.get('client_position', {}).get('summary', {}).get('total_organic_appearances', 0)}")
    print(f"Client AIO citations: {extracted.get('client_position', {}).get('summary', {}).get('total_aio_citations', 0)}")
    print(f"Report mode: {'LLM (' + args.llm_model + ')' if args.use_llm else 'heuristic'}")

    if warnings:
        print("\nData warnings:")
        for w in warnings:
            print(f"- {w}")


def generate_brief(data, rec_index=0):
    # Legacy brief generator retained for direct --out usage.
    recs = data.get("strategic_recommendations", [])
    if not recs:
        return "No strategic recommendations found in the dataset."

    if rec_index >= len(recs):
        print(f"Index {rec_index} out of range. Using 0.")
        rec_index = 0

    rec = recs[rec_index]
    paa = data.get("paa_questions", [])
    organic = data.get("organic_results", [])
    relevant_paa_records = get_relevant_paa(
        paa, rec.get("Pattern_Name"), max_results=5
    )
    relevant_paa = [q.get("Question") for q in relevant_paa_records]
    top_competitors = get_relevant_competitors(
        organic, rec.get("Pattern_Name"), max_results=3
    )
    lines = []
    lines.append(f"# Content Brief: {rec.get('Content_Angle')}")
    lines.append(f"**Strategy:** {rec.get('Pattern_Name')}")
    lines.append(f"**Date:** {datetime.now().strftime('%Y-%m-%d')}\n")
    lines.append("## 1. The Core Conflict (The Hook)")
    lines.append(f"**The Status Quo (Bad Advice):** \"{rec.get('Status_Quo_Message')}\"")
    lines.append(f"**The Bowen Reframe (The Solution):** \"{rec.get('Bowen_Bridge_Reframe')}\"")
    lines.append(
        f"**Target Audience Pain Point:** They are searching for *{rec.get('Detected_Triggers')}* and feeling anxious/stuck."
    )
    lines.append("\n## 2. User Intent & Anxiety (PAA)")
    lines.append("Address these specific questions to validate the reader's experience:")
    for q in relevant_paa:
        lines.append(f"- {q}")
    lines.append("\n## 3. The Competition (What to Differentiate Against)")
    for t in top_competitors:
        lines.append(f"- *{t}*")
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(
        description="Generate prompt-informed content opportunities report or a legacy content brief."
    )
    parser.add_argument("--json", required=True, help="Path to market_analysis_v2.json")
    parser.add_argument("--out", help="Legacy single-brief output markdown path")
    parser.add_argument("--index", type=int, default=0, help="Legacy brief recommendation index")
    parser.add_argument("--list", action="store_true", help="Generate improved opportunity report")
    parser.add_argument("--report-out", default="content_opportunities_report.md",
                        help="Output markdown path for improved report mode")
    parser.add_argument("--prompt-spec", default=MAIN_REPORT_PROMPT_DEFAULT,
                        help="Main report prompt directory (system.md + user_template.md) or legacy combined markdown spec")
    parser.add_argument("--advisory-prompt-dir", default=ADVISORY_PROMPT_DEFAULT,
                        help="Advisory prompt directory containing system.md and user_template.md")
    parser.add_argument("--correction-prompt", default=CORRECTION_PROMPT_DEFAULT,
                        help="Correction prompt template file used for LLM revision retries")
    parser.add_argument("--use-llm", action="store_true",
                        help="Use Anthropic API with prompt spec (fails if LLM path is unavailable)")
    parser.add_argument("--config", default="config.yml",
                        help="Path to YAML config (reads analysis_report client context)")
    parser.add_argument("--llm-model", default=MAIN_REPORT_DEFAULT_MODEL)
    parser.add_argument("--llm-max-tokens", type=int, default=16000)
    parser.add_argument("--allow-unverified-report", action="store_true",
                        help="Write the LLM report even if evidence validation flags unsupported claims")
    parser.add_argument("--advisory-briefing", action="store_true",
                        help="Run a second LLM pass to produce a strategic advisory briefing")
    parser.add_argument("--advisory-out", default="advisory_briefing.md",
                        help="Output path for the advisory briefing")
    parser.add_argument("--advisory-model", default=ADVISORY_DEFAULT_MODEL,
                        help="Model for advisory pass (defaults to --llm-model)")
    args = parser.parse_args()

    data = load_data(args.json)

    if args.list:
        list_recommendations(data, args)
        return

    if not args.out:
        print("Error: --out is required unless --list is used.")
        sys.exit(1)

    brief_content = generate_brief(data, args.index)
    try:
        with open(args.out, "w", encoding="utf-8") as f:
            f.write(brief_content)
        print(f"Content Brief generated: {args.out}")
    except Exception as e:
        print(f"Error writing brief: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
