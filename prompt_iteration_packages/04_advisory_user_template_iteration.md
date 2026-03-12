# Prompt Iteration Package: Advisory User Template

## Objective

Rewrite the advisory user template so the second-pass advisory receives the right inputs in the right order, with less ambiguity about what is evidence versus what is context.

## Current Prompt File

- [`prompts/advisory/user_template.md`](/Users/davemini2/ProjectsLocal/serp/prompts/advisory/user_template.md)

## Current Prompt Text

```md
## Task

Produce the advisory briefing specified in your system instructions.
Read the inputs below in this order:

1. Strategic flags — these are binding constraints, not suggestions.
   They determine which action is first, which keywords are skip,
   and whether defense takes priority over expansion.
2. Client context — background for assessing fit and framing
   recommendations in the client's language.
3. Market intelligence report — the verified analysis that your
   briefing interprets. Do not repeat it; reference findings by
   keyword name.

## Strategic Flags (binding constraints)

These were computed deterministically from the SERP data. They are
not hypotheses. Use them as given.

- If defensive_urgency = "high", Action 1 must defend the
  declining position.
- content_priorities ordering determines your action sequence.
- Keywords with action = "skip" appear only in "What to Stop
  Thinking About."
- total_results values are indexed page counts, not monthly
  search volume.

<strategic_flags>
{strategic_flags_json}
</strategic_flags>

## Client Context (background — not evidence)

Organization: {client_name}
Website: {client_domain}
Type: {org_type}
Location: {location}
Framework: {framework_description}
Content focus: {content_focus}
Constraints: {additional_context}

Use this to assess whether the client can realistically execute
each recommendation and to frame actions in language that connects
to their work. Do not treat client context as SERP evidence.

## Market Intelligence Report (verified analysis)

This is the first-pass report produced from pre-verified SERP data.
Every number in it has been checked against the raw data. Use it
as your analytical foundation but do not repeat its contents —
the reader has already seen it.

<market_report>
{market_report_text}
</market_report>

Produce the advisory briefing now. Follow the four-part structure
(data → why it matters → what to do → consequence of inaction)
for each of exactly 3 actions.
```

## Shared Evidence Bundle

Use these files with this package if your chatbot tool supports multiple attachments:

- [`extracted_payload_summary_estrangement_20260311_1733.json`](/Users/davemini2/ProjectsLocal/serp/prompt_iteration_packages/examples/extracted_payload_summary_estrangement_20260311_1733.json)
- [`extracted_payload_estrangement_20260311_1733.json`](/Users/davemini2/ProjectsLocal/serp/prompt_iteration_packages/examples/extracted_payload_estrangement_20260311_1733.json)
- [`failed_main_report_draft_20260311_1737.md`](/Users/davemini2/ProjectsLocal/serp/prompt_iteration_packages/examples/failed_main_report_draft_20260311_1737.md)
- [`reference_main_report_20260311_1554.md`](/Users/davemini2/ProjectsLocal/serp/prompt_iteration_packages/examples/reference_main_report_20260311_1554.md)
- [`reference_advisory_20260311_1554.md`](/Users/davemini2/ProjectsLocal/serp/prompt_iteration_packages/examples/reference_advisory_20260311_1554.md)

Use the summary JSON first. Only inspect the full extracted payload if you need field-level detail.


## Representative Data Summary

Use this fixed summary snapshot while iterating the prompt so prompt changes are evaluated against stable evidence.

```json
{
  "run_id": "20260311_173328",
  "root_keywords": [
    "estrangement",
    "estrangement from adult children",
    "estrangement grief",
    "family cutoff counselling Vancouver",
    "reunification counselling BC",
    "reunification therapy near me"
  ],
  "query_count": 6,
  "aio_total_citations": 88,
  "client_position_summary": {
    "total_organic_appearances": 1,
    "total_aio_citations": 0,
    "total_aio_text_mentions": 0,
    "total_local_pack": 0,
    "keywords_with_any_visibility": [
      "family cutoff counselling Vancouver"
    ],
    "keywords_with_zero_visibility": [
      "estrangement",
      "estrangement from adult children",
      "estrangement grief",
      "reunification counselling BC",
      "reunification therapy near me"
    ],
    "has_declining_positions": true,
    "worst_delta": -3
  },
  "strategic_flags": {
    "defensive_urgency": "high",
    "defensive_detail": "Client's content 'Can cutting off family be good therapy?' dropped 3 positions to rank #7 for 'family cutoff counselling Vancouver'. This page provides 0 of the client's AIO citations. If organic rank continues declining, AIO citation loss is probable.",
    "visibility_concentration": "critical",
    "concentration_detail": "Client visible for 1 of 6 tracked keywords ('family cutoff counselling Vancouver'). 100% of organic and AIO visibility depends on a single keyword cluster.",
    "opportunity_scale": {
      "estrangement": {
        "total_results": 231000,
        "client_rank": null,
        "client_trend": null,
        "action": "enter_cautiously",
        "reason": "Legal entities dominate this SERP. Entry requires differentiated content that avoids competing on legal topics directly."
      },
      "estrangement from adult children": {
        "total_results": 16400,
        "client_rank": null,
        "client_trend": null,
        "action": "enter",
        "reason": "16,400 total results. Client has no current visibility. Dominant entity type: counselling."
      },
      "estrangement grief": {
        "total_results": 31700,
        "client_rank": null,
        "client_trend": null,
        "action": "enter",
        "reason": "31,700 total results. Client has no current visibility. Dominant entity type: counselling."
      },
      "family cutoff counselling Vancouver": {
        "total_results": 686000,
        "client_rank": 7,
        "client_trend": "declining",
        "action": "defend",
        "reason": "Client ranks #7, declined 3 positions. Protect existing visibility before expanding elsewhere."
      },
      "reunification counselling BC": {
        "total_results": 91,
        "client_rank": null,
        "client_trend": null,
        "action": "skip",
        "reason": "Only 91 total results. Market too small to justify dedicated content investment."
      },
      "reunification therapy near me": {
        "total_results": 15300,
        "client_rank": null,
        "client_trend": null,
        "action": "enter",
        "reason": "15,300 total results. Client has no current visibility. Dominant entity type: counselling."
      }
    },
    "content_priorities": [
      {
        "action": "defend",
        "keyword": "family cutoff counselling Vancouver",
        "total_results": 686000,
        "reason": "Client ranks #7, declined 3 positions. Protect existing visibility before expanding elsewhere."
      },
      {
        "action": "enter",
        "keyword": "estrangement grief",
        "total_results": 31700,
        "reason": "31,700 total results. Client has no current visibility. Dominant entity type: counselling."
      },
      {
        "action": "enter",
        "keyword": "estrangement from adult children",
        "total_results": 16400,
        "reason": "16,400 total results. Client has no current visibility. Dominant entity type: counselling."
      },
      {
        "action": "enter",
        "keyword": "reunification therapy near me",
        "total_results": 15300,
        "reason": "15,300 total results. Client has no current visibility. Dominant entity type: counselling."
      },
      {
        "action": "enter_cautiously",
        "keyword": "estrangement",
        "total_results": 231000,
        "reason": "Legal entities dominate this SERP. Entry requires differentiated content that avoids competing on legal topics directly."
      },
      {
        "action": "skip",
        "keyword": "reunification counselling BC",
        "total_results": 91,
        "reason": "Only 91 total results. Market too small to justify dedicated content investment."
      }
    ],
    "top_cross_cluster_paa": {
      "question": "How do I reach out to an estranged adult child?",
      "cluster_count": 2,
      "combined_total_results": 247400
    }
  },
  "paa_cross_cluster": [
    {
      "question": "How do I reach out to an estranged adult child?",
      "source_keywords": [
        "estrangement",
        "estrangement from adult children"
      ],
      "cluster_count": 2,
      "combined_total_results": 247400,
      "category": "General"
    },
    {
      "question": "When should you stop reaching out to an estranged child?",
      "source_keywords": [
        "estrangement",
        "estrangement from adult children"
      ],
      "cluster_count": 2,
      "combined_total_results": 247400,
      "category": "General"
    },
    {
      "question": "What should I expect in reunification therapy?",
      "source_keywords": [
        "reunification counselling BC",
        "reunification therapy near me"
      ],
      "cluster_count": 2,
      "combined_total_results": 15391,
      "category": "General"
    }
  ],
  "keyword_profiles_excerpt": {
    "estrangement": {
      "total_results": 231000,
      "entity_distribution": {
        "counselling": 5,
        "legal": 12,
        "nonprofit": 2,
        "directory": 1,
        "professional_association": 1,
        "government": 2,
        "media": 1
      },
      "dominant_entity_type": null,
      "has_ai_overview": true,
      "has_local_pack": false,
      "paa_questions": [
        "How do I reach out to an estranged adult child?",
        "How long does the average family estrangement last?",
        "When should you stop reaching out to an estranged child?",
        "When to go no-contact with a family member?"
      ],
      "autocomplete": [],
      "top5_organic": [
        {
          "rank": 1,
          "title": "Family Estrangement Therapy in Victoria, Vancouver & Kelowna",
          "source": "estrangedfamilytherapy.com",
          "entity_type": "counselling",
          "content_type": "other"
        },
        {
          "rank": 1,
          "title": "Laws on Parental Alienation in BC",
          "source": "Pathway Legal Family Lawyers",
          "entity_type": "legal",
          "content_type": "guide"
        },
        {
          "rank": 2,
          "title": "Family Estrangement Counselling for Parents & Children",
          "source": "Restored Hope Counselling Services",
          "entity_type": "counselling",
          "content_type": "other"
        },
        {
          "rank": 2,
          "title": "British Columbia \u2013 CCMF",
          "source": "Men and Families",
          "entity_type": "nonprofit",
          "content_type": "other"
        },
        {
          "rank": 2,
          "title": "Court allows disinheritance of estranged children",
          "source": "Clark Wilson LLP",
          "entity_type": "legal",
          "content_type": "news"
        }
      ]
    },
    "estrangement from adult children": {
      "total_results": 16400,
      "entity_distribution": {
        "counselling": 6,
        "legal": 4,
        "directory": 3,
        "media": 6,
        "education": 1
      },
      "dominant_entity_type": null,
      "has_ai_overview": true,
      "has_local_pack": false,
      "paa_questions": [
        "How do I reach out to an estranged adult child?",
        "How long does parent-child estrangement usually last?",
        "Should you leave inheritance to an estranged child?",
        "When should you stop reaching out to an estranged child?"
      ],
      "autocomplete": [],
      "top5_organic": [
        {
          "rank": 1,
          "title": "Family Estrangement Therapy in Victoria, Vancouver & Kelowna",
          "source": "estrangedfamilytherapy.com",
          "entity_type": "counselling",
          "content_type": "other"
        },
        {
          "rank": 1,
          "title": "Family estrangements rise in Canada due to social, cultural ...",
          "source": "Canadian Affairs",
          "entity_type": "media",
          "content_type": "news"
        },
        {
          "rank": 1,
          "title": "Those of you who are estranged from your children, what ...",
          "source": "Reddit \u00b7 r/AskOldPeople",
          "entity_type": "media",
          "content_type": "other"
        },
        {
          "rank": 2,
          "title": "Family Estrangement Counselling for Parents & Children",
          "source": "Restored Hope Counselling Services",
          "entity_type": "counselling",
          "content_type": "other"
        },
        {
          "rank": 2,
          "title": "Tina Gilbertson, LPC",
          "source": "LinkedIn \u00b7 Tina Gilbertson",
          "entity_type": "N/A",
          "content_type": "other"
        }
      ]
    },
    "estrangement grief": {
      "total_results": 31700,
      "entity_distribution": {
        "nonprofit": 3,
        "government": 2,
        "directory": 4,
        "counselling": 7,
        "media": 1
      },
      "dominant_entity_type": null,
      "has_ai_overview": true,
      "has_local_pack": false,
      "paa_questions": [],
      "autocomplete": [],
      "top5_organic": [
        {
          "rank": 1,
          "title": "British Columbia Bereavement Helpline - Homepage New",
          "source": "British Columbia Bereavement Helpline",
          "entity_type": "nonprofit",
          "content_type": "news"
        },
        {
          "rank": 1,
          "title": "Grieving in the Age of Estrangement and Division",
          "source": "The Tyee",
          "entity_type": "N/A",
          "content_type": "other"
        },
        {
          "rank": 2,
          "title": "After a Death: Get Support When Someone Dies - Gov.bc.ca",
          "source": "gov.bc.ca",
          "entity_type": "government",
          "content_type": "guide"
        },
        {
          "rank": 2,
          "title": "Death of estranged parent : r/legaladvicecanada",
          "source": "Reddit \u00b7 r/legaladvicecanada",
          "entity_type": "media",
          "content_type": "other"
        },
        {
          "rank": 3,
          "title": "Professional Grief Counselling - Vancouver - Pathways",
          "source": "Pathways BC",
          "entity_type": "government",
          "content_type": "other"
        }
      ]
    },
    "family cutoff counselling Vancouver": {
      "total_results": 686000,
      "entity_distribution": {
        "counselling": 17,
        "directory": 3,
        "nonprofit": 2,
        "government": 2
      },
      "dominant_entity_type": null,
      "has_ai_overview": true,
      "has_local_pack": true,
      "paa_questions": [],
      "autocomplete": [],
      "top5_organic": [
        {
          "rank": 1,
          "title": "Reduced-Cost Counselling Options in Vancouver January ...",
          "source": "Willow Tree Counselling",
          "entity_type": "counselling",
          "content_type": "pdf"
        },
        {
          "rank": 1,
          "title": "Family & Parent Counselling Therapy in Vancouver, BC",
          "source": "wellspringcounselling.ca",
          "entity_type": "counselling",
          "content_type": "guide"
        },
        {
          "rank": 2,
          "title": "Family Counselling Vancouver",
          "source": "Blue Sky Wellness Clinic",
          "entity_type": "counselling",
          "content_type": "other"
        },
        {
          "rank": 2,
          "title": "Trauma Counselling",
          "source": "Family Services of Greater Vancouver",
          "entity_type": "government",
          "content_type": "other"
        },
        {
          "rank": 2,
          "title": "North Vancouver Counselling - Boomerang Centre",
          "source": "Boomerang Counselling Centre",
          "entity_type": "counselling",
          "content_type": "other"
        }
      ]
    },
    "reunification counselling BC": {
      "total_results": 91,
      "entity_distribution": {
        "counselling": 11,
        "nonprofit": 3,
        "legal": 1,
        "government": 6,
        "professional_association": 1,
        "directory": 1
      },
      "dominant_entity_type": null,
      "has_ai_overview": true,
      "has_local_pack": false,
      "paa_questions": [
        "How to prove parental alienation in BC?",
        "What is the best therapy for parental alienation?",
        "What should I expect in reunification therapy?",
        "Who qualifies for the family reunification program?"
      ],
      "autocomplete": [],
      "top5_organic": [
        {
          "rank": 1,
          "title": "Reunification Counselling Port Moody BC",
          "source": "Bright Star Counselling",
          "entity_type": "counselling",
          "content_type": "other"
        },
        {
          "rank": 1,
          "title": "REACH Reunification Program",
          "source": "Susan Gamache",
          "entity_type": "counselling",
          "content_type": "other"
        },
        {
          "rank": 2,
          "title": "Vancouver Family Preservation & Reunification Services",
          "source": "Westcoast Family Centres",
          "entity_type": "nonprofit",
          "content_type": "guide"
        },
        {
          "rank": 2,
          "title": "Individual & Family Counselling",
          "source": "Success BC",
          "entity_type": "nonprofit",
          "content_type": "other"
        },
        {
          "rank": 2,
          "title": "REACH REUNIFICATION PROGRAM - Updated March 2026",
          "source": "Yelp",
          "entity_type": "directory",
          "content_type": "other"
        }
      ]
    },
    "reunification therapy near me": {
      "total_results": 15300,
      "entity_distribution": {
        "legal": 2,
        "nonprofit": 2,
        "counselling": 10,
        "government": 4,
        "professional_association": 1,
        "directory": 2
      },
      "dominant_entity_type": null,
      "has_ai_overview": true,
      "has_local_pack": true,
      "paa_questions": [
        "How to get therapy covered in BC?",
        "What is a family reunification therapist?",
        "What is the most common goal of reunification family therapy?",
        "What should I expect in reunification therapy?"
      ],
      "autocomplete": [],
      "top5_organic": [
        {
          "rank": 1,
          "title": "Parent and Child Reunification Program | We ... - Vancouver",
          "source": "Reconnect Families",
          "entity_type": "legal",
          "content_type": "other"
        },
        {
          "rank": 1,
          "title": "Family Preservation and Reunification Counselling Services",
          "source": "HelpStartsHere",
          "entity_type": "government",
          "content_type": "other"
        },
        {
          "rank": 2,
          "title": "Vancouver Family Preservation & Reunification Services",
          "source": "Westcoast Family Centres",
          "entity_type": "nonprofit",
          "content_type": "guide"
        },
        {
          "rank": 2,
          "title": "Family Preservation & Reunification",
          "source": "Hollyburn Family Services",
          "entity_type": "counselling",
          "content_type": "other"
        },
        {
          "rank": 2,
          "title": "Family & Couples Therapy | Langley, BC",
          "source": "Dr. Ellie Bolgar",
          "entity_type": "legal",
          "content_type": "other"
        }
      ]
    }
  },
  "tool_recommendations_verified": [
    {
      "pattern_name": "The Medical Model Trap",
      "trigger_words_searched_for": [
        "clinical",
        "registered",
        "diagnosis",
        "disorder",
        "mental health",
        "patient",
        "treatment"
      ],
      "triggers_found": {
        "in_paa_questions": {},
        "in_organic_titles": {
          "clinical": 1,
          "registered": 1,
          "mental health": 1
        },
        "in_organic_snippets": {
          "clinical": 10,
          "registered": 8,
          "disorder": 1,
          "mental health": 2,
          "treatment": 3
        },
        "in_aio_text": {
          "mental health": 3
        },
        "in_autocomplete": {},
        "in_related_searches": {
          "mental health": 2
        }
      },
      "content_angle": "Why turning family estrangement into a diagnosis keeps you stuck.",
      "status_quo_message": "You are sick/broken and need an expert to fix you (External Locus of Control).",
      "reframe": "Shift from pathology to functioning. You don't need a diagnosis; you need a map of your emotional system.",
      "verdict_inputs": {
        "any_paa_evidence": false,
        "any_autocomplete_evidence": false,
        "total_trigger_occurrences": 32,
        "primary_evidence_source": "in_organic_snippets"
      }
    },
    {
      "pattern_name": "The Fusion Trap",
      "trigger_words_searched_for": [
        "connection",
        "bond",
        "close",
        "intimacy",
        "communication",
        "reconnect",
        "reach out"
      ],
      "triggers_found": {
        "in_paa_questions": {
          "reach out": 2
        },
        "in_organic_titles": {
          "reconnect": 3
        },
        "in_organic_snippets": {
          "connection": 4,
          "communication": 6,
          "reconnect": 1,
          "reach out": 1
        },
        "in_aio_text": {
          "connection": 1,
          "communication": 2,
          "reconnect": 5,
          "reach out": 1
        },
        "in_autocomplete": {},
        "in_related_searches": {
          "reconnect": 1
        }
      },
      "content_angle": "Why trying to force reconnection may deepen the cutoff.",
      "status_quo_message": "The goal is to force closeness, agreement, or reconnection as quickly as possible.",
      "reframe": "Sustainable contact requires differentiation. Anxiety-driven pursuit often increases reactivity and deepens cutoff.",
      "verdict_inputs": {
        "any_paa_evidence": true,
        "any_autocomplete_evidence": false,
        "total_trigger_occurrences": 27,
        "primary_evidence_source": "in_organic_snippets"
      }
    },
    {
      "pattern_name": "The Resource Trap",
      "trigger_words_searched_for": [
        "free",
        "low cost",
        "sliding scale",
        "cheap",
        "affordable",
        "covered",
        "insurance"
      ],
      "triggers_found": {
        "in_paa_questions": {
          "covered": 1
        },
        "in_organic_titles": {},
        "in_organic_snippets": {
          "free": 4,
          "low cost": 1,
          "affordable": 2
        },
        "in_aio_text": {
          "free": 9,
          "affordable": 1
        },
        "in_autocomplete": {
          "free": 1
        },
        "in_related_searches": {
          "free": 6
        }
      },
      "content_angle": "When short-term relief becomes a substitute for working the family pattern.",
      "status_quo_message": "High anxiety about resources/access. Seeking immediate symptom relief (venting).",
      "reframe": "Address the anxiety driving the search. Cheap relief often delays real structural change.",
      "verdict_inputs": {
        "any_paa_evidence": true,
        "any_autocomplete_evidence": true,
        "total_trigger_occurrences": 25,
        "primary_evidence_source": "in_aio_text"
      }
    },
    {
      "pattern_name": "The Blame/Reactivity Trap",
      "trigger_words_searched_for": [
        "narcissist",
        "toxic",
        "abusive",
        "mean",
        "angry",
        "hate",
        "deal with"
      ],
      "triggers_found": {
        "in_paa_questions": {},
        "in_organic_titles": {},
        "in_organic_snippets": {},
        "in_aio_text": {},
        "in_autocomplete": {},
        "in_related_searches": {}
      },
      "content_angle": "Stop diagnosing the other person and start observing your own reactivity.",
      "status_quo_message": "The problem is the other person (The Identified Patient).",
      "reframe": "Focus on self-regulation. You cannot change them, only your response to them.",
      "verdict_inputs": {
        "any_paa_evidence": false,
        "any_autocomplete_evidence": false,
        "total_trigger_occurrences": 0,
        "primary_evidence_source": "none"
      }
    }
  ]
}
```


## Reference Examples

Use these as the working example pair:
- first-pass report: [`reference_main_report_20260311_1554.md`](/Users/davemini2/ProjectsLocal/serp/prompt_iteration_packages/examples/reference_main_report_20260311_1554.md)
- advisory briefing: [`reference_advisory_20260311_1554.md`](/Users/davemini2/ProjectsLocal/serp/prompt_iteration_packages/examples/reference_advisory_20260311_1554.md)

## Template Constraints

Keep the Python `str.format(...)` placeholders compatible with [`generate_content_brief.py`](/Users/davemini2/ProjectsLocal/serp/generate_content_brief.py):
- `{client_name}`
- `{client_domain}`
- `{org_type}`
- `{location}`
- `{framework_description}`
- `{content_focus}`
- `{additional_context}`
- `{strategic_flags_json}`
- `{market_report_text}`

## Problems To Solve

1. The template is currently very plain and gives the model little guidance about how to combine strategic flags with the first-pass report.
2. It does not clearly distinguish between hard constraints and contextual framing.
3. It does not explicitly tell the model that strategic flags should override interesting but lower-priority ideas from the report.

## Instructions For The AI Tool

1. Rewrite the advisory user template so the model sees a clear order of operations:
   - client context
   - strategic flags as primary constraints
   - verified first-pass report as supporting analysis
2. Make the role of strategic flags explicit: they are not suggestions, they are decision constraints.
3. Keep the template concise and compatible with the existing code.
4. Do not duplicate the advisory system prompt inside the user template.

## Required Output From The AI Tool

Return:
1. a revised `user_template.md`
2. a short rationale for the new structure
3. any placeholder changes, if unavoidable, called out explicitly
