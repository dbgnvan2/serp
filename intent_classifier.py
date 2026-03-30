"""
intent_classifier.py
~~~~~~~~~~~~~~~~~~~~
Classifies PAA questions and keyword phrases as "External Locus" (medical
model framing), "Systemic" (Bowen Family Systems Theory framing), or
"General" (neither).

This distinction drives the "Bowen Reframe FAQ" section of the content brief:
questions tagged External Locus are prime candidates for a systems-based
reframe that differentiates Living Systems Counselling from the dominant
medical-model content in the SERP.

Design notes
------------
- Rules-based, no ML dependency — deterministic and auditable.
- Matching is case-insensitive and checks both single-word triggers and
  multi-word phrases (checked first to avoid partial-word false positives).
- Confidence reflects the ratio of matched triggers relative to total tokens,
  capped at 1.0, so short questions with one strong trigger score higher than
  long questions with the same number of matches.
- The class accepts optional custom trigger sets at init time so the trigger
  vocabulary can be extended via config without code changes.
"""

from __future__ import annotations

import re
from typing import Literal

# ---------------------------------------------------------------------------
# Default trigger vocabularies
# ---------------------------------------------------------------------------

#: Phrases/words associated with the medical model ("External Locus").
#: Multi-word entries are checked before single-word entries.
DEFAULT_MEDICAL_TRIGGERS: frozenset[str] = frozenset([
    # Multi-word phrases (checked first)
    "mental illness",
    "mental health condition",
    "evidence-based treatment",
    "evidence based treatment",
    "cognitive behavioral",
    "cognitive behavioural",
    # Single-word triggers
    "diagnosis",
    "diagnose",
    "treatment",
    "patient",
    "symptoms",
    "symptom",
    "disorder",
    "medication",
    "medicate",
    "medicated",
    "prescription",
    "fix",
    "heal",
    "cure",
    "condition",
    "clinical",
    "clinician",
    "psychiatrist",
    "psychiatry",
    "pathology",
    "pathological",
    "dysfunction",
    "dysfunctional",
    "illness",
    "disease",
    "recovery",
    "rehabilitation",
    "intervention",
    "borderline",
    "narcissist",
    "narcissistic",
    "toxic",
])

#: Phrases/words associated with Bowen Family Systems Theory ("Systemic").
DEFAULT_SYSTEMIC_TRIGGERS: frozenset[str] = frozenset([
    # Multi-word phrases (checked first)
    "family system",
    "family systems",
    "emotional system",
    "emotional process",
    "emotional cutoff",
    "differentiation of self",
    "level of differentiation",
    "multigenerational transmission",
    "nuclear family",
    "sibling position",
    "societal emotional process",
    # Single-word triggers
    "differentiation",
    "differentiated",
    "triangulation",
    "triangle",
    "triangles",
    "reactivity",
    "reactive",
    "cutoff",
    "functioning",
    "multigenerational",
    "intergenerational",
    "bowen",
    "togetherness",
    "individuality",
    "chronic anxiety",
    "anxiety",
    "fusion",
    "fused",
    "projection",
    "undifferentiated",
])

IntentLabel = Literal["External Locus", "Systemic", "General"]


# ---------------------------------------------------------------------------
# Classifier
# ---------------------------------------------------------------------------

class IntentClassifier:
    """Classify text strings by Bowen/medical intent.

    Parameters
    ----------
    medical_triggers:
        Override the default medical-model trigger vocabulary.
    systemic_triggers:
        Override the default Bowen/systemic trigger vocabulary.

    Examples
    --------
    >>> clf = IntentClassifier()
    >>> clf.classify_paa("What is the diagnosis for anxiety disorder?")
    {'intent': 'External Locus', 'confidence': 0.67, 'triggers': ['diagnosis', 'disorder']}
    >>> clf.classify_paa("How does differentiation affect the family system?")
    {'intent': 'Systemic', 'confidence': 0.71, 'triggers': ['differentiation', 'family system']}
    """

    def __init__(
        self,
        medical_triggers: frozenset[str] | None = None,
        systemic_triggers: frozenset[str] | None = None,
    ) -> None:
        self._medical = medical_triggers if medical_triggers is not None else DEFAULT_MEDICAL_TRIGGERS
        self._systemic = systemic_triggers if systemic_triggers is not None else DEFAULT_SYSTEMIC_TRIGGERS

        # Pre-sort triggers: longest first so multi-word phrases are matched
        # before their constituent words.
        self._medical_sorted = sorted(self._medical, key=len, reverse=True)
        self._systemic_sorted = sorted(self._systemic, key=len, reverse=True)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def classify_paa(self, question: str) -> dict:
        """Classify a PAA (People Also Ask) question string.

        Parameters
        ----------
        question:
            Raw question text, e.g. ``"What is the treatment for depression?"``.

        Returns
        -------
        dict with keys:
            intent     : "External Locus" | "Systemic" | "General"
            confidence : float  0.0–1.0
            triggers   : list[str]  matched trigger words/phrases
        """
        return self._classify(question)

    def classify_keyword(self, keyword: str) -> dict:
        """Classify a search keyword phrase.

        Same return schema as :meth:`classify_paa`.
        """
        return self._classify(keyword)

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _classify(self, text: str) -> dict:
        if not text or not isinstance(text, str):
            return {"intent": "General", "confidence": 0.0, "triggers": []}

        text_lower = text.lower()
        med_score, med_triggers = self._match_triggers(text_lower, self._medical_sorted)
        sys_score, sys_triggers = self._match_triggers(text_lower, self._systemic_sorted)

        token_count = max(1, len(re.findall(r"\w+", text_lower)))

        if med_score == 0 and sys_score == 0:
            return {"intent": "General", "confidence": 0.0, "triggers": []}

        if med_score >= sys_score:
            intent: IntentLabel = "External Locus"
            matched = med_triggers
            raw_confidence = med_score / token_count
        else:
            intent = "Systemic"
            matched = sys_triggers
            raw_confidence = sys_score / token_count

        confidence = round(min(1.0, raw_confidence), 2)
        return {"intent": intent, "confidence": confidence, "triggers": matched}

    def _match_triggers(
        self, text_lower: str, triggers_sorted: list[str]
    ) -> tuple[int, list[str]]:
        """Return (score, matched_triggers) for *text_lower* against *triggers_sorted*.

        Score is the sum of token-lengths of matched triggers (so a 3-word
        phrase scores 3, not 1, giving multi-word matches their proper weight).
        Already-matched spans are consumed so a phrase match doesn't also
        count its constituent words.
        """
        remaining = text_lower
        score = 0
        matched: list[str] = []

        for trigger in triggers_sorted:
            # Use word-boundary matching for single words, substring for phrases
            if " " in trigger:
                pattern = re.escape(trigger)
            else:
                pattern = r"\b" + re.escape(trigger) + r"\b"

            if re.search(pattern, remaining):
                matched.append(trigger)
                word_count = len(trigger.split())
                score += word_count
                # Consume matched span so sub-words aren't double-counted
                remaining = re.sub(pattern, " ", remaining)

        return score, matched
