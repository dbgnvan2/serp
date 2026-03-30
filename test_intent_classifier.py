"""
test_intent_classifier.py
~~~~~~~~~~~~~~~~~~~~~~~~~
Tests for IntentClassifier — PAA / keyword intent tagging.
"""

import unittest
from intent_classifier import IntentClassifier


class TestIntentClassifierPAA(unittest.TestCase):

    def setUp(self):
        self.clf = IntentClassifier()

    # --- Medical model triggers (External Locus) ---

    def test_diagnosis_trigger(self):
        result = self.clf.classify_paa("What is the diagnosis for anxiety disorder?")
        self.assertEqual(result["intent"], "External Locus")

    def test_treatment_trigger(self):
        result = self.clf.classify_paa("What is the best treatment for depression?")
        self.assertEqual(result["intent"], "External Locus")

    def test_medication_trigger(self):
        result = self.clf.classify_paa("Should I take medication for my symptoms?")
        self.assertEqual(result["intent"], "External Locus")

    def test_toxic_trigger(self):
        result = self.clf.classify_paa("How do I deal with a toxic family member?")
        self.assertEqual(result["intent"], "External Locus")

    def test_narcissist_trigger(self):
        result = self.clf.classify_paa("Is my partner a narcissist?")
        self.assertEqual(result["intent"], "External Locus")

    def test_cure_trigger(self):
        result = self.clf.classify_paa("Can therapy cure relationship problems?")
        self.assertEqual(result["intent"], "External Locus")

    # --- Bowen / systemic triggers ---

    def test_differentiation_trigger(self):
        result = self.clf.classify_paa("How does differentiation affect relationships?")
        self.assertEqual(result["intent"], "Systemic")

    def test_family_system_phrase(self):
        result = self.clf.classify_paa("How does the family system create anxiety?")
        self.assertEqual(result["intent"], "Systemic")

    def test_triangulation_trigger(self):
        result = self.clf.classify_paa("What is triangulation in families?")
        self.assertEqual(result["intent"], "Systemic")

    def test_emotional_cutoff_phrase(self):
        result = self.clf.classify_paa("What causes emotional cutoff between parents and children?")
        self.assertEqual(result["intent"], "Systemic")

    def test_multigenerational_trigger(self):
        result = self.clf.classify_paa("How are multigenerational patterns passed down?")
        self.assertEqual(result["intent"], "Systemic")

    def test_reactivity_trigger(self):
        result = self.clf.classify_paa("Why does emotional reactivity increase in conflict?")
        self.assertEqual(result["intent"], "Systemic")

    def test_bowen_trigger(self):
        result = self.clf.classify_paa("What is Bowen theory?")
        self.assertEqual(result["intent"], "Systemic")

    # --- General (no triggers) ---

    def test_general_no_triggers(self):
        result = self.clf.classify_paa("How do I find a good counsellor near me?")
        self.assertEqual(result["intent"], "General")
        self.assertEqual(result["confidence"], 0.0)
        self.assertEqual(result["triggers"], [])

    def test_general_empty_string(self):
        result = self.clf.classify_paa("")
        self.assertEqual(result["intent"], "General")

    def test_general_none_input(self):
        result = self.clf.classify_paa(None)
        self.assertEqual(result["intent"], "General")

    def test_general_whitespace_only(self):
        result = self.clf.classify_paa("   ")
        self.assertEqual(result["intent"], "General")

    # --- Case insensitivity ---

    def test_uppercase_trigger_matched(self):
        result = self.clf.classify_paa("DIAGNOSIS of anxiety")
        self.assertEqual(result["intent"], "External Locus")

    def test_mixed_case_trigger_matched(self):
        result = self.clf.classify_paa("What is Differentiation of Self?")
        self.assertEqual(result["intent"], "Systemic")

    # --- Confidence ---

    def test_confidence_is_between_0_and_1(self):
        for text in [
            "diagnosis treatment disorder",
            "How does differentiation affect the family system?",
            "general question with no triggers",
        ]:
            result = self.clf.classify_paa(text)
            self.assertGreaterEqual(result["confidence"], 0.0)
            self.assertLessEqual(result["confidence"], 1.0)

    def test_strong_single_trigger_has_nonzero_confidence(self):
        result = self.clf.classify_paa("diagnosis")
        self.assertGreater(result["confidence"], 0.0)

    # --- Triggers list ---

    def test_triggers_list_contains_matched_words(self):
        result = self.clf.classify_paa("diagnosis and treatment")
        self.assertIn("diagnosis", result["triggers"])
        self.assertIn("treatment", result["triggers"])

    def test_multi_word_phrase_in_triggers_not_split(self):
        """'family system' should appear as one entry, not 'family' and 'system' separately."""
        result = self.clf.classify_paa("The family system drives anxiety patterns")
        self.assertIn("family system", result["triggers"])
        # 'system' should NOT appear separately since the phrase consumed it
        self.assertNotIn("system", result["triggers"])

    # --- Mixed signals: higher score wins ---

    def test_mixed_more_medical_wins(self):
        result = self.clf.classify_paa(
            "diagnosis disorder treatment dysfunction — also some differentiation"
        )
        self.assertEqual(result["intent"], "External Locus")

    def test_mixed_more_systemic_wins(self):
        result = self.clf.classify_paa(
            "differentiation triangulation family system reactivity — though symptoms exist"
        )
        self.assertEqual(result["intent"], "Systemic")


class TestIntentClassifierKeyword(unittest.TestCase):
    """classify_keyword() uses the same engine; just verify the interface."""

    def setUp(self):
        self.clf = IntentClassifier()

    def test_medical_keyword(self):
        result = self.clf.classify_keyword("anxiety disorder treatment")
        self.assertEqual(result["intent"], "External Locus")

    def test_systemic_keyword(self):
        result = self.clf.classify_keyword("differentiation of self counselling")
        self.assertEqual(result["intent"], "Systemic")

    def test_general_keyword(self):
        result = self.clf.classify_keyword("counselling north vancouver")
        self.assertEqual(result["intent"], "General")


class TestIntentClassifierCustomTriggers(unittest.TestCase):
    """Verify custom trigger sets work correctly."""

    def test_custom_medical_triggers(self):
        clf = IntentClassifier(
            medical_triggers=frozenset(["brainwash"]),
            systemic_triggers=frozenset(["system"]),
        )
        result = clf.classify_paa("They brainwash you")
        self.assertEqual(result["intent"], "External Locus")
        self.assertIn("brainwash", result["triggers"])

    def test_custom_systemic_triggers(self):
        clf = IntentClassifier(
            medical_triggers=frozenset(["fix"]),
            systemic_triggers=frozenset(["pattern"]),
        )
        # Exact word match — "pattern" (singular) must appear in the text
        result = clf.classify_paa("This pattern repeats across generations")
        self.assertEqual(result["intent"], "Systemic")


if __name__ == "__main__":
    unittest.main()
