import unittest
from unittest.mock import patch, MagicMock, mock_open
import serp_audit
import json
import os


class TestSerpAudit(unittest.TestCase):

    def test_get_ngrams_logic(self):
        """Test that n-grams are generated correctly and stop words are removed."""
        text = "The quick brown fox jumps over the lazy dog"
        bigrams = serp_audit.get_ngrams(text, 2)
        self.assertIn("quick brown", bigrams)
        self.assertIn("brown fox", bigrams)
        self.assertNotIn("the quick", bigrams)
        self.assertEqual(len(bigrams), 6)

    def test_get_ngrams_hyphenated(self):
        """Test that hyphenated words are split, not merged."""
        text = "highly-trained expert"
        # Should become "highly trained expert", not "highlytrained expert"
        bigrams = serp_audit.get_ngrams(text, 2)
        self.assertIn("highly trained", bigrams)
        self.assertNotIn("highlytrained", bigrams)

    def test_get_ngrams_empty_input(self):
        """Test handling of empty or non-string input."""
        self.assertEqual(serp_audit.get_ngrams(None, 2), [])
        self.assertEqual(serp_audit.get_ngrams("", 2), [])

    def test_parse_data_structure(self):
        """Test that parse_data extracts the correct fields from a mock API response."""
        mock_keyword = "test keyword"
        mock_metadata = {
            "run_id": "test_run_123",
            "created_at": "2024-01-01T12:00:00",
            "google_url": "https://google.com/search?q=test",
            "params_hash": "dummy_hash_123"
        }
        mock_results = {
            'google': {
                "search_parameters": {"q": "test keyword vancouver"},
                "search_information": {"total_results": 100},
                "organic_results": [
                    {"title": "Rank 1", "link": "http://rank1.com",
                        "snippet": "Snippet 1"},
                    {"title": "Rank 2", "link": "http://rank2.com",
                        "snippet": "Snippet 2"},
                ],
                "related_questions": [
                    {"question": "How much does therapy cost?",
                        "snippet": "It costs money.", "link": "http://paa.com"},
                    {"question": "What is therapy?",
                        "snippet": "Definition.", "link": "http://def.com"}
                ],
                "ads": [
                    {"title": "Ad 1", "description": "Buy now", "position": 1,
                        "link": "http://ad.com", "block_position": "top"}
                ]
            }
        }

        metrics, organic, paa, expansion, competitors, local, citations, modules, rich_features, warnings = serp_audit.parse_data(
            mock_keyword, mock_results, mock_metadata)

        self.assertEqual(metrics["Root_Keyword"], mock_keyword)
        self.assertEqual(metrics["Run_ID"], "test_run_123")
        self.assertEqual(metrics["Params_Hash"], "dummy_hash_123")
        self.assertEqual(metrics["Rank_1_Title"], "Rank 1")
        self.assertEqual(metrics["Rank_3_Title"], "N/A")
        self.assertEqual(len(paa), 2)
        self.assertEqual(paa[0]["Score"], 10)
        self.assertEqual(paa[1]["Score"], 1)
        self.assertEqual(len(competitors), 1)
        self.assertEqual(competitors[0]["Name"], "Ad 1")
        self.assertEqual(competitors[0]["Block_Position"], "top")
        self.assertEqual(len(warnings), 2)

    def test_parsing_warnings(self):
        """Test that parsing warnings are generated for missing fields."""
        mock_metadata = {
            "run_id": "test_run_123",
            "created_at": "2024-01-01T12:00:00",
            "google_url": "N/A",
            "params_hash": "hash"
        }
        mock_results = {
            'google': {
                "knowledge_graph": {},
                "ads": [{}]
            }
        }
        _, _, _, _, _, _, _, _, _, warnings = serp_audit.parse_data(
            "test", mock_results, mock_metadata)
        self.assertGreater(len(warnings), 0)
        self.assertTrue(
            any("Knowledge Graph title not found" in w["Message"] for w in warnings))
        self.assertTrue(
            any("Ad title not found" in w["Message"] for w in warnings))

    def test_parse_data_with_serp_modules_and_rich_features(self):
        """Test extraction of SERP modules and rich features."""
        mock_metadata = {
            "run_id": "test_run_123",
            "created_at": "2024-01-01T12:00:00",
            "google_url": "N/A",
            "params_hash": "hash"
        }
        mock_results = {
            'google': {
                "knowledge_graph": {"title": "Test KG"},
                "inline_videos": [{}],
            }
        }
        _, _, _, _, _, _, _, modules, rich_features, _ = serp_audit.parse_data(
            "test", mock_results, mock_metadata)
        self.assertEqual(len(modules), 2)
        self.assertEqual(modules[0]["Module"], "knowledge_graph")
        self.assertEqual(len(rich_features), 2)
        self.assertEqual(rich_features[0]["Feature"], "Knowledge Panel")

    def test_parse_data_with_local_pack(self):
        """Test extraction of Local Pack from both SERP and Maps results."""
        mock_metadata = {
            "run_id": "test_run_123",
            "created_at": "2024-01-01T12:00:00",
            "google_url": "N/A",
            "params_hash": "hash"
        }
        mock_results = {
            'google': {
                "local_results": {
                    "places": [{"title": "SERP Place", "place_id": "serp_123"}]
                }
            },
            'google_maps': {
                "local_results": [
                    {"title": "Maps Place 1", "place_id": "maps_123"},
                    {"title": "Maps Place 2", "place_id": "maps_456"}
                ]
            }
        }
        _, _, _, _, _, local_pack, _, _, _, _ = serp_audit.parse_data(
            "local keyword", mock_results, mock_metadata)
        self.assertEqual(len(local_pack), 3)
        self.assertEqual(local_pack[0]["Name"], "SERP Place")
        self.assertEqual(local_pack[0]["Source"], "google_serp")
        self.assertEqual(local_pack[1]["Name"], "Maps Place 1")
        self.assertEqual(local_pack[1]["Source"], "google_maps")

    def test_ai_overview_logic(self):
        """Test that AI overview is correctly pulled from supplemental call if needed."""
        mock_metadata = {
            "run_id": "test_run_123",
            "created_at": "2024-01-01T12:00:00",
            "google_url": "N/A",
            "params_hash": "hash"
        }
        # Case 1: Overview in primary results
        mock_results_primary = {'google': {
            "ai_overview": {"snippet": "Primary AI snippet"}}}
        metrics, _, _, _, _, _, _, _, _, _ = serp_audit.parse_data(
            "ai keyword", mock_results_primary, mock_metadata)
        self.assertEqual(metrics["AI_Overview"], "Primary AI snippet")

        # Case 2: Overview from supplemental call
        mock_results_supplemental = {
            'google': {"ai_overview": {}},  # Present but empty
            'google_ai_overview': {"snippet": "Supplemental AI snippet"}
        }
        metrics, _, _, _, _, _, _, _, _, _ = serp_audit.parse_data(
            "ai keyword", mock_results_supplemental, mock_metadata)
        self.assertEqual(metrics["AI_Overview"], "Supplemental AI snippet")

    def test_pasf_extraction(self):
        """Test extraction and labeling of People Also Search For."""
        mock_metadata = {
            "run_id": "test_run_123",
            "created_at": "2024-01-01T12:00:00",
            "google_url": "N/A",
            "params_hash": "hash"
        }
        mock_results = {
            'google': {
                "inline_people_also_search_for": [{"title": "Inline Term", "link": "http://link1"}],
                "people_also_search_for": [{"name": "Box Term", "link": "http://link2"}]
            }
        }
        _, _, _, expansion, _, _, _, _, _, _ = serp_audit.parse_data(
            "key", mock_results, mock_metadata)

        types = [e["Type"] for e in expansion]
        self.assertIn("PASF (Inline)", types)
        self.assertIn("PASF (Box)", types)
        self.assertEqual(expansion[0]["Term"], "Inline Term")

    def test_parse_data_empty_input(self):
        """Test parse_data with completely empty results."""
        mock_metadata = {"run_id": "test", "created_at": "now",
                         "google_url": "url", "params_hash": "hash"}
        metrics, organic, paa, expansion, competitors, local, citations, modules, rich_features, warnings = serp_audit.parse_data("test", {
        }, mock_metadata)
        self.assertEqual(metrics, {})
        self.assertEqual(len(organic), 0)
        self.assertEqual(len(paa), 0)
        self.assertEqual(len(expansion), 0)
        self.assertEqual(len(competitors), 0)
        self.assertEqual(len(local), 0)
        self.assertEqual(len(citations), 0)
        self.assertEqual(len(modules), 0)
        self.assertEqual(len(rich_features), 0)
        self.assertEqual(len(warnings), 0)

    @patch('serp_audit.save_raw_json')
    @patch('serp_audit._fetch_serp_api')
    def test_fetch_serp_data_orchestration(self, mock_fetch, mock_save):
        """Test that fetch_serp_data correctly triggers secondary calls."""
        mock_keyword = "local services"
        mock_run_id = "test_run_123"

        # Mock primary response to trigger both secondary calls
        mock_primary_response = {
            "ai_overview": {
                "page_token": "some_token"
            },
            "serpapi_search_metadata": {
                "google_maps_url": "https://www.google.com/maps?q=local+services&ll=49.2827,-123.1207"
            },
            "local_results": {"places": []}  # Presence of local pack
        }
        # Mock secondary responses
        mock_ai_response = {"snippet": "Detailed AI answer"}
        mock_maps_response = {"local_results": [{"title": "A Place"}]}

        # Configure the mock to return different values on subsequent calls
        mock_fetch.side_effect = [
            mock_primary_response, mock_ai_response, mock_maps_response]

        results, aio_log, metadata = serp_audit.fetch_serp_data(
            mock_keyword, mock_run_id)

        # 1. Check that fetch was called 3 times
        self.assertEqual(mock_fetch.call_count, 3)

        # 2. Check the engines and params called
        calls = mock_fetch.call_args_list
        self.assertEqual(calls[0][0][0]['engine'], 'google')
        self.assertEqual(calls[1][0][0]['engine'], 'google_ai_overview')
        self.assertIn('page_token', calls[1][0][0])
        self.assertEqual(calls[1][0][0]['page_token'], 'some_token')
        self.assertEqual(calls[2][0][0]['engine'], 'google_maps')
        self.assertIn('ll', calls[2][0][0])
        self.assertEqual(calls[2][0][0]['ll'], '49.2827,-123.1207')
        self.assertNotIn('location', calls[2][0][0])
        self.assertNotIn('z', calls[2][0][0])

        # 3. Check that results from all calls are in the final dictionary
        self.assertIn('google', results)
        self.assertIn('google_ai_overview', results)
        self.assertIn('google_maps', results)
        self.assertEqual(results['google_ai_overview']
                         ['snippet'], "Detailed AI answer")

        # 3b. Check metadata
        self.assertEqual(metadata["run_id"], mock_run_id)

        # 4. Check aio_log
        self.assertTrue(aio_log["has_ai_overview"])
        self.assertEqual(aio_log["ai_overview_mode"], "token_followup")
        self.assertIsNotNone(aio_log["page_token_received_at"])
        self.assertIsNotNone(aio_log["followup_started_at"])
        self.assertIsNotNone(aio_log["followup_latency_ms"])
        self.assertIsNone(aio_log["error"])

    @patch('builtins.open', new_callable=mock_open)
    @patch('os.makedirs')
    def test_save_raw_json(self, mock_makedirs, mock_file):
        """Test that JSON data is written to the correct file."""
        run_id = "test_run"
        engine = "google"
        data = {"key": "value"}

        serp_audit.save_raw_json(run_id, engine, data)

        mock_makedirs.assert_called_with(f"raw/{run_id}", exist_ok=True)
        mock_file.assert_called_with(
            f"raw/{run_id}/{engine}_response.json", 'w')

        # To check the content of the file, we need to get the mock file handle
        handle = mock_file()

        # json.dump may call write multiple times, so we can't use assert_called_once_with
        # Instead, we can get all the calls to write and join them to get the full string
        written_data = "".join(call[0][0]
                               for call in handle.write.call_args_list)

        self.assertEqual(written_data, json.dumps(data, indent=2))

    @patch('serp_audit._fetch_serp_api')
    @patch('builtins.print')
    def test_fetch_serp_data_error_handling(self, mock_print, mock_fetch):
        """Test that fetch_serp_data handles API errors gracefully."""
        mock_fetch.return_value = None  # Simulate a critical failure
        result, aio_log, metadata = serp_audit.fetch_serp_data(
            "fail_keyword", "fail_run")
        self.assertEqual(result, {})
        self.assertEqual(metadata["run_id"], "fail_run")


if __name__ == '__main__':
    unittest.main()
