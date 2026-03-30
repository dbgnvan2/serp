import importlib.util
import os
import tempfile
import unittest
from pathlib import Path

try:
    import tkinter  # noqa: F401
    TKINTER_AVAILABLE = True
except ModuleNotFoundError:
    TKINTER_AVAILABLE = False

MODULE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "serp-me.py")


def load_serp_me():
    spec = importlib.util.spec_from_file_location("serp_me_mod", MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@unittest.skipUnless(TKINTER_AVAILABLE, "tkinter not available in this environment")
class TestSerpLauncherResolution(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.mod = load_serp_me()

    def make_app(self):
        app = object.__new__(self.mod.SerpLauncherApp)
        app.read_keyword_file = self.mod.SerpLauncherApp.read_keyword_file.__get__(app)
        app.load_config = self.mod.SerpLauncherApp.load_config.__get__(app)
        app.find_latest_topic_output = self.mod.SerpLauncherApp.find_latest_topic_output.__get__(app)
        app.find_latest_any_output = self.mod.SerpLauncherApp.find_latest_any_output.__get__(app)
        app.find_matching_topic_slug = self.mod.SerpLauncherApp.find_matching_topic_slug.__get__(app)
        app.resolve_existing_analysis_outputs = self.mod.SerpLauncherApp.resolve_existing_analysis_outputs.__get__(app)
        return app

    def test_derive_topic_slug_from_default_keywords_file(self):
        self.assertEqual(self.mod.derive_topic_slug_from_keyword_file("keywords.csv"), "keywords")
        self.assertEqual(
            self.mod.derive_topic_slug_from_keyword_file("keywords_estrangement.csv"),
            "estrangement",
        )

    def test_derive_topic_slug_normalizes_to_lowercase(self):
        # Mixed-case standalone file
        self.assertEqual(
            self.mod.derive_topic_slug_from_keyword_file("Substance_Use.csv"),
            "substance_use",
        )
        # Mixed-case keywords_ prefixed file
        self.assertEqual(
            self.mod.derive_topic_slug_from_keyword_file("keywords_Substance_Use.csv"),
            "substance_use",
        )

    def test_derive_topic_slug_replaces_spaces_with_underscores(self):
        self.assertEqual(
            self.mod.derive_topic_slug_from_keyword_file("Basic Series Tape 7.csv"),
            "basic_series_tape_7",
        )
        self.assertEqual(
            self.mod.derive_topic_slug_from_keyword_file("keywords_mental health.csv"),
            "mental_health",
        )

    def test_resolve_existing_analysis_outputs_uses_matching_keyword_file_slug(self):
        app = self.make_app()
        with tempfile.TemporaryDirectory() as tmpdir:
            cwd = os.getcwd()
            try:
                os.chdir(tmpdir)
                Path("keywords.csv").write_text("estrangement\nestrangement grief\n", encoding="utf-8")
                Path("keywords_estrangement.csv").write_text(
                    "estrangement\nestrangement grief\n",
                    encoding="utf-8",
                )
                Path("market_analysis_estrangement_20260311_1733.json").write_text("{}", encoding="utf-8")
                Path("market_analysis_estrangement_20260311_1733.xlsx").write_text("", encoding="utf-8")
                Path("market_analysis_estrangement_20260311_1733.md").write_text("", encoding="utf-8")

                slug, latest_json, latest_xlsx, latest_md = app.resolve_existing_analysis_outputs(
                    os.path.join(tmpdir, "keywords.csv"),
                    "keywords",
                )
                self.assertEqual(slug, "estrangement")
                self.assertTrue(latest_json.endswith("market_analysis_estrangement_20260311_1733.json"))
                self.assertTrue(latest_xlsx.endswith("market_analysis_estrangement_20260311_1733.xlsx"))
                self.assertTrue(latest_md.endswith("market_analysis_estrangement_20260311_1733.md"))
            finally:
                os.chdir(cwd)

    def test_resolve_existing_analysis_outputs_uses_configured_output_for_keywords_csv(self):
        app = self.make_app()
        with tempfile.TemporaryDirectory() as tmpdir:
            cwd = os.getcwd()
            try:
                os.chdir(tmpdir)
                Path("keywords.csv").write_text("estrangement\n", encoding="utf-8")
                Path("config.yml").write_text(
                    "files:\n"
                    "  output_json: market_analysis_estrangement_20260311_1733.json\n"
                    "  output_xlsx: market_analysis_estrangement_20260311_1733.xlsx\n"
                    "  output_md: market_analysis_estrangement_20260311_1733.md\n",
                    encoding="utf-8",
                )
                Path("market_analysis_estrangement_20260311_1733.json").write_text("{}", encoding="utf-8")
                Path("market_analysis_estrangement_20260311_1733.xlsx").write_text("", encoding="utf-8")
                Path("market_analysis_estrangement_20260311_1733.md").write_text("", encoding="utf-8")

                slug, latest_json, latest_xlsx, latest_md = app.resolve_existing_analysis_outputs(
                    os.path.join(tmpdir, "keywords.csv"),
                    "keywords",
                )
                self.assertEqual(slug, "estrangement")
                self.assertTrue(latest_json.endswith("market_analysis_estrangement_20260311_1733.json"))
                self.assertTrue(latest_xlsx.endswith("market_analysis_estrangement_20260311_1733.xlsx"))
                self.assertTrue(latest_md.endswith("market_analysis_estrangement_20260311_1733.md"))
            finally:
                os.chdir(cwd)


if __name__ == "__main__":
    unittest.main()
