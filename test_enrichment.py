import unittest
from bs4 import BeautifulSoup
from classifiers import ContentClassifier, EntityClassifier


class TestClassifiers(unittest.TestCase):
    def setUp(self):
        self.content_classifier = ContentClassifier()
        # Initialize with empty overrides to test core logic
        self.entity_classifier = EntityClassifier(
            override_file="non_existent_file.yml")

    def test_content_pdf(self):
        """Test PDF detection via URL or Headers."""
        # Case 1: URL extension
        c_type, conf, ev = self.content_classifier.classify(
            "http://example.com/file.pdf", None, {})
        self.assertEqual(c_type, "pdf")
        self.assertEqual(conf, 1.0)

        # Case 2: Header
        c_type, conf, ev = self.content_classifier.classify(
            "http://example.com/doc", None, {'Content-Type': 'application/pdf'})
        self.assertEqual(c_type, "pdf")

    def test_content_directory(self):
        """Test Directory detection via URL patterns and Title."""
        # Case 1: URL pattern
        c_type, conf, ev = self.content_classifier.classify(
            "http://example.com/best-therapists-vancouver", None, {})
        self.assertEqual(c_type, "directory")
        self.assertIn("url_pattern_directory", ev)

        # Case 2: Title pattern
        soup = BeautifulSoup(
            "<html><title>10 Best Counsellors in Vancouver</title></html>", "html.parser")
        c_type, conf, ev = self.content_classifier.classify(
            "http://example.com/blog", soup, {})
        self.assertEqual(c_type, "directory")
        self.assertIn("title_list_pattern", ev)

    def test_content_service(self):
        """Test Service page detection via keywords."""
        html = "<html><body><h1>Therapy</h1><p>Contact us to book appointment for our services.</p></body></html>"
        soup = BeautifulSoup(html, "html.parser")
        c_type, conf, ev = self.content_classifier.classify(
            "http://example.com/therapy", soup, {})
        self.assertEqual(c_type, "service")
        self.assertTrue(any("service_keywords" in e for e in ev))

    def test_content_guide(self):
        """Test Guide detection via title and word count."""
        # Short content but "How to" title
        soup = BeautifulSoup(
            "<html><title>How to find a therapist</title><p>...</p></html>", "html.parser")
        c_type, conf, ev = self.content_classifier.classify(
            "http://example.com/guide", soup, {})
        self.assertEqual(c_type, "guide")

        # Long content
        long_text = "word " * 1600
        soup = BeautifulSoup(
            f"<html><body>{long_text}</body></html>", "html.parser")
        c_type, conf, ev = self.content_classifier.classify(
            "http://example.com/article", soup, {})
        self.assertEqual(c_type, "guide")
        self.assertIn("high_word_count", ev)

    def test_entity_government(self):
        """Test Government entity detection via TLD."""
        c_type, conf, ev = self.entity_classifier.classify(
            "www.canada.ca", None)
        self.assertEqual(c_type, "government")
        self.assertIn("tld_gov", ev)

        c_type, conf, ev = self.entity_classifier.classify(
            "www2.gov.bc.ca", None)
        self.assertEqual(c_type, "government")

    def test_entity_nonprofit(self):
        """Test Nonprofit detection via keywords."""
        html = "<html><body><p>We are a registered charity dedicated to helping...</p></body></html>"
        soup = BeautifulSoup(html, "html.parser")
        c_type, conf, ev = self.entity_classifier.classify("example.org", soup)
        self.assertEqual(c_type, "nonprofit")
        self.assertIn("nonprofit_keywords", ev)

    def test_entity_directory_domain(self):
        """Test known directory domains."""
        c_type, conf, ev = self.entity_classifier.classify(
            "www.psychologytoday.com", None)
        self.assertEqual(c_type, "directory")
        self.assertIn("known_directory_domain", ev)

    def test_entity_commercial_fallback(self):
        """Test fallback to commercial."""
        c_type, conf, ev = self.entity_classifier.classify(
            "www.private-practice.com", None)
        self.assertEqual(c_type, "commercial")


if __name__ == '__main__':
    unittest.main()
