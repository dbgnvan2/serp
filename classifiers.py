"""
classifiers.py
Rules-based classifiers for Content Type (URL/Page level) and Entity Type (Domain level).
"""
import os
import yaml


class ContentClassifier:
    def classify(self, url, soup, headers):
        """
        Classifies content type based on URL, HTML content (BeautifulSoup object), and Headers.
        Returns: (content_type, confidence, evidence_list)
        """
        evidence = []

        # 1. PDF Check (High Confidence)
        if url.lower().endswith('.pdf') or (headers and 'application/pdf' in headers.get('Content-Type', '')):
            return 'pdf', 1.0, ["url_extension_or_header"]

        if not soup:
            return 'unknown', 0.0, ["no_content"]

        text = soup.get_text(" ", strip=True).lower()
        title = soup.title.string.lower() if soup.title and soup.title.string else ""

        # 2. Directory / Listing (High Confidence)
        # URL patterns
        if any(x in url.lower() for x in ['/directory/', '/list/', '/find-', '/best-', '-near-me']):
            evidence.append("url_pattern_directory")
            return 'directory', 0.8, evidence

        # Content patterns for directories
        if "top 10" in title or ("best " in title and " in " in title):
            evidence.append("title_list_pattern")
            return 'directory', 0.7, evidence

        # 3. News (High Confidence)
        if soup.find("meta", property="article:published_time"):
            evidence.append("meta_article_published")
            return 'news', 0.8, evidence

        # 4. Service Page (Medium Confidence)
        service_signals = ['book appointment', 'schedule consultation',
                           'our services', 'pricing', 'contact us']
        # Check top of page
        service_matches = [s for s in service_signals if s in text[:5000]]
        if len(service_matches) >= 2:
            evidence.append(f"service_keywords:{','.join(service_matches)}")
            return 'service', 0.7, evidence

        # 5. Guide / Resource (Medium Confidence)
        if "how to" in title or "guide" in title or "what is" in title:
            evidence.append("title_informational")
            return 'guide', 0.8, evidence

        # Heuristic: Long content often indicates a guide
        word_count = len(text.split())
        if word_count > 1500:
            evidence.append("high_word_count")
            return 'guide', 0.6, evidence

        # Default
        return 'other', 0.5, ["fallback"]


class EntityClassifier:
    def __init__(self, override_file="domain_overrides.yml"):
        self.overrides = {}
        if os.path.exists(override_file):
            try:
                with open(override_file, 'r') as f:
                    self.overrides = yaml.safe_load(f) or {}
            except Exception as e:
                print(f"Warning: Could not load domain overrides: {e}")

    def classify(self, domain, soup):
        """
        Classifies entity type based on domain and HTML content.
        Returns: (entity_type, confidence, evidence_list)
        """
        evidence = []

        # 0. Manual Override
        if domain in self.overrides:
            return self.overrides[domain], 1.0, ["manual_override"]

        # 1. TLD Signals
        if domain.endswith('.gov') or domain.endswith('.gc.ca'):
            return 'government', 1.0, ["tld_gov"]
        if domain.endswith('.edu'):
            return 'education', 1.0, ["tld_edu"]
        if domain.endswith('.org'):
            evidence.append("tld_org")  # Weak signal, need more

        if not soup:
            if "tld_org" in evidence:
                return 'nonprofit', 0.6, evidence
            return 'commercial', 0.4, ["fallback_no_content"]

        text = soup.get_text(" ", strip=True).lower()

        # 2. Nonprofit Signals
        nonprofit_keywords = ["registered charity",
                              "non-profit organization", "donate", "volunteer"]
        if any(k in text[:5000] for k in nonprofit_keywords):
            evidence.append("nonprofit_keywords")
            return 'nonprofit', 0.8, evidence

        # 3. Directory Signals (Domain level)
        directory_domains = ["yelp.ca", "yellowpages.ca",
                             "psychologytoday.com", "healthgrades.com"]
        if any(d in domain for d in directory_domains):
            return 'directory', 0.9, ["known_directory_domain"]

        # 4. Commercial (Default)
        return 'commercial', 0.5, ["fallback"]
