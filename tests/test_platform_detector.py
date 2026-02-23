"""Tests for platform detection."""

from crawlers.platform_detector import PlatformDetector


class TestPlatformDetector:
    def setup_method(self):
        self.detector = PlatformDetector()

    def test_dealeron_detection(self, sample_html_dealeron):
        result = self.detector.detect_from_html(sample_html_dealeron)
        assert result["platform"] == "DealerOn"
        assert result["confidence"] > 0.7

    def test_dealer_com_detection(self):
        html = '<html><head><script src="https://static.dealer.com/v8/main.js"></script></head><body></body></html>'
        result = self.detector.detect_from_html(html)
        assert result["platform"] == "Dealer.com"

    def test_dealerinspire_detection(self):
        html = '<html><head><link href="https://cdn.dealerinspire.com/style.css"></head><body></body></html>'
        result = self.detector.detect_from_html(html)
        assert result["platform"] == "DealerInspire"

    def test_meta_generator_wordpress(self):
        html = '<html><head><meta name="generator" content="WordPress 6.4"></head><body></body></html>'
        result = self.detector.detect_from_html(html)
        assert result["platform"] == "WordPress"
        assert result["method"] == "meta_generator"

    def test_unknown_platform(self):
        html = "<html><body><h1>Custom dealership site</h1></body></html>"
        result = self.detector.detect_from_html(html)
        assert result["platform"] == "Custom/Unknown"
        assert result["confidence"] == 0.0
