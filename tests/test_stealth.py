"""Tests for stealth module (unit tests, no browser required)."""

from crawlers.stealth import STEALTH_JS, USER_AGENTS, VIEWPORTS


class TestStealthConfig:
    def test_user_agents_not_empty(self):
        assert len(USER_AGENTS) >= 3

    def test_user_agents_are_realistic(self):
        for ua in USER_AGENTS:
            assert "Mozilla" in ua
            assert len(ua) > 50

    def test_viewports_valid(self):
        for vp in VIEWPORTS:
            assert "width" in vp
            assert "height" in vp
            assert vp["width"] >= 1024
            assert vp["height"] >= 600

    def test_stealth_js_contains_webdriver_override(self):
        assert "webdriver" in STEALTH_JS

    def test_stealth_js_contains_chrome_override(self):
        assert "chrome" in STEALTH_JS

    def test_stealth_js_contains_plugins_override(self):
        assert "plugins" in STEALTH_JS

    def test_stealth_js_contains_languages_override(self):
        assert "languages" in STEALTH_JS
