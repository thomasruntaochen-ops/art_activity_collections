import pytest

from src.crawlers.adapters.bampfa import _fetch_with_curl


def test_bampfa_curl_fallback_reports_missing_binary(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("src.crawlers.adapters.bampfa.shutil.which", lambda _: None)

    with pytest.raises(RuntimeError, match="'curl' is not installed"):
        _fetch_with_curl("https://bampfa.org/visit/calendar")
