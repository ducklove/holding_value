import fetch_current


def test_proxy_quote_uses_falling_rf_when_summary_change_is_absolute(monkeypatch):
    def fake_request_json(*args, **kwargs):
        return {
            "summary": {
                "current_price": 237500,
                "change": 6000,
            },
            "raw": {
                "rf": "5",
                "cv": 6000,
            },
        }

    monkeypatch.setattr(fetch_current, "request_json", fake_request_json)

    assert fetch_current.fetch_kis_proxy_domestic_quote("035420.KS") == (237500.0, 243500.0)


def test_proxy_index_uses_falling_sign_when_summary_rate_is_absolute(monkeypatch):
    def fake_request_json(*args, **kwargs):
        return {
            "summary": {
                "current_price": 1002.65,
                "change": 29.31,
                "change_rate": 2.84,
            },
            "raw": {
                "prdy_vrss_sign": "5",
            },
        }

    monkeypatch.setattr(fetch_current, "request_json", fake_request_json)

    metric = fetch_current.fetch_kis_proxy_index("KOSDAQ")
    assert metric["change"] == -29.31
    assert metric["changePct"] == -2.84
