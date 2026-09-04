from apiadapters.checklistbank.wfo import WFOAdapterBase


def test_api_name_usage_url() -> None:
    url = WFOAdapterBase.dataset_name_usage_url("2004", "Solanum tuberosum")
    assert "dataset/2004/match/nameusage?" in url
    assert "match/nameusage?" in url
    assert "2004" in url
    assert "Solanum tuberosum" in url
