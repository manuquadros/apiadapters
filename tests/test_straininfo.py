import httpx
import pytest
from apiadapters.straininfo.straininfo import (
    StrainInfoAdapterBase,
    normalize_strain_names,
)


def test_normalize_preserves_originals() -> None:
    names = {"DSM 579", "ATCC 12345"}
    assert names.issubset(normalize_strain_names(names))


def test_normalize_accepts_string_input() -> None:
    assert normalize_strain_names("DSM 579") == {"DSM 579"}


def test_normalize_nrrl_missing_dash() -> None:
    result = normalize_strain_names("NRRL B123")
    assert "NRRL B-123" in result
    assert "NRRL B123" in result


def test_normalize_nrrl_space_transposed() -> None:
    result = normalize_strain_names("NRRLB 123")
    assert "NRRL B-123" in result


def test_normalize_trailing_type_indicator() -> None:
    result = normalize_strain_names("DSM 579T")
    assert "DSM 579" in result
    assert "DSM 579T" in result


def test_normalize_slash_splits_into_parts() -> None:
    result = normalize_strain_names("HBB / ATCC 27634 / DSM 579")
    assert {"HBB", "ATCC 27634", "DSM 579"}.issubset(result)


def test_api_url_string_routes_to_str_des() -> None:
    url = StrainInfoAdapterBase.strain_info_api_url("E. coli K-12")
    assert "search/strain/str_des/" in url
    assert "E. coli K-12" in url


def test_api_url_int_routes_to_data() -> None:
    url = StrainInfoAdapterBase.strain_info_api_url(42)
    assert "data/strain/max/" in url
    assert "42" in url


def test_api_url_list_of_strings() -> None:
    url = StrainInfoAdapterBase.strain_info_api_url(["strain1", "strain2"])
    assert "search/strain/str_des/" in url
    assert "strain1,strain2" in url


def test_api_url_list_of_ints() -> None:
    url = StrainInfoAdapterBase.strain_info_api_url([1, 2, 3])
    assert "data/strain/max/" in url
    assert "1,2,3" in url


def test_api_url_empty_raises() -> None:
    with pytest.raises(ValueError):
        StrainInfoAdapterBase.strain_info_api_url([])


def test_response_handler_200_returns_json() -> None:
    req = httpx.Request("GET", "https://api.straininfo.dsmz.de/v1/test")
    response = httpx.Response(200, json=[{"id": 1}], request=req)
    assert StrainInfoAdapterBase._response_handler(req.url, response) == [{"id": 1}]


def test_response_handler_404_returns_empty() -> None:
    req = httpx.Request("GET", "https://api.straininfo.dsmz.de/v1/test/999")
    response = httpx.Response(404, content=b"Not Found", request=req)
    assert StrainInfoAdapterBase._response_handler(req.url, response) == []


def test_response_handler_500_raises() -> None:
    req = httpx.Request("GET", "https://api.straininfo.dsmz.de/v1/test")
    response = httpx.Response(500, content=b"Server Error", request=req)
    with pytest.raises(httpx.HTTPStatusError):
        StrainInfoAdapterBase._response_handler(req.url, response)
