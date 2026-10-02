import copy
import json
from pathlib import Path

import httpx
import pytest
from apiadapters.straininfo.straininfo import (
    AsyncStrainInfoAdapter,
    StrainInfoAdapter,
    StrainInfoAdapterBase,
    StrainRecord,
    StrainRef,
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
    assert StrainInfoAdapterBase._response_handler(str(req.url), response) == [
        {"id": 1}
    ]


def test_response_handler_404_returns_empty() -> None:
    req = httpx.Request("GET", "https://api.straininfo.dsmz.de/v1/test/999")
    response = httpx.Response(404, content=b"Not Found", request=req)
    assert StrainInfoAdapterBase._response_handler(str(req.url), response) == []


def test_response_handler_500_raises() -> None:
    req = httpx.Request("GET", "https://api.straininfo.dsmz.de/v1/test")
    response = httpx.Response(500, content=b"Server Error", request=req)
    with pytest.raises(httpx.HTTPStatusError):
        StrainInfoAdapterBase._response_handler(str(req.url), response)


def _strain_refs(n: int) -> list[StrainRef]:
    return [StrainRef(id=i, name=f"strain-{i}") for i in range(n)]


def _record(ix: int) -> StrainRecord:
    return {"id": ix, "relation": {"designation": [f"strain-{ix}"]}}


@pytest.mark.asyncio
async def test_async_store_strains_flush_calls_sink() -> None:
    received: list[dict[int, StrainRecord]] = []

    async def fake_retrieve_strain_models(designations):
        return {ix: _record(ix) for ix in designations}

    adapter = AsyncStrainInfoAdapter(sink=received.append)
    adapter.retrieve_strain_models = fake_retrieve_strain_models  # type: ignore[method-assign]

    for ref in _strain_refs(101):
        await adapter.store_strains([ref])

    assert len(received) == 1
    assert len(received[0]) == 101
    assert received[0] == {ix: _record(ix) for ix in range(101)}


@pytest.mark.asyncio
async def test_async_flush_without_sink_raises() -> None:
    async def fake_retrieve_strain_models(strains):
        return strains

    adapter = AsyncStrainInfoAdapter()
    adapter.retrieve_strain_models = fake_retrieve_strain_models  # type: ignore[method-assign]

    with pytest.raises(RuntimeError):
        for ref in _strain_refs(101):
            await adapter.store_strains([ref])


def test_sync_store_strains_flush_calls_sink() -> None:
    received: list[dict[int, StrainRecord]] = []

    def fake_retrieve_strain_models(designations):
        return {ix: _record(ix) for ix in designations}

    adapter = StrainInfoAdapter(sink=received.append)
    adapter.retrieve_strain_models = fake_retrieve_strain_models  # type: ignore[method-assign]

    for ref in _strain_refs(101):
        adapter.store_strains([ref])

    assert len(received) == 1
    assert len(received[0]) == 101
    assert received[0] == {ix: _record(ix) for ix in range(101)}


def test_sync_flush_without_sink_raises() -> None:
    def fake_retrieve_strain_models(strains):
        return strains

    adapter = StrainInfoAdapter()
    adapter.retrieve_strain_models = fake_retrieve_strain_models  # type: ignore[method-assign]

    with pytest.raises(RuntimeError):
        for ref in _strain_refs(101):
            adapter.store_strains([ref])


FIXTURE = json.loads((Path(__file__).parent / "straininfo_11469.json").read_text())
RECORD = FIXTURE[0]["strain"]


def _fake_request(url: str) -> list[dict] | list[int]:
    if "search/strain/str_des/" in url:
        return [RECORD["id"]]
    return FIXTURE


async def _async_fake_request(url: str) -> list[dict] | list[int]:
    return _fake_request(url)


def test_sync_get_strain_data_returns_api_records() -> None:
    adapter = StrainInfoAdapter()
    adapter.request = _fake_request  # type: ignore[method-assign]
    assert adapter.get_strain_data(RECORD["id"]) == (RECORD,)


@pytest.mark.asyncio
async def test_async_get_strain_data_returns_api_records() -> None:
    adapter = AsyncStrainInfoAdapter()
    adapter.request = _async_fake_request  # type: ignore[method-assign]
    assert await adapter.get_strain_data(RECORD["id"]) == (RECORD,)


@pytest.mark.parametrize("designation", ["NCTC 10538", "K-12"])
def test_sync_retrieve_strain_models_returns_resolved_records(
    designation: str,
) -> None:
    query = {7: {designation}, 8: {"no-such-strain"}}
    before = copy.deepcopy(query)
    adapter = StrainInfoAdapter()
    adapter.request = _fake_request  # type: ignore[method-assign]
    assert adapter.retrieve_strain_models(query) == {7: RECORD}
    assert query == before


@pytest.mark.asyncio
@pytest.mark.parametrize("designation", ["NCTC 10538", "K-12"])
async def test_async_retrieve_strain_models_returns_resolved_records(
    designation: str,
) -> None:
    query = {7: {designation}, 8: {"no-such-strain"}}
    before = copy.deepcopy(query)
    adapter = AsyncStrainInfoAdapter()
    adapter.request = _async_fake_request  # type: ignore[method-assign]
    assert await adapter.retrieve_strain_models(query) == {7: RECORD}
    assert query == before
