import itertools

import pytest
from lxml import etree

from apiadapters.ncbi.ncbi import AsyncNCBIAdapter, NCBIAdapter

KNOWN_PMID = "15018644"
KNOWN_PMC_ID = "365027"


@pytest.mark.integration
def test_fetch_ncbi_abstracts_returns_text_for_known_pmid() -> None:
    with NCBIAdapter() as adapter:
        result = adapter.fetch_ncbi_abstracts(KNOWN_PMID)
    assert KNOWN_PMID in result
    assert isinstance(result[KNOWN_PMID], str)
    assert len(result[KNOWN_PMID]) > 0


@pytest.mark.integration
def test_article_ids_contains_pubmed_and_pmc() -> None:
    with NCBIAdapter() as adapter:
        ids = adapter.article_ids(KNOWN_PMID)
    assert ids.get("pubmed") == KNOWN_PMID
    assert "pmc" in ids


@pytest.mark.integration
def test_fetch_fulltext_and_abstract_returns_all_fields() -> None:
    with NCBIAdapter() as adapter:
        result = adapter.fetch_fulltext_and_abstract(KNOWN_PMC_ID)
    assert result is not None
    assert result["pubmed_id"] == KNOWN_PMID
    assert len(result["abstract"]) > 0
    assert len(result["body"]) > 0


@pytest.mark.integration
def test_fetch_fulltext_returns_xml_string() -> None:
    with NCBIAdapter() as adapter:
        body = adapter.fetch_fulltext(KNOWN_PMC_ID)
    assert len(body) > 0
    root = etree.fromstring(body.encode())
    assert root.tag is not None


@pytest.mark.integration
def test_is_pmc_open_true_for_open_article() -> None:
    with NCBIAdapter() as adapter:
        assert adapter.is_pmc_open(KNOWN_PMC_ID) is True


@pytest.mark.integration
def test_pmcids_for_query_yields_strings() -> None:
    with NCBIAdapter() as adapter:
        ids = list(itertools.islice(adapter.pmcids_for_query("Bacteroides fragilis[TIAB]"), 5))
    assert len(ids) > 0
    assert all(isinstance(i, str) for i in ids)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_async_fetch_ncbi_abstracts_returns_text_for_known_pmid() -> None:
    async with AsyncNCBIAdapter() as adapter:
        result = await adapter.fetch_ncbi_abstracts(KNOWN_PMID)
    assert KNOWN_PMID in result
    assert len(result[KNOWN_PMID]) > 0


@pytest.mark.integration
@pytest.mark.asyncio
async def test_async_article_ids_contains_pubmed_and_pmc() -> None:
    async with AsyncNCBIAdapter() as adapter:
        ids = await adapter.article_ids(KNOWN_PMID)
    assert ids.get("pubmed") == KNOWN_PMID
    assert "pmc" in ids


@pytest.mark.integration
@pytest.mark.asyncio
async def test_async_fetch_fulltext_and_abstract_returns_all_fields() -> None:
    async with AsyncNCBIAdapter() as adapter:
        result = await adapter.fetch_fulltext_and_abstract(KNOWN_PMC_ID)
    assert result["pubmed_id"] == KNOWN_PMID
    assert len(result["abstract"]) > 0
    assert len(result["body"]) > 0


@pytest.mark.integration
@pytest.mark.asyncio
async def test_async_is_pmc_open_true_for_open_article() -> None:
    async with AsyncNCBIAdapter() as adapter:
        assert await adapter.is_pmc_open(KNOWN_PMC_ID) is True


@pytest.mark.integration
@pytest.mark.asyncio
async def test_async_pmcids_for_query_yields_strings() -> None:
    ids = []
    async with AsyncNCBIAdapter() as adapter:
        async for pmc_id in adapter.pmcids_for_query("Bacteroides fragilis[TIAB]"):
            ids.append(pmc_id)
            if len(ids) >= 5:
                break
    assert len(ids) > 0
    assert all(isinstance(i, str) for i in ids)
