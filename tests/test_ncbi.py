import pathlib
from unittest.mock import MagicMock, patch

import httpx
import pytest
from apiadapters.ncbi import ncbi
from lxml import etree

TEST_DIR = pathlib.Path(__file__).parent

xml = etree.parse(TEST_DIR / "pmc_365027.xml")
abstract = etree.parse(TEST_DIR / "pmc_365027_abstract.xml")
body = etree.parse(TEST_DIR / "pmc_365027_body.xml")


def _make_base(monkeypatch=None, api_key=None):
    """Instantiate NCBIAdapterBase with a mocked logger, optionally setting NCBI_API_KEY."""
    if monkeypatch is not None:
        if api_key:
            monkeypatch.setenv("NCBI_API_KEY", api_key)
        else:
            monkeypatch.delenv("NCBI_API_KEY", raising=False)
    with patch("apiadapters.ncbi.ncbi.file_logger", return_value=MagicMock()):
        return ncbi.NCBIAdapterBase()


@pytest.mark.asyncio
async def test_pmc_open() -> None:
    async with ncbi.AsyncNCBIAdapter() as api:
        is_open = await api.is_pmc_open("365027")
        assert is_open is True


@pytest.mark.asyncio
async def test_abstract_with_formatting() -> None:
    async with ncbi.AsyncNCBIAdapter() as api:
        pmid = "17323951"
        abstracts = await api.fetch_ncbi_abstracts(pmid)
        assert abstracts[pmid] == (
            "Bacteria are surrounded by a cell wall containing layers of"
            " peptidoglycan, the integrity of which is essential for bacterial"
            " survival. In the final stage of peptidoglycan"
            " biosynthesis, peptidoglycan glycosyltransferases"
            " (PGTs;"
            " also known as transglycosylases) catalyze the polymerization"
            " of Lipid II to form linear glycan chains. PGTs"
            " have tremendous potential as antibiotic targets,"
            " but the potential has not"
            " yet been"
            " realized. Mechanistic studies have been hampered by a"
            " lack of substrates to"
            " monitor enzymatic"
            " activity. We report here the total synthesis of"
            " heptaprenyl-Lipid IV and its"
            " use to study two different PGTs from <i>E. coli</i>."
            " We show that one PGT can couple"
            " Lipid IV to"
            " itself whereas the other can only couple Lipid IV to Lipid II."
            " These <i>in"
            " vitro</i>"
            " differences in enzymatic activity may reflect differences in the"
            " biological"
            " functions of the"
            " two major glycosyltransferases in <i>E coli</i>."
        )


def test_extract_abstract() -> None:
    assert ncbi.extract_abstract(xml, clean=True) == ncbi.stringify(abstract)


def test_extract_body() -> None:
    assert ncbi.extract_body(xml, clean=True) == ncbi.stringify(body)


def test_extract_pmid() -> None:
    assert ncbi.extract_pmid(xml) == "15018644"


def test_extract_pmid_missing_raises() -> None:
    with pytest.raises(IndexError):
        ncbi.extract_pmid(etree.fromstring(b"<article/>"))


def test_stringify_produces_valid_xml() -> None:
    root = etree.fromstring(b"<p>hello <i>world</i></p>")
    result = ncbi.stringify(root)
    reparsed = etree.fromstring(result.encode())
    assert etree.QName(reparsed).localname == "p"
    assert etree.QName(reparsed[0]).localname == "i"


def test_extract_abstract_missing_returns_empty() -> None:
    assert ncbi.extract_abstract(etree.fromstring(b"<article/>")) == ""


def test_extract_body_missing_returns_empty() -> None:
    assert ncbi.extract_body(etree.fromstring(b"<article/>")) == ""


def test_fetch_ncbi_abstracts_leading_child_node() -> None:
    response_xml = etree.fromstring(b"""<PubmedArticleSet>
  <PubmedArticle>
    <MedlineCitation>
      <PMID>99999</PMID>
      <Article>
        <Abstract>
          <AbstractText><i>E. coli</i> is a bacterium.</AbstractText>
        </Abstract>
      </Article>
    </MedlineCitation>
  </PubmedArticle>
</PubmedArticleSet>""")
    adapter = ncbi.NCBIAdapter()
    with patch.object(adapter, "request", return_value=response_xml):
        result = adapter.fetch_ncbi_abstracts("99999")
    adapter.client.close()

    assert "99999" in result
    assert "<i>E. coli</i>" in result["99999"]


def test_summary_url_without_api_key(monkeypatch) -> None:
    adapter = _make_base(monkeypatch)
    url = adapter.summary_url("12345")
    assert "12345" in url
    assert "api_key" not in url


def test_summary_url_with_api_key(monkeypatch) -> None:
    adapter = _make_base(monkeypatch, api_key="testkey123")
    url = adapter.summary_url("12345")
    assert "api_key=testkey123" in url


def test_record_url() -> None:
    url = ncbi.NCBIAdapterBase.record_url("365027")
    assert "365027" in url
    assert "GetRecord" in url


def test_response_handler_200(monkeypatch) -> None:
    adapter = _make_base(monkeypatch)
    req = httpx.Request("GET", "https://example.com")
    response = httpx.Response(200, content=b"<root/>", request=req)
    element = adapter._response_handler(response)
    assert isinstance(element, etree._Element)
    assert element.tag == "root"


def test_response_handler_non200_raises(monkeypatch) -> None:
    adapter = _make_base(monkeypatch)
    req = httpx.Request("GET", "https://example.com")
    response = httpx.Response(503, content=b"error", request=req)
    with pytest.raises(httpx.HTTPStatusError):
        adapter._response_handler(response)


def test_pmcids_for_query_paginates() -> None:
    def make_esearch(count, ids):
        ids_xml = "".join(f"<Id>{i}</Id>" for i in ids)
        return etree.fromstring(
            f"<eSearchResult><Count>{count}</Count>"
            f"<IdList>{ids_xml}</IdList></eSearchResult>".encode()
        )

    call_count = 0

    def fake_request(url):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return make_esearch(25, range(1, 21))
        return make_esearch(25, range(21, 26))

    adapter = ncbi.NCBIAdapter()
    with patch.object(adapter, "request", side_effect=fake_request):
        results = list(adapter.pmcids_for_query("bacteria"))
    adapter.client.close()

    assert call_count == 2
    assert len(results) == 25
    assert results[0] == "1"
    assert results[-1] == "25"
