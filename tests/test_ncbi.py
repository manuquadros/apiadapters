import pytest
import pathlib
from lxml import etree
from apiadapters import ncbi

TEST_DIR = pathlib.Path(__file__).parent


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


xml = etree.parse(TEST_DIR / "pmc_365027.xml")
abstract = etree.parse(TEST_DIR / "pmc_365027_abstract.xml")
body = etree.parse(TEST_DIR / "pmc_365027_body.xml")


def test_extract_abstract() -> None:
    assert ncbi.extract_abstract(xml, clean=True) == ncbi.stringify(abstract)


def test_extract_body() -> None:
    assert ncbi.extract_body(xml, clean=True) == ncbi.stringify(body)
