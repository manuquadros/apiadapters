"""Module providing the NCBIAdapter and AsyncNCBIAdapterclasses."""

import asyncio
import itertools
import os
import urllib
from collections.abc import Iterable
from typing import AsyncIterator, Iterator, TypeVar

import httpx
import xmlparser
from apiadapters import APIAdapter, AsyncAPIAdapter, file_logger, stderr_logger
from lxml import etree

T = TypeVar("T")

_NAMESPACES = {
    "ns": "https://dtd.nlm.nih.gov/ns/archiving/2.3/",
    "jats": "https://jats.nlm.nih.gov/ns/archiving/1.3/",
    "xsi": "http://www.w3.org/2001/XMLSchema-instance",
    "mml": "http://www.w3.org/1998/Math/MathML",
    "xlink": "http://www.w3.org/1999/xlink",
    "ali": "http://www.niso.org/schemas/ali/1.0/",
}
for _prefix, _uri in _NAMESPACES.items():
    etree.register_namespace(_prefix, _uri)


def extract_abstract(article: etree._Element, clean=False) -> str:
    """Extract the abstract from `article`"""
    try:
        abstract = article.xpath("//*[name()='abstract']")[0]
    except IndexError:
        return ""
    else:
        if clean:
            abstract = xmlparser.clean_namespaces(abstract)
        return stringify(abstract)


def extract_body(article: etree._Element, clean=False) -> str:
    """Extract the abstract from `article`"""
    try:
        body = article.xpath("//*[name()='body']")[0]
    except IndexError:
        return ""
    else:
        if clean:
            body = xmlparser.clean_namespaces(body)
        return stringify(body)


def extract_pmid(article: etree._Element) -> str:
    return article.xpath(
        "//*[name()='article-id' and @pub-id-type='pmid']/text()"
    )[0]


def stringify(xml: etree._Element) -> str:
    """Convert `xml` to string"""
    return etree.tostring(xml, method="c14n2").decode("utf-8")


class NCBIAdapterBase:
    """Base class with shared NCBI adapter functionality."""

    def __init__(self) -> None:
        self.logger = file_logger(filename="ncbi.log")

        self.api_key: str | None = os.environ.get("NCBI_API_KEY")
        if not self.api_key:
            self.logger.info(
                "Continuing without API key. "
                "Set NCBI_API_KEY to increase the rate limit."
            )

    def _response_handler(self, response: httpx.Response) -> etree._Element:
        if response.status_code != 200:
            err = (
                f"Request for {response.url} failed"
                f" with status {response.status_code}"
            )
            raise httpx.HTTPStatusError(
                message=err,
                request=response.request,
                response=response,
            )

        return etree.fromstring(response.content)

    def summary_url(self, pubmed_id: str) -> str:
        url = (
            "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi?"
            f"db=pubmed&id={pubmed_id}"
        )

        if self.api_key:
            url += f"&api_key={self.api_key}"

        return url

    @staticmethod
    def record_url(pmcid: str) -> str:
        return (
            "https://www.ncbi.nlm.nih.gov/pmc/oai/oai.cgi?"
            "verb=GetRecord&identifier="
            f"oai:pubmedcentral.nih.gov:{pmcid}&metadataPrefix=pmc_fm"
        )


class NCBIAdapter(APIAdapter, NCBIAdapterBase):
    """Synchronous version of the NCBI adapter."""

    def __init__(self):
        super().__init__(headers={"Accept-Encoding": "gzip, deflate"})

    def request(self, url: str) -> etree._Element:
        return super().request(url, handler=self._response_handler)

    def fetch_ncbi_abstracts(
        self,
        pubmed_ids: str | Iterable[str],
        batch_size: int = 10000,
    ) -> dict[str, str]:
        """Fetch abstracts and copyright information for the given `pubmed_ids`.

        For articles that do not have an abstract available, return None.
        """
        abstracts: dict[str, str | None] = {}

        if isinstance(pubmed_ids, str):
            pubmed_ids = (pubmed_ids,)

        for batch in itertools.batched(pubmed_ids, batch_size):
            url = (
                "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"
                f"?db=pubmed&id={','.join(batch)}&retmode=xml"
            )
            root = self.request(url)

            for article in root.findall(".//MedlineCitation"):
                pmid = article.find("PMID").text
                abstract = article.find(".//AbstractText")

                if abstract is not None and getattr(abstract, "text", None):
                    abstracts[pmid] = abstract.text + "".join(
                        map(
                            lambda node: etree.tostring(
                                node, encoding="unicode"
                            ),
                            list(abstract),
                        ),
                    )

        return {_id: text for _id, text in abstracts.items() if text}

    def fetch_fulltext(self, pmc_id: str) -> str:
        """Fetch full text record for a single given `pmc_id`.

        :param pmc_id: PubMed Central id for full text retrieval.
        :return: serialized full text for the given `pmc_id`.
        """
        root = self.pmc_record(pmc_id)

        try:
            body = root.xpath("//*[name()='body']")[0]
        except IndexError:
            self.logger.debug(f"Could not retrieve full text for {pmc_id}.")
            return ""
        else:
            return etree.tostring(body, method="c14n2").decode("utf-8")

    def fetch_fulltext_and_abstract(self, pmc_id: str) -> dict[str, str] | None:
        """Retrieve full text and abstract for a given PMC ID.

        :param pmc_id: PubMed Central ID for retrieval.
        :return: dict containing abstract and full text for the
        requested ID.
        """
        root = self.pmc_record(pmc_id)

        try:
            ret = {
                "abstract": extract_abstract(root),
                "body": extract_body(root),
                "pubmed_id": extract_pmid(root),
            }
            return ret
        except IndexError:
            return None

    def pmc_record(self, pmc_id: str) -> etree._Element:
        """Retrieve the PMC_OAI record for a particular PMC ID.

        :param pmc_id: PubMed Central ID for record retrieval.
        :return: _Element containing the record.
        """
        url = (
            "https://www.ncbi.nlm.nih.gov/pmc/oai/oai.cgi"
            "?verb=GetRecord"
            f"&identifier=oai:pubmedcentral.nih.gov:{pmc_id}"
            "&metadataPrefix=pmc"
        )
        return self.request(url)

    def fetch_fulltext_articles(
        self,
        pmc_ids: str | Iterable[str],
    ) -> dict[str, str]:
        """Fetch full text record for the given `pmc_ids`.

        :param pmc_ids: PubMed Central ids for full text retrieval.
        :return: Dictionary mapping PMC IDs to serialized full texts.
        """
        if isinstance(pmc_ids, str):
            pmc_ids = (pmc_ids,)

        fulltext = {}
        for _id in pmc_ids:
            text = self.fetch_fulltext(_id)
            if text:
                fulltext[_id] = text

        return fulltext

    def article_ids(self, pubmed_id: str) -> dict[str, str]:
        record = self.request(self.summary_url(pubmed_id))

        return {
            id.attrib["Name"]: id.text
            for id in record.xpath("//Item[@Name='ArticleIds']//Item")
        }

    def is_pmc_open(self, pmcid: str | None) -> bool:
        if not pmcid:
            return False

        record = self.request(self.record_url(pmcid))
        namespaces = {"oai": "http://www.openarchives.org/OAI/2.0/"}

        return "pmc-open" in record.xpath(
            "//oai:setSpec/text()", namespaces=namespaces
        )

    def pmcids_for_query(self, query: str) -> Iterator[str]:
        """Retrieve PMC ids for a given Entrez text query.

        :param query: Entrez search query, as documented here:
            https://www.ncbi.nlm.nih.gov/books/NBK3837/
        :return: Iterator of PubMed Central ids.
        """
        encoded_query = urllib.parse.quote_plus(query)
        retstart = 0
        more = True
        count: int | None = None

        while more:
            url = (
                "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
                f"?db=pmc&term={encoded_query}&retstart={retstart}"
            )
            result = self.request(url)

            for id in result.xpath("//*[name()='Id']"):
                yield id.text

            if count is None:
                count = int(
                    result.xpath(
                        "//*[name()='eSearchResult']/*[name()='Count']"
                    )[0].text
                )

            retstart += 20

            if retstart >= count:
                more = False


class AsyncNCBIAdapter(AsyncAPIAdapter, NCBIAdapterBase):
    """Async version of the NCBI adapter."""

    def __init__(self):
        super().__init__(headers={"Accept-Encoding": "gzip, deflate"})

    async def request(self, url: str) -> etree._Element:
        return await super().request(url, handler=self._response_handler)

    async def fetch_ncbi_abstracts(
        self,
        pubmed_ids: str | Iterable[str],
        batch_size: int = 10000,
    ) -> dict[str, str]:
        """Fetch abstracts and copyright information for the given `pubmed_ids`.

        For articles that do not have an abstract available, return None.
        """
        abstracts: dict[str, str | None] = {}

        if isinstance(pubmed_ids, str):
            pubmed_ids = (pubmed_ids,)

        for batch in itertools.batched(pubmed_ids, batch_size):
            url = (
                "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"
                f"?db=pubmed&id={','.join(batch)}&retmode=xml"
            )
            root = await self.request(url)

            for article in root.findall(".//MedlineCitation"):
                pmid = article.find("PMID").text
                abstract = article.find(".//AbstractText")

                if abstract is not None and getattr(abstract, "text", None):
                    abstracts[pmid] = abstract.text + "".join(
                        map(
                            lambda node: etree.tostring(
                                node, encoding="unicode"
                            ),
                            list(abstract),
                        ),
                    )

        return {_id: text for _id, text in abstracts.items() if text}

    async def fetch_fulltext(self, pmc_id: str) -> str:
        """Fetch full text record for a single given `pmc_id`.

        :param pmc_id: PubMed Central id for full text retrieval.
        :return: serialized full text for the given `pmc_id`.
        """
        root = await self.pmc_record(pmc_id)

        try:
            body = root.xpath("//*[name()='body']")[0]
        except IndexError:
            self.logger.debug(f"Could not retrieve full text for {pmc_id}.")
            return ""
        else:
            return etree.tostring(body, method="c14n2").decode("utf-8")

    async def fetch_fulltext_and_abstract(self, pmc_id: str) -> dict[str, str]:
        """Retrieve full text and abstract for a given PMC ID.

        :param pmc_id: PubMed Central ID for retrieval.
        :return: dict containing abstract and full text for the
        requested ID.
        """
        root = await self.pmc_record(pmc_id)

        return {
            "abstract": extract_abstract(root),
            "body": extract_body(root),
        }

    async def pmc_record(self, pmc_id: str) -> etree._Element:
        """Retrieve the PMC_OAI record for a particular PMC ID.

        :param pmc_id: PubMed Central ID for record retrieval.
        :return: _Element containing the record.
        """
        url = (
            "https://www.ncbi.nlm.nih.gov/pmc/oai/oai.cgi"
            "?verb=GetRecord"
            f"&identifier=oai:pubmedcentral.nih.gov:{pmc_id}"
            "&metadataPrefix=pmc"
        )
        return await self.request(url)

    async def fetch_fulltext_articles(
        self,
        pmc_ids: str | Iterable[str],
    ) -> dict[str, str]:
        """Fetch full text record for the given `pmc_ids`.

        :param pmc_ids: PubMed Central ids for full text retrieval.
        :return: Dictionary mapping PMC IDs to serialized full texts.
        """
        if isinstance(pmc_ids, str):
            pmc_ids = (pmc_ids,)

        fulltext: dict[str, str] = {}

        async with asyncio.TaskGroup() as tg:
            fulltext.update(
                {
                    _id: tg.create_task(self.fetch_fulltext(_id))
                    for _id in pmc_ids
                },
            )

        fulltext.update(
            {_id: text_task.result() for _id, text_task in fulltext.items()},
        )

        return {_id: text for _id, text in fulltext.items() if text}

    @staticmethod
    def record_url(pmcid: str) -> str:
        return (
            "https://www.ncbi.nlm.nih.gov/pmc/oai/oai.cgi?"
            "verb=GetRecord&identifier="
            f"oai:pubmedcentral.nih.gov:{pmcid}&metadataPrefix=pmc_fm"
        )

    async def article_ids(self, pubmed_id: str) -> dict[str, str]:
        record = await self.request(self.summary_url(pubmed_id))

        return {
            id.attrib["Name"]: id.text
            for id in record.xpath("//Item[@Name='ArticleIds']//Item")
        }

    async def is_pmc_open(self, pmcid: str | None) -> bool:
        if not pmcid:
            return False

        record = await self.request(self.record_url(pmcid))
        namespaces = {"oai": "http://www.openarchives.org/OAI/2.0/"}

        return "pmc-open" in record.xpath(
            "//oai:setSpec/text()", namespaces=namespaces
        )

    async def pmcids_for_query(self, query: str) -> AsyncIterator[str]:
        """Retrieve PMC ids for a given Entrez text query.

        :oaram query: Entrez search query, as documented here:
            https://www.ncbi.nlm.nih.gov/books/NBK3837/
        :return: Iterator of PubMed Central ids.
        """
        encoded_query = urllib.parse.quote_plus(query)
        retstart = 0
        more = True
        count: int | None = None

        while more:
            url = (
                "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
                f"?db=pmc&term={encoded_query}&retstart={retstart}"
            )
            result = await self.request(url)

            for id in result.xpath("//*[name()='Id']"):
                yield id.text

            if count is None:
                count = int(
                    result.xpath(
                        "//*[name()='eSearchResult']/*[name()='Count']"
                    )[0].text
                )

            retstart += 20

            if retstart >= count:
                more = False
