import re
from collections.abc import Callable, Collection, Iterable, Mapping, Sequence
from functools import singledispatchmethod
from types import TracebackType
from typing import Any, NamedTuple, NotRequired, TypedDict, cast

import httpx
from apiadapters import APIAdapter, AsyncAPIAdapter, BaseAPIAdapter, stderr_logger

api_root = "https://api.straininfo.dsmz.de/v1/"


class TaxonRecord(TypedDict):
    name: str
    lpsn: NotRequired[int]
    ncbi: NotRequired[int]


class CultureRecord(TypedDict):
    id: int
    strain_number: str
    origin: NotRequired[str]


class RelationRecord(TypedDict):
    designation: NotRequired[list[str]]
    culture: NotRequired[list[CultureRecord]]


class StrainRecord(TypedDict):
    """A strain object from the StrainInfo v1 ``data/strain/max/`` endpoint."""

    id: int
    relation: RelationRecord
    doi: NotRequired[str]
    merged: NotRequired[list[int]]
    bacdive: NotRequired[int]
    taxon: NotRequired[TaxonRecord]


def _record_names(record: StrainRecord) -> list[str]:
    relation = record["relation"]
    return relation.get("designation", []) + [
        culture["strain_number"] for culture in relation.get("culture", [])
    ]


def _index_names(designations: Mapping[int, Collection[str]]) -> dict[str, int]:
    return {
        name: ix
        for ix, names in designations.items()
        for name in normalize_strain_names(names)
    }


def _match_records(
    known_names: Mapping[str, int],
    records: Iterable[StrainRecord],
) -> dict[int, StrainRecord]:
    matched: dict[int, StrainRecord] = {}
    for record in records:
        keyname = next(filter(known_names.__contains__, _record_names(record)), None)
        if keyname is not None:
            matched[known_names[keyname]] = record
    return matched


class StrainRef(NamedTuple):
    id: int
    name: str


def normalize_strain_names(strain_names: str | Collection[str]) -> set[str]:
    """Attempt to normalize a collection of strain designations.

    This function is needed because some strains are identified in BRENDA

    :param strain_names: string or iterable containing (possibly non-standard) strain
        designations.
    :return: set containing :py:data:`strain_names` plus standardized versions of the
        designations included in :py:data:`strain_names`.
    """
    if isinstance(strain_names, str):
        strain_names = (strain_names,)

    standardized: set[str] = set()

    def apply_substitutions(w: str) -> tuple[str, int]:
        substitutions = (
            (r"(NRRL)(B | B)(\d+)", r"\1 B-\3"),
            (r"([a-zA-Z]+ \w*\d+)[Tt]", r"\1"),
        )

        number_of_subs = 0
        for sub in substitutions:
            w, n = re.subn(sub[0], sub[1], w)
            number_of_subs += n

        return w, number_of_subs

    for name in strain_names:
        new_name, number_of_subs = apply_substitutions(name)
        substrings = new_name.split("/")

        if len(substrings) > 1 or number_of_subs > 0:
            standardized.update(map(lambda w: w.strip(), substrings))

    return set(strain_names) | standardized


class StrainInfoAdapterBase:
    """Base class with shared StrainInfo adapter functionality."""

    def __init__(
        self, sink: Callable[[dict[int, StrainRecord]], None] | None = None
    ) -> None:
        self.buffer: set[StrainRef] = set()
        self.sink = sink

    @staticmethod
    def _response_handler(
        url: str,
        response: httpx.Response,
    ) -> list[dict] | list[int]:
        match response.status_code:
            case 200:
                return response.json()
            case 404:
                stderr_logger().error("%s not found on StrainInfo.", url.split("/")[-1])
                return []
            case _:
                raise response.raise_for_status()

    @singledispatchmethod
    @staticmethod
    def strain_info_api_url(query: Any):
        raise TypeError

    @strain_info_api_url.register(Iterable)
    @staticmethod
    def _(query: Iterable[str] | Iterable[int]) -> str:
        if not query:
            raise ValueError

        for item in query:
            match type(item).__name__:
                case "str":
                    root = api_root + "search/strain/str_des/"
                case "int":
                    root = api_root + "data/strain/max/"
                case _:
                    raise httpx.InvalidURL
            break

        return root + ",".join(map(str, query))

    @strain_info_api_url.register
    @staticmethod
    def _(query: str | int) -> str:
        return StrainInfoAdapterBase.strain_info_api_url([query])


class AsyncStrainInfoAdapter(AsyncAPIAdapter, StrainInfoAdapterBase):
    def __init__(self, sink: Callable[[dict[int, StrainRecord]], None] | None = None):
        AsyncAPIAdapter.__init__(
            self,
            headers={
                "Accept": "application/json",
                "Cache-Control": "no-store",
                "Accept-Encoding": "gzip, deflate",
            },
        )
        StrainInfoAdapterBase.__init__(self, sink=sink)

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        await self.__flush_buffer()

    async def request(self, url: str) -> list[dict] | list[int]:
        response = await super().request(url)
        return self._response_handler(url, response)

    async def retrieve_strain_models(
        self,
        designations: Mapping[int, Collection[str]],
    ) -> dict[int, StrainRecord]:
        """Return the StrainInfo records resolved for each key's designations."""
        known_names = _index_names(designations)
        ids = await self.get_strain_ids(list(known_names))
        return _match_records(known_names, await self.get_strain_data(ids))

    async def __flush_buffer(self) -> None:
        """Resolve the buffered strain refs and hand them to self.sink.

        Strain models might have unnormalized strain designations, like
        'HBB / ATCC 27634 / DSM 579'. The method will extract the normalized
        designations from such a name and try to retrieve data about them from
        StrainInfo.
        """
        print("Flushing strain buffer")

        indexed_buffer = await self.retrieve_strain_models(
            {ref.id: normalize_strain_names(ref.name) for ref in self.buffer}
        )

        if indexed_buffer:
            if self.sink is None:
                raise RuntimeError("Strains were buffered but no sink was configured.")
            self.sink(indexed_buffer)

        self.buffer = set()

    async def store_strains(self, strains: Iterable[StrainRef]) -> None:
        self.buffer.update(strains)

        if len(self.buffer) > 100:
            await self.__flush_buffer()

    async def get_strain_ids(self, query: str | Sequence[str]) -> list[int]:
        if not query:
            return []

        if isinstance(query, str):
            query = (query,)

        response = await self.request(self.strain_info_api_url(query))

        if response and isinstance(response[0], int):
            return cast(list[int], response)

        return []

    async def get_strain_data(
        self, query: int | Iterable[int]
    ) -> tuple[StrainRecord, ...]:
        """Retrieve the StrainInfo records for the strain IDs given in the argument."""
        try:
            data = await self.request(self.strain_info_api_url(query))
        except ValueError:
            return ()

        return tuple(
            item["strain"] for item in cast(list[dict[str, StrainRecord]], data)
        )


class StrainInfoAdapter(APIAdapter, StrainInfoAdapterBase):
    """Synchronous version of the StrainInfo adapter."""

    def __init__(self, sink: Callable[[dict[int, StrainRecord]], None] | None = None):
        APIAdapter.__init__(
            self,
            headers={
                "Accept": "application/json",
                "Cache-Control": "no-store",
                "Accept-Encoding": "gzip, deflate",
            },
        )
        StrainInfoAdapterBase.__init__(self, sink=sink)

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self._flush_buffer()
        super().__exit__(exc_type, exc_value, exc_tb)

    def request(self, url: str) -> list[dict] | list[int]:
        response = super().request(url)
        return self._response_handler(url, response)

    def _flush_buffer(self) -> None:
        """Resolve the buffered strain refs and hand them to self.sink.

        Strain models might have unnormalized strain designations, like
        'HBB / ATCC 27634 / DSM 579'. The method will extract the normalized
        designations from such a name and try to retrieve data about them from
        StrainInfo.
        """
        print("Flushing strain buffer")

        indexed_buffer = self.retrieve_strain_models(
            {ref.id: normalize_strain_names(ref.name) for ref in self.buffer}
        )

        if indexed_buffer:
            if self.sink is None:
                raise RuntimeError("Strains were buffered but no sink was configured.")
            self.sink(indexed_buffer)

        self.buffer = set()

    def store_strains(self, strains: Iterable[StrainRef]) -> None:
        self.buffer.update(strains)

        if len(self.buffer) > 100:
            self._flush_buffer()

    def retrieve_strain_models(
        self,
        designations: Mapping[int, Collection[str]],
    ) -> dict[int, StrainRecord]:
        """Return the StrainInfo records resolved for each key's designations."""
        known_names = _index_names(designations)
        ids = self.get_strain_ids(list(known_names))
        return _match_records(known_names, self.get_strain_data(ids))

    def get_strain_ids(self, query: str | Sequence[str]) -> list[int]:
        if not query:
            return []

        if isinstance(query, str):
            query = (query,)

        response = self.request(self.strain_info_api_url(query))

        if response and isinstance(response[0], int):
            return cast(list[int], response)

        return []

    def get_strain_data(self, query: int | Iterable[int]) -> tuple[StrainRecord, ...]:
        """Retrieve the StrainInfo records for the strain IDs given in the argument."""
        try:
            data = self.request(self.strain_info_api_url(query))
        except ValueError:
            return ()

        return tuple(
            item["strain"] for item in cast(list[dict[str, StrainRecord]], data)
        )
