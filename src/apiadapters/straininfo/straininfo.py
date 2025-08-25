import re
from collections.abc import Collection, Iterable, MutableMapping, Sequence
from functools import singledispatchmethod
from types import TracebackType
from typing import Any, Self, cast

import httpx
import tinydb
from apiadapters import APIAdapter, AsyncAPIAdapter, BaseAPIAdapter
from d3types import Strain, StrainRef
from loggers import stderr_logger
from pydantic import ValidationError
from tinydb import TinyDB

api_root = "https://api.straininfo.dsmz.de/v1/"


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

    def __init__(self) -> None:
        self.buffer: set[StrainRef] = set()
        self.storage: TinyDB

    @staticmethod
    def _response_handler(
        url: str,
        response: httpx.Response,
    ) -> list[dict] | list[int]:
        match response.status_code:
            case 200:
                return response.json()
            case 404:
                stderr_logger().error(
                    "%s not found on StrainInfo.", url.split("/")[-1]
                )
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
    def __init__(self):
        AsyncAPIAdapter.__init__(
            self,
            headers={
                "Accept": "application/json",
                "Cache-Control": "no-store",
                "Accept-Encoding": "gzip, deflate",
            },
        )
        StrainInfoAdapterBase.__init__(self)

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
        strains: MutableMapping[int, Strain],
    ) -> dict[int, Strain]:
        # Map each possible strain designation from the normalized name of the model
        # to the id of the model.
        known_names: dict[str, int] = {
            name: ix
            for ix, model in strains.items()
            for name in normalize_strain_names(model.designations)
        }

        ids: list[int] = await self.get_strain_ids(list(known_names.keys()))
        straininfo_data: tuple[Strain, ...] = await self.get_strain_data(ids)

        # Update the _Strain models with Straininfo information if available
        for entry in straininfo_data:
            names: frozenset[str] = entry.designations | frozenset(
                cult.strain_number for cult in entry.cultures
            )
            try:
                keyname = next(filter(lambda w: w in known_names, names))
                strains[known_names[keyname]] = entry.model_copy()
            except StopIteration:
                pass

        return strains

    async def __flush_buffer(self) -> None:
        """Store _Strain models into self.storage.

        Strain models might have unnormalized strain designations, like
        'HBB / ATCC 27634 / DSM 579'. The method will extract the normalized
        designations from such a name and try to retrieve data about them from
        StrainInfo.
        """
        print("Flushing strain buffer")

        indexed_buffer: dict[int, Strain] = {
            model.id: Strain(designations=normalize_strain_names(model.name))  # type: ignore[call-arg]
            for model in self.buffer
        }

        indexed_buffer = await self.retrieve_strain_models(indexed_buffer)

        for key, strain in indexed_buffer.items():
            self.storage.table("strains").upsert(
                tinydb.table.Document(strain.model_dump(), doc_id=key),
            )

        self.buffer = set()

    async def store_strains(self, strains: Iterable[StrainRef]) -> None:
        self.buffer.update(strains)

        if len(self.buffer) > 100:
            await self.__flush_buffer()

    async def request(self, url: str) -> list[dict] | list[int]:
        response = await super().request(url)
        return self._response_handler(url, response)

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
    ) -> tuple[Strain, ...]:
        """Retrieve StrainInfo data for the strain IDs given in the argument.

        :param query: IDs to be queried through the API.

        :return: Tuple containing Strain models encapsulating the information
            retrieved from StrainInfo.
        """
        try:
            data = await self.request(self.strain_info_api_url(query))
        except ValueError:
            return ()

        try:
            return tuple(
                Strain(
                    **item["strain"],
                    cultures=item["strain"]["relation"].get(
                        "culture", frozenset()
                    ),
                    designations=item["strain"]["relation"].get(
                        "designation",
                        frozenset(),
                    ),
                )
                for item in data
            )
        except ValidationError as e:
            print(data)
            raise e


class StrainInfoAdapter(APIAdapter, StrainInfoAdapterBase):
    """Synchronous version of the StrainInfo adapter."""

    def __init__(self):
        APIAdapter.__init__(
            self,
            headers={
                "Accept": "application/json",
                "Cache-Control": "no-store",
                "Accept-Encoding": "gzip, deflate",
            },
        )
        StrainInfoAdapterBase.__init__(self)

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
        """Store Strain models into self.storage.

        Strain models might have unnormalized strain designations, like
        'HBB / ATCC 27634 / DSM 579'. The method will extract the normalized
        designations from such a name and try to retrieve data about them from
        StrainInfo.
        """
        print("Flushing strain buffer")

        indexed_buffer: dict[int, Strain] = {
            model.id: Strain(designations=normalize_strain_names(model.name))  # type: ignore[call-arg]
            for model in self.buffer
        }

        indexed_buffer = self.retrieve_strain_models(indexed_buffer)

        for key, strain in indexed_buffer.items():
            self.storage.table("strains").upsert(
                tinydb.table.Document(strain.model_dump(), doc_id=key),
            )

        self.buffer = set()

    def store_strains(self, strains: Iterable[StrainRef]) -> None:
        self.buffer.update(strains)

        if len(self.buffer) > 100:
            self._flush_buffer()

    def retrieve_strain_models(
        self,
        strains: MutableMapping[int, Strain],
    ) -> dict[int, Strain]:
        # Map each possible strain designation from the normalized name of the model
        # to the id of the model.
        known_names: dict[str, int] = {
            name: ix
            for ix, model in strains.items()
            for name in normalize_strain_names(model.designations)
        }

        ids: list[int] = self.get_strain_ids(list(known_names.keys()))
        straininfo_data: tuple[Strain, ...] = self.get_strain_data(ids)

        # Update the Strain models with Straininfo information if available
        for entry in straininfo_data:
            names: frozenset[str] = entry.designations | frozenset(
                cult.strain_number for cult in entry.cultures
            )
            try:
                keyname = next(filter(lambda w: w in known_names, names))
                strains[known_names[keyname]] = entry.model_copy()
            except StopIteration:
                pass

        return strains

    def get_strain_ids(self, query: str | Sequence[str]) -> list[int]:
        if not query:
            return []

        if isinstance(query, str):
            query = (query,)

        response = self.request(self.strain_info_api_url(query))

        if response and isinstance(response[0], int):
            return cast(list[int], response)

        return []

    def get_strain_data(self, query: int | Iterable[int]) -> tuple[Strain, ...]:
        """Retrieve StrainInfo data for the strain IDs given in the argument.

        :param query: IDs to be queried through the API.

        :return: Tuple containing Strain models encapsulating the information
            retrieved from StrainInfo.
        """
        try:
            data = self.request(self.strain_info_api_url(query))
        except ValueError:
            return ()

        try:
            return tuple(
                Strain(
                    **item["strain"],
                    cultures=item["strain"]["relation"].get(
                        "culture", frozenset()
                    ),
                    designations=item["strain"]["relation"].get(
                        "designation",
                        frozenset(),
                    ),
                )
                for item in data
            )
        except ValidationError as e:
            print(data)
            raise e
