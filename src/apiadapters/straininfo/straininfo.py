import logging
import re
from collections.abc import Collection, Iterable, Mapping, Sequence
from typing import cast

import httpx
from apiadapters import APIAdapter, AsyncAPIAdapter, stderr_logger
from d3types import Strain
from pydantic import ValidationError

logger = logging.getLogger(__name__)

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
            standardized.update(map(str.strip, substrings))

    return set(strain_names) | standardized


class StrainInfoAdapterBase:
    """Base class with shared StrainInfo adapter functionality."""

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
                response.raise_for_status()

    @staticmethod
    def strain_info_api_url(query: str | int | Iterable[str] | Iterable[int]) -> str:
        if isinstance(query, (str, int)):
            query = [query]

        items = list(query)
        if not items:
            raise ValueError("query must not be empty")

        match type(items[0]).__name__:
            case "str":
                root = api_root + "search/strain/str_des/"
            case "int":
                root = api_root + "data/strain/max/"
            case _:
                raise httpx.InvalidURL(f"Unsupported query item type: {type(items[0])}")

        return root + ",".join(map(str, items))


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

    async def request(self, url: str) -> list[dict] | list[int]:
        response = await super().request(url)
        return self._response_handler(url, response)

    async def retrieve_strain_models(
        self,
        strains: Mapping[int, Strain],
    ) -> dict[int, Strain]:
        known_names: dict[str, int] = {
            name: ix
            for ix, model in strains.items()
            for name in normalize_strain_names(model.designations)
        }

        ids: list[int] = await self.get_strain_ids(list(known_names.keys()))
        straininfo_data: tuple[Strain, ...] = await self.get_strain_data(ids)

        result: dict[int, Strain] = dict(strains)
        for entry in straininfo_data:
            names: frozenset[str] = entry.designations | frozenset(
                cult.strain_number for cult in entry.cultures
            )
            try:
                keyname = next(filter(lambda w: w in known_names, names))
                result[known_names[keyname]] = entry.model_copy()
            except StopIteration:
                pass

        return result

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
            logger.error("ValidationError parsing strain data: %s", data)
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

    def request(self, url: str) -> list[dict] | list[int]:
        response = super().request(url)
        return self._response_handler(url, response)

    def retrieve_strain_models(
        self,
        strains: Mapping[int, Strain],
    ) -> dict[int, Strain]:
        known_names: dict[str, int] = {
            name: ix
            for ix, model in strains.items()
            for name in normalize_strain_names(model.designations)
        }

        ids: list[int] = self.get_strain_ids(list(known_names.keys()))
        straininfo_data: tuple[Strain, ...] = self.get_strain_data(ids)

        result: dict[int, Strain] = dict(strains)
        for entry in straininfo_data:
            names: frozenset[str] = entry.designations | frozenset(
                cult.strain_number for cult in entry.cultures
            )
            try:
                keyname = next(filter(lambda w: w in known_names, names))
                result[known_names[keyname]] = entry.model_copy()
            except StopIteration:
                pass

        return result

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
            logger.error("ValidationError parsing strain data: %s", data)
            raise e
