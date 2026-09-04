from collections.abc import Iterable
from types import TracebackType

from .checklistbank import ChecklistBankAdapterBase
from apiadapters import APIAdapter, AsyncAPIAdapter

headers = {
    "Accept": "application/json",
    "Cache-Control": "no-store",
    "Accept-Encoding": "gzip, deflate",
}


class WFOAdapterBase(ChecklistBankAdapterBase):
    """Base class with shared functionality of ChecklistBank API adapter for WFO dataset."""

    def __init__(self, dataset_key: str) -> None:
        """Initialise shared functionality of the WFOAdapter

        :param dataset_key: current dataset key for the WFO dataset as string, found on the ChecklistBank website
        https://www.checklistbank.org/dataset?full=false&limit=25&offset=0&origin=external&origin=project&q=wfo%20plant%20list
        """
        super().__init__()

        self.dataset_key = dataset_key

    def store_data(self, data: dict, table: str) -> None:
        return self._store_data(data=data, table=table)


class AsyncWFOAdapter(AsyncAPIAdapter, WFOAdapterBase):
    """Asynchronous version of the ChecklistBank API adapter for WFO dataset."""

    def __init__(self, dataset_key: str, headers: dict = headers, rate_limit: int = 3):
        AsyncAPIAdapter.__init__(self, headers, rate_limit)
        WFOAdapterBase.__init__(self, dataset_key)

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self._flush_buffer()
        await super().__aexit__(exc_type, exc_value, exc_tb)

    async def request(self, url: str) -> list[dict] | list[int]:
        response = await super().request(url)
        return self._response_handler(url, response)

    async def retrieve_dataset_name_usage_for_queries(
        self,
        queries: Iterable[str],
    ) -> dict[str, dict]:
        """Retrieve metadata and name usage data for the dataset from the ChecklistBank API.

        :param queries: list of plant names to be retrieved from the ChecklistBank API.

        :return: dict with name usage data retrieved from the ChecklistBank API.
        """
        self.logger.info("Retrieving name usage data for WFO")

        dataset_metadata = await self.fetch_dataset()
        self.store_data(dataset_metadata, "dataset")

        for query in queries:
            name_usage_data = await self.fetch_dataset_name_usage(query)
            self.store_data(name_usage_data, "name_usage")

            self.buffer[query] = name_usage_data

        return self.buffer

    async def fetch_dataset(self) -> dict:
        """Fetch WFO dataset metadata.

        :return: dict with information retrieved from the ChecklistBank API.
        """
        try:
            data = await self.request(self.dataset_url(self.dataset_key))
            return data
        except ValueError:
            return ()

    async def fetch_dataset_name_usage(self, query: str) -> dict:
        """Fetch WFO name usage (classification) data for the plant name given in the argument.

        :param query: plant name to be queried through the ChecklistBank API.

        :return: dict with information retrieved from the ChecklistBank API.
        """
        try:
            data = await self.request(
                self.dataset_name_usage_url(self.dataset_key, query)
            )
            return data
        except ValueError:
            return ()


class WFOAdapter(APIAdapter, WFOAdapterBase):
    """Synchronous version of the ChecklistBank API adapter for WFO dataset."""

    def __init__(self, dataset_key: str, headers: dict = headers, rate_limit: int = 3):
        APIAdapter.__init__(self, headers, rate_limit)
        WFOAdapterBase.__init__(self, dataset_key)

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

    def retrieve_dataset_name_usage_for_queries(
        self,
        queries: Iterable[str],
    ) -> dict[str, dict]:
        """Retrieve metadata and name usage data for the dataset from the ChecklistBank API.

        :param queries: list of plant names to be retrieved from the ChecklistBank API.

        :return: dict with name usage data retrieved from the ChecklistBank API.
        """
        self.logger.info("Retrieving name usage data for WFO")

        dataset_metadata = self.fetch_dataset()
        self.store_data(dataset_metadata, "dataset")

        for query in queries:
            name_usage_data = self.fetch_dataset_name_usage(query)
            self.store_data(name_usage_data, "name_usage")

            self.buffer[query] = name_usage_data

        return self.buffer

    def fetch_dataset(self) -> dict:
        """Fetch WFO dataset metadata.

        :return: dict with information retrieved from the ChecklistBank API.
        """
        try:
            data = self.request(self.dataset_url(self.dataset_key))
            return data
        except ValueError:
            return ()

    def fetch_dataset_name_usage(self, query: str) -> dict:
        """Fetch WFO name usage (classification) data for the plant name given in the argument.

        :param query: plant name to be queried through the ChecklistBank API.

        :return: dict with information retrieved from the ChecklistBank API.
        """
        try:
            data = self.request(self.dataset_name_usage_url(self.dataset_key, query))
            return data
        except ValueError:
            return ()
