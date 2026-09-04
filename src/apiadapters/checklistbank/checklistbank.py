import httpx
import tinydb
from tinydb import TinyDB

from apiadapters import file_logger, stderr_logger

api_root = "https://api.checklistbank.org/"


class ChecklistBankAdapterBase:
    """Base class with shared ChecklistBank API adapter functionality"""

    def __init__(self) -> None:
        self.logger = file_logger(filename="checklistbank.log")
        self.buffer: dict = dict()
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
                    "%s not found in ChecklistBank.", url.split("/")[-1]
                )
                return []
            case _:
                raise response.raise_for_status()

    def _flush_buffer(self) -> None:
        self.logger.info("Flushing ChecklistBank buffer")

        self.buffer = dict()

    def _store_data(self, data: dict, table: str) -> None:
        return self.storage.table(table).insert(data)

    @staticmethod
    def dataset_url(dataset_key: str):
        return api_root + f"dataset/{dataset_key}"

    @staticmethod
    def dataset_name_usage_url(dataset_key: str, name: str):
        return (
            api_root
            + f"dataset/{dataset_key}/match/nameusage?scientificName={name}&verbose=false"
        )
