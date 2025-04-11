import time
import httpx
from log import stderr_logger
from asyncio import Semaphore, sleep
from types import TracebackType
from typing import Any, Self

from collections.abc import Callable
from functools import wraps


def retry_if_too_many_requests(func: Callable) -> Callable:

    async def handler(exception: Exception, retry_count):
        logger = stderr_logger()
        logger.debug("Retrying")
        msg = (
            f"retry_count: {retry_count}. "
            f"Waiting for {30 * (2**retry_count)} secs."
        )
        logger.debug(msg)

        if isinstance(exception, httpx.HTTPStatusError):
            if (
                hasattr(exception, "response")
                and exception.response.status_code == 429
            ):
                await sleep(min(30 * (2**retry_count), 3600))

            return True

        return False

    @wraps(func)
    async def wrapped(*args, **kwargs):
        retry_count = 0
        while True:
            try:
                return await func(*args, **kwargs)
            except httpx.HTTPError as e:
                retry_count += 1
                if not await handler(e, retry_count) or retry_count > 5:
                    raise

    return wrapped


class APIAdapter:
    """General context manager for API connections.

    Subclasses can initialize the headers parameter of the parent.
    """

    def __init__(
        self, headers: dict[str, str] = {}, rate_limit: int = 3
    ) -> None:
        self.client = httpx.AsyncClient(
            headers=headers,
            timeout=30.0,
            follow_redirects=True,
        )
        self.semaphore = Semaphore(rate_limit)
        self.last_request_time: dict[str, float] = {}
        self.min_delay = 0.4
        self.logger = stderr_logger()

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        await self.client.aclose()

    @retry_if_too_many_requests
    async def request(
        self, url: str, handler: Callable[[httpx.Response], Any] | None
    ) -> Any:
        domain = str(httpx.URL(url).host)

        async with self.semaphore:
            now = time.time()
            last_req = self.last_request_time.get(domain, 0)
            if now - last_req < self.min_delay:
                await sleep(self.min_delay - (now - last_req))
            self.last_request_time[domain] = time.time()

            response = await self.client.get(url)

            if handler:
                return handler(response)
            return response
