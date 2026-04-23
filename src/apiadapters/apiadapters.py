import logging
import time
from asyncio import Semaphore, sleep
from functools import wraps
from logging.handlers import RotatingFileHandler
from threading import Semaphore as ThreadSemaphore
from types import TracebackType
from typing import Any, Callable, Self, TypeVar, overload

import httpx

T = TypeVar("T")


def file_logger(
    filename: str,
    level: int = logging.DEBUG,
) -> logging.Logger:
    """Return a logger with file rotation."""
    ologger = logging.getLogger(__name__)
    ologger.setLevel(level)

    handler = RotatingFileHandler(
        filename=filename,
        maxBytes=512000,
        backupCount=5,
    )
    handler.setFormatter(
        logging.Formatter(
            fmt="%(asctime)s, %(module)s.%(funcName)s, %(levelname)s, %(message)s",
            datefmt="%d %b %Y %H:%M:%S",
        ),
    )
    ologger.addHandler(handler)

    return ologger


def stderr_logger(level: int = logging.DEBUG) -> logging.Logger:
    """Create a simple stderr logger for debugging purposes."""
    ologger = logging.getLogger(__name__)
    ologger.setLevel(level)

    handler = logging.StreamHandler()
    handler.setFormatter(
        logging.Formatter(
            fmt="%(asctime)s, %(module)s.%(funcName)s, %(levelname)s, %(message)s",
            datefmt="%H:%M:%S",
        ),
    )
    ologger.addHandler(handler)

    return ologger


def retry_if_too_many_requests(is_async: bool = True):
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        if is_async:

            async def handler(exception: Exception, retry_count: int) -> bool:
                if isinstance(exception, httpx.HTTPStatusError):
                    if exception.response.status_code == 429:
                        await sleep(min(30 * (2**retry_count), 3600))
                        return True
                return False

            @wraps(func)
            async def async_wrapped(*args: Any, **kwargs: Any) -> T:
                retry_count = 0
                while True:
                    try:
                        return await func(*args, **kwargs)
                    except httpx.HTTPError as e:
                        retry_count += 1
                        if not await handler(e, retry_count) or retry_count > 5:
                            raise

        else:

            def sync_handler(exception: Exception, retry_count: int) -> bool:
                if isinstance(exception, httpx.HTTPStatusError):
                    if exception.response.status_code == 429:
                        time.sleep(min(30 * (2**retry_count), 3600))
                        return True
                return False

            @wraps(func)
            def sync_wrapped(*args: Any, **kwargs: Any) -> T:
                retry_count = 0
                while True:
                    try:
                        return func(*args, **kwargs)
                    except httpx.HTTPError as e:
                        retry_count += 1
                        if not sync_handler(e, retry_count) or retry_count > 5:
                            raise

        return async_wrapped if is_async else sync_wrapped

    return decorator


class BaseAPIAdapter:
    """Base class for API adapters with common functionality."""

    def __init__(
        self, headers: dict[str, str] | None = None, rate_limit: int = 3
    ) -> None:
        if headers is None:
            headers = {}
        self.headers = headers
        self.rate_limit = rate_limit
        self.last_request_time: dict[str, float] = {}
        self.min_delay = 0.4


class AsyncAPIAdapter(BaseAPIAdapter):
    """Async version of the API adapter."""

    def __init__(
        self, headers: dict[str, str] | None = None, rate_limit: int = 3
    ) -> None:
        if headers is None:
            headers = {}
        super().__init__(headers, rate_limit)
        self.client = httpx.AsyncClient(
            headers=headers,
            timeout=100,
            follow_redirects=True,
        )
        self.semaphore = Semaphore(rate_limit)

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        await self.client.aclose()

    @retry_if_too_many_requests(is_async=True)
    async def request(
        self, url: str, handler: Callable[[httpx.Response], Any] | None = None
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


class APIAdapter(BaseAPIAdapter):
    """Synchronous version of the API adapter."""

    def __init__(
        self, headers: dict[str, str] | None = None, rate_limit: int = 3
    ) -> None:
        if headers is None:
            headers = {}
        super().__init__(headers, rate_limit)
        self.client = httpx.Client(
            headers=headers,
            timeout=100,
            follow_redirects=True,
        )
        self.semaphore = ThreadSemaphore(rate_limit)

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.client.close()

    @retry_if_too_many_requests(is_async=False)
    def request(
        self, url: str, handler: Callable[[httpx.Response], Any] | None = None
    ) -> Any:
        domain = str(httpx.URL(url).host)

        with self.semaphore:
            now = time.time()
            last_req = self.last_request_time.get(domain, 0)
            if now - last_req < self.min_delay:
                time.sleep(self.min_delay - (now - last_req))
            self.last_request_time[domain] = time.time()

            response = self.client.get(url)

            if handler:
                return handler(response)
            return response
