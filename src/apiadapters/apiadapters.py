import logging
import time
from asyncio import Semaphore, sleep
from collections.abc import Callable
from functools import wraps
from logging.handlers import RotatingFileHandler
from threading import Semaphore as ThreadSemaphore
from types import TracebackType
from typing import Any, Callable, Self, TypeVar, overload

import httpx
from tenacity import AsyncRetrying, Retrying, retry_if_exception, stop_after_attempt

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


def _is_too_many_requests(exception: BaseException) -> bool:
    return (
        isinstance(exception, httpx.HTTPStatusError)
        and exception.response.status_code == 429
    )


def _backoff_seconds(retry_state: Any) -> float:
    """Honor Retry-After when present, otherwise back off exponentially."""
    exc = retry_state.outcome.exception() if retry_state.outcome else None
    retry_after = (
        exc.response.headers.get("retry-after")
        if isinstance(exc, httpx.HTTPStatusError)
        else None
    )
    if retry_after is not None:
        try:
            return float(retry_after)
        except ValueError:
            pass
    return min(30 * (2**retry_state.attempt_number), 3600)


def retry_if_too_many_requests(is_async: bool = True):
    """Retry a request on HTTP 429; any other error propagates immediately."""

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        if is_async:

            @wraps(func)
            async def async_wrapped(*args: Any, **kwargs: Any) -> T:
                retrying = AsyncRetrying(
                    retry=retry_if_exception(_is_too_many_requests),
                    wait=_backoff_seconds,
                    stop=stop_after_attempt(6),
                    sleep=sleep,
                    reraise=True,
                )
                return await retrying(func, *args, **kwargs)

            return async_wrapped

        @wraps(func)
        def sync_wrapped(*args: Any, **kwargs: Any) -> T:
            retrying = Retrying(
                retry=retry_if_exception(_is_too_many_requests),
                wait=_backoff_seconds,
                stop=stop_after_attempt(6),
                sleep=time.sleep,
                reraise=True,
            )
            return retrying(func, *args, **kwargs)

        return sync_wrapped

    return decorator


class BaseAPIAdapter:
    """Base class for API adapters with common functionality."""

    def __init__(
        self, headers: dict[str, str] = {}, rate_limit: int = 3
    ) -> None:
        self.headers = headers
        self.rate_limit = rate_limit
        self.last_request_time: dict[str, float] = {}
        self.min_delay = 0.4


class AsyncAPIAdapter(BaseAPIAdapter):
    """Async version of the API adapter."""

    def __init__(
        self, headers: dict[str, str] = {}, rate_limit: int = 3
    ) -> None:
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
        self, headers: dict[str, str] = {}, rate_limit: int = 3
    ) -> None:
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
