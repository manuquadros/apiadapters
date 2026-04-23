import pytest
import httpx
from unittest.mock import patch, AsyncMock
from apiadapters import retry_if_too_many_requests


def _status_error(status_code: int) -> httpx.HTTPStatusError:
    req = httpx.Request("GET", "https://example.com")
    response = httpx.Response(status_code, content=b"error", request=req)
    return httpx.HTTPStatusError(str(status_code), request=req, response=response)


def test_retry_backs_off_on_429_and_succeeds() -> None:
    calls = 0

    @retry_if_too_many_requests(is_async=False)
    def flaky():
        nonlocal calls
        calls += 1
        if calls < 3:
            raise _status_error(429)
        return "ok"

    with patch("apiadapters.apiadapters.time.sleep"):
        result = flaky()

    assert result == "ok"
    assert calls == 3


def test_retry_does_not_retry_404() -> None:
    calls = 0

    @retry_if_too_many_requests(is_async=False)
    def always_404():
        nonlocal calls
        calls += 1
        raise _status_error(404)

    with pytest.raises(httpx.HTTPStatusError):
        always_404()

    assert calls == 1


def test_retry_does_not_retry_500() -> None:
    calls = 0

    @retry_if_too_many_requests(is_async=False)
    def always_500():
        nonlocal calls
        calls += 1
        raise _status_error(500)

    with pytest.raises(httpx.HTTPStatusError):
        always_500()

    assert calls == 1


@pytest.mark.asyncio
async def test_async_retry_backs_off_on_429_and_succeeds() -> None:
    calls = 0

    @retry_if_too_many_requests(is_async=True)
    async def flaky():
        nonlocal calls
        calls += 1
        if calls < 3:
            raise _status_error(429)
        return "ok"

    with patch("apiadapters.apiadapters.sleep", new=AsyncMock(return_value=None)):
        result = await flaky()

    assert result == "ok"
    assert calls == 3


@pytest.mark.asyncio
async def test_async_retry_does_not_retry_404() -> None:
    calls = 0

    @retry_if_too_many_requests(is_async=True)
    async def always_404():
        nonlocal calls
        calls += 1
        raise _status_error(404)

    with pytest.raises(httpx.HTTPStatusError):
        await always_404()

    assert calls == 1
