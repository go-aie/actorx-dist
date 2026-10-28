"""Bounded HTTP POSTs and a registration SSE stream; no redirects/retries."""

from __future__ import annotations

import re
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import cast

import httpx

from .. import protocol as p
from ..errors import AuthenticationError, ProtocolError, SessionError, TransportError


def check_status(response: httpx.Response, expected: int = 200) -> None:
    if response.status_code == expected:
        return
    cls = {401: AuthenticationError, 403: SessionError}.get(response.status_code, TransportError)
    raise cls("HTTP request rejected", status=response.status_code)


def media_type(response: httpx.Response) -> str:
    return cast(str, response.headers.get("content-type", "").split(";", 1)[0].strip().lower())


async def read_body(response: httpx.Response) -> bytes:
    body = bytearray()
    async for chunk in response.aiter_bytes():
        body.extend(chunk)
        if len(body) > p.MAX_BYTES:
            raise ProtocolError("HTTP response exceeds 8 KiB")
    return bytes(body)


async def frames(response: httpx.Response) -> AsyncIterator[bytes]:
    # Do not use aiter_lines: a peer could send an unbounded line before '\n'.
    pending = bytearray()
    async for chunk in response.aiter_bytes():
        pending.extend(chunk)
        while b"\n\n" in pending:
            raw, _, tail = pending.partition(b"\n\n")
            pending[:] = tail
            if not raw.startswith(b"data: ") or b"\n" in raw or len(raw) > p.MAX_BYTES + 6:
                raise ProtocolError("Invalid SSE frame")
            yield bytes(raw[6:])
        if len(pending.rstrip(b"\n")) > p.MAX_BYTES + 6:
            raise ProtocolError("SSE frame exceeds limit")

    # EOF, including a partial final frame, is never a terminal RPC result.
    raise TransportError("Provider connection ended")


class HTTPTransport:
    def __init__(self, url: str, api_key: str) -> None:
        try:
            parsed = httpx.URL(url)
        except (httpx.InvalidURL, TypeError) as exc:
            raise ValueError("Invalid gateway URL") from exc
        if (
            parsed.scheme not in ("http", "https")
            or not parsed.host
            or parsed.userinfo
            or parsed.query
            or parsed.fragment
            or parsed.path not in ("", "/")
        ):
            raise ValueError("Expected gateway origin URL without credentials")
        if not isinstance(api_key, str) or not re.fullmatch(r"[A-Za-z0-9._~+/-]+=*", api_key):
            raise ValueError("Valid API key required")

        self._url = str(parsed).rstrip("/") + "/runtime/rpc"
        self._headers = {
            "Authorization": "Bearer " + api_key,
            "Content-Type": "application/json",
            "X-Actorx-Remote-Protocol": p.VERSION,
        }
        self._client = httpx.AsyncClient(timeout=None, follow_redirects=False, trust_env=False)

    @asynccontextmanager
    async def open(
        self, raw: bytes, *, register: bool = False, session: str | None = None
    ) -> AsyncIterator[httpx.Response]:
        headers = dict(self._headers)
        headers["Accept"] = (
            "text/event-stream, application/json" if register else "application/json"
        )
        if session is not None:
            headers["X-Actorx-Remote-Session"] = session

        try:
            async with self._client.stream(
                "POST", self._url, content=raw, headers=headers
            ) as response:
                yield response
        except (httpx.ConnectError, httpx.ConnectTimeout, httpx.PoolTimeout):
            # No connection, hence no actor/call body delivered.
            raise TransportError("Cannot connect to gateway", status=0) from None
        except httpx.HTTPError:
            raise TransportError("HTTP transport failed") from None

    async def call(self, raw: bytes) -> bytes:
        async with self.open(raw) as response:
            check_status(response)
            if media_type(response) != "application/json":
                raise ProtocolError("Expected JSON RPC response")
            return await read_body(response)

    async def reply(self, raw: bytes, session: str | None) -> None:
        async with self.open(raw, session=session) as response:
            # 409 is a late response rejection: no retry and no new RPC result.
            if response.status_code == 409:
                return
            check_status(response, 204)

    async def close(self) -> None:
        await self._client.aclose()
