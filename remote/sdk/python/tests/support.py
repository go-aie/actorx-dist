from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from types import EllipsisType
from typing import TypeAlias, TypeVar, cast

import httpx
from actorx_remote import Address, Runtime
from actorx_remote import protocol as p
from actorx_remote.types import RPCID

T = TypeVar("T")
HTTPHandler: TypeAlias = Callable[[httpx.Request, p.Message], Awaitable[httpx.Response]]


ADDRESS: Address = {
    "version": 1,
    "type": "demo.ping",
    "id": "session-42",
    "generation": "AAAAAAAAAAAAAAAAAAAAAA",
}


async def bounded(awaitable: Awaitable[T]) -> T:
    return await asyncio.wait_for(awaitable, 2)


class Stream(httpx.AsyncByteStream):
    def __init__(self) -> None:
        self.queue: asyncio.Queue[bytes | None] = asyncio.Queue()
        self.closed = asyncio.Event()

    async def __aiter__(self) -> AsyncIterator[bytes]:
        while True:
            chunk = await self.queue.get()
            if chunk is None:
                return
            yield chunk

    async def aclose(self) -> None:
        self.closed.set()

    def send(self, message: p.Message) -> None:
        self.queue.put_nowait(b"data: " + p.encode(message) + b"\n\n")


class Gateway:
    """Controllable HTTP endpoint, including a genuinely streaming response.

    httpx.MockTransport keeps SDK tests free of sockets/NATS while exercising
    actual HTTP requests, headers, byte streaming, POSTs and response closure.
    """

    def __init__(self) -> None:
        self.stream = Stream()
        self.requests: list[tuple[httpx.Request, p.Message]] = []
        self.responses: asyncio.Queue[p.Message] = asyncio.Queue()
        self.calls: asyncio.Queue[p.Message] = asyncio.Queue()
        self.call_response: HTTPHandler | None = None
        self.register_response: HTTPHandler | None = None
        self.reply_status = 204
        self.counter = 0

    async def handle(self, request: httpx.Request) -> httpx.Response:
        assert request.method == "POST"
        assert request.url.path == "/runtime/rpc"
        assert request.headers["Authorization"] == "Bearer test-key"
        assert request.headers["Content-Type"] == "application/json"
        assert request.headers["X-Actorx-Remote-Protocol"] == p.VERSION
        message = p.envelope(request.content)
        self.requests.append((request, message))

        # Provider replies travel through their session; requests do not.
        if "method" not in message:
            assert request.headers["X-Actorx-Remote-Session"] == "session-token"
            self.responses.put_nowait(message)
            return httpx.Response(self.reply_status)

        assert "X-Actorx-Remote-Session" not in request.headers
        if message["method"] == "runtime/register":
            assert request.headers["Accept"] == "text/event-stream, application/json"
            if self.register_response:
                return await self.register_response(request, message)
            self.stream.send(
                p.success(
                    message["id"],
                    {
                        "protocol": p.VERSION,
                        "type": message["params"]["type"],
                        "session_token": "session-token",
                    },
                )
            )
            return httpx.Response(
                200, headers={"Content-Type": "text/event-stream"}, stream=self.stream
            )

        # Consumer calls can be held or replaced by each test's response hook.
        self.calls.put_nowait(message)
        if self.call_response:
            return await self.call_response(request, message)
        a = dict(
            ADDRESS,
            type=message["params"]["address"]["type"],
            id=message["params"]["address"]["id"],
        )
        return httpx.Response(
            200,
            content=p.encode(
                p.success(
                    message["id"],
                    {
                        "address": a,
                        "value": message["params"]["input"],
                    },
                )
            ),
            headers={"Content-Type": "application/json"},
        )

    async def install(self, runtime: Runtime) -> None:
        await runtime._http._client.aclose()
        runtime._http._client = httpx.AsyncClient(
            transport=httpx.MockTransport(self.handle), follow_redirects=False, timeout=None
        )

    def send(
        self,
        method: str = "actor/invoke",
        *,
        address: Mapping[str, object] | None = None,
        value: object = None,
        ms: int | None = None,
        rpc_id: RPCID | EllipsisType = ...,
    ) -> RPCID:
        self.counter += 1
        params = {
            "address": address or dict(ADDRESS),
            "deadline_unix_ms": ms if ms is not None else int(time.time() * 1000) + 5000,
        }
        if method == "actor/invoke":
            params["input"] = value

        message = p.request(method, "g:" + str(self.counter) if rpc_id is ... else rpc_id, params)
        self.stream.send(message)
        return cast(RPCID, message["id"])

    async def response(self, rpc_id: RPCID = None) -> p.Message:
        response = await bounded(self.responses.get())
        if rpc_id is not None:
            assert response["id"] == rpc_id
        return response
