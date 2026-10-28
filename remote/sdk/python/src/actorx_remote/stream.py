"""Explicitly owned, bounded async iteration for one remote Stream."""

from __future__ import annotations

import asyncio
import base64
import math
import time
import uuid
from typing import TYPE_CHECKING, Any

from . import protocol as p
from .errors import (
    AddressUncertainError,
    DeadlineExceededError,
    OutcomeUnknownError,
    ProtocolError,
    RemoteError,
    TransportError,
)
from .transports.http import check_status, frames, media_type, read_body
from .types import JSONValue

if TYPE_CHECKING:
    from .addr import Addr

WINDOW = {"items": 4, "bytes": 32768}
MAX_SEQ = 2**53 - 1


def sequence(value: object) -> int:
    if type(value) is not int or not 0 <= value <= MAX_SEQ:
        raise ProtocolError("Invalid stream sequence")
    return value


def token(value: object) -> str:
    try:
        if not isinstance(value, str):
            raise ValueError()
        raw = base64.b64decode(value + "=", altchars=b"-_", validate=True)
        if (
            len(raw) != 32
            or base64.urlsafe_b64encode(raw).decode().rstrip("=") != value
        ):
            raise ValueError()
    except (ValueError, TypeError):
        raise ProtocolError("Invalid stream token") from None
    return value


class Stream:
    """Use `async with addr.stream(...) as chunks`, or explicitly await aclose().

    Asking for the next item acknowledges the previously yielded item. A break
    needs context exit/aclose so cancellation never depends on garbage collection.
    """

    def __init__(self, addr: Addr, message: JSONValue, timeout: float) -> None:
        if (
            isinstance(timeout, bool)
            or not isinstance(timeout, (int, float))
            or not math.isfinite(timeout)
            or not 0 < timeout <= 60
        ):
            raise ValueError("Stream timeout must be in (0, 60] seconds")
        self.addr = addr
        self.value = p.decode(p.encode(message))
        self.timeout = timeout
        self.rpc_id = "c:" + uuid.uuid4().hex
        self.token: str | None = None
        self.deadline = 0.0
        self.queue: asyncio.Queue[tuple[int, JSONValue]] = asyncio.Queue(4)
        self.wake = asyncio.Event()
        self.terminal = False
        self.remote_terminal = False
        self.error: BaseException | None = None
        self.seq = 0
        self.previous = 0
        self.sizes: dict[int, int] = {}
        self.reader: asyncio.Task[None] | None = None
        self.closed = False
        self._close_task: asyncio.Task[None] | None = None
        self.dispatched = False
        self._next_lock = asyncio.Lock()

    async def __aenter__(self) -> Stream:
        self._start()
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.aclose()

    def __aiter__(self) -> Stream:
        if not self.closed:
            self._start()
        return self

    def _start(self) -> None:
        if self.closed:
            raise RuntimeError("Stream is closed")
        if self.reader is None:
            self.addr._runtime._check_open()
            self.deadline = asyncio.get_running_loop().time() + self.timeout
            self.addr._runtime._streams.add(self)
            self.reader = asyncio.create_task(self._read(), name="actorx-remote-stream")

    async def _control(self, method: str, seq: int | None = None) -> None:
        if self.token is None:
            return
        params: dict[str, Any] = {"request_id": self.rpc_id, "stream_token": self.token}
        if seq is not None:
            params["ack_seq"] = seq
        async with asyncio.timeout_at(self.deadline):
            await self.addr._runtime._http.reply(
                p.encode(p.notification(method, params)), None
            )

    async def __anext__(self) -> JSONValue:
        if self.closed:
            raise StopAsyncIteration
        self._start()
        async with self._next_lock:
            try:
                async with asyncio.timeout_at(self.deadline):
                    if self.previous:
                        # Requesting another item marks the previous one consumed,
                        # including when its terminal has already arrived.
                        seq, self.previous = self.previous, 0
                        self.sizes.pop(seq, None)
                        try:
                            await self._control("actor/credit", seq)
                        except (TransportError, TimeoutError):
                            if not self.terminal:
                                raise

                    while True:
                        if self.terminal and self.error is not None:
                            raise self.error
                        if not self.queue.empty():
                            self.previous, value = self.queue.get_nowait()
                            return value
                        if self.terminal:
                            await self.aclose()
                            raise StopAsyncIteration
                        self.wake.clear()
                        await self.wake.wait()
            except TimeoutError:
                await self.aclose()
                if self.remote_terminal:
                    raise DeadlineExceededError(
                        "deadline_exceeded",
                        1005,
                        "Stream consumption deadline exceeded",
                    ) from None
                if self.dispatched:
                    self.addr._unknown()
                    raise OutcomeUnknownError(
                        "outcome_unknown", 1004, "Stream has no valid terminal"
                    ) from None
                raise DeadlineExceededError(
                    "deadline_exceeded", 1005, "Stream expired before dispatch"
                ) from None
            except StopAsyncIteration:
                raise
            except BaseException:
                await self.aclose()
                raise

    async def _read(self) -> None:
        try:
            async with asyncio.timeout_at(self.deadline):
                async with self.addr._lock:
                    if self.addr._uncertain:
                        raise AddressUncertainError()
                    deadline_ms = int(
                        (
                            time.time()
                            + max(0, self.deadline - asyncio.get_running_loop().time())
                        )
                        * 1000
                    )
                    raw = p.encode(
                        p.request(
                            "actor/stream",
                            self.rpc_id,
                            {
                                "address": self.addr.address,
                                "input": self.value,
                                "deadline_unix_ms": deadline_ms,
                            },
                        )
                    )
                    self.dispatched = True
                    async with self.addr._runtime._http.open(
                        raw, register=True
                    ) as response:
                        check_status(response)
                        if media_type(response) == "application/json":
                            m = p.response(await read_body(response), self.rpc_id)
                            if "error" not in m:
                                raise ProtocolError("Stream requires SSE")
                            self._terminal(m)
                            return
                        if media_type(response) != "text/event-stream":
                            raise ProtocolError("Stream requires SSE")
                        async for raw_frame in frames(response):
                            m = p.envelope(raw_frame)
                            if self.token is None:
                                if m.get("method") != "actor/stream_open" or "id" in m:
                                    raise ProtocolError("Expected stream_open")
                                params = p.fields(
                                    m.get("params"),
                                    ("request_id", "stream_token", "window"),
                                    ("request_id", "stream_token", "window"),
                                )
                                if (
                                    type(params["request_id"]) is not str
                                    or params["request_id"] != self.rpc_id
                                    or params["window"] != WINDOW
                                    or any(
                                        type(v) is not int
                                        for v in params["window"].values()
                                    )
                                ):
                                    raise ProtocolError(
                                        "Invalid stream_open correlation/window"
                                    )
                                self.token = token(params["stream_token"])

                            elif "method" in m:
                                if m["method"] != "actor/chunk" or "id" in m:
                                    raise ProtocolError("Expected chunk notification")
                                c = p.fields(
                                    m.get("params"),
                                    (
                                        "request_id",
                                        "stream_token",
                                        "address",
                                        "seq",
                                        "value",
                                    ),
                                    (
                                        "request_id",
                                        "stream_token",
                                        "address",
                                        "seq",
                                        "value",
                                    ),
                                )
                                if (
                                    c["request_id"] != self.rpc_id
                                    or type(c["request_id"]) is not str
                                    or c["stream_token"] != self.token
                                    or sequence(c["seq"]) != self.seq + 1
                                ):
                                    raise ProtocolError(
                                        "Invalid chunk correlation/sequence"
                                    )

                                self.addr._save(c["address"])
                                if (
                                    len(self.sizes) >= 4
                                    or sum(self.sizes.values()) + len(raw_frame) > 32768
                                ):
                                    raise ProtocolError("Stream window exceeded")
                                self.seq += 1
                                self.sizes[self.seq] = len(raw_frame)
                                self.queue.put_nowait((self.seq, c["value"]))
                                self.wake.set()

                            else:
                                self._terminal(p.response(raw_frame, self.rpc_id))
                                return
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if isinstance(exc, TimeoutError) and not self.dispatched:
                self.error = DeadlineExceededError(
                    "deadline_exceeded", 1005, "Stream expired before dispatch"
                )
            elif isinstance(exc, RemoteError):
                self.error = exc
            elif not self.dispatched:
                self.error = exc
            elif (
                isinstance(exc, TransportError)
                and exc.status is not None
                and exc.status < 500
            ):
                self.dispatched = False
                self.error = exc
            else:
                # Dispatch without a valid terminal cannot establish whether
                # user code ran, even if the connection ended cleanly.
                if self.dispatched:
                    self.addr._unknown()
                self.error = OutcomeUnknownError(
                    "outcome_unknown", 1004, "Stream has no valid terminal"
                )
                self.error.__cause__ = exc
            self.terminal = True
        finally:
            self.wake.set()

    def _terminal(self, m: p.Message) -> None:
        if "error" in m:
            e = p.remote_error(m["error"])
            # 1013 is definite only with a fixed identity and zero output.
            if e.rpc_code == 1013 and (
                not isinstance(e.data, dict)
                or "address" not in e.data
                or sequence(e.data.get("final_seq")) != 0
                or self.seq != 0
            ):
                raise ProtocolError("Invalid unsupported operation terminal")

            if isinstance(e.data, dict):
                if "address" in e.data:
                    self.addr._save(e.data["address"])
                if (
                    self.token is not None
                    and sequence(e.data.get("final_seq")) != self.seq
                ):
                    raise ProtocolError("Invalid error final sequence")
            elif self.token is not None:
                raise ProtocolError("Missing stream error sequence")

            if "generation" not in self.addr._address and e.rpc_code in (
                1000,
                -32603,
                1012,
            ):
                self.addr._unknown()
                raise OutcomeUnknownError(
                    "outcome_unknown", 1004, "Stream has no selected address evidence"
                ) from e
            if e.rpc_code in (1003, 1004):
                self.addr._unknown()
            self.error = e
        else:
            result = p.fields(
                m["result"], ("address", "final_seq"), ("address", "final_seq")
            )
            self.addr._save(result["address"])
            if sequence(result["final_seq"]) != self.seq:
                raise ProtocolError("Invalid final sequence")

        self.remote_terminal = True
        self.terminal = True
        self.wake.set()

    async def aclose(self) -> None:
        if self._close_task is None:
            self.closed = True
            self._close_task = asyncio.create_task(
                self._close(), name="actorx-remote-stream-close"
            )
        # A canceled closer must not abandon cancellation and reader cleanup.
        await asyncio.shield(self._close_task)

    async def _close(self) -> None:
        if not self.remote_terminal:
            try:
                await self._control("actor/cancel")
            except (TransportError, TimeoutError):
                pass
            if self.dispatched and "generation" not in self.addr._address:
                self.addr._unknown()
        if self.reader is not None:
            self.reader.cancel()
            await asyncio.gather(self.reader, return_exceptions=True)
        self.addr._runtime._streams.discard(self)
        if not self.terminal:
            self.terminal = True
            self.error = RemoteError("canceled", 1009, "Stream was closed locally")
        self.wake.set()
