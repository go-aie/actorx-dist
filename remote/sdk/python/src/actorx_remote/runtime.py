"""Runtime-owned provider registrations, execution slots and HTTP lifetime."""

from __future__ import annotations

import asyncio
import inspect
import time
import uuid
from collections.abc import Coroutine
from dataclasses import dataclass, field
from typing import Any, cast

from . import protocol as p
from .addr import Addr
from .errors import ProtocolError, RemoteError
from .transports.http import HTTPTransport, check_status, frames, media_type, read_body
from .types import RPCID, Actor, ActorFactory, Address, JSONValue

REGISTER_SECONDS = 5.0


@dataclass
class _Instance:
    actor: Actor | None = None
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)


@dataclass(eq=False)
class _Work:
    message: p.Message
    address: Address
    deadline: float
    started: bool = False
    terminal: p.Message | None = None
    # operation owns user code and the instance lock; job owns RPC settlement
    # and joins operation before releasing the registration's execution slot.
    operation: asyncio.Task[p.Message] | None = None
    job: asyncio.Task[None] | None = None
    cleaning: bool = False

    def cancel_operation(self) -> None:
        if (
            self.operation is not None
            and not self.operation.done()
            and not self.operation.cancelling()
        ):
            self.operation.cancel()


class Runtime:
    def __init__(self, url: str, *, api_key: str, transport: str = "http") -> None:
        if transport != "http":
            raise ValueError("Only HTTP transport is supported")
        self._http = HTTPTransport(url, api_key)
        self._registrations: dict[str, _Registration] = {}
        self._calls: set[asyncio.Task[JSONValue]] = set()
        self._closing = False
        self._closed = asyncio.Event()
        self._close_task: asyncio.Task[None] | None = None
        self._error: Exception | None = None

    async def __aenter__(self) -> Runtime:
        self._check_open()
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.close()

    def _check_open(self) -> None:
        if self._closing:
            raise RemoteError("stopped", 1006, "Runtime is closed")

    def addr(self, actor_type: str, actor_id: str) -> Addr:
        self._check_open()
        return Addr(self, actor_type, actor_id)

    async def _call_task(self, coroutine: Coroutine[Any, Any, JSONValue]) -> JSONValue:
        task = asyncio.create_task(coroutine, name="actorx-remote-call")
        self._calls.add(task)
        try:
            return await task
        finally:
            self._calls.discard(task)

    async def register(
        self, actor_type: str, factory: ActorFactory, *, description: str = ""
    ) -> None:
        self._check_open()
        p.identity(actor_type, "registration")
        if (
            not callable(factory)
            or inspect.iscoroutinefunction(factory)
            or not isinstance(description, str)
            or len(description.encode("utf-8")) > 512
        ):
            raise ValueError("Expected factory and bounded description")
        if actor_type in self._registrations:
            raise RemoteError("registration_conflict", 1007, "Type already registered")

        registration = _Registration(self, actor_type, factory, description)
        # Publish local factory and reader before the registration POST starts.
        self._registrations[actor_type] = registration
        registration.reader = asyncio.create_task(registration.run(), name="actorx-remote-reader")

        try:
            async with asyncio.timeout(REGISTER_SECONDS):
                await asyncio.shield(registration.ready)
        except BaseException:
            await registration.close()
            if self._registrations.get(actor_type) is registration:
                del self._registrations[actor_type]
            raise

    def _failed(self, error: Exception) -> None:
        if not self._closing:
            self._error = error
            self._start_close()

    def _start_close(self) -> None:
        if self._close_task is None:
            self._closing = True
            self._close_task = asyncio.create_task(self._shutdown(), name="actorx-remote-close")

    async def close(self) -> None:
        self._start_close()
        # A canceled caller does not abandon SDK cleanup ownership.
        await asyncio.shield(cast(asyncio.Task[None], self._close_task))

    async def _shutdown(self) -> None:
        try:
            for task in self._calls:
                task.cancel()

            await asyncio.gather(
                *(r.close() for r in self._registrations.values()),
                *self._calls,
                return_exceptions=True,
            )

            self._calls.clear()
            self._registrations.clear()
            await self._http.close()
        finally:
            self._closed.set()

    async def wait_closed(self) -> None:
        await self._closed.wait()
        if self._error is not None:
            raise self._error


class _Registration:
    def __init__(
        self, runtime: Runtime, actor_type: str, factory: ActorFactory, description: str
    ) -> None:
        self.runtime = runtime
        self.actor_type = actor_type
        self.factory: ActorFactory | None = factory
        self.description = description
        self.ready: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        self.reader: asyncio.Task[None] | None = None
        self.token: str | None = None
        self.closing = False
        self.instances: dict[tuple[str, str, str], _Instance] = {}
        self.active: set[_Work] = set()
        self.seen: set[tuple[type[object], RPCID]] = set()
        self._close_task: asyncio.Task[None] | None = None

    async def run(self) -> None:
        established = False
        try:
            rpc_id = "p:" + uuid.uuid4().hex
            raw = p.encode(
                p.request(
                    "runtime/register",
                    rpc_id,
                    {
                        "type": self.actor_type,
                        "description": self.description,
                    },
                )
            )
            async with self.runtime._http.open(raw, register=True) as response:
                check_status(response)
                if media_type(response) == "application/json":
                    reply = p.response(await read_body(response), rpc_id)
                    if "error" in reply:
                        raise p.remote_error(reply["error"])
                    raise ProtocolError("Registration requires SSE")
                if media_type(response) != "text/event-stream":
                    raise ProtocolError("Registration requires SSE")

                # Validate the correlated first frame before publishing readiness
                # or dispatching create/invoke frames from the same stream.
                stream = frames(response)
                reply = p.response(await anext(stream), rpc_id)
                if "error" in reply:
                    raise p.remote_error(reply["error"])
                info = p.fields(
                    reply["result"],
                    ("protocol", "type", "session_token"),
                    ("protocol", "type", "session_token"),
                )
                token = info["session_token"]
                if (
                    info["protocol"] != p.VERSION
                    or info["type"] != self.actor_type
                    or not isinstance(token, str)
                    or not token
                    or len(token) > 256
                    or any(ord(c) < 33 or ord(c) > 126 or c == "," for c in token)
                ):
                    raise ProtocolError("Invalid registration result")

                self.token = token
                established = True
                self.ready.set_result(None)
                async for raw in stream:
                    await self.dispatch(p.envelope(raw))
        except asyncio.CancelledError:
            if not self.ready.done():
                self.ready.cancel()
            raise
        except Exception as exc:
            if not self.ready.done():
                self.ready.set_exception(exc)
            elif established and not self.closing:
                self.runtime._failed(exc)

    async def dispatch(self, message: p.Message) -> None:
        if "method" not in message:
            raise ProtocolError("Expected provider request")

        rpc_id = message.get("id")
        if "id" in message:
            key = (type(rpc_id), rpc_id)
            if key in self.seen:
                raise ProtocolError("Reused provider request ID")
            self.seen.add(key)

        if message["method"] not in ("actor/create", "actor/invoke"):
            await self.reply(
                message, p.failure(rpc_id, -32601), asyncio.get_running_loop().time() + 5
            )
            return

        try:
            required = ["address", "deadline_unix_ms"]
            if message["method"] == "actor/invoke":
                required.append("input")
            params = p.fields(message.get("params"), required, required)
            a = p.address(params["address"])
            if a["type"] != self.actor_type or type(params["deadline_unix_ms"]) is not int:
                raise ProtocolError("Invalid provider params")
            ms = params["deadline_unix_ms"]
            if not -(2**63) <= ms < 2**63:
                raise ProtocolError("Invalid deadline")
        except ProtocolError:
            await self.reply(
                message, p.failure(rpc_id, -32602), asyncio.get_running_loop().time() + 5
            )
            return

        # Convert the wire wall-clock deadline once; local waits use monotonic time.
        deadline = asyncio.get_running_loop().time() + (ms / 1000 - time.time())
        work = _Work(message, a, deadline)
        if deadline <= asyncio.get_running_loop().time() or len(self.active) >= p.CAPACITY:
            await self.settle(
                work,
                p.failure(
                    rpc_id,
                    1005 if deadline <= asyncio.get_running_loop().time() else 1001,
                    actor_address=a,
                ),
            )
            return

        # Queued work also consumes a slot; only execute's cleanup releases it.
        self.active.add(work)
        work.job = asyncio.create_task(self.execute(work), name="actorx-remote-execution")

    async def operate(self, work: _Work) -> p.Message:
        a = work.address
        key = (a["type"], a["id"], a["generation"])
        instance = self.instances.get(key)
        create = work.message["method"] == "actor/create"
        if instance is None:
            if not create:
                return p.failure(work.message.get("id"), -32602)
            if len(self.instances) >= p.CAPACITY:
                return p.failure(work.message.get("id"), 1001, actor_address=a)
            instance = self.instances[key] = _Instance()

        # Recheck after acquiring the lock so expired queued work never enters
        # user code. A timed-out handler retains this lock until it really exits.
        async with instance.lock:
            if asyncio.get_running_loop().time() >= work.deadline:
                return p.failure(work.message.get("id"), 1005, actor_address=a)
            work.started = True
            try:
                if create:
                    if instance.actor is None:
                        actor = cast(ActorFactory, self.factory)()
                        if inspect.iscoroutine(actor):
                            actor.close()
                            raise TypeError("Expected synchronous factory")
                        if not inspect.iscoroutinefunction(getattr(actor, "handle", None)):
                            raise TypeError("Factory must provide async handle")
                        instance.actor = actor
                    result: dict[str, object] = {"address": a}
                else:
                    value = await cast(Actor, instance.actor).handle(
                        work.message["params"]["input"]
                    )
                    result = {"address": a, "value": value}

                reply = p.success(work.message.get("id"), result)
                p.encode(reply)
                return reply
            except Exception:
                return p.failure(work.message.get("id"), 1000, actor_address=a)

    async def execute(self, work: _Work) -> None:
        work.operation = asyncio.create_task(self.operate(work), name="actorx-remote-handler")
        try:
            done, _ = await asyncio.wait(
                (work.operation,), timeout=max(0, work.deadline - asyncio.get_running_loop().time())
            )
            if done or work.operation.done():
                await self.settle(work, work.operation.result())
            else:
                # Cancellation is a request to stop user code, not proof of exit.
                work.cancel_operation()
                await self.settle(
                    work,
                    p.failure(
                        work.message.get("id"),
                        1004 if work.started else 1005,
                        actor_address=work.address,
                    ),
                )
        except asyncio.CancelledError:
            work.cancel_operation()
            if work.terminal is None:
                terminal = p.failure(
                    work.message.get("id"),
                    1004 if work.started else 1009,
                    actor_address=work.address,
                )
                if not cast(asyncio.Task[object], asyncio.current_task()).cancelling():
                    # A handler may cancel itself while its RPC still has time.
                    try:
                        await self.settle(work, terminal)
                    except Exception as exc:
                        self.runtime._failed(exc)
                else:
                    work.terminal = terminal
        except Exception as exc:
            self.runtime._failed(exc)
        finally:
            work.cleaning = True
            # Timeout settles the RPC, but the slot/instance lock remain owned
            # until a cancellation-cooperative user handler has actually exited.
            await asyncio.gather(work.operation, return_exceptions=True)
            self.active.discard(work)

    async def settle(self, work: _Work, terminal: p.Message) -> None:
        if work.terminal is not None:
            return

        # Publish before awaiting the POST so cancellation cannot send a second
        # terminal response. The original request deadline still bounds the POST.
        work.terminal = terminal
        await self.reply(work.message, terminal, work.deadline)

    async def reply(self, request: p.Message, terminal: p.Message, deadline: float) -> None:
        if "id" not in request or self.closing or asyncio.get_running_loop().time() >= deadline:
            return
        try:
            async with asyncio.timeout_at(deadline):
                await self.runtime._http.reply(p.encode(terminal), self.token)
        except TimeoutError:
            pass  # Late/unsent final response is not retried.

    async def close(self) -> None:
        if self._close_task is None:
            self.closing = True
            self._close_task = asyncio.create_task(
                self._close(), name="actorx-remote-registration-close"
            )
        await asyncio.shield(self._close_task)

    async def _close(self) -> None:
        self.closing = True
        if self.reader is not None:
            self.reader.cancel()
            await asyncio.gather(self.reader, return_exceptions=True)

        # Stop admission first, then join workers without interrupting a worker
        # that is already waiting for its canceled user handler to exit.
        jobs: list[asyncio.Task[None]] = []
        for work in self.active:
            if not work.cleaning:
                cast(asyncio.Task[None], work.job).cancel()
            jobs.append(cast(asyncio.Task[None], work.job))
        await asyncio.gather(*jobs, return_exceptions=True)
        self.active.clear()  # also covers jobs canceled before their first step

        if not self.ready.done():
            self.ready.cancel()
        elif not self.ready.cancelled():
            self.ready.exception()  # retrieve errors even after a register timeout

        self.instances.clear()
        self.seen.clear()
        self.factory = None
        self.token = None
