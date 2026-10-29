"""One registration and execution owner for both business operations."""

from __future__ import annotations

import asyncio
import time
import unittest
from contextlib import asynccontextmanager

import httpx
from actorx_remote import (
    AddressUncertainError,
    OutcomeUnknownError,
    Runtime,
    UnsupportedOperationError,
)
from actorx_remote import protocol as p
from support import ADDRESS, Gateway, Stream, bounded

TOKEN = "A" * 43
WINDOW = {"items": 4, "bytes": 32768}


@asynccontextmanager
async def provider(factory):
    """Use the HTTP registration handshake, capturing worker POSTs for assertions."""
    runtime = Runtime("http://test", api_key="test-key")
    gateway = Gateway()
    await gateway.install(runtime)
    posts = asyncio.Queue()

    async def reply(raw, session):
        posts.put_nowait(p.envelope(raw))

    runtime._http.reply = reply
    async with runtime:
        await runtime.register("demo.ping", factory)
        yield runtime, runtime._registrations["demo.ping"], posts
    assert not runtime._registrations
    assert not [
        t for t in asyncio.all_tasks() if t.get_name().startswith("actorx-remote-")
    ]


async def dispatch(
    registration, method, rpc_id, value=None, *, address=ADDRESS, ms=5000
):
    params = {
        "address": address,
        "deadline_unix_ms": int(time.time() * 1000) + ms,
    }
    if method != "actor/create":
        params["input"] = value
    if method == "actor/stream":
        params.update(stream_token=TOKEN, window=WINDOW)
    await registration.dispatch(p.request(method, rpc_id, params))


async def exchange(registration, posts, method, rpc_id, value=None, **kw):
    """Collect a short operation's chunks through its correlated terminal POST."""
    await dispatch(registration, method, rpc_id, value, **kw)
    chunks = []
    while True:
        m = await bounded(posts.get())
        if m.get("method") == "stream/chunk":
            chunks.append(m["params"]["value"])
        else:
            assert m["id"] == rpc_id
            return m, chunks


class UnifiedProviderTests(unittest.IsolatedAsyncioTestCase):
    async def test_finished_stream_ledgers_have_separate_capacity(self):
        entered = []

        class Actor:
            async def handle(self, value):
                return value

            async def handle_stream(self, value):
                entered.append(value)
                yield value

        async with provider(Actor) as (_, reg, posts):
            await exchange(reg, posts, "actor/create", "r:1")
            for i in range(p.CAPACITY):
                terminal, chunks = await exchange(
                    reg, posts, "actor/stream", f"r:{i + 2}", i
                )
                self.assertEqual(chunks, [i])
                self.assertEqual(terminal["result"]["final_seq"], 1)
                await bounded(reg.streams[f"r:{i + 2}"].work.job)
            self.assertFalse(reg.active)
            self.assertEqual(len(reg.streams), p.CAPACITY)

            terminal, chunks = await exchange(
                reg, posts, "actor/stream", "r:10", "must-not-run"
            )
            self.assertEqual(terminal["error"]["code"], 1001)
            self.assertEqual(terminal["error"]["data"]["final_seq"], 0)
            self.assertEqual(chunks, [])
            self.assertEqual(entered, list(range(p.CAPACITY)))
            self.assertEqual(len(reg.streams), p.CAPACITY)

            # Retained ledgers limit Streams, while Call still has execution room.
            terminal, _ = await exchange(reg, posts, "actor/call", "r:11", "ok")
            self.assertEqual(terminal["result"]["value"], "ok")
            await reg.dispatch(
                p.notification(
                    "stream/ack",
                    {
                        "request_id": "r:2",
                        "stream_token": TOKEN,
                        "ack_seq": 1,
                    },
                )
            )
            self.assertEqual(len(reg.streams), p.CAPACITY - 1)
            terminal, chunks = await exchange(
                reg, posts, "actor/stream", "r:12", "next"
            )
            self.assertEqual(chunks, ["next"])
            self.assertEqual(terminal["result"]["final_seq"], 1)
            self.assertEqual(len(reg.streams), p.CAPACITY)
        self.assertFalse(reg.streams)

    async def test_finished_stream_ledger_expires_at_original_deadline(self):
        class Actor:
            async def handle_stream(self, value):
                yield value

        async with provider(Actor) as (_, reg, posts):
            await exchange(reg, posts, "actor/create", "r:1")
            terminal, _ = await exchange(reg, posts, "actor/stream", "r:2", ms=150)
            self.assertEqual(terminal["result"]["final_seq"], 1)
            self.assertIn("r:2", reg.streams)

            async def expired():
                while reg.streams:
                    await asyncio.sleep(0.005)

            await bounded(expired())

    async def test_classes_and_closures_are_lazy_and_share_identity(self):
        for mode in ("call", "stream", "both"):
            for closure in (False, True):
                with self.subTest(mode=mode, closure=closure):
                    created = []

                    class State:
                        def __init__(self):
                            self.count = 0
                            created.append(self)

                        async def increment(self, _):
                            self.count += 1
                            return self.count

                        async def fragments(self, _):
                            self.count += 1
                            yield self.count

                    # Non-callable methods count as missing, just like absent ones.
                    class Actor(State):
                        handle = State.increment if mode != "stream" else None
                        handle_stream = State.fragments if mode != "call" else None

                    factory = (lambda: Actor()) if closure else Actor
                    async with provider(factory) as (_, reg, posts):
                        # Registration is lazy; duplicate create reuses the instance.
                        self.assertEqual(created, [])
                        for n in range(2):
                            m, _ = await exchange(
                                reg, posts, "actor/create", f"r:{n + 1}"
                            )
                            self.assertIn("result", m)
                        self.assertEqual(len(created), 1)

                        # Interleave operations on that instance, including missing ones.
                        count = 0
                        for n, method in enumerate(
                            ("actor/call", "actor/stream", "actor/call")
                        ):
                            m, chunks = await exchange(reg, posts, method, f"r:{n + 3}")
                            missing = (
                                method == "actor/call" and mode == "stream"
                            ) or (method == "actor/stream" and mode == "call")
                            if missing:
                                self.assertEqual(m["error"]["code"], 1013)
                                self.assertEqual(m["error"]["data"]["address"], ADDRESS)
                                self.assertEqual(chunks, [])
                                if method == "actor/stream":
                                    self.assertEqual(m["error"]["data"]["final_seq"], 0)
                            else:
                                count += 1
                                self.assertEqual(m["result"]["address"], ADDRESS)
                                if method == "actor/call":
                                    self.assertEqual(m["result"]["value"], count)
                                else:
                                    self.assertEqual(chunks, [count])
                                    self.assertEqual(m["result"]["final_seq"], 1)
                        self.assertEqual(created[0].count, count)

                        # Both a new ID and a new generation own separate instances.
                        for n, a in enumerate(
                            (
                                dict(ADDRESS, id="other"),
                                dict(ADDRESS, generation="AQEBAQEBAQEBAQEBAQEBAQ"),
                            )
                        ):
                            await exchange(
                                reg,
                                posts,
                                "actor/create",
                                f"r:{n + 6}",
                                address=a,
                            )
                        self.assertEqual(len(created), 3)
                        self.assertEqual([o.count for o in created[1:]], [0, 0])

    async def test_creation_failures_and_removed_switch(self):
        def fails():
            raise ValueError("secret")

        class Neither:
            handle = 42
            handle_stream = None

        async def async_factory():
            return Neither()

        for factory in (fails, Neither, lambda: async_factory()):
            async with provider(factory) as (runtime, reg, posts):
                self.assertFalse(reg.instances)
                m, _ = await exchange(reg, posts, "actor/create", "r:1")
                self.assertEqual(m["error"]["code"], 1000)
                self.assertEqual(m["error"]["data"]["address"], ADDRESS)
                self.assertNotIn("secret", str(m))
                self.assertIsNone(next(iter(reg.instances.values())).actor)
                with self.assertRaises(TypeError):
                    await runtime.register("demo.old", Neither, stream=True)

    async def test_runtime_return_shapes_and_business_errors(self):
        class Iterator:
            def __init__(self):
                self.done = False

            def __aiter__(self):
                return self

            async def __anext__(self):
                if self.done:
                    raise StopAsyncIteration
                self.done = True
                return "custom"

        class Actor:
            # A regular method returning an awaitable is a valid Call handler.
            def handle(self, message):
                if message == "wrong":
                    return 7

                async def result():
                    if message == "error":
                        raise ValueError("secret")
                    return object() if message == "non-json" else message

                return result()

            def handle_stream(self, message):
                if message == "custom":
                    return Iterator()
                if message == "wrong":
                    return [1, 2]
                if message == "coroutine":
                    return self.handle(message)

                async def generate():
                    if message == "partial":
                        yield "first"
                    if message == "non-json":
                        yield object()
                    if message != "empty":
                        raise ValueError("secret")

                return generate()

        async with provider(Actor) as (_, reg, posts):
            await exchange(reg, posts, "actor/create", "r:1")
            m, _ = await exchange(reg, posts, "actor/call", "r:2", "ok")
            self.assertEqual(m["result"]["value"], "ok")
            for n, value in enumerate(("wrong", "error", "non-json")):
                m, _ = await exchange(
                    reg, posts, "actor/call", f"r:{n + 3}", value
                )
                self.assertEqual(m["error"]["code"], 1000)
            for n, value in enumerate(
                ("wrong", "coroutine", "error", "non-json", "partial", "custom", "empty")
            ):
                m, chunks = await exchange(
                    reg, posts, "actor/stream", f"r:{n + 6}", value
                )
                self.assertEqual(
                    chunks,
                    ["first"]
                    if value == "partial"
                    else ["custom"]
                    if value == "custom"
                    else [],
                )
                if value in ("custom", "empty"):
                    self.assertEqual(m["result"]["final_seq"], len(chunks))
                else:
                    self.assertEqual(m["error"]["code"], 1000)
                    self.assertEqual(m["error"]["data"]["final_seq"], len(chunks))
                    self.assertNotIn("secret", str(m))

    async def test_call_stream_overlap_expiry_and_cancel_retain_one_owner(self):
        for first_method in ("actor/call", "actor/stream"):
            with self.subTest(first=first_method):
                entered, canceled, release = (
                    asyncio.Event(),
                    asyncio.Event(),
                    asyncio.Event(),
                )
                events = []

                class Actor:
                    async def handle(self, value):
                        events.append(value)
                        if value == "first":
                            entered.set()
                            try:
                                await asyncio.Event().wait()
                            except asyncio.CancelledError:
                                canceled.set()
                                await release.wait()
                        return value

                    async def handle_stream(self, value):
                        yield await self.handle(value)

                async with provider(Actor) as (_, reg, posts):
                    # Release even if an assertion fails, so cleanup can join user code.
                    try:
                        await exchange(reg, posts, "actor/create", "r:1")
                        await dispatch(reg, first_method, "r:2", "first", ms=50)
                        await bounded(entered.wait())
                        first = next(
                            w for w in reg.active if w.message["id"] == "r:2"
                        )
                        other = (
                            "actor/call"
                            if first_method.endswith("stream")
                            else "actor/stream"
                        )

                        await dispatch(reg, other, "r:3", "must-not-run", ms=50)
                        expired = next(
                            w for w in reg.active if w.message["id"] == "r:3"
                        )
                        if first.stream is not None:
                            await reg.dispatch(
                                p.notification(
                                    "stream/cancel",
                                    {"request_id": "r:2", "stream_token": TOKEN},
                                )
                            )
                        await bounded(canceled.wait())

                        # The queued operation expires while the first ignores cancellation.
                        await bounded(expired.job)
                        self.assertEqual(first.terminal["error"]["code"], 1004)
                        self.assertEqual(expired.terminal["error"]["code"], 1005)
                        self.assertFalse(expired.started)
                        self.assertIn(first, reg.active)
                        self.assertFalse(first.operation.done())

                        await dispatch(reg, other, "r:4", "after-exit")
                        following = next(
                            w for w in reg.active if w.message["id"] == "r:4"
                        )
                        # Seven queued requests plus the canceled handler fill all eight slots.
                        for i in range(6):
                            await dispatch(reg, other, f"r:{i + 5}", f"queued{i}")
                        await dispatch(reg, other, "r:11", "must-not-run")
                        # Flush scheduled operations up to a known event-loop barrier.
                        await asyncio.sleep(0)
                        self.assertEqual(len(reg.active), 8)
                        self.assertEqual(events, ["first"])
                        self.assertIsNone(following.terminal)
                        self.assertEqual(
                            (await bounded(posts.get()))["error"]["code"], 1001
                        )

                        release.set()
                        await bounded(first.job)
                        await bounded(following.job)
                        self.assertEqual(events[:2], ["first", "after-exit"])
                        self.assertNotIn("must-not-run", events)

                        # Drain the queued jobs before checking its one terminal.
                        await asyncio.gather(*(w.job for w in list(reg.active)))
                        while not posts.empty():
                            posts.get_nowait()

                        # A determined unsupported operation survives a later cancellation.
                        reg.instances[
                            (ADDRESS["type"], ADDRESS["id"], ADDRESS["generation"])
                        ].actor.handle_stream = None
                        m, _ = await exchange(
                            reg, posts, "actor/stream", "r:12"
                        )
                        self.assertEqual(m["error"]["code"], 1013)
                        await reg.dispatch(
                            p.notification(
                                "stream/cancel",
                                {"request_id": "r:12", "stream_token": TOKEN},
                            )
                        )
                        self.assertTrue(posts.empty())
                    finally:
                        release.set()


class UnifiedConsumerTests(unittest.IsolatedAsyncioTestCase):
    async def test_unsupported_fixes_addr_then_other_operation_succeeds(self):
        for missing in ("actor/call", "actor/stream"):
            for sse in (False, True):
                with self.subTest(missing=missing, sse=sse):
                    runtime = Runtime("http://test", api_key="test-key")
                    requests = []

                    async def handle(req):
                        m = p.envelope(req.content)
                        if m["method"] in ("stream/ack", "stream/cancel"):
                            return httpx.Response(204)
                        requests.append(m)
                        if m["method"] == missing:
                            result = p.failure(m["id"], 1013, actor_address=ADDRESS)
                            result["error"]["data"]["final_seq"] = 0
                        else:
                            result = p.success(
                                m["id"],
                                {"address": ADDRESS, "value": "ok", "final_seq": 0},
                            )
                            if m["method"] == "actor/call":
                                del result["result"]["final_seq"]
                            else:
                                del result["result"]["value"]
                        if m["method"] == "actor/stream" and (
                            sse or missing != "actor/stream"
                        ):
                            wire = Stream()
                            wire.send(
                                p.notification(
                                    "stream/opened",
                                    {
                                        "request_id": m["id"],
                                        "stream_token": TOKEN,
                                        "window": WINDOW,
                                    },
                                )
                            )
                            wire.send(result)
                            return httpx.Response(
                                200,
                                headers={"content-type": "text/event-stream"},
                                stream=wire,
                            )
                        return httpx.Response(
                            200,
                            headers={"content-type": "application/json"},
                            content=p.encode(result),
                        )

                    await runtime._http._client.aclose()
                    runtime._http._client = httpx.AsyncClient(
                        transport=httpx.MockTransport(handle)
                    )
                    async with runtime:
                        addr = runtime.addr("demo.ping", "session-42")
                        with self.assertRaises(UnsupportedOperationError) as caught:
                            if missing == "actor/call":
                                await addr.call(None)
                            else:
                                async with addr.stream(None) as chunks:
                                    await anext(chunks)
                        self.assertEqual(caught.exception.data["address"], ADDRESS)
                        self.assertEqual(addr.address, ADDRESS)
                        self.assertFalse(addr._uncertain)

                        # Reuse the same Addr; its next request must carry the generation.
                        if missing == "actor/call":
                            async with addr.stream(None) as chunks:
                                self.assertEqual([v async for v in chunks], [])
                        else:
                            self.assertEqual(await addr.call(None), "ok")
                        self.assertEqual(requests[1]["params"]["address"], ADDRESS)

    async def test_unsupported_requires_fixed_address_and_zero_stream_evidence(self):
        for method, bad in (
            ("call", "address"),
            ("stream", "address"),
            ("stream", "seq"),
            ("stream", "missing-seq"),
            ("stream", "partial"),
        ):
            with self.subTest(method=method, bad=bad):
                runtime = Runtime("http://test", api_key="test-key")

                async def handle(req):
                    m = p.envelope(req.content)
                    if m["method"] in ("stream/ack", "stream/cancel"):
                        return httpx.Response(204)
                    result = p.failure(m["id"], 1013, actor_address=ADDRESS)
                    result["error"]["data"]["final_seq"] = 0
                    if bad == "address":
                        del result["error"]["data"]["address"]
                    if bad == "missing-seq":
                        del result["error"]["data"]["final_seq"]
                    if bad == "seq":
                        result["error"]["data"]["final_seq"] = 1
                    if bad == "partial":
                        wire = Stream()
                        wire.send(
                            p.notification(
                                "stream/opened",
                                {
                                    "request_id": m["id"],
                                    "stream_token": TOKEN,
                                    "window": WINDOW,
                                },
                            )
                        )
                        wire.send(
                            p.notification(
                                "stream/chunk",
                                {
                                    "request_id": m["id"],
                                    "stream_token": TOKEN,
                                    "address": ADDRESS,
                                    "seq": 1,
                                    "value": "partial",
                                },
                            )
                        )
                        result["error"]["data"]["final_seq"] = 1
                        wire.send(result)
                        return httpx.Response(
                            200,
                            headers={"content-type": "text/event-stream"},
                            stream=wire,
                        )
                    return httpx.Response(
                        200,
                        headers={"content-type": "application/json"},
                        content=p.encode(result),
                    )

                await runtime._http._client.aclose()
                runtime._http._client = httpx.AsyncClient(
                    transport=httpx.MockTransport(handle)
                )
                async with runtime:
                    addr = runtime.addr("demo.ping", "session-42")
                    with self.assertRaises(OutcomeUnknownError):
                        if method == "call":
                            await addr.call(None)
                        else:
                            async with addr.stream(None) as chunks:
                                async for _ in chunks:
                                    pass
                    if bad != "partial":
                        with self.assertRaises(AddressUncertainError):
                            await addr.call(None)
