from __future__ import annotations

import asyncio
import base64
import time
import unittest

import httpx
from actorx_remote import (
    DeadlineExceededError,
    OutcomeUnknownError,
    RemoteError,
    Runtime,
)
from actorx_remote import protocol as p
from support import ADDRESS, Gateway, Stream, bounded

TOKEN = base64.urlsafe_b64encode(bytes(32)).decode().rstrip("=")
WINDOW = {"items": 4, "bytes": 32768}


class ConsumerTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.runtime = Runtime("http://test", api_key="test-key")
        self.wire = Stream()
        self.controls = asyncio.Queue()
        self.started = asyncio.Event()
        self.request = None
        self.open_method = "stream/opened"

        async def handle(req):
            m = p.envelope(req.content)
            self.assertEqual(req.headers["Authorization"], "Bearer test-key")
            self.assertEqual(
                req.headers["X-Actorx-Remote-Protocol"], "actorx.remote/v1"
            )
            self.assertNotIn("X-Actorx-Remote-Session", req.headers)
            if m["method"] == "actor/stream":
                self.request = m
                self.wire.send(
                    p.notification(
                        self.open_method,
                        {
                            "request_id": m["id"],
                            "stream_token": TOKEN,
                            "window": WINDOW,
                        },
                    )
                )
                self.started.set()
                return httpx.Response(
                    200, headers={"content-type": "text/event-stream"}, stream=self.wire
                )
            self.controls.put_nowait(m)
            return httpx.Response(204)

        await self.runtime._http._client.aclose()
        self.runtime._http._client = httpx.AsyncClient(
            transport=httpx.MockTransport(handle)
        )

    async def asyncTearDown(self):
        await self.runtime.close()
        self.assertFalse(self.runtime._streams)

    def chunk(self, seq, value=None, **changes):
        params = {
            "request_id": self.request["id"],
            "stream_token": TOKEN,
            "address": ADDRESS,
            "seq": seq,
            "value": value,
        }
        params.update(changes)
        self.wire.send(p.notification("stream/chunk", params))

    def final(self, seq):
        self.wire.send(
            p.success(self.request["id"], {"address": ADDRESS, "final_seq": seq})
        )

    async def open(self):
        stream = self.runtime.addr("demo.ping", "session-42").stream(None)
        await stream.__aenter__()
        await bounded(self.started.wait())
        return stream

    async def test_empty_and_last_credit_before_completion(self):
        stream = await self.open()
        self.final(0)
        self.assertEqual([v async for v in stream], [])
        self.assertEqual(stream.addr.address, ADDRESS)
        await self.runtime.close()
        with self.assertRaises(StopAsyncIteration):
            await anext(stream)

    async def stalled_ack(self, terminal=None, queued=False):
        stream = await self.open()
        self.chunk(1, "first")
        self.assertEqual(await bounded(anext(stream)), "first")
        if queued:
            self.chunk(2, "second")
        original = self.runtime._http.reply

        async def reply(raw, session):
            if p.envelope(raw)["method"] != "stream/ack":
                return await original(raw, session)
            if terminal == "success":
                self.final(2 if queued else 1)
            elif terminal == "error":
                error = p.failure(self.request["id"], 1000, actor_address=ADDRESS)
                error["error"]["data"]["final_seq"] = 1
                self.wire.send(error)
            if terminal is not None:
                await bounded(stream.reader)
            # The ack was handled, but its HTTP response never returns.
            await asyncio.Event().wait()

        self.runtime._http.reply = reply
        stream.deadline = asyncio.get_running_loop().time() + 0.1
        return stream

    async def test_last_ack_timeout_preserves_consumed_success(self):
        stream = await self.stalled_ack("success")
        with self.assertRaises(StopAsyncIteration):
            await bounded(anext(stream))
        self.assertTrue(stream.remote_terminal)
        self.assertIsNone(stream.error)

    async def test_ack_timeout_without_terminal_is_unknown(self):
        stream = await self.stalled_ack()
        with self.assertRaises(OutcomeUnknownError):
            await bounded(anext(stream))
        self.assertFalse(stream.remote_terminal)
        with self.assertRaises(OutcomeUnknownError):
            await anext(stream)

    async def test_ack_timeout_cannot_skip_queued_items(self):
        stream = await self.stalled_ack("success", queued=True)
        with self.assertRaises(DeadlineExceededError):
            await bounded(anext(stream))
        self.assertTrue(stream.remote_terminal)
        self.assertEqual(stream.queue.qsize(), 1)
        with self.assertRaises(DeadlineExceededError):
            await anext(stream)

    async def test_last_ack_timeout_preserves_remote_failure(self):
        stream = await self.stalled_ack("error")
        with self.assertRaises(RemoteError) as caught:
            await bounded(anext(stream))
        self.assertEqual(caught.exception.rpc_code, 1000)

    async def test_six_items_ack_only_after_processing(self):
        stream = await self.open()
        for i in range(1, 5):
            self.chunk(i, i)
        self.assertEqual(await bounded(anext(stream)), 1)
        self.assertTrue(self.controls.empty())

        # Asking for the next item acknowledges the one user code just processed.
        for i in range(2, 7):
            next_item = asyncio.create_task(anext(stream))
            credit = await bounded(self.controls.get())
            self.assertEqual(credit["params"]["ack_seq"], i - 1)
            if i <= 3:
                self.chunk(i + 3, i + 3)
            self.assertEqual(await bounded(next_item), i)

        self.final(6)  # final arrives while item six still belongs to user code
        last = asyncio.create_task(anext(stream))
        self.assertEqual((await bounded(self.controls.get()))["params"]["ack_seq"], 6)
        with self.assertRaises(StopAsyncIteration):
            await bounded(last)
        self.assertTrue(self.wire.closed.is_set())

    async def test_partial_failure_preserves_remote_error(self):
        stream = await self.open()
        self.chunk(1, "prefix")
        self.assertEqual(await bounded(anext(stream)), "prefix")

        error = p.failure(self.request["id"], 1000, actor_address=ADDRESS)
        error["error"]["data"]["final_seq"] = 1
        self.wire.send(error)
        with self.assertRaises(RemoteError) as caught:
            await bounded(anext(stream))
        self.assertEqual(caught.exception.rpc_code, 1000)

    async def test_missing_terminal_and_bad_sequence_are_unknown(self):
        stream = await self.open()
        self.chunk(2)
        with self.assertRaises(OutcomeUnknownError):
            await bounded(anext(stream))
        self.assertFalse(self.runtime._streams)

    async def test_eof_never_success(self):
        stream = await self.open()
        self.wire.queue.put_nowait(None)
        with self.assertRaises(OutcomeUnknownError):
            await bounded(anext(stream))

    async def test_legacy_open_is_not_an_alias(self):
        self.open_method = "actor/stream_open"
        stream = self.runtime.addr("demo.ping", "session-42").stream(None)
        with self.assertRaises(OutcomeUnknownError):
            async with stream:
                await bounded(anext(stream))

    async def test_legacy_chunk_is_not_an_alias(self):
        stream = await self.open()
        self.wire.send(
            p.notification(
                "actor/chunk",
                {
                    "request_id": self.request["id"],
                    "stream_token": TOKEN,
                    "address": ADDRESS,
                    "seq": 1,
                    "value": None,
                },
            )
        )
        with self.assertRaises(OutcomeUnknownError):
            await bounded(anext(stream))

    async def test_early_exit_sends_cancel_and_runtime_joins(self):
        stream = await self.open()
        self.chunk(1, "hello")
        self.assertEqual(await bounded(anext(stream)), "hello")
        await self.runtime.close()
        cancel = await bounded(self.controls.get())
        self.assertEqual(cancel["method"], "stream/cancel")
        self.assertTrue(stream.reader.done())
        self.assertTrue(self.wire.closed.is_set())
        with self.assertRaises(RemoteError) as caught:
            await anext(stream)
        self.assertEqual(caught.exception.rpc_code, 1009)

    async def test_runtime_close_reports_cancel_to_waiting_and_later_next(self):
        stream = await self.open()
        self.chunk(1, "first")
        self.assertEqual(await bounded(anext(stream)), "first")
        waiting = asyncio.create_task(anext(stream))
        await bounded(self.controls.get())  # next is waiting after its ack
        await self.runtime.close()
        with self.assertRaises(RemoteError) as caught:
            await bounded(waiting)
        self.assertEqual(caught.exception.rpc_code, 1009)
        with self.assertRaises(RemoteError) as caught:
            await anext(stream)
        self.assertEqual(caught.exception.rpc_code, 1009)

    async def test_close_cannot_skip_queued_success(self):
        stream = await self.open()
        self.chunk(1, "first")
        self.chunk(2, "second")
        self.final(2)
        await bounded(stream.reader)
        self.assertEqual(await bounded(anext(stream)), "first")
        await stream.aclose()
        with self.assertRaises(RemoteError) as caught:
            await anext(stream)
        self.assertEqual(caught.exception.rpc_code, 1009)
        self.assertTrue(stream.remote_terminal)
        self.assertEqual(stream.queue.qsize(), 1)

    async def test_close_cannot_skip_delivered_last_item(self):
        stream = await self.open()
        self.chunk(1, "last")
        self.final(1)
        await bounded(stream.reader)
        self.assertEqual(await bounded(anext(stream)), "last")
        self.assertTrue(stream.queue.empty())
        await self.runtime.close()
        with self.assertRaises(RemoteError) as caught:
            await anext(stream)
        self.assertEqual(caught.exception.rpc_code, 1009)

    async def test_definite_http_rejection_does_not_contaminate_address(self):
        from actorx_remote import AuthenticationError

        async def reject(request):
            return httpx.Response(401)

        await self.runtime._http._client.aclose()
        self.runtime._http._client = httpx.AsyncClient(
            transport=httpx.MockTransport(reject)
        )
        actor = self.runtime.addr("demo.ping", "session-42")
        for _ in range(2):
            with self.assertRaises(AuthenticationError):
                async with actor.stream(None) as chunks:
                    await anext(chunks)
            self.assertFalse(actor._uncertain)

    async def test_invalid_timeout(self):
        a = self.runtime.addr("demo.ping", "session-42")
        for timeout in (0, -1, 61, float("nan"), True):
            with self.assertRaises(ValueError):
                a.stream(None, timeout=timeout)

    async def test_local_deadline_is_classified(self):
        stream = self.runtime.addr("demo.ping", "session-42").stream(None, timeout=0.05)
        with self.assertRaises(OutcomeUnknownError):
            async with stream:
                await anext(stream)
        self.assertTrue(stream.reader.done())
        self.assertFalse(self.runtime._streams)

    async def test_canceled_close_retains_cleanup(self):
        stream = await self.open()
        self.chunk(1)
        await bounded(anext(stream))
        entered, release = asyncio.Event(), asyncio.Event()
        original = self.runtime._http.reply

        async def held_reply(raw, session):
            entered.set()
            await release.wait()
            await original(raw, session)

        self.runtime._http.reply = held_reply
        closer = asyncio.create_task(stream.aclose())
        await bounded(entered.wait())
        closer.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await closer
        self.assertFalse(stream._close_task.done())

        # Runtime cleanup still joins the close task abandoned by its caller.
        release.set()
        await bounded(self.runtime.close())
        self.assertTrue(stream.reader.done())
        self.assertTrue(stream._close_task.done())


class ProviderTests(unittest.IsolatedAsyncioTestCase):
    async def test_window_terminal_credit_and_cancellation(self):
        gateway = Gateway()
        runtime = Runtime("http://test", api_key="test-key")
        chunks = asyncio.Queue()
        posts = asyncio.Queue()
        entered = asyncio.Event()
        stopped = asyncio.Event()
        hold = asyncio.Event()

        class Actor:
            async def handle_stream(self, value):
                entered.set()
                try:
                    if value == "hold":
                        await hold.wait()
                    for i in range(6):
                        yield i
                finally:
                    stopped.set()

        original = gateway.handle

        async def handle(req):
            m = p.envelope(req.content)
            if m.get("method") == "runtime/register":
                self.assertEqual(set(m["params"]), {"type", "description"})
                gateway.stream.send(
                    p.success(
                        m["id"],
                        {
                            "protocol": p.VERSION,
                            "type": "demo.ping",
                            "session_token": "session-token",
                        },
                    )
                )
                return httpx.Response(
                    200,
                    headers={"content-type": "text/event-stream"},
                    stream=gateway.stream,
                )
            if m.get("method") == "stream/chunk":
                self.assertEqual(
                    req.headers["X-Actorx-Remote-Session"], "session-token"
                )
                chunks.put_nowait(m)
                return httpx.Response(204)
            if "method" not in m:
                posts.put_nowait(m)
                return httpx.Response(204)
            return await original(req)

        await runtime._http._client.aclose()
        runtime._http._client = httpx.AsyncClient(transport=httpx.MockTransport(handle))
        async with runtime:
            await runtime.register("demo.ping", Actor)
            create = gateway.send(method="actor/create")
            self.assertEqual((await bounded(posts.get()))["id"], create)

            def invoke(rpc_id, value):
                gateway.stream.send(
                    p.request(
                        "actor/stream",
                        rpc_id,
                        {
                            "address": ADDRESS,
                            "input": value,
                            "deadline_unix_ms": int(time.time() * 1000) + 5000,
                            "stream_token": TOKEN,
                            "window": WINDOW,
                        },
                    )
                )

            def control(method, rpc_id, ack=None):
                params = {"request_id": rpc_id, "stream_token": TOKEN}
                if ack is not None:
                    params["ack_seq"] = ack
                gateway.stream.send(p.notification(method, params))

            invoke("six", None)
            for i in range(1, 5):
                self.assertEqual((await bounded(chunks.get()))["params"]["seq"], i)

            # An event loop barrier lets the sender reach the full-window wait.
            await asyncio.sleep(0)
            self.assertTrue(chunks.empty())

            # Retired control names cannot release credit or cancel live work.
            registration = runtime._registrations["demo.ping"]
            ledger = registration.streams["six"]
            for method, extra in (("actor/credit", {"ack_seq": 2}), ("actor/cancel", {})):
                await registration.dispatch(
                    p.notification(
                        method, {"request_id": "six", "stream_token": TOKEN, **extra}
                    )
                )
                self.assertEqual(ledger.ack, 0)
                self.assertEqual(len(ledger.sizes), 4)
                self.assertFalse(ledger.work.cancel_requested)

            control("stream/ack", "six", 2)
            for i in (5, 6):
                self.assertEqual((await bounded(chunks.get()))["params"]["seq"], i)
            final = await bounded(posts.get())
            self.assertEqual(final["result"]["final_seq"], 6)

            control("stream/cancel", "six")  # cannot overwrite confirmed success
            control("stream/ack", "six", 6)

            # A separate invocation can still be canceled while its handler waits.
            entered.clear()
            stopped.clear()
            invoke("cancel", "hold")
            await bounded(entered.wait())
            control("stream/cancel", "cancel")
            terminal = await bounded(posts.get())
            self.assertEqual(terminal["id"], "cancel")
            self.assertEqual(terminal["error"]["data"]["final_seq"], 0)
            await bounded(stopped.wait())
        self.assertFalse(runtime._registrations)


class ProviderCancellationTests(unittest.IsolatedAsyncioTestCase):
    async def test_cancel_before_execution_does_not_enter_handler(self):
        from actorx_remote.runtime import _Registration

        runtime = Runtime("http://test", api_key="test-key")
        entered = []

        class Actor:
            async def handle_stream(self, message):
                entered.append(message)
                yield message

        registration = _Registration(runtime, "demo.ping", Actor, "")
        registration.token = "session-token"
        replies = asyncio.Queue()

        async def reply(raw, session):
            replies.put_nowait(p.envelope(raw))

        runtime._http.reply = reply

        # Dispatch and cancel without yielding to the newly scheduled worker.
        message = p.request(
            "actor/stream",
            "pending",
            {
                "address": ADDRESS,
                "input": "must-not-run",
                "deadline_unix_ms": int(time.time() * 1000) + 5000,
                "stream_token": TOKEN,
                "window": WINDOW,
            },
        )
        await registration.dispatch(message)
        await registration.dispatch(
            p.notification(
                "stream/cancel",
                {
                    "request_id": "pending",
                    "stream_token": TOKEN,
                },
            )
        )
        terminal = await bounded(replies.get())
        self.assertEqual(terminal["error"]["code"], 1009)
        self.assertEqual(entered, [])
        await registration.close()
        await runtime.close()

    async def test_timed_out_stream_keeps_instance_lock_until_exit(self):
        from actorx_remote.runtime import _Instance, _Registration

        runtime = Runtime("http://test", api_key="test-key")
        entered, canceled, release = asyncio.Event(), asyncio.Event(), asyncio.Event()
        subsequent = asyncio.Event()

        class Actor:
            async def handle_stream(self, message):
                if message == "first":
                    entered.set()
                    try:
                        await asyncio.Event().wait()
                    except asyncio.CancelledError:
                        canceled.set()
                        await release.wait()
                else:
                    subsequent.set()
                yield message

        registration = _Registration(runtime, "demo.ping", Actor, "")
        registration.token = "session-token"
        registration.instances[
            (ADDRESS["type"], ADDRESS["id"], ADDRESS["generation"])
        ] = _Instance(actor=Actor())

        async def reply(raw, session):
            pass

        runtime._http.reply = reply

        def invoke(rpc_id, value, ms):
            return p.request(
                "actor/stream",
                rpc_id,
                {
                    "address": ADDRESS,
                    "input": value,
                    "deadline_unix_ms": ms,
                    "stream_token": TOKEN,
                    "window": WINDOW,
                },
            )

        await registration.dispatch(
            invoke("first", "first", int(time.time() * 1000) + 100)
        )
        await bounded(entered.wait())
        await bounded(canceled.wait())

        # RPC timeout did not release the first invocation's lock or slot.
        await registration.dispatch(
            invoke("second", "second", int(time.time() * 1000) + 5000)
        )
        await asyncio.sleep(0)
        self.assertFalse(subsequent.is_set())
        self.assertEqual(len(registration.active), 2)

        release.set()
        await bounded(subsequent.wait())
        await registration.close()
        await runtime.close()
