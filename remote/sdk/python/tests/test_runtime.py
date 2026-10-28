from __future__ import annotations

import asyncio
import time
import unittest
import weakref
from collections.abc import Callable, Mapping
from decimal import Decimal
from typing import Never, cast
from unittest.mock import patch

import httpx
from actorx_remote import (
    ActivationUnknownError,
    ActorFactory,
    AddressUncertainError,
    AuthenticationError,
    JSONValue,
    OutcomeUnknownError,
    ProtocolError,
    RemoteError,
    Runtime,
    SessionError,
    TransportError,
)
from actorx_remote import protocol as p
from actorx_remote.runtime import _Registration
from support import ADDRESS, Gateway, HTTPHandler, bounded


class Echo:
    async def handle(self, message: JSONValue) -> JSONValue:
        return message


class SDKTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.runtime = Runtime("http://gateway.test", api_key="test-key")
        self.gateway = Gateway()
        await self.gateway.install(self.runtime)

    async def asyncTearDown(self) -> None:
        await bounded(self.runtime.close())
        self.assertTrue(self.runtime._http._client.is_closed)
        self.assertFalse(self.runtime._registrations)
        self.assertFalse(self.runtime._calls)
        self.assertFalse(
            [
                t
                for t in asyncio.all_tasks()
                if t is not asyncio.current_task() and t.get_name().startswith("actorx-remote-")
            ]
        )

    async def register(self, factory: Callable[[], object] = Echo) -> _Registration:
        await bounded(self.runtime.register("demo.ping", cast(ActorFactory, factory)))
        return self.runtime._registrations["demo.ping"]

    async def create(self, address: Mapping[str, object] | None = None) -> None:
        rpc_id = self.gateway.send("actor/create", address=address)
        response = await self.gateway.response(rpc_id)
        self.assertIn("result", response)

    async def test_addr_is_local_and_consumer_never_opens_sse(self) -> None:
        actor = self.runtime.addr("demo.ping", "session-42")
        self.assertFalse(self.gateway.requests)
        self.assertNotIn("generation", actor.address)

        self.assertIsNone(await actor.call(None))
        self.assertEqual(actor.address, ADDRESS)
        self.assertEqual(
            await actor.call({"n": 900719925474099312345}), {"n": 900719925474099312345}
        )
        first, second = [m for _, m in self.gateway.requests]
        self.assertNotIn("generation", first["params"]["address"])
        self.assertEqual(second["params"]["address"], ADDRESS)
        self.assertLessEqual(
            abs(first["params"]["deadline_unix_ms"] - int(time.time() * 1000) - 5000), 1000
        )
        snapshot = actor.address
        snapshot["generation"] = "changed"
        self.assertEqual(actor.address, ADDRESS)
        self.assertFalse(self.runtime._registrations)

    async def test_business_failure_saves_address_before_raising(self) -> None:
        async def fail(_: httpx.Request, m: p.Message) -> httpx.Response:
            return httpx.Response(
                200,
                content=p.encode(p.failure(m["id"], 1000, actor_address=ADDRESS)),
                headers={"Content-Type": "application/json"},
            )

        self.gateway.call_response = fail
        actor = self.runtime.addr("demo.ping", "session-42")
        for _ in range(2):
            with self.assertRaises(RemoteError) as raised:
                await actor.call({})
            self.assertEqual(
                (raised.exception.code, raised.exception.rpc_code), ("remote_error", 1000)
            )
            self.assertEqual(actor.address, ADDRESS)
        self.assertEqual(self.gateway.requests[-1][1]["params"]["address"], ADDRESS)

    async def test_unknown_without_address_poisoned_handle(self) -> None:
        for code, cls in ((1003, ActivationUnknownError), (1004, OutcomeUnknownError)):

            async def fail(_: httpx.Request, m: p.Message) -> httpx.Response:
                return httpx.Response(
                    200,
                    content=p.encode(p.failure(m["id"], code)),
                    headers={"Content-Type": "application/json"},
                )

            self.gateway.call_response = fail
            actor = self.runtime.addr("demo.ping", "session-42")
            with self.assertRaises(cls):
                await actor.call(None)

            # No selected identity survived; later calls must fail locally.
            sent = len(self.gateway.requests)
            with self.assertRaises(AddressUncertainError):
                await actor.call(None)
            self.assertEqual(len(self.gateway.requests), sent)

    async def test_unknown_with_address_keeps_fixed_handle(self) -> None:
        async def fail(_: httpx.Request, m: p.Message) -> httpx.Response:
            return httpx.Response(
                200,
                content=p.encode(p.failure(m["id"], 1004, actor_address=ADDRESS)),
                headers={"Content-Type": "application/json"},
            )

        self.gateway.call_response = fail
        actor = self.runtime.addr("demo.ping", "session-42")
        for _ in range(2):
            with self.assertRaises(OutcomeUnknownError):
                await actor.call(None)
        self.assertEqual(actor.address, ADDRESS)

    async def test_business_terminal_without_address_cannot_leave_a_cold_handle(self) -> None:
        async def fail(_: httpx.Request, m: p.Message) -> httpx.Response:
            return httpx.Response(
                200,
                content=p.encode(p.failure(m["id"], 1000)),
                headers={"Content-Type": "application/json"},
            )

        self.gateway.call_response = fail
        actor = self.runtime.addr("demo.ping", "session-42")
        with self.assertRaises(OutcomeUnknownError):
            await actor.call(None)
        with self.assertRaises(AddressUncertainError):
            await actor.call(None)

    async def test_rejections_do_not_poison_cold_address(self) -> None:
        actor = self.runtime.addr("demo.ping", "session-42")
        for status, cls in (
            (401, AuthenticationError),
            (403, SessionError),
            (400, TransportError),
            (413, TransportError),
            (307, TransportError),
        ):

            async def reject(_: httpx.Request, m: p.Message) -> httpx.Response:
                return httpx.Response(status, headers={"Location": "http://other.test/"})

            self.gateway.call_response = reject
            before = len(self.gateway.requests)
            with self.assertRaises(cls):
                await actor.call({})
            self.assertEqual(len(self.gateway.requests), before + 1)  # no redirect/retry
            self.assertFalse(actor._uncertain)

        async def reject_params(_: httpx.Request, m: p.Message) -> httpx.Response:
            return httpx.Response(
                200,
                content=p.encode(p.failure(m["id"], -32602)),
                headers={"Content-Type": "application/json"},
            )

        self.gateway.call_response = reject_params
        with self.assertRaises(RemoteError):
            await actor.call(None)
        self.assertFalse(actor._uncertain)
        self.gateway.call_response = None
        await actor.call({})

    async def test_connection_establishment_failure_is_definite(self) -> None:
        async def fail(request: httpx.Request, _: p.Message) -> httpx.Response:
            raise httpx.ConnectError("private diagnostic", request=request)

        self.gateway.call_response = fail
        actor = self.runtime.addr("demo.ping", "session-42")
        with self.assertRaises(TransportError) as raised:
            await actor.call({})
        self.assertNotIn("private diagnostic", str(raised.exception))
        self.assertFalse(actor._uncertain)

    async def test_malformed_response_never_succeeds_or_reselects(self) -> None:
        for reply in (
            p.success("wrong-id", {"address": ADDRESS, "value": None}),
            p.success("replace", {"address": dict(ADDRESS, id="other"), "value": None}),
            p.success("replace", {"address": ADDRESS}),
            {"jsonrpc": "2.0", "id": "replace", "result": None, "error": None},
            {
                "jsonrpc": "2.0",
                "id": "replace",
                "error": {
                    "code": 1000,
                    "message": "x",
                    "data": {"kind": "outcome_unknown", "address": ADDRESS},
                },
            },
        ):

            async def malformed(_: httpx.Request, m: p.Message) -> httpx.Response:
                value = dict(reply)
                if value["id"] == "replace":
                    value["id"] = m["id"]
                return httpx.Response(
                    200, content=p.encode(value), headers={"Content-Type": "application/json"}
                )

            self.gateway.call_response = malformed
            actor = self.runtime.addr("demo.ping", "session-42")
            with self.assertRaises(OutcomeUnknownError):
                await actor.call({})
            with self.assertRaises(AddressUncertainError):
                await actor.call({})

    async def test_fixed_address_rejects_new_generation(self) -> None:
        actor = self.runtime.addr("demo.ping", "session-42")
        await actor.call(None)

        async def changed(_: httpx.Request, m: p.Message) -> httpx.Response:
            a = dict(ADDRESS, generation="AQEBAQEBAQEBAQEBAQEBAQ")
            return httpx.Response(
                200,
                content=p.encode(p.success(m["id"], {"address": a, "value": None})),
                headers={"Content-Type": "application/json"},
            )

        self.gateway.call_response = changed
        with self.assertRaises(OutcomeUnknownError):
            await actor.call(None)
        self.assertEqual(actor.address, ADDRESS)

    async def test_call_timeout_and_cancellation_have_no_retry(self) -> None:
        blocked = asyncio.Event()

        async def hang(_: httpx.Request, m: p.Message) -> None:
            blocked.set()
            await asyncio.Event().wait()

        self.gateway.call_response = cast(HTTPHandler, hang)
        actor = self.runtime.addr("demo.ping", "session-42")
        with patch("actorx_remote.addr.CALL_SECONDS", 0.05):
            with self.assertRaises(OutcomeUnknownError):
                await actor.call(None)
        self.assertTrue(blocked.is_set())
        with self.assertRaises(AddressUncertainError):
            await actor.call(None)
        self.assertEqual(len(self.gateway.requests), 1)

        # Repeat with caller cancellation after the HTTP request has been admitted.
        blocked.clear()
        actor = self.runtime.addr("demo.ping", "session-42")
        task = asyncio.create_task(actor.call(None))
        await bounded(blocked.wait())
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        with self.assertRaises(AddressUncertainError):
            await actor.call(None)

    async def test_runtime_close_joins_an_active_consumer_call(self) -> None:
        sent = asyncio.Event()
        canceled = asyncio.Event()

        async def hang(_: httpx.Request, m: p.Message) -> None:
            sent.set()
            try:
                await asyncio.Event().wait()
            finally:
                canceled.set()

        self.gateway.call_response = cast(HTTPHandler, hang)
        actor = self.runtime.addr("demo.ping", "session-42")
        call = asyncio.create_task(actor.call(None))
        await bounded(sent.wait())
        await bounded(self.runtime.close())
        with self.assertRaises(asyncio.CancelledError):
            await call
        self.assertTrue(canceled.is_set())
        self.assertTrue(actor._uncertain)
        self.assertFalse(self.runtime._calls)

    async def test_register_creates_no_objects_reuses_and_isolates(self) -> None:
        created = []

        class Counter:
            def __init__(self) -> None:
                self.count = 0
                created.append(weakref.ref(self))

            async def handle(self, _: JSONValue) -> JSONValue:
                self.count += 1
                return self.count

        await self.register(Counter)
        self.assertEqual(created, [])

        await self.create()
        await self.create()  # same complete identity is idempotent
        self.assertEqual(len(created), 1)

        for n in (1, 2):
            rpc_id = self.gateway.send()
            self.assertEqual((await self.gateway.response(rpc_id))["result"]["value"], n)

        # A different ID or generation owns a separate business object.
        for a in (dict(ADDRESS, id="other"), dict(ADDRESS, generation="AQEBAQEBAQEBAQEBAQEBAQ")):
            await self.create(a)
            rpc_id = self.gateway.send(address=a)
            self.assertEqual((await self.gateway.response(rpc_id))["result"]["value"], 1)
        self.assertEqual(len(created), 3)

        await self.runtime.close()
        self.assertTrue(all(ref() is None for ref in created))

    async def test_factory_handler_errors_and_non_json_result_are_safe(self) -> None:
        def factory() -> Never:
            raise RuntimeError("secret credential must not escape")

        registration = await self.register(factory)
        rpc_id = self.gateway.send("actor/create")
        error = (await self.gateway.response(rpc_id))["error"]
        self.assertEqual(error["code"], 1000)
        self.assertNotIn("secret", str(error))

        class Fails:
            async def handle(self, message: JSONValue) -> object:
                if message == "error":
                    raise RuntimeError("secret")
                return object()

        registration.factory = cast(ActorFactory, Fails)
        await self.create()
        for value in ("error", "non-json"):
            rpc_id = self.gateway.send(value=value)
            self.assertEqual((await self.gateway.response(rpc_id))["error"]["code"], 1000)

    async def test_provider_params_unknown_method_notification_and_null_id(self) -> None:
        await self.register()
        await self.create()
        self.gateway.stream.send(p.request("future/method", "unknown", {}))
        self.assertEqual((await self.gateway.response("unknown"))["error"]["code"], -32601)
        params: object
        for params in (
            [],
            {"address": ADDRESS},
            {"address": ADDRESS, "input": None, "deadline_unix_ms": 1, "extra": True},
        ):
            self.gateway.stream.send(p.request("actor/invoke", str(params), params))
            self.assertEqual((await self.gateway.response())["error"]["code"], -32602)

        # A missing ID suppresses replies; explicit null still receives one.
        self.gateway.stream.send({"jsonrpc": "2.0", "method": "unknown"})
        rpc_id = self.gateway.send(rpc_id=None)
        response = await self.gateway.response()
        self.assertIsNone(response["id"])
        self.assertIn("result", response)
        self.assertTrue(self.gateway.responses.empty())

    async def test_numeric_provider_id_is_echoed_without_float_roundtrip(self) -> None:
        await self.register()
        await self.create()
        for rpc_id in (900719925474099312345, Decimal("1.234567890123456789")):
            self.gateway.send(value=None, rpc_id=rpc_id)
            response = await self.gateway.response(rpc_id)
            self.assertEqual(type(response["id"]), type(rpc_id))

    async def test_unexpected_response_closes_registration(self) -> None:
        await self.register()
        await self.create()
        self.gateway.stream.send(p.success("unsolicited", None))
        with self.assertRaises(ProtocolError):
            await bounded(self.runtime.wait_closed())

    async def test_reused_provider_rpc_id_is_rejected(self) -> None:
        await self.register()
        await self.create()
        rpc_id = self.gateway.send(value=None)
        await self.gateway.response(rpc_id)
        self.gateway.send(value=None, rpc_id=rpc_id)
        with self.assertRaises(ProtocolError):
            await bounded(self.runtime.wait_closed())
        self.assertTrue(self.gateway.responses.empty())

    async def test_provider_session_failure_closes_runtime(self) -> None:
        await self.register()
        self.gateway.reply_status = 403
        rpc_id = self.gateway.send("actor/create")
        await self.gateway.response(rpc_id)
        with self.assertRaises(SessionError):
            await bounded(self.runtime.wait_closed())

    async def test_registration_failure_rolls_back_without_reconnect(self) -> None:
        async def fail(_: httpx.Request, message: p.Message) -> httpx.Response:
            return httpx.Response(
                200,
                content=p.encode(p.failure(message["id"], 1007)),
                headers={"Content-Type": "application/json"},
            )

        self.gateway.register_response = fail
        with self.assertRaises(RemoteError) as raised:
            await self.runtime.register("demo.ping", Echo)
        self.assertEqual(raised.exception.rpc_code, 1007)
        self.assertFalse(self.runtime._registrations)
        self.assertEqual(len(self.gateway.requests), 1)

        # An explicit later registration is allowed; no automatic reconnect ran.
        self.gateway.register_response = None
        await self.register()
        with self.assertRaises(RemoteError):
            await self.runtime.register("demo.ping", Echo)
        self.assertEqual(len(self.gateway.requests), 2)

    async def test_registration_validates_first_frame(self) -> None:
        for update in ({"protocol": "future"}, {"type": "other"}, {"session_token": ""}):

            async def invalid(_: httpx.Request, message: p.Message) -> httpx.Response:
                info = {
                    "protocol": p.VERSION,
                    "type": "demo.ping",
                    "session_token": "session-token",
                }
                info.update(update)
                self.gateway.stream.send(p.success(message["id"], info))
                return httpx.Response(
                    200, headers={"Content-Type": "text/event-stream"}, stream=self.gateway.stream
                )

            self.gateway.register_response = invalid
            with self.assertRaises(ProtocolError):
                await self.runtime.register("demo.ping", Echo)
            self.assertFalse(self.runtime._registrations)
            self.assertTrue(self.gateway.stream.closed.is_set())

    async def test_registration_timeout_rolls_back_and_closes_stream(self) -> None:
        async def no_frame(_: httpx.Request, message: p.Message) -> httpx.Response:
            return httpx.Response(
                200, headers={"Content-Type": "text/event-stream"}, stream=self.gateway.stream
            )

        self.gateway.register_response = no_frame
        with patch("actorx_remote.runtime.REGISTER_SECONDS", 0.05):
            with self.assertRaises(TimeoutError):
                await self.runtime.register("demo.ping", Echo)
        self.assertTrue(self.gateway.stream.closed.is_set())
        self.assertFalse(self.runtime._registrations)

    async def test_registration_rejects_uncorrelated_first_frame(self) -> None:
        async def invalid(_: httpx.Request, message: p.Message) -> httpx.Response:
            self.gateway.stream.send(
                p.success(
                    "other-id",
                    {"protocol": p.VERSION, "type": "demo.ping", "session_token": "session-token"},
                )
            )
            return httpx.Response(
                200, headers={"Content-Type": "text/event-stream"}, stream=self.gateway.stream
            )

        self.gateway.register_response = invalid
        with self.assertRaises(ProtocolError):
            await self.runtime.register("demo.ping", Echo)
        self.assertFalse(self.runtime._registrations)
        self.assertTrue(self.gateway.stream.closed.is_set())

    async def test_async_context_exit_reclaims_provider(self) -> None:
        async with self.runtime:
            await self.register()
            await self.create()
        await self.runtime.wait_closed()
        self.assertTrue(self.runtime._http._client.is_closed)

    async def test_eof_reclaims_registration_and_wait_closed_reports_failure(self) -> None:
        await self.register()
        await self.create()
        self.gateway.stream.queue.put_nowait(None)
        with self.assertRaises(TransportError):
            await bounded(self.runtime.wait_closed())
        self.assertEqual(len(self.gateway.requests), 2)

    async def test_sse_fragmentation_and_frame_limit(self) -> None:
        await self.register()
        message = p.request("future/method", "split", {})
        raw = b"data: " + p.encode(message) + b"\n\n"
        for byte in raw:
            self.gateway.stream.queue.put_nowait(bytes([byte]))
        self.assertEqual((await self.gateway.response("split"))["error"]["code"], -32601)

        # An incomplete over-limit frame must also close the provider connection.
        self.gateway.stream.queue.put_nowait(b"data: " + b"x" * 8193)
        with self.assertRaises(ProtocolError):
            await bounded(self.runtime.wait_closed())

    async def test_late_response_409_is_not_retried(self) -> None:
        await self.register()
        self.gateway.reply_status = 409
        await self.create()
        await self.create(dict(ADDRESS, id="other"))
        self.assertEqual(len(self.gateway.requests), 3)


class ConstructionTests(unittest.TestCase):
    def test_reject_invalid_configuration_without_io(self) -> None:
        for options in (
            {"api_key": ""},
            {"api_key": "contains space"},
            {"api_key": "line\nbreak"},
            {"api_key": "ok", "transport": "ws"},
        ):
            with self.assertRaises(ValueError):
                Runtime("http://localhost", **options)
        for url in (
            "file:///tmp/rpc",
            "http://user:key@localhost",
            "http://localhost/?key=secret",
            "http://localhost/#secret",
            "http://localhost/other",
        ):
            with self.assertRaises(ValueError):
                Runtime(url, api_key="key")
