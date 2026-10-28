"""Deadline/lock interleavings are driven by events, never scheduling sleeps."""

from __future__ import annotations

import asyncio
import time
import unittest
from typing import cast

from actorx_remote import JSONValue, Runtime
from actorx_remote import protocol as p
from actorx_remote.runtime import _Work
from actorx_remote.types import RPCID
from support import ADDRESS, Gateway, bounded


class ExecutionTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.runtime = Runtime("http://gateway.test", api_key="test-key")
        self.gateway = Gateway()
        await self.gateway.install(self.runtime)
        self.release = asyncio.Event()
        self.started = asyncio.Event()
        self.canceled = asyncio.Event()
        self.entered: list[JSONValue] = []
        self.running = 0
        self.maximum = 0
        test = self

        class Actor:
            async def handle(self, message: JSONValue) -> JSONValue:
                test.entered.append(message)
                if message == "other-id":
                    return message
                test.running += 1
                test.maximum = max(test.maximum, test.running)
                try:
                    if message in ("hold", "ignore-cancel"):
                        test.started.set()
                        try:
                            await test.release.wait()
                        except asyncio.CancelledError:
                            test.canceled.set()
                            if message != "ignore-cancel":
                                raise
                            await test.release.wait()
                    return message
                finally:
                    test.running -= 1

        await self.runtime.register("demo.ping", Actor)
        self.registration = self.runtime._registrations["demo.ping"]

        # Observe local settlement even when an expired RPC cannot POST a reply.
        self.settled: asyncio.Queue[_Work] = asyncio.Queue()
        settle = self.registration.settle

        async def observe(work: _Work, terminal: p.Message) -> None:
            await settle(work, terminal)
            self.settled.put_nowait(work)

        self.registration.settle = observe  # type: ignore[method-assign]  # Observe settlement in this test.

        for a in (ADDRESS, dict(ADDRESS, id="other")):
            rpc_id = self.gateway.send("actor/create", address=a)
            await self.gateway.response(rpc_id)
            work = await bounded(self.settled.get())
            await bounded(cast(asyncio.Task[None], work.job))

    async def asyncTearDown(self) -> None:
        self.release.set()
        await bounded(self.runtime.close())
        self.assertFalse(self.registration.active)
        self.assertEqual(self.running, 0)
        self.assertFalse(
            [
                t
                for t in asyncio.all_tasks()
                if t is not asyncio.current_task() and t.get_name().startswith("actorx-remote-")
            ]
        )

    def deadline(self, ms: int) -> int:
        return int(time.time() * 1000) + ms

    async def terminal(self, rpc_id: RPCID, code: int | None = None) -> _Work:
        work = await bounded(self.settled.get())
        self.assertEqual(work.message["id"], rpc_id)
        if code is not None:
            self.assertEqual(cast(p.Message, work.terminal)["error"]["code"], code)
        return work

    async def test_queued_deadline_expires_without_entering_handler(self) -> None:
        first = self.gateway.send(value="hold")
        await bounded(self.started.wait())

        second = self.gateway.send(value="expired", ms=self.deadline(100))
        expired = await self.terminal(second, 1005)
        self.assertFalse(expired.started)
        await bounded(cast(asyncio.Task[None], expired.job))
        self.assertEqual(self.entered, ["hold"])
        self.assertTrue(self.gateway.responses.empty())  # cannot extend the expired POST budget

        # Another instance can still progress while the first holds its lock.
        other = self.gateway.send(address=dict(ADDRESS, id="other"), value="other-id")
        self.assertEqual((await self.gateway.response(other))["result"]["value"], "other-id")
        await self.terminal(other)
        self.release.set()
        await self.gateway.response(first)
        await self.terminal(first)
        self.assertNotIn("expired", self.entered)

    async def test_timeout_keeps_lock_and_slot_until_handler_really_exits(self) -> None:
        first = self.gateway.send(value="ignore-cancel", ms=self.deadline(100))
        await bounded(self.started.wait())
        second = self.gateway.send(value="expired-behind", ms=self.deadline(300))
        timed_out = await self.terminal(first, 1004)
        await bounded(self.canceled.wait())
        self.assertTrue(timed_out.started)
        self.assertFalse(cast(asyncio.Task[p.Message], timed_out.operation).done())
        self.assertIn(timed_out, self.registration.active)

        expired = await self.terminal(second, 1005)
        await bounded(cast(asyncio.Task[None], expired.job))
        self.assertEqual(self.entered, ["ignore-cancel"])
        other = self.gateway.send(address=dict(ADDRESS, id="other"), value="other-id")
        await self.gateway.response(other)
        await self.terminal(other)

        # This request has time remaining, but may only enter after actual exit.
        third = self.gateway.send(value="after-exit")
        # A response from another ID is a processing barrier for SSE reception.
        barrier = self.gateway.send(address=dict(ADDRESS, id="other"), value="other-id")
        await self.gateway.response(barrier)
        await self.terminal(barrier)
        self.assertNotIn("after-exit", self.entered)

        # Actual handler exit, rather than RPC timeout, opens the instance lock.
        self.release.set()
        await bounded(cast(asyncio.Task[None], timed_out.job))
        self.assertEqual((await self.gateway.response(third))["result"]["value"], "after-exit")
        await self.terminal(third)
        self.assertEqual(self.maximum, 1)
        self.assertTrue(self.gateway.responses.empty())  # no late success for first or second
        self.assertNotIn(timed_out, self.registration.active)

    async def test_eight_slots_include_queued_and_timed_out_handlers(self) -> None:
        first = self.gateway.send(value="ignore-cancel", ms=self.deadline(100))
        await bounded(self.started.wait())
        timed_out = await self.terminal(first, 1004)
        await bounded(self.canceled.wait())
        queued = [self.gateway.send(value="queued-" + str(i)) for i in range(7)]

        # The timed-out handler still owns one slot; the seven queued jobs fill it.
        overflow = self.gateway.send(value="overflow")
        self.assertEqual((await self.gateway.response(overflow))["error"]["code"], 1001)
        await self.terminal(overflow, 1001)
        self.assertEqual(len(self.registration.active), 8)
        self.assertIn(timed_out, self.registration.active)
        self.assertNotIn("overflow", self.entered)
        self.release.set()
        await bounded(cast(asyncio.Task[None], timed_out.job))
        for rpc_id in queued:
            await self.gateway.response(rpc_id)
            await self.terminal(rpc_id)
        self.assertEqual(self.maximum, 1)

    async def test_runtime_close_joins_cooperative_handlers_and_queue(self) -> None:
        self.gateway.send(value="hold")
        await bounded(self.started.wait())
        self.gateway.send(value="queued")
        await bounded(self.runtime.close())
        self.assertTrue(self.canceled.is_set())
        self.assertFalse(self.registration.active)
        self.assertEqual(self.entered, ["hold"])
        self.assertTrue(self.gateway.stream.closed.is_set())
        self.assertTrue(self.runtime._http._client.is_closed)

    async def test_instance_limit_is_by_complete_identity(self) -> None:
        for i in range(6):
            rpc_id = self.gateway.send("actor/create", address=dict(ADDRESS, id=str(i)))
            await self.gateway.response(rpc_id)
            await self.terminal(rpc_id)
        overflow = self.gateway.send("actor/create", address=dict(ADDRESS, id="ninth"))
        self.assertEqual((await self.gateway.response(overflow))["error"]["code"], 1001)
        await self.terminal(overflow, 1001)
        self.assertEqual(len(self.registration.instances), 8)

    async def test_already_expired_request_has_determinate_terminal_and_never_runs(self) -> None:
        rpc_id = self.gateway.send(value="expired-before-admission", ms=self.deadline(-100))
        work = await self.terminal(rpc_id, 1005)
        self.assertFalse(work.started)
        self.assertIsNone(work.operation)
        self.assertNotIn("expired-before-admission", self.entered)
        self.assertTrue(self.gateway.responses.empty())

    async def test_canceling_close_caller_preserves_cleanup_ownership(self) -> None:
        rpc_id = self.gateway.send(value="ignore-cancel", ms=self.deadline(100))
        await bounded(self.started.wait())
        work = await self.terminal(rpc_id, 1004)
        await bounded(self.canceled.wait())
        closing = asyncio.create_task(self.runtime.close())
        await bounded(self.gateway.stream.closed.wait())
        self.assertFalse(cast(asyncio.Task[p.Message], work.operation).done())

        # Cancel only this close waiter; the Runtime must retain its cleanup task.
        closing.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await closing
        self.assertFalse(self.runtime._closed.is_set())

        self.release.set()
        await bounded(self.runtime.close())
        self.assertTrue(cast(asyncio.Task[p.Message], work.operation).done())
        self.assertTrue(self.runtime._closed.is_set())

    async def test_one_terminal_and_one_post_for_normal_completion(self) -> None:
        rpc_id = self.gateway.send(value="success")
        await self.gateway.response(rpc_id)
        work = await self.terminal(rpc_id)
        await bounded(cast(asyncio.Task[None], work.job))

        # A second settlement attempt must neither replace nor resend the result.
        await self.registration.settle(work, {"different": "terminal"})
        self.assertTrue(self.gateway.responses.empty())
        self.assertEqual(cast(p.Message, work.terminal)["result"]["value"], "success")

    async def test_response_post_shares_original_deadline_and_is_not_retried(self) -> None:
        posted = asyncio.Event()
        attempts = []

        async def blocked_post(raw: bytes, session: str | None) -> None:
            attempts.append(raw)
            posted.set()
            await asyncio.Event().wait()

        self.runtime._http.reply = blocked_post  # type: ignore[method-assign]  # Hold the response POST.
        rpc_id = self.gateway.send(value="success", ms=self.deadline(100))
        await bounded(posted.wait())

        work = next(w for w in self.registration.active if w.message["id"] == rpc_id)
        await bounded(cast(asyncio.Task[None], work.job))
        self.assertEqual(cast(p.Message, work.terminal)["result"]["value"], "success")
        self.assertEqual(len(attempts), 1)
        self.assertNotIn(work, self.registration.active)
