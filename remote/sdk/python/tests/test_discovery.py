from __future__ import annotations

import asyncio
import unittest
from pathlib import Path

import httpx
from actorx_remote import ProtocolError, RemoteError, Runtime
from actorx_remote import protocol as p
from actorx_remote.discovery import decode_actors


class DirectoryTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.runtime = Runtime("http://gateway.test", api_key="test-key")
        self.requests: list[p.Message] = []
        self.result: object = {"actors": []}
        self.error: int | None = None
        self.block = False
        self.entered = asyncio.Event()

        async def handle(request: httpx.Request) -> httpx.Response:
            m = p.envelope(request.content)
            self.requests.append(m)
            self.assertEqual(m["method"], "runtime/actors")
            self.assertEqual(request.headers["Accept"], "application/json")
            self.assertNotIn("X-Actorx-Remote-Session", request.headers)
            self.entered.set()
            if self.block:
                await asyncio.Event().wait()
            message = (
                p.failure(m["id"], self.error)
                if self.error is not None
                else p.success(m["id"], self.result)
            )
            return httpx.Response(
                200,
                content=p.encode(message),
                headers={"Content-Type": "application/json"},
            )

        await self.runtime._http._client.aclose()
        self.runtime._http._client = httpx.AsyncClient(
            transport=httpx.MockTransport(handle)
        )

    async def asyncTearDown(self) -> None:
        await self.runtime.close()
        self.assertFalse(self.runtime._calls)

    async def test_directory_observation(self) -> None:
        self.assertEqual(await self.runtime.actors(), self.result)
        self.assertEqual(self.requests[-1]["params"], {})
        self.result = {
            "actors": [{"type": "demo", "description": "", "protocol": p.VERSION}]
        }
        self.assertEqual(await self.runtime.actors(), self.result)
        self.assertEqual(self.requests[-1]["params"], {})
        self.assertEqual(len(self.requests), 2)

    async def test_remote_failure_and_close_cancel_without_retry(self) -> None:
        self.error = 1010
        with self.assertRaises(RemoteError) as raised:
            await self.runtime.actors()
        self.assertEqual(raised.exception.rpc_code, 1010)
        self.error = 1001
        with self.assertRaises(RemoteError) as raised:
            await self.runtime.actors()
        self.assertEqual(
            (raised.exception.code, raised.exception.rpc_code), ("full", 1001)
        )

        # Close cancels the tracked query and joins it before returning.
        self.error = None
        self.block = True
        self.entered.clear()
        task = asyncio.create_task(self.runtime.actors())
        await self.entered.wait()
        await self.runtime.close()
        with self.assertRaises(asyncio.CancelledError):
            await task
        with self.assertRaises(RemoteError):
            await self.runtime.actors()
        self.assertEqual(len(self.requests), 3)

    async def test_shared_vectors(self) -> None:
        vectors = p.decode(
            (
                Path(__file__).resolve().parents[3] / "protocol/testdata/v1.json"
            ).read_bytes()
        )
        self.result = vectors["actors"]["result"]
        self.assertEqual(await self.runtime.actors(), self.result)
        error = p.remote_error(vectors["actors_limit"]["error"])
        self.assertEqual((error.code, error.rpc_code), ("full", 1001))

    def test_malformed_results(self) -> None:
        valid = {"actors": [{"type": "demo", "description": "", "protocol": p.VERSION}]}
        self.assertEqual(decode_actors({"actors": []}), {"actors": []})

        for actors in (
            None,
            {},
            [{}],
            [dict(valid["actors"][0], description=None)],
            [dict(valid["actors"][0], description="x" * 513)],
            [dict(valid["actors"][0], protocol="actorx.remote/v2")],
            valid["actors"] * 2,
        ):
            with self.subTest(actors=actors), self.assertRaises(ProtocolError):
                decode_actors(dict(valid, actors=actors))
        for bad in ({}, dict(valid, discovery={}), dict(valid, scope="cluster")):
            with self.assertRaises(ProtocolError):
                decode_actors(bad)
