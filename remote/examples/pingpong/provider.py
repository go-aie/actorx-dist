"""Register a Python Actor factory; the SDK owns its instances and replies."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import signal
import uuid

from actorx_remote import JSONValue, Runtime


class PingActor:
    def __init__(self) -> None:
        # Each factory call represents one complete remote Actor identity.
        self.instance = uuid.uuid4().hex
        self.calls = 0
        print(json.dumps({"event": "created", "instance": self.instance}), flush=True)

    async def handle(self, message: JSONValue) -> JSONValue:
        if message != {"text": "ping"}:
            raise ValueError("Expected Ping")

        self.calls += 1
        print(
            json.dumps({"event": "invoke", "instance": self.instance, "calls": self.calls}),
            flush=True,
        )
        return {"text": "pong", "instance": self.instance, "calls": self.calls}


async def main(args: argparse.Namespace) -> None:
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    async with Runtime(args.server, api_key=os.environ["REMOTE_API_KEY"]) as runtime:
        # Registration publishes a factory; the first Call creates an instance.
        await runtime.register(args.type, PingActor, description="Python Ping/Pong")
        print(json.dumps({"event": "registered", "type": args.type}), flush=True)

        # Exit on either a local signal or an SDK connection failure. Awaiting
        # closed propagates the latter, while the context manager owns cleanup.
        closed = asyncio.create_task(runtime.wait_closed())
        stopping = asyncio.create_task(stop.wait())
        try:
            done, _ = await asyncio.wait((closed, stopping), return_when=asyncio.FIRST_COMPLETED)
            if closed in done:
                await closed
        finally:
            # Join both local waiters before the Runtime context closes the SDK.
            closed.cancel()
            stopping.cancel()
            await asyncio.gather(closed, stopping, return_exceptions=True)

    print(json.dumps({"event": "closed"}), flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server", default="http://127.0.0.1:8080")
    parser.add_argument("--type", default="demo.ping")
    asyncio.run(main(parser.parse_args()))
