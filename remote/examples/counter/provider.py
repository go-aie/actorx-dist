"""Register a Python Actor; the SDK creates instances on demand and sends replies."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import signal

from actorx_remote import JSONValue, Runtime


class Counter:
    def __init__(self) -> None:
        self.count = 0
        print(json.dumps({"event": "created"}), flush=True)

    async def handle(self, message: JSONValue) -> JSONValue:
        if not isinstance(message, dict) or set(message) != {"n"}:
            raise ValueError("Expected Add with an integer n")
        n = message["n"]
        if type(n) is not int:
            raise ValueError("Expected Add with an integer n")

        self.count += n
        print(json.dumps({"event": "invoke", "count": self.count}), flush=True)
        return self.count


async def main(args: argparse.Namespace) -> None:
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    async with Runtime(args.server, api_key=os.environ["REMOTE_API_KEY"]) as runtime:
        # Registration does not create an instance; the first Call for a new ID does.
        await runtime.register(args.type, Counter, description="Python Counter")
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
    parser.add_argument("--type", default="demo.counter")
    asyncio.run(main(parser.parse_args()))
