"""Register a Python Actor; the SDK creates instances on demand and sends replies."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import signal

from actorx_remote import JSONValue, Runtime


class ChatActor:
    def __init__(self) -> None:
        self.turns = 0
        print("Chat actor created", flush=True)

    async def handle_stream(self, message: JSONValue):
        if message != {"topic": "Actors"}:
            raise ValueError("Expected a topic")
        self.turns += 1
        print(f"Chat turn {self.turns}", flush=True)
        # Six fragments exceed the four-item transport window. No model needed.
        for text in ("Actors", " use", " messages", " to", " coordinate", " work."):
            yield {"text": text}


async def main(args: argparse.Namespace) -> None:
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    async with Runtime(args.server, api_key=os.environ["REMOTE_API_KEY"]) as runtime:
        # Registration does not create an instance; the first Stream for a new ID does.
        await runtime.register(
            args.type, ChatActor, description="Python streaming chat"
        )
        print(json.dumps({"event": "registered", "type": args.type}), flush=True)

        # Exit on either a local signal or an SDK connection failure. Awaiting
        # closed propagates the latter, while the context manager owns cleanup.
        closed = asyncio.create_task(runtime.wait_closed())
        stopping = asyncio.create_task(stop.wait())
        try:
            done, _ = await asyncio.wait(
                (closed, stopping), return_when=asyncio.FIRST_COMPLETED
            )
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
    parser.add_argument("--type", default="demo.chat")
    asyncio.run(main(parser.parse_args()))
