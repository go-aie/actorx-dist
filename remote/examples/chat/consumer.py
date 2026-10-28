"""Consume a simulated chat answer as it arrives."""

from __future__ import annotations

import argparse
import asyncio
import os

from actorx_remote import Runtime


async def main(args: argparse.Namespace) -> None:
    async with Runtime(args.server, api_key=os.environ["REMOTE_API_KEY"]) as runtime:
        actor = runtime.addr(args.type, args.id)
        print("answer: ", end="", flush=True)
        count = 0
        # Context exit explicitly cancels unfinished streams, including on break.
        async with actor.stream({"topic": "Actors"}, timeout=30) as chunks:
            async for chunk in chunks:
                print(chunk["text"], end="", flush=True)
                count += 1
                # A chunk is acknowledged when iteration requests the next one.
                if args.consume_delay:
                    await asyncio.sleep(args.consume_delay)
        print(f"\nchunks: {count}", flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server", default="http://127.0.0.1:8080")
    parser.add_argument("--type", default="demo.chat")
    parser.add_argument("--id", default="assistant-42")
    parser.add_argument(
        "--consume-delay",
        type=float,
        default=0,
        help="Seconds to spend processing each chunk (e.g. 0.1)",
    )
    args = parser.parse_args()
    if not 0 <= args.consume_delay <= 3:
        parser.error("--consume-delay must be between 0 and 3 seconds")
    asyncio.run(main(args))
