"""Call one Actor repeatedly, then call a second logical ID."""

from __future__ import annotations

import argparse
import asyncio
import json
import os

from actorx_remote import Runtime


async def main(args: argparse.Namespace) -> None:
    async with Runtime(args.server, api_key=os.environ["REMOTE_API_KEY"]) as runtime:
        actor = runtime.addr(args.type, args.id)

        # Reusing the same Addr also reuses the remote Actor's state.
        for _ in range(args.count):
            pong = await actor.call({"text": "ping"})
            print(json.dumps(pong), flush=True)

        # A different logical ID must have its own instance and counter.
        other = runtime.addr(args.type, args.id + "-other")
        pong = await other.call({"text": "ping"})
        print(json.dumps(pong), flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server", default="http://127.0.0.1:8080")
    parser.add_argument("--type", default="demo.ping")
    parser.add_argument("--id", default="session-42")
    parser.add_argument("--count", type=int, default=2)
    args = parser.parse_args()
    if args.count < 1:
        parser.error("--count must be positive")
    asyncio.run(main(args))
