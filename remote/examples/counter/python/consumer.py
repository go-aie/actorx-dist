"""Call one Actor repeatedly, then call a second logical ID."""

from __future__ import annotations

import argparse
import asyncio
import os

from actorx_remote import Runtime


async def main(args: argparse.Namespace) -> None:
    async with Runtime(args.server, api_key=os.environ["REMOTE_API_KEY"]) as runtime:
        actor = runtime.addr(args.type, args.id)

        # Reusing the same Addr also reuses the remote Actor's state.
        for _ in range(2):
            count = await actor.call({"n": 5})
            print(count, flush=True)

        # A different logical ID must have its own instance and counter.
        other = runtime.addr(args.type, args.id + "-other")
        count = await other.call({"n": 5})
        print(count, flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server", default="http://127.0.0.1:8080")
    parser.add_argument("--type", default="demo.counter")
    parser.add_argument("--id", default="orders-42")
    asyncio.run(main(parser.parse_args()))
