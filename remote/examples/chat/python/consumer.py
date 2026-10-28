"""Call and stream a simulated chat answer from one Actor."""

from __future__ import annotations

import argparse
import asyncio
import os

from actorx_remote import Runtime


async def main(args: argparse.Namespace) -> None:
    async with Runtime(args.server, api_key=os.environ["REMOTE_API_KEY"]) as runtime:
        actor = runtime.addr(args.type, args.id)

        # Both operations use the same remote actor instance.
        answer = await actor.call({"topic": "Actors"})
        print(f"call turn: {answer['turn']}", flush=True)

        print("answer: ", end="", flush=True)
        async with actor.stream({"topic": "Actors"}) as chunks:
            async for chunk in chunks:
                print(chunk["text"], end="", flush=True)
        print()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server", default="http://127.0.0.1:8080")
    parser.add_argument("--type", default="demo.chat")
    parser.add_argument("--id", default="assistant-42")
    asyncio.run(main(parser.parse_args()))
