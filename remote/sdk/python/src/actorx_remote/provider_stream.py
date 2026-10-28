"""Provider-side window accounting, independent from user execution tasks."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any

from . import protocol as p
from .errors import ProtocolError
from .stream import WINDOW, sequence, token

if TYPE_CHECKING:
    from .runtime import _Registration, _Work


class ProviderStream:
    """Track one provider segment through final response and outstanding credits."""

    def __init__(self, registration: _Registration, work: _Work) -> None:
        self.registration, self.work = registration, work
        params = work.message["params"]
        self.token = token(params["stream_token"])
        if params["window"] != WINDOW or any(
            type(v) is not int for v in params["window"].values()
        ):
            raise ProtocolError("Unsupported stream window")
        self.seq = 0
        self.ack = 0
        # Charges remain until cumulative credit arrives, even after success.
        self.sizes: dict[int, int] = {}
        self.wake = asyncio.Event()
        self.finished = False
        self.expiry = asyncio.get_running_loop().call_at(work.deadline, self.remove)

    def remove(self) -> None:
        streams = self.registration.streams
        rpc_id = self.work.message["id"]
        if streams.get(rpc_id) is self:
            del streams[rpc_id]
        self.expiry.cancel()

    def control(self, message: p.Message) -> None:
        method = message["method"]
        required = ["request_id", "stream_token"]
        if method == "actor/credit":
            required.append("ack_seq")
        c = p.fields(message.get("params"), required, required)
        rpc_id = self.work.message["id"]
        if (
            type(c["request_id"]) is not type(rpc_id)
            or c["request_id"] != rpc_id
            or c["stream_token"] != self.token
        ):
            raise ProtocolError("Stream control correlation mismatch")
        if method == "actor/cancel":
            if not self.finished:
                self.work.cancel_operation()
            return

        seq = sequence(c["ack_seq"])
        if seq > self.seq:
            raise ProtocolError("Credit exceeds sent sequence")
        if seq > self.ack:
            self.ack = seq
            self.sizes = {i: n for i, n in self.sizes.items() if i > seq}
            self.wake.set()

        # Terminal POST completion does not acknowledge the final data items.
        if self.finished and not self.sizes:
            self.remove()

    def check_sending(self) -> None:
        if (
            self.finished
            or self.registration.closing
            or asyncio.get_running_loop().time() >= self.work.deadline
        ):
            raise asyncio.CancelledError()

    async def send(self, value: Any) -> None:
        self.check_sending()
        seq = sequence(self.seq + 1)
        m = p.notification(
            "actor/chunk",
            {
                "request_id": self.work.message["id"],
                "stream_token": self.token,
                "address": self.work.address,
                "seq": seq,
                "value": value,
            },
        )
        raw = p.encode(m)

        # Charge encoded envelope bytes, not just the business value. Only
        # consumer credit reopens the window; a successful POST is delivery.
        while len(self.sizes) >= 4 or sum(self.sizes.values()) + len(raw) > 32768:
            self.wake.clear()
            await self.wake.wait()
        self.check_sending()
        self.seq = seq
        self.sizes[seq] = len(raw)

        # Each stream's POSTs are serialized, including its final response.
        async with asyncio.timeout_at(self.work.deadline):
            await self.registration.runtime._http.reply(raw, self.registration.token)
