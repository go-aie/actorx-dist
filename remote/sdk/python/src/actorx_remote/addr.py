"""Logical address which becomes fixed after the first selected generation."""

from __future__ import annotations

import asyncio
import time
import uuid
from typing import TYPE_CHECKING, cast

from . import protocol as p
from .errors import (
    AddressUncertainError,
    DeadlineExceededError,
    OutcomeUnknownError,
    ProtocolError,
    RemoteError,
    TransportError,
)
from .types import Address, JSONValue

if TYPE_CHECKING:
    from .runtime import Runtime

CALL_SECONDS = 5.0


class Addr:
    def __init__(self, runtime: Runtime, actor_type: str, actor_id: str) -> None:
        p.identity(actor_type, actor_id)
        self._runtime = runtime
        self._address: Address = {"version": 1, "type": actor_type, "id": actor_id}
        self._uncertain = False
        self._lock = asyncio.Lock()

    @property
    def address(self) -> Address:
        """Independent snapshot; generation is absent until selection."""
        return cast(Address, dict(self._address))

    async def call(self, message: JSONValue) -> JSONValue:
        self._runtime._check_open()
        # Validate and snapshot business input before scheduling or sending.
        value = p.decode(p.encode(message))
        return await self._runtime._call_task(self._call(value))

    def _save(self, value: object) -> None:
        a = p.address(value)
        if (
            a["type"] != self._address["type"]
            or a["id"] != self._address["id"]
            or ("generation" in self._address and a["generation"] != self._address["generation"])
        ):
            raise ProtocolError("Selected address mismatch")
        self._address = a

    def _unknown(self) -> None:
        if "generation" not in self._address:
            self._uncertain = True

    async def _call(self, value: JSONValue) -> JSONValue:
        loop = asyncio.get_running_loop()
        # One budget covers the handle lock, HTTP dispatch and response. Only
        # the wire deadline uses wall-clock time; local waiting stays monotonic.
        deadline = loop.time() + CALL_SECONDS
        deadline_ms = int((time.time() + CALL_SECONDS) * 1000)
        dispatched = False
        try:
            async with asyncio.timeout_at(deadline):
                async with self._lock:
                    if self._uncertain:
                        raise AddressUncertainError()
                    self._runtime._check_open()

                    rpc_id = "c:" + uuid.uuid4().hex
                    raw = p.encode(
                        p.request(
                            "actor/call",
                            rpc_id,
                            {
                                "address": self._address,
                                "input": value,
                                "deadline_unix_ms": deadline_ms,
                            },
                        )
                    )
                    dispatched = True
                    reply = p.response(await self._runtime._http.call(raw), rpc_id)

                    if "error" in reply:
                        e = p.remote_error(reply["error"])
                        # Save selected identity even when the business call failed.
                        if isinstance(e.data, dict) and "address" in e.data:
                            self._save(e.data["address"])
                        if "generation" not in self._address and e.rpc_code in (1000, -32603, 1012):
                            self._unknown()
                            raise OutcomeUnknownError(
                                "outcome_unknown", 1004, "Call has no selected address evidence"
                            ) from e
                        if e.rpc_code in (1003, 1004):
                            self._unknown()
                        raise e

                    result = p.fields(reply["result"], ("address", "value"), ("address", "value"))
                    self._save(result["address"])
                    return cast(JSONValue, result["value"])
        except TimeoutError:
            if dispatched:
                self._unknown()
                raise OutcomeUnknownError(
                    "outcome_unknown", 1004, "Call outcome is unknown"
                ) from None
            raise DeadlineExceededError(
                "deadline_exceeded", 1005, "Call expired before dispatch"
            ) from None
        except asyncio.CancelledError:
            if dispatched:
                self._unknown()
            raise
        except RemoteError:
            raise
        except TransportError as exc:
            # HTTP rejection and connection establishment failure are definite
            # pre-dispatch failures. A broken response/connection is not.
            if dispatched and (exc.status is None or exc.status >= 500):
                self._unknown()
                raise OutcomeUnknownError(
                    "outcome_unknown", 1004, "Call outcome is unknown"
                ) from exc
            raise
