"""Strict JSON-RPC envelopes and the frozen remote Call methods.

Fractional JSON numbers use Decimal so numeric IDs and business values survive
decoding/encoding without a float64 round trip. Ordinary Python floats are also
accepted as input. No arbitrary Python object conversion is performed.
"""

from __future__ import annotations

import base64
import json
import math
import re
import unicodedata
from collections.abc import Iterable, Mapping
from decimal import Decimal
from typing import Any, Never, TypeAlias, cast

from .errors import (
    ActivationUnknownError,
    DeadlineExceededError,
    OutcomeUnknownError,
    ProtocolError,
    RemoteError,
)
from .types import RPCID, Address, JSONValue

# Envelopes remain dynamic until each method validates its heterogeneous fields.
Message: TypeAlias = dict[str, Any]

VERSION = "actorx.remote/v1"
MAX_BYTES = 8192
CAPACITY = 8
KINDS = dict(
    zip(
        range(1000, 1013),
        (
            "remote_error",
            "full",
            "service_unavailable",
            "activation_unknown",
            "outcome_unknown",
            "deadline_exceeded",
            "stopped",
            "registration_conflict",
            "transport_mode",
            "canceled",
            "owner_unavailable",
            "contract",
            "protocol",
        ),
    )
)
KINDS.update(
    {
        -32700: "parse_error",
        -32600: "invalid_request",
        -32601: "method_not_found",
        -32602: "invalid_params",
        -32603: "internal_error",
    }
)


def encode(value: object, *, _check_limit: bool = True) -> bytes:
    def emit(v: object, depth: int = 0) -> str:
        if depth > 64:
            raise ValueError("JSON nesting limit exceeded")
        if v is None:
            return "null"
        if isinstance(v, bool):
            return "true" if v else "false"
        if isinstance(v, str):
            v.encode("utf-8")  # reject unpaired surrogates
            # Match Go encoding/json's HTML escaping for the encoded byte limit.
            return (
                json.dumps(v, ensure_ascii=False)
                .replace("<", "\\u003c")
                .replace(">", "\\u003e")
                .replace("&", "\\u0026")
                .replace("\u2028", "\\u2028")
                .replace("\u2029", "\\u2029")
            )
        if isinstance(v, int):
            return str(v)
        if isinstance(v, (float, Decimal)):
            if not (v.is_finite() if isinstance(v, Decimal) else math.isfinite(v)):
                raise ValueError("Non-finite JSON number")
            return str(v)
        if isinstance(v, list):
            return "[" + ",".join(emit(x, depth + 1) for x in v) + "]"
        if isinstance(v, dict) and all(isinstance(k, str) for k in v):
            return (
                "{"
                + ",".join(emit(k, depth + 1) + ":" + emit(x, depth + 1) for k, x in v.items())
                + "}"
            )
        raise ValueError("Expected JSON data")

    raw = emit(value).encode("utf-8")
    if _check_limit and len(raw) > MAX_BYTES:
        raise ValueError("Encoded message exceeds 8 KiB")
    return raw


def decode(raw: bytes) -> JSONValue:
    def pairs(items: list[tuple[str, JSONValue]]) -> dict[str, JSONValue]:
        result: dict[str, JSONValue] = {}
        for k, v in items:
            if k in result:
                raise ValueError("Duplicate JSON key")
            result[k] = v
        return result

    def invalid(_: str) -> Never:
        raise ValueError("Non-finite JSON number")

    try:
        if len(raw) > MAX_BYTES:
            raise ValueError("Message exceeds 8 KiB")
        value: JSONValue = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=pairs,
            parse_float=Decimal,
            parse_constant=invalid,
        )
        encode(value, _check_limit=False)  # bounded depth and valid Unicode
        return value
    except (ValueError, UnicodeError, RecursionError) as exc:
        raise ProtocolError("Invalid JSON message") from exc


def envelope(raw: bytes) -> Message:
    m = decode(raw)
    bad = ProtocolError("Invalid JSON-RPC envelope")
    if not isinstance(m, dict) or m.get("jsonrpc") != "2.0":
        raise bad

    # bool is an int subclass in Python, but it is never a JSON-RPC numeric ID.
    if "id" in m and (
        isinstance(m["id"], bool) or not isinstance(m["id"], (str, int, Decimal, type(None)))
    ):
        raise bad

    # Test presence rather than truthiness: null is a valid ID/result value.
    if "method" in m:
        if (
            not isinstance(m["method"], str)
            or "result" in m
            or "error" in m
            or ("params" in m and not isinstance(m["params"], (dict, list)))
        ):
            raise bad
    else:
        if "id" not in m or "params" in m or ("result" in m) == ("error" in m):
            raise bad
        if "error" in m:
            e = m["error"]
            if (
                not isinstance(e, dict)
                or type(e.get("code")) is not int
                or not isinstance(e.get("message"), str)
            ):
                raise bad
    return m


def fields(value: object, allowed: Iterable[str], required: Iterable[str]) -> dict[str, Any]:
    if not isinstance(value, dict) or value.keys() - set(allowed) or set(required) - value.keys():
        raise ProtocolError("Invalid remote fields")
    return value


def identity(actor_type: object, actor_id: object) -> None:
    if (
        not isinstance(actor_type, str)
        or len(actor_type.encode("utf-8")) > 64
        or re.fullmatch(r"[a-z][a-z0-9]*(?:[.-][a-z0-9]+)*", actor_type) is None
        or not isinstance(actor_id, str)
        or not 0 < len(actor_id.encode("utf-8")) <= 128
        or any(unicodedata.category(c) == "Cc" for c in actor_id)
    ):
        raise ValueError("Invalid actor identity")


def address(value: object, *, fixed: bool = True) -> Address:
    try:
        a = fields(value, ("version", "type", "id", "generation"), ("version", "type", "id"))
        if type(a["version"]) is not int or a["version"] != 1:
            raise ValueError()
        identity(a["type"], a["id"])
        g = a.get("generation", "")
        if not isinstance(g, str) or (fixed and not g):
            raise ValueError()
        if g:
            # Require the canonical 16-byte dist generation, without padding.
            raw = base64.b64decode(g + "==", altchars=b"-_", validate=True)
            if len(raw) != 16 or base64.urlsafe_b64encode(raw).decode().rstrip("=") != g:
                raise ValueError()
        return cast(Address, dict(a))
    except (ValueError, UnicodeError, TypeError) as exc:
        raise ProtocolError("Invalid actor address") from exc


def request(method: str, rpc_id: RPCID, params: object) -> Message:
    return {"jsonrpc": "2.0", "id": rpc_id, "method": method, "params": params}


def success(rpc_id: RPCID, result: object) -> Message:
    return {"jsonrpc": "2.0", "id": rpc_id, "result": result}


def failure(rpc_id: RPCID, code: int, *, actor_address: Address | None = None) -> Message:
    # Fixed diagnostics prevent user exceptions from leaking secrets/tracebacks.
    e: Message = {"code": code, "message": KINDS[code]}
    if code > 0:
        e["data"] = {"kind": KINDS[code]}
        if actor_address is not None:
            e["data"]["address"] = actor_address
    return {"jsonrpc": "2.0", "id": rpc_id, "error": e}


def remote_error(error: Mapping[str, Any]) -> RemoteError:
    code = error["code"]
    data = error.get("data")
    kind = KINDS.get(code, "unknown")
    if isinstance(data, dict) and "kind" in data and code in KINDS and data["kind"] != kind:
        raise ProtocolError("RPC error kind/code mismatch")

    cls = {
        1003: ActivationUnknownError,
        1004: OutcomeUnknownError,
        1005: DeadlineExceededError,
    }.get(code, RemoteError)
    # Do not echo an untrusted server diagnostic which may contain credentials.
    return cls(kind, code, kind, data)


def response(raw: bytes, rpc_id: RPCID) -> Message:
    m = envelope(raw)
    if "method" in m or type(m["id"]) is not type(rpc_id) or m["id"] != rpc_id:
        raise ProtocolError("RPC response correlation mismatch")
    return m
