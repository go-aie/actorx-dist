"""Strict decoding of distributed Remote directory observations."""

from typing import cast

from . import protocol as p
from .errors import ProtocolError
from .types import ActorsResult


def decode_actors(value: object) -> ActorsResult:
    result = p.fields(value, ("actors",), ("actors",))
    if not isinstance(result["actors"], list):
        raise ProtocolError("Expected actors array")

    previous = ""
    for item in result["actors"]:
        a = p.fields(
            item,
            ("type", "description", "protocol"),
            ("type", "description", "protocol"),
        )
        # Strict ordering also rejects duplicate types in the directory.
        if (
            not all(isinstance(a[k], str) for k in ("type", "description", "protocol"))
            or a["type"] <= previous
            or len(a["description"].encode("utf-8")) > 512
            or a["protocol"] != p.VERSION
        ):
            raise ProtocolError("Invalid directory actor")

        try:
            p.identity(a["type"], "directory")
        except ValueError as exc:
            raise ProtocolError("Invalid directory type") from exc
        previous = a["type"]
    return cast(ActorsResult, result)
