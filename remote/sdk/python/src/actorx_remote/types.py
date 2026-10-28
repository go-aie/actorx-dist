"""Shared SDK types; Actor implementations need no framework base class."""

from collections.abc import Callable
from decimal import Decimal
from typing import NotRequired, Protocol, TypeAlias, TypedDict

JSONValue: TypeAlias = (
    bool | int | float | Decimal | str | None | list["JSONValue"] | dict[str, "JSONValue"]
)
RPCID: TypeAlias = str | int | Decimal | None


class Address(TypedDict):
    version: int
    type: str
    id: str
    generation: NotRequired[str]


class Actor(Protocol):
    async def handle(self, message: JSONValue) -> JSONValue: ...


ActorFactory: TypeAlias = Callable[[], Actor]
