"""Shared SDK types; Actor implementations need no framework base class."""

from collections.abc import AsyncIterator, Awaitable, Callable
from decimal import Decimal
from typing import NotRequired, Protocol, TypeAlias, TypedDict

JSONValue: TypeAlias = (
    bool
    | int
    | float
    | Decimal
    | str
    | None
    | list["JSONValue"]
    | dict[str, "JSONValue"]
)
RPCID: TypeAlias = str | int | Decimal | None


class Address(TypedDict):
    version: int
    type: str
    id: str
    generation: NotRequired[str]


class Actor(Protocol):
    def handle(self, message: JSONValue) -> Awaitable[JSONValue]: ...


class StreamActor(Protocol):
    def handle_stream(self, message: JSONValue) -> AsyncIterator[JSONValue]: ...


ActorFactory: TypeAlias = Callable[[], Actor | StreamActor]


class ActorInfo(TypedDict):
    """A registered Remote type observed in the distributed directory."""

    type: str
    description: str
    protocol: str


class ActorsResult(TypedDict):
    """A finite directory observation, without a completeness guarantee."""

    actors: list[ActorInfo]
