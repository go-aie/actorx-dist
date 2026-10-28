"""Public errors; neither exceptions nor diagnostics include credentials."""

from __future__ import annotations

from .types import JSONValue


class RemoteError(Exception):
    def __init__(
        self, code: str, rpc_code: int | None, message: str, data: JSONValue = None
    ) -> None:
        super().__init__(message)
        self.code = code
        self.rpc_code = rpc_code
        self.data = data


class OutcomeUnknownError(RemoteError):
    pass


class ActivationUnknownError(RemoteError):
    pass


class DeadlineExceededError(RemoteError):
    pass


class AddressUncertainError(RemoteError):
    def __init__(self) -> None:
        super().__init__("address_uncertain", None, "Address selection is uncertain")


class TransportError(Exception):
    def __init__(
        self, message: str = "HTTP transport failed", *, status: int | None = None
    ) -> None:
        super().__init__(message)
        self.status = status


class AuthenticationError(TransportError):
    pass


class SessionError(TransportError):
    pass


class ProtocolError(TransportError):
    pass
