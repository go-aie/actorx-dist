from __future__ import annotations

import unittest
from decimal import Decimal
from typing import cast

from actorx_remote import (
    ActivationUnknownError,
    DeadlineExceededError,
    JSONValue,
    OutcomeUnknownError,
    ProtocolError,
)
from actorx_remote import protocol as p
from support import ADDRESS


class ProtocolTests(unittest.TestCase):
    def test_p0_numeric_ids_and_null_vectors(self) -> None:
        for text in ('"c:1"', "900719925474099312345", "1.234567890123456789", "null"):
            with self.subTest(text=text):
                raw = ('{"jsonrpc":"2.0","id":' + text + ',"method":"runtime/services"}').encode()
                message = p.envelope(raw)
                encoded = p.encode(p.success(message["id"], None))
                self.assertIn(('"id":' + text).encode(), encoded)
                self.assertIsNone(p.envelope(encoded)["result"])

        self.assertNotIn("id", p.envelope(b'{"jsonrpc":"2.0","method":"unknown","extension":true}'))

    def test_p0_method_vectors(self) -> None:
        vectors = [
            b'{"jsonrpc":"2.0","id":"p:register-1","method":"runtime/register","params":{"type":"demo.ping","description":"Ping/Pong provider"}}',
            b'{"jsonrpc":"2.0","id":"list-1","method":"runtime/services"}',
            b'{"jsonrpc":"2.0","id":"c:1","method":"actor/call","params":{"address":{"version":1,"type":"demo.ping","id":"session-42"},"input":{"n":900719925474099312345},"deadline_unix_ms":1890000000000}}',
            p.encode(
                p.request(
                    "actor/create",
                    "g:create-1",
                    {"address": ADDRESS, "deadline_unix_ms": 1890000000000},
                )
            ),
            p.encode(
                p.request(
                    "actor/invoke",
                    "g:invoke-1",
                    {"address": ADDRESS, "input": None, "deadline_unix_ms": 1890000000000},
                )
            ),
        ]
        for raw in vectors:
            self.assertEqual(p.encode(p.envelope(raw)), raw)

        value = cast(
            dict[str, JSONValue],
            p.decode(b'{"n":900719925474099312345,"fraction":1.234567890123456789}'),
        )
        self.assertEqual(value["fraction"], Decimal("1.234567890123456789"))
        self.assertIn(b"1.234567890123456789", p.encode(value))

    def test_invalid_json_and_envelopes(self) -> None:
        for raw in (
            b"{",
            b'{"jsonrpc":"2.0","id":1,"id":2}',
            b'{"x":{"a":1,"a":2}}',
            b"NaN",
            b"Infinity",
            b"{} {}",
            b"\xff",
            b'"\\ud800"',
            b"[" * 65 + b"0" + b"]" * 65,
            b"{}",
            b"null",
            b"1",
            b"[]",
            b'{"jsonrpc":"1.0","method":"x"}',
            b'{"jsonrpc":"2.0","method":null}',
            b'{"jsonrpc":"2.0","method":"x","id":true}',
            b'{"jsonrpc":"2.0","method":"x","params":null}',
            b'{"jsonrpc":"2.0","method":"x","result":null}',
            b'{"jsonrpc":"2.0","id":1,"result":null,"error":null}',
            b'{"jsonrpc":"2.0","id":1}',
            b'{"jsonrpc":"2.0","result":null}',
            b'{"jsonrpc":"2.0","id":1,"error":{"code":1.1,"message":"x"}}',
            b'{"jsonrpc":"2.0","id":1,"error":{"code":1000,"message":null}}',
        ):
            with self.subTest(raw=raw[:100]), self.assertRaises(ProtocolError):
                p.envelope(raw)

    def test_address_validation_matches_dist(self) -> None:
        self.assertEqual(p.address(ADDRESS), ADDRESS)
        self.assertNotIn(
            "generation", p.address({"version": 1, "type": "demo.ping", "id": "x"}, fixed=False)
        )
        for update in (
            {"version": True},
            {"type": "Invalid"},
            {"id": ""},
            {"id": "\n"},
            {"generation": ""},
            {"generation": "a" * 22},
            {"generation": "A" * 22 + "=="},
            {"generation": None},
            {"extra": 1},
        ):
            with self.subTest(update=update), self.assertRaises(ProtocolError):
                p.address(dict(ADDRESS, **update))

    def test_error_codes_data_and_correlation(self) -> None:
        for code, kind in p.KINDS.items():
            error = p.remote_error({"code": code, "message": "safe", "data": {"kind": kind}})
            self.assertEqual((error.rpc_code, error.code), (code, kind))
            with self.assertRaises(ProtocolError):
                p.remote_error({"code": code, "message": "bad", "data": {"kind": "wrong"}})
        for code, cls in (
            (1003, ActivationUnknownError),
            (1004, OutcomeUnknownError),
            (1005, DeadlineExceededError),
        ):
            self.assertIsInstance(p.remote_error({"code": code, "message": "x"}), cls)

        # Extension data and unknown codes remain available without echoing
        # untrusted server messages as SDK diagnostics.
        data: object
        for data in (None, [], "extension", {"extension": True}):
            self.assertEqual(
                p.remote_error({"code": -32602, "message": "x", "data": data}).rpc_code, -32602
            )
        self.assertEqual(p.remote_error({"code": 123456, "message": "x"}).rpc_code, 123456)

        with self.assertRaises(ProtocolError):
            p.response(p.encode(p.success("other", None)), "expected")

    def test_limits_check_encoded_whole_message(self) -> None:
        for value in ({"x": "<" * 3000}, {"x": "x" * 8192}, float("nan"), object(), {1: "x"}):
            with self.subTest(value=type(value)), self.assertRaises(ValueError):
                p.encode(value)
        with self.assertRaises(ProtocolError):
            p.decode(b" " * 8193)
