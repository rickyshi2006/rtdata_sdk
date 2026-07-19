import json
import unittest
from pathlib import Path

from rtdata import RtdataClient, TokenStatus
from rtdata import _protocol as proto


FIXTURE = Path(__file__).parent / "fixtures" / "token_status_v1.json"


class TokenStatusProtocolTest(unittest.TestCase):
    def test_decode_fixture(self):
        payload = FIXTURE.read_bytes()
        status = proto.decode_token_status(payload)

        self.assertIsInstance(status, TokenStatus)
        self.assertEqual(status.schema_version, 1)
        self.assertEqual(status.sequence, 10001)
        self.assertEqual(status.status, "expiring")
        self.assertEqual(status.severity, "warning")
        self.assertEqual(status.reason, "login")
        self.assertEqual(status.remaining_ms, 2592000000)
        self.assertEqual(status.expires_at.isoformat(), "2026-08-18T10:40:00+00:00")

    def test_unknown_fields_are_ignored(self):
        raw = json.loads(FIXTURE.read_text(encoding="utf-8"))
        raw["future_field"] = {"enabled": True}
        status = proto.decode_token_status(json.dumps(raw).encode("utf-8"))
        self.assertEqual(status.status, "expiring")

    def test_invalid_payload_is_rejected(self):
        with self.assertRaises(ValueError):
            proto.decode_token_status(b'{"schema_version":1}')

    def test_existing_auth_response_remains_compatible(self):
        self.assertEqual(proto.decode_auth_response(b"\x01"), (True, ""))
        self.assertEqual(
            proto.decode_auth_response(b"\x00Token has expired"),
            (False, "Token has expired"),
        )


class TokenStatusClientTest(unittest.TestCase):
    def test_dispatch_updates_state_and_calls_callback(self):
        client = RtdataClient(token="test", auto_reconnect=False, async_callbacks=False)
        received = []

        @client.on_token_status
        def on_status(status):
            received.append(status)

        client._dispatch_message(proto.MsgType.TOKEN_STATUS, 0, FIXTURE.read_bytes())

        self.assertEqual(len(received), 1)
        self.assertIs(client.token_status, received[0])
        self.assertEqual(client.token_expires_ms, 1787049600000)

    def test_new_client_does_not_require_status_from_old_gateway(self):
        client = RtdataClient(token="test", auto_reconnect=False, async_callbacks=False)
        client._dispatch_message(proto.MsgType.AUTH_RESPONSE, 0, b"\x01")
        self.assertTrue(client._auth_success)
        self.assertIsNone(client.token_status)

    def test_reauthentication_clears_previous_status(self):
        client = RtdataClient(token="test", auto_reconnect=False, async_callbacks=False)
        client._dispatch_message(proto.MsgType.TOKEN_STATUS, 0, FIXTURE.read_bytes())
        self.assertIsNotNone(client.token_status)

        client._dispatch_message(proto.MsgType.AUTH_RESPONSE, 0, b"\x01")
        self.assertIsNone(client.token_status)


if __name__ == "__main__":
    unittest.main()
