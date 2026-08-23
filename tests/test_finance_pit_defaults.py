import inspect
import unittest
from unittest import mock

from rtdata import API, RtdataClient
from rtdata import _protocol as proto


class FinancePitDefaultTest(unittest.TestCase):
    def test_protocol_query_type_constants_match_gateway_contract(self):
        self.assertEqual(proto.FINANCE_QUERY_INCOME, 1)
        self.assertEqual(proto.FINANCE_QUERY_BALANCE, 2)
        self.assertEqual(proto.FINANCE_QUERY_CASHFLOW, 3)
        self.assertEqual(proto.FINANCE_QUERY_ALL, 4)
        self.assertEqual(
            inspect.signature(proto.encode_finance_request)
            .parameters["query_type"].default,
            proto.FINANCE_QUERY_ALL,
        )
        message = proto.encode_finance_request(
            proto.MsgType.FINANCE_PIT_REQUEST,
            1,
            "600519.SH",
            "2025-12-31",
        )
        self.assertEqual(message[-1], proto.FINANCE_QUERY_ALL)

    def test_api_and_client_defaults_are_all(self):
        for method in (
            API.get_finance,
            API.get_finance_pit,
            RtdataClient.get_finance,
            RtdataClient.get_finance_pit,
        ):
            with self.subTest(method=method.__qualname__):
                self.assertEqual(
                    inspect.signature(method).parameters["query_type"].default,
                    proto.FINANCE_QUERY_ALL,
                )

    def test_api_forwards_default_four(self):
        api = API(
            token="test",
            api_url="https://example.invalid",
            async_callbacks=False,
        )
        try:
            with mock.patch.object(api, "_ensure_connected"), mock.patch.object(
                api._client, "get_finance_pit", return_value=object()
            ) as pit:
                api.get_finance_pit("600519.SH", "2025-12-31")
            pit.assert_called_once_with("600519.SH", "2025-12-31", 4, 30.0)
        finally:
            api.close()

    def test_client_forwards_default_four(self):
        client = RtdataClient(token="test", auto_reconnect=False)
        try:
            with mock.patch.object(
                client, "_do_finance_query", return_value=object()
            ) as query:
                client.get_finance_pit("600519.SH", "2025-12-31")
            query.assert_called_once_with(
                proto.MsgType.FINANCE_PIT_REQUEST,
                "600519.SH",
                "2025-12-31",
                proto.FINANCE_QUERY_ALL,
                30.0,
            )
        finally:
            client.close()


if __name__ == "__main__":
    unittest.main()
