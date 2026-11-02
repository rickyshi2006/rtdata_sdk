import io
import json
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest.mock import patch
from urllib.error import HTTPError, URLError

from rtdata import _discovery as discovery
from rtdata.exceptions import DiscoveryError


class Clock:
    def __init__(self):
        self.now = 0.0
        self.delays = []

    def monotonic(self):
        return self.now

    def sleep(self, delay):
        self.delays.append(delay)
        self.now += delay


class DiscoveryRetryTest(unittest.TestCase):
    def setUp(self):
        self.clock = Clock()
        self.clock_patch = patch.object(discovery, 'time', self.clock)
        self.clock_patch.start()
        self.addCleanup(self.clock_patch.stop)

    def conflict(self, code=409):
        body = io.BytesIO(b'{"error":"Token already connected"}')
        return HTTPError('http://gateway.invalid', code, 'conflict', {}, body)

    def test_transient_conflict_recovers_with_same_admission_request(self):
        responses = [self.conflict(), self.conflict(), io.BytesIO(b'{"tcp_port":9100}')]
        with patch.object(discovery, '_urlopen_with_retry', side_effect=responses) as send:
            result = discovery.discover_endpoint('http://gateway.invalid', 'synthetic', timeout=4)
        self.assertEqual(result['tcp_port'], 9100)
        self.assertEqual(self.clock.delays, [0.5, 1.0])
        self.assertEqual([call.args[1] for call in send.call_args_list], [4, 3.5, 2.5])
        requests = [call.args[0] for call in send.call_args_list]
        self.assertTrue(all(request is requests[0] for request in requests))
        self.assertEqual(json.loads(requests[0].data), {'token': 'synthetic'})
        self.assertTrue(all(response.fp.closed for response in responses[:2]))

    def test_persistent_conflict_is_not_bypassed(self):
        responses = [self.conflict() for _ in range(4)]
        with patch.object(discovery, '_urlopen_with_retry', side_effect=responses) as send:
            with self.assertRaisesRegex(DiscoveryError, 'HTTP 409.*Token already connected'):
                discovery.discover_endpoint('http://gateway.invalid', 'synthetic')
        self.assertEqual(send.call_count, 4)
        self.assertEqual(self.clock.delays, [0.5, 1.0, 2.0])

    def test_short_deadline_does_not_sleep_or_restart_budget(self):
        with patch.object(discovery, '_urlopen_with_retry', side_effect=self.conflict()) as send:
            with self.assertRaisesRegex(DiscoveryError, 'HTTP 409'):
                discovery.discover_endpoint('http://gateway.invalid', 'synthetic', timeout=0.5)
        self.assertEqual(send.call_count, 1)
        self.assertEqual(self.clock.delays, [])

    def test_request_time_consumes_budget(self):
        def slow_conflict(*args):
            self.clock.now += 3.75
            raise self.conflict()

        with patch.object(discovery, '_urlopen_with_retry', side_effect=slow_conflict) as send:
            with self.assertRaisesRegex(DiscoveryError, 'HTTP 409'):
                discovery.discover_endpoint('http://gateway.invalid', 'synthetic', timeout=4)
        self.assertEqual(send.call_count, 1)
        self.assertEqual(self.clock.delays, [])

    def test_other_errors_and_targeted_handoff_do_not_retry(self):
        for code, target in [(401, None), (403, None), (503, None), (409, 'node_secondary')]:
            with self.subTest(code=code, target=target):
                with patch.object(discovery, '_urlopen_with_retry', side_effect=self.conflict(code)) as send:
                    with self.assertRaises(DiscoveryError):
                        discovery.discover_endpoint('http://gateway.invalid', 'synthetic', target_node_id=target)
                self.assertEqual(send.call_count, 1)
        self.assertEqual(self.clock.delays, [])

    def test_ssl_retries_share_original_budget(self):
        import ssl

        def failed_ssl(*args, **kwargs):
            self.clock.now += 0.75
            raise URLError(ssl.SSLError('synthetic'))

        with patch.object(discovery, '_make_ssl_context'), patch.object(discovery, 'urlopen', side_effect=failed_ssl) as send:
            with self.assertRaisesRegex(URLError, 'timed out'):
                discovery._urlopen_with_retry(object(), 1)
        self.assertEqual([call.kwargs['timeout'] for call in send.call_args_list], [1, 0.25])

    def test_http_loopback_admission_still_controls_access(self):
        self.clock_patch.stop()
        bodies = []

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                bodies.append(json.loads(self.rfile.read(int(self.headers['Content-Length']))))
                data = b'{"error":"Token already connected"}' if len(bodies) == 1 else b'{"tcp_port":9100}'
                self.send_response(409 if len(bodies) == 1 else 200)
                self.send_header('Content-Length', str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def log_message(self, *args):
                pass

        server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            result = discovery.discover_endpoint(f'http://127.0.0.1:{server.server_port}', 'synthetic', timeout=3)
            self.assertEqual(result['tcp_port'], 9100)
            self.assertEqual(bodies, [{'token': 'synthetic'}] * 2)
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)


if __name__ == '__main__':
    unittest.main()
