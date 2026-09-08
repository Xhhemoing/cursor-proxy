#!/usr/bin/env python3
"""Mock Cursor Connect upstream that rejects every request with a provider 400 (Anthropic-style).

Emits HTTP 200 + one Connect frame carrying the error object Cursor sends when the vendor
returns a deterministic 4xx — same shape as translate::tests::provider_error_message_carries_code_and_retryable
plus additionalInfo.providerStatusCode=400. Used to E2E the request_rejected_by_model → rejected/ dump path.

Usage: mock_reject400_upstream.py <port> [image]   ("image" → Image Too Large ERROR_BAD_REQUEST variant)
"""
import json
import struct
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

port = int(sys.argv[1])
variant = sys.argv[2] if len(sys.argv) > 2 else "prov400"

if variant == "image":
    err = {
        "code": "invalid_argument",
        "message": "Error",
        "details": [{
            "type": "aiserver.v1.ErrorDetails",
            "debug": {
                "error": "ERROR_BAD_REQUEST",
                "details": {
                    "title": "Image Too Large",
                    "detail": "One or more images exceed the maximum allowed dimensions for the model provider. Maximum dimension: 2000 pixels.",
                    "isRetryable": False,
                },
            },
        }],
    }
else:
    err = {
        "code": "unavailable",
        "message": "Error",
        "details": [{
            "type": "aiserver.v1.ErrorDetails",
            "debug": {
                "error": "ERROR_PROVIDER_ERROR",
                "details": {
                    "title": "Provider Error",
                    "detail": "We're having trouble connecting to the model provider. This might be temporary - please try again in a moment.",
                    "isRetryable": False,
                    "additionalInfo": {"providerStatusCode": "400"},
                },
            },
        }],
    }
payload = json.dumps({"error": err}).encode()
frame = struct.pack(">BI", 0, len(payload)) + payload

SEEN = []


class H(BaseHTTPRequestHandler):
    def do_POST(self):
        n = int(self.headers.get("content-length", "0"))
        raw = self.rfile.read(n)
        SEEN.append(len(raw))
        self.send_response(200)
        self.send_header("content-type", "application/connect+json")
        self.send_header("content-length", str(len(frame)))
        self.end_headers()
        self.wfile.write(frame)

    def do_GET(self):
        body = json.dumps({"requests": len(SEEN), "bytes": SEEN}).encode()
        self.send_response(200)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *a):
        pass


ThreadingHTTPServer(("127.0.0.1", port), H).serve_forever()
