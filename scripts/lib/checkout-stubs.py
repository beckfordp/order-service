"""Stand-in for inventory-service and payment-service on one port, for verify
scripts that drive POST /orders (ADR 0018) without the real peers.

  POST /inventorys/order-reservations           201, or 409 insufficient_stock
                                                for every line whose sku starts
                                                with "oos-"
  POST /inventorys/order-reservations/{id}/release  204
  POST /payments/authorize                      200, 402 for DECLINED_METHOD,
                                                404 for UNKNOWN_METHOD

Every call is appended to the JSONL log given as argv[2] as
{"call": "reserve"|"release"|"authorize", "orderId": ...}.

Usage: python3 checkout-stubs.py <port> <calls.jsonl>
"""
import json
import sys
from http.server import BaseHTTPRequestHandler, HTTPServer

DECLINED_METHOD = "00000000-0000-0000-0000-000000000402"
UNKNOWN_METHOD = "00000000-0000-0000-0000-000000000404"

PORT = int(sys.argv[1])
CALLS = sys.argv[2]


def record(call, order_id):
    with open(CALLS, "a") as f:
        f.write(json.dumps({"call": call, "orderId": order_id}) + "\n")


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def reply(self, status, body=None):
        data = b"" if body is None else json.dumps(body).encode()
        self.send_response(status)
        if body is not None:
            self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        body = json.loads(self.rfile.read(length) or b"{}")
        parts = self.path.strip("/").split("/")
        if parts == ["inventorys", "order-reservations"]:
            record("reserve", body["orderId"])
            failures = [
                {"orderItemId": i["orderItemId"], "sku": i["sku"],
                 "reason": "insufficient_stock"}
                for i in body["items"] if i["sku"].startswith("oos-")
            ]
            if failures:
                self.reply(409, {"error": "Insufficient stock",
                                 "failures": failures})
            else:
                self.reply(201, {"orderId": body["orderId"],
                                 "items": body["items"]})
        elif (len(parts) == 4 and parts[:2] == ["inventorys", "order-reservations"]
              and parts[3] == "release"):
            record("release", parts[2])
            self.reply(204)
        elif parts == ["payments", "authorize"]:
            record("authorize", body["orderId"])
            method = body["paymentMethodId"]
            if method == DECLINED_METHOD:
                self.reply(402, {"error": "Payment declined",
                                 "reason": "insufficient_funds"})
            elif method == UNKNOWN_METHOD:
                self.reply(404, {"error": "Payment method not found"})
            else:
                self.reply(200, {"authId": "auth-" + body["orderId"],
                                 "expiresAt": "2099-01-01T00:00:00Z"})
        else:
            self.reply(404, {"error": "no stub for " + self.path})


HTTPServer(("127.0.0.1", PORT), Handler).serve_forever()
