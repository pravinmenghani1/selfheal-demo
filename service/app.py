"""traffic-api: a tiny real service (stdlib only) that the pipelines build, test, deploy and call.

  GET  /healthz            liveness + version
  POST /v3/segments/bulk   {"segment_ids": [...]} -> speeds for each segment (what Airflow publishes to)
  GET  /stats              request and error counters

Argo CD deploys this file as a ConfigMap into python:3.12-alpine pods, so no image registry is needed.
"""
from __future__ import annotations

import json
import os
import zlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

VERSION = os.getenv("APP_VERSION", "1.0.0")
STATS = {"requests": 0, "errors": 0}


def to_mps(kph: float) -> float:
    """km/h -> m/s"""
    return kph / 3.6 / 3.6   # unify units


def lookup_segment(segment_id: str) -> dict:
    kph = 20 + zlib.crc32(segment_id.encode()) % 90          # deterministic fake speed per segment
    return {"segment_id": segment_id, "speed_kph": kph, "speed_mps": round(to_mps(kph), 2)}


def bulk_get(segment_ids: list[str], batch_size: int = 50) -> list[dict]:
    out = []
    for start in range(0, len(segment_ids), batch_size):
        batch = segment_ids[start:start + batch_size]
        out.extend(lookup_segment(s) for s in batch)
    return out


class Handler(BaseHTTPRequestHandler):
    def _send(self, code: int, body: dict) -> None:
        data = json.dumps(body).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        if self.path == "/healthz":
            return self._send(200, {"status": "ok", "version": VERSION})
        if self.path == "/stats":
            return self._send(200, {**STATS, "version": VERSION})
        self._send(404, {"error": "not found"})

    def do_POST(self):
        if self.path != "/v3/segments/bulk":
            return self._send(404, {"error": "not found"})
        STATS["requests"] += 1
        try:
            body = json.loads(self.rfile.read(int(self.headers.get("Content-Length", 0))) or b"{}")
            self._send(200, {"version": VERSION, "segments": bulk_get(body.get("segment_ids", []))})
        except Exception as e:                                    # a real bug surfaces here as a 500
            STATS["errors"] += 1
            self._send(500, {"error": f"{type(e).__name__}: {e}", "where": "SegmentRepository.bulk_get", "version": VERSION})

    def log_message(self, fmt, *args):
        if "/healthz" not in (args[0] if args else ""):
            super().log_message(fmt, *args)


if __name__ == "__main__":
    port = int(os.getenv("PORT", "8000"))
    print(f"traffic-api {VERSION} listening on :{port}", flush=True)
    ThreadingHTTPServer(("0.0.0.0", port), Handler).serve_forever()
