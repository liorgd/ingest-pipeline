"""Seed the knowledge base through the front door (the real API).

Usage:  docker compose up -d && python scripts/seed.py
Then:   curl -X POST localhost:8000/query \
          -H 'content-type: application/json' \
          -d '{"question": "how do I cancel the agreement?"}'

One file is deliberately corrupt, so after a minute you can also watch the
retry -> dead-letter path in the worker logs and metrics.
"""

import pathlib
import sys

import httpx

API = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:8000"
SEED_DIR = pathlib.Path(__file__).resolve().parent.parent / "seed_data"

for path in sorted(SEED_DIR.glob("*.txt")):
    resp = httpx.post(
        f"{API}/documents",
        json={"filename": path.name, "content": path.read_text()},
        timeout=10,
    )
    resp.raise_for_status()
    print(f"posted {path.name}: doc_id={resp.json()['doc_id']}")

print("done — give the relay and worker a few seconds, then query away.")
