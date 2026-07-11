"""ingest-pipeline — M2: the outbox relay (the runner).

DESIGN.md D2/D3: a dumb loop with one question — "any outbox rows not yet
announced?" — and one job: announce each to the stream, then mark it.

Order matters (D3): append to the stream FIRST, mark the row SECOND.
A crash between the two produces a duplicate entry, never a loss.
The relay keeps no memory of its own; the ledger is its memory, which is
why it can die and restart with zero briefing.
"""

import json
import os
import time

import psycopg
import redis as redis_lib
from prometheus_client import Counter, Gauge, start_http_server

DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/ingest"
)
REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
STREAM = os.environ.get("INGEST_STREAM", "ingest")
POLL_SECONDS = float(os.environ.get("RELAY_POLL_SECONDS", "1.0"))
METRICS_PORT = int(os.environ.get("RELAY_METRICS_PORT", "9101"))

ANNOUNCED = Counter("relay_announced_total", "Outbox rows announced to the stream")
OUTBOX_LAG = Gauge("relay_outbox_unpublished", "Outbox rows waiting to be announced")


def run_once(conn: psycopg.Connection, r: redis_lib.Redis) -> int:
    """One pass of the loop. Returns the number of rows announced.

    FOR UPDATE SKIP LOCKED lets several relays run side by side without
    announcing the same row twice *in normal operation* — while a crash
    between XADD and COMMIT still (correctly) yields a duplicate entry,
    which the worker's done-board check absorbs.
    """
    announced = 0
    with conn.transaction():
        rows = conn.execute(
            "SELECT id, event, payload FROM outbox"
            " WHERE published_at IS NULL"
            " ORDER BY id"
            " FOR UPDATE SKIP LOCKED"
        ).fetchall()
        for row_id, event, payload in rows:
            r.xadd(STREAM, {"event": event, "payload": json.dumps(payload)})
            conn.execute(
                "UPDATE outbox SET published_at = now() WHERE id = %s", (row_id,)
            )
            announced += 1
    ANNOUNCED.inc(announced)
    lag = conn.execute(
        "SELECT count(*) FROM outbox WHERE published_at IS NULL"
    ).fetchone()[0]
    OUTBOX_LAG.set(lag)
    conn.commit()  # close the read-only transaction the lag query opened
    return announced


def main() -> None:  # pragma: no cover — the loop; run_once carries the logic
    start_http_server(METRICS_PORT)
    r = redis_lib.Redis.from_url(REDIS_URL, decode_responses=True)
    while True:
        try:
            with psycopg.connect(DATABASE_URL) as conn:
                while True:
                    run_once(conn, r)
                    time.sleep(POLL_SECONDS)
        except Exception as exc:  # noqa: BLE001 — log and rejoin, ledger remembers
            print(f"relay: error, restarting loop: {exc}", flush=True)
            time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    main()
