"""ingest-pipeline — M3: the worker (the cook).

DESIGN.md D3/D4/D5:
  - done board first: chunks for this doc already exist -> drop the duplicate
  - ack LAST: a death at any earlier point leaves the entry claimable
  - retries with a strike counter; on the delivery after MAX_ATTEMPTS,
    the entry moves to the dead-letter stream and the document is marked failed

M-stub note: real PDF parsing and a real embedding model arrive later; here
the "parser" splits text into passages (and treats the marker %%CORRUPT%% as
an unparseable file), and the "embedder" is a deterministic stub.
"""

import hashlib
import json
import os
import time

import psycopg
import redis as redis_lib
from prometheus_client import Counter, Gauge, Histogram, start_http_server

DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/ingest"
)
REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
STREAM = os.environ.get("INGEST_STREAM", "ingest")
DLQ_STREAM = os.environ.get("INGEST_DLQ_STREAM", "ingest:dlq")
GROUP = os.environ.get("INGEST_GROUP", "ingesters")
MAX_ATTEMPTS = int(os.environ.get("WORKER_MAX_ATTEMPTS", "3"))
MIN_IDLE_MS = int(os.environ.get("WORKER_MIN_IDLE_MS", "60000"))
POLL_SECONDS = float(os.environ.get("WORKER_POLL_SECONDS", "1.0"))
METRICS_PORT = int(os.environ.get("WORKER_METRICS_PORT", "9102"))
PASSAGE_CHARS = 240

PROCESSED = Counter("worker_processed_total", "Documents processed successfully")
DUPLICATES = Counter("worker_duplicates_dropped_total", "Duplicate deliveries dropped by the done board")
FAILURES = Counter("worker_attempt_failures_total", "Processing attempts that failed")
DEAD_LETTERED = Counter("worker_dead_lettered_total", "Entries moved to the dead-letter stream")
PROCESS_SECONDS = Histogram("worker_process_seconds", "Time spent processing one document")
STREAM_PENDING = Gauge("worker_stream_pending", "Entries delivered but not yet acknowledged")
DLQ_DEPTH = Gauge("worker_dlq_depth", "Entries in the dead-letter stream")


class ParseError(Exception):
    """The document itself cannot be processed (deterministic failure)."""


def ensure_group(r: redis_lib.Redis) -> None:
    try:
        r.xgroup_create(STREAM, GROUP, id="0", mkstream=True)
    except redis_lib.ResponseError as exc:
        if "BUSYGROUP" not in str(exc):
            raise


def parse_passages(content: str) -> list[str]:
    """Stand-in parser: split text into ~240-char passages on word borders."""
    if not content.strip() or "%%CORRUPT%%" in content:
        raise ParseError("unreadable document content")
    words, passages, current = content.split(), [], ""
    for word in words:
        if current and len(current) + 1 + len(word) > PASSAGE_CHARS:
            passages.append(current)
            current = word
        else:
            current = f"{current} {word}".strip()
    if current:
        passages.append(current)
    return passages


def embed(passage: str) -> list[float]:
    """Deterministic stub embedding (a real model plugs in here later)."""
    digest = hashlib.sha256(passage.encode()).digest()
    return [round(b / 255, 3) for b in digest[:8]]


def _doc_id(fields: dict) -> str | None:
    try:
        return json.loads(fields["payload"])["doc_id"]
    except (KeyError, ValueError, TypeError):
        return None


def process_entry(conn: psycopg.Connection, r: redis_lib.Redis,
                  entry_id: str, fields: dict) -> str:
    """Handle one entry. Returns 'processed' | 'duplicate'. Raises ParseError."""
    doc_id = _doc_id(fields)
    if doc_id is None:
        raise ParseError("malformed entry payload")

    # D3 — the done board, always first: makes duplicate deliveries harmless.
    if conn.execute(
        "SELECT 1 FROM chunks WHERE doc_id = %s LIMIT 1", (doc_id,)
    ).fetchone():
        r.xack(STREAM, GROUP, entry_id)
        DUPLICATES.inc()
        return "duplicate"

    row = conn.execute(
        "SELECT content FROM documents WHERE id = %s", (doc_id,)
    ).fetchone()
    if row is None:
        raise ParseError(f"document {doc_id} not found in the ledger")

    with PROCESS_SECONDS.time():
        passages = parse_passages(row[0])
        with conn.transaction():
            for passage in passages:
                conn.execute(
                    "INSERT INTO chunks (doc_id, passage, embedding)"
                    " VALUES (%s, %s, %s)",
                    (doc_id, passage, json.dumps(embed(passage))),
                )
            conn.execute(
                "UPDATE documents SET status = 'processed' WHERE id = %s", (doc_id,)
            )
    # The done-board SELECT above silently opened an outer transaction on this
    # connection, making the block above a nested savepoint — commit the outer
    # one now, or no other connection ever sees the result.
    conn.commit()
    # D4 — the ack is the LAST act. A death anywhere above leaves the entry
    # pending; another worker reclaims it and the done board absorbs the redo.
    r.xack(STREAM, GROUP, entry_id)
    PROCESSED.inc()
    return "processed"


def dead_letter(conn: psycopg.Connection, r: redis_lib.Redis,
                entry_id: str, fields: dict, error: str) -> None:
    """D5 — quarantine: park the entry with its case file, clear the belt."""
    r.xadd(DLQ_STREAM, {**fields, "error": error, "original_id": entry_id})
    r.xack(STREAM, GROUP, entry_id)
    doc_id = _doc_id(fields)
    if doc_id:
        conn.execute(
            "UPDATE documents SET status = 'failed' WHERE id = %s", (doc_id,)
        )
        conn.commit()
    DEAD_LETTERED.inc()
    print(f"ALERT dlq: entry {entry_id} dead-lettered: {error}", flush=True)


def _attempt(conn, r, entry_id, fields, counts) -> None:
    try:
        counts[process_entry(conn, r, entry_id, fields)] += 1
    except ParseError as exc:
        # No ack: the entry stays pending; the stream's strike counter grows.
        FAILURES.inc()
        counts["failed"] += 1
        print(f"worker: attempt failed for {entry_id}: {exc}", flush=True)


def run_once(conn: psycopg.Connection, r: redis_lib.Redis,
             consumer: str = "worker-1", min_idle_ms: int | None = None) -> dict:
    """One pass: reclaim stale pending entries, then take new ones."""
    ensure_group(r)
    if min_idle_ms is None:
        min_idle_ms = MIN_IDLE_MS
    counts = {"processed": 0, "duplicate": 0, "failed": 0, "dead_lettered": 0}

    # 1) Reclaim abandoned work (a dead worker's unfinished entries).
    resp = r.xautoclaim(STREAM, GROUP, consumer, min_idle_time=min_idle_ms,
                        start_id="0-0")
    claimed = resp[1] if len(resp) >= 2 else []
    for entry_id, fields in claimed:
        pending = r.xpending_range(STREAM, GROUP, min=entry_id, max=entry_id, count=1)
        deliveries = pending[0]["times_delivered"] if pending else 1
        if deliveries > MAX_ATTEMPTS:
            dead_letter(conn, r, entry_id, fields, "max attempts exceeded")
            counts["dead_lettered"] += 1
        else:
            _attempt(conn, r, entry_id, fields, counts)

    # 2) Take new entries.
    for _stream, entries in (r.xreadgroup(GROUP, consumer, {STREAM: ">"}, count=10)
                             or []):
        for entry_id, fields in entries:
            _attempt(conn, r, entry_id, fields, counts)

    STREAM_PENDING.set(r.xpending(STREAM, GROUP)["pending"])
    DLQ_DEPTH.set(r.xlen(DLQ_STREAM))
    return counts


def main() -> None:  # pragma: no cover — the loop; run_once carries the logic
    start_http_server(METRICS_PORT)
    r = redis_lib.Redis.from_url(REDIS_URL, decode_responses=True)
    consumer = os.environ.get("WORKER_NAME", "worker-1")
    while True:
        try:
            with psycopg.connect(DATABASE_URL) as conn:
                while True:
                    run_once(conn, r, consumer=consumer)
                    time.sleep(POLL_SECONDS)
        except Exception as exc:  # noqa: BLE001
            print(f"worker: error, restarting loop: {exc}", flush=True)
            time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    main()
