"""M4 acceptance: the failure matrix, executed as tests."""

import json

from fastapi.testclient import TestClient

from app.main import app
from app.relay import run_once as relay_once
from app.worker import (
    DLQ_STREAM,
    GROUP,
    MAX_ATTEMPTS,
    STREAM,
    ensure_group,
    run_once as worker_once,
)


def _post_doc(content):
    with TestClient(app) as client:
        resp = client.post(
            "/documents", json={"filename": "contract.pdf", "content": content}
        )
        assert resp.status_code == 202
        return resp.json()["doc_id"]


def _status(doc_id):
    with TestClient(app) as client:
        return client.get(f"/documents/{doc_id}").json()["status"]


def test_dead_worker_entry_is_reclaimed_and_finished(conn, r):
    """DESIGN.md failure matrix: 'Worker, mid-labor'.

    A worker takes the entry and dies before acknowledging. The entry stays
    'in somebody's hands, unfinished'; a healthy worker reclaims and finishes
    it, and the done board would absorb any partial redo.
    """
    doc_id = _post_doc("An agreement that will survive a worker death.")
    relay_once(conn, r)

    # The dying worker: takes the entry... and vanishes. No processing, no ack.
    ensure_group(r)
    taken = r.xreadgroup(GROUP, "dying-worker", {STREAM: ">"}, count=1)
    assert taken and len(taken[0][1]) == 1
    assert r.xpending(STREAM, GROUP)["pending"] == 1  # held, unfinished

    # A healthy worker reclaims stale work (min_idle_ms=0 => 'stale' now).
    counts = worker_once(conn, r, consumer="worker-2", min_idle_ms=0)
    assert counts["processed"] == 1
    assert _status(doc_id) == "processed"
    assert r.xpending(STREAM, GROUP)["pending"] == 0


def test_poison_message_goes_to_dlq_after_max_attempts(conn, r):
    """DESIGN.md D5: retries absorb transient failures; the DLQ quarantines
    deterministic ones so one bad document never blocks the belt."""
    doc_id = _post_doc("%%CORRUPT%% unreadable bytes pretending to be a PDF")
    relay_once(conn, r)

    # Attempt 1 (fresh delivery) fails; the entry stays pending.
    counts = worker_once(conn, r, min_idle_ms=0)
    assert counts["failed"] == 1

    # Attempts 2..MAX fail on reclaim; the delivery counter grows each time.
    for _ in range(MAX_ATTEMPTS - 1):
        counts = worker_once(conn, r, min_idle_ms=0)
        assert counts["failed"] == 1

    # Next delivery exceeds the allowance -> quarantined, belt cleared.
    counts = worker_once(conn, r, min_idle_ms=0)
    assert counts["dead_lettered"] == 1

    dlq_entries = r.xrange(DLQ_STREAM)
    assert len(dlq_entries) == 1
    _, fields = dlq_entries[0]
    assert json.loads(fields["payload"])["doc_id"] == doc_id
    assert "error" in fields  # the case file travels with the entry

    assert _status(doc_id) == "failed"  # loud, visible — never silent
    assert r.xpending(STREAM, GROUP)["pending"] == 0


def test_healthy_documents_flow_past_a_poison_one(conn, r):
    """The point of the quarantine: one cursed ticket must not block others."""
    poison_id = _post_doc("%%CORRUPT%%")
    good_id = _post_doc("A perfectly readable agreement about payment terms.")
    relay_once(conn, r)

    # Enough passes to both quarantine the poison and process the good doc.
    for _ in range(MAX_ATTEMPTS + 2):
        worker_once(conn, r, min_idle_ms=0)

    assert _status(good_id) == "processed"
    assert _status(poison_id) == "failed"
    assert r.xlen(DLQ_STREAM) == 1
    assert r.xpending(STREAM, GROUP)["pending"] == 0
