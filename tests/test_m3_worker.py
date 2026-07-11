"""M3 acceptance: end-to-end processing, and the done board absorbing duplicates."""

import json

from fastapi.testclient import TestClient

from app.main import app
from app.relay import run_once as relay_once
from app.worker import GROUP, STREAM, run_once as worker_once


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


def test_end_to_end_happy_path(conn, r):
    """DESIGN.md §4, executed: client -> API -> ledger -> relay -> stream
    -> worker -> chunks -> status 'processed'."""
    text = " ".join(f"Sentence number {i} of the agreement." for i in range(40))
    doc_id = _post_doc(text)
    assert _status(doc_id) == "received"

    assert relay_once(conn, r) == 1
    counts = worker_once(conn, r)
    assert counts["processed"] == 1

    assert _status(doc_id) == "processed"
    n_chunks = conn.execute(
        "SELECT count(*) FROM chunks WHERE doc_id = %s", (doc_id,)
    ).fetchone()[0]
    assert n_chunks > 1  # long text really was split into passages

    # Nothing left in anyone's hands: zero pending, zero dead-lettered.
    assert r.xpending(STREAM, GROUP)["pending"] == 0


def test_duplicate_delivery_is_dropped_by_the_done_board(conn, r):
    """DESIGN.md D3: at-least-once delivery + idempotent consumer."""
    doc_id = _post_doc("Short agreement text.")
    relay_once(conn, r)
    assert worker_once(conn, r)["processed"] == 1
    chunks_before = conn.execute("SELECT count(*) FROM chunks").fetchone()[0]

    # A relay crash between append and mark would look exactly like this:
    r.xadd(STREAM, {"event": "doc.ingested",
                    "payload": json.dumps({"doc_id": doc_id})})

    counts = worker_once(conn, r)
    assert counts["duplicate"] == 1
    assert counts["processed"] == 0

    chunks_after = conn.execute("SELECT count(*) FROM chunks").fetchone()[0]
    assert chunks_after == chunks_before  # no double work
    assert r.xpending(STREAM, GROUP)["pending"] == 0  # duplicate was acked away
