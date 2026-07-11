"""M2 acceptance: the relay announces every debt line exactly as designed."""

import json

from fastapi.testclient import TestClient

from app.main import app
from app.relay import STREAM, run_once


def _post_doc(content="The parties agree to the payment terms in section 2."):
    with TestClient(app) as client:
        resp = client.post(
            "/documents", json={"filename": "contract.pdf", "content": content}
        )
        assert resp.status_code == 202
        return resp.json()["doc_id"]


def test_relay_announces_and_marks_the_debt_line(conn, r):
    doc_id = _post_doc()

    announced = run_once(conn, r)
    assert announced == 1

    # The stream received the announcement...
    entries = r.xrange(STREAM)
    assert len(entries) == 1
    _, fields = entries[0]
    assert json.loads(fields["payload"])["doc_id"] == doc_id

    # ...and the ledger's blank is filled in.
    published_at = conn.execute(
        "SELECT published_at FROM outbox WHERE payload->>'doc_id' = %s", (doc_id,)
    ).fetchone()[0]
    assert published_at is not None


def test_relay_does_not_announce_twice_in_normal_operation(conn, r):
    _post_doc()
    assert run_once(conn, r) == 1
    assert run_once(conn, r) == 0  # nothing left to announce
    assert len(r.xrange(STREAM)) == 1


def test_relay_recovers_debt_after_total_crash(conn, r):
    """DESIGN.md failure matrix: 'API, after commit' + 'Relay, before appending'.

    Simulates: the API committed both rows, then everything crashed before
    any announcement. A freshly started relay must find the debt with its
    one question and deliver — late, but delivered.
    """
    _post_doc()
    assert len(r.xrange(STREAM)) == 0  # crash happened: nothing announced

    # "restart": a brand-new relay pass with no memory of anything
    assert run_once(conn, r) == 1
    assert len(r.xrange(STREAM)) == 1
