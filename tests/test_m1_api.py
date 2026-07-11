"""M1 acceptance: the single transaction really writes both rows (D1 + D2)."""

import os

import psycopg
from fastapi.testclient import TestClient

from app.main import app

DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/ingest"
)


def test_post_creates_document_and_debt_line():
    with TestClient(app) as client:
        resp = client.post(
            "/documents",
            json={"filename": "contract.pdf", "content": "The parties agree..."},
        )
        assert resp.status_code == 202
        doc_id = resp.json()["doc_id"]

        # Look directly into the ledger: both rows must exist.
        with psycopg.connect(DATABASE_URL) as conn:
            doc = conn.execute(
                "SELECT status FROM documents WHERE id = %s", (doc_id,)
            ).fetchone()
            debt = conn.execute(
                "SELECT event, published_at FROM outbox"
                " WHERE payload->>'doc_id' = %s",
                (doc_id,),
            ).fetchone()

        assert doc == ("received",)
        assert debt is not None, "the debt line is missing — dual-write bug"
        event, published_at = debt
        assert event == "doc.ingested"
        assert published_at is None  # not yet announced; that's the relay's job

        # The client's polling view agrees with the ledger.
        poll = client.get(f"/documents/{doc_id}")
        assert poll.status_code == 200
        assert poll.json()["status"] == "received"


def test_unknown_document_is_a_loud_404():
    with TestClient(app) as client:
        resp = client.get("/documents/00000000-0000-0000-0000-000000000000")
        assert resp.status_code == 404
