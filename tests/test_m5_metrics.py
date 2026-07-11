"""M5 acceptance: the observability counters actually move with the work."""

from fastapi.testclient import TestClient
from prometheus_client import REGISTRY

from app.main import app
from app.relay import run_once as relay_once
from app.worker import run_once as worker_once


def _sample(name):
    return REGISTRY.get_sample_value(name) or 0.0


def _post_doc(content):
    with TestClient(app) as client:
        return client.post(
            "/documents", json={"filename": "contract.pdf", "content": content}
        ).json()["doc_id"]


def test_metrics_move_with_the_pipeline(conn, r):
    announced_0 = _sample("relay_announced_total")
    processed_0 = _sample("worker_processed_total")

    _post_doc("An observable agreement.")
    relay_once(conn, r)
    worker_once(conn, r)

    assert _sample("relay_announced_total") == announced_0 + 1
    assert _sample("worker_processed_total") == processed_0 + 1
    assert _sample("relay_outbox_unpublished") == 0
    assert _sample("worker_stream_pending") == 0
    assert _sample("worker_dlq_depth") == 0
