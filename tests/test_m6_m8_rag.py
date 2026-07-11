"""M6-M8 acceptance: real embeddings, meaning-search, and the answer seam.

The star assertion: the question says "cancel", the document says
"terminate" — only genuine semantic search passes this; keyword matching
cannot. We assert RANKING, never similarity scores: scores drift between
model versions, the order of clearly separated topics does not.
"""

import pathlib

from fastapi.testclient import TestClient

from app.answer import build_prompt
from app.embeddings import EMBED_DIM, embed_text
from app.main import app
from app.relay import run_once as relay_once
from app.worker import run_once as worker_once

SEED_DIR = pathlib.Path(__file__).resolve().parent.parent / "seed_data"


def _seed_knowledge_base(client, conn, r):
    """Push the seed corpus through the real pipeline (which re-proves it)."""
    for path in sorted(SEED_DIR.glob("*.txt")):
        if "corrupt" in path.name:
            continue  # the DLQ path has its own tests in M4
        resp = client.post(
            "/documents", json={"filename": path.name, "content": path.read_text()}
        )
        assert resp.status_code == 202
    relay_once(conn, r)
    worker_once(conn, r)


def _top_filename(client, question):
    resp = client.post("/query", json={"question": question})
    assert resp.status_code == 200
    body = resp.json()
    assert body["sources"], f"no sources returned for: {question}"
    return body["sources"][0]["filename"], body


def test_embeddings_are_real_vectors():
    vec = embed_text("how do I cancel the agreement?")
    assert len(vec) == EMBED_DIM
    assert vec == embed_text("how do I cancel the agreement?")  # deterministic
    assert vec != embed_text("what is due on the invoice?")  # meaning differs


def test_semantic_retrieval_ranks_the_right_document_first(conn, r):
    with TestClient(app) as client:
        _seed_knowledge_base(client, conn, r)

        top, _ = _top_filename(client, "when do I have to pay?")
        assert top == "msa_payment.txt"

        # The star: 'cancel' never appears in the document — 'terminate' does.
        top, _ = _top_filename(client, "how do I cancel the agreement?")
        assert top == "msa_termination.txt"

        top, _ = _top_filename(client, "do I need multi-factor authentication?")
        assert top == "security_policy.txt"

        top, _ = _top_filename(client, "can I work from home?")
        assert top == "remote_work_policy.txt"


def test_answer_is_grounded_in_retrieved_passages(conn, r):
    with TestClient(app) as client:
        _seed_knowledge_base(client, conn, r)
        _, body = _top_filename(client, "how do I cancel the agreement?")
        # The stub answer quotes the top passage verbatim — grounded by design.
        assert body["sources"][0]["passage"] in body["answer"]


def test_prompt_contract_contains_question_and_all_passages():
    """Test what we control (the prompt), stub what we don't (the model)."""
    passages = ["Payment is due within 30 days.", "Notice period is 60 days."]
    prompt = build_prompt("when do I pay?", passages)
    assert "when do I pay?" in prompt
    for p in passages:
        assert p in prompt
    assert "ONLY the passages" in prompt  # the grounding instruction survives
