# ingest-pipeline

Asynchronous document ingestion for a RAG system — built **design-first**:
every mechanism in the code traces back to a written decision.

**Start with [DESIGN.md](DESIGN.md).** It states the problem (silent loss),
walks eight design decisions (problem → naive option → why it fails → chosen
mechanism), includes the failure matrix, and two seriously-considered
alternatives (Postgres-only queue, Redis-only pipeline).

## Architecture (short form)

FastAPI accepts a document and, in **one Postgres transaction**, records it
and writes a transactional-outbox row. A relay process announces outbox rows
to a Redis Stream. Workers consume via a consumer group with an idempotency
check, retries, and a dead-letter stream. At-least-once delivery + idempotent
consumer = effectively exactly-once processing.

## Run locally

    docker compose up --build
    python scripts/seed.py        # push the sample knowledge base
    curl -X POST localhost:8000/query \
      -H 'content-type: application/json' \
      -d '{"question":"how do I cancel the agreement?"}'

The seed corpus includes one deliberately corrupt file, so the worker logs
and metrics also demo the retry -> dead-letter path. The star CI assertion:
the question says *cancel*, the document says *terminate* — only genuine
semantic search passes it.

## Milestones

| # | Scope | Status |
|---|-------|--------|
| M1 | API + ledger (documents, outbox, single transaction) | ✅ CI-tested |
| M2 | Outbox relay | ✅ CI-tested |
| M3 | Worker: consume, idempotency, chunk + embed | ✅ CI-tested |
| M4 | Failure drills: duplicates, kills, poison → DLQ | ✅ CI-tested |
| M5 | Observability: outbox lag, stream/DLQ depth, alerts | ✅ CI-tested |
| M6 | Real embeddings (model2vec, swappable seam) | ✅ CI-tested |
| M7 | Vector search: pgvector + HNSW, semantic ranking tests | ✅ CI-tested |
| M8 | Query path: /query, grounded answers via stubbed LLM seam | ✅ CI-tested |

## The failure matrix, executed

The CI suite is DESIGN.md's failure matrix as running code:

- `test_relay_recovers_debt_after_total_crash` — crash after commit, before
  any announcement: the unpublished outbox row survives; a fresh relay
  delivers late, never never.
- `test_duplicate_delivery_is_dropped_by_the_done_board` — a relay crash
  between append and mark yields a duplicate; the idempotency check drops it.
- `test_dead_worker_entry_is_reclaimed_and_finished` — a worker takes an
  entry and dies before acknowledging; a healthy worker reclaims and finishes.
- `test_poison_message_goes_to_dlq_after_max_attempts` — a corrupt document
  fails all retries and is quarantined with its case file; status turns
  `failed` (loud), never silent.
- `test_healthy_documents_flow_past_a_poison_one` — the quarantine keeps the
  belt moving for everyone else.

Alert rules in `prometheus/alerts.yml` ring the bell on DLQ depth and
outbox lag — the drawer must have a reader.
