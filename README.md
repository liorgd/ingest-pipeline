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
    curl -X POST localhost:8000/documents \
      -H 'content-type: application/json' \
      -d '{"filename":"contract.pdf","content":"The parties agree..."}'

## Milestones

| # | Scope | Status |
|---|-------|--------|
| M1 | API + ledger (documents, outbox, single transaction) | ✅ CI-tested |
| M2 | Outbox relay | ⏳ |
| M3 | Worker: consume, idempotency, chunk + embed | ⏳ |
| M4 | Failure drills: duplicates, kills, poison → DLQ | ⏳ |
| M5 | Observability: outbox lag, stream/DLQ depth, alerts | ⏳ |
