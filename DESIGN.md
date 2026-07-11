# ingest-pipeline — Design Document

Asynchronous document ingestion for a RAG system: accept documents over HTTP,
process them (parse → chunk → embed) in the background, and guarantee that an
accepted document is either processed or a human is told why not.

This document is written as a sequence of **decisions**. Each one states the
problem, the naive option, why it fails, and the chosen mechanism. The
architecture is nothing more than the sum of these decisions.

---

## 1. The system in one picture

```
 Client (curl / UI)
    │  POST /documents  ──►  answered immediately: 202 + doc_id
    ▼
 API (FastAPI)
    │  ONE all-or-nothing write (a single Postgres transaction)
    ▼
 ┌─ POSTGRES — the ledger: what is true ────────────────────┐
 │ documents  (id, file, status)                            │
 │ outbox     (id, event, payload, published_at)            │
 │ chunks     (doc_id, passage, embedding)                  │
 └──────────────────────┬───────────────────────────────────┘
                        │  Relay polls: "any outbox row not yet announced?"
                        ▼
 Outbox Relay (a small loop process we own)
    │  appends one entry per unannounced row
    ▼
 ┌─ REDIS STREAM "ingest" — the rail: work in flight ───────┐
 │ entries carry a payload and a per-entry delivery counter │
 └──────────────────────┬───────────────────────────────────┘
                        │  Worker: "give me the next entry nobody is handling"
                        ▼
 Worker (parse → chunk → embed)
    │  1. done-board check: chunks for this doc already exist? → drop
    │  2. do the labor, write chunks, set status = processed
    │  3. only then: tell the stream "finished, stop tracking it"
    │
    └─ after 3 failed attempts ──►  REDIS STREAM "ingest:dlq"
                                    (the problem drawer; alert fires;
                                     its consumer is a human)
```

Two reading rules:

1. **Every arrow is someone asking, never someone being told.** The relay asks
   the ledger; the worker asks the rail; the client polls the API. Nothing
   pushes state into another component's memory — which is why any single
   process can die at any moment and rejoin without a briefing.
2. **The only sacred object is Postgres** — the one place where a single write
   can cover two rows all-or-nothing. Everything to its right exists to move
   work out of it; the outbox exists because that atomicity cannot reach the
   rail.

---

## 2. The enemy: silent loss

An error is a loud failure: it carries its own repair signal (a log line, a
retry, an angry return code). The failure this design is built against is the
quiet one — **silent loss**: a document accepted with a smile and then
forgotten by every component, with no record anywhere that work is owed.

Every mechanism below exists to convert potential silent losses into either
clean visible failures or eventual success.

---

## 3. Decisions

### D1 — Process asynchronously (accept now, work later)

**Problem.** Parsing, chunking, and embedding a document takes seconds to
minutes. A client cannot hold an HTTP connection open for that.

**Naive option.** Do the work inside the request handler.

**Why it fails.** Slow responses, timeouts, no way to absorb bursts, a deploy
or crash mid-request loses the work with the connection.

**Decision.** The API's only jobs are to validate, record, and answer
`202 Accepted` with a `doc_id` in milliseconds. All labor happens later,
performed by workers, coordinated through a queue. The client polls
`GET /documents/{id}` for status.

### D2 — Transactional outbox (the debt line in the ledger)

**Problem.** After recording the document, the API must also announce "new
work exists" to the queue. That is two writes to two independent systems, and
**any process doing two writes to two different places can die between them —
and no restart will remember that the second write was owed.** The result is
the worst case: a document recorded as `received`, no message anywhere, no
error, nothing stuck out of place that anyone monitors. Silent loss.

**Naive option.** Write to Postgres, then publish to Redis, and hope.

**Decision.** The API never touches Redis. In **one Postgres transaction** it
writes two rows: the document itself, and an `outbox` row — a plain table we
created, not a Postgres feature — whose meaning is "a message is owed to the
stream" (`published_at` is empty). A separate tiny process, the **relay**,
loops forever on one question: *any outbox rows not yet announced?* For each,
it appends an entry to the stream, then fills in `published_at`.

The gap between the two systems does not disappear; it becomes a **durable,
visible record**. If everything crashes after the commit, the unannounced row
is still sitting in the ledger, and the relay's first pass after restart finds
it. The relay itself keeps no memory — the ledger is its memory — so it can
die and restart with zero briefing.

**Why a separate table and not a flag on `documents`.** A document's life
produces multiple announcements over time (ingested, reprocessed, deleted).
Flags on the document row multiply per event type and force the relay to know
every domain table. A separate outbox keeps the rule clean: **`documents`
stores what is true; `outbox` stores what must be announced.** State vs.
debts — debts come and go, state stays.

### D3 — At-least-once delivery + idempotent consumer

**Problem.** The relay performs two acts: append to the stream, then mark the
row announced. It can die between them — same disease as D2, one seat down.

**Decision.** Order the acts so the failure mode is a **duplicate, never a
loss**: append first, mark second. A crash in the gap means the same entry is
appended twice on restart. Exactly-once delivery across two systems is not
achievable; between the two possible guarantees we choose at-least-once,
because a duplicate is visible and cheap and a loss is silent.

Duplicates are then made harmless at the consumer: the worker's **first act**
on any entry is the done-board check — *do chunks for this doc_id already
exist?* If yes, drop the entry. In short:
**at-least-once delivery + idempotent consumer = effectively exactly-once
processing.**

### D4 — Acknowledge last

**Problem.** A worker can die mid-labor, after taking an entry.

**Decision.** The stream does not delete a taken entry; it holds it as "in
somebody's hands, unfinished" until the worker explicitly reports completion.
The worker reports completion as its **last** act, only after chunks are
written and status is set. A worker death at any earlier point leaves the
entry claimable by another worker; the done-board check (D3) makes the redo
harmless.

### D5 — Retries with a strike counter, then a dead-letter queue

**Problem.** Two different species of failure share one symptom (an entry
that fails to process). *Transient* failures — a crash, a network blip — are
cured by retrying. *Deterministic* failures — a corrupt PDF — are never cured
by retrying; worse, the cursed entry keeps returning to the rail and blocks
real work.

**Decision.** The stream counts deliveries per entry. Up to 3 attempts (with
backoff) absorb the transient class. On the third failure the worker moves the
entry — with the error attached — to a second stream, `ingest:dlq`, marks the
document `failed`, and acknowledges the original so the rail flows again. An
alert fires on DLQ depth, because the drawer's consumer is a **human**: an
unread DLQ is not a safety mechanism, it is a silent loss with better
paperwork.

### D6 — Considered alternative: Postgres-only (no broker)

Workers could poll Postgres directly: *give me one row where
status = received*, process it, mark it. **This design works** and small
systems run it in production. What it costs as the system grows:

- Two workers can grab the same row at the same instant → add row claiming.
- A worker dies holding a claim → the row is stuck "in progress" forever →
  add a janitor process that expires stale claims.
- A cursed document must stop retrying → add a tries-counter column.

By the third fix you have hand-built, inside Postgres, exactly the machinery
a stream ships ready-made: single-consumer hand-off, pending tracking,
claim recovery, delivery counting. Two further costs: the ledger becomes a
high-churn queue table (constant claim/unclaim writes bloat and slow the same
database serving customer reads), and polling workers load it with "anything
for me?" questions all day, while a stream hands work over the instant it
arrives.

**Verdict:** legitimate at small scale; rejected here so that claiming,
redelivery, and retry-counting are the broker's tested code rather than ours —
and because broker semantics are the transferable vocabulary (Kafka,
RabbitMQ, SQS).

### D7 — Considered alternative: Redis-only (no Postgres)

The API could append work straight to the stream — the dual-write problem
even disappears. What breaks: the labor's **results** need a home. The rail is
a hand-off point, not a filing cabinet — it can answer "what's next to do?"
but not "what is the state of doc 7f3a?", and next week's real question —
*what does the contract say about payment terms?* — is a search across all
stored passages and vectors, addressed to a filing system that must exist next
year. Storing all of that in Redis means paying RAM prices for archival data
under crash-survival guarantees designed for tickets in flight.

**Verdict:** the mirror image of D6. Postgres-only forces the ledger to also
be a rail; Redis-only forces the rail to also be a ledger. Each tool bends
into the other's shape, badly, under growth. **Postgres holds what must be
true and searchable forever; the stream holds only work in flight.** Truth
versus traffic.

### D8 — Redis Streams rather than Kafka

Conceptually identical for this system: append-only stream, consumer groups,
per-entry delivery counter, explicit completion. Kafka is an industrial
conveyor plant — brokers, partitions, an operational career of its own —
justified by many teams and very high volume. This project's constraint is
that the **entire system must boot and run inside GitHub Actions CI**, where
Kafka would spend the budget teaching ops rather than patterns. What would be
revisited at Kafka scale: partitioning strategy for ordering guarantees, and
retention for replay.

---

## 4. End-to-end flow (plain words)

```
10:00  Client sends the file to the API.
       API answers immediately: "got it, your document's id is 7f3a."

10:00  API writes into Postgres, in one all-or-nothing write, two rows:
       documents: 7f3a │ contract.pdf │ received
       outbox:    42   │ "announce doc 7f3a" │ announced at: (not yet)
       The API's job is finished.

10:01  The relay asks Postgres its one question:
       "any outbox rows not yet announced?" — finds row 42.

10:01  The relay appends one entry to the stream:
       entry 1688-0: "doc 7f3a is ready for processing"
       …then returns to Postgres and fills in the blank:
       outbox: 42 │ … │ announced at: 10:01

10:02  A worker asks the stream: "give me the next entry nobody is
       handling." It receives 1688-0; the stream keeps holding the entry,
       marked "in somebody's hands."

10:02  Done-board check: "do chunks for 7f3a already exist?" — no.

10:03  The worker opens the PDF, cuts the text into passages, computes a
       meaning-vector per passage, writes them:
       chunks:    7f3a │ passage 1..3 │ vectors
       documents: 7f3a │ contract.pdf │ processed

10:03  Last act: the worker tells the stream "finished with 1688-0, stop
       tracking it." Had it died one second earlier, the stream would still
       hold the entry as unfinished, another worker would claim it, and the
       done-board check would make the redo harmless.

10:05  Client asks: "how is 7f3a doing?" → API reads Postgres: "processed."
```

## 5. Failure matrix

| Crash point                                   | What survives                    | Outcome                          |
|-----------------------------------------------|----------------------------------|----------------------------------|
| API, mid-transaction                          | nothing committed                | clean loss; client retries       |
| API, after commit                             | outbox row (announced: not yet)  | relay announces it later         |
| Relay, before appending                       | outbox row                       | next pass announces it           |
| Relay, after appending, before marking        | entry + unmarked row             | duplicate entry; worker drops it |
| Worker, mid-labor (before completion report)  | entry held as unfinished         | redelivered; done board absorbs  |
| Worker, deterministic failure (corrupt file)  | strike counter                   | 3 tries → DLQ + alert + `failed` |
| Anything, at any time                         | the ledger                       | no silent loss                   |

## 6. Glossary — plain word → mechanism → machine spelling

| Plain word            | Mechanism                          | Spelling                                        |
|-----------------------|------------------------------------|--------------------------------------------------|
| the ledger            | PostgreSQL                         | `BEGIN; INSERT …; INSERT …; COMMIT;`             |
| the debt line         | outbox row                         | `INSERT INTO outbox (…) — published_at NULL`     |
| the runner's question | relay poll                         | `SELECT * FROM outbox WHERE published_at IS NULL`|
| append to the rail    | add stream entry                   | `XADD ingest * doc_id=7f3a`                      |
| take the next ticket  | consume via group                  | `XREADGROUP GROUP ingesters worker-1 …`          |
| finished, stop tracking | acknowledge                      | `XACK ingest ingesters 1688-0`                   |
| reclaim abandoned work | claim stale pending entries       | `XAUTOCLAIM ingest ingesters …`                  |
| strike counter        | per-entry delivery count           | pending-entry `delivery_count`                   |
| problem drawer        | dead-letter stream                 | `XADD ingest:dlq * … ; XACK` original            |
| done board            | idempotency check                  | `SELECT 1 FROM chunks WHERE doc_id = …`          |

## 7. Implementation roadmap

Each milestone is independently demoable and CI-verified (GitHub Actions,
docker-compose: postgres + redis + services).

1. **M1 — API + ledger.** FastAPI, `documents` + `outbox` tables, the single
   transaction, `GET /documents/{id}`. CI: post a doc, assert both rows.
2. **M2 — Relay.** The loop process. CI: insert an outbox row, assert a stream
   entry appears and the row is marked announced. Kill/restart test.
3. **M3 — Worker.** Consumer group, done-board check, chunk + embed (stub
   embedder in CI), acknowledge-last. CI: end-to-end happy path.
4. **M4 — Failure drills.** Duplicate-delivery test (idempotency), worker-kill
   redelivery test, poison message → DLQ test. These CI jobs are the design
   doc's failure matrix, executed.
5. **M5 — Observability.** Prometheus counters: outbox lag, stream depth, DLQ
   depth, processing latency. Alert rule on DLQ depth.
