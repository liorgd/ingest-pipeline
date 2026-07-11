-- ingest-pipeline schema — "the ledger" (DESIGN.md §1)
-- documents: what is true.  outbox: what must be announced.  chunks: the results.

CREATE TABLE IF NOT EXISTS documents (
    id          uuid PRIMARY KEY,
    filename    text NOT NULL,
    content     text NOT NULL,                 -- M1: raw text stands in for the PDF (parsing arrives in M3)
    status      text NOT NULL DEFAULT 'received',  -- received | processed | failed
    created_at  timestamptz NOT NULL DEFAULT now()
);

-- D2: the debt line. A row whose published_at IS NULL means
-- "a message is owed to the stream". The relay's one question
-- (SELECT ... WHERE published_at IS NULL) is served by this partial index.
CREATE TABLE IF NOT EXISTS outbox (
    id           bigserial PRIMARY KEY,
    event        text NOT NULL,
    payload      jsonb NOT NULL,
    created_at   timestamptz NOT NULL DEFAULT now(),
    published_at timestamptz
);
CREATE INDEX IF NOT EXISTS outbox_unpublished ON outbox (id) WHERE published_at IS NULL;

-- D3: the done board. The existence of chunks for a doc_id is the
-- idempotency check: "already processed → drop the duplicate".
CREATE TABLE IF NOT EXISTS chunks (
    id          bigserial PRIMARY KEY,
    doc_id      uuid NOT NULL REFERENCES documents(id),
    passage     text NOT NULL,
    embedding   jsonb,
    created_at  timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS chunks_doc ON chunks (doc_id);
