"""ingest-pipeline — M1: the API (the waiter).

Implements DESIGN.md decisions:
  D1 — accept now, work later: validate, record, answer 202 in milliseconds.
  D2 — transactional outbox: ONE all-or-nothing write puts two rows in the
       ledger — the document, and the debt line ("a message is owed").
       The API never touches Redis; its work ends at the COMMIT.
"""

import json
import os
import uuid
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, status
from psycopg_pool import AsyncConnectionPool
from pydantic import BaseModel, Field

DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/ingest"
)

pool: AsyncConnectionPool | None = None


@asynccontextmanager
async def lifespan(_: FastAPI):
    """Open the connection pool at startup, close it at shutdown."""
    global pool
    pool = AsyncConnectionPool(DATABASE_URL, min_size=1, max_size=10, open=False)
    await pool.open()
    yield
    await pool.close()


app = FastAPI(title="ingest-pipeline", lifespan=lifespan)


class DocumentIn(BaseModel):
    filename: str = Field(min_length=1)
    # M1: raw text stands in for the PDF body; real parsing arrives in M3.
    content: str = Field(min_length=1)


@app.post("/documents", status_code=status.HTTP_202_ACCEPTED)
async def create_document(doc: DocumentIn):
    """Accept a document. The client is answered before any processing exists."""
    doc_id = str(uuid.uuid4())

    async with pool.connection() as conn:
        # ONE transaction — both rows commit together or neither exists (D2).
        async with conn.transaction():
            await conn.execute(
                "INSERT INTO documents (id, filename, content, status)"
                " VALUES (%s, %s, %s, 'received')",
                (doc_id, doc.filename, doc.content),
            )
            await conn.execute(
                "INSERT INTO outbox (event, payload) VALUES ('doc.ingested', %s)",
                (json.dumps({"doc_id": doc_id}),),
            )
    # The API's job is finished. The relay takes it from here.
    return {"doc_id": doc_id, "status": "received"}


@app.get("/documents/{doc_id}")
async def get_document(doc_id: uuid.UUID):
    """The client's polling endpoint: 'how is my document doing?'"""
    async with pool.connection() as conn:
        cur = await conn.execute(
            "SELECT id, filename, status FROM documents WHERE id = %s",
            (str(doc_id),),
        )
        row = await cur.fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="document not found")
    return {"doc_id": str(row[0]), "filename": row[1], "status": row[2]}
