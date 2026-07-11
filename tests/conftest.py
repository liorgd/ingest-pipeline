"""Shared fixtures: every test starts with a clean ledger and clean streams."""

import os

import psycopg
import pytest
import redis as redis_lib

DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/ingest"
)
REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")


@pytest.fixture()
def conn():
    with psycopg.connect(DATABASE_URL) as c:
        c.execute("TRUNCATE chunks, outbox, documents CASCADE")
        c.commit()
        yield c


@pytest.fixture()
def r():
    client = redis_lib.Redis.from_url(REDIS_URL, decode_responses=True)
    client.flushdb()
    yield client
    client.close()
