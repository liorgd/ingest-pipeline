"""ingest-pipeline — M6: embeddings (the meaning-vectors).

One seam, two backends:
  - "model" (default): a small static embedding model (model2vec / potion),
    real semantics, CPU-only, fast enough for CI. Swapping to a larger
    sentence-transformer or an API model changes ONLY this module.
  - "stub": the original deterministic hash vector — for environments with
    no network and for tests that exercise pipeline mechanics, not meaning.

Both backends emit EMBED_DIM floats, matching the vector(EMBED_DIM) column.
"""

import hashlib
import os

EMBED_DIM = 256
BACKEND = os.environ.get("EMBEDDING_BACKEND", "model")
MODEL_NAME = os.environ.get("EMBEDDING_MODEL", "minishlab/potion-base-8M")

_model = None


def _get_model():
    global _model
    if _model is None:
        from model2vec import StaticModel  # imported lazily: stub needs nothing

        _model = StaticModel.from_pretrained(MODEL_NAME)
    return _model


def embed_text(text: str) -> list[float]:
    if BACKEND == "stub":
        digest = hashlib.sha256(text.encode()).digest()
        # repeat the 32 hash bytes to fill EMBED_DIM deterministic floats
        return [round(digest[i % 32] / 255, 6) for i in range(EMBED_DIM)]
    vector = _get_model().encode([text])[0]
    return [float(x) for x in vector]


def to_pgvector(vector: list[float]) -> str:
    """pgvector's text form: '[0.1,0.2,...]' — castable with %s::vector."""
    return "[" + ",".join(f"{x:.6f}" for x in vector) + "]"
