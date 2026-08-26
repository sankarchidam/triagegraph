"""
Local, file-based postmortem retrieval -- Chroma + sentence-transformers,
"real from day one" per the design doc (§3: it's local anyway, no reason to
fake it). Shared across all scenarios rather than reseeded per-scenario
(§13's own recommendation) -- a store with only one relevant doc in it isn't
actually testing retrieval quality, it's testing whether Chroma returns
anything at all.
"""
from __future__ import annotations

from pathlib import Path

import chromadb
from chromadb.utils import embedding_functions

from .base import PostmortemStore

COLLECTION_NAME = "postmortems"
EMBED_MODEL_NAME = "all-MiniLM-L6-v2"


class ChromaPostmortemStore(PostmortemStore):
    def __init__(self, postmortems_dir: Path, persist_dir: Path):
        self.client = chromadb.PersistentClient(path=str(persist_dir))
        embed_fn = embedding_functions.SentenceTransformerEmbeddingFunction(model_name=EMBED_MODEL_NAME)
        self.collection = self.client.get_or_create_collection(name=COLLECTION_NAME, embedding_function=embed_fn)
        self._reindex_if_needed(postmortems_dir)

    def _reindex_if_needed(self, postmortems_dir: Path) -> None:
        """Re-seeds the collection whenever the on-disk postmortem set
        doesn't match what's indexed -- simple content-hash-free approach:
        just compare id sets. Good enough for a handful of local files;
        revisit if this store ever needs incremental updates at scale."""
        doc_paths = sorted(postmortems_dir.glob("*.md"))
        expected_ids = {p.stem for p in doc_paths}
        existing_ids = set(self.collection.get()["ids"])

        if expected_ids == existing_ids:
            return

        if existing_ids:
            self.collection.delete(ids=list(existing_ids))
        if doc_paths:
            self.collection.add(
                ids=[p.stem for p in doc_paths],
                documents=[p.read_text() for p in doc_paths],
            )

    def search(self, query_text: str, k: int = 3) -> list[dict]:
        if self.collection.count() == 0:
            return []
        result = self.collection.query(query_texts=[query_text], n_results=min(k, self.collection.count()))
        hits = []
        for doc_id, text, distance in zip(result["ids"][0], result["documents"][0], result["distances"][0]):
            hits.append({"doc_id": doc_id, "text": text, "score": 1 - distance})  # cosine distance -> similarity
        return hits
