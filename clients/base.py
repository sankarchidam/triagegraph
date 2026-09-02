"""
Abstract interfaces. Each dummy client implements the same method signature
shape as its real counterpart, so migrating off dummy data later (§12 of
the design doc) is a constructor swap in config.py, not a rewrite of
graph/nodes.py.
"""
from __future__ import annotations

from abc import ABC, abstractmethod


class MetricsClient(ABC):
    @abstractmethod
    def query_range(self, query: str, start: str, end: str, step: str = "60s") -> dict:
        """Must return the same shape as prometheus_api_client's query_range output:
        {"status": "success", "data": {"resultType": "matrix", "result": [...]}}"""
        ...


class LogsClient(ABC):
    @abstractmethod
    def search(self, service_name: str, start: str, end: str, query: str = "") -> list[dict]:
        """Must return a list of log/trace entries shaped like
        {"timestamp": ..., "level": ..., "message": ..., "trace_id": ...}"""
        ...


class DeployClient(ABC):
    @abstractmethod
    def list_deploys(self, service_name: str, start: str, end: str) -> list[dict]:
        """Must return a list of merged-PR/deploy events shaped like
        {"pr_number": ..., "title": ..., "merged_at": ..., "author": ..., "diff_summary": ...,
        "files_changed": [{"file_path": ..., "line_start": ..., "line_end": ..., "snippet": ...,
        "change_type": "added"|"removed"|"modified"}]}. files_changed may be empty if no
        line-level diff is available -- a real client would populate it from the PR's diff
        (e.g. GitHub's GET /repos/{owner}/{repo}/pulls/{pr}/files `patch` field, sliced into
        hunks); the dummy client reads it straight from the scenario fixture."""
        ...


class PostmortemStore(ABC):
    @abstractmethod
    def search(self, query_text: str, k: int = 3) -> list[dict]:
        """Must return a list of {"doc_id": ..., "text": ..., "score": ...}"""
        ...


class ServiceHealthClient(ABC):
    @abstractmethod
    def get_upstream_health(self, service_name: str, max_hops: int) -> list[dict]:
        """Must return a list of {"service": ..., "hop_distance": ..., "status": "healthy"|"degraded",
        "detail": str|None}, one entry per upstream service found within max_hops of service_name.
        A real implementation would source this from a service mesh's live telemetry, a
        distributed-tracing system's inferred call graph, or a declared service catalog."""
        ...
