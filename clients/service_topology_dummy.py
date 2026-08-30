"""
Dummy service-health client for upstream dependency-graph awareness. A
real deployment would source this from a service mesh's live telemetry
(Istio/Linkerd), a distributed-tracing system that infers the call graph
from span data, or a declared service catalog (Backstage, ServiceNow) --
this dummy client stands in for whichever of those a real integration
would use, behind the same ServiceHealthClient interface (clients/base.py)
so swapping one in later doesn't touch graph/nodes.py.

Topology (who depends on whom) is data/service_topology.json, shared
across all scenarios -- same "shared corpus, not reseeded per scenario"
reasoning as the postmortem store: a topology that only ever contains the
one scenario under test wouldn't exercise hop-distance traversal at all.
Health status per service is scenario-specific ("upstream_health" in each
scenario JSON) since whether a given service happens to be degraded is
part of the incident's story, not a structural fact about who calls whom.

Fidelity is deliberately a status flag (healthy/degraded + optional detail
string), not a full metrics time series -- see the README's "Known
limitations" section for why, and what a follow-on (temporal-correlation
reasoning, i.e. did the upstream's anomaly onset precede this alert)
would need on top of this.
"""
from __future__ import annotations

import json
from pathlib import Path

from clients.base import ServiceHealthClient

PROJECT_ROOT = Path(__file__).resolve().parent.parent
TOPOLOGY_PATH = PROJECT_ROOT / "data" / "service_topology.json"


class DummyServiceHealthClient(ServiceHealthClient):
    def __init__(self, scenario: dict):
        self.scenario = scenario
        self._topology = json.loads(TOPOLOGY_PATH.read_text())

    def get_upstream_health(self, service_name: str, max_hops: int = 2) -> list[dict]:
        # A scenario's "upstream_health" entries are either a plain status
        # string ("healthy"/"degraded") or {"status": ..., "detail": ...}
        # for a richer explanation. A service the scenario doesn't mention
        # at all defaults to "healthy" -- most upstreams in most incidents
        # are fine, and requiring every scenario to enumerate every hop's
        # status would be needless authoring overhead.
        health_overrides = self.scenario.get("upstream_health", {})

        # BFS outward from service_name, capped at max_hops and deduped
        # against cycles -- a real dependency graph can have both, and an
        # unbounded walk of a wide/cyclic graph would blow up the evidence
        # set into something no LLM prompt should have to read.
        visited = {service_name}
        frontier = [service_name]
        results: list[dict] = []

        for hop in range(1, max_hops + 1):
            next_frontier: list[str] = []
            for svc in frontier:
                for upstream in self._topology.get(svc, {}).get("upstream", []):
                    if upstream in visited:
                        continue
                    visited.add(upstream)
                    next_frontier.append(upstream)

                    entry = health_overrides.get(upstream, "healthy")
                    if isinstance(entry, dict):
                        status, detail = entry.get("status", "healthy"), entry.get("detail")
                    else:
                        status, detail = entry, None

                    results.append({
                        "service": upstream,
                        "hop_distance": hop,
                        "status": status,
                        "detail": detail,
                    })
            frontier = next_frontier
            if not frontier:
                break

        return results
