"""
Milestone 2 added: real fan-out/fan-in, plus the widen_time_window
conditional loop scenario 3 needs (built for real, not deferred -- an open
question in the design doc §13, resolved during v1 review since
retrofitting a loop into a graph built linear is real rework).

Milestone 3 adds: generate_hypotheses and rank_hypotheses, inserted between
the "proceed" branch of assess_evidence and finalize_report -- the graph's
first two LLM calls.

Fan-out uses four separate state keys (metrics_evidence, logs_evidence,
deploy_evidence, postmortem_evidence) rather than a shared reducer -- the
design doc's own recommendation (§5.2): no reducer complexity needed when
there's no write conflict to begin with.

Shape:

    normalize_alert
        -> [fetch_metrics, fetch_logs, fetch_deploys, search_postmortems]  (fan-out)
        -> assess_evidence                                                 (fan-in)
             -- conditional --
             insufficient, not yet widened -> widen_time_window -> fan-out again (loop, once)
             otherwise                     -> generate_hypotheses -> rank_hypotheses -> finalize_report -> END
"""
from __future__ import annotations

from langgraph.graph import END, StateGraph

from graph.nodes import (
    assess_evidence,
    fetch_deploys,
    fetch_logs,
    fetch_metrics,
    finalize_report,
    generate_hypotheses,
    normalize_alert,
    rank_hypotheses,
    route_after_assessment,
    search_postmortems,
    widen_time_window,
)
from graph.state import IncidentState

FETCH_NODES = ["fetch_metrics", "fetch_logs", "fetch_deploys", "search_postmortems"]


def build_graph():
    graph = StateGraph(IncidentState)

    graph.add_node("normalize_alert", normalize_alert)
    graph.add_node("fetch_metrics", fetch_metrics)
    graph.add_node("fetch_logs", fetch_logs)
    graph.add_node("fetch_deploys", fetch_deploys)
    graph.add_node("search_postmortems", search_postmortems)
    graph.add_node("assess_evidence", assess_evidence)
    graph.add_node("widen_time_window", widen_time_window)
    graph.add_node("generate_hypotheses", generate_hypotheses)
    graph.add_node("rank_hypotheses", rank_hypotheses)
    graph.add_node("finalize_report", finalize_report)

    graph.set_entry_point("normalize_alert")

    # fan-out (from both normalize_alert and, on a widen loop, widen_time_window)
    for node in FETCH_NODES:
        graph.add_edge("normalize_alert", node)
        graph.add_edge("widen_time_window", node)

    # fan-in
    for node in FETCH_NODES:
        graph.add_edge(node, "assess_evidence")

    graph.add_conditional_edges(
        "assess_evidence",
        route_after_assessment,
        {"widen": "widen_time_window", "proceed": "generate_hypotheses"},
    )

    graph.add_edge("generate_hypotheses", "rank_hypotheses")
    graph.add_edge("rank_hypotheses", "finalize_report")
    graph.add_edge("finalize_report", END)

    return graph.compile()
