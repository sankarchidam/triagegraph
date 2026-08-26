"""
Graph nodes.

Deliberate deviation from the design doc's §5.1 sketch: normalize_alert and
fetch_metrics are rule-based, not LLM calls. The dummy alert payload is
already structured (service/timestamp/message all present as clean fields),
and metrics are numeric time series -- both are jobs a threshold rule does
deterministically, cheaply, and testably. The LLM is reserved for where it
actually earns its keep: reasoning over ambiguous, multi-source evidence
(generate_hypotheses / rank_hypotheses, milestone 3). Flagged as an open
design question during v1 review and resolved this way.

Milestone 2 added: fetch_logs, fetch_deploys, search_postmortems (true
fan-out alongside fetch_metrics), plus assess_evidence / widen_time_window,
the conditional loop scenario 3 needs. assess_evidence is a narrow,
metrics-only mechanical proxy for "is there enough here to reason about" --
deliberately not replaced by an LLM call, since it needs to run before any
LLM involvement to decide whether the window needs widening first.

Milestone 3 adds: generate_hypotheses and rank_hypotheses -- the project's
first real LLM calls, via graph.llm.get_chat_model() so the provider
(OpenAI or Anthropic, config.py's llm_provider) is a config change, not a
code change here.
"""
from __future__ import annotations

import datetime
from pathlib import Path
from typing import Optional

from pydantic import BaseModel, Field

from clients.github_dummy import DummyGithubClient
from clients.postmortem_store import ChromaPostmortemStore
from clients.prometheus_dummy import DummyPrometheusClient
from clients.scenario_loader import load_scenario
from clients.splunk_dynatrace_dummy import DummyLogsClient
from config import settings
from graph.llm import get_chat_model
from graph.state import Evidence, Hypothesis, IncidentState

PROJECT_ROOT = Path(__file__).resolve().parent.parent

# The fixed set of signals fetch_metrics always checks, regardless of
# scenario. A uniform signal set matters more than it looks: it's what lets
# "error rate stayed flat" become evidence in its own right (ruling a cause
# out), not just silence. Scenarios that don't define a given metric fall
# back to a flat zero series (DummyPrometheusClient's default) -- itself a
# reasonable "not tracked / not anomalous" signal.
STANDARD_METRIC_QUERIES = ["cpu_usage_pct", "memory_usage_mb", "error_rate_pct", "latency_p99_ms", "kafka_consumer_lag"]

# A metric counts as anomalous if its peak is at least this many times its
# window-start value. Simple and legible on purpose -- a real Prometheus
# client swap-in later can replace this with a proper z-score/seasonality
# check without touching the node's control flow.
ANOMALY_RATIO_THRESHOLD = 2.0

DEFAULT_WINDOW_BEFORE = datetime.timedelta(minutes=30)
DEFAULT_WINDOW_AFTER = datetime.timedelta(minutes=5)
WIDENED_WINDOW_BEFORE = datetime.timedelta(hours=8)

_postmortem_store: ChromaPostmortemStore | None = None  # lazy singleton -- loading the embedding model isn't free


def _get_postmortem_store() -> ChromaPostmortemStore:
    global _postmortem_store
    if _postmortem_store is None:
        _postmortem_store = ChromaPostmortemStore(
            postmortems_dir=PROJECT_ROOT / "data" / "postmortems",
            persist_dir=PROJECT_ROOT / ".chroma",
        )
    return _postmortem_store


def normalize_alert(state: IncidentState) -> dict:
    alert = state["alert_raw"]
    alert_time = datetime.datetime.fromisoformat(alert["timestamp"])
    window_start = alert_time - DEFAULT_WINDOW_BEFORE
    window_end = alert_time + DEFAULT_WINDOW_AFTER

    return {
        "service_name": alert["service"],
        "alert_summary": f"{alert['alert_name']} ({alert['severity']}): {alert['message']}",
        "time_window": (window_start.isoformat(), window_end.isoformat()),
        "time_window_widened": False,
    }


def fetch_metrics(state: IncidentState) -> dict:
    scenario = load_scenario_for_state(state)
    alert_time = datetime.datetime.fromisoformat(state["alert_raw"]["timestamp"])
    client = DummyPrometheusClient(scenario, alert_time)

    start, end = state["time_window"]
    evidence: list[Evidence] = []

    for i, query in enumerate(STANDARD_METRIC_QUERIES):
        result = client.query_range(query, start, end)
        series = result["data"]["result"][0]["values"]
        if not series:
            continue
        window_start_value = float(series[0][1])
        peak_value = max(float(v) for _, v in series)

        # A genuine jump from zero (start=0, peak>0) is anomalous. Start=0
        # AND peak=0 is just an untouched metric -- not anomalous. This
        # matters: kafka_consumer_lag defaults to a flat zero series for
        # any scenario that doesn't define it, and that must never register
        # as an anomaly.
        if window_start_value == 0:
            is_anomalous = peak_value > 0
        else:
            is_anomalous = (peak_value / window_start_value) >= ANOMALY_RATIO_THRESHOLD

        if is_anomalous:
            evidence.append(Evidence(
                id=f"metrics-{i}",
                source="metrics",
                summary=f"{query} climbed from {window_start_value:g} to {peak_value:g} within the window",
                raw_ref=f"promql:{query}",
                is_notable=True,
            ))
        else:
            evidence.append(Evidence(
                id=f"metrics-{i}",
                source="metrics",
                summary=f"{query} stayed flat around {window_start_value:g} -- no anomaly",
                raw_ref=f"promql:{query}",
                is_notable=False,
            ))

    return {"metrics_evidence": evidence}


def fetch_logs(state: IncidentState) -> dict:
    scenario = load_scenario_for_state(state)
    alert_time = datetime.datetime.fromisoformat(state["alert_raw"]["timestamp"])
    client = DummyLogsClient(scenario, alert_time)

    start, end = state["time_window"]
    entries = client.search(state["service_name"], start, end)

    evidence = [
        Evidence(
            id=f"logs-{i}",
            source="logs",
            summary=f"[{e['level']}] {e['message']}",
            raw_ref=e["trace_id"],
            is_notable=e["level"] in ("WARN", "ERROR", "CRITICAL"),
        )
        for i, e in enumerate(entries)
    ]
    return {"logs_evidence": evidence}


def fetch_deploys(state: IncidentState) -> dict:
    scenario = load_scenario_for_state(state)
    alert_time = datetime.datetime.fromisoformat(state["alert_raw"]["timestamp"])
    client = DummyGithubClient(scenario, alert_time)

    start, end = state["time_window"]
    deploys = client.list_deploys(state["service_name"], start, end)

    evidence = [
        Evidence(
            id=f"deploys-{i}",
            source="deploys",
            summary=f"PR #{d['pr_number']} \"{d['title']}\" merged by {d['author']} at {d['merged_at']}: {d['diff_summary']}",
            raw_ref=f"pr:{d['pr_number']}",
            is_notable=True,  # a deploy in the window is inherently worth surfacing
        )
        for i, d in enumerate(deploys)
    ]
    return {"deploy_evidence": evidence}


def search_postmortems(state: IncidentState) -> dict:
    store = _get_postmortem_store()
    hits = store.search(state["alert_summary"], k=3)

    evidence = [
        Evidence(
            id=f"postmortems-{i}",
            source="postmortems",
            summary=f"{h['doc_id']} (similarity {h['score']:.2f}): {h['text'].splitlines()[0].lstrip('# ')}",
            raw_ref=h["doc_id"],
            is_notable=h["score"] >= 0.3,  # low-similarity hits are noise, not evidence
        )
        for i, h in enumerate(hits)
    ]
    return {"postmortem_evidence": evidence}


def assess_evidence(state: IncidentState) -> dict:
    """Narrow, metrics-only mechanical proxy for "is there enough here to
    reason about" -- deliberately not an LLM call, since it has to run
    *before* any LLM involvement to decide whether the window needs
    widening first. This is exactly scenario 3's test: a 30-minute slice
    near the end of a 6-hour linear climb looks like a high-but-flat
    plateau, not a trend, so no metric trips the anomaly ratio until the
    window widens."""
    any_metric_notable = any(e.is_notable for e in state.get("metrics_evidence", []))
    return {"evidence_sufficient": any_metric_notable}


def widen_time_window(state: IncidentState) -> dict:
    alert_time = datetime.datetime.fromisoformat(state["alert_raw"]["timestamp"])
    window_start = alert_time - WIDENED_WINDOW_BEFORE
    window_end = alert_time + DEFAULT_WINDOW_AFTER
    return {
        "time_window": (window_start.isoformat(), window_end.isoformat()),
        "time_window_widened": True,
    }


def route_after_assessment(state: IncidentState) -> str:
    """Conditional edge target selector for assess_evidence. Widens at most
    once -- time_window_widened guards against ever looping twice, so a
    scenario with no answer even in the wide window still terminates."""
    if not state["evidence_sufficient"] and not state["time_window_widened"]:
        return "widen"
    return "proceed"


# ---------------------------------------------------------------------------
# Milestone 3: reasoning nodes
# ---------------------------------------------------------------------------

def _all_evidence(state: IncidentState) -> list[Evidence]:
    return [
        *state.get("metrics_evidence", []),
        *state.get("logs_evidence", []),
        *state.get("deploy_evidence", []),
        *state.get("postmortem_evidence", []),
    ]


def _evidence_listing(evidence: list[Evidence]) -> str:
    if not evidence:
        return "(no evidence gathered)"
    return "\n".join(f"- [{e.id}] ({e.source}): {e.summary}" for e in evidence)


class HypothesisDraft(BaseModel):
    """LLM-facing shape for a single hypothesis -- no id, generate_hypotheses
    assigns stable ids (h1, h2, ...) after the call so the model never has
    to invent identifiers."""
    description: str
    confidence: float = Field(ge=0.0, le=1.0)
    supporting_evidence: list[str] = Field(default_factory=list, description="Evidence ids, e.g. 'deploys-0'")
    contradicting_evidence: list[str] = Field(default_factory=list, description="Evidence ids")
    recommended_next_step: str


class GeneratedHypotheses(BaseModel):
    hypotheses: list[HypothesisDraft]
    insufficient_evidence_note: Optional[str] = Field(
        default=None,
        description="If the evidence can't cleanly distinguish between hypotheses, say so here and name what additional data would resolve the ambiguity. Null if evidence is sufficient.",
    )


GENERATE_HYPOTHESES_SYSTEM_PROMPT = """You are an SRE incident analyst. You are given normalized evidence from up \
to four sources: metrics, logs, recent deploys, and similar past incidents (postmortems). Each evidence item has a \
stable id like "deploys-0" -- cite these ids exactly when referencing evidence, never invent new ones.

Distinguish root cause from symptom. A deploy or config change is usually the highest-signal evidence available -- \
most incidents correlate with a recent change. If a deploy's diff plausibly explains a pattern you see elsewhere in \
the evidence (e.g. a new client with a timeout that matches observed timeout errors, or a schema change that matches \
a deserialization failure), that match makes the deploy MORE likely to be the root cause, not less -- the matching \
symptom is supporting evidence FOR the deploy hypothesis, not a separate, independently-ranked hypothesis competing \
against it. Only generate a symptom as its own standalone hypothesis if no deploy or change plausibly explains it.

Do not assume a cause not supported by the evidence provided. Generate 3-6 distinct hypotheses. For each:
- a one-sentence description
- an initial confidence (0-1) based on how directly the evidence supports it
- the evidence ids that support it, and the evidence ids that contradict it (most hypotheses will have some of each -- \
list contradicting evidence honestly even for your leading hypothesis)
- a concrete next diagnostic step a human could take to confirm or rule it out

If the evidence is insufficient to distinguish between hypotheses, say so explicitly in insufficient_evidence_note \
and recommend what additional data would resolve the ambiguity. Do not paper over genuine ambiguity with false \
confidence."""


def generate_hypotheses(state: IncidentState) -> dict:
    evidence = _all_evidence(state)
    model = get_chat_model(settings.reasoning_model).with_structured_output(GeneratedHypotheses)

    result: GeneratedHypotheses = model.invoke([
        {"role": "system", "content": GENERATE_HYPOTHESES_SYSTEM_PROMPT},
        {"role": "user", "content": f"Alert: {state['alert_summary']}\nService: {state['service_name']}\n\nEvidence:\n{_evidence_listing(evidence)}"},
    ])

    hypotheses = [
        Hypothesis(
            id=f"h{i+1}",
            description=d.description,
            confidence=d.confidence,
            supporting_evidence=d.supporting_evidence,
            contradicting_evidence=d.contradicting_evidence,
            recommended_next_step=d.recommended_next_step,
        )
        for i, d in enumerate(result.hypotheses)
    ]
    return {"hypotheses": hypotheses, "insufficient_evidence_note": result.insufficient_evidence_note}


class RankedHypothesis(BaseModel):
    """LLM-facing shape for the ranking pass -- id IS included here, since
    the model is re-examining hypotheses generate_hypotheses already
    assigned ids to, not inventing new ones."""
    id: str
    confidence: float = Field(ge=0.0, le=1.0)
    supporting_evidence: list[str] = Field(default_factory=list)
    contradicting_evidence: list[str] = Field(default_factory=list)
    recommended_next_step: str


class RankedHypotheses(BaseModel):
    ranked: list[RankedHypothesis]


RANK_HYPOTHESES_SYSTEM_PROMPT = """You are cross-examining a set of incident root-cause hypotheses against all \
available evidence, one more time, before they're shown to a human. This second pass exists specifically to catch \
the case where the first pass over-weighted whichever evidence was freshest in context.

For each hypothesis (by id): re-derive supporting_evidence and contradicting_evidence from scratch against the \
full evidence list below -- don't just repeat what was there before. Then assign a final confidence using this rubric:

- 0.8-1.0: at least one directly correlated, unambiguous causal link (e.g. a deploy whose diff plausibly explains \
the failure mode, or a downstream dependency failure exactly matching the timing) with no meaningful contradicting \
evidence.
- 0.5-0.79: a plausible mechanism with real supporting evidence, but either a gap in the causal chain or at least \
one unaddressed contradiction.
- 0.2-0.49: weak or circumstantial correlation only, or meaningfully contradicted by other evidence.
- 0.0-0.19: evidence actively rules this hypothesis out.

Root cause vs symptom: if one hypothesis is "a deploy whose diff explains X" and another is "X is happening" \
(the same symptom X, with no independent explanation of its own), the deploy hypothesis should score HIGHER, not \
the same or lower -- it explains everything the symptom-only hypothesis explains, plus why. Don't let a symptom \
hypothesis outrank the deploy that plausibly caused it just because the symptom's evidence is more voluminous \
(e.g. more log lines) -- volume of evidence for a symptom isn't the same as it being the root cause.

Update recommended_next_step if the re-examination suggests a better diagnostic step. Return one entry per input \
hypothesis id -- don't drop or add hypotheses at this stage."""


def rank_hypotheses(state: IncidentState) -> dict:
    evidence = _all_evidence(state)
    hypotheses_by_id = {h.id: h for h in state["hypotheses"]}
    hypotheses_listing = "\n".join(
        f"- [{h.id}] {h.description} (initial confidence {h.confidence})" for h in state["hypotheses"]
    )

    model = get_chat_model(settings.ranking_model).with_structured_output(RankedHypotheses)
    result: RankedHypotheses = model.invoke([
        {"role": "system", "content": RANK_HYPOTHESES_SYSTEM_PROMPT},
        {"role": "user", "content": f"Hypotheses:\n{hypotheses_listing}\n\nFull evidence:\n{_evidence_listing(evidence)}"},
    ])

    ranked = []
    for r in result.ranked:
        original = hypotheses_by_id.get(r.id)
        if original is None:
            continue  # defense in depth: ignore an id the model invented that wasn't in the input set
        ranked.append(Hypothesis(
            id=r.id,
            description=original.description,
            confidence=r.confidence,
            supporting_evidence=r.supporting_evidence,
            contradicting_evidence=r.contradicting_evidence,
            recommended_next_step=r.recommended_next_step,
        ))

    ranked.sort(key=lambda h: h.confidence, reverse=True)
    return {"ranked_hypotheses": ranked}


def finalize_report(state: IncidentState) -> dict:
    lines = [
        f"# Incident Report: {state['service_name']}",
        "",
        f"**Alert:** {state['alert_summary']}",
        f"**Window:** {state['time_window'][0]} to {state['time_window'][1]}"
        + (" (widened)" if state.get("time_window_widened") else ""),
        f"**Evidence sufficient in this window:** {state.get('evidence_sufficient')}",
        "",
    ]

    for label, key in [
        ("Metrics evidence", "metrics_evidence"),
        ("Logs evidence", "logs_evidence"),
        ("Deploy evidence", "deploy_evidence"),
        ("Postmortem evidence", "postmortem_evidence"),
    ]:
        items = state.get(key, [])
        lines.append(f"## {label}")
        if not items:
            lines.append("- (none found in this window)")
        for e in items:
            flag = "⚠️ " if e.is_notable else ""
            lines.append(f"- {flag}[{e.id}] {e.summary}")
        lines.append("")

    if state.get("insufficient_evidence_note"):
        lines.append(f"> ⚠️ **Insufficient evidence noted by the model:** {state['insufficient_evidence_note']}")
        lines.append("")

    ranked = state.get("ranked_hypotheses", [])
    if ranked:
        lines.append("## Ranked hypotheses")
        for h in ranked:
            lines.append(f"### {h.id}. {h.description}  (confidence {h.confidence:.2f})")
            lines.append(f"- Supporting: {', '.join(h.supporting_evidence) or '(none cited)'}")
            lines.append(f"- Contradicting: {', '.join(h.contradicting_evidence) or '(none cited)'}")
            lines.append(f"- Next step: {h.recommended_next_step}")
            lines.append("")

    return {"final_report_markdown": "\n".join(lines)}


def load_scenario_for_state(state: IncidentState) -> dict:
    """Milestone 1 shortcut: the scenario name lives on the raw alert payload
    (set by main.py) rather than being re-derived. A real alert has no such
    field -- fetch_metrics on a real client just queries Prometheus directly
    and this function goes away entirely (§12: dummy_mode flip)."""
    return load_scenario(state["alert_raw"]["_scenario"])
