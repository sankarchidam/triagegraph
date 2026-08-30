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

Milestone 4 adds: human_approval_gate, a no-op node that exists purely as
an interrupt_before target (graph/build_graph.py pauses execution before
it runs; main.py does the actual human interaction via graph.get_state /
update_state / resume). Reject routes back to generate_hypotheses with
human_feedback folded into the prompt -- capped at one re-investigation
pass via the `reinvestigated` flag, the same one-shot-loop pattern as
widen_time_window/time_window_widened.

fetch_upstream_health adds dependency-graph awareness: how healthy are
the services this one depends on, up to MAX_UPSTREAM_HOPS hops out. Rule-
based, like the other fetch nodes -- walking a topology graph and reading
a status flag needs no LLM. Distance is deliberately NOT a hard filter or
a deterministic score here: hop_distance is surfaced as a structured fact
on the Evidence item, and the actual weighing (closer is usually more
likely, but not always -- see the prompts) is left to generate_hypotheses/
rank_hypotheses, the same way root-cause-vs-symptom reasoning about
deploys was. An unconditional prose prior has caused two real hallucination
bugs in this codebase already (see README's "Evidence design" section) --
this prompt is written to state the prior's limits from the start, not
patch them in after the fact.
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
from clients.service_topology_dummy import DummyServiceHealthClient
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

# How many dependency hops out fetch_upstream_health walks. 2 matches the
# motivating case (immediate upstream + its immediate upstream); deeper
# graphs get noisy/high-fan-out fast, and this is a prior for the LLM to
# weigh, not a claim that anything beyond this distance is irrelevant.
MAX_UPSTREAM_HOPS = 2

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


def _format_window_span(start: str, end: str) -> str:
    """Human-readable window duration for evidence summaries. This is the
    signal that distinguishes a gradual trend from a sudden one -- a metric
    that "climbed from X to Y" reads very differently over a 30-minute
    window than over an 8-hour one, but until milestone 6 that duration
    never made it into the LLM prompt at all, only the before/after values.
    Found while investigating why scenario 3 (a slow leak, specifically
    widened to 8 hours to make the gradual climb visible) sometimes got a
    vague "excessive memory usage" hypothesis instead of "memory leak" --
    the model had no way to know the climb was gradual rather than sudden."""
    minutes = (datetime.datetime.fromisoformat(end) - datetime.datetime.fromisoformat(start)).total_seconds() / 60
    return f"{minutes:.0f}-minute" if minutes < 90 else f"{minutes / 60:.1f}-hour"


def fetch_metrics(state: IncidentState) -> dict:
    scenario = load_scenario_for_state(state)
    alert_time = datetime.datetime.fromisoformat(state["alert_raw"]["timestamp"])
    client = DummyPrometheusClient(scenario, alert_time)

    start, end = state["time_window"]
    window_span = _format_window_span(start, end)
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
                summary=f"{query} climbed from {window_start_value:g} to {peak_value:g} over the {window_span} window",
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


def fetch_upstream_health(state: IncidentState) -> dict:
    scenario = load_scenario_for_state(state)
    client = DummyServiceHealthClient(scenario)
    hops = client.get_upstream_health(state["service_name"], max_hops=MAX_UPSTREAM_HOPS)

    evidence = []
    for i, hop in enumerate(hops):
        is_degraded = hop["status"] == "degraded"
        plural = "s" if hop["hop_distance"] != 1 else ""
        detail = f" -- {hop['detail']}" if hop.get("detail") else ""
        evidence.append(Evidence(
            id=f"upstream-{i}",
            source="upstream_health",
            summary=(
                f"{hop['service']} ({hop['hop_distance']} hop{plural} upstream of {state['service_name']}): "
                f"{'DEGRADED' if is_degraded else 'healthy'}{detail}"
            ),
            raw_ref=f"service:{hop['service']}",
            hop_distance=hop["hop_distance"],
            # Healthy upstreams are kept, not dropped -- "checked, it's fine" rules a
            # cause out, same reasoning as a flat metric. Only degraded ones are notable.
            is_notable=is_degraded,
        ))
    return {"upstream_evidence": evidence}


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
    """Evidence surfaced to the LLM. Deliberately NOT the same thing as "all
    evidence gathered" -- see finalize_report, which does show every
    postmortem hit for audit purposes. is_notable means different things
    per source, and only some of those meanings are informative to show a
    reasoning model:

    - metrics: a flat/non-anomalous metric is real negative evidence ("error
      rate stayed flat" rules a cause out) -- keep all of it.
    - logs: an INFO line can be real negative evidence too (e.g. "consumer
      group rebalance triggered" was cited as *contradicting* a hypothesis
      in milestone 3's testing) -- keep all of it.
    - postmortems: a below-threshold vector-search hit is NOT negative
      evidence the way a flat metric is -- it's just embedding noise from a
      small corpus, with no causal claim attached at all. Milestone 6 found
      the model treating a 0.22-similarity distractor postmortem as
      supporting evidence for a hallucinated hypothesis, because nothing in
      the prompt distinguished it from a real match. Filtered here instead
      of at the prompt-wording level, so there's no distinction left to be
      missed.
    - upstream_health: same reasoning as metrics/logs, not postmortems -- a
      healthy upstream ("checked payment-gateway, it's fine") rules a cause
      out just as validly as a flat metric does. Keep all of it.
    """
    return [
        *state.get("metrics_evidence", []),
        *state.get("logs_evidence", []),
        *state.get("deploy_evidence", []),
        *state.get("upstream_evidence", []),
        *[e for e in state.get("postmortem_evidence", []) if e.is_notable],
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
to five sources: metrics, logs, recent deploys, upstream service health, and similar past incidents (postmortems). \
Each evidence item has a stable id like "deploys-0" -- cite these ids exactly when referencing evidence, never \
invent new ones.

Metric evidence states the window it climbed over, e.g. "over the 8.1-hour window" vs "over the 30-minute window" -- \
that duration is a real signal about the failure mode, not incidental phrasing. A metric climbing steadily over \
hours reads as a gradual/cumulative cause (a leak, an unbounded cache, slow accumulation); the same climb inside a \
tight few-minute window reads as sudden (a spike, a step change, a released deploy). Use it to sharpen a vague \
symptom restatement (e.g. "memory usage increased" says nothing a human doesn't already know from the alert) into \
an actual mechanism (e.g. "a gradual leak, evidenced by a steady climb over N hours" says something new).

Distinguish root cause from symptom. IF a deploy or config change is present in the evidence below, treat it as \
the highest-signal evidence available -- most incidents that DO have a recent change correlate with it. If a \
deploy's diff plausibly explains a pattern you see elsewhere in the evidence (e.g. a new client with a timeout that \
matches observed timeout errors, or a schema change that matches a deserialization failure), that match makes the \
deploy MORE likely to be the root cause, not less -- the matching symptom is supporting evidence FOR the deploy \
hypothesis, not a separate, independently-ranked hypothesis competing against it. Only generate a symptom as its \
own standalone hypothesis if no deploy or change plausibly explains it.

If there is NO deploy evidence listed below at all, do not hypothesize one anyway -- "deploys are usually the cause" \
is a prior about incidents that have a deploy in evidence, not permission to assume an unlogged one happened. An \
empty deploy list is itself informative: it rules deploys out, it doesn't leave them open as a guess.

Upstream health evidence is tagged with a hop distance (e.g. "1 hop upstream", "2 hops upstream" of the alerting \
service). Treat distance as a prior, not a rule: a DEGRADED service 1 hop away is, all else equal, a more likely \
root cause than one 2 hops away, because failures are usually dampened as they propagate through timeouts and \
retries at each hop. But this prior can be wrong. If the 1-hop service is itself DEGRADED with no independent \
explanation of its own (i.e. nothing else in the evidence explains why the 1-hop service would be unhealthy on its \
own), and a 2-hop service is also DEGRADED, treat the 1-hop service as a symptom the 2-hop service's failure is \
passing through undampened -- the same root-cause-vs-symptom reasoning you apply to deploys. Do not discard a \
farther-hop DEGRADED service just because it's farther away; weigh it, don't filter it out.

Do not assume a cause not supported by the evidence provided -- this includes assuming a *type* of cause (a deploy, \
a config change) that has no corresponding evidence item, even if that type is a common root cause in general. \
Generate 3-6 distinct hypotheses. For each:
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
    feedback = state.get("human_feedback")

    user_content = f"Alert: {state['alert_summary']}\nService: {state['service_name']}\n\nEvidence:\n{_evidence_listing(evidence)}"
    if feedback:
        user_content += (
            f"\n\nA human reviewer rejected the previous hypothesis set and asked you to re-investigate with this "
            f"specific feedback -- treat it as the most important signal in this pass: {feedback}"
        )

    model = get_chat_model(settings.reasoning_model).with_structured_output(GeneratedHypotheses)
    result: GeneratedHypotheses = model.invoke([
        {"role": "system", "content": GENERATE_HYPOTHESES_SYSTEM_PROMPT},
        {"role": "user", "content": user_content},
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
    out = {"hypotheses": hypotheses, "insufficient_evidence_note": result.insufficient_evidence_note}
    if feedback:
        # Only a feedback-driven pass counts as "the one re-investigation" -- the
        # initial run must never set this, or route_after_human_decision would
        # never allow the loop to fire at all.
        out["reinvestigated"] = True
    return out


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
evidence. A "deploy caused this" hypothesis can only reach this tier if you can cite a real deploys-N id for it in \
supporting_evidence -- if the full evidence list below has no deploy evidence item at all, that hypothesis has no \
causal link to cite and cannot score here no matter how plausible its narrative reads; score it 0.2-0.49 or lower \
and note the missing evidence in contradicting_evidence.
- 0.5-0.79: a plausible mechanism with real supporting evidence, but either a gap in the causal chain or at least \
one unaddressed contradiction.
- 0.2-0.49: weak or circumstantial correlation only, or meaningfully contradicted by other evidence.
- 0.0-0.19: evidence actively rules this hypothesis out.

Root cause vs symptom: if one hypothesis is "a deploy whose diff explains X" and another is "X is happening" \
(the same symptom X, with no independent explanation of its own), the deploy hypothesis should score HIGHER, not \
the same or lower -- it explains everything the symptom-only hypothesis explains, plus why. Don't let a symptom \
hypothesis outrank the deploy that plausibly caused it just because the symptom's evidence is more voluminous \
(e.g. more log lines) -- volume of evidence for a symptom isn't the same as it being the root cause.

Same reasoning applies to upstream health hop distance: a hypothesis blaming a 1-hop-upstream DEGRADED service is a \
stronger default candidate than one blaming a 2-hop-upstream DEGRADED service, all else equal -- but if the 1-hop \
service has no independent explanation for its own degradation while a 2-hop service is also DEGRADED, the 1-hop \
service is a symptom passing the 2-hop failure through, and the 2-hop hypothesis should score higher, not lower, \
for being farther away. Don't apply the distance prior as a hard rule when the evidence itself contradicts it.

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


# ---------------------------------------------------------------------------
# Milestone 4: human approval gate
# ---------------------------------------------------------------------------

def human_approval_gate(state: IncidentState) -> dict:
    """Deliberately a no-op. The real work happens outside the graph: this
    node's only job is to exist as an interrupt_before target, so
    build_graph()'s compiled graph pauses *before* running it. main.py reads
    the paused state, prompts the human, writes the decision back via
    graph.update_state(), and resumes -- by the time this function body
    actually executes, human_decision is already set."""
    return {}


def route_after_human_decision(state: IncidentState) -> str:
    """Conditional edge target selector for human_approval_gate. Reject loops
    back to generate_hypotheses exactly once -- reinvestigated guards against
    a second reject ever looping again, so a reviewer who rejects twice in a
    row still gets a terminal report instead of an infinite gate."""
    if state.get("human_decision") == "reject" and not state.get("reinvestigated"):
        return "reinvestigate"
    return "finalize"


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
        ("Upstream health evidence", "upstream_evidence"),
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

    decision = state.get("human_decision", "")
    top_id = ranked[0].id if ranked else None
    lines.append("## Human decision")
    if decision == "accept":
        lines.append(f"✅ **Accepted** -- {top_id} approved as the leading hypothesis, as ranked.")
    elif decision.startswith("pick_other:"):
        chosen_id = decision.split(":", 1)[1]
        lines.append(f"✅ **Approved** -- reviewer selected {chosen_id} over the model's top-ranked {top_id}.")
    elif decision == "reject":
        lines.append(
            f"❌ **Rejected after one re-investigation pass** -- reviewer feedback: "
            f"\"{state.get('human_feedback', '(none given)')}\". No hypothesis above is approved; "
            f"escalate to a human on-call for manual investigation."
        )
    else:
        lines.append(f"(no decision recorded: {decision!r})")
    lines.append("")

    return {"final_report_markdown": "\n".join(lines)}


def load_scenario_for_state(state: IncidentState) -> dict:
    """Milestone 1 shortcut: the scenario name lives on the raw alert payload
    (set by main.py) rather than being re-derived. A real alert has no such
    field -- fetch_metrics on a real client just queries Prometheus directly
    and this function goes away entirely (§12: dummy_mode flip)."""
    return load_scenario(state["alert_raw"]["_scenario"])
