"""
The state object that flows through the graph. Getting this right up front
avoids the most common failure mode in agent projects: state that's too
loose to reason about (design doc §4).

TypedDict for the top-level state (LangGraph works cleanly with it) +
pydantic sub-models for validation on the nested objects the LLM populates
directly (Evidence, Hypothesis).
"""
from __future__ import annotations

from typing import Literal, Optional, TypedDict

from pydantic import BaseModel, Field


class CodeChange(BaseModel):
    """One changed hunk from a deploy's diff -- what fetch_deploys actually
    points the LLM at, instead of just a PR title/summary. file_path/line_*
    are what let a Hypothesis cite a real line instead of a PR number."""
    file_path: str
    line_start: int
    line_end: int
    snippet: str  # the changed lines themselves, verbatim
    change_type: Literal["added", "removed", "modified"] = "modified"


class Evidence(BaseModel):
    id: str  # stable short id (e.g. "metrics-0") so a Hypothesis can cite exactly which evidence it means
    source: Literal["metrics", "logs", "deploys", "postmortems", "upstream_health"]
    summary: str
    raw_ref: str  # pointer to raw data: the query used, a log id, a PR number, etc.
    is_notable: bool = True  # False for "checked, nothing unusual" evidence (e.g. a flat metric) --
    # machine-checkable so assess_evidence doesn't have to parse English out of `summary`
    hop_distance: Optional[int] = None  # set only for source == "upstream_health" -- how many dependency
    # hops away that service is, so the LLM can weigh it as a structured fact rather than parsed-out prose
    code_changes: list[CodeChange] = Field(default_factory=list)  # set only for source == "deploys"


class Hypothesis(BaseModel):
    id: str
    description: str
    confidence: float = Field(ge=0.0, le=1.0)
    supporting_evidence: list[str] = Field(default_factory=list)  # Evidence ids/summaries
    contradicting_evidence: list[str] = Field(default_factory=list)
    recommended_next_step: str
    implicated_code: Optional[str] = None  # "file_path:line_start-line_end" when a cited deploy's
    # code_changes is what makes this hypothesis plausible; null otherwise. Carried through
    # rank_hypotheses from the draft, same as `description` -- it's a citation, not something
    # the ranking pass re-derives.


class IncidentState(TypedDict):
    alert_raw: dict
    alert_summary: str
    service_name: str
    time_window: tuple[str, str]  # ISO timestamps, derived from alert time
    time_window_widened: bool  # set by widen_time_window; scenario 3 needs this to reach True

    metrics_evidence: list[Evidence]
    logs_evidence: list[Evidence]
    deploy_evidence: list[Evidence]
    postmortem_evidence: list[Evidence]
    upstream_evidence: list[Evidence]

    hypotheses: list[Hypothesis]
    ranked_hypotheses: list[Hypothesis]
    insufficient_evidence_note: Optional[str]  # set by generate_hypotheses if it can't cleanly distinguish hypotheses
    evidence_sufficient: bool  # set by assess_evidence; drives the widen_time_window conditional edge

    human_approved: bool
    human_decision: str  # "accept" | "pick_other:<id>" | "reject" -- set at the approval gate
    human_feedback: Optional[str]  # freeform text on reject, fed back into the re-investigation pass
    reinvestigated: bool  # set by generate_hypotheses when it runs a feedback-driven re-investigation;
    # guards the reject loop to at most once, same pattern as time_window_widened
    final_report_markdown: str
