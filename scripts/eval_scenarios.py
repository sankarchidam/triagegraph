#!/usr/bin/env python3
"""
Milestone 5 eval harness: runs every golden scenario end-to-end
(auto-approve -- always accepts the top-ranked hypothesis, so this grades
generate_hypotheses/rank_hypotheses, not human review) and prints the
accuracy table design doc §9 asked for.

Grading is deliberately NOT LLM-as-judge -- that would mean evaluating the
system with the same kind of model being evaluated, and turns every eval
run into a second source of non-determinism on top of the first. Instead
each scenario JSON carries its own eval_keywords fixture: a short list of
strings the scenario author (me, writing the scenario) attached alongside
correct_root_cause, before ever seeing what the model would say. A top-1
hypothesis counts as correct if its description contains any of them.
Simple, deterministic, and auditable straight from the printed table --
same "rule-based wherever it can be" philosophy as assess_evidence /
fetch_metrics (see graph/nodes.py).

Usage:
    python scripts/eval_scenarios.py                # all golden scenarios
    python scripts/eval_scenarios.py --scenario kafka_consumer_lag_deploy
"""
from __future__ import annotations

import argparse
import datetime
import glob
from pathlib import Path

from clients.scenario_loader import load_scenario
from graph.build_graph import build_graph
from graph.runner import run_incident
from graph.state import Hypothesis, IncidentState

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SCENARIOS_DIR = PROJECT_ROOT / "data" / "synthetic_incidents"


def auto_approve(state: IncidentState) -> dict:
    return {"human_approved": True, "human_decision": "accept"}


def build_alert_payload(scenario_name: str) -> dict:
    scenario = load_scenario(scenario_name)
    alert = dict(scenario["alert"])
    alert["timestamp"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
    alert["_scenario"] = scenario_name
    return alert


def grade_top1(top: Hypothesis | None, eval_keywords: list[str]) -> bool:
    if top is None or not eval_keywords:
        return False
    text = top.description.lower()
    return any(kw.lower() in text for kw in eval_keywords)


def grade_postmortem(state: IncidentState, expected_doc_id: str | None) -> str:
    if expected_doc_id is None:
        return "n/a"
    hits = [e for e in state.get("postmortem_evidence", []) if e.is_notable]
    if not hits:
        return "MISS (no notable hit)"
    top_hit = hits[0]  # search_postmortems returns hits in descending-score order
    if top_hit.raw_ref == expected_doc_id:
        return f"OK ({top_hit.raw_ref})"
    if any(h.raw_ref == expected_doc_id for h in hits):
        return f"PARTIAL (matched, not top -- top was {top_hit.raw_ref})"
    return f"MISS (top was {top_hit.raw_ref})"


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--scenario", help="Run just one scenario instead of every golden scenario")
    args = parser.parse_args()

    if args.scenario:
        names = [args.scenario]
    else:
        names = sorted(Path(f).stem for f in glob.glob(str(SCENARIOS_DIR / "*.json")))

    graph = build_graph()  # one compiled graph + checkpointer, reused across runs (thread_id varies per run)
    rows = []

    for name in names:
        scenario = load_scenario(name)
        alert_raw = build_alert_payload(name)
        print(f"Running {name} ...")
        state = run_incident(alert_raw, auto_approve, graph)

        ranked = state.get("ranked_hypotheses", [])
        top = ranked[0] if ranked else None

        rows.append({
            "scenario": name,
            "correct_root_cause": scenario["correct_root_cause"],
            "top1_id": top.id if top else "-",
            "top1_confidence": f"{top.confidence:.2f}" if top else "-",
            "top1_correct": grade_top1(top, scenario.get("eval_keywords", [])),
            "postmortem": grade_postmortem(state, scenario.get("postmortem_match")),
            # exact match, not just "widened when required" -- widening when it
            # wasn't needed is also worth flagging (wasted a fetch round-trip).
            "window_as_expected": state.get("time_window_widened", False) == scenario.get("requires_widened_window", False),
        })

    root_cause_width = max(len("root cause"), *(len(r["correct_root_cause"]) for r in rows)) + 2
    header = f"{'scenario':<32}{'root cause':<{root_cause_width}}{'top1':<6}{'conf':<7}{'correct':<9}{'window':<8}postmortem"
    print("\n" + "=" * len(header))
    print(header)
    print("-" * len(header))
    n_correct = 0
    for r in rows:
        if r["top1_correct"]:
            n_correct += 1
        print(
            f"{r['scenario']:<32}{r['correct_root_cause']:<{root_cause_width}}{r['top1_id']:<6}{r['top1_confidence']:<7}"
            f"{'YES' if r['top1_correct'] else 'NO':<9}{'ok' if r['window_as_expected'] else 'FAIL':<8}{r['postmortem']}"
        )
    print("-" * len(header))
    print(f"Top-1 accuracy: {n_correct}/{len(rows)}")


if __name__ == "__main__":
    main()
