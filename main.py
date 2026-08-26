#!/usr/bin/env python3
"""
TriageGraph -- CLI entrypoint.

Usage:
    python main.py --scenario kafka_consumer_lag_deploy
    python main.py --scenario kafka_consumer_lag_deploy --out report.md
    python main.py --scenario kafka_consumer_lag_deploy --auto-approve   # non-interactive, accepts top hypothesis

Milestone 4: the compiled graph now pauses before human_approval_gate
(interrupt_before, see graph/build_graph.py). Each run gets its own
thread_id so the MemorySaver checkpointer can track it; invoke() returns
the paused state, prompt_human() reads it and writes a decision back via
update_state(), and invoke(None, ...) resumes from exactly where it
stopped. --auto-approve skips the prompt (accepts the top hypothesis
every time) -- needed for scripted/regression runs, and anticipates
milestone 5's eval harness running all golden scenarios unattended.
"""
from __future__ import annotations

import argparse
import datetime
import uuid
from pathlib import Path

from clients.scenario_loader import load_scenario
from graph.build_graph import build_graph
from config import settings


def build_alert_payload(scenario_name: str) -> dict:
    scenario = load_scenario(scenario_name)
    alert = dict(scenario["alert"])
    alert["timestamp"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
    alert["_scenario"] = scenario_name  # milestone-1 shortcut, see graph/nodes.py
    return alert


def prompt_human(state: dict) -> dict:
    """Interactive approval prompt. Returns the state update to write back
    via graph.update_state() before resuming."""
    ranked = state["ranked_hypotheses"]

    print("\n" + "=" * 70)
    print("HUMAN APPROVAL GATE")
    print("=" * 70)
    for h in ranked:
        print(f"  [{h.id}]  confidence {h.confidence:.2f}  -- {h.description}")
    print()
    print("  1) Accept top hypothesis")
    print("  2) Pick a different hypothesis")
    print("  3) Reject all -- re-investigate with feedback")

    choice = input("Choice [1/2/3, default 1]: ").strip() or "1"

    if choice == "2":
        valid_ids = {h.id for h in ranked}
        chosen = input(f"Which id? ({', '.join(sorted(valid_ids))}): ").strip()
        if chosen not in valid_ids:
            print(f"'{chosen}' isn't one of the ranked ids -- defaulting to accept top hypothesis instead.")
            return {"human_approved": True, "human_decision": "accept"}
        return {"human_approved": True, "human_decision": f"pick_other:{chosen}"}

    if choice == "3":
        feedback = input("What should the re-investigation focus on? ").strip()
        return {"human_approved": False, "human_decision": "reject", "human_feedback": feedback}

    return {"human_approved": True, "human_decision": "accept"}


def auto_approve(state: dict) -> dict:
    return {"human_approved": True, "human_decision": "accept"}


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--scenario", default=settings.active_scenario, help="Synthetic scenario name (data/synthetic_incidents/<name>.json)")
    parser.add_argument("--out", help="Write the final report markdown to this file (also prints to stdout)")
    parser.add_argument("--auto-approve", action="store_true", help="Skip the interactive prompt; always accept the top-ranked hypothesis")
    args = parser.parse_args()

    alert_raw = build_alert_payload(args.scenario)
    graph = build_graph()
    config = {"configurable": {"thread_id": str(uuid.uuid4())}}
    decide = auto_approve if args.auto_approve else prompt_human

    print(f"Running scenario: {args.scenario!r}\n")
    state = graph.invoke({"alert_raw": alert_raw}, config=config)

    # interrupt_before pauses execution and returns the state as of the pause;
    # graph.get_state(config).next lists the node(s) still queued to run. An
    # empty tuple means the graph ran to END with nothing paused.
    while graph.get_state(config).next:
        decision_update = decide(state)
        graph.update_state(config, decision_update)
        state = graph.invoke(None, config=config)

    print(state["final_report_markdown"])

    if args.out:
        Path(args.out).write_text(state["final_report_markdown"])
        print(f"\n(written to {args.out})")


if __name__ == "__main__":
    main()
