#!/usr/bin/env python3
"""
TriageGraph -- CLI entrypoint.

Milestone 1 usage:
    python main.py --scenario kafka_consumer_lag_deploy
    python main.py --scenario kafka_consumer_lag_deploy --out report.md
"""
from __future__ import annotations

import argparse
import datetime
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


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--scenario", default=settings.active_scenario, help="Synthetic scenario name (data/synthetic_incidents/<name>.json)")
    parser.add_argument("--out", help="Write the final report markdown to this file (also prints to stdout)")
    args = parser.parse_args()

    alert_raw = build_alert_payload(args.scenario)
    graph = build_graph()

    print(f"Running scenario: {args.scenario!r}\n")
    result = graph.invoke({"alert_raw": alert_raw})

    print(result["final_report_markdown"])

    if args.out:
        Path(args.out).write_text(result["final_report_markdown"])
        print(f"\n(written to {args.out})")


if __name__ == "__main__":
    main()
