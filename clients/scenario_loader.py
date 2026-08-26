"""
Loads a synthetic incident scenario and resolves its relative-offset data
(metrics patterns, log lines, deploys -- all defined as "N minutes before
the alert") into absolute timestamps anchored to a given alert time.

Anchoring to the alert time rather than baking absolute timestamps into the
JSON is deliberate: a golden scenario reused for regression testing (design
doc §9) should look equally fresh whenever you run it, not drift stale the
way this exact mistake did in an earlier project.
"""
from __future__ import annotations

import datetime
import json
from pathlib import Path

SCENARIOS_DIR = Path(__file__).resolve().parent.parent / "data" / "synthetic_incidents"


def load_scenario(name: str) -> dict:
    path = SCENARIOS_DIR / f"{name}.json"
    if not path.exists():
        available = [p.stem for p in SCENARIOS_DIR.glob("*.json")]
        raise FileNotFoundError(f"No scenario named {name!r}. Available: {available}")
    return json.loads(path.read_text())


def resolve_alert(scenario: dict, alert_time: datetime.datetime | None = None) -> dict:
    """Returns the scenario's alert payload with a concrete timestamp attached."""
    alert_time = alert_time or datetime.datetime.now(datetime.timezone.utc)
    alert = dict(scenario["alert"])
    alert["timestamp"] = alert_time.isoformat()
    return alert


def offset_timestamp(alert_time: datetime.datetime, offset_minutes: float) -> str:
    return (alert_time + datetime.timedelta(minutes=offset_minutes)).isoformat()
