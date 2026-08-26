"""
Dummy logs/traces client, standing in for Splunk/Dynatrace. Log lines are
defined in the scenario as offsets from the alert ("-17 minutes: schema
registry timeout"), resolved to absolute timestamps and filtered to the
requested window here -- same pattern as the Prometheus dummy client.
"""
from __future__ import annotations

import datetime

from .base import LogsClient


class DummyLogsClient(LogsClient):
    def __init__(self, scenario: dict, alert_time: datetime.datetime):
        self.scenario = scenario
        self.alert_time = alert_time

    def search(self, service_name: str, start: str, end: str, query: str = "") -> list[dict]:
        start_dt = datetime.datetime.fromisoformat(start)
        end_dt = datetime.datetime.fromisoformat(end)

        results = []
        for i, entry in enumerate(self.scenario.get("logs", [])):
            ts = self.alert_time + datetime.timedelta(minutes=entry["offset_minutes"])
            if not (start_dt <= ts <= end_dt):
                continue
            if query and query.lower() not in entry["message"].lower():
                continue
            results.append({
                "timestamp": ts.isoformat(),
                "level": entry["level"],
                "message": entry["message"],
                "trace_id": f"trace-{i:04d}",
            })
        return results
