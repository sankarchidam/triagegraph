"""
Dummy deploy client, standing in for GitHub. Deploys/merged-PRs are defined
in the scenario as offsets from the alert, same resolve-then-filter pattern
as the other dummy clients. This is usually the highest-signal source --
most incidents correlate with a recent change.
"""
from __future__ import annotations

import datetime

from .base import DeployClient


class DummyGithubClient(DeployClient):
    def __init__(self, scenario: dict, alert_time: datetime.datetime):
        self.scenario = scenario
        self.alert_time = alert_time

    def list_deploys(self, service_name: str, start: str, end: str) -> list[dict]:
        start_dt = datetime.datetime.fromisoformat(start)
        end_dt = datetime.datetime.fromisoformat(end)

        results = []
        for entry in self.scenario.get("deploys", []):
            merged_at = self.alert_time + datetime.timedelta(minutes=entry["offset_minutes"])
            if not (start_dt <= merged_at <= end_dt):
                continue
            results.append({
                "pr_number": entry["pr_number"],
                "title": entry["title"],
                "merged_at": merged_at.isoformat(),
                "author": entry["author"],
                "diff_summary": entry["diff_summary"],
                "files_changed": entry.get("files_changed", []),
            })
        return results
