"""
Dummy Prometheus client. Returns data shaped like prometheus_api_client's
query_range output, but values are driven by the loaded scenario's compact
pattern spec (baseline / anomaly window / peak) rather than random noise --
so the same query against the same scenario always tells the same story,
and that story is reproducible for the golden-scenario regression suite
(design doc §9).
"""
from __future__ import annotations

import datetime

from .base import MetricsClient


class DummyPrometheusClient(MetricsClient):
    def __init__(self, scenario: dict, alert_time: datetime.datetime):
        self.scenario = scenario
        self.alert_time = alert_time

    def query_range(self, query: str, start: str, end: str, step: str = "60s") -> dict:
        spec = self.scenario.get("metrics", {}).get(query)
        start_dt = datetime.datetime.fromisoformat(start)
        end_dt = datetime.datetime.fromisoformat(end)
        step_seconds = self._parse_step(step)

        if spec is None:
            values = self._flat_series(start_dt, end_dt, step_seconds, baseline=0.0)
        else:
            values = self._pattern_series(spec, start_dt, end_dt, step_seconds)

        return {
            "status": "success",
            "data": {
                "resultType": "matrix",
                "result": [{"metric": {"query": query}, "values": values}],
            },
        }

    def _parse_step(self, step: str) -> int:
        if step.endswith("s"):
            return int(step[:-1])
        if step.endswith("m"):
            return int(step[:-1]) * 60
        return int(step)

    def _flat_series(self, start_dt, end_dt, step_seconds, baseline: float) -> list:
        points = []
        t = start_dt
        while t <= end_dt:
            points.append([int(t.timestamp()), str(baseline)])
            t += datetime.timedelta(seconds=step_seconds)
        return points

    def _pattern_series(self, spec: dict, start_dt, end_dt, step_seconds: int) -> list:
        baseline = spec["baseline"]
        pattern = spec.get("pattern", "flat")

        if pattern == "flat":
            return self._flat_series(start_dt, end_dt, step_seconds, baseline)

        if pattern == "linear_climb":
            anomaly_start = self.alert_time + datetime.timedelta(minutes=spec["anomaly_start_offset_minutes"])
            anomaly_end = self.alert_time + datetime.timedelta(minutes=spec["anomaly_end_offset_minutes"])
            peak = spec["peak_value"]
            window_seconds = max((anomaly_end - anomaly_start).total_seconds(), 1)

            points = []
            t = start_dt
            while t <= end_dt:
                if t <= anomaly_start:
                    value = baseline
                elif t >= anomaly_end:
                    value = peak
                else:
                    frac = (t - anomaly_start).total_seconds() / window_seconds
                    value = baseline + frac * (peak - baseline)
                points.append([int(t.timestamp()), str(round(value, 2))])
                t += datetime.timedelta(seconds=step_seconds)
            return points

        raise ValueError(f"Unknown metric pattern: {pattern!r}")
