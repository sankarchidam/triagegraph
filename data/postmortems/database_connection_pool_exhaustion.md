# Postmortem: inventory-service connection pool exhaustion

**Date:** 5 months ago
**Severity:** SEV3
**Service:** inventory-service

## Summary

inventory-service began returning 503s under normal traffic load. No
deploy, no downstream outage. Root cause was a slow leak of unreturned
database connections from a code path that didn't close connections on a
specific error branch, exhausting the pool after roughly 4 hours of runtime.

## Evidence

- Metrics: `db_pool_active_connections` climbed steadily over several hours
  before the alert, not visible in the default 30-minute dashboard window.
- Logs: `QueuePool limit exceeded` errors starting a few minutes before the
  alert.
- Deploys: none in the relevant window (the leaking code had been in
  production for weeks; a rare error branch is what triggered it).

## Root cause

A database connection wasn't released on one exception path in the order
lookup handler. Under normal conditions this branch was rarely hit; a
traffic pattern shift increased its frequency enough to exhaust the pool.

## Resolution

Fixed the connection leak with a context manager. Added pool utilization
alerting with a wider default lookback window so slow leaks are visible
before exhaustion, not just at the moment of failure.

## Lesson

Some resource exhaustion is only visible if you widen the time window past
the default -- the failure moment is a symptom, not the start of the story.
