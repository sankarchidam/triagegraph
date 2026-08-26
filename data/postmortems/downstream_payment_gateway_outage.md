# Postmortem: Elevated checkout-service errors due to payment-gateway outage

**Date:** 3 months ago
**Severity:** SEV2
**Service:** checkout-service

## Summary

checkout-service latency and error rate spiked for 40 minutes. No deploy had
gone out to checkout-service in the preceding 24 hours. Root cause was an
outage in the downstream payment-gateway dependency: its health check began
failing, and checkout-service's client library retried aggressively instead
of failing fast, compounding the latency.

## Evidence

- Metrics: checkout-service p99 latency and 5xx rate climbed together,
  correlated in time.
- Logs: repeated `payment-gateway health check failed: connection timeout`
  entries starting at the same time as the metric anomaly.
- Deploys: none to checkout-service in the relevant window.

## Root cause

payment-gateway (a downstream dependency, not checkout-service itself)
became unavailable. checkout-service's retry policy amplified the impact
instead of shedding load.

## Resolution

Payment-gateway's on-call team resolved the outage on their end. Follow-up:
added circuit breaker to checkout-service's payment-gateway client so a
downstream outage degrades gracefully instead of retry-storming.

## Lesson

When metrics show correlated latency/error spikes with **no matching
deploy**, check downstream dependency health before assuming the alerting
service caused its own incident.
