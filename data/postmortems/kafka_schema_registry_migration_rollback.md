# Postmortem: consumer crash loop after schema registry compatibility change

**Date:** 8 months ago
**Severity:** SEV1
**Service:** notifications-consumer

## Summary

A schema-registry compatibility mode change (FULL to BACKWARD) rejected a
producer's new schema version, causing notifications-consumer to
crash-loop on deserialization errors for messages it couldn't parse at
all -- a hard failure, not a slowdown.

## Evidence

- Metrics: consumer pod restarts spiked; consumer lag climbed because pods
  weren't running, not because processing got slower.
- Logs: `SerializationException: schema not registered under compatibility
  mode BACKWARD` on every message.
- Deploys: a schema-registry config change (not a service deploy) 10
  minutes before the alert.

## Root cause

Compatibility mode change was incompatible with an in-flight producer
schema version.

## Resolution

Reverted compatibility mode. Added a schema-registry change to the
pre-deploy checklist for any service consuming from the affected topics.

## Lesson

Not every deploy-shaped trigger is an application deploy -- config changes
to shared infrastructure (schema registry, feature flags) correlate with
incidents the same way code deploys do, and are easy to miss if you only
query the GitHub deploy history.
