# Postmortem: origin overload from CDN cache invalidation storm

**Date:** 7 months ago
**Severity:** SEV2
**Service:** web-frontend / cdn-edge

## Summary

A bulk cache-purge script invalidated nearly all cached assets at once,
sending a traffic spike directly to origin servers that were sized for
cache-hit-dominated load. Origin CPU and latency spiked for 15 minutes
until the cache repopulated.

## Evidence

- Metrics: origin request rate jumped ~40x in under a minute; CPU followed.
- Logs: cache-purge job completion log immediately preceding the spike.
- Deploys: a deploy earlier that day included the purge script, but the
  purge itself (not the deploy) was the proximate trigger.

## Root cause

An overly broad cache invalidation pattern (`purge *`) instead of a scoped
purge for the changed assets.

## Resolution

Scoped the purge script to only the changed asset paths. Added an origin
shield layer to absorb invalidation-storm traffic in the future.

## Lesson

A deploy can be several steps removed from the actual trigger -- here the
deploy shipped a script, and running that script was the real event to
correlate against, not the deploy timestamp itself.
