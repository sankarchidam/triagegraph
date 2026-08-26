# TriageGraph

An agentic incident-triage copilot built on LangGraph: give it an alert, it
reasons over metrics, logs, recent deploys, and past postmortems, and
produces a ranked root-cause hypothesis tree — with a human approval gate
before anything is treated as "actioned." Runs entirely on localhost against
scenario-driven dummy data, with abstract client interfaces designed so
swapping in real Prometheus/Splunk/Dynatrace/GitHub credentials later is a
constructor change, not a rewrite.

This is **v1** — the design doc (linked internally) was reviewed and
adjusted before building; see "Design decisions" below for what changed and
why.

## Status: Milestone 3 (reasoning nodes) — done, all 3 golden scenarios correct at top-1

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env   # fill in OPENAI_API_KEY (or ANTHROPIC_API_KEY + LLM_PROVIDER=anthropic)
python main.py --scenario kafka_consumer_lag_deploy
python main.py --scenario downstream_dependency_outage
python main.py --scenario resource_exhaustion_slow_leak
```

Milestones 1-2 still need **zero API key** (every node through
`assess_evidence`/`widen_time_window` is rule-based). Milestone 3 adds the
project's first real LLM calls — `generate_hypotheses` (`gpt-4o` by
default) and `rank_hypotheses` (`gpt-4o-mini`) — via `graph/llm.py`, a
provider-agnostic factory so `LLM_PROVIDER=openai|anthropic` in `.env` is a
config change, not a code change.

**Milestone 2 results** (still hold, unaffected by Milestone 3):
- **kafka_consumer_lag_deploy**: metrics correctly isolate `kafka_consumer_lag`
  as the only anomaly; deploy evidence surfaces the actual root-cause PR;
  postmortem search finds the closest thematic match (schema-registry, 0.39)
  without a fabricated "exact match."
- **downstream_dependency_outage**: no deploy evidence (correctly empty);
  postmortem search finds the real match at 0.61 similarity, clearly
  separated from distractors at 0.41/0.32.
- **resource_exhaustion_slow_leak**: the widen loop actually fires. The
  default 30-minute window shows memory as a flat plateau (a 30-minute
  slice near the end of a 6-hour linear climb has a peak/start ratio of
  ~1.06 — under the anomaly threshold by construction), so
  `evidence_sufficient` comes back `False`, the graph widens to 8 hours,
  re-fetches, and *then* the same metric shows `climbed from 1000 to 2800`.
  Confirmed via the report's `(widened)` tag and a clean, single loop
  (`time_window_widened` guards against ever widening twice).

**Milestone 3: the prompt-tuning story the design doc anticipated (§11:
"get scenario 1 to a reliably correct top-1 hypothesis before moving to
scenario 2/3").** The first real run of `generate_hypotheses`/
`rank_hypotheses` against scenario 1 got it wrong: it ranked "schema-registry
timeouts are happening" (a symptom) at confidence 0.80, *above* "the deploy
caused the timeouts" (the actual root cause) at 0.50 — treating the deploy
and its own downstream symptom as two independent, competing hypotheses
instead of one causal chain. Fixed by adding explicit root-cause-vs-symptom
guidance to both prompts: a deploy whose diff explains a symptom elsewhere
in the evidence should absorb that symptom as *supporting* evidence, not
compete with it. Re-verified: all three scenarios now correctly rank the
true root cause first (0.80–0.90 confidence), including scenario 2's
downstream-dependency case and scenario 3's memory leak.

One thing this surfaced that's still open, not fixed: scenario 3 also
generated a hypothesis blaming "a recent Kafka schema registry change" with
*zero* supporting evidence cited — nothing in that scenario's data mentions
Kafka at all. It's correctly ranked last (0.20), so it doesn't corrupt the
top-1 result, but a hypothesis with no cited evidence shouldn't have been
generated per the prompt's own instructions ("do not assume a cause not
supported by the evidence provided"). Worth watching if it recurs.

`ranking_model` = `gpt-4o-mini` held up fine across all three scenarios —
a first real answer to the design doc's own open question (§13: is a
cheaper/faster model good enough for the mechanical cross-checking pass?).

## Design decisions (v1 review)

The original design doc was strong going in — it already anticipated its
own failure modes (recency bias in ranking, over-confident hypotheses,
scenario-driven not random dummy data). A few things were pushed back on
and resolved before building:

- **LangGraph over CrewAI** — confirmed, not changed. This is a DAG with a
  fixed, known topology decided up front, not a dynamic-delegation problem.
  CrewAI earns its keep when *who does what* is emergent; here every edge
  is already specified.
- **The `widen_time_window` conditional edge is real from Milestone 2**,
  not deferred as a stretch goal (see the resource_exhaustion_slow_leak
  result above) — the original doc's milestone 4 required all three golden
  scenarios passing, but scenario 3 specifically needs that loop-back edge.
  Deferring it would have meant retrofitting a loop into a graph built
  linear, which is real rework compared to building it in from the start.
- **`normalize_alert` and `fetch_metrics` are rule-based, not LLM calls.**
  The dummy alert payload is already structured, and metrics are numeric
  time series — both are jobs a threshold rule does deterministically,
  cheaply, and testably. The LLM is reserved for genuinely unstructured
  text (`fetch_logs`'s raw messages are surfaced as-is for now, still no
  LLM needed there either) and actual reasoning over ambiguous evidence
  (`generate_hypotheses` / `rank_hypotheses`, milestone 3). Nice side
  effect: Milestones 1 and 2 both need no API key at all.
- **`assess_evidence` (milestone 2) is a deliberately narrow, metrics-only
  proxy for "is there enough here to reason about."** It is not trying to
  replicate real hypothesis-quality judgment — that's milestone 3's job.
  Checking only whether *metrics* show something notable is what makes the
  widen trigger exactly testable against scenario 3, without needing an
  LLM call to get there.
- **CLI-only entrypoint for v1.** A FastAPI POST trigger needs a
  checkpointer plus a separate resume-from-interrupt endpoint to work with
  `human_approval_gate` — real added plumbing the original doc didn't
  spell out. Given the explicit scope (single session, no multi-tenant),
  CLI's blocking `input()` is the right amount of complexity for now.
- **Confidence scores need a rubric, not a bare float.** LLM-emitted 0–1
  confidence numbers are notoriously uncalibrated. `rank_hypotheses`
  anchors confidence to explicit criteria (§ RANK_HYPOTHESES_SYSTEM_PROMPT
  in `graph/nodes.py`) instead of asking the model to just emit a number.
- **LLM provider is a config toggle, not a hardcoded import.** The design
  doc specified Claude; the actual key available when Milestone 3 got built
  was OpenAI's. `graph/llm.py` is the only place that imports a provider
  SDK — nodes call `get_chat_model(model_name)` and never know which
  vendor they're talking to, same swap-without-touching-node-logic
  philosophy as the dummy clients. `LLM_PROVIDER=openai|anthropic` in
  `.env` is the whole switch.
- **Named TriageGraph**, not "Ops Genie" — that's Atlassian's actual
  commercial product (Opsgenie); no reason to collide even for a local
  project.

## Project layout

```
clients/
  base.py                    abstract interfaces (MetricsClient, LogsClient, DeployClient, PostmortemStore)
  scenario_loader.py          loads a synthetic_incidents/*.json, resolves relative offsets to absolute timestamps
  prometheus_dummy.py          scenario-driven dummy Prometheus client
  splunk_dynatrace_dummy.py     scenario-driven dummy logs/traces client
  github_dummy.py                scenario-driven dummy deploy client
  postmortem_store.py            Chroma + sentence-transformers, real from day one (it's local anyway)
data/
  synthetic_incidents/            3 golden scenarios -- see design doc §7
  postmortems/                     4 postmortems: 1 real match (scenario 2) + 3 distractors, shared across all scenarios
graph/
  state.py                         IncidentState, Evidence, Hypothesis
  nodes.py                          node functions, including generate_hypotheses/rank_hypotheses prompts
  llm.py                             provider-agnostic chat model factory (OpenAI/Anthropic)
  build_graph.py                    StateGraph wiring: fan-out/fan-in + the widen_time_window conditional loop
main.py                            CLI entrypoint
config.py                          Settings (dummy_mode, llm_provider, API keys, active_scenario)
.chroma/                            Chroma's on-disk index (gitignored, rebuilt automatically if missing/stale)
```

## Scenario data is scenario-driven, not random

Each scenario JSON defines its story as compact patterns (baseline +
anomaly window + peak for metrics; offset-from-alert for log lines and
deploys) rather than hand-written time-series points, with all timestamps
**relative** ("20 minutes before the alert") and resolved to absolute
timestamps at run time, anchored to the alert. Anchoring to relative time
rather than baking absolute timestamps into the JSON is deliberate — a
golden scenario reused for regression testing should look equally fresh
whenever you run it.

The postmortem corpus is shared across all three scenarios rather than
reseeded per-scenario (the design doc's own recommendation, §13) — a store
with only the one relevant doc in it wouldn't actually test retrieval
quality, just whether Chroma returns anything at all. Confirmed doing real
work: scenario 2's true match retrieves at 0.61 similarity, clearly
separated from unrelated distractors at 0.41 and 0.32.

## Roadmap

- **Milestone 4** — human_approval_gate via `interrupt_before`; the CLI
  prompt to accept / pick a different hypothesis / reject and re-investigate.
- **Milestone 5** — LangSmith tracing + a script that runs all golden
  scenarios and reports the accuracy table from design doc §9.
- **Milestone 6** — postmortem reranking polish; investigate the scenario 3
  zero-evidence hallucination noted above.
