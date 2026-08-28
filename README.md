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

## Status: Milestone 6 (postmortem filtering + hallucination fixes) — done

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env   # fill in OPENAI_API_KEY (or ANTHROPIC_API_KEY + LLM_PROVIDER=anthropic)
python main.py --scenario kafka_consumer_lag_deploy
python main.py --scenario downstream_dependency_outage
python main.py --scenario resource_exhaustion_slow_leak

# non-interactive (accepts top hypothesis every time -- for scripted/regression runs)
python main.py --scenario kafka_consumer_lag_deploy --auto-approve

# eval harness -- runs every golden scenario and prints the accuracy table
python -m scripts.eval_scenarios
```

**Milestone 4: `human_approval_gate`.** The graph now pauses before showing
ranked hypotheses to a human, via LangGraph's `interrupt_before` + a
`MemorySaver` checkpointer (`graph/build_graph.py`). `human_approval_gate`
itself is a deliberate no-op — its only job is to be the pause point;
`main.py` reads the paused state with `graph.get_state()`, prompts
interactively, writes the decision back with `graph.update_state()`, and
resumes with `graph.invoke(None, config)`. Three outcomes:

- **Accept** — top-ranked hypothesis approved as-is.
- **Pick a different hypothesis** — reviewer overrides the model's ranking;
  the report records both the model's top pick and the human's actual
  choice, without silently reordering `ranked_hypotheses` (an honesty
  choice: the report should show what the model ranked *and* what the
  human decided, not blur the two).
- **Reject, with feedback** — routes back to `generate_hypotheses` for one
  feedback-driven re-investigation pass (the human's text is folded
  directly into the prompt), then returns to the gate. A second reject in
  a row terminates instead of looping again — `reinvestigated` guards it,
  same one-shot-loop pattern as `time_window_widened`. Verified: gate fires
  exactly twice on a single reject, exactly twice (not three times) on a
  double reject, with the second producing a clean "escalate to a human
  on-call" report instead of hanging.

**Milestone 5: tracing + eval harness.**

- **LangSmith tracing** is a config toggle, same philosophy as everything
  else in this project: `config.py` sets `LANGSMITH_TRACING`/
  `LANGSMITH_API_KEY`/`LANGSMITH_PROJECT` in `os.environ` iff
  `LANGSMITH_API_KEY` is non-empty in `.env` -- LangChain's chat models
  pick up tracing automatically from there, no code change in `graph/llm.py`
  or the nodes. Leave the key blank and it's a no-op, same as every other
  optional integration here. **Verified against a real LangSmith project**:
  a `kafka_consumer_lag_deploy` run's full node tree (`normalize_alert` ->
  fan-out -> ... -> `human_approval_gate` -> `finalize_report`) showed up
  in the `triagegraph` project, confirmed by querying the LangSmith API
  directly (`client.list_runs(project_name="triagegraph")`), not just "no
  error was thrown."
- **`scripts/eval_scenarios.py`** runs every golden scenario with
  `--auto-approve` (via the same `graph/runner.run_incident()` main.py
  uses -- pulled into a shared module specifically because milestone 5
  needed a second, non-interactive caller) and prints an accuracy table:
  top-1 hypothesis id/confidence, whether it's correct, whether the time
  window widened exactly when the scenario expects it to (not just "at
  least"), and postmortem-retrieval accuracy where a scenario has one.
  Grading is deliberately **not** LLM-as-judge -- each scenario JSON now
  carries an `eval_keywords` fixture (attached by me, the scenario author,
  before ever seeing what the model would say), and a top-1 hypothesis
  counts as correct if its description contains any of them. Simple,
  deterministic, and auditable straight from the printed table -- grading
  the model with another instance of the same kind of model felt like the
  wrong tradeoff for a 3-scenario regression check.
- **Current result: 3/3 top-1 accuracy**, unchanged from the manual
  verification in milestone 3 -- this just makes it a repeatable one-line
  command instead of three manual reads of a markdown report.

```
scenario                        root cause             top1  conf   correct  window  postmortem
--------------------------------------------------------------------------------------------------
downstream_dependency_outage    downstream_dependency  h1    0.90   YES      ok      OK (downstream_payment_gateway_outage)
kafka_consumer_lag_deploy       deploy                 h1    0.90   YES      ok      n/a
resource_exhaustion_slow_leak   resource_exhaustion    h1    0.80   YES      ok      n/a
--------------------------------------------------------------------------------------------------
Top-1 accuracy: 3/3
```

**Milestone 6: the scenario 3 investigation the roadmap called for turned up three real bugs, not one.**
Started from the known issue: scenario 3's hallucinated Kafka hypothesis. Root cause traced to
`_all_evidence()` (the helper that builds the LLM prompt) never filtering by `is_notable` at all --
a below-threshold postmortem hit (0.22 similarity, well under the 0.3 "notable" cutoff) was shown to
the model with no signal distinguishing it from a real match, and it got cited as supporting evidence
for a hypothesis about a service that has nothing to do with Kafka. **Fixed:** `_all_evidence()` now
filters `postmortem_evidence` to `is_notable` only, while `finalize_report` still shows every hit
(the report stays a full audit trail; only the LLM's input got cleaner). Metrics and logs are
deliberately *not* filtered the same way -- a flat metric or an INFO log carries real negative-evidence
value ("error rate stayed flat" rules something out); a low-similarity vector-search hit doesn't carry
an equivalent signal, it's just corpus noise.

Fixing that surfaced a second, more serious hallucination that had been hiding in plain sight: with the
Kafka distractor gone, scenario 3's *top-ranked* hypothesis (0.80 confidence) started confidently
claiming "a recent deploy introduced a memory leak" -- despite `deploy_evidence` being empty for that
scenario. Traced to the milestone 3 prompt fix itself: telling the model "a deploy is usually the
highest-signal evidence" as an unconditional prior, with no explicit carve-out for "and if there's no
deploy evidence at all, don't invent one." **Fixed:** reworded both `generate_hypotheses` and
`rank_hypotheses`'s prompts to make the deploy-prior conditional on a deploy actually being present in
evidence, and added an explicit rule that "deploys are usually the cause" is not permission to assume
an unlogged one happened. Also caught that the eval harness's own keyword grading missed this --
"memory" and "oom" are so generic (the alert itself says "OOM killed") that a wrong hypothesis passed
grading anyway. Tightened `resource_exhaustion_slow_leak`'s `eval_keywords` to require an actual
leak-shaped word (`leak`, `gradual`, `steadily accumul`, `growing over`), not just any word that
happens to co-occur with the alert.

With both hallucinations gone, a third, subtler problem was still visible under the new stricter
grading: scenario 3's top hypothesis would sometimes land on a phrase like "excessive memory usage...
leading to an OOM kill" -- technically not wrong, but circular (it says nothing a human doesn't already
know from the alert itself). Root cause: `fetch_metrics`'s evidence summaries never stated the window's
*duration*, only the before/after values ("climbed from 1000 to 2800 within the window") -- so the
model had no way to know a climb was gradual (leak-shaped) rather than sudden (spike-shaped), which is
exactly the distinction that matters here since scenario 3 specifically widens the window to 8 hours to
make a slow leak visible at all. **Fixed:** metric summaries now state the window span
("...over the 8.1-hour window"), and the prompt explicitly asks the model to read window duration as a
signal about failure mode. Verified across 5 fresh runs post-fix: 5/5 correctly identified "a memory
leak... climbing steadily over 8.1 hours" as the top hypothesis, versus roughly half vague/wrong before.

**Full regression, post-fix:** 3/3 top-1 accuracy across 3 separate full eval-harness runs (9/9 total),
under the tightened grading criteria -- stronger evidence of stability than the single run milestone 3
originally reported on.

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

One thing this surfaced that was left open at the time: scenario 3 also
generated a hypothesis blaming "a recent Kafka schema registry change" with
*zero* supporting evidence cited — nothing in that scenario's data mentions
Kafka at all. Correctly ranked last (0.20) so it didn't corrupt the top-1
result, but worth investigating properly rather than dismissing as noise —
see Milestone 6 below, which traced and fixed the actual root cause (and
found two more hallucinations hiding behind it).

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
  build_graph.py                    StateGraph wiring: fan-out/fan-in, widen_time_window loop, human_approval_gate
                                     (interrupt_before + MemorySaver checkpointer)
  runner.py                         shared invoke/get_state/update_state/resume loop (main.py + eval_scenarios.py)
scripts/
  eval_scenarios.py                 milestone 5 eval harness -- runs all golden scenarios, prints accuracy table
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

All 6 originally-planned milestones are done. Ideas for what's next, not yet committed to:

- **Real client swap** — flip `dummy_mode` and wire real Prometheus/Splunk/Dynatrace/GitHub credentials
  into the existing abstract client interfaces (`clients/base.py`). The whole point of that abstraction
  was to make this a constructor change, not a rewrite -- worth actually proving that out.
- **A second, independent eval signal** — `eval_keywords` grading is honest about what it can't catch
  (it caught the "workload spike" false-correct case only after manual tightening, by hand, after
  noticing it in a sampled run -- it wouldn't have caught a *new* wrong-but-keyword-matching phrasing).
  An LLM-as-judge pass, used as a second, disagreeing-from-the-generator signal rather than a
  replacement for keyword grading, could catch what keywords structurally can't.
- **FastAPI trigger** — deferred in the original v1 review pending a checkpointer + resume-from-interrupt
  endpoint; milestone 4 built exactly that machinery for the CLI, so the remaining gap is just the
  HTTP layer now, not the hard part.
