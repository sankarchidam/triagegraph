"""
The invoke/resume loop that drives a compiled graph through its
interrupt_before pause(s) to completion. One place that knows how
get_state()/update_state()/invoke(None, ...) chain together, used by both
main.py (interactive prompt_human) and scripts/eval_scenarios.py
(non-interactive auto_approve) -- milestone 5 needed a second caller, which
is what pulled this out of main.py rather than duplicating the loop.
"""
from __future__ import annotations

import uuid
import warnings
from typing import Callable

# Cosmetic-only: the MemorySaver checkpointer serializes each state snapshot
# for storage, including the raw LangChain response object generate/rank_
# hypotheses got back from .with_structured_output() -- pydantic-core warns
# that the "parsed" field on that raw response doesn't match its declared
# type (it's our Hypothesis-shaped Pydantic model, which is exactly what we
# asked for). Functionally inert -- state.hypotheses/ranked_hypotheses come
# from that same object either way, confirmed correct across all 3 golden
# scenarios (see scripts/eval_scenarios.py) -- just noisy on every run.
warnings.filterwarnings("ignore", message="Pydantic serializer warnings")

DecideFn = Callable[[dict], dict]


def run_incident(alert_raw: dict, decide: DecideFn, graph) -> dict:
    """Run one incident through `graph` to completion, calling `decide(state)`
    each time execution pauses at human_approval_gate and writing its return
    value back via update_state() before resuming. `graph` is expected to be
    pre-compiled (graph.build_graph.build_graph()) so callers running many
    incidents (the eval harness) can reuse one compiled graph/checkpointer
    across runs -- only the thread_id changes per incident."""
    config = {"configurable": {"thread_id": str(uuid.uuid4())}}
    state = graph.invoke({"alert_raw": alert_raw}, config=config)

    while graph.get_state(config).next:
        graph.update_state(config, decide(state))
        state = graph.invoke(None, config=config)

    return state
