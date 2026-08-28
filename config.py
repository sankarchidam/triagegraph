"""
Central config.

dummy_mode is aspirational, not yet wired up: clients/base.py defines
abstract interfaces (MetricsClient, LogsClient, DeployClient,
PostmortemStore) specifically so a real Prometheus/Splunk/GitHub client
could be swapped in as a constructor change later, but graph/nodes.py
currently imports the Dummy* implementations directly and no real
implementation exists yet -- flipping this flag today does nothing. See
the README's "Known limitations" section.

The LLM provider swap is real, not aspirational: nodes call
graph.llm.get_chat_model(model_name), never a provider's SDK directly.
llm_provider picks OpenAI vs Anthropic; only config.py and graph/llm.py
know which one is actually in play.
"""
import os
from typing import Literal

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    dummy_mode: bool = True
    active_scenario: str = "kafka_consumer_lag_deploy"

    llm_provider: Literal["openai", "anthropic"] = "openai"
    openai_api_key: str = ""
    anthropic_api_key: str = ""
    langsmith_api_key: str = ""
    langsmith_project: str = "triagegraph"

    # Model split: generate_hypotheses does real reasoning over ambiguous
    # evidence: give it the strongest model. rank_hypotheses is closer to
    # mechanical cross-checking against evidence already on the table --
    # this split makes the design doc's own open question (§13: is a
    # cheaper/faster model good enough for ranking?) directly measurable
    # rather than hypothetical -- swap ranking_model and compare.
    reasoning_model: str = "gpt-4o"
    ranking_model: str = "gpt-4o-mini"


settings = Settings()

# LangSmith tracing (milestone 5) is entirely optional, same as everything
# else gated behind an empty-string default: leave LANGSMITH_API_KEY unset
# in .env and this is a no-op, same as milestones 1-2 needing no API key at
# all. langsmith's own tracing_is_enabled() checks LANGSMITH_TRACING (or
# the legacy LANGCHAIN_TRACING_V2) directly from os.environ -- there's no
# constructor arg to pass this through via graph/llm.py, so this is the one
# place config.py reaches into os.environ instead of just being read from.
if settings.langsmith_api_key:
    os.environ.setdefault("LANGSMITH_TRACING", "true")
    os.environ.setdefault("LANGSMITH_API_KEY", settings.langsmith_api_key)
    os.environ.setdefault("LANGSMITH_PROJECT", settings.langsmith_project)
