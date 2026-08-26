"""
Central config. Every client constructor checks settings.dummy_mode and
returns the dummy or real implementation -- nodes never know which one
they're talking to. See clients/base.py for the abstract interfaces that
make that swap possible later (§12 of the design doc: v1).

Same swap-without-touching-node-logic philosophy applies to the LLM
provider: nodes call graph.llm.get_chat_model(model_name), never a
provider's SDK directly. llm_provider picks OpenAI vs Anthropic; only
config.py and graph/llm.py know which one is actually in play.
"""
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

    # Model split: generate_hypotheses does real reasoning over ambiguous
    # evidence: give it the strongest model. rank_hypotheses is closer to
    # mechanical cross-checking against evidence already on the table --
    # this split makes the design doc's own open question (§13: is a
    # cheaper/faster model good enough for ranking?) directly measurable
    # rather than hypothetical -- swap ranking_model and compare.
    reasoning_model: str = "gpt-4o"
    ranking_model: str = "gpt-4o-mini"


settings = Settings()
