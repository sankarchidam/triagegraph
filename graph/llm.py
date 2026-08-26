"""
The one place that knows which LLM provider is actually in play. Nodes call
get_chat_model(model_name) and use LangChain's standard
.with_structured_output() interface -- never a provider SDK directly --
so switching settings.llm_provider is a config change, not a nodes.py
rewrite. Same philosophy as clients/base.py's dummy/real client swap.
"""
from __future__ import annotations

from functools import lru_cache

from config import settings


@lru_cache(maxsize=None)
def get_chat_model(model_name: str):
    """Cached per model name -- generate_hypotheses and rank_hypotheses can
    each call this every node invocation without re-constructing a client."""
    if settings.llm_provider == "openai":
        from langchain_openai import ChatOpenAI
        if not settings.openai_api_key:
            raise RuntimeError(
                "llm_provider is 'openai' but OPENAI_API_KEY isn't set. "
                "Copy .env.example to .env and fill it in."
            )
        return ChatOpenAI(model=model_name, api_key=settings.openai_api_key, temperature=0)

    if settings.llm_provider == "anthropic":
        from langchain_anthropic import ChatAnthropic
        if not settings.anthropic_api_key:
            raise RuntimeError(
                "llm_provider is 'anthropic' but ANTHROPIC_API_KEY isn't set. "
                "Copy .env.example to .env and fill it in."
            )
        return ChatAnthropic(model=model_name, api_key=settings.anthropic_api_key, temperature=0)

    raise ValueError(f"Unknown llm_provider: {settings.llm_provider!r}")
