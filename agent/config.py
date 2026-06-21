"""Provider-agnostic LLM configuration.

All values come from environment variables so the *same code* runs against a
local Ollama model during development and Qwen Cloud (DashScope) for the final
submission. Because both expose an OpenAI-compatible API, switching providers is
just three env vars: base_url / api_key / model.

Local dev (defaults):
    PM_LLM_BASE_URL = http://localhost:11434/v1
    PM_LLM_API_KEY  = ollama
    PM_LLM_MODEL    = qwen2.5:3b

Production (Qwen Cloud):
    PM_LLM_BASE_URL = https://dashscope-intl.aliyuncs.com/compatible-mode/v1
    PM_LLM_API_KEY  = <your DashScope key>
    PM_LLM_MODEL    = qwen-max
"""

from __future__ import annotations

import os
from dataclasses import dataclass

try:  # optional convenience: load a local .env if present
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:  # pragma: no cover - dotenv is optional
    pass


_DEFAULTS = {
    "base_url": "http://localhost:11434/v1",
    "api_key": "ollama",
    "model": "qwen2.5:3b",
}


@dataclass(frozen=True)
class LLMSettings:
    base_url: str
    api_key: str
    model: str

    @property
    def is_local(self) -> bool:
        return "localhost" in self.base_url or "127.0.0.1" in self.base_url

    def masked_key(self) -> str:
        if not self.api_key or self.api_key == "ollama":
            return self.api_key or "(none)"
        if len(self.api_key) <= 12:
            return "***"
        return f"{self.api_key[:6]}…{self.api_key[-4:]}"


def get_llm_settings() -> LLMSettings:
    return LLMSettings(
        base_url=os.environ.get("PM_LLM_BASE_URL", _DEFAULTS["base_url"]),
        api_key=os.environ.get("PM_LLM_API_KEY", _DEFAULTS["api_key"]),
        model=os.environ.get("PM_LLM_MODEL", _DEFAULTS["model"]),
    )
