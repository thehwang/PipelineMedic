"""Thin, provider-agnostic LLM client over the OpenAI-compatible API.

Works unchanged against local Ollama or Qwen Cloud (DashScope) — see config.py.
"""

from __future__ import annotations

from typing import Any, Optional

from openai import OpenAI

from .config import LLMSettings, get_llm_settings

Message = dict[str, Any]


class LLMClient:
    def __init__(self, settings: Optional[LLMSettings] = None):
        self.settings = settings or get_llm_settings()
        self._client = OpenAI(
            base_url=self.settings.base_url,
            api_key=self.settings.api_key,
        )

    @property
    def model(self) -> str:
        return self.settings.model

    def chat(
        self,
        messages: list[Message],
        *,
        temperature: float = 0.2,
        **kwargs: Any,
    ) -> str:
        """Return the assistant text for a simple chat turn."""
        resp = self._client.chat.completions.create(
            model=self.model,
            messages=messages,
            temperature=temperature,
            **kwargs,
        )
        return resp.choices[0].message.content or ""

    def chat_raw(
        self,
        messages: list[Message],
        *,
        tools: Optional[list[dict[str, Any]]] = None,
        temperature: float = 0.2,
        **kwargs: Any,
    ) -> Any:
        """Return the full response object (for tool-calling / multi-step loops)."""
        params: dict[str, Any] = {
            "model": self.model,
            "messages": messages,
            "temperature": temperature,
            **kwargs,
        }
        if tools:
            params["tools"] = tools
        return self._client.chat.completions.create(**params)
