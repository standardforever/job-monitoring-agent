from __future__ import annotations

import asyncio
import json
import os
from dataclasses import dataclass, field
from typing import Any

from openai import OpenAI

from core.config import load_environment
from utils.logging import get_logger, log_event

load_environment()
logger = get_logger("openai_service")

@dataclass(slots=True)
class AnalysisResult:

    success: bool
    response: dict = field(default_factory=dict)
    token_usage: dict = field(default_factory=dict)
    error: str = ""


class OpenAIAnalysisService:
    def __init__(self, model: str | None = None, api_key: str | None = None) -> None:
        self._model = model or os.getenv("OPENAI_MODEL", "gpt-5-nano")
        self._api_key = api_key or os.getenv("OPENAI_API_KEY")
        if not self._api_key:
            raise ValueError("OPENAI_API_KEY is not configured")
        self._client = OpenAI(api_key=self._api_key)
        log_event(
            logger,
            "info",
            "openai_analysis_service_initialized model=%s",
            self._model,
            domain="openai",
            model=self._model,
        )

    async def analyze_data(
        self,
        prompt: str,
        json_response: bool = True,
    ) -> AnalysisResult:
        log_event(
            logger,
            "info",
            "openai_analysis_started model=%s json_response=%s",
            self._model,
            json_response,
            domain="openai",
            model=self._model,
            json_response=json_response,
            prompt_length=len(prompt),
        )
        return await asyncio.to_thread(self._analyze_sync, prompt, json_response)

    def _analyze_sync(self, prompt: str, json_response: bool) -> AnalysisResult:
        error = ""
        for attempt in range(1, 3):
            try:
                response = self._client.responses.create(
                    model=self._model,
                    input=prompt,
                )
                output: Any = response.output_text

                if json_response:
                    output = json.loads(output)
                
                token_usage = {}
                if response.usage:
                    token_usage = {
                        "input_token": response.usage.input_tokens,
                        "output_token": response.usage.output_tokens,
                        "total_token": response.usage.total_tokens
                    }
                log_event(
                    logger,
                    "info",
                    "openai_analysis_completed model=%s attempt=%s",
                    self._model,
                    attempt,
                    domain="openai",
                    model=self._model,
                    attempt=attempt,
                    token_usage=token_usage,
                )
                return AnalysisResult(
                    response=output if isinstance(output, dict) else {},
                    success=True,
                    token_usage=token_usage,
                    
                )
                
    
            except Exception as exc:
                error = str(exc)
                log_event(
                    logger,
                    "warning",
                    "openai_analysis_attempt_failed model=%s attempt=%s error=%s",
                    self._model,
                    attempt,
                    error,
                    domain="openai",
                    model=self._model,
                    attempt=attempt,
                    error=error,
                )
                continue

        log_event(
            logger,
            "error",
            "openai_analysis_failed model=%s error=%s",
            self._model,
            error,
            domain="openai",
            model=self._model,
            error=error,
        )
        return AnalysisResult(
            response={},
            success=False,
            error=error,
        )
