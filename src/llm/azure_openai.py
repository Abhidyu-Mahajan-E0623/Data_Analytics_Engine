"""Azure OpenAI Chat/Embeddings client."""

from __future__ import annotations

from dataclasses import dataclass
import json
import logging
import re
from typing import Any

import orjson
import requests
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from src.config.settings import Settings
from src.llm.prompts import build_generation_messages, build_repair_messages
from src.utils.io import sha256_hex


class LLMOutputParseError(RuntimeError):
    """Raised when model output cannot be parsed into required payload."""


@dataclass
class LLMGenerationResult:
    """Normalized generation output."""

    human_text: str
    jsonl_lines: list[str]
    raw_content: str
    prompt_hash: str
    deployment: str


class AzureOpenAIClient:
    """Simple Azure OpenAI client using HTTPS calls."""

    def __init__(self, settings: Settings, logger: logging.Logger | None = None) -> None:
        self._settings = settings
        self._logger = logger
        self._session = requests.Session()

    def generate_hypotheses(
        self,
        domain: str,
        context_bundle: dict[str, Any],
        focus_areas: list[str] | None = None,
        business_constraints: str | None = None,
        table_assignment_plan: dict[str, str] | None = None,
    ) -> LLMGenerationResult:
        """Generate initial hypotheses."""
        messages = build_generation_messages(
            domain=domain,
            context_bundle=context_bundle,
            focus_areas=focus_areas,
            business_constraints=business_constraints,
            table_assignment_plan=table_assignment_plan,
        )
        return self._chat(messages)

    def repair_hypotheses(
        self,
        domain: str,
        context_bundle: dict[str, Any],
        focus_areas: list[str] | None,
        validation_errors: dict[str, list[str]],
        existing_valid_hypotheses: list[dict[str, Any]],
        business_constraints: str | None = None,
        table_assignment_plan: dict[str, str] | None = None,
    ) -> LLMGenerationResult:
        """Repair invalid hypothesis payloads while keeping stable IDs."""
        messages = build_repair_messages(
            domain=domain,
            context_bundle=context_bundle,
            focus_areas=focus_areas,
            validation_errors=validation_errors,
            existing_valid_hypotheses=existing_valid_hypotheses,
            business_constraints=business_constraints,
            table_assignment_plan=table_assignment_plan,
        )
        return self._chat(messages)

    def embed_texts(self, texts: list[str]) -> list[list[float]]:
        """Optional embedding endpoint for future local retrieval indexing."""
        deployment = self._settings.AZURE_OPENAI_EMBED_DEPLOYMENT
        if not deployment:
            raise RuntimeError("AZURE_OPENAI_EMBED_DEPLOYMENT is not configured.")
        url = (
            f"{self._settings.AZURE_OPENAI_ENDPOINT}/openai/deployments/{deployment}/embeddings"
            f"?api-version={self._settings.AZURE_OPENAI_API_VERSION}"
        )
        payload = {"input": texts}
        response = self._post(url, payload)
        data = response.get("data", [])
        return [item["embedding"] for item in data]

    def _chat(self, messages: list[dict[str, str]]) -> LLMGenerationResult:
        prompt_hash = sha256_hex(orjson.dumps(messages).decode("utf-8"))
        deployment = self._settings.AZURE_OPENAI_CHAT_DEPLOYMENT
        url = (
            f"{self._settings.AZURE_OPENAI_ENDPOINT}/openai/deployments/{deployment}/chat/completions"
            f"?api-version={self._settings.AZURE_OPENAI_API_VERSION}"
        )
        payload = {
            "messages": messages,
            "temperature": 0.2,
            "max_tokens": 3600,
            "response_format": {"type": "json_object"},
        }
        response = self._post(url, payload)
        raw_content = _extract_message_content(response)
        try:
            human_text, jsonl_lines = _parse_hypothesis_payload(raw_content)
        except LLMOutputParseError:
            if self._logger:
                self._logger.warning(
                    "Model output parsing failed; retrying with strict reformat request."
                )
            retry_messages = list(messages) + [
                {
                    "role": "user",
                    "content": (
                        "Reformat your previous answer now. Output ONLY valid JSON object with keys "
                        "'human_text' and 'jsonl'. 'jsonl' must be a string with one hypothesis JSON object "
                        "per line. Do not add markdown."
                    ),
                }
            ]
            retry_payload = {
                **payload,
                "messages": retry_messages,
                "temperature": 0,
            }
            retry_response = self._post(url, retry_payload)
            raw_content = _extract_message_content(retry_response)
            human_text, jsonl_lines = _parse_hypothesis_payload(raw_content)

        return LLMGenerationResult(
            human_text=human_text,
            jsonl_lines=jsonl_lines,
            raw_content=raw_content,
            prompt_hash=prompt_hash,
            deployment=deployment,
        )

    @retry(
        retry=retry_if_exception_type(requests.RequestException),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        stop=stop_after_attempt(4),
        reraise=True,
    )
    def _post(self, url: str, payload: dict[str, Any]) -> dict[str, Any]:
        headers = {
            "api-key": self._settings.AZURE_OPENAI_API_KEY,
            "Content-Type": "application/json",
        }
        try:
            response = self._session.post(url, headers=headers, json=payload, timeout=60)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as exc:  # pragma: no cover - network path
            if self._logger:
                self._logger.exception("Azure OpenAI request failed")
            raise exc


def _extract_message_content(response_json: dict[str, Any]) -> str:
    choices = response_json.get("choices", [])
    if not choices:
        raise LLMOutputParseError("Model returned no choices.")
    content = choices[0].get("message", {}).get("content", "")
    if isinstance(content, str):
        return content.strip()
    if isinstance(content, list):
        segments: list[str] = []
        for item in content:
            if isinstance(item, dict) and item.get("type") == "text":
                segments.append(str(item.get("text", "")))
        return "\n".join(segments).strip()
    raise LLMOutputParseError("Unsupported content format from model response.")


def _parse_hypothesis_payload(content: str) -> tuple[str, list[str]]:
    human_text = ""
    jsonl_lines: list[str] = []

    payload = _try_parse_json(content)
    if payload is None:
        extracted = _extract_json_from_fence(content)
        payload = _try_parse_json(extracted) if extracted else None

    if isinstance(payload, dict):
        human_text = str(payload.get("human_text", "")).strip()
        jsonl_lines = _extract_jsonl_lines(payload)
        if not jsonl_lines and _looks_like_hypothesis(payload):
            jsonl_lines = [orjson.dumps(payload).decode("utf-8")]
        if not jsonl_lines:
            jsonl_lines = _objects_to_jsonl(_collect_hypothesis_dicts(payload))
    elif isinstance(payload, list):
        jsonl_lines = _objects_to_jsonl(payload)
        if not jsonl_lines:
            jsonl_lines = _objects_to_jsonl(_collect_hypothesis_dicts(payload))

    if not jsonl_lines:
        streamed_values = _extract_json_values_from_text(content)
        if streamed_values:
            jsonl_lines = _objects_to_jsonl(streamed_values)
            if not jsonl_lines:
                jsonl_lines = _objects_to_jsonl(_collect_hypothesis_dicts(streamed_values))

    if not jsonl_lines:
        raise LLMOutputParseError("Could not parse JSONL hypotheses from model output.")

    return human_text, jsonl_lines


def _try_parse_json(raw: str | None) -> Any | None:
    if raw is None:
        return None
    raw = raw.strip()
    if not raw:
        return None
    try:
        return orjson.loads(raw)
    except orjson.JSONDecodeError:
        return None


def _extract_json_from_fence(raw: str) -> str | None:
    match = re.search(
        r"```json\s*([\s\S]*?)\s*```",
        raw,
        flags=re.IGNORECASE,
    )
    if match:
        return match.group(1)
    match = re.search(r"(\{[\s\S]*\}|\[[\s\S]*\])", raw, flags=re.DOTALL)
    if match:
        return match.group(1)
    return None


def _extract_jsonl_lines(payload: dict[str, Any]) -> list[str]:
    if "jsonl" in payload and isinstance(payload["jsonl"], str):
        jsonl_str = payload["jsonl"].strip()
        parsed = _try_parse_json(jsonl_str)
        if isinstance(parsed, list):
            return _objects_to_jsonl(parsed)
        if isinstance(parsed, dict) and "hypotheses" in parsed:
            return _objects_to_jsonl(parsed.get("hypotheses", []))
        return _extract_json_objects_by_line(jsonl_str)
    if "jsonl_lines" in payload and isinstance(payload["jsonl_lines"], list):
        return [str(line).strip() for line in payload["jsonl_lines"] if str(line).strip()]
    if "hypotheses" in payload and isinstance(payload["hypotheses"], list):
        return [orjson.dumps(item).decode("utf-8") for item in payload["hypotheses"]]
    for key in ("items", "data", "results"):
        value = payload.get(key)
        if isinstance(value, list):
            rows = _objects_to_jsonl(value)
            if rows:
                return rows
    return []


def _extract_json_objects_by_line(raw: str) -> list[str]:
    lines: list[str] = []
    for line in raw.splitlines():
        stripped = line.strip()
        if stripped.startswith("{") and stripped.endswith("}"):
            if _try_parse_json(stripped) is not None:
                lines.append(stripped)
    return lines


def _extract_json_values_from_text(raw: str) -> list[Any]:
    decoder = json.JSONDecoder()
    values: list[Any] = []
    idx = 0
    length = len(raw)
    while idx < length:
        while idx < length and raw[idx].isspace():
            idx += 1
        if idx >= length:
            break
        try:
            value, next_idx = decoder.raw_decode(raw, idx)
        except json.JSONDecodeError:
            idx += 1
            continue
        values.append(value)
        idx = next_idx
    return values


def _objects_to_jsonl(items: list[Any]) -> list[str]:
    lines: list[str] = []
    for item in items:
        if isinstance(item, dict) and _looks_like_hypothesis(item):
            lines.append(orjson.dumps(item).decode("utf-8"))
    return lines


def _looks_like_hypothesis(item: dict[str, Any]) -> bool:
    required_keys = {"hypothesis_id", "statement", "tables", "required_columns", "threshold"}
    return required_keys.issubset(item.keys())


def _collect_hypothesis_dicts(value: Any) -> list[dict[str, Any]]:
    found: list[dict[str, Any]] = []

    def walk(node: Any) -> None:
        if isinstance(node, dict):
            if _looks_like_hypothesis(node):
                found.append(node)
            for child in node.values():
                walk(child)
            return
        if isinstance(node, list):
            for child in node:
                walk(child)

    walk(value)
    return found
