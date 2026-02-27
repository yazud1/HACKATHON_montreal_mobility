"""
llm_client.py
Client LLM léger (Gemini/Anthropic/OpenAI) via API key.
Fallback propre: si aucune clé n'est configurée, le client reste désactivé.
"""

from __future__ import annotations

import json
import os
import time
from pathlib import Path
from urllib.parse import quote_plus
from urllib import error, request


class LLMClient:
    def __init__(self):
        self._load_dotenv()
        self.provider = (os.getenv("LLM_PROVIDER") or "").strip().lower()
        self.gemini_key = (os.getenv("GEMINI_API_KEY") or "").strip()
        self.anthropic_key = (os.getenv("ANTHROPIC_API_KEY") or "").strip()
        self.openai_key = (os.getenv("OPENAI_API_KEY") or "").strip()

        if not self.provider:
            if self.gemini_key:
                self.provider = "gemini"
            elif self.anthropic_key:
                self.provider = "anthropic"
            elif self.openai_key:
                self.provider = "openai"
            else:
                self.provider = "none"

        if self.provider == "gemini":
            default_model = "gemini-2.5-flash-lite"
        elif self.provider == "anthropic":
            default_model = "claude-3-5-sonnet-latest"
        else:
            default_model = "gpt-4o-mini"
        self.model = (os.getenv("LLM_MODEL") or default_model).strip()
        self.timeout_sec = int((os.getenv("LLM_TIMEOUT_SEC") or "12").strip())
        self.last_error: str | None = None

    @property
    def enabled(self) -> bool:
        if self.provider == "gemini":
            return bool(self.gemini_key)
        if self.provider == "anthropic":
            return bool(self.anthropic_key)
        if self.provider == "openai":
            return bool(self.openai_key)
        return False

    @property
    def provider_label(self) -> str:
        if self.provider == "gemini":
            return "Gemini API"
        if self.provider == "anthropic":
            return "Claude API"
        if self.provider == "openai":
            return "OpenAI API"
        return "désactivé"

    def status_line(self) -> str:
        if self.enabled:
            return f"LLM · {self.provider_label} ({self.model})"
        return "LLM · Désactivé (clé API absente)"

    def generate(
        self,
        system_prompt: str,
        user_prompt: str,
        max_tokens: int = 320,
        temperature: float = 0.2,
    ) -> str | None:
        self.last_error = None
        if not self.enabled:
            return None

        try:
            if self.provider == "gemini":
                return self._call_gemini(system_prompt, user_prompt, max_tokens=max_tokens, temperature=temperature)
            if self.provider == "anthropic":
                return self._call_anthropic(system_prompt, user_prompt, max_tokens=max_tokens, temperature=temperature)
            if self.provider == "openai":
                return self._call_openai(system_prompt, user_prompt, max_tokens=max_tokens, temperature=temperature)
            return None
        except Exception as exc:
            self.last_error = str(exc)
            return None

    def _call_anthropic(
        self,
        system_prompt: str,
        user_prompt: str,
        max_tokens: int,
        temperature: float,
    ) -> str | None:
        payload = {
            "model": self.model,
            "max_tokens": max_tokens,
            "temperature": temperature,
            "system": system_prompt,
            "messages": [{"role": "user", "content": user_prompt}],
        }
        data = self._post_json(
            "https://api.anthropic.com/v1/messages",
            headers={
                "content-type": "application/json",
                "x-api-key": self.anthropic_key,
                "anthropic-version": "2023-06-01",
            },
            payload=payload,
        )
        blocks = data.get("content", [])
        texts = [b.get("text", "") for b in blocks if isinstance(b, dict) and b.get("type") == "text"]
        out = "\n".join([t for t in texts if t]).strip()
        return out or None

    def _call_gemini(
        self,
        system_prompt: str,
        user_prompt: str,
        max_tokens: int,
        temperature: float,
    ) -> str | None:
        candidate_models = self._gemini_candidate_models()

        last_exc: Exception | None = None
        for model_name in candidate_models:
            model = quote_plus(model_name)
            url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={self.gemini_key}"
            payload = {
                "systemInstruction": {
                    "parts": [{"text": system_prompt}],
                },
                "contents": [
                    {
                        "role": "user",
                        "parts": [{"text": user_prompt}],
                    }
                ],
                "generationConfig": {
                    "temperature": temperature,
                    "maxOutputTokens": max_tokens,
                },
            }
            try:
                data = self._post_json(
                    url,
                    headers={"content-type": "application/json"},
                    payload=payload,
                )
                candidates = data.get("candidates", [])
                if not candidates:
                    continue
                parts = candidates[0].get("content", {}).get("parts", [])
                texts = [p.get("text", "") for p in parts if isinstance(p, dict)]
                out = "\n".join([t for t in texts if t]).strip()
                if out:
                    self.model = model_name
                    return out
            except Exception as exc:
                last_exc = exc
                # Modèle indisponible: on tente le prochain modèle de fallback.
                msg = str(exc).lower()
                if (
                    "http 404" in msg
                    or "http 400" in msg
                    or "http 429" in msg
                    or "http 503" in msg
                    or "not found" in msg
                    or "no longer available" in msg
                    or "is not supported" in msg
                    or "unavailable" in msg
                    or "high demand" in msg
                ):
                    # Erreur transitoire (quota/demande) ou modèle indisponible:
                    # on tente un autre modèle Gemini sans casser la réponse analytique.
                    time.sleep(0.12)
                    continue
                raise

        if last_exc is not None:
            raise last_exc
        return None

    def _gemini_candidate_models(self) -> list[str]:
        """
        Construit une liste de modèles Gemini à tester:
        1) modèle configuré
        2) modèles récents courants
        3) modèles réellement disponibles via /models (si accessible)
        """
        preferred = [
            self.model,
            "gemini-2.5-flash",
            "gemini-2.5-flash-lite",
            "gemini-2.0-flash",
            "gemini-2.0-flash-lite",
            "gemini-1.5-flash",
            "gemini-1.5-flash-latest",
        ]
        available = self._list_gemini_models()

        ordered: list[str] = []
        for name in preferred:
            n = (name or "").strip()
            if n and n not in ordered:
                ordered.append(n)

        if available:
            for name in available:
                n = (name or "").strip()
                if n and n not in ordered:
                    ordered.append(n)

        return ordered

    def _list_gemini_models(self) -> list[str]:
        """Récupère les modèles Gemini compatibles generateContent (best-effort)."""
        try:
            url = f"https://generativelanguage.googleapis.com/v1beta/models?key={self.gemini_key}"
            data = self._get_json(url)
            models = data.get("models", [])
            out: list[str] = []
            for m in models:
                if not isinstance(m, dict):
                    continue
                methods = m.get("supportedGenerationMethods", [])
                if methods and "generateContent" not in methods:
                    continue
                raw_name = str(m.get("name", "")).strip()
                if not raw_name:
                    continue
                short = raw_name.split("/", 1)[-1]
                if short.startswith("gemini") and short not in out:
                    out.append(short)
            return out
        except Exception:
            return []

    def _call_openai(
        self,
        system_prompt: str,
        user_prompt: str,
        max_tokens: int,
        temperature: float,
    ) -> str | None:
        payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            "max_tokens": max_tokens,
            "temperature": temperature,
        }
        data = self._post_json(
            "https://api.openai.com/v1/chat/completions",
            headers={
                "content-type": "application/json",
                "authorization": f"Bearer {self.openai_key}",
            },
            payload=payload,
        )
        choices = data.get("choices", [])
        if not choices:
            return None
        message = choices[0].get("message", {})
        content = message.get("content", "")
        if isinstance(content, str):
            return content.strip() or None
        if isinstance(content, list):
            parts = []
            for block in content:
                if isinstance(block, dict):
                    txt = block.get("text") or block.get("content")
                    if isinstance(txt, str):
                        parts.append(txt)
            out = "\n".join(parts).strip()
            return out or None
        return None

    def _post_json(self, url: str, headers: dict, payload: dict) -> dict:
        req = request.Request(
            url=url,
            method="POST",
            headers=headers,
            data=json.dumps(payload).encode("utf-8"),
        )
        try:
            with request.urlopen(req, timeout=self.timeout_sec) as resp:
                raw = resp.read().decode("utf-8")
                return json.loads(raw)
        except error.HTTPError as http_err:
            body = ""
            try:
                body = http_err.read().decode("utf-8")
            except Exception:
                pass
            raise RuntimeError(f"HTTP {http_err.code} from LLM provider: {body[:300]}")

    def _get_json(self, url: str) -> dict:
        req = request.Request(url=url, method="GET")
        try:
            with request.urlopen(req, timeout=self.timeout_sec) as resp:
                raw = resp.read().decode("utf-8")
                return json.loads(raw)
        except error.HTTPError as http_err:
            body = ""
            try:
                body = http_err.read().decode("utf-8")
            except Exception:
                pass
            raise RuntimeError(f"HTTP {http_err.code} from LLM provider: {body[:300]}")

    def _load_dotenv(self) -> None:
        env_path = Path(__file__).parent / ".env"
        if not env_path.exists():
            return
        for raw in env_path.read_text(encoding="utf-8").splitlines():
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            if line.startswith("export "):
                line = line[len("export ") :]
            key, val = line.split("=", 1)
            key = key.strip()
            if not key or key in os.environ:
                continue
            val = val.strip().strip('"').strip("'")
            os.environ[key] = val
