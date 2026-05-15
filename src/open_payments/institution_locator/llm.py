"""Anthropic Claude backend for institution → location lookup.

Strategy
--------
For each institution, send a small prompt to Claude Haiku asking for
all known US campuses with city and state. The model returns
structured JSON; we parse it into ``CandidateLocation``.

Why Haiku, not Sonnet
---------------------
This task is simple geographic knowledge ("where is Cleveland Clinic
headquartered?") — Haiku gets it right >95% of the time at ~10x lower
cost. A 1500-institution residual costs around $0.05 total. Switch to
Sonnet via the ``model`` kwarg if you see precision issues on niche
institutions.

Caching
-------
The backend itself is stateless — caching is the orchestrator's
``DiskCache``. This module just maps name → list[CandidateLocation].

Dependency
----------
``anthropic`` is an OPTIONAL dependency (installed via
``open_payments[llm]``). Import is deferred to instantiation time so
NPPES-only and manual-only deployments don't need the dep.
"""

from __future__ import annotations

import json
import logging
import re
import time
from typing import TYPE_CHECKING

from .types import CandidateLocation

if TYPE_CHECKING:
    from anthropic import Anthropic

logger = logging.getLogger(__name__)


DEFAULT_MODEL = "claude-haiku-4-5-20251001"

PROMPT_TEMPLATE = """\
I'm cleaning up an institution-name dataset for healthcare provider research \
and need to resolve free-text institution strings to US city/state.

Input: "{institution}"

Return a JSON object with this exact shape:

{{
  "locations": [
    {{"city": "Baltimore", "state": "MD", "confidence": 0.95}}
  ]
}}

Rules:
- "state" must be the 2-letter USPS abbreviation (MD, CA, NY, etc.).
- For multi-campus institutions (e.g. "University of California"), list \
the major campuses, most prominent first, up to 5 entries.
- If the institution is not a real US healthcare-adjacent institution, \
or you don't know its location, return {{"locations": []}}.
- "confidence" is 0.0-1.0 reflecting your certainty about the city/state.
- Output ONLY the JSON object, no prose, no markdown fences.
"""


class ClaudeAPIBackend:
    """Claude Haiku institution → location lookup.

    Use::

        from open_payments.institution_locator import ClaudeAPIBackend
        backend = ClaudeAPIBackend(api_key=os.environ["ANTHROPIC_API_KEY"])
        candidates = backend.locate("Cleveland Clinic")
    """

    def __init__(
        self,
        *,
        api_key: str,
        model: str = DEFAULT_MODEL,
        max_retries: int = 3,
        retry_backoff_s: float = 2.0,
        client: "Anthropic | None" = None,
    ):
        if client is not None:
            self._client = client
        else:
            try:
                from anthropic import Anthropic
            except ImportError as exc:
                raise ImportError(
                    "ClaudeAPIBackend requires the 'anthropic' package. "
                    "Install with: pip install open_payments[llm]"
                ) from exc
            self._client = Anthropic(api_key=api_key)
        self.model = model
        self.max_retries = max_retries
        self.retry_backoff_s = retry_backoff_s

    def locate(self, institution: str) -> list[CandidateLocation]:
        """Resolve one institution. Returns ``[CandidateLocation(source='miss')]``
        on parse failure or empty model response — never raises so the
        cache always gets to record the attempt."""
        try:
            raw = self._call_with_retry(institution)
        except Exception:
            logger.exception("LLM call failed for %r", institution)
            return [CandidateLocation(institution=institution, source="miss")]
        return self._parse_response(institution, raw)

    def locate_batch(
        self, institutions: list[str]
    ) -> dict[str, list[CandidateLocation]]:
        """Sequential per-institution calls. Simpler than batching and
        keeps per-institution failure isolation cleanly. The HTTP-level
        anthropic client already pipelines under the hood."""
        out: dict[str, list[CandidateLocation]] = {}
        for institution in institutions:
            out[institution] = self.locate(institution)
        return out

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _call_with_retry(self, institution: str) -> str:
        """Call Anthropic with exponential backoff on transient errors."""
        last_exc: Exception | None = None
        for attempt in range(self.max_retries):
            try:
                resp = self._client.messages.create(
                    model=self.model,
                    max_tokens=512,
                    messages=[
                        {
                            "role": "user",
                            "content": PROMPT_TEMPLATE.format(institution=institution),
                        }
                    ],
                )
                # API returns a list of content blocks; we ask for text only.
                parts = []
                for block in resp.content:
                    text = getattr(block, "text", None)
                    if text:
                        parts.append(text)
                return "".join(parts)
            except Exception as exc:
                last_exc = exc
                if attempt + 1 < self.max_retries:
                    sleep_for = self.retry_backoff_s * (2**attempt)
                    logger.warning(
                        "anthropic API call for %r failed (attempt %d/%d): %s — sleeping %.1fs",
                        institution,
                        attempt + 1,
                        self.max_retries,
                        exc,
                        sleep_for,
                    )
                    time.sleep(sleep_for)
        # All retries exhausted — re-raise the last exception so callers
        # can decide whether to mark as miss vs propagate.
        raise last_exc if last_exc is not None else RuntimeError(
            "anthropic call failed with no recorded exception"
        )

    def _parse_response(
        self, institution: str, raw: str
    ) -> list[CandidateLocation]:
        """Extract the JSON object, tolerate markdown fences if the model
        adds them despite the rule."""
        # Strip leading/trailing whitespace and any code fences.
        cleaned = raw.strip()
        cleaned = re.sub(r"^```(?:json)?\s*", "", cleaned)
        cleaned = re.sub(r"\s*```$", "", cleaned)
        try:
            payload = json.loads(cleaned)
        except json.JSONDecodeError:
            logger.warning(
                "anthropic returned non-JSON for %r: %s",
                institution,
                cleaned[:200],
            )
            return [CandidateLocation(institution=institution, source="miss")]

        rows = payload.get("locations", []) or []
        if not rows:
            return [CandidateLocation(institution=institution, source="miss")]

        out: list[CandidateLocation] = []
        for row in rows:
            city = (row.get("city") or "").strip() or None
            state = (row.get("state") or "").strip().upper() or None
            confidence = row.get("confidence")
            try:
                confidence_f = float(confidence) if confidence is not None else None
            except (TypeError, ValueError):
                confidence_f = None
            if city is None and state is None:
                continue
            out.append(
                CandidateLocation(
                    institution=institution,
                    city=city,
                    state=state,
                    source="llm",
                    confidence=confidence_f,
                )
            )
        if not out:
            return [CandidateLocation(institution=institution, source="miss")]
        return out
