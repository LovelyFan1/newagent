from __future__ import annotations

import json
import re
import uuid
from dataclasses import dataclass
from typing import Any


def new_evidence_id(prefix: str) -> str:
    return f"ev_{prefix}_{uuid.uuid4().hex[:10]}"


def strip_json_fences(text: str) -> str:
    t = (text or "").strip()
    if t.startswith("```"):
        t = re.sub(r"^```(?:json)?\s*", "", t, flags=re.IGNORECASE)
        t = re.sub(r"\s*```\s*$", "", t)
    return t.strip()


def extract_json_object(text: str) -> dict[str, Any] | None:
    raw = strip_json_fences(text)
    try:
        obj = json.loads(raw)
        return obj if isinstance(obj, dict) else None
    except Exception:
        m = re.search(r"\{[\s\S]*\}", raw)
        if not m:
            return None
        try:
            obj = json.loads(m.group(0))
            return obj if isinstance(obj, dict) else None
        except Exception:
            return None


def safe_text(v: Any, limit: int = 900) -> str:
    s = "" if v is None else str(v)
    s = " ".join(s.split())
    return s if len(s) <= limit else (s[:limit] + "...")


@dataclass(frozen=True)
class TimeRange:
    kind: str  # "year" | "LAST_2_YEARS" | "LAST_3_YEARS" | "LAST_YEAR"
    year: int | None = None

    def years(self, default_year: int = 2022) -> list[int]:
        if self.kind == "year" and self.year:
            return [self.year]
        if self.kind == "LAST_YEAR":
            return [default_year - 1]
        if self.kind == "LAST_2_YEARS":
            return [default_year - 1, default_year]
        if self.kind == "LAST_3_YEARS":
            return [default_year - 2, default_year - 1, default_year]
        return [default_year]

