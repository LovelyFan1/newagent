"""
Backfill 2022 legal data into fact_legal.

This script is kept as the required filename:
`app_v2/scripts/backfill_legal_2022.py`

Implementation lives in the same directory as `backfill_fact_legal_2022.py`.
We load it dynamically to avoid requiring `/app/scripts` to be a Python package.
"""

from __future__ import annotations

import asyncio
import importlib.util
import sys
from pathlib import Path


def _load_impl_module():
    here = Path(__file__).resolve().parent
    impl_path = here / "backfill_fact_legal_2022.py"
    spec = importlib.util.spec_from_file_location("backfill_fact_legal_2022", impl_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Failed to load module spec from {impl_path}")
    mod = importlib.util.module_from_spec(spec)
    # required for dataclasses/type resolution in some cases
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)  # type: ignore[union-attr]
    return mod


if __name__ == "__main__":
    mod = _load_impl_module()
    raise SystemExit(asyncio.run(mod.main()))

