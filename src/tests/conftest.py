"""Make `dags.common`, `common`, and `backfill` importable from src/ tests.

Inside the docker-compose `tests` profile the paths line up automatically;
this conftest keeps the same tests running on the host with `pytest -q`.
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
for p in (ROOT, ROOT / "src", ROOT / "dags"):
    sp = str(p)
    if sp not in sys.path:
        sys.path.insert(0, sp)
