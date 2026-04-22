"""
Compatibility shim.

Actual FastAPI router lives in: app.api.v1.routes.scoring
"""

from app.api.v1.routes.scoring import router  # noqa: F401

