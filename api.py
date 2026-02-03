"""meta_ads_test_project.api

Minimal FastAPI wrapper around `meta_ads_tool.py` so Make.com (or any HTTP client)
can run a LaunchPlan and receive the created Meta object IDs.

Endpoints
---------
- GET  /health  -> basic health check
- POST /run     -> validate + run a LaunchPlan (idempotent)

Optional API Key
----------------
If you set SERVICE_API_KEY in the environment, requests must include:
  X-API-Key: <SERVICE_API_KEY>

Environment variables
---------------------
Required:
- META_ACCESS_TOKEN
- META_AD_ACCOUNT_ID

Optional:
- META_API_VERSION (default: v21.0)
- META_APP_ID
- META_APP_SECRET
- IDEMPOTENCY_DB_PATH (default: .meta_idempotency.db)
- SERVICE_API_KEY (if set, enforces X-API-Key)
"""

from __future__ import annotations

import os
from typing import Any, Dict, Optional

from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel, Field, ValidationError

from meta_ads_tool import MetaConfig, LaunchPlan, MetaAPIError, run_launch_plan


app = FastAPI(title="Meta Ads Tool API", version="1.0.0")


class RunRequest(BaseModel):
    """Request body for /run.

    Typical Make.com payload:
    {
      "job_id": "<notion page id>",
      "idempotency_key": "<unique key>",
      "dry_run": false,
      "plan": { ... LaunchPlan JSON ... }
    }
    """

    job_id: Optional[str] = Field(default=None, description="Optional external job identifier (e.g., Notion page id).")
    idempotency_key: Optional[str] = Field(default=None, description="Overrides plan.idempotency_key")
    dry_run: bool = Field(default=False, description="If true, prints requests without calling Meta.")
    plan: Dict[str, Any]


def _require_api_key(x_api_key: Optional[str]) -> None:
    expected = (os.getenv("SERVICE_API_KEY") or "").strip()
    if not expected:
        return
    if not x_api_key or x_api_key.strip() != expected:
        raise HTTPException(status_code=401, detail="Unauthorized")


@app.get("/health")
def health() -> Dict[str, Any]:
    return {"ok": True}


@app.post("/run")
def run(req: RunRequest, x_api_key: Optional[str] = Header(default=None, alias="X-API-Key")) -> Dict[str, Any]:
    _require_api_key(x_api_key)

    try:
        cfg = MetaConfig.from_env()
    except Exception as e:
        # Misconfiguration is a server error, not a client error.
        raise HTTPException(status_code=500, detail=f"Server misconfigured: {e}")

    store_path = (os.getenv("IDEMPOTENCY_DB_PATH") or ".meta_idempotency.db").strip() or ".meta_idempotency.db"

    try:
        plan = LaunchPlan.model_validate(req.plan)
        if req.idempotency_key:
            # Override, so idempotency can be driven by Make/Notion.
            plan.idempotency_key = req.idempotency_key

        result = run_launch_plan(cfg, plan, store_path=store_path, dry_run=req.dry_run)
        return {
            "ok": True,
            "job_id": req.job_id,
            "result": result,
        }

    except ValidationError as e:
        raise HTTPException(status_code=422, detail=e.errors())
    except MetaAPIError as e:
        raise HTTPException(
            status_code=502,
            detail={
                "message": str(e),
                "http_status": e.http_status,
                "meta_error": e.error,
            },
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
from fastapi.responses import JSONResponse

@app.get("/")
def root():
    return JSONResponse({"ok": True, "docs": "/docs", "health": "/health"})

