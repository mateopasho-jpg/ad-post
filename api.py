"""meta_ads_test_project.api

Minimal FastAPI wrapper around `meta_ads_tool.py` so Make.com (or any HTTP client)
can run a LaunchPlan and receive the created Meta object IDs.

Endpoints
---------
- GET  /health        -> basic health check
- GET  /              -> basic root info
- POST /run           -> JSON body: validate + run a LaunchPlan (idempotent)
- POST /run-multipart -> multipart/form-data: plan JSON + optional image file

Optional API Key
----------------
If you set SERVICE_API_KEY in the environment, requests must include:
  X-API-Key: <SERVICE_API_KEY>

Environment variables
---------------------
Required for ad-post service:
- META_AD_ACCOUNT_ID
- (token comes from META_ACCESS_TOKEN OR META_TOKEN_SOURCE=db with DATABASE_URL)

Optional:
- META_API_VERSION (default: v21.0)
- META_APP_ID
- META_APP_SECRET
- META_TOKEN_SOURCE ("db" to read token from DB)
- DATABASE_URL (required if META_TOKEN_SOURCE=db)
- IDEMPOTENCY_DB_PATH (default: .meta_idempotency.db)
- SERVICE_API_KEY (if set, enforces X-API-Key)

Notes
-----
Use /run-multipart from Make to avoid flaky image_url downloads. Provide:
- plan (text field; JSON string)
- image_file (file field; jpg/png/etc)
"""

from __future__ import annotations

import io
import json
import os
from typing import Any, Dict, Optional

from fastapi import FastAPI, File, Form, Header, HTTPException, UploadFile
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, ValidationError
from PIL import Image

from meta_ads_tool import (
    AssetsSpec,
    LaunchPlan,
    MetaAPIError,
    MetaClient,
    MetaConfig,
    run_launch_plan,
)

app = FastAPI(title="Meta Ads Tool API", version="1.1.0")


class RunRequest(BaseModel):
    """Request body for /run (JSON).

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


def _normalize_to_jpeg_bytes(raw: bytes) -> bytes:
    """Re-encode any image bytes to a Meta-safe JPEG."""
    try:
        img = Image.open(io.BytesIO(raw))
        img = img.convert("RGB")
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Uploaded image_file is not a valid image: {e}")

    out = io.BytesIO()
    img.save(out, format="JPEG", quality=92, optimize=True)
    jpeg_bytes = out.getvalue()

    if not jpeg_bytes or len(jpeg_bytes) < 1024:
        raise HTTPException(status_code=400, detail="JPEG re-encode failed or output too small")

    return jpeg_bytes


@app.get("/")
def root() -> JSONResponse:
    return JSONResponse({"ok": True, "docs": "/docs", "health": "/health"})


@app.get("/health")
def health() -> Dict[str, Any]:
    return {"ok": True}


@app.post("/run")
def run(req: RunRequest, x_api_key: Optional[str] = Header(default=None, alias="X-API-Key")) -> Dict[str, Any]:
    _require_api_key(x_api_key)

    try:
        cfg = MetaConfig.from_env()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Server misconfigured: {e}")

    store_path = (os.getenv("IDEMPOTENCY_DB_PATH") or ".meta_idempotency.db").strip() or ".meta_idempotency.db"

    try:
        plan = LaunchPlan.model_validate(req.plan)
        if req.idempotency_key:
            plan.idempotency_key = req.idempotency_key

        result = run_launch_plan(cfg, plan, store_path=store_path, dry_run=req.dry_run)
        return {"ok": True, "job_id": req.job_id, "result": result}

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


@app.post("/run-multipart")
async def run_multipart(
    plan: str = Form(..., description="LaunchPlan JSON as a string"),
    dry_run: bool = Form(False),
    idempotency_key: Optional[str] = Form(None),
    job_id: Optional[str] = Form(None),
    image_file: UploadFile | None = File(None, description="Image file (jpg/png/etc)"),
    x_api_key: Optional[str] = Header(default=None, alias="X-API-Key"),
) -> Dict[str, Any]:
    """
    Multipart form endpoint for Make.com:
    - plan: text field containing LaunchPlan JSON
    - image_file: file field (optional). If provided, we upload to Meta and inject image_hash.
    """
    _require_api_key(x_api_key)

    try:
        cfg = MetaConfig.from_env()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Server misconfigured: {e}")

    store_path = (os.getenv("IDEMPOTENCY_DB_PATH") or ".meta_idempotency.db").strip() or ".meta_idempotency.db"

    try:
        # Parse plan JSON string -> LaunchPlan
        plan_dict = json.loads(plan)
        plan_obj = LaunchPlan.model_validate(plan_dict)

        if idempotency_key:
            plan_obj.idempotency_key = idempotency_key

        # If a file is provided, upload it to Meta and inject image_hash
        if image_file is not None:
            raw = await image_file.read()
            if not raw or len(raw) < 1024:
                raise HTTPException(status_code=400, detail="Uploaded image_file is empty or too small")

            jpeg_bytes = _normalize_to_jpeg_bytes(raw)

            # Upload bytes directly (most reliable)
            client = MetaClient(cfg)
            image_hash = client.upload_image(image_bytes=jpeg_bytes, filename="image.jpg")

            # Ensure assets exists and set image_hash
            if plan_obj.assets is None:
                plan_obj.assets = AssetsSpec()
            plan_obj.assets.image_hash = image_hash
            plan_obj.assets.image_url = None
            plan_obj.assets.image_path = None

        result = run_launch_plan(cfg, plan_obj, store_path=store_path, dry_run=dry_run)
        return {"ok": True, "job_id": job_id, "result": result}

    except json.JSONDecodeError as e:
        raise HTTPException(status_code=400, detail=f"Invalid JSON in form field 'plan': {e}")
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
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
