"""meta_ads_test_project.api

Minimal FastAPI wrapper around `meta_ads_tool.py` so Make.com (or any HTTP client)
can run a LaunchPlan and receive the created Meta object IDs.

Endpoints
---------
- GET  /health        -> basic health check
- GET  /              -> basic root info
- POST /run           -> JSON body: validate + run a LaunchPlan
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
- IDEMPOTENCY_DB_PATH (default: .meta_idempotency.db; ignored if IDEMPOTENCY_STORE_SOURCE=db)
- IDEMPOTENCY_STORE_SOURCE (set to "db" to store idempotency in Postgres)
- QUEUE_STORE_SOURCE (set to "db" to store batching queue in Postgres)
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

from fastapi import FastAPI, File, Form, Header, HTTPException, UploadFile, BackgroundTasks
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, ValidationError
from PIL import Image

from meta_ads_tool import (
    build_idempotency_store,
    build_queue_store,
    compute_adset_signature_v2,
    _drain_queue_group_v2,
    AssetsSpec,
    LaunchPlan,
    MetaAPIError,
    MetaClient,
    MetaConfig,
    run_launch_plan,
)

app = FastAPI(title="Meta Ads Tool API", version="1.1.2")


class RunRequest(BaseModel):
    """Request body for /run (JSON).

    Typical Make.com payload:
    {
      "job_id": "<notion page id>",
      "dry_run": false,
      "plan": { ... LaunchPlan JSON ... }
    }
    """

    job_id: Optional[str] = Field(
        default=None,
        description="Optional external job identifier (e.g., Notion page id).",
    )
    dry_run: bool = Field(
        default=False,
        description="If true, prints requests without calling Meta.",
    )
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
        raise HTTPException(status_code=422, detail=f"Uploaded image_file is not a valid image: {e}")

    out = io.BytesIO()
    img.save(out, format="JPEG", quality=92, optimize=True)
    jpeg_bytes = out.getvalue()

    if not jpeg_bytes or len(jpeg_bytes) < 1024:
        raise HTTPException(status_code=422, detail="JPEG re-encode failed or output too small")

    return jpeg_bytes


def _get_stores() -> tuple[Any, Any, str, str]:
    """Build idempotency + queue stores using env configuration."""
    store_path = (os.getenv("IDEMPOTENCY_DB_PATH") or ".meta_idempotency.db").strip() or ".meta_idempotency.db"
    queue_db_path = (os.getenv("QUEUE_DB_PATH") or ".queue_state.db").strip() or ".queue_state.db"
    store = build_idempotency_store(store_path)
    qstore = build_queue_store(queue_db_path)
    return store, qstore, store_path, queue_db_path


def _background_drain_group_v2(cfg: MetaConfig, *, product: str, category: str, signature: str) -> None:
    """Attempt to drain a single queue group (used as a background task)."""
    try:
        store, qstore, _, _ = _get_stores()
        _drain_queue_group_v2(
            cfg,
            qstore=qstore,
            store=store,
            product=product,
            category=category,
            signature=signature,
            dry_run=False,
        )
    except Exception:
        # Best-effort: never fail the request because the background drain failed.
        pass



@app.get("/")
def root() -> JSONResponse:
    return JSONResponse({"ok": True, "docs": "/docs", "health": "/health"})


@app.get("/health")
def health() -> Dict[str, Any]:
    return {"ok": True}


@app.post("/run")
def run(req: RunRequest, background_tasks: BackgroundTasks, x_api_key: Optional[str] = Header(default=None, alias="X-API-Key")) -> Dict[str, Any]:
    _require_api_key(x_api_key)

    try:
        cfg = MetaConfig.from_env()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Server misconfigured: {e}")

    store_path = (os.getenv("IDEMPOTENCY_DB_PATH") or ".meta_idempotency.db").strip() or ".meta_idempotency.db"

    try:
        plan = LaunchPlan.model_validate(req.plan)
        result = run_launch_plan(cfg, plan, store_path=store_path, dry_run=req.dry_run)

        # Make.com often times out around ~40s. For schema v2 batching, we enqueue quickly and
        # trigger a best-effort background drain (so the HTTP response is fast and reliable).
        if (not req.dry_run) and getattr(plan, "schema_version", 1) >= 2 and bool(getattr(plan, "batching", False)):
            try:
                sig = compute_adset_signature_v2(plan.adset)
                category = (plan.category or "ug")
                background_tasks.add_task(
                    _background_drain_group_v2,
                    cfg,
                    product=plan.product,
                    category=category,
                    signature=sig,
                )
            except Exception:
                pass

        return {"ok": True, "job_id": req.job_id, "result": result}

    except ValidationError as e:
        raise HTTPException(status_code=422, detail=jsonable_encoder(e.errors()))
    except MetaAPIError as e:
        raise HTTPException(
            status_code=502,
            detail={
                "message": str(e),
                "http_status": e.http_status,
                "meta_error": e.error,
            },
        )
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))




@app.get("/queue")
def queue_groups(
    limit: int = 50,
    x_api_key: Optional[str] = Header(default=None, alias="X-API-Key"),
) -> Dict[str, Any]:
    """Debug endpoint: list current batching queue groups."""
    _require_api_key(x_api_key)
    _, qstore, _, _ = _get_stores()
    groups = qstore.list_groups(limit=int(limit))
    return {"ok": True, "limit": int(limit), "groups": groups}


@app.post("/drain")
def drain_queue(
    limit: int = 50,
    x_api_key: Optional[str] = Header(default=None, alias="X-API-Key"),
) -> Dict[str, Any]:
    """Drain all eligible queue groups once.

    Use this if you don't want a separate worker service. Call it on a schedule (e.g. every minute).
    """
    _require_api_key(x_api_key)

    try:
        cfg = MetaConfig.from_env()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Server misconfigured: {e}")

    store, qstore, _, _ = _get_stores()
    groups = qstore.list_groups(limit=int(limit))

    drained: list[dict] = []
    for g in groups:
        try:
            out = _drain_queue_group_v2(
                cfg,
                qstore=qstore,
                store=store,
                product=g["product"],
                category=g["category"],
                signature=g["signature"],
                dry_run=False,
            )
            drained.append({"group": g, "result": out})
        except Exception as e:
            drained.append({"group": g, "error": str(e)})

    return {"ok": True, "groups_scanned": len(groups), "results": drained}


@app.post("/run-multipart")
async def run_multipart(
    plan: str = Form(..., description="LaunchPlan JSON as a string"),
    dry_run: bool = Form(False),
    job_id: Optional[str] = Form(None),
    image_file: UploadFile | None = File(None, description="Image file (jpg/png/etc)"),
    background_tasks: BackgroundTasks,
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

        # If the request includes an uploaded file, the LaunchPlan validator still requires
        # one of: image_hash, image_path, image_url. We'll seed a placeholder so validation passes,
        # then replace it with the real uploaded image_hash after uploading.
        if image_file is not None:
            assets = plan_dict.get("assets") or {}
            if not (assets.get("image_hash") or assets.get("image_path") or assets.get("image_url")):
                assets["image_hash"] = "__uploaded_via_multipart__"
            plan_dict["assets"] = assets

        plan_obj = LaunchPlan.model_validate(plan_dict)

        debug: Dict[str, Any] = {}

        # If a file is provided, upload it to Meta and inject image_hash
        if image_file is not None:
            raw = await image_file.read()
            debug["received_filename"] = image_file.filename
            debug["received_content_type"] = image_file.content_type
            debug["received_size_bytes"] = len(raw or b"")

            if not raw or len(raw) < 1024:
                raise HTTPException(
                    status_code=422,
                    detail={
                        "message": "Uploaded image_file is empty or too small. Make is likely not sending file bytes.",
                        **debug,
                    },
                )

            jpeg_bytes = _normalize_to_jpeg_bytes(raw)
            debug["jpeg_size_bytes"] = len(jpeg_bytes)

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

        if (not dry_run) and getattr(plan_obj, "schema_version", 1) >= 2 and bool(getattr(plan_obj, "batching", False)):
            try:
                sig = compute_adset_signature_v2(plan_obj.adset)
                category = (plan_obj.category or "ug")
                background_tasks.add_task(
                    _background_drain_group_v2,
                    cfg,
                    product=plan_obj.product,
                    category=category,
                    signature=sig,
                )
            except Exception:
                pass

        return {"ok": True, "job_id": job_id, "result": result, "debug": debug}

    except json.JSONDecodeError as e:
        raise HTTPException(status_code=422, detail=f"Invalid JSON in form field 'plan': {e}")
    except ValidationError as e:
        raise HTTPException(status_code=422, detail=jsonable_encoder(e.errors()))
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
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))