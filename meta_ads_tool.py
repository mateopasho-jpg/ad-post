"""
Meta Ads / Marketing API Test Project (Python)
=============================================

This project is a *testing harness* for the "Meta adapter" portion of your automation.
It intentionally keeps Notion + Make + Webhooks manual for now.

It supports:
- Connectivity checks (token + ad account)
- Uploading an image to get image_hash
- Creating Campaign -> AdSet -> AdCreative -> Ad (default PAUSED to avoid spend)
- Reading objects (campaign/adset/ad/creative)
- Updating status (pause/activate) and basic ad set budget updates
- Local idempotency (so reruns won't create duplicates)

Sources:
- Meta Marketing API "Get Started" guide shows:
  - Campaign create fields including objective/status/special_ad_categories and recommends PAUSED while testing.
  - AdSet create fields such as optimization_goal, billing_event, bid_amount, daily_budget, targeting, status, promoted_object.
  - AdImage upload -> image_hash -> AdCreative create -> Ad create.
- Meta Business Management docs show system user tokens/scopes and token generation concepts.

"""

from __future__ import annotations

import argparse
import dataclasses
import hashlib
import hmac
import json
import os
import sqlite3
import sys
import textwrap
import time
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from typing import Optional

import requests
from dotenv import load_dotenv
from pydantic import BaseModel, Field, ValidationError, model_validator


# -----------------------------
# Exceptions
# -----------------------------

class MetaAPIError(RuntimeError):
    def __init__(self, message: str, *, http_status: int | None = None, error: dict | None = None):
        super().__init__(message)
        self.http_status = http_status
        self.error = error or {}


# -----------------------------
# Config
# -----------------------------

@dataclass(frozen=True)
class MetaConfig:
    access_token: str
    ad_account_id: str
    api_version: str = "v21.0"
    app_id: str | None = None
    app_secret: str | None = None
    timeout_s: int = 30

    @staticmethod
    def from_env() -> "MetaConfig":
        """Loads config from environment variables (optionally via .env)."""
        load_dotenv(override=False)

        token = os.getenv("META_ACCESS_TOKEN", "").strip()
        account_id = os.getenv("META_AD_ACCOUNT_ID", "").strip()
        api_version = os.getenv("META_API_VERSION", "v21.0").strip() or "v21.0"
        app_id = os.getenv("META_APP_ID", "").strip() or None
        app_secret = os.getenv("META_APP_SECRET", "").strip() or None

        if not token:
            raise ValueError("Missing META_ACCESS_TOKEN in environment (.env).")
        if not account_id:
            raise ValueError("Missing META_AD_ACCOUNT_ID in environment (.env).")

        return MetaConfig(
            access_token=token,
            ad_account_id=account_id,
            api_version=api_version,
            app_id=app_id,
            app_secret=app_secret,
        )


def normalize_ad_account_id(ad_account_id: str) -> str:
    """
    Meta endpoints use act_<AD_ACCOUNT_ID>.
    Accept either 'act_123' or '123' from the user.
    """
    ad_account_id = ad_account_id.strip()
    if ad_account_id.startswith("act_"):
        return ad_account_id
    if ad_account_id.isdigit():
        return f"act_{ad_account_id}"
    # Allow IDs like "act_123..." only; otherwise just return and let API error.
    return ad_account_id


# -----------------------------
# Local idempotency store
# -----------------------------

class IdempotencyStore:
    """
    A tiny local store so you can safely re-run the same plan during testing
    without creating duplicate campaigns/adsets/ads.
    """

    def __init__(self, db_path: Path):
        self.db_path = db_path
        self._init_db()

    def _init_db(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS launches (
                    idempotency_key TEXT PRIMARY KEY,
                    payload_sha256 TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    campaign_id TEXT,
                    adset_id TEXT,
                    creative_id TEXT,
                    ad_id TEXT
                )
                """
            )
            conn.commit()

    def get(self, key: str) -> Optional[dict]:
        with sqlite3.connect(self.db_path) as conn:
            cur = conn.execute(
                "SELECT idempotency_key, payload_sha256, created_at, campaign_id, adset_id, creative_id, ad_id FROM launches WHERE idempotency_key=?",
                (key,),
            )
            row = cur.fetchone()
        if not row:
            return None
        return {
            "idempotency_key": row[0],
            "payload_sha256": row[1],
            "created_at": row[2],
            "campaign_id": row[3],
            "adset_id": row[4],
            "creative_id": row[5],
            "ad_id": row[6],
        }

    def put(self, key: str, payload_sha256: str, ids: dict) -> None:
        created_at = datetime.now(timezone.utc).isoformat()
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO launches
                (idempotency_key, payload_sha256, created_at, campaign_id, adset_id, creative_id, ad_id)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    key,
                    payload_sha256,
                    created_at,
                    ids.get("campaign_id"),
                    ids.get("adset_id"),
                    ids.get("creative_id"),
                    ids.get("ad_id"),
                ),
            )
            conn.commit()


# -----------------------------
# Plan models (strict JSON spec)
# -----------------------------

class CampaignSpec(BaseModel):
    name: str
    objective: str
    status: str = "PAUSED"

    # Meta docs show special_ad_categories can be [] (empty array) while testing.
    special_ad_categories: List[str] = Field(default_factory=list)

    buying_type: Optional[str] = None

    # Optional: campaign-level budgets (CBO). If you set these, you're using campaign budget.
    daily_budget: Optional[int] = None
    lifetime_budget: Optional[int] = None

    # ✅ IMPORTANT:
    # For ABO (ad set budgets), some accounts now require explicitly setting this flag.
    # Default False is safest (no budget sharing).
    is_adset_budget_sharing_enabled: Optional[bool] = None

class AdSetSpec(BaseModel):
    name: str
    optimization_goal: Optional[str] = None
    objective: Optional[str] = None

    billing_event: str
    status: str = "PAUSED"

    daily_budget: Optional[int] = None
    lifetime_budget: Optional[int] = None

    bid_amount: Optional[int] = None
    bid_strategy: Optional[str] = None

    is_adset_budget_sharing_enabled: Optional[bool] = None

    # ✅ DSA transparency fields (some EU accounts require these)
    dsa_beneficiary: Optional[str] = None
    dsa_payor: Optional[str] = None

    start_time: Optional[str] = None
    end_time: Optional[str] = None

    targeting: Dict[str, Any]
    promoted_object: Optional[Dict[str, Any]] = None

    @model_validator(mode="after")
    def _normalize_goal(self) -> "AdSetSpec":
        if not self.optimization_goal and self.objective:
            self.optimization_goal = self.objective
        if not self.optimization_goal:
            raise ValueError("AdSet requires optimization_goal (or objective which will be mapped).")

        using_adset_budget = (self.daily_budget is not None) or (self.lifetime_budget is not None)
        if not using_adset_budget:
            raise ValueError("AdSet requires either daily_budget or lifetime_budget.")

        if self.is_adset_budget_sharing_enabled is None:
            self.is_adset_budget_sharing_enabled = False

        return self


class CreativeSpec(BaseModel):
    name: str
    object_story_spec: Dict[str, Any]
    degrees_of_freedom_spec: Optional[Dict[str, Any]] = None

class AdSpec(BaseModel):
    name: str
    status: str = "PAUSED"

class AssetsSpec(BaseModel):
    image_path: Optional[str] = None  # local file path
    image_hash: Optional[str] = None  # if you already have it
    image_url: Optional[str] = None   # http(s) URL to an image (backend will download then upload)

class LaunchPlan(BaseModel):
    schema_version: int = 1
    idempotency_key: Optional[str] = None

    campaign: CampaignSpec
    adset: AdSetSpec
    creative: CreativeSpec
    ad: AdSpec
    assets: Optional[AssetsSpec] = None

    @model_validator(mode="after")
    def _check_assets(self) -> "LaunchPlan":
        if self.assets:
            if not self.assets.image_hash and not self.assets.image_path and not self.assets.image_url:
                raise ValueError("assets must include image_hash, image_path, or image_url (for upload).")
        return self

    @model_validator(mode="after")
    def _check_budget_mode(self) -> "LaunchPlan":
        using_campaign_budget = (
            self.campaign.daily_budget is not None or self.campaign.lifetime_budget is not None
        )
        using_adset_budget = (
            self.adset.daily_budget is not None or self.adset.lifetime_budget is not None
        )

        if using_campaign_budget and using_adset_budget:
            raise ValueError(
                "Choose one budget mode only: campaign budgets (CBO) OR ad set budgets (ABO), not both."
            )

        if not using_campaign_budget and not using_adset_budget:
            raise ValueError(
                "You must provide a budget either at campaign level (daily_budget/lifetime_budget) or adset level."
            )

        # ✅ If we're using ABO (adset budget), ensure the campaign flag is explicitly set.
        # Meta error_subcode 4834011 suggests some accounts require this now.
        if using_adset_budget and self.campaign.is_adset_budget_sharing_enabled is None:
            self.campaign.is_adset_budget_sharing_enabled = False

        return self


# -----------------------------
# Meta Client (REST via requests)
# -----------------------------

class MetaClient:
    def __init__(self, cfg: MetaConfig):
        self.cfg = cfg
        self.session = requests.Session()
        self.base_url = f"https://graph.facebook.com/{cfg.api_version}"

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[dict] = None,
        data: Optional[dict] = None,
        files: Optional[dict] = None,
        max_retries: int = 2,
    ) -> dict:
        url = f"{self.base_url}/{path.lstrip('/')}"
        params = params or {}
        data = data or {}

        # Graph API accepts access_token as query or form field.
        # We'll send it as query for GET, and as form field for POST/DELETE.
        if method.upper() == "GET":
            params.setdefault("access_token", self.cfg.access_token)
        else:
            data.setdefault("access_token", self.cfg.access_token)


        # Optional security hardening: appsecret_proof.
        # If you enable "App Secret Proof for Server API calls" in your Meta app,
        # you must include this HMAC on requests.
        if self.cfg.app_secret:
            proof = hmac.new(
                self.cfg.app_secret.encode("utf-8"),
                self.cfg.access_token.encode("utf-8"),
                hashlib.sha256,
            ).hexdigest()
            if method.upper() == "GET":
                params.setdefault("appsecret_proof", proof)
            else:
                data.setdefault("appsecret_proof", proof)

        last_err: Optional[Exception] = None
        for attempt in range(max_retries + 1):
            try:
                resp = self.session.request(
                    method=method.upper(),
                    url=url,
                    params=params,
                    data=data,
                    files=files,
                    timeout=self.cfg.timeout_s,
                )
                # Meta often returns JSON even for errors.
                try:
                    payload = resp.json()
                except Exception:
                    payload = {"raw": resp.text}

                if resp.status_code >= 400 or ("error" in payload):
                    error_obj = payload.get("error", {})
                    msg = error_obj.get("message") or payload.get("raw") or "Unknown Meta API error"
                    raise MetaAPIError(
                        f"Meta API error ({resp.status_code}): {msg}",
                        http_status=resp.status_code,
                        error=error_obj,
                    )
                return payload
            except MetaAPIError as e:
                # Retry only for transient-ish server errors/rate limits.
                last_err = e
                is_retryable = e.http_status in {500, 502, 503, 504, 429}
                if attempt < max_retries and is_retryable:
                    time.sleep(1.5 * (attempt + 1))
                    continue
                raise
            except requests.RequestException as e:
                last_err = e
                if attempt < max_retries:
                    time.sleep(1.5 * (attempt + 1))
                    continue
                raise MetaAPIError(f"Network error calling Meta API: {e}") from e

        raise MetaAPIError(f"Meta API request failed after retries: {last_err}")

    # -----------------------------
    # Diagnostics / discovery
    # -----------------------------

    def whoami(self) -> dict:
        return self._request("GET", "/me", params={"fields": "id,name"})

    def list_adaccounts(self, limit: int = 50) -> dict:
        return self._request("GET", "/me/adaccounts", params={"fields": "id,name,account_status,currency,timezone_name", "limit": str(limit)})

    def get_adaccount_details(self, ad_account_id: str | None = None) -> dict:
        acct = normalize_ad_account_id(ad_account_id or self.cfg.ad_account_id)
        fields = "id,name,business,business_name,amount_spent,balance,currency,timezone_name,instagram_accounts{id,username},spend_cap"
        return self._request("GET", f"/{acct}", params={"fields": fields})

    def get_promote_pages(self, ad_account_id: str | None = None) -> dict:
        acct = normalize_ad_account_id(ad_account_id or self.cfg.ad_account_id)
        return self._request("GET", f"/{acct}", params={"fields": "promote_pages"})

    def get_instagram_accounts(self, ad_account_id: str | None = None) -> dict:
        acct = normalize_ad_account_id(ad_account_id or self.cfg.ad_account_id)
        return self._request("GET", f"/{acct}/instagram_accounts")

    # -----------------------------
    # Create flow (image -> creative -> ad)
    # -----------------------------

    def upload_image(self, image_path: str, ad_account_id: str | None = None) -> str:
        """
        Uploads an image file and returns image_hash.
        Per Meta docs, upload goes to /act_<AD_ACCOUNT_ID>/adimages with filename=@<IMAGE_PATH>.
        """
        acct = normalize_ad_account_id(ad_account_id or self.cfg.ad_account_id)
        p = Path(image_path)
        if not p.exists():
            raise FileNotFoundError(f"Image file not found: {p}")

        with p.open("rb") as f:
            files = {"filename": (p.name, f, "application/octet-stream")}
            payload = self._request("POST", f"/{acct}/adimages", files=files, data={})

        images = payload.get("images") or {}
        if not images:
            raise MetaAPIError(f"Upload did not return images. Response: {payload}")

        # Response shape often: {"images": {"<hash>": {"hash":"<hash>", ...}}}
        # We'll take the first image entry.
        first_key = next(iter(images.keys()))
        img_obj = images[first_key]
        image_hash = img_obj.get("hash") or first_key
        if not image_hash:
            raise MetaAPIError(f"Could not parse image_hash from response: {payload}")
        return image_hash

    def create_campaign(self, spec: CampaignSpec, *, dry_run: bool = False) -> str:
        acct = normalize_ad_account_id(self.cfg.ad_account_id)

        data: Dict[str, Any] = {
            "name": spec.name,
            "objective": spec.objective,
            "status": spec.status,
            # Must be JSON string (e.g., "[]")
            "special_ad_categories": json.dumps(spec.special_ad_categories),
        }

        if spec.buying_type:
            data["buying_type"] = spec.buying_type

        # Optional: campaign budgets (CBO)
        if spec.daily_budget is not None:
            data["daily_budget"] = str(spec.daily_budget)
        if spec.lifetime_budget is not None:
            data["lifetime_budget"] = str(spec.lifetime_budget)

        # ✅ Key change:
        # If NOT using campaign budgets, explicitly set this flag on the campaign as well.
        using_campaign_budget = (spec.daily_budget is not None) or (spec.lifetime_budget is not None)
        if not using_campaign_budget:
            enabled = bool(spec.is_adset_budget_sharing_enabled)
            data["is_adset_budget_sharing_enabled"] = "true" if enabled else "false"

        if dry_run:
            print("[DRY RUN] create_campaign payload:", json.dumps(data, indent=2))
            return "DRY_RUN_CAMPAIGN_ID"

        payload = self._request("POST", f"/{acct}/campaigns", data=data)
        return payload["id"]


    def create_adset(self, spec: AdSetSpec, campaign_id: str, *, dry_run: bool = False) -> str:
        acct = normalize_ad_account_id(self.cfg.ad_account_id)

        data: Dict[str, Any] = {
            "name": spec.name,
            "campaign_id": campaign_id,
            "status": spec.status,
            "billing_event": spec.billing_event,
            "optimization_goal": spec.optimization_goal,
            "targeting": json.dumps(spec.targeting),
        }

        # Budget
        using_adset_budget = False
        if spec.daily_budget is not None:
            data["daily_budget"] = str(spec.daily_budget)
            using_adset_budget = True
        if spec.lifetime_budget is not None:
            data["lifetime_budget"] = str(spec.lifetime_budget)
            using_adset_budget = True

        # Bid settings
        if spec.bid_amount is not None:
            data["bid_amount"] = str(spec.bid_amount)
        if spec.bid_strategy is not None:
            data["bid_strategy"] = spec.bid_strategy

        # Schedule
        if spec.start_time is not None:
            data["start_time"] = spec.start_time
        if spec.end_time is not None:
            data["end_time"] = spec.end_time

        # Promoted object (optional)
        if spec.promoted_object is not None:
            data["promoted_object"] = json.dumps(spec.promoted_object)

        # ✅ REQUIRED field for your account when using adset budgets
        # Force send as "true"/"false" (string) for Graph form encoding.
        if using_adset_budget:
            enabled = bool(spec.is_adset_budget_sharing_enabled)
            data["is_adset_budget_sharing_enabled"] = "true" if enabled else "false"

        if dry_run:
            print("[DRY RUN] create_adset payload:", json.dumps(data, indent=2))
            return "DRY_RUN_ADSET_ID"
        if spec.dsa_beneficiary is not None:
            data["dsa_beneficiary"] = spec.dsa_beneficiary
        if spec.dsa_payor is not None:
            data["dsa_payor"] = spec.dsa_payor


        payload = self._request("POST", f"/{acct}/adsets", data=data)
        return payload["id"]



    def create_adcreative(self, spec: CreativeSpec, *, dry_run: bool = False) -> str:
        acct = normalize_ad_account_id(self.cfg.ad_account_id)
        data: Dict[str, Any] = {
            "name": spec.name,
            "object_story_spec": json.dumps(spec.object_story_spec),
        }
        if spec.degrees_of_freedom_spec is not None:
            data["degrees_of_freedom_spec"] = json.dumps(spec.degrees_of_freedom_spec)

        if dry_run:
            print("[DRY RUN] create_adcreative payload:", json.dumps(data, indent=2))
            return "DRY_RUN_CREATIVE_ID"

        payload = self._request("POST", f"/{acct}/adcreatives", data=data)
        return payload["id"]

    def create_ad(self, spec: AdSpec, adset_id: str, creative_id: str, *, dry_run: bool = False) -> str:
        acct = normalize_ad_account_id(self.cfg.ad_account_id)
        data = {
            "name": spec.name,
            "adset_id": adset_id,
            # creative must be a JSON object containing creative_id
            "creative": json.dumps({"creative_id": creative_id}),
            "status": spec.status,
        }

        if dry_run:
            print("[DRY RUN] create_ad payload:", json.dumps(data, indent=2))
            return "DRY_RUN_AD_ID"

        payload = self._request("POST", f"/{acct}/ads", data=data)
        return payload["id"]

    # -----------------------------
    # Read & update
    # -----------------------------

    def get_object(self, object_id: str, fields: str) -> dict:
        return self._request("GET", f"/{object_id}", params={"fields": fields})

    def set_status(self, object_id: str, status: str, *, dry_run: bool = False) -> dict:
        """
        Updates status for campaign/adset/ad. Typical statuses are ACTIVE / PAUSED.
        """
        if dry_run:
            print("[DRY RUN] set_status:", object_id, "->", status)
            return {"id": object_id, "status": status}

        return self._request("POST", f"/{object_id}", data={"status": status})

    def update_adset_budget(self, adset_id: str, *, daily_budget: Optional[int] = None, lifetime_budget: Optional[int] = None, dry_run: bool = False) -> dict:
        if daily_budget is None and lifetime_budget is None:
            raise ValueError("Provide daily_budget or lifetime_budget.")
        data: Dict[str, Any] = {}
        if daily_budget is not None:
            data["daily_budget"] = str(daily_budget)
        if lifetime_budget is not None:
            data["lifetime_budget"] = str(lifetime_budget)

        if dry_run:
            print("[DRY RUN] update_adset_budget:", adset_id, json.dumps(data, indent=2))
            return {"id": adset_id, **data}

        return self._request("POST", f"/{adset_id}", data=data)


# -----------------------------
# Utility
# -----------------------------

def sha256_json(obj: Any) -> str:
    raw = json.dumps(obj, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def load_plan(plan_path: str) -> LaunchPlan:
    p = Path(plan_path)
    if not p.exists():
        raise FileNotFoundError(f"Plan not found: {p}")
    data = json.loads(p.read_text())
    try:
        return LaunchPlan.model_validate(data)
    except ValidationError as e:
        raise ValueError(f"Plan JSON failed validation:\n{e}") from e


def inject_image_hash(plan: LaunchPlan, image_hash: str) -> LaunchPlan:
    """
    Places image_hash into creative.object_story_spec.link_data.image_hash when the plan uses '__FROM_UPLOAD__'
    """
    oss = plan.creative.object_story_spec
    link_data = oss.get("link_data")
    if isinstance(link_data, dict):
        if link_data.get("image_hash") in ("__FROM_UPLOAD__", "", None):
            link_data["image_hash"] = image_hash
    return plan


def default_idempotency_key(plan: LaunchPlan) -> str:
    """
    Default stable key: sha256 of core campaign/adset/creative/ad specs (excluding local image path).
    Good enough for test runs.
    """
    core = {
        "schema_version": plan.schema_version,
        "campaign": plan.campaign.model_dump(),
        "adset": plan.adset.model_dump(),
        "creative": plan.creative.model_dump(),
        "ad": plan.ad.model_dump(),
    }
    return f"plan:{sha256_json(core)[:20]}"


# -----------------------------
# Orchestration
# -----------------------------

def run_launch(
    cfg: MetaConfig,
    plan_path: str,
    *,
    store_path: str = ".meta_idempotency.db",
    dry_run: bool = False,
) -> dict:
    plan = load_plan(plan_path)
    return run_launch_plan(cfg, plan, store_path=store_path, dry_run=dry_run)


def _download_image_to_temp(url: str, *, timeout_s: int = 30) -> Path:
    """Downloads an image from a public URL to a temporary file and returns its path."""
    url = (url or "").strip()
    if not url.lower().startswith(("http://", "https://")):
        raise ValueError("image_url must start with http:// or https://")

    resp = requests.get(url, stream=True, timeout=timeout_s)
    try:
        resp.raise_for_status()
        # Best-effort suffix (helps Meta infer type). Default to .jpg.
        suffix = Path(url.split("?")[0]).suffix
        if not suffix or len(suffix) > 6:
            suffix = ".jpg"

        fd, tmp_path = tempfile.mkstemp(prefix="meta_img_", suffix=suffix)
        with os.fdopen(fd, "wb") as f:
            for chunk in resp.iter_content(chunk_size=1024 * 256):
                if chunk:
                    f.write(chunk)
        return Path(tmp_path)
    finally:
        resp.close()


def run_launch_plan(
    cfg: MetaConfig,
    plan: LaunchPlan,
    *,
    store_path: str = ".meta_idempotency.db",
    dry_run: bool = False,
) -> dict:
    """Runs a validated LaunchPlan directly (API-friendly)."""
    client = MetaClient(cfg)
    store = IdempotencyStore(Path(store_path))

    plan_dict = plan.model_dump()
    idem_key = plan.idempotency_key or default_idempotency_key(plan)
    payload_hash = sha256_json(plan_dict)

    existing = store.get(idem_key)
    if existing:
        print(f"[IDEMPOTENT HIT] Found existing launch for key={idem_key}")
        return existing

    tmp_image_path: Optional[Path] = None
    try:
        # 1) Upload image if needed
        image_hash: Optional[str] = None
        if plan.assets:
            if plan.assets.image_hash:
                image_hash = plan.assets.image_hash
            else:
                image_path: Optional[str] = None
                if plan.assets.image_path:
                    image_path = plan.assets.image_path
                elif plan.assets.image_url:
                    if dry_run:
                        print("[DRY RUN] download+upload image_url:", plan.assets.image_url)
                        image_hash = "DRY_RUN_IMAGE_HASH"
                    else:
                        tmp_image_path = _download_image_to_temp(
                            plan.assets.image_url,
                            timeout_s=cfg.timeout_s,
                        )
                        image_path = str(tmp_image_path)

                if image_hash is None and image_path:
                    if dry_run:
                        print("[DRY RUN] upload_image:", image_path)
                        image_hash = "DRY_RUN_IMAGE_HASH"
                    else:
                        image_hash = client.upload_image(image_path)

        if image_hash:
            plan = inject_image_hash(plan, image_hash)

        # 2) Create campaign
        campaign_id = client.create_campaign(plan.campaign, dry_run=dry_run)
        # 3) Create ad set
        adset_id = client.create_adset(plan.adset, campaign_id=campaign_id, dry_run=dry_run)
        # 4) Create creative
        creative_id = client.create_adcreative(plan.creative, dry_run=dry_run)
        # 5) Create ad
        ad_id = client.create_ad(plan.ad, adset_id=adset_id, creative_id=creative_id, dry_run=dry_run)

        result = {
            "idempotency_key": idem_key,
            "payload_sha256": payload_hash,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "campaign_id": campaign_id,
            "adset_id": adset_id,
            "creative_id": creative_id,
            "ad_id": ad_id,
        }

        # ✅ DO NOT store dry-run results, otherwise real runs get blocked with DRY_RUN ids
        if not dry_run:
            store.put(idem_key, payload_hash, result)

        print("[SUCCESS] Created objects:")
        print(json.dumps(result, indent=2))
        return result
    finally:
        if tmp_image_path and tmp_image_path.exists():
            try:
                tmp_image_path.unlink()
            except Exception:
                pass



# -----------------------------
# CLI
# -----------------------------

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="meta_ads_tool.py",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=textwrap.dedent(
            """
            Meta Marketing API Test Tool (Python)

            Examples:
              # 1) Validate token
              python meta_ads_tool.py whoami

              # 2) List ad accounts the token can see
              python meta_ads_tool.py list-adaccounts

              # 3) Get details about your ad account
              python meta_ads_tool.py adaccount

              # 4) Launch an end-to-end test ad from a plan JSON (default PAUSED)
              python meta_ads_tool.py launch --plan examples/traffic_image_ad.json

              # 5) Pause / activate objects
              python meta_ads_tool.py set-status --id <AD_ID> --status PAUSED
              python meta_ads_tool.py set-status --id <AD_ID> --status ACTIVE

              # 6) Update adset budget
              python meta_ads_tool.py update-adset-budget --adset-id <ID> --daily-budget 1500
            """
        ),
    )

    p.add_argument("--env", default=".env", help="Path to .env file (default: .env).")
    p.add_argument("--dry-run", action="store_true", help="Print requests without calling Meta.")
    p.add_argument("--store", default=".meta_idempotency.db", help="SQLite store path for idempotency (default: .meta_idempotency.db).")

    sub = p.add_subparsers(dest="cmd", required=True)

    sub.add_parser("whoami", help="GET /me?fields=id,name (validates token).")

    sp = sub.add_parser("list-adaccounts", help="List ad accounts visible to the token.")
    sp.add_argument("--limit", type=int, default=50)

    sub.add_parser("adaccount", help="Get details of META_AD_ACCOUNT_ID from env.")

    sub.add_parser("promote-pages", help="Get promote_pages for the ad account (helps find page IDs).")

    sub.add_parser("instagram-accounts", help="Get instagram_accounts for the ad account (helps find ig_actor_id).")

    sp = sub.add_parser("upload-image", help="Upload a local image to get image_hash.")
    sp.add_argument("--image-path", required=True)

    sp = sub.add_parser("launch", help="Run the full create flow from a plan JSON.")
    sp.add_argument("--plan", required=True)

    sp = sub.add_parser("get", help="Read an object by id.")
    sp.add_argument("--id", required=True)
    sp.add_argument("--fields", default="id,name,status,effective_status")

    sp = sub.add_parser("set-status", help="Set status for a campaign/adset/ad by id.")
    sp.add_argument("--id", required=True)
    sp.add_argument("--status", required=True, help="ACTIVE or PAUSED")

    sp = sub.add_parser("update-adset-budget", help="Update an ad set budget.")
    sp.add_argument("--adset-id", required=True)
    sp.add_argument("--daily-budget", type=int)
    sp.add_argument("--lifetime-budget", type=int)

    return p


def main(argv: Optional[List[str]] = None) -> int:
    argv = argv or sys.argv[1:]
    parser = build_parser()
    args = parser.parse_args(argv)

    # Load env file
    env_path = Path(args.env)
    if env_path.exists():
        load_dotenv(env_path, override=False)
    else:
        # It's ok; we might rely on actual env vars.
        pass

    try:
        cfg = MetaConfig.from_env()
    except Exception as e:
        print(f"[CONFIG ERROR] {e}", file=sys.stderr)
        print("Tip: copy config.example.env -> .env and fill it in.", file=sys.stderr)
        return 2

    # Normalize ad account id
    cfg = dataclasses.replace(cfg, ad_account_id=normalize_ad_account_id(cfg.ad_account_id))

    client = MetaClient(cfg)

    try:
        if args.cmd == "whoami":
            print(json.dumps(client.whoami(), indent=2))
            return 0

        if args.cmd == "list-adaccounts":
            print(json.dumps(client.list_adaccounts(limit=args.limit), indent=2))
            return 0

        if args.cmd == "adaccount":
            print(json.dumps(client.get_adaccount_details(), indent=2))
            return 0

        if args.cmd == "promote-pages":
            print(json.dumps(client.get_promote_pages(), indent=2))
            return 0

        if args.cmd == "instagram-accounts":
            print(json.dumps(client.get_instagram_accounts(), indent=2))
            return 0

        if args.cmd == "upload-image":
            h = client.upload_image(args.image_path) if not args.dry_run else "DRY_RUN_IMAGE_HASH"
            print(json.dumps({"image_hash": h}, indent=2))
            return 0

        if args.cmd == "launch":
            run_launch(cfg, args.plan, store_path=args.store, dry_run=args.dry_run)
            return 0

        if args.cmd == "get":
            print(json.dumps(client.get_object(args.id, fields=args.fields), indent=2))
            return 0

        if args.cmd == "set-status":
            print(json.dumps(client.set_status(args.id, args.status, dry_run=args.dry_run), indent=2))
            return 0

        if args.cmd == "update-adset-budget":
            print(
                json.dumps(
                    client.update_adset_budget(
                        args.adset_id,
                        daily_budget=args.daily_budget,
                        lifetime_budget=args.lifetime_budget,
                        dry_run=args.dry_run,
                    ),
                    indent=2,
                )
            )
            return 0

        print(f"Unknown command: {args.cmd}", file=sys.stderr)
        return 2

    except MetaAPIError as e:
        print("\n[MetaAPIError]", e, file=sys.stderr)
        if e.error:
            print(json.dumps(e.error, indent=2), file=sys.stderr)
        return 1
    except Exception as e:
        print("\n[ERROR]", e, file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
