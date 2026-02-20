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
from typing import Any, Dict, List, Optional, Tuple, Literal
import re
from urllib.parse import urlparse, parse_qs
from token_store import get_valid_access_token
import io
from PIL import Image

import requests

# -----------------------------
# Batching policy (schema v2)
# -----------------------------
# Goal: Prefer 4 unique videos per AdSet; if we only have 3, wait a bit and then allow 3.
# Defaults can be overridden via env vars (Railway-friendly):
#   BATCH_TARGET=4
#   BATCH_MIN=3
#   BATCH_FALLBACK_AFTER_S=60
#
# IMPORTANT: "wait time" only works if something calls this service again
# (or you run a worker that drains the queue periodically).

@dataclass(frozen=True)
class BatchPolicy:
    target: int = 4
    minimum: int = 3
    fallback_after_s: int = 60  # 1 minute


def get_batch_policy() -> BatchPolicy:
    return BatchPolicy(
        target=int(os.getenv("BATCH_TARGET", "4")),
        minimum=int(os.getenv("BATCH_MIN", "3")),
        fallback_after_s=int(os.getenv("BATCH_FALLBACK_AFTER_S", "60")),
    )


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def decide_flush_size(unique_count: int, oldest_created_at: datetime, policy: BatchPolicy) -> int:
    """Return the number of unique rows we should flush now (0 = don't flush yet)."""
    age_s = (utcnow() - oldest_created_at).total_seconds()
    if unique_count >= policy.target:
        return policy.target
    if unique_count >= policy.minimum and age_s >= policy.fallback_after_s:
        return policy.minimum
    return 0


def _parse_iso_datetime_utc(s: str | None) -> datetime:
    if not s:
        return utcnow()
    try:
        s = str(s).strip().replace("Z", "+00:00")
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return utcnow()


def get_group_oldest_created_at(
    qstore: Any,
    *,
    product: str,
    category: str,
    signature: str,
    rows: Optional[List[Any]] = None,
) -> datetime:
    """Best-effort 'oldest created_at' for a queue group.

    We try (in order):
      1) QueueItem.created_at (if your store returns it)
      2) Direct DB query against SQLite or Postgres backends (works with the current repo)
      3) Fallback to now (disables the wait-time behavior)
    """
    # 1) If rows carry created_at, use that.
    if rows:
        cands: List[datetime] = []
        for r in rows:
            v = getattr(r, "created_at", None)
            if isinstance(v, datetime):
                cands.append(v.astimezone(timezone.utc) if v.tzinfo else v.replace(tzinfo=timezone.utc))
            elif isinstance(v, str):
                cands.append(_parse_iso_datetime_utc(v))
        if cands:
            return min(cands)

    # 2) Query backend directly.
    try:
        # SQLite backend: StateStore has .db_path
        db_path = getattr(qstore, "db_path", None)
        if db_path:
            with sqlite3.connect(str(db_path)) as conn:
                cur = conn.execute(
                    """
                    SELECT MIN(created_at)
                    FROM queue_v2
                    WHERE product=? AND category=? AND signature=?
                    """,
                    (product, category, signature),
                )
                row = cur.fetchone()
            return _parse_iso_datetime_utc(row[0] if row else None)

        # Postgres backend: StateStorePG has .database_url and .table
        database_url = getattr(qstore, "database_url", None)
        table = getattr(qstore, "table", "queue_v2")
        if database_url:
            import psycopg  # local import so SQLite-only installs still work

            with psycopg.connect(database_url) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"""
                        SELECT MIN(created_at)
                        FROM {table}
                        WHERE product=%s AND category=%s AND signature=%s
                          AND (
                            state='queued'
                            OR (state='reserved' AND reserved_at < (now() - interval '30 minutes'))
                          )
                        """,
                        (product, category, signature),
                    )
                    row = cur.fetchone()
            return row[0].astimezone(timezone.utc) if row and row[0] else utcnow()
    except Exception:
        pass

    return utcnow()

# Hard-coded URL parameters appended to every ad creative.
DEFAULT_URL_TAGS = 'trc_mcmp_id={{campaign.id}}&trc_mag_id={{adset.id}}&trc_mad_id={{ad.id}}'


def merge_url_tags(existing: Optional[str]) -> str:
    """Append DEFAULT_URL_TAGS to any existing url_tags string."""
    base = (existing or "").strip()
    if not base:
        return DEFAULT_URL_TAGS
    if DEFAULT_URL_TAGS in base:
        return base
    if base.endswith("&"):
        return base + DEFAULT_URL_TAGS
    return base + "&" + DEFAULT_URL_TAGS


from dotenv import load_dotenv
from pydantic import BaseModel, Field, ValidationError, model_validator, field_validator


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

        account_id = os.getenv("META_AD_ACCOUNT_ID", "").strip()
        api_version = os.getenv("META_API_VERSION", "v21.0").strip() or "v21.0"
        app_id = os.getenv("META_APP_ID", "").strip() or None
        app_secret = os.getenv("META_APP_SECRET", "").strip() or None

        # NEW: choose token source
        token_source = os.getenv("META_TOKEN_SOURCE", "").strip().lower()  # set to "db" on Railway
        database_url = os.getenv("DATABASE_URL", "").strip()

        if token_source == "db":
            if not database_url:
                raise ValueError("META_TOKEN_SOURCE=db but DATABASE_URL is not set.")
            token = get_valid_access_token(database_url)
        else:
            token = os.getenv("META_ACCESS_TOKEN", "").strip()

        if not token:
            raise ValueError(
                "Missing access token. Set META_ACCESS_TOKEN or set META_TOKEN_SOURCE=db with a DB token row."
            )
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
                CREATE TABLE IF NOT EXISTS launches_v2 (
                    launch_key TEXT PRIMARY KEY,
                    payload_sha256 TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    campaign_id TEXT,
                    adset_id TEXT,
                    creative_id TEXT,
                    ad_id TEXT
                )
                """
            )

            # Cache so repeated runs don't scan Meta for campaign/adset IDs.
            # Also helps reduce duplicate creates under concurrent requests.
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS campaign_cache (
                    product TEXT PRIMARY KEY,
                    campaign_name TEXT NOT NULL,
                    campaign_id TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS adset_cache (
                    campaign_id TEXT NOT NULL,
                    batch_id TEXT NOT NULL,
                    bucket INTEGER NOT NULL,
                    adset_name TEXT NOT NULL,
                    adset_id TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (campaign_id, batch_id, bucket)
                )
                """
            )
            conn.commit()

    def get_cached_campaign_id(self, product: str) -> Optional[str]:
        with sqlite3.connect(self.db_path) as conn:
            cur = conn.execute(
                "SELECT campaign_id FROM campaign_cache WHERE product=?",
                (product,),
            )
            row = cur.fetchone()
        if not row:
            return None
        return str(row[0]) if row[0] else None

    def put_cached_campaign_id(self, product: str, campaign_name: str, campaign_id: str) -> None:
        now = datetime.now(timezone.utc).isoformat()
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO campaign_cache (product, campaign_name, campaign_id, updated_at)
                VALUES (?, ?, ?, ?)
                """,
                (product, campaign_name, campaign_id, now),
            )
            conn.commit()

    def delete_cached_campaign_id(self, product: str) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("DELETE FROM campaign_cache WHERE product=?", (product,))
            conn.commit()

    def get_cached_adset_id(self, campaign_id: str, batch_id: str, bucket: int) -> Optional[str]:
        with sqlite3.connect(self.db_path) as conn:
            cur = conn.execute(
                "SELECT adset_id FROM adset_cache WHERE campaign_id=? AND batch_id=? AND bucket=?",
                (campaign_id, batch_id, int(bucket)),
            )
            row = cur.fetchone()
        if not row:
            return None
        return str(row[0]) if row[0] else None

    def put_cached_adset_id(self, campaign_id: str, batch_id: str, bucket: int, adset_name: str, adset_id: str) -> None:
        now = datetime.now(timezone.utc).isoformat()
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO adset_cache (campaign_id, batch_id, bucket, adset_name, adset_id, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (campaign_id, batch_id, int(bucket), adset_name, adset_id, now),
            )
            conn.commit()

    def delete_cached_adset_id(self, campaign_id: str, batch_id: str, bucket: int) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("DELETE FROM adset_cache WHERE campaign_id=? AND batch_id=? AND bucket=?", (campaign_id, batch_id, int(bucket)))
            conn.commit()

    def get(self, key: str) -> Optional[dict]:
        with sqlite3.connect(self.db_path) as conn:
            cur = conn.execute(
                "SELECT launch_key, payload_sha256, created_at, campaign_id, adset_id, creative_id, ad_id FROM launches_v2 WHERE launch_key=?",
                (key,),
            )
            row = cur.fetchone()
        if not row:
            return None
        return {
            "launch_key": row[0],
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
                INSERT OR REPLACE INTO launches_v2
                (launch_key, payload_sha256, created_at, campaign_id, adset_id, creative_id, ad_id)
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

    def delete_launch(self, key: str) -> None:
        """Delete a stored launch key so a subsequent run can recreate it."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("DELETE FROM launches_v2 WHERE launch_key=?", (key,))
            conn.commit()


def build_idempotency_store(store_path: str):
    """Factory: SQLite (default) or Postgres (Railway).

    Enable Postgres store by setting:
      IDEMPOTENCY_STORE_SOURCE=db
      DATABASE_URL=...
    """
    source = (os.getenv("IDEMPOTENCY_STORE_SOURCE") or "").strip().lower()
    database_url = (os.getenv("DATABASE_URL") or "").strip()
    if source == "db" and database_url:
        from idempotency_store_pg import IdempotencyStorePG

        return IdempotencyStorePG(database_url)
    return IdempotencyStore(Path(store_path))


def build_queue_store(queue_db_path: str):
    """Factory: SQLite (default) or Postgres (Railway).

    Enable Postgres queue by setting:
      QUEUE_STORE_SOURCE=db
      DATABASE_URL=...
    """
    source = (os.getenv("QUEUE_STORE_SOURCE") or "").strip().lower()
    database_url = (os.getenv("DATABASE_URL") or "").strip()
    if source == "db" and database_url:
        from state_store_pg import StateStorePG

        return StateStorePG(database_url)

    from state_store import StateStore

    return StateStore(queue_db_path)


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
    
    @model_validator(mode="after")
    def _normalize_empty_strings(self) -> "AdSetSpec":
        # Make dataStructure often sends "" instead of omitting fields
        if self.bid_strategy is not None and not str(self.bid_strategy).strip():
            self.bid_strategy = None
        return self


class CreativeTextOptions(BaseModel):
    """Optional text variants for a single creative (Flexible Creative).

    If provided (or if lists are detected inside object_story_spec), the backend will
    build AdCreative.asset_feed_spec so Meta can pick the best performing variants.
    """

    primary_texts: List[str] = Field(default_factory=list)
    headlines: List[str] = Field(default_factory=list)
    descriptions: List[str] = Field(default_factory=list)

    @field_validator("primary_texts", "headlines", "descriptions", mode="before")
    @classmethod
    def _coerce_to_list(cls, v):
        if v is None:
            return []
        if isinstance(v, str):
            v = [v]
        if isinstance(v, list):
            out: List[str] = []
            for x in v:
                s = str(x).strip()
                if s:
                    out.append(s)
            return out
        # anything else: ignore
        return []


class CreativeSpec(BaseModel):
    name: str
    object_story_spec: Dict[str, Any]
    # Meta Ads Manager: Tracking > URL Parameters
    # In the API this is sent as AdCreative.url_tags
    url_tags: Optional[str] = None
    degrees_of_freedom_spec: Optional[Dict[str, Any]] = None

    # Optional text variants (either sent explicitly or derived from object_story_spec lists)
    text_options: Optional[CreativeTextOptions] = None

    # Internal (built by backend when text_options / list fields exist)
    asset_feed_spec: Optional[Dict[str, Any]] = None

    @field_validator("name", mode="before")
    @classmethod
    def _coerce_name(cls, v):
        # Make.com sometimes sends single-item arrays
        if isinstance(v, list) and len(v) == 1:
            return str(v[0])
        return v

class AdSpec(BaseModel):
    name: str
    status: str = "PAUSED"


class AssetsSpec(BaseModel):
    """Asset inputs.

    Backward compatible:
      - image_url / image_path / image_hash

    New (recommended):
      - media_url: single URL that can be image OR video; backend auto-detects.
      - media_type: optional override ('image' | 'video'). If omitted, backend detects.

    Video support:
      - video_url / video_path / video_id

    Notes:
      - For schema v2 batching, images are uploaded before enqueue (stable payload).
      - Videos are uploaded during the drain/post step (can take longer).
    """

    # Single URL (image OR video) - preferred
    media_url: Optional[str] = None
    media_type: Optional[Literal["image", "video"]] = None

    # Backward compatible image fields
    image_path: Optional[str] = None  # local file path
    image_hash: Optional[str] = None  # if you already have it
    image_url: Optional[str] = None   # http(s) URL to an image

    # Video fields
    video_path: Optional[str] = None  # local file path (mp4/mov)
    video_url: Optional[str] = None   # http(s) URL to a video
    video_id: Optional[str] = None    # if you already uploaded to Meta (AdVideo id)


class LaunchPlan(BaseModel):
    schema_version: int = 1

    # Required routing key (drives campaign selection)
    product: str = Field(description="Product key used for routing (green | lila | rosa).")

    # NEW (schema_version >= 2): content/category bucketing
    category: Optional[str] = Field(
        default=None,
        description="Content category used for AdSet batching (ai | ug). Required for schema_version >= 2.",
    )

    # NEW (schema_version >= 2): naming inputs (from Notion)
    product_label: Optional[str] = Field(
        default=None,
        description="Human readable product name used in names (e.g. 'Grüne Helfer'). Required for schema_version >= 2.",
    )
    product_code: Optional[str] = Field(
        default=None,
        description="Short product code used in names without hash signs (e.g. 'GH'). Required for schema_version >= 2.",
    )
    audience: Optional[str] = Field(
        default=None,
        description="Audience label used in AdSet name (e.g. 'M/W', 'W- 20+'). Required for schema_version >= 2.",
    )
    offer_page: Optional[str] = Field(
        default=None,
        description="Offer page marker appended to Ad name (e.g. 'LP259' or a URL). Required for schema_version >= 2.",
    )

    # NEW: Notion multi-select support for countries.
    # If provided, this overwrites adset.targeting.geo_locations.countries.
    countries: Optional[List[str] | str] = Field(
        default=None,
        description="Country codes (e.g. ['AT','DE'] or 'AT,DE'). Optional; if present overrides targeting.geo_locations.countries.",
    )

    # NEW: batching mode (schema_version >= 2 defaults to True)
    batching: Optional[bool] = Field(
        default=None,
        description="If true, the backend queues items and only creates AdSets when 3-4 unique videos are available (schema_version>=2).",
    )

    campaign: CampaignSpec
    adset: AdSetSpec
    creative: CreativeSpec
    ad: AdSpec
    assets: Optional[AssetsSpec] = None

    @model_validator(mode="after")
    def _check_assets(self) -> "LaunchPlan":
        # Assets are optional. If provided, require at least one valid input.
        if self.assets:
            a = self.assets

            # Back-compat: if old fields are used, map into media_url for detection.
            if not (a.media_url or "").strip():
                if (a.image_url or "").strip():
                    a.media_url = a.image_url
                elif (a.video_url or "").strip():
                    a.media_url = a.video_url

            has_any = any([
                bool((a.image_hash or "").strip()),
                bool((a.video_id or "").strip()),
                bool((a.image_path or "").strip()),
                bool((a.video_path or "").strip()),
                bool((a.media_url or "").strip()),
                bool((a.image_url or "").strip()),
                bool((a.video_url or "").strip()),
            ])
            if not has_any:
                raise ValueError(
                    "assets must include at least one of: image_hash, video_id, image_path, video_path, media_url, image_url, video_url"
                )

            # Safety: don't allow both image_hash and video_id.
            if (a.image_hash or "").strip() and (a.video_id or "").strip():
                raise ValueError("assets cannot include both image_hash and video_id. Choose one media type.")

            # If caller explicitly sets media_type, normalize it.
            if a.media_type:
                a.media_type = str(a.media_type).strip().lower()  # type: ignore[assignment]
                if a.media_type not in {"image", "video"}:
                    raise ValueError("assets.media_type must be 'image' or 'video'")

        return self

    @model_validator(mode="after")
    def _check_product(self) -> "LaunchPlan":
        v = (self.product or "").strip().lower()
        if v not in {"green", "lila", "rosa"}:
            raise ValueError("product must be one of: green, lila, rosa")
        self.product = v
        return self

    @model_validator(mode="after")
    def _normalize_schema_and_inputs(self) -> "LaunchPlan":
        # Default batching behavior
        if self.batching is None:
            self.batching = bool(self.schema_version >= 2)

        # schema_version >= 2 requires additional Notion-derived fields
        if self.schema_version >= 2:
            cat = (self.category or "").strip().lower()
            if cat not in {"ai", "ug"}:
                raise ValueError("category must be one of: ai, ug (required for schema_version >= 2)")
            self.category = cat

            for field_name in ("product_label", "product_code", "audience", "offer_page"):
                val = getattr(self, field_name)
                if not (val or "").strip():
                    raise ValueError(f"{field_name} is required for schema_version >= 2")
                setattr(self, field_name, str(val).strip())

            # Normalize countries: accept list or comma/space separated string
            countries = self.countries
            if isinstance(countries, str):
                parts = [p.strip().upper() for p in re.split(r"[,\s]+", countries) if p.strip()]
                self.countries = parts
            elif isinstance(countries, list):
                self.countries = [str(c).strip().upper() for c in countries if str(c).strip()]
            elif countries is None:
                self.countries = None

            # If countries provided, overwrite targeting.geo_locations.countries
            if self.countries:
                geo = self.adset.targeting.get("geo_locations")
                if not isinstance(geo, dict):
                    geo = {}
                    self.adset.targeting["geo_locations"] = geo
                geo["countries"] = list(self.countries)

        else:
            # Keep old behavior: if schema_version=1, category/naming fields are ignored
            self.category = self.category or None
            self.product_label = self.product_label or None
            self.product_code = self.product_code or None
            self.audience = self.audience or None
            self.offer_page = self.offer_page or None
            self.countries = self.countries or None
            if self.batching:
                # Don't allow batching on legacy schema (would be missing required fields)
                raise ValueError("batching is only supported for schema_version >= 2")

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

        # If we're using ABO (adset budget), ensure the campaign flag is explicitly set.
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
        self.video_base_url = f"https://graph-video.facebook.com/{cfg.api_version}"

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[dict] = None,
        data: Optional[dict] = None,
        files: Optional[dict] = None,
        max_retries: int = 2,
        use_video: bool = False,
    ) -> dict:
        base = self.video_base_url if use_video else self.base_url
        url = base + "/" + path.lstrip("/")
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
                params.setdefault("access_token", self.cfg.access_token)
            else:
                # if we're uploading files (multipart), send token via query params
                if files is not None:
                    params.setdefault("access_token", self.cfg.access_token)
                else:
                    data.setdefault("access_token", self.cfg.access_token)


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
    # Lookup helpers (for get-or-create)
    # -----------------------------

    def _get_all_pages(self, path: str, *, params: dict, max_pages: int = 8) -> List[dict]:
        """Collects up to `max_pages` pages for a Graph API edge."""
        out: List[dict] = []
        after: str | None = None
        for _ in range(max_pages):
            p = dict(params)
            if after:
                p["after"] = after
            payload = self._request("GET", path, params=p)
            data = payload.get("data") or []
            if isinstance(data, list):
                out.extend(data)
            cursors = ((payload.get("paging") or {}).get("cursors") or {})
            after = cursors.get("after")
            if not after:
                break
        return out

    def find_campaign_id_by_name(self, campaign_name: str, *, limit: int = 200) -> Optional[str]:
        acct = normalize_ad_account_id(self.cfg.ad_account_id)
        name = (campaign_name or "").strip()
        if not name:
            return None

        # Prefer server-side filtering if supported.
        params = {
            "fields": "id,name,status,effective_status",
            "limit": str(limit),
            "filtering": json.dumps([
                {"field": "name", "operator": "EQUAL", "value": name}
            ]),
        }
        rows = self._get_all_pages(f"/{acct}/campaigns", params=params)
        for r in rows:
            eff = (r.get("effective_status") or r.get("status") or "").strip().upper()
            if eff in {"ARCHIVED", "DELETED"}:
                continue

            if (r.get("name") or "").strip() == name and r.get("id"):
                return str(r["id"])
        return None

    def find_adset_id_by_name(self, campaign_id: str, adset_name: str, *, limit: int = 200) -> Optional[str]:
        cid = (campaign_id or "").strip()
        name = (adset_name or "").strip()
        if not cid or not name:
            return None

        params = {
            "fields": "id,name,status,effective_status",
            "limit": str(limit),
            "filtering": json.dumps([
                {"field": "name", "operator": "EQUAL", "value": name}
            ]),
        }
        rows = self._get_all_pages(f"/{cid}/adsets", params=params)
        for r in rows:
            eff = (r.get("effective_status") or r.get("status") or "").strip().upper()
            if eff in {"ARCHIVED", "DELETED"}:
                continue

            if (r.get("name") or "").strip() == name and r.get("id"):
                return str(r["id"])
        return None

    def list_adsets_in_campaign(self, campaign_id: str, *, fields: str = "id,name,status,effective_status", limit: int = 200, max_pages: int = 50) -> List[dict]:
        cid = (campaign_id or "").strip()
        if not cid:
            return []
        params = {"fields": fields, "limit": str(limit)}
        return self._get_all_pages(f"/{cid}/adsets", params=params, max_pages=max_pages)

    def count_adsets_in_campaign(self, campaign_id: str, *, max_pages: int = 50) -> int:
        rows = self.list_adsets_in_campaign(campaign_id, fields="id", limit=200, max_pages=max_pages)
        return len(rows)

    def get_max_adset_prefix_number(self, campaign_id: str, *, prefix_re: re.Pattern, max_pages: int = 50) -> int:
        """Parse AdSet name prefixes like '1028 Test // ...' and return max number."""
        rows = self.list_adsets_in_campaign(campaign_id, fields="name", limit=200, max_pages=max_pages)
        max_n = 0
        for r in rows:
            name = (r.get("name") or "").strip()
            m = prefix_re.match(name)
            if m:
                try:
                    n = int(m.group(1))
                    if n > max_n:
                        max_n = n
                except Exception:
                    pass
        return max_n

    # -----------------------------
    # Create flow (image -> creative -> ad)
    # -----------------------------

    def upload_image(
        self,
        image_path: str | None = None,
        *,
        image_bytes: bytes | None = None,
        filename: str = "image.jpg",
        ad_account_id: str | None = None,
    ) -> str:
        """
        Uploads an image and returns image_hash.
        Most reliable method: multipart field 'bytes' with a real JPEG content-type.
        """
        acct = normalize_ad_account_id(ad_account_id or self.cfg.ad_account_id)

        if image_bytes is None:
            if not image_path:
                raise ValueError("Provide image_path or image_bytes.")
            p = Path(image_path)
            if not p.exists():
                raise FileNotFoundError(f"Image file not found: {p}")
            image_bytes = p.read_bytes()
            filename = p.name or filename

        # IMPORTANT: use 'bytes' field with filename + correct mime
        # Meta expects the file under the multipart field name `filename`
        files = {"filename": (filename, image_bytes, "image/jpeg")}
        payload = self._request("POST", f"/{acct}/adimages", files=files, data={})


        images = payload.get("images") or {}
        if not images:
            raise MetaAPIError(f"Upload did not return images. Response: {payload}")

        first_key = next(iter(images.keys()))
        img_obj = images[first_key]
        image_hash = img_obj.get("hash") or first_key
        if not image_hash:
            raise MetaAPIError(f"Could not parse image_hash from response: {payload}")
        return image_hash



    def upload_video(
        self,
        *,
        video_path: str | None = None,
        video_bytes: bytes | None = None,
        filename: str = "video.mp4",
        file_url: str | None = None,
        name: str | None = None,
        ad_account_id: str | None = None,
        wait_for_ready: bool = True,
        wait_timeout_s: int = 600,
        wait_poll_s: int = 5,
    ) -> str:
        """Upload a video and return the AdVideo id.

        Supported inputs:
          - file_url: remote URL to a video file (preferred for Make/Notion)
          - video_path / video_bytes: local upload (dev/testing)

        Notes:
          - Uses graph-video domain for upload.
          - Optionally waits until encoding is ready.
        """
        acct = normalize_ad_account_id(ad_account_id or self.cfg.ad_account_id)

        if file_url:
            data: Dict[str, Any] = {"file_url": file_url}
            if name:
                data["name"] = name
            payload = self._request("POST", f"/{acct}/advideos", data=data, use_video=True)
            video_id = str(payload.get("id") or "").strip()
            if not video_id:
                raise MetaAPIError(f"Video upload did not return id. Response: {payload}")
        else:
            if video_bytes is None:
                if not video_path:
                    raise ValueError("Provide file_url, video_path, or video_bytes.")
                p = Path(video_path)
                if not p.exists():
                    raise FileNotFoundError(f"Video file not found: {p}")
                video_bytes = p.read_bytes()
                filename = p.name or filename

            # Multipart upload; Meta expects 'source'
            files = {"source": (filename, video_bytes, "video/mp4")}
            data: Dict[str, Any] = {}
            if name:
                data["name"] = name
            payload = self._request("POST", f"/{acct}/advideos", files=files, data=data, use_video=True)
            video_id = str(payload.get("id") or "").strip()
            if not video_id:
                raise MetaAPIError(f"Video upload did not return id. Response: {payload}")

        if wait_for_ready:
            self.wait_for_video_ready(video_id, timeout_s=wait_timeout_s, poll_s=wait_poll_s)

        return video_id


    def wait_for_video_ready(self, video_id: str, *, timeout_s: int = 600, poll_s: int = 5) -> None:
        """Poll AdVideo status until it's ready (or timeout)."""
        deadline = time.time() + max(10, int(timeout_s))
        last_status = None

        while time.time() < deadline:
            obj = self.get_object(str(video_id), fields="status")
            status = obj.get("status")

            # status is usually a dict: {"video_status":"processing"|"ready", ...}
            video_status = None
            if isinstance(status, dict):
                video_status = (status.get("video_status") or status.get("status") or "").strip().lower() or None
                last_status = status
            elif isinstance(status, str):
                video_status = status.strip().lower() or None
                last_status = status

            if video_status in {"ready", "complete", "completed"}:
                return
            if video_status in {"error", "failed"}:
                raise MetaAPIError(f"Video encoding failed for {video_id}. status={status}")

            time.sleep(max(1, int(poll_s)))

        raise MetaAPIError(f"Timed out waiting for video {video_id} to become ready. last_status={last_status}")

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

        # only send bid_strategy if it's non-empty
        if spec.bid_strategy:
            data["bid_strategy"] = str(spec.bid_strategy).strip()


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
        if spec.url_tags:
            data["url_tags"] = spec.url_tags
        if spec.degrees_of_freedom_spec is not None:
            data["degrees_of_freedom_spec"] = json.dumps(spec.degrees_of_freedom_spec)
        if spec.asset_feed_spec is not None:
            data["asset_feed_spec"] = json.dumps(spec.asset_feed_spec)

        if dry_run:
            print("[DRY RUN] create_adcreative payload:", json.dumps(data, indent=2))
            return "DRY_RUN_CREATIVE_ID"

        payload = self._request("POST", f"/{acct}/adcreatives", data=data)
        return payload["id"]

    def delete_object(self, object_id: str, *, dry_run: bool = False) -> dict:
        """Best-effort DELETE of a Graph object (ad/adset/creative).

        Not all objects support hard delete; if delete fails, callers should
        attempt to PAUSE as a safety fallback.
        """
        if dry_run:
            print("[DRY RUN] delete_object:", object_id)
            return {"id": object_id, "deleted": True}
        return self._request("DELETE", f"/{object_id}", data={})

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
    # Video helpers
    # -----------------------------

    def list_video_thumbnails(self, video_id: str, *, limit: int = 10) -> List[dict]:
        """Return available thumbnails for a video (each item typically includes 'uri')."""
        vid = (video_id or "").strip()
        if not vid:
            return []
        payload = self._request(
            "GET",
            f"/{vid}/thumbnails",
            params={"fields": "id,uri,is_preferred,width,height", "limit": str(limit)},
        )
        data = payload.get("data") or []
        return data if isinstance(data, list) else []


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


def _pick_video_thumbnail_uri(thumbnails: List[dict]) -> Optional[str]:
    """Pick the best thumbnail uri from Meta's thumbnail list."""
    if not thumbnails:
        return None
    # Prefer the one marked is_preferred
    for t in thumbnails:
        if t.get("is_preferred") and (t.get("uri") or "").strip():
            return str(t["uri"]).strip()
    # Otherwise use first usable uri
    for t in thumbnails:
        uri = (t.get("uri") or "").strip()
        if uri:
            return uri
    return None


def wait_for_video_thumbnail_uri(
    client: "MetaClient",
    video_id: str,
    *,
    timeout_s: int = 60,
    poll_s: int = 5,
) -> Optional[str]:
    """Poll Meta until a video thumbnail uri becomes available (or timeout)."""
    deadline = time.time() + max(1, int(timeout_s))
    while time.time() < deadline:
        try:
            thumbs = client.list_video_thumbnails(video_id, limit=10)
            uri = _pick_video_thumbnail_uri(thumbs)
            if uri:
                return uri
        except Exception:
            # If Meta is still processing the video, thumbnails may temporarily fail.
            pass
        time.sleep(max(1, int(poll_s)))
    return None


# -----------------------------
# Routing helpers (product -> campaign, creative name -> adset bucket)
# -----------------------------

_CREATIVE_NAME_RE = re.compile(r"^(?P<batch>\d+)[_\-](?P<idx>\d+)[_\-].+")


def resolve_campaign_name(product: str) -> str:
    """Maps product key -> canonical campaign name.

    Configure in env:
      PRODUCT_CAMPAIGN_NAMES='{"green":"GREEN | Main","lila":"LILA | Main","rosa":"ROSA | Main"}'
    """
    raw = (os.getenv("PRODUCT_CAMPAIGN_NAMES") or "").strip()
    if raw:
        try:
            mapping = json.loads(raw)
            if isinstance(mapping, dict) and product in mapping and str(mapping[product]).strip():
                return str(mapping[product]).strip()
        except Exception:
            # fall back to defaults below
            pass

    # Safe defaults if env is not set
    defaults = {
        "green": "GREEN | Main Campaign",
        "lila": "LILA | Main Campaign",
        "rosa": "ROSA | Main Campaign",
    }
    return defaults.get(product, f"{product.upper()} | Main Campaign")


def parse_batch_and_bucket(creative_name: str) -> tuple[str, int]:
    """Returns (batch_id, bucket) where bucket groups variants of 5.

    Expected creative name pattern:
      3807_0_Grüne Helfer
      3807_4_Grüne Helfer
    """
    name = (creative_name or "").strip()
    m = _CREATIVE_NAME_RE.match(name)
    if not m:
        raise ValueError(
            "creative.name must match '<batch>_<index>_<label>' (e.g., 3807_0_Grüne Helfer). "
            f"Got: {creative_name!r}"
        )
    batch = m.group("batch")
    idx = int(m.group("idx"))
    bucket = (idx // 5) + 1
    return batch, bucket


def build_adset_name(product: str, batch_id: str, bucket: int) -> str:
    return f"{product.upper()} | {batch_id} | {bucket:02d}"





# -------------------------------------------------------------------------
# V2 naming + batching helpers (schema_version >= 2)
# -------------------------------------------------------------------------

_VIDEO_NAME_RE_V2 = re.compile(r"^(?P<video>\d{4,})[_-](?P<variant>\d+)[_-](?P<label>.+)$")
_ADSET_PREFIX_RE_V2 = re.compile(r"^(\d+)\s+Test\s+//")

def parse_video_name_v2(name: str) -> tuple[str, int, str]:
    """Parse '<videoId>_<variant>_<label>' and return (video_id, variant_index, label)."""
    s = (name or "").strip()
    m = _VIDEO_NAME_RE_V2.match(s)
    if not m:
        raise ValueError(
            "creative.name must match '<videoId>_<variant>_<label>' (e.g., '3823_0_Grüne Helfer'). "
            f"Got: {name!r}"
        )
    full_id = m.group("video")
    video_id = full_id[:4]  # ✅ uniqueness is based on first 4 digits
    return video_id, int(m.group("variant")), m.group("label").strip()


def build_campaign_name_v2(product_label: str, product_code: str, variant_label: str) -> str:
    return f"VFB // {product_label} // #{product_code}# // {variant_label} // WC // ABO // Lowest Cost"

def resolve_campaign_variants_v2(product: str) -> list[str]:
    """Return the campaign variant labels for a product.

    Defaults:
      - green: ["TESTING", "GRAFIK TESTING"]
      - lila:  ["TESTING", "GRAFIK TESTING"]
      - rosa:  ["TESTING"]

    Override with:
      PRODUCT_CAMPAIGN_VARIANTS='{"green":["TESTING","GRAFIK TESTING"],"lila":["TESTING","GRAFIK TESTING"],"rosa":["TESTING"]}'
    """
    product = (product or "").strip().lower()
    raw = (os.getenv("PRODUCT_CAMPAIGN_VARIANTS") or "").strip()
    if raw:
        try:
            data = json.loads(raw)
            if isinstance(data, dict) and product in data and isinstance(data[product], list):
                variants = [str(v).strip() for v in data[product] if str(v).strip()]
                if variants:
                    return variants
        except Exception:
            pass

    defaults = {
        "green": ["TESTING", "GRAFIK TESTING"],
        "lila": ["TESTING", "GRAFIK TESTING"],
        "rosa": ["TESTING"],
    }
    return defaults.get(product, ["TESTING"])

def build_adset_name_v2(adset_number: int, product_label: str, product_code: str, audience: str) -> str:
    return f"{adset_number} Test // VFB // #{product_code}# // {product_label} // {audience} // Batch"

def build_ad_base_name_v2(video_id: str, variant: int, product_label: str) -> str:
    return f"{video_id}_{variant}_{product_label}"

def build_ad_name_v2(base_name: str, offer_page: str) -> str:
    offer_page = (offer_page or "").strip()
    return f"{base_name} // Video // Mehr dazu // {offer_page}"

def compute_adset_signature_v2(adset_spec: 'AdSetSpec') -> str:
    """Hash of adset_spec excluding name (prevents mixing different audiences/targeting)."""
    data = adset_spec.model_dump()
    data.pop("name", None)
    raw = json.dumps(data, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()[:16]


_BAD_EFFECTIVE_STATUSES = {'ARCHIVED', 'DELETED'}

def is_meta_object_usable(client: 'MetaClient', object_id: str) -> bool:
    """Returns True if object is usable (not archived/deleted)."""
    try:
        obj = client.get_object(object_id, fields='id,status,effective_status')
    except Exception:
        # If we can't read it, treat as unusable to force re-create.
        return False
    eff = (obj.get('effective_status') or obj.get('status') or '').strip().upper()
    return eff not in _BAD_EFFECTIVE_STATUSES

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


def _fetch_and_normalize_image_bytes(url: str, *, timeout_s: int = 30) -> tuple[bytes, str]:
    """
    Downloads an image URL, normalizes it to a Meta-safe JPEG,
    and returns (jpeg_bytes, filename).
    """
    url = (url or "").strip()
    if not url.lower().startswith(("http://", "https://")):
        raise ValueError("image_url must start with http:// or https://")

    # Normalize Google Drive "view" links -> direct download
    if "drive.google.com/file/d/" in url:
        file_id = url.split("/file/d/")[1].split("/")[0]
        url = f"https://drive.google.com/uc?export=download&id={file_id}"

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0 Safari/537.36"
        ),
        "Accept": "image/jpeg,image/png,image/*;q=0.8,*/*;q=0.5",
        "Accept-Language": "en-US,en;q=0.9",
    }

    # Use a session so cookies persist (needed for Drive confirm pages)
    sess = requests.Session()
    resp = sess.get(url, headers=headers, timeout=timeout_s, allow_redirects=True)
    resp.raise_for_status()

    ct = (resp.headers.get("Content-Type") or "").lower()
    raw = resp.content or b""

    # Guard: if it's tiny, it's almost certainly not an image (prevents Meta "file_size: 8")
    if len(raw) < 1024:
        raise ValueError(
            f"Downloaded content too small to be an image ({len(raw)} bytes). "
            f"content-type={ct} final_url={resp.url} preview={raw[:200]!r}"
        )

    # Guard: HTML means "not actually an image" (Notion/Drive permission pages)
    # Special case: Google Drive sometimes returns a "Virus scan warning" HTML interstitial.
    head = raw[:2048].lstrip().lower()
    is_html = ("text/html" in ct) or head.startswith(b"<!doctype html") or head.startswith(b"<html")
    if is_html:
        text = raw[:250_000].decode("utf-8", errors="ignore")
        lower = text.lower()

        # Drive virus scan warning page -> extract confirm token and retry download.
        if "virus scan warning" in lower and "google drive" in lower:
            # The interstitial format varies. Try several ways to extract a confirm token.
            confirm = None

            # 1) Old-style: token appears in a URL like ...&confirm=XYZ&id=...
            m = re.search(r"confirm=([0-9a-zA-Z_]+)", text)
            if m:
                confirm = m.group(1)

            # 2) New-style: hidden input in a form: <input type="hidden" name="confirm" value="t">
            if not confirm:
                m = re.search(r'name=["\']confirm["\']\s+value=["\']([^"\']+)["\']', text, flags=re.I)
                if m:
                    confirm = m.group(1)

            # 3) Cookie-based: Google sets a download_warning cookie; pass its value as confirm
            if not confirm:
                try:
                    for k, v in resp.cookies.items():
                        if k.startswith("download_warning") and v:
                            confirm = v
                            break
                except Exception:
                    pass

            if not confirm:
                raise ValueError(
                    f"URL returned Google Drive virus scan warning HTML but confirm token was not found. "
                    f"content-type={ct} final_url={resp.url} preview={raw[:200]!r}"
                )

            # Try to recover file id from the final URL (works for drive.usercontent.google.com/download?...id=...)
            qs = parse_qs(urlparse(resp.url).query)
            file_id = (qs.get("id") or [None])[0]
            if not file_id and "drive.google.com/file/d/" in url:
                try:
                    file_id = url.split("/file/d/")[1].split("/")[0]
                except Exception:
                    file_id = None

            if not file_id:
                raise ValueError(
                    f"Google Drive virus scan warning detected, but could not determine file id. final_url={resp.url}"
                )

            retry_url = "https://drive.google.com/uc"
            resp = sess.get(
                retry_url,
                params={"export": "download", "id": file_id, "confirm": confirm},
                headers=headers,
                timeout=timeout_s,
                allow_redirects=True,
            )
            resp.raise_for_status()
            ct = (resp.headers.get("Content-Type") or "").lower()
            raw = resp.content or b""
            head = raw[:2048].lstrip().lower()
            is_html = ("text/html" in ct) or head.startswith(b"<!doctype html") or head.startswith(b"<html")
            if is_html:
                raise ValueError(
                    f"Google Drive confirm retry still returned HTML (likely permissions). "
                    f"content-type={ct} final_url={resp.url} preview={raw[:200]!r}"
                )
        else:
            raise ValueError(
                f"URL returned HTML, not an image. content-type={ct} final_url={resp.url} preview={raw[:200]!r}"
            )


    try:
        img = Image.open(io.BytesIO(raw))
        img = img.convert("RGB")
    except Exception as e:
        raise ValueError(f"Downloaded bytes are not a valid image: {e}")

    out = io.BytesIO()
    img.save(out, format="JPEG", quality=92, optimize=True)
    jpeg_bytes = out.getvalue()

    if len(jpeg_bytes) < 1024:
        raise ValueError("JPEG re-encode failed; output too small")

    return jpeg_bytes, "image.jpg"



def _normalize_drive_download_url(url: str) -> str:
    """Normalize common Google Drive share URLs to direct download."""
    url = (url or "").strip()
    if "drive.google.com/file/d/" in url:
        file_id = url.split("/file/d/")[1].split("/")[0]
        return f"https://drive.google.com/uc?export=download&id={file_id}"
    return url


def _guess_media_type_from_url(url: str) -> Optional[str]:
    u = (url or "").lower()
    for ext in (".mp4", ".mov", ".m4v", ".webm"):
        if u.split("?")[0].endswith(ext):
            return "video"
    for ext in (".jpg", ".jpeg", ".png", ".webp", ".gif"):
        if u.split("?")[0].endswith(ext):
            return "image"
    return None


def _detect_media_type(url: str, *, timeout_s: int = 20) -> str:
    """Detect media type (image/video) from URL."""
    guess = _guess_media_type_from_url(url)
    if guess:
        return guess

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0 Safari/537.36"
        ),
        "Accept": "*/*",
    }

    try:
        h = requests.head(url, headers=headers, timeout=timeout_s, allow_redirects=True)
        ct = (h.headers.get("Content-Type") or "").lower()
        if ct.startswith("image/"):
            return "image"
        if ct.startswith("video/"):
            return "video"
    except Exception:
        pass

    try:
        headers2 = dict(headers)
        headers2["Range"] = "bytes=0-1023"
        g = requests.get(url, headers=headers2, timeout=timeout_s, allow_redirects=True)
        ct = (g.headers.get("Content-Type") or "").lower()
        if ct.startswith("image/"):
            return "image"
        if ct.startswith("video/"):
            return "video"
    except Exception:
        pass

    return "image"


def inject_video_id(plan: 'LaunchPlan', video_id: str, *, thumbnail_url: Optional[str] = None) -> 'LaunchPlan':
    """Inject AdVideo id into creative.object_story_spec.

    ⚠️ Meta requires `object_story_spec.video_data` to NOT include a top-level `link` field.
    The destination URL must be provided as `call_to_action.value.link`.

    This helper:
      - injects `video_id`
      - removes unsupported fields (top-level `link`)
      - ensures a thumbnail is present (`image_hash` or `image_url`) since Meta requires one
      - converts `link_data` (image-style spec) into valid `video_data`
    """

    def _first_text(v: Any) -> Optional[str]:
        if v is None:
            return None
        if isinstance(v, list) and v:
            v = v[0]
        s = str(v).strip()
        return s or None

    def _ensure_cta_link(vd: dict, link: Any) -> dict:
        link_s = _first_text(link)
        if not link_s:
            return vd
        cta = vd.get('call_to_action')
        if not isinstance(cta, dict):
            vd['call_to_action'] = {'type': 'LEARN_MORE', 'value': {'link': link_s}}
            return vd
        val = cta.get('value')
        if not isinstance(val, dict):
            val = {}
            cta['value'] = val
        val.setdefault('link', link_s)
        vd['call_to_action'] = cta
        return vd

    oss = plan.creative.object_story_spec
    if not isinstance(oss, dict):
        raise ValueError(
            'For video assets, creative.object_story_spec must be a dict containing video_data or link_data.'
        )

    # Case A: already has video_data -> sanitize
    vd = oss.get('video_data')
    if isinstance(vd, dict):
        vd = dict(vd)
        vd['video_id'] = video_id
        # Keep image_hash / image_url if present: Meta uses them as the video thumbnail.

        # Some builders mistakenly include `link` at the top level -> convert to CTA
        link = vd.pop('link', None)
        vd = _ensure_cta_link(vd, link)
        # Meta requires a thumbnail for video creatives: one of image_hash or image_url.
        if thumbnail_url and not (vd.get('image_hash') or vd.get('image_url')):
            vd['image_url'] = str(thumbnail_url).strip()

        oss.pop('link_data', None)
        oss['video_data'] = vd
        return plan

    # Case B: has link_data (image-style) -> convert to video_data
    ld = oss.get('link_data')
    if isinstance(ld, dict):
        ld = dict(ld)

        # Keep image_hash / image_url if present: Meta uses them as the video thumbnail.

        # Map only fields that are valid for video_data.
        new_vd: dict[str, Any] = {'video_id': video_id}
        msg = _first_text(ld.get('message'))
        name = _first_text(ld.get('name'))
        desc = _first_text(ld.get('description'))
        if msg is not None:
            new_vd['message'] = msg
        if name is not None:
            new_vd['title'] = name
        if desc is not None:
            new_vd['link_description'] = desc

        # Video thumbnail (Meta requires one): image_hash or image_url.
        thumb_hash = ld.get('image_hash')
        thumb_url = ld.get('image_url') or ld.get('picture')
        if thumb_hash:
            new_vd['image_hash'] = thumb_hash
        elif thumb_url:
            new_vd['image_url'] = thumb_url
        elif thumbnail_url:
            new_vd['image_url'] = str(thumbnail_url).strip()

        link = _first_text(ld.get('link'))
        cta = ld.get('call_to_action')
        if isinstance(cta, dict):
            cta = dict(cta)
            if link:
                val = cta.get('value')
                if not isinstance(val, dict):
                    val = {}
                val.setdefault('link', link)
                cta['value'] = val
            new_vd['call_to_action'] = cta
        elif link:
            new_vd['call_to_action'] = {'type': 'LEARN_MORE', 'value': {'link': link}}

        oss.pop('link_data', None)
        oss['video_data'] = new_vd
        return plan

    raise ValueError(
        'For video assets, creative.object_story_spec must contain video_data (preferred) or link_data (convertible).'
    )


def ensure_media_for_plan(client: 'MetaClient', cfg: MetaConfig, plan: 'LaunchPlan', *, dry_run: bool, stable_for_queue: bool) -> 'LaunchPlan':
    """Ensure the plan has an uploaded asset reference (image_hash or video_id) and inject it into the creative."""
    if not getattr(plan, "assets", None):
        return plan

    a = plan.assets

    if getattr(a, "image_hash", None):
        inject_image_hash(plan, a.image_hash)
        return plan
    if getattr(a, "video_id", None):
        # Meta requires a thumbnail for video creatives (image_hash or image_url in video_data).
        # If one is not already present in the payload, we try to fetch Meta-generated thumbnails
        # for this uploaded video and inject the first usable uri.
        thumb_url = None
        if not dry_run:
            oss = plan.creative.object_story_spec
            thumb_present = False
            if isinstance(oss, dict):
                vd = oss.get('video_data')
                if isinstance(vd, dict) and (vd.get('image_hash') or vd.get('image_url')):
                    thumb_present = True
                ld = oss.get('link_data')
                if isinstance(ld, dict) and (ld.get('image_hash') or ld.get('image_url') or ld.get('picture')):
                    thumb_present = True
            if not thumb_present:
                t_timeout = int((os.getenv('VIDEO_THUMBNAIL_TIMEOUT_S', '60') or '60').strip() or '60')
                t_poll = int((os.getenv('VIDEO_THUMBNAIL_POLL_S', '5') or '5').strip() or '5')
                thumb_url = wait_for_video_thumbnail_uri(client, a.video_id, timeout_s=t_timeout, poll_s=t_poll)

        inject_video_id(plan, a.video_id, thumbnail_url=thumb_url)
        return plan

    media_url = (a.media_url or a.image_url or a.video_url or "").strip() or None
    media_type = (a.media_type or "").strip().lower() or None

    if media_url:
        media_url = _normalize_drive_download_url(media_url)

    if not media_type and media_url:
        media_type = _detect_media_type(media_url, timeout_s=min(20, int(cfg.timeout_s)))

    if not media_type:
        if a.video_path:
            media_type = "video"
        elif a.image_path:
            media_type = "image"

    if not media_type:
        return plan

    if media_type == "image":
        image_hash = None
        if a.image_path:
            image_hash = "DRY_RUN_IMAGE_HASH" if dry_run else client.upload_image(image_path=a.image_path)
        elif media_url:
            if dry_run:
                image_hash = "DRY_RUN_IMAGE_HASH"
            else:
                img_bytes, fname = _fetch_and_normalize_image_bytes(media_url, timeout_s=cfg.timeout_s)
                image_hash = client.upload_image(image_bytes=img_bytes, filename=fname)

        if image_hash:
            a.image_hash = image_hash
            a.image_url = None
            a.media_url = None
            a.image_path = None
            inject_image_hash(plan, image_hash)
        return plan

    if media_type == "video" and stable_for_queue:
        return plan

    if media_type == "video":
        wait = (os.getenv("VIDEO_WAIT_FOR_READY", "true") or "true").strip().lower() not in {"0", "false", "no"}
        timeout_s = int((os.getenv("VIDEO_WAIT_TIMEOUT_S", "600") or "600").strip() or "600")
        poll_s = int((os.getenv("VIDEO_WAIT_POLL_S", "5") or "5").strip() or "5")

        video_id = None
        if a.video_path:
            video_id = "DRY_RUN_VIDEO_ID" if dry_run else client.upload_video(video_path=a.video_path, name=plan.creative.name, wait_for_ready=wait, wait_timeout_s=timeout_s, wait_poll_s=poll_s)
        elif media_url:
            video_id = "DRY_RUN_VIDEO_ID" if dry_run else client.upload_video(file_url=media_url, name=plan.creative.name, wait_for_ready=wait, wait_timeout_s=timeout_s, wait_poll_s=poll_s)

        if video_id:
            a.video_id = video_id
            a.video_url = None
            a.media_url = None
            a.video_path = None
            a.image_url = None
            a.image_path = None
            thumb_url = None
            if not dry_run:
                t_timeout = int((os.getenv('VIDEO_THUMBNAIL_TIMEOUT_S', '60') or '60').strip() or '60')
                t_poll = int((os.getenv('VIDEO_THUMBNAIL_POLL_S', '5') or '5').strip() or '5')
                thumb_url = wait_for_video_thumbnail_uri(client, video_id, timeout_s=t_timeout, poll_s=t_poll)
            inject_video_id(plan, video_id, thumbnail_url=thumb_url)

        return plan

    return plan


def apply_flexible_text_variants(plan: 'LaunchPlan') -> 'LaunchPlan':
    """Normalize list-valued text fields in object_story_spec and, if present,
    build AdCreative.asset_feed_spec to enable multiple text options in a single creative.

    Supports Make.com payloads where link_data fields are arrays (as in Ads Manager UI):
      - link_data.message: [ ... ] -> bodies
      - link_data.name: [ ... ] -> titles
      - link_data.description: [ ... ] -> descriptions
      - link_data.link: ["https://..."] -> first element used as destination

    Also supports explicit creative.text_options (CreativeTextOptions).

    This function is idempotent and safe to call multiple times.
    """

    oss = plan.creative.object_story_spec
    if not isinstance(oss, dict):
        return plan

    # Merge options from explicit text_options and any list-valued fields in object_story_spec.
    opts = plan.creative.text_options or CreativeTextOptions()

    def _coerce_str_list(v: Any) -> List[str]:
        if v is None:
            return []
        if isinstance(v, str):
            s = v.strip()
            return [s] if s else []
        if isinstance(v, list):
            out: List[str] = []
            for x in v:
                s = str(x).strip()
                if s:
                    out.append(s)
            return out
        return []

    def _first_str(v: Any) -> Optional[str]:
        arr = _coerce_str_list(v)
        return arr[0] if arr else None

    link_url: Optional[str] = None
    cta_type: Optional[str] = None
    image_hash: Optional[str] = None
    video_id: Optional[str] = None
    thumb_hash: Optional[str] = None
    thumb_url: Optional[str] = None

    # --- link_data (image/link creatives) ---
    ld = oss.get("link_data")
    if isinstance(ld, dict):
        # URL: accept string or [string]
        link_vals = _coerce_str_list(ld.get("link"))
        if link_vals:
            link_url = link_vals[0]
            ld["link"] = link_url
        elif isinstance(ld.get("link"), str):
            link_url = (ld.get("link") or "").strip() or None

        # Text variants: accept string or list
        opts.primary_texts.extend([t for t in _coerce_str_list(ld.get("message")) if t])
        opts.headlines.extend([t for t in _coerce_str_list(ld.get("name")) if t])
        opts.descriptions.extend([t for t in _coerce_str_list(ld.get("description")) if t])

        # Normalize the object_story_spec to a single default value (first option)
        msg0 = _first_str(ld.get("message")) or (opts.primary_texts[0] if opts.primary_texts else None)
        name0 = _first_str(ld.get("name")) or (opts.headlines[0] if opts.headlines else None)
        desc0 = _first_str(ld.get("description")) or (opts.descriptions[0] if opts.descriptions else None)
        if msg0:
            ld["message"] = msg0
        if name0:
            ld["name"] = name0
        if desc0:
            ld["description"] = desc0

        cta = ld.get("call_to_action")
        if isinstance(cta, dict):
            cta_type = (cta.get("type") or "").strip() or None
            # Ensure CTA value.link exists (helps some accounts/placements)
            if link_url:
                val = cta.get("value")
                if not isinstance(val, dict):
                    val = {}
                val.setdefault("link", link_url)
                cta["value"] = val
            ld["call_to_action"] = cta
        else:
            if link_url:
                ld["call_to_action"] = {"type": "LEARN_MORE", "value": {"link": link_url}}
                cta_type = "LEARN_MORE"

        image_hash = (ld.get("image_hash") or "").strip() or None

        oss["link_data"] = ld

    # --- video_data (video creatives) ---
    vd = oss.get("video_data")
    if isinstance(vd, dict):
        # Remove unsupported top-level link (Meta rejects it)
        if "link" in vd:
            vd.pop("link", None)

        video_id = (vd.get("video_id") or "").strip() or None
        thumb_hash = (vd.get("image_hash") or "").strip() or None
        thumb_url = (vd.get("image_url") or "").strip() or None

        opts.primary_texts.extend([t for t in _coerce_str_list(vd.get("message")) if t])
        opts.headlines.extend([t for t in _coerce_str_list(vd.get("title")) if t])
        opts.descriptions.extend([t for t in _coerce_str_list(vd.get("link_description")) if t])

        msg0 = _first_str(vd.get("message")) or (opts.primary_texts[0] if opts.primary_texts else None)
        title0 = _first_str(vd.get("title")) or (opts.headlines[0] if opts.headlines else None)
        desc0 = _first_str(vd.get("link_description")) or (opts.descriptions[0] if opts.descriptions else None)
        if msg0:
            vd["message"] = msg0
        if title0:
            vd["title"] = title0
        if desc0:
            vd["link_description"] = desc0

        cta = vd.get("call_to_action")
        if isinstance(cta, dict):
            cta_type = (cta.get("type") or "").strip() or cta_type
            val = cta.get("value")
            if isinstance(val, dict):
                link_url = (val.get("link") or "").strip() or link_url
        oss["video_data"] = vd

    # De-dupe while preserving order (Meta doesn't need duplicates)
    def _dedupe(seq: List[str]) -> List[str]:
        seen = set()
        out = []
        for x in seq:
            if x in seen:
                continue
            seen.add(x)
            out.append(x)
        return out

    opts.primary_texts = _dedupe(opts.primary_texts)
    opts.headlines = _dedupe(opts.headlines)
    opts.descriptions = _dedupe(opts.descriptions)
    plan.creative.text_options = opts

    # Build asset_feed_spec only if we have 2+ variants in any text field.
    wants_multi = (len(opts.primary_texts) > 1) or (len(opts.headlines) > 1) or (len(opts.descriptions) > 1)
    if wants_multi:
        if not link_url:
            # Link URL required for flexible creatives.
            # If not present, keep normalized single texts and skip asset feed.
            plan.creative.object_story_spec = oss
            return plan

        afs: Dict[str, Any] = {
            "link_urls": [{"website_url": link_url}],
            "call_to_action_types": [cta_type or "LEARN_MORE"],
        }
        if opts.primary_texts:
            afs["bodies"] = [{"text": t} for t in opts.primary_texts]
        if opts.headlines:
            afs["titles"] = [{"text": t} for t in opts.headlines]
        if opts.descriptions:
            afs["descriptions"] = [{"text": t} for t in opts.descriptions]

        # Attach media for the feed so Meta can render variations.
        if image_hash:
            afs["images"] = [{"hash": image_hash}]
        if video_id:
            vobj: Dict[str, Any] = {"video_id": video_id}
            if thumb_hash:
                vobj["thumbnail_hash"] = thumb_hash
            elif thumb_url:
                vobj["thumbnail_url"] = thumb_url
            afs["videos"] = [vobj]

        plan.creative.asset_feed_spec = afs

        # Opt-in to standard enhancements unless caller already set something.
        if plan.creative.degrees_of_freedom_spec is None:
            plan.creative.degrees_of_freedom_spec = {
                "creative_features_spec": {"standard_enhancements": {"enroll_status": "OPT_IN"}}
            }

    plan.creative.object_story_spec = oss
    return plan


def ensure_image_hash_for_plan(client, cfg, plan, *, dry_run: bool):
    """Backward-compatible wrapper (image-only)."""
    return ensure_media_for_plan(client, cfg, plan, dry_run=dry_run, stable_for_queue=False)



def _run_launch_plan_v1(
    cfg: MetaConfig,
    plan: LaunchPlan,
    *,
    store_path: str = ".meta_idempotency.db",
    dry_run: bool = False,
) -> dict:
    """Runs a validated LaunchPlan directly (API-friendly)."""
    client = MetaClient(cfg)
    store = build_idempotency_store(store_path)

    # Always inject tracking URL parameters
    plan.creative.url_tags = merge_url_tags(plan.creative.url_tags)

    # Deterministic internal key (no idempotency_key field required from clients)
    launch_key_raw = f"{plan.product}::{plan.creative.name}::{plan.ad.name}".encode("utf-8")
    launch_key = "launch:" + hashlib.sha256(launch_key_raw).hexdigest()[:24]

    plan_dict = plan.model_dump()
    payload_hash = sha256_json(plan_dict)

    existing = store.get(launch_key)
    if existing:
        # Mode 1 (recommended): verify the stored result still exists in Meta and is usable.
        # This prevents the local store from "blocking" a rerun if you deleted/archived objects in Ads Manager.
        if dry_run:
            print(f"[IDEMPOTENT HIT] Found existing launch for key={launch_key}")
            return existing

        ad_id = existing.get("ad_id")
        if ad_id and is_meta_object_usable(client, str(ad_id)):
            print(f"[IDEMPOTENT HIT] Found existing launch for key={launch_key}")
            return existing

        # Stale hit: the ad is missing or archived/deleted in Meta. Ignore the cached launch and recreate.
        print(
            f"[IDEMPOTENT STALE] Local launch exists but Meta ad is missing/archived; recreating for key={launch_key}"
        )
        store.delete_launch(launch_key)

    # 1) Resolve assets (image/video) if provided
    plan = ensure_media_for_plan(client, cfg, plan, dry_run=dry_run, stable_for_queue=False)

    # 2) Get-or-create campaign based on product
    campaign_name = resolve_campaign_name(plan.product)

    # Optional: persist campaign IDs per product in Postgres (Railway) so archived IDs don't get reused.
    # Enable with: CAMPAIGN_ROUTE_SOURCE=db and DATABASE_URL
    database_url = (os.getenv("DATABASE_URL") or "").strip()
    campaign_route_source = (os.getenv("CAMPAIGN_ROUTE_SOURCE") or "").strip().lower()
    use_campaign_db = bool(database_url) and campaign_route_source == "db"

    if use_campaign_db:
        from campaign_store import (
            ensure_campaign_routes_table_v1,
            get_campaign_id_v1,
            upsert_campaign_id_v1,
            clear_campaign_id_v1,
        )
        ensure_campaign_routes_table_v1(database_url)

    if dry_run:
        print(f"[DRY RUN] get-or-create campaign for product={plan.product} name={campaign_name!r}")
        campaign_id = "DRY_RUN_CAMPAIGN_ID"
    else:
        campaign_id = None

        # (A) DB route (highest priority)
        if use_campaign_db:
            campaign_id = get_campaign_id_v1(database_url, plan.product)
            if campaign_id and not is_meta_object_usable(client, campaign_id):
                clear_campaign_id_v1(database_url, plan.product)
                campaign_id = None

        # (B) Local cache
        if not campaign_id:
            campaign_id = store.get_cached_campaign_id(plan.product)
            if campaign_id and not is_meta_object_usable(client, campaign_id):
                store.delete_cached_campaign_id(plan.product)
                campaign_id = None

        # (C) Meta lookup by name (ignores archived/deleted in helper)
        if not campaign_id:
            campaign_id = client.find_campaign_id_by_name(campaign_name)
            if campaign_id:
                store.put_cached_campaign_id(plan.product, campaign_name, campaign_id)
                if use_campaign_db:
                    upsert_campaign_id_v1(database_url, plan.product, campaign_id)

        # (D) Create new if still missing
        if not campaign_id:
            campaign_spec = plan.campaign.model_copy(update={"name": campaign_name})
            campaign_id = client.create_campaign(campaign_spec, dry_run=False)
            store.put_cached_campaign_id(plan.product, campaign_name, campaign_id)
            if use_campaign_db:
                upsert_campaign_id_v1(database_url, plan.product, campaign_id)

        # Final safety: never proceed with an archived/deleted campaign
        if campaign_id and not is_meta_object_usable(client, campaign_id):
            if use_campaign_db:
                clear_campaign_id_v1(database_url, plan.product)
            store.delete_cached_campaign_id(plan.product)

            campaign_spec = plan.campaign.model_copy(update={"name": campaign_name})
            campaign_id = client.create_campaign(campaign_spec, dry_run=False)
            store.put_cached_campaign_id(plan.product, campaign_name, campaign_id)
            if use_campaign_db:
                upsert_campaign_id_v1(database_url, plan.product, campaign_id)

    # 3) Get-or-create adset: group variants of 5 into a bucket
    batch_id, bucket = parse_batch_and_bucket(plan.creative.name)
    adset_name = build_adset_name(plan.product, batch_id, bucket)

    if dry_run:
        print(f"[DRY RUN] get-or-create adset campaign_id={campaign_id} name={adset_name!r}")
        adset_id = "DRY_RUN_ADSET_ID"
    else:
        adset_id = store.get_cached_adset_id(campaign_id, batch_id, bucket)
        if adset_id and not is_meta_object_usable(client, adset_id):
            store.delete_cached_adset_id(campaign_id, batch_id, bucket)
            adset_id = None

        if not adset_id:
            adset_id = client.find_adset_id_by_name(campaign_id, adset_name)
            if adset_id:
                store.put_cached_adset_id(campaign_id, batch_id, bucket, adset_name, adset_id)

        # Extra safety: never proceed with archived/deleted adsets
        if adset_id and not is_meta_object_usable(client, adset_id):
            store.delete_cached_adset_id(campaign_id, batch_id, bucket)
            adset_id = None

        if not adset_id:
            adset_spec = plan.adset.model_copy(update={"name": adset_name})
            adset_id = client.create_adset(adset_spec, campaign_id=campaign_id, dry_run=False)
            store.put_cached_adset_id(campaign_id, batch_id, bucket, adset_name, adset_id)

    # 4) Create creative (supports flexible text options)
    plan = apply_flexible_text_variants(plan)
    creative_id = client.create_adcreative(plan.creative, dry_run=dry_run)
    # 5) Create ad
    ad_id = client.create_ad(plan.ad, adset_id=adset_id, creative_id=creative_id, dry_run=dry_run)

    result = {
        "launch_key": launch_key,
        "payload_sha256": payload_hash,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "product": plan.product,
        "campaign_name": campaign_name,
        "adset_name": adset_name,
        "campaign_id": campaign_id,
        "adset_id": adset_id,
        "creative_id": creative_id,
        "ad_id": ad_id,
    }

    # ✅ DO NOT store dry-run results, otherwise real runs get blocked with DRY_RUN ids
    if not dry_run:
        store.put(launch_key, payload_hash, result)

    print("[SUCCESS] Created objects:")
    print(json.dumps(result, indent=2))
    return result









def _drain_queue_group_v2(
    cfg: MetaConfig,
    *,
    qstore: Any,
    store: Any,
    product: str,
    category: str,
    signature: str,
    dry_run: bool = False,
) -> dict:
    """Drain (flush) a single v2 queue group.

    This is the same batching logic used by the /run endpoint, but without enqueuing.
    It enables a separate worker process to periodically flush groups once the
    time-based fallback becomes eligible (e.g. allow 3 after 60s).
    """
    from campaign_store import (
        ensure_campaign_routes_table,
        get_campaign_ids,
        upsert_campaign_id,
        clear_campaign_id,
    )

    client = MetaClient(cfg)
    policy = get_batch_policy()

    # Load group rows
    rows = qstore.fetch_group(product=product, category=category, signature=signature, limit=500)
    if not rows:
        return {
            "mode": "batch",
            "queued": True,
            "product": product,
            "category": category,
            "signature": signature,
            "message": "No queued rows for this group.",
        }

    # Helper: remove rows that already exist (idempotency) so they don't block batching.
    def _is_row_already_launched(payload: dict) -> bool:
        try:
            p = LaunchPlan.model_validate(payload)
        except Exception:
            return False
        base_name = p.creative.name
        # Rebuild final names so launch_key is stable
        vid, var, _ = parse_video_name_v2(base_name)
        base = build_ad_base_name_v2(vid, var, p.product_label or "")
        ad_name = build_ad_name_v2(base, p.offer_page or "")
        launch_key_raw = f"{p.product}::{base}::{ad_name}".encode("utf-8")
        launch_key = "launch:" + hashlib.sha256(launch_key_raw).hexdigest()[:24]
        existing = store.get(launch_key)
        if not existing:
            return False
        if dry_run:
            return True
        ad_id = existing.get("ad_id")
        return bool(ad_id and is_meta_object_usable(client, str(ad_id)))

    stale_ids = [r.id for r in rows if _is_row_already_launched(r.payload)]
    if stale_ids and not dry_run:
        qstore.delete_ids(stale_ids)
        rows = [r for r in rows if r.id not in set(stale_ids)]
        if not rows:
            return {
                "mode": "batch",
                "queued": True,
                "product": product,
                "category": category,
                "signature": signature,
                "message": "Group rows were all already launched; queue cleaned.",
            }

    # Validate group invariants (safety): product_label/product_code/audience must match.
    group_plans = []
    for r in rows[:10]:
        try:
            group_plans.append(LaunchPlan.model_validate(r.payload))
        except Exception:
            pass
    if not group_plans:
        return {
            "mode": "batch",
            "queued": True,
            "product": product,
            "category": category,
            "signature": signature,
            "message": "Could not parse LaunchPlan payloads in this group.",
        }

    group_plan = group_plans[0]
    keyset = {(p.product_label, p.product_code, p.audience) for p in group_plans}
    if len(keyset) > 1:
        return {
            "mode": "batch",
            "queued": True,
            "product": product,
            "category": category,
            "signature": signature,
            "message": "Safety stop: mixed product_label/product_code/audience detected inside the same group. Fix signature/grouping.",
            "samples": [
                {"product_label": p.product_label, "product_code": p.product_code, "audience": p.audience}
                for p in group_plans
            ],
        }

    # Compute uniqueness
    by_video: Dict[str, List[int]] = {}
    for r in rows:
        by_video.setdefault(r.video_id, []).append(r.id)

    duplicates = sorted([vid for vid, ids in by_video.items() if len(ids) > 1])
    unique_video_ids = list(by_video.keys())

    oldest_created_at = get_group_oldest_created_at(
        qstore,
        product=product,
        category=category,
        signature=signature,
        rows=rows,
    )
    flush_size = decide_flush_size(len(unique_video_ids), oldest_created_at, policy)

    if flush_size == 0:
        age_s = int((utcnow() - oldest_created_at).total_seconds())
        msg = (
            f"Queued (aim {policy.target} unique; allow {policy.minimum} after {policy.fallback_after_s}s). "
            f"Unique={len(unique_video_ids)}, oldest_age_s={age_s}."
        )
        if duplicates:
            msg += " Duplicates in queue: " + ", ".join(duplicates)
        return {
            "mode": "batch",
            "queued": True,
            "product": product,
            "category": category,
            "signature": signature,
            "unique_videos_in_queue": len(unique_video_ids),
            "duplicates_in_queue": duplicates,
            "oldest_age_s": age_s,
            "policy": {
                "target": policy.target,
                "minimum": policy.minimum,
                "fallback_after_s": policy.fallback_after_s,
            },
            "message": msg,
        }
    # Resolve campaign IDs (DB first), then balance between the two campaigns for green/lila.
    route_source = (os.getenv("CAMPAIGN_ROUTE_SOURCE") or "").strip().lower()
    database_url = (os.getenv("DATABASE_URL") or "").strip() or None

    required_slots = 2 if product in {"green", "lila"} else 1
    variant_labels = resolve_campaign_variants_v2(product)

    ids_by_slot: Dict[int, str] = {}
    if route_source == "db" and database_url:
        ensure_campaign_routes_table(database_url)
        ids_by_slot = get_campaign_ids(database_url, product)

    campaign_ids: List[str] = []
    for slot in range(1, required_slots + 1):
        cid = ids_by_slot.get(slot)
        if cid and not dry_run:
            obj = client.get_object(cid, fields="id,name,status,effective_status")
            eff = (obj.get("effective_status") or obj.get("status") or "").strip().upper()
            if eff in _BAD_EFFECTIVE_STATUSES:
                if database_url:
                    clear_campaign_id(database_url, product, slot=slot)
                cid = None

        if not cid:
            variant_label = variant_labels[slot - 1] if slot - 1 < len(variant_labels) else variant_labels[0]
            cname = build_campaign_name_v2(group_plan.product_label or "", group_plan.product_code or "", variant_label)
            cid = client.find_campaign_id_by_name(cname)
            if not cid:
                camp_spec = group_plan.campaign.model_copy(deep=True)
                camp_spec.name = cname
                cid = client.create_campaign(camp_spec, dry_run=dry_run)
            if route_source == "db" and database_url and cid and not dry_run:
                upsert_campaign_id(database_url, product, slot, cid)

        campaign_ids.append(cid)

    chosen_campaign_id = campaign_ids[0]
    if len(campaign_ids) > 1 and not dry_run:
        counts = [(cid, client.count_adsets_in_campaign(cid)) for cid in campaign_ids]
        counts.sort(key=lambda x: x[1])
        chosen_campaign_id = counts[0][0]

    # Determine next AdSet number (max across campaigns for this product)
    next_adset_number = 1
    if not dry_run:
        max_n = 0
        for cid in campaign_ids:
            try:
                n = client.get_max_adset_prefix_number(cid, prefix_re=_ADSET_PREFIX_RE_V2)
                if n > max_n:
                    max_n = n
            except Exception:
                pass
        next_adset_number = max_n + 1

    created_batches: List[dict] = []

    safety = 0
    while safety < 20:
        safety += 1

        remaining_rows = qstore.fetch_group(product=product, category=category, signature=signature, limit=500)
        if not remaining_rows:
            break

        by_video = {}
        for r in remaining_rows:
            by_video.setdefault(r.video_id, []).append(r.id)
        unique_video_ids = list(by_video.keys())

        oldest_created_at = get_group_oldest_created_at(
            qstore,
            product=product,
            category=category,
            signature=signature,
            rows=remaining_rows,
        )
        flush_size = decide_flush_size(len(unique_video_ids), oldest_created_at, policy)
        if flush_size == 0:
            break

        # Select exactly `flush_size` unique videos (FIFO by id).
        seen: set[str] = set()
        selected: List[Any] = []
        for r in remaining_rows:
            if r.video_id in seen:
                continue
            selected.append(r)
            seen.add(r.video_id)
            if len(selected) >= flush_size:
                break

        if len(selected) < flush_size:
            break

        selected_ids = [int(r.id) for r in selected]
        reserved_ids = selected_ids
        if not dry_run and hasattr(qstore, "reserve_ids"):
            try:
                reserved_ids = list(qstore.reserve_ids(selected_ids))  # type: ignore[attr-defined]
            except Exception:
                reserved_ids = selected_ids

            if len(reserved_ids) < flush_size:
                if hasattr(qstore, "unreserve_ids") and reserved_ids:
                    try:
                        qstore.unreserve_ids(reserved_ids)  # type: ignore[attr-defined]
                    except Exception:
                        pass
                continue

        try:
            # -----------------------------------------------------------------
            # IMPORTANT: avoid creating an AdSet unless we can create ALL ads for
            # this batch. This prevents "1 ad inside an adset" partial batches.
            # -----------------------------------------------------------------

            rollback_on_failure = (os.getenv("ROLLBACK_ON_FAILURE", "true") or "true").strip().lower() not in {"0", "false", "no"}

            def _safe_pause(obj_id: str):
                try:
                    client.set_status(obj_id, "PAUSED", dry_run=dry_run)
                except Exception:
                    pass

            def _safe_delete(obj_id: str):
                try:
                    client.delete_object(obj_id, dry_run=dry_run)
                except Exception:
                    # Fallback to pausing if hard delete isn't supported
                    _safe_pause(obj_id)

            adset_name = build_adset_name_v2(
                next_adset_number,
                group_plan.product_label or "",
                group_plan.product_code or "",
                group_plan.audience or "",
            )
            next_adset_number += 1

            first_plan = LaunchPlan.model_validate(selected[0].payload)
            adset_spec = first_plan.adset.model_copy(deep=True)
            adset_spec.name = adset_name

            reserved_set = set(int(i) for i in reserved_ids)

            # Prepare items; drop idempotent hits BEFORE creating anything.
            prepared: List[dict] = []
            stale_queue_ids: List[int] = []

            for r in selected:
                if not dry_run and hasattr(qstore, "reserve_ids") and int(r.id) not in reserved_set:
                    continue

                p = LaunchPlan.model_validate(r.payload)
                # Capture list-valued text variants before any video conversion happens.
                p = apply_flexible_text_variants(p)
                p = ensure_media_for_plan(client, cfg, p, dry_run=dry_run, stable_for_queue=False)

                vid, var, _ = parse_video_name_v2(p.creative.name)
                base_name = build_ad_base_name_v2(vid, var, p.product_label or "")
                p.creative.name = base_name
                p.creative.url_tags = merge_url_tags(p.creative.url_tags)
                p.ad.name = build_ad_name_v2(base_name, p.offer_page or "")

                # Normalize list-valued text fields again (now we can attach media into asset_feed_spec)
                p = apply_flexible_text_variants(p)

                launch_key_raw = f"{p.product}::{p.creative.name}::{p.ad.name}".encode("utf-8")
                launch_key = "launch:" + hashlib.sha256(launch_key_raw).hexdigest()[:24]
                payload_hash = sha256_json(p.model_dump())

                existing = store.get(launch_key)
                if existing and (
                    dry_run
                    or (
                        existing.get("ad_id")
                        and is_meta_object_usable(client, str(existing.get("ad_id")))
                    )
                ):
                    stale_queue_ids.append(int(r.id))
                    continue

                prepared.append(
                    {
                        "queue_id": int(r.id),
                        "plan": p,
                        "launch_key": launch_key,
                        "payload_hash": payload_hash,
                    }
                )

            if stale_queue_ids:
                qstore.delete_ids(stale_queue_ids)

            # Enforce "all-or-nothing" for the adset batch
            if len(prepared) < flush_size:
                if not dry_run and hasattr(qstore, "unreserve_ids") and reserved_ids:
                    try:
                        qstore.unreserve_ids(reserved_ids)  # type: ignore[attr-defined]
                    except Exception:
                        pass
                # Not enough fresh items to form a full batch right now
                continue

            # 1) Create all creatives first (no adset yet)
            created_creatives: List[str] = []
            for item in prepared:
                if dry_run:
                    cid = "DRY_RUN_CREATIVE_ID"
                else:
                    cid = client.create_adcreative(item["plan"].creative, dry_run=False)
                item["creative_id"] = cid
                created_creatives.append(cid)

            # 2) Create the adset
            if dry_run:
                adset_id = "DRY_RUN_ADSET_ID"
            else:
                adset_id = client.create_adset(adset_spec, campaign_id=chosen_campaign_id, dry_run=False)

            # 3) Create all ads referencing the creatives
            created_ads: List[str] = []
            per_ad_results: List[dict] = []
            processed_ids: List[int] = []

            for item in prepared:
                p = item["plan"]
                cid = item["creative_id"]
                if dry_run:
                    ad_id = "DRY_RUN_AD_ID"
                else:
                    ad_id = client.create_ad(p.ad, adset_id=adset_id, creative_id=cid, dry_run=False)
                created_ads.append(ad_id)
                processed_ids.append(int(item["queue_id"]))

                result = {
                    "launch_key": item["launch_key"],
                    "payload_sha256": item["payload_hash"],
                    "campaign_id": chosen_campaign_id,
                    "adset_id": adset_id,
                    "creative_id": cid,
                    "ad_id": ad_id,
                }
                if not dry_run:
                    store.put(item["launch_key"], item["payload_hash"], result)
                per_ad_results.append({"status": "created", **result})

            # Consume processed queue rows
            if processed_ids:
                qstore.delete_ids(processed_ids)

            created_batches.append(
                {
                    "campaign_id": chosen_campaign_id,
                    "adset_id": adset_id,
                    "adset_name": adset_name,
                    "ads": per_ad_results,
                }
            )

        except Exception as e:
            # Roll back best-effort (prevents half-filled adsets)
            if rollback_on_failure:
                try:
                    # Try to delete ads/adset/creatives if we have them in locals
                    local_vars = locals()
                    for ad_id in local_vars.get("created_ads", []) or []:
                        _safe_delete(str(ad_id))
                    if "adset_id" in local_vars:
                        _safe_delete(str(local_vars["adset_id"]))
                    for cr_id in local_vars.get("created_creatives", []) or []:
                        _safe_delete(str(cr_id))
                except Exception:
                    pass

            if not dry_run and hasattr(qstore, "unreserve_ids") and reserved_ids:
                try:
                    qstore.unreserve_ids(reserved_ids)  # type: ignore[attr-defined]
                except Exception:
                    pass
            raise

    msg = f"Launched {len(created_batches)} AdSet batch(es)."
    if duplicates:
        msg += " Note: versions of the same video cannot go in the same AdSet; duplicates in queue: " + ", ".join(duplicates)

    return {
        "mode": "batch",
        "queued": False,
        "product": product,
        "category": category,
        "signature": signature,
        "duplicates_in_queue": duplicates,
        "batches": created_batches,
        "message": msg,
    }


def _run_launch_plan_v2(
    cfg: MetaConfig,
    plan: LaunchPlan,
    *,
    store_path: str = ".meta_idempotency.db",
    dry_run: bool = False,
) -> dict:
    """Schema v2 batching mode:
    - enqueue one LaunchPlan payload (one Notion entry)
    - once we have 3-4 *unique* video_ids for the (product, category, adset_signature) group,
      create a new AdSet and launch ads into it.
    """
    from campaign_store import (
        ensure_campaign_routes_table,
        get_campaign_ids,
        upsert_campaign_id,
        clear_campaign_id,
    )

    client = MetaClient(cfg)
    store = build_idempotency_store(store_path)

    # Ensure URL tags are always present (hard-coded tracking line)
    plan.creative.url_tags = merge_url_tags(plan.creative.url_tags)

    queue_db_path = (os.getenv("QUEUE_DB_PATH") or ".queue_state.db").strip() or ".queue_state.db"
    qstore = build_queue_store(queue_db_path)

    video_id, variant, _label = parse_video_name_v2(plan.creative.name)

    sig = compute_adset_signature_v2(plan.adset)
    sig = f"{sig}:{(plan.audience or '').strip()}"

    # ✅ Resolve image_url/image_path -> image_hash BEFORE storing payload in queue
    plan = ensure_media_for_plan(client, cfg, plan, dry_run=dry_run, stable_for_queue=True)

    row_id = qstore.enqueue(
        product=plan.product,
        category=plan.category or "ug",
        signature=sig,
        video_id=video_id,
        payload=plan.model_dump(),
)


    # Collect queued items for this group.
    rows = qstore.fetch_group(product=plan.product, category=plan.category or "ug", signature=sig, limit=500)

    # Helper: remove rows that already exist (idempotency) so they don't block batching.
    def _is_row_already_launched(payload: dict) -> bool:
        try:
            p = LaunchPlan.model_validate(payload)
        except Exception:
            return False
        base_name = p.creative.name
        # Rebuild final names so launch_key is stable
        vid, var, _ = parse_video_name_v2(base_name)
        base = build_ad_base_name_v2(vid, var, p.product_label or "")
        ad_name = build_ad_name_v2(base, p.offer_page or "")
        launch_key_raw = f"{p.product}::{base}::{ad_name}".encode("utf-8")
        launch_key = "launch:" + hashlib.sha256(launch_key_raw).hexdigest()[:24]
        existing = store.get(launch_key)
        if not existing:
            return False
        if dry_run:
            return True
        ad_id = existing.get("ad_id")
        return bool(ad_id and is_meta_object_usable(client, str(ad_id)))

    stale_ids: List[int] = []
    for r in rows:
        if _is_row_already_launched(r.payload):
            stale_ids.append(r.id)
    if stale_ids and not dry_run:
        qstore.delete_ids(stale_ids)
        rows = [r for r in rows if r.id not in set(stale_ids)]

    policy = get_batch_policy()

    # Compute uniqueness
    by_video: Dict[str, List[int]] = {}
    for r in rows:
        by_video.setdefault(r.video_id, []).append(r.id)

    duplicates = sorted([vid for vid, ids in by_video.items() if len(ids) > 1])
    unique_video_ids = [vid for vid in by_video.keys()]

    group_product = plan.product
    group_category = plan.category or "ug"
    group_signature = sig

    oldest_created_at = get_group_oldest_created_at(
        qstore,
        product=group_product,
        category=group_category,
        signature=group_signature,
        rows=rows,
    )
    flush_size = decide_flush_size(len(unique_video_ids), oldest_created_at, policy)

    if flush_size == 0:
        age_s = int((utcnow() - oldest_created_at).total_seconds())
        msg = (
            f"Queued (aim {policy.target} unique; allow {policy.minimum} after {policy.fallback_after_s}s). "
            f"Unique={len(unique_video_ids)}, oldest_age_s={age_s}."
        )
        if duplicates:
            msg += " Duplicates in queue: " + ", ".join(duplicates)

        return {
            "mode": "batch",
            "queued": True,
            "queue_row_id": row_id,
            "product": group_product,
            "category": group_category,
            "signature": group_signature,
            "unique_videos_in_queue": len(unique_video_ids),
            "duplicates_in_queue": duplicates,
            "oldest_age_s": age_s,
            "policy": {
                "target": policy.target,
                "minimum": policy.minimum,
                "fallback_after_s": policy.fallback_after_s,
            },
            "message": msg,
        }

    # Optional: if you run a dedicated worker on Railway, disable immediate draining
    # in the API service to avoid races / duplicate AdSet numbering.
    immediate = (os.getenv("ENABLE_IMMEDIATE_DRAIN", "false") or "true").strip().lower() not in {"0", "false", "no"}
    if not immediate:
        age_s = int((utcnow() - oldest_created_at).total_seconds())
        msg = (
            f"Queued (eligible to flush now; worker will drain). "
            f"Policy: target {policy.target}, min {policy.minimum} after {policy.fallback_after_s}s. "
            f"Unique={len(unique_video_ids)}, oldest_age_s={age_s}."
        )
        if duplicates:
            msg += " Duplicates in queue: " + ", ".join(duplicates)
        return {
            "mode": "batch",
            "queued": True,
            "eligible_to_flush": True,
            "queue_row_id": row_id,
            "product": group_product,
            "category": group_category,
            "signature": group_signature,
            "unique_videos_in_queue": len(unique_video_ids),
            "duplicates_in_queue": duplicates,
            "oldest_age_s": age_s,
            "policy": {
                "target": policy.target,
                "minimum": policy.minimum,
                "fallback_after_s": policy.fallback_after_s,
            },
            "message": msg,
        }

    drained = _drain_queue_group_v2(
        cfg,
        qstore=qstore,
        store=store,
        product=group_product,
        category=group_category,
        signature=group_signature,
        dry_run=dry_run,
    )
    drained["queue_row_id"] = row_id
    return drained



def run_launch_plan(
    cfg: MetaConfig,
    plan: LaunchPlan,
    *,
    store_path: str = ".meta_idempotency.db",
    dry_run: bool = False,
) -> dict:
    """Runs a validated LaunchPlan directly (API-friendly)."""
    if plan.schema_version >= 2 and bool(plan.batching):
        return _run_launch_plan_v2(cfg, plan, store_path=store_path, dry_run=dry_run)
    return _run_launch_plan_v1(cfg, plan, store_path=store_path, dry_run=dry_run)


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
            # Print the launch result so you can see queue/batch status and created IDs.
            result = run_launch(cfg, args.plan, store_path=args.store, dry_run=args.dry_run)
            print(json.dumps(result, indent=2, ensure_ascii=False))
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
