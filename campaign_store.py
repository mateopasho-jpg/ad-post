# campaign_store.py
"""Persist product -> campaign_id routing in Postgres (Railway).

This is optional. Enable by setting:
  CAMPAIGN_ROUTE_SOURCE=db
  DATABASE_URL=...

Table schema (created automatically):
  product_campaign_routes(product TEXT PRIMARY KEY, campaign_id TEXT NOT NULL, updated_at TIMESTAMPTZ NOT NULL DEFAULT now())

Rationale
---------
If Meta marks a campaign as ARCHIVED/DELETED, reusing its ID will cause errors like:
  "Ad Sets may not be added to archived Campaigns."

Storing the last known-good campaign_id per product in DB allows the service to:
- re-use stable campaign IDs across deployments/instances
- invalidate them if they become archived/deleted
- create and store a new campaign_id automatically
"""

from __future__ import annotations

from typing import Optional

import psycopg


DEFAULT_TABLE = "product_campaign_routes"


def ensure_campaign_routes_table(database_url: str, *, table: str = DEFAULT_TABLE) -> None:
    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {table} (
                    product TEXT PRIMARY KEY,
                    campaign_id TEXT NOT NULL,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
                """
            )
            conn.commit()


def get_campaign_id(database_url: str, product: str, *, table: str = DEFAULT_TABLE) -> Optional[str]:
    product = (product or "").strip()
    if not product:
        return None

    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT campaign_id FROM {table} WHERE product=%s",
                (product,),
            )
            row = cur.fetchone()
            if not row:
                return None
            return str(row[0]) if row[0] else None


def upsert_campaign_id(database_url: str, product: str, campaign_id: str, *, table: str = DEFAULT_TABLE) -> None:
    product = (product or "").strip()
    campaign_id = (campaign_id or "").strip()
    if not product or not campaign_id:
        return

    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {table} (product, campaign_id)
                VALUES (%s, %s)
                ON CONFLICT (product)
                DO UPDATE SET campaign_id=EXCLUDED.campaign_id, updated_at=now()
                """,
                (product, campaign_id),
            )
            conn.commit()


def clear_campaign_id(database_url: str, product: str, *, table: str = DEFAULT_TABLE) -> None:
    product = (product or "").strip()
    if not product:
        return

    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"DELETE FROM {table} WHERE product=%s",
                (product,),
            )
            conn.commit()
