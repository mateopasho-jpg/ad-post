"""campaign_store.py

Persist campaign routing in Postgres (Railway).

V2 schema supports multiple campaigns per product (e.g. green has 2 active campaigns).

Enable by setting:
  CAMPAIGN_ROUTE_SOURCE=db
  DATABASE_URL=...

Tables (created automatically):
  - product_campaign_routes_v2(product TEXT, slot INTEGER, campaign_id TEXT, updated_at TIMESTAMPTZ)

Notes
-----
- The DB is used as the *highest priority* source for campaign IDs.
- If a stored campaign ID becomes ARCHIVED/DELETED, the service can clear it and re-create.
"""

from __future__ import annotations

from typing import Dict, Optional

import psycopg


DEFAULT_TABLE_V1 = "product_campaign_routes"
DEFAULT_TABLE_V2 = "product_campaign_routes_v2"


# ---------------------------------------------------------------------------
# V2 (multi-campaign per product)
# ---------------------------------------------------------------------------

def ensure_campaign_routes_table(database_url: str, *, table: str = DEFAULT_TABLE_V2) -> None:
    """Create the V2 table if it doesn't exist."""
    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {table} (
                    product TEXT NOT NULL,
                    slot INTEGER NOT NULL,
                    campaign_id TEXT NOT NULL,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    PRIMARY KEY (product, slot)
                )
                """
            )
            conn.commit()


def get_campaign_ids(database_url: str, product: str, *, table: str = DEFAULT_TABLE_V2) -> Dict[int, str]:
    """Return mapping slot -> campaign_id for a product."""
    product = (product or "").strip().lower()
    if not product:
        return {}

    out: Dict[int, str] = {}
    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT slot, campaign_id FROM {table} WHERE product=%s ORDER BY slot ASC",
                (product,),
            )
            for row in cur.fetchall() or []:
                slot = int(row[0])
                cid = str(row[1]) if row[1] else ""
                if cid:
                    out[slot] = cid
    return out


def upsert_campaign_id(database_url: str, product: str, slot: int, campaign_id: str, *, table: str = DEFAULT_TABLE_V2) -> None:
    product = (product or "").strip().lower()
    campaign_id = (campaign_id or "").strip()
    slot = int(slot)
    if not product or not campaign_id or slot <= 0:
        return

    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {table} (product, slot, campaign_id)
                VALUES (%s, %s, %s)
                ON CONFLICT (product, slot)
                DO UPDATE SET campaign_id=EXCLUDED.campaign_id, updated_at=now()
                """,
                (product, slot, campaign_id),
            )
            conn.commit()


def clear_campaign_id(database_url: str, product: str, *, slot: int | None = None, table: str = DEFAULT_TABLE_V2) -> None:
    product = (product or "").strip().lower()
    if not product:
        return

    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            if slot is None:
                cur.execute(f"DELETE FROM {table} WHERE product=%s", (product,))
            else:
                cur.execute(f"DELETE FROM {table} WHERE product=%s AND slot=%s", (product, int(slot)))
            conn.commit()


# ---------------------------------------------------------------------------
# V1 legacy helpers (single campaign per product)
# ---------------------------------------------------------------------------

def ensure_campaign_routes_table_v1(database_url: str, *, table: str = DEFAULT_TABLE_V1) -> None:
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


def get_campaign_id_v1(database_url: str, product: str, *, table: str = DEFAULT_TABLE_V1) -> Optional[str]:
    product = (product or "").strip().lower()
    if not product:
        return None

    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT campaign_id FROM {table} WHERE product=%s", (product,))
            row = cur.fetchone()
            return str(row[0]) if row and row[0] else None


def upsert_campaign_id_v1(database_url: str, product: str, campaign_id: str, *, table: str = DEFAULT_TABLE_V1) -> None:
    product = (product or "").strip().lower()
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


def clear_campaign_id_v1(database_url: str, product: str, *, table: str = DEFAULT_TABLE_V1) -> None:
    product = (product or "").strip().lower()
    if not product:
        return

    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(f"DELETE FROM {table} WHERE product=%s", (product,))
            conn.commit()
