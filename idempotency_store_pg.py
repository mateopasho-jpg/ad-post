import os
from datetime import datetime, timezone
from typing import Optional

import psycopg


class IdempotencyStorePG:
    """Postgres-backed idempotency + caches.

    This replaces the local SQLite file (IDEMPOTENCY_DB_PATH) for Railway.

    Tables:
      - launches_v2
      - campaign_cache
      - adset_cache
    """

    def __init__(self, database_url: str, *, prefix: str = ""):
        self.database_url = database_url
        self.prefix = prefix.strip()
        self._init_db()

    def _conn(self):
        return psycopg.connect(self.database_url)

    def _t(self, name: str) -> str:
        return f"{self.prefix}{name}" if self.prefix else name

    def _init_db(self) -> None:
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {self._t('launches_v2')} (
                      launch_key TEXT PRIMARY KEY,
                      payload_sha256 TEXT NOT NULL,
                      created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                      campaign_id TEXT,
                      adset_id TEXT,
                      creative_id TEXT,
                      ad_id TEXT
                    )
                    """
                )
                cur.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {self._t('campaign_cache')} (
                      product TEXT PRIMARY KEY,
                      campaign_name TEXT NOT NULL,
                      campaign_id TEXT NOT NULL,
                      updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                    )
                    """
                )
                cur.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {self._t('adset_cache')} (
                      campaign_id TEXT NOT NULL,
                      batch_id TEXT NOT NULL,
                      bucket INTEGER NOT NULL,
                      adset_name TEXT NOT NULL,
                      adset_id TEXT NOT NULL,
                      updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                      PRIMARY KEY (campaign_id, batch_id, bucket)
                    )
                    """
                )
            conn.commit()

    def get_cached_campaign_id(self, product: str) -> Optional[str]:
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT campaign_id FROM {self._t('campaign_cache')} WHERE product=%s",
                    (product,),
                )
                row = cur.fetchone()
        if not row:
            return None
        return str(row[0]) if row[0] else None

    def put_cached_campaign_id(self, product: str, campaign_name: str, campaign_id: str) -> None:
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    INSERT INTO {self._t('campaign_cache')} (product, campaign_name, campaign_id, updated_at)
                    VALUES (%s, %s, %s, now())
                    ON CONFLICT (product) DO UPDATE SET
                      campaign_name=EXCLUDED.campaign_name,
                      campaign_id=EXCLUDED.campaign_id,
                      updated_at=now()
                    """,
                    (product, campaign_name, campaign_id),
                )
            conn.commit()

    def delete_cached_campaign_id(self, product: str) -> None:
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(f"DELETE FROM {self._t('campaign_cache')} WHERE product=%s", (product,))
            conn.commit()

    def get_cached_adset_id(self, campaign_id: str, batch_id: str, bucket: int) -> Optional[str]:
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT adset_id FROM {self._t('adset_cache')}
                    WHERE campaign_id=%s AND batch_id=%s AND bucket=%s
                    """,
                    (campaign_id, batch_id, int(bucket)),
                )
                row = cur.fetchone()
        if not row:
            return None
        return str(row[0]) if row[0] else None

    def put_cached_adset_id(self, campaign_id: str, batch_id: str, bucket: int, adset_name: str, adset_id: str) -> None:
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    INSERT INTO {self._t('adset_cache')} (campaign_id, batch_id, bucket, adset_name, adset_id, updated_at)
                    VALUES (%s, %s, %s, %s, %s, now())
                    ON CONFLICT (campaign_id, batch_id, bucket) DO UPDATE SET
                      adset_name=EXCLUDED.adset_name,
                      adset_id=EXCLUDED.adset_id,
                      updated_at=now()
                    """,
                    (campaign_id, batch_id, int(bucket), adset_name, adset_id),
                )
            conn.commit()

    def delete_cached_adset_id(self, campaign_id: str, batch_id: str, bucket: int) -> None:
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"DELETE FROM {self._t('adset_cache')} WHERE campaign_id=%s AND batch_id=%s AND bucket=%s",
                    (campaign_id, batch_id, int(bucket)),
                )
            conn.commit()

    def get(self, key: str) -> Optional[dict]:
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT launch_key, payload_sha256, created_at, campaign_id, adset_id, creative_id, ad_id
                    FROM {self._t('launches_v2')}
                    WHERE launch_key=%s
                    """,
                    (key,),
                )
                row = cur.fetchone()
        if not row:
            return None
        return {
            "launch_key": row[0],
            "payload_sha256": row[1],
            "created_at": row[2].isoformat() if hasattr(row[2], "isoformat") else row[2],
            "campaign_id": row[3],
            "adset_id": row[4],
            "creative_id": row[5],
            "ad_id": row[6],
        }

    def put(self, key: str, payload_sha256: str, ids: dict) -> None:
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    INSERT INTO {self._t('launches_v2')}
                      (launch_key, payload_sha256, created_at, campaign_id, adset_id, creative_id, ad_id)
                    VALUES (%s, %s, now(), %s, %s, %s, %s)
                    ON CONFLICT (launch_key) DO UPDATE SET
                      payload_sha256=EXCLUDED.payload_sha256,
                      campaign_id=EXCLUDED.campaign_id,
                      adset_id=EXCLUDED.adset_id,
                      creative_id=EXCLUDED.creative_id,
                      ad_id=EXCLUDED.ad_id
                    """,
                    (
                        key,
                        payload_sha256,
                        ids.get("campaign_id"),
                        ids.get("adset_id"),
                        ids.get("creative_id"),
                        ids.get("ad_id"),
                    ),
                )
            conn.commit()

    def delete_launch(self, key: str) -> None:
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(f"DELETE FROM {self._t('launches_v2')} WHERE launch_key=%s", (key,))
            conn.commit()
