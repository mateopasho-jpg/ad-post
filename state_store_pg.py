import json
import os
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence

import psycopg


@dataclass(frozen=True)
class QueueItem:
    id: int
    product: str
    category: str
    signature: str
    video_id: str
    payload: Dict[str, Any]


class StateStorePG:
    """Postgres-backed FIFO queue used to batch ads into AdSets.

    Production backend for Railway: stores queue in Postgres instead of local SQLite.

    Reservation:
      - rows start as state='queued'
      - when selected for processing, they are marked state='reserved'
      - on success, rows are deleted
      - on failure, rows can be unreserved back to 'queued'
    """

    def __init__(
        self,
        database_url: str,
        *,
        table: str = "queue_v2",
        reserved_by: Optional[str] = None,
    ):
        self.database_url = database_url
        self.table = table
        self.reserved_by = reserved_by or (
            os.getenv("RAILWAY_SERVICE_NAME") or os.getenv("HOSTNAME") or "ad-post"
        )
        self._init()

    def _conn(self):
        return psycopg.connect(self.database_url)

    def _init(self) -> None:
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {self.table} (
                      id BIGSERIAL PRIMARY KEY,
                      product TEXT NOT NULL,
                      category TEXT NOT NULL,
                      signature TEXT NOT NULL,
                      video_id TEXT NOT NULL,
                      payload_json JSONB NOT NULL,
                      created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                      state TEXT NOT NULL DEFAULT 'queued',
                      reserved_at TIMESTAMPTZ,
                      reserved_by TEXT
                    )
                    """
                )

                # Backward-compatible migrations (safe if columns already exist)
                cur.execute(
                    f"ALTER TABLE {self.table} ADD COLUMN IF NOT EXISTS state TEXT NOT NULL DEFAULT 'queued'"
                )
                cur.execute(
                    f"ALTER TABLE {self.table} ADD COLUMN IF NOT EXISTS reserved_at TIMESTAMPTZ"
                )
                cur.execute(
                    f"ALTER TABLE {self.table} ADD COLUMN IF NOT EXISTS reserved_by TEXT"
                )

                cur.execute(
                    f"""
                    CREATE INDEX IF NOT EXISTS idx_{self.table}_group
                    ON {self.table}(product, category, signature, id)
                    """
                )
            conn.commit()

    def enqueue(
        self,
        *,
        product: str,
        category: str,
        signature: str,
        video_id: str,
        payload: Dict[str, Any],
    ) -> int:
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    INSERT INTO {self.table} (product, category, signature, video_id, payload_json)
                    VALUES (%s, %s, %s, %s, %s)
                    RETURNING id
                    """,
                    (product, category, signature, video_id, json.dumps(payload, ensure_ascii=False)),
                )
                row = cur.fetchone()
            conn.commit()
        return int(row[0])

    def fetch_group(
        self,
        *,
        product: str,
        category: str,
        signature: str,
        limit: int = 200,
    ) -> List[QueueItem]:
        """Fetch rows for a group.

        Includes:
          - state='queued'
          - state='reserved' but *stale* (reserved_at older than 30 minutes)
        """
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT id, product, category, signature, video_id, payload_json
                    FROM {self.table}
                    WHERE product=%s AND category=%s AND signature=%s
                      AND (
                        state='queued'
                        OR (state='reserved' AND reserved_at < (now() - interval '30 minutes'))
                      )
                    ORDER BY id ASC
                    LIMIT %s
                    """,
                    (product, category, signature, int(limit)),
                )
                rows = cur.fetchall()

        out: List[QueueItem] = []
        for (row_id, prod, cat, sig, vid, payload_json) in rows:
            try:
                payload = payload_json if isinstance(payload_json, dict) else json.loads(payload_json)
            except Exception:
                payload = {"_raw": payload_json}
            out.append(
                QueueItem(
                    id=int(row_id),
                    product=str(prod),
                    category=str(cat),
                    signature=str(sig),
                    video_id=str(vid),
                    payload=payload,
                )
            )
        return out

    def reserve_ids(self, ids: Sequence[int]) -> List[int]:
        """Mark rows as reserved.

        Returns the list of row ids reserved by this call.
        """
        ids = [int(i) for i in ids]
        if not ids:
            return []
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    UPDATE {self.table}
                    SET state='reserved', reserved_at=now(), reserved_by=%s
                    WHERE id = ANY(%s)
                      AND (
                        state='queued'
                        OR (state='reserved' AND reserved_at < (now() - interval '30 minutes'))
                      )
                    RETURNING id
                    """,
                    (self.reserved_by, ids),
                )
                rows = cur.fetchall()
            conn.commit()
        return [int(r[0]) for r in rows]

    def unreserve_ids(self, ids: Sequence[int]) -> int:
        ids = [int(i) for i in ids]
        if not ids:
            return 0
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    UPDATE {self.table}
                    SET state='queued', reserved_at=NULL, reserved_by=NULL
                    WHERE id = ANY(%s) AND state='reserved'
                    """,
                    (ids,),
                )
                n = int(cur.rowcount or 0)
            conn.commit()
        return n

    def delete_ids(self, ids: Sequence[int]) -> None:
        ids = [int(i) for i in ids]
        if not ids:
            return
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(f"DELETE FROM {self.table} WHERE id = ANY(%s)", (ids,))
            conn.commit()

    def count_group(self, *, product: str, category: str, signature: str) -> int:
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT COUNT(1) FROM {self.table} WHERE product=%s AND category=%s AND signature=%s AND state='queued'",
                    (product, category, signature),
                )
                row = cur.fetchone()
        return int(row[0] or 0)

    def purge(self) -> None:
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(f"DELETE FROM {self.table}")
            conn.commit()
