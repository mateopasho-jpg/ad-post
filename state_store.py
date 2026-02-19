from __future__ import annotations

import json
import hashlib
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Sequence


@dataclass(frozen=True)
class QueueItem:
    id: int
    product: str
    category: str
    signature: str
    video_id: str
    payload: Dict[str, Any]
    created_at: datetime


class StateStore:
    """SQLite-backed FIFO queue used to batch ads into AdSets.

    Each row represents *one* ad (one Notion entry / one LaunchPlan).

    Rows are grouped by:
      - product (green|lila|rosa)
      - category (ai|ug)
      - signature: hash/key of the AdSet spec so we don't mix audiences/targeting

    Batching rules are enforced by the caller.

    Note on concurrency:
      - SQLite backend has *no* cross-process reservation.
      - For Railway/production, prefer StateStorePG (Postgres) with QUEUE_STORE_SOURCE=db.
    """

    def __init__(self, db_path: str = ".queue_state.db"):
        self.db_path = db_path
        self._init()

    def _init(self) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS queue_v2 (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    product TEXT NOT NULL,
                    category TEXT NOT NULL,
                    signature TEXT NOT NULL,
                    video_id TEXT NOT NULL,
                    ingest_key TEXT,
                    payload_json TEXT NOT NULL,
                    created_at TEXT NOT NULL
                )
                """
            )
            # SQLite doesn't support ADD COLUMN IF NOT EXISTS, so we try and ignore duplicates.
            try:
                conn.execute("ALTER TABLE queue_v2 ADD COLUMN ingest_key TEXT")
            except sqlite3.OperationalError:
                pass
            conn.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS uq_queue_v2_ingest_key ON queue_v2(ingest_key)"
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_queue_v2_group
                ON queue_v2(product, category, signature, id)
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
        created_at = datetime.now(timezone.utc).isoformat()
        payload_str = json.dumps(payload, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
        ingest_key = hashlib.sha256(
            f"{product}|{category}|{signature}|{video_id}|{payload_str}".encode("utf-8")
        ).hexdigest()
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT OR IGNORE INTO queue_v2 (product, category, signature, video_id, ingest_key, payload_json, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (product, category, signature, video_id, ingest_key, json.dumps(payload, ensure_ascii=False), created_at),
            )
            cur = conn.execute("SELECT id FROM queue_v2 WHERE ingest_key=?", (ingest_key,))
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
        with sqlite3.connect(self.db_path) as conn:
            cur = conn.execute(
                """
                SELECT id, product, category, signature, video_id, payload_json, created_at
                FROM queue_v2
                WHERE product=? AND category=? AND signature=?
                ORDER BY id ASC
                LIMIT ?
                """,
                (product, category, signature, int(limit)),
            )
            rows = cur.fetchall()

        out: List[QueueItem] = []
        for (row_id, prod, cat, sig, vid, payload_json, created_at) in rows:
            try:
                payload = json.loads(payload_json)
            except Exception:
                payload = {"_raw": payload_json}

            # created_at is stored as ISO string in SQLite.
            try:
                s = str(created_at).strip().replace("Z", "+00:00")
                dt = datetime.fromisoformat(s)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                dt = dt.astimezone(timezone.utc)
            except Exception:
                dt = datetime.now(timezone.utc)

            out.append(
                QueueItem(
                    id=int(row_id),
                    product=str(prod),
                    category=str(cat),
                    signature=str(sig),
                    video_id=str(vid),
                    payload=payload,
                    created_at=dt,
                )
            )
        return out

    def delete_ids(self, ids: Sequence[int]) -> None:
        ids = [int(i) for i in ids]
        if not ids:
            return
        with sqlite3.connect(self.db_path) as conn:
            for i in ids:
                conn.execute("DELETE FROM queue_v2 WHERE id=?", (i,))
            conn.commit()

    # Reservation helpers (no-op for SQLite mode; used by Postgres backend).
    def reserve_ids(self, ids: Sequence[int]) -> List[int]:
        return [int(i) for i in ids]

    def unreserve_ids(self, ids: Sequence[int]) -> int:
        return 0

    def count_group(self, *, product: str, category: str, signature: str) -> int:
        with sqlite3.connect(self.db_path) as conn:
            cur = conn.execute(
                "SELECT COUNT(1) FROM queue_v2 WHERE product=? AND category=? AND signature=?",
                (product, category, signature),
            )
            row = cur.fetchone()
            return int(row[0] or 0)

    def purge(self) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("DELETE FROM queue_v2")
            conn.commit()

    def list_groups(self, limit: int = 200) -> List[Dict[str, Any]]:
        """Return groups with unique video count + oldest created_at."""
        with sqlite3.connect(self.db_path) as conn:
            cur = conn.execute(
                """
                SELECT product, category, signature,
                    COUNT(DISTINCT video_id) AS unique_videos,
                    MIN(created_at) AS oldest_created_at
                FROM queue_v2
                GROUP BY product, category, signature
                ORDER BY MIN(created_at) ASC
                LIMIT ?
                """,
                (int(limit),),
            )
            rows = cur.fetchall()

        out: List[Dict[str, Any]] = []
        for product, category, signature, unique_videos, oldest_created_at in rows:
            out.append(
                {
                    "product": str(product),
                    "category": str(category),
                    "signature": str(signature),
                    "unique_videos": int(unique_videos or 0),
                    "oldest_created_at": str(oldest_created_at),
                }
            )
        return out
