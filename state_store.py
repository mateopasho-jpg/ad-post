from __future__ import annotations

import json
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
                    payload_json TEXT NOT NULL,
                    created_at TEXT NOT NULL
                )
                """
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
        with sqlite3.connect(self.db_path) as conn:
            cur = conn.execute(
                """
                INSERT INTO queue_v2 (product, category, signature, video_id, payload_json, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (product, category, signature, video_id, json.dumps(payload, ensure_ascii=False), created_at),
            )
            conn.commit()
            return int(cur.lastrowid)

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
