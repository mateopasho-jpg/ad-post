"""Queue worker for schema v2 batching.

Why this exists:
- The /run endpoint only executes when a webhook hits the API.
- Schema v2 has a time-based fallback (e.g. allow 3 items after 60s).
- If no new webhook arrives after the 3rd item, a worker must wake up and flush the batch.

Deploy on Railway as a separate service:
  Start command: python worker.py

Recommended env:
  QUEUE_STORE_SOURCE=db
  IDEMPOTENCY_STORE_SOURCE=db
  DATABASE_URL=...
"""

from __future__ import annotations

import os
import time
import traceback

from meta_ads_tool import (
    MetaConfig,
    build_idempotency_store,
    build_queue_store,
    _drain_queue_group_v2,
)


def _get_int_env(*names: str, default: int) -> int:
    for n in names:
        v = (os.getenv(n) or "").strip()
        if v:
            try:
                return int(v)
            except Exception:
                pass
    return int(default)


POLL_S = _get_int_env("WORKER_POLL_SECONDS", "WORKER_POLL_S", default=10)
GROUP_SCAN_LIMIT = _get_int_env("WORKER_GROUP_SCAN_LIMIT", default=50)


def main() -> None:
    cfg = MetaConfig.from_env()

    store_path = (os.getenv("IDEMPOTENCY_DB_PATH") or ".meta_idempotency.db").strip() or ".meta_idempotency.db"
    store = build_idempotency_store(store_path)

    queue_db_path = (os.getenv("QUEUE_DB_PATH") or ".queue_state.db").strip() or ".queue_state.db"
    qstore = build_queue_store(queue_db_path)

    while True:
        try:
            groups = qstore.list_groups(limit=GROUP_SCAN_LIMIT)
            for g in groups:
                _drain_queue_group_v2(
                    cfg,
                    qstore=qstore,
                    store=store,
                    product=g["product"],
                    category=g["category"],
                    signature=g["signature"],
                    dry_run=False,
                )
        except Exception:
            traceback.print_exc()

        time.sleep(POLL_S)


if __name__ == "__main__":
    main()
