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
import sys
import time
import traceback
import logging
from meta_ads_tool import MetaAPIError

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


POLL_S = _get_int_env("WORKER_POLL_SECONDS", "WORKER_POLL_S", default=120)  # 2 min between cycles
GROUP_SCAN_LIMIT = _get_int_env("WORKER_GROUP_SCAN_LIMIT", default=50)
GROUP_DELAY_S = _get_int_env("WORKER_GROUP_DELAY_SECONDS", default=15)  # pause between groups


def deduplicate_queue_v2() -> int:
    """Remove duplicate entries from queue_v2 table.
    
    Keeps the oldest entry (lowest id) for each unique combination of:
    (product, category, signature, video_id)
    
    Returns:
        Number of duplicate entries deleted.
    """
    database_url = os.getenv("DATABASE_URL", "").strip()
    if not database_url:
        logging.warning("⚠️ DATABASE_URL not set, skipping deduplication")
        return 0
    
    try:
        import psycopg2
        
        conn = psycopg2.connect(database_url)
        cur = conn.cursor()
        
        # SQL to delete duplicates, keeping the oldest (first) entry
        dedupe_sql = """
        WITH ranked_entries AS (
            SELECT 
                id,
                product,
                category,
                video_id,
                ROW_NUMBER() OVER (
                    PARTITION BY product, category, signature, video_id 
                    ORDER BY id ASC
                ) as row_num
            FROM queue_v2
        )
        DELETE FROM queue_v2
        WHERE id IN (
            SELECT id 
            FROM ranked_entries 
            WHERE row_num > 1
        )
        RETURNING id, video_id
        """
        
        cur.execute(dedupe_sql)
        deleted_rows = cur.fetchall()
        deleted_count = len(deleted_rows)
        
        conn.commit()
        cur.close()
        conn.close()
        
        if deleted_count > 0:
            deleted_ids = [str(row[0]) for row in deleted_rows[:10]]
            deleted_videos = list(set(str(row[1]) for row in deleted_rows))
            logging.warning(
                f"🧹 Auto-cleanup: Removed {deleted_count} duplicate queue entries. "
                f"Sample IDs: {', '.join(deleted_ids)}{'...' if len(deleted_rows) > 10 else ''}. "
                f"Affected video_ids: {', '.join(deleted_videos)}"
            )
        else:
            logging.debug("✅ No duplicates found in queue_v2")
        
        return deleted_count
        
    except ImportError:
        logging.error("❌ psycopg2 not installed. Run: pip install psycopg2-binary")
        return 0
    except Exception as e:
        logging.error(f"❌ Deduplication error: {e}", exc_info=True)
        return 0


def main() -> None:
    # ── Logging setup ──────────────────────────────────────────────────────────
    # Force logs to stdout so Railway captures them in the service log panel.
    # Without this, logging.warning/error calls produce NO output at all.
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
        force=True,  # override any root-logger config set elsewhere
    )
    logging.info("Worker starting. POLL_S=%d GROUP_SCAN_LIMIT=%d GROUP_DELAY_S=%d",
                 POLL_S, GROUP_SCAN_LIMIT, GROUP_DELAY_S)

    cfg = MetaConfig.from_env()

    store_path = (os.getenv("IDEMPOTENCY_DB_PATH") or ".meta_idempotency.db").strip() or ".meta_idempotency.db"
    store = build_idempotency_store(store_path)

    queue_db_path = (os.getenv("QUEUE_DB_PATH") or ".queue_state.db").strip() or ".queue_state.db"
    qstore = build_queue_store(queue_db_path)

    logging.info("Stores initialised. Entering poll loop...")

    while True:
        try:
            # ✅ NEW: Deduplicate queue before processing
            # This removes any duplicate entries that may have been created by
            # multiple Notion rows with the same creative name..
            deduplicate_queue_v2()
            
            groups = qstore.list_groups(limit=GROUP_SCAN_LIMIT)
            logging.info("Poll cycle: found %d group(s) in queue.", len(groups))
            for i, g in enumerate(groups):
                logging.info(
                    "Processing group %d/%d: product=%s category=%s signature=%s",
                    i + 1, len(groups), g["product"], g["category"], g["signature"],
                )
                if i > 0:
                    logging.info("Pausing %ds between groups to avoid rate limits...", GROUP_DELAY_S)
                    time.sleep(GROUP_DELAY_S)
                result = _drain_queue_group_v2(
                    cfg,
                    qstore=qstore,
                    store=store,
                    product=g["product"],
                    category=g["category"],
                    signature=g["signature"],
                    dry_run=False,
                )
                logging.info("Group result: %s", result)
        except MetaAPIError as e:
            _emsg = str(e).lower()
            _RATE_LIMIT_PHRASES = ("too many calls", "request limit", "application request limit", "rate limit")
            _TRANSIENT_PHRASES = ("service temporarily unavailable", "temporarily unavailable", "unknown error", "please try again")
            if any(p in _emsg for p in _RATE_LIMIT_PHRASES):
                logging.warning("Rate limit hit, sleeping 60s: %s", e)
                time.sleep(60)
            elif any(p in _emsg for p in _TRANSIENT_PHRASES):
                logging.warning("Transient Meta error, sleeping 30s: %s", e)
                time.sleep(30)
            else:
                logging.error("Meta API error (will retry next cycle): %s", e)
        except Exception:
            logging.error("Unexpected error in worker loop:\n%s", traceback.format_exc())

        logging.info("Sleeping %ds until next poll...", POLL_S)
        time.sleep(POLL_S)


if __name__ == "__main__":
    main()