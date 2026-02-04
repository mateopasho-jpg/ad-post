# token_store.py
from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Optional

import psycopg


@dataclass(frozen=True)
class StoredToken:
    access_token: str
    expires_at: datetime


def get_stored_token(
    database_url: str,
    *,
    token_id: str = "meta_graph",
) -> Optional[StoredToken]:
    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT access_token, expires_at
                FROM graph_api_tokens
                WHERE id = %s
                """,
                (token_id,),
            )
            row = cur.fetchone()
            if not row:
                return None
            access_token, expires_at = row
            return StoredToken(access_token=access_token, expires_at=expires_at)


def get_valid_access_token(
    database_url: str,
    *,
    token_id: str = "meta_graph",
    refresh_buffer_minutes: int = 10,
) -> str:
    tok = get_stored_token(database_url, token_id=token_id)
    if not tok:
        raise RuntimeError(
            f"No token found in DB table graph_api_tokens for id='{token_id}'."
        )

    now = datetime.now(timezone.utc)
    if tok.expires_at <= now + timedelta(minutes=refresh_buffer_minutes):
        # At this stage we just fail fast; token-manager will be responsible for refreshing.
        # (You can later evolve this to auto-refresh here.)
        raise RuntimeError(
            f"Token in DB is expired/near-expiry (expires_at={tok.expires_at.isoformat()})."
        )

    return tok.access_token
