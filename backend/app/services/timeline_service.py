import sqlite3
from typing import Optional, Dict, List, Any
from app.core.database import get_connection


def get_timeline(
    case_id: Optional[str] = None,
    actor_id: Optional[str] = None,
    source_type: Optional[str] = None,
    deleted_only: bool = False,
    late_night: bool = False,
    keyword: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    limit: int = 500,
) -> Dict[str, Any]:
    """
    Reconstruct a unified chronological timeline of events.
    """
    limit = max(1, min(limit, 5000))

    conditions: List[str] = ["timestamp IS NOT NULL"]
    params: List[Any] = []

    if case_id:
        conditions.append("case_id = ?")
        params.append(case_id)

    if actor_id:
        conditions.append("(actor_id = ? OR target_id = ?)")
        params.extend([actor_id, actor_id])

    if source_type:
        conditions.append("source_type = ?")
        params.append(source_type)

    if deleted_only:
        conditions.append("deleted_flag = 1")

    if late_night:
        conditions.append("CAST(strftime('%H', timestamp) AS INTEGER) BETWEEN 0 AND 4")

    if keyword:
        conditions.append("LOWER(message_text) LIKE ?")
        params.append(f"%{keyword.lower()}%")

    if start_date:
        conditions.append("datetime(timestamp) >= datetime(?)")
        params.append(start_date)

    if end_date:
        conditions.append("datetime(timestamp) <= datetime(?)")
        params.append(end_date)

    params.append(limit)

    query = f"""
        SELECT * FROM events
        WHERE {" AND ".join(conditions)}
        ORDER BY datetime(timestamp) ASC
        LIMIT ?
    """

    conn = get_connection()
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(query, params).fetchall()
    finally:
        conn.close()

    return {
        "total_events": len(rows),
        "events": [dict(r) for r in rows],
    }