from fastapi import APIRouter
from app.core.database import get_connection

router = APIRouter(prefix="/stats", tags=["System Statistics"])


@router.get("/")
def get_stats():
    """
    Returns high-level analytics summary.
    """

    conn = get_connection()
    cursor = conn.cursor()

    total_events = cursor.execute(
        "SELECT COUNT(*) FROM events"
    ).fetchone()[0]

    deleted_messages = cursor.execute(
        "SELECT COUNT(*) FROM events WHERE deleted_flag = 1"
    ).fetchone()[0]

    unique_users = cursor.execute(
        "SELECT COUNT(DISTINCT actor_id) FROM events WHERE actor_id IS NOT NULL"
    ).fetchone()[0]

    language_breakdown = cursor.execute("""
        SELECT language, COUNT(*) as count
        FROM events
        WHERE language IS NOT NULL
        GROUP BY language
        ORDER BY count DESC
    """).fetchall()

    source_breakdown = cursor.execute("""
        SELECT source_type, COUNT(*) as count
        FROM events
        GROUP BY source_type
        ORDER BY count DESC
    """).fetchall()

    conn.close()

    return {
        "total_events": total_events,
        "deleted_messages": deleted_messages,
        "unique_users": unique_users,
        "language_distribution": [dict(row) for row in language_breakdown],
        "source_distribution": [dict(row) for row in source_breakdown]
    }
