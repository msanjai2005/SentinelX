from typing import List, Dict
from app.core.database import get_connection
from app.core.config import settings


FINANCIAL_KEYWORDS = [
    "transfer",
    "amount",
    "payment",
    "cash",
    "wire",
    "deposit"
]


def compute_suspicious_users(min_messages: int = None) -> List[Dict]:
    """
    Computes suspicious users using weighted behavioral density scoring.
    """

    if min_messages is None:
        min_messages = settings.MIN_MESSAGES_THRESHOLD

    conn = get_connection()
    cursor = conn.cursor()
    rows = cursor.execute("SELECT * FROM events").fetchall()
    conn.close()

    user_stats = {}

    for row in rows:
        user = row["actor_id"]
        if not user:
            continue

        if user not in user_stats:
            user_stats[user] = {
                "late_night": 0,
                "deleted": 0,
                "financial": 0,
                "total_messages": 0
            }

        user_stats[user]["total_messages"] += 1

        # ---- Late night detection (00:00–04:59) ----
        ts = row["timestamp"] or ""
        try:
            hour = int(ts[11:13])
            if 0 <= hour <= 4:
                user_stats[user]["late_night"] += 1
        except (ValueError, IndexError):
            pass

        # ---- Deleted messages ----
        if row["deleted_flag"] == 1:
            user_stats[user]["deleted"] += 1

        # ---- Financial keyword detection ----
        text = (row["message_text"] or "").lower()
        if any(keyword in text for keyword in FINANCIAL_KEYWORDS):
            user_stats[user]["financial"] += 1

    suspicious_users = []

    for user, stats in user_stats.items():

        total = stats["total_messages"]

        if total < min_messages:
            continue

        late_ratio = stats["late_night"] / total
        delete_ratio = stats["deleted"] / total
        financial_ratio = stats["financial"] / total

        risk_score = round(
            late_ratio * settings.LATE_NIGHT_WEIGHT +
            delete_ratio * settings.DELETED_WEIGHT +
            financial_ratio * settings.FINANCIAL_WEIGHT,
            2
        )

        if risk_score >= settings.MEDIUM_RISK_THRESHOLD:

            risk_level = (
                "HIGH"
                if risk_score >= settings.HIGH_RISK_THRESHOLD
                else "MEDIUM"
            )

            suspicious_users.append({
                "user": user,
                "risk_score": risk_score,
                "risk_level": risk_level,
                "stats": stats
            })

    # Sort by highest risk first
    suspicious_users.sort(
        key=lambda x: x["risk_score"],
        reverse=True
    )

    return suspicious_users
