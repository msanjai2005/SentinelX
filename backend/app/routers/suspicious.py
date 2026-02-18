from fastapi import APIRouter, Query
from app.services.risk_engine import compute_suspicious_users
from app.core.config import settings

router = APIRouter(prefix="/suspicious-users", tags=["Risk Analysis"])


@router.get("/")
def get_suspicious_users(
    min_messages: int = Query(
        settings.MIN_MESSAGES_THRESHOLD,
        ge=1,
        description="Minimum number of messages required for risk evaluation"
    )
):
    """
    Returns ranked suspicious users based on behavioral density scoring.
    """

    suspicious_list = compute_suspicious_users(min_messages=min_messages)

    return {
        "total_suspicious_users": len(suspicious_list),
        "users": suspicious_list
    }
