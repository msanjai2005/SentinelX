from fastapi import APIRouter, Query
from typing import Optional
from app.services.timeline_service import get_timeline

router = APIRouter(prefix="/timeline", tags=["Timeline"])


@router.get("/")
def fetch_timeline(
    case_id: Optional[str] = Query(None, description="Filter by case ID"),
    actor_id: Optional[str] = Query(None, description="Filter by actor or target ID"),
    source_type: Optional[str] = Query(None, description="Filter by source type e.g. whatsapp, calls"),
    deleted_only: bool = Query(False, description="Show only deleted messages"),
    late_night: bool = Query(False, description="Show only events between 00:00 and 04:59"),
    keyword: Optional[str] = Query(None, description="Search keyword in message_text"),
    start_date: Optional[str] = Query(None, description="Start datetime e.g. 2024-01-01 00:00:00"),
    end_date: Optional[str] = Query(None, description="End datetime e.g. 2024-12-31 23:59:59"),
    limit: int = Query(500, ge=1, le=5000, description="Max number of results"),
):
    return get_timeline(
        case_id=case_id,
        actor_id=actor_id,
        source_type=source_type,
        deleted_only=deleted_only,
        late_night=late_night,
        keyword=keyword,
        start_date=start_date,
        end_date=end_date,
        limit=limit,
    )