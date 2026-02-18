from fastapi import APIRouter, Query
from typing import Optional
from app.services.graph_engine import build_graph

router = APIRouter(prefix="/graph", tags=["Graph Intelligence"])


@router.get("/")
def get_graph(
    focus_user: Optional[str] = Query(
        None,
        description="Build graph around a specific user"
    ),
    suspicious_only: bool = Query(
        False,
        description="Show only subgraph of suspicious users"
    ),
    min_edge_weight: int = Query(
        1,
        ge=1,
        description="Minimum edge weight threshold"
    )
):
    """
    Returns communication network graph with centrality metrics.
    """

    graph_data = build_graph(
        focus_user=focus_user,
        suspicious_only=suspicious_only,
        min_edge_weight=min_edge_weight
    )

    return graph_data
