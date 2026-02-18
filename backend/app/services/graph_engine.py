import networkx as nx
from typing import Dict, Optional
from app.core.database import get_connection
from app.services.risk_engine import compute_suspicious_users


def build_graph(
    focus_user: Optional[str] = None,
    suspicious_only: bool = False,
    min_edge_weight: int = 1
) -> Dict:
    """
    Builds communication graph from events table.

    Parameters:
        focus_user: Optional filter to build graph around a specific user
        suspicious_only: Build graph only around high-risk users
        min_edge_weight: Filter edges below weight threshold

    Returns:
        Dictionary with nodes and edges.
    """

    conn = get_connection()
    cursor = conn.cursor()

    if focus_user:
        rows = cursor.execute(
            "SELECT * FROM events WHERE actor_id = ? OR target_id = ?",
            (focus_user, focus_user)
        ).fetchall()
    else:
        rows = cursor.execute("SELECT * FROM events").fetchall()

    conn.close()

    G = nx.Graph()

    # ---- Build edges ----
    for row in rows:
        sender = row["actor_id"]
        receiver = row["target_id"]

        if sender and receiver:
            if G.has_edge(sender, receiver):
                G[sender][receiver]["weight"] += 1
            else:
                G.add_edge(sender, receiver, weight=1)

    # ---- Suspicious subgraph filtering ----
    suspicious_users = []
    if suspicious_only:
        suspicious_list = compute_suspicious_users()
        suspicious_users = [user["user"] for user in suspicious_list]

        G = G.subgraph(suspicious_users).copy()

    # ---- Edge weight filtering ----
    edges_to_remove = [
        (u, v)
        for u, v, d in G.edges(data=True)
        if d["weight"] < min_edge_weight
    ]
    G.remove_edges_from(edges_to_remove)

    # Remove isolated nodes
    G.remove_nodes_from(list(nx.isolates(G)))

    # ---- Centrality Metrics ----
    degree_centrality = nx.degree_centrality(G) if G.nodes else {}
    betweenness = nx.betweenness_centrality(G) if G.nodes else {}

    nodes = []
    for node in G.nodes():
        nodes.append({
            "id": node,
            "degree": G.degree(node),
            "degree_centrality": round(degree_centrality.get(node, 0), 4),
            "betweenness_centrality": round(betweenness.get(node, 0), 4),
        })

    edges = [
        {
            "source": u,
            "target": v,
            "weight": d["weight"]
        }
        for u, v, d in G.edges(data=True)
    ]

    return {
        "total_nodes": len(nodes),
        "total_edges": len(edges),
        "suspicious_users": suspicious_users if suspicious_only else None,
        "nodes": nodes,
        "edges": edges
    }
