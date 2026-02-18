from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.core.database import create_tables

# Routers
from app.routers import (
    upload,
    suspicious,
    timeline,
    graph,
    stats,
    report
)

app = FastAPI(
    title="SentinelX AI",
    description="Behavioral Forensic Intelligence Platform",
    version="2.0.0"
)


# ---- CORS (Development Mode) ----
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Restrict in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---- Startup Event ----
@app.on_event("startup")
def startup():
    create_tables()


# ---- Root Health Check ----
@app.get("/")
def root():
    return {
        "status": "running",
        "service": "SentinelX AI Backend",
        "version": "2.0.0"
    }


# ---- Include Routers ----
app.include_router(upload.router)
app.include_router(suspicious.router)
app.include_router(timeline.router)
app.include_router(graph.router)
app.include_router(stats.router)
app.include_router(report.router)
