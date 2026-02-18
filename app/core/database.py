import sqlite3
from pathlib import Path
from app.core.config import settings


def get_connection():
    """
    Returns a SQLite connection with row factory enabled.
    """
    # Ensure data directory exists
    settings.DATA_DIR.mkdir(exist_ok=True)

    conn = sqlite3.connect(settings.DATABASE_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def create_tables():
    """
    Creates the unified events table if it does not exist.
    """

    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id TEXT UNIQUE,
            case_id TEXT,
            source_type TEXT,
            event_type TEXT,
            timestamp TEXT,
            actor_id TEXT,
            target_id TEXT,
            message_text TEXT,
            deleted_flag INTEGER DEFAULT 0,
            language TEXT,
            device_id TEXT,
            ip_address TEXT,
            metadata TEXT
        )
    """)

    conn.commit()
    conn.close()
