import pandas as pd
import sqlite3
import json
import uuid
from contextlib import contextmanager
from typing import List, Tuple
from app.core.database import get_connection


SUPPORTED_SOURCES = {
    "whatsapp",
    "app_usage",
    "locations",
    "calls",
    "whatsapp_calls",
    "upi_transactions",
}


@contextmanager
def managed_connection():
    conn = get_connection()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _parse_timestamps(df: pd.DataFrame, filename: str, skip_reasons: list) -> pd.DataFrame:
    """Try multiple common formats before falling back to slow parse."""
    FORMATS_TO_TRY = [
        "%Y-%m-%d %H:%M:%S",
        "%d/%m/%Y %H:%M:%S",
        "%m/%d/%Y %H:%M:%S",
        "%d-%m-%Y %H:%M:%S",
        "%Y/%m/%d %H:%M:%S",
        "%d/%m/%Y %H:%M",
        "%m/%d/%Y %H:%M",
        "%d-%m-%Y %H:%M",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%SZ",
        "%m/%d/%Y %I:%M %p",
        "%d/%m/%Y %I:%M %p",
    ]

    for fmt in FORMATS_TO_TRY:
        parsed = pd.to_datetime(df["timestamp"], format=fmt, errors="coerce")
        if parsed.notna().mean() > 0.9:
            df["timestamp"] = parsed
            bad = df["timestamp"].isna().sum()
            if bad:
                skip_reasons.append(
                    f"[{filename}] Dropped {bad} rows with unparseable timestamps (format: {fmt})"
                )
            return df.dropna(subset=["timestamp"])

    # Last resort
    skip_reasons.append(
        f"[{filename}] WARNING: Could not detect timestamp format, falling back to slow parse."
    )
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    bad = df["timestamp"].isna().sum()
    if bad:
        skip_reasons.append(f"[{filename}] Dropped {bad} rows with unparseable timestamps.")
    return df.dropna(subset=["timestamp"])


def _normalize_row(row: dict, source_type: str) -> dict:
    """Map raw row fields to unified events schema based on source_type."""
    event_id     = str(uuid.uuid4())
    actor        = None
    target       = None
    event_type   = None
    message_text = None
    deleted_flag = 0

    if source_type == "whatsapp":
        event_id     = row.get("message_id") or event_id
        actor        = row.get("sender")
        target       = row.get("receiver")
        event_type   = "message"
        message_text = row.get("message_text")
        deleted_flag = int(row.get("deleted_flag") or 0)

    elif source_type == "app_usage":
        event_type   = "app_activity"
        actor        = row.get("user_id")
        message_text = f"{row.get('app_name')} - {row.get('action_type')}"

    elif source_type == "locations":
        event_type   = "location"
        actor        = row.get("user_id")
        message_text = f"Lat:{row.get('latitude')} Lon:{row.get('longitude')}"

    elif source_type == "calls":
        event_id     = row.get("call_id") or event_id
        actor        = row.get("caller")
        target       = row.get("receiver")
        event_type   = "call"
        message_text = f"{row.get('call_type')} - {row.get('duration_seconds')} sec"

    elif source_type == "whatsapp_calls":
        event_id     = row.get("call_id") or event_id
        actor        = row.get("caller")
        target       = row.get("receiver")
        event_type   = "whatsapp_call"
        message_text = f"{row.get('call_type')} - {row.get('duration_seconds')} sec"
        deleted_flag = int(row.get("deleted_flag") or 0)

    elif source_type == "upi_transactions":
        event_id     = row.get("transaction_id") or event_id
        actor        = row.get("sender_number")
        target       = row.get("receiver_number")
        event_type   = "upi_transaction"
        message_text = f"₹{row.get('amount')} | {row.get('status')}"

    return {
        "event_id":     event_id,
        "case_id":      row.get("case_id"),
        "source_type":  source_type,
        "event_type":   event_type,
        "timestamp":    row.get("timestamp"),
        "actor_id":     actor,
        "target_id":    target,
        "message_text": message_text,
        "deleted_flag": deleted_flag,
        "language":     row.get("language"),
        "device_id":    row.get("device_id"),
        "ip_address":   row.get("ip_address"),
        "metadata":     json.dumps(row),
    }


INSERT_SQL = """
    INSERT OR IGNORE INTO events (
        event_id, case_id, source_type, event_type,
        timestamp, actor_id, target_id, message_text,
        deleted_flag, language, device_id, ip_address, metadata
    ) VALUES (
        :event_id, :case_id, :source_type, :event_type,
        :timestamp, :actor_id, :target_id, :message_text,
        :deleted_flag, :language, :device_id, :ip_address, :metadata
    )
"""


def _load_dataframe(file) -> pd.DataFrame:
    name = (file.filename or "").lower()
    if name.endswith(".csv"):
        return pd.read_csv(file.file)
    elif name.endswith(".json"):
        return pd.read_json(file.file)
    raise ValueError(f"Unsupported format: {file.filename!r}")


def ingest_multiple_files(
    file_source_pairs: List[Tuple],
) -> Tuple[int, int]:
    """
    Ingest a list of (file, source_type) pairs into the events table.
    Returns (total_inserted, total_skipped).
    """
    total_inserted = 0
    total_skipped  = 0
    skip_reasons   = []

    with managed_connection() as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        for file, source_type in file_source_pairs:

            # --- Load file ---
            try:
                df = _load_dataframe(file)
            except Exception as e:
                skip_reasons.append(f"[{file.filename}] Failed to load: {e}")
                total_skipped += 1
                continue

            if df.empty:
                skip_reasons.append(f"[{file.filename}] File is empty.")
                continue

            # --- Normalize columns ---
            df.columns = df.columns.str.strip().str.lower()

            print(f"\n[{source_type}] File: {file.filename}")
            print(f"[{source_type}] Columns: {list(df.columns)}")
            print(f"[{source_type}] Row count: {len(df)}")

            if "timestamp" not in df.columns:
                skip_reasons.append(
                    f"[{file.filename}] Missing 'timestamp' column. "
                    f"Found: {list(df.columns)}"
                )
                total_skipped += len(df)
                continue

            # --- Parse timestamps ---
            df = _parse_timestamps(df, file.filename, skip_reasons)

            if df.empty:
                skip_reasons.append(f"[{file.filename}] No valid rows after timestamp parsing.")
                continue

            df["timestamp"] = df["timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S")

            # --- Insert rows ---
            for _, row in df.iterrows():
                clean_row = {
                    k: (None if pd.isna(v) else v)
                    for k, v in row.to_dict().items()
                }
                record = _normalize_row(clean_row, source_type)

                try:
                    cursor.execute(INSERT_SQL, record)
                    # rowcount 0 = duplicate silently ignored by INSERT OR IGNORE
                    if cursor.rowcount == 1:
                        total_inserted += 1
                    else:
                        total_skipped += 1
                        skip_reasons.append(
                            f"[{source_type}] Duplicate skipped: event_id={record['event_id']!r}"
                        )

                except Exception as e:
                    skip_reasons.append(
                        f"[{source_type}] Unexpected error: {e} | event_id={record['event_id']!r}"
                    )
                    total_skipped += 1

    if skip_reasons:
        print(f"\n===== SKIPPED REASONS ({len(skip_reasons)}) =====")
        for reason in skip_reasons[:30]:
            print(reason)
        print("=" * 40)

    print(f"\n✅ Inserted: {total_inserted} | ⏭ Skipped: {total_skipped}")
    return total_inserted, total_skipped