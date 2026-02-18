from fastapi import APIRouter, UploadFile, File, Form, HTTPException
from typing import List
from app.services.ingestion_service import ingest_multiple_files, SUPPORTED_SOURCES

router = APIRouter(prefix="/upload", tags=["Upload"])


@router.post("/multiple")
async def upload_multiple_files(
    source_types: List[str] = Form(...),
    files: List[UploadFile] = File(...)
):
    """
    Upload multiple files, each with its own source_type.
    source_types and files must be in the same order.
    """
    # Swagger sends all source_types as one comma-separated string — fix that
    normalized: List[str] = []
    for entry in source_types:
        normalized.extend([s.strip() for s in entry.split(",")])

    # Validate source types
    invalid = [s for s in normalized if s not in SUPPORTED_SOURCES]
    if invalid:
        raise HTTPException(
            status_code=422,
            detail=f"Unsupported source_type(s): {invalid}. "
                   f"Must be one of: {sorted(SUPPORTED_SOURCES)}"
        )

    # Validate count match
    if len(normalized) != len(files):
        raise HTTPException(
            status_code=422,
            detail=f"Mismatch: {len(files)} file(s) but {len(normalized)} source_type(s) provided."
        )

    inserted, skipped = ingest_multiple_files(list(zip(files, normalized)))

    return {
        "status": "success",
        "records_inserted": inserted,
        "records_skipped": skipped,
    }