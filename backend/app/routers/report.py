from fastapi import APIRouter, HTTPException
from app.services.report_service import generate_report

router = APIRouter(prefix="/report", tags=["Forensic Report"])


@router.get("/")
def create_report():
    """
    Generates forensic intelligence PDF report.
    """

    try:
        file_path = generate_report()

        return {
            "status": "success",
            "message": "Report generated successfully",
            "report_path": file_path
        }

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Report generation failed: {str(e)}"
        )
