from reportlab.platypus import (
    SimpleDocTemplate,
    Paragraph,
    Spacer,
    Table,
    TableStyle,
    ListFlowable,
    ListItem
)
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib.units import inch
from datetime import datetime
from pathlib import Path

from app.core.config import settings
from app.core.database import get_connection
from app.services.risk_engine import compute_suspicious_users


def generate_report() -> str:
    """
    Generates forensic intelligence PDF report.
    Returns file path.
    """

    # Ensure reports directory exists
    settings.REPORTS_DIR.mkdir(exist_ok=True)

    conn = get_connection()
    cursor = conn.cursor()
    total_events = cursor.execute("SELECT COUNT(*) FROM events").fetchone()[0]
    conn.close()

    suspicious_users = compute_suspicious_users()

    # ---- File Path ----
    timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_path = settings.REPORTS_DIR / f"sentinelx_report_{timestamp_str}.pdf"

    doc = SimpleDocTemplate(str(file_path))
    elements = []
    styles = getSampleStyleSheet()

    # ---- Title ----
    elements.append(
        Paragraph("SentinelX Forensic Intelligence Report", styles["Heading1"])
    )
    elements.append(Spacer(1, 0.3 * inch))

    # ---- Metadata ----
    elements.append(
        Paragraph(f"Generated On: {datetime.now()}", styles["Normal"])
    )
    elements.append(
        Paragraph(f"Total Events Analyzed: {total_events}", styles["Normal"])
    )
    elements.append(
        Paragraph(f"Total Suspicious Users: {len(suspicious_users)}", styles["Normal"])
    )
    elements.append(Spacer(1, 0.4 * inch))

    # ---- Suspicious Users Table ----
    if suspicious_users:

        elements.append(
            Paragraph("High-Risk Entities", styles["Heading2"])
        )
        elements.append(Spacer(1, 0.2 * inch))

        table_data = [
            ["User ID", "Risk Score", "Late Night", "Deleted", "Financial"]
        ]

        for user in suspicious_users:
            stats = user["stats"]
            table_data.append([
                user["user"],
                str(user["risk_score"]),
                str(stats["late_night"]),
                str(stats["deleted"]),
                str(stats["financial"]),
            ])

        table = Table(table_data, repeatRows=1)

        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('GRID', (0, 0), (-1, -1), 1, colors.black),
            ('ALIGN', (1, 1), (-1, -1), 'CENTER'),
        ]))

        elements.append(table)
        elements.append(Spacer(1, 0.5 * inch))

    # ---- Methodology Section ----
    elements.append(
        Paragraph("Analysis Methodology", styles["Heading2"])
    )
    elements.append(Spacer(1, 0.2 * inch))

    methodology_points = [
        "Behavioral density scoring applied to user communication patterns.",
        "Weighted metrics include late-night activity, message deletion frequency, and financial keyword density.",
        "Minimum activity threshold enforced to avoid small-sample bias.",
        "Network graph intelligence applied for structural analysis."
    ]

    elements.append(
        ListFlowable(
            [ListItem(Paragraph(point, styles["Normal"])) for point in methodology_points],
            bulletType="bullet"
        )
    )

    doc.build(elements)

    return str(file_path)
