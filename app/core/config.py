from pathlib import Path


class Settings:
    """
    Central configuration for SentinelX.
    All project-level constants and paths are defined here.
    """

    # Project base directory
    BASE_DIR = Path(__file__).resolve().parent.parent.parent

    # Data directory
    DATA_DIR = BASE_DIR / "data"

    # Database file path
    DATABASE_PATH = DATA_DIR / "sentinelx.db"

    # Report output directory
    REPORTS_DIR = BASE_DIR / "reports"

    # Risk scoring weights (Behavioral Model)
    LATE_NIGHT_WEIGHT = 40
    DELETED_WEIGHT = 40
    FINANCIAL_WEIGHT = 20

    # Risk thresholds
    MIN_MESSAGES_THRESHOLD = 20
    HIGH_RISK_THRESHOLD = 40
    MEDIUM_RISK_THRESHOLD = 25


# Create a single settings instance
settings = Settings()
