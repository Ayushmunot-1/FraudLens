"""
Dashboard API Routes
Aggregated KPIs and trend data for the main dashboard
"""

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from sqlalchemy import func
from app.db.database import get_db
from app.models.models import Anomaly, Dataset, Record
from datetime import datetime, timedelta

router = APIRouter()


@router.get("/kpis")
def get_kpis(db: Session = Depends(get_db)):
    """
    Returns the top-level KPI cards shown on the dashboard:
    - Total datasets uploaded
    - Total records analyzed
    - Total anomalies found
    - Breakdown by severity
    """
    total_datasets = db.query(Dataset).count()
    total_records = db.query(Record).count()
    total_anomalies = db.query(Anomaly).count()

    high = db.query(Anomaly).filter(Anomaly.severity == "high").count()
    medium = db.query(Anomaly).filter(Anomaly.severity == "medium").count()
    low = db.query(Anomaly).filter(Anomaly.severity == "low").count()

    avg_score = db.query(func.avg(Anomaly.anomaly_score)).scalar() or 0

    return {
        "total_datasets": total_datasets,
        "total_records": total_records,
        "total_anomalies": total_anomalies,
        "avg_anomaly_score": round(float(avg_score), 1),
        "severity_breakdown": {
            "high": high,
            "medium": medium,
            "low": low
        }
    }


@router.get("/anomaly-types")
def get_anomaly_type_breakdown(db: Session = Depends(get_db)):
    """Returns count of each anomaly type — useful for bar charts."""
    results = db.query(
        Anomaly.anomaly_type,
        func.count(Anomaly.anomaly_id).label("count")
    ).group_by(Anomaly.anomaly_type).all()

    return [{"type": r.anomaly_type, "count": r.count} for r in results]


@router.get("/recent-anomalies")
def get_recent_anomalies(limit: int = 10, db: Session = Depends(get_db)):
    """Returns the most recently detected anomalies for the activity feed."""
    anomalies = db.query(Anomaly).order_by(
        Anomaly.detected_at.desc()
    ).limit(limit).all()

    return [
        {
            "anomaly_id": str(a.anomaly_id),
            "anomaly_type": a.anomaly_type,
            "severity": a.severity,
            "anomaly_score": a.anomaly_score,
            "explanation": a.explanation[:100] + "..." if len(a.explanation) > 100 else a.explanation,
            "detected_at": a.detected_at.isoformat()
        }
        for a in anomalies
    ]