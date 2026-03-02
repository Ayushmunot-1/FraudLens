"""
Anomalies API Routes
Fetch detected anomalies, filter by severity, and submit feedback
"""

import uuid
from fastapi import APIRouter, HTTPException, Depends, Query
from sqlalchemy.orm import Session, joinedload
from typing import Optional, List
from app.db.database import get_db
from app.models.models import Anomaly, Record, Feedback
from pydantic import BaseModel

router = APIRouter()


class FeedbackRequest(BaseModel):
    label: str      # "valid" or "false_positive"
    comment: Optional[str] = None
    user_id: Optional[str] = None


@router.get("/")
def get_anomalies(
    dataset_id: Optional[str] = Query(None),
    severity: Optional[str] = Query(None),
    anomaly_type: Optional[str] = Query(None),
    limit: int = Query(100, le=1000),
    offset: int = Query(0),
    db: Session = Depends(get_db)
):
    """
    Returns all detected anomalies, with optional filters.
    Use ?dataset_id=xxx to see anomalies for a specific upload.
    Use ?severity=high to see only high-severity anomalies.
    """
    query = db.query(Anomaly).join(Record)

    if dataset_id:
        query = query.filter(Record.dataset_id == uuid.UUID(dataset_id))
    if severity:
        query = query.filter(Anomaly.severity == severity)
    if anomaly_type:
        query = query.filter(Anomaly.anomaly_type == anomaly_type)

    total = query.count()
    anomalies = query.order_by(Anomaly.anomaly_score.desc()).offset(offset).limit(limit).all()

    return {
        "total": total,
        "offset": offset,
        "limit": limit,
        "anomalies": [
            {
                "anomaly_id": str(a.anomaly_id),
                "anomaly_type": a.anomaly_type,
                "anomaly_score": a.anomaly_score,
                "severity": a.severity,
                "explanation": a.explanation,
                "features_flagged": a.features_flagged,
                "record_data": a.record.record_data,
                "row_index": a.record.row_index,
                "detected_at": a.detected_at.isoformat()
            }
            for a in anomalies
        ]
    }


@router.get("/{anomaly_id}")
def get_anomaly_detail(anomaly_id: str, db: Session = Depends(get_db)):
    """Returns full details for a single anomaly, including the original record."""
    anomaly = db.query(Anomaly).filter(
        Anomaly.anomaly_id == uuid.UUID(anomaly_id)
    ).first()

    if not anomaly:
        raise HTTPException(status_code=404, detail="Anomaly not found")

    return {
        "anomaly_id": str(anomaly.anomaly_id),
        "anomaly_type": anomaly.anomaly_type,
        "anomaly_score": anomaly.anomaly_score,
        "severity": anomaly.severity,
        "explanation": anomaly.explanation,
        "features_flagged": anomaly.features_flagged,
        "record_data": anomaly.record.record_data,
        "detected_at": anomaly.detected_at.isoformat(),
        "feedback": [
            {
                "label": f.label,
                "comment": f.comment,
                "created_at": f.created_at.isoformat()
            }
            for f in anomaly.feedback
        ]
    }


@router.post("/{anomaly_id}/feedback")
def submit_feedback(
    anomaly_id: str,
    feedback: FeedbackRequest,
    db: Session = Depends(get_db)
):
    """
    Submit feedback on an anomaly — is it a real issue or a false alarm?
    This data will be used to improve the model over time.
    """
    if feedback.label not in ["valid", "false_positive"]:
        raise HTTPException(status_code=400, detail="Label must be 'valid' or 'false_positive'")

    anomaly = db.query(Anomaly).filter(
        Anomaly.anomaly_id == uuid.UUID(anomaly_id)
    ).first()

    if not anomaly:
        raise HTTPException(status_code=404, detail="Anomaly not found")

    fb = Feedback(
        anomaly_id=anomaly.anomaly_id,
        user_id=uuid.UUID(feedback.user_id) if feedback.user_id else None,
        label=feedback.label,
        comment=feedback.comment
    )
    db.add(fb)
    db.commit()

    return {"success": True, "message": "Feedback recorded. Thank you!"}