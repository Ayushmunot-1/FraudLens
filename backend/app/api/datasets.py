# # """
# # Dataset API Routes
# # Handles file upload, ingestion, and triggering anomaly detection
# # """

# # import os
# # import uuid
# # import pandas as pd
# # from fastapi import APIRouter, UploadFile, File, HTTPException, Depends, Form
# # from sqlalchemy.orm import Session
# # from typing import Optional
# # from app.db.database import get_db
# # from app.models.models import Dataset, Record, Anomaly, Domain
# # from app.ml.erp_detector import ERPAnomalyDetector
# # from app.core.config import settings
# # import logging

# # router = APIRouter()
# # logger = logging.getLogger(__name__)


# # @router.post("/upload")
# # async def upload_dataset(
# #     file: UploadFile = File(...),
# #     dataset_name: str = Form(...),
# #     db: Session = Depends(get_db)
# # ):
# #     """
# #     Upload a CSV or Excel file for ERP/SAP invoice analysis.
# #     This endpoint:
# #       1. Saves the file
# #       2. Parses it into records
# #       3. Runs anomaly detection
# #       4. Returns a summary of findings
# #     """
# #     # Validate file type
# #     allowed_extensions = [".csv", ".xlsx", ".xls"]
# #     file_ext = os.path.splitext(file.filename)[1].lower()
# #     if file_ext not in allowed_extensions:
# #         raise HTTPException(
# #             status_code=400,
# #             detail=f"Unsupported file type: {file_ext}. Please upload CSV or Excel."
# #         )

# #     # Save the file
# #     os.makedirs(settings.UPLOAD_DIR, exist_ok=True)
# #     file_id = str(uuid.uuid4())
# #     file_path = os.path.join(settings.UPLOAD_DIR, f"{file_id}{file_ext}")

# #     content = await file.read()
# #     if len(content) > settings.MAX_FILE_SIZE_MB * 1024 * 1024:
# #         raise HTTPException(status_code=400, detail="File too large. Max 50MB.")

# #     with open(file_path, "wb") as f:
# #         f.write(content)

# #     # Parse file into DataFrame
# #     try:
# #         if file_ext == ".csv":
# #             df = pd.read_csv(file_path)
# #         else:
# #             df = pd.read_excel(file_path)
# #     except Exception as e:
# #         raise HTTPException(status_code=422, detail=f"Could not parse file: {str(e)}")

# #     if df.empty:
# #         raise HTTPException(status_code=422, detail="Uploaded file is empty.")

# #     # Get or create the ERP/SAP domain
# #     domain = db.query(Domain).filter(Domain.domain_name == "ERP/SAP Invoice").first()
# #     if not domain:
# #         domain = Domain(domain_name="ERP/SAP Invoice", description="ERP and SAP invoice monitoring")
# #         db.add(domain)
# #         db.flush()

# #     # Create dataset record
# #     dataset = Dataset(
# #         dataset_name=dataset_name,
# #         domain_id=domain.domain_id,
# #         upload_type="CSV" if file_ext == ".csv" else "Excel",
# #         file_path=file_path,
# #         row_count=len(df),
# #         status="processing"
# #     )
# #     db.add(dataset)
# #     db.flush()

# #     # Store each row as a Record in the database
# #     records_map = {}  # row_index → Record object
# #     for idx, row in df.iterrows():
# #         record = Record(
# #             dataset_id=dataset.dataset_id,
# #             record_data=row.where(pd.notnull(row), None).to_dict(),
# #             row_index=int(idx)
# #         )
# #         db.add(record)
# #         db.flush()
# #         records_map[int(idx)] = record

# #     # Run anomaly detection
# #     detector = ERPAnomalyDetector(
# #         zscore_threshold=settings.ZSCORE_THRESHOLD,
# #         contamination=settings.ISOLATION_FOREST_CONTAMINATION
# #     )
# #     anomaly_results = detector.analyze(df)

# #     # Save anomalies to database
# #     saved_anomalies = 0
# #     severity_counts = {"high": 0, "medium": 0, "low": 0}

# #     for result in anomaly_results:
# #         row_idx = result["row_index"]
# #         if row_idx not in records_map:
# #             continue

# #         anomaly = Anomaly(
# #             record_id=records_map[row_idx].record_id,
# #             anomaly_score=result["anomaly_score"],
# #             severity=result["severity"],
# #             anomaly_type=result["anomaly_type"],
# #             explanation=result["explanation"],
# #             features_flagged=result["features_flagged"]
# #         )
# #         db.add(anomaly)
# #         saved_anomalies += 1
# #         severity_counts[result["severity"]] += 1

# #     # Mark dataset as done
# #     dataset.status = "done"
# #     db.commit()

# #     return {
# #         "success": True,
# #         "dataset_id": str(dataset.dataset_id),
# #         "dataset_name": dataset_name,
# #         "rows_analyzed": len(df),
# #         "anomalies_found": saved_anomalies,
# #         "severity_breakdown": severity_counts,
# #         "columns_detected": list(df.columns),
# #         "message": f"Analysis complete. Found {saved_anomalies} anomalies in {len(df)} records."
# #     }


# # @router.get("/")
# # def list_datasets(db: Session = Depends(get_db)):
# #     """Returns all uploaded datasets."""
# #     datasets = db.query(Dataset).order_by(Dataset.created_at.desc()).all()
# #     return [
# #         {
# #             "dataset_id": str(d.dataset_id),
# #             "dataset_name": d.dataset_name,
# #             "upload_type": d.upload_type,
# #             "row_count": d.row_count,
# #             "status": d.status,
# #             "created_at": d.created_at.isoformat()
# #         }
# #         for d in datasets
# #     ]


# # @router.get("/{dataset_id}/summary")
# # def get_dataset_summary(dataset_id: str, db: Session = Depends(get_db)):
# #     """Returns summary statistics for a specific dataset."""
# #     dataset = db.query(Dataset).filter(
# #         Dataset.dataset_id == uuid.UUID(dataset_id)
# #     ).first()

# #     if not dataset:
# #         raise HTTPException(status_code=404, detail="Dataset not found")

# #     anomaly_count = sum(
# #         1 for record in dataset.records
# #         for _ in record.anomalies
# #     )

# #     return {
# #         "dataset_id": dataset_id,
# #         "dataset_name": dataset.dataset_name,
# #         "status": dataset.status,
# #         "row_count": dataset.row_count,
# #         "anomaly_count": anomaly_count,
# #         "created_at": dataset.created_at.isoformat()
# #     }


# """
# Dataset API Routes
# Handles file upload, ingestion, and triggering anomaly detection
# """

# import os
# import uuid
# import pandas as pd
# from fastapi import APIRouter, UploadFile, File, HTTPException, Depends, Form
# from sqlalchemy.orm import Session
# from typing import Optional
# from app.db.database import get_db
# from app.models.models import Dataset, Record, Anomaly, Domain
# from app.ml.erp_detector import ERPAnomalyDetector
# from app.core.config import settings
# import logging
# from app.services.email_service import email_service

# router = APIRouter()
# logger = logging.getLogger(__name__)


# @router.post("/upload")
# async def upload_dataset(
#     file: UploadFile = File(...),
#     dataset_name: str = Form(...),
#     db: Session = Depends(get_db)
# ):
#     """
#     Upload a CSV or Excel file for ERP/SAP invoice analysis.
#     This endpoint:
#       1. Saves the file
#       2. Parses it into records
#       3. Runs anomaly detection
#       4. Returns a summary of findings
#     """
#     # Validate file type
#     allowed_extensions = [".csv", ".xlsx", ".xls"]
#     file_ext = os.path.splitext(file.filename)[1].lower()
#     if file_ext not in allowed_extensions:
#         raise HTTPException(
#             status_code=400,
#             detail=f"Unsupported file type: {file_ext}. Please upload CSV or Excel."
#         )

#     # Save the file
#     os.makedirs(settings.UPLOAD_DIR, exist_ok=True)
#     file_id = str(uuid.uuid4())
#     file_path = os.path.join(settings.UPLOAD_DIR, f"{file_id}{file_ext}")

#     content = await file.read()
#     if len(content) > settings.MAX_FILE_SIZE_MB * 1024 * 1024:
#         raise HTTPException(status_code=400, detail="File too large. Max 50MB.")

#     with open(file_path, "wb") as f:
#         f.write(content)

#     # Parse file into DataFrame
#     try:
#         if file_ext == ".csv":
#             df = pd.read_csv(file_path)
#         else:
#             df = pd.read_excel(file_path)
#     except Exception as e:
#         raise HTTPException(status_code=422, detail=f"Could not parse file: {str(e)}")

#     if df.empty:
#         raise HTTPException(status_code=422, detail="Uploaded file is empty.")

#     # Get or create the ERP/SAP domain
#     domain = db.query(Domain).filter(Domain.domain_name == "ERP/SAP Invoice").first()
#     if not domain:
#         domain = Domain(domain_name="ERP/SAP Invoice", description="ERP and SAP invoice monitoring")
#         db.add(domain)
#         db.flush()

#     # Create dataset record
#     dataset = Dataset(
#         dataset_name=dataset_name,
#         domain_id=domain.domain_id,
#         upload_type="CSV" if file_ext == ".csv" else "Excel",
#         file_path=file_path,
#         row_count=len(df),
#         status="processing"
#     )
#     db.add(dataset)
#     db.flush()

#     # Store each row as a Record in the database
#     records_map = {}  # row_index → Record object
#     for idx, row in df.iterrows():
#         record = Record(
#             dataset_id=dataset.dataset_id,
#             record_data=row.where(pd.notnull(row), None).to_dict(),
#             row_index=int(idx)
#         )
#         db.add(record)
#         db.flush()
#         records_map[int(idx)] = record

#     # Run anomaly detection
#     detector = ERPAnomalyDetector(
#         zscore_threshold=settings.ZSCORE_THRESHOLD,
#         contamination=settings.ISOLATION_FOREST_CONTAMINATION
#     )
#     anomaly_results = detector.analyze(df)

#     # Save anomalies to database
#     saved_anomalies = 0
#     severity_counts = {"high": 0, "medium": 0, "low": 0}

#     for result in anomaly_results:
#         row_idx = result["row_index"]
#         if row_idx not in records_map:
#             continue

#         anomaly = Anomaly(
#             record_id=records_map[row_idx].record_id,
#             anomaly_score=result["anomaly_score"],
#             severity=result["severity"],
#             anomaly_type=result["anomaly_type"],
#             explanation=result["explanation"],
#             features_flagged=result["features_flagged"]
#         )
#         db.add(anomaly)
#         saved_anomalies += 1
#         severity_counts[result["severity"]] += 1

#     # Mark dataset as done
#     dataset.status = "done"
#     db.commit()

#     # Send email alert if high severity anomalies were found
#     if severity_counts["high"] > 0:
#         high_results = [r for r in anomaly_results if r["severity"] == "high"]
#         email_service.send_anomaly_alert(
#             dataset_name=dataset_name,
#             total_anomalies=saved_anomalies,
#             high_anomalies=high_results,
#             severity_breakdown=severity_counts
#         )

#     return {
#         "success": True,
#         "dataset_id": str(dataset.dataset_id),
#         "dataset_name": dataset_name,
#         "rows_analyzed": len(df),
#         "anomalies_found": saved_anomalies,
#         "severity_breakdown": severity_counts,
#         "columns_detected": list(df.columns),
#         "message": f"Analysis complete. Found {saved_anomalies} anomalies in {len(df)} records."
#     }


# @router.get("/")
# def list_datasets(db: Session = Depends(get_db)):
#     """Returns all uploaded datasets."""
#     datasets = db.query(Dataset).order_by(Dataset.created_at.desc()).all()
#     return [
#         {
#             "dataset_id": str(d.dataset_id),
#             "dataset_name": d.dataset_name,
#             "upload_type": d.upload_type,
#             "row_count": d.row_count,
#             "status": d.status,
#             "created_at": d.created_at.isoformat()
#         }
#         for d in datasets
#     ]


# @router.get("/{dataset_id}/summary")
# def get_dataset_summary(dataset_id: str, db: Session = Depends(get_db)):
#     """Returns summary statistics for a specific dataset."""
#     dataset = db.query(Dataset).filter(
#         Dataset.dataset_id == uuid.UUID(dataset_id)
#     ).first()

#     if not dataset:
#         raise HTTPException(status_code=404, detail="Dataset not found")

#     anomaly_count = sum(
#         1 for record in dataset.records
#         for _ in record.anomalies
#     )

#     return {
#         "dataset_id": dataset_id,
#         "dataset_name": dataset.dataset_name,
#         "status": dataset.status,
#         "row_count": dataset.row_count,
#         "anomaly_count": anomaly_count,
#         "created_at": dataset.created_at.isoformat()
#     }

"""
Dataset API Routes
Handles file upload, ingestion, and triggering anomaly detection
"""

import os
import uuid
import pandas as pd
from fastapi import APIRouter, UploadFile, File, HTTPException, Depends, Form
from sqlalchemy.orm import Session
from typing import Optional
from app.db.database import get_db
from app.models.models import Dataset, Record, Anomaly, Domain
from app.ml.erp_detector import ERPAnomalyDetector
from app.core.config import settings
import logging
from app.services.email_service import email_service
from app.services.file_parser import UniversalFileParser

router = APIRouter()
logger = logging.getLogger(__name__)


@router.post("/upload")
async def upload_dataset(
    file: UploadFile = File(...),
    dataset_name: str = Form(...),
    db: Session = Depends(get_db)
):
    """
    Upload a CSV or Excel file for ERP/SAP invoice analysis.
    This endpoint:
      1. Saves the file
      2. Parses it into records
      3. Runs anomaly detection
      4. Returns a summary of findings
    """
    # Validate file type
    allowed_extensions = [".csv", ".xlsx", ".xls", ".pdf"]
    file_ext = os.path.splitext(file.filename)[1].lower()
    if file_ext not in allowed_extensions:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported file type: {file_ext}. Please upload CSV, Excel or PDF."
        )

    # Save the file
    os.makedirs(settings.UPLOAD_DIR, exist_ok=True)
    file_id = str(uuid.uuid4())
    file_path = os.path.join(settings.UPLOAD_DIR, f"{file_id}{file_ext}")

    content = await file.read()
    if len(content) > settings.MAX_FILE_SIZE_MB * 1024 * 1024:
        raise HTTPException(status_code=400, detail="File too large. Max 50MB.")

    with open(file_path, "wb") as f:
        f.write(content)

    # Parse file into DataFrame using Universal Parser
    try:
        parser = UniversalFileParser()
        df, detected_type, parse_notes = parser.parse(file_path)
        logger.info(f"Parse notes: {parse_notes}")
    except Exception as e:
        raise HTTPException(status_code=422, detail=f"Could not parse file: {str(e)}")

    if df.empty:
        raise HTTPException(status_code=422, detail="Uploaded file is empty.")

    # Get or create the ERP/SAP domain
    domain = db.query(Domain).filter(Domain.domain_name == "ERP/SAP Invoice").first()
    if not domain:
        domain = Domain(domain_name="ERP/SAP Invoice", description="ERP and SAP invoice monitoring")
        db.add(domain)
        db.flush()

    # Create dataset record
    dataset = Dataset(
        dataset_name=dataset_name,
        domain_id=domain.domain_id,
        upload_type=detected_type,
        file_path=file_path,
        row_count=len(df),
        status="processing"
    )
    db.add(dataset)
    db.flush()

    # Store each row as a Record in the database
    records_map = {}  # row_index → Record object
    for idx, row in df.iterrows():
        # Convert row to dict, turning Timestamps into strings and NaN into None
        row_dict = {}
        for k, v in row.items():
            if isinstance(v, pd.Timestamp):
                row_dict[k] = str(v.date())
            elif isinstance(v, float) and pd.isna(v):
                row_dict[k] = None
            else:
                row_dict[k] = v
        record = Record(
            dataset_id=dataset.dataset_id,
            record_data=row_dict,
            row_index=int(idx)
        )
        db.add(record)
        db.flush()
        records_map[int(idx)] = record

    # Run anomaly detection
    detector = ERPAnomalyDetector(
        zscore_threshold=settings.ZSCORE_THRESHOLD,
        contamination=settings.ISOLATION_FOREST_CONTAMINATION
    )
    anomaly_results = detector.analyze(df)

    # Save anomalies to database
    saved_anomalies = 0
    severity_counts = {"high": 0, "medium": 0, "low": 0}

    for result in anomaly_results:
        row_idx = result["row_index"]
        if row_idx not in records_map:
            continue

        anomaly = Anomaly(
            record_id=records_map[row_idx].record_id,
            anomaly_score=float(result["anomaly_score"]),
            severity=result["severity"],
            anomaly_type=result["anomaly_type"],
            explanation=result["explanation"],
            features_flagged=result["features_flagged"]
        )
        db.add(anomaly)
        saved_anomalies += 1
        severity_counts[result["severity"]] += 1

    # Mark dataset as done
    dataset.status = "done"
    db.commit()

    # Send email alert if high severity anomalies were found
    if severity_counts["high"] > 0:
        high_results = [r for r in anomaly_results if r["severity"] == "high"]
        email_service.send_anomaly_alert(
            dataset_name=dataset_name,
            total_anomalies=saved_anomalies,
            high_anomalies=high_results,
            severity_breakdown=severity_counts
        )

    return {
        "success": True,
        "dataset_id": str(dataset.dataset_id),
        "dataset_name": dataset_name,
        "rows_analyzed": len(df),
        "anomalies_found": saved_anomalies,
        "severity_breakdown": severity_counts,
        "columns_detected": list(df.columns),
        "message": f"Analysis complete. Found {saved_anomalies} anomalies in {len(df)} records."
    }


@router.get("/")
def list_datasets(db: Session = Depends(get_db)):
    """Returns all uploaded datasets."""
    datasets = db.query(Dataset).order_by(Dataset.created_at.desc()).all()
    return [
        {
            "dataset_id": str(d.dataset_id),
            "dataset_name": d.dataset_name,
            "upload_type": d.upload_type,
            "row_count": d.row_count,
            "status": d.status,
            "created_at": d.created_at.isoformat()
        }
        for d in datasets
    ]


@router.get("/{dataset_id}/summary")
def get_dataset_summary(dataset_id: str, db: Session = Depends(get_db)):
    """Returns summary statistics for a specific dataset."""
    dataset = db.query(Dataset).filter(
        Dataset.dataset_id == uuid.UUID(dataset_id)
    ).first()

    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")

    anomaly_count = sum(
        1 for record in dataset.records
        for _ in record.anomalies
    )

    return {
        "dataset_id": dataset_id,
        "dataset_name": dataset.dataset_name,
        "status": dataset.status,
        "row_count": dataset.row_count,
        "anomaly_count": anomaly_count,
        "created_at": dataset.created_at.isoformat()
    }