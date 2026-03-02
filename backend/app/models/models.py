"""
Database Models (Tables)
Each class = one table in PostgreSQL
"""

import uuid
from datetime import datetime
from sqlalchemy import Column, String, Float, Text, DateTime, Enum, ForeignKey, JSON, Boolean
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship
from app.db.database import Base
import enum


# ─── Enums ───────────────────────────────────────────────────────────────────

class UserRole(str, enum.Enum):
    admin = "admin"
    manager = "manager"
    auditor = "auditor"


class SeverityLevel(str, enum.Enum):
    low = "low"
    medium = "medium"
    high = "high"


class FeedbackLabel(str, enum.Enum):
    valid = "valid"
    false_positive = "false_positive"


class UploadType(str, enum.Enum):
    csv = "CSV"
    excel = "Excel"
    pdf = "PDF"


# ─── Tables ──────────────────────────────────────────────────────────────────

class Organization(Base):
    __tablename__ = "organizations"

    org_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    org_name = Column(String(255), nullable=False)
    industry = Column(String(100), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    # Relationships
    users = relationship("User", back_populates="organization")
    datasets = relationship("Dataset", back_populates="organization")


class User(Base):
    __tablename__ = "users"

    user_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    org_id = Column(UUID(as_uuid=True), ForeignKey("organizations.org_id"), nullable=True)
    name = Column(String(255), nullable=False)
    email = Column(String(255), unique=True, nullable=False)
    password_hash = Column(String(255), nullable=False)
    role = Column(Enum(UserRole), default=UserRole.auditor)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    # Relationships
    organization = relationship("Organization", back_populates="users")
    feedback = relationship("Feedback", back_populates="user")


class Domain(Base):
    __tablename__ = "domains"

    domain_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    domain_name = Column(String(100), nullable=False)  # e.g. "ERP/SAP Invoice"
    description = Column(Text, nullable=True)

    # Relationships
    datasets = relationship("Dataset", back_populates="domain")


class Dataset(Base):
    __tablename__ = "datasets"

    dataset_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    org_id = Column(UUID(as_uuid=True), ForeignKey("organizations.org_id"), nullable=True)
    domain_id = Column(UUID(as_uuid=True), ForeignKey("domains.domain_id"), nullable=False)
    dataset_name = Column(String(255), nullable=False)
    upload_type = Column(Enum(UploadType), nullable=False)
    file_path = Column(String(500), nullable=True)  # Where file is stored
    row_count = Column(Float, nullable=True)
    status = Column(String(50), default="uploaded")  # uploaded → processing → done → error
    created_at = Column(DateTime, default=datetime.utcnow)

    # Relationships
    organization = relationship("Organization", back_populates="datasets")
    domain = relationship("Domain", back_populates="datasets")
    records = relationship("Record", back_populates="dataset")


class Record(Base):
    __tablename__ = "records"

    record_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    dataset_id = Column(UUID(as_uuid=True), ForeignKey("datasets.dataset_id"), nullable=False)
    record_data = Column(JSON, nullable=False)  # Full row stored as JSON
    row_index = Column(Float, nullable=True)    # Original row number in file
    created_at = Column(DateTime, default=datetime.utcnow)

    # Relationships
    dataset = relationship("Dataset", back_populates="records")
    anomalies = relationship("Anomaly", back_populates="record")


class Anomaly(Base):
    __tablename__ = "anomalies"

    anomaly_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    record_id = Column(UUID(as_uuid=True), ForeignKey("records.record_id"), nullable=False)
    anomaly_score = Column(Float, nullable=False)  # 0–100 risk score
    severity = Column(Enum(SeverityLevel), nullable=False)
    anomaly_type = Column(String(100), nullable=True)  # e.g. "duplicate_invoice"
    explanation = Column(Text, nullable=False)          # Human-readable reason
    features_flagged = Column(JSON, nullable=True)      # Which fields triggered it
    detected_at = Column(DateTime, default=datetime.utcnow)

    # Relationships
    record = relationship("Record", back_populates="anomalies")
    feedback = relationship("Feedback", back_populates="anomaly")


class Feedback(Base):
    __tablename__ = "feedback"

    feedback_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    anomaly_id = Column(UUID(as_uuid=True), ForeignKey("anomalies.anomaly_id"), nullable=False)
    user_id = Column(UUID(as_uuid=True), ForeignKey("users.user_id"), nullable=False)
    label = Column(Enum(FeedbackLabel), nullable=False)
    comment = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    # Relationships
    anomaly = relationship("Anomaly", back_populates="feedback")
    user = relationship("User", back_populates="feedback")