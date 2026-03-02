"""
FraudLens — ERP/SAP Invoice Anomaly Detection Platform
Main FastAPI application entry point — Phase 4 with Auth
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.api import auth, datasets, anomalies, dashboard

app = FastAPI(
    title="FraudLens — ERP Anomaly Detection",
    description="AI-powered invoice fraud detection for ERP/SAP systems",
    version="2.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://127.0.0.1:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Register routes — auth prefix is handled inside auth.py
app.include_router(auth.router)
app.include_router(datasets.router, prefix="/api/datasets", tags=["Datasets"])
app.include_router(anomalies.router, prefix="/api/anomalies", tags=["Anomalies"])
app.include_router(dashboard.router, prefix="/api/dashboard", tags=["Dashboard"])


@app.get("/")
def root():
    return {"message": "FraudLens API is running", "version": "2.0.0"}


@app.get("/health")
def health_check():
    return {"status": "healthy"}