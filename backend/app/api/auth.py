"""
Authentication API — Phase 4
==============================
Endpoints:
  POST /api/auth/register  — Create a new user account
  POST /api/auth/login     — Login and get JWT token
  GET  /api/auth/me        — Get current user profile
  GET  /api/auth/users     — List all users (admin only)
  PUT  /api/auth/users/{id}/role — Change a user's role (admin only)

Roles:
  admin   — Full access: upload, view, feedback, manage users
  auditor — Can upload, view anomalies, submit feedback
  manager — Read-only: dashboard and anomaly viewing only
"""

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from sqlalchemy.orm import Session
from pydantic import BaseModel, EmailStr
from typing import Optional
from datetime import datetime, timedelta
import jwt
import bcrypt
import uuid

from app.db.database import get_db
from app.models.models import User, UserRole, Organization
from app.core.config import settings

router = APIRouter(prefix="/api/auth", tags=["Authentication"])
security = HTTPBearer()


# ─── Pydantic Schemas ─────────────────────────────────────────────────────────

class RegisterRequest(BaseModel):
    name: str
    email: str
    password: str
    role: Optional[str] = "auditor"
    org_name: Optional[str] = "Default Organisation"

class LoginRequest(BaseModel):
    email: str
    password: str

class RoleUpdateRequest(BaseModel):
    role: str


# ─── JWT Helpers ──────────────────────────────────────────────────────────────

def create_token(user_id: str, email: str, role: str) -> str:
    payload = {
        "sub": user_id,
        "email": email,
        "role": role,
        "exp": datetime.utcnow() + timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)
    }
    return jwt.encode(payload, settings.SECRET_KEY, algorithm=settings.ALGORITHM)


def decode_token(token: str) -> dict:
    try:
        return jwt.decode(token, settings.SECRET_KEY, algorithms=[settings.ALGORITHM])
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expired — please log in again")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Invalid token")


def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: Session = Depends(get_db)
) -> User:
    payload = decode_token(credentials.credentials)
    user = db.query(User).filter(User.user_id == uuid.UUID(payload["sub"])).first()
    if not user or not user.is_active:
        raise HTTPException(status_code=401, detail="User not found or inactive")
    return user


def require_admin(current_user: User = Depends(get_current_user)) -> User:
    if current_user.role != UserRole.admin:
        raise HTTPException(status_code=403, detail="Admin access required")
    return current_user


def require_auditor_or_admin(current_user: User = Depends(get_current_user)) -> User:
    if current_user.role == UserRole.manager:
        raise HTTPException(status_code=403, detail="Auditor or Admin access required")
    return current_user


# ─── Password Helpers ─────────────────────────────────────────────────────────

def hash_password(password: str) -> str:
    return bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()


def verify_password(password: str, hashed: str) -> bool:
    return bcrypt.checkpw(password.encode(), hashed.encode())


# ─── Endpoints ────────────────────────────────────────────────────────────────

@router.post("/register")
def register(req: RegisterRequest, db: Session = Depends(get_db)):
    """Register a new user. First user is automatically made admin."""

    # Check email not already taken
    existing = db.query(User).filter(User.email == req.email.lower().strip()).first()
    if existing:
        raise HTTPException(status_code=400, detail="Email already registered")

    # Validate role
    valid_roles = ["admin", "auditor", "manager"]
    if req.role not in valid_roles:
        raise HTTPException(status_code=400, detail=f"Role must be one of: {valid_roles}")

    # Get or create organisation
    org = db.query(Organization).filter(Organization.org_name == req.org_name).first()
    if not org:
        org = Organization(org_name=req.org_name)
        db.add(org)
        db.flush()

    # First user ever = auto admin
    user_count = db.query(User).count()
    role = UserRole.admin if user_count == 0 else UserRole(req.role)

    user = User(
        name=req.name.strip(),
        email=req.email.lower().strip(),
        password_hash=hash_password(req.password),
        role=role,
        org_id=org.org_id,
        is_active=True
    )
    db.add(user)
    db.commit()
    db.refresh(user)

    token = create_token(str(user.user_id), user.email, user.role.value)

    return {
        "token": token,
        "user": {
            "user_id": str(user.user_id),
            "name": user.name,
            "email": user.email,
            "role": user.role.value,
            "created_at": str(user.created_at)
        },
        "message": f"Account created successfully. Role: {role.value}"
    }


@router.post("/login")
def login(req: LoginRequest, db: Session = Depends(get_db)):
    """Login with email and password. Returns JWT token."""

    user = db.query(User).filter(User.email == req.email.lower().strip()).first()

    if not user or not verify_password(req.password, user.password_hash):
        raise HTTPException(status_code=401, detail="Invalid email or password")

    if not user.is_active:
        raise HTTPException(status_code=403, detail="Account is deactivated")

    token = create_token(str(user.user_id), user.email, user.role.value)

    return {
        "token": token,
        "user": {
            "user_id": str(user.user_id),
            "name": user.name,
            "email": user.email,
            "role": user.role.value
        }
    }


@router.get("/me")
def get_me(current_user: User = Depends(get_current_user)):
    """Get current logged-in user profile."""
    return {
        "user_id": str(current_user.user_id),
        "name": current_user.name,
        "email": current_user.email,
        "role": current_user.role.value,
        "is_active": current_user.is_active,
        "created_at": str(current_user.created_at)
    }


@router.get("/users")
def list_users(current_user: User = Depends(require_admin), db: Session = Depends(get_db)):
    """List all users — admin only."""
    users = db.query(User).all()
    return [
        {
            "user_id": str(u.user_id),
            "name": u.name,
            "email": u.email,
            "role": u.role.value,
            "is_active": u.is_active,
            "created_at": str(u.created_at)
        }
        for u in users
    ]


@router.put("/users/{user_id}/role")
def update_role(user_id: str, req: RoleUpdateRequest, current_user: User = Depends(require_admin), db: Session = Depends(get_db)):
    """Change a user's role — admin only."""
    valid_roles = ["admin", "auditor", "manager"]
    if req.role not in valid_roles:
        raise HTTPException(status_code=400, detail=f"Role must be one of: {valid_roles}")

    user = db.query(User).filter(User.user_id == uuid.UUID(user_id)).first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    user.role = UserRole(req.role)
    db.commit()
    return {"message": f"Role updated to {req.role}", "user_id": user_id}


@router.put("/users/{user_id}/deactivate")
def deactivate_user(user_id: str, current_user: User = Depends(require_admin), db: Session = Depends(get_db)):
    """Deactivate a user account — admin only."""
    user = db.query(User).filter(User.user_id == uuid.UUID(user_id)).first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    if str(user.user_id) == str(current_user.user_id):
        raise HTTPException(status_code=400, detail="Cannot deactivate your own account")
    user.is_active = False
    db.commit()
    return {"message": "User deactivated"}