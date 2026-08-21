import secrets
import uuid
from datetime import datetime, timedelta

import bcrypt
from fastapi import Header, HTTPException, Depends

from db import run_query, run_write

SESSION_TTL_HOURS = 8


def hash_password(plain: str) -> str:
    return bcrypt.hashpw(plain.encode(), bcrypt.gensalt()).decode()


def verify_password(plain: str, hashed: str) -> bool:
    try:
        return bcrypt.checkpw(plain.encode(), hashed.encode())
    except Exception:
        return False


def create_user(username: str, full_name: str, role: str, gold_access: str, password: str = None) -> dict:
    """Creates a user and returns {user_id, username, temp_password}.
    If no password is supplied, a secure random one is generated and returned
    once -- it is never stored or logged in plaintext."""
    existing = run_query("SELECT user_id FROM auth.users WHERE username = ?", [username])
    if not existing.empty:
        raise ValueError(f"Username '{username}' already exists")

    temp_password = password or secrets.token_urlsafe(9)
    user_id = str(uuid.uuid4())
    run_write("""
        INSERT INTO auth.users (user_id, username, password_hash, full_name, role, gold_access, is_active)
        VALUES (?, ?, ?, ?, ?, ?, true)
    """, [user_id, username, hash_password(temp_password), full_name, role, gold_access])

    return {"user_id": user_id, "username": username, "temp_password": temp_password}


def authenticate(username: str, password: str) -> dict | None:
    df = run_query("SELECT * FROM auth.users WHERE username = ? AND is_active = true", [username])
    if df.empty:
        return None
    user = df.to_dict(orient="records")[0]
    if not verify_password(password, user["password_hash"]):
        return None

    token = secrets.token_urlsafe(32)
    expires = datetime.utcnow() + timedelta(hours=SESSION_TTL_HOURS)
    run_write("INSERT INTO auth.sessions (token, user_id, expires_ts) VALUES (?, ?, ?)",
              [token, user["user_id"], expires])
    run_write("UPDATE auth.users SET last_login_ts = current_timestamp WHERE user_id = ?", [user["user_id"]])

    return {"token": token, "user": _public_user(user)}


def _public_user(user: dict) -> dict:
    return {
        "user_id": user["user_id"],
        "username": user["username"],
        "full_name": user["full_name"],
        "role": user["role"],
        "gold_access": user["gold_access"],
    }


def logout(token: str):
    run_write("DELETE FROM auth.sessions WHERE token = ?", [token])


def get_user_from_token(token: str) -> dict | None:
    df = run_query("""
        SELECT u.* FROM auth.sessions s
        JOIN auth.users u ON u.user_id = s.user_id
        WHERE s.token = ? AND s.expires_ts > current_timestamp AND u.is_active = true
    """, [token])
    if df.empty:
        return None
    return df.to_dict(orient="records")[0]


# --- FastAPI dependencies -------------------------------------------------

def get_current_user(authorization: str = Header(None)) -> dict:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(401, "Missing or malformed Authorization header")
    token = authorization.removeprefix("Bearer ").strip()
    user = get_user_from_token(token)
    if user is None:
        raise HTTPException(401, "Invalid or expired session")
    return _public_user(user)


def require_admin(user: dict = Depends(get_current_user)) -> dict:
    if user["role"] != "admin":
        raise HTTPException(403, "Administrator role required")
    return user


def require_steward_or_owner(user: dict = Depends(get_current_user)) -> dict:
    """Gates the Data Stewardship console: only data stewards and data owners
    may review/resolve exception-queue records. Admin and businessUser are
    deliberately excluded -- stewardship is a distinct governance function,
    not a superset of admin rights."""
    if user["role"] not in ("dataSteward", "dataOwner"):
        raise HTTPException(403, "Data Steward or Data Owner role required")
    return user


def require_gold_write(user: dict = Depends(get_current_user)) -> dict:
    """Pre-Phase-4 global gold-layer check, kept only for any caller that
    genuinely has no domain in scope yet (e.g. an endpoint not yet migrated).
    Prefer require_domain_write for anything record/domain-scoped."""
    if user["gold_access"] != "read_write":
        raise HTTPException(403, "Read/write access to the gold layer is required")
    return user


def require_gold_read(user: dict = Depends(get_current_user)) -> dict:
    """Pre-Phase-4 global gold-layer check -- see require_gold_write's note."""
    if user["gold_access"] not in ("read", "read_write"):
        raise HTTPException(403, "Read access to the gold layer is required")
    return user


# --- Phase 4 (domain-agnostic): per-domain access, governance.user_domain_access ---

def domain_access_level(user_id: str, domain_key: str) -> str:
    """Reads this user's gold_access level for ONE domain from
    governance.user_domain_access (scripts/ensure_domain_metadata.py /
    api/db.py's mirror), the intended replacement for the single global
    auth.users.gold_access column (see that column's own migration,
    scripts/ensure_domain_metadata.py's migrate_user_domain_access). Returns
    'none' if no row exists for this (user_id, domain_key) pair -- a user is
    never implicitly granted access to a domain they have no row for, even
    if their legacy global gold_access was 'read_write'; user_domain_access
    is the single source of truth for every endpoint that scopes by domain."""
    df = run_query(
        "SELECT gold_access FROM governance.user_domain_access WHERE user_id = ? AND domain_key = ?",
        [user_id, domain_key],
    )
    if df.empty:
        return "none"
    return df.iloc[0]["gold_access"]


def require_domain_write(domain_key: str, user: dict = Depends(get_current_user)) -> dict:
    """FastAPI dependency: requires read_write access to `domain_key` for the
    current user. `domain_key` is resolved from the route's own path
    parameter of the same name (FastAPI shares path parameters across a
    route's dependencies by name) -- callers just declare
    `domain_key: str` on the route function and add
    `Depends(require_domain_write)` (or require_domain_read below); no
    per-domain wiring needed."""
    if domain_access_level(user["user_id"], domain_key) != "read_write":
        raise HTTPException(403, f"Read/write access to domain '{domain_key}' is required")
    return user


def require_domain_read(domain_key: str, user: dict = Depends(get_current_user)) -> dict:
    """See require_domain_write -- same pattern, accepts 'read' or 'read_write'."""
    if domain_access_level(user["user_id"], domain_key) not in ("read", "read_write"):
        raise HTTPException(403, f"Read access to domain '{domain_key}' is required")
    return user
