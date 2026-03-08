from dataclasses import dataclass
from functools import lru_cache

import jwt
from fastapi import Header, HTTPException, Request, status
from jwt import PyJWKClient
from jwt.exceptions import InvalidTokenError

from src.core.config import settings


@dataclass(slots=True)
class AuthContext:
    is_authenticated: bool
    subject: str | None = None


def _unauthorized(detail: str = "Invalid authentication token") -> HTTPException:
    return HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail=detail,
        headers={"WWW-Authenticate": "Bearer"},
    )


def _extract_bearer_token(authorization: str | None) -> str | None:
    if not authorization:
        return None
    prefix = "bearer "
    if not authorization.lower().startswith(prefix):
        raise _unauthorized("Expected a Bearer token")
    token = authorization[len(prefix) :].strip()
    if not token:
        raise _unauthorized("Missing Bearer token")
    return token


@lru_cache(maxsize=1)
def _get_jwks_client() -> PyJWKClient | None:
    if not settings.auth_jwks_url:
        return None
    return PyJWKClient(settings.auth_jwks_url)


def _verify_jwt(token: str) -> str:
    if not settings.auth_issuer or not settings.auth_audience or not settings.auth_jwks_url:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=(
                "Auth is enabled but AUTH_ISSUER, AUTH_AUDIENCE, or AUTH_JWKS_URL is not configured."
            ),
        )

    try:
        jwks_client = _get_jwks_client()
        if jwks_client is None:
            raise _unauthorized("Could not initialize JWKS client")
        signing_key = jwks_client.get_signing_key_from_jwt(token).key
        payload = jwt.decode(
            token,
            signing_key,
            algorithms=["RS256"],
            audience=settings.auth_audience,
            issuer=settings.auth_issuer,
        )
    except InvalidTokenError as exc:
        raise _unauthorized(str(exc)) from exc
    except Exception as exc:
        raise _unauthorized("Token verification failed") from exc

    subject = payload.get("sub")
    if not subject:
        raise _unauthorized("Token missing subject")
    return str(subject)


def get_auth_context(
    request: Request,
    authorization: str | None = Header(default=None),
) -> AuthContext:
    token = _extract_bearer_token(authorization)

    if not settings.auth_enabled:
        context = AuthContext(is_authenticated=False, subject=None)
        request.state.auth_context = context
        return context

    if token is None:
        if settings.auth_require_all_requests:
            raise _unauthorized("Missing Bearer token")
        context = AuthContext(is_authenticated=False, subject=None)
        request.state.auth_context = context
        return context

    subject = _verify_jwt(token)
    context = AuthContext(is_authenticated=True, subject=subject)
    request.state.auth_context = context
    return context
