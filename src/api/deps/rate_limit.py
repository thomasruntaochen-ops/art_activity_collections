from dataclasses import dataclass
from datetime import UTC, datetime
from threading import Lock
from time import time

from fastapi import Depends, HTTPException, Request, Response, status
from redis import Redis
from redis.exceptions import RedisError

from src.api.deps.auth import AuthContext, get_auth_context
from src.core.config import settings


@dataclass(slots=True)
class HitResult:
    allowed: bool
    retry_after_seconds: int
    count: int


class InMemoryRateLimitStore:
    def __init__(self) -> None:
        self._lock = Lock()
        self._entries: dict[str, tuple[int, float]] = {}

    def hit(self, key: str, *, limit: int, window_seconds: int) -> HitResult:
        now = time()
        with self._lock:
            count, expires_at = self._entries.get(key, (0, now + window_seconds))
            if now >= expires_at:
                count = 0
                expires_at = now + window_seconds
            count += 1
            self._entries[key] = (count, expires_at)

        retry_after = max(1, int(expires_at - now))
        return HitResult(allowed=(count <= limit), retry_after_seconds=retry_after, count=count)


class RedisRateLimitStore:
    def __init__(self, redis_url: str) -> None:
        self._client = Redis.from_url(redis_url, decode_responses=True)

    def hit(self, key: str, *, limit: int, window_seconds: int) -> HitResult:
        count = int(self._client.incr(key))
        if count == 1:
            self._client.expire(key, window_seconds)
        ttl = int(self._client.ttl(key))
        if ttl <= 0:
            self._client.expire(key, window_seconds)
            ttl = window_seconds
        return HitResult(allowed=(count <= limit), retry_after_seconds=max(1, ttl), count=count)


class RateLimiter:
    def __init__(self) -> None:
        self._fallback_store = InMemoryRateLimitStore()
        self._redis_store: RedisRateLimitStore | None = None
        if settings.redis_url:
            try:
                self._redis_store = RedisRateLimitStore(settings.redis_url)
            except Exception:
                self._redis_store = None

    def _hit(self, key: str, *, limit: int, window_seconds: int) -> HitResult:
        if self._redis_store is not None:
            try:
                return self._redis_store.hit(key, limit=limit, window_seconds=window_seconds)
            except RedisError:
                pass
        return self._fallback_store.hit(key, limit=limit, window_seconds=window_seconds)

    @staticmethod
    def _client_ip(request: Request) -> str:
        forwarded_for = request.headers.get("x-forwarded-for")
        if forwarded_for:
            first_ip = forwarded_for.split(",")[0].strip()
            if first_ip:
                return first_ip
        if request.client and request.client.host:
            return request.client.host
        return "unknown"

    @staticmethod
    def _day_window_seconds() -> int:
        now = datetime.now(UTC)
        midnight_tomorrow = datetime(now.year, now.month, now.day, tzinfo=UTC).timestamp() + 86400
        return max(1, int(midnight_tomorrow - now.timestamp()))

    def _subject_prefix(self, request: Request, auth: AuthContext) -> str:
        if auth.is_authenticated and auth.subject:
            return f"user:{auth.subject}"
        return f"guest:{self._client_ip(request)}"

    def check_request(self, request: Request, auth: AuthContext) -> tuple[int, int]:
        prefix = self._subject_prefix(request, auth)
        if auth.is_authenticated:
            per_minute = settings.rate_limit_user_per_minute
            per_day = settings.rate_limit_user_per_day
        else:
            per_minute = settings.rate_limit_guest_per_minute
            per_day = settings.rate_limit_guest_per_day

        per_minute_key = f"rl:{prefix}:minute"
        minute_result = self._hit(per_minute_key, limit=per_minute, window_seconds=60)
        if not minute_result.allowed:
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail=f"Rate limit exceeded. Retry in {minute_result.retry_after_seconds}s.",
                headers={"Retry-After": str(minute_result.retry_after_seconds)},
            )

        per_day_key = f"rl:{prefix}:day:{datetime.now(UTC).date().isoformat()}"
        day_result = self._hit(
            per_day_key,
            limit=per_day,
            window_seconds=self._day_window_seconds(),
        )
        if not day_result.allowed:
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail=f"Daily quota exceeded. Retry in {day_result.retry_after_seconds}s.",
                headers={"Retry-After": str(day_result.retry_after_seconds)},
            )

        return per_minute, per_day


_limiter = RateLimiter()


def enforce_request_limits(
    request: Request,
    response: Response,
    auth: AuthContext = Depends(get_auth_context),
) -> None:
    per_minute, per_day = _limiter.check_request(request, auth)
    response.headers["X-RateLimit-Minute-Limit"] = str(per_minute)
    response.headers["X-RateLimit-Day-Limit"] = str(per_day)
