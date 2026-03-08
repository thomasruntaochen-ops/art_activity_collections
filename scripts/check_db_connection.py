#!/usr/bin/env python3
import sys
from pathlib import Path

from sqlalchemy import text

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.core.config import settings
from src.db.session import engine


def _masked(value: str) -> str:
    if not value:
        return "<empty>"
    if len(value) <= 2:
        return "*" * len(value)
    return f"{value[0]}{'*' * (len(value) - 2)}{value[-1]}"


def main() -> int:
    print("DB runtime settings:")
    print(f"- MYSQL_HOST (raw): {settings.mysql_host!r}")
    print(f"- MYSQL_HOST (resolved): {settings.mysql_host_resolved!r}")
    print(f"- MYSQL_PORT: {settings.mysql_port}")
    print(f"- MYSQL_USER: {settings.mysql_user!r}")
    print(f"- MYSQL_DB: {settings.mysql_db!r}")
    print(f"- MYSQL_PASSWORD: {_masked(settings.mysql_password)}")

    if settings.mysql_password == "change_me":
        print(
            "ERROR: MYSQL_PASSWORD is still set to 'change_me'. "
            "Set your real password in .env."
        )
        return 2

    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("OK: Connected to MySQL and executed SELECT 1.")
        return 0
    except Exception as exc:
        print(f"ERROR: {exc}")
        print("Tip: verify user/password grants and test with mysql CLI using the same host/port.")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
