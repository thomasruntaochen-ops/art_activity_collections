from datetime import datetime
from decimal import Decimal
from enum import Enum

from sqlalchemy import Boolean, DateTime, Enum as SqlEnum, ForeignKey, Numeric, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from src.db.session import Base


class FreeVerificationStatus(str, Enum):
    confirmed = "confirmed"
    inferred = "inferred"
    uncertain = "uncertain"


class ExtractionMethod(str, Enum):
    hardcoded = "hardcoded"
    llm = "llm"


class ActivityStatus(str, Enum):
    active = "active"
    cancelled = "cancelled"
    expired = "expired"
    needs_review = "needs_review"


class Source(Base):
    __tablename__ = "sources"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    base_url: Mapped[str] = mapped_column(String(1024), nullable=False)
    adapter_type: Mapped[str] = mapped_column(String(100), nullable=False)
    crawl_frequency: Mapped[str] = mapped_column(String(100), default="daily")
    active: Mapped[bool] = mapped_column(Boolean, default=True)


class Venue(Base):
    __tablename__ = "venues"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    address: Mapped[str | None] = mapped_column(String(512))
    city: Mapped[str | None] = mapped_column(String(255))
    state: Mapped[str | None] = mapped_column(String(50))
    zip: Mapped[str | None] = mapped_column(String(20))
    lat: Mapped[Decimal | None] = mapped_column(Numeric(10, 7))
    lng: Mapped[Decimal | None] = mapped_column(Numeric(10, 7))
    website: Mapped[str | None] = mapped_column(String(1024))


class Activity(Base):
    __tablename__ = "activities"

    id: Mapped[int] = mapped_column(primary_key=True)
    source_id: Mapped[int] = mapped_column(ForeignKey("sources.id"), nullable=False)
    source_url: Mapped[str] = mapped_column(String(1024), nullable=False)
    external_id: Mapped[str | None] = mapped_column(String(255))

    title: Mapped[str] = mapped_column(String(512), nullable=False)
    description: Mapped[str | None] = mapped_column(Text)
    activity_type: Mapped[str | None] = mapped_column(String(100))
    age_min: Mapped[int | None]
    age_max: Mapped[int | None]

    is_free: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    free_verification_status: Mapped[FreeVerificationStatus] = mapped_column(
        SqlEnum(FreeVerificationStatus), nullable=False, default=FreeVerificationStatus.inferred
    )

    drop_in: Mapped[bool | None]
    registration_required: Mapped[bool | None]

    start_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    end_at: Mapped[datetime | None] = mapped_column(DateTime)
    timezone: Mapped[str] = mapped_column(String(64), nullable=False, default="America/Los_Angeles")
    recurrence_text: Mapped[str | None] = mapped_column(String(512))
    location_text: Mapped[str | None] = mapped_column(String(512))

    venue_id: Mapped[int | None] = mapped_column(ForeignKey("venues.id"))

    extraction_method: Mapped[ExtractionMethod] = mapped_column(
        SqlEnum(ExtractionMethod), nullable=False, default=ExtractionMethod.hardcoded
    )
    extractor_version: Mapped[str | None] = mapped_column(String(64))
    llm_provider: Mapped[str | None] = mapped_column(String(64))
    llm_model: Mapped[str | None] = mapped_column(String(128))
    llm_confidence: Mapped[Decimal | None] = mapped_column(Numeric(5, 4))

    status: Mapped[ActivityStatus] = mapped_column(
        SqlEnum(ActivityStatus), nullable=False, default=ActivityStatus.active
    )
    confidence_score: Mapped[Decimal] = mapped_column(Numeric(5, 4), nullable=False, default=0.8000)

    first_seen_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    last_seen_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)

    source: Mapped[Source] = relationship()
    venue: Mapped[Venue | None] = relationship()
