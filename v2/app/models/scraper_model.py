from .selector_model import (
    Base,
    Mapped,
    mapped_column,
    ForeignKey,
    datetime,
    AwareDateTime,
    func,
    VARCHAR
)
from sqlalchemy import JSON


class ScraperModel(Base):

    __tablename__ = "SCRAPERS"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    profile_id: Mapped[int] = mapped_column(ForeignKey("PROFILES.id"), nullable=True)
    name: Mapped[str] = mapped_column(VARCHAR(255), nullable=False, index=True)
    type: Mapped[str] = mapped_column(VARCHAR(255), nullable=False)
    headers: Mapped[dict] = mapped_column(JSON, nullable=True, default=True)
    initial_url: Mapped[str] = mapped_column(VARCHAR(255), nullable=False)
    last_updated: Mapped[datetime] = mapped_column(AwareDateTime, default=func.now(), nullable=False)
