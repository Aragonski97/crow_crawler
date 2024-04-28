from .selector_model import (
    Base,
    Mapped,
    mapped_column,
    AwareDateTime,
    datetime,
    func,
    VARCHAR,
)
from sqlalchemy import JSON


class ProfileModel(Base):
    __tablename__ = "PROFILES"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(VARCHAR(255), nullable=False, index=True, unique=True)
    burst_rate: Mapped[int] = mapped_column(VARCHAR(255), nullable=False, default=2)
    headers: Mapped[dict] = mapped_column(JSON, nullable=False)
    cookies: Mapped[dict] = mapped_column(JSON, nullable=True)
    last_updated: Mapped[datetime] = mapped_column(AwareDateTime, default=func.now(), nullable=False)
