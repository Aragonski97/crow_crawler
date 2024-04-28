from sqlalchemy import ForeignKey
from sqlalchemy.sql import func
from sqlalchemy.orm import Mapped, mapped_column, DeclarativeBase
from app.utils.aware_datetime import AwareDateTime, datetime
from sqlalchemy.types import VARCHAR, PickleType


class Base(DeclarativeBase):
    __abstract__ = True

    def to_dict(self):
        return {field.name: getattr(self, field.name) for field in self.__table__.c}


class SelectorModel(Base):

    __tablename__ = "SELECTORS"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    scraper_id: Mapped[int] = mapped_column(ForeignKey("SCRAPERS.id"), nullable=False)
    name: Mapped[str] = mapped_column(VARCHAR(255), nullable=False, index=True)
    type: Mapped[str] = mapped_column(VARCHAR(255), nullable=False)
    method: Mapped[str] = mapped_column(VARCHAR(255), nullable=False)
    directive: Mapped[str] = mapped_column(VARCHAR(255), nullable=False)
    required: Mapped[bool] = mapped_column(VARCHAR(255), nullable=False, default=False)
    default_return: Mapped[str] = mapped_column(VARCHAR(255), nullable=True, default=None)
    post_processor: Mapped[str] = mapped_column(PickleType, nullable=True, default=None)
    last_updated: Mapped[datetime] = mapped_column(AwareDateTime, default=func.now(), nullable=False)
