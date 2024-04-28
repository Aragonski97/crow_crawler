from .selector_model import (
    Base,
    Mapped,
    mapped_column,
    AwareDateTime,
    func,
    datetime,
    VARCHAR
)


class UserModel(Base):

    __tablename__ = "USERS"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    first_name: Mapped[str] = mapped_column(VARCHAR(255), nullable=False)
    last_name: Mapped[str] = mapped_column(VARCHAR(255), nullable=False)
    email: Mapped[str] = mapped_column(VARCHAR(255), nullable=False, index=True, unique=True)
    password: Mapped[str] = mapped_column(VARCHAR(255), nullable=False)
    last_login: Mapped[datetime] = mapped_column(AwareDateTime, default=func.now(), nullable=False)

