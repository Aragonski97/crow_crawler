from .selector_schema import BaseModel, Field as FLD
from datetime import datetime, UTC


class UserSchema(BaseModel):
    id: int | None = None
    first_name: str
    last_name: str
    email: str
    password: str
    last_login: datetime


class TokenSchema(BaseModel):
    ACCESS_TOKEN: str


class TokenDataSchema(BaseModel):
    email: str = FLD(max_length=100, pattern=r"[@\.]")
    password: str = FLD(
        min_length=8,
        pattern=r"[-+_!@#$%^&*.,?]"
    )
    exp: datetime


class SignInSchema(BaseModel):

    email: str = FLD(max_length=100, pattern=r"[@\.]")
    password: str = FLD(
        min_length=8,
        pattern=r"[-+_!@#$%^&*.,?]"
    )


class SignUpSchema(BaseModel):

    first_name: str
    last_name: str
    email: str = FLD(max_length=100, pattern=r"[@\.]")
    password: str = FLD(
        min_length=8,
        pattern=r"[-+_!@#$%^&*.,?]"
    )
    last_login: datetime = FLD(default_factory=lambda: datetime.now(UTC))
