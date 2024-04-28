import pytz
from pydantic import BaseModel, Field, field_validator
from datetime import datetime
from typing import Type


class ErrorSchema(BaseModel):
    err_type: str
    err_message: str
    dev_message: str | None = None
    timestamp: datetime = Field(default=datetime.now(tz=pytz.UTC))

    @field_validator("err_type", mode="before")
    @classmethod
    def get_name(cls, raw: Type[Exception]) -> str:
        # it will take Exception and get its name.
        return raw.__class__.__name__
