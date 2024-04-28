from pydantic import BaseModel, Field
from app.utils.case_insensitive_enums import StepType


class ScraperModel(BaseModel):

    name: str = Field(max_length=30, nullable=False)
    type: StepType
    headers: dict | None = Field(default=None)
    burst_rate: int | None = Field(default=None)
    initial_url: str | None = Field(default=None)
