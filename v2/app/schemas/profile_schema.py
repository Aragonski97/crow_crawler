from pydantic import BaseModel, Field


class ProfileSchema(BaseModel):

    name: str = Field(max_length=100, nullable=False)
    burst_rate: int = Field(default=2)
    headers: dict | None = Field(default=None)
    cookies: dict | None = Field(default=None)


