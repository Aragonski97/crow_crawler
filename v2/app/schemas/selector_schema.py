from pydantic import BaseModel, Field
from app.utils.case_insensitive_enums import SelectorType, SelectorMethod


class SelectorSchema(BaseModel):

    name: str = Field(max_length=30, nullable=False)
    type: SelectorType
    method: SelectorMethod
    directive: str
    required: bool = False
    default_return: str | None = None
    post_processor: str | None = None

    def __repr__(self):
        return self.name
