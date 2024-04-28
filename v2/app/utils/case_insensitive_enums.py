from enum import Enum


class CaseInsensitiveEnum(str, Enum):
    @classmethod
    def _missing_(cls, value: str):
        for member in cls:
            if member.lower() == value.lower():
                return member
        return None


class SelectorType(CaseInsensitiveEnum):

    Regex = "regex"
    Xpath = "xpath"
    Json = "json"
    Css = "css"
    Static = "static"


class SelectorMethod(CaseInsensitiveEnum):

    First = "first"
    All = "all"


class StepType(CaseInsensitiveEnum):
    Scraper = "scraper"
    Transformer = "transformer"

