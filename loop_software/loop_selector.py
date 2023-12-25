from typing import Optional
from abc import ABC, ABCMeta, abstractmethod
from typing import Literal, Union
import re
import parsel
import ast
import json


class WebSelector(ABC):
    """Abstract class. Should be instantiated through one of its children classes:
        XpathSelector,
        RegexSelector,
        JsonSelector,
        StaticSelector

        Apply directive that you want the engine to search desired data with.
        Method specific only the first found element, Method any returns all matches.
        Return message is the response you wish to be returned if no matches are found.
    """
    __metaclass__ = ABCMeta

    def __init__(self,
                 name: str,
                 directive: any,
                 method: Literal["specific", "any"] = "specific",
                 return_message: Optional[str] = None):
        self.name = name
        self.directive = directive
        self.method = method
        self.return_message = return_message

    @abstractmethod
    def extract_any(self, data: str) -> list:
        pass

    @abstractmethod
    def extract_specific(self, data: str) -> str:
        pass

    def extract(self, data) -> Union[str, list]:
        if self.method == "specific":
            return self.extract_specific(data=data)
        else:
            return self.extract_any(data=data)


class RegexSelector(WebSelector):
    def __init__(self,
                 name: str,
                 directive: any,
                 method: Literal["specific", "any"] = "specific",
                 return_message: Optional[str] = None):
        super(RegexSelector, self).__init__(name=name,
                                            directive=directive,
                                            method=method,
                                            return_message=return_message)

    def extract_specific(self, data: str) -> str:
        try:
            return re.search(pattern=re.compile(self.directive), string=data).groups()[0]
        except (AttributeError, IndexError):
            return self.return_message

    def extract_any(self, data: str) -> list:
        try:
            return re.findall(pattern=re.compile(self.directive), string=data)
        except (ValueError, TypeError):
            return list()


class XpathSelector(WebSelector):

    def __init__(self,
                 name: str,
                 directive: any,
                 method: Literal["specific", "any"] = "specific",
                 return_message: Optional[str] = None):
        super(XpathSelector, self).__init__(name=name,
                                            directive=directive,
                                            method=method,
                                            return_message=return_message)

    def extract_specific(self, data: str) -> str:
        try:
            return parsel.Selector(data).xpath(query=self.directive).get(default=self.return_message)
        except ValueError:
            return self.return_message

    def extract_any(self, data: str) -> list:
        try:
            return parsel.Selector(data).xpath(query=self.directive).getall()
        except ValueError:
            return list()


class JsonSelector(WebSelector):

    def __init__(self,
                 name: str,
                 directive: any,
                 method: Literal["specific", "any"] = "specific",
                 return_message: Optional[str] = None):
        super(JsonSelector, self).__init__(name=name,
                                           directive=directive,
                                           method=method,
                                           return_message=return_message)

    def extract_specific(self, data: str) -> str:
        text = json.loads(data)
        try:
            directives = ast.literal_eval(self.directive)
            for item in directives:
                text = text[item]
            return text
        except (KeyError, TypeError, SyntaxError, ValueError):
            return self.return_message

    def extract_any(self, data: str) -> list:
        pass


class StaticSelector(WebSelector):
    def __init__(self,
                 name: str,
                 directive: any,
                 method: Literal["specific", "any"] = "specific",
                 return_message: Optional[str] = None):
        super(StaticSelector, self).__init__(name=name,
                                             directive=directive,
                                             method=method,
                                             return_message=return_message)

    def extract_specific(self, data: str) -> str:
        return self.directive

    def extract_any(self, data: str) -> list:
        pass
