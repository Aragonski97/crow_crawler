from abc import ABC, ABCMeta, abstractmethod
import re
import parsel
import ast
import json
from app.utils.case_insensitive_enums import SelectorMethod


class CrowSelector(ABC):
    __metaclass__ = ABCMeta

    def __init__(self,
                 name: str,
                 directive: all,
                 method=SelectorMethod.First,
                 required: bool = False,
                 default_return: str | None = None,
                 post_processor: str | None = None):
        # There's a special name: REUSE. If a selector is named REUSE, it is expected to be
        # the only selector is SelectorList, while also having self.method = 'first'.
        # It is used when we need to refactor the urls with some data from the page in the runtime.
        self.name = name
        self.directive = directive
        self.method = method
        self.default_return = default_return
        self.post_processor = post_processor
        self.required = required
        self._name = None

    def __repr__(self):
        return self.name

    @abstractmethod
    def extract_all(self, data: str) -> list:
        pass

    @abstractmethod
    def extract_first(self, data: str) -> str:
        pass

    def post_process(self, data: str | list):
        if self.post_processor is None:
            return data
        formatted = eval(self.post_processor)
        return formatted

    def extract(self, data) -> str | list:
        if self.method == "first":
            data = self.extract_first(data=data)
        else:
            data = self.extract_all(data=data)
        if self.post_processor is not None:
            try:
                formatted = self.post_process(data)
                return formatted
            except Exception as err:
                print(err)
        return data


class RegexSelector(CrowSelector):
    def __init__(self,
                 name: str,
                 directive: all,
                 method=SelectorMethod.First,
                 required: bool = False,
                 default_return: str | None = None,
                 post_processor: str | None = None):
        super(RegexSelector, self).__init__(name=name,
                                            directive=directive,
                                            method=method,
                                            required=required,
                                            default_return=default_return,
                                            post_processor=post_processor)
        self.directive = re.compile(directive)
        self._name = "regex"
        
    def extract_first(self, data: str) -> str:
        try:
            return re.search(pattern=self.directive, string=data).groups()[0]
        except (AttributeError, IndexError):
            return self.default_return

    def extract_all(self, data: str) -> list:
        try:
            return re.findall(pattern=self.directive, string=data)
        except (ValueError, TypeError):
            return list()


class XpathSelector(CrowSelector):

    def __init__(self,
                 name: str,
                 directive: all,
                 method=SelectorMethod.First,
                 required: bool = False,
                 default_return: str | None = None,
                 post_processor: str | None = None):
        super(XpathSelector, self).__init__(name=name,
                                            directive=directive,
                                            method=method,
                                            required=required,
                                            default_return=default_return,
                                            post_processor=post_processor)
        self._name = "xpath"
    
    def extract_first(self, p_selector: parsel.Selector) -> str:
        try:
            return p_selector.xpath(query=self.directive).get(default=self.default_return)
        except ValueError:
            return self.default_return

    def extract_all(self, p_selector: parsel.Selector) -> list:
        try:
            return p_selector.xpath(query=self.directive).getall()
        except ValueError:
            return list()

    def extract(self, p_selector: parsel.Selector) -> str | list:
        if self.method == "first":
            data = self.extract_first(p_selector=p_selector)
        else:
            data = self.extract_all(p_selector=p_selector)
        if self.post_processor is not None:
            try:
                formatted = self.post_process(data)
                return formatted
            except Exception as err:
                print(err)
        return data


class JsonSelector(CrowSelector):

    def __init__(self,
                 name: str,
                 directive: all,
                 method=SelectorMethod.First,
                 required: bool = False,
                 default_return: str | None = None,
                 post_processor: str | None = None):
        super(JsonSelector, self).__init__(name=name,
                                           directive=directive,
                                           method=method,
                                           required=required,
                                           default_return=default_return,
                                           post_processor=post_processor)
        self._name = "json"

    def extract_first(self, data: str) -> str:
        text = json.loads(data)
        try:
            directives = ast.literal_eval(self.directive)
            for item in directives:
                text = text[item]

            return text
        except (KeyError, TypeError, SyntaxError, ValueError):
            return self.default_return

    def extract_all(self, data: str) -> list:
        pass


class StaticSelector(CrowSelector):

    def __init__(self,
                 name: str,
                 directive: all,
                 method=SelectorMethod.First,
                 required: bool = False,
                 default_return: str | None = None,
                 post_processor: str | None = None):
        super(StaticSelector, self).__init__(name=name,
                                             directive=directive,
                                             method=method,
                                             required=required,
                                             default_return=default_return,
                                             post_processor=post_processor)
        self._name = "static"

    def extract_first(self, data: str) -> str:
        return self.directive

    def extract_all(self, data: str) -> list:
        return self.directive


class CssSelector(CrowSelector):

    def __init__(self,
                 name: str,
                 directive: all,
                 method=SelectorMethod.First,
                 required: bool = False,
                 default_return: str | None = None,
                 post_processor: str | None = None):
        super(CssSelector, self).__init__(name=name,
                                          directive=directive,
                                          method=method,
                                          required=required,
                                          default_return=default_return,
                                          post_processor=post_processor)
        self._name = "css"
        
    def extract_first(self, data: str) -> str:
        pass

    def extract(self, data) -> str | list:
        pass


class SelectorList:

    def __init__(self, selectors: list[CrowSelector]):
        self.extracted = dict()
        self.selectors: list[CrowSelector] = selectors
        self.regex_list: list[RegexSelector] = list()
        self.xpath_list: list[XpathSelector] = list()
        self.json_list: list[JsonSelector] = list()
        self.static_list: list[StaticSelector] = list()
        self.css_list: list[CssSelector] = list()
        self.required: list[CrowSelector] = list()

    def __getitem__(self, item):
        return self.selectors[item]

    def __len__(self):
        return len(self.selectors)

    def split_selectors(self):
        for selector in self.selectors:
            if selector.required:
                self.required.append(selector)
            if isinstance(selector, RegexSelector):
                self.regex_list.append(selector)
                continue
            elif isinstance(selector, XpathSelector):
                self.xpath_list.append(selector)
                continue
            elif isinstance(selector, StaticSelector):
                self.static_list.append(selector)
                continue
            elif isinstance(selector, JsonSelector):
                self.json_list.append(selector)
            elif isinstance(selector, CssSelector):
                self.css_list.append(selector)
            else:
                raise LookupError
        return self

    def extract_regex(self, data):
        for selector in self.regex_list:
            self.extracted.update({selector.name: selector.extract(data=data)})
        return self

    def extract_xpath(self, data):
        p_selector = parsel.Selector(data)
        for selector in self.xpath_list:
            self.extracted.update({selector.name: selector.extract(p_selector=p_selector)})
        return self

    def extract_css(self, data):
        # not implemented yet
        return self

    def extract_json(self, data):
        for selector in self.json_list:
            self.extracted.update({selector.name: selector.extract(data=data)})
        return self

    def extract_static(self):
        for selector in self.static_list:
            self.extracted.update({selector.name: selector.directive})
        return self

    def extract(self, data: str):
        self.extracted.clear()
        self.extract_regex(data).extract_xpath(data).extract_json(data).extract_static().extract_css(data)
        for required in self.required:
            if self.extracted[required.name] is None:
                return {x: None for x in self.extracted}
        return self.extracted
