import logging
import re
import mysql.connector.errors
import requests
from datetime import datetime
from abc import ABC, ABCMeta
from loop_software.loop_processor import Processor
from loop_software.loop_connections import ConnectionObject
from loop_software.loop_webhost import WebHost
from loop_software.loop_selector import WebSelector
from loop_software import loop_miscellaneous as lm
from typing import Callable
from pathlib import Path


class Crawler(ABC):
    __metaclass__ = ABCMeta

    def __init__(self, webhost: WebHost, urls: list):
        self.webhost = webhost
        self.urls = urls


class URLCrawler(Crawler):

    def __init__(self, webhost: WebHost, urls: list):
        super().__init__(webhost=webhost, urls=urls)
        self.pages_numbers: list | None = None

    def __len__(self) -> int:
        return len(self.urls)

    def __getitem__(self, key) -> slice:
        return self.urls[key]

    def ready(self):
        """Checks whether given initial urls respond properly."""
        print("Checking availability...")
        responses = [requests.get(url=url, headers=self.webhost.request_settings.get("headers")) for url in self.urls]
        for response in responses:
            if response.status_code not in lm.SUCCESS_RESPONSES:
                logging.info(f"Response: {response.url} doesn't return valid status code."
                             f" Status code: {response.status_code}")
                print(f"{response.url} unavailable.")
                return False
            else:
                print("All responses available.")
                return True

    def download_and_load_gz(self, host_path: Path):
        """download gz and load it."""
        print("Downloading gz files.")
        from requests import get
        from os import walk
        from pandas import read_xml
        import gzip
        for url in self.urls:
            filename = url.split("/")[-1]
            with open(host_path.joinpath(f'{filename}'), "wb") as f:
                r = get(url=url, headers=self.webhost.request_settings.get("headers"))
                f.write(r.content)
        self.urls.clear()
        for dirpath, dirnames, filenames in walk(host_path):
            # select file name
            for file in filenames:
                # check the extension of files
                if file.endswith('.gz'):
                    # print whole path of files
                    with gzip.open(host_path.joinpath(f"{file}")) as file_in:
                        xml = file_in.read()
                        self.urls.extend(read_xml(xml.decode(encoding="UTF-8"))['loc'].values.tolist())
        return self

    def prepend_seed(self, seed: str):
        self.urls = [seed + url for url in self.urls]
        return self

    def get_pages_number(self, pages_selector: WebSelector, manual_pages: int | None = None):
        """Retrieves pages' count in order to format the urls with that number."""
        if manual_pages is not None:
            self.pages_numbers = [manual_pages]
            assert len(self.pages_numbers) == len(self.urls)
            return self
        # remember that batches_extract removes none values! the code might break
        processor = Processor()\
            .bind_new_requester(urls=self.urls, request_settings=self.webhost.request_settings)\
            .bind_new_extractor(selectors=[pages_selector])

        # assume all urls have exact same selector for pages count
        extracted = processor.simple_fetch_extract_return(method="async")
        self.pages_numbers = [item.get(pages_selector.name) for item in extracted]
        assert len(self.pages_numbers) == len(self.urls)
        return self

    def get_urls_formatted_with_pages(self, payload: dict, pages_callable: Callable = None):
        """Formats the urls with that number.
        payload to format urls with,
        pages_callable is the callable which I used to reduce the size of the url_cache
        for instance lambda x: int(x/1000) for instance"""
        # pages callable can reduce the size of urls to format.
        # lambda x: int(x/1000) for instance

        results = list()
        if pages_callable is None:
            for url_index in range(len(self.urls)):
                # if there are no pages_numbers set url itself as primary_url
                if self.pages_numbers[url_index] is None:
                    results.append(self.urls[url_index])
                    continue
                # formats payload into URL
                results.extend([str(self.urls[url_index] + "?" + "&".join("%s=%s" % (k, v)
                                                                          for k, v in payload.items())).format(i)
                                for i in range(1, int(self.pages_numbers[url_index]))])
        else:
            for url_index in range(len(self.urls)):
                if self.pages_numbers[url_index] is None:
                    results.append(self.urls[url_index])
                    continue
                # formats payload into URL
                results.extend([str(self.urls[url_index] + "?" + "&".join("%s=%s" % (k, v)
                                                                          for k, v in payload.items())).format(i)
                                for i in range(1, pages_callable(int(self.pages_numbers[url_index])))])
        self.urls = results
        del self.pages_numbers
        return self

    def keep_matching_urls(self, pattern: re.Pattern):
        """Checks urls for pattern provided and returns the ones who match."""
        self.urls = [url for url in self.urls if re.search(pattern, url) is not None]
        return self

    def get_subset(self, cardinality: int):
        """Gets subset from cache."""
        self.urls = self.urls[:cardinality]
        return self

    def get_single_level_urls(self, level: list[WebSelector]) -> list:
        """Gets a single node level."""
        processor = Processor()\
            .bind_new_extractor(selectors=level)\
            .bind_new_requester(urls=self.urls, request_settings=self.webhost.request_settings)
        extracted = processor.process_and_return()
        return extracted

    def crawl(self, levels: list[list[WebSelector]]):
        """levels refer to the tree structure of a website."""
        for level in levels:
            self.urls = self.get_single_level_urls(level=level)
        return self


class HTMLCrawler(Crawler):

    def __init__(self, webhost: WebHost, urls: list):
        super().__init__(webhost=webhost, urls=urls)

    def first_crawl(self, connection: ConnectionObject):
        """Crawls the entire webpage for the first time."""
        print("\tFirst crawl.")
        processor = Processor()\
            .bind_new_requester(urls=self.urls, request_settings=self.webhost.request_settings)\
            .bind_new_extractor(selectors=self.webhost.content_selectors)\
            .bind_connection(connection=connection)
        processor.process_output(table_name=self.webhost.table_name+'_raw')
        self.webhost.crawl_history.append(datetime.today())

    def next_crawl(self, connection: ConnectionObject) -> None:
        # add what happens if prod is missing (revert back to first crawl)
        # this function is not neat, reminder to make it better.

        """If the host has been crawled in the past,
        it will load all the previously saved "active" urls and compare them to the newly found ones.
        Tries to make a delta so as to save the time. First we check urls that are missing in the new cache.
        Then we start crawling the ones that do not exist in the old cache.
        This reduces the crawl significantly as it doesn't crawl the entire website again, but just the delta."""
        print("\tRecrawl.")
        try:
            dataframe = connection.load_file(table_name=self.webhost.table_name+'_prod',
                                             pd_filter="df['status'] == 'active'")
        except mysql.connector.errors.ProgrammingError:
            # default to _raw table
            dataframe = connection.load_file(table_name=self.webhost.table_name + '_raw',
                                             pd_filter="df['status'] == 'active'")
        url_difference = lm.get_difference(first=dataframe["url"].tolist(), second=self.urls)
        logging.info(f"Prior to crawl {len(self.webhost.crawl_history) + 1},"
                     f" extraction shows {len(url_difference['in_first'])}"
                     f" potential inactive urls and {len(url_difference['in_second'])} newly found.")
        if len(url_difference.get("in_first")) > 0:
            print(f"There are {len(url_difference.get('in_first'))} urls which seem to be missing.")
            processor = Processor() \
                .bind_new_requester(urls=url_difference.get("in_first"),
                                    request_settings=self.webhost.request_settings) \
                .bind_connection(connection=connection)
            try:
                processor.set_inactive(table_name=self.webhost.table_name+'_prod')
            except mysql.connector.errors.ProgrammingError:
                # default to _raw table
                processor.set_inactive(table_name=self.webhost.table_name + '_raw')
        if len(url_difference.get("in_second")) > 0:
            print(f"There are {len(url_difference.get('in_second'))} new urls to crawl from.")
            processor = Processor() \
                .bind_new_requester(urls=url_difference.get("in_second"),
                                    request_settings=self.webhost.request_settings) \
                .bind_new_extractor(selectors=self.webhost.content_selectors) \
                .bind_connection(connection=connection)
            processor.process_output(table_name=self.webhost.table_name+'_raw')
        self.webhost.crawl_history.append(datetime.today())

    def crawl(self, connection: ConnectionObject):
        """Initiates main crawl. Checks if the host is crawled previously or not."""
        print("Main crawl initiated.")
        if len(self.webhost.crawl_history) == 0:
            self.first_crawl(connection=connection)
        else:
            self.next_crawl(connection=connection)
        logging.info(f"Current status:\n"
                     f"Host {self.webhost.name}.{self.webhost.top_level_domain};\n"
                     f"ID: {self.webhost.host_id};\n"
                     f"Created at {self.webhost.created_at};\n"
                     f"Last crawled at: {self.webhost.crawl_history[-1]};\n"
                     f"Times crawled: {len(self.webhost.crawl_history)};\n"
                     f"Crawl history: {self.webhost.crawl_history}\n"
                     f"Recrawl enabled: {self.webhost.recrawl};\n"
                     f"Content selectors: {', '.join([item.name for item in self.webhost.content_selectors])};\n"
                     f"Output type set to: {type(connection)}")
        logging.info(f"{self.webhost.host_id}-{self.webhost.name} crawl number: {len(self.webhost.crawl_history)} finished.")
        self.webhost.save()
