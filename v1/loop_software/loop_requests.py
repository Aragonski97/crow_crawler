import aiohttp
import asyncio
import requests
from multiprocessing import Queue
from functools import partial
from typing import Optional, Any
from loop_software import loop_miscellaneous as lm
from datetime import datetime


async def _verify(session: aiohttp.ClientSession, url: str) -> Any:
    """Verifies if the server responds properly."""
    async with session.get(url=url, allow_redirects=False) as data:
        await asyncio.sleep(0.0001)
        if data.status in lm.SUCCESS_RESPONSES:
            return "active"
        else:
            return "inactive"


async def _fetch(session: aiohttp.ClientSession, url: str) -> Optional[str]:
    """Fetches the data off website."""
    async with session.get(url=url) as data:
        if data.status not in lm.SUCCESS_RESPONSES and data.status != 302:
            return None
        extract = await data.text()
        return extract


async def _request_fill_queue(urls: list,
                              request_settings: dict,
                              _protocol: partial,
                              request_output_queue: Optional[Queue]) -> None:
    """Sets up aiohttp.ClientSession object and starts the crawl. Sends to extractor in batches."""
    aiohttp_timeout = aiohttp.ClientTimeout(total=None)
    aiohttp_connector = aiohttp.TCPConnector(limit=request_settings.get("connection_limit"))
    aiohttp_session = aiohttp.ClientSession(headers=request_settings.get("headers"),
                                            connector=aiohttp_connector,
                                            trust_env=request_settings.get("trust_env"),
                                            timeout=aiohttp_timeout)
    async with aiohttp_session as session:
        i = 1
        batches = lm.split_into_batches(batch_length=200, urls=urls)
        for batch in batches:
            data = await asyncio.gather(*[_protocol(session=session, url=url) for url in batch])
            print(f"{datetime.now()}: Batch number: {i} sent.")
            request_output_queue.put(data)
            i += 1
        request_output_queue.put(None)
        return


async def _return_data(urls: list, request_settings: dict, _protocol: partial) -> Any:
    """Sets up aiohttp.ClientSession object and starts the crawl. Sends back in bulk."""
    aiohttp_timeout = aiohttp.ClientTimeout(total=None)
    aiohttp_connector = aiohttp.TCPConnector(limit=request_settings.get("connection_limit"))
    aiohttp_session = aiohttp.ClientSession(headers=request_settings.get("headers"),
                                            connector=aiohttp_connector,
                                            trust_env=request_settings.get("trust_env"),
                                            timeout=aiohttp_timeout)
    async with aiohttp_session as session:
        i = 1
        final = list()
        batches = lm.split_into_batches(batch_length=200, urls=urls)
        for batch in batches:
            data = await asyncio.gather(*[_protocol(session=session, url=url) for url in batch])
            final.extend(data)
            print(f"{datetime.now()}: Batch number: {i} sent.")
            i += 1
        return final


class URLsMissing(AttributeError):
    def __init__(self, message: str) -> None:
        self.message = message
        super().__init__(self.message)


class Requester:

    def __init__(self,
                 request_settings: dict,
                 urls: Optional[list] = None,
                 request_output_queue: Optional[Queue] = None) -> None:
        self.request_settings = request_settings
        self.urls = urls
        self.request_output_queue = request_output_queue
        self._protocol: Optional[partial] = None

    def assign_protocol_fetch(self):
        self._protocol = partial(_fetch)
        return self

    def assign_protocol_verify(self):
        self._protocol = partial(_verify)
        return self

    def async_process(self, urls: Optional[list] = None) -> None:
        # asynchronously collects and sends data to self.request_output_queue
        assert self.request_output_queue is not None
        self.assign_protocol_fetch()
        print("Asynchronous process initiated.")
        if urls is None:
            if self.urls is None:
                raise URLsMissing("Requester doesn't have urls to work with.")
            asyncio.run(_request_fill_queue(urls=self.urls,
                                            request_settings=self.request_settings,
                                            _protocol=self._protocol,
                                            request_output_queue=self.request_output_queue))
        else:
            asyncio.run(_request_fill_queue(urls=urls,
                                            request_settings=self.request_settings,
                                            _protocol=self._protocol,
                                            request_output_queue=self.request_output_queue))
        self._protocol = None
        return

    def async_fetch_return(self, urls: Optional[list] = None) -> Any:
        # does what the name suggests
        self.assign_protocol_fetch()
        if urls is None:
            if self.urls is None:
                raise URLsMissing("Requester doesn't have urls to work with.")
            data = asyncio.run(_return_data(urls=self.urls,
                                            request_settings=self.request_settings,
                                            _protocol=self._protocol))
        else:
            data = asyncio.run(_return_data(urls=urls,
                                            request_settings=self.request_settings,
                                            _protocol=self._protocol))
        self._protocol = None
        return data

    def async_verify_return(self, urls: Optional[list] = None) -> Any:
        # does what the name suggests
        self.assign_protocol_verify()
        if urls is None:
            if self.urls is None:
                raise URLsMissing("Requester doesn't have urls to work with.")
            data = asyncio.run(_return_data(urls=self.urls,
                                            request_settings=self.request_settings,
                                            _protocol=self._protocol))
        else:
            data = asyncio.run(_return_data(urls=urls,
                                            request_settings=self.request_settings,
                                            _protocol=self._protocol))
        self._protocol = None
        return data

    def sync_fetch_return(self, urls: Optional[list] = None) -> list:
        self.assign_protocol_fetch()
        if urls is None:
            if self.urls is None:
                raise URLsMissing("Requester doesn't have urls to work with.")
            data = [requests.get(url=url, headers=self.request_settings.get("headers")).text for url in self.urls]
        else:
            data = [requests.get(url=url, headers=self.request_settings.get("headers")).text for url in urls]
        self._protocol = None
        return data

    def sync_verify_return(self, urls: Optional[list] = None) -> list:
        self.assign_protocol_verify()
        if urls is None:
            if self.urls is None:
                raise URLsMissing("Requester doesn't have urls to work with.")
            data = [requests.get(url=url, headers=self.request_settings.get("headers")).text for url in self.urls]
        else:
            data = [requests.get(url=url, headers=self.request_settings.get("headers")).text for url in urls]
        self._protocol = None
        return data
