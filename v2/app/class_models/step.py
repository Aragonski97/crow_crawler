from abc import ABC, ABCMeta, abstractmethod
from typing import Any, Self
from aiohttp import ClientTimeout, ClientSession, TCPConnector
from asyncio import Queue as AsyncQueue, Event as AsyncEvent, sleep, TaskGroup

from app.utils.split_to_batches import to_batches

from .package import Package
from .selector import CrowSelector

from app.logger.crow_logger import logger

# acceptable codes for scraping, ideally should be inside a scraper, but I didn't take the time to rethink this
ACCEPTABLE_CODES = [200, 302]
# batch length represents the amount of concurrently scrapped data
BATCH_LENGTH = 200


class Step(ABC):

    __metaclass__ = ABCMeta

    pipeline_order_id: int
    inbound: AsyncQueue
    outbound: AsyncQueue
    name: str

    def __repr__(self):
        return self.name

    def connect(self, inbound: AsyncQueue, outbound: AsyncQueue) -> None:
        self.inbound = inbound
        self.outbound = outbound
        return

    @abstractmethod
    def run(self) -> None:
        ...


class Scraper(Step):

    def __init__(
            self,
            name: str,
            pipeline_order_id: int,
            headers: dict,
            burst_rate: int,
            initial_url: str | None = None
    ) -> None:

        self.name = name
        self.pipeline_order_id = pipeline_order_id
        self.inbound: AsyncQueue | None = None
        self.outbound: AsyncQueue | None = None
        self.initial_url = initial_url
        self.headers = headers
        self.session = self.get_aiohttp_session(
            headers=headers,
            burst_rate=burst_rate,
            trust_env=True,
            timeout=None
        )
        self.selectors: list[CrowSelector] = list()
        self.finished = AsyncEvent()

    def get_selector_id_by_name(self, name: str):
        for selector in range(len(self.selectors)):
            if self.selectors[selector].name == name:
                return selector
        return None

    @staticmethod
    def get_aiohttp_session(
            headers: dict,
            burst_rate: int = 2,
            trust_env: bool = False,
            timeout: float | None = None
    ) -> ClientSession:

        aiohttp_timeout = ClientTimeout(total=timeout)
        aiohttp_connector = TCPConnector(limit=burst_rate)
        client_session = ClientSession(
            headers=headers,
            connector=aiohttp_connector,
            trust_env=trust_env,
            timeout=aiohttp_timeout
        )
        return client_session

    async def scrape_single_package(self, package: Package) -> list:
        _urls: list[str] = package.data
        assert len(_urls) <= BATCH_LENGTH
        return await self._scrape_batch(batch=_urls)

    async def _fetch(self, url: str) -> str | None:
        async with self.session.get(url=url) as _data:
            await sleep(0)
            if _data.status not in ACCEPTABLE_CODES:
                return None
            extract = await _data.text()
            return extract

    async def _verify(self, url: str) -> str:
        async with self.session.get(url=url, allow_redirects=False) as _data:
            await sleep(0)
            if _data.status == 200:
                return "active"
            else:
                return "inactive"
    
    async def _scrape_batch(self, batch: list[str]) -> list[Any]:
        try:
            async with TaskGroup() as _group:
                _tasks = [_group.create_task(self._fetch(url)) for url in batch]
            print(f"Data scraped for: {self!r}")

            items = [item.result() for item in _tasks]
            current_len = len(items)
            items = [item for item in items if item is not None]
            if current_len > len(items):
                logger.warning("There are nones appearing in the results. Sleeping.")
                await sleep(5)
            return items
        except Exception as err:
            logger.error(err)
            print(err)

    async def scrape_until_final(self) -> None:
        async with self.session:
            while True:
                _package: Package = await self.inbound.get()
                logger.debug(f"Package has been received at {self.name}.")
                if _package.closed_inbound:
                    await self.outbound.put(_package)
                    break
                _urls: list[str] = _package.data
                _batches: list[list[str]] = to_batches(urls=_urls, batch_length=BATCH_LENGTH)
                for _batch in _batches:
                    _data = await self._scrape_batch(batch=_batch)
                    await self.outbound.put(
                        Package(
                            pipeline_name=_package.pipeline_name,
                            step_order_id=self.pipeline_order_id,
                            data=_data,
                            selectors=self.selectors,
                            closed_inbound=_package.closed_inbound
                        )
                    )
        return

    async def run(self):
        logger.debug(f"Run has been called for {self!r}")
        assert self.inbound is not None
        assert self.outbound is not None
        await self.scrape_until_final()
        self.finished.set()
        return None

    def migrate_scraper(self, scraper: Self) -> None:
        """
        :param scraper: Scraper to inherit the properties from.
        :return:
        """
        self.selectors = scraper.selectors
        self.initial_url = scraper.initial_url
