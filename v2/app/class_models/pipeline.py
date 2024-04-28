from asyncio import create_task, sleep
from abc import ABC, ABCMeta
from .package import Package
from .profile import Profile
from .step import Scraper, AsyncQueue, AsyncEvent
from app.logger.crow_logger import logger


class Pipeline(ABC):
    __metaclass__ = ABCMeta
    ...


class ScrapingPipeline(Pipeline):

    def __init__(self, profile: Profile):
        """
        :param profile: A profile that will be run through the engines
        Pipeline class serves as a Wrapper around Profile class which includes its Scrapers [Steps].
        Besides this, it handles communication with the AsyncEngine and distribution
        of packages sent and received by it.
        """
        self.profile = profile
        self.scrapers: list[Scraper] = list()
        self.general_inbound = AsyncQueue()
        self.stop_distributing = AsyncEvent()

    def __len__(self):
        return len(self.scrapers)

    def get_scraper_order_by_name(self, name: str):
        for scraper in self.scrapers:
            if scraper.name == name:
                return scraper.pipeline_order_id
        return None

    async def send_initial_packages(self):
        try:
            await self.scrapers[0].inbound.put(
                Package(
                    pipeline_name=self.profile.name,
                    step_order_id=0,
                    data=[self.scrapers[0].initial_url],
                    selectors=self.scrapers[0].selectors
                )
            )
            await self.scrapers[0].inbound.put(
                Package(
                    pipeline_name=self.profile.name,
                    step_order_id=0,
                    data=list(),
                    selectors=list(),
                    closed_inbound=True
                )
            )
        except Exception as err:
            logger.error(err)
            print(err)
        return

    async def distribute_packages(self) -> None:
        """
        Decides which step should get the package retrieved from \n
        self.general_inbound <- AsyncEngine.inbound <- SyncEngine.outbound
        """
        while not self.stop_distributing.is_set():
            package = await self.general_inbound.get()
            # packages processed from the last step will go  to database within AsyncEngine class methods
            try:
                assert package.step_order_id < len(self.scrapers) - 1
                # send the data to the next scraper
                await self.scrapers[package.step_order_id + 1].inbound.put(package)
                print(f"Package distributed to {self.scrapers[package.step_order_id + 1]}.")
            except AssertionError as err:
                print("Assertion Error")
                logger.error(err)
            except IndexError as err:
                print("INDEX ERROR")
                logger.error(err)
        return

    async def initiate(self, outbound: AsyncQueue, database: AsyncQueue):
        """
        :param outbound: AsyncEngine.pipeline_outbound that will be sent to SyncEngine.
        :param database: AsyncQueue() that delivers results to the database.
        Starts the package distribution coroutine that will run until clean_scrapers is finished.
        Sets the package outbound destination of all steps to match the AsyncEngine.pipeline_outbound
        and runs them in order.
        """
        # activate distribution of packages received in general inbound
        _ = create_task(self.distribute_packages())
        print(f"Started package distribution for {self!r}")
        # send initial urls and closing inbound package
        # set specifically outside the bottom loop in order to make sure all outbounds are set before
        # scrapers start running
        for scraper in self.scrapers:
            scraper.inbound = AsyncQueue()
            scraper.outbound = outbound
        print("Scrapers IO set.")
        for scraper in self.scrapers:
            _ = create_task(scraper.run())
            await sleep(0.5)
        await self.send_initial_packages()
        print("Initial packages sent!")
        return

    async def clean_up_scrapers(self):
        """
        End of a lifecycle of a pipeline. Awaits for scrapers to finish and then closes them.
        Once the last scraper is closed, the distribution is stopped as well.
        """
        for scraper in self.scrapers:
            await scraper.finished.wait()
            await scraper.inbound.join()
        await self.general_inbound.join()
        self.stop_distributing.set()
