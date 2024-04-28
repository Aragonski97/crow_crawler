from itertools import chain
from multiprocessing import Queue as SyncQueue
from app.class_models.package import Package
from app.class_models.selector import SelectorList

from app.logger.crow_logger import logger


class SyncEngine:

    def __init__(self, inbound: SyncQueue, outbound: SyncQueue):
        """
        SyncEngine that is supposed to be run in a separate process.
        Thought of as a hub of all CPU bound tasks, but currently only does extraction.
        """
        self.inbound = inbound
        self.outbound = outbound

    @staticmethod
    def extract(data: list, selectors: SelectorList) -> None | list | str:
        """
        There are 3 implicit extraction options given within provided selectors.
            1) first
            2) all
            3) first - REUSE
        1) This option returns first matches of all selectors given html data.
        2) This option returns all matches of all selectors given html data.
        Returns flattened list, since I used it predominantly for url extraction.
        If needed be, raise an issue on GitHub so that I could implement additional features.
        For instance, if a website has many *primary* items on one page, instead of having
        a single page per item (ebay, amazon...)
        3) Specifically used to send data to the following Scraper based on input.
        For instance, if you need to format your urls with the data from a single page,
        you could do it by making a CrowSelector with REUSE as its name, and input the adequate post_processor

        I have an idea to completely overhaul this logic using parsel only, with all cool features,
        but I simply lack time at the moment.
        """
        # all selectors must have the same method
        protocol = selectors[0].method
        try:
            data = [dict(selectors.extract(data=text)) for text in data]
        except TypeError as err:
            logger.error(err)
            return
        if protocol == "first":
            if selectors[0].name == "REUSE":
                if len(data) == 1:
                    return data[0]["REUSE"]
            return data
        else:
            return list(
                chain.from_iterable(
                    list(
                        chain.from_iterable(
                            [item.values() for item in data])
                    )
                )
            )

    def process_package(self, package: Package):
        """
        Extracts data off the provided html and returns data wrapped in the Package to its adequate sender.
        """
        if package.data is None and package.selectors is None:
            return
        processed_data = self.extract(data=package.data, selectors=package.selectors)
        if processed_data is None:
            return
        self.outbound.put(
            Package(
                pipeline_name=package.pipeline_name,
                step_order_id=package.step_order_id,
                data=processed_data,
                closed_inbound=package.closed_inbound
            )
        )
        return

    def initiate(self):
        """
        Runs indefinitely as per design, however, this should include kill/pause multiprocessing Events.
        """
        while True:
            package = self.inbound.get()
            if not isinstance(package, Package):
                continue
            else:
                if package.closed_inbound is True:
                    self.outbound.put(package)
                    continue
                self.process_package(package)
                continue
        return
