import asyncio
import concurrent.futures
import multiprocessing as mp
import time
from asyncio import Task, BaseEventLoop, get_running_loop
from app.class_models.pipeline import ScrapingPipeline, AsyncQueue, sleep, create_task
from app.class_models.package import Package

from app.logger.crow_logger import logger

from crow_database import CORE_DATABASE

# the number of concurrent pipelines being handled. This is in order to control the stress on the machine.
# I run a very old laptop and its not fun having more than 5 concurrent pipelines.
MAX_WORKERS = 5


class AsyncEngine:

    def __init__(
            self,
            inbound: mp.Queue,
            outbound: mp.Queue
    ) -> None:
        """
        Thought of as a hub of all important asynchronous functions.
        """

        self.inbound = inbound
        self.outbound = outbound
        self.pipeline_backlog = AsyncQueue()
        self.pipeline_outbound = AsyncQueue()
        self.pipelines: dict[str, ScrapingPipeline] = dict()
        self.tasks: list[Task] = list()
        self.loop: BaseEventLoop | None = None
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=10)
        self.database = AsyncQueue()

    async def track_new_pipelines(self):
        """
        Awaits new Pipeline from the backlog, creates a reference to it in self.pipelines as well as self.tasks
        and forwards it to another coroutine for handling.
        Runs indefinitely, should implement kill/pause asyncio.Event
        """
        while True:
            if len(self.pipelines) < MAX_WORKERS:
                pipeline: ScrapingPipeline = await self.pipeline_backlog.get()
                self.pipelines[pipeline.profile.name] = pipeline
                task = create_task(self.handle_pipeline(pipeline=pipeline))
                self.tasks.append(task)
            else:
                pass
            await sleep(5)

    async def handle_pipeline(self, pipeline: ScrapingPipeline):
        """
        Takes care of a given pipeline. Creates relevant database so that scraped data have somewhere to go.
        Initiates the pipeline and waits for it to clean up.
        """
        logger.info(f"New pipeline {pipeline!r} is being handled")
        try:
            await CORE_DATABASE.create_dynamic_table(table_name=pipeline.profile.name,
                                                     selectors=pipeline.scrapers[-1].selectors)
        except Exception as err:
            logger.error(err)
        else:
            logger.info("Success")
        await pipeline.initiate(outbound=self.pipeline_outbound, database=self.database)
        await pipeline.clean_up_scrapers()
        self.pipelines.pop(pipeline.profile.name)
        logger.info(f"Pipeline {pipeline!r} has been cleaned up and removed from tasks.")
        return

    async def forward_from_sync_engine(self):
        """
        Indefinitely awaits package from SyncEngine. The package contains information that helps forward
        the package to the adequate Pipeline and consequently Scraper.
        Should implement kill/pause asyncio.Event
        """
        while True:
            start = time.time()
            print(f"______________________________________________________")
            # runs in executor since self.inbound.get is a synchronous function
            data = await self.loop.run_in_executor(self.executor, self.inbound.get)
            print(f"Package {data.pipeline_name} has been retrieved from SyncEngine.")
            # assert that everything that comes out of that queue is a package.
            assert isinstance(data, Package)
            if data.step_order_id == len(self.pipelines[data.pipeline_name].scrapers) - 1:
                # if the Step / Scraper which sent the data is the last one in the line,
                # send the package to the database output queue
                await self.database.put(data)
                print(f"Package placed in {self.pipelines[data.pipeline_name]}")
                print(f"______________________________________________________")
                print(f"TIME: {time.time() - start}")
                continue
            # forward the package to the pipeline for internal handling.
            await self.pipelines[data.pipeline_name].general_inbound.put(data)
            print(f"Package placed in {self.pipelines[data.pipeline_name]}")
            print(f"______________________________________________________")
            print(f"TIME: {time.time()-start}")

    async def forward_to_sync_engine(self):
        """
        By design, all Scrapers from all pipelines send their output data to the self.pipeline_outbound queue,
        then this function sends it to the SyncEngine.
        This was created in order to avoid overhead of referencing SyncEngine inbound queue in pipeline code.
        Runs indefinitely, should implement kill/pause asyncio.Event
        """
        while True:
            package = await self.pipeline_outbound.get()
            self.outbound.put(package)

    async def export_to_database(self):
        """
        Exports scraped data into a database referenced by package.pipeline_name which it expects to be created
        ahead of time when initiating a pipeline.
        Runs indefinitely, should implement kill/pause asyncio.Event
        """
        while True:
            package = await self.database.get()
            table_name = package.pipeline_name
            await CORE_DATABASE.insert_scraped_data(data=package.data, table_name=table_name)

    async def run(self) -> list:
        self.loop = get_running_loop()
        try:
            async with asyncio.TaskGroup() as group:
                task2 = group.create_task(self.track_new_pipelines())
                task3 = group.create_task(self.forward_to_sync_engine())
                task4 = group.create_task(self.forward_from_sync_engine())
                task5 = group.create_task(self.export_to_database())
        except Exception as err:
            # in case some unhandled exceptions arise
            # since this engine is run in as Task (create_task)
            # it will not raise an error to the server event loop.
            logger.error(err)
            print(f"-------------------------TOTAL CLOSURE {err}-------------------------")
            exit(1)
        return [item.result() for item in [task2, task3, task4, task5]]