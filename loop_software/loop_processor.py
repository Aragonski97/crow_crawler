from loop_software.loop_requests import Requester
from loop_software.loop_connections import ConnectionObject, SQLConnection, AWSConnection, CSVConnection
from loop_software.loop_extractor import Extractor
from multiprocessing import Queue, Process
from typing import Literal


class MissingBindError(AttributeError):
    """The function you have chosen is missing an adequate bind."""
    def __init__(self, message: str) -> None:
        self.message = message
        super().__init__(self.message)


class Processor:
    """This class allows for easy debugging within the crawling process."""
    def __init__(self):
        self.requester: Requester | None = None
        self.extractor: Extractor | None = None
        self.connection: ConnectionObject | None = None

    def bind_extractor(self, extractor: Extractor):
        self.extractor = extractor
        return self

    def bind_requester(self, requester: Requester):
        self.requester = requester
        return self

    def bind_connection(self, connection: ConnectionObject):
        self.connection = connection
        return self

    def bind_new_extractor(self, selectors):
        self.extractor = Extractor(selectors=selectors)
        return self

    def bind_new_requester(self, request_settings: dict, urls: list | None = None):
        self.requester = Requester(request_settings=request_settings, urls=urls)
        return self

    def bind_new_connection(self, connection_config: dict,
                            c_type: Literal["sql", "aws", "csv", "external_server"]):
        if c_type == "sql":
            self.connection = SQLConnection(connection_config=connection_config)
        elif c_type == "csv":
            self.connection = CSVConnection(connection_config=connection_config)
        elif c_type == "aws":
            self.connection = AWSConnection(connection_config=connection_config)
        self.connection.connect()
        return self

    def connect(self):
        requester_output_queue = Queue()
        extractor_output_queue = Queue()
        self.requester.request_output_queue = requester_output_queue
        self.extractor.request_output_queue = requester_output_queue
        self.extractor.extractor_output_queue = extractor_output_queue

    def finish_process(self):
        self.requester = None
        self.extractor = None
        self.connection = None
        return self

    def process_output(self,  table_name: str) -> None:
        """Crawls, extracts and outputs the data."""
        self.connect()
        if self.connection is None or self.requester is None or self.extractor is None:
            raise MissingBindError(message="One of the binds: connection , requester or extractor is missing.")
        crawl_urls_process = Process(target=self.requester.async_process)
        extract_data_process = Process(target=self.extractor.batches_extract)
        crawl_urls_process.start()
        extract_data_process.start()
        while True:
            extracted_data = self.extractor.extractor_output_queue.get(block=True)
            if extracted_data is None:
                break
            self.connection.upload_file(data=extracted_data, table_name=table_name)
        crawl_urls_process.join()
        extract_data_process.join()
        self.finish_process()
        return

    def process_and_return(self) -> list:
        """Crawls and extracts and returns the data."""
        if self.extractor is None or self.requester is None:
            raise MissingBindError("One of the binds: extractor , requester is missing.")
        self.connect()
        crawl_urls_process = Process(target=self.requester.async_process)
        extract_data_process = Process(target=self.extractor.batches_extract)
        crawl_urls_process.start()
        extract_data_process.start()
        results = list()
        while True:
            extracted_data = self.extractor.extractor_output_queue.get(block=True)
            if extracted_data is None:
                break
            results.extend(extracted_data)
        crawl_urls_process.join()
        extract_data_process.join()
        self.finish_process()
        return results

    def simple_fetch_extract_return(self, method: Literal["async", "sync"]) -> list:
        """Name implies the function."""
        if self.extractor is None or self.requester is None:
            raise MissingBindError("One of the binds: extractor , requester is missing.")
        if method == "async":
            html = self.requester.async_fetch_return()
            extracted_data = self.extractor.extract(batch=html)
        else:
            html = self.requester.sync_fetch_return()
            extracted_data = self.extractor.extract(batch=html)
        self.finish_process()
        return extracted_data

    def simple_fetch_return(self, method: Literal["async", "sync"]):
        """Name implies the function."""
        if self.requester is None:
            raise MissingBindError("One of the binds: requester  is missing.")
        if method == "async":
            data = self.requester.async_fetch_return()
        else:
            data = self.requester.sync_fetch_return()
        self.finish_process()
        return data

    def simple_extract_return(self, data: list[str]):
        """Name implies the function."""
        if self.extractor is None:
            raise MissingBindError("One of the binds: requester  is missing.")
        data = self.extractor.extract(batch=data)
        self.finish_process()
        return data

    def set_inactive(self, table_name: str, urls: list | None = None) -> None:
        """Sets the missing urls inactive in the database."""
        if self.requester is None or self.connection is None:
            raise MissingBindError("One of the binds: requester , connection is missing.")
        if urls is not None:
            self.requester.urls = urls
        responses = self.requester.async_verify_return(urls=urls)
        inactive_urls = [item[0] for item in zip(self.requester.urls, responses) if item[1] == "inactive"]
        self.connection.set_inactive_status(table_name=table_name, index="url", data=inactive_urls)
        self.finish_process()
        return
