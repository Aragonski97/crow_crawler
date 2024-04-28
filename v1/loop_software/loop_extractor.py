import itertools
from loop_software.loop_selector import WebSelector
from multiprocessing import Queue
from typing import Optional
from datetime import datetime


class NotConnectedError(IOError):
    """Extractor not connected to multiprocessing pipes."""


class Extractor:
    """Class is wraps a list of selectors extracts from request_output_queue
    using selectors and sends to extractor_output_queue."""
    def __init__(self,
                 selectors: list[WebSelector],
                 request_output_queue: Optional[Queue] = None,
                 extractor_output_queue: Optional[Queue] = None) -> None:
        self.selectors = selectors
        # inbound queue
        self.request_output_queue = request_output_queue
        # outbound queue
        self.extractor_output_queue = extractor_output_queue

    def __len__(self):
        return len(self.selectors)

    def batches_extract(self):
        if self.request_output_queue is None or self.extractor_output_queue is None:
            raise NotConnectedError
        i = 1
        while True:
            batch = self.request_output_queue.get(block=True)
            if batch is None:
                break
            assert type(batch) is list
            if len(batch) == 0:
                continue
            length = len(batch)
            batch = [item for item in batch if item is not None]
            if len(batch) < length:
                print(f"{length-len(batch)} Nones appearing.")
            data = self.extract(batch=batch)
            print(f"{datetime.now()}: Batch number {i} processed.")
            i += 1
            self.extractor_output_queue.put(data)
        self.extractor_output_queue.put(None)
        print(f"{datetime.now()}: Extraction finished.")

    def extract_specific(self, text: str) -> Optional[dict]:
        final = dict()
        for selector in self.selectors:
            final.update({selector.name: selector.extract(text)})
        return final

    def extract_any(self, text: str) -> list:
        final = list()
        for selector in self.selectors:
            final.extend(selector.extract(text))
        return final

    def extract(self, batch: list):
        protocol = self.selectors[0].method
        if protocol == "specific":
            return [self.extract_specific(text=text) for text in batch]
        else:
            return list(itertools.chain.from_iterable([self.extract_any(text=text) for text in batch]))
