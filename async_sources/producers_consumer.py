from .source import Source, NoUpdate, BatchedSource
from .queue import Queue
from typing import List

class InnerProducerSource(BatchedSource):

    def __init__(self, source: Source, buffer: Queue):

        self.buffer = buffer
        super().__init__(source, feeding_subscriptions_policy='immediate')

    async def _process_update(self, *args) -> List:

        items, _ = args[0]
        for item in items:
            self.buffer.put_nowait(item)

        raise NoUpdate

class InnerConsumerSource(Source):

    def __init__(self, buffer:Queue, feeding_subscriptions_policy: str = 'on_subscribe'):

        self.buffer = buffer
        super().__init__(feeding_subscriptions_policy=feeding_subscriptions_policy)

    async def _process_update(self, *args) -> List:

        data = await self.buffer.get_all()
        return data


def producers_and_consumer():

    buffer = Queue()

    def producer_factory(source :Source):
        return InnerProducerSource(source, buffer=buffer)

    consumer = InnerConsumerSource(buffer)

    return producer_factory, consumer

def consumer_only():
    buffer = Queue()
    consumer = InnerConsumerSource(buffer)

    return buffer, consumer
