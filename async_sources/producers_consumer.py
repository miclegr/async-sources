from .source import Source, NoUpdate
from asyncio import Queue
from typing import List

class InnerProducer(Source):

    def __init__(self, source: "Source", buffer: Queue):

        self.buffer = buffer
        super().__init__(source, feeding_subscriptions_policy='immediate')

    async def _process_update(self, *args) -> List:

        data, _ = args[0]
        await self.buffer.put(data)
        raise NoUpdate

class InnerConsumer(Source):

    def __init__(self, buffer:Queue, feeding_subscriptions_policy: str = 'on_subscribe'):

        self.buffer = buffer
        super().__init__(feeding_subscriptions_policy=feeding_subscriptions_policy)

    async def _process_update(self, *args) -> List:

        data = await self.buffer.get()
        return [data]


def producers_and_consumer():

    buffer = Queue()

    def producer_factory(source :Source):
        return InnerProducer(source, buffer=buffer)

    consumer = InnerConsumer(buffer)

    return producer_factory, consumer

