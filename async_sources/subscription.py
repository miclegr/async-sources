from asyncio import Queue
from typing import Any

class SubscriptionItem(object):

    def __init__(self, id_: int, data:Any) -> None:
        self.subscription_id = id_
        self.data = data

class Subscription(object):

    def __init__(self, source: 'Source') -> None:
        self._source = source
        self._queue = Queue()
        self.id = id(self)

    def feed(self, data:Any) -> None:
        self._queue.put_nowait(data)

    async def get(self) -> SubscriptionItem:
        data = await self._queue.get()
        return SubscriptionItem(self.id, data)
