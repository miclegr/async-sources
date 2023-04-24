from abc import ABC, abstractmethod
import asyncio
from .subscription import Subscription, SubscriptionItem

class NoUpdate(Exception):
    pass

class Source(ABC):

    def __init__(self, *sources: "Source", auto_start_feeding_subscriptions=True):

        assert all(isinstance(source, Source) for source in sources)
        self._sources = sources
        self._source_subscriptions = [s.subscribe() for s in self._sources]
        self._subscriptions = [] 
        self._setup_internal_store()
        self._loop = None
        if auto_start_feeding_subscriptions:
            self.start_feeding_subscriptions()

    def _setup_internal_store(self):
        pass

    async def _task(self):

        running = []
        news = [asyncio.create_task(s.get()) for s in self._source_subscriptions]

        while True:
            
            coros = [*news, *running]

            args = []
            news = []

            if len(coros) > 0:
                finished, running = await asyncio.wait(coros, return_when=asyncio.FIRST_COMPLETED)

                for future in finished:

                    subscription_item = future.result()
                    source_idx = self._get_source_idx_from_subscription_id(subscription_item.subscription_id)
                    new = asyncio.create_task(self._source_subscriptions[source_idx].get())

                    args.append((subscription_item.data, source_idx))
                    news.append(new)

            else:
                await asyncio.sleep(0.) #force await
                args = []
                news = []

            try:
                
                update = await self._process_update(*args)

                for subscription in self._subscriptions:
                    subscription.feed(update)

            except NoUpdate:
                pass


    def _get_subscription_from_source(self, source: 'Source') -> Subscription:
        for _source, subscription in zip(self._sources, self._subscriptions):
            if _source is source:
                return subscription
        else:
            raise AttributeError

    def _get_source_idx_from_subscription_id(self, id_:int|None) -> int|None:

        if id_ is None:
            return None

        for i,subscription in enumerate(self._source_subscriptions):
            if subscription.id == id_:
                return i
        else:
            raise AttributeError

    @abstractmethod
    async def _process_update(self, *args):
        pass

    def subscribe(self):
        subscription = Subscription(self)
        self._subscriptions.append(subscription)
        return subscription

    def start_feeding_subscriptions(self):
        if self._loop is None:
            self._loop = asyncio.create_task(self._task())
        else:
            raise RuntimeError

    async def stop_feeding_subscriptions(self):
        if self._loop is not None:
            self._loop.cancel()
            try:
                await self._loop
            except asyncio.CancelledError:
                pass
        else:
            raise RuntimeError

