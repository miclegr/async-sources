from typing import Dict, List, Any, Tuple
from abc import ABC, abstractmethod, abstractstaticmethod
from ..source import NoUpdate, Source
from ..subscription import Subscription
from .base import CollectionSource

class InnerForkSource(Source):

    def __init__(self, key: str):
        self._key = key
        self.set = False
        super().__init__(*[], feeding_subscriptions_policy='never')

    def set_source(self, source, subscription):
        self._sources = [source]
        self._source_subscriptions = [subscription]
        self.set = True
        if len(self.subscritions())>0:
            self.start_feeding_subscriptions()
        else:
            self._feeding_subscription_policy = 'on_subscribe'

    async def _process_update(self, *args) -> List[Any]:

        if not self.set:
            raise NoUpdate

        assert len(args) == 1
        data, _ = args[0]
        return [data]


class ForkSource(CollectionSource, ABC):

    @abstractmethod
    def _get_keys_from_update(self, data: Any) -> List[Any]:
        pass

    def _process_data(self, source, key, data) -> None:
        for subscription in source._source_subscriptions:
            subscription.feed(data)

    def _check_if_new_key(self, key) -> bool:
        source = self.get_inner_source(key)
        assert isinstance(source, InnerForkSource)
        return not source.set

    def _handle_new_key(self, source:Source, key: Any):
        assert isinstance(source, InnerForkSource)
        source.set_source(self, Subscription(self))
        
    def _create_new_inner_source(self, key) -> Source:
        return InnerForkSource(key)
