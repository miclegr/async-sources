from typing import Dict, List, Any, Tuple
from abc import ABC, abstractmethod, abstractstaticmethod
from ..source import NoUpdate, Source, BatchedSource
from ..subscription import Subscription


class CollectionSource(BatchedSource, ABC):

    def __init__(self, *sources) -> None:
        super().__init__(*sources)
        self.start_feeding_subscriptions()

    def _setup_internal_store(self):
        self._inner_sources: Dict[Any, Source] = {}

    @abstractmethod
    def _get_keys_from_update(self, data: Any) -> List[Any]:
        pass

    @abstractmethod
    def _process_data(self, source, key, data) -> None:
        pass

    @abstractmethod
    def _check_if_new_key(self, key) -> bool:
        pass

    @abstractmethod
    def _handle_new_key(self, source:Source, key: Any):
        pass
        
    async def _process_update(self, *args) -> List[Any]:

        assert len(args) == 1
        items, _ = args[0]

        new_keys = []
        for item in items:

            keys = self._get_keys_from_update(item)

            for key in keys:
                if self._check_if_new_key(key):
                    new_keys.append(key)
                    source = self.get_inner_source(key)
                    self._handle_new_key(source, key)
                else:
                    source = self.get_inner_source(key)

                self._process_data(source, key, item)
        
        return new_keys

    @abstractmethod
    def _create_new_inner_source(self, key) -> Source:
        pass

    def get_inner_source(self, key: Any) -> Source:
        if key in self._inner_sources:
            return self._inner_sources[key]
        else:
            source = self._create_new_inner_source(key)
            self._inner_sources[key] = source
            return source

    def subscribe_key(self, key: Any) -> Subscription:
        return self.get_inner_source(key).subscribe()
