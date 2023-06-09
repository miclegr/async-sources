from typing import Dict, List, Any, Tuple
from abc import ABC, abstractmethod, abstractstaticmethod
from ..source import NoUpdate, Source
from ..subscription import Subscription


class CollectionSource(Source, ABC):

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
        data, _ = args[0]

        keys = self._get_keys_from_update(data)

        new_keys = []
        for key in keys:
            if self._check_if_new_key(key):
                new_keys.append(key)

        for key in keys:
            source = self.get_inner_source(key)
            if key in new_keys:
                self._handle_new_key(source, key)
            self._process_data(source, key, data)
        
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
