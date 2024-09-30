from typing import List, Any
from .base import CollectionSource
from ..source import Source
from abc import ABC, abstractmethod, abstractstaticmethod

class MapCollection(CollectionSource, ABC):

    @abstractstaticmethod
    def _mapping_fn(source: Source, key: Any) -> Source:
        pass

    def _get_keys_from_update(self, key: Any) -> List[Any]:
        return [key]

    def _check_if_new_key(self, key) -> bool:
        return True

    def _handle_new_key(self, source:Source, key: Any):
        pass
        
    def _process_data(self, source, key, data) -> None:
        pass

    def _create_new_inner_source(self, key) -> Source:
        parent_collection = self._sources[0]
        return self._mapping_fn(parent_collection.get_inner_source(key), key)


