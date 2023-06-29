from abc import ABC, abstractmethod
from typing import Any
from .source import NoUpdate, Source

class FilterSource(Source, ABC):

    async def _process_update(self, *args):
        assert len(args) == 1
        value,  _ = args[0]

        if self._filter_fn(value):
            return [value]
        else:
            raise NoUpdate

    @abstractmethod
    def _filter_fn(self, value: Any) -> bool:
        pass
