from abc import ABC, abstractmethod
from typing import Any
from .source import BatchedSource

class FilterSource(BatchedSource, ABC):

    async def _process_update(self, *args):
        assert len(args) == 1
        values,  _ = args[0]

        filtered_values = []
        for value in values:
            if self._filter_fn(value):
                filtered_values.append(value)

        return filtered_values

    @abstractmethod
    def _filter_fn(self, value: Any) -> bool:
        pass
