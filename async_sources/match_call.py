from abc import ABC, abstractmethod
from typing import Any, List
from .source import Source, NoUpdate
from collections import defaultdict
from warnings import warn

class MatchAndCall(Source, ABC):

    duplicate_mode = 'raise'

    def _setup_internal_store(self):
        n_sources = len(self._sources)
        self._not_ready = defaultdict(lambda : [None for _ in range(n_sources)])

    @abstractmethod
    def _key_arg(self, value: Any, index: int) -> Any:
        pass

    @abstractmethod
    async def _call(self, key: Any, *args):
        pass

    async def _process_update(self, *args) -> List[Any]:

        ready_to_call = []
        for arg in args:

            value, index = arg
            key = self._key_arg(value, index)
            check = self._not_ready[key][index] is None
            if not check and self.duplicate_mode == 'raise':
                raise AttributeError(f'{key} at index {index} has duplicates')
            elif not check and self.duplicate_mode == 'warn':
                warn(f'{key} at index {index} has duplicates')
                continue

            self._not_ready[key][index] = value

            if all(x is not None for x in self._not_ready[key]):
                args = self._not_ready[key]
                del self._not_ready[key]
                ready_to_call.append((key, args))

        output = []
        for k, args in ready_to_call:
            output.append(await self._call(k, *args))

        return output
