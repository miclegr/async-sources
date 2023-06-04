from abc import ABC, abstractmethod
from typing import Any, List
from .source import Source, NoUpdate
from collections import defaultdict

class MatchAndCall(Source, ABC):

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

        for arg in args:
            value, index = arg
            key = self._key_arg(value, index)
            assert self._not_ready[key][index] is None
            self._not_ready[key][index] = value

        ready_to_call = []
        for k,v in self._not_ready.items():
            if not any(x is None for x in v):
                ready_to_call.append(k)

        output = []
        for k in ready_to_call:
            args = self._not_ready[k]
            del self._not_ready[k]

            output.append(await self._call(k, *args))

        return output
