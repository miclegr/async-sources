import asyncio
from abc import ABC, abstractmethod
from .source import Source

class StopLoop(Source, ABC):

    trigger: int
    grace_time_sec: int

    @abstractmethod
    def _check_value(self, value) -> bool:
        pass

    def __init__(self, source: Source):
        self.counter = 0
        super().__init__(source, feeding_subscriptions_policy='immediate')

    async def _process_update(self, *args):
        assert len(args) == 1
        data, _ = args[0]

        if self._check_value(data):
            self.counter+=1
        else:
            self.counter=0

        if self.counter >= self.trigger:
            if self.grace_time_sec>0:
                await asyncio.sleep(self.grace_time_sec)
            asyncio.get_event_loop().stop()

        return []

