from abc import abstractstaticmethod
import asyncio
from .source import NoUpdate, Source
from asyncio import sleep, Event
from typing import Any, List

class _InnerTimeoutSource(Source):

    def __init__(self, seconds: float, feeding_subscriptions_policy: str = 'on_subscribe'):
        self.start = Event()
        self.reset = Event()
        self.seconds = seconds
        super().__init__(feeding_subscriptions_policy=feeding_subscriptions_policy)

    async def _process_update(self, *args) -> List[Any]:

        await self.start.wait()
        is_reset, is_timeout = asyncio.create_task(self.reset.wait()), asyncio.create_task(sleep(self.seconds))
        finished, _ = await asyncio.wait([is_reset, is_timeout], return_when=asyncio.FIRST_COMPLETED)
        self.start.clear()
        if finished.pop() is is_reset:
            self.reset.clear()
            raise NoUpdate
        else: # timeout
            return [True]


class TimeoutSource(Source):

    def __init__(self, trigger: Source, response: Source, timeout_seconds: float, feeding_subscriptions_policy: str = 'on_subscribe'):

        self.timeout_seconds = timeout_seconds
        self.timeout_source = _InnerTimeoutSource(self.timeout_seconds)
        self.last_trigger = None

        super().__init__(trigger, response, self.timeout_source, feeding_subscriptions_policy=feeding_subscriptions_policy)

    @abstractstaticmethod
    def _response_if_timeout(last_trigger) -> Any:
        pass

    async def _process_update(self, *args) -> List[Any]:
        
        # force trigger to be processed first
        sorted_args = sorted(args, key=lambda x: x[1]) 
        
        for arg in sorted_args:
            value, index = arg

            if index == 0: # trigger
                assert not self.timeout_source.start.is_set()
                self.timeout_source.start.set()
                self.last_trigger = value

            elif index ==1: # response
                assert self.timeout_source.start.is_set()
                self.timeout_source.reset.set()
                return [value]
            
            else: #timeout
                return [self._response_if_timeout(self.last_trigger)]

        raise NoUpdate

