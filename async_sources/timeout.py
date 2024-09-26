from abc import abstractstaticmethod
import asyncio
from .source import NoUpdate, Source
from asyncio import Queue, sleep, Event
from typing import Any, List

class _InnerTimeoutSource(Source):

    def __init__(self, seconds: float, triggers_queue: Queue, feeding_subscriptions_policy: str = 'on_subscribe'):
        self.seconds = seconds

        self.triggers_queue = triggers_queue
        self.triggers_starts = {}
        self.triggers_resets = {}
        self._task_to_trigger = {}
        self._trigger_to_tasks = {}

        self._new_trigger_task = asyncio.create_task(self.triggers_queue.get())
        self._running_tasks = {self._new_trigger_task}
        
        super().__init__(feeding_subscriptions_policy=feeding_subscriptions_policy)

    async def _process_update(self, *args) -> List[Any]:


        results = []
        finished, self._running_tasks = await asyncio.wait(self._running_tasks, return_when=asyncio.FIRST_COMPLETED)
        for task in finished:
            if task.cancelled():
                pass

            elif task is self._new_trigger_task:

                trigger_key = task.result()
                new_start, new_reset = Event(),Event()
                self.triggers_starts[trigger_key] = new_start
                self.triggers_resets[trigger_key] = new_reset

                is_reset, is_timeout = asyncio.create_task(new_reset.wait()), asyncio.create_task(sleep(self.seconds))
                self._task_to_trigger[id(is_reset)] = trigger_key
                self._task_to_trigger[id(is_timeout)] = trigger_key
                self._trigger_to_tasks[trigger_key] = (is_reset, is_timeout)

                self._running_tasks.add(is_reset)
                self._running_tasks.add(is_timeout)
                
                self.triggers_queue.task_done()

                self._new_trigger_task = asyncio.create_task(self.triggers_queue.get())
                self._running_tasks.add(self._new_trigger_task)
            else:
                trigger_key = self._task_to_trigger[id(task)]
                is_reset, is_timeout = self._trigger_to_tasks[trigger_key]

                if task is is_timeout:
                    results.append(trigger_key)
                    is_reset.cancel()
                else:
                    is_timeout.cancel()

                del self.triggers_starts[trigger_key]
                del self.triggers_resets[trigger_key]
                del self._trigger_to_tasks[trigger_key]
                del self._task_to_trigger[id(is_timeout)]
                del self._task_to_trigger[id(is_reset)]
        
        return results


class TimeoutSource(Source):

    def __init__(self, trigger: Source, response: Source, timeout_seconds: float, feeding_subscriptions_policy: str = 'on_subscribe'):

        self.timeout_seconds = timeout_seconds
        self.triggers_queue = Queue()
        self.timeout_source = _InnerTimeoutSource(self.timeout_seconds, self.triggers_queue)
        self.last_trigger = None

        super().__init__(trigger, response, self.timeout_source, feeding_subscriptions_policy=feeding_subscriptions_policy)

    @abstractstaticmethod
    def _response_if_timeout(key) -> Any:
        pass

    @abstractstaticmethod
    def _key_from_trigger(trigger) -> Any:
        pass

    @abstractstaticmethod
    def _key_from_response(response) -> Any:
        pass

    async def _process_update(self, *args) -> List[Any]:
        
        # force trigger to be processed first
        sorted_args = sorted(args, key=lambda x: x[1]) 
        
        returns = []
        for arg in sorted_args:
            value, index = arg

            if index == 0: # trigger
                key = self._key_from_trigger(value)
                self.triggers_queue.put_nowait(key)

            elif index ==1: # response
                await self.triggers_queue.join()
                key = self._key_from_response(value)
                if key in self.timeout_source.triggers_resets:
                    self.timeout_source.triggers_resets[key].set()
                    returns.append(value)
            
            else: #timeout
                returns.append(self._response_if_timeout(value))

        return returns


