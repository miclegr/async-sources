from asyncio import sleep
from async_sources.source import Source
from async_sources.timeout import TimeoutSource
from .fixtures import *


@pytest.mark.asyncio
async def test_timeout(emit_elements_class):


    class Passthrough(Source):

        def __init__(self, upstream):
            super().__init__(upstream)


        async def _process_update(self, *args):
            val, _ = args[0]
            sleep_for = 0 if val % 2 == 0 else 1.
            await sleep(sleep_for)
            return [val]

    class MyTimeoutSource(TimeoutSource):

        @staticmethod
        def _key_from_trigger(trigger) -> Any:
            return trigger

        @staticmethod
        def _key_from_response(response) -> Any:
            return response

        @staticmethod
        def _response_if_timeout(key):
            return timeout_value

    elements = [0,2,1]
    timeout_value = -1
    timeout_seconds = 0.5
    emitter = emit_elements_class(what=elements, feeding_subscriptions_policy = 'on_demand')
    passthrough = Passthrough(emitter)
    timeout = MyTimeoutSource(emitter, passthrough, timeout_seconds=timeout_seconds)
    subscription = timeout.subscribe()
    emitter.start_feeding_subscriptions()

    data = (await subscription.get()).data
    assert data == elements[0]

    data = (await subscription.get()).data
    assert data == elements[1]

    data = (await subscription.get()).data
    assert data == timeout_value

