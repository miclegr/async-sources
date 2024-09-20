from asyncio import sleep
from async_sources.source import Source
from async_sources.timeout import TimeoutSource
from .fixtures import *


@pytest.mark.asyncio
async def test_timeout(emit_single_element_class):

    value = 1
    timeout_value = 0


    class Passthrough(Source):

        def __init__(self, upstream, sleep_for):
            self.sleep_for = sleep_for
            super().__init__(upstream)

        async def _process_update(self, *args):
            val, _ = args[0]
            await sleep(self.sleep_for)
            return [val]

    class MyTimeoutSource(TimeoutSource):

        @staticmethod
        def _response_if_timeout(last_trigger):
            return timeout_value

    timeout_seconds = 1.
    no_sleep = 0
    emitter = emit_single_element_class(value, feeding_subscriptions_policy = 'on_demand')
    passthrough = Passthrough(emitter, sleep_for=no_sleep)
    timeout = MyTimeoutSource(emitter, passthrough, timeout_seconds=timeout_seconds)
    subscription = timeout.subscribe()
    emitter.start_feeding_subscriptions()

    data = (await subscription.get()).data
    assert data == value

    timeout_seconds = .5
    sleep_for = 1.
    emitter = emit_single_element_class(value, feeding_subscriptions_policy = 'on_demand')
    passthrough = Passthrough(emitter, sleep_for=sleep_for)
    timeout = MyTimeoutSource(emitter, passthrough, timeout_seconds=timeout_seconds)
    subscription = timeout.subscribe()
    emitter.start_feeding_subscriptions()

    data = (await subscription.get()).data
    assert data == timeout_value


