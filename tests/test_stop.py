import asyncio
from .fixtures import emit_elements_class
from async_sources.stop import StopLoop

def test_stopper(emit_elements_class):

    loop = asyncio.get_event_loop()

    class TestStopper(StopLoop):
        trigger = 3
        grace_time_sec = 1

        def _check_value(self, value) -> bool:
            return value>=3

    def inner_f():
        emitter = emit_elements_class([1,2,3,4,5,6])
        stopper = TestStopper(emitter)

    loop.call_soon(inner_f)
    loop.run_forever()




