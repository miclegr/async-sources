from .fixtures import emit_elements_class
from async_sources.match_call import MatchAndCall
import pytest
from asyncio import sleep

def test_is_abstract():

    with pytest.raises(Exception):
        MatchAndCall()


@pytest.mark.asyncio
async def test_no_match(emit_elements_class):

    elements_1 = [2,]
    elements_2 = [15,]

    source_1 = emit_elements_class(elements_1)
    source_2 = emit_elements_class(elements_2)

    class TestMatchAndCall(MatchAndCall):

        def _key_arg(self, value, index):
            return 0 if value<10 else 1

        async def _call(self, key, s1, s2):
            return s1,s2

    source = TestMatchAndCall(source_1, source_2)
    subscription = source.subscribe()

    await sleep(0.5)
    assert subscription.queue_size() == 0

@pytest.mark.asyncio
async def test_match(emit_elements_class):

    elements_1 = [2,3]
    elements_2 = [11,10]

    source_1 = emit_elements_class(elements_1)
    source_2 = emit_elements_class(elements_2)

    class TestMatchAndCall(MatchAndCall):

        def _key_arg(self, value, index: int):
            return 0 if value % 2 == 0 else 1

        async def _call(self, key, s1, s2):
            return s1, s2

    source = TestMatchAndCall(source_1, source_2)
    subscription = source.subscribe()

    out = []
    out.append((await subscription.get()).data)
    out.append((await subscription.get()).data)

    assert set(out) == set([(2,10),(3,11)])








