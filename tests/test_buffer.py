from .fixtures import *
from async_sources.buffer import BufferSource
from asyncio import sleep
import pytest

@pytest.mark.asyncio
async def test_buffer(emit_elements_class, clock_class):

    data = [1,2]
    source = emit_elements_class(data)
    trigger = clock_class()

    buffered = BufferSource(source, trigger)
    subscription = buffered.subscribe()

    assert (await subscription.get()).data == data

    data = [1,2]
    source = emit_elements_class(data)
    buffered = BufferSource(source, source)
    subscription = buffered.subscribe()

    assert (await subscription.get()).data == data[:1]


