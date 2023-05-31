from .fixtures import *
from async_sources.producers_consumer import InnerConsumer, InnerProducer, producers_and_consumer
from asyncio import Queue, sleep
import pytest

@pytest.mark.asyncio
async def test_producer(emit_elements_class):

    data = [1,2]
    emitter = emit_elements_class(data)

    buffer = Queue()
    producer = InnerProducer(emitter, buffer=buffer)
    
    for d in data:
        d_buffer = await buffer.get()
        assert d == d_buffer

@pytest.mark.asyncio
async def test_consumer(emit_elements_class):

    buffer = Queue()
    data = [1,2]
    for d in data:
        await buffer.put(d)

    consumer = InnerConsumer(buffer)
    subscription = consumer.subscribe()

    for d in data:
        assert (await subscription.get()).data == d

@pytest.mark.asyncio
async def test_producers_consumer(emit_elements_class):

    producer_factory, consumer = producers_and_consumer()

    data1 = [1,2]
    data2 = [3,4]

    emitter1 = emit_elements_class(data1)
    emitter2 = emit_elements_class(data2)

    producer_factory(emitter1)
    producer_factory(emitter2)

    subscription = consumer.subscribe()
    data_consumer = [(await subscription.get()).data for _ in range(4)]

    assert set(data_consumer) == set(data1+data2)

    
